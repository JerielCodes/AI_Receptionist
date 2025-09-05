"""
ELvara AI Receptionist — Twilio <-> OpenAI Realtime bridge
Python 3.11 / FastAPI / aiohttp

What this file adds/fixes:
- Robust single-response gate (queue + lifecycle): avoids
  "conversation_already_has_active_response".
- Barge-in handling: on user speech, we 'clear' Twilio audio and send
  response.cancel to OpenAI for the active TTS, then queue next reply.
- Greet-once flag: prevents greeting loops when the stream reconnects.
- End-intent (“bye”, etc.) path: say one closing line, then shut down
  cleanly (stop producing responses and close the sockets).
- Booking path: only calls book when check_slots exactMatch AND identity
  (name + valid email + phone) are complete; includes idempotency_key and
  handles 409/422 with alternates prompt.
- Default slot suggestion uses current Eastern time to pick a sensible
  suggestion (today 3pm ET, or next available hour if past 3pm).

Environment variables expected:
  OPENAI_API_KEY, TRANSFER_NUMBER (E.164), TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN,
  MAIN_WEBHOOK_URL (n8n booking), CANCEL_WEBHOOK_URL (n8n cancel, optional),
  EVENT_TYPE_ID (Cal.com event type), PUBLIC_BASE_URL (public https base)

Endpoints:
  POST /twilio/voice          -> returns TwiML <Connect><Stream> to /media
  WS   /twilio/voice/media    -> alias to /media
  WS   /media                 -> Twilio Media Stream <-> OpenAI Realtime bridge
  POST /twilio/after-transfer -> returns to stream if human transfer fails
  GET  /                      -> health

Logs are verbose around response lifecycle and tool calls.
"""

import os, json, asyncio, logging, aiohttp, re, uuid
from datetime import datetime, time
from zoneinfo import ZoneInfo
from contextlib import suppress
from typing import Optional, Dict, Any, List

from fastapi import FastAPI, Request, WebSocket
from fastapi.responses import PlainTextResponse
from twilio.twiml.voice_response import VoiceResponse, Dial
from twilio.rest import Client
from enum import Enum
from dataclasses import dataclass, field

# ---------- LOGGING ----------
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("elvara")

# ---------- APP ----------
app = FastAPI()

# ---------- ENV ----------
OPENAI_API_KEY     = os.getenv("OPENAI_API_KEY", "")
TRANSFER_NUMBER    = os.getenv("TRANSFER_NUMBER", "")
TW_SID             = os.getenv("TWILIO_ACCOUNT_SID", "")
TW_TOKEN           = os.getenv("TWILIO_AUTH_TOKEN", "")
MAIN_WEBHOOK_URL   = os.getenv("MAIN_WEBHOOK_URL", "")   # checkAvailableSlot + book/reschedule
CANCEL_WEBHOOK_URL = os.getenv("CANCEL_WEBHOOK_URL", "")
EVENT_TYPE_ID      = int(os.getenv("EVENT_TYPE_ID", "3117986"))
DEFAULT_TZ         = "America/New_York"
PUBLIC_BASE_URL    = os.getenv("PUBLIC_BASE_URL", "")    # e.g. https://your-app.onrender.com

tw_client = Client(TW_SID, TW_TOKEN) if (TW_SID and TW_TOKEN) else None

# ---------- STATE ----------
class ConversationState(Enum):
    GREETING = "greeting"
    DISCOVERY = "discovery"
    SCHEDULING = "scheduling"
    CONFIRMING = "confirming"
    BOOKING = "booking"
    TRANSFERRING = "transferring"
    CLOSING = "closing"

@dataclass
class CallContext:
    call_sid: str
    stream_sid: str
    caller_number: Optional[str] = None
    caller_last4: Optional[str] = None
    state: ConversationState = ConversationState.GREETING
    last_user_input: str = ""
    misunderstand_count: int = 0
    collected: Dict[str, str] = field(default_factory=dict)  # name/email/phone/startTime
    rejoin_reason: Optional[str] = None
    greeted_once: bool = False
    end_intent_pending: bool = False

# ---------- UTIL ----------
def valid_e164(n: str | None) -> bool:
    return bool(n and re.fullmatch(r"\+\d{7,15}", n))

def to_e164(s: str | None) -> str | None:
    if not s: return None
    s = s.strip()
    if s.startswith("+"):
        digits = re.sub(r"\D", "", s)
        return f"+{digits}" if len(digits) >= 8 else None
    digits = re.sub(r"\D", "", s)
    if len(digits) >= 10:
        return "+1" + digits[-10:]
    return None

def last4(n: str | None) -> str | None:
    if not n: return None
    d = re.sub(r"\D", "", n)
    return d[-4:] if len(d) >= 4 else None

def https_to_wss(url: str) -> str:
    if url.startswith("https://"): return "wss://" + url[len("https://"):]
    if url.startswith("http://"):  return "ws://"  + url[len("http://"):]
    return url

EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$")

def is_valid_email(v: str | None) -> bool:
    return bool(v and EMAIL_RE.fullmatch(v.strip()))

def default_today_start_iso() -> str:
    """Today 3pm ET, or next top-of-hour if already past 3pm (cap 7pm)."""
    now = datetime.now(ZoneInfo(DEFAULT_TZ))
    target = datetime.combine(now.date(), time(15, 0), tzinfo=ZoneInfo(DEFAULT_TZ))
    if now > target:
        hour = min((now.hour + 1), 19)
        target = datetime.combine(now.date(), time(hour, 0), tzinfo=ZoneInfo(DEFAULT_TZ))
    return target.isoformat()

# ---------- BUSINESS KB ----------
def load_kb():
    try:
        with open("business.json", "r") as f:
            return json.load(f)
    except Exception:
        return {}

KB    = load_kb()
brand = KB.get("brand", "ELvara")
loc   = KB.get("location", "Philadelphia, PA")
value_prop = KB.get("value_proposition", {})
elevator_pitch = value_prop.get("elevator_pitch", "We build custom business solutions that save you time and help you grow.")
key_benefits = value_prop.get("key_benefits", []) or ["AI receptionist", "Custom automations", "Professional websites"]
greetings = KB.get("greeting_templates", {}).get("primary", [
    "Hey, thanks for calling ELvara. How can I help you today?",
    "Hi there, this is ELvara. What can I do for you?",
    "Thanks for calling ELvara. What brings you here today?"
])
trial_offer = KB.get("pricing", {}).get("trial_offer", "One-week free trial; free setup/removal; no contracts.")

INSTRUCTIONS = f"""
You are the {brand} AI receptionist based in {loc}.
Default to ENGLISH; only switch languages if the caller speaks them first.
Be warm and concise (1–2 sentences), outcome-focused.

VALUE: {elevator_pitch}
KEY BENEFITS: {' • '.join(key_benefits)}
TRIAL: {trial_offer}

FLOW:
- Greet once. Learn their business + goal. Offer examples. Propose a quick demo.
- When time is mentioned, call check_slots first.
- Book only after you have name + valid email + phone and a confirmed time.
- Read the email back verbatim before booking (“So that’s name@example.com — correct?”).
- If you don’t understand: say once “Sorry—I didn’t catch that, could you repeat?”
  If it happens twice: add “Calls are clearest off speaker/headphones.”

TOOLS (never call with missing fields):
- check_slots(startTime, search_days?): verify availability (default startTime = today 3:00 PM ET; scan next 14 days)
- appointment_webhook(booking_type, name, email, phone, startTime): book after confirmation
- cancel_workflow(name, phone|email): cancel (prefer phone; use caller ID if they agree)
- transfer_call(to?, reason?): connect to a human now
"""

# ---------- TwiML ----------
def transfer_twiml(to_number: str, action_url: str | None = None) -> str:
    vr = VoiceResponse()
    d = Dial(answer_on_bridge=True, action=action_url, method="POST") if action_url else Dial(answer_on_bridge=True)
    d.number(to_number)
    vr.append(d)
    return str(vr)

def connect_stream_twiml(base_url: str, params: dict | None = None) -> str:
    vr = VoiceResponse()
    conn = vr.connect()
    ws_url = https_to_wss(base_url.rstrip("/") + "/media")
    s = conn.stream(url=ws_url)
    if params:
        for k, v in params.items():
            with suppress(Exception):
                s.parameter(name=str(k), value=str(v))
    return str(vr)

# ---------- HEALTH ----------
@app.get("/")
def health():
    return {
        "ok": True,
        "ai_enabled": bool(OPENAI_API_KEY),
        "twilio_ready": bool(tw_client),
        "transfer_ready": valid_e164(TRANSFER_NUMBER),
        "transfer_number": TRANSFER_NUMBER if valid_e164(TRANSFER_NUMBER) else None,
        "main_webhook": bool(MAIN_WEBHOOK_URL),
        "cancel_webhook": bool(CANCEL_WEBHOOK_URL),
        "public_base_url": bool(PUBLIC_BASE_URL),
    }

# ---------- VOICE WEBHOOK ----------
@app.post("/twilio/voice")
async def voice(request: Request):
    host = (
        os.getenv("RENDER_EXTERNAL_HOSTNAME")
        or request.headers.get("x-forwarded-host")
        or request.headers.get("host")
        or request.url.hostname
    )
    base_url = PUBLIC_BASE_URL or f"https://{host}"
    vr_xml = connect_stream_twiml(base_url, params={"reason": "new"})
    log.info(f"Returning TwiML with stream URL: {https_to_wss(base_url.rstrip('/') + '/media')}")
    return PlainTextResponse(vr_xml, media_type="application/xml")

@app.websocket("/twilio/voice/media")
async def media_alias(ws: WebSocket):
    return await media(ws)

# ---------- AFTER-TRANSFER ----------
@app.post("/twilio/after-transfer")
async def after_transfer(request: Request):
    if not PUBLIC_BASE_URL:
        vr = VoiceResponse(); vr.hangup()
        return PlainTextResponse(str(vr), media_type="application/xml")

    form = await request.form()
    status = (form.get("DialCallStatus") or "").lower()
    log.info(f"after-transfer DialCallStatus={status!r}")

    if status in {"completed", "answered"}:
        vr = VoiceResponse(); vr.hangup()
        return PlainTextResponse(str(vr), media_type="application/xml")

    # rejoin media stream
    vr_xml = connect_stream_twiml(PUBLIC_BASE_URL, params={"reason": "transfer_fail"})
    return PlainTextResponse(vr_xml, media_type="application/xml")

# ---------- RESPONSE GATE ----------
class ResponseGate:
    """
    Ensures only ONE OpenAI response is active at a time.
    - Queues outgoing responses; sends next only after 'response.done'.
    - On barge-in, calls response.cancel for the active response.
    - Tracks response ids for logging/telemetry.
    """
    def __init__(self, oai_ws, log):
        self.oai = oai_ws
        self.log = log
        self.queue: asyncio.Queue = asyncio.Queue()
        self.busy: bool = False
        self.active_id: Optional[str] = None
        self.closed: bool = False

    async def create(self, instructions: str | None = None, tool_choice: str = "auto",
                     modalities: list[str] = ["audio", "text"]):
        if self.closed:
            return
        await self.queue.put({
            "modalities": modalities,
            "instructions": instructions or "",
            "tool_choice": tool_choice
        })
        await self._drain()

    async def _drain(self):
        if self.closed or self.busy or self.queue.empty():
            return
        payload = await self.queue.get()
        self.busy = True
        self.active_id = None
        self.log.info(f"[GATE] response.create queued -> sending")
        await self.oai.send_json({"type": "response.create", "response": payload})

    async def on_created(self, resp_id: str):
        self.active_id = resp_id
        self.busy = True
        self.log.info(f"[GATE] response.created id={resp_id}")

    async def on_done(self, terminal: str = "done"):
        self.log.info(f"[GATE] response.{terminal} id={self.active_id}")
        self.busy = False
        self.active_id = None
        await self._drain()

    async def cancel_active(self, reason: str = "barge_in"):
        if self.active_id:
            self.log.info(f"[GATE] response.cancel id={self.active_id} reason={reason}")
            with suppress(Exception):
                await self.oai.send_json({"type": "response.cancel", "response_id": self.active_id})
        # Drop any duplicate queued greetings/lines that got stacked up during overlap
        try:
            while not self.queue.empty():
                _ = self.queue.get_nowait()
        except Exception:
            pass

    def close(self):
        self.closed = True

# ---------- WS: MEDIA ----------
@app.websocket("/media")
async def media(ws: WebSocket):
    if not OPENAI_API_KEY:
        log.error("No OPENAI_API_KEY; closing media socket.")
        await ws.close(); return
    await ws.accept()

    # Twilio 'start'
    ctx: Optional[CallContext] = None
    try:
        while True:
            first = await asyncio.wait_for(ws.receive_text(), timeout=10)
            data0 = json.loads(first)
            if data0.get("event") == "start":
                start = data0["start"]
                caller = to_e164(start.get("from") or "")
                ctx = CallContext(
                    call_sid=start.get("callSid",""),
                    stream_sid=start["streamSid"],
                    caller_number=caller,
                    caller_last4=last4(caller),
                )
                params = (start.get("customParameters") or {})
                ctx.rejoin_reason = params.get("reason")
                log.info(f"Twilio stream start: streamSid={ctx.stream_sid}, callSid={ctx.call_sid}, caller={ctx.caller_number!r}, reason={ctx.rejoin_reason!r}")
                break
    except Exception as e:
        log.error(f"Twilio start wait failed: {e}")
        await ws.close(); return

    # OpenAI Realtime WS
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "OpenAI-Beta": "realtime=v1"}
    try:
        session = aiohttp.ClientSession()
        oai = await session.ws_connect(
            "wss://api.openai.com/v1/realtime?model=gpt-4o-realtime-preview",
            headers=headers, heartbeat=20, max_msg_size=2**23
        )
        log.info("Connected to OpenAI Realtime")
    except Exception as e:
        log.error(f"OpenAI connect failed: {e}")
        if tw_client and ctx and ctx.call_sid and valid_e164(TRANSFER_NUMBER):
            with suppress(Exception):
                action_url = (PUBLIC_BASE_URL.rstrip("/") + "/twilio/after-transfer") if PUBLIC_BASE_URL else None
                tw_client.calls(ctx.call_sid).update(twiml=transfer_twiml(TRANSFER_NUMBER, action_url=action_url))
        await ws.close(); return

    async def cleanup():
        with suppress(Exception): await oai.close()
        with suppress(Exception): await session.close()
        with suppress(Exception): await ws.close()

    # ---------- Tools (schema only; logic in handler) ----------
    tools = [
        {
            "type": "function",
            "name": "check_slots",
            "description": "Check availability (first step). Requires startTime (ISO with offset). Returns exact/alternates.",
            "parameters": {
                "type": "object",
                "required": ["startTime"],
                "properties": {
                    "startTime": {"type": "string", "description": "ISO with offset, e.g. 2025-09-26T15:00:00-04:00"},
                    "search_days": {"type": "integer", "default": 14}
                }
            }
        },
        {
            "type": "function",
            "name": "appointment_webhook",
            "description": "Book/reschedule via main workflow. Call ONLY after check_slots confirmed an exact time and identity is complete.",
            "parameters": {
                "type": "object",
                "required": ["booking_type","name","email","phone","startTime"],
                "properties": {
                    "booking_type": {"type": "string", "enum": ["book","reschedule"], "default": "book"},
                    "name": {"type": "string"},
                    "email": {"type": "string"},
                    "phone": {"type": "string"},
                    "startTime": {"type": "string"},
                    "event_type_id": {"type": "integer", "default": EVENT_TYPE_ID},
                    "notes": {"type": "string"}
                }
            }
        },
        {
            "type": "function",
            "name": "cancel_workflow",
            "description": "Cancel booking (name + phone OR email). Prefer phone.",
            "parameters": {
                "type": "object",
                "required": ["name"],
                "properties": {"name": {"type":"string"}, "phone": {"type":"string"}, "email":{"type":"string"}}
            }
        },
        {"type":"function","name":"transfer_call","description":"Bridge to a human now.","parameters":{"type":"object","properties":{"to":{"type":"string"},"reason":{"type":"string"}}}}
    ]

    # ---------- Session setup ----------
    try:
        now_et = datetime.now(ZoneInfo(DEFAULT_TZ))
        now_line = now_et.strftime("%A, %B %d, %Y, %-I:%M %p %Z")
        await oai.send_json({
            "type": "session.update",
            "session": {
                "turn_detection": {
                    "type": "server_vad",
                    "silence_duration_ms": 1200,
                    "create_response": False,            # we gate responses ourselves
                    "interrupt_response": True
                },
                "input_audio_format": "g711_ulaw",
                "output_audio_format": "g711_ulaw",
                "voice": "alloy",
                "modalities": ["audio","text"],
                "input_audio_transcription": {"model": "gpt-4o-mini-transcribe", "language": "en"},
                "instructions": INSTRUCTIONS + f"\nCURRENT_TIME_ET: {now_line}\nTIMEZONE: {DEFAULT_TZ}\nCALLER_LAST4: {ctx.caller_last4 or 'unknown'}\n",
                "tools": tools
            }
        })
    except Exception as e:
        log.error(f"OpenAI session.setup failed: {e}")
        if tw_client and ctx and ctx.call_sid and valid_e164(TRANSFER_NUMBER):
            with suppress(Exception):
                action_url = (PUBLIC_BASE_URL.rstrip("/") + "/twilio/after-transfer") if PUBLIC_BASE_URL else None
                tw_client.calls(ctx.call_sid).update(twiml=transfer_twiml(TRANSFER_NUMBER, action_url=action_url))
        await cleanup(); return

    # Instantiate the response gate
    gate = ResponseGate(oai_ws=oai, log=log)

    # Send greeting once
    async def send_greeting_once():
        if ctx.greeted_once:
            return
        ctx.greeted_once = True
        if ctx.rejoin_reason == "transfer_fail":
            line = "Looks like no one is available right now. I can book you for later today or tomorrow—what time works? (Eastern Time)"
        else:
            line = ('Say EXACTLY one of: '
                    f'"{greetings[0]}" OR '
                    f'"{greetings[1]}" OR '
                    f'"{greetings[2]}"')
        await gate.create(instructions=line, tool_choice="none")

    await send_greeting_once()

    # ---------- Small helpers ----------
    async def say(text: str):
        await gate.create(instructions=text, tool_choice="none")

    async def respond_naturally():
        # Ask the model to respond using tools if needed
        await gate.create(instructions=None, tool_choice="auto")

    async def json_post(url: str, payload: dict) -> tuple[int, dict | str]:
        try:
            async with session.post(url, json=payload, timeout=25) as resp:
                ct = resp.headers.get("content-type","")
                status = resp.status
                data = await (resp.json() if "application/json" in ct else resp.text())
                preview = data if isinstance(data,str) else {k: data.get(k) for k in ("status","matchedSlot","exactMatch","sameDayAlternates","nearest")}
                log.info(f"POST {url} -> {status} {preview}")
                return status, data
        except Exception as e:
            log.error(f"POST {url} failed: {e}")
            return 0, str(e)

    def missing_fields(required_keys: list[str], args: dict) -> list[str]:
        miss = []
        for k in required_keys:
            v = args.get(k)
            if v is None: miss.append(k); continue
            if isinstance(v,str) and not v.strip(): miss.append(k)
        return miss

    def prompt_for_missing(miss: list[str], caller_last: Optional[str]) -> str:
        labels = {"name":"your name","email":"an email for confirmation","phone":"the best phone number","startTime":"a day and time","booking_type":"whether this is a new booking or reschedule"}
        parts = [labels.get(k,k) for k in miss]
        tail = f" I can use the number ending in {caller_last} if that works." if (caller_last and "phone" in miss) else ""
        return "To set that up I just need " + ", ".join(parts) + "." + tail + " You can tell me now."

    # ---------- Pumps ----------
    call_map: Dict[str, Dict[str,str]] = {}
    user_buf: List[str] = []

    async def twilio_to_openai():
        try:
            while True:
                msg = await ws.receive_text()
                data = json.loads(msg)
                ev = data.get("event")
                if ev == "media":
                    payload = (data.get("media", {}).get("payload") or "")
                    await oai.send_json({"type":"input_audio_buffer.append","audio": payload})
                elif ev == "stop":
                    reason = (data.get("stop") or {}).get("reason")
                    log.info(f"[TWILIO] stop received reason={reason!r}")
                    break
        except asyncio.CancelledError:
            pass
        except Exception as e:
            log.info(f"twilio_to_openai ended: {e}")

    async def openai_to_twilio():
        try:
            while True:
                msg = await oai.receive()
                if msg.type == aiohttp.WSMsgType.TEXT:
                    evt = json.loads(msg.data)
                    t = evt.get("type")

                    # Gate lifecycle hooks
                    if t == "response.created":
                        rid = (evt.get("response") or {}).get("id")
                        await gate.on_created(rid or "unknown")
                    elif t == "response.done":
                        await gate.on_done("done")

                    # assistant → Twilio audio
                    if t == "response.audio.delta" and ctx and ctx.stream_sid:
                        try:
                            await ws.send_text(json.dumps({"event":"media","streamSid": ctx.stream_sid,"media":{"payload": evt["delta"]}}))
                        except Exception:
                            break

                    # barge-in: user started talking -> cancel current speech + clear audio
                    elif t == "input_audio_buffer.speech_started" and ctx and ctx.stream_sid:
                        log.info("[VAD] speech_started -> barge-in cancel & clear")
                        with suppress(Exception):
                            await ws.send_text(json.dumps({"event":"clear","streamSid": ctx.stream_sid}))
                        await gate.cancel_active("barge_in")

                    # user transcript streaming
                    elif t == "conversation.item.input_audio_transcription.delta":
                        user_buf.append(evt.get("delta") or "")

                    # user utterance completed
                    elif t == "conversation.item.input_audio_transcription.completed":
                        text = "".join(user_buf).strip(); user_buf.clear()
                        ctx.last_user_input = text
                        log.info(f"[USER] {text!r}")

                        # end intent handling
                        if re.search(r"\b(hang\s*up|end\s+the\s+call|that'?s\s+all|we'?re\s+done|goodbye|bye)\b", text.lower()):
                            if not ctx.end_intent_pending:
                                ctx.end_intent_pending = True
                                await say("Thanks for calling—have a great day!")
                            # after closing line finishes, end sockets
                            # We'll close when gate becomes idle again.
                            continue

                        # short / unclear
                        if not text or len(text.split()) <= 2:
                            ctx.misunderstand_count += 1
                            tip = " Calls are clearest off speaker or headphones." if ctx.misunderstand_count >= 2 else ""
                            await say("Sorry—I didn’t catch that, could you repeat?" + tip)
                            continue
                        else:
                            ctx.misunderstand_count = 0

                        # transfer intent
                        if re.search(r"\b(transfer|connect|human|agent|representative|manager|owner|live\s+agent|operator|jeriel)\b", text.lower()):
                            await say("Absolutely—one moment while I connect you.")
                            target = TRANSFER_NUMBER
                            if tw_client and ctx and ctx.call_sid and valid_e164(target):
                                action_url = (PUBLIC_BASE_URL.rstrip("/") + "/twilio/after-transfer") if PUBLIC_BASE_URL else None
                                with suppress(Exception):
                                    tw_client.calls(ctx.call_sid).update(twiml=transfer_twiml(target, action_url=action_url))
                            continue

                        # otherwise let model reply/tool
                        await respond_naturally()

                    # function call wiring
                    elif t == "response.output_item.added":
                        item = evt.get("item", {})
                        if item.get("type") == "function_call":
                            cid = item.get("call_id"); name = item.get("name")
                            if cid: call_map[cid] = {"name": name, "args": ""}

                    elif t == "response.function_call_arguments.delta":
                        cid = evt.get("call_id"); delta = evt.get("arguments","")
                        if cid and cid in call_map: call_map[cid]["args"] += delta

                    elif t == "response.function_call_arguments.done":
                        cid = evt.get("call_id")
                        if not cid or cid not in call_map: continue
                        entry = call_map.pop(cid)
                        name  = entry.get("name")
                        try: args = json.loads(entry.get("args") or "{}")
                        except Exception: args = {}
                        await handle_tool_call(name, args)

                    elif t == "error":
                        log.error(f"[OAI ERROR] {evt}")
                        await say("Sorry—something glitched on my end. Could you say that again?")

                elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                    break
        except asyncio.CancelledError:
            pass
        except Exception as e:
            log.info(f"openai_to_twilio ended: {e}")

    async def twilio_heartbeat():
        try:
            while True:
                await asyncio.sleep(10)
                if ctx and ctx.stream_sid:
                    with suppress(Exception):
                        await ws.send_text(json.dumps({"event":"mark","streamSid": ctx.stream_sid,"mark":{"name":"hb"}}))
                # If caller said bye and gate drained, end call
                if ctx.end_intent_pending and not gate.busy and gate.queue.empty():
                    log.info("[END] closing sockets after goodbye")
                    gate.close()
                    await cleanup()
                    return
        except asyncio.CancelledError:
            pass
        except Exception:
            pass

    # ---------- Tool handlers ----------
    async def handle_tool_call(tool_name: str, args: Dict[str, Any]):
        if tool_name == "check_slots":
            start_time = (args.get("startTime") or "").strip()
            search_days = int(args.get("search_days") or 14)
            if not start_time:
                start_time = default_today_start_iso()

            if not MAIN_WEBHOOK_URL:
                await say("I can check availability once my booking line is connected. Meanwhile, what day generally works for you?")
                return

            payload = {
                "tool": "checkAvailableSlot",
                "event_type_id": EVENT_TYPE_ID,
                "tz": DEFAULT_TZ,
                "startTime": start_time,
                "search_days": search_days
            }
            status, data = await json_post(MAIN_WEBHOOK_URL, payload)
            body = data if isinstance(data, dict) else {}
            exact   = bool(body.get("exactMatch"))
            matched = body.get("matchedSlot")
            same    = body.get("sameDayAlternates") or []
            near    = body.get("nearest") or []
            first_d = body.get("firstAvailableThatDay")

            # remember proposed
            ctx.collected["startTime"] = matched or start_time

            have_identity = all([
                bool(ctx.collected.get("name") or args.get("name")),
                is_valid_email(ctx.collected.get("email") or args.get("email")),
                bool(ctx.collected.get("phone") or args.get("phone") or ctx.caller_number)
            ])

            if exact and matched and have_identity:
                # proceed to book
                await handle_tool_call("appointment_webhook", {
                    "booking_type": "book",
                    "name": (ctx.collected.get("name") or args.get("name")),
                    "email": (ctx.collected.get("email") or args.get("email")),
                    "phone": (ctx.collected.get("phone") or args.get("phone") or ctx.caller_number),
                    "startTime": matched
                })
                return

            if exact and matched and not have_identity:
                miss = []
                if not (ctx.collected.get("name") or args.get("name")): miss.append("name")
                if not is_valid_email(ctx.collected.get("email") or args.get("email")): miss.append("email")
                if not (ctx.collected.get("phone") or args.get("phone") or ctx.caller_number): miss.append("phone")
                need = ", ".join(["your name" if m=="name" else "an email for confirmation" if m=="email" else "the best phone number" for m in miss])
                tip = (f" I can use the number ending in {ctx.caller_last4} if that works. " if "phone" in miss and ctx.caller_last4 else " ")
                await say(f"Good news—{matched} Eastern is open. To lock it in I just need {need}.{tip}What should I use?")
                return

            # no exact match
            options = (same or near)[:3]
            lead = f"The first opening that day is {first_d}. " if first_d else ""
            if options:
                await say(("That exact time isn’t open. " if not exact else "") + lead + f"Closest options: {', '.join(options)}. What works best?")
            else:
                # expand search window once
                if search_days < 30:
                    await handle_tool_call("check_slots", {"startTime": start_time, "search_days": 30})
                else:
                    await say("I don’t see anything nearby that time. Another day might be better—what day works?")

        elif tool_name == "appointment_webhook":
            req = ["booking_type","name","email","phone","startTime"]
            if not args.get("phone") and ctx.caller_number:
                args["phone"] = ctx.caller_number

            miss = [k for k in req if not (args.get(k) or "").strip()]
            if "email" not in miss and not is_valid_email(args.get("email")):
                miss.append("email")

            if miss:
                await say(prompt_for_missing(miss, ctx.caller_last4))
                return

            if not MAIN_WEBHOOK_URL:
                await say("My booking line isn’t connected yet. I can take your details and have Jeriel confirm shortly.")
                return

            email = args["email"].strip()
            await say(f"Just to confirm, your email is {email}, correct?")

            payload = {
                "tool": "book",
                "booking_type": args.get("booking_type","book"),
                "name": args["name"].strip(),
                "email": email,
                "phone": (args["phone"] or "").strip(),
                "tz": DEFAULT_TZ,
                "startTime": args["startTime"].strip(),
                "event_type_id": int(args.get("event_type_id", EVENT_TYPE_ID)),
                "idempotency_key": f"ai-{uuid.uuid4().hex}",
                "notes": (args.get("notes") or None)
            }
            status, data = await json_post(MAIN_WEBHOOK_URL, payload)
            body_status = data.get("status") if isinstance(data, dict) else None
            phone_last  = last4(payload["phone"])

            if status == 200 and body_status == "booked":
                await say("All set. I’ve scheduled that in Eastern Time. "
                          f"I have your email as {payload['email']}"
                          + (f" and phone ending in {phone_last}" if phone_last else "")
                          + ". Anything else I can help with?")
            elif status in (409, 422) or body_status in {"conflict","conflict_or_error"}:
                await say("Looks like that time isn’t available. Earlier or later that day, or a nearby day?")
            else:
                await say("I couldn’t finalize that just now. Want me to try again or pick a different time?")

        elif tool_name == "cancel_workflow":
            need_name = not (args.get("name") or "").strip()
            have_contact = (args.get("phone") or ctx.caller_number or args.get("email"))
            if need_name or not have_contact:
                need = ["name"] if need_name else []
                if not have_contact: need.append("a phone or email")
                await say("To cancel, I just need " + ", ".join(need) + ".")
                return
            if not CANCEL_WEBHOOK_URL:
                await say("I can cancel that once my cancel line is connected. Want me to connect you to the Business Solutions Lead?")
                return
            payload = {
                "action": "cancel",
                "name": (args.get("name") or "").strip(),
                "phone": args.get("phone") or ctx.caller_number or None,
                "email": (args.get("email") or None)
            }
            status, data = await json_post(CANCEL_WEBHOOK_URL, payload)
            phone_last = last4(payload.get("phone"))
            body_status = data.get("status") if isinstance(data, dict) else None
            if status == 200 and body_status in {"cancelled","ok","success"}:
                await say("Done. I’ve canceled your appointment"
                          + (f" for the number ending in {phone_last}" if phone_last else "")
                          + (f" and email {payload.get('email')}" if payload.get("email") else "")
                          + ". Anything else?")
            elif status == 404 or body_status in {"not_found","conflict_or_error"}:
                await say("I couldn’t find an active booking for that info. Do you use another email or phone?")
            else:
                await say("I hit a snag canceling that. Try again or connect you to the lead?")

        elif tool_name == "transfer_call":
            await say("Absolutely—one moment while I connect you.")
            target = args.get("to") or TRANSFER_NUMBER
            if tw_client and ctx and ctx.call_sid and valid_e164(target):
                action_url = (PUBLIC_BASE_URL.rstrip("/") + "/twilio/after-transfer") if PUBLIC_BASE_URL else None
                with suppress(Exception):
                    tw_client.calls(ctx.call_sid).update(twiml=transfer_twiml(target, action_url=action_url))

    # ---------- Run pumps ----------
    t1 = asyncio.create_task(twilio_to_openai())
    t2 = asyncio.create_task(openai_to_twilio())
    t3 = asyncio.create_task(twilio_heartbeat())
    done, pending = await asyncio.wait({t1, t2, t3}, return_when=asyncio.FIRST_COMPLETED)
    for p in pending: p.cancel()
    for p in pending:
        with suppress(Exception, asyncio.CancelledError): await p
    await cleanup()
