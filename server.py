# server.py
# ELvara AI Receptionist — Twilio <-> OpenAI Realtime Bridge
#
# ✅ This version forces ENGLISH by default and will NOT switch languages
#    unless the caller explicitly asks for Spanish. (Transcription language
#    is pinned to "en"; we also add guardrails in instructions.)
#
# ✅ Includes robust single-response gating (no overlapping responses),
#    barge-in cancel, greet-once with “ELvara” in the line, clean “bye”
#    handling, and reliable booking via your n8n webhooks.
#
# ENV VARS (set in Render):
#   OPENAI_API_KEY
#   TWILIO_ACCOUNT_SID
#   TWILIO_AUTH_TOKEN
#   TRANSFER_NUMBER                (optional, E.164 like +12672134362)
#   PUBLIC_BASE_URL                (e.g., https://your-app.onrender.com)
#   MAIN_WEBHOOK_URL               (defaults to your n8n booking webhook)
#   CANCEL_WEBHOOK_URL             (defaults to your n8n cancel webhook)
#   EVENT_TYPE_ID                  (Cal.com event type id; default 3117986)

import os, json, re, uuid, asyncio, logging, aiohttp
from datetime import datetime, time
from zoneinfo import ZoneInfo
from contextlib import suppress
from enum import Enum
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List

from fastapi import FastAPI, Request, WebSocket
from fastapi.responses import PlainTextResponse
from twilio.twiml.voice_response import VoiceResponse, Dial
from twilio.rest import Client

# ------------- Logging -------------
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("elvara")

# ------------- FastAPI -------------
app = FastAPI()

# ------------- ENV -------------
OPENAI_API_KEY     = os.getenv("OPENAI_API_KEY", "")
TW_SID             = os.getenv("TWILIO_ACCOUNT_SID", "")
TW_TOKEN           = os.getenv("TWILIO_AUTH_TOKEN", "")
TRANSFER_NUMBER    = os.getenv("TRANSFER_NUMBER", "")
PUBLIC_BASE_URL    = os.getenv("PUBLIC_BASE_URL", "")
DEFAULT_TZ         = "America/New_York"
EVENT_TYPE_ID      = int(os.getenv("EVENT_TYPE_ID", "3117986"))

MAIN_WEBHOOK_URL   = os.getenv("MAIN_WEBHOOK_URL", "https://elevara.app.n8n.cloud/webhook/appointment-webhook")
CANCEL_WEBHOOK_URL = os.getenv("CANCEL_WEBHOOK_URL", "https://elevara.app.n8n.cloud/webhook/appointment-reschedule-2step")

tw_client = Client(TW_SID, TW_TOKEN) if (TW_SID and TW_TOKEN) else None

# ------------- State/Models -------------
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
    collected: Dict[str, str] = field(default_factory=dict)          # name/email/phone/startTime
    pending_booking: Optional[Dict[str, Any]] = None
    awaiting_confirmation: bool = False
    greeted: bool = False
    rejoin_reason: Optional[str] = None
    closed: bool = False
    lang: str = "en"  # 'en' by default. Only switch to 'es' on explicit request.

# ------------- Utilities -------------
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

def default_today_anchor_iso() -> str:
    # Anchor to current hour (or 3pm if earlier) in ET
    now = datetime.now(ZoneInfo(DEFAULT_TZ))
    hour = now.hour if now.hour >= 15 else 15
    target = datetime.combine(now.date(), time(hour, 0), tzinfo=ZoneInfo(DEFAULT_TZ))
    return target.isoformat()

# ------------- Business instructions -------------
BRAND = "ELvara"
OWNER = "Jeriel"
MISSION_LINE = "Our mission is growth—handling the systems and strategies that move your business forward."
SERVICES_SHORT = [
    "SEO websites that actually bring customers",
    "AI receptionist & booking",
    "Workflow automations",
    "POS integrations",
    "CRM setup",
    "Social media management",
]
GREETS = [
    "Thank you for calling ELvara. How can I help you today?",
    "Hey—thank you for calling ELvara. How can I help you today?",
    "Hi, thanks for calling ELvara. How may I assist you?",
]

INSTRUCTIONS = f"""
You are the {BRAND} AI receptionist in Philadelphia, PA. Speak warm, concise (1–2 sentences), professional, conversational.

LANGUAGE:
- Respond ONLY in English. Do NOT switch languages unless the caller explicitly asks to speak Spanish.
- If the caller asks for Spanish, acknowledge and switch; otherwise stay in English.
- If you didn’t hear them: say once “Sorry—I didn’t catch that, could you repeat?”; second time add “Calls are clearest off speaker/headphones.”

GREET FIRST (exactly one line, no mission line yet):
- "{GREETS[0]}" OR "{GREETS[1]}" OR "{GREETS[2]}"

MISSION (only if they ask what we do or how we can help):
- "{MISSION_LINE}"

DISCOVERY:
- If they ask “how can you help?”, FIRST ask what business they run, then suggest 1–2 tailored solutions from:
  {", ".join(SERVICES_SHORT)}.
- Keep it outcome-focused. Offer a quick consultation to see what's best.

PRICING:
- Pricing is bespoke and depends on the business. Say: “Everything is tailored—let’s do a free consultation and then we’ll send a quote.”

BOOKING POLICY:
- Do NOT book until:
  1) You've verified a specific time via check_slots, and
  2) You have name, email, phone, AND you have spelled them out and read them back for confirmation.
- Ask callers to spell their full name and email letter by letter. Read them back verbatim and ask “Is that correct?”

TOOLS:
- check_slots(startTime, search_days?): FIRST when a time is discussed. If vague, anchor to the current hour (ET) today.
- appointment_webhook(booking_type, name, email, phone, startTime, notes?): Only AFTER explicit “yes” confirmation of details.
- cancel_workflow(name, phone|email): Prefer phone (use caller ID if they agree).
- transfer_call(to?, reason?): Connect to a human immediately when requested.

Keep answers short; ask one question at a time; always offer a next step.
"""

# ------------- TwiML builders -------------
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

# ------------- Health -------------
@app.get("/")
def health():
    return {
        "ok": True,
        "ai_enabled": bool(OPENAI_API_KEY),
        "twilio_ready": bool(tw_client),
        "transfer_ready": valid_e164(TRANSFER_NUMBER),
        "public_base_url": bool(PUBLIC_BASE_URL),
        "main_webhook": bool(MAIN_WEBHOOK_URL),
        "cancel_webhook": bool(CANCEL_WEBHOOK_URL),
        "event_type_id": EVENT_TYPE_ID,
    }

# ------------- Voice webhook -------------
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
    # Rejoin the media stream if transfer failed
    vr_xml = connect_stream_twiml(PUBLIC_BASE_URL, params={"reason": "transfer_fail"})
    return PlainTextResponse(vr_xml, media_type="application/xml")

# ------------- Response Gate (single active response + cancel) -------------
class ResponseGate:
    """
    Ensures only ONE response is active at a time.
    - Queue new responses while one is active.
    - On barge-in, send response.cancel, then drain queue.
    """
    def __init__(self, oai_ws, log=log):
        self.oai = oai_ws
        self.log = log
        self.queue: asyncio.Queue = asyncio.Queue()
        self.busy = False
        self.active_id: Optional[str] = None
        self.closed = False
        self._drainer: Optional[asyncio.Task] = None

    async def create(self, instructions: str, modalities: list[str] = ["audio", "text"], tool_choice: str = "auto"):
        if self.closed: return
        await self.queue.put({"modalities": modalities, "instructions": instructions, "tool_choice": tool_choice})
        if not self.busy and (self._drainer is None or self._drainer.done()):
            self._drainer = asyncio.create_task(self._drain())

    async def _drain(self):
        if self.closed or self.busy:
            return
        while not self.queue.empty() and not self.closed:
            payload = await self.queue.get()
            self.busy = True
            try:
                self.log.info("OAI: response.create (queued -> sending)")
                await self.oai.send_json({"type": "response.create", "response": payload})
            except Exception as e:
                self.log.error(f"OAI send failed: {e}")
                self.busy = False
                self.active_id = None

    async def on_created(self, resp_id: Optional[str]):
        self.busy = True
        self.active_id = resp_id
        log.info(f"OAI: response.created id={resp_id or 'unknown'}")

    async def on_done(self):
        log.info(f"OAI: response.done id={self.active_id or 'unknown'}")
        self.busy = False
        self.active_id = None
        if not self.queue.empty():
            await self._drain()

    async def cancel(self):
        if self.active_id:
            try:
                log.info(f"OAI: response.cancel id={self.active_id}")
                await self.oai.send_json({"type": "response.cancel", "response_id": self.active_id})
            except Exception as e:
                log.warning(f"OAI cancel failed: {e}")
        self.busy = False
        self.active_id = None

    async def close(self):
        self.closed = True
        while not self.queue.empty():
            with suppress(Exception):
                self.queue.get_nowait()
        if self.active_id:
            with suppress(Exception):
                await self.cancel()

# ------------- Media (Realtime) -------------
@app.websocket("/media")
async def media(ws: WebSocket):
    if not OPENAI_API_KEY:
        log.error("No OPENAI_API_KEY; closing media socket.")
        await ws.close(); return
    await ws.accept()

    # Receive Twilio "start"
    ctx: Optional[CallContext] = None
    try:
        while True:
            first = await asyncio.wait_for(ws.receive_text(), timeout=10)
            data0 = json.loads(first)
            if data0.get("event") == "start":
                s = data0["start"]
                caller = to_e164(s.get("from") or "")
                ctx = CallContext(
                    call_sid=s.get("callSid",""),
                    stream_sid=s["streamSid"],
                    caller_number=caller,
                    caller_last4=last4(caller),
                    greeted=False,
                )
                params = (s.get("customParameters") or {})
                ctx.rejoin_reason = params.get("reason")
                log.info(f"Twilio stream start: streamSid={ctx.stream_sid}, callSid={ctx.call_sid}, caller={ctx.caller_number!r}, reason={ctx.rejoin_reason!r}")
                break
    except Exception as e:
        log.error(f"Twilio start wait failed: {e}")
        await ws.close(); return

    # Connect to OpenAI Realtime
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
        with suppress(Exception): await gate.close()
        with suppress(Exception): await oai.close()
        with suppress(Exception): await session.close()
        with suppress(Exception): await ws.close()

    # Tools schema
    tools = [
        {
            "type":"function",
            "name":"check_slots",
            "description":"Check availability. Call FIRST when caller mentions a time.",
            "parameters":{"type":"object","required":["startTime"],"properties":{
                "startTime":{"type":"string","description":"ISO datetime with offset"},
                "search_days":{"type":"integer","default":14}
            }}
        },
        {
            "type":"function",
            "name":"appointment_webhook",
            "description":"Book/reschedule AFTER identity is spelled/confirmed and a time is confirmed.",
            "parameters":{"type":"object","required":["booking_type","name","email","phone","startTime"],"properties":{
                "booking_type":{"type":"string","enum":["book","reschedule"],"default":"book"},
                "name":{"type":"string"},
                "email":{"type":"string"},
                "phone":{"type":"string"},
                "startTime":{"type":"string"},
                "notes":{"type":"string"},
                "event_type_id":{"type":"integer","default":EVENT_TYPE_ID}
            }}
        },
        {
            "type":"function",
            "name":"cancel_workflow",
            "description":"Cancel appointment (name + phone OR email). Prefer phone; use caller ID if permitted.",
            "parameters":{"type":"object","required":["name"],"properties":{
                "name":{"type":"string"},
                "phone":{"type":"string"},
                "email":{"type":"string"}
            }}
        },
        {
            "type":"function",
            "name":"transfer_call",
            "description":"Transfer to human immediately.",
            "parameters":{"type":"object","properties":{
                "to":{"type":"string"},
                "reason":{"type":"string"}
            }}
        }
    ]

    # Session.update (pin transcription to ENGLISH)
    try:
        now_et = datetime.now(ZoneInfo(DEFAULT_TZ))
        now_line = now_et.strftime("%A, %B %d, %Y, %-I:%M %p %Z")
        await oai.send_json({
            "type":"session.update",
            "session":{
                "turn_detection":{
                    "type":"server_vad",
                    "silence_duration_ms":1100,
                    "create_response":False,       # we gate responses ourselves
                    "interrupt_response":True
                },
                "input_audio_format":"g711_ulaw",
                "output_audio_format":"g711_ulaw",
                "voice":"alloy",
                "modalities":["audio","text"],
                "input_audio_transcription":{"model":"gpt-4o-mini-transcribe","language":"en"},
                "instructions": INSTRUCTIONS + f"\nCURRENT_TIME_ET: {now_line}\nTIMEZONE: {DEFAULT_TZ}\n",
                "tools": tools
            }
        })
    except Exception as e:
        log.error(f"OpenAI session.setup failed: {e}")
        await cleanup(); return

    # Response Gate
    gate = ResponseGate(oai_ws=oai, log=log)

    # State holders
    call_map: Dict[str, Dict[str,str]] = {}
    user_buf: List[str] = []
    pending_transfer: Optional[Dict[str,str]] = None

    # Helpers
    async def say(text: str):
        await gate.create(text)

    async def natural():
        await gate.create("", tool_choice="auto")

    async def json_post(url: str, payload: dict) -> tuple[int, dict | str, str]:
        try:
            async with session.post(url, json=payload, timeout=30) as resp:
                ct = resp.headers.get("content-type","")
                status = resp.status
                body = await (resp.json() if "application/json" in ct else resp.text())
                preview = body if isinstance(body,str) else {k: body.get(k) for k in ("status","matchedSlot","exactMatch","sameDayAlternates","nearest")}
                log.info(f"POST {url} -> {status} {preview}")
                return status, body, ct
        except Exception as e:
            log.error(f"POST {url} failed: {e}")
            return 0, str(e), ""

    # Language switcher (only on explicit ask)
    async def maybe_switch_to_spanish(user_text_lower: str):
        nonlocal ctx
        if ctx.lang == "en" and re.search(r"\b(spanish|español)\b", user_text_lower):
            ctx.lang = "es"
            # Update transcription + a brief acknowledgement in English, then Spanish
            await oai.send_json({
                "type":"session.update",
                "session":{
                    "input_audio_transcription":{"model":"gpt-4o-mini-transcribe","language":"es"},
                    "instructions": INSTRUCTIONS + "\n(Caller requested Spanish. From now on, reply in Spanish.)\n"
                }
            })
            await say("Claro, puedo hablar en español. ¿En qué puedo ayudarte?")
            return True
        return False

    # Pumps ----------------------------------------------------------------
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
                    log.info(f"Twilio stop: {reason!r}")
                    break
        except Exception as e:
            log.info(f"twilio_to_openai ended: {e}")

    async def openai_to_twilio():
        nonlocal pending_transfer
        try:
            # Initial greeting (once)
            if not ctx.greeted:
                ctx.greeted = True
                if ctx.rejoin_reason == "transfer_fail":
                    await say("Looks like Jeriel isn’t available. I can book you for later today or tomorrow—what time works best?")
                else:
                    await say(GREETS[0])

            while True:
                msg = await oai.receive()
                if msg.type == aiohttp.WSMsgType.TEXT:
                    evt = json.loads(msg.data)
                    t = evt.get("type")

                    # Lifecycle -> ResponseGate
                    if t == "response.created":
                        rid = (evt.get("response") or {}).get("id")
                        await gate.on_created(rid)

                    elif t == "response.done":
                        await gate.on_done()
                        # After finishing a transfer message, perform transfer
                        if pending_transfer:
                            target = pending_transfer.get("to") or TRANSFER_NUMBER
                            pending_transfer = None
                            if tw_client and ctx and ctx.call_sid and valid_e164(target):
                                action_url = (PUBLIC_BASE_URL.rstrip("/") + "/twilio/after-transfer") if PUBLIC_BASE_URL else None
                                with suppress(Exception):
                                    tw_client.calls(ctx.call_sid).update(twiml=transfer_twiml(target, action_url=action_url))

                    # Audio out -> Twilio
                    elif t == "response.audio.delta" and ctx and ctx.stream_sid:
                        try:
                            await ws.send_text(json.dumps({"event":"media","streamSid": ctx.stream_sid,"media":{"payload": evt["delta"]}}))
                        except Exception:
                            break

                    # Barge-in: cancel current response
                    elif t == "input_audio_buffer.speech_started":
                        with suppress(Exception):
                            await ws.send_text(json.dumps({"event":"clear","streamSid": ctx.stream_sid}))
                        await gate.cancel()

                    # Transcript aggregation
                    elif t == "conversation.item.input_audio_transcription.delta":
                        user_buf.append(evt.get("delta") or "")

                    elif t == "conversation.item.input_audio_transcription.completed":
                        text = "".join(user_buf).strip(); user_buf.clear()
                        ctx.last_user_input = text
                        low = text.lower()

                        # short / unclear
                        if not text or len(text.split()) <= 2:
                            ctx.misunderstand_count += 1
                            tip = " Calls are clearest off speaker or headphones." if ctx.misunderstand_count >= 2 else ""
                            await say("Sorry—I didn’t catch that, could you repeat?" + tip)
                            continue
                        else:
                            ctx.misunderstand_count = 0

                        # Language: switch to Spanish only if explicitly requested
                        if await maybe_switch_to_spanish(low):
                            continue

                        # Goodbye intent
                        if re.search(r"\b(goodbye|bye|hang\s*up|we'?re\s+done|that'?s\s+all|end\s+the\s+call)\b", low):
                            ctx.state = ConversationState.CLOSING
                            await say("Thanks for calling—have a great day!")
                            ctx.closed = True
                            asyncio.create_task(asyncio.sleep(0.8))
                            break

                        # Transfer intent
                        if re.search(r"\b(transfer|connect|human|agent|representative|manager|owner|operator|jeriel)\b", low):
                            pending_transfer = {"type":"transfer","to": None}
                            await say("Absolutely—one moment while I connect you.")
                            continue

                        # Confirmation gate: if we asked them to confirm details
                        if ctx.awaiting_confirmation:
                            if re.search(r"\b(yes|correct|that'?s\s+right|sounds\s+good|confirm)\b", low):
                                args = dict(ctx.pending_booking or {})
                                ctx.awaiting_confirmation = False
                                ctx.pending_booking = None
                                await handle_tool_call("appointment_webhook", args, force_book=True)
                                continue
                            if re.search(r"\b(no|incorrect|that'?s\s+wrong|change)\b", low):
                                ctx.awaiting_confirmation = False
                                ctx.pending_booking = None
                                await say("No problem—please spell your full name, then your email, letter by letter.")
                                continue

                        # Normal handoff to the model tools/reply
                        await natural()

                    # Tool call framing
                    elif t == "response.output_item.added":
                        item = evt.get("item", {})
                        if item.get("type") == "function_call":
                            call_id = item.get("call_id"); name = item.get("name")
                            if call_id: call_map[call_id] = {"name": name, "args": ""}

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
                        log.error(f"OpenAI error: {evt}")
                        await say("Sorry—something glitched. Could you say that again?")

                else:
                    if msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        break
        except Exception as e:
            log.info(f"openai_to_twilio ended: {e}")

    # Tool handlers ---------------------------------------------------------
    async def handle_tool_call(tool_name: str, args: Dict[str, Any], force_book: bool = False):
        # ---- Availability ----
        if tool_name == "check_slots":
            start_time = (args.get("startTime") or "").strip() or default_today_anchor_iso()
            search_days = int(args.get("search_days") or 14)

            payload = {
                "tool": "checkAvailableSlot",
                "event_type_id": EVENT_TYPE_ID,
                "tz": DEFAULT_TZ,
                "startTime": start_time,
                "search_days": search_days
            }

            status, data, _ = await json_post(MAIN_WEBHOOK_URL, payload)
            body = data if isinstance(data, dict) else {}
            exact   = bool(body.get("exactMatch"))
            matched = body.get("matchedSlot")
            same    = body.get("sameDayAlternates") or []
            near    = body.get("nearest") or []
            first_d = body.get("firstAvailableThatDay")

            # Keep the candidate time in ctx
            ctx.collected["startTime"] = matched or start_time

            if exact and matched:
                ctx.state = ConversationState.CONFIRMING
                await say(f"Great—{matched} Eastern is open. To lock it in, please tell me your full name and email, spelling each letter.")
                return

            # otherwise offer alternates
            options = (same or near)[:3]
            lead = f"The first opening that day is {first_d}. " if first_d else ""
            if options:
                await say(("That exact time isn’t open. " if not exact else "") + lead + f"Closest options: {', '.join(options)}. Which works best?")
            else:
                if search_days < 30:
                    await handle_tool_call("check_slots", {"startTime": start_time, "search_days": 30})
                else:
                    await say("I don’t see anything close to that time. Another day might be better—what day works?")

        # ---- Book / Reschedule ----
        elif tool_name == "appointment_webhook":
            # Allow phone fallback to caller ID
            if not args.get("phone") and ctx.caller_number:
                args["phone"] = ctx.caller_number

            required = ["booking_type","name","email","phone","startTime"]
            missing = [k for k in required if not (args.get(k) or "").strip()]
            if "email" not in missing and not is_valid_email(args.get("email")):
                missing.append("email")

            if missing:
                need_map = {"name":"your name","email":"an email for confirmation","phone":"the best phone number","startTime":"the day and time","booking_type":"whether this is new or reschedule"}
                needed = ", ".join(need_map.get(m,m) for m in missing)
                tail = f" I can use the number ending in {ctx.caller_last4} if that helps." if ("phone" in missing and ctx.caller_last4) else ""
                await say(f"To set that up, I just need {needed}.{tail} You can tell me now.")
                return

            # If not forced, pause to confirm identity & details BEFORE booking
            if not force_book:
                ctx.pending_booking = dict(args)
                ctx.awaiting_confirmation = True
                t = args["startTime"]
                phone_last = last4(args.get("phone"))
                confirm_line = (
                    f"Here’s what I have. Name: {args['name']}. Email: {args['email']}. "
                    f"Phone ending in {phone_last or 'unknown'}. Time: {t} Eastern. "
                    "Is that correct? Please spell your name and email once more to confirm."
                )
                await say(confirm_line)
                return

            # Proceed to book (forced after explicit “yes”)
            payload = {
                "tool": "book",
                "booking_type": (args.get("booking_type") or "book"),
                "name": args["name"].strip(),
                "email": args["email"].strip(),
                "phone": (args["phone"] or "").strip(),
                "tz": DEFAULT_TZ,
                "startTime": args["startTime"].strip(),
                "event_type_id": int(args.get("event_type_id", EVENT_TYPE_ID)),
                "idempotency_key": f"ai-{uuid.uuid4().hex}",
                "notes": (args.get("notes") or None)
            }
            status, data, _ = await json_post(MAIN_WEBHOOK_URL, payload)
            body = data if isinstance(data, dict) else {}
            bstat = (body.get("status") if isinstance(body, dict) else None) or ""
            if status == 200 and bstat == "booked":
                await say("All set—your consultation is on the calendar. You’ll get an email with the details and a calendar invite. Anything else I can help with?")
                ctx.state = ConversationState.BOOKING
            elif status in (409, 422) or bstat in {"conflict","conflict_or_error"}:
                sug = body.get("suggestions") or []
                if sug:
                    await say(f"Looks like that time was taken. I can do {', '.join(sug[:3])}. What works?")
                else:
                    await say("Looks like that time isn’t available. Earlier or later that day?")
            else:
                await say("I couldn’t finalize that just now. Want me to try again or pick a different time?")

        # ---- Cancel ----
        elif tool_name == "cancel_workflow":
            name = (args.get("name") or "").strip()
            phone = (args.get("phone") or "").strip() or ctx.caller_number or ""
            email = (args.get("email") or "").strip()

            if not name or (not phone and not email):
                need = []
                if not name: need.append("name")
                if not (phone or email): need.append("a phone or email")
                await say("To cancel, I just need " + ", ".join(need) + ".")
                return

            payload = {
                "action": "cancel",
                "name": name,
                "phone": phone or None,
                "email": email or None,
                "tz": DEFAULT_TZ
            }
            status, data, _ = await json_post(CANCEL_WEBHOOK_URL, payload)
            body = data if isinstance(data, dict) else {}
            bstat = body.get("status") if isinstance(body, dict) else None
            if status == 200 and bstat in {"cancelled","ok","success"}:
                await say("Done. I’ve canceled that for you. Anything else?")
            elif status == 404 or bstat in {"not_found","conflict_or_error"}:
                await say("I couldn’t find an active booking with that info. Do you use another email or phone?")
            else:
                await say("I hit a snag canceling that. Try again, or I can connect you with our lead.")

        # ---- Transfer ----
        elif tool_name == "transfer_call":
            nonlocal pending_transfer
            pending_transfer = {"to": args.get("to")}
            await say("Absolutely—one moment while I connect you.")

    # Kick off tasks
    t1 = asyncio.create_task(twilio_to_openai())
    t2 = asyncio.create_task(openai_to_twilio())
    done, pending = await asyncio.wait({t1, t2}, return_when=asyncio.FIRST_COMPLETED)
    for p in pending: p.cancel()
    for p in pending:
        with suppress(Exception, asyncio.CancelledError): await p
    await cleanup()
