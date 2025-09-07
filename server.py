# server.py
"""
ELvara AI Receptionist — Twilio <-> OpenAI Realtime bridge (stable)

What this build fixes:
- Language lock: ENGLISH only unless the caller explicitly requests Spanish.
- Greet-once: “Thank you for calling ELvara...” exactly once, never re-greets.
- No pre-emptive booking: tools only run AFTER a real user utterance & intent.
- Robust response gate: one active response at a time (no overlap). Cancels
  only when an active response exists (no response_cancel_not_active spam).
- Clean end intent: single goodbye → hang up via Twilio REST and close sockets.
- Booking path: check → collect/confirm by spelling name/email → read-back →
  ask “Is that correct?” → only then book (idempotency key). Handles 409/422.

Env you can set (or rely on the defaults below):
- OPENAI_API_KEY
- TRANSFER_NUMBER           (E.164, e.g. +12672134362)
- TWILIO_ACCOUNT_SID / TWILIO_AUTH_TOKEN
- MAIN_WEBHOOK_URL          (defaults to your n8n booking endpoint)
- CANCEL_WEBHOOK_URL        (defaults to your n8n cancel endpoint)
- EVENT_TYPE_ID             (Cal.com type id)
- PUBLIC_BASE_URL           (e.g. https://your-app.onrender.com)
"""

import os, json, asyncio, logging, aiohttp, re, uuid
from datetime import datetime, time
from zoneinfo import ZoneInfo
from contextlib import suppress
from fastapi import FastAPI, Request, WebSocket
from fastapi.responses import PlainTextResponse
from twilio.twiml.voice_response import VoiceResponse, Dial
from twilio.rest import Client
from enum import Enum
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List

# ---------- LOGGING ----------
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("elvara")

app = FastAPI()

# ---------- ENV ----------
OPENAI_API_KEY     = os.getenv("OPENAI_API_KEY", "")
TRANSFER_NUMBER    = os.getenv("TRANSFER_NUMBER", "")
TW_SID             = os.getenv("TWILIO_ACCOUNT_SID", "")
TW_TOKEN           = os.getenv("TWILIO_AUTH_TOKEN", "")
# Defaults to your provided n8n endpoints
MAIN_WEBHOOK_URL   = os.getenv("MAIN_WEBHOOK_URL", "https://elevara.app.n8n.cloud/webhook/appointment-webhook")
CANCEL_WEBHOOK_URL = os.getenv("CANCEL_WEBHOOK_URL", "https://elevara.app.n8n.cloud/webhook/appointment-reschedule-2step")
EVENT_TYPE_ID      = int(os.getenv("EVENT_TYPE_ID", "3117986"))
DEFAULT_TZ         = "America/New_York"
PUBLIC_BASE_URL    = os.getenv("PUBLIC_BASE_URL", "")

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
    greeted: bool = False
    state: ConversationState = ConversationState.GREETING
    last_user_input: str = ""
    misunderstand_count: int = 0
    lang: str = "en"  # hard-lock to English unless explicitly switched
    # Scheduling intent gating
    heard_any_user_utterance: bool = False
    user_intends_scheduling: bool = False
    # Identity & confirmation
    pending_name: Optional[str] = None
    pending_email: Optional[str] = None
    pending_phone: Optional[str] = None
    pending_time_iso: Optional[str] = None
    awaiting_confirmation: bool = False  # waiting for "yes / correct" after read-back
    closing: bool = False

# ---------- UTILS ----------
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
    now = datetime.now(ZoneInfo(DEFAULT_TZ))
    # If it’s before 3pm, propose 3pm; else next whole hour (up to 7pm) as anchor
    anchor = datetime.combine(now.date(), time(15, 0), tzinfo=ZoneInfo(DEFAULT_TZ))
    if now > anchor:
        hour = min(now.hour + 1, 19)
        anchor = datetime.combine(now.date(), time(hour, 0), tzinfo=ZoneInfo(DEFAULT_TZ))
    return anchor.isoformat()

def detect_schedule_intent(txt: str) -> bool:
    low = txt.lower()
    return any(w in low for w in [
        "book", "schedule", "reschedule", "set up", "appointment",
        "today", "tomorrow", "monday", "tuesday", "wednesday", "thursday", "friday",
        "am", "pm", "noon", "evening", "morning"
    ])

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
greetings = KB.get("greeting_templates", {}).get("primary", [
    "Thank you for calling ELvara. How can I help you today?",
    "Hey—thanks for calling ELvara. How can I help you today?",
    "Hi, thanks for calling ELvara. How may I assist you?"
])

elevator = (KB.get("value_proposition", {}).get("elevator_pitch")
            or "We build custom business solutions that save you time and help you grow—AI receptionists, booking systems, SEO websites, and workflow automations.")
mission_slogan = "Our mission is growth—handling the systems and strategies that move your business forward."
pricing_line = "Everything is tailored to your business. We start with a free consultation, then provide a quote that fits exactly what you need."

solutions_short = "We help with SEO websites, AI booking, automations, POS, CRM, and social media management."

# ---------- INSTRUCTIONS ----------
INSTRUCTIONS = f"""
System rules for the assistant voice:
- Speak ENGLISH ONLY unless the caller explicitly asks to switch languages. Do NOT switch on your own.
- Say the brand name exactly as “ELvara”.
- Keep replies brief (1–2 sentences) unless asked for more.
- If asked “what do you do?”, mention: “{mission_slogan}” and briefly list: {solutions_short}.
- If asked “how can you help?”, ask what business they run FIRST, then offer 1–2 specific solutions from: SEO websites, AI booking, automations, POS, CRM, social media management.
- If asked about price: say pricing is tailored, free consultation first; then a custom quote. Offer to book a call.
- For unclear audio, if you don’t understand: say once “Sorry—I didn’t catch that, could you repeat?”
  If it happens twice: add “Calls are clearest off speaker/headphones.”

Scheduling guardrails:
- NEVER call tools until you hear at least one user utterance (a real sentence) AND they show scheduling intent.
- When collecting details, ask the caller to SPELL their full name and email letter-by-letter.
- ALWAYS read back the name and email and ask “Is that correct?” before booking.
- Book only after they say yes/correct.

Transfer:
- If they ask for a human/owner/representative, say “Absolutely—one moment while I connect you.” then transfer.
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

# ---------- TWILIO VOICE WEBHOOK ----------
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

# ---------- AFTER-TRANSFER CALLBACK ----------
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
    vr_xml = connect_stream_twiml(PUBLIC_BASE_URL, params={"reason": "transfer_fail"})
    return PlainTextResponse(vr_xml, media_type="application/xml")

# ---------- MEDIA STREAM ----------
@app.websocket("/media")
async def media(ws: WebSocket):
    if not OPENAI_API_KEY:
        log.error("No OPENAI_API_KEY; closing media socket.")
        await ws.close(); return
    await ws.accept()

    # Twilio 'start' → create context
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
                rejoin_reason = params.get("reason")
                log.info(f"Twilio stream start: streamSid={ctx.stream_sid}, callSid={ctx.call_sid}, caller={ctx.caller_number!r}, reason={rejoin_reason!r}")
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

    # ---------- Response gate (single-flight queue) ----------
    send_lock = asyncio.Lock()
    active_response_id: Optional[str] = None
    assistant_busy: bool = False

    async def send_response(payload: Dict[str, Any]):
        nonlocal assistant_busy, active_response_id
        async with send_lock:
            log.info("OAI: response.create (queued -> sending)")
            await oai.send_json({"type": "response.create", "response": payload})

    async def cancel_active_response():
        nonlocal assistant_busy, active_response_id
        if assistant_busy and active_response_id:
            try:
                await oai.send_json({"type": "response.cancel", "response": {"id": active_response_id}})
                log.info(f"OAI: response.cancel id={active_response_id}")
            except Exception as e:
                log.error(f"OAI cancel failed: {e}")

    # ---------- Tools ----------
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
                    "create_response": False,  # we create responses through the gate
                    "interrupt_response": True
                },
                "input_audio_format": "g711_ulaw",
                "output_audio_format": "g711_ulaw",
                "voice": "alloy",
                "modalities": ["audio","text"],
                # HARD LOCK transcription to English
                "input_audio_transcription": {"model": "gpt-4o-mini-transcribe", "language": "en"},
                "instructions": (
                    INSTRUCTIONS +
                    f"\nCURRENT_TIME_ET: {now_line}\nTIMEZONE: {DEFAULT_TZ}\n"
                    "Remember: reply in ENGLISH unless the caller explicitly asks to switch.\n"
                ),
                "tools": tools
            }
        })

        # Initial greeting (once)
        greet_text = greetings[0]
        await send_response({"modalities": ["audio","text"], "instructions": greet_text})
        ctx.greeted = True

    except Exception as e:
        log.error(f"OpenAI session.setup failed: {e}")
        if tw_client and ctx and ctx.call_sid and valid_e164(TRANSFER_NUMBER):
            with suppress(Exception):
                action_url = (PUBLIC_BASE_URL.rstrip("/") + "/twilio/after-transfer") if PUBLIC_BASE_URL else None
                tw_client.calls(ctx.call_sid).update(twiml=transfer_twiml(TRANSFER_NUMBER, action_url=action_url))
        await cleanup(); return

    # ---------- State holders ----------
    call_map: Dict[str, Dict[str,str]] = {}
    user_buf: List[str] = []
    closing_sent = False

    # ---------- Helper: POST ----------
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

    # ---------- Twilio→OpenAI ----------
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
                    log.info(f"Twilio stop: {reason}")
                    break
        except asyncio.CancelledError:
            pass
        except Exception as e:
            log.info(f"twilio_to_openai ended: {e}")

    # ---------- OpenAI→Twilio ----------
    async def openai_to_twilio():
        nonlocal assistant_busy, active_response_id, closing_sent
        try:
            while True:
                msg = await oai.receive()
                if msg.type == aiohttp.WSMsgType.TEXT:
                    evt = json.loads(msg.data)
                    t = evt.get("type")

                    # Lifecycle
                    if t == "response.created":
                        assistant_busy = True
                        rid = (evt.get("response") or {}).get("id")
                        if rid: active_response_id = rid
                        log.info(f"OAI: response.created id={active_response_id or 'unknown'}")

                    elif t == "response.done":
                        rid = (evt.get("response") or {}).get("id")
                        log.info(f"OAI: response.done id={rid or active_response_id or 'unknown'}")
                        assistant_busy = False
                        active_response_id = None
                        if ctx.closing and not closing_sent:
                            closing_sent = True
                            # End call via Twilio then cleanup
                            if tw_client and ctx.call_sid:
                                with suppress(Exception):
                                    tw_client.calls(ctx.call_sid).update(status="completed")
                            await asyncio.sleep(0.2)
                            break

                    # Stream audio to Twilio
                    elif t == "response.audio.delta" and ctx and ctx.stream_sid:
                        try:
                            await ws.send_text(json.dumps({"event":"media","streamSid": ctx.stream_sid,"media":{"payload": evt["delta"]}}))
                        except Exception:
                            break

                    # Barge-in → clear and cancel ONLY if active
                    elif t == "input_audio_buffer.speech_started" and ctx and ctx.stream_sid:
                        with suppress(Exception):
                            await ws.send_text(json.dumps({"event":"clear","streamSid": ctx.stream_sid}))
                        await cancel_active_response()

                    # Collect transcript
                    elif t == "conversation.item.input_audio_transcription.delta":
                        user_buf.append(evt.get("delta") or "")

                    elif t == "conversation.item.input_audio_transcription.completed":
                        text = "".join(user_buf).strip(); user_buf.clear()
                        if not text:
                            continue

                        ctx.heard_any_user_utterance = True
                        ctx.last_user_input = text
                        low = text.lower()

                        # Language switch ONLY if explicitly asked
                        if ("spanish" in low) or ("español" in low):
                            ctx.lang = "es"
                            await send_response({"modalities":["audio","text"], "instructions":
                                "Claro. Puedo hablar en español. ¿En qué negocio trabajas y qué objetivo tienes?"})
                            continue

                        # Clarity rules
                        if len(text.split()) <= 2:
                            ctx.misunderstand_count += 1
                            tip = " Calls are clearest off speaker or headphones." if ctx.misunderstand_count >= 2 else ""
                            await send_response({"modalities":["audio","text"], "instructions": "Sorry—I didn’t catch that, could you repeat?" + tip})
                            continue
                        else:
                            ctx.misunderstand_count = 0

                        # Detect scheduling intent for gating
                        if detect_schedule_intent(text):
                            ctx.user_intends_scheduling = True

                        # Transfer intent
                        if re.search(r"\b(transfer|connect|human|agent|representative|manager|owner|live\s+agent|operator|jeriel)\b", low):
                            await send_response({"modalities":["audio","text"], "instructions": "Absolutely—one moment while I connect you."})
                            # After done, do the transfer
                            if tw_client and ctx.call_sid and valid_e164(TRANSFER_NUMBER):
                                with suppress(Exception):
                                    action_url = (PUBLIC_BASE_URL.rstrip("/") + "/twilio/after-transfer") if PUBLIC_BASE_URL else None
                                    tw_client.calls(ctx.call_sid).update(twiml=transfer_twiml(TRANSFER_NUMBER, action_url=action_url))
                            continue

                        # End intent
                        if re.search(r"\b(hang\s*up|end\s+the\s+call|that'?s\s+all|we'?re\s+done|goodbye|bye)\b", low):
                            ctx.closing = True
                            await send_response({"modalities":["audio","text"], "instructions": "Thanks for calling—have a great day!"})
                            continue

                        # Q: “what do you do?”
                        if re.search(r"\b(what\s+do\s+you\s+do|what\s+can\s+you\s+do|how\s+do\s+you\s+help|what\s+are\s+you)\b", low):
                            await send_response({"modalities":["audio","text"], "instructions":
                                f"{mission_slogan} We build SEO websites, AI booking, automations, POS, CRM, and social media systems that fit your business. What kind of business are you running?"})
                            continue

                        # Q: pricing
                        if re.search(r"\b(price|pricing|cost|how\s+much)\b", low):
                            await send_response({"modalities":["audio","text"], "instructions":
                                f"{pricing_line} If you’d like, I can book a quick consultation—what day and time work?"})
                            ctx.user_intends_scheduling = True
                            continue

                        # If they asked “how can you help?”
                        if re.search(r"\b(how\s+can\s+you\s+help|help\s+me)\b", low):
                            await send_response({"modalities":["audio","text"], "instructions":
                                "Happy to help—what kind of business do you run? I’ll suggest the best mix of SEO site, AI booking, automations, POS, CRM, or social media."})
                            continue

                        # If they want to schedule but didn’t give a time yet
                        if ctx.user_intends_scheduling and not ctx.pending_time_iso:
                            await send_response({"modalities":["audio","text"], "instructions":
                                "What day and time work best? If you’d like, we can look at options starting from this afternoon."})
                            continue

                        # Otherwise, natural follow-up
                        await send_response({"modalities":["audio","text"], "instructions":
                            "Got it. Tell me a bit about your business and your current goals—then we’ll pick the best next step."})

                    # Tool call wiring
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
                        log.error(f"OpenAI error: {evt}")
                        await send_response({"modalities":["audio","text"], "instructions":
                            "Sorry—something glitched on my end. Could you say that again?"})

                elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                    break
        except asyncio.CancelledError:
            pass
        except Exception as e:
            log.info(f"openai_to_twilio ended: {e}")

    # ---------- Tool handlers ----------
    async def handle_tool_call(tool_name: str, args: Dict[str, Any]):
        # BLOCK tools until we heard a real utterance & intent
        if not ctx.heard_any_user_utterance or (tool_name.startswith("check") and not ctx.user_intends_scheduling):
            await send_response({"modalities":["audio","text"], "instructions":
                "Happy to set that up. What day and time work best?"})
            return

        if tool_name == "check_slots":
            start_time = (args.get("startTime") or "").strip()
            search_days = int(args.get("search_days") or 14)
            if not start_time:
                start_time = default_today_anchor_iso()

            ctx.pending_time_iso = start_time  # keep anchor

            if not MAIN_WEBHOOK_URL:
                await send_response({"modalities":["audio","text"], "instructions":
                    "I can check availability once my booking line is connected. Meanwhile, what day generally works?"})
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

            if exact and matched:
                # Ask for identity with spelling instructions
                ctx.pending_time_iso = matched
                ctx.state = ConversationState.CONFIRMING
                await send_response({"modalities":["audio","text"], "instructions":
                    f"{matched} Eastern is available. To lock it in, please SPELL your full name slowly."})
                return

            # no exact match
            options = (same or near)[:3]
            lead = f"The first opening that day is {first_d}. " if first_d else ""
            if options:
                await send_response({"modalities":["audio","text"], "instructions":
                    (("That exact time isn’t open. " if not exact else "") + lead +
                     f"Closest options: {', '.join(options)}. What works best?")})
            else:
                if search_days < 30:
                    # expand window once
                    await handle_tool_call("check_slots", {"startTime": start_time, "search_days": 30})
                else:
                    await send_response({"modalities":["audio","text"], "instructions":
                        "I don’t see anything nearby that time. Another day might be better—what day works?"})

        elif tool_name == "appointment_webhook":
            # We only call this AFTER explicit confirm
            req = ["booking_type","name","email","phone","startTime"]

            # Accumulate from ctx if missing
            name  = (args.get("name") or ctx.pending_name or "").strip()
            email = (args.get("email") or ctx.pending_email or "").strip()
            phone = (args.get("phone") or ctx.pending_phone or ctx.caller_number or "").strip()
            start = (args.get("startTime") or ctx.pending_time_iso or "").strip()
            btype = (args.get("booking_type") or "book").strip()

            miss = [k for k,v in {"name":name,"email":email,"phone":phone,"startTime":start}.items() if not v]
            if email and not is_valid_email(email):
                miss.append("email")

            if miss:
                labels = {"name":"your name (spelled)","email":"an email address (spelled)","phone":"a phone number","startTime":"the day and time"}
                need = ", ".join(labels.get(m,m) for m in miss)
                await send_response({"modalities":["audio","text"], "instructions":
                    f"To finalize, I just need {need}. You can say them now—please spell name and email."})
                return

            # Read-back step if we haven’t confirmed yet
            if not ctx.awaiting_confirmation:
                ctx.awaiting_confirmation = True
                ctx.pending_name  = name
                ctx.pending_email = email
                ctx.pending_phone = phone
                ctx.pending_time_iso = start
                last = last4(phone)
                await send_response({"modalities":["audio","text"], "instructions":
                    f"Just to confirm, I heard: name {name}; email {email}; phone ending in {last or 'unknown'}; time {start} Eastern. Is that correct?"})
                return

            # They said yes/correct → proceed to book
            ctx.awaiting_confirmation = False
            payload = {
                "tool": "book",
                "booking_type": btype,
                "name": name,
                "email": email,
                "phone": phone,
                "tz": DEFAULT_TZ,
                "startTime": start,
                "event_type_id": EVENT_TYPE_ID,
                "idempotency_key": f"ai-{uuid.uuid4().hex}"
            }
            status, data = await json_post(MAIN_WEBHOOK_URL, payload)
            body_status = data.get("status") if isinstance(data, dict) else None
            phone_last  = last4(phone)

            if status == 200 and body_status == "booked":
                await send_response({"modalities":["audio","text"], "instructions":
                    ("All set. I’ve scheduled that in Eastern Time. "
                     f"I have your email as {email}" + (f" and phone ending in {phone_last}" if phone_last else "") +
                     ". Anything else I can help with?")})
                ctx.state = ConversationState.BOOKING
            elif status in (409, 422) or body_status in {"conflict","conflict_or_error"}:
                await send_response({"modalities":["audio","text"], "instructions":
                    "Looks like that time isn’t available. Earlier or later that day, or a nearby day?"})
            else:
                await send_response({"modalities":["audio","text"], "instructions":
                    "I couldn’t finalize that just now. Want me to try again or pick a different time?"})

        elif tool_name == "cancel_workflow":
            name = (args.get("name") or "").strip()
            phone = (args.get("phone") or ctx.caller_number or "").strip()
            email = (args.get("email") or "").strip()
            if not name or not (phone or email):
                need = []
                if not name: need.append("name")
                if not (phone or email): need.append("a phone or email")
                await send_response({"modalities":["audio","text"], "instructions":
                    "To cancel, I just need " + ", ".join(need) + "."})
                return
            if not CANCEL_WEBHOOK_URL:
                await send_response({"modalities":["audio","text"], "instructions":
                    "I can cancel that once my cancel line is connected. Want me to connect you to the Business Solutions Lead?"})
                return
            payload = {"action":"cancel", "name":name, "phone":phone or None, "email": email or None}
            status, data = await json_post(CANCEL_WEBHOOK_URL, payload)
            phone_last = last4(phone)
            body_status = data.get("status") if isinstance(data, dict) else None
            if status == 200 and body_status in {"cancelled","ok","success"}:
                await send_response({"modalities":["audio","text"], "instructions":
                    ("Done. I’ve canceled your appointment"
                     + (f" for the number ending in {phone_last}" if phone_last else "")
                     + (f" and email {email}" if email else "")
                     + ". Anything else?")})
            elif status == 404 or body_status in {"not_found","conflict_or_error"}:
                await send_response({"modalities":["audio","text"], "instructions":
                    "I couldn’t find an active booking for that info. Do you use another email or phone?"})
            else:
                await send_response({"modalities":["audio","text"], "instructions":
                    "I hit a snag canceling that. Try again or connect you to the lead?"})

        elif tool_name == "transfer_call":
            await send_response({"modalities":["audio","text"], "instructions": "Absolutely—one moment while I connect you."})
            if tw_client and ctx.call_sid and valid_e164(TRANSFER_NUMBER):
                with suppress(Exception):
                    action_url = (PUBLIC_BASE_URL.rstrip("/") + "/twilio/after-transfer") if PUBLIC_BASE_URL else None
                    tw_client.calls(ctx.call_sid).update(twiml=transfer_twiml(TRANSFER_NUMBER, action_url=action_url))

    # ---------- Pumps ----------
    t1 = asyncio.create_task(twilio_to_openai())
    t2 = asyncio.create_task(openai_to_twilio())
    done, pending = await asyncio.wait({t1, t2}, return_when=asyncio.FIRST_COMPLETED)
    for p in pending: p.cancel()
    for p in pending:
        with suppress(Exception, asyncio.CancelledError): await p
    await cleanup()
