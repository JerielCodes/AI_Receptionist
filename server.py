# server.py
import os, json, asyncio, logging, aiohttp, re, uuid
from contextlib import suppress
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from fastapi import FastAPI, Request, WebSocket
from fastapi.responses import PlainTextResponse
from twilio.twiml.voice_response import VoiceResponse, Dial
from twilio.rest import Client

# ---------- LOGGING ----------
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("elvara")

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
PUBLIC_BASE_URL    = os.getenv("PUBLIC_BASE_URL", "")    # e.g. https://ai-receptionist-xxxx.onrender.com

tw_client = Client(TW_SID, TW_TOKEN) if (TW_SID and TW_TOKEN) else None

def valid_e164(n: str | None) -> bool:
    return bool(n and re.fullmatch(r"\+\d{7,15}", n))

def to_e164(s: str | None) -> str | None:
    if not s:
        return None
    s = s.strip()
    if s.startswith("+"):
        digits = re.sub(r"\D", "", s)
        return f"+{digits}" if len(digits) >= 8 else None
    digits = re.sub(r"\D", "", s)
    if len(digits) >= 10:
        return "+1" + digits[-10:]
    return None

def last4(n: str | None) -> str | None:
    if not n:
        return None
    d = re.sub(r"\D", "", n)
    return d[-4:] if len(d) >= 4 else None

def https_to_wss(url: str) -> str:
    if url.startswith("https://"): return "wss://" + url[len("https://"):]
    if url.startswith("http://"):  return "ws://"  + url[len("http://"):]
    return url

# ---------- BUSINESS PROMPT ----------
def load_kb():
    try:
        with open("business.json", "r") as f:
            return json.load(f)
    except Exception:
        return {}

KB    = load_kb()
brand = KB.get("brand", "ELvara")
loc   = KB.get("location", "Philadelphia, PA")
vals  = " • ".join(KB.get("value_props", [])) or "Custom AI booking • SEO websites • Workflow automations"
trial = KB.get("trial_offer", "One-week free trial; install/uninstall is free.")

# Short, flexible pitch
PITCH_SHORT_EN = ("We build tailored solutions that save time and help you grow—"
                  "from custom AI-integrated booking and CRM automations to SEO websites and apps that keep you organized. "
                  "Tell me what kind of business you run or the goal you have, and I’ll make it specific.")
PITCH_SHORT_ES = ("Creamos soluciones a la medida para ahorrar tiempo y ayudarte a crecer—"
                  "desde reservas con IA y automatizaciones CRM hasta sitios SEO y apps para organizarte. "
                  "Cuéntame qué tipo de negocio tienes o tu objetivo y te doy ideas concretas.")

def now_context():
    et = datetime.now(ZoneInfo(DEFAULT_TZ))
    utc = datetime.now(timezone.utc)
    return {
        "now_utc_iso": utc.isoformat(timespec="seconds"),
        "now_et_iso": et.isoformat(timespec="seconds"),
        "now_et_date": et.strftime("%Y-%m-%d"),
        "now_et_time": et.strftime("%H:%M"),
        "now_et_weekday": et.strftime("%A"),
    }

def build_instructions(now_ctx: dict, caller_number: str | None, caller_last4: str | None) -> str:
    INSTRUCTIONS = (
        f"You are the {brand} AI receptionist (company based in {loc}). "
        "Start in ENGLISH. Only switch to Spanish if the caller explicitly asks. "
        "Tone: warm, professional, friendly, concise (1–2 sentences unless asked). "
        f"Value props: {vals}. Offer the trial when interest is shown: {trial}. "
        "Never invent business facts; if unsure, say so and offer to connect the caller. "
        "PRIMARY GOALS: (1) Explain benefits & answer questions; (2) Book a demo/installation; "
        "(3) Transfer to a human on request; (4) Think like a teammate focused on converting qualified leads."
        "\n\nTIME CONTEXT (authoritative; do not guess):\n"
        f" - NOW_UTC: {now_ctx['now_utc_iso']}\n"
        f" - NOW_ET:  {now_ctx['now_et_iso']} (Eastern Time)\n"
        f" - TODAY_ET: {now_ctx['now_et_date']} ({now_ctx['now_et_weekday']}) at {now_ctx['now_et_time']}\n"
        "All scheduling is in Eastern Time. If the caller mentions durations like “in two hours,” evaluate against NOW_ET."
        "\n\nTOOLS (when & how to use):\n"
        " 1) check_slots(startTime, search_days=14, ...)\n"
        "    • ALWAYS call this first before booking.\n"
        "    • Required: startTime (ISO with offset). If the caller gave only a time-of-day (e.g., “3pm”), assume TODAY in Eastern Time.\n"
        "    • If caller didn’t specify a day: default to TODAY at the stated time.\n"
        "    • If nothing is available and no alternates are returned, try again with search_days=30.\n"
        "    • If exactMatch=true: book matchedSlot (after you have name + email + phone).\n"
        "    • If exactMatch=false: offer same-day alternates and nearest options, then book the chosen one.\n"
        "    Examples:\n"
        "     - Caller: “Can you do 12:30?” → call check_slots with startTime=TODAY 12:30 ET.\n"
        "     - Caller: “Sometime tomorrow afternoon” → pick 3:00 PM ET tomorrow and call check_slots.\n"
        "\n 2) appointment_webhook(booking_type, name, email, phone, startTime, ...)\n"
        "    • Only call after check_slots indicates the selected time is available OR the caller has chosen an alternate.\n"
        "    • Required fields: booking_type ('book' or 'reschedule'), name, email, phone (prefer callerID), startTime (ISO with offset).\n"
        "    • After success: briefly read back name, email, last-4 phone, and date/time in ET.\n"
        "    Example: After exactMatch=true and you have identity → book matchedSlot silently.\n"
        "\n 3) cancel_workflow(name, phone|email)\n"
        "    • Need name + (phone OR email). Prefer phone (use callerID when appropriate).\n"
        "\n 4) transfer_call(to?, reason?)\n"
        "    • Use when the caller asks for a person/human or is unsure and wants a consult.\n"
        "    • Say one short line first: “Absolutely—one moment while I connect you.”\n"
        "    • If no one answers and the call returns to you, offer to book for later today or tomorrow.\n"
        "\n 5) end_call(reason?)\n"
        "    • Only if the caller clearly asked to end.\n"
        "\nBOOKING FLOW SUMMARY:\n"
        " • Collect name, email, and phone (confirm by repeating the last 4 digits). Use timezone 'America/New_York'.\n"
        " • check_slots with the requested time. If exact and identity present → appointment_webhook immediately.\n"
        " • If not exact → propose the firstAvailableThatDay, sameDayAlternates, or nearest options; book chosen.\n"
        " • Keep responses 1–2 sentences; be natural and focused on outcomes (missed calls, follow-ups, scheduling, payments, job tracking).\n"
        "\nGREETING (choose one line, first turn):\n"
        "  “Hey, thank you for calling ELvara. What can I help you with?”\n"
        "  “Hey—thanks for calling ELvara. How can I help you today?”\n"
        "  “Hi, thanks for calling ELvara. How may I assist you?”\n"
        "\nPITCH EXAMPLES:\n"
        f"  EN: {PITCH_SHORT_EN}\n"
        f"  ES: {PITCH_SHORT_ES}\n"
        "\nRUNTIME CONTEXT:\n"
        f" - caller_id_e164={caller_number or 'unknown'}\n"
        f" - caller_id_last4={caller_last4 or 'unknown'}\n"
        f" - ALWAYS use timezone: {DEFAULT_TZ}\n" +
        (
            f" - If phone is missing, propose using caller_id_e164 and confirm: “Is this the best number ending in {caller_last4}?”\n"
            if caller_last4 else
            " - If phone is missing, ask for it directly and confirm back the digits.\n"
        ) +
        "LANGUAGE LOCK: Respond only in English unless the caller explicitly asks for Spanish."
    )
    return INSTRUCTIONS

# ---------- TwiML builders ----------
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
    et = datetime.now(ZoneInfo(DEFAULT_TZ)).isoformat(timespec="seconds")
    return {
        "ok": True,
        "ai_enabled": bool(OPENAI_API_KEY),
        "twilio_ready": bool(tw_client),
        "transfer_ready": valid_e164(TRANSFER_NUMBER),
        "transfer_number": TRANSFER_NUMBER if valid_e164(TRANSFER_NUMBER) else None,
        "main_webhook": bool(MAIN_WEBHOOK_URL),
        "cancel_webhook": bool(CANCEL_WEBHOOK_URL),
        "public_base_url": bool(PUBLIC_BASE_URL),
        "now_et": et
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

# Accept Twilio’s alternate path if configured that way
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

    # No-answer / busy / failed → reconnect stream
    vr_xml = connect_stream_twiml(PUBLIC_BASE_URL, params={"reason": "transfer_fail"})
    return PlainTextResponse(vr_xml, media_type="application/xml")

# ---------- MEDIA STREAM ----------
@app.websocket("/media")
async def media(ws: WebSocket):
    if not OPENAI_API_KEY:
        log.error("No OPENAI_API_KEY; closing media socket.")
        await ws.close(); return
    await ws.accept()

    # Twilio 'start'
    stream_sid = call_sid = caller_number = caller_last4 = None
    rejoin_reason = None
    try:
        while True:
            first = await asyncio.wait_for(ws.receive_text(), timeout=10)
            data0 = json.loads(first)
            if data0.get("event") == "start":
                stream_sid    = data0["start"]["streamSid"]
                call_sid      = data0["start"].get("callSid", "")
                caller_raw    = data0["start"].get("from") or ""
                caller_number = to_e164(caller_raw)
                caller_last4  = last4(caller_number)
                params        = (data0["start"].get("customParameters") or {})
                rejoin_reason = params.get("reason")
                log.info(f"Twilio stream start: streamSid={stream_sid}, callSid={call_sid}, caller={caller_number!r}, last4={caller_last4!r}, reason={rejoin_reason!r}")
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
        if tw_client and call_sid and valid_e164(TRANSFER_NUMBER):
            with suppress(Exception):
                action_url = (PUBLIC_BASE_URL.rstrip("/") + "/twilio/after-transfer") if PUBLIC_BASE_URL else None
                tw_client.calls(call_sid).update(twiml=transfer_twiml(TRANSFER_NUMBER, action_url=action_url))
        await ws.close(); return

    async def cleanup():
        with suppress(Exception): await oai.close()
        with suppress(Exception): await session.close()
        with suppress(Exception): await ws.close()

    # ---------- Tools ----------
    tools = [
        {
            "type": "function",
            "name": "check_slots",
            "description": "Check availability first. Returns exact/alternates and can auto-book if exact.",
            "parameters": {
                "type": "object",
                "required": ["startTime"],
                "properties": {
                    "startTime": {"type": "string", "description": "Requested ISO with offset, e.g., 2025-09-26T11:30:00-04:00. If only time-of-day is known (e.g. '15:00' or '3pm'), send that and server will assume TODAY in ET."},
                    "search_days": {"type": "integer", "default": 14},
                    "booking_type": {"type": "string", "enum": ["book", "reschedule"], "default": "book"},
                    "name": {"type": "string"},
                    "email": {"type": "string"},
                    "phone": {"type": "string", "description": "E.164 like +15551234567"},
                    "event_type_id": {"type": "integer", "default": EVENT_TYPE_ID},
                    "notes": {"type": "string", "description": "Optional lead notes to pass to the sheet"}
                }
            }
        },
        {
            "type": "function",
            "name": "appointment_webhook",
            "description": "Book or reschedule via main n8n workflow.",
            "parameters": {
                "type": "object",
                "required": ["booking_type", "name", "email", "phone", "startTime"],
                "properties": {
                    "booking_type": {"type": "string", "enum": ["book", "reschedule"]},
                    "name": {"type": "string"},
                    "email": {"type": "string"},
                    "phone": {"type": "string", "description": "E.164 like +15551234567"},
                    "startTime": {"type": "string", "description": "ISO 8601 with offset"},
                    "event_type_id": {"type": "integer", "default": EVENT_TYPE_ID},
                    "notes": {"type": "string"}
                }
            }
        },
        {
            "type": "function",
            "name": "cancel_workflow",
            "description": "Cancel booking using name + (phone OR email). Prefer phone.",
            "parameters": {
                "type": "object",
                "required": ["name"],
                "properties": {
                    "name": {"type": "string"},
                    "phone": {"type": "string", "nullable": True},
                    "email": {"type": "string", "nullable": True}
                }
            }
        },
        {"type": "function","name": "transfer_call","description": "Bridge to a human now.","parameters": {"type": "object","properties": {"to": {"type": "string"},"reason": {"type": "string"}}}},
        {"type": "function","name": "end_call","description": "Politely end the call.","parameters": {"type": "object","properties": {"reason": {"type": "string"}}}},
    ]

    # ---------- Session setup ----------
    try:
        now_ctx = now_context()
        base_instructions = build_instructions(now_ctx, caller_number, caller_last4)
        await oai.send_json({
            "type": "session.update",
            "session": {
                "turn_detection": {"type": "server_vad","silence_duration_ms": 900,"create_response": True,"interrupt_response": True},
                "input_audio_format": "g711_ulaw",
                "output_audio_format": "g711_ulaw",
                "voice": "alloy",
                "modalities": ["audio", "text"],
                # Lock ASR to English initially
                "input_audio_transcription": {"model": "gpt-4o-mini-transcribe", "language": "en"},
                "instructions": base_instructions,
                "tools": tools
            }
        })
        if rejoin_reason == "transfer_fail":
            await oai.send_json({"type": "response.create","response": {"modalities": ["audio","text"],"tool_choice": "auto","instructions": (
                "Looks like no one is available right now. If you’d like, I can book you for later today or tomorrow. "
                "What time works? (Eastern Time)")}})
        else:
            await oai.send_json({"type": "response.create","response": {"modalities": ["audio","text"],"tool_choice": "auto","instructions":
                ("Start in English. Say EXACTLY one of: "
                 "\"Hey, thank you for calling ELvara. What can I help you with?\" "
                 "OR \"Hey—thanks for calling ELvara. How can I help you today?\" "
                 "OR \"Hi, thanks for calling ELvara. How may I assist you?\"")}})
    except Exception as e:
        log.error(f"OpenAI session.setup failed: {e}")
        if tw_client and call_sid and valid_e164(TRANSFER_NUMBER):
            with suppress(Exception):
                action_url = (PUBLIC_BASE_URL.rstrip("/") + "/twilio/after-transfer") if PUBLIC_BASE_URL else None
                tw_client.calls(call_sid).update(twiml=transfer_twiml(TRANSFER_NUMBER, action_url=action_url))
        await cleanup(); return

    # ---------- State ----------
    call_map: dict[str, dict] = {}
    user_buf: list[str] = []
    pending_action: dict | None = None
    reply_lang: str = "en"
    hangup_ok: bool = False  # guard for end_call

    async def lock_language(new_lang: str):
        nonlocal reply_lang
        if new_lang not in ("en", "es") or new_lang == reply_lang:
            return
        reply_lang = new_lang
        label = "English" if new_lang == "en" else "Spanish"
        log.info(f"LANGUAGE LOCK → {label}")
        now_ctx = now_context()
        await oai.send_json({"type": "session.update","session": {
            "instructions": build_instructions(now_ctx, caller_number, caller_last4).replace(
                "LANGUAGE LOCK: Respond only in English unless the caller explicitly asks for Spanish.",
                f"LANGUAGE LOCK: Respond only in {label}."
            ),
            "input_audio_transcription": {"model": "gpt-4o-mini-transcribe", "language": new_lang}
        }})

    # ---------- Twilio actions ----------
    def do_transfer(reason: str | None = None, to: str | None = None):
        target = to or TRANSFER_NUMBER
        if not (tw_client and call_sid and valid_e164(target)):
            log.error(f"TRANSFER blocked (client? {bool(tw_client)} call? {bool(call_sid)} target_valid? {valid_e164(target)})")
            return
        log.info(f"TRANSFERRING via Twilio REST -> {target} reason={reason!r}")
        action_url = (PUBLIC_BASE_URL.rstrip("/") + "/twilio/after-transfer") if PUBLIC_BASE_URL else None
        with suppress(Exception):
            tw_client.calls(call_sid).update(twiml=transfer_twiml(target, action_url=action_url))

    def do_hangup(reason: str | None = None):
        if not (tw_client and call_sid):
            log.error("HANGUP blocked (missing Twilio client or call_sid)")
            return
        log.info(f"HANGUP via Twilio REST. reason={reason!r}")
        with suppress(Exception):
            tw_client.calls(call_sid).update(status="completed")

    async def announce_then(action: dict):
        nonlocal pending_action
        say = ("Claro—dame un momento mientras te conecto." if action.get("type") == "transfer"
               else "Gracias por llamar—voy a finalizar la llamada ahora.") if reply_lang=="es" \
              else ("Absolutely—one moment while I connect you." if action.get("type") == "transfer"
               else "Thanks for calling—ending the call now.")
        pending_action = action
        log.info(f"ANNOUNCE queued → {action}")
        await oai.send_json({"type": "response.create","response": {"modalities": ["audio","text"], "instructions": say}})

    # ---------- Helper: JSON POST ----------
    async def json_post(url: str, payload: dict) -> tuple[int, dict | str]:
        try:
            async with session.post(url, json=payload, timeout=20) as resp:
                ct = (resp.headers.get("content-type") or "")
                status = resp.status
                if "application/json" in ct:
                    try:
                        data = await resp.json()
                    except Exception:
                        data = (await resp.text()) or ""
                else:
                    data = (await resp.text()) or ""
                # safe preview
                if isinstance(data, dict):
                    preview = {k: data.get(k) for k in ("status","code","event_id","start_iso","matchedSlot","exactMatch")}
                else:
                    preview = (data[:160] + "…") if isinstance(data, str) and len(data) > 160 else data
                log.info(f"POST {url} -> {status} {preview}")
                return status, data
        except Exception as e:
            log.error(f"POST {url} failed: {e}")
            return 0, str(e)

    # ---------- Time parsing helpers for startTime ----------
    TIME_ONLY_RE = re.compile(r"^\s*(\d{1,2})(?::(\d{2}))?\s*(am|pm)?\s*$", re.IGNORECASE)

    def to_et_iso_from_time_only(time_str: str) -> str | None:
        """
        Accepts '3', '15', '3pm', '15:30', '3:30pm' and returns TODAY's ET ISO with offset.
        If ambiguous (1-12 w/o am/pm), we assume 24h only if >=13; else we DEFAULT TO PM for 1-11 (sales-friendly).
        """
        m = TIME_ONLY_RE.match(time_str or "")
        if not m:
            return None
        hh = int(m.group(1))
        mm = int(m.group(2) or "0")
        ampm = (m.group(3) or "").lower()
        if ampm == "am":
            hour24 = 0 if hh == 12 else hh
        elif ampm == "pm":
            hour24 = 12 if hh == 12 else hh + 12
        else:
            # no am/pm
            if hh >= 13:
                hour24 = hh
            elif hh == 12:
                hour24 = 12
            else:
                # assume PM for 1..11 to bias toward business hours
                hour24 = hh + 12
        et_now = datetime.now(ZoneInfo(DEFAULT_TZ))
        dt = et_now.replace(hour=hour24, minute=mm, second=0, microsecond=0)
        return dt.isoformat()

    def normalize_start_time(start_time_raw: str) -> str | None:
        """
        If full ISO with offset -> return as-is (validated by parse attempt).
        If only time-of-day -> convert to TODAY ET ISO.
        Otherwise return None.
        """
        s = (start_time_raw or "").strip()
        if not s:
            return None
        # Try ISO parse
        try:
            # If no timezone info present, reject (we only accept with tz or time-only pattern)
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                return None
            return dt.isoformat()
        except Exception:
            pass
        # Try time-only
        iso = to_et_iso_from_time_only(s)
        return iso

    # ---------- Utility: field validation & prompts ----------
    def missing_fields(required_keys: list[str], args: dict) -> list[str]:
        miss = []
        for k in required_keys:
            v = args.get(k)
            if v is None: miss.append(k); continue
            if isinstance(v, str) and not v.strip(): miss.append(k)
        return miss

    def prompt_for_missing(miss: list[str], caller_last4: str | None, lang: str) -> str:
        # Compact, natural prompts in the right language
        need = []
        label_map_en = {"name":"your name","email":"email for confirmation","phone":"best phone number","startTime":"a day and time","booking_type":"whether this is a new booking or reschedule"}
        label_map_es = {"name":"tu nombre","email":"tu correo para confirmación","phone":"tu número de teléfono","startTime":"un día y hora","booking_type":"si es una nueva cita o reprogramación"}
        for k in miss:
            need.append((label_map_es if lang=="es" else label_map_en).get(k,k))
        if lang == "es":
            tail = f" ¿Es correcto el número que termina en {caller_last4}? " if (caller_last4 and "phone" in miss) else ""
            return "Para programarlo necesito " + ", ".join(need) + "." + tail + "Puedes decírmelo cuando gustes."
        else:
            tail = f" Is the number ending in {caller_last4} the best one? " if (caller_last4 and "phone" in miss) else ""
            return "To set that up I just need " + ", ".join(need) + "." + tail + "You can tell me now."

    # ---------- Pumps ----------
    async def twilio_to_openai():
        try:
            while True:
                msg = await ws.receive_text()
                data = json.loads(msg)
                ev = data.get("event")
                if ev == "media":
                    payload = (data.get("media", {}).get("payload") or "")
                    await oai.send_json({"type": "input_audio_buffer.append", "audio": payload})
                elif ev == "stop":
                    log.info(f"Twilio sent stop.")
                    break
        except asyncio.CancelledError:
            pass
        except Exception as e:
            log.info(f"twilio_to_openai ended: {e}")

    async def openai_to_twilio():
        nonlocal pending_action, reply_lang, hangup_ok
        try:
            while True:
                msg = await oai.receive()
                if msg.type == aiohttp.WSMsgType.TEXT:
                    evt = json.loads(msg.data)
                    t = evt.get("type")
                    log.info(f"OAI EVT: {t}")

                    if t == "conversation.item.input_audio_transcription.delta":
                        user_buf.append(evt.get("delta") or "")
                    if t == "response.audio_transcript.delta":
                        pass

                    if t == "response.audio.delta" and stream_sid:
                        try:
                            await ws.send_text(json.dumps({"event": "media","streamSid": stream_sid,"media": {"payload": evt["delta"]}}))
                        except Exception:
                            break

                    elif t == "input_audio_buffer.speech_started" and stream_sid:
                        with suppress(Exception):
                            await ws.send_text(json.dumps({"event": "clear", "streamSid": stream_sid}))

                    elif t == "response.done" and pending_action:
                        action = pending_action; pending_action = None
                        if action.get("type") == "transfer": do_transfer(reason=action.get("reason"), to=action.get("to"))
                        else: do_hangup(reason=action.get("reason"))

                    elif t == "conversation.item.input_audio_transcription.completed":
                        text = "".join(user_buf).strip(); user_buf.clear()
                        if text:
                            low = text.lower()
                            # explicit language requests only
                            if re.search(r"\b(en\s+español|habla\s+español|puedes\s+hablar\s+español)\b", low):
                                await lock_language("es")
                            elif re.search(r"\b(in\s+english|speak\s+english|english\s+please)\b", low):
                                await lock_language("en")

                            # intents
                            if re.search(r"(transfer|connect|human|agent|representative|manager|owner|person\s+in\s+charge|live\s+agent|operator)\b", low):
                                await announce_then({"type":"transfer","reason":"user_requested_transfer"})
                            elif re.search(r"(hang\s*up|end\s+the\s+call|that's\s+all|bye|good\s*bye|not\s+interested|we're\s+done|colgar|terminar.*llamada|ad(i|í)os)", low):
                                hangup_ok = True
                                await announce_then({"type":"hangup","reason":"user_requested_hangup"})

                    elif t == "response.output_item.added":
                        item = evt.get("item", {})
                        if item.get("type") == "function_call":
                            call_id = item.get("call_id")
                            name    = item.get("name")
                            if call_id:
                                call_map.setdefault(call_id, {"name": name, "args": ""})

                    elif t == "response.function_call_arguments.delta":
                        cid   = evt.get("call_id")
                        delta = evt.get("arguments", "")
                        if cid:
                            entry = call_map.setdefault(cid, {"name": None, "args": ""})
                            entry["args"] += delta

                    elif t == "response.function_call_arguments.done":
                        cid = evt.get("call_id")
                        if not cid: continue
                        info = call_map.pop(cid, {"name": None, "args": ""})
                        name = info.get("name")
                        raw  = info.get("args") or ""
                        try:
                            args = json.loads(raw or "{}")
                        except Exception:
                            args = {}
                        log.info(f"TOOL (fn_args) COMPLETE: {name} args={args}")

                        # ---- Guard: collect missing fields BEFORE hitting webhooks ----
                        if name == "appointment_webhook":
                            req = ["booking_type","name","email","phone","startTime"]
                            miss = missing_fields(req, args)
                            # allow phone fallback to callerID
                            if "phone" in miss and caller_number:
                                args["phone"] = caller_number
                                miss = [m for m in miss if m != "phone"]
                            if miss:
                                prompt = prompt_for_missing(miss, last4(args.get("phone") or caller_number), reply_lang)
                                await oai.send_json({"type": "response.create","response": {"modalities": ["audio","text"], "instructions": prompt}})
                                continue  # do not call webhook with missing data

                        if name == "cancel_workflow":
                            # need at least name + (phone OR email)
                            base_miss = []
                            if not (args.get("name") or ""): base_miss.append("name")
                            if not ((args.get("phone") or caller_number) or args.get("email")):
                                base_miss.append("a phone or email")
                            if base_miss:
                                prompt = ("To cancel, I just need " + ", ".join(base_miss) + ". "
                                          + (f"Is the number ending in {last4(caller_number)} yours?" if caller_number else ""))
                                if reply_lang=="es":
                                    prompt = ("Para cancelar, necesito " + ", ".join(base_miss) + ". "
                                              + (f"¿El número que termina en {last4(caller_number)} es tuyo?" if caller_number else ""))
                                await oai.send_json({"type":"response.create","response":{"modalities":["audio","text"],"instructions":prompt}})
                                continue

                        # ---- Tools proper ----
                        if name == "check_slots":
                            # Normalize startTime: accept ISO-with-offset OR time-only (assume TODAY ET)
                            raw_start = (args.get("startTime") or "").strip()
                            norm_start = normalize_start_time(raw_start)
                            if not norm_start:
                                ask = ("What day and time should I check (Eastern)? For example: "
                                       "'Sep 26 at 12:30pm' or 'today 3pm'. "
                                       "If you only say a time, I’ll assume today.")
                                if reply_lang == "es":
                                    ask = ("¿Qué día y hora verifico (horario del Este)? Por ejemplo: "
                                           "'26 sep a las 12:30pm' o 'hoy 3pm'. "
                                           "Si solo dices la hora, asumiré hoy.")
                                await oai.send_json({
                                    "type": "response.create",
                                    "response": {"modalities": ["audio","text"], "instructions": ask}
                                })
                                continue
                            search_days = int(args.get("search_days") or 14)
                            payload_chk = {
                                "tool": "checkAvailableSlot",
                                "event_type_id": int(args.get("event_type_id", EVENT_TYPE_ID)),
                                "tz": DEFAULT_TZ,
                                "startTime": norm_start,
                                "search_days": search_days
                            }
                            s1, d1 = await json_post(MAIN_WEBHOOK_URL, payload_chk)
                            body = d1 if isinstance(d1, dict) else {}
                            exact     = bool(body.get("exactMatch"))
                            matched   = body.get("matchedSlot")
                            same_day  = body.get("sameDayAlternates") or []
                            nearest   = body.get("nearest") or []
                            first_day = body.get("firstAvailableThatDay")

                            # If no options at all, try auto-extend to 30 days once
                            if (not exact) and (not same_day) and (not nearest) and search_days < 30:
                                payload_chk["search_days"] = 30
                                s1b, d1b = await json_post(MAIN_WEBHOOK_URL, payload_chk)
                                body = d1b if isinstance(d1b, dict) else {}
                                exact     = bool(body.get("exactMatch"))
                                matched   = body.get("matchedSlot")
                                same_day  = body.get("sameDayAlternates") or []
                                nearest   = body.get("nearest") or []
                                first_day = body.get("firstAvailableThatDay")

                            have_identity = all([
                                (args.get("name") or "").strip(),
                                (args.get("email") or "").strip(),
                                (args.get("phone") or caller_number or "").strip()
                            ])

                            # Exact + identity ⇒ auto-book
                            if exact and matched and have_identity:
                                payload_book = {
                                    "tool": "book",
                                    "booking_type": args.get("booking_type", "book"),
                                    "name": (args.get("name") or "").strip(),
                                    "email": (args.get("email") or "").strip(),
                                    "phone": (args.get("phone") or caller_number or "").strip(),
                                    "tz": DEFAULT_TZ,
                                    "startTime": matched,
                                    "event_type_id": int(args.get("event_type_id", EVENT_TYPE_ID)),
                                    "idempotency_key": f"ai-{uuid.uuid4().hex}",
                                    "notes": (args.get("notes") or None)
                                }
                                s2, d2 = await json_post(MAIN_WEBHOOK_URL, payload_book)
                                phone_last = last4(payload_book["phone"])
                                ok = (isinstance(d2, dict) and d2.get("status") == "booked" and s2 == 200)
                                if ok:
                                    txt_en = (f"All set. I booked {matched} Eastern Time. "
                                              f"I have your email as {payload_book['email']}"
                                              + (f" and phone ending in {phone_last}" if phone_last else "")
                                              + ". A calendar invite is on its way. Anything else?")
                                    txt_es = (f"Listo. Reservé {matched} hora del Este. "
                                              f"Tengo tu correo como {payload_book['email']}"
                                              + (f" y el número terminando en {phone_last}" if phone_last else "")
                                              + ". Te llegará la invitación del calendario. ¿Algo más?")
                                    await oai.send_json({"type":"response.create","response":{"modalities":["audio","text"],"instructions": txt_es if reply_lang=="es" else txt_en}})
                                else:
                                    txt = ("No pude finalizarlo ahora mismo. ¿Intento de nuevo o prefieres otra hora?"
                                           if reply_lang=="es" else
                                           "I couldn’t finalize that just now. Want me to try again or pick a different time?")
                                    await oai.send_json({"type":"response.create","response":{"modalities":["audio","text"],"instructions": txt}})
                                continue

                            # Exact but missing identity ⇒ compact ask then we’ll book
                            if exact and matched and not have_identity:
                                miss = []
                                if not (args.get("name") or "").strip():  miss.append("name")
                                if not (args.get("email") or "").strip(): miss.append("email")
                                if not (args.get("phone") or caller_number or "").strip(): miss.append("phone")
                                phone_last = last4(caller_number)
                                if reply_lang == "es":
                                    ask = (f"Buena noticia: {matched} (hora del Este) está disponible. "
                                           f"Para reservarlo necesito " +
                                           ", ".join(["tu nombre" if m=="name" else "tu correo" if m=="email" else "tu número" for m in miss]) +
                                           (f". Puedo usar el número que termina en {phone_last} si te sirve. " if "phone" in miss and phone_last else ". ") +
                                           "¿Me lo compartes?")
                                else:
                                    ask = (f"Good news—{matched} Eastern is open. "
                                           f"To lock it in I just need your " +
                                           ", ".join(["name" if m=="name" else "email" if m=="email" else "best phone" for m in miss]) +
                                           (f". I can use the number ending in {phone_last} if that works. " if "phone" in miss and phone_last else ". ") +
                                           "What should I use?")
                                await oai.send_json({"type":"response.create","response":{"modalities":["audio","text"],"instructions": ask}})
                                continue

                            # No exact match ⇒ offer alternates
                            lead_en = f"The first opening that day is {first_day}. " if first_day else ""
                            lead_es = f"La primera apertura ese día es {first_day}. " if first_day else ""
                            options = (same_day or nearest)[:3]

                            if reply_lang == "es":
                                txt = (("Esa hora exacta no está libre. " + lead_es) if not exact else "") + \
                                      (f"Opciones cercanas: {', '.join(options)}. ¿Cuál te conviene?"
                                       if options else "También puedo revisar otros días. ¿Qué día prefieres?")
                            else:
                                txt = (("That exact time isn’t open. " + lead_en) if not exact else "") + \
                                      (f"Closest options: {', '.join(options)}. What works best?"
                                       if options else "I can check other days too—what day works?")
                            await oai.send_json({"type":"response.create","response":{"modalities":["audio","text"],"instructions": txt}})
                            continue

                        elif name == "appointment_webhook":
                            if not hangup_ok:  # unrelated to hangup, just keeping guard var around
                                pass
                            payload_phone = (args.get("phone") or caller_number or "").strip()
                            payload = {
                                "tool": "book",
                                "booking_type": args.get("booking_type", "book"),
                                "name": args.get("name", "").strip(),
                                "email": args.get("email", "").strip(),
                                "phone": payload_phone,
                                "tz": DEFAULT_TZ,
                                "startTime": args.get("startTime", "").strip(),
                                "event_type_id": int(args.get("event_type_id", EVENT_TYPE_ID)),
                                "idempotency_key": f"ai-{uuid.uuid4().hex}",
                                "notes": (args.get("notes") or None)
                            }
                            # Final guard: require required fields (should be satisfied already)
                            miss = missing_fields(["booking_type","name","email","phone","startTime"], payload)
                            if miss:
                                prompt = prompt_for_missing(miss, last4(payload_phone), reply_lang)
                                await oai.send_json({"type":"response.create","response":{"modalities":["audio","text"],"instructions": prompt}})
                                continue

                            status, data = await json_post(MAIN_WEBHOOK_URL, payload)
                            body_status = data.get("status") if isinstance(data, dict) else None
                            phone_last4 = last4(payload_phone)
                            if status == 200 and body_status == "booked":
                                txt = ("Listo. Ya programé la cita en horario del Este. "
                                       f"Tengo tu correo como {payload['email']}" + (f" y el número terminando en {phone_last4}" if phone_last4 else "") +
                                       ". ¿Te ayudo con algo más?") if reply_lang=="es" else \
                                      ("All set. I’ve scheduled that in Eastern Time. "
                                       f"I have your email as {payload['email']}" + (f" and phone ending in {phone_last4}" if phone_last4 else "") +
                                       ". Anything else I can help with?")
                            elif status in (409, 422) or body_status in {"conflict","conflict_or_error"}:
                                txt = ("Parece que esa hora no está disponible. ¿Un poco más temprano o más tarde ese mismo día, o un día cercano?"
                                       if reply_lang=="es" else
                                       "Looks like that time isn’t available. Earlier or later that day, or a nearby day?")
                            else:
                                txt = ("No pude finalizarlo ahora mismo. ¿Intento de nuevo o prefieres otra hora?"
                                       if reply_lang=="es" else
                                       "I couldn’t finalize that just now. Want me to try again or pick a different time?")
                            await oai.send_json({"type": "response.create","response": {"modalities": ["audio","text"],"instructions": txt}})

                        elif name == "cancel_workflow":
                            payload = {
                                "action": "cancel",
                                "name": (args.get("name") or "").strip(),
                                "phone": args.get("phone") or caller_number or None,
                                "email": (args.get("email") or None)
                            }
                            if not CANCEL_WEBHOOK_URL:
                                txt = ("Puedo cancelarlo, pero mi línea de cancelación aún no está conectada. "
                                       "¿Te conecto con el responsable o quieres dejar un mensaje?" if reply_lang=="es" else
                                       "I can cancel that, but my cancel line isn’t connected yet. Connect you to the Business Solutions Lead, or take a message?")
                                await oai.send_json({"type":"response.create","response":{"modalities":["audio","text"],"instructions": txt}})
                            else:
                                status, data = await json_post(CANCEL_WEBHOOK_URL, payload)
                                phone_last = last4(payload.get("phone"))
                                body_status = data.get("status") if isinstance(data, dict) else None
                                if status == 200 and body_status in {"cancelled","ok","success"}:
                                    txt = ("Listo. Cancelé tu cita" + (f" para el número que termina en {phone_last}" if phone_last else "") +
                                           (f" y el correo {payload.get('email')}" if payload.get("email") else "") + ". ¿Necesitas algo más?") \
                                          if reply_lang=="es" else \
                                          ("Done. I’ve canceled your appointment" + (f" for the number ending in {phone_last}" if phone_last else "") +
                                           (f" and email {payload.get('email')}" if payload.get("email") else "") + ". Anything else?")
                                elif status == 404 or body_status in {"not_found","conflict_or_error"}:
                                    txt = ("No encontré una reserva activa con esos datos. ¿Usas otro correo o teléfono?"
                                           if reply_lang=="es" else
                                           "I couldn’t find an active booking for that info. Do you use another email or phone?")
                                else:
                                    txt = ("Tuve un problema al cancelar. ¿Intento de nuevo o te conecto con el responsable?"
                                           if reply_lang=="es" else
                                           "I hit a snag canceling that. Try again or connect you to the lead?")
                                await oai.send_json({"type":"response.create","response":{"modalities":["audio","text"],"instructions": txt}})

                        elif name == "transfer_call":
                            await announce_then({"type":"transfer","to":(args.get("to") or None),"reason":args.get("reason")})
                        elif name == "end_call":
                            if not hangup_ok:
                                confirm = "Do you want me to end the call, or keep going?" if reply_lang=="en" \
                                          else "¿Quieres que termine la llamada o seguimos?"
                                await oai.send_json({
                                    "type":"response.create",
                                    "response":{"modalities":["audio","text"],"instructions": confirm}
                                })
                                continue
                            await announce_then({"type":"hangup","reason":args.get("reason")})

                    elif t == "response.output_text.delta":
                        txt = (evt.get("delta") or "").lower()
                        if any(k in txt for k in ["connecting you","i'll transfer you","transfer you now","patch you through","let me connect"]):
                            await announce_then({"type":"transfer"})
                        elif any(k in txt for k in ["ending the call now","hanging up now","end the call"]):
                            await announce_then({"type":"hangup"})

                    elif t == "error":
                        log.error(f"OpenAI error event: {evt}")
                        apology = "Lo siento, no entendí eso. ¿Puedes repetirlo?" if reply_lang=="es" else "Sorry—I didn’t catch that. Could you say that again?"
                        await oai.send_json({"type":"response.create","response":{"modalities":["audio","text"],"instructions": apology}})

                elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                    break
        except asyncio.CancelledError:
            pass
        except Exception as e:
            log.info(f"openai_to_twilio ended: {e}")

    async def twilio_heartbeat():
        try:
            while True:
                await asyncio.sleep(15)
                if stream_sid:
                    with suppress(Exception):
                        await ws.send_text(json.dumps({"event": "mark","streamSid": stream_sid,"mark": {"name": "hb"}}))
        except asyncio.CancelledError:
            pass
        except Exception:
            pass

    t1 = asyncio.create_task(twilio_to_openai())
    t2 = asyncio.create_task(openai_to_twilio())
    t3 = asyncio.create_task(twilio_heartbeat())
    done, pending = await asyncio.wait({t1, t2, t3}, return_when=asyncio.FIRST_COMPLETED)
    for p in pending: p.cancel()
    for p in pending:
        with suppress(Exception, asyncio.CancelledError):
            await p
    await cleanup()
