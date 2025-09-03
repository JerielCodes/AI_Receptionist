# server.py
import os, json, asyncio, logging, aiohttp, re, uuid
from contextlib import suppress
from fastapi import FastAPI, Request, WebSocket, Form
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
MAIN_WEBHOOK_URL   = os.getenv("MAIN_WEBHOOK_URL", "")   # book+reschedule+checkAvailableSlot
CANCEL_WEBHOOK_URL = os.getenv("CANCEL_WEBHOOK_URL", "")
EVENT_TYPE_ID      = int(os.getenv("EVENT_TYPE_ID", "3117986"))
DEFAULT_TZ         = "America/New_York"

# Used to build Twilio callbacks & rejoin streams after failed transfers
# Example: https://ai-receptionist-xxxx.onrender.com
PUBLIC_BASE_URL    = os.getenv("PUBLIC_BASE_URL", "")

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

# Short, flexible pitch (employee-like, not script-bound)
PITCH_SHORT_EN = ("We build tailored solutions that save time and help you grow—"
                  "from custom AI-integrated booking and CRM automations to SEO websites and apps that keep you organized. "
                  "Tell me what kind of business you run or the goal you have, and I’ll make it specific.")
PITCH_SHORT_ES = ("Creamos soluciones a la medida para ahorrar tiempo y ayudarte a crecer—"
                  "desde reservas con IA y automatizaciones CRM hasta sitios SEO y apps para organizarte. "
                  "Cuéntame qué tipo de negocio tienes o tu objetivo y te doy ideas concretas.")

INSTRUCTIONS = (
    f"You are the {brand} AI receptionist (company based in {loc}). "
    "Start in ENGLISH. Match the caller’s language only after they speak a full sentence in that language or explicitly ask. "
    "Tone: warm, professional, friendly, concise (1–2 sentences unless asked). "
    f"Value props: {vals}. Offer the trial when interest is shown: {trial}. "
    "Never invent business facts; if unsure, say so and offer to connect the caller. "
    "PRIMARY GOALS: (1) Explain benefits & answer questions; (2) Book a demo/installation; (3) Transfer to a human on request; "
    "(4) Think like a teammate focused on converting qualified leads."
    "\n\nTOOLS:\n"
    " - check_slots → Always call before booking; silently verifies availability and returns exact/alternates.\n"
    " - appointment_webhook(book|reschedule) → Creates/updates the booking when a time is chosen.\n"
    " - cancel_workflow(cancel) → Cancels an existing booking.\n"
    " - transfer_call(to?, reason?) → Bridge to a human now (omit 'to' to use the default).\n"
    " - end_call(reason?) → Politely end the call.\n"
    "IMPORTANT WORKFLOW BEHAVIOR:\n"
    " - For booking/rescheduling: ALWAYS use tz 'America/New_York'. Collect name, email (for confirmations), phone, and time.\n"
    " - Prefer the callerID phone; confirm by repeating the actual last 4 digits.\n"
    " - For cancel: need name + (phone OR email). Prefer phone. No time required.\n"
    " - Before booking: CALL check_slots with the requested time. If exactMatch=true, proceed to book silently. "
    "   If not, offer same-day alternates and nearest options, then book the chosen one.\n"
    " - Read back details after success: name, email, last-4 phone, and the date/time in Eastern Time.\n"
    " - When transferring, first say one short line like: “Absolutely—one moment while I connect you.” Then call the tool and stop speaking.\n"
    " - If transfer fails or no one answers, the system may reconnect you to the caller; offer to book for today/tomorrow after rejoining.\n"
    " - Don’t stick to a script; be natural and persuasive. If the caller is unsure, offer a quick transfer to the Business Solutions Lead.\n"
    "\nGREETING (pick one line, English first turn):\n"
    "  “Hey, thank you for calling ELvara. What can I help you with?”\n"
    "  “Hey—thanks for calling ELvara. How can I help you today?”\n"
    "  “Hi, thanks for calling ELvara. How may I assist you?”\n"
    "\nPITCH EXAMPLES:\n"
    f"  EN: {PITCH_SHORT_EN}\n"
    f"  ES: {PITCH_SHORT_ES}\n"
)

# ---------- TwiML builders ----------
def transfer_twiml(to_number: str, action_url: str | None = None) -> str:
    vr = VoiceResponse()
    if action_url:
        d = Dial(answer_on_bridge=True, action=action_url, method="POST")
    else:
        d = Dial(answer_on_bridge=True)
    d.number(to_number)
    vr.append(d)
    return str(vr)

def connect_stream_twiml(base_url: str, params: dict | None = None) -> str:
    """Return TwiML that connects a new media stream, optionally with <Parameter>s."""
    vr = VoiceResponse()
    conn = vr.connect()
    ws_url = https_to_wss(base_url.rstrip("/") + "/media")
    s = conn.stream(url=ws_url)
    # Pass lightweight flags back into /media via Twilio customParameters
    if params:
        for k, v in params.items():
            try:
                s.parameter(name=str(k), value=str(v))
            except Exception:
                pass
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

# ---------- TWILIO VOICE WEBHOOK (initial connect; silent) ----------
@app.post("/twilio/voice")
async def voice(request: Request):
    # Prefer explicit PUBLIC_BASE_URL; else derive from request
    base_url = PUBLIC_BASE_URL or f"{request.url.scheme}://{request.url.hostname}"
    vr_xml = connect_stream_twiml(base_url, params={"reason": "new"})
    log.info(f"Returning TwiML with stream URL (initial).")
    return PlainTextResponse(vr_xml, media_type="application/xml")

# ---------- AFTER-TRANSFER CALLBACK (handles no-answer/busy/failed) ----------
@app.post("/twilio/after-transfer")
async def after_transfer(request: Request):
    if not PUBLIC_BASE_URL:
        # If we can't rejoin, just end the call
        vr = VoiceResponse()
        vr.hangup()
        return PlainTextResponse(str(vr), media_type="application/xml")

    form = await request.form()
    status = (form.get("DialCallStatus") or "").lower()
    log.info(f"after-transfer DialCallStatus={status!r}")

    if status in {"completed", "answered"}:
        vr = VoiceResponse()
        vr.hangup()
        return PlainTextResponse(str(vr), media_type="application/xml")

    # No-answer / busy / failed → reconnect to AI with a hint
    vr_xml = connect_stream_twiml(PUBLIC_BASE_URL, params={"reason": "transfer_fail"})
    return PlainTextResponse(vr_xml, media_type="application/xml")

# ---------- MEDIA STREAM ----------
@app.websocket("/media")
async def media(ws: WebSocket):
    if not OPENAI_API_KEY:
        log.error("No OPENAI_API_KEY; closing media socket.")
        await ws.close(); return
    await ws.accept()

    # Wait for Twilio 'start'
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
                # Twilio custom parameters from <Stream><Parameter>
                params = (data0["start"].get("customParameters") or {})
                rejoin_reason = params.get("reason")
                log.info(f"Twilio stream start: streamSid={stream_sid}, callSid={call_sid}, caller_e164={caller_number!r}, last4={caller_last4!r}, reason={rejoin_reason!r}")
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
                log.info("AI failed; falling back to immediate transfer.")
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
                    "startTime": {"type": "string", "description": "Requested ISO with offset, e.g., 2025-09-26T11:30:00-04:00"},
                    "search_days": {"type": "integer", "default": 30},
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
        {
            "type": "function",
            "name": "transfer_call",
            "description": "Bridge the caller to a human immediately. If 'to' is omitted, use the default business line.",
            "parameters": {
                "type": "object",
                "properties": {
                    "to": {"type": "string", "description": "E.164 number to transfer to (optional)"},
                    "reason": {"type": "string", "description": "Short reason (optional)"},
                },
            },
        },
        {
            "type": "function",
            "name": "end_call",
            "description": "Politely end the call.",
            "parameters": {
                "type": "object",
                "properties": {"reason": {"type": "string", "description": "Optional closing reason"}},
            },
        },
    ]

    # ---------- Session setup ----------
    try:
        base_instructions = (
            INSTRUCTIONS +
            f"\nRUNTIME CONTEXT:\n- caller_id_e164={caller_number or 'unknown'}\n"
            f"- caller_id_last4={caller_last4 or 'unknown'}\n"
            f"- ALWAYS use timezone: {DEFAULT_TZ}\n" +
            (
                f"- If phone is missing from the user, propose using caller_id_e164 and confirm explicitly: "
                f"“Is this the best number ending in {caller_last4}?”\n" if caller_last4 else
                "- If phone is missing from the user, ask for it directly and confirm back the digits.\n"
            ) +
            "\nLANGUAGE LOCK: Respond only in English unless the caller clearly switches languages or asks to switch."
        )
        await oai.send_json({
            "type": "session.update",
            "session": {
                "turn_detection": {
                    "type": "server_vad",
                    "silence_duration_ms": 900,
                    "create_response": True,
                    "interrupt_response": True
                },
                "input_audio_format": "g711_ulaw",
                "output_audio_format": "g711_ulaw",
                "voice": "alloy",
                "modalities": ["audio", "text"],
                "input_audio_transcription": {"model": "gpt-4o-mini-transcribe"},
                "instructions": base_instructions,
                "tools": tools
            }
        })
        # First line
        if rejoin_reason == "transfer_fail":
            # Rejoin fallback
            await oai.send_json({
                "type": "response.create",
                "response": {
                    "modalities": ["audio", "text"],
                    "tool_choice": "auto",
                    "instructions": (
                        "Looks like no one is available right now. "
                        "If you’d like, I can book you for later today or tomorrow. What time works? (Eastern Time)"
                    )
                }
            })
        else:
            # Initial greeting (English)
            await oai.send_json({
                "type": "response.create",
                "response": {
                    "modalities": ["audio", "text"],
                    "tool_choice": "auto",
                    "instructions": (
                        "Start in English. Say EXACTLY one of the following (pick naturally, only one line): "
                        "\"Hey, thank you for calling ELvara. What can I help you with?\" "
                        "OR \"Hey—thanks for calling ELvara. How can I help you today?\" "
                        "OR \"Hi, thanks for calling ELvara. How may I assist you?\""
                    )
                }
            })
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

    async def lock_language(new_lang: str):
        nonlocal reply_lang
        if new_lang not in ("en", "es") or new_lang == reply_lang:
            return
        reply_lang = new_lang
        label = "English" if new_lang == "en" else "Spanish"
        log.info(f"LANGUAGE LOCK → {label}")
        await oai.send_json({
            "type": "session.update",
            "session": {
                "instructions": (
                    INSTRUCTIONS +
                    f"\nRUNTIME CONTEXT:\n- caller_id_e164={caller_number or 'unknown'}\n"
                    f"- caller_id_last4={caller_last4 or 'unknown'}\n"
                    f"- ALWAYS use timezone: {DEFAULT_TZ}\n" +
                    (
                        f"- If phone is missing from the user, propose using caller_id_e164 and confirm explicitly: "
                        f"“Is this the best number ending in {caller_last4}?”\n" if caller_last4 else
                        "- If phone is missing from the user, ask for it directly and confirm back the digits.\n"
                    ) +
                    f"\nLANGUAGE LOCK: Respond only in {label}. Do not switch unless the caller clearly continues in the other language or asks to switch."
                ),
                "input_audio_transcription": {"model": "gpt-4o-mini-transcribe", "language": new_lang}
            }
        })

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
        say_en = "Absolutely—one moment while I connect you." if action.get("type") == "transfer" else "Thanks for calling—ending the call now."
        say_es = "Claro—dame un momento mientras te conecto." if action.get("type") == "transfer" else "Gracias por llamar—voy a finalizar la llamada ahora."
        say = say_es if reply_lang == "es" else say_en
        pending_action = action
        log.info(f"ANNOUNCE queued → {action}")
        await oai.send_json({"type": "response.create", "response": {"modalities": ["audio","text"], "instructions": say}})

    # ---------- Helper: JSON POST ----------
    async def json_post(url: str, payload: dict) -> tuple[int, dict | str]:
        try:
            async with session.post(url, json=payload, timeout=20) as resp:
                ct = resp.headers.get("content-type", "")
                status = resp.status
                if "application/json" in ct:
                    data = await resp.json()
                else:
                    data = await resp.text()
                preview = data if isinstance(data, str) else {k: data.get(k) for k in ("status","code","event_id","start_iso","matchedSlot","exactMatch")}
                log.info(f"POST {url} -> {status} {preview}")
                return status, data
        except Exception as e:
            log.error(f"POST {url} failed: {e}")
            return 0, str(e)

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
                    reason = (data.get("stop") or {}).get("reason")
                    log.info(f"Twilio sent stop. reason={reason!r}")
                    break
        except asyncio.CancelledError:
            pass
        except Exception as e:
            log.info(f"twilio_to_openai ended: {e}")

    async def openai_to_twilio():
        nonlocal pending_action
        try:
            while True:
                msg = await oai.receive()
                if msg.type == aiohttp.WSMsgType.TEXT:
                    evt = json.loads(msg.data)
                    t = evt.get("type")
                    log.info(f"OAI EVT: {t}")

                    # Debug transcripts
                    if t == "conversation.item.input_audio_transcription.delta":
                        delta = evt.get("delta") or ""
                        user_buf.append(delta)
                        log.info(f"USER>> {delta}")
                    if t == "response.audio_transcript.delta":
                        log.info(f"ASSISTANT>> {evt.get('delta')}")

                    # assistant audio -> Twilio
                    if t == "response.audio.delta" and stream_sid:
                        try:
                            await ws.send_text(json.dumps({
                                "event": "media",
                                "streamSid": stream_sid,
                                "media": {"payload": evt["delta"]}
                            }))
                        except Exception:
                            break

                    # barge-in
                    elif t == "input_audio_buffer.speech_started" and stream_sid:
                        with suppress(Exception):
                            await ws.send_text(json.dumps({"event": "clear", "streamSid": stream_sid}))

                    # After the announcement finishes, run the pending action
                    elif t == "response.done" and pending_action:
                        action = pending_action
                        pending_action = None
                        log.info(f"ANNOUNCE done → executing {action}")
                        if action.get("type") == "transfer":
                            do_transfer(reason=action.get("reason"), to=action.get("to"))
                        else:
                            do_hangup(reason=action.get("reason"))

                    # user utterance completed → simple intent & language locks
                    elif t == "conversation.item.input_audio_transcription.completed":
                        text = "".join(user_buf).strip()
                        user_buf.clear()
                        if text:
                            low = text.lower()
                            # Language switch requests
                            if re.search(r"\b(en\s+español|habla\s+español|puedes\s+hablar\s+español)\b", low): await lock_language("es")
                            elif re.search(r"\b(in\s+english|speak\s+english|english\s+please)\b", low): await lock_language("en")
                            else:
                                # light heuristic
                                toks = re.findall(r"[\wñáéíóúü]+", low)
                                es = sum(1 for w in toks if w in {"hola","gracias","por","favor","cita","reprogramar","cancelar","mañana","tarde","hoy","disponible"})
                                en = sum(1 for w in toks if w in {"hello","thanks","please","book","schedule","reschedule","cancel","appointment","tomorrow","afternoon","today","available"})
                                if es >= en + 2: await lock_language("es")
                                elif en >= es + 2: await lock_language("en")

                            # backup intents
                            if re.search(r"(transfer|connect|human|agent|representative|manager|owner|person\s+in\s+charge|live\s+agent|operator)\b", low):
                                log.info(f"SERVER-INTENT: transfer (from user text): {text!r}")
                                await announce_then({"type":"transfer","reason":"user_requested_transfer"})
                            elif re.search(r"(hang\s*up|end\s+the\s+call|that's\s+all|bye|good\s*bye|not\s+interested|we're\s+done|colgar|terminar.*llamada|ad(i|í)os)", low):
                                log.info(f"SERVER-INTENT: hangup (from user text): {text!r}")
                                await announce_then({"type":"hangup","reason":"user_requested_hangup"})

                    # PATH A: function call announced
                    elif t == "response.output_item.added":
                        item = evt.get("item", {})
                        if item.get("type") == "function_call":
                            call_id = item.get("call_id")
                            name    = item.get("name")
                            if call_id:
                                call_map.setdefault(call_id, {"name": name, "args": ""})
                                log.info(f"FUNC ANNOUNCED: call_id={call_id} name={name}")

                    # PATH B: function args stream in
                    elif t == "response.function_call_arguments.delta":
                        cid   = evt.get("call_id")
                        delta = evt.get("arguments", "")
                        if cid:
                            entry = call_map.setdefault(cid, {"name": None, "args": ""})
                            entry["args"] += delta

                    elif t == "response.function_call_arguments.done":
                        cid = evt.get("call_id")
                        if cid:
                            info = call_map.pop(cid, {"name": None, "args": ""})
                            name = info.get("name")
                            raw  = info.get("args") or ""
                            try:
                                args = json.loads(raw or "{}")
                            except Exception:
                                log.error(f"Bad JSON for function args: {raw!r}")
                                args = {}
                            log.info(f"TOOL (fn_args) COMPLETE: {name} args={args}")

                            # ---- Check availability (auto-commit on exact) ----
                            if name == "check_slots":
                                startTime   = (args.get("startTime") or "").strip()
                                search_days = int(args.get("search_days") or 30)
                                phone_in    = args.get("phone") or caller_number or ""
                                payload_chk = {
                                    "tool": "checkAvailableSlot",
                                    "event_type_id": int(args.get("event_type_id", EVENT_TYPE_ID)),
                                    "tz": DEFAULT_TZ,
                                    "startTime": startTime,
                                    "search_days": search_days
                                }
                                s1, d1 = await json_post(MAIN_WEBHOOK_URL, payload_chk)
                                body = d1 if isinstance(d1, dict) else {}
                                exact = bool(body.get("exactMatch"))
                                matched = body.get("matchedSlot")
                                same_day = body.get("sameDayAlternates") or []
                                nearest  = body.get("nearest") or []
                                # If we already have identity AND exact match, auto-book silently
                                have_identity = all([(args.get("name") or "").strip(), (args.get("email") or "").strip(), phone_in])
                                if exact and matched and have_identity:
                                    payload_book = {
                                        "tool": "book",
                                        "booking_type": args.get("booking_type", "book"),
                                        "name": (args.get("name") or "").strip(),
                                        "email": (args.get("email") or "").strip(),
                                        "phone": phone_in,
                                        "tz": DEFAULT_TZ,
                                        "startTime": matched,
                                        "event_type_id": int(args.get("event_type_id", EVENT_TYPE_ID)),
                                        "idempotency_key": f"ai-{uuid.uuid4().hex}",
                                        "notes": (args.get("notes") or None)
                                    }
                                    s2, d2 = await json_post(MAIN_WEBHOOK_URL, payload_book)
                                    phone_last = last4(phone_in)
                                    ok = (isinstance(d2, dict) and d2.get("status") == "booked" and s2 == 200)
                                    if ok:
                                        text_en = (f"All set. I booked {matched} Eastern Time. "
                                                   f"I have your email as {payload_book['email']}"
                                                   + (f" and phone ending in {phone_last}" if phone_last else "")
                                                   + ". A calendar invite is on its way. Anything else?")
                                        text_es = (f"Listo. Reservé {matched} hora del Este. "
                                                   f"Tengo tu correo como {payload_book['email']}"
                                                   + (f" y el número terminando en {phone_last}" if phone_last else "")
                                                   + ". Te llegará la invitación del calendario. ¿Algo más?")
                                        await oai.send_json({"type":"response.create","response":{"modalities":["audio","text"],"instructions": text_es if reply_lang=="es" else text_en}})
                                    else:
                                        text_en = ("I couldn’t finalize that just now. "
                                                   "Would you like me to try again, or pick a different time?")
                                        text_es = ("No pude finalizarlo ahora mismo. "
                                                   "¿Quieres que lo intente de nuevo o prefieres otra hora?")
                                        await oai.send_json({"type":"response.create","response":{"modalities":["audio","text"],"instructions": text_es if reply_lang=="es" else text_en}})
                                else:
                                    # Present options for the model to offer
                                    if exact and matched:
                                        text_en = (f"Good news—{matched} Eastern is open. Would you like me to book that, "
                                                   f"or would you prefer one of these nearby times: {', '.join(nearest[:3])}?")
                                        text_es = (f"Buenas noticias: {matched} (hora del Este) está disponible. "
                                                   f"¿Quieres que lo reserve o prefieres alguna de estas horas cercanas: {', '.join(nearest[:3])}?")
                                    else:
                                        alt_line_en = ""
                                        if body.get("firstAvailableThatDay"):
                                            alt_line_en = f"The first opening that day is {body['firstAvailableThatDay']}. "
                                        text_en = (f"I couldn’t get that exact time. {alt_line_en}"
                                                   f"Closest options: {', '.join((same_day or nearest)[:3]) or 'I can check other days too.'} "
                                                   "What works best?")
                                        text_es = (f"Esa hora exacta no está libre. "
                                                   + (f"La primera apertura ese día es {body['firstAvailableThatDay']}. " if body.get("firstAvailableThatDay") else "")
                                                   + f"Opciones cercanas: {', '.join((same_day or nearest)[:3]) or 'También puedo revisar otros días.'} "
                                                   "¿Cuál te conviene?")
                                    await oai.send_json({"type":"response.create","response":{"modalities":["audio","text"],"instructions": text_es if reply_lang=="es" else text_en}})

                            # ---- Book/Reschedule (manual call by the model) ----
                            elif name == "appointment_webhook":
                                payload_phone = args.get("phone") or caller_number or ""
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
                                status, data = await json_post(MAIN_WEBHOOK_URL, payload)
                                phone_last4 = last4(payload_phone)
                                body_status = data.get("status") if isinstance(data, dict) else None
                                is_booked = (status == 200 and body_status == "booked")
                                is_conflict = (status in (409, 422)) or (status == 200 and body_status in {"conflict","conflict_or_error"})

                                confirm_text_en = (
                                    f"All set. I’ve scheduled that in Eastern Time. "
                                    f"I have your email as {payload['email']}"
                                    + (f" and phone ending in {phone_last4}" if phone_last4 else "")
                                    + ". You’ll receive confirmation emails with calendar invites. "
                                    "Anything else I can help you with?"
                                )
                                confirm_text_es = (
                                    f"Listo. Ya programé la cita en horario del Este. "
                                    f"Tengo tu correo como {payload['email']}"
                                    + (f" y el número terminando en {phone_last4}" if phone_last4 else "")
                                    + ". Te llegará un correo con la invitación del calendario. "
                                    "¿Te ayudo con algo más?"
                                )

                                if is_booked:
                                    await oai.send_json({"type": "response.create","response": {"modalities": ["audio","text"],"instructions": confirm_text_es if reply_lang=="es" else confirm_text_en}})
                                elif is_conflict:
                                    text_en = ("Looks like that time isn’t available. Would you like an earlier or later time the same day, "
                                               "or a nearby day around that time?")
                                    text_es = ("Parece que esa hora no está disponible. ¿Prefieres un poco más temprano o más tarde ese mismo día, "
                                               "o un día cercano a esa hora?")
                                    await oai.send_json({"type": "response.create","response": {"modalities": ["audio","text"],"instructions": text_es if reply_lang=="es" else text_en}})
                                else:
                                    text_en = "I couldn’t finalize that just now. Would you like me to try again, or pick a different time?"
                                    text_es = "No pude finalizarlo ahora mismo. ¿Quieres que lo intente de nuevo o prefieres otra hora?"
                                    await oai.send_json({"type": "response.create","response": {"modalities": ["audio","text"],"instructions": text_es if reply_lang=="es" else text_en}})

                            # ---- Cancel ----
                            elif name == "cancel_workflow":
                                payload = {
                                    "action": "cancel",
                                    "name": (args.get("name") or "").strip(),
                                    "phone": args.get("phone") or caller_number or None,
                                    "email": (args.get("email") or None)
                                }
                                if not CANCEL_WEBHOOK_URL:
                                    text_en = ("I can cancel that, but my cancel line isn’t connected yet. "
                                               "Could I connect you to the Business Solutions Lead, or would you like me to take a message?")
                                    text_es = ("Puedo cancelarlo, pero mi línea de cancelación aún no está conectada. "
                                               "¿Te conecto con el responsable o quieres dejar un mensaje?")
                                    await oai.send_json({"type":"response.create","response":{"modalities":["audio","text"],"instructions": text_es if reply_lang=="es" else text_en}})
                                else:
                                    status, data = await json_post(CANCEL_WEBHOOK_URL, payload)
                                    phone_last = last4(payload.get("phone"))
                                    body_status = data.get("status") if isinstance(data, dict) else None
                                    is_cancelled = (status == 200 and body_status in {"cancelled","ok","success"})
                                    not_found = (status == 404) or (status == 200 and body_status in {"not_found","conflict_or_error"})

                                    if is_cancelled:
                                        text_en = ("Done. I’ve canceled your appointment"
                                                   + (f" for the number ending in {phone_last}" if phone_last else "")
                                                   + (f" and email {payload.get('email')}" if payload.get("email") else "")
                                                   + ". Is there anything else I can help you with?")
                                        text_es = ("Listo. Cancelé tu cita"
                                                   + (f" para el número que termina en {phone_last}" if phone_last else "")
                                                   + (f" y el correo {payload.get('email')}" if payload.get("email") else "")
                                                   + ". ¿Necesitas algo más?")
                                        await oai.send_json({"type":"response.create","response":{"modalities":["audio","text"],"instructions": text_es if reply_lang=='es' else text_en}})
                                    elif not_found:
                                        text_en = ("I couldn’t find an active booking for that name and number. "
                                                   "Do you use another email or phone I can try?")
                                        text_es = ("No pude encontrar una reserva activa con ese nombre y número. "
                                                   "¿Usas otro correo o teléfono que pueda intentar?")
                                        await oai.send_json({"type":"response.create","response":{"modalities":["audio","text"],"instructions": text_es if reply_lang=='es' else text_en}})
                                    else:
                                        text_en = ("I ran into a problem canceling that. Want me to try again or connect you to the Business Solutions Lead?")
                                        text_es = ("Tuve un problema al cancelar. ¿Quieres que lo intente de nuevo o te conecto con el responsable?")
                                        await oai.send_json({"type":"response.create","response":{"modalities":["audio","text"],"instructions": text_es if reply_lang=='es' else text_en}})

                            elif name == "transfer_call":
                                await announce_then({"type":"transfer","to":(args.get("to") or None),"reason":args.get("reason")})

                            elif name == "end_call":
                                await announce_then({"type":"hangup","reason":args.get("reason")})

                    # text fallbacks just in case
                    elif t == "response.output_text.delta":
                        txt = (evt.get("delta") or "").lower()
                        if any(k in txt for k in ["connecting you", "let me connect", "i'll transfer you", "transfer you now", "patch you through"]):
                            log.info("Text fallback: transfer hint in delta.")
                            await announce_then({"type":"transfer"})
                        elif any(k in txt for k in ["ending the call now", "hanging up now", "end the call"]):
                            log.info("Text fallback: hangup hint in delta.")
                            await announce_then({"type":"hangup"})

                    elif t == "error":
                        log.error(f"OpenAI error: {evt}")

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

    # Race tasks; cancel others when one ends
    t1 = asyncio.create_task(twilio_to_openai())
    t2 = asyncio.create_task(openai_to_twilio())
    t3 = asyncio.create_task(twilio_heartbeat())
    done, pending = await asyncio.wait({t1, t2, t3}, return_when=asyncio.FIRST_COMPLETED)
    for p in pending:
        p.cancel()
    for p in pending:
        with suppress(Exception, asyncio.CancelledError):
            await p
    await cleanup()
