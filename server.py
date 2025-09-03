# server.py
import os, json, asyncio, logging, aiohttp, re, uuid
from contextlib import suppress
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

PITCH_SHORT_EN = ("We build tailored solutions that save time and help you grow—"
                  "from custom AI-integrated booking and CRM automations to SEO websites and apps that keep you organized. "
                  "Tell me what kind of business you run or the goal you have, and I’ll make it specific.")
PITCH_SHORT_ES = ("Creamos soluciones a la medida para ahorrar tiempo y ayudarte a crecer—"
                  "desde reservas con IA y automatizaciones CRM hasta sitios SEO y apps para organizarte. "
                  "Cuéntame qué tipo de negocio tienes o tu objetivo y te doy ideas concretas.")

INSTRUCTIONS = (
    f"You are the {brand} AI receptionist (company based in {loc}). "
    "Start in ENGLISH. Only switch to Spanish if the caller explicitly asks. "
    "Tone: warm, professional, friendly, concise (1–2 sentences unless asked). "
    f"Value props: {vals}. Offer the trial when interest is shown: {trial}. "
    "Never invent business facts; if unsure, say so and offer to connect the caller. "
    "PRIMARY GOALS: (1) Explain benefits & answer questions; (2) Book a demo/installation; "
    "(3) Transfer to a human on request; (4) Think like a teammate focused on converting qualified leads."
    "\n\nTOOLS:\n"
    " - check_slots → Always call before booking; silently verifies availability and returns exact/alternates.\n"
    " - appointment_webhook(book|reschedule) → Creates/updates the booking when a time is chosen.\n"
    " - cancel_workflow(cancel) → Cancels an existing booking.\n"
    " - transfer_call(to?, reason?) → Bridge to a human now (omit 'to' to use the default).\n"
    " - end_call(reason?) → Politely end the call.\n"
    "IMPORTANT WORKFLOW BEHAVIOR:\n"
    " - For booking/rescheduling: ALWAYS use tz 'America/New_York'. Collect name, email (for confirmations), phone, and time.\n"
    " - Prefer the callerID phone; confirm by repeating the last 4 digits.\n"
    " - Before booking: CALL check_slots with the requested time.\n"
    "     • If exactMatch=true and identity (name+email+phone) is present → book silently using matchedSlot.\n"
    "     • If exactMatch=true but identity is missing → ask for the missing fields in ONE short sentence, then book using matchedSlot.\n"
    "     • If no exact match → offer same-day alternates and nearest options; book the chosen one.\n"
    " - After success, read back: name, email, last-4 phone, and the date/time in Eastern Time.\n"
    " - When transferring, first: “Absolutely—one moment while I connect you.” Then call the tool and stop speaking.\n"
    " - If transfer fails/no-answer, you may be reconnected; offer to book for today/tomorrow after rejoining.\n"
    " - Be natural and persuasive. If the caller is unsure, offer a quick transfer to the Business Solutions Lead."
    "\n\nGREETING (pick one line, English first turn):\n"
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
        {"type": "function","name": "transfer_call","description": "Bridge to a human now.","parameters": {"type": "object","properties": {"to": {"type": "string"},"reason": {"type": "string"}}}},
        {"type": "function","name": "end_call","description": "Politely end the call.","parameters": {"type": "object","properties": {"reason": {"type": "string"}}}},
    ]

    # ---------- Session setup ----------
    try:
        base_instructions = (
            INSTRUCTIONS +
            f"\nRUNTIME CONTEXT:\n- caller_id_e164={caller_number or 'unknown'}\n"
            f"- caller_id_last4={caller_last4 or 'unknown'}\n"
            f"- ALWAYS use timezone: {DEFAULT_TZ}\n" +
            (
                f"- If phone is missing, propose using caller_id_e164 and confirm: "
                f"“Is this the best number ending in {caller_last4}?”\n" if caller_last4 else
                "- If phone is missing, ask for it directly and confirm back the digits.\n"
            ) +
            "\nLANGUAGE LOCK: Respond only in English unless the caller explicitly asks for Spanish."
        )
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

    async def lock_language(new_lang: str):
        nonlocal reply_lang
        if new_lang not in ("en", "es") or new_lang == reply_lang:
            return
        reply_lang = new_lang
        label = "English" if new_lang == "en" else "Spanish"
        log.info(f"LANGUAGE LOCK → {label}")
        await oai.send_json({"type": "session.update","session": {
            "instructions": (
                INSTRUCTIONS +
                f"\nRUNTIME CONTEXT:\n- caller_id_e164={caller_number or 'unknown'}\n"
                f"- caller_id_last4={caller_last4 or 'unknown'}\n"
                f"- ALWAYS use timezone: {DEFAULT_TZ}\n" +
                (f"- If phone is missing, propose caller_id_e164 “ending in {caller_last4}”.\n" if caller_last4 else "") +
                f"\nLANGUAGE LOCK: Respond only in {label}."
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
        nonlocal pending_action, reply_lang
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

                            # quick intents
                            if re.search(r"(transfer|connect|human|agent|representative|manager|owner|person\s+in\s+charge|live\s+agent|operator)\b", low):
                                await announce_then({"type":"transfer","reason":"user_requested_transfer"})
                            elif re.search(r"(hang\s*up|end\s+the\s+call|that's\s+all|bye|good\s*bye|not\s+interested|we're\s+done|colgar|terminar.*llamada|ad(i|í)os)", low):
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
                            startTime   = (args.get("startTime") or "").strip()
                            search_days = int(args.get("search_days") or 30)
                            payload_chk = {
                                "tool": "checkAvailableSlot",
                                "event_type_id": int(args.get("event_type_id", EVENT_TYPE_ID)),
                                "tz": DEFAULT_TZ,
                                "startTime": startTime,
                                "search_days": search_days
                            }
                            s1, d1 = await json_post(MAIN_WEBHOOK_URL, payload_chk)
                            body = d1 if isinstance(d1, dict) else {}
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
