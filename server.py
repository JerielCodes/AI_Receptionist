# server.py
import os, json, asyncio, logging, aiohttp, re, uuid
from datetime import datetime
from zoneinfo import ZoneInfo
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
PUBLIC_BASE_URL    = os.getenv("PUBLIC_BASE_URL", "")    # e.g. https://your-app.onrender.com

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

EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$")

def is_valid_email(v: str | None) -> bool:
    return bool(v and EMAIL_RE.fullmatch(v.strip()))

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
vals  = " • ".join(KB.get("value_props", [])) or "Custom AI booking • SEO websites • Workflow automations"
trial = KB.get("trial_offer", "One-week free trial; install/uninstall is free.")

PITCH_SHORT_EN = (
    "We build tailored solutions that save time and help you grow—"
    "from custom AI-integrated booking and CRM automations to SEO websites and apps that keep you organized. "
    "Tell me what kind of business you run or the goal you have, and I’ll make it specific."
)

INSTRUCTIONS = (
    f"You are the {brand} AI receptionist (company based in {loc}). "
    "Multilingual but default to ENGLISH for this call. Keep replies short (1–2 sentences) unless asked. "
    f"Value props: {vals}. Offer the trial when interest is shown: {trial}. "
    "Think like a teammate focused on converting qualified leads: answer questions, give relevant examples, and ask for the next step."
    "\n\nTOOLS (Never call a tool with missing fields. Ask one short question to collect what’s missing first):\n"
    " - check_slots(startTime, search_days?) → Verify availability for a requested time. Returns exact/alternates.\n"
    "   Examples:\n"
    "   • Caller: “Can we do Friday 2pm?” → check_slots('2025-09-26T14:00:00-04:00').\n"
    "   • Caller unsure: propose a default (today 3:00 PM ET), and check next 14 days.\n"
    " - appointment_webhook(booking_type, name, email, phone, startTime) → Book/reschedule after a time is chosen.\n"
    "   Always read back: name, **email** (say it out loud) and phone last-4, plus the date/time in Eastern Time.\n"
    " - cancel_workflow(name, phone|email) → Cancel a booking. Prefer phone.\n"
    " - transfer_call(to?, reason?) → Bridge to a human now. Say: “Absolutely—one moment while I connect you.” then call the tool.\n"
    "\nBOOKING PLAYBOOK:\n"
    " 1) Collect intent + business type. Give 1–2 tailored examples.\n"
    " 2) Ask for a day & time. If not specific, offer a default (today 3:00 PM ET) and say you’ll check the next 14 days.\n"
    " 3) Call check_slots with startTime. If exactMatch=true and you already have name+email+phone → book silently.\n"
    "    Otherwise, ask ONLY for the missing fields (email must be read back and confirmed), then book.\n"
    " 4) After success, confirm details succinctly and offer help with anything else.\n"
    "\nGREETING (choose one line):\n"
    "  “Hey, thank you for calling ELvara. What can I help you with?”\n"
    "  “Hey—thanks for calling ELvara. How can I help you today?”\n"
    "  “Hi, thanks for calling ELvara. How may I assist you?”\n"
    "\nCLARITY RULES:\n"
    " - If you don’t understand, say once: “Sorry—I didn’t catch that, could you repeat?” "
    "If it happens twice, add: “Calls are clearest off speaker/headphones.”\n"
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
            "description": "Verify availability before booking. Requires startTime (ISO with offset). Returns exact match and alternates.",
            "parameters": {
                "type": "object",
                "required": ["startTime"],
                "properties": {
                    "startTime": {"type": "string", "description": "Requested ISO with offset, e.g., 2025-09-26T15:00:00-04:00"},
                    "search_days": {"type": "integer", "default": 14},
                    "booking_type": {"type": "string", "enum": ["book", "reschedule"], "default": "book"},
                    "name": {"type": "string"},
                    "email": {"type": "string"},
                    "phone": {"type": "string", "description": "E.164 like +15551234567"},
                    "event_type_id": {"type": "integer", "default": EVENT_TYPE_ID},
                    "notes": {"type": "string", "description": "Optional lead notes for the sheet"}
                }
            }
        },
        {
            "type": "function",
            "name": "appointment_webhook",
            "description": "Book or reschedule via the main n8n workflow.",
            "parameters": {
                "type": "object",
                "required": ["booking_type", "name", "email", "phone", "startTime"],
                "properties": {
                    "booking_type": {"type": "string", "enum": ["book", "reschedule"]},
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
                "properties": {"to": {"type": "string"}, "reason": {"type": "string"}}
            }
        }
    ]

    # ---------- Session setup ----------
    try:
        # Current time for model awareness
        now_et = datetime.now(ZoneInfo(DEFAULT_TZ))
        now_line = now_et.strftime("%A, %B %d, %Y, %-I:%M %p %Z")

        base_instructions = (
            INSTRUCTIONS +
            f"\nRUNTIME CONTEXT:\n- caller_id_e164={caller_number or 'unknown'}\n"
            f"- caller_id_last4={caller_last4 or 'unknown'}\n"
            f"- ALWAYS use timezone: {DEFAULT_TZ}\n"
            f"- CURRENT_TIME_ET: {now_line}\n"
            "- Don’t call tools with missing fields. Ask for what you need first in one short sentence.\n"
        )
        await oai.send_json({
            "type": "session.update",
            "session": {
                "turn_detection": {
                    "type": "server_vad",
                    "silence_duration_ms": 1200,   # calmer
                    "create_response": True,
                    "interrupt_response": True
                },
                "input_audio_format": "g711_ulaw",
                "output_audio_format": "g711_ulaw",
                "voice": "alloy",
                "modalities": ["audio", "text"],
                "input_audio_transcription": {"model": "gpt-4o-mini-transcribe", "language": "en"},
                "instructions": base_instructions,
                "tools": tools
            }
        })
        # Greeting or rejoin
        if rejoin_reason == "transfer_fail":
            await oai.send_json({
                "type": "response.create",
                "response": {
                    "modalities": ["audio","text"],
                    "tool_choice": "auto",
                    "instructions": "Looks like no one is available right now. I can book you for later today or tomorrow—what time works? (Eastern Time)"
                }
            })
        else:
            await oai.send_json({
                "type": "response.create",
                "response": {
                    "modalities": ["audio","text"],
                    "tool_choice": "auto",
                    "instructions": (
                        'Say EXACTLY one of: '
                        '"Hey, thank you for calling ELvara. What can I help you with?" '
                        'OR "Hey—thanks for calling ELvara. How can I help you today?" '
                        'OR "Hi, thanks for calling ELvara. How may I assist you?"'
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
    misunderstand = 0

    # response send-queue (prevents "active response" errors)
    assistant_busy = False
    async def wait_idle():
        nonlocal assistant_busy
        while assistant_busy:
            await asyncio.sleep(0.05)

    async def say(text: str):
        await wait_idle()
        await oai.send_json({"type": "response.create", "response": {"modalities": ["audio","text"], "instructions": text}})

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
                preview = data if isinstance(data, str) else {k: data.get(k) for k in ("status","code","event_id","start_iso","matchedSlot","exactMatch")}
                log.info(f"POST {url} -> {status} {preview}")
                return status, data
        except Exception as e:
            log.error(f"POST {url} failed: {e}")
            return 0, str(e)

    # ---------- Utility: validation & prompts ----------
    def missing_fields(required_keys: list[str], args: dict) -> list[str]:
        miss = []
        for k in required_keys:
            v = args.get(k)
            if v is None: miss.append(k); continue
            if isinstance(v, str) and not v.strip(): miss.append(k)
        return miss

    def prompt_for_missing(miss: list[str], caller_last: str | None) -> str:
        labels = {"name":"your name","email":"an email for confirmation","phone":"the best phone number",
                  "startTime":"a day and time","booking_type":"whether this is a new booking or reschedule"}
        parts = [labels.get(k,k) for k in miss]
        tail = f" I can use the number ending in {caller_last} if that works." if (caller_last and "phone" in miss) else ""
        return "To set that up I just need " + ", ".join(parts) + "." + tail + " You can tell me now."

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
                    log.info("Twilio sent stop.")
                    break
        except asyncio.CancelledError:
            pass
        except Exception as e:
            log.info(f"twilio_to_openai ended: {e}")

    async def openai_to_twilio():
        nonlocal pending_action, assistant_busy, misunderstand
        try:
            while True:
                msg = await oai.receive()
                if msg.type == aiohttp.WSMsgType.TEXT:
                    evt = json.loads(msg.data)
                    t = evt.get("type")
                    # log.info(f"OAI EVT: {t}")

                    # mark busy/idle to avoid overlapping responses
                    if t == "response.created":
                        assistant_busy = True
                    elif t == "response.done":
                        if pending_action:
                            action = pending_action; pending_action = None
                            if action.get("type") == "transfer":
                                do_transfer(reason=action.get("reason"), to=action.get("to"))
                        assistant_busy = False

                    # transcripts
                    if t == "conversation.item.input_audio_transcription.delta":
                        user_buf.append(evt.get("delta") or "")

                    # assistant audio → Twilio
                    if t == "response.audio.delta" and stream_sid:
                        try:
                            await ws.send_text(json.dumps({"event": "media","streamSid": stream_sid,"media": {"payload": evt["delta"]}}))
                        except Exception:
                            break

                    # barge-in
                    elif t == "input_audio_buffer.speech_started" and stream_sid:
                        with suppress(Exception):
                            await ws.send_text(json.dumps({"event": "clear", "streamSid": stream_sid}))

                    # utterance completed
                    elif t == "conversation.item.input_audio_transcription.completed":
                        text = "".join(user_buf).strip()
                        user_buf.clear()

                        # very short / empty → ask once to repeat
                        if not text or len(text.split()) <= 2:
                            misunderstand += 1
                            tip = " Calls are clearest off speaker or headphones." if misunderstand >= 2 else ""
                            await say("Sorry—I didn’t catch that, could you repeat?" + tip)
                            continue
                        else:
                            misunderstand = 0

                        low = text.lower()

                        # transfer intent (explicit)
                        if re.search(r"\b(transfer|connect|human|agent|representative|manager|owner|person\s+in\s+charge|live\s+agent|operator)\b", low):
                            pending_action = {"type":"transfer","reason":"user_requested_transfer"}
                            await say("Absolutely—one moment while I connect you.")
                            continue

                        # goodbye intent (explicit)
                        if re.search(r"\b(hang\s*up|end\s+the\s+call|that'?s\s+all|we'?re\s+done|goodbye|bye)\b", low):
                            await say("Thanks for calling—have a great day!")
                            # do not actively hang up; let caller end
                            continue

                    # tool call announced / streamed
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

                        # ---- Guards BEFORE hitting webhooks ----
                        if name == "appointment_webhook":
                            req = ["booking_type","name","email","phone","startTime"]
                            miss = missing_fields(req, args)
                            # phone can default to callerID
                            if "phone" in miss and caller_number:
                                args["phone"] = caller_number
                                miss = [m for m in miss if m != "phone"]

                            # email validity/readback
                            if "email" not in miss and not is_valid_email(args.get("email")):
                                miss.append("email")

                            if miss:
                                prompt = prompt_for_missing(miss, last4(args.get("phone") or caller_number))
                                await say(prompt)
                                continue

                        if name == "cancel_workflow":
                            need_name = not (args.get("name") or "").strip()
                            have_contact = (args.get("phone") or caller_number or args.get("email"))
                            if need_name or not have_contact:
                                need = ["name"] if need_name else []
                                if not have_contact: need.append("a phone or email")
                                await say("To cancel, I just need " + ", ".join(need) + ".")
                                continue

                        # ---- Tools proper ----
                        if name == "check_slots":
                            startTime   = (args.get("startTime") or "").strip()
                            if not startTime:
                                await say("What day and time work for you? If you’d like, I can try today at 3:00 PM Eastern and scan the next 14 days.")
                                continue
                            search_days = int(args.get("search_days") or 14)
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
                                is_valid_email(args.get("email")),
                                (args.get("phone") or caller_number or "").strip()
                            ])

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
                                    txt = (f"All set. I booked {matched} Eastern Time. "
                                           f"I have your email as {payload_book['email']}"
                                           + (f" and phone ending in {phone_last}" if phone_last else "")
                                           + ". You’ll get a calendar invite. Anything else?")
                                else:
                                    txt = "I couldn’t finalize that just now. Want me to try again or pick a different time?"
                                await say(txt)
                                continue

                            # exact but missing identity → ask just what's missing
                            if exact and matched and not have_identity:
                                miss = []
                                if not (args.get("name") or "").strip():  miss.append("name")
                                if not is_valid_email(args.get("email")): miss.append("email")
                                if not (args.get("phone") or caller_number or "").strip(): miss.append("phone")
                                phone_last = last4(caller_number)
                                ask = (f"Good news—{matched} Eastern is open. To lock it in I just need "
                                       + ", ".join(["your name" if m=="name" else "an email for confirmation" if m=="email" else "the best phone number" for m in miss])
                                       + (f". I can use the number ending in {phone_last} if that works. " if "phone" in miss and phone_last else ". ")
                                       + "What should I use?")
                                await say(ask)
                                continue

                            # no exact → offer alternates
                            lead = f"The first opening that day is {first_day}. " if first_day else ""
                            options = (same_day or nearest)[:3]
                            tail = (f"Closest options: {', '.join(options)}. What works best?"
                                    if options else "I can check other days too—what day works?")
                            await say(("That exact time isn’t open. " if not exact else "") + lead + tail)
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
                            # final email sanity
                            if not is_valid_email(payload["email"]):
                                await say("I want to make sure your confirmation reaches you—what’s the best email? I’ll read it back.")
                                continue

                            status, data = await json_post(MAIN_WEBHOOK_URL, payload)
                            body_status = data.get("status") if isinstance(data, dict) else None
                            phone_last4 = last4(payload_phone)
                            if status == 200 and body_status == "booked":
                                txt = ("All set. I’ve scheduled that in Eastern Time. "
                                       f"I have your email as {payload['email']}"
                                       + (f" and phone ending in {phone_last4}" if phone_last4 else "")
                                       + ". Anything else I can help with?")
                            elif status in (409, 422) or body_status in {"conflict","conflict_or_error"}:
                                txt = "Looks like that time isn’t available. Earlier or later that day, or a nearby day?"
                            else:
                                txt = "I couldn’t finalize that just now. Want me to try again or pick a different time?"
                            await say(txt)

                        elif name == "cancel_workflow":
                            payload = {
                                "action": "cancel",
                                "name": (args.get("name") or "").strip(),
                                "phone": args.get("phone") or caller_number or None,
                                "email": (args.get("email") or None)
                            }
                            if not CANCEL_WEBHOOK_URL:
                                await say("I can cancel that, but my cancel line isn’t connected yet. Want me to connect you to the Business Solutions Lead?")
                            else:
                                status, data = await json_post(CANCEL_WEBHOOK_URL, payload)
                                phone_last = last4(payload.get("phone"))
                                body_status = data.get("status") if isinstance(data, dict) else None
                                if status == 200 and body_status in {"cancelled","ok","success"}:
                                    txt = ("Done. I’ve canceled your appointment"
                                           + (f" for the number ending in {phone_last}" if phone_last else "")
                                           + (f" and email {payload.get('email')}" if payload.get("email") else "")
                                           + ". Anything else?")
                                elif status == 404 or body_status in {"not_found","conflict_or_error"}:
                                    txt = "I couldn’t find an active booking for that info. Do you use another email or phone?"
                                else:
                                    txt = "I hit a snag canceling that. Try again or connect you to the lead?"
                                await say(txt)

                    elif t == "error":
                        log.error(f"OpenAI error event: {evt}")
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
    for p in pending: p.cancel()
    for p in pending:
        with suppress(Exception, asyncio.CancelledError):
            await p
    await cleanup()
