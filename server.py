# server.py
"""
ELvara AI Receptionist — Twilio <-> OpenAI Realtime bridge

========================
WEBHOOKS: WHAT/WHEN/HOW
========================
We never call a webhook without the required info. The model is instructed to
FIRST speak a quick acknowledgement (e.g., “One moment while I check…”), THEN call the tool.

1) MAIN_WEBHOOK_URL (n8n)
   a) checkAvailableSlot
      - WHEN: Any time the caller mentions a day/time (even vaguely).
      - NEEDS: startTime (ISO with offset), tz, event_type_id, search_days.
      - DEFAULTS:
          * If the caller is not specific: use TODAY at a sensible next slot in ET
            (3:00 PM if earlier; otherwise the next top-of-hour, not past 7:00 PM).
          * search_days default = 14; if no nearby options, we expand to 30 once.
      - RETURNS (example):
          {
            "status": "ok",
            "exactMatch": false,
            "matchedSlot": null,
            "sameDayAlternates": [],
            "nearest": ["2025-09-05T13:00:00.000Z", "2025-09-05T13:30:00.000Z"],
            "firstAvailableThatDay": "2025-09-05T10:00:00.000Z"
          }
      - MODEL BEHAVIOR:
          * Say “One moment while I check availability…”, then call check_slots().
          * If exactMatch=true AND we already have name+valid email+phone:
              auto-proceed to booking.
          * Otherwise, ask only for what’s missing (read email back).

   b) book / reschedule
      - WHEN: After checkAvailableSlot confirms a time AND we have identity.
      - NEEDS: booking_type ("book"|"reschedule"), name, email, phone (E.164),
               startTime (ISO), tz, event_type_id, idempotency_key.
      - MUST: read back the email aloud; confirm last-4 of phone.

2) CANCEL_WEBHOOK_URL (n8n)
   - WHEN: Caller asks to cancel.
   - NEEDS: name + (phone OR email). Prefer phone; use caller ID if they agree.

VOICE / UX RULES
- Default to ENGLISH. Only auto-switch if the caller speaks another language.
- If you don’t understand: say once “Sorry—I didn’t catch that, could you repeat?”
  If it happens twice: add “Calls are clearest off speaker or headphones.”
- Before any potentially long action (slot check, booking, cancel), say a quick
  acknowledgement so there’s no dead air.

TECH NOTES
- We let the model create a single response that *speaks first* then calls a tool.
- We ALWAYS return tool results to the model using `response.function_call_output`
  (this avoids “conversation_already_has_active_response”).
- All time math is timezone-aware; the assistant announces the actual default time
  it’s checking (e.g., “Today 4:00 PM Eastern”).
"""

import os, json, asyncio, logging, aiohttp, re, uuid
from datetime import datetime, time
from zoneinfo import ZoneInfo
from contextlib import suppress
from fastapi import FastAPI, Request, WebSocket
from fastapi.responses import PlainTextResponse
from twilio.twiml.voice_response import VoiceResponse, Dial
from twilio.rest import Client
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
MAIN_WEBHOOK_URL   = os.getenv("MAIN_WEBHOOK_URL", "")   # checkAvailableSlot + book/reschedule
CANCEL_WEBHOOK_URL = os.getenv("CANCEL_WEBHOOK_URL", "")
EVENT_TYPE_ID      = int(os.getenv("EVENT_TYPE_ID", "3117986"))
DEFAULT_TZ         = "America/New_York"
PUBLIC_BASE_URL    = os.getenv("PUBLIC_BASE_URL", "")    # e.g. https://your-app.onrender.com

tw_client = Client(TW_SID, TW_TOKEN) if (TW_SID and TW_TOKEN) else None

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

def default_today_sensible_et_iso() -> str:
    """
    If before 3pm → 3:00 PM today; else next top-of-hour today (not past 7:00 PM).
    """
    now = datetime.now(ZoneInfo(DEFAULT_TZ))
    target = datetime.combine(now.date(), time(15, 0), tzinfo=ZoneInfo(DEFAULT_TZ))
    if now > target:
        hour = min(now.replace(minute=0, second=0, microsecond=0).hour + 1, 19)
        target = datetime.combine(now.date(), time(hour, 0), tzinfo=ZoneInfo(DEFAULT_TZ))
    return target.isoformat()

def human_et_from_iso(iso_str: str) -> str:
    dt = datetime.fromisoformat(iso_str)
    dt = dt.astimezone(ZoneInfo(DEFAULT_TZ))
    today = datetime.now(ZoneInfo(DEFAULT_TZ)).date()
    prefix = "Today " if dt.date() == today else dt.strftime("%a ")
    return prefix + dt.strftime("%-I:%M %p")

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
greeting_templates = KB.get("greeting_templates", {}).get("primary", [
    "Hey, thanks for calling ELvara. How can I help you today?",
    "Hi there, this is ELvara. What can I do for you?",
    "Thanks for calling ELvara. What brings you here today?"
])
value_prop = KB.get("value_proposition", {})
elevator_pitch = value_prop.get("elevator_pitch", "We build custom business solutions that save you time and help you grow.")
key_benefits = value_prop.get("key_benefits", []) or [
    "24/7 AI receptionist", "Custom automations", "SEO websites"
]
trial_offer = KB.get("pricing", {}).get("trial_offer", "One-week free trial. Free setup/removal. No contracts.")

INSTRUCTIONS = f"""
You are the {brand} AI receptionist based in {loc}.
Default to ENGLISH; only auto-switch if the caller speaks another language.
Be warm, concise (1–2 sentences), and outcome-focused.

VALUE: {elevator_pitch}
KEY BENEFITS: {' • '.join(key_benefits)}
TRIAL: {trial_offer}

BOOKING PLAYBOOK
1) Ask what they need and their preferred day/time.
2) When you intend to check availability, in ONE response:
   - First SAY: "One moment while I check availability."
   - Then CALL the tool: check_slots(startTime, search_days?).
   - StartTime defaults to a sensible time TODAY in Eastern if they are vague.
3) If check shows an exact opening and you already have name + valid email + phone:
   - Proceed to booking (appointment_webhook).
4) Otherwise, ask ONLY for missing items. ALWAYS read the email back verbatim.
5) After booking: repeat name, email, last-4 phone, and the date/time in Eastern.

CANCELS: Need name + (phone OR email). Prefer phone; use caller ID if they agree.

CLARITY RULES
- If you don’t understand, say once: “Sorry—I didn’t catch that, could you repeat?”
- If it happens twice, add: “Calls are clearest off speaker or headphones.”

Start with EXACTLY one greeting line from this set:
  {greeting_templates[0]}
  {greeting_templates[1]}
  {greeting_templates[2]}
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
                log.info(f"Twilio stream start: streamSid={stream_sid}, callSid={call_sid}, caller={caller_number!r}, reason={rejoin_reason!r}")
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

    # ---------- Tools (schema only) ----------
    tools = [
        {
            "type": "function",
            "name": "check_slots",
            "description": "Check availability. Call this as soon as the caller mentions any time. Speak first, then call.",
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
            "description": "Book/reschedule. Only call after check_slots confirms time AND identity is complete.",
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
                    "silence_duration_ms": 1000,   # responsive, but not too twitchy
                    "create_response": True,       # let the model create ONE response per utterance
                    "interrupt_response": True
                },
                "input_audio_format": "g711_ulaw",
                "output_audio_format": "g711_ulaw",
                "voice": "alloy",
                "modalities": ["audio","text"],
                "input_audio_transcription": {"model": "gpt-4o-mini-transcribe", "language": "en"},
                "instructions": (
                    INSTRUCTIONS +
                    f"\nCURRENT_TIME_ET: {now_line}\nTIMEZONE: {DEFAULT_TZ}\nCALLER_LAST4: {caller_last4 or 'unknown'}\n"
                    "IMPORTANT: When you call a tool, do it in the SAME response in which you just spoke "
                    "(e.g., 'One moment…' then call the tool)."
                ),
                "tools": tools
            }
        })

        line = ("Looks like no one is available right now. I can book you for later today or tomorrow—what time works? (Eastern Time)"
                if rejoin_reason == "transfer_fail"
                else ('Say EXACTLY one of: '
                      '"Hey, thanks for calling ELvara. How can I help you today?" '
                      'OR "Hi there, this is ELvara. What can I do for you?" '
                      'OR "Thanks for calling ELvara. What brings you here today?"'))
        await oai.send_json({"type":"response.create","response":{"modalities":["audio","text"],"instructions": line}})
    except Exception as e:
        log.error(f"OpenAI session.setup failed: {e}")
        if tw_client and call_sid and valid_e164(TRANSFER_NUMBER):
            with suppress(Exception):
                action_url = (PUBLIC_BASE_URL.rstrip("/") + "/twilio/after-transfer") if PUBLIC_BASE_URL else None
                tw_client.calls(call_sid).update(twiml=transfer_twiml(TRANSFER_NUMBER, action_url=action_url))
        await cleanup(); return

    # ---------- State holders ----------
    call_map: Dict[str, Dict[str, str]] = {}  # call_id -> {"name":..., "args": "..."}
    user_buf: List[str] = []

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

    # ---------- Tool result bridge ----------
    async def send_tool_output(call_id: str, obj: dict):
        # Feed tool result back into the SAME active response
        await oai.send_json({
            "type": "response.function_call_output",
            "call_id": call_id,
            "output": json.dumps(obj)
        })

    # ---------- Pumps ----------
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
                    log.info(f"Twilio sent stop. reason={reason!r}")
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

                    # Transcripts
                    if t == "conversation.item.input_audio_transcription.delta":
                        user_buf.append(evt.get("delta") or "")

                    # Assistant audio → Twilio
                    if t == "response.audio.delta" and stream_sid:
                        try:
                            await ws.send_text(json.dumps({"event":"media","streamSid": stream_sid,"media":{"payload": evt["delta"]}}))
                        except Exception:
                            break

                    # Barge-in: clear Twilio stream buffer
                    elif t == "input_audio_buffer.speech_started" and stream_sid:
                        with suppress(Exception):
                            await ws.send_text(json.dumps({"event":"clear","streamSid": stream_sid}))

                    # Utterance done (user finished speaking)
                    elif t == "conversation.item.input_audio_transcription.completed":
                        text = "".join(user_buf).strip(); user_buf.clear()
                        if not text or len(text.split()) <= 2:
                            # Short/unclear → gentle ask; model will create the response itself.
                            await oai.send_json({"type":"response.create","response":{"modalities":["audio","text"],"instructions":
                                "Sorry—I didn’t catch that, could you repeat? If it’s still hard to hear, mention that calls are clearest off speaker or headphones."}})
                            continue

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
                        entry = call_map[cid]
                        name  = entry.get("name")
                        try: args = json.loads(entry.get("args") or "{}")
                        except Exception: args = {}

                        # Run the tool and return results to the SAME response:
                        await handle_tool_call_and_return(cid, name, args)

                    elif t == "error":
                        log.error(f"OpenAI error event: {evt}")
                        await oai.send_json({"type":"response.create","response":{"modalities":["audio","text"],"instructions":
                            "Sorry—something glitched on my end. Could you say that again?"}})

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
                        await ws.send_text(json.dumps({"event":"mark","streamSid": stream_sid,"mark":{"name":"hb"}}))
        except asyncio.CancelledError:
            pass
        except Exception:
            pass

    # ---------- Tool handlers (return results to model) ----------
    async def handle_tool_call_and_return(call_id: str, tool_name: str, args: Dict[str, Any]):
        # Tool: check_slots
        if tool_name == "check_slots":
            start_time   = (args.get("startTime") or "").strip()
            search_days  = int(args.get("search_days") or 14)
            if not start_time:
                start_time = default_today_sensible_et_iso()

            if not MAIN_WEBHOOK_URL:
                await send_tool_output(call_id, {
                    "status": "error",
                    "code": "webhook_unconfigured",
                    "message": "Booking line is not connected."
                })
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
            # Normalize shape for the model
            result = {
                "status": "ok" if status == 200 else "error",
                "http": status,
                "requestedStart": start_time,
                "humanRequested": human_et_from_iso(start_time),
                "exactMatch": bool(body.get("exactMatch")),
                "matchedSlot": body.get("matchedSlot"),
                "sameDayAlternates": body.get("sameDayAlternates") or [],
                "nearest": body.get("nearest") or [],
                "firstAvailableThatDay": body.get("firstAvailableThatDay"),
            }
            # If no options and we searched only 14 days, hint to expand to 30
            if not result["exactMatch"] and not (result["sameDayAlternates"] or result["nearest"]) and search_days < 30:
                result["hint"] = "expand_search_to_30_days"
            await send_tool_output(call_id, result)
            return

        # Tool: appointment_webhook
        if tool_name == "appointment_webhook":
            req = ["booking_type","name","email","phone","startTime"]
            phone = (args.get("phone") or caller_number or "").strip()
            args["phone"] = phone

            missing = [k for k in req if not (args.get(k) or "").strip()]
            if "email" not in missing and not is_valid_email(args.get("email")):
                missing.append("email")

            if missing:
                await send_tool_output(call_id, {
                    "status": "error",
                    "code": "missing_fields",
                    "missing": missing,
                    "message": "To book we need: " + ", ".join(missing)
                })
                return

            if not MAIN_WEBHOOK_URL:
                await send_tool_output(call_id, {
                    "status": "error",
                    "code": "webhook_unconfigured",
                    "message": "Booking line is not connected."
                })
                return

            payload = {
                "tool": "book",
                "booking_type": args.get("booking_type","book"),
                "name": args["name"].strip(),
                "email": args["email"].strip(),
                "phone": args["phone"],
                "tz": DEFAULT_TZ,
                "startTime": args["startTime"].strip(),
                "event_type_id": int(args.get("event_type_id", EVENT_TYPE_ID)),
                "idempotency_key": f"ai-{uuid.uuid4().hex}",
                "notes": (args.get("notes") or None)
            }
            status, data = await json_post(MAIN_WEBHOOK_URL, payload)
            body = data if isinstance(data, dict) else {}
            await send_tool_output(call_id, {
                "status": body.get("status") if status == 200 else "error",
                "http": status,
                "bookedStart": payload["startTime"],
                "humanBooked": human_et_from_iso(payload["startTime"]),
                "email": payload["email"],
                "phone_last4": last4(payload["phone"]),
                "raw": body
            })
            return

        # Tool: cancel_workflow
        if tool_name == "cancel_workflow":
            name = (args.get("name") or "").strip()
            phone = args.get("phone") or caller_number or None
            email = (args.get("email") or None)
            if not name or not (phone or email):
                await send_tool_output(call_id, {
                    "status": "error",
                    "code": "missing_fields",
                    "missing": [m for m in ["name", "phone_or_email"] if (m=="name" and not name) or (m=="phone_or_email" and not (phone or email))],
                    "message": "To cancel we need a name and a phone or email."
                })
                return
            if not CANCEL_WEBHOOK_URL:
                await send_tool_output(call_id, {
                    "status": "error",
                    "code": "webhook_unconfigured",
                    "message": "Cancel line is not connected."
                })
                return
            payload = {"action":"cancel","name":name,"phone":phone,"email":email}
            status, data = await json_post(CANCEL_WEBHOOK_URL, payload)
            body = data if isinstance(data, dict) else {}
            await send_tool_output(call_id, {
                "status": body.get("status") if status == 200 else "error",
                "http": status,
                "phone_last4": last4(phone),
                "raw": body
            })
            return

        # Tool: transfer_call
        if tool_name == "transfer_call":
            target = args.get("to") or TRANSFER_NUMBER
            ok = bool(tw_client and call_sid and valid_e164(target))
            if ok:
                action_url = (PUBLIC_BASE_URL.rstrip("/") + "/twilio/after-transfer") if PUBLIC_BASE_URL else None
                with suppress(Exception):
                    tw_client.calls(call_sid).update(twiml=transfer_twiml(target, action_url=action_url))
            await send_tool_output(call_id, {
                "status": "ok" if ok else "error",
                "message": "Transferring now." if ok else "Transfer not configured."
            })
            return

        # Unknown tool
        await send_tool_output(call_id, {"status":"error","code":"unknown_tool","message":tool_name})

    # ---------- Run pumps ----------
    t1 = asyncio.create_task(twilio_to_openai())
    t2 = asyncio.create_task(openai_to_twilio())
    t3 = asyncio.create_task(twilio_heartbeat())
    done, pending = await asyncio.wait({t1, t2, t3}, return_when=asyncio.FIRST_COMPLETED)
    for p in pending: p.cancel()
    for p in pending:
        with suppress(Exception, asyncio.CancelledError): await p
    await cleanup()
