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
OPENAI_API_KEY   = os.getenv("OPENAI_API_KEY", "")
TRANSFER_NUMBER  = os.getenv("TRANSFER_NUMBER", "")
TW_SID           = os.getenv("TWILIO_ACCOUNT_SID", "")
TW_TOKEN         = os.getenv("TWILIO_AUTH_TOKEN", "")
MAIN_WEBHOOK_URL = os.getenv("MAIN_WEBHOOK_URL", "")  # book+reschedule
CANCEL_WEBHOOK_URL = os.getenv("CANCEL_WEBHOOK_URL", "")
EVENT_TYPE_ID    = int(os.getenv("EVENT_TYPE_ID", "3117986"))
DEFAULT_TZ       = "America/New_York"

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
vals  = " • ".join(KB.get("value_props", [])) or "24/7 AI receptionist • instant answers • books & transfers calls"
trial = KB.get("trial_offer", "One-week free trial; install/uninstall is free.")

INSTRUCTIONS = (
    f"You are the {brand} AI receptionist (company based in {loc}). "
    "Start in ENGLISH. Only switch languages after the caller speaks a full sentence in that language or explicitly asks. "
    "Tone: warm, professional, friendly, emotion-aware. Keep replies to 1–2 concise sentences unless asked. "
    f"Value props: {vals}. Offer the trial when interest is shown: {trial}. "
    "Never invent business facts; if unsure, say so and offer to connect the caller. "
    "GOALS: (1) Explain benefits & answer questions. (2) Offer to book a demo/installation. (3) Transfer to a human on request. "
    "TOOLS:\n"
    " - appointment_webhook(book|reschedule): Schedules via our n8n main workflow.\n"
    " - cancel_workflow(cancel): Cancels via our n8n cancel workflow.\n"
    " - transfer_call(to?, reason?) → Bridge to a human now (omit 'to' to use the default).\n"
    " - end_call(reason?) → Politely end the call.\n"
    "IMPORTANT:\n"
    " - For booking/rescheduling: ALWAYS use tz 'America/New_York'. Collect name, email (for confirmations), phone, and time.\n"
    " - Prefer the callerID phone number; confirm by repeating the actual last 4 digits.\n"
    " - For cancel: need name + (phone OR email). Prefer phone. No time required.\n"
    " - When transferring, first say one short line like: “Absolutely—one moment while I connect you.”\n"
    " - When ending, first say one short line like: “Thanks for calling—ending the call now.”\n"
    "Then CALL the tool and stop speaking.\n"
    "TRANSFER INTENT examples: connect me, human, person, agent, manager, person in charge, owner, Jeriel, live agent."
)

INSTRUCTIONS += (
    "\n\nGREETING VARIANTS:\n"
    "• On the very first response of the call, start in English and use one of these exact lines:\n"
    "  - \"Hey, thank you for calling ELvara. What can I help you with?\"\n"
    "  - \"Hey—thanks for calling ELvara. How can I help you today?\"\n"
    "  - \"Hi, thanks for calling ELvara. How may I assist you?\"\n"
    "• Keep it to one sentence; do not add extra lines before the question.\n"
)

INSTRUCTIONS += (
    "\n\nDISCOVERY-FIRST, FLEXIBLE SCRIPTING:\n"
    "• When asked “What do you do?” or “How can this help?”, FIRST give a natural 1-sentence answer like: "
    "  “We provide business solutions that save you time and money and help you grow—things like CRM/workflow automations, websites/SEO, and custom apps.” "
    "  THEN immediately ask: “What kind of business do you run so I can tailor examples?”\n"
    "• Be flexible—paraphrase naturally and do NOT stick to any script word-for-word.\n"
    "• After they share the business, give 1–2 outcome-focused examples for that vertical, then a focused follow-up: "
    "  “What’s the top thing you want to improve—missed calls, no-shows, follow-ups, scheduling, payments, or job tracking?”\n"
    "• Pricing & timelines: both depend on needs/integrations; offer a one-week free trial and a tailored quote. "
    "  Setup time depends on chosen services (simple receptionist is quick; deeper POS/CRM or custom apps take longer).\n"
    "• Always offer a next step: (a) live transfer to the person in charge, or (b) book a demo/installation in Eastern Time."
)

INSTRUCTIONS += (
    "\n\nBOOKING EXECUTION + READBACK RULES:\n"
    "• When you have name, email, phone, and a time, CALL appointment_webhook immediately (booking_type 'book' or 'reschedule').\n"
    "• Before confirming or right after success, REPEAT BACK the details: name, email, phone (say only the last four digits), and the date/time in Eastern Time.\n"
    "• Do NOT say goodbye or end the call until the tool returns success or an error.\n"
    "• On conflict/error: briefly apologize and ask for an earlier/later time or nearby day."
)

# ---------- TwiML builders (SILENT) ----------
def transfer_twiml(to_number: str) -> str:
    vr = VoiceResponse()
    d = Dial(answer_on_bridge=True)
    d.number(to_number)
    vr.append(d)
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
    }

# ---------- TWILIO VOICE WEBHOOK (SILENT) ----------
@app.post("/twilio/voice")
async def voice(request: Request):
    host = os.getenv("RENDER_EXTERNAL_HOSTNAME") or request.url.hostname
    vr = VoiceResponse()
    vr.connect().stream(url=f"wss://{host}/media")
    log.info(f"Returning TwiML with stream URL: wss://{host}/media")
    return PlainTextResponse(str(vr), media_type="application/xml")

# ---------- MEDIA STREAM ----------
@app.websocket("/media")
async def media(ws: WebSocket):
    if not OPENAI_API_KEY:
        log.error("No OPENAI_API_KEY; closing media socket.")
        await ws.close(); return
    await ws.accept()

    # Wait for Twilio 'start'
    stream_sid = call_sid = caller_number = caller_last4 = None
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
                log.info(f"Twilio stream started: streamSid={stream_sid}, callSid={call_sid}, caller_raw={caller_raw!r}, caller_e164={caller_number!r}, last4={caller_last4!r}")
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
                tw_client.calls(call_sid).update(twiml=transfer_twiml(TRANSFER_NUMBER))
        await ws.close(); return

    async def cleanup():
        with suppress(Exception): await oai.close()
        with suppress(Exception): await session.close()
        with suppress(Exception): await ws.close()

    # ---------- Tools ----------
    tools = [
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
                    "startTime": {"type": "string", "description": "ISO 8601 with offset, e.g., 2025-09-26T11:30:00-04:00"},
                    "event_type_id": {"type": "integer", "default": EVENT_TYPE_ID}
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
        dyn_instructions = (
            INSTRUCTIONS +
            f"\nRUNTIME CONTEXT:\n- caller_id_e164={caller_number or 'unknown'}\n"
            f"- caller_id_last4={caller_last4 or 'unknown'}\n"
            f"- ALWAYS use timezone: {DEFAULT_TZ}\n" +
            (
                f"- If phone is missing from the user, propose using caller_id_e164 and confirm explicitly: "
                f"“Is this the best number ending in {caller_last4}?”\n" if caller_last4 else
                "- If phone is missing from the user, ask for it directly and confirm back the digits.\n"
            )
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
                "input_audio_transcription": {
                    "model": "gpt-4o-mini-transcribe",
                    "language": "en",
                    "prompt": "Transcribe in English. Only switch if the caller speaks a full sentence in another language."
                },
                "instructions": dyn_instructions,
                "tools": tools
            }
        })
        # Initial greeting — choose one variant (verbatim)
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
                tw_client.calls(call_sid).update(twiml=transfer_twiml(TRANSFER_NUMBER))
        await cleanup(); return

    # ---------- State ----------
    call_map: dict[str, dict] = {}
    user_buf: list[str] = []
    pending_action: dict | None = None

    # ---------- Twilio actions ----------
    def do_transfer(reason: str | None = None, to: str | None = None):
        target = to or TRANSFER_NUMBER
        if not (tw_client and call_sid and valid_e164(target)):
            log.error(f"TRANSFER blocked (client? {bool(tw_client)} call? {bool(call_sid)} target_valid? {valid_e164(target)})")
            return
        log.info(f"TRANSFERRING via Twilio REST -> {target} reason={reason!r}")
        with suppress(Exception):
            tw_client.calls(call_sid).update(twiml=transfer_twiml(target))

    def do_hangup(reason: str | None = None):
        if not (tw_client and call_sid):
            log.error("HANGUP blocked (missing Twilio client or call_sid)")
            return
        log.info(f"HANGUP via Twilio REST. reason={reason!r}")
        with suppress(Exception):
            tw_client.calls(call_sid).update(status="completed")

    async def announce_then(action: dict):
        nonlocal pending_action
        say = "Absolutely—one moment while I connect you." if action.get("type") == "transfer" else "Thanks for calling—ending the call now."
        pending_action = action
        log.info(f"ANNOUNCE queued → {action}")
        await oai.send_json({
            "type": "response.create",
            "response": {
                "modalities": ["audio", "text"],
                "instructions": f"Say exactly: \"{say}\" Keep it under 2 seconds."
            }
        })

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
                # compact log
                preview = data if isinstance(data, str) else {k: data.get(k) for k in ("status","code","event_id","start_iso")}
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

                    # When the announcement finishes, run the pending action
                    elif t == "response.done" and pending_action:
                        action = pending_action
                        pending_action = None
                        log.info(f"ANNOUNCE done → executing {action}")
                        if action.get("type") == "transfer":
                            do_transfer(reason=action.get("reason"), to=action.get("to"))
                        else:
                            do_hangup(reason=action.get("reason"))

                    # user utterance completed → backup intent
                    elif t == "conversation.item.input_audio_transcription.completed":
                        text = "".join(user_buf).strip()
                        user_buf.clear()
                        if text:
                            low = text.lower()
                            if re.search(r"(transfer|connect|human|agent|representative|manager|owner|person\s+in\s+charge|live\s+agent|operator|jeriel)\b", low):
                                log.info(f"SERVER-INTENT: transfer (from user text): {text!r}")
                                await announce_then({"type":"transfer","reason":"user_requested_transfer"})
                            elif re.search(r"(hang\s*up|end\s+the\s+call|that's\s+all|that is all|bye|good\s*bye|not\s+interested|we're\s+done|colgar|terminar.*llamada|ad(i|í)os)", low):
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

                            # ---- Book/Reschedule ----
                            if name == "appointment_webhook":
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
                                    "idempotency_key": f"ai-{uuid.uuid4().hex}"
                                }
                                status, data = await json_post(MAIN_WEBHOOK_URL, payload)
                                phone_last4 = last4(payload_phone)

                                # n8n often responds 200 for both success & conflict; inspect JSON body
                                body_status = data.get("status") if isinstance(data, dict) else None
                                is_booked = (status == 200 and body_status == "booked")
                                is_conflict = (status in (409, 422)) or (status == 200 and body_status == "conflict_or_error")

                                if is_booked:
                                    readback = (
                                        f"All set. I’ve scheduled that in Eastern Time. "
                                        f"I have your email as {payload['email']}"
                                        + (f" and phone ending in {phone_last4}" if phone_last4 else "")
                                        + ". You’ll receive confirmation emails with calendar invites. "
                                        "Anything else I can help you with?"
                                    )
                                    await oai.send_json({"type": "response.create","response": {"modalities": ["audio","text"], "instructions": readback}})
                                elif is_conflict:
                                    await oai.send_json({
                                        "type": "response.create",
                                        "response": {
                                            "modalities": ["audio","text"],
                                            "instructions": (
                                                "Looks like that time isn’t available. Would you like an earlier or later time the same day, "
                                                "or a nearby day around that time?"
                                            )
                                        }
                                    })
                                else:
                                    await oai.send_json({
                                        "type": "response.create",
                                        "response": {
                                            "modalities": ["audio","text"],
                                            "instructions": (
                                                "I couldn’t finalize that just now. Would you like me to try again, or pick a different time?"
                                            )
                                        }
                                    })

                            # ---- Cancel ----
                            elif name == "cancel_workflow":
                                payload = {
                                    "action": "cancel",
                                    "name": (args.get("name") or "").strip(),
                                    "phone": args.get("phone") or caller_number or None,
                                    "email": (args.get("email") or None)
                                }
                                if not CANCEL_WEBHOOK_URL:
                                    log.error("CANCEL_WEBHOOK_URL not set; cannot cancel.")
                                    await oai.send_json({
                                        "type": "response.create",
                                        "response": {
                                            "modalities": ["audio","text"],
                                            "instructions": (
                                                "I can cancel that, but my cancel line isn’t connected yet. "
                                                "Could I connect you to the person in charge, or would you like me to take a message?"
                                            )
                                        }
                                    })
                                else:
                                    status, data = await json_post(CANCEL_WEBHOOK_URL, payload)
                                    phone_last4 = last4(payload.get("phone"))
                                    body_status = data.get("status") if isinstance(data, dict) else None
                                    is_cancelled = (status == 200 and body_status in {"cancelled","ok","success"})
                                    not_found = (status == 404) or (status == 200 and body_status in {"not_found","conflict_or_error"})

                                    if is_cancelled:
                                        readback = (
                                            "Done. I’ve canceled your appointment"
                                            + (f" for the number ending in {phone_last4}" if phone_last4 else "")
                                            + (f" and email {payload.get('email')}" if payload.get("email") else "")
                                            + ". Is there anything else I can help you with?"
                                        )
                                        await oai.send_json({"type": "response.create","response": {"modalities": ["audio","text"], "instructions": readback}})
                                    elif not_found:
                                        await oai.send_json({
                                            "type": "response.create",
                                            "response": {
                                                "modalities": ["audio","text"],
                                                "instructions": (
                                                    "I couldn’t find an active booking for that name and number. "
                                                    "Do you use another email or phone I can try?"
                                                )
                                            }
                                        })
                                    else:
                                        await oai.send_json({
                                            "type": "response.create",
                                            "response": {
                                                "modalities": ["audio","text"],
                                                "instructions": (
                                                    "I ran into a problem canceling that. Want me to try again or connect you to the person in charge?"
                                                )
                                            }
                                        })

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
