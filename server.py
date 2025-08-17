# server.py
import os, json, asyncio, logging, aiohttp, re
from contextlib import suppress
from fastapi import FastAPI, Request, WebSocket
from fastapi.responses import PlainTextResponse
from twilio.twiml.voice_response import VoiceResponse, Dial
from twilio.rest import Client

# ---------- LOGGING ----------
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("elevara")

app = FastAPI()

# ---------- ENV ----------
OPENAI_API_KEY  = os.getenv("OPENAI_API_KEY", "")
TRANSFER_NUMBER = os.getenv("TRANSFER_NUMBER", "")
TW_SID          = os.getenv("TWILIO_ACCOUNT_SID", "")
TW_TOKEN        = os.getenv("TWILIO_AUTH_TOKEN", "")
tw_client = Client(TW_SID, TW_TOKEN) if (TW_SID and TW_TOKEN) else None

def valid_e164(n: str | None) -> bool:
    return bool(n and re.fullmatch(r"\+\d{7,15}", n))

# ---------- BUSINESS PROMPT ----------
def load_kb():
    try:
        with open("business.json", "r") as f:
            return json.load(f)
    except Exception:
        return {}

KB    = load_kb()
brand = KB.get("brand", "Elevara")
loc   = KB.get("location", "Philadelphia, PA")
vals  = " • ".join(KB.get("value_props", [])) or "24/7 AI receptionist • instant answers • books & transfers calls"
trial = KB.get("trial_offer", "One-week free trial; install/uninstall is free.")

INSTRUCTIONS = (
    f"You are the {brand} AI receptionist (company based in {loc}). "
    "Default to ENGLISH. Only switch languages after the caller speaks a full sentence in that language "
    "(or explicitly asks). If they code-switch briefly, keep replying in English. "
    "Tone: warm, professional, friendly, emotion-aware. Keep replies to 1–2 concise sentences unless asked. "
    f"Value props: {vals}. Offer the trial when interest is shown: {trial}. "
    "Never invent business facts; if unsure, say so and offer to connect the caller. "
    "GOALS: (1) Explain benefits & answer questions. (2) Offer to book a demo. (3) Transfer to a human on request. "
    "TOOLS:\n"
    " - transfer_call(to?, reason?) → Bridge to a human now (omit 'to' to use the default).\n"
    " - end_call(reason?) → Politely end the call.\n"
    "IMPORTANT:\n"
    " - When transferring, first say one short line like: “Absolutely—one moment while I connect you.”\n"
    " - When ending, first say one short line like: “Thanks for calling—ending the call now.”\n"
    "Then CALL the tool and stop speaking.\n"
    "TRANSFER INTENT examples: connect me, human, person, agent, manager, person in charge, owner, Jeriel, live agent."
)

# ---------- TwiML builders (SILENT) ----------
def transfer_twiml(to_number: str) -> str:
    vr = VoiceResponse()
    d = Dial(answer_on_bridge=True)  # silent while dialing (carrier ringback may still be heard)
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
    }

# ---------- TWILIO VOICE WEBHOOK (SILENT) ----------
@app.post("/twilio/voice")
async def voice(request: Request):
    host = os.getenv("RENDER_EXTERNAL_HOSTNAME") or request.url.hostname
    vr = VoiceResponse()
    # No <Say>; the AI owns the greeting
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
    stream_sid = call_sid = twilio_number = None
    try:
        while True:
            first = await asyncio.wait_for(ws.receive_text(), timeout=10)
            data0 = json.loads(first)
            if data0.get("event") == "start":
                stream_sid = data0["start"]["streamSid"]
                call_sid   = data0["start"].get("callSid", "")
                twilio_number = data0["start"].get("to")
                log.info(f"Twilio stream started: streamSid={stream_sid}, callSid={call_sid}")
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
                "instructions": INSTRUCTIONS,
                "tools": tools
            }
        })
        # Initial greeting
        await oai.send_json({
            "type": "response.create",
            "response": {
                "modalities": ["audio", "text"],
                "tool_choice": "auto",
                "instructions": "Start in ENGLISH. Greet briefly and ask how you can help."
            }
        })
    except Exception as e:
        log.error(f"OpenAI session.setup failed: {e}")
        if tw_client and call_sid and valid_e164(TRANSFER_NUMBER):
            with suppress(Exception):
                tw_client.calls(call_sid).update(twiml=transfer_twiml(TRANSFER_NUMBER))
        await cleanup(); return

    # ---------- State ----------
    call_map: dict[str, dict] = {}   # call_id -> {"name": str, "args": str}
    user_buf: list[str] = []         # accumulate user transcript per turn

    # Announcement queue: after the next response.done, run action
    pending_action: dict | None = None  # {"type":"transfer"/"hangup", "to":..., "reason":...}

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
        """Queue a short spoken confirmation, then run the action after response.done."""
        nonlocal pending_action
        if action.get("type") == "transfer":
            say = "Absolutely—one moment while I connect you."
        else:
            say = "Thanks for calling—ending the call now."
        pending_action = action
        log.info(f"ANNOUNCE queued → {action}")
        await oai.send_json({
            "type": "response.create",
            "response": {
                "modalities": ["audio", "text"],
                "instructions": f"Say exactly: \"{say}\" Keep it under 2 seconds."
            }
        })

    # Simple server-side intent backup (EN + ES)
    TRANSFER_PAT = re.compile(
        r"(transfer|connect|patch|talk\s+to|speak\s+to|human|agent|representative|manager|owner|person\s+in\s+charge|someone|live\s+agent|operator|jeriel|person)\b"
        r"|(\btransferir\b|\bconectar\b|\bagente\b|\bhumano\b|\bpersona\b|\bencargad[oa]\b|\bdueñ[oa]\b|\boperador[ae]?\b)",
        re.IGNORECASE
    )
    HANGUP_PAT = re.compile(
        r"(hang\s*up|end\s+the\s+call|that'?s\s+all|that is all|bye|good\s*bye|not\s+interested|we'?re\s+done)"
        r"|(\bcolgar\b|\bterminar\b.*\bllamad[ao]?\b|\bad(i|í)os\b|\bno\s+interesad[oa]\b)",
        re.IGNORECASE
    )

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
                            if TRANSFER_PAT.search(low):
                                log.info(f"SERVER-INTENT: transfer (from user text): {text!r}")
                                await announce_then({"type":"transfer","reason":"user_requested_transfer"})
                            elif HANGUP_PAT.search(low):
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
                            if name == "transfer_call":
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
                        await ws.send_text(json.dumps({
                            "event": "mark",
                            "streamSid": stream_sid,
                            "mark": {"name": "hb"}
                        }))
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
        with suppress(Exception, asyncio.CancelledError):
            await p
    await cleanup()
