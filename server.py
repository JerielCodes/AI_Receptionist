# server.py
import os, json, asyncio, logging, aiohttp
from fastapi import FastAPI, Request, WebSocket
from fastapi.responses import PlainTextResponse
from twilio.twiml.voice_response import VoiceResponse, Dial
from twilio.rest import Client

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("elevara")

app = FastAPI()

# ---------- ENV ----------
OPENAI_API_KEY  = os.getenv("OPENAI_API_KEY", "")
TRANSFER_NUMBER = os.getenv("TRANSFER_NUMBER", "+1YOURCELLPHONE")  # <-- set to your cell (E.164)
TW_SID          = os.getenv("TWILIO_ACCOUNT_SID", "")
TW_TOKEN        = os.getenv("TWILIO_AUTH_TOKEN", "")
tw_client = Client(TW_SID, TW_TOKEN) if (TW_SID and TW_TOKEN) else None

# ---------- BUSINESS PROMPT ----------
def load_kb():
    try:
        with open("business.json", "r") as f:
            return json.load(f)
    except Exception:
        return {}

KB = load_kb()
brand = KB.get("brand", "Elevara")
owner = KB.get("owner_name", "Jeriel")  # internal only; the AI won’t say this unless asked
loc   = KB.get("location", "Philadelphia, PA")
vals  = " • ".join(KB.get("value_props", [])) or "24/7 AI receptionist • instant answers • books & transfers calls"
trial = KB.get("trial_offer", "One-week free trial; install/uninstall is free.")

INSTRUCTIONS = (
    # Identity & tone
    f"You are the {brand} AI receptionist. You represent the product {brand}, not a person. "
    f"The company is based in {loc}. "
    "Start in ENGLISH; switch to SPANISH only if the caller clearly uses Spanish or asks. "
    "Tone: warm, professional, friendly, emotion-aware. Keep replies to 1–2 concise sentences unless asked. "
    # What to say / not say
    f"Value props: {vals}. Offer the trial when interest is shown: {trial}. "
    "Never invent business facts; if unsure, say so and offer to connect the caller. "
    # Goals
    "GOALS: (1) Explain benefits & answer questions. (2) Offer to book a demo. (3) Transfer to a human on request. "
    # Tool policy
    "TOOLS:\n"
    " - transfer_call(to, reason?) → Bridge the caller to a human immediately.\n"
    " - end_call(reason?) → Politely end the call.\n"
    # Intent mapping
    "INTENT MAP (use the tools; don't just say you'll do it):\n"
    " - Transfer if caller says anything like: transfer, connect me, speak to a human, talk to a person, "
    "   the manager, the person in charge, the owner, a representative, operator, live agent, or similar.\n"
    " - End the call if caller says anything like: that's all, I'm done, no thanks, not interested, "
    "   call me later, goodbye/bye/thanks bye, hang up, end the call.\n"
    # Confirmation rules
    "CONFIRMATION RULES:\n"
    " - For transfer: brief one-liner such as “Absolutely—one moment while I connect you to the person in charge,” "
    "   then call transfer_call and stop speaking.\n"
    " - For end: brief closing such as “Thanks for calling—ending the call now,” "
    "   then call end_call and stop speaking.\n"
    "After you call a tool, do not continue speaking."
)

def transfer_twiml(to_number: str, caller_id: str | None) -> str:
    """Create TwiML that bridges the current call to `to_number`."""
    vr = VoiceResponse()
    vr.say("Connecting you now.")
    d = Dial(answer_on_bridge=True)
    # Let Twilio use your Twilio number as caller ID by default (safer).
    d.number(to_number)
    vr.append(d)
    return str(vr)

# ---------- HEALTH ----------
@app.get("/")
def health():
    return {"ok": True, "ai_enabled": bool(OPENAI_API_KEY)}

# ---------- TWILIO VOICE WEBHOOK ----------
@app.post("/twilio/voice")
async def voice(request: Request):
    host = os.getenv("RENDER_EXTERNAL_HOSTNAME") or request.url.hostname
    vr = VoiceResponse()
    # Small TTS so Twilio hears immediate audio and keeps the stream open
    vr.say(f"Thanks for calling {brand}. One moment while I connect you.")
    vr.connect().stream(url=f"wss://{host}/media")
    log.info(f"Returning TwiML with stream URL: wss://{host}/media")
    return PlainTextResponse(str(vr), media_type="application/xml")

# ---------- MEDIA STREAM ----------
@app.websocket("/media")
async def media(ws: WebSocket):
    if not OPENAI_API_KEY:
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
                # 'to' is your Twilio number; fine if absent
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
        # Fallback: transfer straight away if AI can’t join
        if tw_client and call_sid:
            try:
                tw_client.calls(call_sid).update(twiml=transfer_twiml(TRANSFER_NUMBER, twilio_number))
            except Exception as ee:
                log.error(f"Transfer fallback failed: {ee}")
        await ws.close(); return

    async def cleanup():
        try: await oai.close()
        except Exception: pass
        try: await session.close()
        except Exception: pass
        try: await ws.close()
        except Exception: pass

    # ---------- Realtime Session Setup ----------
    tools = [
        {
            "type": "function",
            "name": "transfer_call",
            "description": "Bridge the caller to a human immediately.",
            "parameters": {
                "type": "object",
                "properties": {
                    "to": {"type": "string", "description": "E.164 phone number to transfer to"},
                    "reason": {"type": "string", "description": "Short reason for the transfer"}
                },
                "required": ["to"]
            }
        },
        {
            "type": "function",
            "name": "end_call",
            "description": "Politely end the call.",
            "parameters": {
                "type": "object",
                "properties": {
                    "reason": {"type": "string", "description": "Optional closing reason"}
                }
            }
        }
    ]

    try:
        await oai.send_json({
            "type": "session.update",
            "session": {
                "turn_detection": {"type": "server_vad", "silence_duration_ms": 1000},
                "input_audio_format": "g711_ulaw",
                "output_audio_format": "g711_ulaw",
                "voice": "alloy",
                "modalities": ["audio", "text"],
                "instructions": INSTRUCTIONS,
                "tools": tools
            }
        })
        # Immediate, English greeting
        await oai.send_json({
            "type": "response.create",
            "response": {
                "modalities": ["audio", "text"],
                "instructions": "Start in ENGLISH. Greet briefly and ask how you can help."
            }
        })
    except Exception as e:
        log.error(f"OpenAI session.setup failed: {e}")
        if tw_client and call_sid:
            try:
                tw_client.calls(call_sid).update(twiml=transfer_twiml(TRANSFER_NUMBER, twilio_number))
            except Exception as ee:
                log.error(f"Transfer fallback failed: {ee}")
        await cleanup(); return

    # ---------- State ----------
    response_active = True     # greeting in progress
    english_lock_turns = 2     # force EN for first two user turns
    tool_calls: dict[str, dict] = {}  # accumulate tool args across deltas

    # ---------- Pumps ----------
    async def twilio_to_openai():
        try:
            while True:
                msg = await ws.receive_text()
                data = json.loads(msg)
                ev = data.get("event")
                if ev == "media":
                    payload = (data.get("media", {}).get("payload") or "")
                    # Append caller audio; server VAD will commit
                    await oai.send_json({"type": "input_audio_buffer.append", "audio": payload})
                elif ev == "stop":
                    break
        except Exception as e:
            log.info(f"twilio_to_openai ended: {e}")

    async def openai_to_twilio():
        nonlocal response_active, english_lock_turns
        try:
            while True:
                msg = await oai.receive()
                if msg.type == aiohttp.WSMsgType.TEXT:
                    evt = json.loads(msg.data)
                    t = evt.get("type")

                    # Response lifecycle
                    if t == "response.created":
                        response_active = True
                    elif t in ("response.completed", "response.failed", "response.canceled"):
                        response_active = False

                    # Audio back to Twilio
                    elif t == "response.audio.delta" and stream_sid:
                        await ws.send_text(json.dumps({
                            "event": "media",
                            "streamSid": stream_sid,
                            "media": {"payload": evt["delta"]}
                        }))

                    # Barge-in
                    elif t == "input_audio_buffer.speech_started" and stream_sid:
                        await ws.send_text(json.dumps({"event": "clear", "streamSid": stream_sid}))

                    # Server VAD committed user turn → request reply
                    elif t == "input_audio_buffer.committed":
                        if not response_active:
                            response_active = True
                            if english_lock_turns > 0:
                                english_lock_turns -= 1
                                await oai.send_json({
                                    "type": "response.create",
                                    "response": {
                                        "modalities": ["audio", "text"],
                                        "instructions": "Respond in ENGLISH."
                                    }
                                })
                            else:
                                await oai.send_json({
                                    "type": "response.create",
                                    "response": {"modalities": ["audio", "text"]}
                                })

                    # Tool calling
                    elif t == "response.tool_call.created":
                        tc = evt.get("tool_call", {})
                        tool_calls[tc["id"]] = {"name": tc.get("name"), "args": ""}

                    elif t == "response.tool_call.delta":
                        tc = evt.get("tool_call", {})
                        tid = tc.get("id")
                        if tid in tool_calls:
                            tool_calls[tid]["args"] += tc.get("arguments", "")

                    elif t == "response.tool_call.completed":
                        tc = evt.get("tool_call", {})
                        tid = tc.get("id")
                        info = tool_calls.pop(tid, None)
                        if not info:
                            continue
                        name = info["name"]
                        # Merge args (final or accumulated)
                        try:
                            args = json.loads(tc.get("arguments") or info["args"] or "{}")
                        except Exception:
                            args = {}

                        if name == "transfer_call":
                            target = args.get("to") or TRANSFER_NUMBER
                            log.info(f"TOOL transfer_call -> {target}")
                            if tw_client and call_sid:
                                try:
                                    tw_client.calls(call_sid).update(
                                        twiml=transfer_twiml(target, twilio_number)
                                    )
                                except Exception as e:
                                    log.error(f"Live transfer failed (tool): {e}")

                        elif name == "end_call":
                            log.info("TOOL end_call")
                            if tw_client and call_sid:
                                try:
                                    tw_client.calls(call_sid).update(status="completed")
                                except Exception as e:
                                    log.error(f"End call failed (tool): {e}")

                    elif t == "error":
                        log.error(f"OpenAI error: {evt}")

                elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                    break
        except Exception as e:
            log.info(f"openai_to_twilio ended: {e}")

    await asyncio.gather(twilio_to_openai(), openai_to_twilio())
    await cleanup()
