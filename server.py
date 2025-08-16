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
TRANSFER_NUMBER = os.getenv("TRANSFER_NUMBER", "+10000000000")  # your cell
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
owner = KB.get("owner_name", "Jeriel")
loc   = KB.get("location", "Philadelphia, PA")
vals  = " • ".join(KB.get("value_props", [])) or "24/7 AI receptionist • instant answers • books & transfers calls"
trial = KB.get("trial_offer", "One-week free trial; install/uninstall is free.")

INSTRUCTIONS = (
    f"You are the {brand} AI receptionist for {owner} in {loc}. "
    "Start in ENGLISH; switch to SPANISH only if the caller clearly speaks Spanish or asks. "
    "Tone: warm, professional, friendly, emotion-aware. Keep replies to 1–2 concise sentences unless asked. "
    f"Value props: {vals}. Offer the trial when interest is shown: {trial}. "
    "Never invent business facts; if unsure, say so and offer to connect the caller.\n"
    "GOALS: (1) Explain benefits & answer questions. (2) Offer to book a demo. (3) Transfer to a human on request.\n"
    "TRANSFER PROTOCOL: If the caller clearly asks to speak to a person/the person in charge/Jeriel, "
    "say one line like “Absolutely—one moment while I connect you to the person in charge.” "
    "Then in the TEXT channel output exactly <<TRANSFER>> (no other text). Do NOT say that token aloud. "
    "After you output <<TRANSFER>>, stop speaking."
)

def transfer_twiml(to_number: str, caller_id: str) -> str:
    vr = VoiceResponse()
    vr.say("Connecting you now.")
    kwargs = {"answer_on_bridge": True}
    if caller_id and caller_id.startswith("+"):
        kwargs["caller_id"] = caller_id
    d = Dial(**kwargs); d.number(to_number); vr.append(d)
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
    # Tiny TTS so Twilio hears immediate audio and keeps the stream happy
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
                twilio_number = data0["start"].get("to", "")
                log.info(f"Twilio stream started: streamSid={stream_sid}, callSid={call_sid}")
                break
    except Exception as e:
        log.error(f"Twilio start wait failed: {e}")
        await ws.close(); return

    # OpenAI Realtime
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

    # Configure session + greet (let server VAD do all commits)
    try:
        await oai.send_json({
            "type": "session.update",
            "session": {
                "turn_detection": {"type": "server_vad", "silence_duration_ms": 1000},
                "input_audio_format": "g711_ulaw",
                "output_audio_format": "g711_ulaw",
                "voice": "alloy",
                "modalities": ["audio", "text"],
                "instructions": INSTRUCTIONS
            }
        })
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
    response_active = True            # greeting in progress
    english_lock_turns = 2            # force English for first 2 user turns
    pending_transfer = False

    # ---------- Pumps ----------
    async def twilio_to_openai():
        try:
            while True:
                msg = await ws.receive_text()
                data = json.loads(msg)
                ev = data.get("event")
                if ev == "media":
                    payload = (data.get("media", {}).get("payload") or "")
                    # Only append; NO manual commit when using server_vad
                    await oai.send_json({"type": "input_audio_buffer.append", "audio": payload})
                elif ev == "stop":
                    break
        except Exception as e:
            log.info(f"twilio_to_openai ended: {e}")

    async def openai_to_twilio():
        nonlocal response_active, english_lock_turns, pending_transfer
        try:
            while True:
                msg = await oai.receive()
                if msg.type == aiohttp.WSMsgType.TEXT:
                    evt = json.loads(msg.data)
                    t = evt.get("type")

                    if t == "response.created":
                        response_active = True

                    elif t in ("response.completed", "response.failed", "response.canceled"):
                        response_active = False
                        if t == "response.completed":
                            out = evt.get("response", {}).get("output_text", [])
                            joined = " ".join(x or "" for x in out)
                            if "<<TRANSFER>>" in joined:
                                pending_transfer = True
                                if call_sid and tw_client:
                                    try:
                                        tw_client.calls(call_sid).update(
                                            twiml=transfer_twiml(TRANSFER_NUMBER, twilio_number)
                                        )
                                    except Exception as e:
                                        log.error(f"Live transfer failed (completed): {e}")

                    elif t == "response.audio.delta" and stream_sid:
                        await ws.send_text(json.dumps({
                            "event": "media",
                            "streamSid": stream_sid,
                            "media": {"payload": evt["delta"]}
                        }))

                    elif t == "input_audio_buffer.speech_started" and stream_sid:
                        # Barge-in: clear any queued TTS
                        await ws.send_text(json.dumps({"event": "clear", "streamSid": stream_sid}))

                    elif t == "input_audio_buffer.committed":
                        # OpenAI's server VAD decided the caller finished speaking.
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

                    elif t == "response.output_text.delta":
                        # Mid-stream transfer catch (token appears during generation)
                        if "<<TRANSFER>>" in (evt.get("delta") or ""):
                            pending_transfer = True
                            if call_sid and tw_client:
                                try:
                                    tw_client.calls(call_sid).update(
                                        twiml=transfer_twiml(TRANSFER_NUMBER, twilio_number)
                                    )
                                except Exception as e:
                                    log.error(f"Live transfer failed (delta): {e}")

                    elif t == "error":
                        log.error(f"OpenAI error: {evt}")

                elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                    break
        except Exception as e:
            log.info(f"openai_to_twilio ended: {e}")

    await asyncio.gather(twilio_to_openai(), openai_to_twilio())
    await cleanup()
