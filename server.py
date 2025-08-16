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
TRANSFER_NUMBER = os.getenv("TRANSFER_NUMBER", "+10000000000")
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
langs = ", ".join(KB.get("languages", ["English","Spanish"]))
vals  = " • ".join(KB.get("value_props", [])) or "24/7 AI receptionist • instant answers • books & transfers calls"
trial = KB.get("trial_offer", "One-week free trial; install/uninstall is free.")

INSTRUCTIONS = (
    f"You are the {brand} AI receptionist for {owner} in {loc}. "
    f"Speak {langs}. Tone: warm, professional, friendly, emotion-aware. "
    "Keep replies to 1–2 short sentences unless asked; mirror caller language (EN/ES). "
    "Use light humor only if the caller jokes about AI. "
    f"Value props: {vals}. Offer a brief demo explanation; then ask how you can help. "
    f"Offer the trial when interest is shown: {trial}. "
    "NEVER invent business facts. If unsure, say so and offer to connect the caller.\n"
    "GOALS: (1) Explain benefits & answer questions. (2) Offer to book a demo. "
    "(3) Transfer to a human on request.\n"
    "TRANSFER PROTOCOL: If caller clearly asks to speak to a person/the person in charge/Jeriel, "
    "first say one line like “Absolutely—one moment while I connect you to the person in charge.” "
    "Then in the TEXT stream only output exactly <<TRANSFER>> (no other text). Do NOT say <<TRANSFER>> aloud."
)

def transfer_twiml(to_number: str, caller_id: str) -> str:
    vr = VoiceResponse()
    vr.say("Connecting you now.")
    dial_kwargs = {"answer_on_bridge": True}
    if caller_id and caller_id.startswith("+"):
        dial_kwargs["caller_id"] = caller_id
    d = Dial(**dial_kwargs)
    d.number(to_number)
    vr.append(d)
    return str(vr)

# ---------- HEALTH ----------
@app.get("/")
def health():
    return {"ok": True, "ai_enabled": bool(OPENAI_API_KEY)}

# ---------- TWILIO VOICE WEBHOOK (force media stream) ----------
@app.post("/twilio/voice")
async def voice(request: Request):
    host = os.getenv("RENDER_EXTERNAL_HOSTNAME") or request.url.hostname
    vr = VoiceResponse()
    vr.say(f"Thanks for calling {brand}. One moment while I connect you.")
    vr.connect().stream(url=f"wss://{host}/media")
    log.info(f"Returning TwiML with stream URL: wss://{host}/media")
    return PlainTextResponse(str(vr), media_type="application/xml")

# ---------- MEDIA STREAM (OpenAI Realtime) ----------
@app.websocket("/media")
async def media(ws: WebSocket):
    if not OPENAI_API_KEY:
        await ws.close(); return
    await ws.accept()

    # Wait for Twilio start event
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

    # Connect to OpenAI Realtime
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
            try: tw_client.calls(call_sid).update(twiml=transfer_twiml(TRANSFER_NUMBER, twilio_number))
            except Exception as ee: log.error(f"Transfer fallback failed: {ee}")
        await ws.close(); return

    async def cleanup():
        try: await oai.close()
        except Exception: pass
        try: await session.close()
        except Exception: pass
        try: await ws.close()
        except Exception: pass

    # OpenAI session config + greeting
    try:
        await oai.send_json({
            "type": "session.update",
            "session": {
                "turn_detection": {"type": "server_vad", "silence_duration_ms": 700},
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
                "instructions": "Greet briefly and ask how you can help."
            }
        })
    except Exception as e:
        log.error(f"OpenAI session.setup failed: {e}")
        if tw_client and call_sid:
            try: tw_client.calls(call_sid).update(twiml=transfer_twiml(TRANSFER_NUMBER, twilio_number))
            except Exception as ee: log.error(f"Transfer fallback failed: {ee}")
        await cleanup(); return

    # ---- State flags to prevent your errors ----
    buffer_has_audio = False     # becomes True once we receive Twilio media
    response_active  = True      # we just created the greeting response

    # ---- Pumps ----
    async def twilio_to_openai():
        nonlocal buffer_has_audio
        try:
            while True:
                msg = await ws.receive_text()
                data = json.loads(msg)
                ev = data.get("event")
                if ev == "media":
                    buffer_has_audio = True
                    await oai.send_json({
                        "type": "input_audio_buffer.append",
                        "audio": data["media"]["payload"]  # base64 μ-law 8k
                    })
                elif ev == "stop":
                    break
        except Exception as e:
            log.info(f"twilio_to_openai ended: {e}")

    async def openai_to_twilio():
        nonlocal response_active, buffer_has_audio
        try:
            while True:
                msg = await oai.receive()
                if msg.type == aiohttp.WSMsgType.TEXT:
                    evt = json.loads(msg.data)
                    t = evt.get("type")

                    # Track response lifecycle
                    if t == "response.created":
                        response_active = True

                    elif t in ("response.completed", "response.failed", "response.canceled"):
                        response_active = False
                        # check transfer intent on completed
                        if t == "response.completed":
                            out = evt.get("response", {}).get("output_text", [])
                            joined = " ".join(x or "" for x in out)
                            if "<<TRANSFER>>" in joined and call_sid and tw_client:
                                try:
                                    tw_client.calls(call_sid).update(
                                        twiml=transfer_twiml(TRANSFER_NUMBER, twilio_number)
                                    )
                                except Exception as e:
                                    log.error(f"Live transfer failed (completed): {e}")

                    # Audio back to Twilio
                    elif t == "response.audio.delta" and stream_sid:
                        await ws.send_text(json.dumps({
                            "event": "media",
                            "streamSid": stream_sid,
                            "media": {"payload": evt["delta"]}
                        }))

                    # Barge-in (clear Twilio buffer)
                    elif t == "input_audio_buffer.speech_started" and stream_sid:
                        await ws.send_text(json.dumps({"event": "clear", "streamSid": stream_sid}))

                    # When VAD thinks the caller stopped, commit only if we *actually* had audio
                    elif t == "input_audio_buffer.speech_stopped":
                        if buffer_has_audio:
                            await oai.send_json({"type": "input_audio_buffer.commit"})
                            buffer_has_audio = False

                    # After commit, request a reply ONLY if one isn't already active
                    elif t == "input_audio_buffer.committed":
                        if not response_active:
                            response_active = True
                            await oai.send_json({
                                "type": "response.create",
                                "response": {"modalities": ["audio", "text"]}
                            })

                    # Mid-stream transfer trigger (text delta)
                    elif t == "response.output_text.delta":
                        if "<<TRANSFER>>" in (evt.get("delta") or "") and call_sid and tw_client:
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
