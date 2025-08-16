# server.py
import os, json, asyncio, logging
from fastapi import FastAPI, Request, WebSocket
from fastapi.responses import PlainTextResponse
from twilio.twiml.voice_response import VoiceResponse, Dial
from twilio.rest import Client
import aiohttp

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("elevara")
app = FastAPI()

# ========= ENV =========
OPENAI_API_KEY  = os.getenv("OPENAI_API_KEY", "")
TRANSFER_NUMBER = os.getenv("TRANSFER_NUMBER", "+10000000000")
TW_SID          = os.getenv("TWILIO_ACCOUNT_SID", "")
TW_TOKEN        = os.getenv("TWILIO_AUTH_TOKEN", "")
tw_client = Client(TW_SID, TW_TOKEN) if (TW_SID and TW_TOKEN) else None

# ========= BUSINESS PROMPT (customize via business.json if present) =========
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
    d = Dial(answer_on_bridge=True)
    if caller_id and caller_id.startswith("+"):
        d = Dial(answer_on_bridge=True, caller_id=caller_id)
    d.number(to_number)
    vr.append(d)
    return str(vr)

# ========= HEALTH =========
@app.get("/")
def health():
    return {"ok": True, "ai_enabled": bool(OPENAI_API_KEY)}

# ========= TWILIO VOICE WEBHOOK (minimal TwiML to force Media Stream) =========
@app.post("/twilio/voice")
async def voice(request: Request):
    host = os.getenv("RENDER_EXTERNAL_HOSTNAME") or request.url.hostname
    vr = VoiceResponse()
    vr.say(f"Thanks for calling {brand}. One moment while I connect you.")
    # absolutely minimal: SAY + CONNECT.STREAM
    vr.connect().stream(url=f"wss://{host}/media")
    log.info(f"Returning TwiML with stream URL: wss://{host}/media")
    return PlainTextResponse(str(vr), media_type="application/xml")

# ========= MEDIA STREAMS (OpenAI Realtime) =========
@app.websocket("/media")
async def media(ws: WebSocket):
    if not OPENAI_API_KEY:
        await ws.close(); return
    await ws.accept()

    # Get start event (streamSid/callSid)
    stream_sid, call_sid, twilio_number = None, None, ""
    try:
        while True:
            first = await asyncio.wait_for(ws.receive_text(), timeout=10)
            data0 = json.loads(first)
            if data0.get("event") == "start":
                stream_sid = data0["start"]["streamSid"]
                call_sid   = data0["start"].get("callSid", "")
                twilio_number = data0["start"].get("to", "")
                log.info(f'Twilio stream started: streamSid={stream_sid}, callSid={call_sid}')
                break
    except Exception as e:
        log.error(f"Twilio start wait failed: {e}")
        await ws.close(); return

    # Connect to OpenAI Realtime via aiohttp
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
        # graceful fallback: live transfer
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

    # Configure session + quick hello
    try:
        await oai.send_json({
            "type": "session.update",
            "session": {
                "turn_detection": {"type": "server_vad", "silence_duration_ms": 700},
                "input_audio_format": "g711_ulaw",
                "output_audio_format": "g711_ulaw",
                "voice": "alloy",
                "modalities": ["audio","text"],
                "instructions": INSTRUCTIONS
            }
        })
        await oai.send_json({
            "type": "response.create",
            "response": {"modalities": ["audio","text"], "instructions": "Greet briefly and ask how you can help."}
        })
    except Exception as e:
        log.error(f"OpenAI session.setup failed: {e}")
        if tw_client and call_sid:
            try: tw_client.calls(call_sid).update(twiml=transfer_twiml(TRANSFER_NUMBER, twilio_number))
            except Exception as ee: log.error(f"Transfer fallback failed: {ee}")
        await cleanup(); return

    # ---- Pumps ----
    async def twilio_to_openai():
        try:
            while True:
                msg = await ws.receive_text()
                data = json.loads(msg)
                ev = data.get("event")
                if ev == "media":
                    await oai.send_json({"type": "input_audio_buffer.append",
                                         "audio": data["media"]["payload"]})
                elif ev == "stop":
                    break
        except Exception as e:
            log.info(f"twilio_to_openai ended: {e}")

    async def openai_to_twilio():
        try:
            while True:
                msg = await oai.receive()
                if msg.type == aiohttp.WSMsgType.TEXT:
                    evt = json.loads(msg.data)
                    t = evt.get("type")

                    # audio back to caller
                    if t == "response.audio.delta" and stream_sid:
                        await ws.send_text(json.dumps({
                            "event": "media",
                            "streamSid": stream_sid,
                            "media": {"payload": evt["delta"]}
                        }))

                    # barge-in
                    elif t == "input_audio_buffer.speech_started" and stream_sid:
                        await ws.send_text(json.dumps({"event": "clear", "streamSid": stream_sid}))

                    # commit on stop and request a reply
                    elif t == "input_audio_buffer.speech_stopped":
                        await oai.send_json({"type": "input_audio_buffer.commit"})
                        await oai.send_json({"type": "response.create",
                                             "response": {"modalities": ["audio","text"]}})

                    # sometimes server sends committed event; be robust
                    elif t == "input_audio_buffer.committed":
                        await oai.send_json({"type": "response.create",
                                             "response": {"modalities": ["audio","text"]}})

                    # Live transfer detection
                    elif t == "response.output_text.delta":
                        if "<<TRANSFER>>" in (evt.get("delta") or "") and call_sid and tw_client:
                            try: tw_client.calls(call_sid).update(twiml=transfer_twiml(TRANSFER_NUMBER, twilio_number))
                            except Exception as e: log.error(f"Live transfer failed (delta): {e}")

                    elif t == "response.completed":
                        out = evt.get("response", {}).get("output_text", [])
                        joined = " ".join(x or "" for x in out)
                        if "<<TRANSFER>>" in joined and call_sid and tw_client:
                            try: tw_client.calls(call_sid).update(twiml=transfer_twiml(TRANSFER_NUMBER, twilio_number))
                            except Exception as e: log.error(f"Live transfer failed (completed): {e}")

                    elif t == "error":
                        log.error(f"OpenAI error: {evt}")

                elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                    break
        except Exception as e:
            log.info(f"openai_to_twilio ended: {e}")

    await asyncio.gather(twilio_to_openai(), openai_to_twilio())
    await cleanup()
