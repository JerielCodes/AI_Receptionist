from fastapi import FastAPI, Request, WebSocket
from fastapi.responses import PlainTextResponse, HTMLResponse
from twilio.twiml.voice_response import VoiceResponse, Connect
import os, json, asyncio, websockets

app = FastAPI()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")  # set in Render
TRANSFER_NUMBER = os.getenv("TRANSFER_NUMBER", "+1XXXXXXXXXX")

@app.get("/")
def health():
    return {"ok": True}

@app.post("/twilio/voice")
async def voice(request: Request):
    # Greet, then open a Media Stream to our /media endpoint
    host = request.url.hostname  # your Render host
    vr = VoiceResponse()
    vr.say("Thanks for calling Jeriel. I'm his AI receptionist. Start talking any time.")
    c = Connect()
    c.stream(url=f"wss://{host}/media")
    vr.append(c)
    return HTMLResponse(str(vr), media_type="application/xml")

@app.websocket("/media")
async def media(ws: WebSocket):
    await ws.accept()
    stream_sid = None

    # Connect to OpenAI Realtime (bidirectional audio)
    oai_headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "OpenAI-Beta": "realtime=v1",
    }
    async with websockets.connect(
        "wss://api.openai.com/v1/realtime?model=gpt-4o-realtime-preview",
        extra_headers=oai_headers,
        ping_interval=20,
        ping_timeout=20,
        max_size=2**23,  # ~8MB
    ) as oai:

        # Configure session for Twilio audio pass-through (μ-law 8k) + VAD
        await oai.send(json.dumps({
            "type": "session.update",
            "session": {
                "turn_detection": {"type": "server_vad"},
                "input_audio_format": "g711_ulaw",
                "output_audio_format": "g711_ulaw",
                "voice": "alloy",
                "instructions": (
                    "You are Jeriel's AI receptionist. Be warm, concise, bilingual "
                    "(English/Spanish). Explain how the service helps businesses and "
                    "offer to book a demo or answer questions. Keep responses short."
                ),
                "modalities": ["audio", "text"],
            }
        }))

        # Relay Twilio->OpenAI and OpenAI->Twilio concurrently
        async def twilio_to_openai():
            nonlocal stream_sid
            try:
                while True:
                    msg = await ws.receive_text()
                    data = json.loads(msg)
                    ev = data.get("event")
                    if ev == "start":
                        stream_sid = data["start"]["streamSid"]
                    elif ev == "media":
                        # base64 g711 μ-law 8k from Twilio → straight to OpenAI buffer
                        await oai.send(json.dumps({
                            "type": "input_audio_buffer.append",
                            "audio": data["media"]["payload"]
                        }))
                    elif ev == "stop":
                        break
            except Exception:
                pass

        async def openai_to_twilio():
            try:
                async for raw in oai:
                    evt = json.loads(raw)
                    t = evt.get("type")
                    # Send AI audio back to caller
                    if t == "response.audio.delta" and stream_sid:
                        await ws.send_text(json.dumps({
                            "event": "media",
                            "streamSid": stream_sid,
                            "media": {"payload": evt["delta"]}
                        }))
                    # If user barges in, clear Twilio buffer for snappy interrupt
                    elif t == "input_audio_buffer.speech_started" and stream_sid:
                        await ws.send_text(json.dumps({"event": "clear", "streamSid": stream_sid}))
            except Exception:
                pass

        await asyncio.gather(twilio_to_openai(), openai_to_twilio())

    await ws.close()
