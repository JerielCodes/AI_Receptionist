from fastapi import FastAPI, Request, WebSocket
from fastapi.responses import PlainTextResponse
from twilio.twiml.voice_response import VoiceResponse, Connect
import os, json, base64

app = FastAPI()

# Set this in Render → Environment: TRANSFER_NUMBER = +1XXXXXXXXXX
TRANSFER_NUMBER = os.getenv("TRANSFER_NUMBER", "+1XXXXXXXXXX")

@app.get("/")
def health():
    return {"ok": True}

@app.post("/twilio/voice")
async def voice(request: Request):
    # Twilio posts form data here
    form = await request.form()
    said = (form.get("SpeechResult") or "").lower()

    vr = VoiceResponse()

    # If caller asked to talk to you, transfer
    if any(k in said for k in ["transfer", "human", "agent", "person"]):
        vr.say("Connecting you now.")
        vr.dial(TRANSFER_NUMBER)
        return PlainTextResponse(str(vr), media_type="application/xml")

    # Greeting + open media stream (AI hook comes next)
    vr.say("Thanks for calling Jeriel. I'm his AI receptionist. Say 'transfer' to talk to him.")
    c = Connect()
    c.stream(url="wss://ai-receptionist-4bzj.onrender.com/media")  # your Render host
    vr.append(c)

    # Simple follow-up to catch 'transfer' on next turn
    with vr.gather(input="speech", action="/twilio/voice", method="POST", timeout=3):
        vr.say("How can I help?")
    return PlainTextResponse(str(vr), media_type="application/xml")

@app.websocket("/media")
async def media(ws: WebSocket):
    # Twilio Media Streams websocket (audio comes in here)
    await ws.accept()
    try:
        while True:
            msg = await ws.receive_text()
            data = json.loads(msg)
            if data.get("event") == "media":
                # base64-encoded PCM audio (mono 8k) — hook to OpenAI Realtime later
                _ = base64.b64decode(data["media"]["payload"])
            elif data.get("event") == "stop":
                break
    finally:
        await ws.close()
