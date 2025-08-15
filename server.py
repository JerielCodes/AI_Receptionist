from fastapi import FastAPI, Request, WebSocket
from fastapi.responses import PlainTextResponse
from twilio.twiml.voice_response import VoiceResponse, Connect
import os, json, base64

app = FastAPI()
TRANSFER_NUMBER = os.getenv("TRANSFER_NUMBER", "+1XXXXXXXXXX")  # set in Render

@app.get("/")
def health():
    return {"ok": True}

@app.post("/twilio/voice")
async def voice(request: Request):
    form = await request.form()
    said = (form.get("SpeechResult") or "").lower()

    vr = VoiceResponse()
    if any(k in said for k in ["transfer","human","agent","person"]):
        vr.say("Connecting you now.")
        vr.dial(TRANSFER_NUMBER)
        return PlainTextResponse(str(vr), media_type="application/xml")

    vr.say("Thanks for calling Jeriel. I'm his AI receptionist. Say 'transfer' to talk to him.")
    c = Connect()
    c.stream(url="wss://YOUR-RENDER-HOST/media")  # replace after deploy
    vr.append(c)
    with vr.gather(input="speech", action="/twilio/voice", method="POST", timeout=3):
        vr.say("How can I help?")
    return PlainTextResponse(str(vr), media_type="application/xml")

@app.websocket("/media")
async def media(ws: WebSocket):
    await ws.accept()
    try:
        while True:
            data = json.loads(await ws.receive_text())
            if data.get("event") == "media":
                _ = base64.b64decode(data["media"]["payload"])
            elif data.get("event") == "stop":
                break
    finally:
        await ws.close()
