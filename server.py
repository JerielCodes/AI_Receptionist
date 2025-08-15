from fastapi import FastAPI, Request
from fastapi.responses import PlainTextResponse
from twilio.twiml.voice_response import VoiceResponse, Dial
import os

app = FastAPI()
TRANSFER_NUMBER = os.getenv("TRANSFER_NUMBER", "+1XXXXXXXXXX")  # set in Render

@app.get("/")
def health():
    return {"ok": True}

@app.post("/twilio/voice")
async def voice(request: Request):
    form = await request.form()
    said = (form.get("SpeechResult") or "").lower()
    digits = (form.get("Digits") or "").strip()
    caller = form.get("From")

    vr = VoiceResponse()

    # If user asks for human OR presses 1 → transfer
    if digits == "1" or any(k in said for k in ["transfer","human","agent","person","representative","talk to"]):
        vr.say("Connecting you now.")
        d = Dial(caller_id=caller, answer_on_bridge=True)
        d.number(TRANSFER_NUMBER)
        vr.append(d)
        return PlainTextResponse(str(vr), media_type="application/xml")

    # Prompt + gather again (speech or DTMF)
    with vr.gather(input="speech dtmf", num_digits=1, timeout=4, action="/twilio/voice", method="POST"):
        vr.say("Thanks for calling Jeriel. Say 'transfer' or press 1 to talk to him.")
    return PlainTextResponse(str(vr), media_type="application/xml")
