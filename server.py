from fastapi import FastAPI, Request, WebSocket
from fastapi.responses import PlainTextResponse, HTMLResponse
from twilio.twiml.voice_response import VoiceResponse, Dial, Gather, Connect
from twilio.rest import Client
import os, json, asyncio, websockets

app = FastAPI()

# --- Env ---
TRANSFER_NUMBER = os.getenv("TRANSFER_NUMBER", "+10000000000")   # e.g., +12672134362
OPENAI_API_KEY  = os.getenv("OPENAI_API_KEY", "")                 # optional; enables AI mode
TW_SID          = os.getenv("TWILIO_ACCOUNT_SID", "")             # optional; needed for AI-triggered transfer
TW_TOKEN        = os.getenv("TWILIO_AUTH_TOKEN", "")
tw_client = Client(TW_SID, TW_TOKEN) if (TW_SID and TW_TOKEN) else None

# --- Helpers ---
def should_transfer(said: str, digits: str) -> bool:
    said = (said or "").lower()
    digits = (digits or "").strip()
    keywords = ("transfer","human","agent","person","representative","talk to","connect me","operator")
    return digits == "1" or any(k in said for k in keywords)

def transfer_twiml(to_number: str, caller_id: str) -> str:
    vr = VoiceResponse()
    vr.say("Connecting you now.")
    # Use your Twilio number as callerId (works on trial)
    kwargs = {"answer_on_bridge": True}
    if caller_id and caller_id.startswith("+"):
        kwargs["caller_id"] = caller_id
    d = Dial(**kwargs)
    d.number(to_number)
    vr.append(d)
    return str(vr)

@app.get("/")
def health():
    return {"ok": True, "ai_enabled": bool(OPENAI_API_KEY)}

# --- ENTRYPOINT (works in both modes) ---
@app.post("/twilio/voice")
async def voice(request: Request):
    form = await request.form()
    twilio_number = (form.get("To") or "").strip()     # safe callerId
    said   = form.get("SpeechResult") or ""
    digits = form.get("Digits") or ""

    # If AI key present -> AI mode (Media Streams). Otherwise -> classic gather/transfer.
    if OPENAI_API_KEY:
        host = request.url.hostname
        vr = VoiceResponse()
        vr.say("Thanks for calling Jeriel. I'm his AI receptionist. Start talking any time.")
        c = Connect()
        c.stream(url=f"wss://{host}/media")
        vr.append(c)
        return HTMLResponse(str(vr), media_type="application/xml")
    else:
        if should_transfer(said, digits):
            return PlainTextResponse(transfer_twiml(TRANSFER_NUMBER, twilio_number),
                                     media_type="application/xml")
        # Prompt + gather again
        g1 = Gather(input="speech dtmf", num_digits=1, timeout=4, action="/twilio/voice", method="POST")
        g1.say("Thanks for calling Jeriel. Say 'transfer' or press 1 to talk to him.")
        vr = VoiceResponse()
        vr.append(g1)
        vr.say("I didn't catch that.")
        g2 = Gather(input="speech dtmf", num_digits=1, timeout=4, action="/twilio/voice", method="POST")
        g2.say("Say 'transfer' or press 1 to be connected.")
        vr.append(g2)
        vr.say("Goodbye.")
        return PlainTextResponse(str(vr), media_type="application/xml")

# --- MEDIA STREAMS (AI mode only) ---
@app.websocket("/media")
async def media(ws: WebSocket):
    # If AI not configured, just close the stream (defensive)
    if not OPENAI_API_KEY:
        await ws.close()
        return

    await ws.accept()
    stream_sid, call_sid = None, None

    # Connect to OpenAI Realtime
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "OpenAI-Beta": "realtime=v1"}
    async with websockets.connect(
        "wss://api.openai.com/v1/realtime?model=gpt-4o-realtime-preview",
        extra_headers=headers, ping_interval=20, ping_timeout=20, max_size=2**23
    ) as oai:
        # Configure session once
        await oai.send(json.dumps({
            "type": "session.update",
            "session": {
                "turn_detection": {"type": "server_vad"},
                "input_audio_format": "g711_ulaw",
                "output_audio_format": "g711_ulaw",
                "voice": "alloy",
                "modalities": ["audio","text"],
                "instructions": (
                    "You are Jeriel's AI receptionist. Be warm, concise, bilingual (EN/ES). "
                    "Explain the service and answer questions. "
                    "If the caller clearly asks to talk to Jeriel or a live person, "
                    "output ONLY the token <<TRANSFER>> in text."
                ),
            }
        }))

        async def twilio_to_oai():
            nonlocal stream_sid, call_sid
            while True:
                try:
                    msg = await ws.receive_text()
                    data = json.loads(msg)
                    ev = data.get("event")
                    if ev == "start":
                        stream_sid = data["start"]["streamSid"]
                        call_sid   = data["start"].get("callSid")
                    elif ev == "media":
                        await oai.send(json.dumps({
                            "type": "input_audio_buffer.append",
                            "audio": data["media"]["payload"]
                        }))
                    elif ev == "stop":
                        break
                except Exception:
                    break

        async def oai_to_twilio():
            # We’ll use your Twilio number as callerId on transfer (trial-safe)
            safe_caller_id = ""
            try:
                # Try to grab it from the initial webhook cached in memory? Not available here.
                # Fallback: Twilio will keep the existing callerId if not given; but on trial,
                # we want a Twilio number. If REST update below runs, Twilio will accept the XML
                # with no callerId and default to your number that received the call.
                pass
            except Exception:
                pass

            try:
                async for raw in oai:
                    evt = json.loads(raw)
                    t = evt.get("type")

                    # Send AI audio back to the caller
                    if t == "response.audio.delta" and stream_sid:
                        await ws.send_text(json.dumps({
                            "event": "media",
                            "streamSid": stream_sid,
                            "media": {"payload": evt["delta"]}
                        }))

                    # Watch for transfer cue in text and redirect the live call
                    elif t == "response.output_text.delta":
                        if "<<TRANSFER>>" in evt.get("delta", "") and call_sid and tw_client:
                            try:
                                twiml = transfer_twiml(TRANSFER_NUMBER, caller_id="")  # Twilio defaults to your number
                                tw_client.calls(call_sid).update(twiml=twiml)
                            except Exception as e:
                                print("Transfer failed:", e)
            except Exception:
                pass

        await asyncio.gather(twilio_to_oai(), oai_to_twilio())

    await ws.close()
