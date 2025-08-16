import os, json, asyncio, logging
from fastapi import FastAPI, Request, WebSocket
from fastapi.responses import PlainTextResponse
from twilio.twiml.voice_response import VoiceResponse, Dial, Gather, Connect
from twilio.rest import Client
import aiohttp

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("elevara")
app = FastAPI()

# ----- Env -----
TRANSFER_NUMBER = os.getenv("TRANSFER_NUMBER", "+10000000000")
OPENAI_API_KEY  = os.getenv("OPENAI_API_KEY", "")
TW_SID          = os.getenv("TWILIO_ACCOUNT_SID", "")
TW_TOKEN        = os.getenv("TWILIO_AUTH_TOKEN", "")
tw_client = Client(TW_SID, TW_TOKEN) if (TW_SID and TW_TOKEN) else None

# ----- Load business knowledge (optional file) -----
def load_kb():
    try:
        with open("business.json", "r") as f:
            return json.load(f)
    except Exception:
        return {}
KB = load_kb()

def kb_to_instructions(kb: dict) -> str:
    brand = kb.get("brand", "Elevara")
    owner = kb.get("owner_name", "Jeriel")
    loc   = kb.get("location", "Philadelphia, PA")
    langs = ", ".join(kb.get("languages", ["English","Spanish"]))
    vals  = " • ".join(kb.get("value_props", [])) or "24/7 AI receptionist • instant answers • books & transfers calls"
    pricing_line = kb.get("pricing", {}).get("line", "")
    trial_offer  = kb.get("trial_offer", "We offer a one-week free trial; install/uninstall is free.")
    faqs = " ".join([f"Q: {x.get('q','')} A: {x.get('a','')}" for x in kb.get("faqs", [])])
    qqs  = " ".join([f"{i+1}. {q}" for i, q in enumerate(kb.get("qualifying_questions", ["What type of business?","Best call times?"]))])
    return (
        f"You are the {brand} AI receptionist for {owner} in {loc}. "
        f"Speak {langs}. Tone: warm, professional, friendly, emotion-aware. "
        "Keep replies to 1–2 sentences unless asked; mirror the caller’s language (EN/ES). "
        "Use light humor only if the caller jokes about AI.\n"
        f"VALUE: {vals}\n"
        f"PRICING/TRIAL: {pricing_line} | {trial_offer}\n"
        f"FAQs: {faqs}\n"
        f"QUALIFY (ask briefly if useful): {qqs}\n"
        "FACTS: Never invent details. If unsure, say so and offer to connect the caller.\n"
        "GOALS: (1) Explain benefits & answer questions. (2) Offer to book a demo. (3) Transfer to a human on request.\n"
        "TRANSFER PROTOCOL: If the caller clearly asks to speak to a human/the person in charge/Jeriel, "
        "say one natural line like “Absolutely—one moment while I connect you to the person in charge.” "
        "Then in the TEXT stream only, output exactly <<TRANSFER>> (no other text). Do NOT say <<TRANSFER>> aloud."
    )
INSTRUCTIONS = kb_to_instructions(KB)

def should_transfer(said: str, digits: str) -> bool:
    said = (said or "").lower()
    digits = (digits or "").strip()
    keys = [k.lower() for k in KB.get("transfer", {}).get("triggers_keywords", [])]
    keys += ["transfer","human","person in charge","manager","representative","operator","talk to jeriel","press 1"]
    return digits == "1" or any(k in said for k in keys)

def transfer_twiml(to_number: str, caller_id: str) -> str:
    vr = VoiceResponse()
    rec = KB.get("recording", {})
    if rec.get("enabled"):
        vr.say(rec.get("notice_en", "This call may be recorded to improve service quality."))
    vr.say("Connecting you now.")
    d = Dial(answer_on_bridge=True)
    if caller_id and caller_id.startswith("+"):
        d = Dial(answer_on_bridge=True, caller_id=caller_id)
    d.number(to_number); vr.append(d)
    return str(vr)

@app.get("/")
def health():
    return {"ok": True, "ai_enabled": bool(OPENAI_API_KEY)}

# ---------- Voice webhook ----------
@app.post("/twilio/voice")
async def voice(request: Request):
    form = await request.form()
    twilio_number = (form.get("To") or "").strip()
    said   = form.get("SpeechResult") or ""
    digits = form.get("Digits") or ""

    greet_en = KB.get("greeting", {}).get("en") or "Thanks for calling Elevara. I'm the AI receptionist."

    if OPENAI_API_KEY:
        host = request.url.hostname
        vr = VoiceResponse()
        rec = KB.get("recording", {})
        if rec.get("enabled"):
            vr.say(rec.get("notice_en", "This call may be recorded to improve service quality."))
        vr.say(greet_en)
        c = Connect(); c.stream(url=f"wss://{host}/media"); vr.append(c)
        # Fallback gather in case stream ends
        g = Gather(input="speech dtmf", num_digits=1, timeout=5, action="/twilio/voice", method="POST")
        g.say("If you need help, say 'transfer' or press 1.")
        vr.append(g)
        return PlainTextResponse(str(vr), media_type="application/xml")
    else:
        if should_transfer(said, digits):
            return PlainTextResponse(transfer_twiml(TRANSFER_NUMBER, twilio_number),
                                     media_type="application/xml")
        vr = VoiceResponse()
        g1 = Gather(input="speech dtmf", num_digits=1, timeout=4, action="/twilio/voice", method="POST")
        g1.say(greet_en + " Say 'transfer' or press 1 to talk to the person in charge.")
        vr.append(g1)
        vr.say("I didn't catch that.")
        g2 = Gather(input="speech dtmf", num_digits=1, timeout=4, action="/twilio/voice", method="POST")
        g2.say("Say 'transfer' or press 1 to be connected.")
        vr.append(g2); vr.say("Goodbye.")
        return PlainTextResponse(str(vr), media_type="application/xml")

# ---------- Media Streams (AI mode) ----------
@app.websocket("/media")
async def media(ws: WebSocket):
    if not OPENAI_API_KEY:
        await ws.close(); return
    await ws.accept()

    stream_sid, call_sid = None, None
    try:
        first = await asyncio.wait_for(ws.receive_text(), timeout=5)
        data0 = json.loads(first)
        while data0.get("event") != "start":
            data0 = json.loads(await ws.receive_text())
        stream_sid = data0["start"]["streamSid"]
        call_sid   = data0["start"].get("callSid")
        logging.info(f"Twilio stream started: streamSid={stream_sid}, callSid={call_sid}")
    except Exception as e:
        logging.error(f"Twilio start wait failed: {e}")
        await ws.close(); return

    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "OpenAI-Beta": "realtime=v1",
    }
    try:
        session = aiohttp.ClientSession()
        oai = await session.ws_connect(
            "wss://api.openai.com/v1/realtime?model=gpt-4o-realtime-preview",
            headers=headers,
            heartbeat=20,
            max_msg_size=2**23,
        )
        logging.info("Connected to OpenAI Realtime")
    except Exception as e:
        logging.error(f"OpenAI connect failed: {e}")
        if tw_client and call_sid:
            try: tw_client.calls(call_sid).update(twiml=transfer_twiml(TRANSFER_NUMBER, ""))
            except Exception as ee: logging.error(f"Transfer fallback failed: {ee}")
        await ws.close(); return

    async def cleanup():
        try: await oai.close()
        except Exception: pass
        try: await session.close()
        except Exception: pass
        try: await ws.close()
        except Exception: pass

    # Configure session and send a short greeting
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
        logging.error(f"OpenAI session.setup failed: {e}")
        if tw_client and call_sid:
            try: tw_client.calls(call_sid).update(twiml=transfer_twiml(TRANSFER_NUMBER, ""))
            except Exception as ee: logging.error(f"Transfer fallback failed: {ee}")
        await cleanup(); return

    async def twilio_to_openai():
        try:
            while True:
                msg = await ws.receive_text()
                data = json.loads(msg)
                ev = data.get("event")
                if ev == "media":
                    await oai.send_json({"type": "input_audio_buffer.append", "audio": data["media"]["payload"]})
                elif ev == "stop":
                    break
        except Exception as e:
            logging.info(f"twilio_to_openai ended: {e}")

    async def openai_to_twilio():
        try:
            while True:
                msg = await oai.receive()
                if msg.type == aiohttp.WSMsgType.TEXT:
                    evt = json.loads(msg.data)
                    t = evt.get("type")

                    if t == "response.audio.delta" and stream_sid:
                        await ws.send_text(json.dumps({
                            "event": "media",
                            "streamSid": stream_sid,
                            "media": {"payload": evt["delta"]}
                        }))

                    elif t == "input_audio_buffer.speech_started" and stream_sid:
                        # barge-in: stop current TTS
                        await ws.send_text(json.dumps({"event": "clear", "streamSid": stream_sid}))

                    elif t == "input_audio_buffer.speech_stopped":
                        # close the chunk and ask the model to respond
                        await oai.send_json({"type": "input_audio_buffer.commit"})
                        await oai.send_json({"type": "response.create",
                                             "response": {"modalities": ["audio","text"]}})

                    elif t == "input_audio_buffer.committed":
                        # (sometimes VAD will emit committed; be robust)
                        await oai.send_json({"type": "response.create",
                                             "response": {"modalities": ["audio","text"]}})

                    # ---- Transfer detection ----
                    elif t == "response.output_text.delta":
                        if "<<TRANSFER>>" in evt.get("delta","") and call_sid and tw_client:
                            try: tw_client.calls(call_sid).update(twiml=transfer_twiml(TRANSFER_NUMBER, ""))
                            except Exception as e: logging.error(f"Live transfer failed (delta): {e}")

                    elif t == "response.completed":
                        out = evt.get("response", {}).get("output_text", [])
                        joined = " ".join(x or "" for x in out)
                        if "<<TRANSFER>>" in joined and call_sid and tw_client:
                            try: tw_client.calls(call_sid).update(twiml=transfer_twiml(TRANSFER_NUMBER, ""))
                            except Exception as e: logging.error(f"Live transfer failed (completed): {e}")

                    elif t == "error":
                        logging.error(f"OpenAI error: {evt}")

                elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                    break
        except Exception as e:
            logging.info(f"openai_to_twilio ended: {e}")

    await asyncio.gather(twilio_to_openai(), openai_to_twilio())
    await cleanup()
