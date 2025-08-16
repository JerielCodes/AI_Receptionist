from fastapi import FastAPI, Request, WebSocket
from fastapi.responses import PlainTextResponse
from twilio.twiml.voice_response import VoiceResponse, Dial, Gather, Connect
from twilio.rest import Client
import os, json, asyncio

app = FastAPI()

# --- Env ---
TRANSFER_NUMBER = os.getenv("TRANSFER_NUMBER", "+10000000000")   # e.g. +12672134362 (set in Render)
OPENAI_API_KEY  = os.getenv("OPENAI_API_KEY", "")                 # add to enable AI mode
TW_SID          = os.getenv("TWILIO_ACCOUNT_SID", "")
TW_TOKEN        = os.getenv("TWILIO_AUTH_TOKEN", "")
tw_client = Client(TW_SID, TW_TOKEN) if (TW_SID and TW_TOKEN) else None

# --- Load business knowledge ---
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
    langs = ", ".join(kb.get("languages", ["English"]))
    vals  = " • ".join(kb.get("value_props", []))
    pricing_line = kb.get("pricing", {}).get("line", "")
    trial_offer  = kb.get("trial_offer", "")
    faqs = " ".join([f"Q: {x.get('q','')} A: {x.get('a','')}" for x in kb.get("faqs", [])])
    qqs  = " ".join([f"{i+1}. {q}" for i, q in enumerate(kb.get("qualifying_questions", []))])
    human_alias = " or ".join(kb.get("transfer", {}).get("phrase_for_human", ["the person in charge"]))

    return (
        f"You are the {brand} AI receptionist for {owner} in {loc}. "
        f"Speak {langs}. Tone: warm, professional, friendly; emotion-aware. "
        f"Use light humor only if the caller jokes about AI, then pivot back.\n"
        "STYLE: Sound like a real receptionist (not scripted). Keep replies to 1–2 sentences unless asked. "
        "Vary phrasing naturally; mirror the caller’s energy and language (EN/ES).\n"
        f"VALUE: {vals}\n"
        f"PRICING/TRIAL: {pricing_line} | {trial_offer}\n"
        f"FAQs: {faqs}\n"
        f"QUALIFY (ask 1–2 only if relevant): {qqs}\n"
        "FACTS: Never invent details (prices, capabilities, timelines, policies). "
        "If unsure or not in your knowledge, say you’re not certain and offer to connect the caller.\n"
        "GOALS: (1) Explain benefits briefly and answer questions. "
        "(2) Offer to book an installation/demo. "
        "(3) Transfer to a human on request.\n"
        "TRANSFER: If the caller clearly asks to speak to a human / the person in charge / Jeriel:\n"
        "- AUDIO to caller: say one natural line like “Absolutely—one moment while I connect you to the person in charge.”\n"
        "- TEXT stream only: output ONLY the token <<TRANSFER>> (no other text). Do NOT say <<TRANSFER>> aloud.\n"
    )

INSTRUCTIONS = kb_to_instructions(KB)

# --- Helpers ---
def should_transfer(said: str, digits: str) -> bool:
    said = (said or "").lower()
    digits = (digits or "").strip()
    keys = KB.get("transfer", {}).get("triggers_keywords", [])
    keys = [k.lower() for k in keys] + ["representative", "operator"]
    return digits == "1" or any(k in said for k in keys)

def transfer_twiml(to_number: str, caller_id: str) -> str:
    vr = VoiceResponse()
    # Optional recording notice from KB
    rec = KB.get("recording", {})
    if rec.get("enabled"):
        vr.say(rec.get("notice_en", "This call may be recorded to improve service quality."))
    vr.say("Connecting you now.")
    kwargs = {"answer_on_bridge": True}
    if caller_id and caller_id.startswith("+"):
        kwargs["caller_id"] = caller_id  # use your Twilio DID (trial-safe)
    d = Dial(**kwargs); d.number(to_number); vr.append(d)
    return str(vr)

@app.get("/")
def health():
    return {"ok": True, "ai_enabled": bool(OPENAI_API_KEY)}

# --- Entry: voice webhook (works in both modes) ---
@app.post("/twilio/voice")
async def voice(request: Request):
    form = await request.form()
    twilio_number = (form.get("To") or "").strip()
    said   = form.get("SpeechResult") or ""
    digits = form.get("Digits") or ""

    greet_en = KB.get("greeting", {}).get("en") or "Thanks for calling Elevara. I'm the AI receptionist."
    # greet_es exists in KB but we let the AI handle bilingual replies after first line

    if OPENAI_API_KEY:
        host = request.url.hostname
        vr = VoiceResponse()
        rec = KB.get("recording", {})
        if rec.get("enabled"):
            vr.say(rec.get("notice_en"))
        vr.say(greet_en)  # short line; AI will take over conversationally
        c = Connect(); c.stream(url=f"wss://{host}/media"); vr.append(c)
        return PlainTextResponse(str(vr), media_type="application/xml")
    else:
        if should_transfer(said, digits):
            return PlainTextResponse(
                transfer_twiml(TRANSFER_NUMBER, twilio_number),
                media_type="application/xml"
            )
        vr = VoiceResponse()
        rec = KB.get("recording", {})
        if rec.get("enabled"):
            vr.say(rec.get("notice_en"))
        g1 = Gather(input="speech dtmf", num_digits=1, timeout=4, action="/twilio/voice", method="POST")
        g1.say(greet_en + " Say 'transfer' or press 1 to talk to the person in charge.")
        vr.append(g1)
        vr.say("I didn't catch that.")
        g2 = Gather(input="speech dtmf", num_digits=1, timeout=4, action="/twilio/voice", method="POST")
        g2.say("Say 'transfer' or press 1 to be connected.")
        vr.append(g2); vr.say("Goodbye.")
        return PlainTextResponse(str(vr), media_type="application/xml")

# --- Media Streams (AI mode) ---
@app.websocket("/media")
async def media(ws: WebSocket):
    if not OPENAI_API_KEY:
        await ws.close(); return
    await ws.accept()
    stream_sid, call_sid = None, None

    # lazy import so transfer-only mode doesn't require the package at startup
    import websockets

    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "OpenAI-Beta": "realtime=v1"}
    async with websockets.connect(
        "wss://api.openai.com/v1/realtime?model=gpt-4o-realtime-preview",
        extra_headers=headers, ping_interval=20, ping_timeout=20, max_size=2**23
    ) as oai:
        # Configure session with your instructions
        await oai.send(json.dumps({
            "type": "session.update",
            "session": {
                "turn_detection": {"type": "server_vad"},
                "input_audio_format": "g711_ulaw",
                "output_audio_format": "g711_ulaw",
                "voice": "alloy",
                "modalities": ["audio","text"],
                "instructions": INSTRUCTIONS
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
            while True:
                try:
                    raw = await oai.recv()
                    evt = json.loads(raw); t = evt.get("type")

                    # Stream AI audio back to caller (bidirectional stream)
                    if t == "response.audio.delta" and stream_sid:
                        await ws.send_text(json.dumps({
                            "event": "media",
                            "streamSid": stream_sid,
                            "media": {"payload": evt["delta"]}
                        }))

                    # AI requests transfer via text token
                    elif t == "response.output_text.delta":
                        if "<<TRANSFER>>" in evt.get("delta","") and call_sid and tw_client:
                            try:
                                tw_client.calls(call_sid).update(
                                    twiml=transfer_twiml(TRANSFER_NUMBER, caller_id="")
                                )
                            except Exception as e:
                                print("Transfer failed:", e)
                except Exception:
                    break

        await asyncio.gather(twilio_to_oai(), oai_to_twilio())
    await ws.close()
