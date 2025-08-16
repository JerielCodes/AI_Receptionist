from fastapi import FastAPI, Request, WebSocket
from fastapi.responses import PlainTextResponse
from twilio.twiml.voice_response import VoiceResponse, Dial, Gather, Connect
from twilio.rest import Client
import os, json, asyncio

app = FastAPI()

# --- Env ---
TRANSFER_NUMBER = os.getenv("TRANSFER_NUMBER", "+10000000000")   # set in Render, e.g. +12672134362
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

    if OPENAI_API_KEY:
        host = request.url.hostname
        vr = VoiceResponse()
        rec = KB.get("recording", {})
        if rec.get("enabled"):
            vr.say(rec.get("notice_en"))
        vr.say(greet_en)
        # Start media stream
        c = Connect(); c.stream(url=f"wss://{host}/media"); vr.append(c)
        # Fallback: if stream ends unexpectedly, continue with a minimal gather so the call doesn't just hang up
        g = Gather(input="speech dtmf", num_digits=1, timeout=4, action="/twilio/voice", method="POST")
        g.say("If you need help, say 'transfer' or press 1.")
        vr.append(g)
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

    # 1) FIRST, wait for Twilio 'start' so we have callSid for failover
    try:
        first = await asyncio.wait_for(ws.receive_text(), timeout=5)
        data0 = json.loads(first)
        if data0.get("event") == "start":
            stream_sid = data0["start"]["streamSid"]
            call_sid   = data0["start"].get("callSid")
        else:
            # keep reading until we see 'start'
            while data0.get("event") != "start":
                msg = await ws.receive_text()
                data0 = json.loads(msg)
            stream_sid = data0["start"]["streamSid"]
            call_sid   = data0["start"].get("callSid")
    except Exception:
        await ws.close()
        return

    # lazy import so transfer-only mode doesn't require the package at startup
    try:
        import websockets as ws_client
    except ModuleNotFoundError:
        # Fallback: if we can't do AI, transfer to human if possible
        if tw_client and call_sid:
            try:
                tw_client.calls(call_sid).update(
                    twiml=transfer_twiml(TRANSFER_NUMBER, caller_id="")
                )
            except Exception as e:
                print("Transfer fallback failed (no websockets):", e)
        await ws.close()
        return

    # Prepare OpenAI headers as list of tuples (works across versions)
    oai_headers = [
        ("Authorization", f"Bearer {OPENAI_API_KEY}"),
        ("OpenAI-Beta", "realtime=v1"),
    ]

    # 2) Connect to OpenAI Realtime. If it fails, redirect the live call to you.
    try:
        oai = await ws_client.connect(
            "wss://api.openai.com/v1/realtime?model=gpt-4o-realtime-preview",
            extra_headers=oai_headers,
            ping_interval=20,
            ping_timeout=20,
            max_size=2**23,
        )
    except Exception as e:
        print("OpenAI connect failed:", e)
        if tw_client and call_sid:
            try:
                tw_client.calls(call_sid).update(
                    twiml=transfer_twiml(TRANSFER_NUMBER, caller_id="")
                )
            except Exception as ee:
                print("Transfer fallback failed:", ee)
        await ws.close()
        return

    async def cleanup():
        try:
            await oai.close()
        except Exception:
            pass
        try:
            await ws.close()
        except Exception:
            pass

    # 3) Configure Realtime session
    try:
        await oai.send(json.dumps({
            "type": "session.update",
            "session": {
                "turn_detection": {"type": "server_vad"},
                "input_audio_format": "g711_ulaw",
                "output_audio_format": "g711_ulaw",
                "voice": "alloy",
                "modalities": ["audio", "text"],
                "instructions": INSTRUCTIONS
            }
        }))
    except Exception as e:
        print("OpenAI session.update failed:", e)
        if tw_client and call_sid:
            try:
                tw_client.calls(call_sid).update(
                    twiml=transfer_twiml(TRANSFER_NUMBER, caller_id="")
                )
            except Exception as ee:
                print("Transfer fallback failed:", ee)
        await cleanup()
        return

    # 4) Bridge audio both ways
    async def twilio_to_openai():
        try:
            while True:
                msg = await ws.receive_text()
                data = json.loads(msg)
                ev = data.get("event")
                if ev == "media":
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
            while True:
                raw = await oai.recv()
                evt = json.loads(raw)
                t = evt.get("type")

                if t == "response.audio.delta" and stream_sid:
                    await ws.send_text(json.dumps({
                        "event": "media",
                        "streamSid": stream_sid,
                        "media": {"payload": evt["delta"]}
                    }))

                elif t == "response.output_text.delta":
                    if "<<TRANSFER>>" in evt.get("delta", "") and call_sid and tw_client:
                        try:
                            tw_client.calls(call_sid).update(
                                twiml=transfer_twiml(TRANSFER_NUMBER, caller_id="")
                            )
                        except Exception as e:
                            print("Live transfer failed:", e)
        except Exception:
            pass

    await asyncio.gather(twilio_to_openai(), openai_to_twilio())
    await cleanup()
