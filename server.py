# server.py
import os, json, asyncio, logging, aiohttp, re
from contextlib import suppress
from fastapi import FastAPI, Request, WebSocket
from fastapi.responses import PlainTextResponse
from twilio.twiml.voice_response import VoiceResponse, Dial
from twilio.rest import Client

# ---------- LOGGING ----------
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("elevara")

app = FastAPI()

# ---------- ENV ----------
OPENAI_API_KEY  = os.getenv("OPENAI_API_KEY", "")
TRANSFER_NUMBER = os.getenv("TRANSFER_NUMBER", "")
TW_SID          = os.getenv("TWILIO_ACCOUNT_SID", "")
TW_TOKEN        = os.getenv("TWILIO_AUTH_TOKEN", "")
tw_client = Client(TW_SID, TW_TOKEN) if (TW_SID and TW_TOKEN) else None

def valid_e164(n: str | None) -> bool:
    return bool(n and re.fullmatch(r"\+\d{7,15}", n))

# ---------- BUSINESS PROMPT ----------
def load_kb():
    try:
        with open("business.json", "r") as f:
            return json.load(f)
    except Exception:
        return {}

KB    = load_kb()
brand = KB.get("brand", "Elevara")
loc   = KB.get("location", "Philadelphia, PA")
vals  = " • ".join(KB.get("value_props", [])) or "24/7 AI receptionist • instant answers • books & transfers calls"
trial = KB.get("trial_offer", "One-week free trial; install/uninstall is free.")

INSTRUCTIONS = (
    f"You are the {brand} AI receptionist. You represent the product {brand} (based in {loc}). "
    "Default to ENGLISH; switch to SPANISH only if the caller clearly speaks Spanish or asks. "
    "Tone: warm, professional, friendly, emotion-aware. Keep replies to 1–2 concise sentences unless asked. "
    f"Value props: {vals}. Offer the trial when interest is shown: {trial}. "
    "Never invent business facts; if unsure, say so and offer to connect the caller. "
    "GOALS: (1) Explain benefits & answer questions. (2) Offer to book a demo. (3) Transfer to a human on request. "
    "TOOLS:\n"
    " - transfer_call(to?, reason?) → Bridge to a human now (omit 'to' to use the default).\n"
    " - end_call(reason?) → Politely end the call.\n"
    "IMPORTANT: Actually CALL tools; don’t merely mention them. After calling a tool, stop speaking.\n"
    "TRANSFER INTENT examples: connect me, human, person, agent, manager, person in charge, owner, Jeriel, live agent."
)

def transfer_twiml(to_number: str) -> str:
    """Build TwiML to bridge the caller to a human."""
    vr = VoiceResponse()
    vr.say("Connecting you now.")
    d = Dial(answer_on_bridge=True)
    d.number(to_number)
    vr.append(d)
    return str(vr)

# ---------- HEALTH ----------
@app.get("/")
def health():
    return {
        "ok": True,
        "ai_enabled": bool(OPENAI_API_KEY),
        "twilio_ready": bool(tw_client),
        "transfer_ready": valid_e164(TRANSFER_NUMBER),
        "transfer_number": TRANSFER_NUMBER if valid_e164(TRANSFER_NUMBER) else None,
    }

# ---------- TWILIO VOICE WEBHOOK ----------
@app.post("/twilio/voice")
async def voice(request: Request):
    host = os.getenv("RENDER_EXTERNAL_HOSTNAME") or request.url.hostname
    vr = VoiceResponse()
    # Small prompt so Twilio hears audio immediately and keeps the stream alive
    vr.say(f"Thanks for calling {brand}. One moment while I connect you.")
    vr.connect().stream(url=f"wss://{host}/media")
    log.info(f"Returning TwiML with stream URL: wss://{host}/media")
    return PlainTextResponse(str(vr), media_type="application/xml")

# ---------- MEDIA STREAM ----------
@app.websocket("/media")
async def media(ws: WebSocket):
    if not OPENAI_API_KEY:
        log.error("No OPENAI_API_KEY; closing media socket.")
        await ws.close(); return
    await ws.accept()

    # Wait for Twilio 'start'
    stream_sid = call_sid = twilio_number = None
    try:
        while True:
            first = await asyncio.wait_for(ws.receive_text(), timeout=10)
            data0 = json.loads(first)
            if data0.get("event") == "start":
                stream_sid = data0["start"]["streamSid"]
                call_sid   = data0["start"].get("callSid", "")
                twilio_number = data0["start"].get("to")
                log.info(f"Twilio stream started: streamSid={stream_sid}, callSid={call_sid}")
                break
    except Exception as e:
        log.error(f"Twilio start wait failed: {e}")
        await ws.close(); return

    # OpenAI Realtime WS
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "OpenAI-Beta": "realtime=v1"}
    try:
        session = aiohttp.ClientSession()
        oai = await session.ws_connect(
            "wss://api.openai.com/v1/realtime?model=gpt-4o-realtime-preview",
            headers=headers,
            heartbeat=20,
            max_msg_size=2**23,
        )
        log.info("Connected to OpenAI Realtime")
    except Exception as e:
        log.error(f"OpenAI connect failed: {e}")
        if tw_client and call_sid and valid_e164(TRANSFER_NUMBER):
            with suppress(Exception):
                log.info("AI failed; falling back to immediate transfer.")
                tw_client.calls(call_sid).update(twiml=transfer_twiml(TRANSFER_NUMBER))
        await ws.close(); return

    async def cleanup():
        with suppress(Exception): await oai.close()
        with suppress(Exception): await session.close()
        with suppress(Exception): await ws.close()

    # ---------- Tools ----------
    tools = [
        {
            "type": "function",
            "name": "transfer_call",
            "description": "Bridge the caller to a human immediately. If 'to' is omitted, use the default business line.",
            "parameters": {
                "type": "object",
                "properties": {
                    "to": {"type": "string", "description": "E.164 number to transfer to (optional)"},
                    "reason": {"type": "string", "description": "Short reason (optional)"},
                },
            },
        },
        {
            "type": "function",
            "name": "end_call",
            "description": "Politely end the call.",
            "parameters": {
                "type": "object",
                "properties": {"reason": {"type": "string", "description": "Optional closing reason"}},
            },
        },
    ]

    # ---------- Session setup ----------
    try:
        await oai.send_json({
            "type": "session.update",
            "session": {
                "turn_detection": {
                    "type": "server_vad",
                    "silence_duration_ms": 900,
                    "create_response": True,     # let OpenAI create responses after each user pause
                    "interrupt_response": True,
                },
                "input_audio_format": "g711_ulaw",
                "output_audio_format": "g711_ulaw",
                "voice": "alloy",
                "modalities": ["audio", "text"],
                "input_audio_transcription": {"model": "gpt-4o-mini-transcribe"},
                "instructions": INSTRUCTIONS,
                "tools": tools,
            }
        })
        # Initial greeting (tools allowed from the first turn)
        await oai.send_json({
            "type": "response.create",
            "response": {
                "modalities": ["audio", "text"],
                "tool_choice": "auto",
                "instructions": "Start in ENGLISH. Greet briefly and ask how you can help."
            }
        })
    except Exception as e:
        log.error(f"OpenAI session.setup failed: {e}")
        if tw_client and call_sid and valid_e164(TRANSFER_NUMBER):
            with suppress(Exception):
                tw_client.calls(call_sid).update(twiml=transfer_twiml(TRANSFER_NUMBER))
        await cleanup(); return

    # ---------- State ----------
    tool_items: dict[str, dict] = {}

    # ---------- Helpers ----------
    def do_transfer(reason: str | None = None, to: str | None = None):
        target = to or TRANSFER_NUMBER
        if not (tw_client and call_sid and valid_e164(target)):
            log.error(f"TRANSFER blocked (client? {bool(tw_client)} call? {bool(call_sid)} target_valid? {valid_e164(target)})")
            return
        log.info(f"TRANSFERRING via Twilio REST -> {target} reason={reason!r}")
        with suppress(Exception):
            tw_client.calls(call_sid).update(twiml=transfer_twiml(target))

    def do_hangup(reason: str | None = None):
        if not (tw_client and call_sid):
            log.error("HANGUP blocked (missing Twilio client or call_sid)")
            return
        log.info(f"HANGUP via Twilio REST. reason={reason!r}")
        with suppress(Exception):
            tw_client.calls(call_sid).update(status="completed")

    # Conservative, generic fallback intent detector (assistant text)
    TRANSFER_HINTS = [
        "connecting you", "let me connect", "i'll transfer you", "transfer you now",
        "connect you now", "patch you through", "transfer now"
    ]
    HANGUP_HINTS = ["ending the call", "goodbye", "bye for now"]

    # ---------- Pumps ----------
    async def twilio_to_openai():
        try:
            while True:
                msg = await ws.receive_text()
                data = json.loads(msg)
                ev = data.get("event")
                if ev == "media":
                    payload = (data.get("media", {}).get("payload") or "")
                    # forward audio to OpenAI
                    await oai.send_json({"type": "input_audio_buffer.append", "audio": payload})
                elif ev == "mark":
                    pass
                elif ev == "stop":
                    reason = (data.get("stop") or {}).get("reason")
                    log.info(f"Twilio sent stop. reason={reason!r}")
                    break
        except asyncio.CancelledError:
            pass
        except Exception as e:
            log.info(f"twilio_to_openai ended: {e}")

    async def openai_to_twilio():
        try:
            while True:
                msg = await oai.receive()
                if msg.type == aiohttp.WSMsgType.TEXT:
                    evt = json.loads(msg.data)
                    t = evt.get("type")
                    log.info(f"OAI EVT: {t}")

                    # assistant audio -> Twilio
                    if t == "response.audio.delta" and stream_sid:
                        try:
                            await ws.send_text(json.dumps({
                                "event": "media",
                                "streamSid": stream_sid,
                                "media": {"payload": evt["delta"]}
                            }))
                        except Exception:
                            break  # Twilio socket closed

                    # barge-in: clear any queued TTS
                    elif t == "input_audio_buffer.speech_started" and stream_sid:
                        with suppress(Exception):
                            await ws.send_text(json.dumps({"event": "clear", "streamSid": stream_sid}))

                    # --------- TOOL CALLS (handle all current shapes) ----------
                    elif t == "response.output_item.added":
                        item = evt.get("item", {})
                        if item.get("type") in ("tool_call", "function_call"):
                            tool_items[item["id"]] = {"name": item.get("name"), "args": ""}

                    elif t == "response.output_item.delta":
                        item = evt.get("item", {})
                        iid = item.get("id")
                        if iid in tool_items and "arguments" in item:
                            tool_items[iid]["args"] += item["arguments"]

                    elif t == "response.output_item.completed":
                        item = evt.get("item", {})
                        iid = item.get("id")
                        info = tool_items.pop(iid, None)
                        if info:
                            name = info.get("name")
                            try:
                                args = json.loads(item.get("arguments") or info.get("args") or "{}")
                            except Exception:
                                args = {}
                            log.info(f"TOOL COMPLETE: {name} args={args}")

                            if name == "transfer_call":
                                do_transfer(reason=args.get("reason"), to=(args.get("to") or None))

                            elif name == "end_call":
                                do_hangup(reason=args.get("reason"))

                    # --------- TEXT FALLBACKS ----------
                    elif t == "response.output_text.delta":
                        txt = (evt.get("delta") or "").lower()
                        if any(k in txt for k in TRANSFER_HINTS):
                            log.info("Text fallback: transfer hint detected in delta.")
                            do_transfer()
                        elif any(k in txt for k in HANGUP_HINTS):
                            log.info("Text fallback: hangup hint detected in delta.")
                            do_hangup()

                    elif t == "error":
                        log.error(f"OpenAI error: {evt}")

                elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                    break
        except asyncio.CancelledError:
            pass
        except Exception as e:
            log.info(f"openai_to_twilio ended: {e}")

    async def twilio_heartbeat():
        # keep the media socket warm (avoid idle/proxy drops)
        try:
            while True:
                await asyncio.sleep(15)
                if stream_sid:
                    with suppress(Exception):
                        await ws.send_text(json.dumps({
                            "event": "mark",
                            "streamSid": stream_sid,
                            "mark": {"name": "hb"}
                        }))
        except asyncio.CancelledError:
            pass
        except Exception:
            pass

    # Race tasks; cancel others when one ends
    t1 = asyncio.create_task(twilio_to_openai())
    t2 = asyncio.create_task(openai_to_twilio())
    t3 = asyncio.create_task(twilio_heartbeat())
    done, pending = await asyncio.wait({t1, t2, t3}, return_when=asyncio.FIRST_COMPLETED)
    for p in pending:
        p.cancel()
        with suppress(Exception, asyncio.CancelledError):
            await p
    await cleanup()
