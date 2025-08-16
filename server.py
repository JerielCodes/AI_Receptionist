# server.py
import os, re, json, asyncio, logging, aiohttp
from contextlib import suppress
from fastapi import FastAPI, Request, WebSocket
from fastapi.responses import PlainTextResponse
from twilio.twiml.voice_response import VoiceResponse, Dial
from twilio.rest import Client

# ---------- Logging ----------
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("elevara")
app = FastAPI()

# ---------- ENV ----------
OPENAI_API_KEY  = os.getenv("OPENAI_API_KEY", "")
TRANSFER_NUMBER = os.getenv("TRANSFER_NUMBER", "+1YOURCELLPHONE")   # E.164 like +1267...
TW_SID          = os.getenv("TWILIO_ACCOUNT_SID", "")
TW_TOKEN        = os.getenv("TWILIO_AUTH_TOKEN", "")
tw_client = Client(TW_SID, TW_TOKEN) if (TW_SID and TW_TOKEN) else None

E164 = re.compile(r"^\+[1-9]\d{7,14}$")

if not OPENAI_API_KEY:
    log.warning("OPENAI_API_KEY missing -> AI disabled.")
if not tw_client:
    log.warning("Twilio creds missing -> transfer/hangup disabled (tw_client=None).")
if "YOURCELLPHONE" in TRANSFER_NUMBER or not E164.match(TRANSFER_NUMBER):
    log.warning("TRANSFER_NUMBER invalid or placeholder; set E.164 like +1267...")

# ---------- Business Prompt ----------
def load_kb():
    try:
        with open("business.json", "r") as f:
            return json.load(f)
    except Exception as e:
        log.warning(f"business.json not loaded (defaults): {e}")
        return {}

KB    = load_kb()
brand = KB.get("brand", "Elevara")
loc   = KB.get("location", "Philadelphia, PA")
vals  = " • ".join(KB.get("value_props", [])) or "24/7 AI receptionist • instant answers • books & transfers calls"
trial = KB.get("trial_offer", "One-week free trial; install/uninstall is free.")

INSTRUCTIONS = (
    f"You are the {brand} AI receptionist for {brand}. Represent the product, not a person. "
    f"The company is based in {loc}. "
    "DEFAULT LANGUAGE: ENGLISH. Only switch to SPANISH if the caller clearly speaks Spanish or asks. "
    "Do NOT start in Spanish or any other language. "
    "Tone: warm, professional, friendly, emotion-aware. Keep replies to 1–2 concise sentences unless asked. "
    f"Value props: {vals}. Offer the trial when interest is shown: {trial}. "
    "Never invent business facts; if unsure, offer to connect the caller. "
    "GOALS: (1) Explain benefits & answer questions. (2) Offer to book a demo. (3) Transfer to a human on request. "
    "TOOLS (actually use them when appropriate):\n"
    " - transfer_call(to?, reason?) → Bridge to a human (omit 'to' to use the default).\n"
    " - end_call(reason?) → Politely end the call.\n"
    "ACTION RULES:\n"
    " - If the caller asks for a human in any way (connect me / speak to a person / manager / person in charge / owner / Jeriel / live agent), "
    "   say a brief confirmation line, then call transfer_call and STOP speaking.\n"
    " - If the caller indicates they are done (that's all / bye / end the call / not interested), say a brief closing line, then call end_call.\n"
    "SAFETY TOKENS (FALLBACK): When you perform a transfer include the TEXT token <<TRANSFER>>; for hangup include <<HANGUP>>. "
    "Never speak these tokens aloud; include them ONLY in the text channel."
)

# ---------- Helpers ----------
def transfer_twiml(to_number: str) -> str:
    vr = VoiceResponse()
    vr.say("Connecting you now.")
    d = Dial(answer_on_bridge=True)
    d.number(to_number)  # caller_id defaults to the Twilio DID
    vr.append(d)
    return str(vr)

# ---------- Health ----------
@app.get("/")
def health():
    return {
        "ok": bool(OPENAI_API_KEY),
        "twilio_ready": bool(tw_client),
        "transfer_ready": E164.match(TRANSFER_NUMBER) is not None,
        "transfer_number": TRANSFER_NUMBER,
    }

# ---------- Twilio webhook ----------
@app.post("/twilio/voice")
async def voice(request: Request):
    host = os.getenv("RENDER_EXTERNAL_HOSTNAME") or request.url.hostname
    vr = VoiceResponse()
    vr.say(f"Thanks for calling {brand}. One moment while I connect you.")
    vr.connect().stream(url=f"wss://{host}/media")
    log.info(f"Returning TwiML with stream URL: wss://{host}/media")
    return PlainTextResponse(str(vr), media_type="application/xml")

# ---------- Media stream ----------
@app.websocket("/media")
async def media(ws: WebSocket):
    if not OPENAI_API_KEY:
        await ws.close(); return
    await ws.accept()

    # Twilio START
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

    # OpenAI Realtime
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
        if tw_client and call_sid and E164.match(TRANSFER_NUMBER):
            with suppress(Exception):
                res = tw_client.calls(call_sid).update(twiml=transfer_twiml(TRANSFER_NUMBER))
                log.info(f"Fallback transfer issued. sid={res.sid} status={res.status}")
        await ws.close(); return

    async def cleanup():
        with suppress(Exception): await oai.close()
        with suppress(Exception): await session.close()
        with suppress(Exception): await ws.close()

    # Realtime session config
    tools = [
        {
            "type": "function",
            "name": "transfer_call",
            "description": "Bridge the caller to a human immediately. If 'to' is omitted, use the default business line.",
            "parameters": {
                "type": "object",
                "properties": {
                    "to": {"type": "string", "description": "E.164 number to transfer to (optional)"},
                    "reason": {"type": "string", "description": "Short reason (optional)"}
                }
            }
        },
        {
            "type": "function",
            "name": "end_call",
            "description": "Politely end the call.",
            "parameters": {
                "type": "object",
                "properties": {
                    "reason": {"type": "string", "description": "Optional closing reason"}
                }
            }
        }
    ]

    try:
        await oai.send_json({
            "type": "session.update",
            "session": {
                "turn_detection": {
                    "type": "server_vad",
                    "silence_duration_ms": 900,
                    "create_response": True,     # let OpenAI manage commits & replying
                    "interrupt_response": True
                },
                "input_audio_format": "g711_ulaw",
                "output_audio_format": "g711_ulaw",
                "voice": "alloy",
                "modalities": ["audio", "text"],
                "input_audio_transcription": {"model": "gpt-4o-mini-transcribe"},
                "instructions": INSTRUCTIONS,
                "tools": tools
            }
        })
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
        if tw_client and call_sid and E164.match(TRANSFER_NUMBER):
            with suppress(Exception):
                res = tw_client.calls(call_sid).update(twiml=transfer_twiml(TRANSFER_NUMBER))
                log.info(f"Fallback transfer issued. sid={res.sid} status={res.status}")
        await cleanup(); return

    # ---- State / helpers ----
    tool_parts: dict[str, dict] = {}
    action_done = False  # guard: transfer/hangup only once

    async def do_transfer(target: str | None, source: str):
        nonlocal action_done
        if action_done: 
            return
        action_done = True
        target = target or TRANSFER_NUMBER
        if not E164.match(target):
            log.error(f"[{source}] Transfer aborted: invalid target {target!r}")
            await cleanup(); return
        if not (tw_client and call_sid):
            log.error(f"[{source}] Transfer aborted: Twilio client/callSid not ready.")
            await cleanup(); return
        try:
            res = tw_client.calls(call_sid).update(twiml=transfer_twiml(target))
            log.info(f"[{source}] TRANSFER -> {target} (sid={res.sid}, status={res.status})")
        except Exception as e:
            log.exception(f"[{source}] Transfer failed: {e}")
        finally:
            await cleanup()

    async def do_hangup(source: str):
        nonlocal action_done
        if action_done:
            return
        action_done = True
        if not (tw_client and call_sid):
            log.error(f"[{source}] Hangup aborted: Twilio client/callSid not ready.")
            await cleanup(); return
        try:
            res = tw_client.calls(call_sid).update(status="completed")
            log.info(f"[{source}] HANGUP -> completed (sid={res.sid}, status={res.status})")
        except Exception as e:
            log.exception(f"[{source}] Hangup failed: {e}")
        finally:
            await cleanup()

    # Phrase lists (fallback if tools not used)
    TRANSFER_HINTS = [
        "connecting you", "let me connect", "i'll transfer you", "transfer you now",
        "connecting now", "putting you through", "patch you through"
    ]
    HANGUP_HINTS = [
        "ending the call", "i'll end the call", "goodbye now", "ending now"
    ]
    # Spanish fallbacks
    TRANSFER_HINTS_ES = [
        "conectarte ahora", "te transfiero", "transferirte", "pasarte con", "comunicarte con"
    ]
    HANGUP_HINTS_ES = [
        "terminando la llamada", "colgar ahora", "finalizar la llamada"
    ]

    # ---- Pumps ----
    async def twilio_to_openai():
        try:
            while True:
                msg = await ws.receive_text()
                data = json.loads(msg)
                ev = data.get("event")
                if ev == "media":
                    payload = (data.get("media", {}).get("payload") or "")
                    if payload:
                        await oai.send_json({"type": "input_audio_buffer.append", "audio": payload})
                elif ev == "stop":
                    log.info("Twilio sent stop.")
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
                    et = evt.get("type")

                    # Log key events for visibility
                    if et in ("response.created", "response.completed", "response.failed"):
                        log.info(f"OAI: {et}")

                    # Assistant audio back to Twilio
                    if et == "response.audio.delta" and stream_sid:
                        try:
                            await ws.send_text(json.dumps({
                                "event": "media",
                                "streamSid": stream_sid,
                                "media": {"payload": evt["delta"]}
                            }))
                        except Exception:
                            break

                    # Barge-in
                    elif et == "input_audio_buffer.speech_started" and stream_sid:
                        with suppress(Exception):
                            await ws.send_text(json.dumps({"event": "clear", "streamSid": stream_sid}))

                    # Tool calls (generic)
                    elif et == "response.output_item.added":
                        item = evt.get("item", {})
                        if item.get("type") in ("tool_call", "function_call"):
                            tool_parts[item["id"]] = {"name": item.get("name"), "args": ""}

                    elif et == "response.output_item.delta":
                        item = evt.get("item", {})
                        iid = item.get("id")
                        if iid in tool_parts and "arguments" in item:
                            tool_parts[iid]["args"] += item["arguments"]

                    elif et == "response.output_item.completed":
                        item = evt.get("item", {})
                        iid  = item.get("id")
                        info = tool_parts.pop(iid, None)
                        if info:
                            name = info.get("name")
                            try:
                                args = json.loads(item.get("arguments") or info.get("args") or "{}")
                            except Exception:
                                args = {}
                            log.info(f"OAI tool completed: {name} args={args}")

                            if name == "transfer_call":
                                await do_transfer(args.get("to"), "tool")
                                return
                            if name == "end_call":
                                await do_hangup("tool")
                                return

                    # Text output (fallback triggers + full visibility)
                    elif et in ("response.output_text.delta", "response.output_text.done"):
                        if et.endswith("delta"):
                            text = (evt.get("delta") or "")
                            log.info(f"AI says (delta): {text!r}")
                            low = text.lower()
                        else:
                            chunks = evt.get("output_text", []) or []
                            text = " ".join(chunks)
                            log.info(f"AI says (done): {text!r}")
                            low = text.lower()

                        if ("<<transfer>>" in low) or any(h in low for h in TRANSFER_HINTS + TRANSFER_HINTS_ES):
                            await do_transfer(None, "text-fallback")
                            return

                        if ("<<hangup>>" in low) or any(h in low for h in HANGUP_HINTS + HANGUP_HINTS_ES):
                            await do_hangup("text-fallback")
                            return

                    elif et == "error":
                        log.error(f"OAI error: {evt}")

                elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                    break
        except asyncio.CancelledError:
            pass
        except Exception as e:
            log.info(f"openai_to_twilio ended: {e}")

    async def twilio_heartbeat():
        # keep the media socket warm
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

    # Race tasks; cancel the rest when one ends
    t1 = asyncio.create_task(twilio_to_openai())
    t2 = asyncio.create_task(openai_to_twilio())
    t3 = asyncio.create_task(twilio_heartbeat())
    done, pending = await asyncio.wait({t1, t2, t3}, return_when=asyncio.FIRST_COMPLETED)
    for p in pending:
        p.cancel()
        with suppress(Exception, asyncio.CancelledError):
            await p
    await cleanup()
