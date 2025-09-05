# ======== PATCH START ========
# Place these near the other imports
import asyncio, uuid, hashlib

# ---------------- Response Gate (single-flight queue + cancel) ----------------
class ResponseGate:
    """
    Ensures only ONE response is active at a time.
    - Queue new responses while one is active.
    - On barge-in, send response.cancel, then drain queue.
    - Tracks response ids for logging/telemetry.
    """
    def __init__(self, oai_ws, log):
        self.oai = oai_ws
        self.log = log
        self.busy = False
        self.active_id = None
        self.queue = asyncio.Queue()
        self.closed = False

    async def create(self, instructions: str | None = None, tool_choice: str = "auto",
                     modalities: list[str] = ["audio", "text"]):
        if self.closed:
            return
        await self.queue.put({
            "modalities": modalities,
            "instructions": instructions or "",
            "tool_choice": tool_choice
        })
        await self._drain()

    async def _drain(self):
        if self.closed or self.busy:
            return
        if self.queue.empty():
            return
        payload = await self.queue.get()
        self.busy = True
        self.active_id = None
        self.log.info(f"RGATE: response.create queued -> sending")
        await self.oai.send_json({"type": "response.create", "response": payload})

    async def on_created(self, resp_id: str | None):
        self.active_id = resp_id
        self.busy = True
        self.log.info(f"RGATE: response.created id={resp_id}")

    async def on_done(self, terminal: str = "done"):
        self.log.info(f"RGATE: response.{terminal} id={self.active_id}")
        self.busy = False
        self.active_id = None
        await self._drain()

    async def cancel_active(self):
        if self.closed:
            return
        if self.busy:
            self.log.info(f"RGATE: response.cancel sent (id={self.active_id})")
            await self.oai.send_json({"type": "response.cancel"})

    def close(self):
        self.closed = True
        self.log.info("RGATE: closed")

# ---------------- Helpers for idempotency & time presentation ----------------
def deterministic_booking_key(call_sid: str, start_iso: str, email: str) -> str:
    base = f"{(call_sid or '').strip()}|{(start_iso or '').strip()}|{(email or '').lower().strip()}"
    return "ai-" + hashlib.sha1(base.encode("utf-8")).hexdigest()[:24]

def human_et_from_iso(iso_str: str) -> str:
    # Re-declare locally if not already in your file
    from datetime import datetime
    from zoneinfo import ZoneInfo
    dt = datetime.fromisoformat(iso_str)
    dt = dt.astimezone(ZoneInfo(DEFAULT_TZ))
    today = datetime.now(ZoneInfo(DEFAULT_TZ)).date()
    prefix = "Today " if dt.date() == today else dt.strftime("%a ")
    return prefix + dt.strftime("%-I:%M %p")

# ================== In your media() right after oai connection ==================
    # Add session state flags
    greet_sent = False
    session_ending = False   # set True after "bye" so we stop creating more turns
    booked_once = set()      # cache of idempotency keys we've sent to n8n

    # Instantiate the response gate
    rgate = ResponseGate(oai, log)

# ================== Replace your session.update block with: ==================
    try:
        now_et = datetime.now(ZoneInfo(DEFAULT_TZ))
        now_line = now_et.strftime("%A, %B %d, %Y, %-I:%M %p %Z")
        await oai.send_json({
            "type": "session.update",
            "session": {
                "turn_detection": {
                    "type": "server_vad",
                    "silence_duration_ms": 1100,
                    "create_response": False,      # <<< WE own response.create calls
                    "interrupt_response": True
                },
                "input_audio_format": "g711_ulaw",
                "output_audio_format": "g711_ulaw",
                "voice": "alloy",
                "modalities": ["audio","text"],
                "input_audio_transcription": {"model": "gpt-4o-mini-transcribe", "language": "en"},
                "instructions": (
                    INSTRUCTIONS +
                    f"\nCURRENT_TIME_ET: {now_line}\nTIMEZONE: {DEFAULT_TZ}\nCALLER_LAST4: {caller_last4 or 'unknown'}\n"
                    "IMPORTANT:\n"
                    "- Do not produce more than one response per user turn.\n"
                    "- When you check availability, SAY 'One moment while I check availability…' then call a tool in the same turn.\n"
                ),
                "tools": tools
            }
        })

        # Send greeting ONCE through the gate
        greet_line = ("Looks like no one is available right now. I can book you for later today or tomorrow—what time works? (Eastern Time)"
                      if rejoin_reason == "transfer_fail"
                      else ('Say EXACTLY one of: '
                            '"Hey, thanks for calling ELvara. How can I help you today?" '
                            'OR "Hi there, this is ELvara. What can I do for you?" '
                            'OR "Thanks for calling ELvara. What brings you here today?"'))
        await rgate.create(greet_line, tool_choice="auto")
        greet_sent = True
    except Exception as e:
        # ... keep your existing fallback/transfer code here ...
        await cleanup(); return

# ================== In openai_to_twilio(), UPDATE event handling: ==================
    async def openai_to_twilio():
        nonlocal session_ending, greet_sent
        call_map: Dict[str, Dict[str,str]] = {}
        user_buf: List[str] = []

        try:
            while True:
                msg = await oai.receive()
                if msg.type != aiohttp.WSMsgType.TEXT:
                    if msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        break
                    continue

                evt = json.loads(msg.data)
                t = evt.get("type")

                # ----- Response lifecycle telemetry -----
                if t == "response.created":
                    await rgate.on_created(evt.get("response", {}).get("id"))
                elif t in ("response.done", "response.completed"):
                    await rgate.on_done("done")
                    if session_ending:
                        # after closing line spoken, end sockets
                        rgate.close()
                        await cleanup()
                        return
                elif t == "error":
                    log.error(f"OAI error event: {evt}")
                    await rgate.on_done("error")
                elif t == "response.canceled":
                    await rgate.on_done("canceled")

                # ----- Stream audio to Twilio -----
                if t == "response.audio.delta" and stream_sid:
                    try:
                        await ws.send_text(json.dumps({
                            "event": "media","streamSid": stream_sid,
                            "media": {"payload": evt["delta"]}
                        }))
                    except Exception:
                        break

                # ----- Barge-in: clear Twilio + cancel active response -----
                elif t == "input_audio_buffer.speech_started" and stream_sid:
                    with suppress(Exception):
                        await ws.send_text(json.dumps({"event":"clear","streamSid": stream_sid}))
                    await rgate.cancel_active()

                # ----- Transcription accumulation -----
                elif t == "conversation.item.input_audio_transcription.delta":
                    user_buf.append(evt.get("delta") or "")

                elif t == "conversation.item.input_audio_transcription.completed":
                    text = "".join(user_buf).strip()
                    user_buf.clear()
                    if session_ending:
                        continue

                    if not text or len(text.split()) <= 2:
                        await rgate.create(
                            "Sorry—I didn’t catch that, could you repeat? If it’s still hard to hear, calls are clearest off speaker or headphones.",
                            tool_choice="auto"
                        )
                        continue

                    low = text.lower()

                    # ----- End intent -----
                    if re.search(r"\b(hang\s*up|end\s+the\s+call|that'?s\s+all|we'?re\s+done|goodbye|bye)\b", low):
                        session_ending = True
                        await rgate.cancel_active()
                        await rgate.create("Thanks for calling—have a great day!", tool_choice="none")
                        continue

                    # ----- Human transfer intent -----
                    if re.search(r"\b(transfer|connect|human|agent|representative|manager|owner|live\s+agent|operator|jeriel)\b", low):
                        await rgate.cancel_active()
                        await rgate.create("Absolutely—one moment while I connect you.", tool_choice="none")
                        # Perform transfer AFTER the line finishes (handled by your after-done code or keep your existing pending_action if you prefer)
                        try:
                            target = TRANSFER_NUMBER
                            if tw_client and call_sid and valid_e164(target):
                                action_url = (PUBLIC_BASE_URL.rstrip("/") + "/twilio/after-transfer") if PUBLIC_BASE_URL else None
                                with suppress(Exception):
                                    tw_client.calls(call_sid).update(twiml=transfer_twiml(target, action_url=action_url))
                        except Exception as e:
                            log.error(f"Transfer failed: {e}")
                        continue

                    # For normal turns, let the model act (speak + tool calls) via ONE response
                    await rgate.create(None, tool_choice="auto")

                # ----- Tool call wiring -----
                elif t == "response.output_item.added":
                    item = evt.get("item", {})
                    if item.get("type") == "function_call":
                        cid = item.get("call_id"); name = item.get("name")
                        if cid: call_map[cid] = {"name": name, "args": ""}

                elif t == "response.function_call_arguments.delta":
                    cid = evt.get("call_id"); delta = evt.get("arguments","")
                    if cid and cid in call_map: call_map[cid]["args"] += delta

                elif t == "response.function_call_arguments.done":
                    cid = evt.get("call_id")
                    if not cid or cid not in call_map: continue
                    entry = call_map.pop(cid)
                    tool_name = entry.get("name")
                    try:
                        args = json.loads(entry.get("args") or "{}")
                    except Exception:
                        args = {}
                    # Execute tool and return output via conversation.item.create
                    await handle_tool_and_return(cid, tool_name, args)

        except asyncio.CancelledError:
            pass
        except Exception as e:
            log.info(f"openai_to_twilio ended: {e}")

# ================== Tool result bridge: replace any response.function_call_output with this ==================
    async def tool_output_to_model(call_id: str, payload: dict):
        # Return tool result item
        await oai.send_json({
            "type": "conversation.item.create",
            "item": {
                "type": "function_call_output",
                "call_id": call_id,
                "output": json.dumps(payload)
            }
        })
        # Prompt the model to continue with ONE new turn
        await rgate.create(None, tool_choice="auto")

# ================== Tool handlers: update to call tool_output_to_model(...) ==================
    async def handle_tool_and_return(call_id: str, tool_name: str, args: Dict[str, Any]):
        # check_slots
        if tool_name == "check_slots":
            start_time = (args.get("startTime") or "").strip()
            search_days = int(args.get("search_days") or 14)
            if not start_time:
                start_time = default_today_sensible_et_iso()

            if not MAIN_WEBHOOK_URL:
                await tool_output_to_model(call_id, {
                    "status": "error", "code": "webhook_unconfigured",
                    "message": "Booking line is not connected."
                })
                return

            payload = {
                "tool": "checkAvailableSlot",
                "event_type_id": EVENT_TYPE_ID,
                "tz": DEFAULT_TZ,
                "startTime": start_time,
                "search_days": search_days
            }
            status, data = await json_post(MAIN_WEBHOOK_URL, payload)
            body = data if isinstance(data, dict) else {}
            result = {
                "status": "ok" if status == 200 else "error",
                "http": status,
                "requestedStart": start_time,
                "humanRequested": human_et_from_iso(start_time),
                "exactMatch": bool(body.get("exactMatch")),
                "matchedSlot": body.get("matchedSlot"),
                "sameDayAlternates": body.get("sameDayAlternates") or [],
                "nearest": body.get("nearest") or [],
                "firstAvailableThatDay": body.get("firstAvailableThatDay"),
            }
            if (not result["exactMatch"]) and (not result["sameDayAlternates"] and not result["nearest"]) and search_days < 30:
                result["hint"] = "expand_search_to_30_days"
            await tool_output_to_model(call_id, result)
            return

        # appointment_webhook
        if tool_name == "appointment_webhook":
            req = ["booking_type","name","email","phone","startTime"]
            # Allow phone fallback to caller ID
            phone = (args.get("phone") or caller_number or "").strip()
            args["phone"] = phone

            missing = [k for k in req if not (args.get(k) or "").strip()]
            if "email" not in missing and not is_valid_email(args.get("email")):
                missing.append("email")

            if missing:
                await tool_output_to_model(call_id, {
                    "status": "error", "code": "missing_fields",
                    "missing": missing, "message": "To book we need: " + ", ".join(missing)
                })
                return

            if not MAIN_WEBHOOK_URL:
                await tool_output_to_model(call_id, {
                    "status": "error", "code": "webhook_unconfigured",
                    "message": "Booking line is not connected."
                })
                return

            # Deterministic idempotency
            idem = deterministic_booking_key(call_sid, args["startTime"], args["email"])
            if idem in booked_once:
                await tool_output_to_model(call_id, {"status": "ok", "note": "duplicate_suppressed", "idempotency_key": idem})
                return

            payload = {
                "tool": "book",
                "booking_type": args.get("booking_type","book"),
                "name": args["name"].strip(),
                "email": args["email"].strip(),
                "phone": args["phone"],
                "tz": DEFAULT_TZ,
                "startTime": args["startTime"].strip(),
                "event_type_id": int(args.get("event_type_id", EVENT_TYPE_ID)),
                "idempotency_key": idem,
                "notes": (args.get("notes") or None)
            }
            status, data = await json_post(MAIN_WEBHOOK_URL, payload)
            body = data if isinstance(data, dict) else {}
            if status == 200 and body.get("status") == "booked":
                booked_once.add(idem)
            await tool_output_to_model(call_id, {
                "status": body.get("status") if status == 200 else "error",
                "http": status,
                "bookedStart": payload["startTime"],
                "humanBooked": human_et_from_iso(payload["startTime"]),
                "email": payload["email"],
                "phone_last4": last4(payload["phone"]),
                "raw": body,
                "idempotency_key": idem
            })
            return

        # cancel_workflow
        if tool_name == "cancel_workflow":
            name = (args.get("name") or "").strip()
            phone = args.get("phone") or caller_number or None
            email = (args.get("email") or None)
            if not name or not (phone or email):
                await tool_output_to_model(call_id, {
                    "status":"error","code":"missing_fields",
                    "missing": [m for m in ["name","phone_or_email"] if (m=="name" and not name) or (m=="phone_or_email" and not (phone or email))],
                    "message": "To cancel we need a name and a phone or email."
                })
                return
            if not CANCEL_WEBHOOK_URL:
                await tool_output_to_model(call_id, {
                    "status":"error","code":"webhook_unconfigured","message":"Cancel line is not connected."
                })
                return
            payload = {"action":"cancel","name":name,"phone":phone,"email":email}
            status, data = await json_post(CANCEL_WEBHOOK_URL, payload)
            body = data if isinstance(data, dict) else {}
            await tool_output_to_model(call_id, {
                "status": body.get("status") if status == 200 else "error",
                "http": status, "phone_last4": last4(phone), "raw": body
            })
            return

        # transfer_call
        if tool_name == "transfer_call":
            target = args.get("to") or TRANSFER_NUMBER
            ok = bool(tw_client and call_sid and valid_e164(target))
            if ok:
                action_url = (PUBLIC_BASE_URL.rstrip("/") + "/twilio/after-transfer") if PUBLIC_BASE_URL else None
                with suppress(Exception):
                    tw_client.calls(call_sid).update(twiml=transfer_twiml(target, action_url=action_url))
            await tool_output_to_model(call_id, {
                "status": "ok" if ok else "error",
                "message": "Transferring now." if ok else "Transfer not configured."
            })
            return

        # Unknown
        await tool_output_to_model(call_id, {"status":"error","code":"unknown_tool","tool": tool_name})

# ================== Twilio 'speech_started' hook already cancels via rgate ==================
# ================== Ensure /media Twilio 'stop' just breaks without rejoin loops =============

# ======== PATCH END ========
