# ==== 1) CallContext: add identity-confirm fields (near the dataclass) ====
@dataclass
class CallContext:
    call_sid: str
    stream_sid: str
    caller_number: Optional[str] = None
    caller_last4: Optional[str] = None
    state: ConversationState = ConversationState.GREETING
    last_user_input: str = ""
    misunderstand_count: int = 0
    collected: Dict[str, str] = field(default_factory=dict)  # name/email/phone/startTime
    rejoin_reason: Optional[str] = None
    greeted_once: bool = False
    end_intent_pending: bool = False

    # NEW:
    pending_booking: Optional[Dict[str, Any]] = None   # payload to book once confirmed
    awaiting_confirm: Optional[str] = None             # "identity" while waiting for yes/no
    identity_confirmed: bool = False                   # set True after user says yes


# ==== 2) INSTRUCTIONS: replace the whole INSTRUCTIONS block ====
INSTRUCTIONS = f"""
You are the {brand} AI receptionist based in {loc}.
Default to ENGLISH; only switch languages if the caller speaks them first.
Be warm and concise (1–2 sentences), outcome-focused.

MISSION: Our mission is growth—handling the systems and strategies that move your business forward.
VALUE: {elevator_pitch}
KEY BENEFITS: {' • '.join(key_benefits)}
TRIAL: {trial_offer}

HOW WE HELP (examples to tailor after asking their business):
- SEO websites that actually bring customers
- AI receptionist & booking flows
- Workflow automations & integrations
- POS and CRM implementations
- Social media management & content systems

CONVERSATION FLOW:
- If they ask "what do you do / how can you help?": FIRST ask what business they run and what they're trying to achieve.
  Then give 1–2 specific ideas from the list above, tailored to that business.
- If they ask pricing: explain pricing is tailored to the business scope. Invite them to book a free consultation;
  after booking, the confirmation email includes next steps and all info needed.

BOOKING RULES:
- When time is mentioned, call check_slots first (narrate: "Let me check that time...").
- Book only after you have name + valid email + phone AND you have repeated them back and the caller says “yes.”
- Read the email back verbatim (“So that’s name@example.com — correct?”).
- Ask them to SPELL their full name letter-by-letter and spell the email (say “at” and “dot”) if audio seems unclear.

CLARITY:
- If you don’t understand: say once “Sorry—I didn’t catch that, could you repeat?”
  If it happens twice: add “Calls are clearest off speaker/headphones.”

TOOLS (never call with missing fields):
- check_slots(startTime, search_days?): verify availability (default startTime = today 3:00 PM ET; scan next 14 days)
- appointment_webhook(booking_type, name, email, phone, startTime): book after identity is confirmed
- cancel_workflow(name, phone|email): cancel (prefer phone; use caller ID if they agree)
- transfer_call(to?, reason?): connect to a human now
"""


# ==== 3) prompt_for_missing: strengthen spell instructions (find this helper and replace) ====
def prompt_for_missing(miss: list[str], caller_last: Optional[str]) -> str:
    labels = {
        "name": "your full name (please SPELL it letter by letter)",
        "email": "an email for confirmation (please SPELL it and say 'at' and 'dot')",
        "phone": "the best phone number",
        "startTime": "a day and time",
        "booking_type": "whether this is a new booking or reschedule"
    }
    parts = [labels.get(k, k) for k in miss]
    tail = f" I can use the number ending in {caller_last} if that works." if (caller_last and "phone" in miss) else ""
    return "To set that up I just need " + ", ".join(parts) + "." + tail + " You can tell me now."


# ==== 4) “Let me check that time …” and identity-confirm gate in check_slots (inside handle_tool_call) ====
if tool_name == "check_slots":
    start_time = (args.get("startTime") or "").strip()
    search_days = int(args.get("search_days") or 14)
    if not start_time:
        start_time = default_today_start_iso()

    if not MAIN_WEBHOOK_URL:
        await say("I can check availability once my booking line is connected. Meanwhile, what day generally works for you?")
        return

    # NEW: narrate the check
    await say("Let me check that time real quick.")

    payload = {
        "tool": "checkAvailableSlot",
        "event_type_id": EVENT_TYPE_ID,
        "tz": DEFAULT_TZ,
        "startTime": start_time,
        "search_days": search_days
    }
    status, data = await json_post(MAIN_WEBHOOK_URL, payload)
    body = data if isinstance(data, dict) else {}
    exact   = bool(body.get("exactMatch"))
    matched = body.get("matchedSlot")
    same    = body.get("sameDayAlternates") or []
    near    = body.get("nearest") or []
    first_d = body.get("firstAvailableThatDay")

    ctx.collected["startTime"] = matched or start_time

    have_identity = all([
        bool(ctx.collected.get("name") or args.get("name")),
        is_valid_email(ctx.collected.get("email") or args.get("email")),
        bool(ctx.collected.get("phone") or args.get("phone") or ctx.caller_number)
    ])

    # NEW: never auto-book; ask for confirmation first
    if exact and matched and have_identity:
        ctx.pending_booking = {
            "booking_type": "book",
            "name": (ctx.collected.get("name") or args.get("name")),
            "email": (ctx.collected.get("email") or args.get("email")),
            "phone": (ctx.collected.get("phone") or args.get("phone") or ctx.caller_number),
            "startTime": matched,
            "event_type_id": EVENT_TYPE_ID
        }
        ctx.awaiting_confirm = "identity"
        ctx.identity_confirmed = False
        phone_last = last4(ctx.pending_booking["phone"])
        await say(
            f"Great—{matched} Eastern is open. I heard your name as "
            f"{ctx.pending_booking['name']}, email {ctx.pending_booking['email']}, "
            f"and phone ending in {phone_last}. Please say “yes” to confirm, or spell the corrections."
        )
        return

    if exact and matched and not have_identity:
        miss = []
        if not (ctx.collected.get("name") or args.get("name")): miss.append("name")
        if not is_valid_email(ctx.collected.get("email") or args.get("email")): miss.append("email")
        if not (ctx.collected.get("phone") or args.get("phone") or ctx.caller_number): miss.append("phone")
        need = ", ".join(["your full name (spell it)" if m=="name" else
                          "an email for confirmation (spell it)" if m=="email" else
                          "the best phone number" for m in miss])
        tip = (f" I can use the number ending in {ctx.caller_last4} if that works. " if "phone" in miss and ctx.caller_last4 else " ")
        await say(f"Good news—{matched} Eastern is open. To lock it in I just need {need}.{tip}What should I use?")
        return

    # No exact match → offer options / widen search
    options = (same or near)[:3]
    lead = f"The first opening that day is {first_d}. " if first_d else ""
    if options:
        await say(("That exact time isn’t open. " if not exact else "") + lead + f"Closest options: {', '.join(options)}. What works best?")
    else:
        if search_days < 30:
            await handle_tool_call("check_slots", {"startTime": start_time, "search_days": 30})
        else:
            await say("I don’t see anything nearby that time. Another day might be better—what day works?")


# ==== 5) Identity-confirm gate in appointment_webhook (inside handle_tool_call) ====
elif tool_name == "appointment_webhook":
    req = ["booking_type","name","email","phone","startTime"]
    if not args.get("phone") and ctx.caller_number:
        args["phone"] = ctx.caller_number

    miss = [k for k in req if not (args.get(k) or "").strip()]
    if "email" not in miss and not is_valid_email(args.get("email")):
        miss.append("email")

    if miss:
        await say(prompt_for_missing(miss, ctx.caller_last4))
        return

    # NEW: hold booking until identity is confirmed by user
    payload = {
        "tool": "book",
        "booking_type": args.get("booking_type","book"),
        "name": args["name"].strip(),
        "email": args["email"].strip(),
        "phone": (args["phone"] or "").strip(),
        "tz": DEFAULT_TZ,
        "startTime": args["startTime"].strip(),
        "event_type_id": int(args.get("event_type_id", EVENT_TYPE_ID)),
        "idempotency_key": f"ai-{uuid.uuid4().hex}",
        "notes": (args.get("notes") or None)
    }

    if not ctx.identity_confirmed:
        ctx.pending_booking = payload
        ctx.awaiting_confirm = "identity"
        phone_last = last4(payload["phone"])
        await say(
            f"Just to be sure, I heard your name as {payload['name']}, "
            f"email {payload['email']}, and phone ending in {phone_last}. "
            "Please say “yes” to confirm, or spell the corrections."
        )
        return

    # Proceed with booking (user already confirmed)
    status, data = await json_post(MAIN_WEBHOOK_URL, payload)
    body_status = data.get("status") if isinstance(data, dict) else None
    phone_last  = last4(payload["phone"])

    if status == 200 and body_status == "booked":
        ctx.pending_booking = None
        ctx.awaiting_confirm = None
        ctx.identity_confirmed = False
        await say("All set. I’ve scheduled that in Eastern Time. "
                  f"I have your email as {payload['email']}"
                  + (f" and phone ending in {phone_last}" if phone_last else "")
                  + ". Anything else I can help with?")
    elif status in (409, 422) or body_status in {"conflict","conflict_or_error"}:
        await say("Looks like that time isn’t available. Earlier or later that day, or a nearby day?")
    else:
        await say("I couldn’t finalize that just now. Want me to try again or pick a different time?")


# ==== 6) Handle the “yes/no” after identity readback (in openai_to_twilio(), inside
#         the 'conversation.item.input_audio_transcription.completed' branch —
#         put this RIGHT AFTER you set `text = ...` and before transfer/bye checks) ====
# Identity confirmation path
if ctx.awaiting_confirm == "identity":
    low = text.lower()
    if re.search(r"\b(yes|yeah|yep|correct|that'?s (right|correct)|sounds good|affirmative)\b", low):
        if ctx.pending_booking:
            ctx.identity_confirmed = True
            # Re-enter appointment_webhook with the stored payload (now confirmed)
            pb = ctx.pending_booking
            ctx.pending_booking = None
            ctx.awaiting_confirm = None
            await handle_tool_call("appointment_webhook", {
                "booking_type": pb.get("booking_type","book"),
                "name": pb.get("name",""),
                "email": pb.get("email",""),
                "phone": pb.get("phone",""),
                "startTime": pb.get("startTime",""),
                "event_type_id": pb.get("event_type_id", EVENT_TYPE_ID),
                "notes": pb.get("notes")
            })
        continue
    if re.search(r"\b(no|nope|not (right|correct)|incorrect|wrong)\b", low):
        # Ask user to spell corrections and let the model collect them
        ctx.pending_booking = None
        ctx.awaiting_confirm = None
        ctx.identity_confirmed = False
        await say("No problem—please SPELL your full name and your email slowly, and I’ll read them back.")
        await respond_naturally()
        continue
# (…then your existing transfer/bye/other logic follows)
