# 03 — Payments: Telegram Stars + Stripe (and how Pro is granted)

Prerequisites: [00b](00b_http_and_backend_foundations.md) (HTTP, webhooks), [02](02_telegram_auth_initdata.md)
(`initData` HMAC, signatures), [04](04_shared_word_cache.md) (`ON CONFLICT` idempotency). Payments
lean on all three.

**The model in one paragraph.** "Pro" (full access) is sold **only via Telegram Stars** (⭐) as a
recurring monthly subscription (≈**292⭐**). Donations (coffee/cheesecake) are one-time Stars
purchases that **grant bonus Pro days**. Book-audio is a one-time Stars unlock. **Stripe is
retired** — the code is still there but behind an off-by-default flag; Pro cannot be bought with a
card right now. There is **no** `payments.py` module — the logic lives in `bot_3.py` (Telegram
handlers), `backend/backend_server.py` (invoice minting + scheduled jobs), and `backend/database.py`
(grants + entitlement).

Money code is where security matters most, so this file is heavy on the "why". Standard frame:
`⚙️1 Under the hood → 🥷2 Threats → 🛡️3 Defenses → 📈4 Recommendations`.

# ⚙️ 1. Under the hood

## 1.1 Price is computed on the server, never sent by the client

The Stars price is derived from EUR constants server-side (`backend_server.py:19075`):
`PRO_PRICE_EUR_MINOR=449` (€4.49) × `STARS_MARKUP=1.30` × `STARS_PER_EUR=50` → `ceil(...) = 292⭐`.
The client only ever names *what* it wants (`plan_code="pro"`, a `document_id`, a donation tier) —
validated against allow-lists — and the server decides the amount. The client **cannot** supply a
price. Remember this; it's the backbone of defense T-forgery.

## 1.2 The Stars purchase flow, step by step

Telegram's digital-goods flow has three moments: **invoice → pre-checkout → successful payment**.

### Step 1 — mint the invoice (server)

Two Mini-App endpoints (`billing_stars_invoice` `backend_server.py:55239`, `reader_audio_stars_invoice`
`:55167`), both gated by the block-02 `initData` HMAC, call one minter,
`create_stars_invoice_link` (`backend_server.py:19120`):

```python
body = {
    "title": title[:32], "description": description[:255],
    "payload": json.dumps(payload_obj, ensure_ascii=False)[:128],  # what-was-bought, ≤128 bytes
    "provider_token": "",          # 19137 — for Stars (XTR) this MUST be empty
    "currency": "XTR",             # 19138 — XTR = Telegram Stars
    "prices": [{"label": ..., "amount": max(1, int(stars))}],       # the SERVER-decided amount
}
if subscription_period:            # 2592000s = 30 days → recurring monthly Pro
    body["subscription_period"] = int(subscription_period)
resp = requests.post(f"https://api.telegram.org/bot{token}/createInvoiceLink", json=body, ...)
```

Terms: an **invoice** is a payment request; **XTR** is the currency code for Stars; a Stars invoice
has an **empty `provider_token`** (no external payment provider — Telegram is the processor). The
`payload_obj` is a small JSON blob (e.g. `{"purpose":"pro","user_id":123}`) that **Telegram echoes
back verbatim** in the success event, so the bot knows what was paid for. `subscription_period`
makes it a **recurring** subscription. The Mini App opens the returned link with
`WebApp.openInvoice`.

### Step 2 — pre-checkout (bot)

When the user confirms, Telegram sends a `pre_checkout_query` the bot must answer within 10 seconds.
`on_stars_pre_checkout` (`bot_3.py:36922`) **approves unconditionally**:

```python
await q.answer(ok=True)
```

Why no validation here? Because **there is nothing left for the user to tamper with**: the price and
payload were fixed server-side at mint time (Step 1), and the mint was `initData`-gated. The docstring
says exactly this. (Security note for §3/§4: the entire trust boundary is the mint step; nothing is
re-checked at pre-checkout.)

### Step 3 — successful payment / fulfillment (bot) — and the load-bearing bug

Telegram charges the Stars and sends a `successful_payment` update. `on_stars_successful_payment`
(`bot_3.py:36934`) fulfills it. **Its handler registration is the single most important line in the
payments system** (`bot_3.py:40181`):

```python
application.add_handler(MessageHandler(filters.SUCCESSFUL_PAYMENT, on_stars_successful_payment), group=1)
```

The comment above it (`bot_3.py:40174`) documents a **critical past bug**: python-telegram-bot runs
only the **first** matching handler *per group*, and `group=-1` already holds a catch-all
`TypeHandler(Update, _set_billing_user_context)`. When fulfillment lived in `group=-1`, that catch-all
**swallowed every `successful_payment`** → users were **charged but never granted Pro** (no grant, no
DM). Moving it to its own `group=1` guarantees it runs. This is exactly the kind of invisible,
money-losing bug that has no error — it just silently doesn't happen.

The handler then:

```python
charge_id = str(sp.telegram_payment_charge_id or "")     # Telegram's unique id for this charge
stars = int(sp.total_amount or 0)                        # XTR total = number of Stars paid
payload = json.loads(sp.invoice_payload or "{}")         # the blob we set at mint time
purpose = payload.get("purpose")                         # "pro" | "book_audio" | "support_coffee" | ...

is_new = record_star_payment_once(charge_id, uid, purpose, stars, payload)   # idempotency (below)
if not is_new:
    return                                               # re-delivered update → already fulfilled
# ... dispatch by purpose → grant ...
```

### Step 4 — idempotency (don't grant twice for one payment)

`record_star_payment_once` (`database.py:31612`) is the guard, run **before** any grant:

```sql
INSERT INTO bt_3_star_payments (telegram_payment_charge_id, user_id, purpose, stars, payload)
VALUES (%s, %s, %s, %s, %s::jsonb)
ON CONFLICT (telegram_payment_charge_id) DO NOTHING
RETURNING id;                       -- returns a row ONLY if this charge was new
```

`telegram_payment_charge_id` is `UNIQUE`, so a re-delivered `successful_payment` (Telegram retries)
inserts nothing, `RETURNING id` yields nothing, the function returns `False`, and the handler
returns early — **no double grant**. This is the same `ON CONFLICT DO NOTHING` idempotency pattern
as blocks 04/05.

⚠️ **One deliberate risk to know:** the caller **fails open** (`bot_3.py:36957`): if
`record_star_payment_once` *raises* (DB hiccup), `is_new` is forced to `True` — "better to grant than
to silently drop a paid purchase." So a DB outage mid-fulfillment could double-grant. That's an
availability-over-consistency choice; flagged in §4.

## 1.3 Granting the entitlement

Dispatch by `purpose` (`bot_3.py:36963`+):
- **`pro`** → `set_subscription_from_stripe(..., stripe_subscription_id=f"stars_{charge_id}",
  current_period_end=exp)` (`database.py:36601`). Note the reused Stripe-era table, with a
  **`stars_` prefix** on the subscription id — that prefix is load-bearing for lapse handling (1.5).
- **donations** → `record_sponsorship(...)` + `grant_pro_days(uid, days=7|14, source_charge_id=charge_id)`.
- **`book_audio`** → `grant_book_audio_unlock(...)`.

`grant_pro_days` (`database.py:18801`) has a nice **"banking"** rule:

```python
# if the user is on a PAID period right now, start the earned days at paid_end (not NOW),
# so they EXTEND the subscription instead of burning concurrently:
INSERT INTO bt_3_pro_grants (user_id, granted_until, reason, source_charge_id)
VALUES (%s, GREATEST(NOW(), COALESCE(%s, NOW()), COALESCE(%s, NOW())) + (%s || ' days')::interval, %s, %s);
```

`GREATEST(NOW(), existing_grant_end, paid_end)` picks the latest of "now / your current earned end /
your paid-period end" as the start, then adds the days — so donated days never overlap paid days.
`source_charge_id` links the grant to the paying charge, which is what lets a refund revoke *exactly*
those days (1.7). All writes are parameterized (`%s`).

## 1.4 "Is this user Pro right now?"

`is_user_pro` (`database.py:48031`) → `resolve_entitlement` (`database.py:37984`), the authority,
Redis-cached and busted on every grant/refund. A user is Pro if **either** a paid subscription is
active **or** an active earned grant exists (`bt_3_pro_grants.granted_until > NOW()`).

**Lapse-on-read (security-relevant).** Telegram Stars subscriptions send **no** "renewal failed /
canceled" event, so a lapsed subscription would otherwise read `active` forever. The code lapses it
*at read time* — but **only** for ids starting with `"stars_"** and only past
`current_period_end + 2 days` grace (`database.py:38060`), forcing `effective_mode="free"`. So a
non-renewing Stars sub self-expires without any webhook.

Free-tier limits are enforced by `reserve_free_feature_usage` (`database.py:37730`): Pro/trial users
**bypass** it entirely; free users hit a per-feature daily cap guarded by a **Postgres advisory
transaction lock** (so concurrent requests can't both slip past the limit). You saw the `429` side of
this in [01 §3](01_sentence_translation.md).

## 1.5 Refunds + reconciliation (closing the "pay → refund → keep Pro" loophole)

A refund returns the Stars, so the perks must go too. `revoke_star_payment_fulfillment`
(`database.py:31680`) reverses **by purpose**, using the recorded `bt_3_star_payments` row:

```python
if purpose in ("support_coffee", "support_cheesecake"):
    DELETE FROM bt_3_pro_grants WHERE user_id=%s AND source_charge_id=%s   # exactly the days this charge gave
    DELETE FROM bt_3_sponsorships WHERE stripe_checkout_session_id=%s      # its wall entry
elif purpose == "book_audio":
    DELETE FROM bt_3_book_audio_unlocks WHERE user_id=%s AND document_id=%s AND voice_tier=%s
elif purpose == "pro":
    deactivate_user_subscription(..., status="canceled", plan_code="free")
# then bust the entitlement cache
```

The `source_charge_id` link (set in 1.3) is what makes "delete **exactly** the days that charge
granted" possible. Idempotent — a second call finds nothing.

The clever part is **how a refund is even detected.** A Telegram-side or support refund fires **no
bot event**. So a **daily reconcile job** (`_run_stars_refund_reconcile_job`, `backend_server.py:61240`,
cron 04:20) reads **Telegram's own ledger** via `getStarTransactions` and treats every **outgoing**
transaction (no incoming `source`) whose id matches one of our still-active fulfilled charges as a
refund:

```python
ours = {p["charge_id"]: p for p in list_unrefunded_star_payments()}       # our fulfilled, not-yet-refunded charges
# page through the Telegram ledger:
for t in txns:
    if not isinstance(t.get("source"), dict):        # OUTGOING = a refund/withdrawal
        if str(t.get("id")) in ours:                 # ...whose id == one of our charges
            refunded_ids.add(str(t.get("id")))
for cid in refunded_ids:
    mark_star_payment_refunded(cid, via="reconcile")
    revoke_star_payment_fulfillment(cid)             # claw the perks back
```

This is the loophole closure: even a refund that bypassed the app entirely is caught within a day by
diffing Telegram's ledger against our fulfilled charges. (Admins can also refund manually via
`/refund_star` → `context.bot.refund_star_payment(...)`, `bot_3.py:9759`, which runs the same
clawback; a weekly report flags reconcile-caught vs manual refunds.)

## 1.6 Stripe (retired) — the webhook and why signatures matter

Stripe is off, but the code teaches the canonical **webhook-signature** pattern, so it's worth
reading. A webhook is a POST *Stripe* sends to *our* public URL to say "a payment happened". Unlike a
Telegram update (which arrives over the bot's authenticated connection), a public webhook URL can be
POSTed by **anyone** — so we must cryptographically verify it really came from Stripe.
`stripe_billing_webhook` (`backend_server.py:36161`):

```python
payload = request.get_data(cache=False, as_text=False)   # the RAW bytes (not re-parsed JSON) — critical
signature = request.headers.get("Stripe-Signature") or ""
event = stripe.Webhook.construct_event(payload, signature, STRIPE_WEBHOOK_SECRET)  # HMAC verify or raise
```

`construct_event` recomputes an HMAC of the raw body with our shared `STRIPE_WEBHOOK_SECRET` and
compares it to the `Stripe-Signature` header — same idea as the `initData` HMAC in block 02. It must
use the **raw bytes**, because re-serializing the JSON would change them and break the signature.
**But** the very first line of the route returns `200 {"ignored":"stripe_disabled"}` when
`STRIPE_BILLING_ENABLED` is off (`backend_server.py:36165`, default off at `:1083`), so in production
the signature check is never reached — dead code behind the flag. (It returns `200`, not an error, so
Stripe stops retrying.)

## 1.7 Diagram

```
  MINI APP  ──POST /billing/stars_invoice {plan_code}──▶  BACKEND (initData HMAC gate)
    (client names WHAT, never the price)                    │ price = server EUR→Stars (292⭐)
                                                            ▼ createInvoiceLink (XTR, empty provider_token)
  MINI APP ◀────────────── invoice link ──────────────────┘
    │ openInvoice → user pays Stars
    ▼
  TELEGRAM  ──pre_checkout_query──▶ BOT: answer ok=True (nothing left to validate)
  TELEGRAM  ──successful_payment──▶ BOT group=1 on_stars_successful_payment   ⚠️ MUST be its own group
                                      │ record_star_payment_once (UNIQUE charge_id) → dup? stop
                                      ▼ grant by purpose: set_subscription / grant_pro_days / audio unlock
  DB: bt_3_star_payments (ledger) · bt_3_pro_grants (days) · user_subscriptions (stars_<charge>)

  Daily 04:20 ── getStarTransactions ledger ── diff vs our fulfilled charges ── revoke refunds (clawback)

  LEGEND
    XTR  Telegram Stars currency        charge_id  Telegram's unique id per payment (idempotency key)
    group  PTB handler group; only the first matching handler per group runs
    stars_<charge>  subscription id prefix that marks a Stars sub for lapse-on-read
```

# 🥷 2. Threats

- **T1 — Payment forgery / price tampering.** Make the server grant Pro without paying, or pay less
  than 292⭐ — e.g. POST a fake amount, or inject a fake `successful_payment`.
- **T2 — Replay.** Capture one real `successful_payment` and replay it to get granted repeatedly.
- **T3 — Entitlement bypass.** Skip payment entirely and convince the app you're Pro (forge the
  entitlement check, or a never-expiring lapsed sub).
- **T4 — Refund abuse ("pay → refund → keep Pro").** Buy Pro, refund the Stars via Telegram/support
  (which sends the bot no event), and keep the perks.
- **T5 — Webhook spoofing (Stripe).** POST a fake "payment succeeded" event to the public
  `/api/billing/webhook` URL.
- **T6 — Fulfillment shadowing.** A refactor puts the `successful_payment` handler back where a
  catch-all swallows it → users charged but not granted (a *self-inflicted* money bug).

# 🛡️ 3. Defenses

- **T1 fails at two points.** (a) The **price is server-computed** from EUR constants at mint
  (§1.1); the client only sends an allow-listed `plan_code`, never an amount. (b) A `successful_payment`
  update is **minted by Telegram** after a real Stars charge and delivered over the **bot's
  authenticated connection** (getUpdates polling / secret-token webhook, keyed by the bot token). You
  can't inject a fake one without Telegram's cooperation — unlike a public HTTP endpoint. So "grant
  without paying" would require forging Telegram itself. The amount Telegram charges is the amount on
  the invoice *we* minted, so "pay less" is impossible too.
- **T2 fails on idempotency.** `record_star_payment_once` (`database.py:31625`) inserts on a `UNIQUE`
  `telegram_payment_charge_id` with `ON CONFLICT DO NOTHING`; a replayed update grants nothing.
- **T3 fails on server-side entitlement + lapse-on-read.** "Is Pro" is resolved **on the server**
  from the DB (`resolve_entitlement`), never trusted from the client; and a non-renewing Stars sub
  **self-expires** at read time past its grace window (`database.py:38060`), so a lapsed subscription
  can't read `active` forever.
- **T4 fails on the daily ledger reconcile (§1.5).** Because a bypassed refund fires no event, the
  job diffs **Telegram's own `getStarTransactions` ledger** against our fulfilled charges and claws
  back the perks (`backend_server.py:61240`) — deleting exactly the `source_charge_id`-linked grants.
  This is the specific mechanism that closes the loophole.
- **T5 fails on HMAC signature verification** — `stripe.Webhook.construct_event` on the **raw** body
  with the shared secret (`backend_server.py:36171`); a forged POST without the secret can't produce a
  valid `Stripe-Signature`. (Moot in prod since Stripe is flag-disabled, but the pattern is correct.)
- **T6 is defended by the dedicated `group=1`** (`bot_3.py:40181`) and a big warning comment — but
  it's a *convention*, not enforced. This is the top hardening item (§4).

# 📈 4. Recommendations

1. **Add an integration test that `successful_payment` is handled in its own group** (T6). This bug
   loses money silently and has already happened once; a test that asserts fulfillment runs (and the
   `group=-1` catch-all doesn't swallow it) is the cheapest insurance in the whole codebase.
2. **Reconsider the fail-open idempotency** (`bot_3.py:36957`). Failing open can double-grant on a DB
   error. At minimum, alert when a grant happens without a recorded ledger row; better, retry the
   `record_star_payment_once` write before deciding.
3. **Assert the paid amount at fulfillment.** Today the grant ignores `total_amount`. Telegram enforces
   the invoice amount, so a mismatch shouldn't happen — but logging/alerting when `stars` ≠ the
   expected price for that `purpose` would catch pricing bugs and tampering attempts early.
4. **Optionally re-validate at pre-checkout** against a server-side record of the pending invoice
   (defense in depth), instead of unconditionally `ok=True` — so the trust doesn't rest entirely on
   the mint step.
5. **Monitor grant/refund anomalies**: spikes in refunds-per-user, grants without matching charges, or
   reconcile catching many out-of-band refunds (could indicate abuse or a fulfillment regression).

# Self-check

1. The client calls `/billing/stars_invoice`. Which fields can it control, and which one can it
   **not** — and why does that make "pay less than 292⭐" impossible?
2. Why can't an attacker forge a `successful_payment` the way they could POST a fake Stripe webhook?
   What's the difference in how the two events reach our code?
3. `record_star_payment_once` uses `ON CONFLICT (telegram_payment_charge_id) DO NOTHING`. What attack
   does that stop, and what's the deliberate risk in the caller's `except: is_new = True`?
4. A user buys Pro, then refunds via Telegram support (the bot gets no event). Exactly how does the
   app still take the perks back, and which SQL column lets it remove *only* that charge's days?
5. Why must the Stripe webhook verify the **raw request bytes** rather than the parsed JSON?

Last checked against the code: 2026-07-29 (line numbers verified against current source; repo edited
concurrently — grep the function name if a line is off).
