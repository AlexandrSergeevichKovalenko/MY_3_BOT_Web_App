# 02 — Telegram authentication (`initData`): the security foundation

Prerequisites: [00a foundations](00a_frontend_foundations.md), [00b HTTP/Flask](00b_http_and_backend_foundations.md),
and [01 sentence translation](01_sentence_translation.md) (which used the auth gate without fully
opening it). This block opens it.

**Why this block matters most.** Every protected endpoint in the app answers one question first:
"who is making this request, and can I trust that?" If that check is weak, nothing else matters — an
attacker just claims to be you. In our backend, `_telegram_hash_is_valid` is called in **98 places**
(`grep -c "_telegram_hash_is_valid" backend/backend_server.py` → 98). Understanding this one
mechanism explains the security of 98 endpoints at once.

This file follows the standard frame: `⚙️1 Under the hood → 🥷2 Threats → 🛡️3 Defenses →
📈4 Recommendations`. Section 1 is a full technical dissection, including the cryptography, explained
from zero.

# ⚙️ 1. Under the hood

## 1.1 The problem this solves

Our frontend runs inside Telegram's webview (a browser). When it calls our backend, the backend must
know **which Telegram user** this is — and be sure the frontend didn't just *claim* a user id. A
naive design would send `{ "user_id": 12345 }` and trust it. That's forgeable: anyone can type any
number. We need a claim the client **cannot fake**.

Telegram's answer: when it opens our Mini App, it hands the page a signed string called **`initData`**.
It contains the user's identity **plus a cryptographic signature** that only Telegram (and our
server, which shares a secret with Telegram) can produce or verify. The frontend forwards `initData`
untouched; the backend verifies the signature. A forged identity fails verification.

## 1.2 What `initData` physically is

It's a URL query string (foundations 00b §1 — same `key=value&key=value` format):

```
user=%7B%22id%22%3A12345%2C%22first_name%22%3A%22Alex%22%7D&auth_date=1718000000&hash=9a3f...e1
```

Decoded, three fields matter:
- **`user`** — URL-encoded JSON of the user: `{"id":12345,"first_name":"Alex",...}`. This is the
  identity claim.
- **`auth_date`** — a **unix timestamp** (seconds since 1 Jan 1970) of when Telegram issued this.
- **`hash`** — the **signature**: a hex string that authenticates all the *other* fields.

There can be more fields (`query_id`, `chat_instance`, …). The rule: `hash` signs **everything
except itself**.

## 1.3 Cryptography from zero: hash, bytes, HMAC

You need four concepts before the code makes sense. Each is defined once here and reused everywhere.

**Bytes vs strings.** Text you read ("WebAppData") is a **string**. Cryptography operates on raw
**bytes** (numbers 0–255), not human text. So we convert: `"abc".encode("utf-8")` turns a string
into bytes. In Python a **bytes literal** is written with a `b` prefix: `b"WebAppData"` is already
bytes, no conversion needed. This is why you'll see `.encode("utf-8")` sprinkled around — it's
"string → bytes" so the crypto functions accept it.

**Hash function (SHA-256).** A **hash** is a one-way function: feed it any input, get a fixed-size
output (SHA-256 → 256 bits = 32 bytes = 64 hex characters). Two properties: (a) the same input always
gives the same output; (b) you **cannot** run it backwards to recover the input, and you can't
practically find two inputs with the same output. Think of it as a tamper-evident fingerprint of the
data. `hashlib.sha256(data_bytes).digest()` computes it.

**`.digest()` vs `.hexdigest()`.** Same hash, two encodings of the result: `.digest()` returns raw
**bytes**; `.hexdigest()` returns the same value as a **hex string** (e.g. `"9a3f...e1"`). Telegram's
`hash` field is hex, so verification ends in `.hexdigest()`.

**HMAC (keyed hash).** A plain hash proves "the data wasn't changed", but anyone can compute it, so it
proves nothing about *who* made it. **HMAC** fixes that: it mixes a **secret key** into the hash.
`HMAC(key, message)` produces a code that only someone holding `key` can produce — but anyone holding
`key` can verify. `hmac.new(key_bytes, message_bytes, hashlib.sha256)` computes it. Our secret key is
derived from our **Telegram bot token** (which lives only on our server and Telegram's servers).
That's the whole trust anchor: **possession of the bot token = ability to sign.**

## 1.4 The verification code, dissected

The gate is `_telegram_hash_is_valid` (`backend/backend_server.py:4021`). It rebuilds the signature
from the data and checks it matches the `hash` the client sent.

```python
def _telegram_hash_is_valid(init_data: str) -> bool:
    if not _telegram_init_data_auth_date_is_fresh(init_data):   # step 0: not too old / not future (1.6)
        return False
    telegram_bot_token = _ensure_telegram_bot_token()           # our secret (backend_server.py:1955)
    data_check_string = _build_telegram_data_check_string(init_data)   # step 1 (below)
    secret_key = hmac.new(                                       # step 2: derive the signing key
        b"WebAppData",                                          #   key   = literal bytes "WebAppData"
        telegram_bot_token.encode("utf-8"),                    #   msg   = our bot token as bytes
        hashlib.sha256,                                        #   algo  = SHA-256
    ).digest()                                                 #   → raw bytes (this is Telegram's recipe)
    calculated_hash = hmac.new(                                 # step 3: sign the data with that key
        secret_key,
        data_check_string.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()                                              #   → hex string, to compare with client's hash
    received_hash = dict(parse_qsl(init_data, keep_blank_values=True)).get("hash")
    return hmac.compare_digest(calculated_hash, received_hash or "")   # step 4: constant-time compare
```

Read the recipe carefully — Telegram defines a **two-step** key derivation, and getting the two
steps in the right order is the whole thing:
- **Step 2** — the signing key is *itself* an HMAC: `secret_key = HMAC(key="WebAppData", msg=bot_token)`.
  So the literal string `"WebAppData"` is the key and our bot token is the message. Result: raw bytes.
- **Step 3** — the actual signature: `calculated_hash = HMAC(key=secret_key, msg=data_check_string)`.
  Result: a hex string.
- **Step 4** — compare our `calculated_hash` to the client's `received_hash`. If equal, the client
  possessed a genuine, Telegram-signed `initData` → authentic. If not → reject.

### Step 1 in detail — `data_check_string` (`backend_server.py:4014`)

Both Telegram and we must sign the **exact same bytes**, or the hashes won't match. So the fields are
put into one canonical string:

```python
def _build_telegram_data_check_string(init_data: str) -> str:
    pairs = parse_qsl(init_data, keep_blank_values=True)          # "a=1&b=2" → [("a","1"),("b","2")]
    data = {key: value for key, value in pairs if key != "hash"}  # dict comprehension: drop the hash field
    sorted_pairs = [f"{key}={data[key]}" for key in sorted(data.keys())]  # "key=value", sorted by key
    return "\n".join(sorted_pairs)                               # join with newline between each
```

Three syntax pieces, each explained once:
- `parse_qsl(s)` — a Python stdlib function that parses a query string into a **list of (key, value)
  tuples**. `keep_blank_values=True` keeps keys whose value is empty.
- `{key: value for key, value in pairs if key != "hash"}` — a **dict comprehension**: build a dict by
  looping over `pairs`, keeping every pair except the one named `hash` (you can't include the
  signature in the thing being signed).
- `sorted(data.keys())` — sort the field names **alphabetically**. This is critical: Telegram sorts
  them too, so both sides produce the identical string. Different order → different bytes → different
  hash → false rejection. `"\n".join(list)` glues the `key=value` lines with a newline between them.

So for our example the signed string is literally:
```
auth_date=1718000000
user={"id":12345,"first_name":"Alex",...}
```
(`auth_date` before `user` because `a` < `u`.) That exact text is what gets HMAC-signed in step 3.

## 1.5 A second, DIFFERENT algorithm: the Login Widget

Telegram has **two** sign-in surfaces, and they use **different** key derivations. Knowing the
difference stops a real class of bug (using the wrong recipe). Our code has both:

- **Mini App `initData`** (what everything above uses): `secret_key = HMAC(key="WebAppData", token)`.
- **Login Widget** (website "Log in with Telegram" button): `secret_key = SHA256(token)` — a plain
  hash of the token, **not** an HMAC. See `_telegram_login_hash_is_valid` (`backend_server.py:4076`):

```python
# backend/backend_server.py:4089  — note: sha256(token), NOT hmac(key="WebAppData", token)
secret_key = hashlib.sha256(telegram_bot_token.encode("utf-8")).digest()
calculated_hash = hmac.new(secret_key, data_check_string.encode("utf-8"), hashlib.sha256).hexdigest()
return hmac.compare_digest(calculated_hash, received_hash)
```

Same idea (rebuild the signature, constant-time compare), different key. If you validated Login
Widget data with the Mini App recipe (or vice-versa), every legitimate login would fail — or worse,
if you got it backwards you might accept forgeries. They are deliberately separate functions.

## 1.6 Freshness — the replay window (`backend_server.py:4040`)

A correct signature isn't enough; we also check the `auth_date` isn't stale or from the future:

```python
def _telegram_init_data_auth_date_is_fresh(init_data, *, max_age_seconds=None,
                                           max_future_skew_seconds=300):
    ttl_seconds = max(60, int(max_age_seconds or TELEGRAM_WEBAPP_INIT_TTL_SECONDS))
    auth_date = int(parsed["auth_date"])
    now_ts = int(time.time())
    if auth_date > now_ts + max(0, max_future_skew_seconds):   # not dated in the future (>300s ahead)
        return False
    return (now_ts - auth_date) <= ttl_seconds                 # not older than the TTL
```

`TELEGRAM_WEBAPP_INIT_TTL_SECONDS` defaults to **2592000 = 30 days** (`backend_server.py:936`). So a
captured `initData` keeps working for up to 30 days. That's convenient (users don't get logged out
mid-session) but it's a **long replay window** — flagged in recommendations.

## 1.7 How the request carries `initData`, and the unified resolver

Block 01 noted "sometimes header, sometimes body." The truth: there's one extractor that accepts
`initData` from **five** places (`_extract_request_init_data`, `backend_server.py:4132`):

```python
return str(
    request.headers.get("X-Telegram-InitData")        # 1. custom header (most of the app)
    or request.headers.get("X-Telegram-Init-Data")    # 2. hyphen variant
    or bearer                                          # 3. Authorization: Bearer <initData>
    or body.get("initData")                            # 4. JSON body field (the translation flow)
    or request.args.get("initData")                    # 5. ?initData=... in the URL (GET routes)
).strip()
```

On top of that, `_resolve_webapp_user_id` (`backend_server.py:4173`, used by 12 endpoints) tries
`initData` first, then falls back to durable **browser tokens** for the standalone PWA (which has no
Telegram `initData` because it runs outside Telegram):

```python
def _resolve_webapp_user_id(payload=None) -> int | None:
    init_data = _extract_request_init_data(body)
    if init_data and _telegram_hash_is_valid(init_data):     # path A: real Telegram initData (HMAC)
        return int(user_id)
    token = _extract_dict_browser_token(body)                # path B: durable dictionary PWA token
    if token: ... return int(rec["user_id"])
    app_token = _extract_app_browser_token(body)             # path C: durable home-screen app token
    if app_token: ... return int(rec["user_id"])
    return None                                              # nobody authenticated
```

So there are **three ways to be authenticated**: a valid Telegram `initData` (strong, HMAC-verified),
or one of two long-lived opaque tokens issued to installed PWAs. The tokens exist because `initData`
only lives inside Telegram and expires; a home-screen app needs its own durable credential. Each
token is looked up in the DB (`resolve_dict_browser_token` / `resolve_app_browser_token`) to find the
user it belongs to.

## 1.8 The server can MINT `initData` (`backend_server.py:4094`)

Because we hold the bot token, we can *produce* valid `initData`, not just verify it —
`_build_signed_init_data_for_user` does exactly the step-2/step-3 recipe to sign a fresh string for a
user (used after a Login Widget sign-in, to hand the web session a WebApp-style credential). This is
normal "the server issues sessions" behavior; it's safe **only** because the bot token never leaks
(section 2, T3).

## 1.9 Diagram

```
  Telegram servers ──(sign with bot token)──▶ initData string ──▶ our frontend (untouched)
                                                                        │
                                    header / body / query  ────────────┘
                                                                        ▼
   backend: _extract_request_init_data (5 sources) ─▶ _telegram_hash_is_valid
                                                          │  rebuild signature with bot token
                                                          │  + freshness (≤30d, not future)
                                              ┌───────────┴───────────┐
                                        match │                       │ no match / stale
                                              ▼                       ▼
                                     _parse → user.id            401 Unauthorized
                                     (you are authenticated)

  LEGEND
    bot token   the shared secret between Telegram and our server; the entire trust anchor
    HMAC        keyed hash: only the token-holder can produce a valid `hash`
    initData    Telegram-signed identity string; frontend forwards it, never edits it
```

# 🥷 2. Threats

- **T1 — Identity forgery.** Craft `initData` with `user.id` = a victim (or an admin) and a made-up
  `hash`. Goal: act as someone else across all 98 protected endpoints.
- **T2 — Field tampering.** Take your *own* valid `initData` and change one field (e.g. bump `id`,
  or edit `auth_date` to stay fresh forever) while keeping the old `hash`.
- **T3 — Bot-token theft.** The token is the master key. Steal it (from a leaked env var, a public
  git commit, a logged error) and you can sign valid `initData` for **anyone** — total game over.
- **T4 — Replay.** Capture a real signed `initData` (from network logs, a shared device, a URL) and
  reuse it later.
- **T5 — Algorithm confusion.** Feed Login-Widget data to the Mini-App verifier or vice-versa, hoping
  a recipe mismatch accepts something it shouldn't.
- **T6 — Browser-token theft / downgrade.** Steal a durable PWA token (path B/C in 1.7), which has no
  30-day expiry, to impersonate that user indefinitely.
- **T7 — Missing gate.** Find one of the endpoints that *forgot* to call the check (out of ~100) and
  hit it directly with no auth at all.

# 🛡️ 3. Defenses

- **T1 & T2 fail because of the HMAC.** The `hash` signs `user` and `auth_date` (1.4). Change any
  signed field and `calculated_hash != received_hash` → `False` → `401`. You can't compute a fresh
  valid `hash` without the bot token. Editing `auth_date` to stay "fresh" also breaks the hash,
  because `auth_date` is inside the signed `data_check_string` — so T2's "keep the old hash" never
  matches.
- **T3 is mitigated by keeping the token server-only.** It's read via `_ensure_telegram_bot_token`
  from an **environment variable** (never in code, never committed — see
  [stack_explained.md → env vars](../toolbox/stack_explained.md)). It never appears in a response or
  in frontend code. (Operational discipline is the defense here; there's no cryptographic fallback if
  it leaks — hence the recommendation to rotate on suspicion.)
- **T4 is bounded by freshness.** `_telegram_init_data_auth_date_is_fresh` (1.6) caps a replay to the
  TTL window and rejects future-dated tokens. (Bounded, not eliminated — see recommendation 1: 30
  days is long.)
- **T5 fails because the two recipes are separate, correct functions.** Mini-App uses
  `HMAC("WebAppData", token)`; Login Widget uses `SHA256(token)` (1.5). Neither validator accepts the
  other's data, because the derived key differs, so a swapped payload just fails the compare.
- **Timing attacks fail because of `hmac.compare_digest`.** Every compare (both recipes) uses
  constant-time comparison, not `==`, so an attacker can't recover the correct `hash` byte-by-byte by
  measuring response times (explained in [01 §3](01_sentence_translation.md)).
- **T6: browser tokens are opaque, DB-backed, and revocable.** They're random values resolved to a
  user id via a DB lookup; a "user left/blocked the bot" event busts the gate (`_dict_user_has_left_bot`,
  `backend_server.py:4216`). They are weaker than `initData` (no built-in expiry), which is exactly
  why they're scoped narrowly (dictionary PWA / app PWA) and separated by distinct headers so scopes
  can't collide.

# 📈 4. Recommendations

1. **Shorten the TTL for sensitive actions.** 30 days (`backend_server.py:936`) is a large replay
   window. Keep it for low-risk reads, but require a *fresh* `initData` (minutes, not days) for
   money/entitlement/admin actions.
2. **Turn the 98 inline checks into one decorator.** Auth is copy-pasted into ~100 handlers; that's
   how one endpoint eventually ships *without* it (threat T7). A `@require_telegram_user` decorator
   that runs the gate and injects `user_id` would make "is this route protected?" auditable in one
   place. (Compare the 98 vs 12 split between `_telegram_hash_is_valid` and `_resolve_webapp_user_id`
   — inconsistency is itself a smell.)
3. **Add a startup assertion that the bot token is set and not a placeholder**, and document a
   **rotation** procedure (regenerate via BotFather, update the env var) for the day it leaks —
   because T3 has no cryptographic backstop.
4. **Give browser tokens an expiry + rotation**, since they lack `initData`'s freshness (T6). Even a
   long sliding expiry is better than "valid forever."
5. **Log and alert on `401` spikes and on any use of the URL/query `initData` source** (path 5 in
   1.7), since putting `initData` in a URL is the most leak-prone transport (00b §2).
6. **Add a test that every `/api/webapp/*` route rejects a request with no/invalid `initData`** — a
   single parametrized pytest (see [running_and_testing.md](../toolbox/running_and_testing.md)) that
   guards against a future unprotected endpoint.

# Self-check

1. An attacker sends `initData` with `user.id=999` (an admin) and a random `hash`. Walk through
   `_telegram_hash_is_valid` and name the exact line where it's rejected and why.
2. Why does `_build_telegram_data_check_string` **sort** the keys before joining? What breaks if our
   server sorted but Telegram didn't (or vice-versa)?
3. Mini-App and Login-Widget both end in `hmac.compare_digest(...)` but differ in one earlier line.
   Which line, and what's the difference?
4. The bot token leaks into a public git commit. Which threat (T1–T7) does that unlock, and why is
   there no code-level defense — only rotation?

Next: block **03 (Payments)** builds directly on this — it's how we know *who* is paying/entitled,
and where forging identity would mean stealing Pro.

Last checked against the code: 2026-07-20 (line numbers verified against current source).
