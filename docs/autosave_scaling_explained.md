# Nightly Auto-Save — How It Works & Why It Scales (explained from scratch)

> A plain-language tour of the "🌙 Ночной автосейв" feature: every technology we use
> (Redis, PgBouncer, caches, queues, workers, locks…), **why** it exists, and **where** in
> our code it lives. Written for someone new to backend engineering. File references look
> like `backend/backend_server.py:37272` — click them in your editor to jump to the code.

---

## Table of contents

1. [What the feature does (the 30-second version)](#1-what-the-feature-does)
2. [The big idea: keep the user's path light, push heavy work elsewhere](#2-the-big-idea)
3. [Our building blocks (each technology explained)](#3-building-blocks)
   - [Services / tiers](#31-services--tiers)
   - [Database, connections, pooling, PgBouncer](#32-database-connections-pooling-pgbouncer)
   - [Cache & TTL](#33-cache--ttl)
   - [Redis](#34-redis)
   - [Queues, brokers, workers, Dramatiq](#35-queues-brokers-workers-dramatiq)
   - [The scheduler (cron) & the "sweep"](#36-the-scheduler--the-sweep)
   - [Debounce](#37-debounce)
   - [Locks, races, idempotency, "exactly-once"](#38-locks-races-idempotency)
4. [The full pipeline, step by step (with code)](#4-the-full-pipeline)
5. [The three scaling bugs we fixed (before → after)](#5-the-three-bugs-we-fixed)
6. [How this behaves at 1k–10k users](#6-at-1k10k-users)
7. [Keeping it clean & correct — anti-garbage & anti-duplicate techniques](#7-clean-and-correct)
   - [7.1 No garbage — three layers of filtering](#71-three-layers)
   - [7.2 No duplicates — exactly-once with NX-locks & claims](#72-no-duplicates)
   - [7.3 No loss — at-least-once durability (process → send → consume)](#73-durability)
   - [7.4 Supporting techniques (chunking, overflow, batched upload, instant save)](#74-supporting)
8. [Glossary](#8-glossary)

---

## 1. What the feature does

You run an iPhone **Shortcut** at night that takes ~30 screenshots, OCRs the German text out of
them, and sends each photo's text to our server. With auto-save **ON**, instead of spamming you
with one card per word, the bot quietly collects everything, and in the morning sends **one**
message: a numbered list of words + translations with checkboxes. You untick the junk, press
**"Сохранить выбранные"**, and the good ones land in your dictionary (with correct article,
lemma, and folder). Done.

The hard part isn't the feature — it's making it **cheap and fast when 10,000 people do it the
same night**. That's what most of this document is about.

---

## 2. The big idea

There are two kinds of work in any app:

- **Critical-path work** — things that must happen *while the user waits* for a response.
  This has to be **fast**. Example: when your Shortcut sends a photo's text, our server must
  reply "got it" quickly.
- **Background work** — things the user doesn't need to see happen immediately. This can be
  **slow and heavy**. Example: asking OpenAI to split text into words and translate them
  (that takes many seconds).

**The golden rule of scaling:** do as little as possible on the critical path; shove everything
heavy into the background, onto separate machines (**workers**) the user never waits for.

A useful distinction:

- **Latency** = how long *one* request takes (how long the user waits).
- **Throughput** = how *many* requests per second the whole system can handle.

If you do heavy work on the critical path, you hurt **both**: each user waits longer *and* you
can serve fewer users at once.

---

## 3. Building blocks

### 3.1 Services / tiers

Our app is not one program — it's several separate programs ("**services**" / "**tiers**")
running on **Railway** (our hosting platform). Each is its own process, often on its own
machine, and they talk to each other. The ones relevant here:

| Service | Job |
|---|---|
| **BACKEND_WEB** | The web server. Receives the Shortcut's HTTP requests. Must stay fast. |
| **MY_3_BOT** | The Telegram bot process. Handles button taps in chat. |
| **BACKGROUND_JOBS** | A **worker**. Does heavy background jobs pulled from a queue. |
| **SCHEDULER_SERVICE** | A clock. Fires periodic "tick" events (like cron). Runs as exactly one copy. |
| **Postgres** | The database (permanent storage). |
| **PgBouncer** | A connection pooler sitting in front of Postgres (explained below). |
| **Redis** | A super-fast in-memory store, used as scratch space + message queue. |

Why split into services? So you can **scale them independently**. If splitting text is slow, you
add more **BACKGROUND_JOBS** workers without touching the web tier. The web tier stays lean and
fast. This is called **horizontal scaling** (add more copies of the part that's the bottleneck).

### 3.2 Database, connections, pooling, PgBouncer

The **database** (Postgres) is where data lives permanently (your dictionary, your settings).

To talk to it, a program opens a **connection** — basically a phone call to the database. Opening
that call is **expensive**: it's a TCP network handshake + authentication + the database
allocating memory for your session. It can take tens of milliseconds. If you open and close a
fresh connection for every tiny query, you waste huge amounts of time on setup/teardown, and the
database falls over when thousands of users hit it at once (each user holding an expensive call).

Two layers fix this:

1. **Connection pool** — instead of opening/closing per query, the app keeps a small set of
   connections open and **reuses** them. Need to run a query? "Check out" a connection from the
   pool, use it, "check it back in." No handshake each time. Our code does this through
   `get_db_connection_context()` — `backend/database.py:2872`. (A "checkout + round-trip" means:
   borrow a connection from the pool, send the query, wait for the answer to come back.)

2. **PgBouncer** — a **connection pooler** that sits *between* our app and Postgres. Postgres
   itself can only handle a limited number of real connections (each is heavy). PgBouncer lets
   hundreds of app-side connections share a *small* number of real Postgres connections, in
   **transaction pooling** mode (a real connection is only tied up for the duration of one
   transaction, then immediately reused by someone else). All 5 of our services already go
   through PgBouncer, so we never pay the "open a brand-new Postgres connection" cost on the hot
   path.

**Takeaway:** "going through PgBouncer + a pool" = we reuse cheap, already-open connections
instead of dialing the database from scratch every time. That's why one of our fixes was about
*avoiding even the pooled query* on every request — see C3 below.

### 3.3 Cache & TTL

A **cache** is a small, fast copy of an answer you computed recently, so you don't recompute it.

Analogy: instead of phoning a colleague every time to ask "is the office open?", you write the
answer on a sticky note. For the next few minutes you just read the note. After a while the note
might be stale, so you throw it away and ask again.

That "throw away after a while" is the **TTL** (*time to live*) — how long the cached answer is
considered fresh. Short TTL = fresher but more recomputes; long TTL = fewer recomputes but
staler data.

We use this for the auto-save toggle (is it ON for this user?). The answer rarely changes, so we
cache it for **30 seconds** in the process's own memory, instead of asking the database every
time. Code: `_autosave_toggle_cached()` — `backend/backend_server.py:37257`:

```python
def _autosave_toggle_cached(user_id: int) -> bool:
    """Toggle state with a 30s in-process cache so the hot shortcut path doesn't hit the DB."""
    now = time.time()
    hit = _AUTOSAVE_TOGGLE_CACHE.get(int(user_id))
    if hit and hit[0] > now:          # cached answer still fresh? use it.
        return hit[1]
    try:
        val = bool(get_shortcut_autosave_enabled(int(user_id)))  # else: ask the DB once
    except Exception:
        val = False
    _AUTOSAVE_TOGGLE_CACHE[int(user_id)] = (now + _AUTOSAVE_TOGGLE_CACHE_TTL, val)  # write the sticky note
    return val
```

`_AUTOSAVE_TOGGLE_CACHE` is just a Python dictionary `{user_id: (expiry_time, value)}`.
"In-process" means the note lives in that one program's RAM (fast, but not shared between
services — that's fine here).

> There's a second, identical cache on the **bot** side for the reply-keyboard label
> (ВКЛ/ВЫКЛ): `_autosave_state_cached()` — `bot_3.py:554`. Same idea: don't hit the DB every time
> we redraw the menu.

### 3.4 Redis

**Redis** is an **in-memory data store**: a database that keeps everything in RAM, so reads and
writes take *microseconds* (vs *milliseconds* for Postgres on disk). The trade-off: RAM is
smaller and more volatile than disk, so Redis is for **temporary / fast-changing** data, not
your permanent records.

Redis isn't just key→value; it has **data structures**. We use three:

- **String** — a single value under a key. We use it for a timestamp and a lock.
- **List** — an ordered sequence you can push onto and read back. We use it as the **queue of
  raw photo texts** for a user.
- (Redis can also do hashes, sets, etc. — we used a hash in the old design, removed now.)

Redis is also **single-threaded** for command execution, which means each command is **atomic**
(it fully finishes before the next one starts). That property is what makes Redis a safe place to
implement **locks** and **claims** between many machines (see 3.8).

We give every Redis key a **TTL** too, so leftover data auto-expires (e.g. raw texts live 6h).

Our Redis keys for this feature (all defined near `backend/backend_server.py:37245`):

| Key | Type | Purpose |
|---|---|---|
| `autosave_raw:{user_id}` | List | The raw OCR text of each photo, appended as it arrives |
| `autosave_flush_at:{user_id}` | String | A timestamp: "flush this user at/after this time" |
| `autosave_flush_lock:{user_id}` | String | A lock so only one flush runs per user at a time |
| `autosave_digest:{id}` | String (JSON) | The prepared morning digest (words, translations, selection state) |

### 3.5 Queues, brokers, workers, Dramatiq

A **message queue** is how one program hands a job to another program to do **later**, without
waiting for it.

- A **producer** puts a "job message" into the queue ("please flush user 123").
- A **broker** is the middleman that stores the queue of messages. **Our broker is Redis.**
- A **worker** (**consumer**) is a separate process that pulls messages off the queue and
  actually does the work.

This decouples *who asks* from *who does*. The web tier can ask for heavy work and immediately
move on; the worker tier chews through the queue at its own pace. Need more capacity? Add more
workers — that's horizontal scaling again.

We use **Dramatiq**, a Python library that makes this easy. You mark a function as an "**actor**"
and it becomes a job that can be sent to the queue and run by a worker. Each actor belongs to a
named **queue**; a worker is configured to listen to certain queue names.

Example — our flush job, `backend/background_jobs.py:1298`:

```python
@dramatiq.actor(max_retries=0, queue_name="shortcut_lookup")
def run_autosave_flush_job(user_id: int) -> None:
    ...
    from backend.backend_server import _run_autosave_flush
    _run_autosave_flush(safe_user_id)   # the heavy work, on the worker
```

- `@dramatiq.actor(...)` = "this function is a background job."
- `queue_name="shortcut_lookup"` = it goes on the queue our **BACKGROUND_JOBS** worker already
  listens to. (Importantly, BACKGROUND_JOBS also has the Telegram **bot token**, which the flush
  needs to actually send you the digest. A worker without the token would silently fail to send —
  that exact bug bit us once, so we deliberately put the flush where the token lives.)
- Somewhere else, `run_autosave_flush_job.send(user_id=123)` = "enqueue this job." `.send()`
  returns instantly; the work happens later on the worker.

### 3.6 The scheduler & the "sweep"

We need something to fire **periodically** ("every 30 seconds, check who's ready"). That's the
**SCHEDULER_SERVICE** — it runs **APScheduler** (a timer library) as **exactly one copy** (so a
tick fires once, not 5 times). Its only job is to send a tiny "tick" message; it does no heavy
work itself.

Our tick, registered at `backend/scheduler_service.py:729`, calls `_dispatch_autosave_sweep()`
(`backend/scheduler_service.py:356`), which just enqueues the **sweep** actor:

```python
def _dispatch_autosave_sweep() -> None:
    run_autosave_sweep_job.send()     # "go check who's due", runs on the worker
```

The **sweep** (`run_autosave_sweep_job`, `backend/background_jobs.py:1277`) is a cheap once-per-
tick scan: it looks at all the `autosave_flush_at:*` timestamps, finds users whose batch has gone
quiet, and **fans out** one flush job per such user. "**Fan-out**" = one job spawns many smaller
jobs that workers then run in parallel.

### 3.7 Debounce

**Debounce** = "wait until things go quiet before acting."

Your 30 photos arrive as 30 *separate* requests over a few minutes. We don't want 30 morning
digests — we want **one**, after the **last** photo. So:

- Every photo sets `autosave_flush_at:{user} = now + 90 seconds` (it keeps pushing the deadline
  forward — see `_run_shortcut_autosave_staging`, `backend/backend_server.py:37272`).
- The sweep only flushes a user once `now ≥ flush_at`, i.e. once **90 seconds have passed with no
  new photo**. As long as photos keep coming, the deadline keeps moving and nothing fires.

Think of an elevator door that resets its timer every time someone steps in; it only closes once
people stop coming.

`_AUTOSAVE_DEBOUNCE_SECONDS` (default 90) is at `backend/backend_server.py:37239`. The sweep
interval (default 30s) is at `backend/scheduler_service.py:729`.

### 3.8 Locks, races, idempotency

When many machines touch the same data, you get **race conditions**: two of them act on the same
thing at the same time and step on each other (e.g. two flushes send *two* digests for one user).

We prevent that with three ideas:

- **Lock** — a "do not disturb" flag only one party can hold. We use Redis `SET key value NX`,
  where **NX** means *"set only if it doesn't already exist."* Because Redis commands are atomic,
  exactly one caller wins the `NX` and gets the lock; everyone else sees it's taken and backs
  off. See the start of `_run_autosave_flush` (`backend/backend_server.py:37463`):

  ```python
  lock_key = _autosave_flush_lock_key(safe_user_id)
  if client.set(lock_key, "1", nx=True, ex=300) is None:
      return   # someone else is already flushing this user → stop
  ```

  (`ex=300` = the lock auto-expires after 300s so a crash can't deadlock us forever.)

- **Claim** — when the sweep decides a user is due, it **deletes** that user's `flush_at` key in
  the same pass (`_autosave_collect_due_user_ids`, `backend/backend_server.py:37310`). Deleting =
  "I've claimed this one." The next sweep won't see it, so it can't enqueue a duplicate.
  Likewise, the flush **reads then deletes** the raw list, so a second run finds nothing to do.

- **Idempotency** — designing an operation so doing it twice does no harm. Even our final dictionary
  save is idempotent: saving the same word twice doesn't create a duplicate row (the database
  upsert returns "already existed"). Lock + claim + idempotency together give us
  **"exactly-once"** behavior in practice: the digest is sent once, the words saved once.

---

## 4. The full pipeline

Here's the whole journey of one nightly batch, end to end.

```
 iPhone Shortcut (×30 photos)
        │  HTTP POST /api/shortcut/lookup   (one per photo)
        ▼
 ┌─────────────────────────────────────────────────────────┐
 │ BACKEND_WEB  (must stay FAST — critical path)            │
 │  • is auto-save ON?  → _autosave_toggle_cached (30s cache)│
 │  • _run_shortcut_autosave_staging:                        │
 │       RPUSH autosave_raw:{uid}   (append this photo text) │
 │       SET   autosave_flush_at:{uid} = now+90s  (debounce) │
 │  • return 200 immediately.  NO OpenAI here. NO waiting.   │
 └─────────────────────────────────────────────────────────┘
        │ (raw text now sitting in Redis)
        ▼
 ┌─────────────────────────────────────────────────────────┐
 │ SCHEDULER_SERVICE  — every 30s sends a "sweep" tick       │
 └─────────────────────────────────────────────────────────┘
        │ run_autosave_sweep_job.send()
        ▼
 ┌─────────────────────────────────────────────────────────┐
 │ BACKGROUND_JOBS worker                                    │
 │  sweep: scan autosave_flush_at:*; for each user whose     │
 │         90s went quiet → claim it → enqueue a flush job   │
 └─────────────────────────────────────────────────────────┘
        │ run_autosave_flush_job.send(uid)   (fan-out)
        ▼
 ┌─────────────────────────────────────────────────────────┐
 │ BACKGROUND_JOBS worker  (has the bot token)               │
 │  _run_autosave_flush(uid):                                │
 │    • NX-lock the user (exactly-once)                      │
 │    • read + delete the raw text list (claim the batch)    │
 │    • ONE OpenAI "split" call over ALL the text            │
 │    • ONE OpenAI "normalize+translate+category" call       │
 │    • store the digest in Redis, send ONE Telegram message │
 └─────────────────────────────────────────────────────────┘
        │ (morning) you tick words, press "Сохранить выбранные"
        ▼
 ┌─────────────────────────────────────────────────────────┐
 │ MY_3_BOT  handle_autosave_digest_save_callback            │
 │  • instantly remove the buttons + "⏳ Сохраняю…"           │
 │  • save the chosen words in the BACKGROUND, then "✅ …"    │
 └─────────────────────────────────────────────────────────┘
```

**Step A — the request path** (`_run_shortcut_autosave_staging`,
`backend/backend_server.py:37272`). This is the only part the user's Shortcut waits on, so it
does the absolute minimum — two Redis writes:

```python
length = int(client.rpush(raw_key, normalized_text))   # append this photo's text to the list
client.expire(raw_key, _AUTOSAVE_RAW_TTL)               # auto-clean after 6h
client.setex(                                            # (re)arm the debounce deadline
    _autosave_flush_at_key(safe_user_id),
    _AUTOSAVE_RAW_TTL,
    f"{time.time() + _AUTOSAVE_DEBOUNCE_SECONDS:.3f}",
)
```

No OpenAI, no database, no threads — microseconds of Redis. The web tier can absorb a huge burst.

**Step B — the sweep finds who's due** (`_autosave_collect_due_user_ids`,
`backend/backend_server.py:37310`). One cheap scan over the timestamps, claiming the ripe ones:

```python
for key in client.scan_iter(match="autosave_flush_at:*", count=200):
    flush_at = float(client.get(key))
    if now >= flush_at:            # 90s of quiet have passed
        client.delete(key)        # claim it (so next sweep won't re-enqueue)
        due.append(user_id)
```

**Step C — the heavy work, on the worker** (`_run_autosave_flush`,
`backend/backend_server.py:37463`): lock, claim the raw list, **one** split call for the whole
batch (instead of one per photo), **one** prepare call (`_autosave_prepare_cards`,
`backend/backend_server.py:37358`, which returns the dictionary form + translation + folder
category in a single request), then build and send the digest.

**Step D — saving** (`handle_autosave_digest_save_callback`, `bot_3.py:1992`): the tap gets an
**instant** acknowledgement and the buttons vanish, while the actual database writes happen in the
background — so you never sit there wondering if it worked.

---

## 5. The three bugs we fixed

The earlier version technically worked but would have collapsed under load. The audit found three
issues; here's each in plain terms.

### C1 — Debounce implemented with *sleeping threads* (critical)

A **thread** is a single lane of execution inside a process; a process has a limited number of
them. The old code, for every photo, started a task that literally did `sleep(91 seconds)` —
holding a thread hostage for a minute and a half, on a pool of only **2** threads.

30 photos → 30 sleeping tasks → only 2 can "sleep" at once → they pile up; and other users'
flushes get stuck behind them. At 1,000 users this jams completely.

**Fix:** no sleeping. The deadline is just a Redis **timestamp**; a single periodic **sweep**
checks all deadlines at once. Nothing is held; nothing waits. (Old `time.sleep` debounce and its
thread pool are deleted.)

### C2 — Heavy OpenAI work ran on the web tier, per photo (critical)

The old code called OpenAI to "split" text into words **inside each request**, on the web tier's
small thread pool. That's 30 slow AI calls per user per night, clogging the very tier that must
stay fast — and ~30,000 AI calls/night at 1,000 users.

**Fix:** the request path stores raw text only. **One** worker job later does **one** split over
the *whole* batch + **one** translate/normalize call. From ~30 AI calls per user down to ~2, and
all of it on the worker tier, off the user's path. (`_run_autosave_flush`,
`backend/backend_server.py:37463`.)

### C3 — A database query on *every* request just to check the toggle (high)

The old code asked the database "is auto-save ON for this user?" on every single incoming photo
request. Even with PgBouncer making that query cheap, it's still a connection checkout + a network
round-trip *per request*, multiplied by every user and every photo.

**Fix:** a **30-second in-process cache** (`_autosave_toggle_cached`,
`backend/backend_server.py:37257`). The first request asks the DB; the next ones (for 30s) read
the answer from RAM. The toggle almost never changes, so this is essentially free correctness.

---

## 6. At 1k–10k users

Why the new shape holds up:

- **Web tier (BACKEND_WEB):** each photo = two Redis writes (microseconds) + a cached boolean.
  A nightly stampede of thousands of photos is trivial. No AI, no DB, no threads on this path.
- **AI cost:** ~2 OpenAI calls per *user per night*, not ~30 per user. At 10k users that's ~20k
  calls/night total instead of ~300k — an order of magnitude cheaper and lighter.
- **Worker tier (BACKGROUND_JOBS):** the only place heavy work runs. If it's ever the
  bottleneck, you add more worker copies — the queue spreads jobs across them automatically.
  This is the whole point of the queue/worker split.
- **Database:** unchanged volume of *saves* (only words you actually keep), all via the pooled,
  PgBouncer-fronted connections; no per-request toggle queries anymore.
- **Scheduler:** one tiny tick every 30s regardless of user count; the scan is cheap and touches
  nothing when no one is using the feature.
- **Correctness under concurrency:** NX-lock + claim + idempotent saves = each digest sent once,
  each word saved once, even if jobs get retried or overlap.

**Deployment note:** none of this needed new infrastructure. The `shortcut_lookup` and
`scheduler_jobs` queues were already handled by **BACKGROUND_JOBS**, and that worker already
holds the Telegram bot token — so the flush can both run heavy work *and* send your message from
the same place. We just redeploy the code; no new env vars, no new queues.

---

<a name="7-clean-and-correct"></a>
## 7. Keeping it clean & correct — anti-garbage & anti-duplicate techniques

The hardest part of this feature wasn't sending words — it was making sure **only good words**
reach you, **exactly once**, and **never lost**. Early on we had real disasters: a queue that
piled up to 1660 stale entries, batches that translated 7 of 352 items, garbage symbols flooding
the chat. This chapter explains the techniques we used to fix all of that — they're the same
ideas you'll use anywhere you build reliable systems.

<a name="71-three-layers"></a>
### 7.1 No garbage — three layers of filtering ("рубежи")

You can never filter perfectly in one place, so we filter in **three independent layers**. If
junk slips past one, the next catches it. This "defense in depth" is the key idea.

**Layer 1 — at the entry (when we split the text into units).** A deterministic gate drops the
obviously-not-German stuff *before* it ever becomes a card: URLs/brands, code, math formulas,
English-heavy text, pure symbols/numbers. Crucially it's **conservative** — anything with a
German signal (an umlaut/ß, a German-only word, a grammar term) is **always kept**, so we never
lose real German. Code: `_shortcut_looks_non_german` — `backend/backend_server.py:36181` — used
inside the gate `_shortcut_is_learnable_unit` — `backend/backend_server.py:36216`.

> Why conservative? Because dropping a real German word is worse than letting one piece of junk
> through — the junk gets caught later (Layers 2–3), but a lost word is gone.

**Layer 2 — at translation time.** When the translator processes a word and itself reports "this
isn't valid German / no translation" (e.g. for `/\` it returns *"нет подходящего немецкого
слова"*), we simply **don't show a card** for it. So junk that slipped past Layer 1 silently
disappears the moment you press translate. Code: `_dictionary_result_is_garbage` — `bot_3.py:8608`.

> This is what stopped "Быстрый перевод" from dumping garbage: the translator already *knows*
> the input is junk, so we just trust that signal and drop it.

**Layer 3 — the self-cleaning queue.** The card-flow "inbox" only removes a word when you act on
it. A word you *ignore* used to sit there forever (the 1660 pile-up). Now every entry carries a
`created_at` timestamp, and a nightly sweep drops anything older than a day — junk you never
touched expires on its own. Code: `_dict_pending_entry_age_seconds` — `bot_3.py:6248` and the
nightly job `_purge_stale_pending_all_users` — `bot_3.py:6259`. The window is long (24h) on
purpose, so today's real words you mean to batch tomorrow are never lost.

<a name="72-no-duplicates"></a>
### 7.2 No duplicates — exactly-once with NX-locks, claims & dedup

When many machines (or many retries) touch the same batch, the danger is sending the **same
digest twice** or saving the **same word twice**. Three tools prevent that:

**NX-lock — "only one at a time."** Before a worker flushes a user's batch, it tries to grab a
Redis lock with `SET key value NX` — *set only if it doesn't already exist*. Because Redis runs
commands one at a time, **exactly one** worker wins; any other worker (a retry, an overlapping
sweep) sees the lock is taken and backs off. So a user's digest is built by **one** worker at a
time — no parallel duplicates. Code: the top of `_run_autosave_flush` — `backend/backend_server.py:37686`:

```python
lock_key = _autosave_flush_lock_key(safe_user_id)
if client.set(lock_key, "1", nx=True, ex=300) is None:
    return  # another flush for this user is already running
```

(`ex=300` auto-releases the lock after 5 minutes, so a crashed worker can't deadlock the user
forever.)

**Dedup — "the same word collapses to one."** The same German word from several photos is
normalized to one key (lowercase, no punctuation; ß≈ss) so it only appears once in a batch. Code:
`_shortcut_dedup_norm` — `backend/backend_server.py:37212`.

**Idempotent save — "saving twice is harmless."** The final dictionary save checks for an
existing entry first, so pressing save twice (or a retry) never creates a duplicate row.

Together: **NX-lock + dedup + idempotent save = "exactly-once" in practice.** The digest is sent
once, each word stored once — even if jobs retry or overlap.

<a name="73-durability"></a>
### 7.3 No loss — at-least-once durability (process → send → consume)

This one matters because the iPhone Shortcut **deletes the photos** once the server says `ok`.
If we then lost the text, you'd have neither photos nor words.

Two safeguards:

1. **Persist before acknowledging.** The server writes the raw text to Redis *before* it returns
   `ok` to the Shortcut. Redis is a **separate service** — redeploying the app doesn't touch it —
   so an app redeploy can never lose accepted text.

2. **Process, send, *then* consume.** The flush used to *delete* the queue first, then spend
   seconds translating + sending. A redeploy in that window lost the batch. Now it reads the
   queue (no delete), sends the digest, and **only then** removes the items it processed
   (`LTRIM` the processed prefix — photos that arrived mid-flush survive). If the worker is killed
   in the middle, the queue + trigger are untouched, so the next sweep just re-processes it. The
   sweep also no longer deletes the trigger early — the flush owns clearing it on success. Code:
   `_run_autosave_flush` — `backend/backend_server.py:37686` and the sweep
   `_autosave_collect_due_user_ids` — `backend/backend_server.py:37444`.

This is the classic **at-least-once vs at-most-once** choice:
- *At-most-once* (delete first) risks **loss** — unacceptable when photos are already gone.
- *At-least-once* (consume last) risks a rare **duplicate** — but only if the worker dies in the
  tiny gap between Telegram delivering the digest and the consume, and it's a duplicate of the
  **same clean words**, not garbage. We chose at-least-once: **a duplicate is annoying; a loss is
  unacceptable.** A small attempt counter caps retries so a persistently-failing batch can't spam
  the chat.

<a name="74-supporting"></a>
### 7.4 Supporting techniques

- **LLM batch chunking.** One LLM call can't return hundreds of translations — the JSON answer
  gets truncated (this caused "requested 352, returned 7"). We split the work into small chunks
  (~20), run them with limited concurrency, and merge — reliable at any size. Code:
  `run_dictionary_lookup_multilang_core_fast_batch` — `backend/openai_manager.py:4860`.
- **Decide by real units, not raw lines.** Whether a batch becomes individual cards or one
  digest is decided *after* the split, on the number of actual learnable units (> 12 → digest),
  not on OCR line count — so a single Reel screenshot (a multi-line paragraph → 2–3 units) stays
  as cards. Code: `_run_shortcut_lookup_delivery` — `backend/backend_server.py:37248`,
  threshold `_SHORTCUT_CARD_OVERFLOW_UNITS` — `backend/backend_server.py:37606`.
- **Batched upload.** The Shortcut OCRs a whole folder into one list and sends it in **one** HTTP
  request (`{"screenshots": [...]}`) instead of one request per photo — far lighter on the web
  tier. The parser accepts the list in any shape (array, newline string, JSON-in-a-string). Code:
  `_shortcut_lookup_request_payload` — `backend/backend_server.py:36901`.
- **Instant in-place save.** Tapping a save button instantly shows "💾 Сохраняем…" in place, does
  the DB write in the background, then flips to "✅ Сохранено" — no waiting, no extra chat
  message. Same "instant ack + background work" pattern as the digest. Code:
  `_save_dictionary_variants_in_place` — `bot_3.py:10831`.

---

## 8. Glossary

- **Service / tier** — a separate running program (web, bot, worker, scheduler, db). Scaled
  independently.
- **Process / thread** — a process is a running program; a thread is one lane of execution inside
  it. Limited resource; don't waste them sleeping.
- **Critical path** — work done while the user waits. Keep it minimal.
- **Latency** — time for one request. **Throughput** — requests/second the system can handle.
- **Connection** — an open session to the database; expensive to create (TCP + auth).
- **Connection pool** — a reusable set of open connections, so you don't reconnect each time.
- **PgBouncer** — a pooler in front of Postgres letting many app connections share few real DB
  connections (transaction pooling).
- **Round-trip** — sending a request and waiting for the response to come back.
- **Cache** — a fast stored copy of a recent answer to avoid recomputing.
- **TTL (time to live)** — how long a cached value / Redis key stays valid before expiring.
- **Redis** — in-memory store (RAM-fast), used as scratch space, queue, and lock holder.
- **Atomic** — an operation that fully completes as one indivisible step (no half-states).
- **Queue / broker / producer / consumer (worker)** — a queue holds jobs; the broker (Redis for
  us) stores it; a producer adds jobs; a worker pulls and runs them.
- **Dramatiq** — our Python library for queues/workers. An **actor** is a function runnable as a
  background job; each actor has a **queue_name**.
- **`.send()`** — enqueue a job and return immediately (the work happens later, elsewhere).
- **Scheduler / APScheduler / cron** — fires periodic ticks. Ours runs as one copy and only emits
  tiny "go check" messages.
- **Sweep** — a periodic scan that finds ready items and enqueues work for them.
- **Fan-out** — one job spawning many smaller jobs handled in parallel.
- **Debounce** — wait until activity goes quiet before acting (so a burst yields one action).
- **Race condition** — two actors touching the same data at once and conflicting.
- **Lock** — a flag only one actor can hold (`SET … NX` in Redis) to serialize access.
- **Claim** — deleting/marking an item so no one else picks it up.
- **Idempotent** — doing it twice has the same effect as once (no duplicates).
- **Exactly-once** — the practical guarantee, via lock + claim + idempotency, that an effect
  happens a single time.
- **NX-lock** — `SET key value NX` in Redis: "set only if absent." Because Redis is atomic,
  exactly one caller wins it — our way to make a flush run for one user at a time.
- **At-most-once vs at-least-once** — two delivery guarantees. *At-most-once* (consume/delete
  first) can lose work on a crash. *At-least-once* (consume after success) can produce a rare
  duplicate but never loses work. We pick at-least-once where loss is unacceptable.
- **Defense in depth / layered filtering** — filtering the same problem (garbage) at several
  independent points, so what slips past one layer is caught by the next.
- **Dedup / normalization** — collapsing variants of the same thing to one canonical key
  (lowercase, no punctuation) so it appears once.
- **Chunking** — splitting a too-big request into small pieces (e.g. ~20 items per LLM call) so
  the response isn't truncated, then merging the pieces.
- **Debounce** — waiting until activity goes quiet before acting (so a burst yields one action).
- **Horizontal scaling** — adding more copies of the bottleneck tier to handle more load.

---

*Code map (jump points):*
`_run_shortcut_autosave_staging` `backend/backend_server.py:37393` ·
`_autosave_toggle_cached` `backend/backend_server.py:37378` ·
`_autosave_collect_due_user_ids` `backend/backend_server.py:37444` ·
`_run_autosave_flush` `backend/backend_server.py:37686` ·
`_autosave_send_digest_from_blocks` `backend/backend_server.py:37609` ·
`_autosave_prepare_cards` `backend/backend_server.py:37494` ·
`_shortcut_looks_non_german` (Layer 1) `backend/backend_server.py:36181` ·
`_shortcut_is_learnable_unit` `backend/backend_server.py:36216` ·
`_dictionary_result_is_garbage` (Layer 2) `bot_3.py:8608` ·
`_purge_stale_pending_all_users` (Layer 3) `bot_3.py:6259` ·
`_shortcut_dedup_norm` `backend/backend_server.py:37212` ·
`_run_shortcut_lookup_delivery` (card↔digest, >12 units) `backend/backend_server.py:37248` ·
`_shortcut_lookup_request_payload` (batched screenshots) `backend/backend_server.py:36901` ·
`run_dictionary_lookup_multilang_core_fast_batch` (chunking) `backend/openai_manager.py:4860` ·
`_save_dictionary_variants_in_place` (instant save) `bot_3.py:10831` ·
`run_autosave_sweep_job` `backend/background_jobs.py:1277` ·
`run_autosave_flush_job` `backend/background_jobs.py:1298` ·
`_dispatch_autosave_sweep` `backend/scheduler_service.py:356` ·
sweep interval `backend/scheduler_service.py:729` ·
`get_db_connection_context` `backend/database.py:2872` ·
`get_shortcut_autosave_enabled` `backend/database.py:17699` ·
`handle_autosave_digest_save_callback` `bot_3.py:1992` ·
`_send_private_message` `backend/telegram_notify.py:18`
