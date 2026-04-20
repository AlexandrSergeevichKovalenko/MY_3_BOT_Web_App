# TTS Generation Subsystem Audit

**Branch:** `refactor/interface`  
**Date:** 2026-04-20  
**Scope:** All TTS generation, prewarm, recovery, and object lifecycle code in `backend/backend_server.py`  
**Purpose:** Establish exact boundaries before extracting `backend/tts_generation.py`

---

## 1. ENTRY POINTS

Every path that leads to TTS generation, prewarm, recovery, or object creation.

### 1.1 Scheduler Actors (`backend/background_jobs.py`)

| Actor | Line | Import Source | Execution Model |
|-------|------|---------------|-----------------|
| `run_tts_prewarm_scheduler_actor()` | 1450 | `backend.tts_scheduler` → `backend_server._dispatch_tts_prewarm` | Queue-based (Dramatiq), then **synchronous** generation inside |
| `run_tts_generation_recovery_actor()` | 1456 | `backend.tts_scheduler` → `backend_server._recover_stale_tts_generation_jobs` | Queue-based (Dramatiq), then thread-pool dispatch |
| `run_tts_prewarm_quota_control_actor()` | 1462 | `backend.tts_scheduler` → `backend_server._run_tts_prewarm_quota_control_scheduler_job` | Queue-based (Dramatiq), then Telegram send |

### 1.2 Scheduler Job Wrappers (`backend/backend_server.py`)

| Function | Line | Called By | Execution Model |
|----------|------|-----------|-----------------|
| `_run_tts_prewarm_scheduler_job()` | 14238 | Dramatiq actor + APScheduler | Calls `_dispatch_tts_prewarm(force=False)` |
| `_run_tts_generation_recovery_scheduler_job()` | 30707 | Dramatiq actor + APScheduler | Calls `_recover_stale_tts_generation_jobs(source="scheduler")` |
| `_run_tts_prewarm_quota_control_scheduler_job()` | 12518 | Dramatiq actor + APScheduler | Calls `_send_tts_prewarm_quota_control_message(force=False)` |
| `_run_sentence_prewarm_scheduler_job()` | 15554 | Dramatiq actor + APScheduler | Calls `_dispatch_sentence_prewarm(force=False)` |

### 1.3 HTTP Routes (`backend/backend_server.py`)

| Route | Method | Handler | Line | Execution Model |
|-------|--------|---------|------|-----------------|
| `/api/webapp/tts/generate` | POST | `webapp_tts_generate()` | 30840 | Sync: calls `_enqueue_tts_generation_job_result()`, returns 200/pending |
| `/api/webapp/tts/url` | GET | `webapp_tts_url()` | 30747 | Sync: polls `get_tts_object_meta()`, returns status/URL |
| `/api/webapp/tts` | POST | `webapp_tts()` | 31080 | Sync (legacy): calls `_run_tts_generation_job()` directly |
| `/api/admin/prewarm-tts` | POST | `prewarm_tts_now()` | 39331 | Sync: calls `_dispatch_tts_prewarm(force=True)` |
| `/api/admin/send-tts-prewarm-quota-control` | POST | `send_tts_prewarm_quota_control_now()` | 39316 | Sync: calls `_send_tts_prewarm_quota_control_message(force=True)` |

### 1.4 Core Dispatcher Functions

| Function | Line | Called By | Role |
|----------|------|-----------|------|
| `_dispatch_tts_prewarm(force, tz_name)` | 13775 | Scheduler job, admin route | Plans + executes prewarm for all active users (synchronous generation) |
| `_recover_stale_tts_generation_jobs(source)` | 30647 | Recovery scheduler | Finds stale pending DB records → pushes to thread-pool queue |
| `_enqueue_tts_generation_job_result(**kwargs)` | 30604 | Route, recovery, prewarm | Dedup check + `queue.Queue.put()` |
| `_enqueue_tts_generation_job(**kwargs)` | 30643 | Various callers | Thin bool wrapper around `_enqueue_tts_generation_job_result` |
| `_run_tts_generation_job(...)` | 30273 | Worker thread loop, legacy route, prewarm loop | **Core execution unit**: R2 check → Google TTS → R2 upload → DB mark ready |
| `_synthesize_mp3(text, language, voice, speed)` | 16361 | `_run_tts_generation_job` | Google Cloud TTS API call + pydub MP3 merge |

---

## 2. STATEFUL IN-PROCESS COMPONENTS

Every piece of process-local state related to TTS. **All block horizontal scaling.**

### 2.1 Locks

| Symbol | Type | Line | Writers | Readers | Process-local | Blocks Scale |
|--------|------|------|---------|---------|---------------|--------------|
| `_TTS_PREWARM_LOCK` | `threading.Lock` | 933 | `_dispatch_tts_prewarm` (acquire+release) | `_dispatch_tts_prewarm` (acquire) | Yes | Yes: prevents concurrent prewarm within one process only |
| `_TTS_GENERATION_QUEUE_LOCK` | `threading.RLock` | 617 | `_get_tts_generation_queue`, `_ensure_tts_generation_workers_started` | Same | Yes | No (init guard only) |
| `_TTS_GENERATION_JOBS_LOCK` | `threading.Lock` | 934 | `_enqueue_tts_generation_job_result`, `_run_tts_generation_job` | Same | Yes | Yes: per-process dedup only |
| `_TTS_ADMIN_MONITOR_LOCK` | `threading.Lock` | 614 | `_record_tts_admin_monitor_event`, `_get_tts_admin_monitor_window`, `_maybe_send_tts_admin_failure_alert` | Same | Yes | No (local only) |

### 2.2 Queues and Thread Pools

| Symbol | Type | Line | Writers | Readers | Process-local | Blocks Scale |
|--------|------|------|---------|---------|---------------|--------------|
| `_TTS_GENERATION_QUEUE` | `queue.Queue \| None` | 618 | `_get_tts_generation_queue` (init), `_enqueue_tts_generation_job_result` (put) | `_tts_generation_worker_loop` (get), `_tts_generation_queue_size` | Yes | **Critical blocker**: jobs enqueued in one process never visible to another |
| `_TTS_GENERATION_WORKER_THREADS` | `list[threading.Thread]` | 619 | `_ensure_tts_generation_workers_started` | `_ensure_tts_generation_workers_started` (liveness check) | Yes | Yes: daemon threads are per-process |

### 2.3 In-Memory State Dictionaries / Sets / Deques

| Symbol | Type | Line | Writers | Readers | Process-local | Blocks Scale |
|--------|------|------|---------|---------|---------------|--------------|
| `_TTS_GENERATION_JOBS` | `set[str]` | 935 | `_enqueue_tts_generation_job_result` (add), `_run_tts_generation_job` (discard) | `_enqueue_tts_generation_job_result` (membership check) | Yes | **Critical blocker**: duplicate job detected only within same process |
| `_TTS_ADMIN_MONITOR_EVENTS` | `deque` | 615 | `_record_tts_admin_monitor_event` (append+prune) | `_get_tts_admin_monitor_window`, `_maybe_send_tts_admin_failure_alert` | Yes | Yes: admin dashboard reads stale data if multiple workers run |
| `_TTS_ADMIN_ALERT_LAST_SENT` | `dict[str, float]` | 616 | `_should_send_tts_admin_alert` (write timestamp) | `_should_send_tts_admin_alert` (cooldown check) | Yes | Yes: alert cooldown not shared across processes |
| `_TTS_URL_POLL_ATTEMPTS` | `dict[str, int]` | 606 | `_increment_tts_url_poll_attempt` (line 2839) | `webapp_tts_url`, `webapp_tts_generate` | Yes | Yes: poll counts not shared across web workers |

### 2.4 Configuration Constants (module-level, read-only after init)

All defined lines 752–881 of `backend_server.py`. Key ones:

| Constant | Line | Default | Role |
|----------|------|---------|------|
| `TTS_GENERATION_WORKERS` | 755 | 4 | Thread pool size |
| `TTS_GENERATION_QUEUE_MAXSIZE` | 756–759 | 2000 | Queue backpressure limit |
| `TTS_GENERATION_RECOVERY_ENABLED` | 764 | False | Recovery scheduler on/off |
| `TTS_PREWARM_ENABLED` | 825 | False | Prewarm scheduler on/off |
| `TTS_PREWARM_QUOTA_CONTROL_ENABLED` | 848 | True | Quota control on/off |
| `GoogleTTSBudgetBlockedError` | 16129 | — | Custom exception class, raised inside `_enforce_google_tts_monthly_budget` |

---

## 3. CORE EXECUTION CHAINS

### Path A: Prewarm Path

```
run_tts_prewarm_scheduler_actor()          background_jobs.py:1450
  └─ run_tts_prewarm_scheduler_job()        tts_scheduler.py:11
       └─ _run_tts_prewarm_scheduler_job()  backend_server.py:14238
            └─ _dispatch_tts_prewarm(force=False, tz_name)  backend_server.py:13775
                 ├─ _should_run_tts_prewarm_now(tz_name)    backend_server.py:13416
                 ├─ _TTS_PREWARM_LOCK.acquire(blocking=False)
                 ├─ _list_tts_prewarm_active_user_ids(...)   backend_server.py:13469
                 │    └─ DB SELECT (dictionary_queries, card_review_log, billing_events)
                 ├─ _get_tts_prewarm_user_activity_map(...)  backend_server.py:13511
                 │    └─ DB SELECT (card_srs_state, dictionary_queries, review_log, billing_events)
                 ├─ per user: _get_user_language_pair(user_id)  backend_server.py:3694
                 ├─ per user: _list_predicted_tts_candidates_for_user(...)  backend_server.py:13630
                 │    └─ DB SELECT (card_srs_state, dictionary_queries)
                 ├─ per candidate: get_tts_object_meta(cache_key)  database.py:12376
                 ├─ per candidate: create_tts_object_pending() OR requeue_tts_object_pending()  database.py:12380/12399
                 ├─ per candidate: _run_tts_generation_job(...)   ← SYNCHRONOUS  backend_server.py:30273
                 │    ├─ r2_exists(object_key)                    r2_storage.py:82
                 │    ├─ _billing_log_event_safe(r2_head)         backend_server.py:8681
                 │    ├─ _synthesize_mp3(text, language, voice, speed)  backend_server.py:16361
                 │    │    ├─ _enforce_google_tts_monthly_budget()  backend_server.py:16308
                 │    │    │    └─ get_google_tts_monthly_budget_status()  database.py:18669
                 │    │    ├─ prepare_google_creds_for_tts()      backend/utils.py:12
                 │    │    └─ texttospeech.TextToSpeechClient().synthesize_speech(...)  Google API
                 │    ├─ r2_put_bytes(object_key, audio_bytes)    r2_storage.py:102
                 │    ├─ _billing_log_event_safe(r2_put × 3)      backend_server.py:8681
                 │    ├─ mark_tts_object_ready(cache_key, url)    database.py:12535
                 │    ├─ _clear_tts_url_poll_attempt(cache_key)   backend_server.py:2851
                 │    ├─ _record_tts_admin_monitor_event()        backend_server.py:11788
                 │    └─ _log_flow_observation(generation_runner_finished)
                 ├─ _record_tts_admin_monitor_event(prewarm_run)  backend_server.py:11788
                 ├─ _maybe_send_tts_admin_failure_alert()         backend_server.py:12258
                 └─ _TTS_PREWARM_LOCK.release()
```

**Critical note:** `_run_tts_generation_job()` is called **synchronously** inside the prewarm loop (line 14010). There is no queue dispatch here. The scheduler actor blocks until every MP3 is generated. A prewarm run for 50 users × 15 items can run for minutes inside a single Dramatiq actor message.

---

### Path B: Recovery Path

```
run_tts_generation_recovery_actor()                    background_jobs.py:1456
  └─ run_tts_generation_recovery_scheduler_job()        tts_scheduler.py:14
       └─ _run_tts_generation_recovery_scheduler_job()  backend_server.py:30707
            └─ _recover_stale_tts_generation_jobs(source="scheduler")  backend_server.py:30647
                 ├─ _ensure_tts_generation_workers_started()            backend_server.py:30547
                 │    ├─ _TTS_GENERATION_QUEUE_LOCK.acquire()
                 │    ├─ _get_tts_generation_queue()   → creates queue.Queue if None
                 │    └─ spawns N daemon threads running _tts_generation_worker_loop()
                 ├─ list_stale_pending_tts_objects(limit, older_than_minutes)  database.py
                 ├─ per candidate: _build_tts_generation_job_kwargs_from_meta()  backend_server.py:30573
                 ├─ per candidate: _enqueue_tts_generation_job_result(**kwargs)  backend_server.py:30604
                 │    ├─ _ensure_tts_generation_workers_started()
                 │    ├─ _TTS_GENERATION_JOBS_LOCK: check cache_key in _TTS_GENERATION_JOBS
                 │    └─ _TTS_GENERATION_QUEUE.put(kwargs, timeout=...)
                 └─ _record_tts_admin_monitor_event(recovery_run)

  Worker thread loop (_tts_generation_worker_loop):
    └─ _TTS_GENERATION_QUEUE.get()  →  _run_tts_generation_job(**kwargs)
         └─ [same chain as Path A inner section]
```

**Note:** Recovery dispatches to the in-process thread pool, not synchronously. The actor returns quickly; actual generation happens in daemon threads.

---

### Path C: On-Demand Generation (HTTP)

```
POST /api/webapp/tts/generate
  └─ webapp_tts_generate()                          backend_server.py:30840
       ├─ _read_webapp_tts_request_payload()        (validates input, extracts cache_key/text/lang/voice/speed)
       ├─ get_tts_object_meta(cache_key)            database.py:12376
       │    → If ready: return URL immediately
       │    → If pending: return pending status + retry_after
       │    → If failed: fall through to re-enqueue
       ├─ create_tts_object_pending() OR requeue_tts_object_pending()  database.py:12380/12399
       ├─ _enqueue_tts_generation_job_result(**kwargs)                 backend_server.py:30604
       │    ├─ _ensure_tts_generation_workers_started()
       │    ├─ _TTS_GENERATION_JOBS check (dedup)
       │    └─ _TTS_GENERATION_QUEUE.put(kwargs)
       └─ return {"status": "pending", "retry_after_ms": TTS_URL_PENDING_RETRY_MS}

GET /api/webapp/tts/url  (polling)
  └─ webapp_tts_url()                               backend_server.py:30747
       ├─ _increment_tts_url_poll_attempt(cache_key)
       ├─ get_tts_object_meta(cache_key, touch_hit=True)
       └─ return status (ready/pending/failed) + URL if ready

Worker thread (daemon, started by _ensure_tts_generation_workers_started):
  └─ _run_tts_generation_job(...)  [same chain as Path A inner section]
```

---

## 4. EXTERNAL SIDE EFFECTS

### 4.1 Database Writes

| Function | File:Line | Table | Idempotent? | Purpose |
|----------|-----------|-------|-------------|---------|
| `create_tts_object_pending()` | database.py:12380 | `bt_3_tts_object_cache` | Yes (INSERT OR IGNORE / upsert) | Create pending record |
| `requeue_tts_object_pending()` | database.py:12399 | `bt_3_tts_object_cache` | Yes | Reset failed record to pending |
| `mark_tts_object_ready()` | database.py:12535 | `bt_3_tts_object_cache` | Yes (last-write wins) | Set status=ready, store URL |
| `mark_tts_object_failed()` | database.py:12558 | `bt_3_tts_object_cache` | Yes | Set status=failed, store error |
| `record_tts_admin_monitor_event()` | database.py (imported as `persist_tts_admin_monitor_event` at backend_server.py:320) | `bt_3_tts_admin_monitor_events` | No (appends) | Observability |
| `_billing_log_event_safe()` | backend_server.py:8681 | `bt_3_billing_events` | No (append with idempotency_seed) | Cost tracking |
| `set_provider_budget_block_state()` | database.py:18743 | budget table | Yes | Persist budget block |
| `has_admin_scheduler_run()` | database.py (implied) | scheduler run guard table | Read | Guard against duplicate sends |
| `mark_admin_scheduler_run()` | database.py (implied) | scheduler run guard table | Yes | Record run for guard |

### 4.2 Database Reads

| Function | File:Line | Table(s) | Purpose |
|----------|-----------|---------|---------|
| `get_tts_object_meta()` | database.py:12376 | `bt_3_tts_object_cache` | Check generation status |
| `list_stale_pending_tts_objects()` | database.py | `bt_3_tts_object_cache` | Recovery candidates |
| `_list_tts_prewarm_active_user_ids()` | backend_server.py:13469 | `bt_3_webapp_dictionary_queries`, `bt_3_card_review_log`, `bt_3_billing_events` | Active user selection |
| `_get_tts_prewarm_user_activity_map()` | backend_server.py:13511 | card_srs_state, dictionary_queries, review_log, billing_events | Activity scoring |
| `_list_predicted_tts_candidates_for_user()` | backend_server.py:13630 | `bt_3_card_srs_state`, `bt_3_webapp_dictionary_queries` | FSRS-based prediction |
| `get_google_tts_monthly_budget_status()` | database.py:18669 | budget table | Budget enforcement |

### 4.3 R2 Storage Operations

| Function | File:Line | Type | Cost Class | Idempotent? |
|----------|-----------|------|-----------|-------------|
| `r2_exists(object_key)` | r2_storage.py:82 | HEAD | Class B | Yes |
| `r2_put_bytes(object_key, bytes)` | r2_storage.py:102 | PUT | Class A | Yes (overwrites) |
| `r2_public_url(object_key)` | r2_storage.py:75 | None (local) | Free | Yes |

### 4.4 Google TTS API

| Function | File:Line | Side Effect | Idempotent? |
|----------|-----------|------------|-------------|
| `texttospeech.TextToSpeechClient().synthesize_speech()` | backend_server.py:~16470 | HTTP to Google Cloud TTS API, consumes character quota | No (quota consumed each call) |
| `_enforce_google_tts_monthly_budget()` | backend_server.py:16308 | DB read + optional DB write (block state) | Read is yes; write is yes |
| `prepare_google_creds_for_tts()` | backend/utils.py:12 | Writes credentials file to filesystem, sets `GOOGLE_APPLICATION_CREDENTIALS` env var | Side-effectful |

### 4.5 Telegram (Admin Notifications)

| Function | File:Line | Purpose |
|----------|-----------|---------|
| `_send_private_message()` (via `_send_tts_admin_monitor_digest`) | backend_server.py:11535 | Send admin digest/alerts |
| `_maybe_send_tts_admin_failure_alert()` | backend_server.py:12258 | Burst/failure threshold alerts |
| `_send_tts_prewarm_quota_control_message()` | backend_server.py:12004 | Daily quota control Telegram message |

### 4.6 In-Process State Mutations (no I/O)

| Operation | Symbol | Location |
|-----------|--------|----------|
| Append event | `_TTS_ADMIN_MONITOR_EVENTS` | backend_server.py:11810 |
| Add to dedup set | `_TTS_GENERATION_JOBS` | backend_server.py:30612 |
| Remove from dedup set | `_TTS_GENERATION_JOBS` | backend_server.py:30475 |
| Increment poll counter | `_TTS_URL_POLL_ATTEMPTS` | backend_server.py:2839 |
| Clear poll counter | `_TTS_URL_POLL_ATTEMPTS` | backend_server.py:2851 |

---

## 5. FLASK / APP CONTEXT DEPENDENCY CHECK

### Functions That Directly Use Flask Context

| Function | Line | Usage | In Generation Path? |
|----------|------|-------|---------------------|
| `webapp_tts_generate()` | 30840 | `request.get_json()`, `request.headers` | Yes (HTTP entry point only) |
| `webapp_tts_url()` | 30747 | `request.get_json()`, `request.args` | Yes (HTTP entry point only) |
| `webapp_tts()` | 31080 | `request.get_json()` | Yes (legacy HTTP entry point only) |
| `prewarm_tts_now()` | 39331 | `request.get_json()`, `request.headers` | Admin entry point only |
| `_extract_observability_request_id()` | 2358 | `has_request_context()` check, then `request.headers` | Called from routes; guards with `has_request_context()` |

### Functions That Do NOT Use Flask Context

**`_run_tts_generation_job()`** — no Flask context. All parameters passed explicitly as kwargs.  
**`_synthesize_mp3()`** — no Flask context.  
**`_dispatch_tts_prewarm()`** — no Flask context.  
**`_recover_stale_tts_generation_jobs()`** — no Flask context.  
**`_enqueue_tts_generation_job_result()`** — no Flask context.  
**`_ensure_tts_generation_workers_started()`** — no Flask context.  
**`_tts_generation_worker_loop()`** — runs in daemon thread, no Flask context.

### Conclusion

**Flask context is only required in HTTP route handlers** (entry points). The core generation engine — `_run_tts_generation_job`, `_synthesize_mp3`, `_dispatch_tts_prewarm`, `_recover_stale_tts_generation_jobs`, the queue/thread subsystem — has **zero Flask dependency**. Safe to extract.

---

## 6. EXTRACTION BOUNDARY PROPOSAL: `backend/tts_generation.py`

### 6.1 Functions That Can Move First (no blockers)

These have no inbound callers from other `backend_server.py` subsystems and depend only on `database.py`, `r2_storage.py`, `utils.py`, stdlib, and Google TTS SDK:

| Function | Line | Dependencies (all movable) |
|----------|------|---------------------------|
| `_synthesize_mp3()` | 16361 | `utils.prepare_google_creds_for_tts`, `google.cloud.texttospeech`, `pydub.AudioSegment`, `_enforce_google_tts_monthly_budget` |
| `_enforce_google_tts_monthly_budget()` | 16308 | `database.get_google_tts_monthly_budget_status`, `database.set_provider_budget_block_state`, `_notify_google_tts_budget_thresholds` |
| `GoogleTTSBudgetBlockedError` | 16129 | stdlib only |
| `_normalize_tts_language_code()` | 13193 | Pure (no I/O) |
| `_normalize_tts_voice_name()` | 13199 | Pure (no I/O) |
| `_tts_object_key()` | 13223 | Pure (no I/O) |
| `_build_tts_generation_job_kwargs_from_meta()` | 30573 | Calls `_normalize_*` + `_tts_object_key` (all pure) |

### 6.2 Functions That Can Move in the Second Slice

These require the first slice to be complete and stable:

| Function | Line | Requires |
|----------|------|---------|
| `_run_tts_generation_job()` | 30273 | Slice 1 functions + `_billing_log_event_safe` + `_log_flow_observation` (decide: move or import) |
| `_get_tts_generation_queue()` | 30517 | `_TTS_GENERATION_QUEUE` + `_TTS_GENERATION_QUEUE_LOCK` globals |
| `_ensure_tts_generation_workers_started()` | 30547 | Thread pool globals |
| `_tts_generation_worker_loop()` | 30533 | `_run_tts_generation_job` + queue globals |
| `_enqueue_tts_generation_job_result()` | 30604 | Queue globals + dedup set |
| `_enqueue_tts_generation_job()` | 30643 | Thin wrapper |
| `_tts_generation_queue_size()` | 30525 | Queue global |

### 6.3 Functions That Must Stay in `backend_server.py` Temporarily

These have too many inbound callers across other subsystems to move safely in early slices:

| Function | Reason |
|----------|--------|
| `_billing_log_event_safe()` | Used by 50+ callers across backend_server.py |
| `_log_flow_observation()` | Used by 100+ callers across backend_server.py |
| `_get_user_language_pair()` | Used in many non-TTS paths |
| `_is_webapp_user_allowed()` | Used in many non-TTS paths |
| `_send_private_message()` | Used across all messaging subsystems |
| `_record_tts_admin_monitor_event()` | Reads `_TTS_ADMIN_MONITOR_EVENTS` deque |

### 6.4 Globals That Must Be Eliminated Before Full Extraction

| Global | Why Blocking | Elimination Path |
|--------|-------------|-----------------|
| `_TTS_GENERATION_QUEUE` | In-process queue — cannot survive across processes/replicas | Replace with Redis or Dramatiq queue in a future step |
| `_TTS_GENERATION_JOBS` | In-process dedup set — no cross-process dedup | Replace with Redis SET or DB flag |
| `_TTS_GENERATION_WORKER_THREADS` | Daemon threads in `BACKGROUND_JOBS` process | OK as-is if queue is single-process; becomes dead code if queue moves to Redis |
| `_TTS_PREWARM_LOCK` | Single-process mutex | OK as-is for now (prewarm runs in BACKGROUND_JOBS); blocks if multiple replicas |
| `_TTS_ADMIN_MONITOR_EVENTS` | In-process deque for admin dashboard | Replace with DB-only reads (already dual-persisted) |
| `_TTS_ADMIN_ALERT_LAST_SENT` | Alert cooldown not shared | Replace with DB or Redis TTL key |

### 6.5 Minimum First Safe Extraction Slice

Extract these into `backend/tts_generation.py` with **zero behavior change**:

```
GoogleTTSBudgetBlockedError          (class, no deps)
_normalize_tts_language_code()       (pure)
_normalize_tts_voice_name()          (pure)
_tts_object_key()                    (pure)
_enforce_google_tts_monthly_budget() (DB read/write only, no backend_server deps)
_synthesize_mp3()                    (calls _enforce_google_tts_monthly_budget + Google TTS)
_build_tts_generation_job_kwargs_from_meta()  (calls pure helpers)
```

**These 7 items have zero inbound callers from non-TTS code in backend_server.py.**  
All their dependencies are in `database.py`, `utils.py`, `r2_storage.py`, or stdlib.  
`_notify_google_tts_budget_thresholds()` must come with `_enforce_google_tts_monthly_budget` — audit it before the slice.

After Slice 1 is live and stable, Slice 2 (the thread pool + `_run_tts_generation_job`) becomes a straightforward move.

---

## 7. KEY RISKS AND BLOCKERS

### Risk 1: Synchronous generation in prewarm (CRITICAL)
`_dispatch_tts_prewarm()` calls `_run_tts_generation_job()` synchronously at line 14010.  
A 50-user prewarm with 15 items each = 750 Google TTS calls in a single blocking scheduler actor message.  
This is the biggest scalability bottleneck in the system today.  
**Fix:** Change prewarm to enqueue to `_TTS_GENERATION_QUEUE` (async dispatch) rather than calling inline.  
This requires `_TTS_GENERATION_QUEUE` to live in the same process as the prewarm actor — already true in `BACKGROUND_JOBS`.

### Risk 2: In-process queue blocks horizontal scaling
`_TTS_GENERATION_QUEUE` and `_TTS_GENERATION_JOBS` are process-local.  
Running 2 replicas of `BACKGROUND_JOBS` would cause duplicate generation and no load distribution.  
**Fix:** Redis-backed queue (future step).

### Risk 3: `prepare_google_creds_for_tts()` writes to filesystem + mutates env var
`_synthesize_mp3()` calls `prepare_google_creds_for_tts()` which writes the Google credentials JSON to disk and sets `GOOGLE_APPLICATION_CREDENTIALS`.  
This must be audited before extraction to confirm it is thread-safe (reentrant writes to the same path).

### Risk 4: `_TTS_ADMIN_MONITOR_EVENTS` read by HTTP admin endpoints
The deque is read at `/api/admin/tts-monitor` (in `BACKEND_WEB`) and written in `BACKGROUND_JOBS`.  
After extraction, admin monitor in `BACKEND_WEB` will always see an empty deque.  
**Current state:** Events are dual-persisted to DB. Switching admin dashboard to read from DB only eliminates this risk completely.

---

## 8. FILES INVOLVED IN FULL EXTRACTION

| File | Role |
|------|------|
| `backend/backend_server.py` | Source of all TTS subsystem code today |
| `backend/database.py` | TTS DB functions (lines ~12376–12600, ~18669, ~18743) |
| `backend/r2_storage.py` | `r2_exists`, `r2_put_bytes`, `r2_public_url` (lines 75–120) |
| `backend/utils.py` | `prepare_google_creds_for_tts()` (line 12) |
| `backend/tts_scheduler.py` | Already-created seam module (3 wrappers) |
| `backend/tts_generation.py` | **Does not exist yet** — target for extraction |
| `backend/background_jobs.py` | Actor registrations for TTS jobs |

---

## 9. SLICE 1 EXTRACTION RESULTS

**Completed** — `backend/tts_generation.py` created; `backend_server.py` updated to import from it.

### Moved (4 safe items)

| Symbol | Why safe |
|--------|----------|
| `_TTS_VOICES` dict | Pure constant, no deps |
| `_TTS_LANG_CODES` dict | Pure constant, no deps |
| `TTS_OBJECT_PREFIX` | `os.getenv` read, no other imports |
| `_normalize_short_lang_code` | Pure string transform, no deps |
| `_sanitize_object_segment` | Pure string transform, no deps |
| `_normalize_tts_language_code` | Calls only the two helpers above |
| `_normalize_tts_voice_name` | Calls only `_TTS_VOICES` |
| `_tts_object_key` | Calls `_sanitize_object_segment` + `TTS_OBJECT_PREFIX` |
| `GoogleTTSBudgetBlockedError` | Pure exception class |

`backend_server.py` now imports all of the above from `backend.tts_generation`.  No circular dependency: `tts_generation` imports only `os` and `re`.

### NOT moved (3 blocked items)

| Symbol | Blocker |
|--------|---------|
| `_enforce_google_tts_monthly_budget` (line 16308) | Calls `_notify_google_tts_budget_thresholds` → `_send_private_message`, which has 50+ other callers across `backend_server.py` and cannot be moved without a larger refactor |
| `_synthesize_mp3` (line 16361) | Depends on `_enforce_google_tts_monthly_budget` (blocked above) |
| `_build_tts_generation_job_kwargs_from_meta` (line 30573) | Calls `_build_observability_correlation_id` (line 2380) and `_to_epoch_ms` (line 2410) — generic helpers with 50–100+ callers across unrelated subsystems in `backend_server.py`; extracting them as part of TTS would be wrong scope |

### What stays in backend_server.py (intentionally)

- `_tts_object_cache_key` — uses `_TTS_CACHE_HMAC_SECRET` (process-wide secret read at startup)
- All TTS in-process state globals (`_TTS_GENERATION_QUEUE`, `_TTS_GENERATION_JOBS`, etc.)
- `_run_tts_prewarm_scheduler_job`, `_run_tts_generation_recovery_scheduler_job`, `_run_tts_prewarm_quota_control_scheduler_job` — exposed via `tts_scheduler.py` seam

### Next slice options (superseded by Slice 2 blocker analysis — see §10)

---

## 10. SLICE 2 BLOCKER ANALYSIS

### Remaining blocked functions

| Function | Line | Blocker helper | Blocker location |
|----------|------|----------------|-----------------|
| `_build_tts_generation_job_kwargs_from_meta` | 30538 | `_build_observability_correlation_id` + `_to_epoch_ms` | backend_server.py:2391, 2421 |
| `_enforce_google_tts_monthly_budget` | 16273 | `_notify_google_tts_budget_thresholds` → `_send_private_message` | backend_server.py:11540 |
| `_synthesize_mp3` | 16326 | `_enforce_google_tts_monthly_budget` (above) | — |

---

### Blocker 1: `_build_observability_correlation_id` (backend_server.py:2391)

- **What it does:** Builds a sanitized correlation ID. Checks Flask request context (`has_request_context()`), reads request headers/args if in-request; falls back to `f"{prefix}_{uuid4().hex}"` when not.
- **Callers:** 50+ across ALL subsystems (translation, analytics, billing, YouTube, reader, TTS routes, TTS recovery).
- **Callees:** `_sanitize_observability_id` (pure, line 2360), Flask `has_request_context`, `request.headers`, `request.args`.
- **Side effects:** None. Pure except for UUID generation.
- **Depends on backend_server globals:** `has_request_context`, `request`, `g` — Flask thread-locals only. No module-level state.
- **Broadly reused:** Yes — 50+ callers, spans every subsystem.

**TTS-specific usage (line 30561):**
```python
"correlation_id": _build_observability_correlation_id(
    fallback_seed=f"recover:{cache_key[:16]}",
    prefix="tts",
)
```
Called from `_build_tts_generation_job_kwargs_from_meta`, which is called ONLY from the background recovery scheduler — never from a Flask route. Therefore `has_request_context()` is **always False** here, and the function always takes the fallback path: `f"tts_{sanitize(fallback_seed)}"`. The Flask-context branches are dead code for this call site.

---

### Blocker 2: `_to_epoch_ms` (backend_server.py:2421)

- **What it does:** `int(time.time() * 1000)`. Literally a one-liner.
- **Callers:** 22 across all subsystems (translation execution, TTS generation, analytics, etc.).
- **Side effects:** None.
- **Depends on backend_server globals:** None.
- **Broadly reused:** Yes — 22 callers.

**TTS-specific usage (line 30565):** `"enqueue_ts_ms": _to_epoch_ms()` — records job enqueue timestamp. Trivial.

---

### Blocker 3: `_send_private_message` (backend_server.py:11540)

- **What it does:** Posts a message to Telegram via Bot API (`sendMessage`), records `message_id` in DB via `record_telegram_system_message`.
- **Callers:** 17 call sites in backend_server.py spanning admin notifications, user reminders, daily plans, analytics reports, TTS admin alerts.
- **Callees:** `requests.post` (Telegram API), `record_telegram_system_message` (DB side effect).
- **Side effects:** HEAVY — external HTTP call + DB write.
- **Depends on backend_server globals:** `TELEGRAM_Deutsch_BOT_TOKEN` (module-level constant, line ~777).
- **Broadly reused:** Yes — 17 callers across unrelated subsystems (not TTS-only).

**TTS-specific usage:** `_notify_google_tts_budget_thresholds` (line 16210) calls it to alert admins at 50/75/90% budget thresholds. This is a side effect, not core synthesis logic. `_notify_google_tts_budget_thresholds` itself is called only by `_enforce_google_tts_monthly_budget`.

---

### PATH A: Unblock `_build_tts_generation_job_kwargs_from_meta`

**Minimum approach:** Do NOT extract `_build_observability_correlation_id` or `_to_epoch_ms` as shared modules (blast radius too wide). Instead:

1. Add `_to_epoch_ms` as a one-liner duplicate to `tts_generation.py` (2 lines, no callers in backend_server change).
2. Add `_tts_correlation_id(fallback_seed, prefix)` to `tts_generation.py` — a pure, no-Flask subset of `_build_observability_correlation_id`, valid for background job context (where `has_request_context()` is always False). Uses only `re`, `uuid4`. Behaviorally equivalent at this specific call site.
3. Move `_build_tts_generation_job_kwargs_from_meta` to `tts_generation.py`; update import in backend_server.py.

**Files affected:** `tts_generation.py` (+3 symbols), `backend_server.py` (1 def removed, 2 imports added, 0 other callers touched).

**Blast radius:** Minimal — zero existing callers affected.

**Risk:** Low. `_to_epoch_ms` is trivially safe. `_tts_correlation_id` correctness is provable: the call site is only ever reached in background context, so the Flask branches being absent is not a behavioral difference.

**Value:** Moves the job-kwargs builder to `tts_generation.py`. Does NOT unblock `_synthesize_mp3`.

---

### PATH B: Unblock `_enforce_google_tts_monthly_budget` + `_synthesize_mp3`

**Minimum approach (B2 — extract `_send_private_message`):**

1. Create `backend/telegram_notify.py` with `_send_private_message` (change `TELEGRAM_Deutsch_BOT_TOKEN` to `os.getenv(...)`).
2. Rewire all 17 call sites in backend_server.py to import from new module.
3. Move `_notify_google_tts_budget_thresholds` + `_enforce_google_tts_monthly_budget` + `_synthesize_mp3` to `tts_generation.py`.

**Files affected:** 1 new module, backend_server.py (17 call sites + 3 defs removed), tts_generation.py (+3 defs).

**Blast radius:** Substantial — 17 call sites across admin, user, analytics, TTS paths.

**Risk:** Higher. Any missed call site or import error breaks message delivery across the whole bot, not just TTS.

**Value:** High — moves `_synthesize_mp3`, the actual Google TTS synthesis engine.

**Option B1 (TTS-only notifier with callable injection):** Inject `notify_fn: Callable` into `_enforce_google_tts_monthly_budget`. This changes the function signature and all call sites — more churn than B2 with no isolation benefit. Rejected.

---

### Recommendation: PATH A next

PATH A has zero blast radius beyond `tts_generation.py` and `_build_tts_generation_job_kwargs_from_meta`. It completes the job-kwargs builder extraction cleanly.

PATH B is deferred: extracting `_send_private_message` to `telegram_notify.py` should be its own dedicated step, scoped to the 17 notification call sites, not bundled with TTS extraction. Once that step is complete, PATH B becomes equally straightforward.

**Chosen next step:** Add `_to_epoch_ms` + `_tts_correlation_id` to `tts_generation.py`, move `_build_tts_generation_job_kwargs_from_meta`.

**PATH B deferred because:** 17 call sites for `_send_private_message` span unrelated subsystems. Touching them during a TTS extraction increases blast radius unnecessarily. The right time is a dedicated "extract Telegram notification primitives" step.

---

## 11. SLICE 2 RESULTS

**Completed** — `_build_tts_generation_job_kwargs_from_meta` moved to `backend/tts_generation.py`.

### What was moved

| Symbol | From | To |
|--------|------|----|
| `TTS_WEBAPP_DEFAULT_SPEED` | backend_server.py:765 | tts_generation.py |
| `_normalize_utterance_text` | backend_server.py:12996 | tts_generation.py |
| `_build_tts_generation_job_kwargs_from_meta` | backend_server.py:30538 | tts_generation.py |

`backend_server.py` imports all three back; no callers changed.

### Tiny helpers introduced

**`_to_epoch_ms()`** — `int(time.time() * 1000)`. Added to `tts_generation.py` as a local duplicate. The original in `backend_server.py` (line 2421) was intentionally left in place because it has 22 other callers across unrelated subsystems. Extracting it to a shared module for TTS would be wrong scope.

**`_tts_recovery_correlation_id(cache_key_prefix)`** — No-Flask subset of `_build_observability_correlation_id`. Explicitly scoped to TTS background jobs. Correctness: `_build_tts_generation_job_kwargs_from_meta` is only called from `_recover_stale_tts_generation_jobs` (the recovery scheduler), never from a Flask route. Therefore `has_request_context()` is always `False` at that call site — the Flask-header branches of the generic helper are dead code there. The local helper covers only the fallback path: `sanitize(seed)` → `f"tts_{safe_seed}"`. Produces identical output to the generic helper for this context.

### Behavior parity: `correlation_id` field

| Before | After |
|--------|-------|
| `_build_observability_correlation_id(fallback_seed="recover:{key[:16]}", prefix="tts")` in background context | `_tts_recovery_correlation_id("recover:{key[:16]}")` |
| `has_request_context()` → False → fallback path → `f"tts_{sanitize(seed)}"` | `f"tts_{sanitize(seed)}"` |
| Output: `"tts_recover-<16 chars>"` | Output: `"tts_recover-<16 chars>"` — identical |

Sanitization regex is identical: `[^a-zA-Z0-9._:-]+` → `-`.

### Remaining blocker

`_enforce_google_tts_monthly_budget` and `_synthesize_mp3` remain in `backend_server.py`.  
Blocker: `_notify_google_tts_budget_thresholds` → `_send_private_message` (16 callers, Telegram API side effect, `TELEGRAM_Deutsch_BOT_TOKEN` module constant).  
See §12 for full messaging dependency audit and chosen next step.

---

## 12. MESSAGING DEPENDENCY AUDIT (_send_private_message)

### Function inventory

**`_send_private_message` (backend_server.py:11543)**
- Sends Telegram `sendMessage` API call; records `message_id` in DB via `record_telegram_system_message`.
- Params: `user_id, text, reply_markup?, disable_web_page_preview?, parse_mode?, message_type?`
- Module dep: `TELEGRAM_Deutsch_BOT_TOKEN = os.getenv(...)` at line 647 — trivially portable.
- DB dep: `record_telegram_system_message` (database.py) — already imported pattern.
- Side effects: Telegram HTTP POST + DB INSERT — heavy.
- 16 call sites in backend_server.py.

**`_send_private_message_chunks` (backend_server.py:11740)**
- Splits long text at line boundaries, calls `_send_private_message` for each chunk.
- Pure chunking logic + dispatch. No additional deps.
- 3 call sites: grammar explanation (user-facing), semantic audit digest (admin), today plan report (admin).

**`_send_tts_admin_message` (backend_server.py:11757)**
- TTS-specific wrapper: iterates `get_admin_telegram_ids()`, calls `_send_private_message` for each.
- Already scoped to TTS. If `_send_private_message` moves, this can remain in backend_server and import it.

---

### Caller shape classification (all 16 call sites)

| Line | Context | Shape |
|------|---------|-------|
| 11754 | `_send_private_message_chunks` | chunked dispatch |
| 11764 | `_send_tts_admin_message` | **admin alert** (TTS events) |
| 12035 | TTS prewarm quota control | **admin alert** (quota/budget) |
| 15349 | Translation focus pool report (text fallback) | **admin analytics report** |
| 15390 | Translation focus pool report (photo fallback) | **admin analytics report** |
| 16177 | `_notify_provider_budget_thresholds` | **admin alert** (generic provider budget) |
| 16251 | `_notify_google_tts_budget_thresholds` | **admin alert** (TTS budget) |
| 30101 | Feel-word delivery | **user-facing** (HTML, feel_word type) |
| 35096 | Webapp lesson result submit | **user-facing** (plain text) |
| 35517 | Private analytics dispatch | **user-facing** (plain text) |
| 35805 | Weekly plan period reminder | **user-facing** (reply_markup) |
| 36272 | Weekly group badges | **user-facing** (reply_markup) |
| 36394 | Today evening reminder | **user-facing** (reply_markup) |
| 36410 | Today evening celebration | **user-facing** (reply_markup) |
| 38219 | `_send_today_plan_private_message` | **user-facing** (reply_markup) |
| 38270 | Today plan fallback dispatch | **user-facing** (plain text) |

**Split: 6 admin-alert/report + 9 user-facing + 1 chunked wrapper.**

### Is `_send_private_message` homogeneous enough to extract?

**Yes.** All 16 callers use an identical interface: `(user_id_or_chat_id, text, optional_params)`. The function is a thin HTTP wrapper — there is no conditional logic, no caller-specific branching inside the function. Every caller uses it the same way regardless of whether it is admin-alerting or user-facing.

The TTS budget alert path (line 16251) uses it in the simplest form: plain text, `disable_web_page_preview=True`, no markup, no parse_mode. Same shape as 5 other admin-alert callers.

### TTS-specific usage: is the messaging core logic or alerting only?

- `_notify_google_tts_budget_thresholds` → `_send_private_message`: **alerting only**. It sends a text notification to admins when a budget threshold is crossed. This is a side effect of `_enforce_google_tts_monthly_budget`, not its core logic. The core logic (checking budget, raising `GoogleTTSBudgetBlockedError`) does not depend on whether the notification succeeded.
- `_synthesize_mp3` depends on `_enforce_google_tts_monthly_budget` which calls `_notify_google_tts_budget_thresholds`. Same dependency chain.

### Extraction options

**Option A — Extract full `_send_private_message` + `_send_private_message_chunks` to `backend/telegram_notify.py`**
- Files: new module + 16 call sites in backend_server (import change only) + tts_generation.py can then import it
- Blast radius: all 16 send paths touched (import swap, function logic unchanged)
- Risk: LOW — function logic is identical, only the import location changes; import failure would be caught immediately at startup
- Value: directly unblocks `_notify_google_tts_budget_thresholds` → `_enforce_google_tts_monthly_budget` → `_synthesize_mp3`
- Downside: 16 call site edits in a single commit — requires care

**Option B — Extract a narrow TTS-only admin notifier (no full extraction)**
- Define e.g. `_send_tts_budget_alert(admin_id, text)` in `tts_generation.py` that calls Telegram API directly
- Files: tts_generation.py only
- Blast radius: minimal
- Risk: duplicates HTTP transport logic; two implementations of Telegram messaging diverge over time
- Value: unblocks TTS budget path only — `_synthesize_mp3` still unblocked
- Downside: creates a second HTTP transport for Telegram that will need consolidation later. Fake decoupling.

**Option C — Inject a `notify_fn: Callable` into `_enforce_google_tts_monthly_budget`**
- Caller passes the function at call time; `tts_generation.py` has no Telegram dep
- Files: tts_generation.py + backend_server.py (call site change)
- Blast radius: only the 2–3 call sites of `_enforce_google_tts_monthly_budget`
- Risk: changes function interface; complicates testing; not idiomatic
- Value: technically unblocks the move but creates an awkward interface
- Downside: interface change for every future caller; non-obvious parameter

**Option D — Do not extract messaging yet**
- Files: none
- Blast radius: 0
- Risk: 0
- Value: 0 — `_enforce_google_tts_monthly_budget` and `_synthesize_mp3` stay blocked indefinitely
- Downside: real TTS engine extraction is permanently stalled

### Recommendation: Option A

Extract `_send_private_message` and `_send_private_message_chunks` to `backend/telegram_notify.py`.

- The function IS homogeneous — it is a thin transport primitive, not a domain-specific helper.
- 16 call site edits are all identical: replace implicit name resolution with an import. No logic changes.
- Module dep (`TELEGRAM_Deutsch_BOT_TOKEN`) becomes `os.getenv("TELEGRAM_Deutsch_BOT_TOKEN")` inside the function — one-line change.
- `_send_tts_admin_message` (already TTS-scoped) can stay in backend_server.py and import the primitive.
- After this step, `_notify_google_tts_budget_thresholds` → `_enforce_google_tts_monthly_budget` → `_synthesize_mp3` can all move to `tts_generation.py` cleanly.

Broader messaging extraction (group helpers, photo helpers, etc.) is explicitly NOT part of this step — only the two private-message functions move.

---

## 13. SLICE 3 — TELEGRAM TRANSPORT EXTRACTION RESULTS

**Completed** — `backend/telegram_notify.py` created. `backend_server.py` updated.

### What was moved

| Symbol | From | To |
|--------|------|----|
| `_send_private_message` | backend_server.py:11543 | telegram_notify.py |
| `_send_private_message_chunks` | backend_server.py:11740 | telegram_notify.py |

### Token access change

`TELEGRAM_Deutsch_BOT_TOKEN` (module-level constant, backend_server.py:647) replaced with `os.getenv("TELEGRAM_Deutsch_BOT_TOKEN")` read lazily inside `_send_private_message`. Behavior when env var is missing: `token` is `None`, URL becomes `https://api.telegram.org/botNone/sendMessage`, Telegram API returns 401 → `RuntimeError` raised — identical to the previous behavior (the module-level constant was also `None` if env var was absent).

### Callers rewired

**0 call sites modified.** Because the function names are unchanged and both names are imported into `backend_server`'s module namespace at line 520–522, all 16 `_send_private_message` call sites and 3 `_send_private_message_chunks` call sites resolve correctly through the import without any per-site edits.

`_send_tts_admin_message` (line ~11719) remains in `backend_server.py` and calls the imported `_send_private_message` — unchanged.

### Why this is a narrow transport extraction

- `backend/telegram_notify.py` contains exactly 2 functions.
- Group, photo, media helpers stay in `backend_server.py`.
- No behavioral change — only the definition location moved.
- `backend_server.py` re-exports both names via import, so nothing in the call graph changed.

### TTS functions now unblocked for Slice 4

`_notify_google_tts_budget_thresholds` and `_enforce_google_tts_monthly_budget` can now import `_send_private_message` from `backend.telegram_notify` instead of `backend_server`. `_synthesize_mp3` can follow as soon as `_enforce_google_tts_monthly_budget` moves. All three can now land in `backend/tts_generation.py` with no circular import.
