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

---

## 14. SLICE 4 — BUDGET ENFORCEMENT + SYNTHESIS EXTRACTION RESULTS

**Completed** — all three candidates moved to `backend/tts_generation.py`.

### What was moved

| Symbol | From line | To |
|--------|-----------|----|
| `_notify_google_tts_budget_thresholds` | backend_server.py:16165 | tts_generation.py |
| `_enforce_google_tts_monthly_budget` | backend_server.py:16228 | tts_generation.py |
| `_synthesize_mp3` | backend_server.py:16281 | tts_generation.py |

### New imports added to tts_generation.py

| Import | Source | Purpose |
|--------|--------|---------|
| `import io` | stdlib | BytesIO for MP3 multi-chunk assembly |
| `import logging` | stdlib | budget alert warnings |
| `from pydub import AudioSegment` | pydub | multi-chunk MP3 concatenation |
| `from backend.database import get_admin_telegram_ids, get_google_tts_monthly_budget_status, mark_provider_budget_threshold_notified, set_provider_budget_block_state` | backend.database | budget status reads + writes |
| `from backend.telegram_notify import _send_private_message` | backend.telegram_notify | admin alert delivery |
| `from backend.utils import prepare_google_creds_for_tts` | backend.utils | Google credential file path |

No `backend_server` import. No circular dependency.

### Callers rewired

All 4 call sites of `_synthesize_mp3` in backend_server.py (lines 13316, 30053, 30904, 33229) resolve through the import at line 538. No per-site edit needed — name unchanged.

`_notify_google_tts_budget_thresholds` and `_enforce_google_tts_monthly_budget` were only called internally within the moved chain — zero external call sites rewired.

### Thread-safety: no regression

`_synthesize_mp3` mutates `os.environ["GOOGLE_APPLICATION_CREDENTIALS"]`. This is a process-global write and was already subject to a race condition under concurrent TTS worker threads in backend_server.py. Moving the function to `tts_generation.py` does not change this risk — same callers, same call sites, same process model.

### What remains in backend_server.py

- `_notify_provider_budget_thresholds` (generic multi-provider budget alerter) — not TTS-only, stays
- `_provider_budget_alert_thresholds` (reads BILLING_PROVIDER_ALERT_THRESHOLDS env) — generic, stays
- The full TTS worker/queue/thread architecture (`_run_tts_generation_job`, `_tts_generation_worker_loop`, globals) — deferred; these require in-process state coordination

### Next extraction target

The TTS worker and queue architecture (`_TTS_GENERATION_QUEUE`, `_TTS_GENERATION_JOBS`, `_tts_generation_worker_loop`, `_run_tts_generation_job`, prewarm). These are the remaining large block — they depend on in-process threading state and are the scalability bottleneck identified in the original audit.

---

## 15. ORCHESTRATION LAYER AUDIT

### Entry points

| Function | Line | Trigger | Execution model |
|----------|------|---------|----------------|
| `_dispatch_tts_prewarm` | 13701 | APScheduler / Dramatiq actor | **SYNCHRONOUS** — calls `_run_tts_generation_job` directly in scheduler thread |
| `_enqueue_tts_generation_job_result` | 30263 | HTTP request / recovery scheduler / dictionary save | Queue-based: puts kwargs into `_TTS_GENERATION_QUEUE` |
| `_enqueue_tts_generation_job` | 30302 | Thin wrapper over above | Same |
| `_tts_generation_worker_loop` | 30220 | Daemon thread started by `_ensure_tts_generation_workers_started` | Thread-based: blocks on `_TTS_GENERATION_QUEUE.get()` |
| `_run_tts_generation_job` | 29960 | Worker loop (async) or prewarm (sync) | Executes synthesis, R2 upload, DB state transitions |
| `_recover_stale_tts_generation_jobs` | 30306 | Recovery scheduler actor | Queue-based: calls `_enqueue_tts_generation_job_result` per stale record |
| `_run_tts_prewarm_scheduler_job` | 14164 | APScheduler (scheduler service) | Calls `_dispatch_tts_prewarm` |
| `_run_tts_generation_recovery_scheduler_job` | 30366 | APScheduler (scheduler service) | Calls `_recover_stale_tts_generation_jobs` |

### Process-local state — all scaling blockers

| Symbol | Line | Written by | Read by | Invariant maintained | Scaling blocker |
|--------|------|-----------|---------|---------------------|----------------|
| `_TTS_GENERATION_QUEUE` | 639 | `_get_tts_generation_queue` (lazy init) | `_tts_generation_worker_loop`, `_enqueue_tts_generation_job_result` | Bounded job queue | Per-process — jobs in queue are lost on restart; N processes each have their own queue |
| `_TTS_GENERATION_JOBS` | 956 | `_enqueue_tts_generation_job_result`, `_run_tts_generation_job` finally-block | `_enqueue_tts_generation_job_result` | Prevents duplicate in-flight jobs per cache_key | Per-process — no cross-process dedup; two instances enqueue same job |
| `_TTS_GENERATION_QUEUE_LOCK` | 638 | n/a | `_get_tts_generation_queue`, `_ensure_tts_generation_workers_started` | Queue init + thread list update atomicity | n/a (local lock only) |
| `_TTS_GENERATION_WORKER_THREADS` | 640 | `_ensure_tts_generation_workers_started` | Same | Tracks live threads to avoid over-spinning | Per-process list |
| `_TTS_PREWARM_LOCK` | 954 | `_dispatch_tts_prewarm` | Same (non-blocking acquire) | Prevents concurrent prewarm runs | Per-process only — two instances run prewarm concurrently |
| `_TTS_GENERATION_JOBS_LOCK` | 955 | n/a | `_enqueue_tts_generation_job_result`, `_run_tts_generation_job` finally | Guards `_TTS_GENERATION_JOBS` set | n/a (local lock only) |
| `_TTS_ADMIN_MONITOR_EVENTS` | 636 | `_record_tts_admin_monitor_event` | `_get_tts_admin_monitor_window` | In-memory ring buffer for admin failure alerting | Per-process — events from other processes are invisible; DB is secondary but not always consulted |
| `_TTS_URL_POLL_ATTEMPTS` | 627 | `_increment_tts_url_poll_attempt` | `_clear_tts_url_poll_attempt` | Tracks how many times a URL poll was attempted | Per-process — cleared on restart |

### Execution flows

**A — On-demand (webapp TTS request):**
```
HTTP POST /webapp/tts
→ _enqueue_tts_generation_job_result (queue insert + dedup check)
→ _TTS_GENERATION_QUEUE.put(kwargs)           ← async boundary
→ _tts_generation_worker_loop (daemon thread)
→ _run_tts_generation_job
→ r2_exists (R2 HEAD)
→ _synthesize_mp3 (Google TTS API)
→ r2_put_bytes (R2 PUT)
→ mark_tts_object_ready (DB)
→ _clear_tts_url_poll_attempt (in-memory)
```

**B — Recovery (stale pending records):**
```
APScheduler → run_tts_generation_recovery_scheduler_actor (Dramatiq)
→ _run_tts_generation_recovery_scheduler_job
→ _recover_stale_tts_generation_jobs
→ list_stale_pending_tts_objects (DB)
→ per record: _build_tts_generation_job_kwargs_from_meta → _enqueue_tts_generation_job_result
→ _TTS_GENERATION_QUEUE.put(kwargs)           ← async boundary (same worker pool)
→ _run_tts_generation_job (worker thread)
```

**C — Prewarm (SYNCHRONOUS bottleneck):**
```
APScheduler → run_tts_prewarm_scheduler_actor (Dramatiq)
→ _run_tts_prewarm_scheduler_job
→ _dispatch_tts_prewarm
→ _TTS_PREWARM_LOCK.acquire(blocking=False)
→ per candidate: _run_tts_generation_job ← DIRECT SYNCHRONOUS CALL, no queue
→ synthesis blocks scheduler/Dramatiq thread for full duration per candidate
```

### `_run_tts_generation_job` dependency audit

**Already in `tts_generation.py` (no blocker):**
- `_synthesize_mp3`, `GoogleTTSBudgetBlockedError` ✓
- `r2_exists`, `r2_put_bytes`, `r2_public_url` (r2_storage.py) ✓
- `mark_tts_object_ready`, `mark_tts_object_failed` (database.py) ✓

**Backend-server-only blockers:**

| Dep | Line | Why it blocks | Movability |
|-----|------|--------------|------------|
| `_elapsed_ms_since` | 2435 | `int((perf_counter()-start)*1000)` | TRIVIAL — pure one-liner, no state |
| `_log_flow_observation` | 2458 | JSON structured log to `logging.info` | TRIVIAL — pure, no Flask, no state |
| `_shorten_tts_admin_text` | 12101 | 6-line string truncation | TRIVIAL — pure, no state |
| `_sanitize_observability_id` | 2360 | regex string sanitizer | TRIVIAL — pure, no state |
| `_build_observability_correlation_id` | 2391 | Flask-aware, 50+ callers | MODERATE — can use `_tts_recovery_correlation_id` pattern if in background context |
| `_clear_tts_url_poll_attempt` | 2872 | Mutates `_TTS_URL_POLL_ATTEMPTS` (process-local dict, `_OBSERVABILITY_LOCK`) | HARD — process-local in-memory state |
| `_record_tts_admin_monitor_event` | 11753 | Writes to `_TTS_ADMIN_MONITOR_EVENTS` deque (process-local) + DB | HARD — deque is process-local |
| `_maybe_send_tts_admin_failure_alert` | 12223 | Reads `_TTS_ADMIN_MONITOR_EVENTS` deque | HARD — process-local state |
| `_get_user_language_pair` | 3715 | DB query for user language preferences, 50+ callers, many subsystems | HARD — cross-domain DB query |
| `_billing_log_event_safe` | 8702 | Billing DB write, complex subsystem | HARD — billing domain |
| `_TTS_GENERATION_JOBS_LOCK` + `_TTS_GENERATION_JOBS` | 955–956 | Process-local dedup set | HARD — fundamental queue state |

**Conclusion:** `_run_tts_generation_job` cannot move to `tts_generation.py` without also resolving 5 hard-category blockers. The trivial-category helpers (`_elapsed_ms_since`, `_log_flow_observation`, `_shorten_tts_admin_text`, `_sanitize_observability_id`) can be pre-moved but they alone don't unblock the function.

### Prewarm synchronous execution — why it blocks scaling

`_dispatch_tts_prewarm` (line 13936) calls `_run_tts_generation_job(...)` directly — bypassing `_TTS_GENERATION_QUEUE` and the worker pool entirely. The scheduler/Dramatiq thread blocks for the full synthesis duration (multiple Google TTS API calls + R2 uploads) per candidate, sequentially across all users.

The recovery path (`_recover_stale_tts_generation_jobs`) already uses `_enqueue_tts_generation_job_result` — it is queue-driven. Only prewarm is synchronous.

After `_run_tts_generation_job` completes, `_dispatch_tts_prewarm` immediately checks the DB to count whether status became `"ready"`:
```python
_run_tts_generation_job(...)          # line 13936 — blocks until synthesis done
latest = get_tts_object_meta(...)     # line 13953 — check result
if latest_status == "ready":
    generated += 1                    # counted only because we waited
```
This synchronous check is the exact coupling that makes switching to a queue-driven model require a counter-semantics change: `generated` would become `queued`, and the ready-check at line 13953 would always return `"pending"`.

### Process-local state is now the dominant scaling blocker

All remaining TTS orchestration — dedup (`_TTS_GENERATION_JOBS`), queue (`_TTS_GENERATION_QUEUE`), worker threads, prewarm lock, in-memory monitoring events — is invisible across process boundaries. With multiple `BACKGROUND_JOBS` instances:
- Two processes enqueue the same cache_key (dedup fails cross-process)
- Two processes run prewarm simultaneously (only within-process lock)
- Admin failure alerts aggregate only local events

The `_TTS_ADMIN_MONITOR_EVENTS` deque does have a DB fallback (`persist_tts_admin_monitor_event`), but `_maybe_send_tts_admin_failure_alert` reads the deque first and only falls back to DB if deque is empty — so cross-process events are invisible to the alerting path in practice.

### Recommended next step

**Make prewarm queue-driven**: change `_dispatch_tts_prewarm` to call `_enqueue_tts_generation_job_result` instead of `_run_tts_generation_job` directly, and update the counter semantics to count `"queued"` rather than waiting for `"ready"`.

This:
1. Eliminates synchronous synthesis blocking the scheduler/Dramatiq thread
2. Reuses the existing worker pool (same queue mechanism recovery already uses)
3. Is a contained change confined to `_dispatch_tts_prewarm` and its counter logic
4. Does NOT require Redis, new infrastructure, or moving any functions
5. Is the direct predecessor step to horizontal scaling — once prewarm is queue-driven, the next step is replacing `queue.Queue` with a distributed queue (Redis, etc.)

**This is a behavior change** (prewarm becomes async) that must be implemented deliberately, not as a side effect of cleanup. It cannot be made while following the "do not change runtime behavior" constraint of the current task — it is the next standalone step after this audit.

---

## 16. ORCHESTRATION RE-VALIDATION (2026-04-20)

This section re-validates the remaining TTS orchestration layer against the current codebase after the helper extractions and scheduler-wrapper seam work. It is intentionally limited to the remaining process-local execution model.

### Current orchestration entry points

| Function | File | Line | Caller(s) | Execution style |
|----------|------|------|-----------|-----------------|
| `run_tts_prewarm_scheduler_actor()` | `backend/background_jobs.py` | 1450 | Dramatiq `scheduler_jobs` queue | Scheduler actor |
| `run_tts_prewarm_scheduler_job()` | `backend/tts_scheduler.py` | 11 | `run_tts_prewarm_scheduler_actor()` | Scheduler wrapper |
| `_run_tts_prewarm_scheduler_job()` | `backend/backend_server.py` | 14164 | `backend.tts_scheduler.run_tts_prewarm_scheduler_job`, APScheduler registration in `backend_server.py:38676` | Scheduler wrapper |
| `_dispatch_tts_prewarm(force, tz_name)` | `backend/backend_server.py` | 13701 | `_run_tts_prewarm_scheduler_job()`, admin route `prewarm_tts_now()` | **Synchronous inline execution** |
| `prewarm_tts_now()` | `backend/backend_server.py` | 38991 | `POST /api/admin/prewarm-tts` | HTTP-triggered synchronous prewarm |
| `webapp_tts_generate()` | `backend/backend_server.py` | 30500 | `POST /api/webapp/tts/generate` | HTTP-triggered queue enqueue |
| `_enqueue_tts_generation_job_result(**kwargs)` | `backend/backend_server.py` | 30263 | `webapp_tts_generate()`, `_recover_stale_tts_generation_jobs()`, `_run_tts_generation_recovery_scheduler_job()` callers via recovery chain, dictionary-save paths in `backend_server.py` | Queue insertion + in-process dedup |
| `_enqueue_tts_generation_job(**kwargs)` | `backend/backend_server.py` | 30302 | Thin internal wrapper | Queue insertion wrapper |
| `_ensure_tts_generation_workers_started()` | `backend/backend_server.py` | 30234 | `_enqueue_tts_generation_job_result()`, `_recover_stale_tts_generation_jobs()`, startup call in `backend_server.py:38450` | Thread-pool bootstrap |
| `_tts_generation_worker_loop(worker_index)` | `backend/backend_server.py` | 30220 | Threads spawned by `_ensure_tts_generation_workers_started()` | Daemon-thread execution |
| `_run_tts_generation_job(...)` | `backend/backend_server.py` | 29960 | `_tts_generation_worker_loop()`, `_dispatch_tts_prewarm()`, legacy route `webapp_tts()` | Core generation unit; async in worker loop, sync in prewarm/legacy route |
| `run_tts_generation_recovery_actor()` | `backend/background_jobs.py` | 1456 | Dramatiq `scheduler_jobs` queue | Scheduler actor |
| `run_tts_generation_recovery_scheduler_job()` | `backend/tts_scheduler.py` | 16 | `run_tts_generation_recovery_actor()` | Scheduler wrapper |
| `_run_tts_generation_recovery_scheduler_job()` | `backend/backend_server.py` | 30366 | `backend.tts_scheduler.run_tts_generation_recovery_scheduler_job`, APScheduler registration in `backend_server.py:38685` | Scheduler wrapper |
| `_recover_stale_tts_generation_jobs(source)` | `backend/backend_server.py` | 30306 | `_run_tts_generation_recovery_scheduler_job()`, `_maybe_send_tts_admin_pending_alert()` | DB recovery scan + queue enqueue |
| `webapp_tts()` | `backend/backend_server.py` | 30740 | `POST /api/webapp/tts` | Legacy direct synchronous execution |

### Current process-local state

| Symbol | File | Line | Written by | Read by | Invariant | Scaling blocker |
|--------|------|------|-----------|---------|-----------|-----------------|
| `_TTS_GENERATION_QUEUE` | `backend/backend_server.py` | 639 | `_get_tts_generation_queue()` init, `_enqueue_tts_generation_job_result()` put | `_tts_generation_worker_loop()`, `_tts_generation_queue_size()` | Single bounded in-process work queue | Each process sees a different queue; jobs are not shared across replicas |
| `_TTS_GENERATION_WORKER_THREADS` | `backend/backend_server.py` | 640 | `_ensure_tts_generation_workers_started()` | `_ensure_tts_generation_workers_started()` liveness filter | Avoid overspawning threads in one process | Worker capacity is per-process, not globally coordinated |
| `_TTS_PREWARM_LOCK` | `backend/backend_server.py` | 954 | `_dispatch_tts_prewarm()` acquire/release | `_dispatch_tts_prewarm()` | Only one prewarm run per process | Two replicas can prewarm simultaneously |
| `_TTS_GENERATION_JOBS_LOCK` | `backend/backend_server.py` | 955 | Lock only | `_enqueue_tts_generation_job_result()`, `_run_tts_generation_job()` | Atomic access to dedup set | Lock is local and cannot coordinate cross-process dedup |
| `_TTS_GENERATION_JOBS` | `backend/backend_server.py` | 956 | `_enqueue_tts_generation_job_result()` add, `_run_tts_generation_job()` finally discard | `_enqueue_tts_generation_job_result()` membership test | One in-flight generation per `cache_key` within one process | Duplicate enqueue protection fails across replicas |
| `_TTS_ADMIN_MONITOR_EVENTS` | `backend/backend_server.py` | 636 | `_record_tts_admin_monitor_event()` append/prune | `_get_tts_admin_monitor_window()`, `_maybe_send_tts_admin_failure_alert()` | Hot in-memory monitor window for admin alerts | Alerts see only local events unless deque is empty and DB fallback is used |
| `_TTS_URL_POLL_ATTEMPTS` | `backend/backend_server.py` | 627 | `_increment_tts_url_poll_attempt()` | `webapp_tts_url()`, `_clear_tts_url_poll_attempt()` | Track URL poll attempts per cache key | Poll counters diverge by web process and disappear on restart |

### Current exact execution model

**A. On-demand generation**

`POST /api/webapp/tts/generate` → `webapp_tts_generate()` → `get_tts_object_meta()` DB read → `create_tts_object_pending()` / `requeue_tts_object_pending()` DB state transition → `_enqueue_tts_generation_job_result()` → `_ensure_tts_generation_workers_started()` → `_TTS_GENERATION_JOBS` dedup check → `_TTS_GENERATION_QUEUE.put()` async boundary → `_tts_generation_worker_loop()` daemon thread → `_run_tts_generation_job()` → optional `r2_exists()` HEAD → `_synthesize_mp3()` → `r2_put_bytes()` R2 write → `mark_tts_object_ready()` / `mark_tts_object_failed()` DB completion state transition.

**B. Stale recovery**

`run_tts_generation_recovery_actor()` → `backend.tts_scheduler.run_tts_generation_recovery_scheduler_job()` → `_run_tts_generation_recovery_scheduler_job()` → `_recover_stale_tts_generation_jobs(source="scheduler")` → `list_stale_pending_tts_objects()` DB read → `_build_tts_generation_job_kwargs_from_meta()` → `_enqueue_tts_generation_job_result()` → `_TTS_GENERATION_QUEUE.put()` async boundary → `_tts_generation_worker_loop()` → `_run_tts_generation_job()` → R2 + DB completion path as above.

**C. Prewarm**

`run_tts_prewarm_scheduler_actor()` or `POST /api/admin/prewarm-tts` → `backend.tts_scheduler.run_tts_prewarm_scheduler_job()` or `prewarm_tts_now()` → `_run_tts_prewarm_scheduler_job()` / direct route → `_dispatch_tts_prewarm()` → `_TTS_PREWARM_LOCK.acquire(blocking=False)` → planning DB reads (`_list_tts_prewarm_active_user_ids()`, `_get_tts_prewarm_user_activity_map()`, candidate/meta reads) → `create_tts_object_pending()` / `requeue_tts_object_pending()` DB state transition → **direct `_run_tts_generation_job()` inline** → optional `r2_exists()` → `_synthesize_mp3()` → `r2_put_bytes()` → `mark_tts_object_ready()` / `mark_tts_object_failed()` → immediate `get_tts_object_meta()` DB read to count `generated`.

### Current blocker summary for `_run_tts_generation_job()`

`_run_tts_generation_job()` still cannot move to `backend/tts_generation.py` as-is. The current hard blockers remain:

- process-local cleanup/dedup dependency: `_TTS_GENERATION_JOBS_LOCK`, `_TTS_GENERATION_JOBS`
- process-local poll state mutation: `_clear_tts_url_poll_attempt()`
- process-local admin monitoring: `_record_tts_admin_monitor_event()`, `_maybe_send_tts_admin_failure_alert()`
- cross-domain server helper dependency: `_get_user_language_pair()`
- billing subsystem dependency: `_billing_log_event_safe()`

The already-extracted helpers in `backend/tts_generation.py` reduce synthesis-domain coupling, but they do not remove the orchestration-layer dependence on `backend/backend_server.py`.

### Single recommended next step

**Change `_dispatch_tts_prewarm()` to enqueue generation via `_enqueue_tts_generation_job_result()` instead of calling `_run_tts_generation_job()` inline, and explicitly change the prewarm result counters from `generated/ready-now` semantics to `queued` semantics.**

This is the smallest meaningful move because it removes the only remaining synchronous orchestration path that bypasses the existing queue/worker model, without introducing Redis or moving `_run_tts_generation_job()` prematurely.

---

## 17. `_run_tts_generation_job` BLOCKER ISOLATION (2026-04-20)

This section narrows the remaining blockers around `_run_tts_generation_job()` without changing prewarm semantics, queue semantics, or runtime behavior.

### Blocker classification

| Blocker | Class | Why |
|---------|-------|-----|
| `_TTS_GENERATION_JOBS_LOCK` | process-local execution-state blocker | Guards in-process dedup set for the local queue runner |
| `_TTS_GENERATION_JOBS` | process-local execution-state blocker | Tracks in-flight `cache_key` values only within one process |
| `_clear_tts_url_poll_attempt()` | poll-state helper | Clears TTS URL polling state kept in a TTS-specific in-memory dict |
| `_record_tts_admin_monitor_event()` | observability/admin helper | Writes admin-monitor event to in-memory deque and persistent DB table |
| `_maybe_send_tts_admin_failure_alert()` | observability/admin helper | Reads monitor window and may send admin Telegram alert |
| `_get_user_language_pair()` | server-domain helper | Shared web/server language-profile helper with broad non-TTS reuse |
| `_billing_log_event_safe()` | billing helper | Shared billing write helper across multiple product domains |

### Blocker surface details

| Blocker | File | Line | Direct callers | Direct callees / deps | Side effects | Broad reuse outside TTS? | Narrow extraction now? |
|---------|------|------|----------------|------------------------|--------------|--------------------------|------------------------|
| `_clear_tts_url_poll_attempt()` | `backend/backend_server.py` | 2872 | `_run_tts_generation_job()`, `webapp_tts_url()`, `webapp_tts_generate()` | `_OBSERVABILITY_LOCK`, `_TTS_URL_POLL_ATTEMPTS.pop()` | Mutates in-memory TTS poll counter map | No; TTS-only | **Yes** — can move with its TTS-specific dict/lock boundary |
| `_record_tts_admin_monitor_event()` | `backend/backend_server.py` | 11753 | `_run_tts_generation_job()`, enqueue path, recovery path, prewarm path, scheduler wrappers | `_TTS_ADMIN_MONITOR_EVENTS`, `_TTS_ADMIN_MONITOR_LOCK`, `persist_tts_admin_monitor_event()`, `_prune_tts_admin_monitor_events_locked()`, `_prune_tts_admin_monitor_events_persistent()` | Appends to deque, prunes, persists DB event | Mostly TTS-only, but used across all TTS orchestration branches | Not as a tiny move; it drags the admin-monitor subsystem |
| `_maybe_send_tts_admin_failure_alert()` | `backend/backend_server.py` | 12223 | `_run_tts_generation_job()`, enqueue queue-full path, scheduler wrappers | `_get_tts_admin_monitor_window()`, `_summarize_tts_failure_window()`, `_should_send_tts_admin_alert()`, `_send_tts_admin_message()` | Reads monitor window, cooldown state, may send Telegram admin message | TTS-only, but operationally broad inside TTS | Not as a tiny move; it drags alerting/reporting helpers |
| `_get_user_language_pair()` | `backend/backend_server.py` | 3715 | `_run_tts_generation_job()` plus many non-TTS routes and services | `_LANGUAGE_PAIR_CACHE`, `_LANGUAGE_PAIR_CACHE_LOCK`, `get_user_language_profile()`, normalization helpers | DB read on miss, shared cache write | **Yes** — heavily reused outside TTS | No narrow extraction; cross-domain shared server helper |
| `_billing_log_event_safe()` | `backend/backend_server.py` | 8702 | `_run_tts_generation_job()` plus many non-TTS billing sites | `log_billing_event()`, `_notify_provider_budget_thresholds()`, `_log_billing_skip_warning()` | Billing DB write, provider-threshold notification | **Yes** — broad cross-domain reuse | No narrow extraction; billing-domain shared helper |
| `_TTS_GENERATION_JOBS_LOCK` | `backend/backend_server.py` | 955 | `_run_tts_generation_job()`, `_enqueue_tts_generation_job_result()` | `threading.Lock` only | Synchronizes local dedup set access | TTS-only | No narrow extraction by itself; tied to local queue model |
| `_TTS_GENERATION_JOBS` | `backend/backend_server.py` | 956 | `_run_tts_generation_job()`, `_enqueue_tts_generation_job_result()` | local `set[str]` | Adds/discards in-flight `cache_key` | TTS-only | No narrow extraction by itself; tied to local queue model |

### `_run_tts_generation_job()` internal phases

| Phase | Code shape | Blockers touched |
|-------|------------|------------------|
| Setup / correlation | sanitize ids, build request/correlation ids, emit `generation_runner_started` log | none of the hard blockers |
| Optional language-resolution setup | `if user_id_int > 0: _get_user_language_pair(...)` | `_get_user_language_pair()` |
| Existing-meta short-circuit | `r2_exists()` → optional `mark_tts_object_ready()` → `_clear_tts_url_poll_attempt()` | `_billing_log_event_safe()` on R2 HEAD estimate, `_clear_tts_url_poll_attempt()` |
| Synthesis | `_synthesize_mp3(...)` | none of the listed blockers |
| Upload | `r2_put_bytes(...)` | `_billing_log_event_safe()` for R2 PUT + storage allocation |
| Ready transition | `mark_tts_object_ready(...)` | `_billing_log_event_safe()` for Google TTS chars, `_clear_tts_url_poll_attempt()` |
| Failure handling | `mark_tts_object_failed(...)`, shorten error text | none of the listed blockers directly, except later alerting in finally |
| Finally cleanup / monitoring | discard `cache_key` from `_TTS_GENERATION_JOBS`, record monitor event, maybe send admin alert, emit finish log | `_TTS_GENERATION_JOBS_LOCK`, `_TTS_GENERATION_JOBS`, `_record_tts_admin_monitor_event()`, `_maybe_send_tts_admin_failure_alert()` |

### Minimum movable sub-slice

There is **no meaningful success-path or finally-block sub-slice of `_run_tts_generation_job()`** that can move now without dragging one of the architecture-bound blockers with it.

The only truly narrow movable dependency is the **TTS URL poll-state primitive**:
- `_TTS_URL_POLL_ATTEMPTS`
- `_increment_tts_url_poll_attempt()`
- `_clear_tts_url_poll_attempt()`

That slice is:
- TTS-only
- process-local but self-contained
- reused only by TTS URL/generate/runner code
- independent from billing, admin alerting, queue semantics, and prewarm semantics

By contrast:
- admin-monitor helpers pull in deque + persistence + Telegram alerting
- `_get_user_language_pair()` is a broad shared server helper
- `_billing_log_event_safe()` is a broad shared billing helper
- `_TTS_GENERATION_JOBS*` are architecture-bound to the current in-process queue model

### Single recommended next step

**Extract the TTS URL poll-state slice first: move `_TTS_URL_POLL_ATTEMPTS`, `_increment_tts_url_poll_attempt()`, and `_clear_tts_url_poll_attempt()` into a dedicated TTS runtime-state module, then update TTS routes and `_run_tts_generation_job()` to import those primitives from there.**

Why this is next:
1. It removes one real `_run_tts_generation_job()` blocker without changing execution semantics.
2. It is smaller and safer than changing prewarm or queue behavior.
3. It establishes the right extraction pattern for TTS-only process-local state before touching architecture-bound queue/dedup logic.

### Why prewarm queue-conversion is deferred

Prewarm queue-conversion remains the first **execution-model** change, but it is deferred in favor of blocker isolation because:
- it changes result semantics from `generated now` to `queued`
- it changes scheduler/admin observable behavior
- `_run_tts_generation_job()` still carries multiple backend-server-only blockers that should be reduced first

So the next step is not to change prewarm behavior yet, but to remove the narrowest TTS-only blocker from `_run_tts_generation_job()` first.

---

## 18. TTS URL POLL-STATE EXTRACTION (2026-04-20)

Moved from `backend/backend_server.py` to [backend/tts_runtime_state.py](/Users/alexandr/Desktop/TELEGRAM_BOT_DEUTSCHESPRACHE/backend/tts_runtime_state.py):

- `_TTS_URL_POLL_ATTEMPTS`
- `_increment_tts_url_poll_attempt()`
- `_clear_tts_url_poll_attempt()`

Why this moved:
- it was the narrowest `_run_tts_generation_job()` blocker
- it is TTS-only
- it does not change prewarm, queue, or billing behavior

Important limitation:
- this remains **process-local in-memory state**
- it is still **not** a horizontal-scaling solution
- this step is blocker isolation only, so that `_run_tts_generation_job()` depends less directly on `backend/backend_server.py`

Remaining `_run_tts_generation_job()` blockers after this extraction:
- `_TTS_GENERATION_JOBS_LOCK`
- `_TTS_GENERATION_JOBS`
- `_record_tts_admin_monitor_event()`
- `_maybe_send_tts_admin_failure_alert()`
- `_get_user_language_pair()`
- `_billing_log_event_safe()`

---

## 19. REMAINING BLOCKER COMPARISON (2026-04-20)

This section compares the remaining `_run_tts_generation_job()` blockers after the TTS URL poll-state extraction, with the goal of choosing the next smallest blocker-removal step without changing execution semantics.

### Comparison table

| Blocker | Category | File | Line | Direct callers | Direct callees / deps | Side effects | Reuse outside TTS | Extraction blast radius | Material coupling reduction if removed? |
|---------|----------|------|------|----------------|------------------------|--------------|-------------------|-------------------------|-----------------------------------------|
| `_TTS_GENERATION_JOBS_LOCK` | process-local state | `backend/backend_server.py` | 958 | `_run_tts_generation_job()`, `_enqueue_tts_generation_job_result()` | `threading.Lock` | Synchronizes local in-flight set | No | Small if moved together with `_TTS_GENERATION_JOBS`; not meaningful alone | Yes, but only together with `_TTS_GENERATION_JOBS` |
| `_TTS_GENERATION_JOBS` | process-local state | `backend/backend_server.py` | 959 | `_run_tts_generation_job()`, `_enqueue_tts_generation_job_result()` | local `set[str]` | Tracks in-flight `cache_key` values | No | Small if moved together with lock/helper wrapper | Yes, but only together with lock |
| `_record_tts_admin_monitor_event()` | admin-observability | `backend/backend_server.py` | 11736 | `_run_tts_generation_job()`, enqueue path, recovery path, prewarm path, scheduler wrappers | `_TTS_ADMIN_MONITOR_EVENTS`, `_TTS_ADMIN_MONITOR_LOCK`, persistence helpers | Deque append/prune + DB write | Mostly TTS-only, but broad across TTS orchestration | Medium; drags admin monitor storage and prune helpers | Yes |
| `_maybe_send_tts_admin_failure_alert()` | admin-observability | `backend/backend_server.py` | 12206 | `_run_tts_generation_job()`, queue-full path, scheduler wrappers | monitor-window helpers, summary helpers, Telegram send path | Reads monitor window, cooldown state, may send alerts | TTS-only | Medium-to-large; drags alerting/reporting subsystem | Yes |
| `_get_user_language_pair()` | server-domain | `backend/backend_server.py` | 3698 | `_run_tts_generation_job()` plus many non-TTS routes and services | language-profile cache + DB helper + normalization helpers | Shared cache write, DB read on miss | **High** | Large; shared web/server helper | Yes, but too broad right now |
| `_billing_log_event_safe()` | billing | `backend/backend_server.py` | 8685 | `_run_tts_generation_job()` plus many non-TTS billing sites | `log_billing_event()`, threshold notifier, skip-warning helper | Billing DB write + provider budget notifications | **High** | Large; shared billing subsystem | Yes, but too broad right now |

### Coupling / pairing findings

- `_TTS_GENERATION_JOBS_LOCK` + `_TTS_GENERATION_JOBS` are a **hard pair**.
  They are not useful as separate moves. The meaningful unit is an in-flight dedup runtime-state slice or tiny wrapper API around that slice.

- `_record_tts_admin_monitor_event()` + `_maybe_send_tts_admin_failure_alert()` are **loosely paired but operationally adjacent**.
  They can be separated in code, but moving only one leaves `_run_tts_generation_job()` still bound to the same admin-monitor subsystem.

- `_get_user_language_pair()` is **standalone but broad**.
  It is separable in theory, but not as a narrow TTS-only move because it is a heavily shared server-domain helper.

- `_billing_log_event_safe()` is **standalone but broad**.
  It could be hidden behind a TTS billing adapter, but that adapter would still depend on the same shared billing subsystem and would not materially shrink blast radius yet.

### Candidate next-step sizing

| Candidate | Size / risk | Why |
|-----------|-------------|-----|
| Isolate `_record_tts_admin_monitor_event()` only | medium / risky | Requires moving the monitor-event write path plus deque/prune/persistence helpers, but still leaves `_maybe_send_tts_admin_failure_alert()` in `_run_tts_generation_job()` |
| Isolate monitor-event + failure-alert together | too broad right now | Pulls in monitor window, summary helpers, cooldown state, Telegram admin messaging, and reporting behavior |
| Isolate `_get_user_language_pair()` adapter boundary | medium / risky | Still anchored to a broad shared server-domain helper with many non-TTS callers |
| Isolate a TTS-local billing adapter around `_billing_log_event_safe()` | medium / risky | Adds an indirection layer but does not actually isolate the billing subsystem yet |
| Isolate the in-flight dedup state (`_TTS_GENERATION_JOBS_LOCK` + `_TTS_GENERATION_JOBS`) | **tiny / safe** | TTS-only, narrow, directly used by `_run_tts_generation_job()` and enqueue path, no behavior change required |
| Do not move anything yet | too conservative | There is still one clearly smaller blocker-removal move available |

### Single recommended next step

**Isolate the in-flight dedup state next: move `_TTS_GENERATION_JOBS_LOCK` and `_TTS_GENERATION_JOBS` behind a narrow TTS runtime-state boundary, with tiny helper primitives for claim/release semantics used by `_enqueue_tts_generation_job_result()` and `_run_tts_generation_job()`.**

Why this is next:
1. It is smaller than the admin-monitor path.
2. It is far narrower than the language-profile and billing helpers.
3. It removes two remaining `_run_tts_generation_job()` blockers in one TTS-only move without changing execution semantics.

### Why other paths are deferred

- Admin-monitor extraction is deferred because the write path and alert path are not a tiny move once persistence, monitor window, and Telegram alerting are included.
- `_get_user_language_pair()` is deferred because it is a shared server-domain helper, not a narrow TTS-only dependency.
- `_billing_log_event_safe()` is deferred because a TTS-local adapter would mostly add indirection while keeping the same broad billing dependency underneath.
