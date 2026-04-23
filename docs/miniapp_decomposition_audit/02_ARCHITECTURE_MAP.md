# Architecture Map

## 1. Current Product Shape

The current product is a combined ecosystem:

- Telegram bot surface
- Telegram Mini App / web frontend shell
- Flask backend monolith
- background worker/scheduler layer
- shared Redis-backed transient state and async orchestration
- shared Postgres storage
- Stripe billing
- LiveKit/OpenAI voice runtime

## 2. Frontend Surfaces

### Main Mini App shell

Primary shell:

- `frontend/src/App.jsx`

Observed top-level functional sections/guides:

- subscription
- today
- translations
- YouTube
- movies/catalog
- dictionary
- reader
- flashcards
- assistant
- support
- analytics
- skill training

### Notable frontend architecture characteristics

- one large stateful shell with cross-section state and navigation
- home dashboard combines weekly plan, today plan, and skills snapshots
- translation flow contains nested story mode, dictionary jump-outs, and history
- reader/YouTube/dictionary/flashcards all live in the same runtime
- billing and analytics are visible inside the same shell
- LiveKit voice runtime is integrated as a frontend sub-surface rather than a separate app

## 3. Telegram Bot Layer

Primary entrypoint:

- `bot_3.py`

Responsibilities observed in code:

- onboarding and access approval
- bot commands and private chat
- dictionary and flashcard callbacks
- quiz and image quiz flows
- translation command path
- reminder/scheduled send integration
- group enrollment and group context tracking
- budget/admin controls that affect user experience

This means the bot is both:

- a user-facing interface
- a transport/orchestration layer for scheduled and callback-driven experiences

## 4. Backend Monolith Domain Groupings

Primary HTTP backend:

- `backend/backend_server.py`

Major route clusters:

- auth/bootstrap/initData validation
- translations
- dictionary
- flashcards/cards
- TTS
- YouTube
- reader
- today
- skills / weekly plan / analytics
- assistant / voice
- billing
- support
- admin / projection / scheduler triggers

### Domain modules already extracted or partially separated

- translations: `backend/translation_workflow.py`
- TTS: `backend/tts_generation.py`, `backend/tts_runtime_state.py`, `backend/tts_admin_monitor.py`, `backend/tts_scheduler.py`
- jobs/queues: `backend/job_queue.py`, `backend/background_jobs.py`, `backend/scheduler_service.py`, `backend/scheduler_jobs_core.py`
- voice: `backend/agent.py`, `backend/voice_*`
- image quiz: `backend/image_quiz_utils.py`
- observability helpers: `backend/observability.py`
- caching: `backend/hotpath_cache.py`

The architecture is still monolithic at deployment shape, but domain seams already exist in code.

## 5. Background Jobs / Async / Scheduler Layer

### Queue and transient state

- `backend/job_queue.py`

Shared responsibilities:

- Redis client and Dramatiq broker
- async enablement flags
- translation check state
- translation fill state
- YouTube transcript job status
- projection materialization enqueue
- image quiz enqueue
- session presence/today/skills card transient state

### Worker jobs

- `backend/background_jobs.py`

Observed user-affecting jobs:

- translation check
- translation check completion
- translation fill
- finish daily summary
- projection materialization live/backfill
- translation focus pool refill
- image quiz prepare/render/refresh
- translation result side effects

### Schedulers

- `backend/scheduler_service.py`
- `backend/tts_scheduler.py`
- `backend/scheduler_jobs_core.py`

Observed scheduled concerns:

- daily audio
- private analytics
- weekly goals
- today plans
- evening reminders
- daily/weekly group summary
- translation session auto-close
- flashcard feel cleanup
- system message cleanup
- TTS cleanup, prewarm, quota control, recovery
- sentence prewarm
- skill-state aggregation

## 6. Shared Services / Cross-Cutting Concerns

### Auth / identity / access

- Telegram initData validation
- web/mobile token exchange
- allowlist and access-request flow
- webapp bootstrap and single-instance claim/release

### Billing / entitlement

- Stripe plans/status/checkout/portal/webhook
- entitlement resolution
- feature usage limits
- route-level billing guards

### Analytics / economics

- analytics summary/timeseries/compare
- economics summaries/admin syncs
- private and group analytics sends

### TTS

- user-visible generate/url flows
- internal prewarm/recovery/cleanup/quota monitoring
- shared by translations, dictionary, and reader-style flows

### Projections / snapshots / caches

- today card
- skills card
- session presence card
- projection materialization jobs
- hot path cache helpers

## 7. Voice Stack

### Runtime

- `backend/agent.py`
- `frontend/src/components/LiveKitRuntime`

### Context and persistence

- `backend/voice_session_service.py`
- `backend/voice_scenario_service.py`
- `backend/voice_preparation_service.py`
- `backend/voice_assessment_service.py`
- `backend/voice_skill_bridge_service.py`

Voice is operationally distinct, but still depends on shared identity, analytics, and skills context.

## 8. Database Domain Clusters

Table/domain prefixes observed:

- access/allowlist: `bt_3_allowed_users`, `bt_3_access_requests`
- daily plans: `bt_3_daily_plans`, `bt_3_daily_plan_items`
- translations: `bt_3_translation*`, `bt_3_daily_sentences`, `bt_3_translations`
- dictionary: `bt_3_webapp_dictionary*`
- flashcards: `bt_3_flashcard*`
- reader: `bt_3_reader*`
- YouTube: `bt_3_youtube*`
- skills: `bt_3_skill*`
- voice: `bt_3_voice*`
- support: `bt_3_support*`
- image quiz: `bt_3_image_quiz*`
- TTS: `bt_3_tts*`
- projections: `bt_3_projection_jobs`
- billing: `plans`, `user_subscriptions`, `plan_limits`, `stripe_events_processed`

## 9. Current Coupling Summary

### Strongly shared/platform-level

- auth/initData/access
- billing/entitlement
- analytics/economics
- job queue/transient state
- Telegram bot communication surfaces
- user language profile
- TTS pipeline
- projection materialization

### Domain-oriented but still coupled

- translations
- dictionary
- flashcards
- reader
- YouTube
- today
- skills/weekly plan
- voice assistant
- image quiz

## 10. Architectural Implication for Decomposition

The codebase already has domain modules, but the product surface is still organized as one orchestrated ecosystem. The safest migration path is:

- keep the current app as orchestrator shell
- extract domain Mini Apps only after shared core contracts are explicit
- avoid splitting shared aggregates and reminders first
