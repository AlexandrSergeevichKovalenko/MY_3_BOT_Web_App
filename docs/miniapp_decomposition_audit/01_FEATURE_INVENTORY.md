# Feature Inventory

## Inventory Rules

This file inventories current user-facing and user-affecting functionality found in the repository. Items listed here should be treated as **do not remove** unless later proven otherwise by explicit product decision.

## A. Translations

### User-visible purpose

- start a translation session
- receive a set of sentences
- submit translations
- run translation check
- view results, explanations, and history
- finish the session
- use recommendation-driven focus selection
- run story-mode translation as a variant

### Frontend surfaces

- `frontend/src/App.jsx`
  - `TranslationsSection`
  - start configuration
  - sentence drafting
  - translation result rendering
  - daily history
  - story mode

### Backend routes

- `/api/webapp/start`
- `/api/webapp/session`
- `/api/webapp/session/activity`
- `/api/webapp/check/start`
- `/api/webapp/check/status`
- `/api/webapp/finish`
- `/api/webapp/history`
- `/api/webapp/history/daily`
- `/api/webapp/sentences`
- `/api/webapp/sentences/ack`
- `/api/webapp/translation/drafts`
- `/api/webapp/explain`
- `/api/webapp/topics`

### Core backend domain

- `backend/translation_workflow.py`

### Async/jobs

- `run_translation_check_job`
- `run_translation_check_completion_job`
- `run_translation_fill_job`
- `run_translation_result_side_effects_job`
- `run_translation_focus_pool_refill_job`

### Storage/domain tables

- `bt_3_translation_*`
- `bt_3_daily_sentences`
- `bt_3_translations`

### Billing/limits

- feature limit: `translation_daily_sets`

### Do not remove

- translation sessions
- translation history
- story mode
- sentence pool refill
- async check/fill path
- result-side effects

## B. Vocabulary / Dictionary

### User-visible purpose

- quick and GPT-powered lookup
- save words/phrases
- assign folders
- export dictionary PDF/card PDF
- collocations
- starter dictionary onboarding
- mobile dictionary save/lookup

### Frontend surfaces

- dictionary lookup panel in `frontend/src/App.jsx`
- quick lookup integration from translation and other flows

### Backend routes

- `/api/webapp/starter-dictionary/status`
- `/api/webapp/starter-dictionary/apply`
- `/api/webapp/dictionary`
- `/api/webapp/dictionary/status`
- `/api/mobile/dictionary/lookup`
- `/api/webapp/dictionary/collocations`
- `/api/webapp/dictionary/export/pdf`
- `/api/webapp/dictionary/export/card-pdf`
- `/api/webapp/dictionary/save`
- `/api/mobile/dictionary/save`
- `/api/webapp/dictionary/folders`
- `/api/webapp/dictionary/folders/create`

### Telegram bot surfaces

- dictionary pair/mode selection callbacks
- dictionary save/quick save callbacks
- dictionary speak callbacks

### Storage/domain tables

- `bt_3_webapp_dictionary*`

### Do not remove

- lookup
- save
- folder organization
- export
- bot dictionary callbacks
- mobile dictionary access

## C. Flashcards / FSRS / Saved-Word Training

### User-visible purpose

- personal word/card training
- FSRS repetition
- quiz/blocks/sentence modes
- “feel” feedback path
- card enrichment

### Frontend surfaces

- flashcard setup and training in `frontend/src/App.jsx`

### Backend routes

- `/api/webapp/dictionary/cards`
- `/api/webapp/flashcards/set`
- `/api/webapp/flashcards/answer`
- `/api/cards/next`
- `/api/cards/prefetch`
- `/api/cards/review`
- `/api/webapp/flashcards/feel`
- `/api/webapp/flashcards/feel/dispatch`
- `/api/webapp/flashcards/feel/feedback`
- `/api/webapp/flashcards/enrich`

### Telegram bot surfaces

- flashcard feel callbacks
- quiz-style card reminders

### Jobs/schedulers

- `run_flashcard_feel_cleanup_actor`

### Billing/limits

- feature limit: `feel_word_daily`

### Storage/domain tables

- `bt_3_flashcard*`

### Do not remove

- FSRS review
- flashcard modes
- feel feedback loop
- bot reminder/quiz tie-ins

## D. Reader / Document Ingest / Reading Audio

### User-visible purpose

- ingest text, URL, or file
- manage personal library
- open/archive/rename/delete documents
- generate document audio
- track reader session progress

### Frontend surfaces

- reader/library state in `frontend/src/App.jsx`

### Backend routes

- `/api/webapp/reader/ingest`
- `/api/webapp/reader/library`
- `/api/webapp/reader/library/open`
- `/api/webapp/reader/library/state`
- `/api/webapp/reader/library/rename`
- `/api/webapp/reader/library/archive`
- `/api/webapp/reader/library/delete`
- `/api/webapp/reader/audio`
- `/api/reader/session/start`
- `/api/reader/session/complete`
- `/api/reader/session/ping`

### Storage/domain tables

- `bt_3_reader*`

### Shared dependencies

- dictionary and TTS hooks
- analytics/progress

### Do not remove

- document ingest
- library
- reading session tracking
- audio export/playback

## E. YouTube / Transcript / Translation / Catalog

### User-visible purpose

- fetch transcript
- poll transcript status
- translate transcript
- manual transcript fallback
- search videos
- open catalog
- state persistence for video study

### Frontend surfaces

- YouTube section in `frontend/src/App.jsx`
- transcript polling and application
- manual transcript entry
- catalog entry path

### Backend routes

- `/api/webapp/youtube/transcript`
- `/api/webapp/youtube/transcript/status`
- `/api/webapp/youtube/state`
- `/api/webapp/youtube/catalog`
- `/api/webapp/youtube/search`
- `/api/webapp/youtube/manual`
- `/api/webapp/youtube/translate`
- `/api/today/video/recommend`
- `/api/today/video/feedback`

### Async/jobs

- async YouTube transcript job status in `backend/job_queue.py`
- transcript fetch worker in `backend/background_jobs.py`

### Billing/limits

- feature limit: `youtube_fetch_daily`

### Storage/domain tables

- `bt_3_youtube*`

### Do not remove

- automatic transcript path
- manual transcript fallback
- search/catalog path
- translation overlay path

## F. Today / Daily Tasks / Reminders

### User-visible purpose

- daily learning route
- actionable tasks
- timer and completion events
- video/theory recommendations
- reminder preferences

### Frontend surfaces

- home/today panel in `frontend/src/App.jsx`

### Backend routes

- `/api/today`
- `/api/today/regenerate`
- `/api/today/items/<id>/start`
- `/api/today/items/<id>/translation/start`
- `/api/today/items/<id>/timer`
- `/api/today/video/recommend`
- `/api/today/video/feedback`
- `/api/today/theory/prepare`
- `/api/today/theory/check`
- `/api/today/items/<id>/complete`
- `/api/today/reminders`
- `/api/today/reminders/test`

### Jobs/schedulers

- `run_today_plan_scheduler_actor`
- `run_today_evening_reminders_scheduler_actor`
- Telegram daily sends from admin/scheduler routes

### Billing/limits

- theory/skill-training guard uses `skill_training_daily`

### Storage/domain tables

- `bt_3_daily_plans`
- `bt_3_daily_plan_items`

### Do not remove

- today plan
- task progression
- reminder settings
- Telegram reminders

## G. Skills / Skill Training / Weekly Plan / Plan Analytics

### User-visible purpose

- view skills state
- start focused practice on weak skills
- save weekly plan
- inspect plan analytics

### Frontend surfaces

- home skill report
- weekly plan panel
- skill training actions

### Backend routes

- `/api/progress/skills`
- `/api/progress/skills/<skill_id>/practice/start`
- `/api/progress/skills/<skill_id>/practice/event`
- `/api/progress/weekly-plan`
- `/api/progress/plan-analytics`
- `/api/today/theory/prepare`
- `/api/today/theory/check`

### Jobs/schedulers

- `run_skill_state_v2_aggregation_actor`
- admin skill-state compare/seed/upsert endpoints

### Storage/domain tables

- `bt_3_skill*`

### Do not remove

- skill report
- skill practice
- weekly planning
- plan analytics

## H. Voice Assistant / Speaking Practice

### User-visible purpose

- live speaking practice
- scenario/prep-pack context
- transcript capture
- post-session assessment
- optional bridge into skills

### Frontend surfaces

- assistant section in `frontend/src/App.jsx`
- `frontend/src/components/LiveKitRuntime`

### Backend routes

- `/api/assistant/session/start`
- `/api/assistant/scenario/create`
- `/api/assistant/scenario/get`
- `/api/assistant/prep-pack/create`
- `/api/assistant/prep-pack/get`
- `/api/assistant/session/complete`
- `/api/assistant/session/assessment/get`

### Backend/domain modules

- `backend/agent.py`
- `backend/voice_session_service.py`
- `backend/voice_scenario_service.py`
- `backend/voice_preparation_service.py`
- `backend/voice_assessment_service.py`
- `backend/voice_skill_bridge_service.py`

### Storage/domain tables

- `bt_3_voice*`

### Do not remove

- live assistant runtime
- transcript persistence
- scenario/prep-pack path
- post-call assessment

## I. TTS User-Facing Flows

### User-visible purpose

- get TTS URL for content
- trigger generation when needed
- use audio in translation/dictionary/reader flows

### Backend routes

- `/api/webapp/tts/url`
- `/api/webapp/tts/generate`
- `/api/webapp/tts`
- `/api/webapp/reader/audio`

### Async/jobs/schedulers

- TTS generation queue/runtime state
- TTS prewarm
- TTS recovery
- TTS quota control
- TTS DB/R2 cleanup jobs

### Backend/domain modules

- `backend/tts_generation.py`
- `backend/tts_runtime_state.py`
- `backend/tts_admin_monitor.py`
- `backend/tts_scheduler.py`

### Billing/limits

- feature limit: `tts_chars_daily`

### Storage/domain tables

- `bt_3_tts*`

### Do not remove

- user-visible audio generation
- TTS URL/generation endpoints
- recovery/prewarm operational path

## J. Analytics / History / Economics

### User-visible purpose

- view analytics by scope and time range
- compare progress
- inspect summary/timeseries
- review history
- view economics/admin-only surfaces where enabled

### Frontend surfaces

- analytics section
- economics panel (admin-visible)
- translation history and daily history

### Backend routes

- `/api/webapp/analytics/scope`
- `/api/webapp/analytics/scope/select`
- `/api/webapp/analytics/summary`
- `/api/webapp/analytics/timeseries`
- `/api/webapp/analytics/compare`
- `/api/economics/summary`
- `/api/webapp/history`
- `/api/webapp/history/daily`

### Telegram jobs

- private analytics send
- daily group summary
- weekly group summary
- weekly goals

### Do not remove

- private analytics
- group analytics summaries
- history
- economics visibility where currently enabled

## K. Billing / Plans / Access Control

### User-visible purpose

- view plan status
- checkout
- manage Stripe subscription
- access gating and trial/pro/free logic

### Backend routes

- `/api/billing/plans`
- `/api/billing/status`
- `/api/billing/create-checkout-session`
- `/api/billing/create-portal-session`
- `/billing/telegram-return`
- `/api/billing/webhook`

### Admin routes

- `/api/admin/billing/debug-entitlement`
- `/api/admin/billing/normalize-stripe-subscriptions`

### Telegram bot/admin surfaces

- access request flow
- allow/deny/pending commands

### Storage/domain tables

- `plans`
- `user_subscriptions`
- `plan_limits`
- `stripe_events_processed`
- allowlist/request tables

### Do not remove

- Stripe lifecycle
- entitlement enforcement
- trial/free/pro transitions
- access request flow

## L. Image Quiz / Learning Games / Bot Quiz Flows

### User-visible purpose

- generate image quiz templates
- render image quizzes
- quiz feedback flows in Telegram

### Backend/domain modules

- `backend/image_quiz_utils.py`
- async jobs in `backend/background_jobs.py`

### Telegram bot surfaces

- image quiz callback flows
- poll answer handling
- freeform quiz answer handling

### Jobs

- `run_image_quiz_template_prepare_job`
- `run_image_quiz_template_render_job`
- `run_image_quiz_template_refresh_job`

### Storage/domain tables

- `bt_3_image_quiz*`

### Do not remove

- image quiz pipeline
- bot quiz feedback
- rendered template lifecycle

## M. Telegram Chat / Reminders / Support / Group Flows

### User-visible purpose

- private chat onboarding and access
- reminders and summaries
- support messages
- group enrollment and group context

### Telegram bot surfaces

- `/start`
- `/request_access`
- `/allow`
- `/deny`
- `/allowed`
- `/pending`
- `/pending_purges`
- `/mobile_token`
- `/translate`
- bot message handlers
- callback handlers for group enrollment, quizzes, dictionary, TTS budget, etc.

### Backend routes

- support message endpoints
- admin send endpoints for reminders/summaries

### Jobs/schedulers

- today plan sends
- evening reminders
- daily audio
- weekly goals
- daily/weekly group summaries
- weekly badges

### Do not remove

- bot onboarding
- reminder/summaries
- support chat
- group flows

## N. Admin / Hidden but User-Affecting Surfaces

These are not always directly visible to end users, but they materially affect user experience and must be accounted for.

- projection backfill/materialization
- TTS prewarm and quota control
- sentence prewarm
- translation focus pool admin report
- skill-state v2 aggregation and compare
- system-message cleanup
- flashcard feel cleanup
- YouTube debug/access admin paths
- economics admin sync paths

## O. Explicit “Do Not Remove” Summary

The following areas are explicitly in-scope and must not be silently dropped during future decomposition:

- translation sessions and checks
- story mode
- dictionary save/export/folders
- flashcards and FSRS
- reader ingest/library/audio
- YouTube transcript/manual/search/catalog/translation
- today plans and reminders
- skills, skill training, weekly plans, analytics
- live voice assistant
- TTS generation and audio surfaces
- image quizzes and Telegram quiz flows
- support messaging
- billing and entitlement
- Telegram reminders, summaries, and group flows
