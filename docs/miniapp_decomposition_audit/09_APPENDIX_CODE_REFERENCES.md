# Appendix: Code References

## Frontend

- `frontend/src/App.jsx`
- `frontend/src/components/LiveKitRuntime.jsx`

## Telegram Bot

- `bot_3.py`

Key bot surface areas:

- `/start`
- `/request_access`
- `/allow`
- `/deny`
- `/pending`
- `/mobile_token`
- `/translate`
- quiz callbacks
- dictionary callbacks
- flashcard feel callbacks
- image quiz callbacks
- group enrollment callbacks

## Backend HTTP

- `backend/backend_server.py`

Key route clusters:

- auth/bootstrap: around `/api/webapp/bootstrap`, `/api/telegram/validate`, `/api/web/auth/*`
- billing: `/api/billing/*`, `/billing/telegram-return`, `/api/billing/webhook`
- translations: `/api/webapp/start`, `/check/*`, `/finish`, `/session*`, `/sentences*`, `/history*`
- dictionary: `/api/webapp/dictionary*`, `/api/mobile/dictionary*`
- flashcards/cards: `/api/webapp/flashcards*`, `/api/cards/*`
- TTS: `/api/webapp/tts*`
- YouTube: `/api/webapp/youtube/*`
- reader: `/api/webapp/reader/*`
- today: `/api/today*`
- skills/plans: `/api/progress/*`
- assistant/voice: `/api/assistant/*`
- support: `/api/webapp/support/*`
- analytics/economics: `/api/webapp/analytics/*`, `/api/economics/summary`
- admin user-affecting routes: `/api/admin/*`

## Translation Domain

- `backend/translation_workflow.py`
- `backend/background_jobs.py`
- `backend/job_queue.py`

## TTS Domain

- `backend/tts_generation.py`
- `backend/tts_runtime_state.py`
- `backend/tts_admin_monitor.py`
- `backend/tts_scheduler.py`
- `backend/background_jobs.py`

## Voice Domain

- `backend/agent.py`
- `backend/voice_session_service.py`
- `backend/voice_scenario_service.py`
- `backend/voice_preparation_service.py`
- `backend/voice_assessment_service.py`
- `backend/voice_skill_bridge_service.py`

## Reader / Content Domain

- `backend/backend_server.py` reader routes
- `backend/job_queue.py` transcript/job helpers

## Image Quiz Domain

- `backend/image_quiz_utils.py`
- `backend/background_jobs.py`
- `bot_3.py`

## Jobs / Shared Async / Projections

- `backend/job_queue.py`
- `backend/background_jobs.py`
- `backend/scheduler_service.py`
- `backend/scheduler_jobs_core.py`
- `backend/hotpath_cache.py`

## Billing / Entitlement

- `backend/database.py`
  - `plans`
  - `user_subscriptions`
  - `plan_limits`
  - `resolve_entitlement(...)`
  - `enforce_feature_limit(...)`
- `backend/backend_server.py`
  - billing routes
  - billing guard rules

## Storage/Table Prefix Clusters

- `bt_3_translation*`
- `bt_3_webapp_dictionary*`
- `bt_3_flashcard*`
- `bt_3_reader*`
- `bt_3_youtube*`
- `bt_3_skill*`
- `bt_3_voice*`
- `bt_3_support*`
- `bt_3_image_quiz*`
- `bt_3_tts*`
- `bt_3_daily_plans`
- `bt_3_daily_plan_items`
- `bt_3_projection_jobs`
- `plans`
- `user_subscriptions`
- `plan_limits`

## Operational/User-Affecting Scheduler Entrypoints

- `run_today_plan_scheduler_actor`
- `run_today_evening_reminders_scheduler_actor`
- `run_private_analytics_scheduler_actor`
- `run_weekly_goals_scheduler_actor`
- `run_daily_group_summary_scheduler_actor`
- `run_weekly_group_summary_scheduler_actor`
- `run_translation_sessions_auto_close_actor`
- `run_tts_prewarm_scheduler_actor`
- `run_tts_generation_recovery_actor`
- `run_tts_prewarm_quota_control_actor`
- `run_translation_focus_pool_admin_report_actor`
- `run_skill_state_v2_aggregation_actor`
