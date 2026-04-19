"""
Dedicated scheduler service — trigger/dispatch only.

This process owns the APScheduler instance and is the sole source of scheduled
triggers.  It must run as exactly ONE replica.  It does NO heavy work: every
cron/interval handler body does nothing except call actor.send().  All real work
executes in the existing Dramatiq worker services.

Do NOT import backend.backend_server here.  All deferred heavy execution lives
in the actors defined in backend.background_jobs.

Required env vars (same as the web tier):
  REDIS_URL or UPSTASH_REDIS_URL   — Dramatiq broker
  DATABASE_URL_RAILWAY              — DB access (for connection pool init in workers)
  SCHEDULER_SERVICE_ENABLED=1      — enables _start_audio_scheduler() in the web tier
                                     (set this on this service; set 0/unset on web replicas)

Timing env vars (all optional, defaults match original _start_audio_scheduler):
  AUDIO_SCHEDULER_TZ, AUDIO_SCHEDULER_HOUR, AUDIO_SCHEDULER_MINUTE
  ANALYTICS_PRIVATE_SCHEDULER_ENABLED, ANALYTICS_PRIVATE_SCHEDULER_HOUR/MINUTE
  GROUP_DAILY_SUMMARY_ENABLED, GROUP_DAILY_SUMMARY_HOUR/MINUTE, GROUP_SUMMARY_TZ
  GROUP_WEEKLY_SUMMARY_ENABLED, GROUP_WEEKLY_SUMMARY_DAY_OF_WEEK,
    GROUP_WEEKLY_SUMMARY_HOUR/MINUTE
  TODAY_PLAN_SCHEDULER_ENABLED, TODAY_PLAN_SCHEDULER_HOUR/MINUTE, TODAY_PLAN_TZ
  TODAY_EVENING_REMINDER_ENABLED, TODAY_EVENING_REMINDER_HOUR/MINUTE
  TRANSLATION_SESSIONS_AUTO_CLOSE_ENABLED, TRANSLATION_SESSIONS_AUTO_CLOSE_HOUR/MINUTE
  WEEKLY_GOALS_SCHEDULER_ENABLED, WEEKLY_GOALS_SCHEDULER_HOUR/MINUTE
  SEMANTIC_BENCHMARK_PREP_SCHEDULER_ENABLED (+ HOUR/MINUTE/TZ/etc.)
  SEMANTIC_AUDIT_SCHEDULER_ENABLED (+ DAY_OF_WEEK/HOUR/MINUTE/TZ)
  DB_TABLE_SIZE_REPORT_ENABLED, DB_TABLE_SIZE_REPORT_DAY_OF_WEEK, HOUR/MINUTE/TZ
  SYSTEM_MESSAGE_CLEANUP_ENABLED, SYSTEM_MESSAGE_CLEANUP_HOUR/MINUTE
  FLASHCARD_FEEL_CLEANUP_ENABLED, FLASHCARD_FEEL_CLEANUP_DAY/HOUR/MINUTE
  TTS_DB_CACHE_CLEANUP_ENABLED, TTS_DB_CACHE_CLEANUP_HOUR/MINUTE
  TTS_R2_CACHE_CLEANUP_ENABLED, TTS_R2_CACHE_CLEANUP_HOUR/MINUTE
  TTS_PREWARM_ENABLED, TTS_PREWARM_INTERVAL_MINUTES
  TTS_GENERATION_RECOVERY_ENABLED, TTS_GENERATION_RECOVERY_INTERVAL_MINUTES
  TTS_PREWARM_QUOTA_CONTROL_ENABLED, TTS_PREWARM_QUOTA_CONTROL_HOUR/MINUTE/TZ
  SENTENCE_PREWARM_ENABLED, SENTENCE_PREWARM_INTERVAL_MINUTES
  TRANSLATION_FOCUS_POOL_REFILL_ENABLED, TRANSLATION_FOCUS_POOL_REFILL_HOUR/MINUTE/TZ
  TRANSLATION_FOCUS_POOL_ADMIN_REPORT_ENABLED, ...HOUR/MINUTE/TZ
  SKILL_STATE_V2_AGGREGATION_ENABLED, SKILL_STATE_V2_AGGREGATION_INTERVAL_SECONDS
"""

import logging
import os
import signal
import sys
import time
from zoneinfo import ZoneInfo

import dramatiq

# ---------------------------------------------------------------------------
# Broker init — must happen before importing actors
# ---------------------------------------------------------------------------
from backend.job_queue import get_dramatiq_broker

dramatiq.set_broker(get_dramatiq_broker())

# ---------------------------------------------------------------------------
# Import actor objects (no heavy imports — job_queue only above)
# These are the send() targets; no backend_server import anywhere in this file.
# ---------------------------------------------------------------------------
from backend.background_jobs import (  # noqa: E402
    run_daily_audio_scheduler_actor,
    run_private_analytics_scheduler_actor,
    run_weekly_goals_scheduler_actor,
    run_daily_group_summary_scheduler_actor,
    run_weekly_group_summary_scheduler_actor,
    run_today_plan_scheduler_actor,
    run_today_evening_reminders_scheduler_actor,
    run_translation_sessions_auto_close_actor,
    run_system_message_cleanup_actor,
    run_flashcard_feel_cleanup_actor,
    run_tts_db_cache_cleanup_actor,
    run_tts_r2_cache_cleanup_actor,
    run_database_table_sizes_report_actor,
    run_tts_prewarm_scheduler_actor,
    run_tts_generation_recovery_actor,
    run_tts_prewarm_quota_control_actor,
    run_sentence_prewarm_actor,
    run_translation_focus_pool_refill_job,       # existing actor — reused
    run_translation_focus_pool_admin_report_actor,
    run_semantic_benchmark_prep_actor,
    run_semantic_audit_actor,
    run_skill_state_v2_aggregation_actor,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _tz(name: str | None, fallback: str = "UTC") -> ZoneInfo:
    raw = str(name or "").strip() or fallback
    try:
        return ZoneInfo(raw)
    except Exception:
        logging.warning("scheduler_service: invalid tz %r, falling back to %s", raw, fallback)
        return ZoneInfo(fallback)


def _int_env(key: str, default: int) -> int:
    try:
        return int((os.getenv(key) or str(default)).strip())
    except Exception:
        return default


def _enabled(key: str, default: str = "1") -> bool:
    return str(os.getenv(key) or default).strip().lower() in {"1", "true", "yes", "on"}


# ---------------------------------------------------------------------------
# Dispatch wrappers — ONLY actor.send() calls, nothing else
# ---------------------------------------------------------------------------

def _dispatch_daily_audio() -> None:
    run_daily_audio_scheduler_actor.send()


def _dispatch_private_analytics() -> None:
    run_private_analytics_scheduler_actor.send()


def _dispatch_weekly_goals() -> None:
    run_weekly_goals_scheduler_actor.send()


def _dispatch_daily_group_summary() -> None:
    run_daily_group_summary_scheduler_actor.send()


def _dispatch_weekly_group_summary() -> None:
    run_weekly_group_summary_scheduler_actor.send()


def _dispatch_today_plans() -> None:
    run_today_plan_scheduler_actor.send()


def _dispatch_today_evening_reminders() -> None:
    run_today_evening_reminders_scheduler_actor.send()


def _dispatch_translation_sessions_auto_close() -> None:
    run_translation_sessions_auto_close_actor.send()


def _dispatch_system_message_cleanup() -> None:
    run_system_message_cleanup_actor.send()


def _dispatch_flashcard_feel_cleanup() -> None:
    run_flashcard_feel_cleanup_actor.send()


def _dispatch_tts_db_cache_cleanup() -> None:
    run_tts_db_cache_cleanup_actor.send()


def _dispatch_tts_r2_cache_cleanup() -> None:
    run_tts_r2_cache_cleanup_actor.send()


def _dispatch_database_table_sizes_report() -> None:
    run_database_table_sizes_report_actor.send()


def _dispatch_tts_prewarm() -> None:
    run_tts_prewarm_scheduler_actor.send()


def _dispatch_tts_generation_recovery() -> None:
    run_tts_generation_recovery_actor.send()


def _dispatch_tts_prewarm_quota_control() -> None:
    run_tts_prewarm_quota_control_actor.send()


def _dispatch_sentence_prewarm() -> None:
    run_sentence_prewarm_actor.send()


def _dispatch_translation_focus_pool_refill() -> None:
    tz_name = str(os.getenv("TRANSLATION_FOCUS_POOL_REFILL_TZ") or "UTC").strip() or "UTC"
    run_translation_focus_pool_refill_job.send(force=False, tz_name=tz_name)


def _dispatch_translation_focus_pool_admin_report() -> None:
    run_translation_focus_pool_admin_report_actor.send()


def _dispatch_semantic_benchmark_prep() -> None:
    run_semantic_benchmark_prep_actor.send()


def _dispatch_semantic_audit() -> None:
    run_semantic_audit_actor.send()


def _dispatch_skill_state_v2_aggregation() -> None:
    run_skill_state_v2_aggregation_actor.send()


# ---------------------------------------------------------------------------
# Scheduler setup
# ---------------------------------------------------------------------------

def _build_scheduler():
    try:
        from apscheduler.schedulers.background import BackgroundScheduler
    except ImportError:
        logging.error("scheduler_service: APScheduler is not installed; cannot start")
        raise

    default_tz_name = str(os.getenv("AUDIO_SCHEDULER_TZ") or "UTC").strip() or "UTC"
    scheduler = BackgroundScheduler(timezone=default_tz_name)

    # -- Daily audio --
    scheduler.add_job(
        _dispatch_daily_audio,
        "cron",
        hour=_int_env("AUDIO_SCHEDULER_HOUR", 13),
        minute=_int_env("AUDIO_SCHEDULER_MINUTE", 0),
        max_instances=1,
        coalesce=True,
        misfire_grace_time=300,
    )

    # -- Private analytics --
    if _enabled("ANALYTICS_PRIVATE_SCHEDULER_ENABLED"):
        scheduler.add_job(
            _dispatch_private_analytics,
            "cron",
            hour=_int_env("ANALYTICS_PRIVATE_SCHEDULER_HOUR", 19),
            minute=_int_env("ANALYTICS_PRIVATE_SCHEDULER_MINUTE", 30),
            max_instances=1,
            coalesce=True,
            misfire_grace_time=300,
        )

    # -- Daily group summary --
    if _enabled("GROUP_DAILY_SUMMARY_ENABLED"):
        scheduler.add_job(
            _dispatch_daily_group_summary,
            "cron",
            hour=_int_env("GROUP_DAILY_SUMMARY_HOUR", 22),
            minute=_int_env("GROUP_DAILY_SUMMARY_MINUTE", 30),
            timezone=_tz(os.getenv("GROUP_SUMMARY_TZ") or default_tz_name),
            max_instances=1,
            coalesce=True,
            misfire_grace_time=300,
        )

    # -- Weekly group summary --
    if _enabled("GROUP_WEEKLY_SUMMARY_ENABLED"):
        scheduler.add_job(
            _dispatch_weekly_group_summary,
            "cron",
            day_of_week=str(os.getenv("GROUP_WEEKLY_SUMMARY_DAY_OF_WEEK") or "sun").strip() or "sun",
            hour=_int_env("GROUP_WEEKLY_SUMMARY_HOUR", 22),
            minute=_int_env("GROUP_WEEKLY_SUMMARY_MINUTE", 35),
            timezone=_tz(os.getenv("GROUP_SUMMARY_TZ") or default_tz_name),
            max_instances=1,
            coalesce=True,
            misfire_grace_time=300,
        )

    # -- Today plan --
    if _enabled("TODAY_PLAN_SCHEDULER_ENABLED"):
        scheduler.add_job(
            _dispatch_today_plans,
            "cron",
            hour=_int_env("TODAY_PLAN_SCHEDULER_HOUR", 7),
            minute=_int_env("TODAY_PLAN_SCHEDULER_MINUTE", 0),
            max_instances=1,
            coalesce=True,
            misfire_grace_time=300,
        )

    # -- Today evening reminders --
    if _enabled("TODAY_EVENING_REMINDER_ENABLED"):
        scheduler.add_job(
            _dispatch_today_evening_reminders,
            "cron",
            hour=_int_env("TODAY_EVENING_REMINDER_HOUR", 18),
            minute=_int_env("TODAY_EVENING_REMINDER_MINUTE", 0),
            max_instances=1,
            coalesce=True,
            misfire_grace_time=300,
        )

    # -- Translation sessions auto-close --
    if _enabled("TRANSLATION_SESSIONS_AUTO_CLOSE_ENABLED"):
        scheduler.add_job(
            _dispatch_translation_sessions_auto_close,
            "cron",
            hour=_int_env("TRANSLATION_SESSIONS_AUTO_CLOSE_HOUR", 23),
            minute=_int_env("TRANSLATION_SESSIONS_AUTO_CLOSE_MINUTE", 59),
            timezone=_tz(os.getenv("TODAY_PLAN_TZ") or default_tz_name),
            max_instances=1,
            coalesce=True,
            misfire_grace_time=300,
        )

    # -- Weekly goals --
    if _enabled("WEEKLY_GOALS_SCHEDULER_ENABLED"):
        scheduler.add_job(
            _dispatch_weekly_goals,
            "cron",
            hour=_int_env("WEEKLY_GOALS_SCHEDULER_HOUR", 6),
            minute=_int_env("WEEKLY_GOALS_SCHEDULER_MINUTE", 45),
            max_instances=1,
            coalesce=True,
            misfire_grace_time=300,
        )

    # -- Semantic benchmark prep --
    if _enabled("SEMANTIC_BENCHMARK_PREP_SCHEDULER_ENABLED", "0"):
        scheduler.add_job(
            _dispatch_semantic_benchmark_prep,
            "cron",
            hour=_int_env("SEMANTIC_BENCHMARK_PREP_SCHEDULER_HOUR", 2),
            minute=_int_env("SEMANTIC_BENCHMARK_PREP_SCHEDULER_MINUTE", 0),
            timezone=_tz(os.getenv("SEMANTIC_BENCHMARK_PREP_SCHEDULER_TZ") or default_tz_name),
            max_instances=1,
            coalesce=True,
            misfire_grace_time=1800,
        )

    # -- Semantic audit --
    if _enabled("SEMANTIC_AUDIT_SCHEDULER_ENABLED", "0"):
        scheduler.add_job(
            _dispatch_semantic_audit,
            "cron",
            day_of_week=str(os.getenv("SEMANTIC_AUDIT_SCHEDULER_DAY_OF_WEEK") or "mon").strip() or "mon",
            hour=_int_env("SEMANTIC_AUDIT_SCHEDULER_HOUR", 3),
            minute=_int_env("SEMANTIC_AUDIT_SCHEDULER_MINUTE", 0),
            timezone=_tz(os.getenv("SEMANTIC_AUDIT_SCHEDULER_TZ") or default_tz_name),
            max_instances=1,
            coalesce=True,
            misfire_grace_time=1800,
        )

    # -- DB table size report --
    if _enabled("DB_TABLE_SIZE_REPORT_ENABLED"):
        scheduler.add_job(
            _dispatch_database_table_sizes_report,
            "cron",
            day_of_week=str(os.getenv("DB_TABLE_SIZE_REPORT_DAY_OF_WEEK") or "tue,fri").strip() or "tue,fri",
            hour=_int_env("DB_TABLE_SIZE_REPORT_HOUR", 17),
            minute=_int_env("DB_TABLE_SIZE_REPORT_MINUTE", 0),
            timezone=_tz(os.getenv("DB_TABLE_SIZE_REPORT_TZ") or default_tz_name),
            max_instances=1,
            coalesce=True,
            misfire_grace_time=3600,
        )

    # -- System message cleanup --
    if _enabled("SYSTEM_MESSAGE_CLEANUP_ENABLED"):
        scheduler.add_job(
            _dispatch_system_message_cleanup,
            "cron",
            hour=_int_env("SYSTEM_MESSAGE_CLEANUP_HOUR", 23),
            minute=_int_env("SYSTEM_MESSAGE_CLEANUP_MINUTE", 59),
            max_instances=1,
            coalesce=True,
            misfire_grace_time=300,
        )

    # -- Flashcard feel cleanup --
    if _enabled("FLASHCARD_FEEL_CLEANUP_ENABLED"):
        scheduler.add_job(
            _dispatch_flashcard_feel_cleanup,
            "cron",
            day=_int_env("FLASHCARD_FEEL_CLEANUP_DAY", 1),
            hour=_int_env("FLASHCARD_FEEL_CLEANUP_HOUR", 3),
            minute=_int_env("FLASHCARD_FEEL_CLEANUP_MINUTE", 30),
            max_instances=1,
            coalesce=True,
            misfire_grace_time=3600,
        )

    # -- TTS DB cache cleanup --
    if _enabled("TTS_DB_CACHE_CLEANUP_ENABLED"):
        scheduler.add_job(
            _dispatch_tts_db_cache_cleanup,
            "cron",
            hour=_int_env("TTS_DB_CACHE_CLEANUP_HOUR", 4),
            minute=_int_env("TTS_DB_CACHE_CLEANUP_MINUTE", 10),
            max_instances=1,
            coalesce=True,
            misfire_grace_time=3600,
        )

    # -- TTS R2 cache cleanup --
    if _enabled("TTS_R2_CACHE_CLEANUP_ENABLED"):
        scheduler.add_job(
            _dispatch_tts_r2_cache_cleanup,
            "cron",
            hour=_int_env("TTS_R2_CACHE_CLEANUP_HOUR", 4),
            minute=_int_env("TTS_R2_CACHE_CLEANUP_MINUTE", 20),
            max_instances=1,
            coalesce=True,
            misfire_grace_time=3600,
        )

    # -- TTS prewarm (interval) --
    if _enabled("TTS_PREWARM_ENABLED", "0"):
        scheduler.add_job(
            _dispatch_tts_prewarm,
            "interval",
            minutes=_int_env("TTS_PREWARM_INTERVAL_MINUTES", 30),
            max_instances=1,
            coalesce=True,
            misfire_grace_time=120,
        )

    # -- TTS generation recovery (interval) --
    if _enabled("TTS_GENERATION_RECOVERY_ENABLED", "0"):
        scheduler.add_job(
            _dispatch_tts_generation_recovery,
            "interval",
            minutes=_int_env("TTS_GENERATION_RECOVERY_INTERVAL_MINUTES", 10),
            max_instances=1,
            coalesce=True,
            misfire_grace_time=120,
        )

    # -- TTS prewarm quota control --
    if _enabled("TTS_PREWARM_QUOTA_CONTROL_ENABLED", "0"):
        scheduler.add_job(
            _dispatch_tts_prewarm_quota_control,
            "cron",
            hour=_int_env("TTS_PREWARM_QUOTA_CONTROL_HOUR", 0),
            minute=_int_env("TTS_PREWARM_QUOTA_CONTROL_MINUTE", 5),
            timezone=_tz(os.getenv("TTS_PREWARM_QUOTA_CONTROL_TZ") or default_tz_name),
            max_instances=1,
            coalesce=True,
            misfire_grace_time=900,
        )

    # -- Sentence prewarm (interval) --
    if _enabled("SENTENCE_PREWARM_ENABLED", "0"):
        scheduler.add_job(
            _dispatch_sentence_prewarm,
            "interval",
            minutes=_int_env("SENTENCE_PREWARM_INTERVAL_MINUTES", 60),
            max_instances=1,
            coalesce=True,
            misfire_grace_time=180,
        )

    # -- Translation focus pool refill --
    if _enabled("TRANSLATION_FOCUS_POOL_REFILL_ENABLED", "0"):
        scheduler.add_job(
            _dispatch_translation_focus_pool_refill,
            "cron",
            hour=_int_env("TRANSLATION_FOCUS_POOL_REFILL_HOUR", 3),
            minute=_int_env("TRANSLATION_FOCUS_POOL_REFILL_MINUTE", 0),
            timezone=_tz(os.getenv("TRANSLATION_FOCUS_POOL_REFILL_TZ") or default_tz_name),
            max_instances=1,
            coalesce=True,
            misfire_grace_time=1800,
        )

    # -- Translation focus pool admin report --
    if _enabled("TRANSLATION_FOCUS_POOL_ADMIN_REPORT_ENABLED", "0"):
        scheduler.add_job(
            _dispatch_translation_focus_pool_admin_report,
            "cron",
            hour=_int_env("TRANSLATION_FOCUS_POOL_ADMIN_REPORT_HOUR", 8),
            minute=_int_env("TRANSLATION_FOCUS_POOL_ADMIN_REPORT_MINUTE", 0),
            timezone=_tz(os.getenv("TRANSLATION_FOCUS_POOL_ADMIN_REPORT_TZ") or default_tz_name),
            max_instances=1,
            coalesce=True,
            misfire_grace_time=1800,
        )

    # -- Skill state V2 aggregation (interval) --
    if _enabled("SKILL_STATE_V2_AGGREGATION_ENABLED", "0"):
        interval_secs = max(10, _int_env("SKILL_STATE_V2_AGGREGATION_INTERVAL_SECONDS", 60))
        scheduler.add_job(
            _dispatch_skill_state_v2_aggregation,
            "interval",
            seconds=interval_secs,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=max(10, interval_secs),
        )

    return scheduler


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> int:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    logging.info("scheduler_service: starting")

    scheduler = _build_scheduler()

    should_stop = {"value": False}

    def _handle_stop(signum, _frame) -> None:
        logging.info("scheduler_service: received signal %s, stopping", signum)
        should_stop["value"] = True

    signal.signal(signal.SIGTERM, _handle_stop)
    signal.signal(signal.SIGINT, _handle_stop)

    scheduler.start()
    logging.info("scheduler_service: APScheduler started with %d jobs", len(scheduler.get_jobs()))

    while not should_stop["value"]:
        time.sleep(1)

    logging.info("scheduler_service: shutting down APScheduler")
    scheduler.shutdown(wait=False)
    logging.info("scheduler_service: stopped")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
