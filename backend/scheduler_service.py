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
  ADMIN_ECONOMICS_REPORT_ENABLED, ADMIN_ECONOMICS_REPORT_HOUR/MINUTE/TZ
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
  TRANSLATION_CHECK_WORKER_SCHEDULE_ENABLED, ...START_TIMES/STOP_TIMES/TIMEZONE/IDLE_STOP_GRACE_MINUTES
  BACKGROUND_JOBS_RESOURCE_SCHEDULE_ENABLED, ...START_TIMES/STOP_TIMES/TIMEZONE/RECONCILE_MINUTES/DAY_PROFILE/NIGHT_PROFILE
  AUX_BACKGROUND_WORKER_RESOURCE_SCHEDULE_ENABLED, ...START_TIMES/STOP_TIMES/TIMEZONE/RECONCILE_MINUTES/DAY_PROFILE/NIGHT_PROFILE
"""

import logging
import os
import signal
import sys
import time
from datetime import time as dt_time
from uuid import uuid4
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
    run_image_quiz_r2_cleanup_actor,
    run_visual_riddle_r2_cleanup_actor,
    run_database_table_sizes_report_actor,
    run_admin_economics_report_actor,
    run_tts_prewarm_scheduler_actor,
    run_tts_generation_recovery_actor,
    run_tts_prewarm_quota_control_actor,
    run_sentence_prewarm_actor,
    run_translation_focus_pool_refill_job,       # existing actor — reused
    run_translation_focus_pool_admin_report_actor,
    run_weekly_global_ranking_report_actor,
    run_semantic_benchmark_prep_actor,
    run_semantic_audit_actor,
    run_skill_state_v2_aggregation_actor,
    run_agent_worker_schedule_control_actor,
    run_translation_check_worker_schedule_control_actor,
    run_service_resource_schedule_control_actor,
    run_autosave_sweep_job,
    run_dictionary_dedup_actor,
    run_analytics_snapshot_precompute_actor,
    run_shortcut_pairing_code_cleanup_actor,
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


def _float_env(key: str, default: float) -> float:
    try:
        return float((os.getenv(key) or str(default)).strip())
    except Exception:
        return default


def _enabled(key: str, default: str = "1") -> bool:
    return str(os.getenv(key) or default).strip().lower() in {"1", "true", "yes", "on"}


def _parse_hhmm(value: str) -> dt_time:
    parts = value.split(":", 1)
    if len(parts) != 2:
        raise ValueError(f"invalid time {value!r}")
    hour = int(parts[0])
    minute = int(parts[1])
    if hour < 0 or hour > 23 or minute < 0 or minute > 59:
        raise ValueError(f"invalid time {value!r}")
    return dt_time(hour=hour, minute=minute)


def _parse_hhmm_list(raw: str | None) -> list[dt_time]:
    values: list[dt_time] = []
    for item in str(raw or "").split(","):
        candidate = item.strip()
        if not candidate:
            continue
        values.append(_parse_hhmm(candidate))
    return values


def _agent_worker_schedule_timezone_name() -> str:
    return (os.getenv("AGENT_WORKER_TIMEZONE") or "Europe/Vienna").strip() or "Europe/Vienna"


def _agent_worker_idle_stop_grace_minutes() -> int:
    return max(1, _int_env("AGENT_WORKER_IDLE_STOP_GRACE_MINUTES", 10))


def _translation_check_worker_schedule_timezone_name() -> str:
    return (os.getenv("TRANSLATION_CHECK_WORKER_TIMEZONE") or "Europe/Vienna").strip() or "Europe/Vienna"


def _translation_check_worker_idle_stop_grace_minutes() -> int:
    return max(1, _int_env("TRANSLATION_CHECK_WORKER_IDLE_STOP_GRACE_MINUTES", 10))


def _resource_schedule_timezone_name(service_name: str) -> str:
    return (os.getenv(f"{service_name}_RESOURCE_SCHEDULE_TIMEZONE") or "Europe/Vienna").strip() or "Europe/Vienna"


def _resource_schedule_reconcile_minutes(service_name: str) -> int:
    return max(1, _int_env(f"{service_name}_RESOURCE_SCHEDULE_RECONCILE_MINUTES", 10))


def _resource_schedule_enabled(service_name: str) -> bool:
    return _enabled(f"{service_name}_RESOURCE_SCHEDULE_ENABLED", "0")


def _resource_schedule_windows(service_name: str) -> list[tuple[dt_time, dt_time]]:
    start_times = _parse_hhmm_list(os.getenv(f"{service_name}_RESOURCE_SCHEDULE_START_TIMES") or "")
    stop_times = _parse_hhmm_list(os.getenv(f"{service_name}_RESOURCE_SCHEDULE_STOP_TIMES") or "")
    if len(start_times) != len(stop_times):
        raise ValueError(
            f"{service_name}_RESOURCE_SCHEDULE_START_TIMES count {len(start_times)} does not match "
            f"{service_name}_RESOURCE_SCHEDULE_STOP_TIMES count {len(stop_times)}"
        )
    return list(zip(start_times, stop_times))


# ---------------------------------------------------------------------------
# Dispatch wrappers — ONLY actor.send() calls, nothing else
# ---------------------------------------------------------------------------

def _dispatch_daily_audio() -> None:
    run_daily_audio_scheduler_actor.send()


def _dispatch_dictionary_dedup() -> None:
    run_dictionary_dedup_actor.send()


def _dispatch_analytics_snapshot_precompute() -> None:
    run_analytics_snapshot_precompute_actor.send()


def _dispatch_shortcut_pairing_code_cleanup() -> None:
    run_shortcut_pairing_code_cleanup_actor.send()


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


def _dispatch_image_quiz_r2_cleanup() -> None:
    run_image_quiz_r2_cleanup_actor.send()


def _dispatch_visual_riddle_r2_cleanup() -> None:
    run_visual_riddle_r2_cleanup_actor.send()


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
    tz_name = str(
        os.getenv("TRANSLATION_FOCUS_POOL_REFILL_TZ")
        or os.getenv("AUDIO_SCHEDULER_TZ")
        or "UTC"
    ).strip() or "UTC"
    request_id = f"translation_pool_refill_sched_{uuid4().hex[:16]}"
    # Run the refill INLINE in the scheduler process (force=True) instead of
    # enqueuing to the Dramatiq "translation_pool_refill" queue. No worker is
    # subscribed to that queue, so the queued path never executed (Δ +0 daily).
    # The scheduler cron already controls timing, so force=True is correct here
    # and skips the redundant offpeak re-check. On-demand/deficit refills still
    # use the queue path (now consumable by the worker via DRAMATIQ_QUEUES).
    logging.info(
        "scheduler_service: translation_focus_pool_refill inline_start request_id=%s tz_name=%s enabled_env=%r hour_env=%r minute_env=%r",
        request_id,
        tz_name,
        os.getenv("TRANSLATION_FOCUS_POOL_REFILL_ENABLED"),
        os.getenv("TRANSLATION_FOCUS_POOL_REFILL_HOUR"),
        os.getenv("TRANSLATION_FOCUS_POOL_REFILL_MINUTE"),
    )
    try:
        from backend.backend_server import _dispatch_translation_focus_pool_refill as _run_refill_inline
        result = _run_refill_inline(force=True, tz_name=tz_name)
        logging.info(
            "scheduler_service: translation_focus_pool_refill inline_finish request_id=%s tz_name=%s result=%s",
            request_id,
            tz_name,
            result,
        )
    except Exception:
        logging.exception(
            "scheduler_service: translation_focus_pool_refill inline_failed request_id=%s tz_name=%s",
            request_id,
            tz_name,
        )


def _dispatch_translation_focus_pool_admin_report() -> None:
    run_translation_focus_pool_admin_report_actor.send()


def _dispatch_admin_economics_report() -> None:
    run_admin_economics_report_actor.send()


def _dispatch_weekly_global_ranking_report() -> None:
    tz_name = str(os.getenv("WEEKLY_GLOBAL_RANKING_TZ") or "Europe/Vienna").strip() or "Europe/Vienna"
    request_id = f"weekly_global_ranking_{uuid4().hex[:16]}"
    logging.info(
        "scheduler_service: weekly_global_ranking enqueue_start request_id=%s tz_name=%s day_env=%r hour_env=%r minute_env=%r",
        request_id,
        tz_name,
        os.getenv("WEEKLY_GLOBAL_RANKING_DAY_OF_WEEK"),
        os.getenv("WEEKLY_GLOBAL_RANKING_HOUR"),
        os.getenv("WEEKLY_GLOBAL_RANKING_MINUTE"),
    )
    message = run_weekly_global_ranking_report_actor.send(
        request_id=request_id,
        correlation_id=request_id,
        tz_name=tz_name,
    )
    logging.info(
        "scheduler_service: weekly_global_ranking enqueue_finish request_id=%s message_id=%s tz_name=%s",
        request_id,
        str(getattr(message, "message_id", None) or "").strip() or None,
        tz_name,
    )


def _dispatch_semantic_benchmark_prep() -> None:
    run_semantic_benchmark_prep_actor.send()


def _dispatch_semantic_audit() -> None:
    run_semantic_audit_actor.send()


def _dispatch_skill_state_v2_aggregation() -> None:
    run_skill_state_v2_aggregation_actor.send()


def _dispatch_autosave_sweep() -> None:
    run_autosave_sweep_job.send()


def _dispatch_agent_worker_schedule_start() -> None:
    run_agent_worker_schedule_control_actor.send(action="start")


def _dispatch_agent_worker_schedule_stop() -> None:
    run_agent_worker_schedule_control_actor.send(action="stop")


def _dispatch_agent_worker_schedule_reconcile_stop() -> None:
    run_agent_worker_schedule_control_actor.send(action="reconcile_stop")


def _dispatch_translation_check_worker_schedule_start() -> None:
    run_translation_check_worker_schedule_control_actor.send(action="start")


def _dispatch_translation_check_worker_schedule_stop() -> None:
    run_translation_check_worker_schedule_control_actor.send(action="stop")


def _dispatch_translation_check_worker_schedule_reconcile_stop() -> None:
    run_translation_check_worker_schedule_control_actor.send(action="reconcile_stop")


def _dispatch_service_resource_schedule_control(service_name: str, action: str) -> None:
    run_service_resource_schedule_control_actor.send(service_name=service_name, action=action)


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

    # -- Dictionary dedup (re-homed; was dropped in the scheduler migration) --
    if _enabled("DICT_DEDUP_ENABLED"):
        scheduler.add_job(
            _dispatch_dictionary_dedup,
            "cron",
            hour=_int_env("DICT_DEDUP_HOUR", 3),
            minute=_int_env("DICT_DEDUP_MINUTE", 40),
            max_instances=1,
            coalesce=True,
            misfire_grace_time=3600,
        )

    # -- Analytics snapshot precompute (re-homed) --
    if _enabled("ANALYTICS_SNAPSHOT_SCHEDULER_ENABLED"):
        scheduler.add_job(
            _dispatch_analytics_snapshot_precompute,
            "cron",
            hour=_int_env("ANALYTICS_SNAPSHOT_SCHEDULER_HOUR", 4),
            minute=_int_env("ANALYTICS_SNAPSHOT_SCHEDULER_MINUTE", 0),
            timezone=_tz(os.getenv("ANALYTICS_SNAPSHOT_SCHEDULER_TZ") or default_tz_name),
            max_instances=1,
            coalesce=True,
            misfire_grace_time=3600,
        )

    # -- Shortcut pairing-code cleanup (re-homed) --
    if _enabled("SHORTCUT_PAIRING_CODE_CLEANUP_ENABLED"):
        scheduler.add_job(
            _dispatch_shortcut_pairing_code_cleanup,
            "cron",
            hour=_int_env("SHORTCUT_PAIRING_CODE_CLEANUP_HOUR", 3),
            minute=_int_env("SHORTCUT_PAIRING_CODE_CLEANUP_MINUTE", 15),
            max_instances=1,
            coalesce=True,
            misfire_grace_time=3600,
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

    # -- Weekly global ranking cards --
    if _enabled("WEEKLY_GLOBAL_RANKING_ENABLED", "1"):
        scheduler.add_job(
            _dispatch_weekly_global_ranking_report,
            "cron",
            day_of_week=str(os.getenv("WEEKLY_GLOBAL_RANKING_DAY_OF_WEEK") or "sun").strip() or "sun",
            hour=_int_env("WEEKLY_GLOBAL_RANKING_HOUR", 6),
            minute=_int_env("WEEKLY_GLOBAL_RANKING_MINUTE", 40),
            timezone=_tz(os.getenv("WEEKLY_GLOBAL_RANKING_TZ") or "Europe/Vienna"),
            max_instances=1,
            coalesce=True,
            misfire_grace_time=1800,
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

    # -- Admin economics and limits report --
    if _enabled("ADMIN_ECONOMICS_REPORT_ENABLED", "1"):
        scheduler.add_job(
            _dispatch_admin_economics_report,
            "cron",
            hour=_int_env("ADMIN_ECONOMICS_REPORT_HOUR", 23),
            minute=_int_env("ADMIN_ECONOMICS_REPORT_MINUTE", 0),
            timezone=_tz(os.getenv("ADMIN_ECONOMICS_REPORT_TZ") or "Europe/Vienna"),
            max_instances=1,
            coalesce=True,
            misfire_grace_time=1800,
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

    # -- Image-quiz R2 cleanup (weekly) --
    if _enabled("IMAGE_QUIZ_R2_CLEANUP_ENABLED"):
        scheduler.add_job(
            _dispatch_image_quiz_r2_cleanup,
            "cron",
            day_of_week=str(os.getenv("IMAGE_QUIZ_R2_CLEANUP_DAY_OF_WEEK") or "sun").strip() or "sun",
            hour=_int_env("IMAGE_QUIZ_R2_CLEANUP_HOUR", 3),
            minute=_int_env("IMAGE_QUIZ_R2_CLEANUP_MINUTE", 0),
            max_instances=1,
            coalesce=True,
            misfire_grace_time=3600,
        )

    # -- Visual-riddle R2 cleanup (weekly) --
    if _enabled("VISUAL_RIDDLE_R2_CLEANUP_ENABLED", "1"):
        scheduler.add_job(
            _dispatch_visual_riddle_r2_cleanup,
            "cron",
            day_of_week=str(os.getenv("VISUAL_RIDDLE_R2_CLEANUP_DAY_OF_WEEK") or "sun").strip() or "sun",
            hour=_int_env("VISUAL_RIDDLE_R2_CLEANUP_HOUR", 3),
            minute=_int_env("VISUAL_RIDDLE_R2_CLEANUP_MINUTE", 30),
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
    if _enabled("TRANSLATION_FOCUS_POOL_REFILL_ENABLED", "1"):
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

    # -- Nightly auto-save debounce sweep (interval) --
    # Fans out a worker flush for every user whose Shortcut batch went quiet. Cheap one-pass
    # scan; debounce precision is the interval (default 30s). On by default — the feature is
    # inert (no flush-at keys) unless a user has auto-save ON.
    if _enabled("AUTOSAVE_SWEEP_ENABLED", "1"):
        autosave_interval_secs = max(10, _int_env("AUTOSAVE_SWEEP_INTERVAL_SECONDS", 30))
        scheduler.add_job(
            _dispatch_autosave_sweep,
            "interval",
            seconds=autosave_interval_secs,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=max(10, autosave_interval_secs),
        )

    # -- AGENT_WORKER scheduled uptime --
    if _enabled("AGENT_WORKER_SCHEDULE_ENABLED", "0"):
        try:
            start_times = _parse_hhmm_list(os.getenv("AGENT_WORKER_START_TIMES") or "06:55,15:55")
            stop_times = _parse_hhmm_list(os.getenv("AGENT_WORKER_STOP_TIMES") or "10:00,19:00")
            if len(start_times) != len(stop_times):
                raise ValueError(
                    f"AGENT_WORKER_START_TIMES count {len(start_times)} does not match "
                    f"AGENT_WORKER_STOP_TIMES count {len(stop_times)}"
                )
            schedule_windows = list(zip(start_times, stop_times))
        except Exception:
            logging.exception("scheduler_service: failed to parse AGENT_WORKER schedule")
            schedule_windows = []
        if schedule_windows:
            agent_worker_tz_name = _agent_worker_schedule_timezone_name()
            agent_worker_tz = _tz(agent_worker_tz_name, default_tz_name)
            for index, (start_time, stop_time) in enumerate(schedule_windows):
                scheduler.add_job(
                    _dispatch_agent_worker_schedule_start,
                    "cron",
                    hour=start_time.hour,
                    minute=start_time.minute,
                    timezone=agent_worker_tz,
                    id=f"agent_worker_start_{index}",
                    max_instances=1,
                    coalesce=True,
                    misfire_grace_time=300,
                )
                scheduler.add_job(
                    _dispatch_agent_worker_schedule_stop,
                    "cron",
                    hour=stop_time.hour,
                    minute=stop_time.minute,
                    timezone=agent_worker_tz,
                    id=f"agent_worker_stop_{index}",
                    max_instances=1,
                    coalesce=True,
                    misfire_grace_time=300,
                )
            scheduler.add_job(
                _dispatch_agent_worker_schedule_reconcile_stop,
                "interval",
                minutes=_agent_worker_idle_stop_grace_minutes(),
                id="agent_worker_reconcile_stop",
                max_instances=1,
                coalesce=True,
                misfire_grace_time=180,
            )
            logging.info(
                "scheduler_service: AGENT_WORKER schedule enabled tz=%s windows=%s reconcile_minutes=%s",
                agent_worker_tz_name,
                [f"{start.strftime('%H:%M')}-{stop.strftime('%H:%M')}" for start, stop in schedule_windows],
                _agent_worker_idle_stop_grace_minutes(),
            )
        else:
            logging.warning("scheduler_service: AGENT_WORKER schedule enabled but no valid windows were configured")

    # -- TRANSLATION_CHECK_WORKER scheduled uptime --
    if _enabled("TRANSLATION_CHECK_WORKER_SCHEDULE_ENABLED", "0"):
        try:
            start_times = _parse_hhmm_list(os.getenv("TRANSLATION_CHECK_WORKER_START_TIMES") or "06:30")
            stop_times = _parse_hhmm_list(os.getenv("TRANSLATION_CHECK_WORKER_STOP_TIMES") or "00:30")
            if len(start_times) != len(stop_times):
                raise ValueError(
                    f"TRANSLATION_CHECK_WORKER_START_TIMES count {len(start_times)} does not match "
                    f"TRANSLATION_CHECK_WORKER_STOP_TIMES count {len(stop_times)}"
                )
            schedule_windows = list(zip(start_times, stop_times))
        except Exception:
            logging.exception("scheduler_service: failed to parse TRANSLATION_CHECK_WORKER schedule")
            schedule_windows = []
        if schedule_windows:
            translation_check_worker_tz_name = _translation_check_worker_schedule_timezone_name()
            translation_check_worker_tz = _tz(translation_check_worker_tz_name, default_tz_name)
            for index, (start_time, stop_time) in enumerate(schedule_windows):
                scheduler.add_job(
                    _dispatch_translation_check_worker_schedule_start,
                    "cron",
                    hour=start_time.hour,
                    minute=start_time.minute,
                    timezone=translation_check_worker_tz,
                    id=f"translation_check_worker_start_{index}",
                    max_instances=1,
                    coalesce=True,
                    misfire_grace_time=300,
                )
                scheduler.add_job(
                    _dispatch_translation_check_worker_schedule_stop,
                    "cron",
                    hour=stop_time.hour,
                    minute=stop_time.minute,
                    timezone=translation_check_worker_tz,
                    id=f"translation_check_worker_stop_{index}",
                    max_instances=1,
                    coalesce=True,
                    misfire_grace_time=300,
                )
            scheduler.add_job(
                _dispatch_translation_check_worker_schedule_reconcile_stop,
                "interval",
                minutes=_translation_check_worker_idle_stop_grace_minutes(),
                id="translation_check_worker_reconcile_stop",
                max_instances=1,
                coalesce=True,
                misfire_grace_time=180,
            )
            logging.info(
                "scheduler_service: TRANSLATION_CHECK_WORKER schedule enabled tz=%s windows=%s reconcile_minutes=%s",
                translation_check_worker_tz_name,
                [f"{start.strftime('%H:%M')}-{stop.strftime('%H:%M')}" for start, stop in schedule_windows],
                _translation_check_worker_idle_stop_grace_minutes(),
            )
        else:
            logging.warning(
                "scheduler_service: TRANSLATION_CHECK_WORKER schedule enabled but no valid windows were configured"
            )

    # -- Service resource profile schedules --
    for service_name in ("BACKGROUND_JOBS", "AUX_BACKGROUND_WORKER"):
        if not _resource_schedule_enabled(service_name):
            continue
        try:
            schedule_windows = _resource_schedule_windows(service_name)
        except Exception:
            logging.exception(
                "scheduler_service: failed to parse %s resource schedule",
                service_name,
            )
            schedule_windows = []
        if schedule_windows:
            resource_tz_name = _resource_schedule_timezone_name(service_name)
            resource_tz = _tz(resource_tz_name, default_tz_name)
            for index, (start_time, stop_time) in enumerate(schedule_windows):
                scheduler.add_job(
                    _dispatch_service_resource_schedule_control,
                    "cron",
                    hour=start_time.hour,
                    minute=start_time.minute,
                    timezone=resource_tz,
                    kwargs={"service_name": service_name, "action": "day"},
                    id=f"{service_name.lower()}_resource_day_{index}",
                    max_instances=1,
                    coalesce=True,
                    misfire_grace_time=300,
                )
                scheduler.add_job(
                    _dispatch_service_resource_schedule_control,
                    "cron",
                    hour=stop_time.hour,
                    minute=stop_time.minute,
                    timezone=resource_tz,
                    kwargs={"service_name": service_name, "action": "night"},
                    id=f"{service_name.lower()}_resource_night_{index}",
                    max_instances=1,
                    coalesce=True,
                    misfire_grace_time=300,
                )
            scheduler.add_job(
                _dispatch_service_resource_schedule_control,
                "interval",
                minutes=_resource_schedule_reconcile_minutes(service_name),
                kwargs={"service_name": service_name, "action": "reconcile"},
                id=f"{service_name.lower()}_resource_reconcile",
                max_instances=1,
                coalesce=True,
                misfire_grace_time=180,
            )
            logging.info(
                "scheduler_service: %s resource schedule enabled tz=%s windows=%s reconcile_minutes=%s",
                service_name,
                resource_tz_name,
                [f"{start.strftime('%H:%M')}-{stop.strftime('%H:%M')}" for start, stop in schedule_windows],
                _resource_schedule_reconcile_minutes(service_name),
            )
        else:
            logging.warning(
                "scheduler_service: %s resource schedule enabled but no valid windows were configured",
                service_name,
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
