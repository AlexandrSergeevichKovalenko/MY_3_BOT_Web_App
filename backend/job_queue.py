import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

import dramatiq
from dramatiq.brokers.stub import StubBroker

try:
    from dramatiq.brokers.redis import RedisBroker
except Exception:  # pragma: no cover - optional until redis extras are installed
    RedisBroker = None

try:
    import redis
except Exception:  # pragma: no cover - optional until redis is installed
    redis = None


_BROKER = None
_REDIS_CLIENT = None
_TTS_GENERATION_IN_FLIGHT_TTL_SEC = max(
    10, min(600, int((os.getenv("TTS_GENERATION_IN_FLIGHT_TTL_SEC") or "120").strip() or "120"))
)
_YOUTUBE_TRANSCRIPT_JOB_TTL_SEC = max(
    300, int((os.getenv("YOUTUBE_TRANSCRIPT_JOB_TTL_SEC") or "1800").strip())
)
_YOUTUBE_TRANSCRIPT_READY_TTL_SEC = max(
    300, int((os.getenv("YOUTUBE_TRANSCRIPT_READY_TTL_SEC") or "3600").strip())
)
_YOUTUBE_TRANSCRIPT_FAILED_TTL_SEC = max(
    60, int((os.getenv("YOUTUBE_TRANSCRIPT_FAILED_TTL_SEC") or "300").strip())
)
_TRANSLATION_CHECK_JOB_TTL_SEC = max(
    300, int((os.getenv("TRANSLATION_CHECK_JOB_TTL_SEC") or "3600").strip())
)
_TRANSLATION_CHECK_READY_TTL_SEC = max(
    60, int((os.getenv("TRANSLATION_CHECK_READY_TTL_SEC") or "600").strip())
)
_TRANSLATION_CHECK_FAILED_TTL_SEC = max(
    60, int((os.getenv("TRANSLATION_CHECK_FAILED_TTL_SEC") or "600").strip())
)
_TRANSLATION_CHECK_PENDING_STALE_REQUEUE_MS = max(
    30000,
    int((os.getenv("TRANSLATION_CHECK_PENDING_STALE_REQUEUE_MS") or "90000").strip() or "90000"),
)
_TRANSLATION_CHECK_RUNNING_STALE_REQUEUE_MS = max(
    60000,
    int((os.getenv("TRANSLATION_CHECK_RUNNING_STALE_REQUEUE_MS") or "180000").strip() or "180000"),
)
_TRANSLATION_CHECK_FASTPATH_FRESH_MS = max(
    3000,
    int((os.getenv("TRANSLATION_CHECK_FASTPATH_FRESH_MS") or "15000").strip() or "15000"),
)
_TRANSLATION_CHECK_RESUME_COOLDOWN_SEC = max(
    1,
    int((os.getenv("TRANSLATION_CHECK_RESUME_COOLDOWN_SEC") or "8").strip() or "8"),
)
_TRANSLATION_CHECK_COMPLETION_ENQUEUE_TTL_SEC = max(
    30,
    int((os.getenv("TRANSLATION_CHECK_COMPLETION_ENQUEUE_TTL_SEC") or "120").strip() or "120"),
)
_ACTIVE_TRANSLATION_SESSION_STATE_TTL_SEC = max(
    60,
    min(
        43200,
        int((os.getenv("ACTIVE_TRANSLATION_SESSION_STATE_TTL_SEC") or "7200").strip() or "7200"),
    ),
)
_TRANSLATION_SESSION_STATE_TTL_SEC = max(
    _ACTIVE_TRANSLATION_SESSION_STATE_TTL_SEC,
    min(
        86400,
        int((os.getenv("TRANSLATION_SESSION_STATE_TTL_SEC") or "21600").strip() or "21600"),
    ),
)
_TRANSLATION_SESSION_CARD_TTL_SEC = max(
    60,
    min(
        43200,
        int((os.getenv("TRANSLATION_SESSION_CARD_TTL_SEC") or "7200").strip() or "7200"),
    ),
)
_SESSION_PRESENCE_CARD_TTL_SEC = max(
    300,
    min(
        172800,
        int((os.getenv("SESSION_PRESENCE_CARD_TTL_SEC") or "86400").strip() or "86400"),
    ),
)
_TODAY_CARD_TTL_SEC = max(
    300,
    min(
        172800,
        int((os.getenv("TODAY_CARD_TTL_SEC") or "86400").strip() or "86400"),
    ),
)
_SKILLS_CARD_TTL_SEC = max(
    300,
    min(
        172800,
        int((os.getenv("SKILLS_CARD_TTL_SEC") or "86400").strip() or "86400"),
    ),
)
_TRANSLATION_CHECK_STATE_TTL_SEC = max(
    300,
    min(
        43200,
        int((os.getenv("TRANSLATION_CHECK_STATE_TTL_SEC") or "7200").strip() or "7200"),
    ),
)
_TRANSLATION_CHECK_TERMINAL_STATE_TTL_SEC = max(
    _TRANSLATION_CHECK_STATE_TTL_SEC,
    min(
        172800,
        int((os.getenv("TRANSLATION_CHECK_TERMINAL_STATE_TTL_SEC") or "86400").strip() or "86400"),
    ),
)
_TRANSLATION_CHECK_DISPATCH_STATE_TTL_SEC = max(
    300,
    min(
        43200,
        int((os.getenv("TRANSLATION_CHECK_DISPATCH_STATE_TTL_SEC") or "7200").strip() or "7200"),
    ),
)
_TRANSLATION_CHECK_COMPLETION_STATE_TTL_SEC = max(
    _TRANSLATION_CHECK_STATE_TTL_SEC,
    min(
        172800,
        int((os.getenv("TRANSLATION_CHECK_COMPLETION_STATE_TTL_SEC") or "86400").strip() or "86400"),
    ),
)
_TRANSLATION_CHECK_POLL_HINT_TTL_SEC = max(
    60,
    min(
        43200,
        int((os.getenv("TRANSLATION_CHECK_POLL_HINT_TTL_SEC") or "1800").strip() or "1800"),
    ),
)
_TRANSLATION_CHECK_CARD_TTL_SEC = max(
    300,
    min(
        172800,
        int((os.getenv("TRANSLATION_CHECK_CARD_TTL_SEC") or "86400").strip() or "86400"),
    ),
)
_TRANSLATION_CHECK_TERMINAL_SUMMARY_TTL_SEC = max(
    300,
    min(
        172800,
        int((os.getenv("TRANSLATION_CHECK_TERMINAL_SUMMARY_TTL_SEC") or "86400").strip() or "86400"),
    ),
)
_TRANSLATION_CHECK_START_IDEMPOTENCY_TTL_SEC = max(
    5,
    min(
        120,
        int((os.getenv("TRANSLATION_CHECK_START_IDEMPOTENCY_TTL_SEC") or "30").strip() or "30"),
    ),
)
_TRANSLATION_CHECK_COMPLETION_CLAIM_TTL_SEC = max(
    30,
    min(
        3600,
        int((os.getenv("TRANSLATION_CHECK_COMPLETION_CLAIM_TTL_SEC") or "600").strip() or "600"),
    ),
)
_TRANSLATION_CHECK_WATCHDOG_REQUEUE_COOLDOWN_SEC = max(
    5,
    min(
        600,
        int((os.getenv("TRANSLATION_CHECK_WATCHDOG_REQUEUE_COOLDOWN_SEC") or "30").strip() or "30"),
    ),
)
_TRANSLATION_FILL_JOB_TTL_SEC = max(
    300, int((os.getenv("TRANSLATION_FILL_JOB_TTL_SEC") or "3600").strip())
)
_TRANSLATION_FILL_READY_TTL_SEC = max(
    60, int((os.getenv("TRANSLATION_FILL_READY_TTL_SEC") or "600").strip())
)
_TRANSLATION_FILL_FAILED_TTL_SEC = max(
    60, int((os.getenv("TRANSLATION_FILL_FAILED_TTL_SEC") or "600").strip())
)
_TRANSLATION_FILL_FASTPATH_FRESH_MS = max(
    3000,
    int((os.getenv("TRANSLATION_FILL_FASTPATH_FRESH_MS") or "15000").strip() or "15000"),
)
_PROJECTION_MATERIALIZATION_LIVE_QUEUE_NAME = str(
    os.getenv("PROJECTION_MATERIALIZATION_LIVE_QUEUE_NAME") or "projection_materialization_live"
).strip() or "projection_materialization_live"
_PROJECTION_MATERIALIZATION_BACKFILL_QUEUE_NAME = str(
    os.getenv("PROJECTION_MATERIALIZATION_BACKFILL_QUEUE_NAME") or "projection_materialization_backfill"
).strip() or "projection_materialization_backfill"


def _env_flag_enabled(name: str, default: bool = False) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return bool(default)
    return raw in {"1", "true", "yes", "on"}


def get_redis_url() -> str:
    return (
        (os.getenv("REDIS_URL") or "").strip()
        or (os.getenv("RAILWAY_REDIS_URL") or "").strip()
        or (os.getenv("UPSTASH_REDIS_URL") or "").strip()
    )


def get_redis_client():
    global _REDIS_CLIENT
    if _REDIS_CLIENT is not None:
        return _REDIS_CLIENT
    redis_url = get_redis_url()
    if not redis_url or redis is None:
        return None
    _REDIS_CLIENT = redis.Redis.from_url(redis_url, decode_responses=True)
    return _REDIS_CLIENT


def get_dramatiq_broker():
    global _BROKER
    if _BROKER is not None:
        return _BROKER

    redis_url = get_redis_url()
    if redis_url and RedisBroker is not None:
        _BROKER = RedisBroker(url=redis_url)
    else:  # pragma: no cover - local fallback when redis is absent
        _BROKER = StubBroker()
    dramatiq.set_broker(_BROKER)
    return _BROKER


def is_youtube_transcript_async_enabled() -> bool:
    return _env_flag_enabled("YOUTUBE_TRANSCRIPT_ASYNC_ENABLED", default=False) and bool(get_redis_url())


def is_translation_check_async_enabled() -> bool:
    return _env_flag_enabled("TRANSLATION_CHECK_ASYNC_ENABLED", default=False) and bool(get_redis_url())


def is_translation_sentence_fill_async_enabled() -> bool:
    return _env_flag_enabled("TRANSLATION_SENTENCE_FILL_ASYNC_ENABLED", default=False) and bool(get_redis_url())


def is_tts_generation_async_enabled() -> bool:
    return _env_flag_enabled("TTS_GENERATION_ASYNC_ENABLED", default=False) and bool(get_redis_url())


def _tts_in_flight_key(cache_key: str) -> str:
    return f"tts:in_flight:{str(cache_key or '').strip()}"


def claim_tts_generation_in_flight(cache_key: str) -> bool:
    safe_key = str(cache_key or "").strip()
    if not safe_key:
        return False
    client = get_redis_client()
    if client is None:
        logging.warning("claim_tts_generation_in_flight: Redis unavailable, refusing async claim cache_key=%s", safe_key)
        return False
    try:
        claimed = client.set(_tts_in_flight_key(safe_key), "1", nx=True, ex=int(_TTS_GENERATION_IN_FLIGHT_TTL_SEC))
        return bool(claimed)
    except Exception:
        logging.warning("claim_tts_generation_in_flight: Redis error cache_key=%s", safe_key, exc_info=True)
        return False


def release_tts_generation_in_flight(cache_key: str) -> None:
    safe_key = str(cache_key or "").strip()
    if not safe_key:
        return
    client = get_redis_client()
    if client is None:
        return
    try:
        client.delete(_tts_in_flight_key(safe_key))
    except Exception:
        logging.warning("release_tts_generation_in_flight: Redis error cache_key=%s", safe_key, exc_info=True)


def enqueue_tts_generation_job(payload: dict) -> dict:
    if not is_tts_generation_async_enabled():
        return {"queued": False, "reason": "tts_generation_async_disabled"}
    cache_key = str((payload or {}).get("cache_key") or "").strip()
    if not cache_key:
        return {"queued": False, "reason": "missing_cache_key"}
    if not claim_tts_generation_in_flight(cache_key):
        return {"queued": False, "reason": "duplicate_in_flight"}
    try:
        get_dramatiq_broker()
        from backend.background_jobs import run_tts_generation_actor

        run_tts_generation_actor.send(**{k: v for k, v in (payload or {}).items()})
        return {"queued": True, "reason": "queued"}
    except Exception:
        release_tts_generation_in_flight(cache_key)
        logging.exception("enqueue_tts_generation_job failed cache_key=%s", cache_key)
        return {"queued": False, "reason": "broker_error"}


def can_enqueue_background_jobs() -> bool:
    return bool(get_redis_url())


def enqueue_projection_materialization_job(
    *,
    job_id: int,
    job_source: str | None = None,
    request_id: str | None = None,
    correlation_id: str | None = None,
    timing_breakdown: dict[str, int] | None = None,
) -> str | None:
    if not can_enqueue_background_jobs():
        raise RuntimeError("background_jobs_unavailable")
    safe_job_id = int(job_id or 0)
    if safe_job_id <= 0:
        raise ValueError("job_id is required")
    normalized_job_source = str(job_source or "").strip().lower() or "live"
    enqueue_started_perf = time.perf_counter()
    try:
        get_dramatiq_broker()
        from backend.background_jobs import (
            run_projection_materialization_backfill_job,
            run_projection_materialization_live_job,
        )

        actor = (
            run_projection_materialization_backfill_job
            if normalized_job_source == "backfill"
            else run_projection_materialization_live_job
        )
        queue_name = (
            _PROJECTION_MATERIALIZATION_BACKFILL_QUEUE_NAME
            if normalized_job_source == "backfill"
            else _PROJECTION_MATERIALIZATION_LIVE_QUEUE_NAME
        )
        send_started_perf = time.perf_counter()
        message = actor.send(
            job_id=safe_job_id,
            request_id=str(request_id or "").strip() or None,
            correlation_id=str(correlation_id or "").strip() or None,
        )
        send_duration_ms = int((time.perf_counter() - send_started_perf) * 1000)
        logging.info(
            "projection_materialization_enqueue job_id=%s job_source=%s queue_name=%s request_id=%s correlation_id=%s",
            safe_job_id,
            normalized_job_source,
            queue_name,
            request_id,
            correlation_id,
        )
        if isinstance(timing_breakdown, dict):
            total_duration_ms = int((time.perf_counter() - enqueue_started_perf) * 1000)
            timing_breakdown["send_ms"] = send_duration_ms
            timing_breakdown["status_count_ms"] = 0
            timing_breakdown["other_ms"] = max(0, total_duration_ms - send_duration_ms)
        return str(getattr(message, "message_id", None) or "").strip() or None
    except Exception:
        logging.exception(
            "enqueue_projection_materialization_job failed job_id=%s job_source=%s",
            safe_job_id,
            normalized_job_source,
        )
        raise


def enqueue_translation_focus_pool_refill_job(
    *,
    force: bool = False,
    tz_name: str | None = None,
    request_id: str | None = None,
    correlation_id: str | None = None,
) -> str | None:
    if not can_enqueue_background_jobs():
        raise RuntimeError("background_jobs_unavailable")
    try:
        get_dramatiq_broker()
        from backend.background_jobs import run_translation_focus_pool_refill_job

        message = run_translation_focus_pool_refill_job.send(
            force=bool(force),
            tz_name=str(tz_name or "").strip() or None,
            request_id=str(request_id or "").strip() or None,
            correlation_id=str(correlation_id or "").strip() or None,
        )
        return str(getattr(message, "message_id", None) or "").strip() or None
    except Exception:
        logging.exception("enqueue_translation_focus_pool_refill_job failed")
        raise


def enqueue_image_quiz_template_prepare_job(
    *,
    user_id: int,
    source_lang: str,
    target_lang: str,
    requested_count: int = 1,
    request_id: str | None = None,
    correlation_id: str | None = None,
) -> str | None:
    if not can_enqueue_background_jobs():
        raise RuntimeError("background_jobs_unavailable")
    try:
        get_dramatiq_broker()
        from backend.background_jobs import run_image_quiz_template_prepare_job

        message = run_image_quiz_template_prepare_job.send(
            user_id=int(user_id),
            source_lang=str(source_lang or "").strip().lower() or "ru",
            target_lang=str(target_lang or "").strip().lower() or "de",
            requested_count=max(1, int(requested_count or 1)),
            request_id=str(request_id or "").strip() or None,
            correlation_id=str(correlation_id or "").strip() or None,
        )
        return str(getattr(message, "message_id", None) or "").strip() or None
    except Exception:
        logging.exception(
            "enqueue_image_quiz_template_prepare_job failed user_id=%s source_lang=%s target_lang=%s requested_count=%s",
            user_id,
            source_lang,
            target_lang,
            requested_count,
        )
        raise


def enqueue_image_quiz_template_render_job(
    *,
    user_id: int,
    source_lang: str,
    target_lang: str,
    requested_count: int = 1,
    request_id: str | None = None,
    correlation_id: str | None = None,
) -> str | None:
    if not can_enqueue_background_jobs():
        raise RuntimeError("background_jobs_unavailable")
    try:
        get_dramatiq_broker()
        from backend.background_jobs import run_image_quiz_template_render_job

        message = run_image_quiz_template_render_job.send(
            user_id=int(user_id),
            source_lang=str(source_lang or "").strip().lower() or "ru",
            target_lang=str(target_lang or "").strip().lower() or "de",
            requested_count=max(1, int(requested_count or 1)),
            request_id=str(request_id or "").strip() or None,
            correlation_id=str(correlation_id or "").strip() or None,
        )
        return str(getattr(message, "message_id", None) or "").strip() or None
    except Exception:
        logging.exception(
            "enqueue_image_quiz_template_render_job failed user_id=%s source_lang=%s target_lang=%s requested_count=%s",
            user_id,
            source_lang,
            target_lang,
            requested_count,
        )
        raise


def enqueue_image_quiz_template_refresh_job(
    *,
    user_id: int,
    source_lang: str,
    target_lang: str,
    requested_count: int = 1,
    request_id: str | None = None,
    correlation_id: str | None = None,
) -> str | None:
    if not can_enqueue_background_jobs():
        raise RuntimeError("background_jobs_unavailable")
    try:
        get_dramatiq_broker()
        from backend.background_jobs import run_image_quiz_template_refresh_job

        message = run_image_quiz_template_refresh_job.send(
            user_id=int(user_id),
            source_lang=str(source_lang or "").strip().lower() or "ru",
            target_lang=str(target_lang or "").strip().lower() or "de",
            requested_count=max(1, int(requested_count or 1)),
            request_id=str(request_id or "").strip() or None,
            correlation_id=str(correlation_id or "").strip() or None,
        )
        return str(getattr(message, "message_id", None) or "").strip() or None
    except Exception:
        logging.exception(
            "enqueue_image_quiz_template_refresh_job failed user_id=%s source_lang=%s target_lang=%s requested_count=%s",
            user_id,
            source_lang,
            target_lang,
            requested_count,
        )
        raise


def _youtube_transcript_job_key(video_id: str, lang: str | None) -> str:
    normalized_video_id = str(video_id or "").strip()
    normalized_lang = str(lang or "").strip().lower() or "auto"
    return f"yt:transcript:job:{normalized_video_id}:{normalized_lang}"


def _translation_check_job_key(session_id: int) -> str:
    return f"translation_check:job:{int(session_id)}"


def _active_translation_session_state_key(user_id: int) -> str:
    return f"translation:active_session:user:{int(user_id)}"


def _translation_session_state_key(session_id: str | int) -> str:
    return f"translation:session:{str(session_id).strip()}"


def _translation_session_card_key(user_id: int) -> str:
    return f"translation:session_card:user:{int(user_id)}"


def _session_presence_card_key(user_id: int) -> str:
    return f"webapp:session_presence_card:user:{int(user_id)}"


def _today_card_key(user_id: int, snapshot_key: str) -> str:
    return f"webapp:today_card:user:{int(user_id)}:date:{str(snapshot_key or '').strip()}"


def _skills_card_key(user_id: int, snapshot_key: str) -> str:
    return f"webapp:skills_card:user:{int(user_id)}:period:{str(snapshot_key or '').strip()}"


def _translation_check_state_key(session_id: int) -> str:
    return f"translation:check:state:{int(session_id)}"


def _translation_check_card_key(session_id: int) -> str:
    return f"translation:check:card:{int(session_id)}"


def _translation_check_dispatch_state_key(session_id: int) -> str:
    return f"translation:check:dispatch:{int(session_id)}"


def _translation_check_completion_state_key(session_id: int) -> str:
    return f"translation:check:completion:{int(session_id)}"


def _translation_check_poll_hint_key(session_id: int) -> str:
    return f"translation:check:poll_hint:{int(session_id)}"


def _translation_check_terminal_summary_key(session_id: int) -> str:
    return f"translation:check:terminal_summary:{int(session_id)}"


def _translation_check_resume_cooldown_key(session_id: int) -> str:
    return f"translation_check:resume_cooldown:{int(session_id)}"


def _translation_check_watchdog_requeue_key(session_id: int) -> str:
    return f"translation_check:watchdog_requeue:{int(session_id)}"


def _translation_fill_job_key(session_id: int) -> str:
    return f"translation_fill:job:{int(session_id)}"


def translation_start_idempotency_key(user_id: int) -> str:
    return f"translation:idempotency:start:user:{int(user_id)}"


def translation_check_start_idempotency_key(*, user_id: int, source_session_id: str | int | None) -> str:
    normalized_source_session_id = str(source_session_id or "none").strip() or "none"
    return f"translation:idempotency:check_start:session:{normalized_source_session_id}:user:{int(user_id)}"


def translation_finish_idempotency_key(user_id: int) -> str:
    return f"translation:idempotency:finish:user:{int(user_id)}"


def translation_check_completion_enqueue_idempotency_key(session_id: int) -> str:
    return f"translation:idempotency:completion_enqueue:{int(session_id)}"


def translation_check_completion_claim_idempotency_key(session_id: int) -> str:
    return f"translation:idempotency:completion_claim:{int(session_id)}"


def get_translation_check_start_idempotency_ttl_sec() -> int:
    return int(_TRANSLATION_CHECK_START_IDEMPOTENCY_TTL_SEC)


def get_translation_check_completion_claim_ttl_sec() -> int:
    return int(_TRANSLATION_CHECK_COMPLETION_CLAIM_TTL_SEC)


def get_translation_check_watchdog_requeue_cooldown_sec() -> int:
    return int(_TRANSLATION_CHECK_WATCHDOG_REQUEUE_COOLDOWN_SEC)


def _json_payload_ttl(ttl_sec: int) -> int:
    return max(1, int(ttl_sec or 1))


def _load_json_payload(redis_key: str) -> dict[str, Any] | None:
    client = get_redis_client()
    if client is None or not str(redis_key or "").strip():
        return None
    try:
        raw = client.get(redis_key)
    except Exception:
        logging.warning("Failed to read redis JSON payload key=%s", redis_key, exc_info=True)
        return None
    if not raw:
        return None
    try:
        payload = json.loads(raw)
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _store_json_payload(redis_key: str, payload: dict[str, Any], *, ttl_sec: int) -> dict[str, Any] | None:
    client = get_redis_client()
    if client is None or not str(redis_key or "").strip():
        return None
    normalized_payload = dict(payload or {})
    normalized_payload["updated_at_ms"] = int(time.time() * 1000)
    try:
        client.setex(
            redis_key,
            _json_payload_ttl(ttl_sec),
            json.dumps(normalized_payload, ensure_ascii=False),
        )
    except Exception:
        logging.warning("Failed to store redis JSON payload key=%s", redis_key, exc_info=True)
        return None
    return normalized_payload


def _delete_json_payload(redis_key: str) -> None:
    client = get_redis_client()
    if client is None or not str(redis_key or "").strip():
        return
    try:
        client.delete(redis_key)
    except Exception:
        logging.warning("Failed to delete redis JSON payload key=%s", redis_key, exc_info=True)


def claim_shared_idempotency(redis_key: str, *, ttl_sec: int) -> str | None:
    client = get_redis_client()
    if client is None or not str(redis_key or "").strip():
        return uuid4().hex
    token = uuid4().hex
    try:
        claimed = client.set(redis_key, token, nx=True, ex=_json_payload_ttl(ttl_sec))
    except Exception:
        logging.warning("Failed to claim shared idempotency key=%s", redis_key, exc_info=True)
        return uuid4().hex
    return token if claimed else None


def release_shared_idempotency(redis_key: str, token: str | None) -> None:
    client = get_redis_client()
    normalized_token = str(token or "").strip()
    if client is None or not str(redis_key or "").strip() or not normalized_token:
        return
    try:
        current = client.get(redis_key)
        if current and str(current).strip() == normalized_token:
            client.delete(redis_key)
    except Exception:
        logging.warning("Failed to release shared idempotency key=%s", redis_key, exc_info=True)


def get_active_translation_session_state(user_id: int) -> dict[str, Any] | None:
    return _load_json_payload(_active_translation_session_state_key(int(user_id)))


def set_active_translation_session_state(user_id: int, payload: dict[str, Any]) -> dict[str, Any] | None:
    return _store_json_payload(
        _active_translation_session_state_key(int(user_id)),
        payload,
        ttl_sec=_ACTIVE_TRANSLATION_SESSION_STATE_TTL_SEC,
    )


def clear_active_translation_session_state(user_id: int) -> None:
    _delete_json_payload(_active_translation_session_state_key(int(user_id)))


def get_translation_session_state(session_id: str | int) -> dict[str, Any] | None:
    normalized_session_id = str(session_id or "").strip()
    if not normalized_session_id:
        return None
    return _load_json_payload(_translation_session_state_key(normalized_session_id))


def set_translation_session_state(session_id: str | int, payload: dict[str, Any]) -> dict[str, Any] | None:
    normalized_session_id = str(session_id or "").strip()
    if not normalized_session_id:
        return None
    return _store_json_payload(
        _translation_session_state_key(normalized_session_id),
        payload,
        ttl_sec=_TRANSLATION_SESSION_STATE_TTL_SEC,
    )


def clear_translation_session_state(session_id: str | int) -> None:
    normalized_session_id = str(session_id or "").strip()
    if not normalized_session_id:
        return
    _delete_json_payload(_translation_session_state_key(normalized_session_id))


def get_translation_session_card(user_id: int) -> dict[str, Any] | None:
    return _load_json_payload(_translation_session_card_key(int(user_id)))


def set_translation_session_card(user_id: int, payload: dict[str, Any]) -> dict[str, Any] | None:
    return _store_json_payload(
        _translation_session_card_key(int(user_id)),
        payload,
        ttl_sec=_TRANSLATION_SESSION_CARD_TTL_SEC,
    )


def clear_translation_session_card(user_id: int) -> None:
    _delete_json_payload(_translation_session_card_key(int(user_id)))


def get_session_presence_card(user_id: int) -> dict[str, Any] | None:
    return _load_json_payload(_session_presence_card_key(int(user_id)))


def set_session_presence_card(user_id: int, payload: dict[str, Any]) -> dict[str, Any] | None:
    return _store_json_payload(
        _session_presence_card_key(int(user_id)),
        payload,
        ttl_sec=_SESSION_PRESENCE_CARD_TTL_SEC,
    )


def clear_session_presence_card(user_id: int) -> None:
    _delete_json_payload(_session_presence_card_key(int(user_id)))


def get_today_card(user_id: int, snapshot_key: str) -> dict[str, Any] | None:
    normalized_key = str(snapshot_key or "").strip()
    if not normalized_key:
        return None
    return _load_json_payload(_today_card_key(int(user_id), normalized_key))


def set_today_card(user_id: int, snapshot_key: str, payload: dict[str, Any]) -> dict[str, Any] | None:
    normalized_key = str(snapshot_key or "").strip()
    if not normalized_key:
        return None
    return _store_json_payload(
        _today_card_key(int(user_id), normalized_key),
        payload,
        ttl_sec=_TODAY_CARD_TTL_SEC,
    )


def clear_today_card(user_id: int, snapshot_key: str) -> None:
    normalized_key = str(snapshot_key or "").strip()
    if not normalized_key:
        return
    _delete_json_payload(_today_card_key(int(user_id), normalized_key))


def get_skills_card(user_id: int, snapshot_key: str) -> dict[str, Any] | None:
    normalized_key = str(snapshot_key or "").strip()
    if not normalized_key:
        return None
    return _load_json_payload(_skills_card_key(int(user_id), normalized_key))


def set_skills_card(user_id: int, snapshot_key: str, payload: dict[str, Any]) -> dict[str, Any] | None:
    normalized_key = str(snapshot_key or "").strip()
    if not normalized_key:
        return None
    return _store_json_payload(
        _skills_card_key(int(user_id), normalized_key),
        payload,
        ttl_sec=_SKILLS_CARD_TTL_SEC,
    )


def clear_skills_card(user_id: int, snapshot_key: str) -> None:
    normalized_key = str(snapshot_key or "").strip()
    if not normalized_key:
        return
    _delete_json_payload(_skills_card_key(int(user_id), normalized_key))


def get_translation_check_state(session_id: int) -> dict[str, Any] | None:
    return _load_json_payload(_translation_check_state_key(int(session_id)))


def set_translation_check_state(
    session_id: int,
    payload: dict[str, Any],
    *,
    terminal: bool = False,
) -> dict[str, Any] | None:
    ttl_sec = _TRANSLATION_CHECK_TERMINAL_STATE_TTL_SEC if terminal else _TRANSLATION_CHECK_STATE_TTL_SEC
    return _store_json_payload(
        _translation_check_state_key(int(session_id)),
        payload,
        ttl_sec=ttl_sec,
    )


def get_translation_check_dispatch_state(session_id: int) -> dict[str, Any] | None:
    return _load_json_payload(_translation_check_dispatch_state_key(int(session_id)))


def set_translation_check_dispatch_state(session_id: int, payload: dict[str, Any]) -> dict[str, Any] | None:
    return _store_json_payload(
        _translation_check_dispatch_state_key(int(session_id)),
        payload,
        ttl_sec=_TRANSLATION_CHECK_DISPATCH_STATE_TTL_SEC,
    )


def clear_translation_check_dispatch_state(session_id: int) -> None:
    _delete_json_payload(_translation_check_dispatch_state_key(int(session_id)))


def get_translation_check_completion_state(session_id: int) -> dict[str, Any] | None:
    return _load_json_payload(_translation_check_completion_state_key(int(session_id)))


def set_translation_check_completion_state(session_id: int, payload: dict[str, Any]) -> dict[str, Any] | None:
    return _store_json_payload(
        _translation_check_completion_state_key(int(session_id)),
        payload,
        ttl_sec=_TRANSLATION_CHECK_COMPLETION_STATE_TTL_SEC,
    )


def get_translation_check_poll_hint_state(session_id: int) -> dict[str, Any] | None:
    return _load_json_payload(_translation_check_poll_hint_key(int(session_id)))


def set_translation_check_poll_hint_state(session_id: int, payload: dict[str, Any]) -> dict[str, Any] | None:
    return _store_json_payload(
        _translation_check_poll_hint_key(int(session_id)),
        payload,
        ttl_sec=_TRANSLATION_CHECK_POLL_HINT_TTL_SEC,
    )


def get_translation_check_status_card(session_id: int) -> dict[str, Any] | None:
    return _load_json_payload(_translation_check_card_key(int(session_id)))


def set_translation_check_status_card(session_id: int, payload: dict[str, Any]) -> dict[str, Any] | None:
    return _store_json_payload(
        _translation_check_card_key(int(session_id)),
        payload,
        ttl_sec=_TRANSLATION_CHECK_CARD_TTL_SEC,
    )


def clear_translation_check_status_card(session_id: int) -> None:
    _delete_json_payload(_translation_check_card_key(int(session_id)))


def get_translation_check_terminal_summary(session_id: int) -> dict[str, Any] | None:
    return _load_json_payload(_translation_check_terminal_summary_key(int(session_id)))


def set_translation_check_terminal_summary(session_id: int, payload: dict[str, Any]) -> dict[str, Any] | None:
    return _store_json_payload(
        _translation_check_terminal_summary_key(int(session_id)),
        payload,
        ttl_sec=_TRANSLATION_CHECK_TERMINAL_SUMMARY_TTL_SEC,
    )


def clear_translation_check_terminal_summary(session_id: int) -> None:
    _delete_json_payload(_translation_check_terminal_summary_key(int(session_id)))


def get_youtube_transcript_job_status(video_id: str, lang: str | None) -> dict | None:
    client = get_redis_client()
    if client is None:
        return None
    raw = client.get(_youtube_transcript_job_key(video_id, lang))
    if not raw:
        return None
    try:
        payload = json.loads(raw)
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    return payload


def set_youtube_transcript_job_status(
    video_id: str,
    lang: str | None,
    *,
    status: str,
    job_id: str | None = None,
    error: str | None = None,
    source: str | None = None,
    item_count: int | None = None,
    fetch_duration_ms: int | None = None,
) -> dict | None:
    client = get_redis_client()
    if client is None:
        return None
    payload = {
        "status": str(status or "").strip() or "pending",
        "job_id": str(job_id or "").strip() or None,
        "error": str(error or "").strip() or None,
        "source": str(source or "").strip() or None,
        "item_count": int(item_count or 0) if item_count is not None else None,
        "fetch_duration_ms": int(fetch_duration_ms or 0) if fetch_duration_ms is not None else None,
        "updated_at_ms": int(time.time() * 1000),
    }
    ttl = _YOUTUBE_TRANSCRIPT_JOB_TTL_SEC
    if payload["status"] == "ready":
        ttl = _YOUTUBE_TRANSCRIPT_READY_TTL_SEC
    elif payload["status"] == "failed":
        ttl = _YOUTUBE_TRANSCRIPT_FAILED_TTL_SEC
    client.setex(_youtube_transcript_job_key(video_id, lang), ttl, json.dumps(payload, ensure_ascii=False))
    return payload


def enqueue_youtube_transcript_job(video_id: str, lang: str | None, *, allow_proxy: bool) -> dict:
    if not is_youtube_transcript_async_enabled():
        raise RuntimeError("youtube_transcript_async_disabled")

    current = get_youtube_transcript_job_status(video_id, lang)
    if current and current.get("status") in {"pending", "running"}:
        return current

    set_youtube_transcript_job_status(video_id, lang, status="pending")
    try:
        get_dramatiq_broker()
        from backend.background_jobs import fetch_youtube_transcript_job

        message = fetch_youtube_transcript_job.send(
            video_id=str(video_id or "").strip(),
            lang=str(lang or "").strip(),
            allow_proxy=bool(allow_proxy),
        )
        payload = set_youtube_transcript_job_status(
            video_id,
            lang,
            status="pending",
            job_id=getattr(message, "message_id", None),
        )
        return payload or {"status": "pending"}
    except Exception as exc:
        logging.exception("enqueue_youtube_transcript_job failed video_id=%s lang=%s", video_id, lang)
        set_youtube_transcript_job_status(video_id, lang, status="failed", error=str(exc))
        raise


def get_translation_check_job_status(session_id: int) -> dict | None:
    client = get_redis_client()
    if client is None:
        return None
    raw = client.get(_translation_check_job_key(int(session_id)))
    if not raw:
        return None
    try:
        payload = json.loads(raw)
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def is_translation_check_job_status_fast_path_eligible(payload: dict | None) -> bool:
    if not isinstance(payload, dict):
        return False
    status = str(payload.get("status") or "").strip().lower()
    if status not in {"pending", "running"}:
        return False
    try:
        updated_at_ms = int(payload.get("updated_at_ms") or 0)
    except Exception:
        updated_at_ms = 0
    if updated_at_ms <= 0:
        return False
    age_ms = max(0, int(time.time() * 1000) - updated_at_ms)
    return age_ms < _TRANSLATION_CHECK_FASTPATH_FRESH_MS


def _translation_check_poll_delay_for_status(status: str | None) -> int:
    normalized_status = str(status or "").strip().lower()
    if normalized_status in {"done", "failed", "canceled", "ready"}:
        return 0
    if normalized_status in {"queued", "pending"}:
        return 4000
    return 2500


def claim_translation_check_resume_cooldown(session_id: int) -> bool:
    client = get_redis_client()
    if client is None:
        return True
    try:
        claimed = client.set(
            _translation_check_resume_cooldown_key(int(session_id)),
            str(int(time.time() * 1000)),
            nx=True,
            ex=int(_TRANSLATION_CHECK_RESUME_COOLDOWN_SEC),
        )
    except Exception:
        return True
    return bool(claimed)


def claim_translation_check_watchdog_requeue_cooldown(session_id: int) -> bool:
    client = get_redis_client()
    if client is None:
        return True
    try:
        claimed = client.set(
            _translation_check_watchdog_requeue_key(int(session_id)),
            str(int(time.time() * 1000)),
            nx=True,
            ex=int(_TRANSLATION_CHECK_WATCHDOG_REQUEUE_COOLDOWN_SEC),
        )
    except Exception:
        return True
    return bool(claimed)


def set_translation_check_job_status(
    session_id: int,
    *,
    status: str,
    job_id: str | None = None,
    error: str | None = None,
    session: dict | None = None,
    progress: dict | None = None,
    polling: dict | None = None,
) -> dict | None:
    client = get_redis_client()
    if client is None:
        return None
    current = get_translation_check_job_status(int(session_id)) or {}
    current_dispatch_state = get_translation_check_dispatch_state(int(session_id)) or {}
    session_payload = session if isinstance(session, dict) else current.get("session")
    progress_payload = progress if isinstance(progress, dict) else current.get("progress")
    polling_payload = polling if isinstance(polling, dict) else current.get("polling")
    payload = {
        "status": str(status or "").strip() or "pending",
        "job_id": str(job_id or "").strip() or None,
        "error": str(error or "").strip() or None,
        "updated_at_ms": int(time.time() * 1000),
    }
    if isinstance(session_payload, dict):
        normalized_session = dict(session_payload)
        normalized_session["status"] = (
            "queued"
            if payload["status"] == "pending"
            else "done"
            if payload["status"] == "ready"
            else payload["status"]
        )
        if payload["error"] and not normalized_session.get("last_error"):
            normalized_session["last_error"] = payload["error"]
        payload["session"] = normalized_session
    if isinstance(progress_payload, dict):
        payload["progress"] = dict(progress_payload)
    if isinstance(polling_payload, dict):
        payload["polling"] = dict(polling_payload)
    session_payload_dict = payload.get("session") if isinstance(payload.get("session"), dict) else {}
    progress_payload_dict = payload.get("progress") if isinstance(payload.get("progress"), dict) else {}
    session_status = str(session_payload_dict.get("status") or "").strip().lower()
    effective_status = (
        session_status
        or ("queued" if payload["status"] == "pending" else "done" if payload["status"] == "ready" else payload["status"])
    )
    total_items = int(
        session_payload_dict.get("total_items")
        or progress_payload_dict.get("total")
        or 0
    )
    completed_items = int(
        session_payload_dict.get("completed_items")
        or progress_payload_dict.get("completed")
        or 0
    )
    failed_items = int(
        session_payload_dict.get("failed_items")
        or progress_payload_dict.get("failed")
        or 0
    )
    pending_items = int(
        progress_payload_dict.get("pending")
        if progress_payload_dict.get("pending") is not None
        else max(0, total_items - completed_items - failed_items)
    )
    session_id_value = int(
        session_payload_dict.get("id")
        or session_payload_dict.get("session_id")
        or int(session_id)
    )
    poll_hint_payload = {
        "status": effective_status or "queued",
        "next_poll_after_ms": _translation_check_poll_delay_for_status(effective_status),
        "terminal": bool(effective_status in {"done", "failed", "canceled"}),
    }
    set_translation_check_poll_hint_state(int(session_id), poll_hint_payload)
    set_translation_check_state(
        int(session_id),
        {
            "session_id": session_id_value,
            "user_id": session_payload_dict.get("user_id"),
            "source_session_id": session_payload_dict.get("source_session_id"),
            "status": effective_status or "queued",
            "total_items": total_items,
            "completed_items": completed_items,
            "failed_items": failed_items,
            "pending_items": pending_items,
            "started_at_ms": session_payload_dict.get("started_at_ms"),
            "finished_at_ms": session_payload_dict.get("finished_at_ms"),
            "last_heartbeat_ms": int(time.time() * 1000),
            "last_error": payload["error"] or session_payload_dict.get("last_error"),
            "terminal_ready": bool(effective_status in {"done", "failed", "canceled"}),
            "completion_done": bool(session_payload_dict.get("completion_done")),
            "source_lang": session_payload_dict.get("source_lang"),
            "target_lang": session_payload_dict.get("target_lang"),
        },
        terminal=bool(effective_status in {"done", "failed", "canceled"}),
    )
    dispatch_status = None
    if payload["status"] == "pending":
        dispatch_status = (
            str(current_dispatch_state.get("status") or "").strip().lower()
            if str(current_dispatch_state.get("status") or "").strip().lower() in {"broker_enqueued", "worker_received"}
            else "queued"
        )
    elif payload["status"] == "running":
        dispatch_status = (
            str(current_dispatch_state.get("status") or "").strip().lower()
            if str(current_dispatch_state.get("status") or "").strip().lower() in {"claimed", "first_heartbeat"}
            else "claimed"
        )
    elif payload["status"] in {"ready", "failed"}:
        dispatch_status = "finished" if payload["status"] == "ready" else "failed"
    if dispatch_status:
        set_translation_check_dispatch_state(
            int(session_id),
            {
                "status": dispatch_status,
                "worker_job_id": payload.get("job_id") or current_dispatch_state.get("worker_job_id"),
                "message_id": current_dispatch_state.get("message_id"),
                "queue_name": current_dispatch_state.get("queue_name"),
                "dispatched_at_ms": current_dispatch_state.get("dispatched_at_ms") or session_payload_dict.get("dispatched_at_ms"),
                "broker_enqueued_at_ms": current_dispatch_state.get("broker_enqueued_at_ms"),
                "worker_received_at_ms": current_dispatch_state.get("worker_received_at_ms"),
                "claimed_at_ms": current_dispatch_state.get("claimed_at_ms") or session_payload_dict.get("claimed_at_ms"),
                "first_heartbeat_at_ms": current_dispatch_state.get("first_heartbeat_at_ms"),
                "last_heartbeat_ms": int(time.time() * 1000),
                "runtime_status": effective_status or dispatch_status,
            },
        )
    if effective_status in {"done", "failed", "canceled"}:
        set_translation_check_completion_state(
            int(session_id),
            {
                "terminal_ready": True,
                "completion_job_enqueued": bool(session_payload_dict.get("completion_job_enqueued")),
                "completion_job_started_at_ms": session_payload_dict.get("completion_job_started_at_ms"),
                "completion_done": bool(session_payload_dict.get("completion_done")),
                "completion_done_at_ms": session_payload_dict.get("completion_done_at_ms"),
                "summary_available": bool(session_payload_dict.get("summary_available")),
            },
        )
    ttl = _TRANSLATION_CHECK_JOB_TTL_SEC
    if payload["status"] == "ready":
        ttl = _TRANSLATION_CHECK_READY_TTL_SEC
    elif payload["status"] == "failed":
        ttl = _TRANSLATION_CHECK_FAILED_TTL_SEC
    client.setex(_translation_check_job_key(int(session_id)), ttl, json.dumps(payload, ensure_ascii=False))
    return payload


def _parse_iso_datetime(value: str | None) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text)
    except Exception:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _age_ms_from_iso(value: str | None) -> int | None:
    parsed = _parse_iso_datetime(value)
    if parsed is None:
        return None
    return max(0, int((datetime.now(timezone.utc) - parsed).total_seconds() * 1000))


def enqueue_translation_check_job(
    session_id: int,
    *,
    correlation_id: str | None = None,
    request_id: str | None = None,
    accepted_at_ms: int | None = None,
    force_dispatch: bool = False,
) -> dict:
    if not is_translation_check_async_enabled():
        raise RuntimeError("translation_check_async_disabled")
    from backend.database import (
        get_translation_check_session_runtime,
        mark_translation_check_session_dispatched,
    )

    current = get_translation_check_job_status(int(session_id))
    current_job_id = str((current or {}).get("job_id") or "").strip()
    runtime_dispatched_job_id = str((runtime or {}).get("dispatched_job_id") or "").strip() if 'runtime' in locals() else ""
    runtime_worker_job_id = str((runtime or {}).get("worker_job_id") or "").strip() if 'runtime' in locals() else ""
    has_real_dispatch_evidence = bool(current_job_id or runtime_dispatched_job_id or runtime_worker_job_id)
    if not force_dispatch and has_real_dispatch_evidence and current and current.get("status") in {"pending", "running"}:
        current_status = str(current.get("status") or "").strip().lower()
        try:
            updated_at_ms = int(current.get("updated_at_ms") or 0)
        except Exception:
            updated_at_ms = 0
        now_ms = int(time.time() * 1000)
        age_ms = max(0, now_ms - updated_at_ms) if updated_at_ms > 0 else 0
        pending_stale = current_status == "pending" and age_ms >= _TRANSLATION_CHECK_PENDING_STALE_REQUEUE_MS
        running_stale = current_status == "running" and age_ms >= _TRANSLATION_CHECK_RUNNING_STALE_REQUEUE_MS
        if not pending_stale and not running_stale:
            return current

    runtime = get_translation_check_session_runtime(session_id=int(session_id)) or {}
    runtime_dispatched_job_id = str(runtime.get("dispatched_job_id") or "").strip()
    runtime_worker_job_id = str(runtime.get("worker_job_id") or "").strip()
    has_real_dispatch_evidence = bool(current_job_id or runtime_dispatched_job_id or runtime_worker_job_id)
    runtime_status = str(runtime.get("status") or "").strip().lower()
    if runtime_status in {"done", "failed", "canceled"}:
        mapped_status = "ready" if runtime_status == "done" else "failed"
        payload = set_translation_check_job_status(
            int(session_id),
            status=mapped_status,
            error=runtime.get("last_error"),
        )
        return payload or {"status": mapped_status}

    dispatched_age_ms = _age_ms_from_iso(runtime.get("dispatched_at"))
    heartbeat_age_ms = _age_ms_from_iso(runtime.get("heartbeat_at"))
    if (
        not force_dispatch
        and runtime_status == "queued"
        and dispatched_age_ms is not None
        and dispatched_age_ms < _TRANSLATION_CHECK_PENDING_STALE_REQUEUE_MS
    ):
        payload = set_translation_check_job_status(
            int(session_id),
            status="pending",
            job_id=runtime.get("dispatched_job_id"),
        )
        return payload or {"status": "pending"}
    if (
        not force_dispatch
        and runtime_status == "running"
        and heartbeat_age_ms is not None
        and heartbeat_age_ms < _TRANSLATION_CHECK_RUNNING_STALE_REQUEUE_MS
    ):
        payload = set_translation_check_job_status(
            int(session_id),
            status="running",
            job_id=runtime.get("worker_job_id") or runtime.get("dispatched_job_id"),
        )
        return payload or {"status": "running"}

    dispatch_job_id = uuid4().hex
    current_dispatch_state = get_translation_check_dispatch_state(int(session_id)) or {}
    previous_generation = int(current_dispatch_state.get("dispatch_generation") or 0)
    previous_redispatch_count = int(current_dispatch_state.get("redispatch_count") or 0)
    dispatch_generation = max(1, previous_generation + (1 if force_dispatch else 0))
    redispatch_count = max(0, previous_redispatch_count + (1 if force_dispatch else 0))
    force_dispatch_at_ms = int(time.time() * 1000) if force_dispatch else None
    runtime = mark_translation_check_session_dispatched(
        session_id=int(session_id),
        dispatch_job_id=dispatch_job_id,
    )
    if not runtime:
        raise RuntimeError(f"translation_check_session_not_dispatchable:{int(session_id)}")
    dispatched_at_ms = int(time.time() * 1000)
    set_translation_check_dispatch_state(
        int(session_id),
        {
            "status": "queued",
            "worker_job_id": dispatch_job_id,
            "message_id": None,
            "queue_name": None,
            "dispatched_at_ms": dispatched_at_ms,
            "broker_enqueued_at_ms": None,
            "worker_received_at_ms": None,
            "claimed_at_ms": None,
            "first_heartbeat_at_ms": None,
            "last_heartbeat_ms": None,
            "dispatch_generation": dispatch_generation,
            "redispatch_count": redispatch_count,
            "last_force_dispatch_at_ms": (
                force_dispatch_at_ms if force_dispatch else current_dispatch_state.get("last_force_dispatch_at_ms")
            ),
            "runtime_status": "queued",
        },
    )
    set_translation_check_poll_hint_state(
        int(session_id),
        {
            "status": "queued",
            "next_poll_after_ms": 4000,
            "terminal": False,
        },
    )
    set_translation_check_job_status(int(session_id), status="pending", job_id=dispatch_job_id)
    try:
        get_dramatiq_broker()
        from backend.background_jobs import run_translation_check_job

        message = run_translation_check_job.send(
            session_id=int(session_id),
            dispatch_job_id=dispatch_job_id,
            correlation_id=str(correlation_id or "").strip() or None,
            request_id=str(request_id or "").strip() or None,
            accepted_at_ms=int(accepted_at_ms) if accepted_at_ms is not None else None,
        )
        payload = set_translation_check_job_status(
            int(session_id),
            status="pending",
            job_id=dispatch_job_id or getattr(message, "message_id", None),
        )
        broker_enqueued_at_ms = int(time.time() * 1000)
        message_id = str(getattr(message, "message_id", None) or "").strip() or None
        set_translation_check_dispatch_state(
            int(session_id),
            {
                "status": "broker_enqueued",
                "worker_job_id": dispatch_job_id,
                "message_id": message_id,
                "queue_name": str(getattr(run_translation_check_job, "queue_name", None) or "").strip() or None,
                "dispatched_at_ms": dispatched_at_ms,
                "broker_enqueued_at_ms": broker_enqueued_at_ms,
                "worker_received_at_ms": None,
                "claimed_at_ms": None,
                "first_heartbeat_at_ms": None,
                "last_heartbeat_ms": None,
                "dispatch_generation": dispatch_generation,
                "redispatch_count": redispatch_count,
                "last_force_dispatch_at_ms": (
                    force_dispatch_at_ms if force_dispatch else current_dispatch_state.get("last_force_dispatch_at_ms")
                ),
                "runtime_status": "queued",
            },
        )
        logging.info(
            "translation_check_dispatch transition=broker_enqueued queue=%s session_id=%s dispatch_job_id=%s message_id=%s dispatch_generation=%s redispatch_count=%s last_force_dispatch_at_ms=%s ts_ms=%s force_dispatch=%s",
            str(getattr(run_translation_check_job, "queue_name", None) or "").strip() or None,
            int(session_id),
            dispatch_job_id,
            message_id,
            dispatch_generation,
            redispatch_count,
            force_dispatch_at_ms if force_dispatch else current_dispatch_state.get("last_force_dispatch_at_ms"),
            broker_enqueued_at_ms,
            bool(force_dispatch),
        )
        return payload or {"status": "pending"}
    except Exception as exc:
        logging.exception("enqueue_translation_check_job failed session_id=%s", session_id)
        set_translation_check_job_status(int(session_id), status="failed", error=str(exc))
        raise


def _translation_check_completion_enqueue_key(session_id: int) -> str:
    return translation_check_completion_enqueue_idempotency_key(int(session_id))


def enqueue_translation_check_completion_job(
    session_id: int,
    *,
    correlation_id: str | None = None,
    request_id: str | None = None,
    force: bool = False,
) -> dict[str, object]:
    if not can_enqueue_background_jobs():
        raise RuntimeError("background_jobs_unavailable")

    enqueue_key = _translation_check_completion_enqueue_key(int(session_id))
    claimed = False
    client = get_redis_client()
    if client is not None and not force:
        try:
            claimed = bool(
                client.set(
                    enqueue_key,
                    uuid4().hex,
                    nx=True,
                    ex=int(_TRANSLATION_CHECK_COMPLETION_ENQUEUE_TTL_SEC),
                )
            )
        except Exception:
            claimed = False
    else:
        claimed = True

    if not claimed and not force:
        return {"enqueued": False, "session_id": int(session_id), "reason": "cooldown"}

    try:
        set_translation_check_completion_state(
            int(session_id),
            {
                "terminal_ready": True,
                "completion_job_enqueued": True,
                "completion_job_started_at_ms": None,
                "completion_done": False,
                "completion_done_at_ms": None,
                "summary_available": False,
            },
        )
        get_dramatiq_broker()
        from backend.background_jobs import run_translation_check_completion_job

        message = run_translation_check_completion_job.send(
            session_id=int(session_id),
            correlation_id=str(correlation_id or "").strip() or None,
            request_id=str(request_id or "").strip() or None,
        )
        return {
            "enqueued": True,
            "session_id": int(session_id),
            "job_id": str(getattr(message, "message_id", None) or "").strip() or None,
        }
    except Exception:
        if client is not None:
            try:
                client.delete(enqueue_key)
            except Exception:
                pass
        logging.exception("enqueue_translation_check_completion_job failed session_id=%s", session_id)
        raise


def get_translation_fill_job_status(session_id: int) -> dict | None:
    client = get_redis_client()
    if client is None:
        return None
    raw = client.get(_translation_fill_job_key(int(session_id)))
    if not raw:
        return None
    try:
        payload = json.loads(raw)
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def is_translation_fill_job_status_fast_path_eligible(payload: dict | None) -> bool:
    if not isinstance(payload, dict):
        return False
    status = str(payload.get("status") or "").strip().lower()
    if status not in {"pending", "running"}:
        return False
    try:
        updated_at_ms = int(payload.get("updated_at_ms") or 0)
    except Exception:
        updated_at_ms = 0
    if updated_at_ms <= 0:
        return False
    age_ms = max(0, int(time.time() * 1000) - updated_at_ms)
    return age_ms < _TRANSLATION_FILL_FASTPATH_FRESH_MS


def set_translation_fill_job_status(
    session_id: int,
    *,
    status: str,
    job_id: str | None = None,
    error: str | None = None,
) -> dict | None:
    client = get_redis_client()
    if client is None:
        return None
    payload = {
        "status": str(status or "").strip() or "pending",
        "job_id": str(job_id or "").strip() or None,
        "error": str(error or "").strip() or None,
        "updated_at_ms": int(time.time() * 1000),
    }
    ttl = _TRANSLATION_FILL_JOB_TTL_SEC
    if payload["status"] == "ready":
        ttl = _TRANSLATION_FILL_READY_TTL_SEC
    elif payload["status"] == "failed":
        ttl = _TRANSLATION_FILL_FAILED_TTL_SEC
    client.setex(_translation_fill_job_key(int(session_id)), ttl, json.dumps(payload, ensure_ascii=False))
    return payload


def enqueue_translation_fill_job(
    *,
    user_id: int,
    username: str | None,
    session_id: int,
    topic: str,
    level: str | None,
    source_lang: str,
    target_lang: str,
    grammar_focus: dict | None,
    tested_skill_profile_seed: dict | None = None,
) -> dict:
    if not is_translation_sentence_fill_async_enabled():
        raise RuntimeError("translation_sentence_fill_async_disabled")
    current = get_translation_fill_job_status(int(session_id))
    if current and current.get("status") in {"pending", "running"}:
        return current
    set_translation_fill_job_status(int(session_id), status="pending")
    try:
        get_dramatiq_broker()
        from backend.background_jobs import run_translation_fill_job

        message = run_translation_fill_job.send(
            user_id=int(user_id),
            username=str(username or "").strip() or None,
            session_id=int(session_id),
            topic=str(topic or "").strip() or "Random sentences",
            level=str(level or "").strip() or None,
            source_lang=str(source_lang or "").strip() or "ru",
            target_lang=str(target_lang or "").strip() or "de",
            grammar_focus=grammar_focus if isinstance(grammar_focus, dict) else None,
            tested_skill_profile_seed=tested_skill_profile_seed if isinstance(tested_skill_profile_seed, dict) else None,
        )
        payload = set_translation_fill_job_status(
            int(session_id),
            status="pending",
            job_id=getattr(message, "message_id", None),
        )
        return payload or {"status": "pending"}
    except Exception as exc:
        logging.exception("enqueue_translation_fill_job failed session_id=%s", session_id)
        set_translation_fill_job_status(int(session_id), status="failed", error=str(exc))
        raise


def enqueue_finish_daily_summary_job(
    *,
    user_id: int,
    username: str | None,
    user_name: str | None,
    request_id: str | None = None,
    correlation_id: str | None = None,
) -> str | None:
    if not can_enqueue_background_jobs():
        raise RuntimeError("background_jobs_unavailable")
    try:
        get_dramatiq_broker()
        from backend.background_jobs import run_finish_daily_summary_job

        message = run_finish_daily_summary_job.send(
            user_id=int(user_id),
            username=str(username or "").strip() or None,
            user_name=str(user_name or "").strip() or None,
            request_id=str(request_id or "").strip() or None,
            correlation_id=str(correlation_id or "").strip() or None,
        )
        return str(getattr(message, "message_id", None) or "").strip() or None
    except Exception:
        logging.exception("enqueue_finish_daily_summary_job failed user_id=%s", user_id)
        raise


def enqueue_translation_result_side_effects_job(
    *,
    user_id: int,
    original_text: str,
    user_translation: str,
    sentence_pk_id: int | None,
    session_id: int | None,
    sentence_id_for_mistake: int,
    score_value: int,
    correct_translation: str | None,
    categories: list[str] | None,
    subcategories: list[str] | None,
    source_lang: str = "ru",
    target_lang: str = "de",
) -> str | None:
    if not can_enqueue_background_jobs():
        raise RuntimeError("background_jobs_unavailable")
    try:
        get_dramatiq_broker()
        from backend.background_jobs import run_translation_result_side_effects_job

        message = run_translation_result_side_effects_job.send(
            user_id=int(user_id),
            original_text=str(original_text or ""),
            user_translation=str(user_translation or ""),
            sentence_pk_id=int(sentence_pk_id) if sentence_pk_id is not None else None,
            session_id=int(session_id) if session_id is not None else None,
            sentence_id_for_mistake=int(sentence_id_for_mistake),
            score_value=int(score_value),
            correct_translation=str(correct_translation or "").strip() or None,
            categories=list(categories or []),
            subcategories=list(subcategories or []),
            source_lang=str(source_lang or "ru").strip().lower() or "ru",
            target_lang=str(target_lang or "de").strip().lower() or "de",
        )
        return str(getattr(message, "message_id", None) or "").strip() or None
    except Exception:
        logging.exception(
            "enqueue_translation_result_side_effects_job failed user_id=%s sentence_id_for_mistake=%s",
            user_id,
            sentence_id_for_mistake,
        )
        raise
