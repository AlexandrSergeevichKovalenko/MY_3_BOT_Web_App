import json
import logging
import os
import time
from datetime import datetime, timezone
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
_TRANSLATION_FILL_JOB_TTL_SEC = max(
    300, int((os.getenv("TRANSLATION_FILL_JOB_TTL_SEC") or "3600").strip())
)
_TRANSLATION_FILL_READY_TTL_SEC = max(
    60, int((os.getenv("TRANSLATION_FILL_READY_TTL_SEC") or "600").strip())
)
_TRANSLATION_FILL_FAILED_TTL_SEC = max(
    60, int((os.getenv("TRANSLATION_FILL_FAILED_TTL_SEC") or "600").strip())
)


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


def can_enqueue_background_jobs() -> bool:
    return bool(get_redis_url())


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


def _youtube_transcript_job_key(video_id: str, lang: str | None) -> str:
    normalized_video_id = str(video_id or "").strip()
    normalized_lang = str(lang or "").strip().lower() or "auto"
    return f"yt:transcript:job:{normalized_video_id}:{normalized_lang}"


def _translation_check_job_key(session_id: int) -> str:
    return f"translation_check:job:{int(session_id)}"


def _translation_fill_job_key(session_id: int) -> str:
    return f"translation_fill:job:{int(session_id)}"


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


def set_translation_check_job_status(
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
) -> dict:
    if not is_translation_check_async_enabled():
        raise RuntimeError("translation_check_async_disabled")
    from backend.database import (
        get_translation_check_session_runtime,
        mark_translation_check_session_dispatched,
    )

    current = get_translation_check_job_status(int(session_id))
    if current and current.get("status") in {"pending", "running"}:
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
    if runtime_status == "queued" and dispatched_age_ms is not None and dispatched_age_ms < _TRANSLATION_CHECK_PENDING_STALE_REQUEUE_MS:
        payload = set_translation_check_job_status(
            int(session_id),
            status="pending",
            job_id=runtime.get("dispatched_job_id"),
        )
        return payload or {"status": "pending"}
    if runtime_status == "running" and heartbeat_age_ms is not None and heartbeat_age_ms < _TRANSLATION_CHECK_RUNNING_STALE_REQUEUE_MS:
        payload = set_translation_check_job_status(
            int(session_id),
            status="running",
            job_id=runtime.get("worker_job_id") or runtime.get("dispatched_job_id"),
        )
        return payload or {"status": "running"}

    dispatch_job_id = uuid4().hex
    runtime = mark_translation_check_session_dispatched(
        session_id=int(session_id),
        dispatch_job_id=dispatch_job_id,
    )
    if not runtime:
        raise RuntimeError(f"translation_check_session_not_dispatchable:{int(session_id)}")
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
        return payload or {"status": "pending"}
    except Exception as exc:
        logging.exception("enqueue_translation_check_job failed session_id=%s", session_id)
        set_translation_check_job_status(int(session_id), status="failed", error=str(exc))
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
