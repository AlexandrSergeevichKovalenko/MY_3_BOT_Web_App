"""
Narrow TTS-specific admin monitor recording and failure-alert helpers.

This module owns only the TTS admin-monitor write/read/summary/alert slice
needed by TTS generation orchestration. It remains process-local and is a
dependency-isolation step only.
"""

from __future__ import annotations

from collections import Counter, deque
import logging
import os
import threading
import time

from backend.database import (
    delete_old_tts_admin_monitor_events,
    get_admin_telegram_ids,
    list_tts_admin_monitor_events_since,
    record_tts_admin_monitor_event as persist_tts_admin_monitor_event,
)
from backend.telegram_notify import _send_private_message


TTS_ADMIN_DIGEST_WINDOW_MINUTES = max(
    15,
    min(
        720,
        int(
            (
                os.getenv("TTS_ADMIN_DIGEST_WINDOW_MINUTES")
                or os.getenv("TTS_ADMIN_DIGEST_INTERVAL_MINUTES")
                or "720"
            ).strip()
            or "720"
        ),
    ),
)
TTS_ADMIN_ALERT_BURST_WINDOW_MINUTES = max(
    1,
    min(120, int((os.getenv("TTS_ADMIN_ALERT_BURST_WINDOW_MINUTES") or "5").strip() or "5")),
)
TTS_ADMIN_ALERT_FAILURE_WINDOW_MINUTES = max(
    1,
    min(120, int((os.getenv("TTS_ADMIN_ALERT_FAILURE_WINDOW_MINUTES") or "10").strip() or "10")),
)
TTS_ADMIN_ALERT_FAILURE_THRESHOLD = max(
    1,
    min(500, int((os.getenv("TTS_ADMIN_ALERT_FAILURE_THRESHOLD") or "5").strip() or "5")),
)
TTS_ADMIN_ALERT_PENDING_AGE_MINUTES = max(
    1,
    min(240, int((os.getenv("TTS_ADMIN_ALERT_PENDING_AGE_MINUTES") or "10").strip() or "10")),
)
TTS_ADMIN_ALERT_COOLDOWN_MINUTES = max(
    5,
    min(240, int((os.getenv("TTS_ADMIN_ALERT_COOLDOWN_MINUTES") or "30").strip() or "30")),
)

_TTS_ADMIN_MONITOR_LOCK = threading.Lock()
_TTS_ADMIN_MONITOR_EVENTS = deque()
_TTS_ADMIN_ALERT_LAST_SENT: dict[str, float] = {}


def _tts_admin_monitor_retention_seconds() -> int:
    return max(
        4 * 3600,
        int(TTS_ADMIN_DIGEST_WINDOW_MINUTES) * 120,
        int(TTS_ADMIN_ALERT_BURST_WINDOW_MINUTES) * 120,
        int(TTS_ADMIN_ALERT_FAILURE_WINDOW_MINUTES) * 120,
        int(TTS_ADMIN_ALERT_PENDING_AGE_MINUTES) * 120,
    )


def _prune_tts_admin_monitor_events_persistent() -> None:
    retention_seconds = _tts_admin_monitor_retention_seconds()
    try:
        delete_old_tts_admin_monitor_events(older_than_seconds=retention_seconds)
    except Exception:
        logging.debug("Failed to prune persistent TTS admin monitor events", exc_info=True)


def _prune_tts_admin_monitor_events_locked(now_ts: float) -> None:
    cutoff = float(now_ts) - float(_tts_admin_monitor_retention_seconds())
    while _TTS_ADMIN_MONITOR_EVENTS and float(_TTS_ADMIN_MONITOR_EVENTS[0].get("ts") or 0.0) < cutoff:
        _TTS_ADMIN_MONITOR_EVENTS.popleft()


def _record_tts_admin_monitor_event(
    kind: str,
    status: str,
    *,
    source: str = "",
    count: int = 1,
    chars: int = 0,
    duration_ms: int | None = None,
    meta: dict | None = None,
) -> None:
    now_ts = time.time()
    payload = {
        "ts": now_ts,
        "kind": str(kind or "").strip().lower() or "unknown",
        "status": str(status or "").strip().lower() or "unknown",
        "source": str(source or "").strip().lower() or "unknown",
        "count": max(0, int(count or 0)),
        "chars": max(0, int(chars or 0)),
        "duration_ms": int(duration_ms) if duration_ms is not None else None,
        "meta": meta if isinstance(meta, dict) else {},
    }
    with _TTS_ADMIN_MONITOR_LOCK:
        _TTS_ADMIN_MONITOR_EVENTS.append(payload)
        _prune_tts_admin_monitor_events_locked(now_ts)
    try:
        persist_tts_admin_monitor_event(
            kind=payload["kind"],
            status=payload["status"],
            source=payload["source"],
            count=payload["count"],
            chars=payload["chars"],
            duration_ms=payload["duration_ms"],
            meta=payload["meta"],
        )
        _prune_tts_admin_monitor_events_persistent()
    except Exception:
        logging.debug("Failed to persist TTS admin monitor event", exc_info=True)


def _get_tts_admin_monitor_fallback_window(seconds: int) -> list[dict]:
    window_seconds = max(1, int(seconds or 1))
    now_ts = time.time()
    with _TTS_ADMIN_MONITOR_LOCK:
        _prune_tts_admin_monitor_events_locked(now_ts)
        fallback_events = list(_TTS_ADMIN_MONITOR_EVENTS)
    cutoff = now_ts - window_seconds
    return [item for item in fallback_events if float(item.get("ts") or 0.0) >= cutoff]


def _send_tts_admin_message(text: str) -> bool:
    admin_ids = sorted(int(item) for item in get_admin_telegram_ids() if int(item) > 0)
    if not admin_ids:
        return False
    sent = False
    for admin_id in admin_ids:
        try:
            _send_private_message(int(admin_id), text, disable_web_page_preview=True)
            sent = True
        except Exception:
            logging.warning("Failed to send TTS admin message to admin_id=%s", admin_id, exc_info=True)
    return sent


def _get_tts_admin_monitor_window(seconds: int) -> list[dict]:
    window_seconds = max(1, int(seconds or 1))
    fallback_events = _get_tts_admin_monitor_fallback_window(window_seconds)
    _prune_tts_admin_monitor_events_persistent()
    try:
        db_events = list_tts_admin_monitor_events_since(window_seconds=window_seconds)
        if db_events:
            return db_events
    except Exception:
        logging.debug("Failed to load persistent TTS admin monitor window", exc_info=True)
    return fallback_events


def _should_send_tts_admin_alert(alert_key: str) -> bool:
    now_ts = time.time()
    cooldown_seconds = int(TTS_ADMIN_ALERT_COOLDOWN_MINUTES) * 60
    with _TTS_ADMIN_MONITOR_LOCK:
        last_sent_ts = float(_TTS_ADMIN_ALERT_LAST_SENT.get(str(alert_key), 0.0) or 0.0)
        if last_sent_ts and now_ts - last_sent_ts < cooldown_seconds:
            return False
        _TTS_ADMIN_ALERT_LAST_SENT[str(alert_key)] = now_ts
    return True


def _shorten_tts_admin_text(value, limit: int = 160) -> str:
    text = " ".join(str(value or "").split())
    if not text:
        return ""
    safe_limit = max(16, int(limit or 160))
    if len(text) <= safe_limit:
        return text
    return text[: max(1, safe_limit - 3)].rstrip() + "..."


def _tts_admin_event_weight(item: dict) -> int:
    return max(1, int(item.get("count") or 1))


def _summarize_tts_failure_window(events: list[dict]) -> dict:
    failure_events = [
        item
        for item in events
        if item.get("status") == "error" and item.get("kind") in {"generation", "generation_enqueue", "prewarm_run"}
    ]
    success_count = sum(
        int(item.get("count") or 0)
        for item in events
        if item.get("kind") == "generation" and item.get("status") in {"generated", "hit"}
    )
    if not failure_events:
        return {
            "failure_count": 0,
            "success_count": success_count,
            "top_kind": "",
            "top_source": "",
            "top_error_code": "",
            "top_exception_type": "",
            "top_failure_stage": "",
            "recent_examples": [],
        }

    counters = {
        "kind": Counter(),
        "source": Counter(),
        "error_code": Counter(),
        "exception_type": Counter(),
        "failure_stage": Counter(),
    }
    recent_examples: list[str] = []

    for item in failure_events:
        weight = _tts_admin_event_weight(item)
        meta = item.get("meta") or {}
        kind = _shorten_tts_admin_text(item.get("kind"), 48)
        source = _shorten_tts_admin_text(item.get("source"), 48)
        error_code = _shorten_tts_admin_text(meta.get("error_code"), 64)
        exception_type = _shorten_tts_admin_text(meta.get("exception_type"), 64)
        failure_stage = _shorten_tts_admin_text(meta.get("failure_stage"), 64)
        if kind:
            counters["kind"][kind] += weight
        if source:
            counters["source"][source] += weight
        if error_code:
            counters["error_code"][error_code] += weight
        if exception_type:
            counters["exception_type"][exception_type] += weight
        if failure_stage:
            counters["failure_stage"][failure_stage] += weight

    for item in reversed(failure_events):
        meta = item.get("meta") or {}
        parts = []
        kind = _shorten_tts_admin_text(item.get("kind"), 32)
        source = _shorten_tts_admin_text(item.get("source"), 32)
        error_code = _shorten_tts_admin_text(meta.get("error_code"), 48)
        exception_type = _shorten_tts_admin_text(meta.get("exception_type"), 48)
        failure_stage = _shorten_tts_admin_text(meta.get("failure_stage"), 48)
        error_message = _shorten_tts_admin_text(meta.get("error_message"), 120)
        if kind:
            parts.append(kind)
        if source:
            parts.append(source)
        if error_code:
            parts.append(error_code)
        if exception_type:
            parts.append(exception_type)
        if failure_stage:
            parts.append(f"stage={failure_stage}")
        example = " / ".join(parts)
        if error_message:
            example = f"{example}: {error_message}" if example else error_message
        if example:
            recent_examples.append(example)
        if len(recent_examples) >= 2:
            break

    def _top(counter_name: str) -> tuple[str, int]:
        counter = counters[counter_name]
        if not counter:
            return "", 0
        label, count = counter.most_common(1)[0]
        return str(label), int(count)

    top_kind, top_kind_count = _top("kind")
    top_source, top_source_count = _top("source")
    top_error_code, top_error_code_count = _top("error_code")
    top_exception_type, top_exception_type_count = _top("exception_type")
    top_failure_stage, top_failure_stage_count = _top("failure_stage")
    return {
        "failure_count": sum(_tts_admin_event_weight(item) for item in failure_events),
        "success_count": success_count,
        "top_kind": top_kind,
        "top_kind_count": top_kind_count,
        "top_source": top_source,
        "top_source_count": top_source_count,
        "top_error_code": top_error_code,
        "top_error_code_count": top_error_code_count,
        "top_exception_type": top_exception_type,
        "top_exception_type_count": top_exception_type_count,
        "top_failure_stage": top_failure_stage,
        "top_failure_stage_count": top_failure_stage_count,
        "recent_examples": recent_examples,
    }


def _maybe_send_tts_admin_failure_alert() -> None:
    if TTS_ADMIN_ALERT_FAILURE_THRESHOLD <= 0:
        return
    events = _get_tts_admin_monitor_window(int(TTS_ADMIN_ALERT_FAILURE_WINDOW_MINUTES) * 60)
    failure_summary = _summarize_tts_failure_window(events)
    failure_count = int(failure_summary.get("failure_count") or 0)
    if failure_count < int(TTS_ADMIN_ALERT_FAILURE_THRESHOLD):
        return
    if not _should_send_tts_admin_alert("tts_failure_burst"):
        return
    extra_lines = []
    if failure_summary.get("success_count"):
        extra_lines.append(
            f"Successful audio jobs in same window: {int(failure_summary.get('success_count') or 0)}"
        )
    if failure_summary.get("top_kind"):
        extra_lines.append(
            f"Main failing process: {failure_summary['top_kind']} ({int(failure_summary.get('top_kind_count') or 0)})"
        )
    if failure_summary.get("top_source"):
        extra_lines.append(
            f"Main failing source: {failure_summary['top_source']} ({int(failure_summary.get('top_source_count') or 0)})"
        )
    if failure_summary.get("top_error_code"):
        extra_lines.append(
            f"Main error code: {failure_summary['top_error_code']} ({int(failure_summary.get('top_error_code_count') or 0)})"
        )
    if failure_summary.get("top_exception_type"):
        extra_lines.append(
            f"Main exception type: {failure_summary['top_exception_type']} ({int(failure_summary.get('top_exception_type_count') or 0)})"
        )
    if failure_summary.get("top_failure_stage"):
        extra_lines.append(
            f"Main failing step: {failure_summary['top_failure_stage']} ({int(failure_summary.get('top_failure_stage_count') or 0)})"
        )
    for index, sample in enumerate(failure_summary.get("recent_examples") or [], start=1):
        extra_lines.append(f"Recent example {index}: {sample}")
    details_block = ("\n" + "\n".join(extra_lines)) if extra_lines else ""
    message_text = (
        "🚨 TTS failure alert\n\n"
        f"Errors in the last {int(TTS_ADMIN_ALERT_FAILURE_WINDOW_MINUTES)} min: {failure_count}\n"
        f"Threshold: {int(TTS_ADMIN_ALERT_FAILURE_THRESHOLD)}\n\n"
        f"Check Google TTS, R2 and recent deploy/logs.{details_block}"
    )
    _send_tts_admin_message(message_text)
