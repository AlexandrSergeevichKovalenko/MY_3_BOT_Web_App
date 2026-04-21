"""
Narrow TTS-specific admin monitor event recording.

This module owns only the event-recording side of the TTS admin-monitor
subsystem plus the minimal in-memory fallback state it directly requires.
It remains process-local and is a dependency-isolation step only.
"""

from __future__ import annotations

from collections import deque
import logging
import os
import threading
import time

from backend.database import (
    delete_old_tts_admin_monitor_events,
    record_tts_admin_monitor_event as persist_tts_admin_monitor_event,
)


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
TTS_ADMIN_ALERT_PENDING_AGE_MINUTES = max(
    1,
    min(240, int((os.getenv("TTS_ADMIN_ALERT_PENDING_AGE_MINUTES") or "10").strip() or "10")),
)

_TTS_ADMIN_MONITOR_LOCK = threading.Lock()
_TTS_ADMIN_MONITOR_EVENTS = deque()


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
