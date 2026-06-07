"""Lightweight in-process metrics foundation for load testing (Phase 1).

Implements the four metrics the load-testing audit flagged as missing:
  1. event-loop lag sampler
  2. per-handler latency measurement
  3. Telegram send/edit counters
  4. Dramatiq queue-depth gauge

All counters/gauges are in-memory, thread-safe, and passive — they record only
when explicitly called, so importing this module changes no runtime behaviour.
No dashboards; `snapshot()` returns a plain dict for a caller to log/export.
"""

from __future__ import annotations

import asyncio
import threading
import time
from contextlib import contextmanager
from typing import Any

_LOCK = threading.Lock()

# Per-handler latency: name -> {count, total_ms, max_ms}
_HANDLER_LATENCY: dict[str, dict[str, float]] = {}
# Telegram API counters
_TELEGRAM_COUNTERS: dict[str, int] = {"send": 0, "edit": 0, "callback_answer": 0, "other": 0}
# Event-loop lag gauge (milliseconds)
_LOOP_LAG = {"last_ms": 0.0, "max_ms": 0.0, "samples": 0}
# Last sampled Dramatiq queue depths
_QUEUE_DEPTHS: dict[str, int] = {}


# --------------------------------------------------------------------------- #
# 1. Event-loop lag sampler
# --------------------------------------------------------------------------- #

def record_event_loop_lag(lag_ms: float) -> None:
    with _LOCK:
        _LOOP_LAG["last_ms"] = float(lag_ms)
        _LOOP_LAG["max_ms"] = max(float(_LOOP_LAG["max_ms"]), float(lag_ms))
        _LOOP_LAG["samples"] = int(_LOOP_LAG["samples"]) + 1


async def event_loop_lag_sampler(interval_sec: float = 0.5) -> None:
    """Background task: sleeps `interval` and records how much longer it actually
    took than requested — that overshoot is event-loop lag. Run via
    asyncio.create_task(event_loop_lag_sampler())."""
    interval = max(0.05, float(interval_sec))
    loop = asyncio.get_event_loop()
    while True:
        start = loop.time()
        await asyncio.sleep(interval)
        elapsed = loop.time() - start
        record_event_loop_lag(max(0.0, (elapsed - interval) * 1000.0))


# --------------------------------------------------------------------------- #
# 2. Per-handler latency measurement
# --------------------------------------------------------------------------- #

def record_handler_latency(name: str, duration_ms: float) -> None:
    key = str(name or "unknown")
    with _LOCK:
        bucket = _HANDLER_LATENCY.setdefault(key, {"count": 0.0, "total_ms": 0.0, "max_ms": 0.0})
        bucket["count"] += 1
        bucket["total_ms"] += float(duration_ms)
        bucket["max_ms"] = max(bucket["max_ms"], float(duration_ms))


@contextmanager
def handler_latency(name: str):
    """Context manager timing a handler body: `with handler_latency("translation"): ...`."""
    start = time.perf_counter()
    try:
        yield
    finally:
        record_handler_latency(name, (time.perf_counter() - start) * 1000.0)


# --------------------------------------------------------------------------- #
# 3. Telegram send/edit counters
# --------------------------------------------------------------------------- #

def incr_telegram(kind: str = "send", n: int = 1) -> None:
    key = kind if kind in _TELEGRAM_COUNTERS else "other"
    with _LOCK:
        _TELEGRAM_COUNTERS[key] = _TELEGRAM_COUNTERS.get(key, 0) + int(n)


# --------------------------------------------------------------------------- #
# 4. Dramatiq queue-depth gauge
# --------------------------------------------------------------------------- #

def sample_queue_depths(queue_names: list[str], redis_client: Any | None = None) -> dict[str, int]:
    """Best-effort Dramatiq queue depths via the Redis broker.

    Reads the per-queue message list length. The exact key layout can vary by
    dramatiq version, so this probes a couple of known patterns and records 0 on
    miss. Returns the sampled mapping and stores it for snapshot()."""
    if redis_client is None:
        try:
            from backend.job_queue import get_redis_client
            redis_client = get_redis_client()
        except Exception:
            redis_client = None
    depths: dict[str, int] = {}
    for q in queue_names:
        depth = 0
        if redis_client is not None:
            for key in (f"dramatiq:{q}", f"dramatiq:{q}.msgs"):
                try:
                    val = redis_client.llen(key)
                    if val:
                        depth = int(val)
                        break
                except Exception:
                    continue
        depths[str(q)] = depth
    with _LOCK:
        _QUEUE_DEPTHS.clear()
        _QUEUE_DEPTHS.update(depths)
    return depths


# --------------------------------------------------------------------------- #
# Snapshot / reset
# --------------------------------------------------------------------------- #

def snapshot() -> dict[str, Any]:
    with _LOCK:
        handlers = {
            name: {
                "count": int(b["count"]),
                "total_ms": round(b["total_ms"], 3),
                "max_ms": round(b["max_ms"], 3),
                "avg_ms": round(b["total_ms"] / b["count"], 3) if b["count"] else 0.0,
            }
            for name, b in _HANDLER_LATENCY.items()
        }
        return {
            "event_loop_lag": dict(_LOOP_LAG),
            "handler_latency": handlers,
            "telegram": dict(_TELEGRAM_COUNTERS),
            "queue_depths": dict(_QUEUE_DEPTHS),
        }


def reset() -> None:
    with _LOCK:
        _HANDLER_LATENCY.clear()
        for k in _TELEGRAM_COUNTERS:
            _TELEGRAM_COUNTERS[k] = 0
        _LOOP_LAG.update({"last_ms": 0.0, "max_ms": 0.0, "samples": 0})
        _QUEUE_DEPTHS.clear()
