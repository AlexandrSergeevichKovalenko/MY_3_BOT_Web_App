"""
Observability primitives with no backend_server dependency.

Extracted from backend_server.py to allow _run_tts_generation_job and other
execution paths to import them without pulling in the full server module.

Exported:
  _sanitize_observability_id
  _build_observability_correlation_id
  _elapsed_ms_since
  _log_flow_observation
"""

import itertools
import json
import logging
import os
import re
import time
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from flask import has_request_context, request

# Dedicated logger so flow-observation volume can be tuned independently of the
# rest of the app's logging. Set OBS_LOG_LEVEL=WARNING (or higher) on a service
# to silence obs entirely — this is an instant, zero-cost kill switch because
# _log_flow_observation short-circuits before building the event or serializing
# it when the logger is not enabled for INFO.
_OBS_LOGGER = logging.getLogger("flow_obs")
try:
    _OBS_LOGGER.setLevel(os.getenv("OBS_LOG_LEVEL", "INFO").upper())
except (ValueError, TypeError):
    _OBS_LOGGER.setLevel(logging.INFO)

# Deterministic 1-in-N sampling. OBS_SAMPLE_RATE=1 keeps every observation (default);
# =8 keeps ~1/8 and drops the rest before any serialization work. 0/invalid -> 1.
try:
    _OBS_SAMPLE_RATE = max(1, int(os.getenv("OBS_SAMPLE_RATE", "1")))
except (ValueError, TypeError):
    _OBS_SAMPLE_RATE = 1
_OBS_COUNTER = itertools.count()


def _sanitize_observability_id(value: Any, *, max_len: int = 128) -> str | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    candidate = raw[:max_len]
    cleaned = re.sub(r"[^a-zA-Z0-9._:-]+", "-", candidate).strip("-")
    return cleaned or None


def _build_observability_correlation_id(
    *,
    payload: dict | None = None,
    fallback_seed: Any = None,
    prefix: str = "flow",
) -> str:
    body = payload if isinstance(payload, dict) else {}
    candidates = [
        body.get("correlation_id"),
        body.get("request_id"),
    ]
    if has_request_context():
        candidates = [
            request.headers.get("X-Correlation-ID"),
            request.headers.get("X-Request-ID"),
            request.args.get("correlation_id"),
            request.args.get("request_id"),
            *candidates,
        ]
    for candidate in candidates:
        safe = _sanitize_observability_id(candidate)
        if safe:
            return safe
    safe_prefix = _sanitize_observability_id(prefix, max_len=24) or "flow"
    safe_seed = _sanitize_observability_id(fallback_seed, max_len=64)
    if safe_seed:
        return f"{safe_prefix}_{safe_seed}"
    return f"{safe_prefix}_{uuid4().hex[:16]}"


def _elapsed_ms_since(start_perf: float, end_perf: float | None = None) -> int:
    end_value = end_perf if end_perf is not None else time.perf_counter()
    return max(0, int((end_value - start_perf) * 1000))


def _log_flow_observation(flow: str, stage: str, **fields: Any) -> None:
    # Short-circuit BEFORE building the event dict / calling json.dumps. On the
    # hot path (this is invoked from ~300 call sites, several times per request)
    # the serialization is the real cost, not the log write — skip it entirely
    # when obs is muted or when this observation is sampled out.
    if not _OBS_LOGGER.isEnabledFor(logging.INFO):
        return
    if _OBS_SAMPLE_RATE > 1 and next(_OBS_COUNTER) % _OBS_SAMPLE_RATE != 0:
        return
    event: dict[str, Any] = {
        "flow": str(flow or "").strip() or "unknown",
        "stage": str(stage or "").strip() or "unknown",
        "ts": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
    }
    for key, value in fields.items():
        if value is None:
            continue
        event[str(key)] = value
    try:
        _OBS_LOGGER.info("obs %s", json.dumps(event, ensure_ascii=False, separators=(",", ":"), default=str))
    except Exception:
        _OBS_LOGGER.info("obs flow=%s stage=%s fields=%s", event.get("flow"), event.get("stage"), fields)
