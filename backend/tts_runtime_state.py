"""
Narrow TTS-only runtime state.

This module intentionally keeps only process-local in-memory TTS state that is
owned by the current execution model. It is not a scaling solution; it is a
small dependency-isolation step for the TTS generation orchestration layer.
"""

import threading


_TTS_URL_POLL_ATTEMPTS_LOCK = threading.Lock()
_TTS_URL_POLL_ATTEMPTS: dict[str, int] = {}


def _increment_tts_url_poll_attempt(cache_key: str) -> int:
    safe_cache_key = str(cache_key or "").strip()
    if not safe_cache_key:
        return 0
    with _TTS_URL_POLL_ATTEMPTS_LOCK:
        next_value = int(_TTS_URL_POLL_ATTEMPTS.get(safe_cache_key) or 0) + 1
        _TTS_URL_POLL_ATTEMPTS[safe_cache_key] = next_value
        if len(_TTS_URL_POLL_ATTEMPTS) > 10000:
            _TTS_URL_POLL_ATTEMPTS.clear()
        return next_value


def _clear_tts_url_poll_attempt(cache_key: str) -> None:
    safe_cache_key = str(cache_key or "").strip()
    if not safe_cache_key:
        return
    with _TTS_URL_POLL_ATTEMPTS_LOCK:
        _TTS_URL_POLL_ATTEMPTS.pop(safe_cache_key, None)
