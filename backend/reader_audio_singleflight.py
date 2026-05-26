import threading
import time


_READER_AUDIO_INFLIGHT_LOCK = threading.Lock()
_READER_AUDIO_INFLIGHT: dict[str, dict[str, object]] = {}


def acquire_reader_audio_singleflight_slot(
    cache_key: str,
    *,
    stale_seconds: float = 120.0,
) -> tuple[bool, threading.Event | None]:
    safe_cache_key = str(cache_key or "").strip()
    if not safe_cache_key:
        return True, None

    with _READER_AUDIO_INFLIGHT_LOCK:
        existing = _READER_AUDIO_INFLIGHT.get(safe_cache_key)
        if existing and isinstance(existing.get("event"), threading.Event):
            started_at = float(existing.get("started_at") or 0.0)
            if started_at > 0 and (time.time() - started_at) > stale_seconds:
                _READER_AUDIO_INFLIGHT.pop(safe_cache_key, None)
            else:
                return False, existing["event"]

        event = threading.Event()
        _READER_AUDIO_INFLIGHT[safe_cache_key] = {
            "event": event,
            "started_at": time.time(),
        }
        return True, event


def release_reader_audio_singleflight_slot(
    cache_key: str,
    event: threading.Event | None,
) -> None:
    safe_cache_key = str(cache_key or "").strip()
    if not safe_cache_key or event is None:
        return

    with _READER_AUDIO_INFLIGHT_LOCK:
        existing = _READER_AUDIO_INFLIGHT.get(safe_cache_key)
        if existing and existing.get("event") is event:
            event.set()
            _READER_AUDIO_INFLIGHT.pop(safe_cache_key, None)
