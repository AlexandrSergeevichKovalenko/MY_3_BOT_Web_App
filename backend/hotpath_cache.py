import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable


_SHARED_EXECUTOR_LOCK = threading.Lock()
_SHARED_EXECUTORS: dict[tuple[str, int], ThreadPoolExecutor] = {}


def _get_shared_executor(*, name: str, max_workers: int) -> ThreadPoolExecutor:
    safe_workers = max(1, int(max_workers or 1))
    key = (str(name or "hotpath").strip() or "hotpath", safe_workers)
    with _SHARED_EXECUTOR_LOCK:
        executor = _SHARED_EXECUTORS.get(key)
        if executor is None:
            executor = ThreadPoolExecutor(
                max_workers=safe_workers,
                thread_name_prefix=f"{key[0]}_refresh",
            )
            _SHARED_EXECUTORS[key] = executor
        return executor


class HotPathCacheManager:
    def __init__(
        self,
        *,
        name: str,
        max_entries: int = 10000,
        refresh_workers: int = 0,
        shared_executor_name: str | None = None,
    ) -> None:
        self.name = str(name or "hotpath").strip() or "hotpath"
        self.max_entries = max(128, int(max_entries or 10000))
        self._lock = threading.Lock()
        self._entries: dict[Any, dict[str, Any]] = {}
        self._inflight_refreshes: set[Any] = set()
        if int(refresh_workers or 0) > 0:
            if shared_executor_name:
                self._executor = _get_shared_executor(
                    name=str(shared_executor_name),
                    max_workers=max(1, int(refresh_workers)),
                )
            else:
                self._executor = ThreadPoolExecutor(
                    max_workers=max(1, int(refresh_workers)),
                    thread_name_prefix=f"{self.name}_refresh",
                )
        else:
            self._executor = None

    def get(self, key: Any, *, allow_stale: bool = False) -> dict[str, Any] | None:
        now_ts = time.time()
        with self._lock:
            entry = self._entries.get(key)
            if not entry:
                return None
            stale_until_ts = float(entry.get("stale_until_ts") or 0.0)
            if stale_until_ts <= now_ts:
                self._entries.pop(key, None)
                return None
            fresh_until_ts = float(entry.get("fresh_until_ts") or 0.0)
            if not allow_stale and fresh_until_ts <= now_ts:
                return None
            entry["last_access_ts"] = now_ts
            return dict(entry)

    def put(
        self,
        key: Any,
        payload: Any,
        *,
        fresh_ttl_sec: int,
        stale_ttl_sec: int | None = None,
        meta: dict[str, Any] | None = None,
        refreshed_at_ts: float | None = None,
    ) -> dict[str, Any]:
        now_ts = float(refreshed_at_ts or time.time())
        safe_fresh_ttl = max(1, int(fresh_ttl_sec or 1))
        safe_stale_ttl = max(safe_fresh_ttl, int(stale_ttl_sec or safe_fresh_ttl))
        entry = {
            "payload": payload,
            "meta": dict(meta or {}),
            "refreshed_at_ts": now_ts,
            "fresh_until_ts": now_ts + safe_fresh_ttl,
            "stale_until_ts": now_ts + safe_stale_ttl,
            "last_access_ts": now_ts,
        }
        with self._lock:
            self._entries[key] = entry
            self._prune_locked(now_ts)
        return dict(entry)

    def mark_stale(self, key: Any) -> None:
        now_ts = time.time()
        with self._lock:
            entry = self._entries.get(key)
            if not entry:
                return
            entry["fresh_until_ts"] = min(float(entry.get("fresh_until_ts") or now_ts), now_ts - 1.0)

    def invalidate(self, key: Any) -> None:
        with self._lock:
            self._entries.pop(key, None)

    def invalidate_prefix(self, prefix: Any) -> int:
        removed = 0
        with self._lock:
            keys_to_remove = [
                key
                for key in self._entries.keys()
                if isinstance(key, tuple) and isinstance(prefix, tuple) and key[: len(prefix)] == prefix
            ]
            for key in keys_to_remove:
                self._entries.pop(key, None)
                removed += 1
        return removed

    def enqueue_refresh(self, key: Any, refresh_fn: Callable[[], Any]) -> bool:
        if self._executor is None:
            return False
        with self._lock:
            if key in self._inflight_refreshes:
                return False
            self._inflight_refreshes.add(key)

        def _run() -> None:
            try:
                refresh_fn()
            except Exception:
                logging.warning("HotPathCacheManager refresh failed: name=%s key=%s", self.name, key, exc_info=True)
            finally:
                with self._lock:
                    self._inflight_refreshes.discard(key)

        self._executor.submit(_run)
        return True

    def _prune_locked(self, now_ts: float) -> None:
        expired_keys = [
            key
            for key, entry in self._entries.items()
            if float(entry.get("stale_until_ts") or 0.0) <= now_ts
        ]
        for key in expired_keys:
            self._entries.pop(key, None)

        overflow = len(self._entries) - self.max_entries
        if overflow <= 0:
            return
        oldest_keys = sorted(
            self._entries.items(),
            key=lambda item: float(item[1].get("last_access_ts") or 0.0),
        )[:overflow]
        for key, _entry in oldest_keys:
            self._entries.pop(key, None)
