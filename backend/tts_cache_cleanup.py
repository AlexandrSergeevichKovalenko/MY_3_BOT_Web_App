"""
Worker-safe TTS cache cleanup jobs.

No import of backend_server — depends only on database and r2_storage.
"""
import logging
import os

from backend.database import (
    delete_stale_tts_db_cache,
    delete_tts_object_cache_entry,
    list_stale_ready_tts_objects,
)
from backend.r2_storage import r2_delete_object


def run_tts_db_cache_cleanup() -> None:
    enabled = (os.getenv("TTS_DB_CACHE_CLEANUP_ENABLED") or "1").strip().lower()
    if enabled not in ("1", "true", "yes", "on"):
        logging.info("ℹ️ TTS DB cache cleanup disabled by TTS_DB_CACHE_CLEANUP_ENABLED")
        return
    retention_days = int((os.getenv("TTS_DB_CACHE_RETENTION_DAYS") or "90").strip())
    try:
        result = delete_stale_tts_db_cache(older_than_days=retention_days)
        logging.info(
            "✅ TTS DB cache cleanup finished: retention_days=%s audio_rows=%s chunk_rows=%s total_rows=%s",
            retention_days,
            int(result.get("audio_rows") or 0),
            int(result.get("chunk_rows") or 0),
            int(result.get("total_rows") or 0),
        )
    except Exception:
        logging.exception("❌ TTS DB cache cleanup failed")


def run_tts_r2_cache_cleanup() -> None:
    enabled = (os.getenv("TTS_R2_CACHE_CLEANUP_ENABLED") or "1").strip().lower()
    if enabled not in ("1", "true", "yes", "on"):
        logging.info("ℹ️ TTS R2 cache cleanup disabled by TTS_R2_CACHE_CLEANUP_ENABLED")
        return
    retention_days = int((os.getenv("TTS_R2_CACHE_RETENTION_DAYS") or "60").strip())
    batch_limit = max(1, min(5000, int((os.getenv("TTS_R2_CACHE_CLEANUP_BATCH_LIMIT") or "500").strip())))
    deleted_objects = 0
    deleted_rows = 0
    missing_objects = 0
    failed_objects = 0
    try:
        candidates = list_stale_ready_tts_objects(
            limit=batch_limit,
            older_than_days=retention_days,
        )
        for item in candidates:
            cache_key = str(item.get("cache_key") or "").strip()
            object_key = str(item.get("object_key") or "").strip()
            if not cache_key or not object_key:
                continue
            try:
                removed = bool(r2_delete_object(object_key))
                if removed:
                    deleted_objects += 1
                else:
                    missing_objects += 1
                deleted_rows += int(delete_tts_object_cache_entry(cache_key=cache_key) or 0)
            except Exception:
                failed_objects += 1
                logging.exception(
                    "❌ Failed to delete stale R2 TTS object: cache_key=%s object_key=%s",
                    cache_key,
                    object_key,
                )
        logging.info(
            "✅ TTS R2 cache cleanup finished: retention_days=%s candidates=%s deleted_objects=%s missing_objects=%s deleted_rows=%s failed_objects=%s",
            retention_days,
            len(candidates),
            deleted_objects,
            missing_objects,
            deleted_rows,
            failed_objects,
        )
    except Exception:
        logging.exception("❌ TTS R2 cache cleanup failed")
