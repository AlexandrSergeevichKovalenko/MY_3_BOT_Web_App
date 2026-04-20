import logging
import os

from backend.database import get_db_connection_context
from backend.database import delete_stale_tts_db_cache
from backend.translation_workflow import finalize_open_translation_sessions


def run_translation_sessions_auto_close_job() -> None:
    enabled = (os.getenv("TRANSLATION_SESSIONS_AUTO_CLOSE_ENABLED") or "1").strip().lower()
    if enabled not in ("1", "true", "yes", "on"):
        logging.info("ℹ️ Translation sessions auto-close disabled by TRANSLATION_SESSIONS_AUTO_CLOSE_ENABLED")
        return
    try:
        result = finalize_open_translation_sessions()
        logging.info("✅ Translation sessions auto-close finished: %s", result)
    except Exception:
        logging.exception("❌ Translation sessions auto-close failed")
        raise


def run_flashcard_feel_cleanup_job() -> None:
    enabled = (os.getenv("FLASHCARD_FEEL_CLEANUP_ENABLED") or "1").strip().lower()
    if enabled not in ("1", "true", "yes", "on"):
        logging.info("ℹ️ Flashcard feel cleanup disabled by FLASHCARD_FEEL_CLEANUP_ENABLED")
        return
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE bt_3_webapp_dictionary_queries
                    SET response_json = response_json - 'feel_explanation' - 'feel_feedback'
                    WHERE response_json IS NOT NULL
                      AND (
                        response_json ? 'feel_explanation'
                        OR response_json ? 'feel_feedback'
                      );
                    """
                )
                cleaned_rows = int(cursor.rowcount or 0)
        logging.info("✅ Flashcard feel cleanup finished: cleaned_rows=%s", cleaned_rows)
    except Exception:
        logging.exception("❌ Flashcard feel cleanup failed")
        raise


def run_tts_db_cache_cleanup_job() -> None:
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
        raise
