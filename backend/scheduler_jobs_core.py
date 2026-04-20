import logging
import os
from datetime import datetime
from zoneinfo import ZoneInfo

import requests

from backend.database import get_db_connection_context
from backend.database import delete_stale_tts_db_cache
from backend.database import get_pending_telegram_system_messages
from backend.database import mark_telegram_system_message_deleted
from backend.translation_workflow import finalize_open_translation_sessions
from backend.tts_cache_cleanup import run_tts_r2_cache_cleanup


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


def run_tts_r2_cache_cleanup_job() -> None:
    try:
        run_tts_r2_cache_cleanup()
    except Exception:
        logging.exception("❌ TTS R2 cache cleanup failed")
        raise


def _delete_telegram_message(chat_id: int, message_id: int) -> None:
    token = os.getenv("TELEGRAM_Deutsch_BOT_TOKEN")
    url = f"https://api.telegram.org/bot{token}/deleteMessage"
    response = requests.post(
        url,
        json={"chat_id": int(chat_id), "message_id": int(message_id)},
        timeout=15,
    )
    if response.status_code >= 400:
        raise RuntimeError(f"Telegram API error: {response.text}")
    try:
        payload = response.json() if response.content else {}
    except Exception:
        payload = {}
    if not payload.get("ok", False):
        raise RuntimeError(f"Telegram delete failed: {payload}")


def run_system_message_cleanup_job() -> None:
    enabled = (os.getenv("SYSTEM_MESSAGE_CLEANUP_ENABLED") or "1").strip().lower()
    if enabled not in ("1", "true", "yes", "on"):
        logging.info("ℹ️ System message cleanup disabled by SYSTEM_MESSAGE_CLEANUP_ENABLED")
        return
    tz_name = (os.getenv("SYSTEM_MESSAGE_CLEANUP_TZ") or os.getenv("AUDIO_SCHEDULER_TZ") or "UTC").strip()
    max_days_back = int((os.getenv("SYSTEM_MESSAGE_CLEANUP_MAX_DAYS_BACK") or "2").strip())
    excluded_types = [
        item.strip().lower()
        for item in (os.getenv("SYSTEM_MESSAGE_CLEANUP_EXCLUDE_TYPES") or "feel_word").split(",")
        if item.strip()
    ]
    try:
        now = datetime.now(ZoneInfo(tz_name))
    except Exception:
        now = datetime.utcnow()
        tz_name = "UTC"
    target_date = now.date()
    try:
        pending = get_pending_telegram_system_messages(
            target_date=target_date,
            tz_name=tz_name,
            max_days_back=max_days_back,
            limit=10000,
            excluded_types=excluded_types,
        )
    except Exception:
        logging.exception("❌ System message cleanup failed while reading pending list")
        return
    deleted = 0
    failed = 0
    for item in pending:
        row_id = int(item.get("id"))
        chat_id = int(item.get("chat_id"))
        message_id = int(item.get("message_id"))
        try:
            _delete_telegram_message(chat_id=chat_id, message_id=message_id)
            mark_telegram_system_message_deleted(row_id)
            deleted += 1
        except Exception as exc:
            failed += 1
            try:
                mark_telegram_system_message_deleted(row_id, delete_error=str(exc))
            except Exception:
                logging.debug("Failed to store delete error for row %s", row_id, exc_info=True)
    logging.info(
        "✅ System message cleanup finished: date=%s tz=%s pending=%s deleted=%s failed=%s",
        target_date.isoformat(),
        tz_name,
        len(pending),
        deleted,
        failed,
    )
