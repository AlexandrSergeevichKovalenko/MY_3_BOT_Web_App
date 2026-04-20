import logging
import os
from datetime import datetime
from zoneinfo import ZoneInfo

import requests

from backend.database import get_db_connection_context
from backend.database import delete_stale_tts_db_cache
from backend.database import get_pending_telegram_system_messages
from backend.database import mark_telegram_system_message_deleted
from backend.database import get_admin_telegram_ids
from backend.database import record_telegram_system_message
from backend.translation_workflow import finalize_open_translation_sessions
from backend.tts_cache_cleanup import run_tts_r2_cache_cleanup
from backend.r2_storage import r2_bucket_usage_summary


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


def _format_binary_size(num_bytes: int) -> str:
    size = float(max(0, int(num_bytes or 0)))
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    unit_index = 0
    while size >= 1024.0 and unit_index < len(units) - 1:
        size /= 1024.0
        unit_index += 1
    if unit_index == 0:
        return f"{int(size)} {units[unit_index]}"
    return f"{size:.2f} {units[unit_index]}"


def _send_private_message(user_id: int, text: str, message_type: str = "text") -> None:
    token = os.getenv("TELEGRAM_Deutsch_BOT_TOKEN")
    payload = {
        "chat_id": int(user_id),
        "text": text,
        "disable_web_page_preview": True,
    }
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    response = requests.post(url, json=payload, timeout=15)
    if response.status_code >= 400:
        raise RuntimeError(f"Telegram API error: {response.text}")
    try:
        resp_payload = response.json() if response.content else {}
        message_id = (resp_payload.get("result") or {}).get("message_id")
        if message_id is not None:
            record_telegram_system_message(
                chat_id=int(user_id),
                message_id=int(message_id),
                message_type=message_type,
            )
    except Exception:
        pass


def _send_private_message_chunks(user_id: int, text: str, limit: int = 3800) -> None:
    parts: list[str] = []
    buf = ""
    for line in text.splitlines():
        chunk = (buf + "\n" + line) if buf else line
        if len(chunk) > limit:
            if buf:
                parts.append(buf)
            buf = line
        else:
            buf = chunk
    if buf:
        parts.append(buf)
    for part in parts:
        _send_private_message(user_id, part)


def run_database_table_sizes_report_job() -> None:
    threshold_mb = max(1, int((os.getenv("DB_TABLE_SIZE_REPORT_MIN_MB") or "5").strip()))
    threshold_bytes = int(threshold_mb * 1024 * 1024)
    r2_enabled = (os.getenv("DB_TABLE_SIZE_REPORT_INCLUDE_R2") or "1").strip().lower() in ("1", "true", "yes", "on")
    r2_threshold_mb = max(1, int((os.getenv("DB_TABLE_SIZE_REPORT_R2_MIN_MB") or str(threshold_mb)).strip()))
    r2_threshold_bytes = int(r2_threshold_mb * 1024 * 1024)
    r2_max_prefixes = max(1, int((os.getenv("DB_TABLE_SIZE_REPORT_R2_MAX_PREFIXES") or "25").strip()))
    admin_ids = sorted(int(item) for item in get_admin_telegram_ids() if int(item) > 0)
    if not admin_ids:
        logging.warning("⚠️ DB table size report skipped: no admin ids configured")
        return
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT current_database();")
                db_name_row = cursor.fetchone() or ("postgres",)
                db_name = str(db_name_row[0] or "postgres")
                cursor.execute(
                    """
                    SELECT
                        ns.nspname AS schema_name,
                        cls.relname AS table_name,
                        pg_total_relation_size(cls.oid) AS total_bytes,
                        pg_size_pretty(pg_total_relation_size(cls.oid)) AS total_pretty,
                        pg_relation_size(cls.oid) AS table_bytes,
                        pg_size_pretty(pg_relation_size(cls.oid)) AS table_pretty,
                        COALESCE(pg_total_relation_size(cls.oid) - pg_relation_size(cls.oid), 0) AS extra_bytes,
                        pg_size_pretty(COALESCE(pg_total_relation_size(cls.oid) - pg_relation_size(cls.oid), 0)) AS extra_pretty
                    FROM pg_class cls
                    JOIN pg_namespace ns ON ns.oid = cls.relnamespace
                    WHERE cls.relkind IN ('r', 'm')
                      AND ns.nspname NOT IN ('pg_catalog', 'information_schema')
                      AND pg_total_relation_size(cls.oid) >= %s
                    ORDER BY pg_total_relation_size(cls.oid) DESC, ns.nspname ASC, cls.relname ASC;
                    """,
                    (threshold_bytes,),
                )
                rows = cursor.fetchall() or []
        report_blocks: list[str] = []
        postgres_lines = [
            "🗄 Отчёт по размеру таблиц Postgres",
            f"База данных: {db_name}",
            f"Порог: > {threshold_mb} MB",
            "",
        ]
        if not rows:
            postgres_lines.append("Таблиц выше порога сейчас нет.")
        else:
            for schema_name, table_name, total_bytes, total_pretty, table_bytes, table_pretty, extra_bytes, extra_pretty in rows:
                postgres_lines.append(
                    f"- {schema_name}.{table_name} | total={total_pretty} ({int(total_bytes)} B) | "
                    f"table={table_pretty} | indexes_toast={extra_pretty}"
                )
        report_blocks.append("\n".join(postgres_lines))
        if r2_enabled:
            try:
                r2_summary = r2_bucket_usage_summary(
                    prefix_depth=1,
                    min_prefix_bytes=r2_threshold_bytes,
                    max_prefixes=r2_max_prefixes,
                )
                r2_lines = [
                    "☁️ Отчёт по Cloudflare R2",
                    f"Bucket: {r2_summary.get('bucket_name') or '-'}",
                    (
                        "Итого: "
                        f"{_format_binary_size(int(r2_summary.get('total_bytes') or 0))} "
                        f"в {int(r2_summary.get('total_objects') or 0)} objects"
                    ),
                    f"Порог для prefixes: > {r2_threshold_mb} MB",
                    "",
                ]
                r2_prefixes = list(r2_summary.get("prefixes") or [])
                if not r2_prefixes:
                    r2_lines.append("Prefixes выше порога сейчас нет.")
                else:
                    for item in r2_prefixes:
                        r2_lines.append(
                            f"- {item.get('prefix') or '(root)'} | total={_format_binary_size(int(item.get('bytes') or 0))} "
                            f"| objects={int(item.get('objects') or 0)}"
                        )
                report_blocks.append("\n".join(r2_lines))
            except Exception as r2_exc:
                logging.exception("❌ Cloudflare R2 size report failed")
                report_blocks.append(
                    "\n".join([
                        "☁️ Отчёт по Cloudflare R2",
                        f"Не удалось получить usage: {r2_exc}",
                    ])
                )
        report_text = "\n\n".join(block for block in report_blocks if block)
        for admin_id in admin_ids:
            _send_private_message_chunks(int(admin_id), report_text)
        logging.info(
            "✅ DB table size report sent to admins=%s postgres_rows=%s postgres_threshold_mb=%s r2_enabled=%s r2_threshold_mb=%s",
            len(admin_ids),
            len(rows),
            threshold_mb,
            r2_enabled,
            r2_threshold_mb,
        )
    except Exception:
        logging.exception("❌ DB table size report failed")
