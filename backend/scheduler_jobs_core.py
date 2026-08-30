import functools
import logging
import os
from datetime import datetime
from zoneinfo import ZoneInfo

import requests

from backend.database import get_db_connection_context
from backend.database import purge_world_news_before
from backend.database import delete_stale_tts_db_cache
from backend.database import cleanup_stale_translation_check_sessions
from backend.database import get_pending_telegram_system_messages
from backend.database import mark_telegram_system_message_deleted
from backend.database import get_admin_telegram_ids
from backend.database import record_telegram_system_message
from backend.translation_workflow import finalize_open_translation_sessions
from backend.tts_cache_cleanup import run_tts_r2_cache_cleanup
from backend.r2_storage import r2_bucket_usage_summary
from backend.job_queue import clear_translation_check_session_state
from backend.job_queue import get_session_presence_verify_unknown_count


def _heartbeat(job_key: str):
    """Decorator: record a non-gating scheduler heartbeat when a maintenance job runs,
    so /scheduler_health shows a last-run time for these otherwise-untracked cleanups.
    Records 'completed' on normal return (incl. a disabled early-return — the scheduler
    still fired) and 'failed' before re-raising. Best-effort: a heartbeat write can never
    break or block the job. Covers both call paths (dramatiq actor + legacy scheduler),
    since it wraps the shared job function itself."""
    def _decorator(fn):
        @functools.wraps(fn)
        def _wrapper(*args, **kwargs):
            try:
                result = fn(*args, **kwargs)
            except Exception:
                _record_heartbeat_safe(job_key, "failed")
                raise
            _record_heartbeat_safe(job_key, "completed")
            return result
        return _wrapper
    return _decorator


def _record_heartbeat_safe(job_key: str, status: str) -> None:
    try:
        from backend.database import record_scheduler_heartbeat
        record_scheduler_heartbeat(job_key=job_key, status=status)
    except Exception:
        logging.debug("scheduler heartbeat failed job_key=%s", job_key, exc_info=True)


def run_translation_sessions_auto_close_job() -> None:
    enabled = (os.getenv("TRANSLATION_SESSIONS_AUTO_CLOSE_ENABLED") or "1").strip().lower()
    if enabled not in ("1", "true", "yes", "on"):
        logging.info("ℹ️ Translation sessions auto-close disabled by TRANSLATION_SESSIONS_AUTO_CLOSE_ENABLED")
        return
    try:
        result = finalize_open_translation_sessions()
        logging.info("✅ Translation sessions auto-close finished: %s", result)
        _report_session_presence_health_to_admins(result)
    except Exception:
        logging.exception("❌ Translation sessions auto-close failed")
        raise


def _report_session_presence_health_to_admins(result: dict) -> None:
    """Владелец не должен ничего вызывать командой, чтобы узнать, что плашка «набор не
    закончен» опять начала врать. Пишем ему сами и ТОЛЬКО когда есть о чём:

      • погашенные лживые указатели > 0 — значит какой-то путь закрытия сессии снова
        забыл про указатель (после правки 30.08.2026 здесь должен быть ноль);
      • «не смогли проверить» > 0 — база не отвечала в момент открытия приложения,
        и людям в эти разы плашку не показывали.

    Тишина здесь означает «всё чисто», а не «механизм молчит»: ночное задание пишет
    свой итог в лог каждый раз, даже когда сообщения нет.
    """
    cleared_markers = int((result or {}).get("cleared_presence_markers") or 0)
    unknown_verifies = get_session_presence_verify_unknown_count()
    if cleared_markers <= 0 and not unknown_verifies:
        return
    admin_ids = sorted(int(item) for item in get_admin_telegram_ids() if int(item) > 0)
    if not admin_ids:
        logging.warning("⚠️ Session-presence health report skipped: no admin ids configured")
        return
    lines = ["🔎 Плашка «набор переводов не закончен» — проверка за сутки"]
    if cleared_markers > 0:
        user_ids = list((result or {}).get("presence_marker_user_ids") or [])
        lines.append(
            f"• Погашено указателей, которые врали: {cleared_markers}"
            + (f" (люди: {', '.join(str(item) for item in user_ids[:20])})" if user_ids else "")
        )
        lines.append("  Это значит, что какой-то путь закрытия сессии снова не гасит указатель.")
    if unknown_verifies:
        lines.append(f"• Не смогли проверить сессию (база не ответила): {unknown_verifies}")
        lines.append("  В эти разы плашку не показывали — люди ничего лишнего не увидели.")
    report_text = "\n".join(lines)
    for admin_id in admin_ids:
        _send_private_message_chunks(int(admin_id), report_text)
    logging.info(
        "✅ Session-presence health report sent: admins=%s cleared_markers=%s unknown_verifies=%s",
        len(admin_ids), cleared_markers, unknown_verifies,
    )


@_heartbeat("world_news_purge")
def run_world_news_purge_job() -> None:
    """Nightly: fully delete every past-day morning-news entry (row + cached
    subtitles + watch_state + R2 hero). Morning news is not stored — a day's
    news lives only for that day. Runs after midnight in the news timezone, so
    «today» is preserved and only news_date < today is purged."""
    enabled = (os.getenv("WORLD_NEWS_PURGE_ENABLED") or "1").strip().lower()
    if enabled not in ("1", "true", "yes", "on"):
        logging.info("ℹ️ World-news purge disabled by WORLD_NEWS_PURGE_ENABLED")
        return
    tz_name = (os.getenv("WORLD_NEWS_PURGE_TZ") or "Europe/Berlin").strip() or "Europe/Berlin"
    try:
        today = datetime.now(ZoneInfo(tz_name)).date()
    except Exception:
        today = datetime.utcnow().date()
    try:
        result = purge_world_news_before(today)
        logging.info("✅ World-news purge finished (before %s): %s", today, result)
    except Exception:
        logging.exception("❌ World-news purge failed")
        raise


def run_translation_check_stale_cleanup_job() -> None:
    enabled = (os.getenv("TRANSLATION_CHECK_STALE_SESSION_CLEANUP_ENABLED") or "1").strip().lower()
    if enabled not in ("1", "true", "yes", "on"):
        logging.info("ℹ️ Translation-check stale cleanup disabled by TRANSLATION_CHECK_STALE_SESSION_CLEANUP_ENABLED")
        return
    stale_minutes = int((os.getenv("TRANSLATION_CHECK_STALE_SESSION_MAX_AGE_MINUTES") or "60").strip() or "60")
    batch_limit = int((os.getenv("TRANSLATION_CHECK_STALE_SESSION_CLEANUP_BATCH_LIMIT") or "100").strip() or "100")
    try:
        result = cleanup_stale_translation_check_sessions(
            stale_minutes=stale_minutes,
            limit=batch_limit,
            cleanup_reason="translation_check_session_stale_cleanup",
        )
        session_ids = [int(item) for item in list(result.get("session_ids") or []) if str(item).strip()]
        for session_id in session_ids:
            clear_translation_check_session_state(session_id)
        logging.info(
            "✅ Translation-check stale cleanup finished: stale_minutes=%s session_count=%s item_updates=%s session_ids=%s",
            stale_minutes,
            int(result.get("session_count") or 0),
            int(result.get("item_updates") or 0),
            session_ids,
        )
    except Exception:
        logging.exception("❌ Translation-check stale cleanup failed")
        raise


@_heartbeat("flashcard_feel_cleanup")
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


@_heartbeat("tts_db_cache_cleanup")
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


@_heartbeat("tts_r2_cache_cleanup")
def run_tts_r2_cache_cleanup_job() -> None:
    try:
        run_tts_r2_cache_cleanup()
    except Exception:
        logging.exception("❌ TTS R2 cache cleanup failed")
        raise


@_heartbeat("image_quiz_r2_cleanup")
def run_image_quiz_r2_cleanup_job() -> None:
    from backend.image_quiz_cleanup import run_image_quiz_r2_cleanup
    try:
        run_image_quiz_r2_cleanup()
    except Exception:
        logging.exception("❌ Image-quiz R2 cleanup failed")
        raise


@_heartbeat("visual_riddle_r2_cleanup")
def run_visual_riddle_r2_cleanup_job() -> None:
    from backend.visual_riddle_cleanup import run_visual_riddle_r2_cleanup
    try:
        run_visual_riddle_r2_cleanup()
    except Exception:
        logging.exception("❌ Visual-riddle R2 cleanup failed")
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


@_heartbeat("system_message_cleanup")
def run_system_message_cleanup_job() -> None:
    enabled = (os.getenv("SYSTEM_MESSAGE_CLEANUP_ENABLED") or "1").strip().lower()
    if enabled not in ("1", "true", "yes", "on"):
        logging.info("ℹ️ System message cleanup disabled by SYSTEM_MESSAGE_CLEANUP_ENABLED")
        return
    tz_name = (os.getenv("SYSTEM_MESSAGE_CLEANUP_TZ") or os.getenv("AUDIO_SCHEDULER_TZ") or "UTC").strip()
    max_days_back = int((os.getenv("SYSTEM_MESSAGE_CLEANUP_MAX_DAYS_BACK") or "2").strip())
    excluded_types = [
        item.strip().lower()
        for item in (os.getenv("SYSTEM_MESSAGE_CLEANUP_EXCLUDE_TYPES") or "").split(",")
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
