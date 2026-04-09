import psycopg2
from psycopg2 import Binary
from psycopg2 import OperationalError
from psycopg2 import sql
from psycopg2.extras import Json, execute_values
from psycopg2.pool import ThreadedConnectionPool, PoolError
import os
import hashlib
import atexit
import math
import logging
from contextlib import contextmanager
import json
import random
import re
import threading
from decimal import Decimal, InvalidOperation
from datetime import datetime, timezone, date, timedelta, time as dt_time
from pathlib import Path
import time
from uuid import uuid4
from calendar import monthrange
from zoneinfo import ZoneInfo
from typing import Any
from dotenv import load_dotenv
try:
    from backend.config_mistakes_data import (
        VALID_CATEGORIES as VALID_CATEGORIES_DE,
        VALID_SUBCATEGORIES as VALID_SUBCATEGORIES_DE,
    )
except Exception:
    from config_mistakes_data import (
        VALID_CATEGORIES as VALID_CATEGORIES_DE,
        VALID_SUBCATEGORIES as VALID_SUBCATEGORIES_DE,
    )

env_path = Path(__file__).parent / ".env"
load_dotenv(dotenv_path=env_path)

DATABASE_URL = os.getenv("DATABASE_URL_RAILWAY") #
DB_CONNECT_TIMEOUT_SECONDS = int(os.getenv("DB_CONNECT_TIMEOUT_SECONDS", "12"))
DB_CONNECT_RETRIES = max(1, int(os.getenv("DB_CONNECT_RETRIES", "3")))
DB_CONNECT_RETRY_DELAY_SECONDS = float(os.getenv("DB_CONNECT_RETRY_DELAY_SECONDS", "0.6"))
DB_POOL_ENABLED = str(os.getenv("DB_POOL_ENABLED", "1")).strip().lower() not in {"0", "false", "no", "off"}
DB_POOL_MINCONN = max(1, int(os.getenv("DB_POOL_MINCONN", "4")))
DB_POOL_MAXCONN = max(DB_POOL_MINCONN, int(os.getenv("DB_POOL_MAXCONN", "48")))
DB_POOL_ACQUIRE_TIMEOUT_MS = max(0, int(os.getenv("DB_POOL_ACQUIRE_TIMEOUT_MS", "1500")))
DB_POOL_ACQUIRE_RETRY_MS = max(10, int(os.getenv("DB_POOL_ACQUIRE_RETRY_MS", "50")))
DB_POOL_LOG_SLOW_ACQUIRE_MS = max(0, int(os.getenv("DB_POOL_LOG_SLOW_ACQUIRE_MS", "100")))
DB_POOL_ALLOW_DIRECT_FALLBACK = str(os.getenv("DB_POOL_ALLOW_DIRECT_FALLBACK", "0")).strip().lower() in {"1", "true", "yes", "on"}
BILLING_PLAN_CACHE_TTL_SEC = max(5, min(3600, int(os.getenv("BILLING_PLAN_CACHE_TTL_SEC", "300"))))
READER_SESSION_AUTOCLOSE_MAX_SECONDS = max(
    300,
    int(os.getenv("READER_SESSION_AUTOCLOSE_MAX_SECONDS", "10800")),
)
WEBAPP_SCHEMA_MIGRATION_LOCK_KEY = 830420260305001
_ENSURE_WEBAPP_TABLES_MUTEX = threading.Lock()
_ENSURE_WEBAPP_TABLES_DONE = False
_DB_POOL_LOCK = threading.Lock()
_DB_POOL: ThreadedConnectionPool | None = None
_DB_ACQUIRE_LOCAL = threading.local()
_BILLING_PLAN_CACHE_LOCK = threading.Lock()
_BILLING_PLAN_CACHE: dict[str, tuple[float, dict | None]] = {}
PHASE1_SHADOW_SCHEMA_MIGRATION_KEY = "2026_03_12_skill_shadow_phase1_schema"
PHASE2_SHADOW_SCHEMA_MIGRATION_KEY = "2026_03_12_skill_shadow_phase2_schema"
SKILL_MASTERY_GROUPS_SCHEMA_MIGRATION_KEY = "2026_03_12_skill_mastery_groups_schema"
DICTIONARY_CANONICAL_SCHEMA_MIGRATION_KEY = "2026_04_02_dictionary_canonical_v1"
SUPPORTED_LEARNING_LANGUAGES = {"de", "en", "es", "it"}
SUPPORTED_NATIVE_LANGUAGES = {"ru", "en", "de"}
DEFAULT_LEARNING_LANGUAGE = "de"
DEFAULT_NATIVE_LANGUAGE = "ru"
IMAGE_QUIZ_TEMPLATE_REUSE_COOLDOWN_HOURS = max(
    1,
    int((os.getenv("IMAGE_QUIZ_TEMPLATE_REUSE_COOLDOWN_HOURS") or "168").strip() or "168"),
)
IMAGE_QUIZ_RENDERING_STALE_MINUTES = max(
    5,
    int((os.getenv("IMAGE_QUIZ_RENDERING_STALE_MINUTES") or "45").strip() or "45"),
)
USER_REMOVAL_GRACE_DAYS = max(1, int(os.getenv("USER_REMOVAL_GRACE_DAYS", "30")))
SKILL_STATE_V2_RECENT_TAU_DAYS = 10.0
SKILL_STATE_V2_CONFIDENCE_SPREAD = 12.0
SKILL_STATE_V2_MASTERY_RECENT_WEIGHT = 28.0
SKILL_STATE_V2_MASTERY_LIFETIME_WEIGHT = 14.0
SKILL_STATE_V2_MASTERY_RECENT_SPREAD = 4.5
SKILL_STATE_V2_MASTERY_LIFETIME_SPREAD = 12.0
DICTIONARY_ORIGIN_ALLOWED = {
    "unknown",
    "unknown_legacy",
    "webapp_dictionary_save",
    "mobile_dictionary_save",
    "bot_private_save",
    "sentence_gpt_seed",
    "translations_block",
    "youtube",
    "reader",
    "assistant",
    "import",
}

UNCLASSIFIED_ERROR_CATEGORY_ALIASES = {"other mistake", "other mistakes"}
UNCLASSIFIED_ERROR_SUBCATEGORY_ALIASES = {"unclassified mistake", "unclassified mistakes"}
EXCLUDED_UNCLASSIFIED_SKILL_IDS = {
    "other_unclassified",
    "en_other_unclassified",
    "es_other_unclassified",
    "it_other_unclassified",
}


def _normalize_skill_error_label(value: str | None) -> str:
    return str(value or "").strip().lower()


def _is_unclassified_error(main_category: str | None, sub_category: str | None) -> bool:
    normalized_main = _normalize_skill_error_label(main_category)
    normalized_sub = _normalize_skill_error_label(sub_category)
    if normalized_sub in UNCLASSIFIED_ERROR_SUBCATEGORY_ALIASES:
        return True
    return (
        normalized_main in UNCLASSIFIED_ERROR_CATEGORY_ALIASES
        and (not normalized_sub or normalized_sub in UNCLASSIFIED_ERROR_SUBCATEGORY_ALIASES)
    )


def _is_unclassified_skill_seed(skill_id: str | None, title: str | None, category: str | None) -> bool:
    normalized_skill_id = _normalize_skill_error_label(skill_id)
    normalized_title = _normalize_skill_error_label(title)
    normalized_category = _normalize_skill_error_label(category)
    if normalized_skill_id in EXCLUDED_UNCLASSIFIED_SKILL_IDS:
        return True
    if "unclassified" in normalized_title:
        return True
    return (
        normalized_category in {"other", *UNCLASSIFIED_ERROR_CATEGORY_ALIASES}
        and "unclassified" in normalized_title
    )


def _semantic_benchmark_sentence_hash(
    source_sentence: str | None,
    *,
    source_lang: str = "ru",
    target_lang: str = "de",
) -> str:
    normalized_sentence = " ".join(str(source_sentence or "").strip().split())
    payload = f"{str(source_lang or 'ru').strip().lower()}::{str(target_lang or 'de').strip().lower()}::{normalized_sentence}"
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def _normalize_image_quiz_generation_status(value: str | None) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"pending", "blueprint_ready", "rendering", "ready", "failed"}:
        return normalized
    return "pending"


def _normalize_image_quiz_visual_status(value: str | None) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"unknown", "valid", "rejected"}:
        return normalized
    return "unknown"


def _normalize_image_quiz_dispatch_status(value: str | None) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"claimed", "sent", "failed"}:
        return normalized
    return "claimed"


def _normalize_image_quiz_delivery_scope(value: str | None) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"private", "group"}:
        return normalized
    return "private"


def _normalize_image_quiz_options(options: list[str] | tuple[str, ...] | None) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for raw in options or []:
        value = str(raw or "").strip()
        if not value or value in seen:
            continue
        seen.add(value)
        normalized.append(value)
    return normalized


def _map_image_quiz_template_row(row: tuple | None) -> dict | None:
    if not row:
        return None
    raw_options = row[11]
    if isinstance(raw_options, str):
        try:
            raw_options = json.loads(raw_options)
        except Exception:
            raw_options = []
    return {
        "id": int(row[0]),
        "user_id": int(row[1]),
        "source_dictionary_entry_id": int(row[2]) if row[2] is not None else None,
        "canonical_entry_id": int(row[3]) if row[3] is not None else None,
        "source_lang": str(row[4] or "").strip().lower(),
        "target_lang": str(row[5] or "").strip().lower(),
        "source_text": str(row[6] or ""),
        "target_text": str(row[7] or ""),
        "source_sentence": str(row[8] or ""),
        "image_prompt": str(row[9] or ""),
        "question_de": str(row[10] or ""),
        "answer_options": [str(item) for item in (raw_options or []) if str(item or "").strip()],
        "correct_option_index": int(row[12]) if row[12] is not None else None,
        "explanation": str(row[13] or "") or None,
        "provider_name": str(row[14] or "") or None,
        "provider_meta": row[15] if isinstance(row[15], dict) else {},
        "image_object_key": str(row[16] or "") or None,
        "image_url": str(row[17] or "") or None,
        "generation_status": _normalize_image_quiz_generation_status(row[18]),
        "visual_status": _normalize_image_quiz_visual_status(row[19]),
        "last_error": str(row[20] or "") or None,
        "prepared_at": row[21].isoformat() if row[21] else None,
        "last_used_at": row[22].isoformat() if row[22] else None,
        "use_count": int(row[23] or 0),
        "created_at": row[24].isoformat() if row[24] else None,
        "updated_at": row[25].isoformat() if row[25] else None,
    }


def _map_image_quiz_dispatch_row(row: tuple | None) -> dict | None:
    if not row:
        return None
    return {
        "id": int(row[0]),
        "template_id": int(row[1]),
        "target_user_id": int(row[2]),
        "chat_id": int(row[3]),
        "message_id": int(row[4]) if row[4] is not None else None,
        "delivery_scope": _normalize_image_quiz_delivery_scope(row[5]),
        "delivery_slot": str(row[6] or "") or None,
        "delivery_date_local": row[7].isoformat() if row[7] else None,
        "status": _normalize_image_quiz_dispatch_status(row[8]),
        "created_at": row[9].isoformat() if row[9] else None,
        "sent_at": row[10].isoformat() if row[10] else None,
        "updated_at": row[11].isoformat() if row[11] else None,
    }


def _map_image_quiz_answer_row(row: tuple | None) -> dict | None:
    if not row:
        return None
    return {
        "id": int(row[0]),
        "dispatch_id": int(row[1]),
        "user_id": int(row[2]),
        "selected_option_index": int(row[3]),
        "selected_text": str(row[4] or ""),
        "is_correct": bool(row[5]),
        "answered_at": row[6].isoformat() if row[6] else None,
        "feedback_sent_at": row[7].isoformat() if row[7] else None,
    }


def _map_image_quiz_candidate_row(row: tuple | None) -> dict | None:
    if not row:
        return None
    response_json = _coerce_json_object(row[9])
    source_lang = _normalize_lang_code(row[7]) or "ru"
    target_lang = _normalize_lang_code(row[8]) or "de"
    source_text, target_text = _resolve_dictionary_source_target_texts(
        source_lang=source_lang,
        target_lang=target_lang,
        word_ru=row[3],
        translation_de=row[4],
        word_de=row[5],
        translation_ru=row[6],
        response_json=response_json,
    )
    return {
        "entry_id": int(row[0]),
        "user_id": int(row[1]),
        "canonical_entry_id": int(row[2]) if row[2] is not None else None,
        "word_ru": str(row[3] or "") or None,
        "translation_de": str(row[4] or "") or None,
        "word_de": str(row[5] or "") or None,
        "translation_ru": str(row[6] or "") or None,
        "source_lang": source_lang,
        "target_lang": target_lang,
        "response_json": response_json,
        "source_text": source_text,
        "target_text": target_text,
        "created_at": row[10].isoformat() if row[10] else None,
    }


def _normalize_dictionary_origin_process(value: str | None) -> str:
    normalized = str(value or "").strip().lower()
    if normalized == "translations":
        normalized = "translations_block"
    elif normalized == "reader_selection_gpt_save":
        normalized = "reader"
    if normalized in DICTIONARY_ORIGIN_ALLOWED:
        return normalized
    return "unknown"


def build_translation_session_minutes_sql(alias: str = "p") -> str:
    safe_alias = re.sub(r"[^A-Za-z0-9_]", "", str(alias or "p")) or "p"
    return (
        f"CASE "
        f"WHEN {safe_alias}.active_seconds IS NOT NULL THEN "
        f"(COALESCE({safe_alias}.active_seconds, 0)::numeric / 60.0) "
        f"WHEN {safe_alias}.end_time IS NOT NULL AND {safe_alias}.start_time IS NOT NULL THEN "
        f"(EXTRACT(EPOCH FROM ({safe_alias}.end_time - {safe_alias}.start_time)) / 60.0) "
        f"ELSE 0 END"
    )


def sync_translation_session_activity(
    *,
    user_id: int,
    session_id: int | str,
    action: str,
) -> dict[str, Any] | None:
    normalized_action = str(action or "").strip().lower()
    if normalized_action not in {"start", "resume", "pause", "stop"}:
        raise ValueError(f"Unsupported translation session activity action: {action}")

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            if normalized_action in {"start", "resume"}:
                cursor.execute(
                    """
                    UPDATE bt_3_user_progress
                    SET
                        active_seconds = COALESCE(active_seconds, 0),
                        active_started_at = CASE
                            WHEN completed = FALSE THEN NOW()
                            ELSE active_started_at
                        END,
                        active_running = CASE
                            WHEN completed = FALSE THEN TRUE
                            ELSE COALESCE(active_running, FALSE)
                        END
                    WHERE user_id = %s
                      AND session_id = %s
                    RETURNING
                        session_id,
                        completed,
                        COALESCE(active_seconds, 0),
                        COALESCE(active_running, FALSE),
                        active_started_at;
                    """,
                    (int(user_id), str(session_id)),
                )
            else:
                cursor.execute(
                    """
                    UPDATE bt_3_user_progress
                    SET
                        active_seconds = COALESCE(active_seconds, 0)
                            + CASE
                                WHEN COALESCE(active_running, FALSE) = TRUE
                                 AND active_started_at IS NOT NULL
                                    THEN GREATEST(
                                        0,
                                        EXTRACT(EPOCH FROM (NOW() - active_started_at))::BIGINT
                                    )
                                ELSE 0
                            END,
                        active_started_at = NULL,
                        active_running = FALSE
                    WHERE user_id = %s
                      AND session_id = %s
                    RETURNING
                        session_id,
                        completed,
                        COALESCE(active_seconds, 0),
                        COALESCE(active_running, FALSE),
                        active_started_at;
                    """,
                    (int(user_id), str(session_id)),
                )
            row = cursor.fetchone()

    if not row:
        return None

    return {
        "session_id": str(row[0]),
        "completed": bool(row[1]),
        "active_seconds": int(row[2] or 0),
        "active_running": bool(row[3]),
        "active_started_at": row[4].isoformat() if row[4] else None,
    }


def _coerce_json_object(value) -> dict:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, dict):
                return dict(parsed)
        except Exception:
            return {}
    return {}


def _normalize_dictionary_text_key(value: str | None) -> str:
    compact = re.sub(r"\s+", " ", str(value or "").strip())
    return compact.casefold()


def _resolve_dictionary_source_target_texts(
    *,
    source_lang: str,
    target_lang: str,
    word_ru: str | None,
    translation_de: str | None,
    word_de: str | None,
    translation_ru: str | None,
    response_json: dict | None,
) -> tuple[str, str]:
    payload = _coerce_json_object(response_json)
    source_text = str(
        payload.get("source_text")
        or word_ru
        or payload.get("word_ru")
        or translation_ru
        or payload.get("translation_ru")
        or word_de
        or payload.get("word_de")
        or ""
    ).strip()
    target_text = str(
        payload.get("target_text")
        or translation_de
        or payload.get("translation_de")
        or word_de
        or payload.get("word_de")
        or translation_ru
        or payload.get("translation_ru")
        or ""
    ).strip()

    source_lang_value = _normalize_lang_code(source_lang)
    target_lang_value = _normalize_lang_code(target_lang)

    if not source_text and source_lang_value == "de":
        source_text = str(word_de or payload.get("word_de") or "").strip()
    if not target_text and target_lang_value == "de":
        target_text = str(translation_de or payload.get("translation_de") or "").strip()

    return source_text, target_text


def _dedupe_webapp_dictionary_entry_after_insert(
    conn,
    *,
    keep_entry_id: int,
    user_id: int,
    source_lang: str | None,
    target_lang: str | None,
    word_ru: str | None,
    translation_de: str | None,
    word_de: str | None,
    translation_ru: str | None,
    response_json: dict | None,
) -> int:
    normalized_source_lang = _normalize_lang_code(source_lang)
    normalized_target_lang = _normalize_lang_code(target_lang)
    source_text, target_text = _resolve_dictionary_source_target_texts(
        source_lang=normalized_source_lang,
        target_lang=normalized_target_lang,
        word_ru=word_ru,
        translation_de=translation_de,
        word_de=word_de,
        translation_ru=translation_ru,
        response_json=response_json,
    )
    normalized_source_text = _normalize_dictionary_text_key(source_text)
    normalized_target_text = _normalize_dictionary_text_key(target_text)
    if not normalized_source_text or not normalized_target_text:
        return 0

    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                id,
                word_ru,
                translation_de,
                word_de,
                translation_ru,
                response_json,
                is_learned
            FROM bt_3_webapp_dictionary_queries
            WHERE user_id = %s
              AND id <> %s
              AND COALESCE(source_lang, '') = %s
              AND COALESCE(target_lang, '') = %s;
            """,
            (
                int(user_id),
                int(keep_entry_id),
                normalized_source_lang,
                normalized_target_lang,
            ),
        )
        rows = cursor.fetchall() or []

        duplicate_ids: list[int] = []
        preserve_learned = False
        for row in rows:
            existing_source_text, existing_target_text = _resolve_dictionary_source_target_texts(
                source_lang=normalized_source_lang,
                target_lang=normalized_target_lang,
                word_ru=row[1],
                translation_de=row[2],
                word_de=row[3],
                translation_ru=row[4],
                response_json=_coerce_json_object(row[5]),
            )
            if (
                _normalize_dictionary_text_key(existing_source_text) == normalized_source_text
                and _normalize_dictionary_text_key(existing_target_text) == normalized_target_text
            ):
                duplicate_ids.append(int(row[0]))
                preserve_learned = preserve_learned or bool(row[6])

        if preserve_learned:
            cursor.execute(
                """
                UPDATE bt_3_webapp_dictionary_queries
                SET is_learned = TRUE
                WHERE id = %s;
                """,
                (int(keep_entry_id),),
            )

        if duplicate_ids:
            cursor.execute(
                """
                DELETE FROM bt_3_webapp_dictionary_queries
                WHERE user_id = %s
                  AND id = ANY(%s);
                """,
                (int(user_id), duplicate_ids),
            )

    return len(duplicate_ids)


def _upsert_dictionary_canonical_entry_with_cursor(
    cursor,
    *,
    source_lang: str | None,
    target_lang: str | None,
    source_text: str | None,
    target_text: str | None,
    word_ru: str | None,
    translation_de: str | None,
    word_de: str | None,
    translation_ru: str | None,
    response_json: dict | None,
) -> int:
    normalized_source_lang = _normalize_lang_code(source_lang)
    normalized_target_lang = _normalize_lang_code(target_lang)
    resolved_source_text = str(source_text or "").strip()
    resolved_target_text = str(target_text or "").strip()
    normalized_source_text = _normalize_dictionary_text_key(resolved_source_text)
    normalized_target_text = _normalize_dictionary_text_key(resolved_target_text)
    if not normalized_source_lang or not normalized_target_lang:
        raise ValueError("dictionary canonical entry requires language pair")
    if not normalized_source_text or not normalized_target_text:
        raise ValueError("dictionary canonical entry requires source and target text")
    payload = _coerce_json_object(response_json)
    cursor.execute(
        """
        INSERT INTO bt_3_dictionary_entries (
            source_lang,
            target_lang,
            source_text,
            target_text,
            source_text_norm,
            target_text_norm,
            word_ru,
            translation_de,
            word_de,
            translation_ru,
            response_json,
            created_at,
            updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
        ON CONFLICT (source_lang, target_lang, source_text_norm, target_text_norm)
        DO UPDATE SET
            source_text = COALESCE(NULLIF(bt_3_dictionary_entries.source_text, ''), EXCLUDED.source_text),
            target_text = COALESCE(NULLIF(bt_3_dictionary_entries.target_text, ''), EXCLUDED.target_text),
            word_ru = COALESCE(NULLIF(bt_3_dictionary_entries.word_ru, ''), EXCLUDED.word_ru),
            translation_de = COALESCE(NULLIF(bt_3_dictionary_entries.translation_de, ''), EXCLUDED.translation_de),
            word_de = COALESCE(NULLIF(bt_3_dictionary_entries.word_de, ''), EXCLUDED.word_de),
            translation_ru = COALESCE(NULLIF(bt_3_dictionary_entries.translation_ru, ''), EXCLUDED.translation_ru),
            response_json = COALESCE(bt_3_dictionary_entries.response_json, EXCLUDED.response_json),
            updated_at = NOW()
        RETURNING id;
        """,
        (
            normalized_source_lang,
            normalized_target_lang,
            resolved_source_text,
            resolved_target_text,
            normalized_source_text,
            normalized_target_text,
            str(word_ru or "").strip() or None,
            str(translation_de or "").strip() or None,
            str(word_de or "").strip() or None,
            str(translation_ru or "").strip() or None,
            Json(payload) if payload else None,
        ),
    )
    row = cursor.fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def _create_or_attach_user_dictionary_entry_with_cursor(
    cursor,
    *,
    user_id: int,
    word_ru: str | None,
    translation_de: str | None,
    word_de: str | None,
    translation_ru: str | None,
    response_json: dict,
    folder_id: int | None = None,
    source_lang: str | None = None,
    target_lang: str | None = None,
    canonical_entry_id: int | None = None,
    origin_process: str | None = None,
    origin_meta: dict | None = None,
) -> tuple[int, bool]:
    normalized_origin = _normalize_dictionary_origin_process(origin_process)
    normalized_meta = _coerce_json_object(origin_meta)
    normalized_response_json = _coerce_json_object(response_json)
    normalized_source_lang = _normalize_lang_code(source_lang)
    normalized_target_lang = _normalize_lang_code(target_lang)
    if canonical_entry_id:
        cursor.execute(
            """
            INSERT INTO bt_3_webapp_dictionary_queries (
                user_id,
                word_ru,
                folder_id,
                translation_de,
                word_de,
                translation_ru,
                source_lang,
                target_lang,
                canonical_entry_id,
                origin_process,
                origin_meta,
                response_json
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (user_id, canonical_entry_id) WHERE canonical_entry_id IS NOT NULL
            DO UPDATE SET
                folder_id = COALESCE(EXCLUDED.folder_id, bt_3_webapp_dictionary_queries.folder_id),
                origin_process = EXCLUDED.origin_process,
                origin_meta = COALESCE(EXCLUDED.origin_meta, bt_3_webapp_dictionary_queries.origin_meta),
                response_json = COALESCE(bt_3_webapp_dictionary_queries.response_json, EXCLUDED.response_json),
                word_ru = COALESCE(NULLIF(bt_3_webapp_dictionary_queries.word_ru, ''), EXCLUDED.word_ru),
                translation_de = COALESCE(NULLIF(bt_3_webapp_dictionary_queries.translation_de, ''), EXCLUDED.translation_de),
                word_de = COALESCE(NULLIF(bt_3_webapp_dictionary_queries.word_de, ''), EXCLUDED.word_de),
                translation_ru = COALESCE(NULLIF(bt_3_webapp_dictionary_queries.translation_ru, ''), EXCLUDED.translation_ru),
                source_lang = COALESCE(NULLIF(bt_3_webapp_dictionary_queries.source_lang, ''), EXCLUDED.source_lang),
                target_lang = COALESCE(NULLIF(bt_3_webapp_dictionary_queries.target_lang, ''), EXCLUDED.target_lang),
                canonical_entry_id = COALESCE(bt_3_webapp_dictionary_queries.canonical_entry_id, EXCLUDED.canonical_entry_id),
                is_learned = COALESCE(bt_3_webapp_dictionary_queries.is_learned, FALSE) OR COALESCE(EXCLUDED.is_learned, FALSE)
            RETURNING id, (xmax = 0) AS inserted;
            """,
            (
                int(user_id),
                str(word_ru or "").strip() or None,
                int(folder_id) if folder_id is not None else None,
                str(translation_de or "").strip() or None,
                str(word_de or "").strip() or None,
                str(translation_ru or "").strip() or None,
                normalized_source_lang,
                normalized_target_lang,
                int(canonical_entry_id),
                normalized_origin,
                Json(normalized_meta) if normalized_meta else None,
                Json(normalized_response_json),
            ),
        )
        row = cursor.fetchone()
        return (
            int(row[0]) if row and row[0] is not None else 0,
            bool(row[1]) if row and len(row) > 1 else False,
        )

    cursor.execute(
        """
        INSERT INTO bt_3_webapp_dictionary_queries (
            user_id,
            word_ru,
            folder_id,
            translation_de,
            word_de,
            translation_ru,
            source_lang,
            target_lang,
            canonical_entry_id,
            origin_process,
            origin_meta,
            response_json
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NULL, %s, %s, %s)
        RETURNING id;
        """,
        (
            int(user_id),
            str(word_ru or "").strip() or None,
            int(folder_id) if folder_id is not None else None,
            str(translation_de or "").strip() or None,
            str(word_de or "").strip() or None,
            str(translation_ru or "").strip() or None,
            normalized_source_lang,
            normalized_target_lang,
            normalized_origin,
            Json(normalized_meta) if normalized_meta else None,
            Json(normalized_response_json),
        ),
    )
    row = cursor.fetchone()
    return int(row[0]) if row and row[0] is not None else 0, True


def _save_webapp_dictionary_query_returning_id_with_conn(
    conn,
    *,
    user_id: int,
    word_ru: str | None,
    translation_de: str | None,
    word_de: str | None,
    translation_ru: str | None,
    response_json: dict,
    folder_id: int | None = None,
    source_lang: str | None = None,
    target_lang: str | None = None,
    origin_process: str | None = None,
    origin_meta: dict | None = None,
) -> tuple[int, bool]:
    normalized_response_json = _coerce_json_object(response_json)
    normalized_source_lang = _normalize_lang_code(source_lang)
    normalized_target_lang = _normalize_lang_code(target_lang)
    source_text, target_text = _resolve_dictionary_source_target_texts(
        source_lang=normalized_source_lang,
        target_lang=normalized_target_lang,
        word_ru=word_ru,
        translation_de=translation_de,
        word_de=word_de,
        translation_ru=translation_ru,
        response_json=normalized_response_json,
    )
    with conn.cursor() as cursor:
        canonical_entry_id = None
        if source_text and target_text and normalized_source_lang and normalized_target_lang:
            canonical_entry_id = _upsert_dictionary_canonical_entry_with_cursor(
                cursor,
                source_lang=normalized_source_lang,
                target_lang=normalized_target_lang,
                source_text=source_text,
                target_text=target_text,
                word_ru=word_ru,
                translation_de=translation_de,
                word_de=word_de,
                translation_ru=translation_ru,
                response_json=normalized_response_json,
            )
        return _create_or_attach_user_dictionary_entry_with_cursor(
            cursor,
            user_id=int(user_id),
            word_ru=word_ru,
            translation_de=translation_de,
            word_de=word_de,
            translation_ru=translation_ru,
            response_json=normalized_response_json,
            folder_id=folder_id,
            source_lang=normalized_source_lang,
            target_lang=normalized_target_lang,
            canonical_entry_id=canonical_entry_id,
            origin_process=origin_process,
            origin_meta=origin_meta,
        )


def _run_dictionary_canonical_schema_migration(conn, *, batch_size: int = 250) -> None:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT 1
            FROM bt_3_schema_migrations
            WHERE migration_key = %s
            LIMIT 1;
            """,
            (DICTIONARY_CANONICAL_SCHEMA_MIGRATION_KEY,),
        )
        if cursor.fetchone():
            return

    safe_batch_size = max(50, min(int(batch_size or 250), 1000))
    while True:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    id,
                    user_id,
                    word_ru,
                    translation_de,
                    word_de,
                    translation_ru,
                    source_lang,
                    target_lang,
                    response_json
                FROM bt_3_webapp_dictionary_queries
                WHERE canonical_entry_id IS NULL
                ORDER BY id ASC
                LIMIT %s;
                """,
                (safe_batch_size,),
            )
            rows = cursor.fetchall() or []
        if not rows:
            break
        processed_count = 0
        for row in rows:
            entry_id = int(row[0])
            response_payload = _coerce_json_object(row[8])
            normalized_source_lang = _normalize_lang_code(row[6])
            normalized_target_lang = _normalize_lang_code(row[7])
            source_text, target_text = _resolve_dictionary_source_target_texts(
                source_lang=normalized_source_lang,
                target_lang=normalized_target_lang,
                word_ru=row[2],
                translation_de=row[3],
                word_de=row[4],
                translation_ru=row[5],
                response_json=response_payload,
            )
            if not normalized_source_lang or not normalized_target_lang or not source_text or not target_text:
                continue
            with conn.cursor() as cursor:
                canonical_entry_id = _upsert_dictionary_canonical_entry_with_cursor(
                    cursor,
                    source_lang=normalized_source_lang,
                    target_lang=normalized_target_lang,
                    source_text=source_text,
                    target_text=target_text,
                    word_ru=row[2],
                    translation_de=row[3],
                    word_de=row[4],
                    translation_ru=row[5],
                    response_json=response_payload,
                )
                cursor.execute(
                    """
                    UPDATE bt_3_webapp_dictionary_queries
                    SET canonical_entry_id = %s
                    WHERE id = %s;
                    """,
                    (int(canonical_entry_id), entry_id),
                )
            processed_count += 1
        if processed_count <= 0:
            break

    with conn.cursor() as cursor:
        cursor.execute(
            """
            WITH ranked AS (
                SELECT
                    id,
                    user_id,
                    canonical_entry_id,
                    FIRST_VALUE(id) OVER (
                        PARTITION BY user_id, canonical_entry_id
                        ORDER BY created_at DESC NULLS LAST, id DESC
                    ) AS keep_id,
                    BOOL_OR(COALESCE(is_learned, FALSE)) OVER (
                        PARTITION BY user_id, canonical_entry_id
                    ) AS any_learned,
                    FIRST_VALUE(folder_id) OVER (
                        PARTITION BY user_id, canonical_entry_id
                        ORDER BY CASE WHEN folder_id IS NULL THEN 1 ELSE 0 END, created_at DESC NULLS LAST, id DESC
                    ) AS preferred_folder_id
                FROM bt_3_webapp_dictionary_queries
                WHERE canonical_entry_id IS NOT NULL
            ),
            keep_rows AS (
                SELECT DISTINCT keep_id, any_learned, preferred_folder_id
                FROM ranked
            )
            UPDATE bt_3_webapp_dictionary_queries AS q
            SET
                is_learned = COALESCE(keep_rows.any_learned, FALSE),
                folder_id = COALESCE(q.folder_id, keep_rows.preferred_folder_id)
            FROM keep_rows
            WHERE q.id = keep_rows.keep_id;
            """
        )
        cursor.execute(
            """
            WITH ranked AS (
                SELECT
                    id,
                    FIRST_VALUE(id) OVER (
                        PARTITION BY user_id, canonical_entry_id
                        ORDER BY created_at DESC NULLS LAST, id DESC
                    ) AS keep_id
                FROM bt_3_webapp_dictionary_queries
                WHERE canonical_entry_id IS NOT NULL
            )
            DELETE FROM bt_3_webapp_dictionary_queries AS q
            USING ranked
            WHERE q.id = ranked.id
              AND ranked.id <> ranked.keep_id;
            """
        )
        cursor.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS uq_bt_3_webapp_dictionary_queries_user_canonical
            ON bt_3_webapp_dictionary_queries (user_id, canonical_entry_id)
            WHERE canonical_entry_id IS NOT NULL;
            """
        )
        cursor.execute(
            """
            INSERT INTO bt_3_schema_migrations (migration_key)
            VALUES (%s)
            ON CONFLICT (migration_key) DO NOTHING;
            """,
            (DICTIONARY_CANONICAL_SCHEMA_MIGRATION_KEY,),
        )


def get_translation_focus_pool_bucket_counts(
    *,
    source_lang: str,
    target_lang: str,
) -> list[dict[str, Any]]:
    normalized_source_lang = _normalize_lang_code(source_lang)
    normalized_target_lang = _normalize_lang_code(target_lang)
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    focus_key,
                    COALESCE(NULLIF(MAX(focus_label), ''), focus_key) AS focus_label,
                    level,
                    COUNT(*)::BIGINT AS ready_count
                FROM bt_3_translation_sentence_pool
                WHERE source_lang = %s
                  AND target_lang = %s
                  AND is_active = TRUE
                GROUP BY focus_key, level
                ORDER BY focus_key, level;
                """,
                (normalized_source_lang, normalized_target_lang),
            )
            rows = cursor.fetchall() or []
    return [
        {
            "focus_key": str(row[0] or "").strip(),
            "focus_label": str(row[1] or row[0] or "").strip(),
            "level": str(row[2] or "").strip().lower(),
            "ready_count": int(row[3] or 0),
        }
        for row in rows
        if str(row[0] or "").strip() and str(row[2] or "").strip()
    ]


def upsert_translation_focus_pool_daily_snapshot(
    *,
    snapshot_date: date,
    source_lang: str,
    target_lang: str,
    rows: list[dict[str, Any]],
) -> int:
    normalized_source_lang = _normalize_lang_code(source_lang)
    normalized_target_lang = _normalize_lang_code(target_lang)
    payload_rows: list[tuple[Any, ...]] = []
    for item in list(rows or []):
        focus_key = str((item or {}).get("focus_key") or "").strip()
        level = str((item or {}).get("level") or "").strip().lower()
        if not focus_key or not level:
            continue
        payload_rows.append(
            (
                snapshot_date,
                normalized_source_lang,
                normalized_target_lang,
                focus_key,
                str((item or {}).get("focus_label") or focus_key).strip(),
                level,
                int((item or {}).get("ready_count") or 0),
                int((item or {}).get("low_watermark") or 0),
                int((item or {}).get("target_ready") or 0),
            )
        )
    if not payload_rows:
        return 0
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.executemany(
                """
                INSERT INTO bt_3_translation_focus_pool_daily_snapshots (
                    snapshot_date,
                    source_lang,
                    target_lang,
                    focus_key,
                    focus_label,
                    level,
                    ready_count,
                    low_watermark,
                    target_ready
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (snapshot_date, source_lang, target_lang, focus_key, level) DO UPDATE
                SET focus_label = EXCLUDED.focus_label,
                    ready_count = EXCLUDED.ready_count,
                    low_watermark = EXCLUDED.low_watermark,
                    target_ready = EXCLUDED.target_ready,
                    recorded_at = NOW();
                """,
                payload_rows,
            )
    return len(payload_rows)


def get_translation_focus_pool_daily_snapshot(
    *,
    snapshot_date: date,
    source_lang: str,
    target_lang: str,
) -> list[dict[str, Any]]:
    normalized_source_lang = _normalize_lang_code(source_lang)
    normalized_target_lang = _normalize_lang_code(target_lang)
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    focus_key,
                    focus_label,
                    level,
                    ready_count,
                    low_watermark,
                    target_ready,
                    recorded_at
                FROM bt_3_translation_focus_pool_daily_snapshots
                WHERE snapshot_date = %s
                  AND source_lang = %s
                  AND target_lang = %s
                ORDER BY focus_key, level;
                """,
                (
                    snapshot_date,
                    normalized_source_lang,
                    normalized_target_lang,
                ),
            )
            rows = cursor.fetchall() or []
    return [
        {
            "focus_key": str(row[0] or "").strip(),
            "focus_label": str(row[1] or row[0] or "").strip(),
            "level": str(row[2] or "").strip().lower(),
            "ready_count": int(row[3] or 0),
            "low_watermark": int(row[4] or 0),
            "target_ready": int(row[5] or 0),
            "recorded_at": row[6].isoformat() if row[6] else None,
        }
        for row in rows
        if str(row[0] or "").strip() and str(row[2] or "").strip()
    ]


def _env_int(name: str, default: int) -> int:
    try:
        return int(str(os.getenv(name, str(default))).strip() or str(default))
    except Exception:
        return int(default)


def _env_decimal(name: str, default: str | None) -> Decimal | None:
    raw = os.getenv(name)
    if raw is None:
        raw = default
    if raw is None:
        return None
    value = str(raw).strip()
    if not value:
        return None
    if value.lower() == "null":
        return None
    try:
        parsed = Decimal(value)
    except (InvalidOperation, ValueError):
        raise ValueError(f"{name} must be a valid decimal or NULL, got: {value!r}")
    if parsed <= 0:
        raise ValueError(f"{name} must be > 0 or NULL, got: {value!r}")
    return parsed


FLASHCARD_RECENT_SEEN_HOURS = max(1, _env_int("FLASHCARD_RECENT_SEEN_HOURS", 24))


# Used only if billing ledger stores events in USD and caps are enforced in EUR.
FX_USD_TO_EUR = _env_decimal("FX_USD_TO_EUR", "0.92") or Decimal("0.92")
TRIAL_POLICY_DAYS = max(0, _env_int("TRIAL_DAYS", 3))
TRIAL_POLICY_TZ = "Europe/Vienna"
GOOGLE_TTS_MONTHLY_BASE_LIMIT_CHARS = max(1, _env_int("GOOGLE_TTS_MONTHLY_BASE_LIMIT_CHARS", 1_000_000))
GOOGLE_TRANSLATE_MONTHLY_BASE_LIMIT_CHARS = max(1, _env_int("GOOGLE_TRANSLATE_MONTHLY_BASE_LIMIT_CHARS", 500_000))
DEEPL_MONTHLY_BASE_LIMIT_CHARS = max(1, _env_int("DEEPL_MONTHLY_BASE_LIMIT_CHARS", 500_000))
AZURE_TRANSLATOR_MONTHLY_BASE_LIMIT_CHARS = max(1, _env_int("AZURE_TRANSLATOR_MONTHLY_BASE_LIMIT_CHARS", 2_000_000))
PERPLEXITY_MONTHLY_BASE_LIMIT_REQUESTS = max(0, _env_int("PERPLEXITY_MONTHLY_BASE_LIMIT_REQUESTS", 0))
CLOUDFLARE_R2_CLASS_A_MONTHLY_BASE_LIMIT_OPS = max(0, _env_int("CLOUDFLARE_R2_CLASS_A_MONTHLY_BASE_LIMIT_OPS", 1_000_000))
CLOUDFLARE_R2_CLASS_B_MONTHLY_BASE_LIMIT_OPS = max(0, _env_int("CLOUDFLARE_R2_CLASS_B_MONTHLY_BASE_LIMIT_OPS", 10_000_000))
CLOUDFLARE_R2_STORAGE_MONTHLY_BASE_LIMIT_MB = max(0, _env_int("CLOUDFLARE_R2_STORAGE_MONTHLY_BASE_LIMIT_MB", 0))
CLOUDFLARE_R2_STORAGE_MONTHLY_BASE_LIMIT_GB = max(0, _env_int("CLOUDFLARE_R2_STORAGE_MONTHLY_BASE_LIMIT_GB", 10))
STRIPE_MONTHLY_BASE_LIMIT_PAYMENTS = max(0, _env_int("STRIPE_MONTHLY_BASE_LIMIT_PAYMENTS", 0))
READER_AUDIO_PRO_MONTHLY_LIMIT_CHARS = max(1, _env_int("READER_AUDIO_PRO_MONTHLY_LIMIT_CHARS", 10_000))


def convert_cost_to_eur(amount, currency: str | None) -> float:
    try:
        amount_decimal = Decimal(str(amount))
    except (InvalidOperation, ValueError, TypeError):
        amount_decimal = Decimal("0")
    currency_code = str(currency or "EUR").strip().upper() or "EUR"
    if currency_code == "EUR":
        eur_amount = amount_decimal
    elif currency_code == "USD":
        eur_amount = amount_decimal * FX_USD_TO_EUR
    else:
        raise ValueError(f"Unsupported currency for EUR conversion: {currency_code}")
    return float(eur_amount)


def _resolve_timezone(tz_name: str | None) -> ZoneInfo:
    candidate = str(tz_name or TRIAL_POLICY_TZ).strip() or TRIAL_POLICY_TZ
    try:
        return ZoneInfo(candidate)
    except Exception:
        return ZoneInfo(TRIAL_POLICY_TZ)


def _to_aware_datetime(value: datetime | None) -> datetime:
    if value is None:
        return datetime.now(timezone.utc)
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _compute_trial_ends_at(
    now_ts: datetime | None,
    *,
    trial_days: int = TRIAL_POLICY_DAYS,
    tz_name: str = TRIAL_POLICY_TZ,
) -> datetime:
    # Contract (calendar trial in Europe/Vienna):
    # trial_ends_at is set to local 00:00 of (local signup date + TRIAL_DAYS).
    # Example: signup at 2026-02-23 15:00 Europe/Vienna with TRIAL_DAYS=3
    # => trial_ends_at = 2026-02-26 00:00 Europe/Vienna.
    tzinfo = _resolve_timezone(tz_name)
    now_utc = _to_aware_datetime(now_ts)
    local_now = now_utc.astimezone(tzinfo)
    trial_end_date = local_now.date() + timedelta(days=max(0, int(trial_days)))
    trial_end_local = datetime.combine(trial_end_date, dt_time.min, tzinfo=tzinfo)
    return trial_end_local.astimezone(timezone.utc)

LEGACY_ERROR_SKILL_MAP_SEED_DE: list[tuple[str, str, str, float]] = [
    ("Nouns", "Gendered Articles", "nouns_articles_gender", 1.2),
    ("Nouns", "Pluralization", "nouns_plural", 1.0),
    ("Nouns", "Compound Nouns", "nouns_compounds", 0.8),
    ("Nouns", "Declension Errors", "nouns_declension", 1.0),
    ("Cases", "Nominative", "cases_nominative", 0.8),
    ("Cases", "Accusative", "cases_accusative", 1.0),
    ("Cases", "Dative", "cases_dative", 1.0),
    ("Cases", "Genitive", "cases_genitive", 0.9),
    ("Cases", "Akkusativ + Preposition", "cases_preposition_accusative", 1.1),
    ("Cases", "Dative + Preposition", "cases_preposition_dative", 1.1),
    ("Cases", "Genitive + Preposition", "cases_preposition_genitive", 1.0),
    ("Verbs", "Placement", "verbs_placement_general", 1.2),
    ("Verbs", "Conjugation", "verbs_conjugation", 1.1),
    ("Verbs", "Weak Verbs", "verbs_weak", 0.9),
    ("Verbs", "Strong Verbs", "verbs_strong", 1.0),
    ("Verbs", "Mixed Verbs", "verbs_mixed", 1.0),
    ("Verbs", "Separable Verbs", "verbs_separable", 1.1),
    ("Verbs", "Reflexive Verbs", "verbs_reflexive", 1.1),
    ("Verbs", "Auxiliary Verbs", "verbs_auxiliaries", 1.2),
    ("Verbs", "Modal Verbs", "verbs_modals", 1.2),
    ("Verbs", "Verb Placement in Subordinate Clause", "verbs_placement_subordinate", 1.3),
    ("Tenses", "Present", "tenses_present", 0.7),
    ("Tenses", "Past", "tenses_past_general", 0.8),
    ("Tenses", "Simple Past", "tenses_prateritum", 0.9),
    ("Tenses", "Present Perfect", "tenses_perfekt", 1.0),
    ("Tenses", "Past Perfect", "tenses_plusquamperfekt", 1.0),
    ("Tenses", "Future", "tenses_future_general", 0.9),
    ("Tenses", "Future 1", "tenses_futur1", 0.9),
    ("Tenses", "Future 2", "tenses_futur2", 1.0),
    ("Tenses", "Plusquamperfekt Passive", "voice_passive_plusquamperfekt", 1.2),
    ("Tenses", "Futur 1 Passive", "voice_passive_futur1", 1.2),
    ("Tenses", "Futur 2 Passive", "voice_passive_futur2", 1.2),
    ("Adjectives", "Endings", "adjectives_endings_general", 1.3),
    ("Adjectives", "Weak Declension", "adjectives_declension_weak", 1.2),
    ("Adjectives", "Strong Declension", "adjectives_declension_strong", 1.2),
    ("Adjectives", "Mixed Declension", "adjectives_declension_mixed", 1.2),
    ("Adjectives", "Placement", "adjectives_placement", 0.9),
    ("Adjectives", "Comparative", "adjectives_comparative", 0.8),
    ("Adjectives", "Superlative", "adjectives_superlative", 0.8),
    ("Adjectives", "Incorrect Adjective Case Agreement", "adjectives_case_agreement", 1.3),
    ("Adverbs", "Placement", "adverbs_placement", 0.9),
    ("Adverbs", "Multiple Adverbs", "adverbs_multiple_order", 1.0),
    ("Adverbs", "Incorrect Adverb Usage", "adverbs_usage", 1.0),
    ("Conjunctions", "Coordinating", "conj_coordinating", 0.9),
    ("Conjunctions", "Subordinating", "conj_subordinating", 1.1),
    ("Conjunctions", "Incorrect Use of Conjunctions", "conj_usage", 1.1),
    ("Prepositions", "Accusative", "prepositions_accusative_group", 1.0),
    ("Prepositions", "Dative", "prepositions_dative_group", 1.0),
    ("Prepositions", "Genitive", "prepositions_genitive_group", 0.9),
    ("Prepositions", "Two-way", "prepositions_two_way", 1.2),
    ("Prepositions", "Incorrect Preposition Usage", "prepositions_usage", 1.2),
    ("Moods", "Indicative", "moods_indicative", 0.6),
    ("Moods", "Declarative", "moods_declarative", 0.6),
    ("Moods", "Interrogative", "moods_interrogative", 0.7),
    ("Moods", "Imperative", "moods_imperative", 0.9),
    ("Moods", "Subjunctive 1", "moods_subjunctive1", 1.2),
    ("Moods", "Subjunctive 2", "moods_subjunctive2", 1.2),
    ("Word Order", "Standard", "word_order_standard", 0.8),
    ("Word Order", "Inverted", "word_order_inverted", 1.0),
    ("Word Order", "Verb-Second Rule", "word_order_v2_rule", 1.2),
    ("Word Order", "Position of Negation", "word_order_negation_position", 1.1),
    ("Word Order", "Incorrect Order in Subordinate Clause", "word_order_subordinate_clause", 1.3),
    ("Word Order", "Incorrect Order with Modal Verb", "word_order_modal_structure", 1.2),
    ("Other mistake", "Unclassified mistake", "other_unclassified", 1.0),
]

DE_TAXONOMY_ALIAS_TO_LEGACY: dict[tuple[str, str], tuple[str, str]] = {
    ("Verbs", "Auxiliary Verbs (sein/haben/werden)"): ("Verbs", "Auxiliary Verbs"),
    ("Verbs", "Verb Placement in Main Clause"): ("Verbs", "Placement"),
    ("Tenses", "Present (Präsens)"): ("Tenses", "Present"),
    ("Tenses", "Simple Past (Präteritum)"): ("Tenses", "Simple Past"),
    ("Tenses", "Present Perfect (Perfekt)"): ("Tenses", "Present Perfect"),
    ("Tenses", "Past Perfect (Plusquamperfekt)"): ("Tenses", "Past Perfect"),
    ("Tenses", "Future 1 (Futur I)"): ("Tenses", "Future 1"),
    ("Tenses", "Future 2 (Futur II)"): ("Tenses", "Future 2"),
    ("Moods", "Subjunctive 1 (Konjunktiv I)"): ("Moods", "Subjunctive 1"),
    ("Moods", "Subjunctive 2 (Konjunktiv II)"): ("Moods", "Subjunctive 2"),
    ("Adverbs", "Adverb Placement"): ("Adverbs", "Placement"),
    ("Adverbs", "Multiple Adverbs (TEKAMOLO)"): ("Adverbs", "Multiple Adverbs"),
    ("Prepositions", "Accusative Prepositions"): ("Prepositions", "Accusative"),
    ("Prepositions", "Dative Prepositions"): ("Prepositions", "Dative"),
    ("Prepositions", "Genitive Prepositions"): ("Prepositions", "Genitive"),
    ("Prepositions", "Two-way Prepositions"): ("Prepositions", "Two-way"),
    ("Conjunctions", "Coordinating (und/aber/oder/denn)"): ("Conjunctions", "Coordinating"),
    ("Conjunctions", "Subordinating (weil/dass/ob/wenn...)"): ("Conjunctions", "Subordinating"),
    ("Word Order", "Verb-Second Rule (V2)"): ("Word Order", "Verb-Second Rule"),
}

DE_CATEGORY_DEFAULT_WEIGHT: dict[str, float] = {
    "Nouns": 1.0,
    "Articles & Determiners": 1.1,
    "Cases": 1.1,
    "Pronouns": 1.0,
    "Verbs": 1.1,
    "Voice (Active/Passive)": 1.2,
    "Tenses": 1.0,
    "Moods": 1.0,
    "Adjectives": 1.0,
    "Adverbs": 0.9,
    "Prepositions": 1.0,
    "Conjunctions": 1.0,
    "Word Order": 1.1,
    "Negation": 1.0,
    "Particles": 0.9,
    "Clauses & Sentence Types": 1.1,
    "Infinitive & Participles": 1.1,
    "Punctuation": 0.8,
    "Orthography & Spelling": 0.8,
    "Other mistake": 1.0,
}


def _slugify_skill_component(value: str) -> str:
    normalized = str(value or "").lower().strip()
    normalized = (
        normalized.replace("ä", "ae")
        .replace("ö", "oe")
        .replace("ü", "ue")
        .replace("ß", "ss")
    )
    normalized = re.sub(r"[^a-z0-9]+", "_", normalized)
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    return normalized or "x"


def _build_de_seed_from_taxonomy() -> tuple[list[tuple[str, str, str]], list[tuple[str, str, str, float]]]:
    legacy_lookup: dict[tuple[str, str], tuple[str, float]] = {
        (cat, subcat): (skill_id, float(weight))
        for cat, subcat, skill_id, weight in LEGACY_ERROR_SKILL_MAP_SEED_DE
    }
    skill_seed_by_id: dict[str, tuple[str, str, str]] = {}
    error_map_seed: list[tuple[str, str, str, float]] = []

    for category in VALID_CATEGORIES_DE:
        subcategories = list(VALID_SUBCATEGORIES_DE.get(category, []) or [])
        for subcategory in subcategories:
            if _is_unclassified_error(category, subcategory):
                continue
            key = (category, subcategory)
            legacy_key = DE_TAXONOMY_ALIAS_TO_LEGACY.get(key, key)
            legacy_item = legacy_lookup.get(legacy_key)
            if legacy_item:
                skill_id, weight = legacy_item
            else:
                skill_id = f"de_{_slugify_skill_component(category)}_{_slugify_skill_component(subcategory)}"
                weight = float(DE_CATEGORY_DEFAULT_WEIGHT.get(category, 1.0))

            if skill_id not in skill_seed_by_id:
                skill_seed_by_id[skill_id] = (skill_id, f"{category}: {subcategory}", category)
            error_map_seed.append((category, subcategory, skill_id, float(weight)))

    skill_seed = list(skill_seed_by_id.values())
    return skill_seed, error_map_seed


SKILL_SEED_DE, ERROR_SKILL_MAP_SEED_DE = _build_de_seed_from_taxonomy()

SKILL_SEED_EN: list[tuple[str, str, str]] = [
    ("en_nouns_plural", "Nouns: Pluralization", "Nouns"),
    ("en_nouns_countability", "Nouns: Countable vs Uncountable", "Nouns"),
    ("en_determiners_articles", "Articles: a/an/the/zero", "Articles & Determiners"),
    ("en_determiners_quantifiers", "Determiners: some/any/much/many", "Articles & Determiners"),
    ("en_pronouns_case", "Pronouns: Subject/Object", "Pronouns"),
    ("en_pronouns_reflexive", "Pronouns: Reflexive", "Pronouns"),
    ("en_verbs_agreement", "Verbs: Conjugation/Agreement", "Verbs"),
    ("en_aux_do_support", "Auxiliaries: do-support", "Verbs"),
    ("en_aux_be_having", "Auxiliaries: be/have", "Verbs"),
    ("en_modals", "Verbs: Modal Verbs", "Verbs"),
    ("en_phrasal_verbs", "Verbs: Phrasal Verbs", "Verbs"),
    ("en_verb_patterns", "Verbs: to V / V-ing", "Verbs"),
    ("en_tense_present_simple", "Tense: Present Simple", "Tenses & Aspect"),
    ("en_tense_present_continuous", "Tense: Present Continuous", "Tenses & Aspect"),
    ("en_tense_past_simple", "Tense: Past Simple", "Tenses & Aspect"),
    ("en_tense_past_continuous", "Tense: Past Continuous", "Tenses & Aspect"),
    ("en_tense_present_perfect", "Tense: Present Perfect", "Tenses & Aspect"),
    ("en_tense_past_perfect", "Tense: Past Perfect", "Tenses & Aspect"),
    ("en_future", "Tense: Future", "Tenses & Aspect"),
    ("en_conditionals", "Conditionals", "Tenses & Aspect"),
    ("en_adjectives_order", "Adjectives: Order", "Adjectives"),
    ("en_comparative_superlative", "Comparative/Superlative", "Adjectives"),
    ("en_adverbs_placement", "Adverbs: Placement", "Adverbs"),
    ("en_prepositions_time_place", "Prepositions: Time/Place", "Prepositions"),
    ("en_word_order_questions_negation", "Word Order: Questions/Negation", "Word Order"),
    ("en_other_unclassified", "Other: Unclassified", "Other"),
]

ERROR_SKILL_MAP_SEED_EN: list[tuple[str, str, str, float]] = [
    ("Nouns", "Pluralization", "en_nouns_plural", 1.0),
    ("Nouns", "Countable vs Uncountable", "en_nouns_countability", 1.2),
    ("Nouns", "Articles/Determiners", "en_determiners_articles", 1.1),
    ("Pronouns", "Subject/Object", "en_pronouns_case", 1.0),
    ("Pronouns", "Reflexive Pronouns", "en_pronouns_reflexive", 1.0),
    ("Verbs", "Conjugation/Agreement", "en_verbs_agreement", 1.2),
    ("Verbs", "Auxiliaries (do/be/have)", "en_aux_do_support", 1.1),
    ("Verbs", "Modal Verbs", "en_modals", 1.1),
    ("Verbs", "Phrasal Verbs", "en_phrasal_verbs", 1.0),
    ("Verbs", "Verb Patterns (to V / V-ing)", "en_verb_patterns", 1.2),
    ("Tenses & Aspect", "Present Simple", "en_tense_present_simple", 0.9),
    ("Tenses & Aspect", "Present Continuous", "en_tense_present_continuous", 0.9),
    ("Tenses & Aspect", "Past Simple", "en_tense_past_simple", 0.9),
    ("Tenses & Aspect", "Past Continuous", "en_tense_past_continuous", 1.0),
    ("Tenses & Aspect", "Present Perfect", "en_tense_present_perfect", 1.1),
    ("Tenses & Aspect", "Past Perfect", "en_tense_past_perfect", 1.0),
    ("Tenses & Aspect", "Future (will/going to)", "en_future", 0.9),
    ("Tenses & Aspect", "Conditionals", "en_conditionals", 1.2),
    ("Adjectives", "Order of Adjectives", "en_adjectives_order", 1.0),
    ("Adjectives", "Comparative", "en_comparative_superlative", 0.8),
    ("Adjectives", "Superlative", "en_comparative_superlative", 0.8),
    ("Adverbs", "Placement", "en_adverbs_placement", 1.0),
    ("Prepositions", "Time", "en_prepositions_time_place", 1.0),
    ("Prepositions", "Place", "en_prepositions_time_place", 1.0),
    ("Word Order", "Questions (aux inversion)", "en_word_order_questions_negation", 1.2),
    ("Word Order", "Negation", "en_word_order_questions_negation", 1.1),
    ("Articles & Determiners", "a/an/the/zero", "en_determiners_articles", 1.2),
    ("Articles & Determiners", "Some/Any", "en_determiners_quantifiers", 1.0),
    ("Articles & Determiners", "Much/Many", "en_determiners_quantifiers", 1.0),
    ("Other mistake", "Unclassified mistake", "en_other_unclassified", 1.0),
]

SKILL_SEED_ES: list[tuple[str, str, str]] = [
    ("es_articles_gender", "Nouns: Gendered Articles", "Nouns"),
    ("es_nouns_plural", "Nouns: Pluralization", "Nouns"),
    ("es_agreement_gender_number", "Agreement: Gender/Number", "Nouns"),
    ("es_pronouns_object_lo_la_le", "Pronouns: lo/la/le", "Pronouns"),
    ("es_clitics_placement", "Pronouns: Clitic Placement", "Pronouns"),
    ("es_reflexive_se", "Pronouns: Reflexive se", "Pronouns"),
    ("es_conjugation_general", "Verbs: Conjugation", "Verbs"),
    ("es_ser_estar", "Verbs: Ser vs Estar", "Verbs"),
    ("es_periphrasis_modals", "Verbs: Periphrasis", "Verbs"),
    ("es_imperatives", "Verbs: Imperatives", "Verbs"),
    ("es_tense_present", "Tense: Present", "Tenses"),
    ("es_tense_perfecto", "Tense: Preterito Perfecto", "Tenses"),
    ("es_tense_indefinido", "Tense: Preterito Indefinido", "Tenses"),
    ("es_tense_imperfecto", "Tense: Imperfecto", "Tenses"),
    ("es_tense_pluscuamperfecto", "Tense: Pluscuamperfecto", "Tenses"),
    ("es_tense_future", "Tense: Future", "Tenses"),
    ("es_tense_conditional", "Tense: Conditional", "Tenses"),
    ("es_subjunctive_present", "Mood: Subjunctive Present", "Moods"),
    ("es_subjunctive_past", "Mood: Subjunctive Past", "Moods"),
    ("es_subjunctive_selection", "Mood: Indicative vs Subjunctive", "Moods"),
    ("es_por_para", "Prepositions: Por vs Para", "Prepositions"),
    ("es_personal_a", "Prepositions: Personal A", "Prepositions"),
    ("es_prepositions_usage_general", "Prepositions: Usage", "Prepositions"),
    ("es_word_order_questions", "Word Order: Questions", "Word Order"),
    ("es_negation", "Word Order: Negation", "Word Order"),
    ("es_clitic_order_se_lo", "Word Order: se lo order", "Word Order"),
    ("es_adj_adv_comparison", "Adjectives/Adverbs: Comparison", "Adjectives/Adverbs"),
    ("es_adverbs_formation", "Adverbs: Formation", "Adjectives/Adverbs"),
    ("es_orthography_accents", "Orthography: Accent Marks", "Orthography"),
    ("es_orthography_punctuation_spelling", "Orthography: Punctuation/Spelling", "Orthography"),
    ("es_other_unclassified", "Other: Unclassified", "Other"),
]

ERROR_SKILL_MAP_SEED_ES: list[tuple[str, str, str, float]] = [
    ("Nouns", "Gendered Articles", "es_articles_gender", 1.2),
    ("Nouns", "Pluralization", "es_nouns_plural", 1.0),
    ("Nouns", "Agreement (gender/number)", "es_agreement_gender_number", 1.2),
    ("Pronouns", "Object Pronouns (lo/la/le)", "es_pronouns_object_lo_la_le", 1.2),
    ("Pronouns", "Clitic Placement", "es_clitics_placement", 1.2),
    ("Pronouns", "Reflexive (se)", "es_reflexive_se", 1.0),
    ("Verbs", "Conjugation", "es_conjugation_general", 1.1),
    ("Verbs", "Ser vs Estar", "es_ser_estar", 1.3),
    ("Verbs", "Modal/Periphrasis (ir a, tener que)", "es_periphrasis_modals", 1.0),
    ("Verbs", "Imperatives", "es_imperatives", 1.0),
    ("Tenses", "Present", "es_tense_present", 0.8),
    ("Tenses", "Preterito Perfecto", "es_tense_perfecto", 1.0),
    ("Tenses", "Preterito Indefinido", "es_tense_indefinido", 1.1),
    ("Tenses", "Imperfecto", "es_tense_imperfecto", 1.1),
    ("Tenses", "Pluscuamperfecto", "es_tense_pluscuamperfecto", 1.1),
    ("Tenses", "Future", "es_tense_future", 0.9),
    ("Tenses", "Conditional", "es_tense_conditional", 1.0),
    ("Moods", "Subjunctive (Present)", "es_subjunctive_present", 1.2),
    ("Moods", "Subjunctive (Past)", "es_subjunctive_past", 1.2),
    ("Moods", "Indicative vs Subjunctive", "es_subjunctive_selection", 1.3),
    ("Prepositions", "Por vs Para", "es_por_para", 1.3),
    ("Prepositions", "A personal", "es_personal_a", 1.1),
    ("Prepositions", "Preposition Usage", "es_prepositions_usage_general", 1.1),
    ("Word Order", "Questions", "es_word_order_questions", 1.0),
    ("Word Order", "Negation", "es_negation", 0.9),
    ("Word Order", "Clitic order (se lo)", "es_clitic_order_se_lo", 1.3),
    ("Adjectives/Adverbs", "Comparative/Superlative", "es_adj_adv_comparison", 0.9),
    ("Adjectives/Adverbs", "Adverb Formation", "es_adverbs_formation", 0.9),
    ("Orthography", "Accent Marks", "es_orthography_accents", 1.1),
    ("Orthography", "Punctuation (¿¡)", "es_orthography_punctuation_spelling", 0.8),
    ("Orthography", "Spelling", "es_orthography_punctuation_spelling", 0.9),
    ("Other mistake", "Unclassified mistake", "es_other_unclassified", 1.0),
]

SKILL_SEED_IT: list[tuple[str, str, str]] = [
    ("it_articles_gender", "Nouns: Gendered Articles", "Nouns"),
    ("it_nouns_plural", "Nouns: Pluralization", "Nouns"),
    ("it_agreement_gender_number", "Agreement: Gender/Number", "Nouns"),
    ("it_partitive_articles", "Nouns: Partitive Articles", "Nouns"),
    ("it_pronouns_direct_indirect", "Pronouns: Direct/Indirect", "Pronouns"),
    ("it_clitics_placement", "Pronouns: Clitic Placement", "Pronouns"),
    ("it_reflexive_si", "Pronouns: Reflexive si", "Pronouns"),
    ("it_ci_ne", "Pronouns: ci/ne", "Pronouns"),
    ("it_conjugation_general", "Verbs: Conjugation", "Verbs"),
    ("it_aux_essere_avere", "Verbs: essere vs avere", "Verbs"),
    ("it_modals", "Verbs: Modal Verbs", "Verbs"),
    ("it_imperatives", "Verbs: Imperatives", "Verbs"),
    ("it_tense_presente", "Tense: Presente", "Tenses"),
    ("it_tense_passato_prossimo", "Tense: Passato Prossimo", "Tenses"),
    ("it_tense_imperfetto", "Tense: Imperfetto", "Tenses"),
    ("it_tense_trapassato", "Tense: Trapassato Prossimo", "Tenses"),
    ("it_tense_futuro", "Tense: Futuro", "Tenses"),
    ("it_tense_condizionale", "Tense: Condizionale", "Tenses"),
    ("it_congiuntivo_present", "Mood: Congiuntivo Present", "Moods"),
    ("it_congiuntivo_past", "Mood: Congiuntivo Past", "Moods"),
    ("it_congiuntivo_selection", "Mood: Indicative vs Congiuntivo", "Moods"),
    ("it_prepositions_articulated", "Prepositions: Articulated", "Prepositions"),
    ("it_prepositions_usage_general", "Prepositions: Usage", "Prepositions"),
    ("it_word_order_questions", "Word Order: Questions", "Word Order"),
    ("it_negation", "Word Order: Negation", "Word Order"),
    ("it_double_pronouns", "Word Order: Double Pronouns", "Word Order"),
    ("it_adj_adv_comparison", "Adjectives/Adverbs: Comparison", "Adjectives/Adverbs"),
    ("it_orthography_accents_spelling", "Orthography: Accents/Spelling", "Orthography"),
    ("it_other_unclassified", "Other: Unclassified", "Other"),
]

ERROR_SKILL_MAP_SEED_IT: list[tuple[str, str, str, float]] = [
    ("Nouns", "Gendered Articles", "it_articles_gender", 1.2),
    ("Nouns", "Pluralization", "it_nouns_plural", 1.0),
    ("Nouns", "Agreement (gender/number)", "it_agreement_gender_number", 1.2),
    ("Nouns", "Partitive (del, della)", "it_partitive_articles", 1.1),
    ("Pronouns", "Direct/Indirect (lo/la/gli/le)", "it_pronouns_direct_indirect", 1.2),
    ("Pronouns", "Clitic Placement", "it_clitics_placement", 1.2),
    ("Pronouns", "Reflexive (si)", "it_reflexive_si", 1.0),
    ("Pronouns", "Ci/Ne", "it_ci_ne", 1.1),
    ("Verbs", "Conjugation", "it_conjugation_general", 1.1),
    ("Verbs", "Essere vs Avere (aux)", "it_aux_essere_avere", 1.3),
    ("Verbs", "Modal Verbs", "it_modals", 1.0),
    ("Verbs", "Imperatives", "it_imperatives", 1.0),
    ("Tenses", "Presente", "it_tense_presente", 0.8),
    ("Tenses", "Passato Prossimo", "it_tense_passato_prossimo", 1.1),
    ("Tenses", "Imperfetto", "it_tense_imperfetto", 1.1),
    ("Tenses", "Trapassato Prossimo", "it_tense_trapassato", 1.1),
    ("Tenses", "Futuro", "it_tense_futuro", 0.9),
    ("Tenses", "Condizionale", "it_tense_condizionale", 1.0),
    ("Moods", "Congiuntivo (Present)", "it_congiuntivo_present", 1.2),
    ("Moods", "Congiuntivo (Past)", "it_congiuntivo_past", 1.2),
    ("Moods", "Indicative vs Congiuntivo", "it_congiuntivo_selection", 1.3),
    ("Prepositions", "Articulated (nel, sul)", "it_prepositions_articulated", 1.1),
    ("Prepositions", "Preposition Usage", "it_prepositions_usage_general", 1.1),
    ("Word Order", "Questions", "it_word_order_questions", 1.0),
    ("Word Order", "Negation", "it_negation", 0.9),
    ("Word Order", "Double pronouns", "it_double_pronouns", 1.2),
    ("Adjectives/Adverbs", "Comparative/Superlative", "it_adj_adv_comparison", 0.9),
    ("Orthography", "Accents", "it_orthography_accents_spelling", 1.0),
    ("Orthography", "Spelling", "it_orthography_accents_spelling", 1.0),
    ("Other mistake", "Unclassified mistake", "it_other_unclassified", 1.0),
]

SKILL_SEED_DE = [
    row for row in SKILL_SEED_DE
    if not _is_unclassified_skill_seed(row[0], row[1], row[2])
]
SKILL_SEED_EN = [
    row for row in SKILL_SEED_EN
    if not _is_unclassified_skill_seed(row[0], row[1], row[2])
]
SKILL_SEED_ES = [
    row for row in SKILL_SEED_ES
    if not _is_unclassified_skill_seed(row[0], row[1], row[2])
]
SKILL_SEED_IT = [
    row for row in SKILL_SEED_IT
    if not _is_unclassified_skill_seed(row[0], row[1], row[2])
]

ERROR_SKILL_MAP_SEED_DE = [
    row for row in ERROR_SKILL_MAP_SEED_DE
    if not _is_unclassified_error(row[0], row[1])
]
ERROR_SKILL_MAP_SEED_EN = [
    row for row in ERROR_SKILL_MAP_SEED_EN
    if not _is_unclassified_error(row[0], row[1])
]
ERROR_SKILL_MAP_SEED_ES = [
    row for row in ERROR_SKILL_MAP_SEED_ES
    if not _is_unclassified_error(row[0], row[1])
]
ERROR_SKILL_MAP_SEED_IT = [
    row for row in ERROR_SKILL_MAP_SEED_IT
    if not _is_unclassified_error(row[0], row[1])
]

SKILL_SEED: list[tuple[str, str, str, str]] = (
    [(skill_id, title, category, "de") for skill_id, title, category in SKILL_SEED_DE]
    + [(skill_id, title, category, "en") for skill_id, title, category in SKILL_SEED_EN]
    + [(skill_id, title, category, "es") for skill_id, title, category in SKILL_SEED_ES]
    + [(skill_id, title, category, "it") for skill_id, title, category in SKILL_SEED_IT]
)

ERROR_SKILL_MAP_SEED: list[tuple[str, str, str, str, float]] = (
    [("de", cat, subcat, skill_id, weight) for cat, subcat, skill_id, weight in ERROR_SKILL_MAP_SEED_DE]
    + [("en", cat, subcat, skill_id, weight) for cat, subcat, skill_id, weight in ERROR_SKILL_MAP_SEED_EN]
    + [("es", cat, subcat, skill_id, weight) for cat, subcat, skill_id, weight in ERROR_SKILL_MAP_SEED_ES]
    + [("it", cat, subcat, skill_id, weight) for cat, subcat, skill_id, weight in ERROR_SKILL_MAP_SEED_IT]
)

GERMAN_MASTERY_GROUP_SEED: list[tuple[str, str, str, int]] = [
    ("de_nouns_nominals", "Nouns & Nominal Forms", "Noun number, declension, compounds, and nominal formation.", 10),
    ("de_articles_determiners", "Articles & Determiners", "Article choice and the German determiner system.", 20),
    ("de_cases", "Cases & Case Government", "Case selection, case after prepositions, and noun phrase agreement.", 30),
    ("de_pronouns", "Pronouns & Reference", "Pronoun forms, functions, and referential clarity.", 40),
    ("de_verbs_inflection", "Verb Forms & Conjugation", "Verb inflection, auxiliaries, modals, and prefix verbs.", 50),
    ("de_verbs_patterns", "Verb Patterns & Infinitives", "Verb valency, infinitive structures, and participial verb patterns.", 60),
    ("de_tense_aspect", "Tenses & Aspect", "Tense formation, tense choice, and sequence of tenses.", 70),
    ("de_mood_modality", "Mood & Modality", "Imperative, Konjunktiv, hypotheticals, politeness, and reported speech.", 80),
    ("de_passive_voice", "Passive & Voice", "Active/passive contrast and passive constructions.", 90),
    ("de_word_order_main", "Word Order: Main Clause", "V2, inversion, negation, and constituent order in main clauses.", 100),
    ("de_word_order_subordinate", "Word Order: Subordinate Clause", "Subordinate clause order and verbal cluster placement.", 110),
    ("de_clauses_connectors", "Clauses & Connectors", "Clause linking, conjunction choice, and clause types.", 120),
    ("de_prepositions", "Prepositions", "Preposition choice, government, and fixed prepositional phrases.", 130),
    ("de_adjectives", "Adjectives & Agreement", "Adjective endings, agreement, comparison, and adjective use.", 140),
    ("de_adverbs_particles", "Adverbs & Particles", "Adverb order, sentence adverbs, and German particles.", 150),
    ("de_negation", "Negation", "Negation forms, placement, and scope.", 160),
    ("de_writing_mechanics", "Writing Mechanics", "Capitalization, spelling, punctuation, and other writing conventions.", 170),
]

GERMAN_MASTERY_GROUP_DEFAULTS_BY_CATEGORY: dict[str, str] = {
    "Nouns": "de_nouns_nominals",
    "Articles & Determiners": "de_articles_determiners",
    "Cases": "de_cases",
    "Pronouns": "de_pronouns",
    "Verbs": "de_verbs_inflection",
    "Voice (Active/Passive)": "de_passive_voice",
    "Tenses": "de_tense_aspect",
    "Moods": "de_mood_modality",
    "Adjectives": "de_adjectives",
    "Adverbs": "de_adverbs_particles",
    "Prepositions": "de_prepositions",
    "Conjunctions": "de_clauses_connectors",
    "Word Order": "de_word_order_main",
    "Negation": "de_negation",
    "Particles": "de_adverbs_particles",
    "Clauses & Sentence Types": "de_clauses_connectors",
    "Infinitive & Participles": "de_verbs_patterns",
    "Punctuation": "de_writing_mechanics",
    "Orthography & Spelling": "de_writing_mechanics",
}

GERMAN_MASTERY_GROUP_OVERRIDES_BY_SKILL_ID: dict[str, str] = {
    "de_nouns_noun_capitalization": "de_writing_mechanics",
    "verbs_placement_subordinate": "de_word_order_subordinate",
    "word_order_subordinate_clause": "de_word_order_subordinate",
    "word_order_modal_structure": "de_word_order_subordinate",
    "de_word_order_placement_of_participle_perfekt_passive": "de_word_order_subordinate",
    "de_word_order_placement_of_separable_prefix": "de_word_order_subordinate",
    "de_clauses_sentence_types_infinitive_clauses_vs_dass_clause": "de_verbs_patterns",
}

GERMAN_MASTERY_LEAF_SKILL_IDS: set[str] = {
    "nouns_plural",
    "nouns_declension",
    "de_articles_determiners_definite_articles_der_die_das",
    "de_articles_determiners_indefinite_articles_ein_eine",
    "de_articles_determiners_possessive_determiners_mein_dein",
    "de_articles_determiners_negation_article_kein",
    "cases_accusative",
    "cases_dative",
    "cases_genitive",
    "de_cases_case_after_preposition",
    "de_cases_two_way_prepositions_wechselpraepositionen",
    "de_pronouns_personal_pronouns",
    "de_pronouns_reflexive_pronouns",
    "de_pronouns_relative_pronouns",
    "verbs_auxiliaries",
    "verbs_conjugation",
    "verbs_modals",
    "verbs_separable",
    "de_verbs_inseparable_prefix_verbs",
    "de_verbs_verb_valency_missing_object_complement",
    "tenses_present",
    "tenses_perfekt",
    "tenses_prateritum",
    "tenses_plusquamperfekt",
    "tenses_futur1",
    "moods_imperative",
    "moods_subjunctive1",
    "moods_subjunctive2",
    "de_moods_reported_speech_indirekte_rede",
    "de_moods_konjunktiv_ii_wuerde_form",
    "de_voice_active_passive_vorgangspassiv_werden_partizip_ii",
    "de_voice_active_passive_zustandspassiv_sein_partizip_ii",
    "word_order_v2_rule",
    "word_order_subordinate_clause",
    "word_order_modal_structure",
    "de_word_order_position_of_time_manner_place",
    "de_clauses_sentence_types_relative_clauses",
    "de_clauses_sentence_types_conditionals_wenn_falls",
    "de_clauses_sentence_types_purpose_clauses_damit_um_zu",
    "prepositions_two_way",
    "prepositions_usage",
    "adjectives_endings_general",
    "adjectives_case_agreement",
    "adjectives_comparative",
    "adjectives_superlative",
    "de_negation_nicht_vs_kein",
    "de_negation_negation_placement",
    "de_orthography_spelling_common_spelling_errors",
    "de_punctuation_comma_in_subordinate_clause",
}

GERMAN_MASTERY_ROLLUP_ZERO_WEIGHT_SKILL_IDS: set[str] = {
    "moods_indicative",
    "word_order_standard",
    "word_order_inverted",
    "verbs_placement_general",
    "verbs_placement_subordinate",
    "conj_coordinating",
    "conj_subordinating",
    "adverbs_placement",
    "adverbs_usage",
    "de_particles_antwortpartikeln_ja_nein_doch",
    "de_particles_focus_particles_nur_auch_sogar",
    "de_particles_particle_misuse_omission",
    "de_orthography_spelling_hyphenation",
    "de_orthography_spelling_ss_ss",
    "de_punctuation_question_mark_exclamation",
    "de_punctuation_quotation_marks",
    "de_clauses_sentence_types_question_formation",
    "de_word_order_verb_first_questions_imperative",
    "de_adjectives_adjective_placement",
    "adjectives_placement",
}

GERMAN_MASTERY_DISPLAY_TITLE_OVERRIDES: dict[str, str] = {
    "verbs_conjugation": "Verb Conjugation & Agreement",
    "verbs_modals": "Modal Verbs",
    "verbs_auxiliaries": "Auxiliary Verbs",
    "verbs_separable": "Separable Verbs",
    "de_verbs_inseparable_prefix_verbs": "Inseparable Prefix Verbs",
    "de_verbs_verb_valency_missing_object_complement": "Verb Valency & Complements",
    "prepositions_usage": "Preposition Choice",
    "conj_usage": "Conjunction Choice",
    "word_order_v2_rule": "Verb-Second Rule (V2)",
    "word_order_subordinate_clause": "Subordinate Clause Order",
    "word_order_modal_structure": "Word Order with Modal Verb",
    "de_word_order_position_of_time_manner_place": "Time, Manner, Place Order",
    "de_negation_nicht_vs_kein": "nicht vs kein",
    "de_negation_negation_placement": "Negation Placement",
}


def _assign_german_mastery_group_id(skill_id: str, category: str) -> str:
    explicit_group = GERMAN_MASTERY_GROUP_OVERRIDES_BY_SKILL_ID.get(str(skill_id or "").strip())
    if explicit_group:
        return explicit_group
    default_group = GERMAN_MASTERY_GROUP_DEFAULTS_BY_CATEGORY.get(str(category or "").strip())
    if not default_group:
        raise ValueError(f"No German mastery group mapping for skill_id={skill_id!r}, category={category!r}")
    return default_group


def _build_german_mastery_group_membership_seed() -> list[tuple[str, str, str, bool, bool, float, str | None, int]]:
    skill_rows = sorted(SKILL_SEED_DE, key=lambda item: (str(item[2]), str(item[1]), str(item[0])))
    seeded_pairs: list[tuple[str, str, str, bool, bool, float, str | None, int]] = []
    group_sort_order_counters: dict[str, int] = {}
    known_group_ids = {group_id for group_id, _title, _description, _sort_order in GERMAN_MASTERY_GROUP_SEED}

    for skill_id, _title, category in skill_rows:
        mastery_group_id = _assign_german_mastery_group_id(skill_id, category)
        if mastery_group_id not in known_group_ids:
            raise ValueError(f"Unknown German mastery group {mastery_group_id!r} for skill_id={skill_id!r}")
        is_mastery_leaf = skill_id in GERMAN_MASTERY_LEAF_SKILL_IDS
        is_diagnostic_only = not is_mastery_leaf
        rollup_weight = 1.0 if is_mastery_leaf else 0.5
        if skill_id in GERMAN_MASTERY_ROLLUP_ZERO_WEIGHT_SKILL_IDS:
            rollup_weight = 0.0
        title_override = GERMAN_MASTERY_DISPLAY_TITLE_OVERRIDES.get(skill_id)
        group_sort_order_counters[mastery_group_id] = group_sort_order_counters.get(mastery_group_id, 0) + 10
        seeded_pairs.append(
            (
                mastery_group_id,
                skill_id,
                "de",
                bool(is_mastery_leaf),
                bool(is_diagnostic_only),
                float(rollup_weight),
                title_override,
                int(group_sort_order_counters[mastery_group_id]),
            )
        )

    seeded_skill_ids = {row[1] for row in seeded_pairs}
    expected_skill_ids = {skill_id for skill_id, _title, _category in SKILL_SEED_DE}
    if seeded_skill_ids != expected_skill_ids:
        missing = sorted(expected_skill_ids - seeded_skill_ids)
        extra = sorted(seeded_skill_ids - expected_skill_ids)
        raise ValueError(
            f"German mastery membership seed mismatch; missing={missing!r}, extra={extra!r}"
        )
    return seeded_pairs


GERMAN_MASTERY_GROUP_MEMBER_SEED = _build_german_mastery_group_membership_seed()


def _as_sql_text_literals(values: set[str]) -> str:
    normalized = sorted(
        {
            str(value or "").strip()
            for value in values
            if str(value or "").strip()
        }
    )
    if not normalized:
        return "''"
    return ", ".join("'" + item.replace("'", "''") + "'" for item in normalized)


def _build_bt3_detailed_mistakes_allowed_labels() -> tuple[set[str], set[str]]:
    main_categories: set[str] = set()
    sub_categories: set[str] = set()

    for category in VALID_CATEGORIES_DE:
        if str(category or "").strip():
            main_categories.add(str(category).strip())
    for category, values in (VALID_SUBCATEGORIES_DE or {}).items():
        if str(category or "").strip():
            main_categories.add(str(category).strip())
        for sub in values or []:
            if str(sub or "").strip():
                sub_categories.add(str(sub).strip())

    for category, sub_category, _skill_id, _weight in LEGACY_ERROR_SKILL_MAP_SEED_DE:
        if str(category or "").strip():
            main_categories.add(str(category).strip())
        if str(sub_category or "").strip():
            sub_categories.add(str(sub_category).strip())

    for _lang, category, sub_category, _skill_id, _weight in ERROR_SKILL_MAP_SEED:
        if str(category or "").strip():
            main_categories.add(str(category).strip())
        if str(sub_category or "").strip():
            sub_categories.add(str(sub_category).strip())

    main_categories.add("Other mistake")
    sub_categories.add("Unclassified mistake")
    return main_categories, sub_categories


def _ensure_bt3_detailed_mistakes_constraints(cursor) -> None:
    cursor.execute("SELECT to_regclass('public.bt_3_detailed_mistakes');")
    exists_row = cursor.fetchone()
    if not exists_row or exists_row[0] is None:
        return
    cursor.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'bt_3_detailed_mistakes'
          AND column_name IN ('main_category', 'sub_category');
        """
    )
    columns_count = int((cursor.fetchone() or [0])[0] or 0)
    if columns_count < 2:
        return

    main_categories, sub_categories = _build_bt3_detailed_mistakes_allowed_labels()
    main_literals = _as_sql_text_literals(main_categories)
    sub_literals = _as_sql_text_literals(sub_categories)

    cursor.execute(
        """
        SELECT conname
        FROM pg_constraint
        WHERE conrelid = 'public.bt_3_detailed_mistakes'::regclass
          AND contype = 'c'
          AND (
              pg_get_constraintdef(oid) ILIKE '%main_category%'
              OR pg_get_constraintdef(oid) ILIKE '%sub_category%'
          );
        """
    )
    for row in cursor.fetchall() or []:
        conname = str(row[0] or "").strip()
        if not conname:
            continue
        cursor.execute(
            sql.SQL("ALTER TABLE public.bt_3_detailed_mistakes DROP CONSTRAINT IF EXISTS {};").format(
                sql.Identifier(conname)
            )
        )

    cursor.execute(
        f"""
        ALTER TABLE public.bt_3_detailed_mistakes
        ADD CONSTRAINT bt_3_detailed_mistakes_main_category_check
        CHECK (main_category IN ({main_literals})) NOT VALID;
        """
    )
    cursor.execute(
        f"""
        ALTER TABLE public.bt_3_detailed_mistakes
        ADD CONSTRAINT bt_3_detailed_mistakes_sub_category_check
        CHECK (sub_category IN ({sub_literals})) NOT VALID;
        """
    )

# Добавим проверку, чтобы сразу видеть ошибку в логах, если адреса нет
if not DATABASE_URL:
    print("❌ ОШИБКА: DATABASE_URL_RAILWAY не найден в .env или переменных окружения!")
else:
    # Для безопасности печатаем только хост, скрывая пароль
    print(f"✅ database.py успешно загрузил URL (хост: {DATABASE_URL.split('@')[-1].split(':')[0]})")

@contextmanager
def get_db_connection_context(): #
    conn = get_db_connection()
    force_close = False
    try:
        yield conn #
        conn.commit() #
    except Exception:
        try:
            conn.rollback()
        except Exception:
            force_close = True
        raise
    finally:
        if isinstance(conn, _PooledConnectionProxy):
            conn.close(force_close=force_close)
        else:
            conn.close()


@contextmanager
def db_acquire_scope(label: str):
    previous_label = getattr(_DB_ACQUIRE_LOCAL, "label", None)
    previous_events = getattr(_DB_ACQUIRE_LOCAL, "events", None)
    scoped_events: list[dict[str, Any]] = []
    _DB_ACQUIRE_LOCAL.label = str(label or "").strip() or "unknown"
    _DB_ACQUIRE_LOCAL.events = scoped_events
    try:
        yield scoped_events
    finally:
        _DB_ACQUIRE_LOCAL.label = previous_label
        _DB_ACQUIRE_LOCAL.events = previous_events


def summarize_db_acquire_events(events: list[dict[str, Any]] | None) -> dict[str, Any]:
    normalized_events = [item for item in (events or []) if isinstance(item, dict)]
    if not normalized_events:
        return {
            "db_pool_acquire_count": 0,
            "db_pool_acquire_wait_ms_total": 0,
            "db_pool_acquire_wait_ms_max": 0,
            "db_pool_slow_acquire_count": 0,
            "db_pool_exhausted_count": 0,
            "db_pool_direct_fallback_count": 0,
            "db_pool_used_count_max": None,
            "db_pool_available_count_min": None,
        }
    wait_values = [int(item.get("wait_ms") or 0) for item in normalized_events]
    used_values = [
        int(item.get("pool_used_count"))
        for item in normalized_events
        if item.get("pool_used_count") is not None
    ]
    available_values = [
        int(item.get("pool_available_count"))
        for item in normalized_events
        if item.get("pool_available_count") is not None
    ]
    return {
        "db_pool_acquire_count": len(normalized_events),
        "db_pool_acquire_wait_ms_total": sum(wait_values),
        "db_pool_acquire_wait_ms_max": max(wait_values) if wait_values else 0,
        "db_pool_slow_acquire_count": sum(1 for item in normalized_events if bool(item.get("slow_acquire"))),
        "db_pool_exhausted_count": sum(1 for item in normalized_events if bool(item.get("pool_exhausted"))),
        "db_pool_direct_fallback_count": sum(1 for item in normalized_events if bool(item.get("direct_fallback"))),
        "db_pool_used_count_max": max(used_values) if used_values else None,
        "db_pool_available_count_min": min(available_values) if available_values else None,
    }


def _new_raw_db_connection():
    conn = None
    last_error = None
    for attempt in range(1, DB_CONNECT_RETRIES + 1):
        try:
            conn = psycopg2.connect(
                DATABASE_URL,
                sslmode='require',
                connect_timeout=DB_CONNECT_TIMEOUT_SECONDS,
                keepalives=1,
                keepalives_idle=30,
                keepalives_interval=10,
                keepalives_count=5,
            )
            break
        except OperationalError as exc:
            last_error = exc
            if attempt >= DB_CONNECT_RETRIES:
                raise
            time.sleep(DB_CONNECT_RETRY_DELAY_SECONDS * attempt)
    if conn is None and last_error is not None:
        raise last_error
    return conn


def _get_or_init_db_pool() -> ThreadedConnectionPool | None:
    global _DB_POOL
    if not DB_POOL_ENABLED or not DATABASE_URL:
        return None
    with _DB_POOL_LOCK:
        if _DB_POOL is not None:
            return _DB_POOL
        last_error = None
        for attempt in range(1, DB_CONNECT_RETRIES + 1):
            try:
                _DB_POOL = ThreadedConnectionPool(
                    DB_POOL_MINCONN,
                    DB_POOL_MAXCONN,
                    DATABASE_URL,
                    sslmode='require',
                    connect_timeout=DB_CONNECT_TIMEOUT_SECONDS,
                    keepalives=1,
                    keepalives_idle=30,
                    keepalives_interval=10,
                    keepalives_count=5,
                )
                break
            except OperationalError as exc:
                last_error = exc
                if attempt >= DB_CONNECT_RETRIES:
                    raise
                time.sleep(DB_CONNECT_RETRY_DELAY_SECONDS * attempt)
        if _DB_POOL is None and last_error is not None:
            raise last_error
        return _DB_POOL


def _capture_pool_state(pool: ThreadedConnectionPool | None) -> tuple[int | None, int | None]:
    if pool is None:
        return None, None
    try:
        used_connections = len(getattr(pool, "_used", {}) or {})
    except Exception:
        used_connections = None
    try:
        available_connections = len(getattr(pool, "_pool", []) or [])
    except Exception:
        available_connections = None
    return used_connections, available_connections


def _record_db_acquire_event(
    *,
    context_label: str,
    wait_ms: int,
    used_pool: bool,
    pool_exhausted: bool,
    direct_fallback: bool,
    pool: ThreadedConnectionPool | None = None,
) -> None:
    slow_acquire = wait_ms >= DB_POOL_LOG_SLOW_ACQUIRE_MS if DB_POOL_LOG_SLOW_ACQUIRE_MS > 0 else False
    pool_used_count, pool_available_count = _capture_pool_state(pool)
    event = {
        "context": context_label,
        "wait_ms": int(wait_ms),
        "used_pool": bool(used_pool),
        "pool_exhausted": bool(pool_exhausted),
        "direct_fallback": bool(direct_fallback),
        "slow_acquire": bool(slow_acquire),
        "pool_used_count": pool_used_count,
        "pool_available_count": pool_available_count,
    }
    scoped_events = getattr(_DB_ACQUIRE_LOCAL, "events", None)
    if isinstance(scoped_events, list):
        scoped_events.append(event)
    if bool(pool_exhausted) or bool(direct_fallback) or bool(slow_acquire):
        logging.warning(
            "db acquire: context=%s wait_ms=%s used_pool=%s pool_exhausted=%s direct_fallback=%s pool_used=%s pool_available=%s",
            context_label,
            int(wait_ms),
            bool(used_pool),
            bool(pool_exhausted),
            bool(direct_fallback),
            pool_used_count,
            pool_available_count,
        )


class _PooledConnectionProxy:
    def __init__(self, conn, pool: ThreadedConnectionPool):
        self._conn = conn
        self._pool = pool
        self._released = False

    def __getattr__(self, name):
        return getattr(self._conn, name)

    def __enter__(self):
        self._conn.__enter__()
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            return self._conn.__exit__(exc_type, exc, tb)
        finally:
            self.close()

    def close(self, force_close: bool = False):
        if self._released:
            return
        self._released = True
        try:
            self._pool.putconn(self._conn, close=bool(force_close))
        except Exception:
            try:
                self._conn.close()
            except Exception:
                pass


class _DirectConnectionProxy:
    def __init__(self, conn):
        self._conn = conn
        self._closed = False

    def __getattr__(self, name):
        return getattr(self._conn, name)

    def __enter__(self):
        self._conn.__enter__()
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            return self._conn.__exit__(exc_type, exc, tb)
        finally:
            self.close()

    def close(self):
        if self._closed:
            return
        self._closed = True
        self._conn.close()


def get_db_connection():
    pool = _get_or_init_db_pool()
    context_label = str(getattr(_DB_ACQUIRE_LOCAL, "label", "") or "unspecified").strip() or "unspecified"
    if pool is None:
        started_at = time.perf_counter()
        conn = _new_raw_db_connection()
        _record_db_acquire_event(
            context_label=context_label,
            wait_ms=max(0, int((time.perf_counter() - started_at) * 1000)),
            used_pool=False,
            pool_exhausted=False,
            direct_fallback=True,
            pool=None,
        )
        return _DirectConnectionProxy(conn)
    deadline = time.perf_counter() + (DB_POOL_ACQUIRE_TIMEOUT_MS / 1000.0)
    pool_exhausted = False
    acquire_started_at = time.perf_counter()
    while True:
        try:
            conn = pool.getconn()
            _record_db_acquire_event(
                context_label=context_label,
                wait_ms=max(0, int((time.perf_counter() - acquire_started_at) * 1000)),
                used_pool=True,
                pool_exhausted=pool_exhausted,
                direct_fallback=False,
                pool=pool,
            )
            return _PooledConnectionProxy(conn, pool)
        except PoolError:
            pool_exhausted = True
            if time.perf_counter() >= deadline:
                break
            time.sleep(DB_POOL_ACQUIRE_RETRY_MS / 1000.0)
    if DB_POOL_ALLOW_DIRECT_FALLBACK:
        conn = _new_raw_db_connection()
        wait_ms = max(0, int((time.perf_counter() - acquire_started_at) * 1000))
        _record_db_acquire_event(
            context_label=context_label,
            wait_ms=wait_ms,
            used_pool=False,
            pool_exhausted=True,
            direct_fallback=True,
            pool=pool,
        )
        return _DirectConnectionProxy(conn)
    raise RuntimeError(
        f"DB pool exhausted for context={context_label} after {max(0, int((time.perf_counter() - acquire_started_at) * 1000))}ms"
    )


def _close_db_pool():
    global _DB_POOL
    with _DB_POOL_LOCK:
        if _DB_POOL is None:
            return
        try:
            _DB_POOL.closeall()
        except Exception:
            pass
        _DB_POOL = None


atexit.register(_close_db_pool)


def _build_language_pair_filter(
    source_lang: str | None,
    target_lang: str | None,
    *,
    table_alias: str | None = None,
) -> tuple[str, list]:
    if not source_lang or not target_lang:
        return "", []
    alias_prefix = f"{table_alias}." if table_alias else ""
    source_expr = (
        "LOWER(COALESCE("
        f"NULLIF({alias_prefix}source_lang, ''), "
        f"NULLIF({alias_prefix}response_json->>'source_lang', ''), "
        f"NULLIF({alias_prefix}response_json#>>'{{language_pair,source_lang}}', ''), "
        "'ru'"
        "))"
    )
    target_expr = (
        "LOWER(COALESCE("
        f"NULLIF({alias_prefix}target_lang, ''), "
        f"NULLIF({alias_prefix}response_json->>'target_lang', ''), "
        f"NULLIF({alias_prefix}response_json#>>'{{language_pair,target_lang}}', ''), "
        "'de'"
        "))"
    )
    clause = f" AND {source_expr} = %s AND {target_expr} = %s"
    return clause, [str(source_lang).lower(), str(target_lang).lower()]

def init_db(): #
    with get_db_connection_context() as conn: #
        with conn.cursor() as cursor: 
            # 1. Таблица для клиентов
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS clients (
                    id SERIAL PRIMARY KEY,
                    first_name TEXT NOT NULL,
                    last_name TEXT,
                    system_id TEXT UNIQUE, -- Уникальный ID клиента в системе (если есть)
                    phone_number TEXT UNIQUE, -- Телефон клиента
                    email TEXT UNIQUE,
                    location TEXT, -- Город или регион клиента
                    manager_contact TEXT, -- Контакты ответственного менеджера
                    is_existing_client BOOLEAN DEFAULT FALSE, -- Признак, работает ли клиент с нами
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            print("✅ Таблица 'clients' проверена/создана.")

            # 2. Таблица для продуктов/услуг
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS products (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL UNIQUE,
                    description TEXT,
                    price DECIMAL(10, 2) NOT NULL, -- Цена продукта, 10 цифр всего, 2 после запятой
                    is_new BOOLEAN DEFAULT FALSE, -- Признак новинки
                    available_quantity INT DEFAULT 0, -- Доступное количество на складе
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            print("✅ Таблица 'products' проверена/создана.")

            # 3. Таблица для заказов
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS orders (
                    id SERIAL PRIMARY KEY,
                    client_id INT REFERENCES clients(id), -- Внешний ключ на клиента
                    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    status TEXT DEFAULT 'pending', -- Статус заказа (pending, completed, cancelled)
                    total_amount DECIMAL(10, 2), -- Общая сумма заказа
                    order_details JSONB, -- Подробности заказа в JSON-формате (например, {"product_id": 1, "quantity": 2})
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            print("✅ Таблица 'orders' проверена/создана.")

            # Пример: Добавление базовых продуктов (для тестирования)
            # Внимание: для реального использования, эти данные должны управляться через CRM/API
            products_to_insert = [
                ("LapTop ZenBook Pro", "The powerful Laptop for professionals, 16GB RAM, 1TB SSD", 1500.00, True, 100),
                ("Smartphone UltraVision 2000", "Top smartphone with AI-camera and super detailed night mode", 999.99, False, 250),
                ("Monitor ErgoView", "Energy saving 27 inch monitor with full HD", 450.50, False, 50),
                ("Whireless earphones AirPods", "Earpods with noice cancellation and 30 hours autonomous working time", 120.00, True, 300)
            ]
            for name, description, price, is_new, quantity in products_to_insert:
                cursor.execute("""
                    INSERT INTO products (name, description, price, is_new, available_quantity)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (name) DO UPDATE SET
                        description = EXCLUDED.description, -- специальное ключевое слово в PostgreSQL. Оно ссылается на значение, которое было бы вставлено, если бы конфликта не произошло. То есть, это значение description, которое вы пытались вставить в этой конкретной INSERT операции.
                        price = EXCLUDED.price,
                        is_new = EXCLUDED.is_new,
                        available_quantity = EXCLUDED.available_quantity;
                """, (name, description, price, is_new, quantity))
            print("✅ Базовые продукты вставлены/обновлены.")

    print("✅ Инициализация базы данных завершена.")


def ensure_webapp_tables() -> None:
    global _ENSURE_WEBAPP_TABLES_DONE
    if _ENSURE_WEBAPP_TABLES_DONE:
        return
    with _ENSURE_WEBAPP_TABLES_MUTEX:
        if _ENSURE_WEBAPP_TABLES_DONE:
            return
        with get_db_connection_context() as conn:
            cursor = conn.cursor()
            # Serialize startup DDL across parallel workers/services to avoid
            # deadlocks on ALTER TABLE ... ADD COLUMN IF NOT EXISTS.
            cursor.execute("SELECT pg_advisory_xact_lock(%s);", (WEBAPP_SCHEMA_MIGRATION_LOCK_KEY,))
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS assistants (
                    task_name TEXT PRIMARY KEY,
                    assistant_id TEXT NOT NULL
                );
            """)
            # Backward compatibility: if legacy table "assistant" exists,
            # migrate rows into "assistants".
            cursor.execute("""
                DO $$
                BEGIN
                    IF to_regclass('public.assistant') IS NOT NULL THEN
                        INSERT INTO assistants (task_name, assistant_id)
                        SELECT task_name, assistant_id
                        FROM assistant
                        ON CONFLICT (task_name) DO UPDATE
                        SET assistant_id = EXCLUDED.assistant_id;
                    END IF;
                END $$;
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_allowed_users (
                    user_id BIGINT PRIMARY KEY,
                    username TEXT,
                    added_by BIGINT,
                    note TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_allowed_users_updated
                ON bt_3_allowed_users (updated_at);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_user_removal_queue (
                    user_id BIGINT PRIMARY KEY,
                    username TEXT,
                    revoked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    grace_until TIMESTAMPTZ NOT NULL,
                    status TEXT NOT NULL DEFAULT 'scheduled',
                    scheduled_by BIGINT,
                    reason TEXT,
                    notification_sent_at TIMESTAMPTZ,
                    notification_message_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
                    decision_at TIMESTAMPTZ,
                    decision_by BIGINT,
                    decision_note TEXT,
                    purged_at TIMESTAMPTZ,
                    purge_summary JSONB NOT NULL DEFAULT '{}'::jsonb,
                    billing_cancel_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    CHECK (status IN ('scheduled', 'awaiting_admin_confirmation', 'canceled', 'purged'))
                );
            """)
            cursor.execute("""
                ALTER TABLE bt_3_user_removal_queue
                ADD COLUMN IF NOT EXISTS billing_cancel_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb;
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_user_removal_queue_status_grace
                ON bt_3_user_removal_queue (status, grace_until ASC);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_user_language_profile (
                    user_id BIGINT PRIMARY KEY,
                    learning_language TEXT NOT NULL DEFAULT 'de',
                    native_language TEXT NOT NULL DEFAULT 'ru',
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            cursor.execute(
                """
                DO $$
                DECLARE c RECORD;
                BEGIN
                    -- Keep language profile columns as plain TEXT for flexible app-level validation.
                    BEGIN
                        ALTER TABLE bt_3_user_language_profile
                        ALTER COLUMN learning_language TYPE TEXT
                        USING learning_language::text;
                    EXCEPTION WHEN undefined_column THEN
                        NULL;
                    END;
                    BEGIN
                        ALTER TABLE bt_3_user_language_profile
                        ALTER COLUMN native_language TYPE TEXT
                        USING native_language::text;
                    EXCEPTION WHEN undefined_column THEN
                        NULL;
                    END;

                    -- Drop legacy CHECK constraints that may enforce old regex patterns.
                    FOR c IN
                        SELECT conname
                        FROM pg_constraint
                        WHERE conrelid = 'public.bt_3_user_language_profile'::regclass
                          AND contype = 'c'
                    LOOP
                        EXECUTE format(
                            'ALTER TABLE public.bt_3_user_language_profile DROP CONSTRAINT IF EXISTS %I',
                            c.conname
                        );
                    END LOOP;
                END $$;
                """
            )
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_user_language_profile_updated
                ON bt_3_user_language_profile (updated_at DESC);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_user_progress_resets (
                    user_id BIGINT NOT NULL,
                    source_lang TEXT NOT NULL DEFAULT 'ru',
                    target_lang TEXT NOT NULL DEFAULT 'de',
                    reset_date DATE NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (user_id, source_lang, target_lang)
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_user_progress_resets_updated
                ON bt_3_user_progress_resets (updated_at DESC);
            """)
            cursor.execute(
                """
                DO $$
                BEGIN
                    IF to_regclass('public.bt_3_user_progress') IS NOT NULL THEN
                        CREATE INDEX IF NOT EXISTS idx_bt_3_user_progress_user_completed_started
                        ON bt_3_user_progress (user_id, completed, start_time DESC);
                    END IF;
                END $$;
                """
            )
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_webapp_scope_state (
                    user_id BIGINT PRIMARY KEY,
                    scope_kind TEXT NOT NULL DEFAULT 'personal',
                    scope_chat_id BIGINT,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    CHECK (scope_kind IN ('personal', 'group')),
                    CHECK (
                        (scope_kind = 'personal' AND scope_chat_id IS NULL)
                        OR
                        (scope_kind = 'group' AND scope_chat_id IS NOT NULL)
                    )
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_webapp_scope_state_updated
                ON bt_3_webapp_scope_state (updated_at DESC);
            """)
            cursor.execute(
                """
                DO $$
                BEGIN
                    IF to_regclass('public.bt_3_user_progress') IS NOT NULL THEN
                        ALTER TABLE bt_3_user_progress
                            ADD COLUMN IF NOT EXISTS active_seconds BIGINT;
                        ALTER TABLE bt_3_user_progress
                            ADD COLUMN IF NOT EXISTS active_started_at TIMESTAMPTZ;
                        ALTER TABLE bt_3_user_progress
                            ADD COLUMN IF NOT EXISTS active_running BOOLEAN;
                    END IF;
                END $$;
                """
            )
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_webapp_group_contexts (
                    user_id BIGINT NOT NULL,
                    chat_id BIGINT NOT NULL,
                    chat_type TEXT NOT NULL DEFAULT 'group',
                    chat_title TEXT,
                    participation_confirmed BOOLEAN NOT NULL DEFAULT FALSE,
                    participation_confirmed_at TIMESTAMPTZ,
                    participation_confirmed_source TEXT,
                    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (user_id, chat_id),
                    CHECK (chat_type IN ('group', 'supergroup'))
                );
            """)
            cursor.execute("""
                ALTER TABLE bt_3_webapp_group_contexts
                ADD COLUMN IF NOT EXISTS participation_confirmed BOOLEAN NOT NULL DEFAULT FALSE;
            """)
            cursor.execute("""
                ALTER TABLE bt_3_webapp_group_contexts
                ADD COLUMN IF NOT EXISTS participation_confirmed_at TIMESTAMPTZ;
            """)
            cursor.execute("""
                ALTER TABLE bt_3_webapp_group_contexts
                ADD COLUMN IF NOT EXISTS participation_confirmed_source TEXT;
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_webapp_group_contexts_user_seen
                ON bt_3_webapp_group_contexts (user_id, last_seen_at DESC);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_webapp_group_contexts_chat_confirmed
                ON bt_3_webapp_group_contexts (chat_id, participation_confirmed, last_seen_at DESC);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_webapp_instance_leases (
                    user_id BIGINT PRIMARY KEY,
                    instance_id TEXT NOT NULL,
                    session_id TEXT,
                    platform TEXT,
                    app_context TEXT,
                    claimed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_webapp_instance_leases_seen
                ON bt_3_webapp_instance_leases (last_seen_at DESC);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_user_api_snapshots (
                    user_id BIGINT NOT NULL,
                    snapshot_kind TEXT NOT NULL,
                    snapshot_key TEXT NOT NULL,
                    source_lang TEXT,
                    target_lang TEXT,
                    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
                    meta JSONB NOT NULL DEFAULT '{}'::jsonb,
                    refreshed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    fresh_until_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    stale_until_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (user_id, snapshot_kind, snapshot_key)
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_user_api_snapshots_kind_fresh
                ON bt_3_user_api_snapshots (snapshot_kind, fresh_until_at DESC, updated_at DESC);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_user_api_snapshots_user_updated
                ON bt_3_user_api_snapshots (user_id, updated_at DESC);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_webapp_checks (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    username TEXT,
                    session_id TEXT,
                    original_text TEXT NOT NULL,
                    user_translation TEXT NOT NULL,
                    result TEXT NOT NULL,
                    source_lang TEXT,
                    target_lang TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            cursor.execute("""
                ALTER TABLE bt_3_webapp_checks
                ADD COLUMN IF NOT EXISTS source_lang TEXT;
            """)
            cursor.execute("""
                ALTER TABLE bt_3_webapp_checks
                ADD COLUMN IF NOT EXISTS target_lang TEXT;
            """)
            cursor.execute("CREATE SEQUENCE IF NOT EXISTS bt_3_webapp_checks_id_seq;")
            cursor.execute("""
                SELECT setval(
                    'bt_3_webapp_checks_id_seq',
                    COALESCE((SELECT MAX(id) FROM bt_3_webapp_checks), 1),
                    true
                );
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_translation_check_sessions (
                    id BIGSERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    username TEXT,
                    source_session_id TEXT,
                    source_lang TEXT,
                    target_lang TEXT,
                    status TEXT NOT NULL DEFAULT 'queued',
                    total_items INT NOT NULL DEFAULT 0,
                    completed_items INT NOT NULL DEFAULT 0,
                    failed_items INT NOT NULL DEFAULT 0,
                    send_private_grammar_text BOOLEAN NOT NULL DEFAULT FALSE,
                    original_text_bundle TEXT,
                    user_translation_bundle TEXT,
                    last_error TEXT,
                    dispatched_job_id TEXT,
                    dispatched_at TIMESTAMPTZ,
                    worker_job_id TEXT,
                    heartbeat_at TIMESTAMPTZ,
                    started_at TIMESTAMPTZ,
                    finished_at TIMESTAMPTZ,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    CHECK (status IN ('queued', 'running', 'done', 'failed', 'canceled')),
                    CHECK (total_items >= 0),
                    CHECK (completed_items >= 0),
                    CHECK (failed_items >= 0)
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_translation_check_sessions_user_created
                ON bt_3_translation_check_sessions (user_id, created_at DESC);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_translation_check_sessions_status_created
                ON bt_3_translation_check_sessions (status, created_at DESC);
            """)
            cursor.execute("""
                ALTER TABLE bt_3_translation_check_sessions
                ADD COLUMN IF NOT EXISTS dispatched_job_id TEXT;
            """)
            cursor.execute("""
                ALTER TABLE bt_3_translation_check_sessions
                ADD COLUMN IF NOT EXISTS dispatched_at TIMESTAMPTZ;
            """)
            cursor.execute("""
                ALTER TABLE bt_3_translation_check_sessions
                ADD COLUMN IF NOT EXISTS worker_job_id TEXT;
            """)
            cursor.execute("""
                ALTER TABLE bt_3_translation_check_sessions
                ADD COLUMN IF NOT EXISTS heartbeat_at TIMESTAMPTZ;
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_translation_check_sessions_status_heartbeat
                ON bt_3_translation_check_sessions (status, heartbeat_at ASC NULLS FIRST, created_at ASC);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_translation_check_items (
                    id BIGSERIAL PRIMARY KEY,
                    check_session_id BIGINT NOT NULL REFERENCES bt_3_translation_check_sessions(id) ON DELETE CASCADE,
                    item_order INT NOT NULL DEFAULT 0,
                    sentence_number INT,
                    sentence_id_for_mistake_table BIGINT,
                    original_text TEXT NOT NULL,
                    user_translation TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    result_json JSONB,
                    result_text TEXT,
                    error_text TEXT,
                    webapp_check_id BIGINT,
                    started_at TIMESTAMPTZ,
                    finished_at TIMESTAMPTZ,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    CHECK (status IN ('pending', 'running', 'done', 'failed'))
                );
            """)
            cursor.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS uq_bt_3_translation_check_items_session_order
                ON bt_3_translation_check_items (check_session_id, item_order);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_translation_check_items_status
                ON bt_3_translation_check_items (check_session_id, status, item_order);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_webapp_dictionary_queries (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    word_ru TEXT,
                    translation_de TEXT,
                    word_de TEXT,
                    translation_ru TEXT,
                    source_lang TEXT,
                    target_lang TEXT,
                    origin_process TEXT NOT NULL DEFAULT 'unknown',
                    origin_meta JSONB,
                    response_json JSONB,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            cursor.execute("""
                ALTER TABLE bt_3_webapp_dictionary_queries
                ADD COLUMN IF NOT EXISTS folder_id BIGINT;
            """)
            cursor.execute("""
                ALTER TABLE bt_3_webapp_dictionary_queries
                ALTER COLUMN word_ru DROP NOT NULL;
            """)
            cursor.execute("""
                ALTER TABLE bt_3_webapp_dictionary_queries
                ADD COLUMN IF NOT EXISTS word_de TEXT;
            """)
            cursor.execute("""
                ALTER TABLE bt_3_webapp_dictionary_queries
                ADD COLUMN IF NOT EXISTS translation_ru TEXT;
            """)
            cursor.execute("""
                ALTER TABLE bt_3_webapp_dictionary_queries
                ADD COLUMN IF NOT EXISTS source_lang TEXT;
            """)
            cursor.execute("""
                ALTER TABLE bt_3_webapp_dictionary_queries
                ADD COLUMN IF NOT EXISTS target_lang TEXT;
            """)
            cursor.execute("""
                ALTER TABLE bt_3_webapp_dictionary_queries
                ADD COLUMN IF NOT EXISTS origin_process TEXT;
            """)
            cursor.execute("""
                ALTER TABLE bt_3_webapp_dictionary_queries
                ADD COLUMN IF NOT EXISTS origin_meta JSONB;
            """)
            cursor.execute("""
                ALTER TABLE bt_3_webapp_dictionary_queries
                ALTER COLUMN origin_process SET DEFAULT 'unknown';
            """)
            cursor.execute("""
                UPDATE bt_3_webapp_dictionary_queries
                SET origin_process = 'unknown_legacy'
                WHERE origin_process IS NULL OR origin_process = '';
            """)
            cursor.execute("""
                ALTER TABLE bt_3_webapp_dictionary_queries
                ALTER COLUMN origin_process SET NOT NULL;
            """)
            cursor.execute("""
                ALTER TABLE bt_3_webapp_dictionary_queries
                ADD COLUMN IF NOT EXISTS is_learned BOOLEAN NOT NULL DEFAULT FALSE;
            """)
            cursor.execute("""
                ALTER TABLE bt_3_webapp_dictionary_queries
                ADD COLUMN IF NOT EXISTS canonical_entry_id BIGINT;
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_dictionary_entries (
                    id BIGSERIAL PRIMARY KEY,
                    source_lang TEXT NOT NULL,
                    target_lang TEXT NOT NULL,
                    source_text TEXT NOT NULL,
                    target_text TEXT NOT NULL,
                    source_text_norm TEXT NOT NULL,
                    target_text_norm TEXT NOT NULL,
                    word_ru TEXT,
                    translation_de TEXT,
                    word_de TEXT,
                    translation_ru TEXT,
                    response_json JSONB,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
            """)
            cursor.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS uq_bt_3_dictionary_entries_pair_text
                ON bt_3_dictionary_entries (source_lang, target_lang, source_text_norm, target_text_norm);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_dictionary_entries_pair_updated
                ON bt_3_dictionary_entries (source_lang, target_lang, updated_at DESC);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_webapp_dictionary_queries_canonical
                ON bt_3_webapp_dictionary_queries (canonical_entry_id);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_dictionary_folders (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    name TEXT NOT NULL,
                    color TEXT NOT NULL,
                    icon TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_dictionary_folders_user
                ON bt_3_dictionary_folders (user_id);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_webapp_dictionary_queries_user_folder
                ON bt_3_webapp_dictionary_queries (user_id, folder_id);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_webapp_dictionary_queries_user_pair_created
                ON bt_3_webapp_dictionary_queries (user_id, source_lang, target_lang, created_at DESC);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_webapp_dictionary_queries_user_origin_created
                ON bt_3_webapp_dictionary_queries (user_id, origin_process, created_at DESC);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_starter_dictionary_state (
                    user_id BIGINT PRIMARY KEY,
                    decision_status TEXT NOT NULL DEFAULT 'pending',
                    source_user_id BIGINT,
                    template_version TEXT,
                    source_lang TEXT,
                    target_lang TEXT,
                    last_imported_count INT NOT NULL DEFAULT 0,
                    decided_at TIMESTAMPTZ,
                    last_imported_at TIMESTAMPTZ,
                    import_status TEXT NOT NULL DEFAULT 'idle',
                    active_job_id TEXT,
                    last_error TEXT,
                    import_started_at TIMESTAMPTZ,
                    import_finished_at TIMESTAMPTZ,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    CHECK (decision_status IN ('pending', 'accepted', 'declined')),
                    CHECK (import_status IN ('idle', 'running', 'done', 'failed'))
                );
            """)
            cursor.execute("""
                ALTER TABLE bt_3_starter_dictionary_state
                ADD COLUMN IF NOT EXISTS import_status TEXT NOT NULL DEFAULT 'idle';
            """)
            cursor.execute("""
                ALTER TABLE bt_3_starter_dictionary_state
                ADD COLUMN IF NOT EXISTS active_job_id TEXT;
            """)
            cursor.execute("""
                ALTER TABLE bt_3_starter_dictionary_state
                ADD COLUMN IF NOT EXISTS last_error TEXT;
            """)
            cursor.execute("""
                ALTER TABLE bt_3_starter_dictionary_state
                ADD COLUMN IF NOT EXISTS import_started_at TIMESTAMPTZ;
            """)
            cursor.execute("""
                ALTER TABLE bt_3_starter_dictionary_state
                ADD COLUMN IF NOT EXISTS import_finished_at TIMESTAMPTZ;
            """)
            cursor.execute("""
                UPDATE bt_3_starter_dictionary_state
                SET import_status = 'idle'
                WHERE import_status IS NULL OR import_status = '';
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_starter_dictionary_state_updated
                ON bt_3_starter_dictionary_state (updated_at DESC);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_schema_migrations (
                    migration_key TEXT PRIMARY KEY,
                    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_skills (
                    skill_id TEXT PRIMARY KEY,
                    language_code TEXT NOT NULL DEFAULT 'de',
                    title TEXT NOT NULL,
                    category TEXT NOT NULL,
                    is_active BOOLEAN NOT NULL DEFAULT TRUE,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
            """)
            cursor.execute("""
                ALTER TABLE bt_3_skills
                ADD COLUMN IF NOT EXISTS language_code TEXT;
            """)
            cursor.execute("""
                UPDATE bt_3_skills
                SET language_code = COALESCE(NULLIF(language_code, ''), 'de')
                WHERE language_code IS NULL OR language_code = '';
            """)
            cursor.execute("""
                ALTER TABLE bt_3_skills
                ALTER COLUMN language_code SET DEFAULT 'de';
            """)
            cursor.execute("""
                ALTER TABLE bt_3_skills
                ALTER COLUMN language_code SET NOT NULL;
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_skills_category
                ON bt_3_skills (language_code, category, skill_id);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_skill_mastery_groups (
                    mastery_group_id TEXT PRIMARY KEY,
                    language_code TEXT NOT NULL,
                    display_title TEXT NOT NULL,
                    short_description TEXT NOT NULL,
                    sort_order SMALLINT NOT NULL DEFAULT 100,
                    is_active BOOLEAN NOT NULL DEFAULT TRUE,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_skill_mastery_groups_lang_active
                ON bt_3_skill_mastery_groups (language_code, is_active, sort_order, mastery_group_id);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_skill_mastery_group_members (
                    mastery_group_id TEXT NOT NULL REFERENCES bt_3_skill_mastery_groups(mastery_group_id) ON DELETE CASCADE,
                    diagnostic_skill_id TEXT NOT NULL REFERENCES bt_3_skills(skill_id) ON DELETE CASCADE,
                    language_code TEXT NOT NULL,
                    is_mastery_leaf BOOLEAN NOT NULL DEFAULT FALSE,
                    is_diagnostic_only BOOLEAN NOT NULL DEFAULT FALSE,
                    rollup_weight DOUBLE PRECISION NOT NULL DEFAULT 1.0,
                    display_title_override TEXT,
                    sort_order SMALLINT NOT NULL DEFAULT 100,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (mastery_group_id, diagnostic_skill_id),
                    CHECK (rollup_weight >= 0.0 AND rollup_weight <= 1.0),
                    CHECK (NOT (is_mastery_leaf AND is_diagnostic_only))
                );
            """)
            cursor.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS uq_bt_3_skill_mastery_group_members_skill
                ON bt_3_skill_mastery_group_members (language_code, diagnostic_skill_id);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_skill_mastery_group_members_group
                ON bt_3_skill_mastery_group_members (language_code, mastery_group_id, is_mastery_leaf, sort_order);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_error_skill_map (
                    id BIGSERIAL PRIMARY KEY,
                    language_code TEXT NOT NULL DEFAULT 'de',
                    error_category TEXT NOT NULL,
                    error_subcategory TEXT NOT NULL,
                    skill_id TEXT NOT NULL REFERENCES bt_3_skills(skill_id) ON DELETE CASCADE,
                    weight DOUBLE PRECISION NOT NULL DEFAULT 1.0,
                    notes TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (error_category, error_subcategory, skill_id)
                );
            """)
            cursor.execute("""
                ALTER TABLE bt_3_error_skill_map
                ADD COLUMN IF NOT EXISTS language_code TEXT;
            """)
            cursor.execute("""
                UPDATE bt_3_error_skill_map
                SET language_code = COALESCE(NULLIF(language_code, ''), 'de')
                WHERE language_code IS NULL OR language_code = '';
            """)
            cursor.execute("""
                ALTER TABLE bt_3_error_skill_map
                ALTER COLUMN language_code SET DEFAULT 'de';
            """)
            cursor.execute("""
                ALTER TABLE bt_3_error_skill_map
                ALTER COLUMN language_code SET NOT NULL;
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_error_skill_map_err
                ON bt_3_error_skill_map (language_code, error_category, error_subcategory);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_error_skill_map_skill
                ON bt_3_error_skill_map (language_code, skill_id);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_user_skill_state (
                    user_id BIGINT NOT NULL,
                    skill_id TEXT NOT NULL REFERENCES bt_3_skills(skill_id) ON DELETE CASCADE,
                    source_lang TEXT NOT NULL DEFAULT 'ru',
                    target_lang TEXT NOT NULL DEFAULT 'de',
                    mastery DOUBLE PRECISION NOT NULL DEFAULT 50.0,
                    success_streak INTEGER NOT NULL DEFAULT 0,
                    fail_streak INTEGER NOT NULL DEFAULT 0,
                    total_events INTEGER NOT NULL DEFAULT 0,
                    last_event_delta DOUBLE PRECISION NOT NULL DEFAULT 0.0,
                    last_event_at TIMESTAMPTZ,
                    last_practiced_at TIMESTAMPTZ,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (user_id, skill_id, source_lang, target_lang)
                );
            """)
            cursor.execute("""
                ALTER TABLE bt_3_user_skill_state
                ADD COLUMN IF NOT EXISTS source_lang TEXT;
            """)
            cursor.execute("""
                ALTER TABLE bt_3_user_skill_state
                ADD COLUMN IF NOT EXISTS target_lang TEXT;
            """)
            cursor.execute("""
                UPDATE bt_3_user_skill_state
                SET
                    source_lang = COALESCE(NULLIF(source_lang, ''), 'ru'),
                    target_lang = COALESCE(NULLIF(target_lang, ''), 'de')
                WHERE source_lang IS NULL
                   OR target_lang IS NULL
                   OR source_lang = ''
                   OR target_lang = '';
            """)
            cursor.execute("""
                ALTER TABLE bt_3_user_skill_state
                ALTER COLUMN source_lang SET DEFAULT 'ru';
            """)
            cursor.execute("""
                ALTER TABLE bt_3_user_skill_state
                ALTER COLUMN target_lang SET DEFAULT 'de';
            """)
            cursor.execute("""
                ALTER TABLE bt_3_user_skill_state
                ALTER COLUMN source_lang SET NOT NULL;
            """)
            cursor.execute("""
                ALTER TABLE bt_3_user_skill_state
                ALTER COLUMN target_lang SET NOT NULL;
            """)
            cursor.execute(
                """
                DO $$
                BEGIN
                    BEGIN
                        ALTER TABLE bt_3_user_skill_state
                        DROP CONSTRAINT IF EXISTS bt_3_user_skill_state_pkey;
                    EXCEPTION WHEN undefined_object THEN
                        NULL;
                    END;
                    BEGIN
                        ALTER TABLE bt_3_user_skill_state
                        ADD CONSTRAINT bt_3_user_skill_state_pkey
                        PRIMARY KEY (user_id, skill_id, source_lang, target_lang);
                    EXCEPTION
                        WHEN duplicate_object THEN NULL;
                    END;
                END $$;
                """
            )
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_sentence_skill_targets (
                    id BIGSERIAL PRIMARY KEY,
                    sentence_id BIGINT NOT NULL REFERENCES bt_3_daily_sentences(id) ON DELETE CASCADE,
                    skill_id TEXT NOT NULL REFERENCES bt_3_skills(skill_id) ON DELETE CASCADE,
                    role VARCHAR(16) NOT NULL,
                    role_rank SMALLINT NOT NULL,
                    role_weight DOUBLE PRECISION NOT NULL,
                    profile_source VARCHAR(32) NOT NULL,
                    profile_confidence DOUBLE PRECISION NOT NULL,
                    profile_version SMALLINT NOT NULL DEFAULT 1,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    CHECK (role IN ('primary', 'secondary', 'supporting')),
                    UNIQUE (sentence_id, skill_id)
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_sentence_skill_targets_sentence_role
                ON bt_3_sentence_skill_targets (sentence_id, role_rank, skill_id);
            """)
            cursor.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS uq_bt_3_sentence_skill_targets_primary
                ON bt_3_sentence_skill_targets (sentence_id)
                WHERE role = 'primary';
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_sentence_skill_shadow_state_v2 (
                    sentence_id BIGINT PRIMARY KEY REFERENCES bt_3_daily_sentences(id) ON DELETE CASCADE,
                    user_id BIGINT NOT NULL,
                    source_lang VARCHAR(8) NOT NULL,
                    target_lang VARCHAR(8) NOT NULL,
                    last_attempt_no INT NOT NULL,
                    last_score SMALLINT NOT NULL,
                    last_score_band VARCHAR(16) NOT NULL,
                    last_retention_state VARCHAR(24) NOT NULL,
                    last_tested_skill_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
                    last_errored_skill_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
                    last_profile_source VARCHAR(32) NOT NULL,
                    last_profile_confidence DOUBLE PRECISION NOT NULL,
                    last_checked_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_sentence_skill_shadow_state_v2_user
                ON bt_3_sentence_skill_shadow_state_v2 (user_id, last_checked_at DESC);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_skill_events_v2 (
                    id BIGSERIAL PRIMARY KEY,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    user_id BIGINT NOT NULL,
                    sentence_id BIGINT NOT NULL REFERENCES bt_3_daily_sentences(id) ON DELETE CASCADE,
                    session_id BIGINT,
                    source_lang VARCHAR(8) NOT NULL,
                    target_lang VARCHAR(8) NOT NULL,
                    attempt_no INT NOT NULL,
                    overall_score SMALLINT NOT NULL,
                    score_band VARCHAR(16) NOT NULL,
                    retention_state VARCHAR(24) NOT NULL,
                    was_in_error_bank BOOLEAN NOT NULL,
                    tested_profile_available BOOLEAN NOT NULL,
                    skill_id TEXT NOT NULL,
                    skill_role VARCHAR(24) NOT NULL,
                    role_weight DOUBLE PRECISION NOT NULL,
                    is_tested BOOLEAN NOT NULL,
                    is_errored_now BOOLEAN NOT NULL,
                    was_errored_prev BOOLEAN NOT NULL,
                    per_skill_outcome VARCHAR(32) NOT NULL,
                    sentence_progress_kind VARCHAR(32) NOT NULL,
                    recovery_kind VARCHAR(16) NOT NULL,
                    profile_source VARCHAR(32) NOT NULL,
                    profile_confidence DOUBLE PRECISION NOT NULL,
                    profile_version SMALLINT NOT NULL DEFAULT 1,
                    map_weight DOUBLE PRECISION,
                    error_pairs_json JSONB,
                    shadow_delta_signal DOUBLE PRECISION NOT NULL,
                    metadata_json JSONB
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_skill_events_v2_user_created
                ON bt_3_skill_events_v2 (user_id, created_at DESC);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_skill_events_v2_user_skill_created
                ON bt_3_skill_events_v2 (user_id, skill_id, created_at DESC);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_skill_events_v2_sentence_attempt
                ON bt_3_skill_events_v2 (sentence_id, attempt_no, created_at DESC);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_skill_events_v2_skill_incremental
                ON bt_3_skill_events_v2 (user_id, skill_id, source_lang, target_lang, id ASC);
            """)
            cursor.execute(
                """
                INSERT INTO bt_3_schema_migrations (migration_key)
                VALUES (%s)
                ON CONFLICT (migration_key) DO NOTHING;
                """,
                (PHASE1_SHADOW_SCHEMA_MIGRATION_KEY,),
            )
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_user_skill_state_v2 (
                    user_id BIGINT NOT NULL,
                    skill_id TEXT NOT NULL REFERENCES bt_3_skills(skill_id) ON DELETE CASCADE,
                    source_lang VARCHAR(8) NOT NULL,
                    target_lang VARCHAR(8) NOT NULL,
                    mastery DOUBLE PRECISION NOT NULL DEFAULT 50.0,
                    confidence DOUBLE PRECISION NOT NULL DEFAULT 0.0,
                    net_evidence_recent DOUBLE PRECISION NOT NULL DEFAULT 0.0,
                    net_evidence_lifetime DOUBLE PRECISION NOT NULL DEFAULT 0.0,
                    tested_events INT NOT NULL DEFAULT 0,
                    fail_events INT NOT NULL DEFAULT 0,
                    success_events INT NOT NULL DEFAULT 0,
                    recovery_events INT NOT NULL DEFAULT 0,
                    last_event_id BIGINT,
                    last_event_at TIMESTAMPTZ,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (user_id, skill_id, source_lang, target_lang)
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_user_skill_state_v2_user_mastery
                ON bt_3_user_skill_state_v2 (user_id, source_lang, target_lang, mastery ASC);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_skill_state_v2_dirty (
                    user_id BIGINT NOT NULL,
                    skill_id TEXT NOT NULL REFERENCES bt_3_skills(skill_id) ON DELETE CASCADE,
                    source_lang VARCHAR(8) NOT NULL,
                    target_lang VARCHAR(8) NOT NULL,
                    max_event_id BIGINT NOT NULL,
                    lease_owner VARCHAR(64),
                    lease_expires_at TIMESTAMPTZ,
                    retry_count INT NOT NULL DEFAULT 0,
                    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    last_error TEXT,
                    enqueue_count INT NOT NULL DEFAULT 1,
                    first_enqueued_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (user_id, skill_id, source_lang, target_lang)
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_skill_state_v2_dirty_schedule
                ON bt_3_skill_state_v2_dirty (next_attempt_at, lease_expires_at, updated_at);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_skill_state_v2_dirty_lease
                ON bt_3_skill_state_v2_dirty (lease_expires_at);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_skill_state_v2_worker_stats (
                    worker_name VARCHAR(128) PRIMARY KEY,
                    runs_total BIGINT NOT NULL DEFAULT 0,
                    keys_processed_total BIGINT NOT NULL DEFAULT 0,
                    events_processed_total BIGINT NOT NULL DEFAULT 0,
                    last_run_at TIMESTAMPTZ,
                    last_success_at TIMESTAMPTZ,
                    last_duration_ms INT NOT NULL DEFAULT 0,
                    last_keys_processed INT NOT NULL DEFAULT 0,
                    last_events_processed INT NOT NULL DEFAULT 0,
                    last_error TEXT,
                    errors_total BIGINT NOT NULL DEFAULT 0,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_skill_state_v2_worker_stats_updated
                ON bt_3_skill_state_v2_worker_stats (updated_at DESC);
            """)
            cursor.execute(
                """
                INSERT INTO bt_3_schema_migrations (migration_key)
                VALUES (%s)
                ON CONFLICT (migration_key) DO NOTHING;
                """,
                (PHASE2_SHADOW_SCHEMA_MIGRATION_KEY,),
            )
            # Repair incomplete language metadata for dictionary rows inserted by legacy flows.
            cursor.execute(
                """
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1
                        FROM bt_3_schema_migrations
                        WHERE migration_key = '2026_02_25_dictionary_lang_metadata_repair'
                    ) THEN
                        UPDATE bt_3_webapp_dictionary_queries
                        SET
                            source_lang = COALESCE(
                                NULLIF(source_lang, ''),
                                NULLIF(response_json->>'source_lang', ''),
                                NULLIF(response_json#>>'{language_pair,source_lang}', ''),
                                'ru'
                            ),
                            target_lang = COALESCE(
                                NULLIF(target_lang, ''),
                                NULLIF(response_json->>'target_lang', ''),
                                NULLIF(response_json#>>'{language_pair,target_lang}', ''),
                                'de'
                            )
                        WHERE source_lang IS NULL
                           OR target_lang IS NULL
                           OR source_lang = ''
                           OR target_lang = '';

                        UPDATE bt_3_webapp_dictionary_queries
                        SET response_json = jsonb_set(
                            jsonb_set(
                                COALESCE(response_json, '{}'::jsonb),
                                '{source_lang}',
                                to_jsonb(COALESCE(NULLIF(source_lang, ''), 'ru')::text),
                                true
                            ),
                            '{target_lang}',
                            to_jsonb(COALESCE(NULLIF(target_lang, ''), 'de')::text),
                            true
                        )
                        WHERE response_json IS NULL
                           OR COALESCE(response_json->>'source_lang', '') = ''
                           OR COALESCE(response_json->>'target_lang', '') = '';

                        UPDATE bt_3_webapp_dictionary_queries
                        SET response_json = jsonb_set(
                            jsonb_set(
                                COALESCE(response_json, '{}'::jsonb),
                                '{language_pair,source_lang}',
                                to_jsonb(COALESCE(NULLIF(source_lang, ''), 'ru')::text),
                                true
                            ),
                            '{language_pair,target_lang}',
                            to_jsonb(COALESCE(NULLIF(target_lang, ''), 'de')::text),
                            true
                        )
                        WHERE response_json IS NULL
                           OR COALESCE(response_json#>>'{language_pair,source_lang}', '') = ''
                           OR COALESCE(response_json#>>'{language_pair,target_lang}', '') = '';

                        INSERT INTO bt_3_schema_migrations (migration_key)
                        VALUES ('2026_02_25_dictionary_lang_metadata_repair')
                        ON CONFLICT (migration_key) DO NOTHING;
                    END IF;
                END $$;
                """
            )
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_user_skill_state_user_mastery
                ON bt_3_user_skill_state (user_id, source_lang, target_lang, mastery ASC);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_user_skill_state_skill
                ON bt_3_user_skill_state (skill_id, source_lang, target_lang, mastery ASC);
            """)
            cursor.executemany(
                """
                INSERT INTO bt_3_skills (skill_id, title, category, language_code, is_active, updated_at)
                VALUES (%s, %s, %s, %s, TRUE, NOW())
                ON CONFLICT (skill_id) DO UPDATE
                SET
                    title = EXCLUDED.title,
                    category = EXCLUDED.category,
                    language_code = EXCLUDED.language_code,
                    updated_at = NOW();
                """,
                SKILL_SEED,
            )
            cursor.executemany(
                """
                INSERT INTO bt_3_skill_mastery_groups (
                    mastery_group_id,
                    language_code,
                    display_title,
                    short_description,
                    sort_order,
                    is_active,
                    updated_at
                )
                VALUES (%s, 'de', %s, %s, %s, TRUE, NOW())
                ON CONFLICT (mastery_group_id) DO UPDATE
                SET
                    language_code = EXCLUDED.language_code,
                    display_title = EXCLUDED.display_title,
                    short_description = EXCLUDED.short_description,
                    sort_order = EXCLUDED.sort_order,
                    is_active = TRUE,
                    updated_at = NOW();
                """,
                GERMAN_MASTERY_GROUP_SEED,
            )
            cursor.executemany(
                """
                INSERT INTO bt_3_skill_mastery_group_members (
                    mastery_group_id,
                    diagnostic_skill_id,
                    language_code,
                    is_mastery_leaf,
                    is_diagnostic_only,
                    rollup_weight,
                    display_title_override,
                    sort_order,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (mastery_group_id, diagnostic_skill_id) DO UPDATE
                SET
                    language_code = EXCLUDED.language_code,
                    is_mastery_leaf = EXCLUDED.is_mastery_leaf,
                    is_diagnostic_only = EXCLUDED.is_diagnostic_only,
                    rollup_weight = EXCLUDED.rollup_weight,
                    display_title_override = EXCLUDED.display_title_override,
                    sort_order = EXCLUDED.sort_order,
                    updated_at = NOW();
                """,
                GERMAN_MASTERY_GROUP_MEMBER_SEED,
            )
            german_mastery_group_ids = [
                group_id
                for group_id, _title, _description, _sort_order in GERMAN_MASTERY_GROUP_SEED
            ]
            german_mastery_skill_ids = [
                skill_id
                for _group_id, skill_id, _lang, _is_leaf, _is_diag_only, _weight, _override, _sort
                in GERMAN_MASTERY_GROUP_MEMBER_SEED
            ]
            cursor.execute(
                """
                DELETE FROM bt_3_skill_mastery_group_members
                WHERE language_code = 'de'
                  AND diagnostic_skill_id IN (
                      SELECT skill_id
                      FROM bt_3_skills
                      WHERE language_code = 'de'
                  )
                  AND NOT (diagnostic_skill_id = ANY(%s::text[]));
                """,
                (german_mastery_skill_ids,),
            )
            cursor.execute(
                """
                UPDATE bt_3_skill_mastery_groups
                SET is_active = (mastery_group_id = ANY(%s::text[])), updated_at = NOW()
                WHERE language_code = 'de';
                """,
                (german_mastery_group_ids,),
            )
            cursor.execute(
                """
                INSERT INTO bt_3_schema_migrations (migration_key)
                VALUES (%s)
                ON CONFLICT (migration_key) DO NOTHING;
                """,
                (SKILL_MASTERY_GROUPS_SCHEMA_MIGRATION_KEY,),
            )
            cursor.executemany(
                """
                INSERT INTO bt_3_error_skill_map (
                    language_code,
                    error_category,
                    error_subcategory,
                    skill_id,
                    weight,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, NOW())
                ON CONFLICT (error_category, error_subcategory, skill_id) DO UPDATE
                SET
                    language_code = EXCLUDED.language_code,
                    weight = EXCLUDED.weight,
                    updated_at = NOW();
                """,
                ERROR_SKILL_MAP_SEED,
            )
            # Keep error->skill map aligned with current seeds for all supported learning languages.
            valid_pairs_by_lang: dict[str, set[tuple[str, str]]] = {}
            for lang_code, cat, subcat, _skill_id, _weight in ERROR_SKILL_MAP_SEED:
                normalized_lang = str(lang_code or "").strip().lower()
                if not normalized_lang:
                    continue
                valid_pairs_by_lang.setdefault(normalized_lang, set()).add((str(cat), str(subcat)))
            for lang_code, pair_set in valid_pairs_by_lang.items():
                valid_pairs = sorted(pair_set)
                if not valid_pairs:
                    continue
                pair_placeholders = ", ".join(["(%s, %s)"] * len(valid_pairs))
                pair_params: list[str] = [lang_code]
                for cat, subcat in valid_pairs:
                    pair_params.extend([str(cat), str(subcat)])
                cursor.execute(
                    f"""
                    DELETE FROM bt_3_error_skill_map
                    WHERE language_code = %s
                      AND (error_category, error_subcategory) NOT IN ({pair_placeholders});
                    """,
                    tuple(pair_params),
                )

            # Remove uncategorizable "Unclassified" entries from skill map and deactivate legacy unclassified skills.
            cursor.execute(
                """
                DELETE FROM bt_3_error_skill_map
                WHERE LOWER(COALESCE(error_subcategory, '')) IN ('unclassified mistake', 'unclassified mistakes');
                """
            )
            cursor.execute(
                """
                UPDATE bt_3_skills
                SET is_active = FALSE, updated_at = NOW()
                WHERE LOWER(COALESCE(skill_id, '')) IN ('other_unclassified', 'en_other_unclassified', 'es_other_unclassified', 'it_other_unclassified')
                   OR LOWER(COALESCE(skill_id, '')) LIKE '%unclassified%'
                   OR LOWER(COALESCE(title, '')) LIKE '%unclassified%';
                """
            )
            # One-time backfill for legacy imported dictionary rows (pre-multilang).
            # Must run only once, not on every startup.
            cursor.execute(
                """
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1
                        FROM bt_3_schema_migrations
                        WHERE migration_key = '2026_02_19_legacy_dictionary_lang_backfill_once'
                    ) THEN
                        UPDATE bt_3_webapp_dictionary_queries
                        SET
                            source_lang = COALESCE(NULLIF(source_lang, ''), 'ru'),
                            target_lang = COALESCE(NULLIF(target_lang, ''), 'de')
                        WHERE (source_lang IS NULL OR source_lang = '')
                          AND (target_lang IS NULL OR target_lang = '');

                        UPDATE bt_3_webapp_dictionary_queries
                        SET response_json = jsonb_set(
                            jsonb_set(
                                COALESCE(response_json, '{}'::jsonb),
                                '{source_lang}',
                                to_jsonb(COALESCE(NULLIF(source_lang, ''), 'ru')::text),
                                true
                            ),
                            '{target_lang}',
                            to_jsonb(COALESCE(NULLIF(target_lang, ''), 'de')::text),
                            true
                        )
                        WHERE response_json IS NULL
                           OR COALESCE(response_json->>'source_lang', '') = ''
                           OR COALESCE(response_json->>'target_lang', '') = '';

                        UPDATE bt_3_webapp_dictionary_queries
                        SET response_json = jsonb_set(
                            jsonb_set(
                                COALESCE(response_json, '{}'::jsonb),
                                '{language_pair,source_lang}',
                                to_jsonb(COALESCE(NULLIF(source_lang, ''), 'ru')::text),
                                true
                            ),
                            '{language_pair,target_lang}',
                            to_jsonb(COALESCE(NULLIF(target_lang, ''), 'de')::text),
                            true
                        )
                        WHERE response_json IS NULL
                           OR COALESCE(response_json#>>'{language_pair,source_lang}', '') = ''
                           OR COALESCE(response_json#>>'{language_pair,target_lang}', '') = '';

                        INSERT INTO bt_3_schema_migrations (migration_key)
                        VALUES ('2026_02_19_legacy_dictionary_lang_backfill_once')
                        ON CONFLICT (migration_key) DO NOTHING;
                    END IF;
                END $$;
                """
            )
            # One-time repair for DE->RU dictionary rows created via private bot saves
            # where legacy RU/DE columns could be swapped.
            cursor.execute(
                """
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1
                        FROM bt_3_schema_migrations
                        WHERE migration_key = '2026_02_23_dictionary_de_ru_legacy_fix_once'
                    ) THEN
                        WITH fixed AS (
                            SELECT
                                id,
                                COALESCE(
                                    NULLIF(response_json->>'word_source', ''),
                                    NULLIF(response_json->>'source_text', ''),
                                    NULLIF(word_de, ''),
                                    NULLIF(translation_de, ''),
                                    NULLIF(word_ru, '')
                                ) AS fixed_word_de,
                                COALESCE(
                                    NULLIF(response_json->>'word_target', ''),
                                    NULLIF(response_json->>'target_text', ''),
                                    NULLIF(translation_ru, ''),
                                    NULLIF(word_ru, ''),
                                    NULLIF(translation_de, '')
                                ) AS fixed_translation_ru
                            FROM bt_3_webapp_dictionary_queries
                            WHERE COALESCE(source_lang, '') = 'de'
                              AND COALESCE(target_lang, '') = 'ru'
                        )
                        UPDATE bt_3_webapp_dictionary_queries q
                        SET
                            word_de = COALESCE(f.fixed_word_de, q.word_de),
                            translation_ru = COALESCE(f.fixed_translation_ru, q.translation_ru),
                            word_ru = COALESCE(f.fixed_translation_ru, q.word_ru),
                            translation_de = COALESCE(f.fixed_word_de, q.translation_de),
                            response_json = jsonb_set(
                                jsonb_set(
                                    jsonb_set(
                                        jsonb_set(
                                            COALESCE(q.response_json, '{}'::jsonb),
                                            '{word_de}',
                                            to_jsonb(COALESCE(f.fixed_word_de, q.word_de, '')::text),
                                            true
                                        ),
                                        '{translation_ru}',
                                        to_jsonb(COALESCE(f.fixed_translation_ru, q.translation_ru, '')::text),
                                        true
                                    ),
                                    '{word_ru}',
                                    to_jsonb(COALESCE(f.fixed_translation_ru, q.word_ru, '')::text),
                                    true
                                ),
                                '{translation_de}',
                                to_jsonb(COALESCE(f.fixed_word_de, q.translation_de, '')::text),
                                true
                            )
                        FROM fixed f
                        WHERE q.id = f.id;

                        INSERT INTO bt_3_schema_migrations (migration_key)
                        VALUES ('2026_02_23_dictionary_de_ru_legacy_fix_once')
                        ON CONFLICT (migration_key) DO NOTHING;
                    END IF;
                END $$;
                """
            )
            cursor.execute(
                """
                DO $$
                BEGIN
                    IF to_regclass('public.bt_3_translations') IS NOT NULL THEN
                        ALTER TABLE bt_3_translations ADD COLUMN IF NOT EXISTS source_lang TEXT;
                        ALTER TABLE bt_3_translations ADD COLUMN IF NOT EXISTS target_lang TEXT;
                        ALTER TABLE bt_3_translations ADD COLUMN IF NOT EXISTS audio_grammar_opt_in BOOLEAN DEFAULT FALSE;
                        UPDATE bt_3_translations
                        SET audio_grammar_opt_in = FALSE
                        WHERE audio_grammar_opt_in IS NULL;
                        ALTER TABLE bt_3_translations ALTER COLUMN audio_grammar_opt_in SET DEFAULT FALSE;
                        ALTER TABLE bt_3_translations ALTER COLUMN audio_grammar_opt_in SET NOT NULL;
                        DELETE FROM bt_3_translations older
                        USING bt_3_translations newer
                        WHERE older.user_id = newer.user_id
                          AND older.sentence_id = newer.sentence_id
                          AND older.session_id = newer.session_id
                          AND older.session_id IS NOT NULL
                          AND (
                              older.timestamp < newer.timestamp
                              OR (older.timestamp = newer.timestamp AND older.id < newer.id)
                          );
                        CREATE UNIQUE INDEX IF NOT EXISTS uq_bt_3_translations_user_sentence_session
                        ON bt_3_translations (user_id, sentence_id, session_id)
                        WHERE session_id IS NOT NULL;
                        CREATE INDEX IF NOT EXISTS idx_bt_3_translations_user_lang_ts
                        ON bt_3_translations (user_id, source_lang, target_lang, timestamp DESC);
                        CREATE INDEX IF NOT EXISTS idx_bt_3_translations_user_audio_grammar
                        ON bt_3_translations (user_id, audio_grammar_opt_in, timestamp DESC);
                    END IF;
                END $$;
                """
            )
            cursor.execute(
                """
                DO $$
                BEGIN
                    IF to_regclass('public.bt_3_daily_sentences') IS NOT NULL THEN
                        ALTER TABLE bt_3_daily_sentences ADD COLUMN IF NOT EXISTS source_lang TEXT;
                        ALTER TABLE bt_3_daily_sentences ADD COLUMN IF NOT EXISTS target_lang TEXT;
                        ALTER TABLE bt_3_daily_sentences ADD COLUMN IF NOT EXISTS focus_key TEXT;
                        ALTER TABLE bt_3_daily_sentences ADD COLUMN IF NOT EXISTS level TEXT;
                        ALTER TABLE bt_3_daily_sentences ADD COLUMN IF NOT EXISTS shown_to_user BOOLEAN;
                        ALTER TABLE bt_3_daily_sentences ADD COLUMN IF NOT EXISTS shown_to_user_at TIMESTAMPTZ;
                        UPDATE bt_3_daily_sentences
                        SET shown_to_user = FALSE
                        WHERE shown_to_user IS NULL;
                        ALTER TABLE bt_3_daily_sentences ALTER COLUMN shown_to_user SET DEFAULT FALSE;
                        ALTER TABLE bt_3_daily_sentences ALTER COLUMN shown_to_user SET NOT NULL;
                        CREATE INDEX IF NOT EXISTS idx_bt_3_daily_sentences_user_date_lang
                        ON bt_3_daily_sentences (user_id, date, source_lang, target_lang);
                        CREATE INDEX IF NOT EXISTS idx_bt_3_daily_sentences_user_session_shown
                        ON bt_3_daily_sentences (user_id, session_id, shown_to_user, unique_id);
                        CREATE INDEX IF NOT EXISTS idx_bt_3_daily_sentences_user_session_lang
                        ON bt_3_daily_sentences (user_id, session_id, source_lang, target_lang);
                        CREATE INDEX IF NOT EXISTS idx_bt_3_daily_sentences_focus_level_date
                        ON bt_3_daily_sentences (source_lang, target_lang, focus_key, level, date DESC);
                    END IF;
                END $$;
                """
            )
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_translation_bucket_daily_demand (
                    demand_date DATE NOT NULL,
                    source_lang TEXT NOT NULL,
                    target_lang TEXT NOT NULL,
                    focus_key TEXT NOT NULL,
                    level TEXT NOT NULL,
                    sessions_started BIGINT NOT NULL DEFAULT 0,
                    sentences_assigned BIGINT NOT NULL DEFAULT 0,
                    sentences_mastered BIGINT NOT NULL DEFAULT 0,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (demand_date, source_lang, target_lang, focus_key, level)
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_translation_bucket_daily_demand_lookup
                ON bt_3_translation_bucket_daily_demand (source_lang, target_lang, focus_key, level, demand_date DESC);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_quiz_history (
                    id SERIAL PRIMARY KEY,
                    word_ru TEXT NOT NULL,
                    asked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_quiz_history_word_time
                ON bt_3_quiz_history (word_ru, asked_at);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_telegram_quiz_delivery_history (
                    id SERIAL PRIMARY KEY,
                    chat_id BIGINT NOT NULL,
                    poll_id TEXT,
                    word_ru TEXT NOT NULL,
                    quiz_type TEXT,
                    delivery_mode TEXT NOT NULL DEFAULT 'new',
                    asked_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_tg_quiz_delivery_chat_time
                ON bt_3_telegram_quiz_delivery_history (chat_id, asked_at DESC);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_tg_quiz_delivery_chat_word
                ON bt_3_telegram_quiz_delivery_history (chat_id, word_ru, asked_at DESC);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_telegram_quiz_attempts (
                    id SERIAL PRIMARY KEY,
                    poll_id TEXT NOT NULL,
                    chat_id BIGINT NOT NULL,
                    user_id BIGINT NOT NULL,
                    word_ru TEXT,
                    quiz_type TEXT,
                    selected_option_index INTEGER,
                    selected_text TEXT,
                    is_correct BOOLEAN NOT NULL DEFAULT FALSE,
                    answered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (poll_id, user_id)
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_tg_quiz_attempts_chat_time
                ON bt_3_telegram_quiz_attempts (chat_id, answered_at DESC);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_tg_quiz_attempts_chat_word
                ON bt_3_telegram_quiz_attempts (chat_id, word_ru, answered_at DESC);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_telegram_quiz_delivery_state (
                    chat_id BIGINT PRIMARY KEY,
                    next_mode TEXT NOT NULL DEFAULT 'new',
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_prepared_telegram_quizzes (
                    id BIGSERIAL PRIMARY KEY,
                    quiz_type TEXT NOT NULL,
                    word_ru TEXT,
                    payload JSONB NOT NULL,
                    source_lang TEXT NOT NULL DEFAULT 'ru',
                    target_lang TEXT NOT NULL DEFAULT 'de',
                    prepared_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    last_used_at TIMESTAMPTZ,
                    use_count INTEGER NOT NULL DEFAULT 0
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_prepared_tg_quizzes_lookup
                ON bt_3_prepared_telegram_quizzes (
                    source_lang,
                    target_lang,
                    quiz_type,
                    last_used_at,
                    prepared_at DESC
                );
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_image_quiz_templates (
                    id BIGSERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    source_dictionary_entry_id BIGINT,
                    canonical_entry_id BIGINT,
                    source_lang TEXT NOT NULL,
                    target_lang TEXT NOT NULL,
                    source_text TEXT NOT NULL DEFAULT '',
                    target_text TEXT NOT NULL DEFAULT '',
                    source_sentence TEXT NOT NULL DEFAULT '',
                    image_prompt TEXT NOT NULL DEFAULT '',
                    question_de TEXT NOT NULL DEFAULT '',
                    answer_options JSONB NOT NULL DEFAULT '[]'::jsonb,
                    correct_option_index INTEGER,
                    explanation TEXT,
                    provider_name TEXT,
                    provider_meta JSONB NOT NULL DEFAULT '{}'::jsonb,
                    image_object_key TEXT,
                    image_url TEXT,
                    generation_status TEXT NOT NULL DEFAULT 'pending',
                    visual_status TEXT NOT NULL DEFAULT 'unknown',
                    last_error TEXT,
                    prepared_at TIMESTAMPTZ,
                    last_used_at TIMESTAMPTZ,
                    use_count INTEGER NOT NULL DEFAULT 0,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    CHECK (generation_status IN ('pending', 'blueprint_ready', 'rendering', 'ready', 'failed')),
                    CHECK (visual_status IN ('unknown', 'valid', 'rejected'))
                );
            """)
            cursor.execute("""
                ALTER TABLE bt_3_image_quiz_templates
                DROP CONSTRAINT IF EXISTS bt_3_image_quiz_templates_generation_status_check;
            """)
            cursor.execute("""
                ALTER TABLE bt_3_image_quiz_templates
                ADD CONSTRAINT bt_3_image_quiz_templates_generation_status_check
                CHECK (generation_status IN ('pending', 'blueprint_ready', 'rendering', 'ready', 'failed'));
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_image_quiz_templates_ready_lookup
                ON bt_3_image_quiz_templates (
                    user_id,
                    source_lang,
                    target_lang,
                    generation_status,
                    visual_status,
                    last_used_at,
                    prepared_at DESC,
                    created_at DESC
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_image_quiz_templates_canonical
                ON bt_3_image_quiz_templates (canonical_entry_id, user_id, created_at DESC);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_image_quiz_templates_source_entry
                ON bt_3_image_quiz_templates (source_dictionary_entry_id, created_at DESC);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_image_quiz_templates_user_source_status
                ON bt_3_image_quiz_templates (user_id, source_dictionary_entry_id, generation_status, created_at DESC);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_image_quiz_dispatches (
                    id BIGSERIAL PRIMARY KEY,
                    template_id BIGINT NOT NULL,
                    target_user_id BIGINT NOT NULL,
                    chat_id BIGINT NOT NULL,
                    message_id BIGINT,
                    delivery_scope TEXT NOT NULL DEFAULT 'private',
                    delivery_slot TEXT,
                    delivery_date_local DATE,
                    status TEXT NOT NULL DEFAULT 'claimed',
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    sent_at TIMESTAMPTZ,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    CHECK (delivery_scope IN ('private', 'group')),
                    CHECK (status IN ('claimed', 'sent', 'failed'))
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_image_quiz_dispatches_target_time
                ON bt_3_image_quiz_dispatches (target_user_id, created_at DESC);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_image_quiz_dispatches_template_target
                ON bt_3_image_quiz_dispatches (template_id, target_user_id, created_at DESC);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_image_quiz_dispatches_chat_time
                ON bt_3_image_quiz_dispatches (chat_id, created_at DESC);
            """)
            cursor.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS uq_bt_3_image_quiz_dispatches_user_slot
                ON bt_3_image_quiz_dispatches (target_user_id, delivery_date_local, delivery_slot)
                WHERE delivery_date_local IS NOT NULL
                  AND delivery_slot IS NOT NULL;
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_image_quiz_answers (
                    id BIGSERIAL PRIMARY KEY,
                    dispatch_id BIGINT NOT NULL,
                    user_id BIGINT NOT NULL,
                    selected_option_index INTEGER NOT NULL,
                    selected_text TEXT,
                    is_correct BOOLEAN NOT NULL DEFAULT FALSE,
                    answered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    feedback_sent_at TIMESTAMPTZ,
                    UNIQUE (dispatch_id, user_id)
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_image_quiz_answers_dispatch_time
                ON bt_3_image_quiz_answers (dispatch_id, answered_at DESC);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_image_quiz_answers_user_time
                ON bt_3_image_quiz_answers (user_id, answered_at DESC);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_dictionary_cache (
                    word_ru TEXT PRIMARY KEY,
                    response_json JSONB NOT NULL,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS bt_3_dictionary_lookup_cache (
                    cache_key TEXT PRIMARY KEY,
                    source_lang TEXT NOT NULL DEFAULT '',
                    target_lang TEXT NOT NULL DEFAULT '',
                    query_source_lang TEXT NOT NULL DEFAULT '',
                    query_target_lang TEXT NOT NULL DEFAULT '',
                    lookup_lang TEXT NOT NULL DEFAULT '',
                    normalized_word TEXT NOT NULL DEFAULT '',
                    response_json JSONB NOT NULL,
                    hit_count BIGINT NOT NULL DEFAULT 0,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_bt_3_dictionary_lookup_cache_updated
                ON bt_3_dictionary_lookup_cache (updated_at DESC);
                """
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_bt_3_dictionary_lookup_cache_word_lang
                ON bt_3_dictionary_lookup_cache (
                    normalized_word,
                    source_lang,
                    target_lang,
                    query_source_lang,
                    query_target_lang,
                    lookup_lang
                );
                """
            )
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_youtube_transcripts (
                    video_id TEXT PRIMARY KEY,
                    items JSONB NOT NULL,
                    language TEXT,
                    is_generated BOOLEAN,
                    translations JSONB,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            cursor.execute("""
                ALTER TABLE bt_3_youtube_transcripts
                ADD COLUMN IF NOT EXISTS translations JSONB;
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_youtube_transcripts_updated
                ON bt_3_youtube_transcripts (updated_at);
            """)
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS bt_3_youtube_watch_state (
                    user_id BIGINT NOT NULL,
                    video_id TEXT NOT NULL,
                    input_text TEXT,
                    current_time_seconds INTEGER NOT NULL DEFAULT 0,
                    last_opened_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (user_id, video_id)
                );
                """
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_bt_3_youtube_watch_state_updated
                ON bt_3_youtube_watch_state (user_id, updated_at DESC);
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS bt_3_translation_draft_state (
                    user_id BIGINT NOT NULL,
                    source_session_id TEXT NOT NULL,
                    drafts_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (user_id, source_session_id)
                );
                """
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_bt_3_translation_draft_state_updated
                ON bt_3_translation_draft_state (user_id, updated_at DESC);
                """
            )
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_flashcard_stats (
                    user_id BIGINT NOT NULL,
                    entry_id BIGINT NOT NULL,
                    correct_count INT DEFAULT 0,
                    wrong_count INT DEFAULT 0,
                    last_result BOOLEAN,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (user_id, entry_id)
                );
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_flashcard_seen (
                    user_id BIGINT NOT NULL,
                    entry_id BIGINT NOT NULL,
                    seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (user_id, entry_id, seen_at)
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_flashcard_seen_user_seen_desc
                ON bt_3_flashcard_seen (user_id, seen_at DESC);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_flashcard_feel_feedback_queue (
                    token TEXT PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    entry_id BIGINT NOT NULL,
                    feel_explanation TEXT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    consumed_at TIMESTAMPTZ,
                    feedback_action TEXT
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_flashcard_feel_feedback_queue_user_pending
                ON bt_3_flashcard_feel_feedback_queue (user_id, consumed_at, created_at DESC);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_telegram_system_messages (
                    id BIGSERIAL PRIMARY KEY,
                    chat_id BIGINT NOT NULL,
                    message_id BIGINT NOT NULL,
                    message_type TEXT NOT NULL DEFAULT 'text',
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    deleted_at TIMESTAMPTZ,
                    delete_error TEXT
                );
            """)
            cursor.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_bt_3_telegram_sysmsg_chat_msg
                ON bt_3_telegram_system_messages (chat_id, message_id);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_telegram_sysmsg_pending
                ON bt_3_telegram_system_messages (deleted_at, created_at);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_admin_scheduler_runs (
                    id BIGSERIAL PRIMARY KEY,
                    job_key TEXT NOT NULL,
                    run_period TEXT NOT NULL,
                    target_chat_id BIGINT NOT NULL,
                    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
            """)
            cursor.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_bt_3_admin_scheduler_runs_unique
                ON bt_3_admin_scheduler_runs (job_key, run_period, target_chat_id);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_scheduler_run_guards (
                    id BIGSERIAL PRIMARY KEY,
                    job_key TEXT NOT NULL,
                    run_period TEXT NOT NULL,
                    target_scope TEXT NOT NULL DEFAULT 'global',
                    status TEXT NOT NULL DEFAULT 'running',
                    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
                    claimed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    finished_at TIMESTAMPTZ,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    CHECK (status IN ('running', 'completed', 'failed'))
                );
            """)
            cursor.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_bt_3_scheduler_run_guards_unique
                ON bt_3_scheduler_run_guards (job_key, run_period, target_scope);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_scheduler_run_guards_status
                ON bt_3_scheduler_run_guards (status, updated_at DESC);
            """)
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS bt_3_semantic_benchmark_library (
                    id BIGSERIAL PRIMARY KEY,
                    source_lang TEXT NOT NULL DEFAULT 'ru',
                    target_lang TEXT NOT NULL DEFAULT 'de',
                    source_sentence TEXT NOT NULL,
                    source_sentence_hash TEXT NOT NULL,
                    benchmark_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    benchmark_status TEXT NOT NULL DEFAULT 'pending',
                    benchmark_confidence TEXT,
                    sentence_level_anchor TEXT,
                    prompt_version TEXT,
                    llm_model TEXT,
                    notes TEXT,
                    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
                    approved_at TIMESTAMPTZ,
                    last_used_at TIMESTAMPTZ,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    CHECK (benchmark_status IN ('pending', 'ready', 'needs_review', 'approved', 'superseded', 'error')),
                    CHECK (benchmark_confidence IS NULL OR benchmark_confidence IN ('high', 'medium', 'low')),
                    UNIQUE (source_lang, target_lang, source_sentence_hash)
                );
                """
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_bt_3_semantic_benchmark_library_status
                ON bt_3_semantic_benchmark_library (benchmark_status, updated_at DESC);
                """
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_bt_3_semantic_benchmark_library_lang_used
                ON bt_3_semantic_benchmark_library (source_lang, target_lang, last_used_at DESC);
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS bt_3_semantic_benchmark_queue (
                    id BIGSERIAL PRIMARY KEY,
                    source_lang TEXT NOT NULL DEFAULT 'ru',
                    target_lang TEXT NOT NULL DEFAULT 'de',
                    source_sentence TEXT NOT NULL,
                    source_sentence_hash TEXT NOT NULL,
                    queue_status TEXT NOT NULL DEFAULT 'pending',
                    priority DOUBLE PRECISION NOT NULL DEFAULT 0.0,
                    sample_count INT NOT NULL DEFAULT 1,
                    first_seen_at TIMESTAMPTZ,
                    last_seen_at TIMESTAMPTZ,
                    recent_source_session_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
                    recent_check_session_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
                    benchmark_id BIGINT REFERENCES bt_3_semantic_benchmark_library(id) ON DELETE SET NULL,
                    last_error TEXT,
                    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    CHECK (queue_status IN ('pending', 'processing', 'ready', 'skipped', 'error')),
                    UNIQUE (source_lang, target_lang, source_sentence_hash)
                );
                """
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_bt_3_semantic_benchmark_queue_status
                ON bt_3_semantic_benchmark_queue (queue_status, priority DESC, updated_at ASC);
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS bt_3_semantic_audit_runs (
                    id BIGSERIAL PRIMARY KEY,
                    run_key TEXT,
                    run_scope TEXT NOT NULL,
                    source_lang TEXT NOT NULL DEFAULT 'ru',
                    target_lang TEXT NOT NULL DEFAULT 'de',
                    period_start DATE,
                    period_end DATE,
                    sample_size INT NOT NULL DEFAULT 0,
                    benchmark_case_count INT NOT NULL DEFAULT 0,
                    run_status TEXT NOT NULL DEFAULT 'queued',
                    delivery_chat_id BIGINT,
                    delivery_status TEXT NOT NULL DEFAULT 'pending',
                    metrics_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    summary_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    summary_markdown TEXT,
                    last_error TEXT,
                    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
                    started_at TIMESTAMPTZ,
                    completed_at TIMESTAMPTZ,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    CHECK (run_status IN ('queued', 'running', 'done', 'failed')),
                    CHECK (delivery_status IN ('pending', 'sent', 'failed', 'skipped'))
                );
                """
            )
            cursor.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_bt_3_semantic_audit_runs_run_key
                ON bt_3_semantic_audit_runs (run_key)
                WHERE run_key IS NOT NULL;
                """
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_bt_3_semantic_audit_runs_scope_period
                ON bt_3_semantic_audit_runs (run_scope, source_lang, target_lang, created_at DESC);
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS bt_3_semantic_audit_case_results (
                    id BIGSERIAL PRIMARY KEY,
                    audit_run_id BIGINT NOT NULL REFERENCES bt_3_semantic_audit_runs(id) ON DELETE CASCADE,
                    case_id TEXT,
                    source_lang TEXT NOT NULL DEFAULT 'ru',
                    target_lang TEXT NOT NULL DEFAULT 'de',
                    source_sentence TEXT NOT NULL,
                    source_sentence_hash TEXT NOT NULL,
                    source_session_id TEXT,
                    check_session_id BIGINT,
                    sentence_id BIGINT REFERENCES bt_3_daily_sentences(id) ON DELETE SET NULL,
                    benchmark_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    expected_tested_primary TEXT,
                    expected_tested_secondary JSONB NOT NULL DEFAULT '[]'::jsonb,
                    expected_outcome_type TEXT,
                    actual_tested_primary TEXT,
                    actual_tested_secondary JSONB NOT NULL DEFAULT '[]'::jsonb,
                    actual_errored_skills JSONB NOT NULL DEFAULT '[]'::jsonb,
                    actual_outcome_type TEXT,
                    primary_match BOOLEAN,
                    secondary_skill_overlap DOUBLE PRECISION,
                    outcome_match BOOLEAN,
                    classification TEXT,
                    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    CHECK (classification IS NULL OR classification IN ('clearly_correct', 'questionable', 'likely_incorrect'))
                );
                """
            )
            cursor.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_bt_3_semantic_audit_case_results_unique
                ON bt_3_semantic_audit_case_results (audit_run_id, source_sentence_hash, COALESCE(source_session_id, ''), COALESCE(sentence_id, 0));
                """
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_bt_3_semantic_audit_case_results_run
                ON bt_3_semantic_audit_case_results (audit_run_id, case_id, created_at ASC);
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS bt_3_support_messages (
                    id BIGSERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    from_role TEXT NOT NULL,
                    message_text TEXT NOT NULL,
                    attachment_url TEXT,
                    attachment_kind TEXT,
                    attachment_mime_type TEXT,
                    attachment_file_name TEXT,
                    admin_telegram_id BIGINT,
                    telegram_chat_id BIGINT,
                    telegram_message_id BIGINT,
                    reply_to_id BIGINT,
                    is_read_by_user BOOLEAN NOT NULL DEFAULT FALSE,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_bt_3_support_messages_user_created
                ON bt_3_support_messages (user_id, created_at ASC);
                """
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_bt_3_support_messages_unread
                ON bt_3_support_messages (user_id, is_read_by_user, from_role, created_at DESC);
                """
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_bt_3_support_messages_telegram_ref
                ON bt_3_support_messages (telegram_chat_id, telegram_message_id);
                """
            )
            cursor.execute(
                """
                ALTER TABLE bt_3_support_messages
                    ADD COLUMN IF NOT EXISTS attachment_url TEXT,
                    ADD COLUMN IF NOT EXISTS attachment_kind TEXT,
                    ADD COLUMN IF NOT EXISTS attachment_mime_type TEXT,
                    ADD COLUMN IF NOT EXISTS attachment_file_name TEXT;
                """
            )
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_card_srs_state (
                    id BIGSERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    card_id BIGINT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'new',
                    due_at TIMESTAMPTZ NOT NULL,
                    last_review_at TIMESTAMPTZ,
                    interval_days INTEGER NOT NULL DEFAULT 0,
                    reps INTEGER NOT NULL DEFAULT 0,
                    lapses INTEGER NOT NULL DEFAULT 0,
                    stability DOUBLE PRECISION NOT NULL DEFAULT 0,
                    difficulty DOUBLE PRECISION NOT NULL DEFAULT 0,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
            """)
            cursor.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS uq_bt_3_card_srs_state_user_card
                ON bt_3_card_srs_state (user_id, card_id);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_card_srs_state_user_due
                ON bt_3_card_srs_state (user_id, due_at);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_card_srs_state_user_created
                ON bt_3_card_srs_state (user_id, created_at);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_card_srs_state_user_status
                ON bt_3_card_srs_state (user_id, status);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_card_review_log (
                    id BIGSERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    card_id BIGINT NOT NULL,
                    reviewed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    rating SMALLINT NOT NULL,
                    response_ms INTEGER,
                    scheduled_due_before TIMESTAMPTZ,
                    scheduled_due_after TIMESTAMPTZ,
                    stability_before DOUBLE PRECISION,
                    difficulty_before DOUBLE PRECISION,
                    stability_after DOUBLE PRECISION,
                    difficulty_after DOUBLE PRECISION,
                    interval_days_after INTEGER
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_card_review_log_user_reviewed_desc
                ON bt_3_card_review_log (user_id, reviewed_at DESC);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_card_review_log_user_card_reviewed_desc
                ON bt_3_card_review_log (user_id, card_id, reviewed_at DESC);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_daily_plans (
                    id BIGSERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    plan_date DATE NOT NULL,
                    total_minutes INTEGER NOT NULL DEFAULT 0,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (user_id, plan_date)
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_daily_plans_user_date
                ON bt_3_daily_plans (user_id, plan_date DESC);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_weekly_goals (
                    id BIGSERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    source_lang TEXT NOT NULL DEFAULT 'ru',
                    target_lang TEXT NOT NULL DEFAULT 'de',
                    week_start DATE NOT NULL,
                    translations_goal INTEGER NOT NULL DEFAULT 0,
                    learned_words_goal INTEGER NOT NULL DEFAULT 0,
                    agent_minutes_goal INTEGER NOT NULL DEFAULT 0,
                    reading_minutes_goal INTEGER NOT NULL DEFAULT 0,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (user_id, source_lang, target_lang, week_start)
                );
            """)
            cursor.execute("""
                ALTER TABLE bt_3_weekly_goals
                ADD COLUMN IF NOT EXISTS agent_minutes_goal INTEGER NOT NULL DEFAULT 0;
            """)
            cursor.execute("""
                ALTER TABLE bt_3_weekly_goals
                ADD COLUMN IF NOT EXISTS reading_minutes_goal INTEGER NOT NULL DEFAULT 0;
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_weekly_goals_user_week
                ON bt_3_weekly_goals (user_id, source_lang, target_lang, week_start DESC);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_agent_voice_sessions (
                    id BIGSERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    source_lang TEXT NOT NULL DEFAULT 'ru',
                    target_lang TEXT NOT NULL DEFAULT 'de',
                    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    ended_at TIMESTAMPTZ,
                    duration_seconds INTEGER,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_reader_sessions (
                    id BIGSERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    source_lang TEXT NOT NULL DEFAULT 'ru',
                    target_lang TEXT NOT NULL DEFAULT 'de',
                    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    ended_at TIMESTAMPTZ,
                    duration_seconds INTEGER,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_reader_sessions_user_active
                ON bt_3_reader_sessions (user_id, source_lang, target_lang, started_at DESC)
                WHERE ended_at IS NULL;
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_reader_sessions_user_range
                ON bt_3_reader_sessions (user_id, source_lang, target_lang, started_at, ended_at);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_reader_library (
                    id BIGSERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    source_lang TEXT NOT NULL DEFAULT 'ru',
                    target_lang TEXT NOT NULL DEFAULT 'de',
                    title TEXT NOT NULL DEFAULT 'Untitled',
                    source_type TEXT NOT NULL DEFAULT 'text',
                    source_url TEXT,
                    text_hash TEXT NOT NULL,
                    content_text TEXT NOT NULL,
                    content_pages JSONB NOT NULL DEFAULT '[]'::jsonb,
                    total_chars INTEGER NOT NULL DEFAULT 0,
                    progress_percent DOUBLE PRECISION NOT NULL DEFAULT 0,
                    bookmark_percent DOUBLE PRECISION NOT NULL DEFAULT 0,
                    reading_mode TEXT NOT NULL DEFAULT 'vertical',
                    is_archived BOOLEAN NOT NULL DEFAULT FALSE,
                    archived_at TIMESTAMPTZ,
                    last_opened_at TIMESTAMPTZ,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (user_id, source_lang, target_lang, text_hash)
                );
            """)
            cursor.execute("""
                ALTER TABLE bt_3_reader_library
                ADD COLUMN IF NOT EXISTS is_archived BOOLEAN NOT NULL DEFAULT FALSE;
            """)
            cursor.execute("""
                ALTER TABLE bt_3_reader_library
                ADD COLUMN IF NOT EXISTS archived_at TIMESTAMPTZ;
            """)
            cursor.execute("""
                ALTER TABLE bt_3_reader_library
                ADD COLUMN IF NOT EXISTS content_pages JSONB NOT NULL DEFAULT '[]'::jsonb;
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_reader_library_user_pair
                ON bt_3_reader_library (user_id, source_lang, target_lang, updated_at DESC);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_agent_voice_sessions_user_active
                ON bt_3_agent_voice_sessions (user_id, source_lang, target_lang, started_at DESC)
                WHERE ended_at IS NULL;
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_agent_voice_sessions_user_range
                ON bt_3_agent_voice_sessions (user_id, source_lang, target_lang, started_at, ended_at);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_agent_voice_transcript_segments (
                    id BIGSERIAL PRIMARY KEY,
                    session_id BIGINT NOT NULL REFERENCES bt_3_agent_voice_sessions(id) ON DELETE CASCADE,
                    seq_no INTEGER NOT NULL,
                    speaker TEXT NOT NULL,
                    text TEXT NOT NULL,
                    metadata JSONB,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (session_id, seq_no)
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_agent_voice_transcript_segments_session_seq
                ON bt_3_agent_voice_transcript_segments (session_id, seq_no);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_voice_session_assessments (
                    id BIGSERIAL PRIMARY KEY,
                    session_id BIGINT NOT NULL UNIQUE REFERENCES bt_3_agent_voice_sessions(id) ON DELETE CASCADE,
                    summary TEXT,
                    strict_feedback TEXT,
                    lexical_range_note TEXT,
                    grammar_control_note TEXT,
                    fluency_note TEXT,
                    coherence_relevance_note TEXT,
                    self_correction_note TEXT,
                    target_vocab_used JSONB NOT NULL DEFAULT '[]'::jsonb,
                    target_vocab_missed JSONB NOT NULL DEFAULT '[]'::jsonb,
                    recommended_next_focus TEXT,
                    skill_bridge_status TEXT NOT NULL DEFAULT 'pending',
                    skill_bridge_notes JSONB NOT NULL DEFAULT '{}'::jsonb,
                    skill_bridge_updated_at TIMESTAMPTZ,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
            """)
            cursor.execute("""
                ALTER TABLE bt_3_voice_session_assessments
                ADD COLUMN IF NOT EXISTS skill_bridge_status TEXT NOT NULL DEFAULT 'pending';
            """)
            cursor.execute("""
                ALTER TABLE bt_3_voice_session_assessments
                ADD COLUMN IF NOT EXISTS skill_bridge_notes JSONB NOT NULL DEFAULT '{}'::jsonb;
            """)
            cursor.execute("""
                ALTER TABLE bt_3_voice_session_assessments
                ADD COLUMN IF NOT EXISTS skill_bridge_updated_at TIMESTAMPTZ;
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_voice_session_assessments_updated
                ON bt_3_voice_session_assessments (updated_at DESC);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_voice_session_assessments_bridge_status
                ON bt_3_voice_session_assessments (skill_bridge_status, updated_at DESC);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_voice_scenarios (
                    id BIGSERIAL PRIMARY KEY,
                    slug TEXT NOT NULL UNIQUE,
                    title TEXT NOT NULL,
                    topic TEXT NOT NULL,
                    level TEXT NOT NULL DEFAULT 'mixed',
                    system_prompt TEXT,
                    is_active BOOLEAN NOT NULL DEFAULT TRUE,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_voice_scenarios_active
                ON bt_3_voice_scenarios (is_active, updated_at DESC);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_voice_prep_packs (
                    id BIGSERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    scenario_id BIGINT REFERENCES bt_3_voice_scenarios(id) ON DELETE SET NULL,
                    custom_topic_text TEXT,
                    target_vocab JSONB NOT NULL DEFAULT '[]'::jsonb,
                    target_expressions JSONB NOT NULL DEFAULT '[]'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_voice_prep_packs_user_created
                ON bt_3_voice_prep_packs (user_id, created_at DESC);
            """)
            cursor.execute("""
                ALTER TABLE bt_3_agent_voice_sessions
                ADD COLUMN IF NOT EXISTS scenario_id BIGINT REFERENCES bt_3_voice_scenarios(id) ON DELETE SET NULL;
            """)
            cursor.execute("""
                ALTER TABLE bt_3_agent_voice_sessions
                ADD COLUMN IF NOT EXISTS prep_pack_id BIGINT REFERENCES bt_3_voice_prep_packs(id) ON DELETE SET NULL;
            """)
            cursor.execute("""
                ALTER TABLE bt_3_agent_voice_sessions
                ADD COLUMN IF NOT EXISTS topic_mode TEXT;
            """)
            cursor.execute("""
                ALTER TABLE bt_3_agent_voice_sessions
                ADD COLUMN IF NOT EXISTS custom_topic_text TEXT;
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_agent_voice_sessions_scenario
                ON bt_3_agent_voice_sessions (scenario_id);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_agent_voice_sessions_prep_pack
                ON bt_3_agent_voice_sessions (prep_pack_id);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_daily_plan_items (
                    id BIGSERIAL PRIMARY KEY,
                    plan_id BIGINT NOT NULL REFERENCES bt_3_daily_plans(id) ON DELETE CASCADE,
                    order_index INTEGER NOT NULL DEFAULT 0,
                    task_type TEXT NOT NULL,
                    title TEXT NOT NULL,
                    estimated_minutes INTEGER NOT NULL DEFAULT 0,
                    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
                    status TEXT NOT NULL DEFAULT 'todo',
                    completed_at TIMESTAMPTZ
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_daily_plan_items_plan
                ON bt_3_daily_plan_items (plan_id, order_index);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_daily_plan_items_status
                ON bt_3_daily_plan_items (status);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_video_recommendations (
                    id BIGSERIAL PRIMARY KEY,
                    source_lang TEXT NOT NULL DEFAULT 'ru',
                    target_lang TEXT NOT NULL DEFAULT 'de',
                    focus_key TEXT NOT NULL,
                    skill_id TEXT,
                    main_category TEXT,
                    sub_category TEXT,
                    search_query TEXT,
                    video_id TEXT NOT NULL,
                    video_url TEXT,
                    video_title TEXT,
                    like_count INTEGER NOT NULL DEFAULT 0,
                    dislike_count INTEGER NOT NULL DEFAULT 0,
                    score INTEGER NOT NULL DEFAULT 0,
                    is_active BOOLEAN NOT NULL DEFAULT TRUE,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    last_selected_at TIMESTAMPTZ
                );
            """)
            cursor.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS uq_bt_3_video_recommendations_focus_video
                ON bt_3_video_recommendations (focus_key, video_id);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_video_recommendations_focus_active
                ON bt_3_video_recommendations (focus_key, is_active, score DESC, updated_at DESC);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_video_recommendation_votes (
                    id BIGSERIAL PRIMARY KEY,
                    recommendation_id BIGINT NOT NULL REFERENCES bt_3_video_recommendations(id) ON DELETE CASCADE,
                    user_id BIGINT NOT NULL,
                    vote SMALLINT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (recommendation_id, user_id)
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_video_recommendation_votes_rec
                ON bt_3_video_recommendation_votes (recommendation_id, updated_at DESC);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_today_reminder_settings (
                    user_id BIGINT PRIMARY KEY,
                    enabled BOOLEAN NOT NULL DEFAULT FALSE,
                    timezone TEXT NOT NULL DEFAULT 'Europe/Vienna',
                    reminder_hour SMALLINT NOT NULL DEFAULT 7,
                    reminder_minute SMALLINT NOT NULL DEFAULT 0,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_today_reminder_settings_enabled
                ON bt_3_today_reminder_settings (enabled, updated_at DESC);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_audio_grammar_settings (
                    user_id BIGINT PRIMARY KEY,
                    enabled BOOLEAN NOT NULL DEFAULT FALSE,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_audio_grammar_settings_enabled
                ON bt_3_audio_grammar_settings (enabled, updated_at DESC);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_tts_prewarm_settings (
                    settings_key TEXT PRIMARY KEY DEFAULT 'global',
                    per_user_char_limit INTEGER NOT NULL DEFAULT 600,
                    updated_by BIGINT,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_youtube_proxy_subtitles_access (
                    user_id BIGINT PRIMARY KEY,
                    enabled BOOLEAN NOT NULL DEFAULT TRUE,
                    granted_by BIGINT,
                    note TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_youtube_proxy_subtitles_access_enabled
                ON bt_3_youtube_proxy_subtitles_access (enabled, updated_at DESC);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_today_regenerate_limits (
                    user_id BIGINT NOT NULL,
                    limit_date DATE NOT NULL,
                    consumed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (user_id, limit_date)
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_today_regenerate_limits_date
                ON bt_3_today_regenerate_limits (limit_date, consumed_at DESC);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_default_topics (
                    id BIGSERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    target_language TEXT NOT NULL,
                    topic_name TEXT NOT NULL,
                    error_category TEXT,
                    error_subcategory TEXT,
                    skill_id TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_default_topics_user_lang
                ON bt_3_default_topics (user_id, target_language, created_at DESC);
            """)
            cursor.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS uq_bt_3_default_topics_user_lang_topic
                ON bt_3_default_topics (user_id, target_language, topic_name);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_billing_price_snapshots (
                    id BIGSERIAL PRIMARY KEY,
                    provider TEXT NOT NULL,
                    sku TEXT NOT NULL,
                    unit TEXT NOT NULL,
                    price_per_unit NUMERIC(20, 10) NOT NULL DEFAULT 0,
                    currency TEXT NOT NULL DEFAULT 'USD',
                    valid_from TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    source TEXT NOT NULL DEFAULT 'manual',
                    raw_payload JSONB,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    CHECK (price_per_unit >= 0)
                );
            """)
            cursor.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS uq_bt_3_billing_price_snapshots_provider_sku_unit_from
                ON bt_3_billing_price_snapshots (provider, sku, unit, valid_from);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_billing_price_snapshots_lookup
                ON bt_3_billing_price_snapshots (provider, sku, unit, currency, valid_from DESC);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_billing_events (
                    id BIGSERIAL PRIMARY KEY,
                    idempotency_key TEXT NOT NULL,
                    user_id BIGINT,
                    source_lang TEXT,
                    target_lang TEXT,
                    action_type TEXT NOT NULL,
                    provider TEXT NOT NULL,
                    units_type TEXT NOT NULL,
                    units_value DOUBLE PRECISION NOT NULL DEFAULT 0,
                    price_snapshot_id BIGINT REFERENCES bt_3_billing_price_snapshots(id) ON DELETE SET NULL,
                    cost_amount NUMERIC(20, 10) NOT NULL DEFAULT 0,
                    currency TEXT NOT NULL DEFAULT 'USD',
                    status TEXT NOT NULL DEFAULT 'estimated',
                    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
                    event_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    CHECK (units_value >= 0),
                    CHECK (cost_amount >= 0),
                    CHECK (status IN ('estimated', 'final'))
                );
            """)
            cursor.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS uq_bt_3_billing_events_idempotency
                ON bt_3_billing_events (idempotency_key);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_billing_events_user_time
                ON bt_3_billing_events (user_id, event_time DESC);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_billing_events_action_provider
                ON bt_3_billing_events (action_type, provider, event_time DESC);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_billing_events_currency_time
                ON bt_3_billing_events (currency, event_time DESC);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_billing_fixed_costs (
                    id BIGSERIAL PRIMARY KEY,
                    category TEXT NOT NULL,
                    provider TEXT NOT NULL DEFAULT 'infra',
                    amount NUMERIC(20, 10) NOT NULL DEFAULT 0,
                    currency TEXT NOT NULL DEFAULT 'USD',
                    period_start DATE NOT NULL,
                    period_end DATE NOT NULL,
                    allocation_method_default TEXT NOT NULL DEFAULT 'equal',
                    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    CHECK (amount >= 0),
                    CHECK (period_end >= period_start),
                    CHECK (allocation_method_default IN ('equal', 'weighted'))
                );
            """)
            cursor.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS uq_bt_3_billing_fixed_costs_period
                ON bt_3_billing_fixed_costs (category, provider, period_start, period_end, currency);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_billing_fixed_costs_range
                ON bt_3_billing_fixed_costs (currency, period_start, period_end);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_provider_budget_controls (
                    provider TEXT NOT NULL,
                    period_month DATE NOT NULL,
                    base_limit_units BIGINT NOT NULL DEFAULT 0,
                    extra_limit_units BIGINT NOT NULL DEFAULT 0,
                    is_blocked BOOLEAN NOT NULL DEFAULT FALSE,
                    block_reason TEXT,
                    notified_thresholds JSONB NOT NULL DEFAULT '{}'::jsonb,
                    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    CHECK (base_limit_units >= 0),
                    CHECK (extra_limit_units >= 0),
                    PRIMARY KEY (provider, period_month)
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_provider_budget_controls_period
                ON bt_3_provider_budget_controls (period_month DESC, provider);
            """)
            cursor.execute(
                """
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1
                        FROM bt_3_schema_migrations
                        WHERE migration_key = '2026_03_21_cloudflare_r2_free_tier_correction'
                    ) THEN
                        UPDATE bt_3_provider_budget_controls
                        SET base_limit_units = 1000000,
                            updated_at = NOW(),
                            metadata = jsonb_set(
                                COALESCE(metadata, '{}'::jsonb),
                                '{free_tier_corrected_2026_03_21}',
                                'true'::jsonb,
                                true
                            )
                        WHERE provider = 'cloudflare_r2_class_a'
                          AND base_limit_units = 2000000
                          AND extra_limit_units = 0;

                        UPDATE bt_3_provider_budget_controls
                        SET base_limit_units = 10000000,
                            updated_at = NOW(),
                            metadata = jsonb_set(
                                COALESCE(metadata, '{}'::jsonb),
                                '{free_tier_corrected_2026_03_21}',
                                'true'::jsonb,
                                true
                            )
                        WHERE provider = 'cloudflare_r2_class_b'
                          AND base_limit_units = 20000000
                          AND extra_limit_units = 0;

                        UPDATE bt_3_provider_budget_controls
                        SET base_limit_units = 10240,
                            updated_at = NOW(),
                            metadata = jsonb_set(
                                COALESCE(metadata, '{}'::jsonb),
                                '{free_tier_corrected_2026_03_21}',
                                'true'::jsonb,
                                true
                            )
                        WHERE provider = 'cloudflare_r2_storage'
                          AND base_limit_units = 5120
                          AND extra_limit_units = 0;

                        INSERT INTO bt_3_schema_migrations (migration_key)
                        VALUES ('2026_03_21_cloudflare_r2_free_tier_correction')
                        ON CONFLICT (migration_key) DO NOTHING;
                    END IF;
                END $$;
                """
            )
            cursor.execute(
                """
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1
                        FROM bt_3_schema_migrations
                        WHERE migration_key = '2026_03_22_provider_budget_limit_correction'
                    ) THEN
                        UPDATE bt_3_provider_budget_controls
                        SET base_limit_units = 500000,
                            updated_at = NOW(),
                            metadata = jsonb_set(
                                COALESCE(metadata, '{}'::jsonb),
                                '{free_tier_corrected_2026_03_22}',
                                'true'::jsonb,
                                true
                            )
                        WHERE provider = 'deepl_free'
                          AND base_limit_units IN (0, 1000000, 5000000)
                          AND extra_limit_units = 0;

                        UPDATE bt_3_provider_budget_controls
                        SET base_limit_units = 2000000,
                            updated_at = NOW(),
                            metadata = jsonb_set(
                                COALESCE(metadata, '{}'::jsonb),
                                '{free_tier_corrected_2026_03_22}',
                                'true'::jsonb,
                                true
                            )
                        WHERE provider = 'azure_translator'
                          AND base_limit_units IN (0, 1000000)
                          AND extra_limit_units = 0;

                        UPDATE bt_3_provider_budget_controls
                        SET base_limit_units = 1000000,
                            updated_at = NOW(),
                            metadata = jsonb_set(
                                COALESCE(metadata, '{}'::jsonb),
                                '{free_tier_corrected_2026_03_22}',
                                'true'::jsonb,
                                true
                            )
                        WHERE provider = 'cloudflare_r2_class_a'
                          AND base_limit_units = 2000000
                          AND extra_limit_units = 0;

                        UPDATE bt_3_provider_budget_controls
                        SET base_limit_units = 10000000,
                            updated_at = NOW(),
                            metadata = jsonb_set(
                                COALESCE(metadata, '{}'::jsonb),
                                '{free_tier_corrected_2026_03_22}',
                                'true'::jsonb,
                                true
                            )
                        WHERE provider = 'cloudflare_r2_class_b'
                          AND base_limit_units = 20000000
                          AND extra_limit_units = 0;

                        INSERT INTO bt_3_schema_migrations (migration_key)
                        VALUES ('2026_03_22_provider_budget_limit_correction')
                        ON CONFLICT (migration_key) DO NOTHING;
                    END IF;
                END $$;
                """
            )
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS plans (
                    plan_code TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    is_paid BOOLEAN NOT NULL DEFAULT FALSE,
                    stripe_price_id TEXT,
                    daily_cost_cap_eur NUMERIC(20, 10),
                    trial_days INT NOT NULL DEFAULT 0,
                    is_active BOOLEAN NOT NULL DEFAULT TRUE,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    CHECK (trial_days >= 0),
                    CHECK (daily_cost_cap_eur IS NULL OR daily_cost_cap_eur >= 0)
                );
            """)
            cursor.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS uq_plans_stripe_price_id
                ON plans (stripe_price_id)
                WHERE stripe_price_id IS NOT NULL AND stripe_price_id <> '';
            """)
            # Trial policy stays global (TRIAL_DAYS / TRIAL_DAILY_COST_CAP_EUR), not per plan row.
            seed_trial_daily_cap = _env_decimal("TRIAL_DAILY_COST_CAP_EUR", "1.00")
            seed_free_daily_cap = _env_decimal("FREE_DAILY_COST_CAP_EUR", "0.50")
            seed_pro_daily_cap = _env_decimal("PRO_DAILY_COST_CAP_EUR", None)
            seed_pro_price_id = str(os.getenv("STRIPE_PRICE_ID_PRO", "")).strip() or None
            seed_support_coffee_price_id = str(os.getenv("STRIPE_PRICE_ID_SUPPORT_COFFEE", "")).strip() or None
            seed_support_cheesecake_price_id = str(os.getenv("STRIPE_PRICE_ID_SUPPORT_CHEESECAKE", "")).strip() or None
            if not seed_pro_price_id:
                print("⚠️ billing config warning: STRIPE_PRICE_ID_PRO is empty, legacy pro plan will be seeded without stripe_price_id")
            if not seed_support_coffee_price_id:
                print("⚠️ billing config warning: STRIPE_PRICE_ID_SUPPORT_COFFEE is empty, support_coffee plan will be seeded without stripe_price_id")
            if not seed_support_cheesecake_price_id:
                print("⚠️ billing config warning: STRIPE_PRICE_ID_SUPPORT_CHEESECAKE is empty, support_cheesecake plan will be seeded without stripe_price_id")
            if (
                seed_free_daily_cap is not None
                and seed_trial_daily_cap is not None
                and seed_free_daily_cap > seed_trial_daily_cap
            ):
                print(
                    "⚠️ billing config warning: FREE_DAILY_COST_CAP_EUR is greater than TRIAL_DAILY_COST_CAP_EUR"
                )
            cursor.executemany(
                """
                INSERT INTO plans (
                    plan_code,
                    name,
                    is_paid,
                    stripe_price_id,
                    daily_cost_cap_eur,
                    trial_days,
                    is_active,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, TRUE, NOW())
                ON CONFLICT (plan_code) DO UPDATE
                SET
                    name = EXCLUDED.name,
                    is_paid = EXCLUDED.is_paid,
                    stripe_price_id = EXCLUDED.stripe_price_id,
                    daily_cost_cap_eur = EXCLUDED.daily_cost_cap_eur,
                    trial_days = EXCLUDED.trial_days,
                    is_active = TRUE,
                    updated_at = NOW();
                """,
                [
                    (
                        "free",
                        "Free",
                        False,
                        None,
                        seed_free_daily_cap,
                        0,
                    ),
                    (
                        "pro",
                        "Pro",
                        True,
                        seed_pro_price_id,
                        seed_pro_daily_cap,
                        0,
                    ),
                    (
                        "support_coffee",
                        "Поддержать разработчика: кофе ☕️",
                        True,
                        seed_support_coffee_price_id,
                        seed_pro_daily_cap,
                        0,
                    ),
                    (
                        "support_cheesecake",
                        "Поддержать разработчика: кофе ☕️ и чизкейк 🍰",
                        True,
                        seed_support_cheesecake_price_id,
                        seed_pro_daily_cap,
                        0,
                    ),
                ],
            )
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS user_subscriptions (
                    user_id BIGINT UNIQUE NOT NULL,
                    plan_code TEXT NOT NULL REFERENCES plans(plan_code),
                    status TEXT NOT NULL,
                    trial_ends_at TIMESTAMPTZ,
                    current_period_end TIMESTAMPTZ,
                    stripe_customer_id TEXT,
                    stripe_subscription_id TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    CHECK (status IN ('active', 'inactive', 'past_due', 'canceled', 'trialing'))
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_user_subscriptions_plan_status
                ON user_subscriptions (plan_code, status);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_user_subscriptions_customer
                ON user_subscriptions (stripe_customer_id)
                WHERE stripe_customer_id IS NOT NULL AND stripe_customer_id <> '';
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_user_subscriptions_subscription
                ON user_subscriptions (stripe_subscription_id)
                WHERE stripe_subscription_id IS NOT NULL AND stripe_subscription_id <> '';
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS stripe_events_processed (
                    event_id TEXT PRIMARY KEY,
                    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS plan_limits (
                    plan_code TEXT NOT NULL REFERENCES plans(plan_code),
                    feature_code TEXT NOT NULL,
                    limit_value NUMERIC(20, 10) NOT NULL,
                    limit_unit TEXT NOT NULL,
                    period TEXT NOT NULL,
                    is_active BOOLEAN NOT NULL DEFAULT TRUE,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    CHECK (limit_value >= 0),
                    CHECK (limit_unit IN ('count', 'seconds', 'chars', 'tokens', 'eur')),
                    CHECK (period IN ('day')),
                    UNIQUE (plan_code, feature_code, period)
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_plan_limits_lookup
                ON plan_limits (plan_code, is_active, period);
            """)
            free_feel_word_daily = _env_decimal("FREE_FEEL_WORD_DAILY_LIMIT", "3")
            free_skill_training_daily = _env_decimal("FREE_SKILL_TRAINING_DAILY_LIMIT", "1")
            free_translation_daily_sets = _env_decimal("FREE_TRANSLATION_DAILY_SETS_LIMIT", "1")
            plan_limit_seed_rows: list[tuple] = []
            if free_translation_daily_sets is not None and free_translation_daily_sets >= 0:
                plan_limit_seed_rows.append(
                    (
                        "free",
                        "translation_daily_sets",
                        free_translation_daily_sets,
                        "count",
                        "day",
                    )
                )
            if free_feel_word_daily is not None and free_feel_word_daily >= 0:
                plan_limit_seed_rows.append(
                    (
                        "free",
                        "feel_word_daily",
                        free_feel_word_daily,
                        "count",
                        "day",
                    )
                )
            if free_skill_training_daily is not None and free_skill_training_daily >= 0:
                plan_limit_seed_rows.append(
                    (
                        "free",
                        "skill_training_daily",
                        free_skill_training_daily,
                        "count",
                        "day",
                    )
                )
            if plan_limit_seed_rows:
                cursor.executemany(
                    """
                    INSERT INTO plan_limits (
                        plan_code,
                        feature_code,
                        limit_value,
                        limit_unit,
                        period,
                        is_active,
                        updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, TRUE, NOW())
                    ON CONFLICT (plan_code, feature_code, period) DO UPDATE
                    SET
                        limit_value = EXCLUDED.limit_value,
                        limit_unit = EXCLUDED.limit_unit,
                        is_active = TRUE,
                        updated_at = NOW();
                    """,
                    plan_limit_seed_rows,
                )
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS daily_cost_rollup (
                    user_id BIGINT NOT NULL,
                    day DATE NOT NULL,
                    currency TEXT NOT NULL DEFAULT 'EUR',
                    total_cost_eur NUMERIC(20, 10) NOT NULL DEFAULT 0,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    CHECK (total_cost_eur >= 0),
                    UNIQUE (user_id, day)
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_daily_cost_rollup_user_day
                ON daily_cost_rollup (user_id, day DESC);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_tts_chunk_cache (
                    cache_key TEXT PRIMARY KEY,
                    language TEXT NOT NULL,
                    source_text TEXT NOT NULL,
                    chunks JSONB NOT NULL,
                    hit_count BIGINT NOT NULL DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_tts_chunk_cache_updated
                ON bt_3_tts_chunk_cache (updated_at);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_tts_audio_cache (
                    cache_key TEXT PRIMARY KEY,
                    language TEXT NOT NULL,
                    voice TEXT NOT NULL,
                    speed DOUBLE PRECISION NOT NULL,
                    source_text TEXT NOT NULL,
                    audio_mp3 BYTEA NOT NULL,
                    hit_count BIGINT NOT NULL DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_tts_audio_cache_updated
                ON bt_3_tts_audio_cache (updated_at);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_translation_sentence_pool (
                    id BIGSERIAL PRIMARY KEY,
                    source_lang TEXT NOT NULL,
                    target_lang TEXT NOT NULL,
                    focus_key TEXT NOT NULL,
                    focus_label TEXT NOT NULL,
                    level TEXT NOT NULL,
                    sentence TEXT NOT NULL,
                    sentence_hash TEXT NOT NULL,
                    tested_skill_profile JSONB NOT NULL DEFAULT '[]'::jsonb,
                    is_active BOOLEAN NOT NULL DEFAULT TRUE,
                    use_count BIGINT NOT NULL DEFAULT 0,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    last_used_at TIMESTAMPTZ,
                    UNIQUE (source_lang, target_lang, focus_key, level, sentence_hash)
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_translation_sentence_pool_lookup
                ON bt_3_translation_sentence_pool (source_lang, target_lang, focus_key, level, is_active, last_used_at DESC, created_at DESC);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_translation_focus_pool_daily_snapshots (
                    snapshot_date DATE NOT NULL,
                    source_lang TEXT NOT NULL,
                    target_lang TEXT NOT NULL,
                    focus_key TEXT NOT NULL,
                    focus_label TEXT NOT NULL,
                    level TEXT NOT NULL,
                    ready_count BIGINT NOT NULL DEFAULT 0,
                    low_watermark BIGINT NOT NULL DEFAULT 0,
                    target_ready BIGINT NOT NULL DEFAULT 0,
                    recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (snapshot_date, source_lang, target_lang, focus_key, level)
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_translation_focus_pool_daily_snapshots_lookup
                ON bt_3_translation_focus_pool_daily_snapshots (source_lang, target_lang, snapshot_date DESC, focus_key, level);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_translation_sentence_user_state (
                    user_id BIGINT NOT NULL,
                    source_lang TEXT NOT NULL,
                    target_lang TEXT NOT NULL,
                    sentence_hash TEXT NOT NULL,
                    sentence_text TEXT NOT NULL,
                    seen_count BIGINT NOT NULL DEFAULT 0,
                    best_score INT,
                    last_score INT,
                    mastered BOOLEAN NOT NULL DEFAULT FALSE,
                    mastered_at TIMESTAMPTZ,
                    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    last_session_id TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (user_id, source_lang, target_lang, sentence_hash)
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_translation_sentence_user_state_mastered
                ON bt_3_translation_sentence_user_state (user_id, source_lang, target_lang, mastered, updated_at DESC);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_tts_object_cache (
                    cache_key TEXT PRIMARY KEY,
                    status TEXT NOT NULL DEFAULT 'pending',
                    language TEXT,
                    voice TEXT,
                    speed DOUBLE PRECISION,
                    source_text TEXT,
                    object_key TEXT,
                    url TEXT,
                    size_bytes BIGINT,
                    error_code TEXT,
                    error_msg TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    last_hit_at TIMESTAMPTZ,
                    CHECK (status IN ('pending', 'ready', 'failed'))
                );
            """)
            # Backward-compatible migration path: if table existed with older shape,
            # add required columns lazily without destructive schema operations.
            cursor.execute("""
                ALTER TABLE bt_3_tts_object_cache
                ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'pending';
            """)
            cursor.execute("""
                ALTER TABLE bt_3_tts_object_cache
                ADD COLUMN IF NOT EXISTS object_key TEXT;
            """)
            cursor.execute("""
                ALTER TABLE bt_3_tts_object_cache
                ADD COLUMN IF NOT EXISTS url TEXT;
            """)
            cursor.execute("""
                ALTER TABLE bt_3_tts_object_cache
                ADD COLUMN IF NOT EXISTS size_bytes BIGINT;
            """)
            cursor.execute("""
                ALTER TABLE bt_3_tts_object_cache
                ADD COLUMN IF NOT EXISTS error_code TEXT;
            """)
            cursor.execute("""
                ALTER TABLE bt_3_tts_object_cache
                ADD COLUMN IF NOT EXISTS error_msg TEXT;
            """)
            cursor.execute("""
                ALTER TABLE bt_3_tts_object_cache
                ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
            """)
            cursor.execute("""
                ALTER TABLE bt_3_tts_object_cache
                ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
            """)
            cursor.execute("""
                ALTER TABLE bt_3_tts_object_cache
                ADD COLUMN IF NOT EXISTS last_hit_at TIMESTAMPTZ;
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_tts_object_cache_status_updated
                ON bt_3_tts_object_cache (status, updated_at DESC);
            """)
            cursor.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS uq_bt_3_tts_object_cache_object_key
                ON bt_3_tts_object_cache (object_key)
                WHERE object_key IS NOT NULL;
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_tts_object_cache_last_hit
                ON bt_3_tts_object_cache (last_hit_at DESC);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_tts_admin_monitor_events (
                    id BIGSERIAL PRIMARY KEY,
                    kind TEXT NOT NULL,
                    status TEXT NOT NULL,
                    source TEXT NOT NULL DEFAULT 'unknown',
                    count INT NOT NULL DEFAULT 1,
                    chars INT NOT NULL DEFAULT 0,
                    duration_ms INT,
                    meta JSONB NOT NULL DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_tts_admin_monitor_events_created
                ON bt_3_tts_admin_monitor_events (created_at DESC);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_tts_admin_monitor_events_kind_created
                ON bt_3_tts_admin_monitor_events (kind, created_at DESC);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_story_bank (
                    id SERIAL PRIMARY KEY,
                    title TEXT,
                    answer TEXT NOT NULL,
                    answer_aliases JSONB,
                    extra_de TEXT NOT NULL,
                    story_type TEXT,
                    difficulty TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_story_sentences (
                    id SERIAL PRIMARY KEY,
                    story_id INT NOT NULL REFERENCES bt_3_story_bank(id) ON DELETE CASCADE,
                    sentence_index INT NOT NULL,
                    sentence TEXT NOT NULL
                );
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_story_sessions (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    session_id TEXT NOT NULL,
                    story_id INT NOT NULL REFERENCES bt_3_story_bank(id),
                    mode TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    completed_at TIMESTAMP,
                    guess TEXT,
                    guess_correct BOOLEAN,
                    score INT,
                    feedback TEXT
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_story_sessions_user
                ON bt_3_story_sessions (user_id);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_story_sessions_session
                ON bt_3_story_sessions (session_id);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_story_bank_type
                ON bt_3_story_bank (story_type, difficulty);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_active_quizzes (
                    poll_id TEXT PRIMARY KEY,
                    chat_id BIGINT NOT NULL,
                    message_id BIGINT,
                    correct_option_id INTEGER NOT NULL,
                    correct_text TEXT,
                    options JSONB NOT NULL DEFAULT '[]'::jsonb,
                    freeform_option TEXT,
                    quiz_type TEXT,
                    word_ru TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_active_quizzes_created_at
                ON bt_3_active_quizzes (created_at DESC);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_access_requests (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    username TEXT,
                    status TEXT NOT NULL DEFAULT 'pending',
                    requested_via TEXT NOT NULL DEFAULT 'bot',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    reviewed_at TIMESTAMP,
                    reviewed_by BIGINT,
                    review_note TEXT
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_access_requests_user_time
                ON bt_3_access_requests (user_id, created_at DESC);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_access_requests_status
                ON bt_3_access_requests (status, created_at DESC);
            """)
            _ensure_bt3_detailed_mistakes_constraints(cursor)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_detailed_mistakes_user_sentence
                ON bt_3_detailed_mistakes (user_id, sentence_id);
            """)
            cursor.close()
            _run_dictionary_canonical_schema_migration(conn)
        missing_phase1_objects = get_missing_phase1_shadow_schema_objects()
        if missing_phase1_objects:
            raise RuntimeError(
                "Skill shadow schema bootstrap incomplete: "
                + ", ".join(missing_phase1_objects)
            )
        _ENSURE_WEBAPP_TABLES_DONE = True


def require_semantic_audit_tables() -> None:
    required_tables = (
        "bt_3_semantic_benchmark_library",
        "bt_3_semantic_benchmark_queue",
        "bt_3_semantic_audit_runs",
        "bt_3_semantic_audit_case_results",
    )
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_name = ANY(%s::text[]);
                """,
                (list(required_tables),),
            )
            existing = {str(row[0]) for row in cursor.fetchall() or []}
    missing = [table_name for table_name in required_tables if table_name not in existing]
    if missing:
        raise RuntimeError(
            "Semantic audit tables are missing. Run ensure_webapp_tables() once before running semantic audit jobs. "
            f"Missing: {', '.join(missing)}"
        )


def get_missing_phase1_shadow_schema_objects() -> list[str]:
    missing: list[str] = []
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    to_regclass('public.bt_3_skill_mastery_groups'),
                    to_regclass('public.idx_bt_3_skill_mastery_groups_lang_active'),
                    to_regclass('public.bt_3_skill_mastery_group_members'),
                    to_regclass('public.uq_bt_3_skill_mastery_group_members_skill'),
                    to_regclass('public.idx_bt_3_skill_mastery_group_members_group'),
                    to_regclass('public.bt_3_sentence_skill_targets'),
                    to_regclass('public.bt_3_sentence_skill_shadow_state_v2'),
                    to_regclass('public.bt_3_skill_events_v2'),
                    to_regclass('public.idx_bt_3_sentence_skill_targets_sentence_role'),
                    to_regclass('public.uq_bt_3_sentence_skill_targets_primary'),
                    to_regclass('public.idx_bt_3_sentence_skill_shadow_state_v2_user'),
                    to_regclass('public.idx_bt_3_skill_events_v2_user_created'),
                    to_regclass('public.idx_bt_3_skill_events_v2_user_skill_created'),
                    to_regclass('public.idx_bt_3_skill_events_v2_sentence_attempt'),
                    to_regclass('public.idx_bt_3_skill_events_v2_skill_incremental'),
                    to_regclass('public.bt_3_user_skill_state_v2'),
                    to_regclass('public.idx_bt_3_user_skill_state_v2_user_mastery'),
                    to_regclass('public.bt_3_skill_state_v2_dirty'),
                    to_regclass('public.idx_bt_3_skill_state_v2_dirty_schedule'),
                    to_regclass('public.idx_bt_3_skill_state_v2_dirty_lease'),
                    to_regclass('public.bt_3_skill_state_v2_worker_stats'),
                    to_regclass('public.idx_bt_3_skill_state_v2_worker_stats_updated');
                """
            )
            row = cursor.fetchone() or (None,) * 22
            object_names = [
                "bt_3_skill_mastery_groups",
                "idx_bt_3_skill_mastery_groups_lang_active",
                "bt_3_skill_mastery_group_members",
                "uq_bt_3_skill_mastery_group_members_skill",
                "idx_bt_3_skill_mastery_group_members_group",
                "bt_3_sentence_skill_targets",
                "bt_3_sentence_skill_shadow_state_v2",
                "bt_3_skill_events_v2",
                "idx_bt_3_sentence_skill_targets_sentence_role",
                "uq_bt_3_sentence_skill_targets_primary",
                "idx_bt_3_sentence_skill_shadow_state_v2_user",
                "idx_bt_3_skill_events_v2_user_created",
                "idx_bt_3_skill_events_v2_user_skill_created",
                "idx_bt_3_skill_events_v2_sentence_attempt",
                "idx_bt_3_skill_events_v2_skill_incremental",
                "bt_3_user_skill_state_v2",
                "idx_bt_3_user_skill_state_v2_user_mastery",
                "bt_3_skill_state_v2_dirty",
                "idx_bt_3_skill_state_v2_dirty_schedule",
                "idx_bt_3_skill_state_v2_dirty_lease",
                "bt_3_skill_state_v2_worker_stats",
                "idx_bt_3_skill_state_v2_worker_stats_updated",
            ]
            for name, regclass_value in zip(object_names, row):
                if regclass_value is None:
                    missing.append(name)

            if row[0] is not None:
                cursor.execute(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM pg_constraint
                        WHERE conrelid = 'public.bt_3_skill_mastery_groups'::regclass
                          AND contype = 'p'
                    );
                    """
                )
                mastery_groups_pk = bool((cursor.fetchone() or [False])[0])
                if not mastery_groups_pk:
                    missing.append("bt_3_skill_mastery_groups_pkey")

            if row[2] is not None:
                cursor.execute(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM pg_constraint
                        WHERE conrelid = 'public.bt_3_skill_mastery_group_members'::regclass
                          AND contype = 'p'
                    );
                    """
                )
                mastery_members_pk = bool((cursor.fetchone() or [False])[0])
                if not mastery_members_pk:
                    missing.append("bt_3_skill_mastery_group_members_pkey")

            if row[5] is not None:
                cursor.execute(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM pg_constraint
                        WHERE conrelid = 'public.bt_3_sentence_skill_targets'::regclass
                          AND contype = 'u'
                          AND pg_get_constraintdef(oid) ILIKE 'UNIQUE (sentence_id, skill_id)%'
                    );
                    """
                )
                unique_targets = bool((cursor.fetchone() or [False])[0])
                if not unique_targets:
                    missing.append("bt_3_sentence_skill_targets_unique_sentence_skill")

            if row[6] is not None:
                cursor.execute(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM pg_constraint
                        WHERE conrelid = 'public.bt_3_sentence_skill_shadow_state_v2'::regclass
                          AND contype = 'p'
                    );
                    """
                )
                shadow_state_pk = bool((cursor.fetchone() or [False])[0])
                if not shadow_state_pk:
                    missing.append("bt_3_sentence_skill_shadow_state_v2_pkey")

            if row[7] is not None:
                cursor.execute(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM pg_constraint
                        WHERE conrelid = 'public.bt_3_skill_events_v2'::regclass
                          AND contype = 'p'
                    );
                    """
                )
                skill_events_pk = bool((cursor.fetchone() or [False])[0])
                if not skill_events_pk:
                    missing.append("bt_3_skill_events_v2_pkey")
            if row[15] is not None:
                cursor.execute(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM pg_constraint
                        WHERE conrelid = 'public.bt_3_user_skill_state_v2'::regclass
                          AND contype = 'p'
                    );
                    """
                )
                user_skill_state_v2_pk = bool((cursor.fetchone() or [False])[0])
                if not user_skill_state_v2_pk:
                    missing.append("bt_3_user_skill_state_v2_pkey")

            if row[17] is not None:
                cursor.execute(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM pg_constraint
                        WHERE conrelid = 'public.bt_3_skill_state_v2_dirty'::regclass
                          AND contype = 'p'
                    );
                    """
                )
                dirty_pk = bool((cursor.fetchone() or [False])[0])
                if not dirty_pk:
                    missing.append("bt_3_skill_state_v2_dirty_pkey")
            if row[20] is not None:
                cursor.execute(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM pg_constraint
                        WHERE conrelid = 'public.bt_3_skill_state_v2_worker_stats'::regclass
                          AND contype = 'p'
                    );
                    """
                )
                worker_stats_pk = bool((cursor.fetchone() or [False])[0])
                if not worker_stats_pk:
                    missing.append("bt_3_skill_state_v2_worker_stats_pkey")
    return missing


def _skill_state_v2_classify_outcome(per_skill_outcome: str | None) -> str:
    normalized = str(per_skill_outcome or "").strip().lower()
    if normalized in {"fail_new", "fail_repeat_no_progress", "fail_repeat_progress", "untargeted_error_fail", "catastrophic_fail_fallback"}:
        return "fail"
    if normalized in {"recovered_partial", "recovered_final"}:
        return "recovery"
    if normalized in {"clean_success", "clean_progress_credit"}:
        return "success"
    return "neutral"


def _compute_skill_state_v2_confidence(tested_events: int) -> float:
    safe_tested_events = max(0, int(tested_events or 0))
    return max(0.0, min(1.0, 1.0 - math.exp(-float(safe_tested_events) / SKILL_STATE_V2_CONFIDENCE_SPREAD)))


def _compute_skill_state_v2_mastery(net_evidence_recent: float, net_evidence_lifetime: float) -> float:
    mastery = (
        50.0
        + SKILL_STATE_V2_MASTERY_RECENT_WEIGHT
        * math.tanh(float(net_evidence_recent or 0.0) / SKILL_STATE_V2_MASTERY_RECENT_SPREAD)
        + SKILL_STATE_V2_MASTERY_LIFETIME_WEIGHT
        * math.tanh(float(net_evidence_lifetime or 0.0) / SKILL_STATE_V2_MASTERY_LIFETIME_SPREAD)
    )
    return max(5.0, min(99.0, float(mastery)))


def _get_skill_state_v2_snapshot_since_date(
    *,
    user_id: int,
    source_lang: str,
    target_lang: str,
    reset_date: date | None,
) -> dict[str, dict[str, Any]]:
    if reset_date is None:
        return {}
    normalized_source = str(source_lang or "ru").strip().lower() or "ru"
    normalized_target = str(target_lang or "de").strip().lower() or "de"
    start_dt = datetime.combine(reset_date, dt_time.min, tzinfo=timezone.utc)
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    skill_id,
                    created_at,
                    is_tested,
                    per_skill_outcome,
                    shadow_delta_signal
                FROM bt_3_skill_events_v2
                WHERE user_id = %s
                  AND source_lang = %s
                  AND target_lang = %s
                  AND created_at >= %s
                ORDER BY skill_id ASC, created_at ASC, id ASC;
                """,
                (int(user_id), normalized_source, normalized_target, start_dt),
            )
            rows = cursor.fetchall() or []

    state_map: dict[str, dict[str, Any]] = {}
    tau_seconds = SKILL_STATE_V2_RECENT_TAU_DAYS * 24.0 * 60.0 * 60.0
    for row in rows:
        skill_id = str(row[0] or "").strip()
        if not skill_id:
            continue
        event_created_at = row[1] or start_dt
        is_tested = bool(row[2])
        per_skill_outcome = row[3]
        shadow_delta_signal = float(row[4] or 0.0)
        current = state_map.setdefault(
            skill_id,
            {
                "mastery": 50.0,
                "confidence": 0.0,
                "net_evidence_recent": 0.0,
                "net_evidence_lifetime": 0.0,
                "tested_events": 0,
                "total_events": 0,
                "last_practiced_at": None,
                "last_event_at": None,
            },
        )
        last_event_at = current.get("last_event_at")
        if isinstance(last_event_at, datetime):
            delta_seconds = max(0.0, float((event_created_at - last_event_at).total_seconds()))
            if tau_seconds > 0:
                current["net_evidence_recent"] *= math.exp(-delta_seconds / tau_seconds)
        current["net_evidence_recent"] += shadow_delta_signal
        current["net_evidence_lifetime"] += shadow_delta_signal
        current["total_events"] = int(current.get("total_events") or 0) + 1
        if is_tested:
            current["tested_events"] = int(current.get("tested_events") or 0) + 1
        current["confidence"] = _compute_skill_state_v2_confidence(int(current.get("tested_events") or 0))
        current["mastery"] = _compute_skill_state_v2_mastery(
            float(current.get("net_evidence_recent") or 0.0),
            float(current.get("net_evidence_lifetime") or 0.0),
        )
        current["last_event_at"] = event_created_at
        current["last_practiced_at"] = event_created_at
        current["outcome_bucket"] = _skill_state_v2_classify_outcome(per_skill_outcome)

    for item in state_map.values():
        item.pop("last_event_at", None)
        last_practiced_at = item.get("last_practiced_at")
        if isinstance(last_practiced_at, datetime):
            item["last_practiced_at"] = last_practiced_at.isoformat()
        else:
            item["last_practiced_at"] = None
    return state_map


def _list_prorated_weekly_goals(
    *,
    user_id: int,
    source_lang: str,
    target_lang: str,
    start_date: date,
    end_date: date,
) -> dict[str, int]:
    normalized_source = str(source_lang or "ru").strip().lower() or "ru"
    normalized_target = str(target_lang or "de").strip().lower() or "de"
    totals = {
        "translations_goal": 0.0,
        "learned_words_goal": 0.0,
        "agent_minutes_goal": 0.0,
        "reading_minutes_goal": 0.0,
    }
    if end_date < start_date:
        return {key: 0 for key in totals}
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    week_start,
                    translations_goal,
                    learned_words_goal,
                    agent_minutes_goal,
                    reading_minutes_goal
                FROM bt_3_weekly_goals
                WHERE user_id = %s
                  AND source_lang = %s
                  AND target_lang = %s
                  AND week_start <= %s
                  AND (week_start + INTERVAL '6 days')::date >= %s;
                """,
                (int(user_id), normalized_source, normalized_target, end_date, start_date),
            )
            rows = cursor.fetchall() or []
    for row in rows:
        week_start = row[0]
        if not isinstance(week_start, date):
            continue
        week_end = week_start + timedelta(days=6)
        overlap_start = max(start_date, week_start)
        overlap_end = min(end_date, week_end)
        if overlap_end < overlap_start:
            continue
        overlap_days = (overlap_end - overlap_start).days + 1
        ratio = max(0.0, min(1.0, overlap_days / 7.0))
        totals["translations_goal"] += float(row[1] or 0) * ratio
        totals["learned_words_goal"] += float(row[2] or 0) * ratio
        totals["agent_minutes_goal"] += float(row[3] or 0) * ratio
        totals["reading_minutes_goal"] += float(row[4] or 0) * ratio
    return {key: max(0, int(round(value))) for key, value in totals.items()}


def claim_skill_state_v2_dirty_keys(*, limit: int, lease_owner: str, lease_seconds: int) -> list[dict]:
    safe_limit = max(1, min(int(limit or 1), 500))
    safe_lease_owner = str(lease_owner or "").strip()[:64] or f"skill-v2-{uuid4().hex[:12]}"
    safe_lease_seconds = max(5, int(lease_seconds or 30))
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                WITH candidates AS (
                    SELECT user_id, skill_id, source_lang, target_lang
                    FROM bt_3_skill_state_v2_dirty
                    WHERE next_attempt_at <= NOW()
                      AND (lease_expires_at IS NULL OR lease_expires_at <= NOW())
                    ORDER BY updated_at ASC
                    LIMIT %s
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE bt_3_skill_state_v2_dirty AS dirty
                SET
                    lease_owner = %s,
                    lease_expires_at = NOW() + (%s * INTERVAL '1 second'),
                    updated_at = NOW()
                FROM candidates
                WHERE dirty.user_id = candidates.user_id
                  AND dirty.skill_id = candidates.skill_id
                  AND dirty.source_lang = candidates.source_lang
                  AND dirty.target_lang = candidates.target_lang
                RETURNING
                    dirty.user_id,
                    dirty.skill_id,
                    dirty.source_lang,
                    dirty.target_lang,
                    dirty.max_event_id,
                    dirty.retry_count,
                    dirty.enqueue_count,
                    dirty.first_enqueued_at,
                    dirty.updated_at;
                """,
                (safe_limit, safe_lease_owner, safe_lease_seconds),
            )
            rows = cursor.fetchall() or []
    return [
        {
            "user_id": int(row[0]),
            "skill_id": str(row[1]),
            "source_lang": str(row[2] or "ru"),
            "target_lang": str(row[3] or "de"),
            "max_event_id": int(row[4] or 0),
            "retry_count": int(row[5] or 0),
            "enqueue_count": int(row[6] or 0),
            "first_enqueued_at": row[7].isoformat() if row[7] else None,
            "updated_at": row[8].isoformat() if row[8] else None,
        }
        for row in rows
    ]


def process_skill_state_v2_dirty_key(
    *,
    user_id: int,
    skill_id: str,
    source_lang: str,
    target_lang: str,
    claimed_max_event_id: int,
    lease_owner: str,
) -> dict:
    safe_user_id = int(user_id)
    safe_skill_id = str(skill_id or "").strip()
    safe_source_lang = str(source_lang or "ru")
    safe_target_lang = str(target_lang or "de")
    safe_claimed_max_event_id = max(0, int(claimed_max_event_id or 0))
    safe_lease_owner = str(lease_owner or "").strip()[:64]
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        mastery,
                        confidence,
                        net_evidence_recent,
                        net_evidence_lifetime,
                        tested_events,
                        fail_events,
                        success_events,
                        recovery_events,
                        last_event_id,
                        last_event_at
                    FROM bt_3_user_skill_state_v2
                    WHERE user_id = %s
                      AND skill_id = %s
                      AND source_lang = %s
                      AND target_lang = %s
                    FOR UPDATE;
                    """,
                    (safe_user_id, safe_skill_id, safe_source_lang, safe_target_lang),
                )
                state_row = cursor.fetchone()
                mastery = float(state_row[0]) if state_row else 50.0
                confidence = float(state_row[1]) if state_row else 0.0
                net_recent = float(state_row[2]) if state_row else 0.0
                net_lifetime = float(state_row[3]) if state_row else 0.0
                tested_events = int(state_row[4]) if state_row else 0
                fail_events = int(state_row[5]) if state_row else 0
                success_events = int(state_row[6]) if state_row else 0
                recovery_events = int(state_row[7]) if state_row else 0
                last_event_id = int(state_row[8]) if state_row and state_row[8] is not None else 0
                last_event_at = state_row[9] if state_row else None

                cursor.execute(
                    """
                    SELECT
                        id,
                        created_at,
                        is_tested,
                        per_skill_outcome,
                        shadow_delta_signal
                    FROM bt_3_skill_events_v2
                    WHERE user_id = %s
                      AND skill_id = %s
                      AND source_lang = %s
                      AND target_lang = %s
                      AND id > %s
                      AND id <= %s
                    ORDER BY id ASC;
                    """,
                    (
                        safe_user_id,
                        safe_skill_id,
                        safe_source_lang,
                        safe_target_lang,
                        last_event_id,
                        safe_claimed_max_event_id,
                    ),
                )
                event_rows = cursor.fetchall() or []
                if event_rows:
                    for event_id, created_at, is_tested, per_skill_outcome, shadow_delta_signal in event_rows:
                        event_created_at = created_at or datetime.now(timezone.utc)
                        if last_event_at is not None:
                            delta_seconds = max(
                                0.0,
                                float((event_created_at - last_event_at).total_seconds()),
                            )
                            tau_seconds = SKILL_STATE_V2_RECENT_TAU_DAYS * 24.0 * 60.0 * 60.0
                            if tau_seconds > 0:
                                net_recent *= math.exp(-delta_seconds / tau_seconds)
                        delta_value = float(shadow_delta_signal or 0.0)
                        net_recent += delta_value
                        net_lifetime += delta_value
                        if bool(is_tested):
                            tested_events += 1
                        outcome_bucket = _skill_state_v2_classify_outcome(per_skill_outcome)
                        if outcome_bucket == "fail":
                            fail_events += 1
                        elif outcome_bucket == "success":
                            success_events += 1
                        elif outcome_bucket == "recovery":
                            recovery_events += 1
                        last_event_id = int(event_id or last_event_id)
                        last_event_at = event_created_at

                    confidence = _compute_skill_state_v2_confidence(tested_events)
                    mastery = _compute_skill_state_v2_mastery(net_recent, net_lifetime)
                    cursor.execute(
                        """
                        INSERT INTO bt_3_user_skill_state_v2 (
                            user_id,
                            skill_id,
                            source_lang,
                            target_lang,
                            mastery,
                            confidence,
                            net_evidence_recent,
                            net_evidence_lifetime,
                            tested_events,
                            fail_events,
                            success_events,
                            recovery_events,
                            last_event_id,
                            last_event_at,
                            updated_at
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                        ON CONFLICT (user_id, skill_id, source_lang, target_lang) DO UPDATE
                        SET
                            mastery = EXCLUDED.mastery,
                            confidence = EXCLUDED.confidence,
                            net_evidence_recent = EXCLUDED.net_evidence_recent,
                            net_evidence_lifetime = EXCLUDED.net_evidence_lifetime,
                            tested_events = EXCLUDED.tested_events,
                            fail_events = EXCLUDED.fail_events,
                            success_events = EXCLUDED.success_events,
                            recovery_events = EXCLUDED.recovery_events,
                            last_event_id = EXCLUDED.last_event_id,
                            last_event_at = EXCLUDED.last_event_at,
                            updated_at = NOW();
                        """,
                        (
                            safe_user_id,
                            safe_skill_id,
                            safe_source_lang,
                            safe_target_lang,
                            mastery,
                            confidence,
                            net_recent,
                            net_lifetime,
                            tested_events,
                            fail_events,
                            success_events,
                            recovery_events,
                            last_event_id if last_event_id > 0 else None,
                            last_event_at,
                        ),
                    )

                cursor.execute(
                    """
                    DELETE FROM bt_3_skill_state_v2_dirty
                    WHERE user_id = %s
                      AND skill_id = %s
                      AND source_lang = %s
                      AND target_lang = %s
                      AND lease_owner = %s
                      AND max_event_id <= %s;
                    """,
                    (
                        safe_user_id,
                        safe_skill_id,
                        safe_source_lang,
                        safe_target_lang,
                        safe_lease_owner,
                        last_event_id,
                    ),
                )
                deleted_dirty_row = int(cursor.rowcount or 0) > 0
                if not deleted_dirty_row:
                    cursor.execute(
                        """
                        UPDATE bt_3_skill_state_v2_dirty
                        SET
                            lease_owner = NULL,
                            lease_expires_at = NULL,
                            retry_count = 0,
                            next_attempt_at = NOW(),
                            last_error = NULL,
                            updated_at = NOW()
                        WHERE user_id = %s
                          AND skill_id = %s
                          AND source_lang = %s
                          AND target_lang = %s
                          AND lease_owner = %s;
                        """,
                        (safe_user_id, safe_skill_id, safe_source_lang, safe_target_lang, safe_lease_owner),
                    )

        return {
            "ok": True,
            "user_id": safe_user_id,
            "skill_id": safe_skill_id,
            "source_lang": safe_source_lang,
            "target_lang": safe_target_lang,
            "processed_events": len(event_rows),
            "last_event_id": int(last_event_id or 0),
            "dirty_row_deleted": bool(deleted_dirty_row),
            "mastery": float(mastery),
            "confidence": float(confidence),
        }
    except Exception as exc:
        with get_db_connection_context() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE bt_3_skill_state_v2_dirty
                    SET
                        lease_owner = NULL,
                        lease_expires_at = NULL,
                        retry_count = retry_count + 1,
                        next_attempt_at = NOW() + (LEAST(300, 5 * CAST(POWER(2, LEAST(retry_count + 1, 5)) AS INT)) * INTERVAL '1 second'),
                        last_error = %s,
                        updated_at = NOW()
                    WHERE user_id = %s
                      AND skill_id = %s
                      AND source_lang = %s
                      AND target_lang = %s
                      AND lease_owner = %s;
                    """,
                    (
                        str(exc)[:500],
                        safe_user_id,
                        safe_skill_id,
                        safe_source_lang,
                        safe_target_lang,
                        safe_lease_owner,
                    ),
                )
        return {
            "ok": False,
            "user_id": safe_user_id,
            "skill_id": safe_skill_id,
            "source_lang": safe_source_lang,
            "target_lang": safe_target_lang,
            "processed_events": 0,
            "error": str(exc),
        }


def get_skill_state_v2_dirty_summary() -> dict[str, Any]:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    COUNT(*) AS dirty_count,
                    COUNT(*) FILTER (
                        WHERE lease_expires_at IS NOT NULL
                          AND lease_expires_at > NOW()
                    ) AS leased_count,
                    COALESCE(SUM(retry_count), 0) AS retry_count_total,
                    COALESCE(MAX(retry_count), 0) AS retry_count_max,
                    MIN(first_enqueued_at) AS oldest_dirty_at
                FROM bt_3_skill_state_v2_dirty;
                """
            )
            row = cursor.fetchone() or (0, 0, 0, 0, None)
    oldest_dirty_at = row[4]
    oldest_dirty_age_seconds = None
    if oldest_dirty_at:
        oldest_dirty_age_seconds = max(
            0,
            int((datetime.now(timezone.utc) - oldest_dirty_at).total_seconds()),
        )
    return {
        "dirty_count": int(row[0] or 0),
        "leased_count": int(row[1] or 0),
        "retry_count_total": int(row[2] or 0),
        "retry_count_max": int(row[3] or 0),
        "oldest_dirty_at": oldest_dirty_at.isoformat() if oldest_dirty_at else None,
        "oldest_dirty_age_seconds": oldest_dirty_age_seconds,
    }


def record_skill_state_v2_worker_run(
    *,
    worker_name: str,
    keys_processed: int,
    events_processed: int,
    duration_ms: int,
    error: str | None = None,
) -> None:
    safe_worker_name = str(worker_name or "").strip()[:128] or "skill-state-v2-aggregator"
    safe_keys_processed = max(0, int(keys_processed or 0))
    safe_events_processed = max(0, int(events_processed or 0))
    safe_duration_ms = max(0, int(duration_ms or 0))
    safe_error = str(error or "").strip()[:2000] or None
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_skill_state_v2_worker_stats (
                    worker_name,
                    runs_total,
                    keys_processed_total,
                    events_processed_total,
                    last_run_at,
                    last_success_at,
                    last_duration_ms,
                    last_keys_processed,
                    last_events_processed,
                    last_error,
                    errors_total,
                    updated_at
                )
                VALUES (
                    %s,
                    1,
                    %s,
                    %s,
                    NOW(),
                    CASE WHEN %s IS NULL AND %s > 0 THEN NOW() ELSE NULL END,
                    %s,
                    %s,
                    %s,
                    %s,
                    CASE WHEN %s IS NULL THEN 0 ELSE 1 END,
                    NOW()
                )
                ON CONFLICT (worker_name) DO UPDATE
                SET
                    runs_total = bt_3_skill_state_v2_worker_stats.runs_total + 1,
                    keys_processed_total = bt_3_skill_state_v2_worker_stats.keys_processed_total + EXCLUDED.keys_processed_total,
                    events_processed_total = bt_3_skill_state_v2_worker_stats.events_processed_total + EXCLUDED.events_processed_total,
                    last_run_at = NOW(),
                    last_success_at = CASE
                        WHEN EXCLUDED.last_success_at IS NOT NULL THEN EXCLUDED.last_success_at
                        ELSE bt_3_skill_state_v2_worker_stats.last_success_at
                    END,
                    last_duration_ms = EXCLUDED.last_duration_ms,
                    last_keys_processed = EXCLUDED.last_keys_processed,
                    last_events_processed = EXCLUDED.last_events_processed,
                    last_error = EXCLUDED.last_error,
                    errors_total = bt_3_skill_state_v2_worker_stats.errors_total + CASE WHEN EXCLUDED.last_error IS NULL THEN 0 ELSE 1 END,
                    updated_at = NOW();
                """,
                (
                    safe_worker_name,
                    safe_keys_processed,
                    safe_events_processed,
                    safe_error,
                    safe_keys_processed,
                    safe_duration_ms,
                    safe_keys_processed,
                    safe_events_processed,
                    safe_error,
                    safe_error,
                ),
            )


def get_skill_state_v2_worker_summary() -> dict[str, Any]:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                WITH aggregated AS (
                    SELECT
                        COUNT(*) AS worker_rows,
                        COALESCE(SUM(runs_total), 0) AS runs_total,
                        COALESCE(SUM(keys_processed_total), 0) AS keys_processed_total,
                        COALESCE(SUM(events_processed_total), 0) AS events_processed_total,
                        MAX(last_run_at) AS last_run_at,
                        MAX(last_success_at) AS last_success_at,
                        COALESCE(SUM(errors_total), 0) AS errors_total,
                        MAX(updated_at) AS updated_at
                    FROM bt_3_skill_state_v2_worker_stats
                ),
                latest AS (
                    SELECT
                        last_duration_ms,
                        last_keys_processed,
                        last_events_processed,
                        last_error
                    FROM bt_3_skill_state_v2_worker_stats
                    ORDER BY updated_at DESC, worker_name ASC
                    LIMIT 1
                )
                SELECT
                    aggregated.worker_rows,
                    aggregated.runs_total,
                    aggregated.keys_processed_total,
                    aggregated.events_processed_total,
                    aggregated.last_run_at,
                    aggregated.last_success_at,
                    aggregated.errors_total,
                    aggregated.updated_at,
                    COALESCE(latest.last_duration_ms, 0) AS last_duration_ms,
                    COALESCE(latest.last_keys_processed, 0) AS last_keys_processed,
                    COALESCE(latest.last_events_processed, 0) AS last_events_processed,
                    latest.last_error
                FROM aggregated
                LEFT JOIN latest ON TRUE;
                """
            )
            row = cursor.fetchone() or (0, 0, 0, 0, None, None, 0, None, 0, 0, 0, None)
    return {
        "worker_rows": int(row[0] or 0),
        "runs_total": int(row[1] or 0),
        "keys_processed_total": int(row[2] or 0),
        "events_processed_total": int(row[3] or 0),
        "last_run_at": row[4].isoformat() if row[4] else None,
        "last_success_at": row[5].isoformat() if row[5] else None,
        "errors_total": int(row[6] or 0),
        "updated_at": row[7].isoformat() if row[7] else None,
        "last_duration_ms": int(row[8] or 0),
        "last_keys_processed": int(row[9] or 0),
        "last_events_processed": int(row[10] or 0),
        "last_error": str(row[11]).strip() if row[11] is not None and str(row[11]).strip() else None,
    }


def get_skill_state_v2_comparison(
    *,
    user_id: int,
    source_lang: str = "ru",
    target_lang: str = "de",
    limit: int = 20,
) -> list[dict[str, Any]]:
    safe_limit = max(1, min(int(limit or 20), 200))
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                WITH compared AS (
                    SELECT
                        COALESCE(v2.skill_id, v1.skill_id) AS skill_id,
                        v1.mastery AS v1_mastery,
                        v2.mastery AS v2_mastery,
                        v2.confidence,
                        v2.net_evidence_recent,
                        v2.net_evidence_lifetime,
                        v2.tested_events,
                        v2.fail_events,
                        v2.success_events,
                        v2.recovery_events,
                        v2.last_event_id,
                        v2.last_event_at,
                        v2.updated_at
                    FROM bt_3_user_skill_state_v2 v2
                    FULL OUTER JOIN bt_3_user_skill_state v1
                      ON v1.user_id = v2.user_id
                     AND v1.skill_id = v2.skill_id
                     AND v1.source_lang = v2.source_lang
                     AND v1.target_lang = v2.target_lang
                    WHERE COALESCE(v2.user_id, v1.user_id) = %s
                      AND COALESCE(v2.source_lang, v1.source_lang) = %s
                      AND COALESCE(v2.target_lang, v1.target_lang) = %s
                )
                SELECT
                    compared.skill_id,
                    COALESCE(sk.title, compared.skill_id) AS skill_title,
                    compared.v1_mastery,
                    compared.v2_mastery,
                    compared.confidence,
                    compared.net_evidence_recent,
                    compared.net_evidence_lifetime,
                    compared.tested_events,
                    compared.fail_events,
                    compared.success_events,
                    compared.recovery_events,
                    compared.last_event_id,
                    compared.last_event_at,
                    compared.updated_at
                FROM compared
                LEFT JOIN bt_3_skills sk
                  ON sk.skill_id = compared.skill_id
                ORDER BY
                    COALESCE(compared.v2_mastery, compared.v1_mastery, 999.0) ASC,
                    compared.skill_id ASC
                LIMIT %s;
                """,
                (int(user_id), source_lang or "ru", target_lang or "de", safe_limit),
            )
            rows = cursor.fetchall() or []
    results: list[dict[str, Any]] = []
    for row in rows:
        results.append(
            {
                "skill_id": str(row[0]),
                "skill_title": str(row[1] or row[0]),
                "v1_mastery": float(row[2]) if row[2] is not None else None,
                "v2_mastery": float(row[3]) if row[3] is not None else None,
                "confidence": float(row[4]) if row[4] is not None else None,
                "net_evidence_recent": float(row[5]) if row[5] is not None else None,
                "net_evidence_lifetime": float(row[6]) if row[6] is not None else None,
                "tested_events": int(row[7] or 0),
                "fail_events": int(row[8] or 0),
                "success_events": int(row[9] or 0),
                "recovery_events": int(row[10] or 0),
                "last_event_id": int(row[11]) if row[11] is not None else None,
                "last_event_at": row[12].isoformat() if row[12] else None,
                "updated_at": row[13].isoformat() if row[13] else None,
            }
        )
    return results


def get_admin_telegram_ids() -> set[int]:
    raw_values = [
        os.getenv("BOT_ADMIN_TELEGRAM_IDS"),
        os.getenv("TELEGRAM_ADMIN_IDS"),
        os.getenv("BOT_ADMIN_TELEGRAM_ID"),
        os.getenv("TELEGRAM_ADMIN_ID"),
    ]
    merged = ",".join(v for v in raw_values if v)
    if not merged:
        return set()

    result: set[int] = set()
    for token in merged.replace(";", ",").split(","):
        value = token.strip()
        if not value:
            continue
        try:
            result.add(int(value))
        except ValueError:
            continue
    return result


def is_telegram_user_allowed(user_id: int) -> bool:
    if not user_id:
        return False
    if int(user_id) in get_admin_telegram_ids():
        return True

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT 1 FROM bt_3_allowed_users WHERE user_id = %s LIMIT 1;",
                (int(user_id),),
            )
            return cursor.fetchone() is not None


def allow_telegram_user(
    user_id: int,
    username: str | None = None,
    added_by: int | None = None,
    note: str | None = None,
) -> None:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_allowed_users (user_id, username, added_by, note)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (user_id)
                DO UPDATE SET
                    username = COALESCE(EXCLUDED.username, bt_3_allowed_users.username),
                    added_by = EXCLUDED.added_by,
                    note = COALESCE(EXCLUDED.note, bt_3_allowed_users.note),
                    updated_at = CURRENT_TIMESTAMP;
                """,
                (int(user_id), username, added_by, note),
            )


def revoke_telegram_user(user_id: int) -> bool:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "DELETE FROM bt_3_allowed_users WHERE user_id = %s;",
                (int(user_id),),
            )
            return cursor.rowcount > 0


def _serialize_user_removal_row(row) -> dict[str, Any] | None:
    if not row:
        return None
    return {
        "user_id": int(row[0]),
        "username": row[1],
        "revoked_at": row[2].isoformat() if row[2] else None,
        "grace_until": row[3].isoformat() if row[3] else None,
        "status": row[4],
        "scheduled_by": row[5],
        "reason": row[6],
        "notification_sent_at": row[7].isoformat() if row[7] else None,
        "notification_message_refs": row[8] if isinstance(row[8], list) else [],
        "decision_at": row[9].isoformat() if row[9] else None,
        "decision_by": row[10],
        "decision_note": row[11],
        "purged_at": row[12].isoformat() if row[12] else None,
        "purge_summary": row[13] if isinstance(row[13], dict) else {},
        "billing_cancel_snapshot": row[14] if isinstance(row[14], dict) else {},
        "created_at": row[15].isoformat() if row[15] else None,
        "updated_at": row[16].isoformat() if row[16] else None,
    }


def get_user_removal_request(user_id: int) -> dict[str, Any] | None:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    user_id,
                    username,
                    revoked_at,
                    grace_until,
                    status,
                    scheduled_by,
                    reason,
                    notification_sent_at,
                    notification_message_refs,
                    decision_at,
                    decision_by,
                    decision_note,
                    purged_at,
                    purge_summary,
                    billing_cancel_snapshot,
                    created_at,
                    updated_at
                FROM bt_3_user_removal_queue
                WHERE user_id = %s
                LIMIT 1;
                """,
                (int(user_id),),
            )
            row = cursor.fetchone()
    return _serialize_user_removal_row(row)


def schedule_telegram_user_removal(
    *,
    user_id: int,
    username: str | None = None,
    scheduled_by: int | None = None,
    reason: str | None = None,
    grace_days: int | None = None,
) -> dict[str, Any]:
    normalized_username = str(username or "").strip() or None
    normalized_reason = str(reason or "").strip() or None
    resolved_grace_days = max(1, int(grace_days or USER_REMOVAL_GRACE_DAYS))
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_user_removal_queue (
                    user_id,
                    username,
                    revoked_at,
                    grace_until,
                    status,
                    scheduled_by,
                    reason,
                    notification_sent_at,
                    notification_message_refs,
                    decision_at,
                    decision_by,
                    decision_note,
                    purged_at,
                    purge_summary,
                    billing_cancel_snapshot,
                    updated_at
                )
                VALUES (
                    %s,
                    %s,
                    NOW(),
                    NOW() + (%s * INTERVAL '1 day'),
                    'scheduled',
                    %s,
                    %s,
                    NULL,
                    '[]'::jsonb,
                    NULL,
                    NULL,
                    NULL,
                    NULL,
                    '{}'::jsonb,
                    NOW()
                )
                ON CONFLICT (user_id) DO UPDATE
                SET
                    username = COALESCE(EXCLUDED.username, bt_3_user_removal_queue.username),
                    revoked_at = NOW(),
                    grace_until = EXCLUDED.grace_until,
                    status = 'scheduled',
                    scheduled_by = EXCLUDED.scheduled_by,
                    reason = COALESCE(EXCLUDED.reason, bt_3_user_removal_queue.reason),
                    notification_sent_at = NULL,
                    notification_message_refs = '[]'::jsonb,
                    decision_at = NULL,
                    decision_by = NULL,
                    decision_note = NULL,
                    purged_at = NULL,
                    purge_summary = '{}'::jsonb,
                    billing_cancel_snapshot = '{}'::jsonb,
                    updated_at = NOW()
                RETURNING
                    user_id,
                    username,
                    revoked_at,
                    grace_until,
                    status,
                    scheduled_by,
                    reason,
                    notification_sent_at,
                    notification_message_refs,
                    decision_at,
                    decision_by,
                    decision_note,
                    purged_at,
                    purge_summary,
                    billing_cancel_snapshot,
                    created_at,
                    updated_at;
                """,
                (
                    int(user_id),
                    normalized_username,
                    resolved_grace_days,
                    int(scheduled_by) if scheduled_by is not None else None,
                    normalized_reason,
                ),
            )
            row = cursor.fetchone()
    result = _serialize_user_removal_row(row)
    if not result:
        raise RuntimeError("failed to schedule telegram user removal")
    return result


def cancel_telegram_user_removal(
    *,
    user_id: int,
    canceled_by: int | None = None,
    note: str | None = None,
) -> dict[str, Any] | None:
    normalized_note = str(note or "").strip() or None
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE bt_3_user_removal_queue
                SET
                    status = 'canceled',
                    decision_at = NOW(),
                    decision_by = %s,
                    decision_note = %s,
                    updated_at = NOW()
                WHERE user_id = %s
                  AND status IN ('scheduled', 'awaiting_admin_confirmation')
                RETURNING
                    user_id,
                    username,
                    revoked_at,
                    grace_until,
                    status,
                    scheduled_by,
                    reason,
                    notification_sent_at,
                    notification_message_refs,
                    decision_at,
                    decision_by,
                    decision_note,
                    purged_at,
                    purge_summary,
                    billing_cancel_snapshot,
                    created_at,
                    updated_at;
                """,
                (
                    int(canceled_by) if canceled_by is not None else None,
                    normalized_note,
                    int(user_id),
                ),
            )
            row = cursor.fetchone()
    return _serialize_user_removal_row(row)


def list_due_user_removals_for_admin_confirmation(limit: int = 20) -> list[dict[str, Any]]:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    user_id,
                    username,
                    revoked_at,
                    grace_until,
                    status,
                    scheduled_by,
                    reason,
                    notification_sent_at,
                    notification_message_refs,
                    decision_at,
                    decision_by,
                    decision_note,
                    purged_at,
                    purge_summary,
                    billing_cancel_snapshot,
                    created_at,
                    updated_at
                FROM bt_3_user_removal_queue
                WHERE status = 'scheduled'
                  AND grace_until <= NOW()
                  AND notification_sent_at IS NULL
                ORDER BY grace_until ASC, revoked_at ASC
                LIMIT %s;
                """,
                (max(1, int(limit)),),
            )
            rows = cursor.fetchall()
    return [_serialize_user_removal_row(row) for row in rows if row]


def list_user_removal_queue(
    *,
    statuses: list[str] | tuple[str, ...] | set[str] | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    normalized_statuses = [
        str(item or "").strip().lower()
        for item in (statuses or [])
        if str(item or "").strip()
    ]
    allowed_statuses = {"scheduled", "awaiting_admin_confirmation", "canceled", "purged"}
    normalized_statuses = [item for item in normalized_statuses if item in allowed_statuses]
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            if normalized_statuses:
                cursor.execute(
                    """
                    SELECT
                        user_id,
                        username,
                        revoked_at,
                        grace_until,
                        status,
                        scheduled_by,
                        reason,
                        notification_sent_at,
                        notification_message_refs,
                        decision_at,
                        decision_by,
                        decision_note,
                        purged_at,
                        purge_summary,
                        billing_cancel_snapshot,
                        created_at,
                        updated_at
                    FROM bt_3_user_removal_queue
                    WHERE status = ANY(%s)
                    ORDER BY
                        CASE status
                            WHEN 'awaiting_admin_confirmation' THEN 1
                            WHEN 'scheduled' THEN 2
                            WHEN 'canceled' THEN 3
                            WHEN 'purged' THEN 4
                            ELSE 9
                        END ASC,
                        grace_until ASC NULLS LAST,
                        revoked_at DESC
                    LIMIT %s;
                    """,
                    (normalized_statuses, max(1, int(limit))),
                )
            else:
                cursor.execute(
                    """
                    SELECT
                        user_id,
                        username,
                        revoked_at,
                        grace_until,
                        status,
                        scheduled_by,
                        reason,
                        notification_sent_at,
                        notification_message_refs,
                        decision_at,
                        decision_by,
                        decision_note,
                        purged_at,
                        purge_summary,
                        billing_cancel_snapshot,
                        created_at,
                        updated_at
                    FROM bt_3_user_removal_queue
                    ORDER BY
                        CASE status
                            WHEN 'awaiting_admin_confirmation' THEN 1
                            WHEN 'scheduled' THEN 2
                            WHEN 'canceled' THEN 3
                            WHEN 'purged' THEN 4
                            ELSE 9
                        END ASC,
                        grace_until ASC NULLS LAST,
                        revoked_at DESC
                    LIMIT %s;
                    """,
                    (max(1, int(limit)),),
                )
            rows = cursor.fetchall() or []
    return [_serialize_user_removal_row(row) for row in rows if row]


def mark_user_removal_admin_notified(
    *,
    user_id: int,
    notification_message_refs: list[dict[str, Any]] | None = None,
) -> dict[str, Any] | None:
    refs_payload = notification_message_refs if isinstance(notification_message_refs, list) else []
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE bt_3_user_removal_queue
                SET
                    status = 'awaiting_admin_confirmation',
                    notification_sent_at = NOW(),
                    notification_message_refs = %s,
                    updated_at = NOW()
                WHERE user_id = %s
                  AND status = 'scheduled'
                RETURNING
                    user_id,
                    username,
                    revoked_at,
                    grace_until,
                    status,
                    scheduled_by,
                    reason,
                    notification_sent_at,
                    notification_message_refs,
                    decision_at,
                    decision_by,
                    decision_note,
                    purged_at,
                    purge_summary,
                    billing_cancel_snapshot,
                    created_at,
                    updated_at;
                """,
                (Json(refs_payload), int(user_id)),
            )
            row = cursor.fetchone()
    return _serialize_user_removal_row(row)


def update_user_removal_billing_cancel_snapshot(
    *,
    user_id: int,
    billing_cancel_snapshot: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    payload = billing_cancel_snapshot if isinstance(billing_cancel_snapshot, dict) else {}
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE bt_3_user_removal_queue
                SET
                    billing_cancel_snapshot = %s,
                    updated_at = NOW()
                WHERE user_id = %s
                RETURNING
                    user_id,
                    username,
                    revoked_at,
                    grace_until,
                    status,
                    scheduled_by,
                    reason,
                    notification_sent_at,
                    notification_message_refs,
                    decision_at,
                    decision_by,
                    decision_note,
                    purged_at,
                    purge_summary,
                    billing_cancel_snapshot,
                    created_at,
                    updated_at;
                """,
                (Json(payload), int(user_id)),
            )
            row = cursor.fetchone()
    return _serialize_user_removal_row(row)


def deactivate_user_subscription(
    *,
    user_id: int,
    status: str = "canceled",
    plan_code: str = "free",
    clear_stripe_subscription_id: bool = False,
) -> dict | None:
    status_value = _normalize_subscription_status(status)
    plan_code_value = str(plan_code or "free").strip().lower() or "free"
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE user_subscriptions
                SET
                    plan_code = %s,
                    status = %s,
                    trial_ends_at = NULL,
                    current_period_end = NULL,
                    stripe_subscription_id = CASE
                        WHEN %s THEN NULL
                        ELSE stripe_subscription_id
                    END,
                    updated_at = NOW()
                WHERE user_id = %s
                RETURNING
                    user_id,
                    plan_code,
                    status,
                    trial_ends_at,
                    current_period_end,
                    stripe_customer_id,
                    stripe_subscription_id,
                    created_at,
                    updated_at;
                """,
                (
                    plan_code_value,
                    status_value,
                    bool(clear_stripe_subscription_id),
                    int(user_id),
                ),
            )
            row = cursor.fetchone()
    return _subscription_row_to_dict(row) if row else None


def purge_telegram_user_personal_data(
    *,
    user_id: int,
    approved_by: int | None = None,
    note: str | None = None,
) -> dict[str, Any] | None:
    normalized_note = str(note or "").strip() or None
    delete_statements: list[tuple[str, str]] = [
        ("daily_plan_items", """
            DELETE FROM bt_3_daily_plan_items i
            USING bt_3_daily_plans p
            WHERE i.plan_id = p.id
              AND p.user_id = %s;
        """),
        ("translation_check_sessions", "DELETE FROM bt_3_translation_check_sessions WHERE user_id = %s;"),
        ("webapp_dictionary_queries", "DELETE FROM bt_3_webapp_dictionary_queries WHERE user_id = %s;"),
        ("dictionary_folders", "DELETE FROM bt_3_dictionary_folders WHERE user_id = %s;"),
        ("translation_draft_state", "DELETE FROM bt_3_translation_draft_state WHERE user_id = %s;"),
        ("youtube_watch_state", "DELETE FROM bt_3_youtube_watch_state WHERE user_id = %s;"),
        ("flashcard_feel_feedback_queue", "DELETE FROM bt_3_flashcard_feel_feedback_queue WHERE user_id = %s;"),
        ("flashcard_seen", "DELETE FROM bt_3_flashcard_seen WHERE user_id = %s;"),
        ("flashcard_stats", "DELETE FROM bt_3_flashcard_stats WHERE user_id = %s;"),
        ("video_recommendation_votes", "DELETE FROM bt_3_video_recommendation_votes WHERE user_id = %s;"),
        ("support_messages", "DELETE FROM bt_3_support_messages WHERE user_id = %s;"),
        ("card_review_log", "DELETE FROM bt_3_card_review_log WHERE user_id = %s;"),
        ("card_srs_state", "DELETE FROM bt_3_card_srs_state WHERE user_id = %s;"),
        ("daily_plans", "DELETE FROM bt_3_daily_plans WHERE user_id = %s;"),
        ("weekly_goals", "DELETE FROM bt_3_weekly_goals WHERE user_id = %s;"),
        ("agent_voice_sessions", "DELETE FROM bt_3_agent_voice_sessions WHERE user_id = %s;"),
        ("reader_sessions", "DELETE FROM bt_3_reader_sessions WHERE user_id = %s;"),
        ("reader_library", "DELETE FROM bt_3_reader_library WHERE user_id = %s;"),
        ("today_reminder_settings", "DELETE FROM bt_3_today_reminder_settings WHERE user_id = %s;"),
        ("audio_grammar_settings", "DELETE FROM bt_3_audio_grammar_settings WHERE user_id = %s;"),
        ("youtube_proxy_subtitles_access", "DELETE FROM bt_3_youtube_proxy_subtitles_access WHERE user_id = %s;"),
        ("today_regenerate_limits", "DELETE FROM bt_3_today_regenerate_limits WHERE user_id = %s;"),
        ("default_topics", "DELETE FROM bt_3_default_topics WHERE user_id = %s;"),
        ("telegram_quiz_attempts", "DELETE FROM bt_3_telegram_quiz_attempts WHERE user_id = %s;"),
        ("webapp_group_contexts", "DELETE FROM bt_3_webapp_group_contexts WHERE user_id = %s;"),
        ("webapp_scope_state", "DELETE FROM bt_3_webapp_scope_state WHERE user_id = %s;"),
        ("webapp_instance_leases", "DELETE FROM bt_3_webapp_instance_leases WHERE user_id = %s;"),
        ("webapp_checks", "DELETE FROM bt_3_webapp_checks WHERE user_id = %s;"),
        ("user_language_profile", "DELETE FROM bt_3_user_language_profile WHERE user_id = %s;"),
        ("skill_state_v2_dirty", "DELETE FROM bt_3_skill_state_v2_dirty WHERE user_id = %s;"),
        ("user_skill_state_v2", "DELETE FROM bt_3_user_skill_state_v2 WHERE user_id = %s;"),
        ("user_skill_state", "DELETE FROM bt_3_user_skill_state WHERE user_id = %s;"),
        ("daily_sentences", "DELETE FROM bt_3_daily_sentences WHERE user_id = %s;"),
        ("detailed_mistakes", "DELETE FROM bt_3_detailed_mistakes WHERE user_id = %s;"),
        ("attempts", "DELETE FROM bt_3_attempts WHERE user_id = %s;"),
        ("successful_translations", "DELETE FROM bt_3_successful_translations WHERE user_id = %s;"),
        ("translations", "DELETE FROM bt_3_translations WHERE user_id = %s;"),
        ("messages", "DELETE FROM bt_3_messages WHERE user_id = %s;"),
        ("conversation_errors", "DELETE FROM bt_3_conversation_errors WHERE user_id = %s;"),
        ("bookmarks", "DELETE FROM bt_3_bookmarks WHERE user_id = %s;"),
        ("user_progress", "DELETE FROM bt_3_user_progress WHERE user_id = %s;"),
        ("translation_errors", "DELETE FROM bt_3_translation_errors WHERE user_id = %s;"),
        ("access_allowed", "DELETE FROM bt_3_allowed_users WHERE user_id = %s;"),
    ]

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    user_id,
                    username,
                    revoked_at,
                    grace_until,
                    status,
                    scheduled_by,
                    reason,
                    notification_sent_at,
                    notification_message_refs,
                    decision_at,
                    decision_by,
                    decision_note,
                    purged_at,
                    purge_summary,
                    billing_cancel_snapshot,
                    created_at,
                    updated_at
                FROM bt_3_user_removal_queue
                WHERE user_id = %s
                FOR UPDATE;
                """,
                (int(user_id),),
            )
            existing_row = cursor.fetchone()
            if not existing_row:
                return None
            existing = _serialize_user_removal_row(existing_row)
            if not existing:
                return None
            if existing.get("status") == "purged":
                return existing

            purge_summary: dict[str, Any] = {}
            total_deleted_rows = 0
            for label, query in delete_statements:
                cursor.execute(query, (int(user_id),))
                deleted_rows = int(cursor.rowcount or 0)
                purge_summary[label] = deleted_rows
                total_deleted_rows += deleted_rows

            purge_summary["total_deleted_rows"] = total_deleted_rows
            purge_summary["grace_days"] = USER_REMOVAL_GRACE_DAYS

            cursor.execute(
                """
                UPDATE bt_3_user_removal_queue
                SET
                    status = 'purged',
                    decision_at = NOW(),
                    decision_by = %s,
                    decision_note = %s,
                    purged_at = NOW(),
                    purge_summary = %s,
                    updated_at = NOW()
                WHERE user_id = %s
                RETURNING
                    user_id,
                    username,
                    revoked_at,
                    grace_until,
                    status,
                    scheduled_by,
                    reason,
                    notification_sent_at,
                    notification_message_refs,
                    decision_at,
                    decision_by,
                    decision_note,
                    purged_at,
                    purge_summary,
                    billing_cancel_snapshot,
                    created_at,
                    updated_at;
                """,
                (
                    int(approved_by) if approved_by is not None else None,
                    normalized_note,
                    Json(purge_summary),
                    int(user_id),
                ),
            )
            row = cursor.fetchone()
    return _serialize_user_removal_row(row)


def list_allowed_telegram_users(limit: int = 100) -> list[dict]:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT user_id, username, added_by, note, created_at, updated_at
                FROM bt_3_allowed_users
                ORDER BY updated_at DESC
                LIMIT %s;
                """,
                (max(1, min(int(limit), 500)),),
            )
            rows = cursor.fetchall()

    return [
        {
            "user_id": row[0],
            "username": row[1],
            "added_by": row[2],
            "note": row[3],
            "created_at": row[4].isoformat() if row[4] else None,
            "updated_at": row[5].isoformat() if row[5] else None,
        }
        for row in rows
    ]


def _normalize_lang_code(value: str | None) -> str:
    return str(value or "").strip().lower()


def get_user_language_profile(user_id: int) -> dict:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT learning_language, native_language, updated_at
                FROM bt_3_user_language_profile
                WHERE user_id = %s
                LIMIT 1;
                """,
                (int(user_id),),
            )
            row = cursor.fetchone()
    if not row:
        return {
            "user_id": int(user_id),
            "learning_language": DEFAULT_LEARNING_LANGUAGE,
            "native_language": DEFAULT_NATIVE_LANGUAGE,
            "updated_at": None,
            "has_profile": False,
        }
    learning_language = _normalize_lang_code(row[0]) or DEFAULT_LEARNING_LANGUAGE
    native_language = _normalize_lang_code(row[1]) or DEFAULT_NATIVE_LANGUAGE
    if learning_language not in SUPPORTED_LEARNING_LANGUAGES:
        learning_language = DEFAULT_LEARNING_LANGUAGE
    if native_language not in SUPPORTED_NATIVE_LANGUAGES:
        native_language = DEFAULT_NATIVE_LANGUAGE
    return {
        "user_id": int(user_id),
        "learning_language": learning_language,
        "native_language": native_language,
        "updated_at": row[2].isoformat() if row[2] else None,
        "has_profile": True,
    }


def upsert_user_language_profile(user_id: int, learning_language: str, native_language: str) -> dict:
    learning = _normalize_lang_code(learning_language)
    native = _normalize_lang_code(native_language)
    if learning not in SUPPORTED_LEARNING_LANGUAGES:
        raise ValueError(f"Unsupported learning language: {learning_language}")
    if native not in SUPPORTED_NATIVE_LANGUAGES:
        raise ValueError(f"Unsupported native language: {native_language}")
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_user_language_profile (user_id, learning_language, native_language, updated_at)
                VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (user_id) DO UPDATE
                SET learning_language = EXCLUDED.learning_language,
                    native_language = EXCLUDED.native_language,
                    updated_at = CURRENT_TIMESTAMP
                RETURNING learning_language, native_language, updated_at;
                """,
                (int(user_id), learning, native),
            )
            row = cursor.fetchone()
    return {
        "user_id": int(user_id),
        "learning_language": row[0],
        "native_language": row[1],
        "updated_at": row[2].isoformat() if row[2] else None,
        "has_profile": True,
    }


def get_user_progress_reset(
    user_id: int,
    *,
    source_lang: str,
    target_lang: str,
) -> dict:
    normalized_source = str(source_lang or "ru").strip().lower() or "ru"
    normalized_target = str(target_lang or "de").strip().lower() or "de"
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT reset_date, created_at, updated_at
                FROM bt_3_user_progress_resets
                WHERE user_id = %s
                  AND source_lang = %s
                  AND target_lang = %s
                LIMIT 1;
                """,
                (int(user_id), normalized_source, normalized_target),
            )
            row = cursor.fetchone()
    if not row:
        return {
            "user_id": int(user_id),
            "source_lang": normalized_source,
            "target_lang": normalized_target,
            "has_reset": False,
            "reset_date": None,
            "created_at": None,
            "updated_at": None,
        }
    return {
        "user_id": int(user_id),
        "source_lang": normalized_source,
        "target_lang": normalized_target,
        "has_reset": True,
        "reset_date": row[0].isoformat() if row[0] else None,
        "created_at": row[1].isoformat() if row[1] else None,
        "updated_at": row[2].isoformat() if row[2] else None,
    }


def upsert_user_progress_reset(
    user_id: int,
    *,
    source_lang: str,
    target_lang: str,
    reset_date: date,
) -> dict:
    normalized_source = str(source_lang or "ru").strip().lower() or "ru"
    normalized_target = str(target_lang or "de").strip().lower() or "de"
    if not isinstance(reset_date, date):
        raise ValueError("reset_date must be a date")
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_user_progress_resets (
                    user_id,
                    source_lang,
                    target_lang,
                    reset_date,
                    created_at,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, NOW(), NOW())
                ON CONFLICT (user_id, source_lang, target_lang) DO UPDATE
                SET reset_date = EXCLUDED.reset_date,
                    updated_at = NOW()
                RETURNING reset_date, created_at, updated_at;
                """,
                (int(user_id), normalized_source, normalized_target, reset_date),
            )
            row = cursor.fetchone()
    return {
        "user_id": int(user_id),
        "source_lang": normalized_source,
        "target_lang": normalized_target,
        "has_reset": True,
        "reset_date": row[0].isoformat() if row and row[0] else reset_date.isoformat(),
        "created_at": row[1].isoformat() if row and row[1] else None,
        "updated_at": row[2].isoformat() if row and row[2] else None,
    }


def get_user_progress_reset_date(
    user_id: int,
    *,
    source_lang: str,
    target_lang: str,
) -> date | None:
    reset = get_user_progress_reset(
        user_id=user_id,
        source_lang=source_lang,
        target_lang=target_lang,
    )
    raw_reset_date = reset.get("reset_date")
    if not raw_reset_date:
        return None
    try:
        return date.fromisoformat(str(raw_reset_date))
    except Exception:
        return None


def _resolve_progress_window_start(
    user_id: int,
    *,
    source_lang: str,
    target_lang: str,
    start_date: date,
) -> date:
    reset_date = get_user_progress_reset_date(
        user_id=user_id,
        source_lang=source_lang,
        target_lang=target_lang,
    )
    if reset_date is None:
        return start_date
    return max(start_date, reset_date)


def _normalize_webapp_scope_kind(value: str | None) -> str:
    kind = str(value or "").strip().lower()
    if kind == "group":
        return "group"
    return "personal"


def _build_webapp_scope_key(scope_kind: str, scope_chat_id: int | None) -> str:
    if scope_kind == "group" and scope_chat_id is not None:
        return f"group:{int(scope_chat_id)}"
    return "personal"


def _normalize_webapp_instance_id(value: str | None) -> str:
    raw = str(value or "").strip()
    if not raw:
        raise ValueError("instance_id обязателен")
    compact = re.sub(r"[^a-zA-Z0-9._:-]+", "-", raw).strip("-")
    if not compact:
        raise ValueError("instance_id пуст после нормализации")
    return compact[:128]


def _map_webapp_instance_lease_row(row: Any, *, user_id: int | None = None) -> dict | None:
    if not row:
        return None
    resolved_user_id = int(row[0]) if row[0] is not None else int(user_id or 0)
    if resolved_user_id <= 0:
        return None
    return {
        "user_id": resolved_user_id,
        "instance_id": str(row[1] or "").strip(),
        "session_id": str(row[2] or "").strip() or None,
        "platform": str(row[3] or "").strip() or None,
        "app_context": str(row[4] or "").strip() or None,
        "claimed_at": row[5].isoformat() if row[5] else None,
        "last_seen_at": row[6].isoformat() if row[6] else None,
        "updated_at": row[7].isoformat() if row[7] else None,
    }


def get_webapp_instance_lease(user_id: int) -> dict | None:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    user_id,
                    instance_id,
                    session_id,
                    platform,
                    app_context,
                    claimed_at,
                    last_seen_at,
                    updated_at
                FROM bt_3_webapp_instance_leases
                WHERE user_id = %s
                LIMIT 1;
                """,
                (int(user_id),),
            )
            return _map_webapp_instance_lease_row(cursor.fetchone(), user_id=int(user_id))


def claim_webapp_instance_lease(
    *,
    user_id: int,
    instance_id: str,
    session_id: str | None = None,
    platform: str | None = None,
    app_context: str | None = None,
) -> dict:
    normalized_instance_id = _normalize_webapp_instance_id(instance_id)
    normalized_session_id = str(session_id or "").strip()[:128] or None
    normalized_platform = str(platform or "").strip()[:64] or None
    normalized_app_context = str(app_context or "").strip()[:64] or None
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_webapp_instance_leases (
                    user_id,
                    instance_id,
                    session_id,
                    platform,
                    app_context,
                    claimed_at,
                    last_seen_at,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, NOW(), NOW(), NOW())
                ON CONFLICT (user_id) DO UPDATE
                SET
                    instance_id = EXCLUDED.instance_id,
                    session_id = EXCLUDED.session_id,
                    platform = EXCLUDED.platform,
                    app_context = EXCLUDED.app_context,
                    last_seen_at = NOW(),
                    updated_at = NOW()
                RETURNING
                    user_id,
                    instance_id,
                    session_id,
                    platform,
                    app_context,
                    claimed_at,
                    last_seen_at,
                    updated_at;
                """,
                (
                    int(user_id),
                    normalized_instance_id,
                    normalized_session_id,
                    normalized_platform,
                    normalized_app_context,
                ),
            )
            row = cursor.fetchone()
    lease = _map_webapp_instance_lease_row(row, user_id=int(user_id))
    if not lease:
        raise RuntimeError("Не удалось сохранить lease WebApp instance")
    return lease


def release_webapp_instance_lease(*, user_id: int, instance_id: str) -> bool:
    normalized_instance_id = _normalize_webapp_instance_id(instance_id)
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                DELETE FROM bt_3_webapp_instance_leases
                WHERE user_id = %s
                  AND instance_id = %s;
                """,
                (int(user_id), normalized_instance_id),
            )
            return bool(cursor.rowcount)


def _map_user_api_snapshot_row(row) -> dict | None:
    if not row:
        return None
    return {
        "user_id": int(row[0]),
        "snapshot_kind": str(row[1] or "").strip(),
        "snapshot_key": str(row[2] or "").strip(),
        "source_lang": str(row[3] or "").strip() or None,
        "target_lang": str(row[4] or "").strip() or None,
        "payload": row[5] if isinstance(row[5], dict) else {},
        "meta": row[6] if isinstance(row[6], dict) else {},
        "refreshed_at": row[7].isoformat() if row[7] else None,
        "fresh_until_at": row[8].isoformat() if row[8] else None,
        "stale_until_at": row[9].isoformat() if row[9] else None,
        "updated_at": row[10].isoformat() if row[10] else None,
    }


def get_user_api_snapshot(
    *,
    user_id: int,
    snapshot_kind: str,
    snapshot_key: str,
) -> dict | None:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    user_id,
                    snapshot_kind,
                    snapshot_key,
                    source_lang,
                    target_lang,
                    payload,
                    meta,
                    refreshed_at,
                    fresh_until_at,
                    stale_until_at,
                    updated_at
                FROM bt_3_user_api_snapshots
                WHERE user_id = %s
                  AND snapshot_kind = %s
                  AND snapshot_key = %s
                LIMIT 1;
                """,
                (int(user_id), str(snapshot_kind or "").strip(), str(snapshot_key or "").strip()),
            )
            return _map_user_api_snapshot_row(cursor.fetchone())


def upsert_user_api_snapshot(
    *,
    user_id: int,
    snapshot_kind: str,
    snapshot_key: str,
    payload: dict,
    source_lang: str | None = None,
    target_lang: str | None = None,
    meta: dict | None = None,
    fresh_ttl_seconds: int = 30,
    stale_ttl_seconds: int = 300,
) -> dict | None:
    normalized_kind = str(snapshot_kind or "").strip()
    normalized_key = str(snapshot_key or "").strip()
    if not normalized_kind or not normalized_key:
        raise ValueError("snapshot_kind and snapshot_key are required")
    safe_payload = payload if isinstance(payload, dict) else {}
    safe_meta = meta if isinstance(meta, dict) else {}
    safe_fresh_ttl = max(1, int(fresh_ttl_seconds or 1))
    safe_stale_ttl = max(safe_fresh_ttl, int(stale_ttl_seconds or safe_fresh_ttl))
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_user_api_snapshots (
                    user_id,
                    snapshot_kind,
                    snapshot_key,
                    source_lang,
                    target_lang,
                    payload,
                    meta,
                    refreshed_at,
                    fresh_until_at,
                    stale_until_at,
                    updated_at
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb,
                    NOW(),
                    NOW() + (%s * INTERVAL '1 second'),
                    NOW() + (%s * INTERVAL '1 second'),
                    NOW()
                )
                ON CONFLICT (user_id, snapshot_kind, snapshot_key) DO UPDATE
                SET
                    source_lang = EXCLUDED.source_lang,
                    target_lang = EXCLUDED.target_lang,
                    payload = EXCLUDED.payload,
                    meta = EXCLUDED.meta,
                    refreshed_at = EXCLUDED.refreshed_at,
                    fresh_until_at = EXCLUDED.fresh_until_at,
                    stale_until_at = EXCLUDED.stale_until_at,
                    updated_at = NOW()
                RETURNING
                    user_id,
                    snapshot_kind,
                    snapshot_key,
                    source_lang,
                    target_lang,
                    payload,
                    meta,
                    refreshed_at,
                    fresh_until_at,
                    stale_until_at,
                    updated_at;
                """,
                (
                    int(user_id),
                    normalized_kind,
                    normalized_key,
                    str(source_lang or "").strip() or None,
                    str(target_lang or "").strip() or None,
                    json.dumps(safe_payload, ensure_ascii=False),
                    json.dumps(safe_meta, ensure_ascii=False),
                    safe_fresh_ttl,
                    safe_stale_ttl,
                ),
            )
            return _map_user_api_snapshot_row(cursor.fetchone())


def mark_user_api_snapshots_stale(
    *,
    user_id: int,
    snapshot_kind: str | None = None,
    snapshot_key: str | None = None,
) -> int:
    conditions = ["user_id = %s"]
    params: list[Any] = [int(user_id)]
    normalized_kind = str(snapshot_kind or "").strip()
    normalized_key = str(snapshot_key or "").strip()
    if normalized_kind:
        conditions.append("snapshot_kind = %s")
        params.append(normalized_kind)
    if normalized_key:
        conditions.append("snapshot_key = %s")
        params.append(normalized_key)
    where_sql = " AND ".join(conditions)
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                UPDATE bt_3_user_api_snapshots
                SET
                    fresh_until_at = LEAST(fresh_until_at, NOW() - INTERVAL '1 second'),
                    updated_at = NOW()
                WHERE {where_sql};
                """,
                params,
            )
            return int(cursor.rowcount or 0)


def get_webapp_scope_state(user_id: int) -> dict:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    s.scope_kind,
                    s.scope_chat_id,
                    g.chat_title,
                    s.updated_at
                FROM bt_3_webapp_scope_state s
                LEFT JOIN bt_3_webapp_group_contexts g
                  ON g.user_id = s.user_id
                 AND g.chat_id = s.scope_chat_id
                WHERE s.user_id = %s
                LIMIT 1;
                """,
                (int(user_id),),
            )
            row = cursor.fetchone()

    if not row:
        return {
            "user_id": int(user_id),
            "scope_kind": "personal",
            "scope_chat_id": None,
            "scope_chat_title": None,
            "scope_key": "personal",
            "updated_at": None,
            "has_state": False,
        }

    scope_kind = _normalize_webapp_scope_kind(row[0])
    scope_chat_id = int(row[1]) if row[1] is not None else None
    scope_chat_title = str(row[2] or "").strip() or None
    return {
        "user_id": int(user_id),
        "scope_kind": scope_kind,
        "scope_chat_id": scope_chat_id,
        "scope_chat_title": scope_chat_title,
        "scope_key": _build_webapp_scope_key(scope_kind, scope_chat_id),
        "updated_at": row[3].isoformat() if row[3] else None,
        "has_state": True,
    }


def upsert_webapp_scope_state(
    *,
    user_id: int,
    scope_kind: str,
    scope_chat_id: int | None = None,
) -> dict:
    normalized_kind = _normalize_webapp_scope_kind(scope_kind)
    normalized_chat_id = int(scope_chat_id) if scope_chat_id is not None else None
    if normalized_kind == "group" and normalized_chat_id is None:
        raise ValueError("scope_chat_id обязателен для group scope")
    if normalized_kind == "personal":
        normalized_chat_id = None

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_webapp_scope_state (
                    user_id,
                    scope_kind,
                    scope_chat_id,
                    updated_at
                )
                VALUES (%s, %s, %s, NOW())
                ON CONFLICT (user_id) DO UPDATE
                SET
                    scope_kind = EXCLUDED.scope_kind,
                    scope_chat_id = EXCLUDED.scope_chat_id,
                    updated_at = NOW()
                RETURNING scope_kind, scope_chat_id, updated_at;
                """,
                (int(user_id), normalized_kind, normalized_chat_id),
            )
            row = cursor.fetchone()

    resolved_kind = _normalize_webapp_scope_kind(row[0] if row else normalized_kind)
    resolved_chat_id = int(row[1]) if row and row[1] is not None else None
    return {
        "user_id": int(user_id),
        "scope_kind": resolved_kind,
        "scope_chat_id": resolved_chat_id,
        "scope_key": _build_webapp_scope_key(resolved_kind, resolved_chat_id),
        "updated_at": row[2].isoformat() if row and row[2] else None,
        "has_state": True,
    }


def upsert_webapp_group_context(
    *,
    user_id: int,
    chat_id: int,
    chat_type: str | None = None,
    chat_title: str | None = None,
) -> dict:
    normalized_chat_type = str(chat_type or "").strip().lower()
    if normalized_chat_type not in {"group", "supergroup"}:
        normalized_chat_type = "group"
    normalized_chat_title = str(chat_title or "").strip() or None

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_webapp_group_contexts (
                    user_id,
                    chat_id,
                    chat_type,
                    chat_title,
                    participation_confirmed,
                    participation_confirmed_at,
                    participation_confirmed_source,
                    first_seen_at,
                    last_seen_at
                )
                VALUES (%s, %s, %s, %s, FALSE, NULL, NULL, NOW(), NOW())
                ON CONFLICT (user_id, chat_id) DO UPDATE
                SET
                    chat_type = EXCLUDED.chat_type,
                    chat_title = COALESCE(NULLIF(EXCLUDED.chat_title, ''), bt_3_webapp_group_contexts.chat_title),
                    last_seen_at = NOW()
                RETURNING
                    user_id,
                    chat_id,
                    chat_type,
                    chat_title,
                    participation_confirmed,
                    participation_confirmed_at,
                    participation_confirmed_source,
                    first_seen_at,
                    last_seen_at;
                """,
                (int(user_id), int(chat_id), normalized_chat_type, normalized_chat_title),
            )
            row = cursor.fetchone()

    return {
        "user_id": int(row[0]),
        "chat_id": int(row[1]),
        "chat_type": str(row[2] or "group"),
        "chat_title": str(row[3] or "").strip() or None,
        "participation_confirmed": bool(row[4]),
        "participation_confirmed_at": row[5].isoformat() if row[5] else None,
        "participation_confirmed_source": str(row[6] or "").strip() or None,
        "first_seen_at": row[7].isoformat() if row[7] else None,
        "last_seen_at": row[8].isoformat() if row[8] else None,
    }


def confirm_webapp_group_participation(
    *,
    user_id: int,
    chat_id: int,
    chat_type: str | None = None,
    chat_title: str | None = None,
    source: str | None = None,
) -> dict:
    normalized_chat_type = str(chat_type or "").strip().lower()
    if normalized_chat_type not in {"group", "supergroup"}:
        normalized_chat_type = "group"
    normalized_chat_title = str(chat_title or "").strip() or None
    normalized_source = str(source or "").strip() or None

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT participation_confirmed
                FROM bt_3_webapp_group_contexts
                WHERE user_id = %s AND chat_id = %s
                LIMIT 1;
                """,
                (int(user_id), int(chat_id)),
            )
            previous = cursor.fetchone()

            cursor.execute(
                """
                INSERT INTO bt_3_webapp_group_contexts (
                    user_id,
                    chat_id,
                    chat_type,
                    chat_title,
                    participation_confirmed,
                    participation_confirmed_at,
                    participation_confirmed_source,
                    first_seen_at,
                    last_seen_at
                )
                VALUES (%s, %s, %s, %s, TRUE, NOW(), %s, NOW(), NOW())
                ON CONFLICT (user_id, chat_id) DO UPDATE
                SET
                    chat_type = EXCLUDED.chat_type,
                    chat_title = COALESCE(NULLIF(EXCLUDED.chat_title, ''), bt_3_webapp_group_contexts.chat_title),
                    participation_confirmed = TRUE,
                    participation_confirmed_at = COALESCE(
                        bt_3_webapp_group_contexts.participation_confirmed_at,
                        NOW()
                    ),
                    participation_confirmed_source = COALESCE(
                        EXCLUDED.participation_confirmed_source,
                        bt_3_webapp_group_contexts.participation_confirmed_source
                    ),
                    last_seen_at = NOW()
                RETURNING
                    user_id,
                    chat_id,
                    chat_type,
                    chat_title,
                    participation_confirmed,
                    participation_confirmed_at,
                    participation_confirmed_source,
                    first_seen_at,
                    last_seen_at;
                """,
                (
                    int(user_id),
                    int(chat_id),
                    normalized_chat_type,
                    normalized_chat_title,
                    normalized_source,
                ),
            )
            row = cursor.fetchone()

    was_confirmed_before = bool(previous and previous[0])
    return {
        "user_id": int(row[0]),
        "chat_id": int(row[1]),
        "chat_type": str(row[2] or "group"),
        "chat_title": str(row[3] or "").strip() or None,
        "participation_confirmed": bool(row[4]),
        "participation_confirmed_at": row[5].isoformat() if row[5] else None,
        "participation_confirmed_source": str(row[6] or "").strip() or None,
        "first_seen_at": row[7].isoformat() if row[7] else None,
        "last_seen_at": row[8].isoformat() if row[8] else None,
        "was_confirmed_before": was_confirmed_before,
    }


def list_webapp_group_contexts(
    user_id: int,
    limit: int = 20,
    *,
    only_confirmed: bool = False,
) -> list[dict]:
    safe_limit = max(1, min(int(limit or 20), 100))
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    user_id,
                    chat_id,
                    chat_type,
                    chat_title,
                    participation_confirmed,
                    participation_confirmed_at,
                    participation_confirmed_source,
                    first_seen_at,
                    last_seen_at
                FROM bt_3_webapp_group_contexts
                WHERE user_id = %s
                  AND (%s = FALSE OR participation_confirmed = TRUE)
                ORDER BY last_seen_at DESC, chat_id DESC
                LIMIT %s;
                """,
                (int(user_id), bool(only_confirmed), safe_limit),
            )
            rows = cursor.fetchall()
    return [
        {
            "user_id": int(row[0]),
            "chat_id": int(row[1]),
            "chat_type": str(row[2] or "group"),
            "chat_title": str(row[3] or "").strip() or None,
            "participation_confirmed": bool(row[4]),
            "participation_confirmed_at": row[5].isoformat() if row[5] else None,
            "participation_confirmed_source": str(row[6] or "").strip() or None,
            "first_seen_at": row[7].isoformat() if row[7] else None,
            "last_seen_at": row[8].isoformat() if row[8] else None,
        }
        for row in rows
    ]


def list_webapp_group_member_user_ids(
    chat_id: int,
    limit: int = 2000,
    *,
    only_confirmed: bool = True,
) -> list[int]:
    safe_limit = max(1, min(int(limit or 2000), 10000))
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT user_id
                FROM bt_3_webapp_group_contexts
                WHERE chat_id = %s
                  AND (%s = FALSE OR participation_confirmed = TRUE)
                ORDER BY last_seen_at DESC, user_id DESC
                LIMIT %s;
                """,
                (int(chat_id), bool(only_confirmed), safe_limit),
            )
            rows = cursor.fetchall() or []
    seen: set[int] = set()
    result: list[int] = []
    for row in rows:
        try:
            candidate = int(row[0])
        except Exception:
            continue
        if candidate in seen:
            continue
        seen.add(candidate)
        result.append(candidate)
    return result


def list_known_webapp_group_chats(limit: int = 500) -> list[dict]:
    safe_limit = max(1, min(int(limit or 500), 5000))
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT chat_id, chat_type, chat_title, last_seen_at
                FROM (
                    SELECT DISTINCT ON (chat_id)
                        chat_id,
                        chat_type,
                        chat_title,
                        last_seen_at
                    FROM bt_3_webapp_group_contexts
                    ORDER BY chat_id, last_seen_at DESC
                ) latest
                ORDER BY last_seen_at DESC, chat_id DESC
                LIMIT %s;
                """,
                (safe_limit,),
            )
            rows = cursor.fetchall() or []
    result: list[dict] = []
    for row in rows:
        try:
            chat_id = int(row[0])
        except Exception:
            continue
        result.append(
            {
                "chat_id": chat_id,
                "chat_type": str(row[1] or "group"),
                "chat_title": str(row[2] or "").strip() or None,
                "last_seen_at": row[3].isoformat() if row[3] else None,
            }
        )
    return result


def create_access_request(
    user_id: int,
    username: str | None = None,
    requested_via: str = "bot",
) -> int:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_access_requests (user_id, username, status, requested_via)
                VALUES (%s, %s, 'pending', %s)
                RETURNING id;
                """,
                (int(user_id), username, requested_via),
            )
            row = cursor.fetchone()
    return int(row[0])


def get_access_request_by_id(request_id: int) -> dict | None:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT id, user_id, username, status, requested_via, created_at, reviewed_at, reviewed_by, review_note
                FROM bt_3_access_requests
                WHERE id = %s
                LIMIT 1;
                """,
                (int(request_id),),
            )
            row = cursor.fetchone()

    if not row:
        return None
    return {
        "id": row[0],
        "user_id": row[1],
        "username": row[2],
        "status": row[3],
        "requested_via": row[4],
        "created_at": row[5].isoformat() if row[5] else None,
        "reviewed_at": row[6].isoformat() if row[6] else None,
        "reviewed_by": row[7],
        "review_note": row[8],
    }


def resolve_access_request(
    request_id: int,
    status: str,
    reviewed_by: int,
    review_note: str | None = None,
) -> dict | None:
    final_status = (status or "").strip().lower()
    if final_status not in {"approved", "rejected"}:
        raise ValueError("status must be approved or rejected")

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE bt_3_access_requests
                SET
                    status = %s,
                    reviewed_at = CURRENT_TIMESTAMP,
                    reviewed_by = %s,
                    review_note = %s
                WHERE id = %s AND status = 'pending'
                RETURNING id, user_id, username, status, requested_via, created_at, reviewed_at, reviewed_by, review_note;
                """,
                (final_status, int(reviewed_by), review_note, int(request_id)),
            )
            row = cursor.fetchone()

    if not row:
        return None
    return {
        "id": row[0],
        "user_id": row[1],
        "username": row[2],
        "status": row[3],
        "requested_via": row[4],
        "created_at": row[5].isoformat() if row[5] else None,
        "reviewed_at": row[6].isoformat() if row[6] else None,
        "reviewed_by": row[7],
        "review_note": row[8],
    }


def resolve_latest_pending_access_request_for_user(
    user_id: int,
    status: str,
    reviewed_by: int,
    review_note: str | None = None,
) -> dict | None:
    final_status = (status or "").strip().lower()
    if final_status not in {"approved", "rejected"}:
        raise ValueError("status must be approved or rejected")

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                WITH target AS (
                    SELECT id
                    FROM bt_3_access_requests
                    WHERE user_id = %s AND status = 'pending'
                    ORDER BY created_at DESC
                    LIMIT 1
                )
                UPDATE bt_3_access_requests req
                SET
                    status = %s,
                    reviewed_at = CURRENT_TIMESTAMP,
                    reviewed_by = %s,
                    review_note = %s
                FROM target
                WHERE req.id = target.id
                RETURNING req.id, req.user_id, req.username, req.status, req.requested_via, req.created_at, req.reviewed_at, req.reviewed_by, req.review_note;
                """,
                (int(user_id), final_status, int(reviewed_by), review_note),
            )
            row = cursor.fetchone()

    if not row:
        return None
    return {
        "id": row[0],
        "user_id": row[1],
        "username": row[2],
        "status": row[3],
        "requested_via": row[4],
        "created_at": row[5].isoformat() if row[5] else None,
        "reviewed_at": row[6].isoformat() if row[6] else None,
        "reviewed_by": row[7],
        "review_note": row[8],
    }


def list_pending_access_requests(limit: int = 20) -> list[dict]:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT id, user_id, username, status, requested_via, created_at
                FROM bt_3_access_requests
                WHERE status = 'pending'
                ORDER BY created_at DESC
                LIMIT %s;
                """,
                (max(1, min(int(limit), 100)),),
            )
            rows = cursor.fetchall()

    return [
        {
            "id": row[0],
            "user_id": row[1],
            "username": row[2],
            "status": row[3],
            "requested_via": row[4],
            "created_at": row[5].isoformat() if row[5] else None,
        }
        for row in rows
    ]


def save_webapp_translation(
    user_id: int,
    username: str | None,
    session_id: str | None,
    original_text: str,
    user_translation: str,
    result: str,
    source_lang: str | None = None,
    target_lang: str | None = None,
) -> None:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO bt_3_webapp_checks (
                    user_id,
                    username,
                    session_id,
                    original_text,
                    user_translation,
                    result,
                    source_lang,
                    target_lang
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
            """, (
                user_id,
                username,
                session_id,
                original_text,
                user_translation,
                result,
                source_lang,
                target_lang,
            ))


def get_webapp_translation_history(user_id: int, limit: int = 20) -> list[dict]:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT
                    id,
                    session_id,
                    original_text,
                    user_translation,
                    result,
                    source_lang,
                    target_lang,
                    created_at
                FROM bt_3_webapp_checks
                WHERE user_id = %s
                ORDER BY created_at DESC
                LIMIT %s;
            """, (user_id, limit))
            rows = cursor.fetchall()
            return [
                {
                    "id": row[0],
                    "session_id": row[1],
                    "original_text": row[2],
                    "user_translation": row[3],
                    "result": row[4],
                    "source_lang": row[5],
                    "target_lang": row[6],
                    "created_at": row[7].isoformat() if row[7] else None,
                }
                for row in rows
            ]


def _map_translation_check_session_row(row) -> dict | None:
    if not row:
        return None
    return {
        "id": int(row[0]),
        "user_id": int(row[1]),
        "username": str(row[2] or "") or None,
        "source_session_id": str(row[3] or "") or None,
        "source_lang": str(row[4] or "") or None,
        "target_lang": str(row[5] or "") or None,
        "status": str(row[6] or "queued"),
        "total_items": int(row[7] or 0),
        "completed_items": int(row[8] or 0),
        "failed_items": int(row[9] or 0),
        "send_private_grammar_text": bool(row[10]),
        "original_text_bundle": str(row[11] or "") or None,
        "user_translation_bundle": str(row[12] or "") or None,
        "last_error": str(row[13] or "") or None,
        "started_at": row[14].isoformat() if row[14] else None,
        "finished_at": row[15].isoformat() if row[15] else None,
        "created_at": row[16].isoformat() if row[16] else None,
        "updated_at": row[17].isoformat() if row[17] else None,
    }


def _map_translation_check_session_status_row(row) -> dict | None:
    if not row:
        return None
    return {
        "id": int(row[0]),
        "user_id": int(row[1]),
        "username": str(row[2] or "") or None,
        "source_session_id": str(row[3] or "") or None,
        "source_lang": str(row[4] or "") or None,
        "target_lang": str(row[5] or "") or None,
        "status": str(row[6] or "queued"),
        "total_items": int(row[7] or 0),
        "completed_items": int(row[8] or 0),
        "failed_items": int(row[9] or 0),
        "send_private_grammar_text": bool(row[10]),
        "original_text_bundle": None,
        "user_translation_bundle": None,
        "last_error": str(row[11] or "") or None,
        "started_at": row[12].isoformat() if row[12] else None,
        "finished_at": row[13].isoformat() if row[13] else None,
        "created_at": row[14].isoformat() if row[14] else None,
        "updated_at": row[15].isoformat() if row[15] else None,
    }


def _map_translation_check_session_runtime_row(row) -> dict | None:
    if not row:
        return None
    return {
        "id": int(row[0]),
        "user_id": int(row[1]),
        "status": str(row[2] or "queued"),
        "last_error": str(row[3] or "") or None,
        "dispatched_job_id": str(row[4] or "") or None,
        "dispatched_at": row[5].isoformat() if row[5] else None,
        "worker_job_id": str(row[6] or "") or None,
        "heartbeat_at": row[7].isoformat() if row[7] else None,
        "started_at": row[8].isoformat() if row[8] else None,
        "finished_at": row[9].isoformat() if row[9] else None,
        "created_at": row[10].isoformat() if row[10] else None,
        "updated_at": row[11].isoformat() if row[11] else None,
    }


def _map_translation_check_item_row(row) -> dict | None:
    if not row:
        return None
    return {
        "id": int(row[0]),
        "check_session_id": int(row[1]),
        "item_order": int(row[2] or 0),
        "sentence_number": int(row[3]) if row[3] is not None else None,
        "sentence_id_for_mistake_table": int(row[4]) if row[4] is not None else None,
        "original_text": str(row[5] or ""),
        "user_translation": str(row[6] or ""),
        "status": str(row[7] or "pending"),
        "result_json": row[8] if isinstance(row[8], dict) else None,
        "result_text": str(row[9] or "") or None,
        "error_text": str(row[10] or "") or None,
        "webapp_check_id": int(row[11]) if row[11] is not None else None,
        "started_at": row[12].isoformat() if row[12] else None,
        "finished_at": row[13].isoformat() if row[13] else None,
        "created_at": row[14].isoformat() if row[14] else None,
        "updated_at": row[15].isoformat() if row[15] else None,
    }


def create_translation_check_session(
    *,
    user_id: int,
    username: str | None,
    source_session_id: str | None,
    source_lang: str | None,
    target_lang: str | None,
    items: list[dict],
    send_private_grammar_text: bool = False,
    original_text_bundle: str | None = None,
    user_translation_bundle: str | None = None,
) -> dict | None:
    normalized_items = items if isinstance(items, list) else []
    total_items = len(normalized_items)
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_translation_check_sessions (
                    user_id,
                    username,
                    source_session_id,
                    source_lang,
                    target_lang,
                    status,
                    total_items,
                    completed_items,
                    failed_items,
                    send_private_grammar_text,
                    original_text_bundle,
                    user_translation_bundle,
                    created_at,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, 'queued', %s, 0, 0, %s, %s, %s, NOW(), NOW())
                RETURNING
                    id, user_id, username, source_session_id, source_lang, target_lang,
                    status, total_items, completed_items, failed_items, send_private_grammar_text,
                    original_text_bundle, user_translation_bundle, last_error, started_at,
                    finished_at, created_at, updated_at;
                """,
                (
                    int(user_id),
                    str(username or "").strip() or None,
                    str(source_session_id or "").strip() or None,
                    str(source_lang or "").strip().lower() or None,
                    str(target_lang or "").strip().lower() or None,
                    max(0, int(total_items)),
                    bool(send_private_grammar_text),
                    str(original_text_bundle or "").strip() or None,
                    str(user_translation_bundle or "").strip() or None,
                ),
            )
            session_row = cursor.fetchone()
            if not session_row:
                return None
            session_id = int(session_row[0])

            if normalized_items:
                item_rows: list[tuple[int, int, int | None, int | None, str, str]] = []
                for index, item in enumerate(normalized_items):
                    item_rows.append(
                        (
                            session_id,
                            int(item.get("item_order", index)),
                            int(item["sentence_number"]) if item.get("sentence_number") is not None else None,
                            int(item["id_for_mistake_table"]) if item.get("id_for_mistake_table") is not None else None,
                            str(item.get("original_text") or "").strip(),
                            str(item.get("translation") or item.get("user_translation") or "").strip(),
                        )
                    )
                execute_values(
                    cursor,
                    """
                    INSERT INTO bt_3_translation_check_items (
                        check_session_id,
                        item_order,
                        sentence_number,
                        sentence_id_for_mistake_table,
                        original_text,
                        user_translation,
                        status,
                        created_at,
                        updated_at
                    )
                    VALUES %s
                    """,
                    item_rows,
                    template="(%s, %s, %s, %s, %s, %s, 'pending', NOW(), NOW())",
                    page_size=200,
                )

    return get_translation_check_session(session_id=session_id, user_id=int(user_id))


def get_translation_check_session(*, session_id: int, user_id: int | None = None) -> dict | None:
    where_sql = "WHERE id = %s"
    params: list = [int(session_id)]
    if user_id is not None:
        where_sql += " AND user_id = %s"
        params.append(int(user_id))
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT
                    id, user_id, username, source_session_id, source_lang, target_lang,
                    status, total_items, completed_items, failed_items, send_private_grammar_text,
                    original_text_bundle, user_translation_bundle, last_error, started_at,
                    finished_at, created_at, updated_at
                FROM bt_3_translation_check_sessions
                {where_sql}
                LIMIT 1;
                """,
                params,
            )
            row = cursor.fetchone()
    return _map_translation_check_session_row(row)


def get_translation_check_session_status(*, session_id: int, user_id: int | None = None) -> dict | None:
    where_sql = "WHERE id = %s"
    params: list = [int(session_id)]
    if user_id is not None:
        where_sql += " AND user_id = %s"
        params.append(int(user_id))
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT
                    id, user_id, username, source_session_id, source_lang, target_lang,
                    status, total_items, completed_items, failed_items, send_private_grammar_text,
                    last_error, started_at, finished_at, created_at, updated_at
                FROM bt_3_translation_check_sessions
                {where_sql}
                LIMIT 1;
                """,
                params,
            )
            row = cursor.fetchone()
    return _map_translation_check_session_status_row(row)


def get_translation_check_session_runtime(*, session_id: int) -> dict | None:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    id, user_id, status, last_error, dispatched_job_id, dispatched_at,
                    worker_job_id, heartbeat_at, started_at, finished_at, created_at, updated_at
                FROM bt_3_translation_check_sessions
                WHERE id = %s
                LIMIT 1;
                """,
                (int(session_id),),
            )
            row = cursor.fetchone()
    return _map_translation_check_session_runtime_row(row)


def list_translation_check_items(*, session_id: int) -> list[dict]:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    id, check_session_id, item_order, sentence_number, sentence_id_for_mistake_table,
                    original_text, user_translation, status, result_json, result_text, error_text,
                    webapp_check_id, started_at, finished_at, created_at, updated_at
                FROM bt_3_translation_check_items
                WHERE check_session_id = %s
                ORDER BY item_order ASC, id ASC;
                """,
                (int(session_id),),
            )
            rows = cursor.fetchall() or []
    return [_map_translation_check_item_row(row) for row in rows if row]


def get_translation_check_session_with_items(
    *,
    session_id: int,
    user_id: int | None = None,
) -> tuple[dict | None, list[dict]]:
    where_sql = "WHERE id = %s"
    params: list = [int(session_id)]
    if user_id is not None:
        where_sql += " AND user_id = %s"
        params.append(int(user_id))
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT
                    id, user_id, username, source_session_id, source_lang, target_lang,
                    status, total_items, completed_items, failed_items, send_private_grammar_text,
                    original_text_bundle, user_translation_bundle, last_error, started_at,
                    finished_at, created_at, updated_at
                FROM bt_3_translation_check_sessions
                {where_sql}
                LIMIT 1;
                """,
                params,
            )
            session_row = cursor.fetchone()
            session = _map_translation_check_session_row(session_row)
            if not session:
                return None, []
            cursor.execute(
                """
                SELECT
                    id, check_session_id, item_order, sentence_number, sentence_id_for_mistake_table,
                    original_text, user_translation, status, result_json, result_text, error_text,
                    webapp_check_id, started_at, finished_at, created_at, updated_at
                FROM bt_3_translation_check_items
                WHERE check_session_id = %s
                ORDER BY item_order ASC, id ASC;
                """,
                (int(session["id"]),),
            )
            rows = cursor.fetchall() or []
    return session, [_map_translation_check_item_row(row) for row in rows if row]


def get_latest_translation_check_session(
    *,
    user_id: int,
    only_active: bool = False,
) -> dict | None:
    status_sql = "AND status IN ('queued', 'running')" if only_active else ""
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT
                    id, user_id, username, source_session_id, source_lang, target_lang,
                    status, total_items, completed_items, failed_items, send_private_grammar_text,
                    original_text_bundle, user_translation_bundle, last_error, started_at,
                    finished_at, created_at, updated_at
                FROM bt_3_translation_check_sessions
                WHERE user_id = %s
                  {status_sql}
                ORDER BY created_at DESC
                LIMIT 1;
                """,
                (int(user_id),),
            )
            row = cursor.fetchone()
    return _map_translation_check_session_row(row)


def get_latest_translation_check_session_status(
    *,
    user_id: int,
    only_active: bool = False,
) -> dict | None:
    status_sql = "AND status IN ('queued', 'running')" if only_active else ""
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT
                    id, user_id, username, source_session_id, source_lang, target_lang,
                    status, total_items, completed_items, failed_items, send_private_grammar_text,
                    last_error, started_at, finished_at, created_at, updated_at
                FROM bt_3_translation_check_sessions
                WHERE user_id = %s
                  {status_sql}
                ORDER BY created_at DESC
                LIMIT 1;
                """,
                (int(user_id),),
            )
            row = cursor.fetchone()
    return _map_translation_check_session_status_row(row)


def get_latest_translation_check_session_with_items(
    *,
    user_id: int,
    only_active: bool = False,
) -> tuple[dict | None, list[dict]]:
    status_sql = "AND status IN ('queued', 'running')" if only_active else ""
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT
                    id, user_id, username, source_session_id, source_lang, target_lang,
                    status, total_items, completed_items, failed_items, send_private_grammar_text,
                    original_text_bundle, user_translation_bundle, last_error, started_at,
                    finished_at, created_at, updated_at
                FROM bt_3_translation_check_sessions
                WHERE user_id = %s
                  {status_sql}
                ORDER BY created_at DESC
                LIMIT 1;
                """,
                (int(user_id),),
            )
            session_row = cursor.fetchone()
            session = _map_translation_check_session_row(session_row)
            if not session:
                return None, []
            cursor.execute(
                """
                SELECT
                    id, check_session_id, item_order, sentence_number, sentence_id_for_mistake_table,
                    original_text, user_translation, status, result_json, result_text, error_text,
                    webapp_check_id, started_at, finished_at, created_at, updated_at
                FROM bt_3_translation_check_items
                WHERE check_session_id = %s
                ORDER BY item_order ASC, id ASC;
                """,
                (int(session["id"]),),
            )
            rows = cursor.fetchall() or []
    return session, [_map_translation_check_item_row(row) for row in rows if row]


def update_translation_check_session_status(
    *,
    session_id: int,
    status: str,
    last_error: str | None = None,
    started: bool = False,
    finished: bool = False,
) -> dict | None:
    normalized = str(status or "").strip().lower()
    if normalized not in {"queued", "running", "done", "failed", "canceled"}:
        raise ValueError("Invalid translation check session status")
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE bt_3_translation_check_sessions
                SET
                    status = %s,
                    last_error = %s,
                    started_at = CASE WHEN %s THEN COALESCE(started_at, NOW()) ELSE started_at END,
                    finished_at = CASE WHEN %s THEN NOW() ELSE finished_at END,
                    updated_at = NOW()
                WHERE id = %s
                RETURNING
                    id, user_id, username, source_session_id, source_lang, target_lang,
                    status, total_items, completed_items, failed_items, send_private_grammar_text,
                    original_text_bundle, user_translation_bundle, last_error, started_at,
                    finished_at, created_at, updated_at;
                """,
                (
                    normalized,
                    str(last_error or "").strip() or None,
                    bool(started),
                    bool(finished),
                    int(session_id),
                ),
            )
            row = cursor.fetchone()
    return _map_translation_check_session_row(row)


def mark_translation_check_session_dispatched(
    *,
    session_id: int,
    dispatch_job_id: str,
) -> dict | None:
    normalized_dispatch_job_id = str(dispatch_job_id or "").strip() or None
    if not normalized_dispatch_job_id:
        raise ValueError("dispatch_job_id is required")
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE bt_3_translation_check_sessions
                SET
                    status = 'queued',
                    dispatched_job_id = %s,
                    dispatched_at = NOW(),
                    worker_job_id = NULL,
                    heartbeat_at = NULL,
                    last_error = NULL,
                    updated_at = NOW()
                WHERE id = %s
                  AND status NOT IN ('done', 'failed', 'canceled')
                RETURNING
                    id, user_id, status, last_error, dispatched_job_id, dispatched_at,
                    worker_job_id, heartbeat_at, started_at, finished_at, created_at, updated_at;
                """,
                (normalized_dispatch_job_id, int(session_id)),
            )
            row = cursor.fetchone()
    return _map_translation_check_session_runtime_row(row)


def claim_translation_check_session_runner(
    *,
    session_id: int,
    dispatch_job_id: str,
) -> dict | None:
    normalized_dispatch_job_id = str(dispatch_job_id or "").strip() or None
    if not normalized_dispatch_job_id:
        raise ValueError("dispatch_job_id is required")
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE bt_3_translation_check_sessions
                SET
                    status = 'running',
                    worker_job_id = %s,
                    heartbeat_at = NOW(),
                    started_at = COALESCE(started_at, NOW()),
                    updated_at = NOW()
                WHERE id = %s
                  AND status = 'queued'
                  AND finished_at IS NULL
                  AND COALESCE(dispatched_job_id, '') = %s
                RETURNING
                    id, user_id, username, source_session_id, source_lang, target_lang,
                    status, total_items, completed_items, failed_items, send_private_grammar_text,
                    original_text_bundle, user_translation_bundle, last_error, started_at,
                    finished_at, created_at, updated_at;
                """,
                (
                    normalized_dispatch_job_id,
                    int(session_id),
                    normalized_dispatch_job_id,
                ),
            )
            row = cursor.fetchone()
    return _map_translation_check_session_row(row)


def touch_translation_check_session_heartbeat(
    *,
    session_id: int,
    worker_job_id: str,
) -> bool:
    normalized_worker_job_id = str(worker_job_id or "").strip() or None
    if not normalized_worker_job_id:
        return False
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE bt_3_translation_check_sessions
                SET
                    heartbeat_at = NOW(),
                    updated_at = NOW()
                WHERE id = %s
                  AND status = 'running'
                  AND finished_at IS NULL
                  AND COALESCE(worker_job_id, '') = %s;
                """,
                (int(session_id), normalized_worker_job_id),
            )
            return cursor.rowcount > 0


def complete_translation_check_session(
    *,
    session_id: int,
    worker_job_id: str,
    status: str,
    last_error: str | None = None,
) -> dict | None:
    normalized_status = str(status or "").strip().lower()
    if normalized_status not in {"done", "failed", "canceled"}:
        raise ValueError("Invalid translation check terminal status")
    normalized_worker_job_id = str(worker_job_id or "").strip() or None
    if not normalized_worker_job_id:
        raise ValueError("worker_job_id is required")
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE bt_3_translation_check_sessions
                SET
                    status = %s,
                    last_error = %s,
                    finished_at = NOW(),
                    heartbeat_at = NOW(),
                    updated_at = NOW()
                WHERE id = %s
                  AND status = 'running'
                  AND finished_at IS NULL
                  AND COALESCE(worker_job_id, '') = %s
                RETURNING
                    id, user_id, username, source_session_id, source_lang, target_lang,
                    status, total_items, completed_items, failed_items, send_private_grammar_text,
                    original_text_bundle, user_translation_bundle, last_error, started_at,
                    finished_at, created_at, updated_at;
                """,
                (
                    normalized_status,
                    str(last_error or "").strip() or None,
                    int(session_id),
                    normalized_worker_job_id,
                ),
            )
            row = cursor.fetchone()
    return _map_translation_check_session_row(row)


def update_translation_check_item_result(
    *,
    item_id: int,
    status: str,
    result_json: dict | None = None,
    result_text: str | None = None,
    error_text: str | None = None,
    webapp_check_id: int | None = None,
    started: bool = False,
    finished: bool = False,
) -> dict | None:
    normalized = str(status or "").strip().lower()
    if normalized not in {"pending", "running", "done", "failed"}:
        raise ValueError("Invalid translation check item status")
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE bt_3_translation_check_items
                SET
                    status = %s,
                    result_json = %s,
                    result_text = %s,
                    error_text = %s,
                    webapp_check_id = %s,
                    started_at = CASE WHEN %s THEN COALESCE(started_at, NOW()) ELSE started_at END,
                    finished_at = CASE WHEN %s THEN NOW() ELSE finished_at END,
                    updated_at = NOW()
                WHERE id = %s
                RETURNING
                    id, check_session_id, item_order, sentence_number, sentence_id_for_mistake_table,
                    original_text, user_translation, status, result_json, result_text, error_text,
                    webapp_check_id, started_at, finished_at, created_at, updated_at;
                """,
                (
                    normalized,
                    Json(result_json) if isinstance(result_json, dict) else None,
                    str(result_text or "").strip() or None,
                    str(error_text or "").strip() or None,
                    int(webapp_check_id) if webapp_check_id is not None else None,
                    bool(started),
                    bool(finished),
                    int(item_id),
                ),
            )
            row = cursor.fetchone()
    return _map_translation_check_item_row(row)


def finalize_translation_check_item(
    *,
    item_id: int,
    status: str,
    result_json: dict | None = None,
    result_text: str | None = None,
    error_text: str | None = None,
    webapp_check_id: int | None = None,
) -> dict[str, object]:
    normalized = str(status or "").strip().lower()
    if normalized not in {"done", "failed"}:
        raise ValueError("Invalid translation check item terminal status")
    completed_delta = 1 if normalized == "done" else 0
    failed_delta = 1 if normalized == "failed" else 0

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE bt_3_translation_check_items
                SET
                    status = %s,
                    result_json = %s,
                    result_text = %s,
                    error_text = %s,
                    webapp_check_id = %s,
                    started_at = COALESCE(started_at, NOW()),
                    finished_at = NOW(),
                    updated_at = NOW()
                WHERE id = %s
                  AND status NOT IN ('done', 'failed')
                RETURNING
                    id, check_session_id, item_order, sentence_number, sentence_id_for_mistake_table,
                    original_text, user_translation, status, result_json, result_text, error_text,
                    webapp_check_id, started_at, finished_at, created_at, updated_at;
                """,
                (
                    normalized,
                    Json(result_json) if isinstance(result_json, dict) else None,
                    str(result_text or "").strip() or None,
                    str(error_text or "").strip() or None,
                    int(webapp_check_id) if webapp_check_id is not None else None,
                    int(item_id),
                ),
            )
            item_row = cursor.fetchone()
            session_row = None
            finalized = bool(item_row)
            if item_row:
                cursor.execute(
                    """
                    UPDATE bt_3_translation_check_sessions
                    SET
                        completed_items = GREATEST(0, completed_items + %s),
                        failed_items = GREATEST(0, failed_items + %s),
                        updated_at = NOW()
                    WHERE id = %s
                    RETURNING
                        id, user_id, username, source_session_id, source_lang, target_lang,
                        status, total_items, completed_items, failed_items, send_private_grammar_text,
                        original_text_bundle, user_translation_bundle, last_error, started_at,
                        finished_at, created_at, updated_at;
                    """,
                    (int(completed_delta), int(failed_delta), int(item_row[1])),
                )
                session_row = cursor.fetchone()
            else:
                cursor.execute(
                    """
                    SELECT
                        id, check_session_id, item_order, sentence_number, sentence_id_for_mistake_table,
                        original_text, user_translation, status, result_json, result_text, error_text,
                        webapp_check_id, started_at, finished_at, created_at, updated_at
                    FROM bt_3_translation_check_items
                    WHERE id = %s;
                    """,
                    (int(item_id),),
                )
                item_row = cursor.fetchone()

    return {
        "item": _map_translation_check_item_row(item_row),
        "session": _map_translation_check_session_row(session_row),
        "finalized": finalized,
    }


def finalize_translation_check_items_batch(
    *,
    session_id: int,
    item_results: list[dict[str, object]],
    worker_job_id: str | None = None,
) -> dict | None:
    normalized_rows: list[tuple[int, str, object | None, str | None, str | None, int | None]] = []
    for raw in item_results or []:
        if not isinstance(raw, dict):
            continue
        try:
            item_id = int(raw.get("item_id"))
        except Exception:
            continue
        status = str(raw.get("item_status") or "").strip().lower()
        if status not in {"done", "failed"}:
            continue
        result_json = raw.get("result_json") if isinstance(raw.get("result_json"), dict) else None
        result_text = str(raw.get("result_text") or "").strip() or None
        error_text = str(raw.get("error_text") or "").strip() or None
        webapp_check_id = raw.get("webapp_check_id")
        try:
            normalized_webapp_check_id = int(webapp_check_id) if webapp_check_id is not None else None
        except Exception:
            normalized_webapp_check_id = None
        normalized_rows.append(
            (
                item_id,
                status,
                Json(result_json) if isinstance(result_json, dict) else None,
                result_text,
                error_text,
                normalized_webapp_check_id,
            )
        )

    if not normalized_rows:
        return get_translation_check_session(session_id=int(session_id))

    normalized_worker_job_id = str(worker_job_id or "").strip() or None
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            execute_values(
                cursor,
                f"""
                WITH incoming(item_id, status, result_json, result_text, error_text, webapp_check_id) AS (
                    VALUES %s
                )
                UPDATE bt_3_translation_check_items AS items
                SET
                    status = incoming.status,
                    result_json = incoming.result_json,
                    result_text = incoming.result_text,
                    error_text = incoming.error_text,
                    webapp_check_id = incoming.webapp_check_id,
                    started_at = COALESCE(items.started_at, NOW()),
                    finished_at = NOW(),
                    updated_at = NOW()
                FROM incoming
                WHERE items.id = incoming.item_id
                  AND items.check_session_id = {int(session_id)}
                  AND items.status NOT IN ('done', 'failed');
                """,
                normalized_rows,
                template="(%s, %s, %s::jsonb, %s, %s, %s)",
                page_size=200,
            )
            cursor.execute(
                """
                UPDATE bt_3_translation_check_sessions s
                SET
                    total_items = counts.total_items,
                    completed_items = counts.completed_items,
                    failed_items = counts.failed_items,
                    updated_at = NOW()
                FROM (
                    SELECT
                        check_session_id,
                        COUNT(*) AS total_items,
                        COUNT(*) FILTER (WHERE status = 'done') AS completed_items,
                        COUNT(*) FILTER (WHERE status = 'failed') AS failed_items
                    FROM bt_3_translation_check_items
                    WHERE check_session_id = %s
                    GROUP BY check_session_id
                ) AS counts
                WHERE s.id = counts.check_session_id
                  AND s.status = 'running'
                  AND s.finished_at IS NULL
                  AND (%s IS NULL OR COALESCE(s.worker_job_id, '') = %s)
                RETURNING
                    s.id, s.user_id, s.username, s.source_session_id, s.source_lang, s.target_lang,
                    s.status, s.total_items, s.completed_items, s.failed_items, s.send_private_grammar_text,
                    s.original_text_bundle, s.user_translation_bundle, s.last_error, s.started_at,
                    s.finished_at, s.created_at, s.updated_at;
                """,
                (int(session_id), normalized_worker_job_id, normalized_worker_job_id),
            )
            row = cursor.fetchone()
    return _map_translation_check_session_row(row)


def increment_translation_check_session_counters(
    *,
    session_id: int,
    item_status: str,
) -> dict | None:
    normalized_item_status = str(item_status or "").strip().lower()
    if normalized_item_status not in {"done", "failed"}:
        raise ValueError("Invalid translation check item terminal status")
    completed_delta = 1 if normalized_item_status == "done" else 0
    failed_delta = 1 if normalized_item_status == "failed" else 0
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE bt_3_translation_check_sessions
                SET
                    completed_items = GREATEST(0, completed_items + %s),
                    failed_items = GREATEST(0, failed_items + %s),
                    updated_at = NOW()
                WHERE id = %s
                RETURNING
                    id, user_id, username, source_session_id, source_lang, target_lang,
                    status, total_items, completed_items, failed_items, send_private_grammar_text,
                    original_text_bundle, user_translation_bundle, last_error, started_at,
                    finished_at, created_at, updated_at;
                """,
                (int(completed_delta), int(failed_delta), int(session_id)),
            )
            row = cursor.fetchone()
    return _map_translation_check_session_row(row)


def refresh_translation_check_session_counters(*, session_id: int) -> dict | None:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE bt_3_translation_check_sessions s
                SET
                    total_items = counts.total_items,
                    completed_items = counts.completed_items,
                    failed_items = counts.failed_items,
                    updated_at = NOW()
                FROM (
                    SELECT
                        check_session_id,
                        COUNT(*) AS total_items,
                        COUNT(*) FILTER (WHERE status = 'done') AS completed_items,
                        COUNT(*) FILTER (WHERE status = 'failed') AS failed_items
                    FROM bt_3_translation_check_items
                    WHERE check_session_id = %s
                    GROUP BY check_session_id
                ) AS counts
                WHERE s.id = counts.check_session_id
                RETURNING
                    s.id, s.user_id, s.username, s.source_session_id, s.source_lang, s.target_lang,
                    s.status, s.total_items, s.completed_items, s.failed_items, s.send_private_grammar_text,
                    s.original_text_bundle, s.user_translation_bundle, s.last_error, s.started_at,
                    s.finished_at, s.created_at, s.updated_at;
                """,
                (int(session_id),),
            )
            row = cursor.fetchone()
    return _map_translation_check_session_row(row)


def save_webapp_dictionary_query(
    user_id: int,
    word_ru: str | None,
    translation_de: str | None,
    word_de: str | None,
    translation_ru: str | None,
    response_json: dict,
    folder_id: int | None = None,
    source_lang: str | None = None,
    target_lang: str | None = None,
    origin_process: str | None = None,
    origin_meta: dict | None = None,
) -> None:
    save_webapp_dictionary_query_returning_id(
        user_id=user_id,
        word_ru=word_ru,
        translation_de=translation_de,
        word_de=word_de,
        translation_ru=translation_ru,
        response_json=response_json,
        folder_id=folder_id,
        source_lang=source_lang,
        target_lang=target_lang,
        origin_process=origin_process,
        origin_meta=origin_meta,
    )


def save_webapp_dictionary_query_returning_id(
    user_id: int,
    word_ru: str | None,
    translation_de: str | None,
    word_de: str | None,
    translation_ru: str | None,
    response_json: dict,
    folder_id: int | None = None,
    source_lang: str | None = None,
    target_lang: str | None = None,
    origin_process: str | None = None,
    origin_meta: dict | None = None,
) -> int:
    with get_db_connection_context() as conn:
        inserted_id, _inserted = _save_webapp_dictionary_query_returning_id_with_conn(
            conn,
            user_id=int(user_id),
            word_ru=word_ru,
            translation_de=translation_de,
            word_de=word_de,
            translation_ru=translation_ru,
            response_json=response_json,
            folder_id=folder_id,
            source_lang=source_lang,
            target_lang=target_lang,
            origin_process=origin_process,
            origin_meta=origin_meta,
        )
    return inserted_id if inserted_id > 0 else 0


def get_webapp_dictionary_entries(
    user_id: int,
    limit: int = 100,
    folder_mode: str = "all",
    folder_id: int | None = None,
    source_lang: str | None = None,
    target_lang: str | None = None,
) -> list[dict]:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            where_clause = "WHERE user_id = %s"
            params = [user_id]
            language_filter_sql, language_params = _build_language_pair_filter(source_lang, target_lang)
            if language_filter_sql:
                where_clause += language_filter_sql
                params.extend(language_params)
            if folder_mode == "folder" and folder_id is not None:
                where_clause += " AND folder_id = %s"
                params.append(folder_id)
            elif folder_mode == "none":
                where_clause += " AND folder_id IS NULL"
            params.append(limit)
            cursor.execute(f"""
                SELECT id, word_ru, translation_de, word_de, translation_ru, source_lang, target_lang, origin_process, origin_meta, response_json, folder_id, created_at
                FROM bt_3_webapp_dictionary_queries
                {where_clause}
                ORDER BY created_at DESC
                LIMIT %s;
            """, params)
            rows = cursor.fetchall()

    items = []
    for row in rows:
        items.append({
            "id": row[0],
            "word_ru": row[1],
            "translation_de": row[2],
            "word_de": row[3],
            "translation_ru": row[4],
            "source_lang": row[5],
            "target_lang": row[6],
            "origin_process": row[7],
            "origin_meta": row[8],
            "response_json": row[9],
            "folder_id": row[10],
            "created_at": row[11].isoformat() if row[11] else None,
        })
    return items


def has_dictionary_entries_for_language_pair(
    user_id: int,
    source_lang: str | None,
    target_lang: str | None,
    cursor=None,
) -> bool:
    def _check(cur) -> bool:
        where_clause = "WHERE user_id = %s"
        params: list = [int(user_id)]
        language_filter_sql, language_params = _build_language_pair_filter(source_lang, target_lang)
        if language_filter_sql:
            where_clause += language_filter_sql
            params.extend(language_params)
        cur.execute(
            f"""
            SELECT 1
            FROM bt_3_webapp_dictionary_queries
            {where_clause}
            LIMIT 1;
            """,
            params,
        )
        return cur.fetchone() is not None

    if cursor is not None:
        return _check(cursor)
    with get_db_connection_context() as conn:
        with conn.cursor() as own_cursor:
            return _check(own_cursor)


def get_latest_dictionary_language_pair_for_user(user_id: int, cursor=None) -> tuple[str, str] | None:
    def _get(cur):
        cur.execute(
            """
            SELECT
                LOWER(COALESCE(
                    NULLIF(source_lang, ''),
                    NULLIF(response_json->>'source_lang', ''),
                    NULLIF(response_json#>>'{language_pair,source_lang}', ''),
                    'ru'
                )) AS source_lang_value,
                LOWER(COALESCE(
                    NULLIF(target_lang, ''),
                    NULLIF(response_json->>'target_lang', ''),
                    NULLIF(response_json#>>'{language_pair,target_lang}', ''),
                    'de'
                )) AS target_lang_value
            FROM bt_3_webapp_dictionary_queries
            WHERE user_id = %s
            ORDER BY created_at DESC
            LIMIT 1;
            """,
            (int(user_id),),
        )
        row = cur.fetchone()
        if not row:
            return None
        return str(row[0] or "ru"), str(row[1] or "de")

    if cursor is not None:
        return _get(cursor)
    with get_db_connection_context() as conn:
        with conn.cursor() as own_cursor:
            return _get(own_cursor)


def _map_starter_dictionary_state_row(row, *, user_id: int) -> dict:
    if not row:
        return {
            "user_id": int(user_id),
            "decision_status": "pending",
            "source_user_id": None,
            "template_version": None,
            "source_lang": None,
            "target_lang": None,
            "last_imported_count": 0,
            "decided_at": None,
            "last_imported_at": None,
            "import_status": "idle",
            "active_job_id": None,
            "last_error": None,
            "import_started_at": None,
            "import_finished_at": None,
            "updated_at": None,
        }
    return {
        "user_id": int(user_id),
        "decision_status": str(row[0] or "pending").strip().lower() or "pending",
        "source_user_id": int(row[1]) if row[1] is not None else None,
        "template_version": str(row[2] or "").strip() or None,
        "source_lang": _normalize_lang_code(row[3]) or None,
        "target_lang": _normalize_lang_code(row[4]) or None,
        "last_imported_count": max(0, int(row[5] or 0)),
        "decided_at": row[6].isoformat() if row[6] else None,
        "last_imported_at": row[7].isoformat() if row[7] else None,
        "import_status": str(row[8] or "idle").strip().lower() or "idle",
        "active_job_id": str(row[9] or "").strip() or None,
        "last_error": str(row[10] or "").strip() or None,
        "import_started_at": row[11].isoformat() if row[11] else None,
        "import_finished_at": row[12].isoformat() if row[12] else None,
        "updated_at": row[13].isoformat() if row[13] else None,
    }


def get_starter_dictionary_state(user_id: int, cursor=None) -> dict:
    def _get(cur) -> dict:
        cur.execute(
            """
            SELECT
                decision_status,
                source_user_id,
                template_version,
                source_lang,
                target_lang,
                last_imported_count,
                decided_at,
                last_imported_at,
                import_status,
                active_job_id,
                last_error,
                import_started_at,
                import_finished_at,
                updated_at
            FROM bt_3_starter_dictionary_state
            WHERE user_id = %s
            LIMIT 1;
            """,
            (int(user_id),),
        )
        row = cur.fetchone()
        return _map_starter_dictionary_state_row(row, user_id=int(user_id))

    if cursor is not None:
        return _get(cursor)
    with get_db_connection_context() as conn:
        with conn.cursor() as own_cursor:
            return _get(own_cursor)


def upsert_starter_dictionary_state(
    *,
    user_id: int,
    decision_status: str,
    source_user_id: int | None = None,
    template_version: str | None = None,
    source_lang: str | None = None,
    target_lang: str | None = None,
    last_imported_count: int = 0,
    decided_at: datetime | None = None,
    last_imported_at: datetime | None = None,
    import_status: str = "idle",
    active_job_id: str | None = None,
    last_error: str | None = None,
    import_started_at: datetime | None = None,
    import_finished_at: datetime | None = None,
) -> dict:
    normalized_decision = str(decision_status or "").strip().lower() or "pending"
    if normalized_decision not in {"pending", "accepted", "declined"}:
        raise ValueError("Invalid starter dictionary decision status")
    normalized_import_status = str(import_status or "idle").strip().lower() or "idle"
    if normalized_import_status not in {"idle", "running", "done", "failed"}:
        raise ValueError("Invalid starter dictionary import status")
    normalized_source = _normalize_lang_code(source_lang) or None
    normalized_target = _normalize_lang_code(target_lang) or None
    imported_count_value = max(0, int(last_imported_count or 0))
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_starter_dictionary_state (
                    user_id,
                    decision_status,
                    source_user_id,
                    template_version,
                    source_lang,
                    target_lang,
                    last_imported_count,
                    decided_at,
                    last_imported_at,
                    import_status,
                    active_job_id,
                    last_error,
                    import_started_at,
                    import_finished_at,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (user_id) DO UPDATE
                SET
                    decision_status = EXCLUDED.decision_status,
                    source_user_id = EXCLUDED.source_user_id,
                    template_version = EXCLUDED.template_version,
                    source_lang = EXCLUDED.source_lang,
                    target_lang = EXCLUDED.target_lang,
                    last_imported_count = EXCLUDED.last_imported_count,
                    decided_at = EXCLUDED.decided_at,
                    last_imported_at = EXCLUDED.last_imported_at,
                    import_status = EXCLUDED.import_status,
                    active_job_id = EXCLUDED.active_job_id,
                    last_error = EXCLUDED.last_error,
                    import_started_at = EXCLUDED.import_started_at,
                    import_finished_at = EXCLUDED.import_finished_at,
                    updated_at = NOW()
                RETURNING
                    decision_status,
                    source_user_id,
                    template_version,
                    source_lang,
                    target_lang,
                    last_imported_count,
                    decided_at,
                    last_imported_at,
                    import_status,
                    active_job_id,
                    last_error,
                    import_started_at,
                    import_finished_at,
                    updated_at;
                """,
                (
                    int(user_id),
                    normalized_decision,
                    int(source_user_id) if source_user_id is not None else None,
                    str(template_version or "").strip() or None,
                    normalized_source,
                    normalized_target,
                    imported_count_value,
                    decided_at,
                    last_imported_at,
                    normalized_import_status,
                    str(active_job_id or "").strip() or None,
                    str(last_error or "").strip() or None,
                    import_started_at,
                    import_finished_at,
                ),
            )
            row = cursor.fetchone()
    return _map_starter_dictionary_state_row(row, user_id=int(user_id))


def count_dictionary_entries_for_language_pair(
    user_id: int,
    source_lang: str | None,
    target_lang: str | None,
    cursor=None,
) -> int:
    def _count(cur) -> int:
        where_clause = "WHERE user_id = %s"
        params: list = [int(user_id)]
        language_filter_sql, language_params = _build_language_pair_filter(source_lang, target_lang)
        if language_filter_sql:
            where_clause += language_filter_sql
            params.extend(language_params)
        cur.execute(
            f"""
            SELECT COUNT(*)
            FROM bt_3_webapp_dictionary_queries
            {where_clause};
            """,
            params,
        )
        row = cur.fetchone()
        return int((row or [0])[0] or 0)

    if cursor is not None:
        return _count(cursor)
    with get_db_connection_context() as conn:
        with conn.cursor() as own_cursor:
            return _count(own_cursor)


def import_starter_dictionary_snapshot(
    *,
    source_user_id: int,
    target_user_id: int,
    source_lang: str,
    target_lang: str,
    import_limit: int = 1000,
    folder_name: str = "Базовый словарь",
    folder_color: str = "#5ddcff",
    folder_icon: str = "book",
    template_version: str = "v1",
) -> dict:
    source_user = int(source_user_id)
    target_user = int(target_user_id)
    pair_source = _normalize_lang_code(source_lang) or DEFAULT_NATIVE_LANGUAGE
    pair_target = _normalize_lang_code(target_lang) or DEFAULT_LEARNING_LANGUAGE
    safe_limit = max(1, min(int(import_limit or 1000), 5000))
    resolved_folder_name = str(folder_name or "Базовый словарь").strip() or "Базовый словарь"
    resolved_folder_color = str(folder_color or "#5ddcff").strip() or "#5ddcff"
    resolved_folder_icon = str(folder_icon or "book").strip() or "book"
    resolved_template_version = str(template_version or "v1").strip() or "v1"
    advisory_lock_key = int.from_bytes(
        hashlib.blake2b(
            f"starter_dictionary_import:{target_user}:{pair_source}:{pair_target}".encode("utf-8"),
            digest_size=8,
        ).digest(),
        "big",
        signed=True,
    )

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT pg_advisory_xact_lock(%s::bigint);", (advisory_lock_key,))
            cursor.execute(
                """
                SELECT id, name, color, icon, created_at
                FROM bt_3_dictionary_folders
                WHERE user_id = %s AND name = %s
                LIMIT 1;
                """,
                (target_user, resolved_folder_name),
            )
            folder_row = cursor.fetchone()
            if folder_row:
                folder_id = int(folder_row[0])
                folder_payload = {
                    "id": folder_id,
                    "name": folder_row[1],
                    "color": folder_row[2],
                    "icon": folder_row[3],
                    "created_at": folder_row[4].isoformat() if folder_row[4] else None,
                }
            else:
                cursor.execute(
                    """
                    INSERT INTO bt_3_dictionary_folders (user_id, name, color, icon)
                    VALUES (%s, %s, %s, %s)
                    RETURNING id, name, color, icon, created_at;
                    """,
                    (target_user, resolved_folder_name, resolved_folder_color, resolved_folder_icon),
                )
                created_row = cursor.fetchone()
                folder_id = int(created_row[0])
                folder_payload = {
                    "id": folder_id,
                    "name": created_row[1],
                    "color": created_row[2],
                    "icon": created_row[3],
                    "created_at": created_row[4].isoformat() if created_row[4] else None,
                }

            language_filter_sql, language_params = _build_language_pair_filter(pair_source, pair_target)
            source_where = "WHERE user_id = %s"
            source_params: list = [source_user]
            if language_filter_sql:
                source_where += language_filter_sql
                source_params.extend(language_params)
            source_params.append(safe_limit)
            cursor.execute(
                f"""
                SELECT
                    id,
                    word_ru,
                    translation_de,
                    word_de,
                    translation_ru,
                    source_lang,
                    target_lang,
                    response_json
                FROM bt_3_webapp_dictionary_queries
                {source_where}
                ORDER BY created_at ASC, id ASC
                LIMIT %s;
                """,
                source_params,
            )
            source_rows = cursor.fetchall() or []

            candidates: list[dict] = []
            candidate_keys: set[tuple[str, str]] = set()
            for row in source_rows:
                row_source_lang = _normalize_lang_code(row[5]) or pair_source
                row_target_lang = _normalize_lang_code(row[6]) or pair_target
                response_payload = _coerce_json_object(row[7])
                source_text, target_text = _resolve_dictionary_source_target_texts(
                    source_lang=row_source_lang,
                    target_lang=row_target_lang,
                    word_ru=row[1],
                    translation_de=row[2],
                    word_de=row[3],
                    translation_ru=row[4],
                    response_json=response_payload,
                )
                if not source_text or not target_text:
                    continue
                key = (
                    _normalize_dictionary_text_key(source_text),
                    _normalize_dictionary_text_key(target_text),
                )
                if key in candidate_keys:
                    continue
                candidate_keys.add(key)

                resolved_word_ru = None
                resolved_translation_de = None
                resolved_word_de = None
                resolved_translation_ru = None

                if pair_source == "ru":
                    resolved_word_ru = source_text
                if pair_source == "de":
                    resolved_word_de = source_text
                if pair_target == "de":
                    resolved_translation_de = target_text
                    if not resolved_word_de:
                        resolved_word_de = target_text
                if pair_target == "ru":
                    resolved_translation_ru = target_text
                    if not resolved_word_ru:
                        resolved_word_ru = target_text
                if pair_source == "ru" and not resolved_translation_ru:
                    resolved_translation_ru = source_text
                if pair_source == "de" and not resolved_translation_de:
                    resolved_translation_de = source_text

                merged_response = dict(response_payload)
                merged_response["source_text"] = source_text
                merged_response["target_text"] = target_text
                merged_response["source_lang"] = pair_source
                merged_response["target_lang"] = pair_target
                pair_payload = merged_response.get("language_pair")
                if not isinstance(pair_payload, dict):
                    pair_payload = {}
                pair_payload["source_lang"] = pair_source
                pair_payload["target_lang"] = pair_target
                merged_response["language_pair"] = pair_payload

                candidates.append(
                    {
                        "source_entry_id": int(row[0]),
                        "key": key,
                        "word_ru": resolved_word_ru,
                        "translation_de": resolved_translation_de,
                        "word_de": resolved_word_de,
                        "translation_ru": resolved_translation_ru,
                        "response_json": merged_response,
                    }
                )

            target_where = "WHERE user_id = %s"
            target_params: list = [target_user]
            if language_filter_sql:
                target_where += language_filter_sql
                target_params.extend(language_params)
            cursor.execute(
                f"""
                SELECT
                    word_ru,
                    translation_de,
                    word_de,
                    translation_ru,
                    source_lang,
                    target_lang,
                    response_json
                FROM bt_3_webapp_dictionary_queries
                {target_where};
                """,
                target_params,
            )
            target_rows = cursor.fetchall() or []
            existing_keys: set[tuple[str, str]] = set()
            for row in target_rows:
                existing_source, existing_target = _resolve_dictionary_source_target_texts(
                    source_lang=_normalize_lang_code(row[4]) or pair_source,
                    target_lang=_normalize_lang_code(row[5]) or pair_target,
                    word_ru=row[0],
                    translation_de=row[1],
                    word_de=row[2],
                    translation_ru=row[3],
                    response_json=_coerce_json_object(row[6]),
                )
                if not existing_source or not existing_target:
                    continue
                existing_keys.add(
                    (
                        _normalize_dictionary_text_key(existing_source),
                        _normalize_dictionary_text_key(existing_target),
                    )
                )

            skipped_existing_count = 0
            inserted_count = 0
            import_started_at = datetime.now(timezone.utc)
            for item in candidates:
                if item["key"] in existing_keys:
                    skipped_existing_count += 1
                    continue
                origin_meta = {
                    "import_kind": "starter_dictionary_snapshot",
                    "import_source_user_id": source_user,
                    "source_entry_id": item["source_entry_id"],
                    "template_version": resolved_template_version,
                    "source_lang": pair_source,
                    "target_lang": pair_target,
                }
                _entry_id, inserted = _save_webapp_dictionary_query_returning_id_with_conn(
                    conn,
                    user_id=int(target_user),
                    word_ru=item["word_ru"],
                    translation_de=item["translation_de"],
                    word_de=item["word_de"],
                    translation_ru=item["translation_ru"],
                    response_json=item["response_json"],
                    folder_id=int(folder_id) if folder_id is not None else None,
                    source_lang=pair_source,
                    target_lang=pair_target,
                    origin_process="import",
                    origin_meta=origin_meta,
                )
                if inserted:
                    inserted_count += 1
                else:
                    skipped_existing_count += 1
                existing_keys.add(item["key"])

    return {
        "source_user_id": source_user,
        "target_user_id": target_user,
        "source_lang": pair_source,
        "target_lang": pair_target,
        "import_limit": safe_limit,
        "selected_count": len(source_rows),
        "candidates_count": len(candidates),
        "inserted_count": inserted_count,
        "skipped_existing_count": skipped_existing_count,
        "folder": folder_payload,
        "template_version": resolved_template_version,
        "imported_at": import_started_at.isoformat(),
    }


def get_dictionary_entries_for_tts_prewarm(
    *,
    limit: int = 200,
    lookback_hours: int = 168,
) -> list[dict]:
    safe_limit = max(1, min(int(limit), 2000))
    safe_hours = max(1, min(int(lookback_hours), 24 * 90))
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    id,
                    user_id,
                    word_ru,
                    translation_de,
                    word_de,
                    translation_ru,
                    source_lang,
                    target_lang,
                    response_json,
                    created_at
                FROM bt_3_webapp_dictionary_queries
                WHERE created_at >= NOW() - (%s || ' hours')::interval
                ORDER BY created_at DESC
                LIMIT %s;
                """,
                (safe_hours, safe_limit),
            )
            rows = cursor.fetchall()

    items: list[dict] = []
    for row in rows:
        items.append(
            {
                "id": int(row[0]),
                "user_id": int(row[1]),
                "word_ru": row[2],
                "translation_de": row[3],
                "word_de": row[4],
                "translation_ru": row[5],
                "source_lang": row[6],
                "target_lang": row[7],
                "response_json": row[8],
                "created_at": row[9].isoformat() if row[9] else None,
            }
        )
    return items


def get_recent_dictionary_user_ids(
    *,
    limit: int = 100,
    lookback_hours: int = 168,
) -> list[int]:
    safe_limit = max(1, min(int(limit), 2000))
    safe_hours = max(1, min(int(lookback_hours), 24 * 180))
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT user_id, MAX(created_at) AS last_created_at
                FROM bt_3_webapp_dictionary_queries
                WHERE created_at >= NOW() - (%s || ' hours')::interval
                GROUP BY user_id
                ORDER BY last_created_at DESC
                LIMIT %s;
                """,
                (safe_hours, safe_limit),
            )
            rows = cursor.fetchall()
    return [int(row[0]) for row in rows if row and row[0] is not None]


def get_dictionary_entry_by_id(entry_id: int) -> dict | None:
    if not entry_id:
        return None
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT
                    id,
                    word_ru,
                    translation_de,
                    word_de,
                    translation_ru,
                    source_lang,
                    target_lang,
                    origin_process,
                    origin_meta,
                    response_json,
                    folder_id,
                    created_at
                FROM bt_3_webapp_dictionary_queries
                WHERE id = %s
                LIMIT 1;
            """, (entry_id,))
            row = cursor.fetchone()
            if not row:
                return None
            return {
                "id": row[0],
                "word_ru": row[1],
                "translation_de": row[2],
                "word_de": row[3],
                "translation_ru": row[4],
                "source_lang": row[5],
                "target_lang": row[6],
                "origin_process": row[7],
                "origin_meta": row[8],
                "response_json": row[9],
                "folder_id": row[10],
                "created_at": row[11].isoformat() if row[11] else None,
            }


def get_random_dictionary_entry(
    cooldown_days: int = 5,
    source_lang: str = "ru",
    target_lang: str = "de",
) -> dict | None:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT
                    id,
                    user_id,
                    word_ru,
                    translation_de,
                    response_json,
                    source_lang,
                    target_lang
                FROM bt_3_webapp_dictionary_queries
                WHERE response_json IS NOT NULL
                  AND COALESCE(NULLIF(source_lang, ''), response_json->>'source_lang') = %s
                  AND COALESCE(NULLIF(target_lang, ''), response_json->>'target_lang') = %s
                  AND NOT EXISTS (
                      SELECT 1
                      FROM bt_3_quiz_history h
                      WHERE h.word_ru = bt_3_webapp_dictionary_queries.word_ru
                        AND h.asked_at >= NOW() - INTERVAL %s
                  )
                ORDER BY RANDOM()
                LIMIT 1;
            """, (source_lang, target_lang, f"{cooldown_days} days",))
            row = cursor.fetchone()
            if not row:
                cursor.execute("""
                    SELECT
                        id,
                        user_id,
                        word_ru,
                        translation_de,
                        response_json,
                        source_lang,
                        target_lang
                    FROM bt_3_webapp_dictionary_queries
                    WHERE response_json IS NOT NULL
                      AND COALESCE(NULLIF(source_lang, ''), response_json->>'source_lang') = %s
                      AND COALESCE(NULLIF(target_lang, ''), response_json->>'target_lang') = %s
                    ORDER BY RANDOM()
                    LIMIT 1;
                """, (source_lang, target_lang))
                row = cursor.fetchone()
                if not row:
                    return None
            return {
                "id": row[0],
                "user_id": row[1],
                "word_ru": row[2],
                "translation_de": row[3],
                "response_json": row[4],
                "source_lang": row[5],
                "target_lang": row[6],
            }


def get_random_dictionary_entry_for_quiz_type(
    quiz_type: str,
    cooldown_days: int = 5,
    source_lang: str = "ru",
    target_lang: str = "de",
) -> dict | None:
    quiz_type = (quiz_type or "").strip().lower()

    usage_examples_count_expr = (
        "CASE WHEN jsonb_typeof(response_json->'usage_examples') = 'array' "
        "THEN jsonb_array_length(response_json->'usage_examples') ELSE 0 END"
    )
    prefixes_count_expr = (
        "CASE WHEN jsonb_typeof(response_json->'prefixes') = 'array' "
        "THEN jsonb_array_length(response_json->'prefixes') ELSE 0 END"
    )
    base_word_expr = (
        "COALESCE(NULLIF(word_de, ''), NULLIF(response_json->>'word_de', ''), "
        "NULLIF(translation_de, ''), NULLIF(response_json->>'translation_de', ''))"
    )
    letters_only_len_expr = (
        f"LENGTH(REGEXP_REPLACE({base_word_expr}, '[^A-Za-zÄÖÜäöüß]', '', 'g'))"
    )

    where_by_type = {
        "word_order": f"{usage_examples_count_expr} > 0",
        # Prefix quizzes rely on explicit prefix metadata. Falling back to any
        # non-empty German field lets full phrases/sentences leak into the
        # prefix generator, which then produces malformed glued pseudo-verbs.
        "prefix": f"{prefixes_count_expr} > 0",
        "anagram": f"{letters_only_len_expr} >= 4",
        "word": "COALESCE(NULLIF(translation_de, ''), NULLIF(response_json->>'translation_de', '')) IS NOT NULL",
    }
    extra_where = where_by_type.get(quiz_type, "TRUE")

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT
                    id,
                    user_id,
                    word_ru,
                    translation_de,
                    response_json,
                    source_lang,
                    target_lang
                FROM bt_3_webapp_dictionary_queries
                WHERE response_json IS NOT NULL
                  AND COALESCE(NULLIF(source_lang, ''), response_json->>'source_lang') = %s
                  AND COALESCE(NULLIF(target_lang, ''), response_json->>'target_lang') = %s
                  AND {extra_where}
                  AND NOT EXISTS (
                      SELECT 1
                      FROM bt_3_quiz_history h
                      WHERE h.word_ru = bt_3_webapp_dictionary_queries.word_ru
                        AND h.asked_at >= NOW() - INTERVAL %s
                  )
                ORDER BY RANDOM()
                LIMIT 1;
                """,
                (source_lang, target_lang, f"{cooldown_days} days"),
            )
            row = cursor.fetchone()
            if not row:
                cursor.execute(
                    f"""
                    SELECT
                        id,
                        user_id,
                        word_ru,
                        translation_de,
                        response_json,
                        source_lang,
                        target_lang
                    FROM bt_3_webapp_dictionary_queries
                    WHERE response_json IS NOT NULL
                      AND COALESCE(NULLIF(source_lang, ''), response_json->>'source_lang') = %s
                      AND COALESCE(NULLIF(target_lang, ''), response_json->>'target_lang') = %s
                      AND {extra_where}
                    ORDER BY RANDOM()
                    LIMIT 1;
                    """,
                    (source_lang, target_lang),
                )
                row = cursor.fetchone()
                if not row:
                    return None

    return {
        "id": row[0],
        "user_id": row[1],
        "word_ru": row[2],
        "translation_de": row[3],
        "response_json": row[4],
        "source_lang": row[5],
        "target_lang": row[6],
    }


def _normalize_telegram_quiz_delivery_mode(value: str | None) -> str:
    return "repeat" if str(value or "").strip().lower() == "repeat" else "new"


def list_low_accuracy_telegram_quiz_entries(
    chat_id: int,
    *,
    source_lang: str = "ru",
    target_lang: str = "de",
    accuracy_threshold: float = 0.5,
    limit: int = 8,
) -> list[dict]:
    safe_limit = max(1, min(50, int(limit or 1)))
    safe_threshold = min(1.0, max(0.0, float(accuracy_threshold)))
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                WITH attempt_stats AS (
                    SELECT
                        word_ru,
                        COUNT(*)::int AS attempts,
                        SUM(CASE WHEN is_correct THEN 1 ELSE 0 END)::int AS correct_attempts
                    FROM bt_3_telegram_quiz_attempts
                    WHERE chat_id = %s
                      AND COALESCE(NULLIF(word_ru, ''), '') <> ''
                    GROUP BY word_ru
                    HAVING COUNT(*) > 0
                       AND (SUM(CASE WHEN is_correct THEN 1 ELSE 0 END)::float / COUNT(*)) <= %s
                ),
                latest_quiz_type AS (
                    SELECT DISTINCT ON (word_ru)
                        word_ru,
                        COALESCE(NULLIF(quiz_type, ''), 'word') AS quiz_type
                    FROM bt_3_telegram_quiz_attempts
                    WHERE chat_id = %s
                      AND COALESCE(NULLIF(word_ru, ''), '') <> ''
                    ORDER BY word_ru, answered_at DESC
                ),
                delivery_stats AS (
                    SELECT
                        word_ru,
                        MIN(asked_at) AS first_asked_at,
                        MAX(asked_at) AS last_asked_at
                    FROM bt_3_telegram_quiz_delivery_history
                    WHERE chat_id = %s
                      AND COALESCE(NULLIF(word_ru, ''), '') <> ''
                    GROUP BY word_ru
                )
                SELECT
                    q.id,
                    q.user_id,
                    q.word_ru,
                    q.translation_de,
                    q.response_json,
                    q.source_lang,
                    q.target_lang,
                    COALESCE(latest_quiz_type.quiz_type, 'word') AS preferred_quiz_type,
                    attempt_stats.attempts,
                    attempt_stats.correct_attempts,
                    delivery_stats.first_asked_at,
                    delivery_stats.last_asked_at
                FROM bt_3_webapp_dictionary_queries q
                JOIN attempt_stats
                  ON attempt_stats.word_ru = q.word_ru
                JOIN delivery_stats
                  ON delivery_stats.word_ru = q.word_ru
                LEFT JOIN latest_quiz_type
                  ON latest_quiz_type.word_ru = q.word_ru
                WHERE q.response_json IS NOT NULL
                  AND COALESCE(NULLIF(q.source_lang, ''), q.response_json->>'source_lang') = %s
                  AND COALESCE(NULLIF(q.target_lang, ''), q.response_json->>'target_lang') = %s
                ORDER BY delivery_stats.last_asked_at ASC, attempt_stats.attempts DESC, q.id ASC
                LIMIT %s;
                """,
                (
                    int(chat_id),
                    safe_threshold,
                    int(chat_id),
                    int(chat_id),
                    source_lang,
                    target_lang,
                    safe_limit,
                ),
            )
            rows = cursor.fetchall() or []
    items: list[dict] = []
    for row in rows:
        attempts = int(row[8] or 0)
        correct_attempts = int(row[9] or 0)
        accuracy = float(correct_attempts) / float(attempts) if attempts > 0 else 0.0
        items.append(
            {
                "id": row[0],
                "user_id": row[1],
                "word_ru": row[2],
                "translation_de": row[3],
                "response_json": row[4],
                "source_lang": row[5],
                "target_lang": row[6],
                "preferred_quiz_type": row[7] or "word",
                "attempts": attempts,
                "correct_attempts": correct_attempts,
                "accuracy": accuracy,
                "first_asked_at": row[10].isoformat() if row[10] else None,
                "last_asked_at": row[11].isoformat() if row[11] else None,
            }
        )
    return items


def record_quiz_word(word_ru: str) -> None:
    if not word_ru:
        return
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_quiz_history (word_ru, asked_at)
                VALUES (%s, NOW());
                """,
                (word_ru,),
            )


def record_telegram_quiz_delivery(
    chat_id: int,
    *,
    poll_id: str | None = None,
    word_ru: str | None = None,
    quiz_type: str | None = None,
    delivery_mode: str | None = None,
) -> None:
    normalized_word = str(word_ru or "").strip()
    if not normalized_word:
        return
    normalized_mode = _normalize_telegram_quiz_delivery_mode(delivery_mode)
    normalized_quiz_type = str(quiz_type or "").strip().lower() or None
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_telegram_quiz_delivery_history (
                    chat_id,
                    poll_id,
                    word_ru,
                    quiz_type,
                    delivery_mode,
                    asked_at
                )
                VALUES (%s, %s, %s, %s, %s, NOW());
                """,
                (
                    int(chat_id),
                    str(poll_id or "").strip() or None,
                    normalized_word,
                    normalized_quiz_type,
                    normalized_mode,
                ),
            )


def record_telegram_quiz_attempt(
    poll_id: str,
    *,
    chat_id: int,
    user_id: int,
    word_ru: str | None = None,
    quiz_type: str | None = None,
    selected_option_index: int | None = None,
    selected_text: str | None = None,
    is_correct: bool,
) -> None:
    normalized_poll_id = str(poll_id or "").strip()
    if not normalized_poll_id:
        return
    normalized_quiz_type = str(quiz_type or "").strip().lower() or None
    normalized_word = str(word_ru or "").strip() or None
    normalized_selected_text = str(selected_text or "").strip() or None
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_telegram_quiz_attempts (
                    poll_id,
                    chat_id,
                    user_id,
                    word_ru,
                    quiz_type,
                    selected_option_index,
                    selected_text,
                    is_correct,
                    answered_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (poll_id, user_id) DO UPDATE
                SET
                    chat_id = EXCLUDED.chat_id,
                    word_ru = EXCLUDED.word_ru,
                    quiz_type = EXCLUDED.quiz_type,
                    selected_option_index = EXCLUDED.selected_option_index,
                    selected_text = EXCLUDED.selected_text,
                    is_correct = EXCLUDED.is_correct,
                    answered_at = NOW();
                """,
                (
                    normalized_poll_id,
                    int(chat_id),
                    int(user_id),
                    normalized_word,
                    normalized_quiz_type,
                    int(selected_option_index) if selected_option_index is not None else None,
                    normalized_selected_text,
                    bool(is_correct),
                ),
            )


def get_telegram_quiz_next_mode(chat_id: int) -> str:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT next_mode
                FROM bt_3_telegram_quiz_delivery_state
                WHERE chat_id = %s
                LIMIT 1;
                """,
                (int(chat_id),),
            )
            row = cursor.fetchone()
    if not row:
        return "new"
    return _normalize_telegram_quiz_delivery_mode(row[0])


def set_telegram_quiz_next_mode(chat_id: int, next_mode: str) -> None:
    normalized_mode = _normalize_telegram_quiz_delivery_mode(next_mode)
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_telegram_quiz_delivery_state (
                    chat_id,
                    next_mode,
                    updated_at
                )
                VALUES (%s, %s, NOW())
                ON CONFLICT (chat_id) DO UPDATE
                SET
                    next_mode = EXCLUDED.next_mode,
                    updated_at = NOW();
                """,
                (int(chat_id), normalized_mode),
            )


def update_webapp_dictionary_entry(entry_id: int, response_json: dict, translation_de: str | None = None) -> None:
    if not entry_id:
        return
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            if translation_de is not None:
                cursor.execute("""
                    UPDATE bt_3_webapp_dictionary_queries
                    SET response_json = %s,
                        translation_de = %s
                    WHERE id = %s;
                """, (
                    json.dumps(response_json, ensure_ascii=False),
                    translation_de,
                    entry_id,
                ))
            else:
                cursor.execute("""
                    UPDATE bt_3_webapp_dictionary_queries
                    SET response_json = %s
                    WHERE id = %s;
                """, (
                    json.dumps(response_json, ensure_ascii=False),
                    entry_id,
                ))


def create_flashcard_feel_feedback_token(
    *,
    user_id: int,
    entry_id: int,
    feel_explanation: str,
) -> str:
    safe_user_id = int(user_id)
    safe_entry_id = int(entry_id)
    safe_text = str(feel_explanation or "").strip()
    if not safe_text:
        raise ValueError("feel_explanation is required")
    token = uuid4().hex
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_flashcard_feel_feedback_queue (
                    token,
                    user_id,
                    entry_id,
                    feel_explanation
                )
                VALUES (%s, %s, %s, %s);
                """,
                (token, safe_user_id, safe_entry_id, safe_text),
            )
    return token


def apply_flashcard_feel_feedback(
    *,
    token: str,
    user_id: int,
    liked: bool,
) -> dict | None:
    safe_token = str(token or "").strip()
    if not safe_token:
        return None
    safe_user_id = int(user_id)
    action = "like" if bool(liked) else "dislike"
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    entry_id,
                    feel_explanation,
                    consumed_at,
                    feedback_action
                FROM bt_3_flashcard_feel_feedback_queue
                WHERE token = %s
                  AND user_id = %s
                FOR UPDATE;
                """,
                (safe_token, safe_user_id),
            )
            row = cursor.fetchone()
            if not row:
                return None
            entry_id = int(row[0] or 0)
            feel_explanation = str(row[1] or "").strip()
            consumed_at = row[2]
            previous_action = str(row[3] or "").strip().lower()
            if consumed_at is not None:
                return {
                    "ok": True,
                    "already_processed": True,
                    "action": previous_action or action,
                    "entry_id": entry_id,
                }
            if not entry_id:
                cursor.execute(
                    """
                    UPDATE bt_3_flashcard_feel_feedback_queue
                    SET consumed_at = NOW(),
                        feedback_action = %s
                    WHERE token = %s;
                    """,
                    (action, safe_token),
                )
                return {
                    "ok": True,
                    "already_processed": False,
                    "action": action,
                    "entry_id": 0,
                }

            cursor.execute(
                """
                SELECT response_json
                FROM bt_3_webapp_dictionary_queries
                WHERE id = %s
                  AND user_id = %s
                FOR UPDATE;
                """,
                (entry_id, safe_user_id),
            )
            entry_row = cursor.fetchone()
            if not entry_row:
                cursor.execute(
                    """
                    UPDATE bt_3_flashcard_feel_feedback_queue
                    SET consumed_at = NOW(),
                        feedback_action = %s
                    WHERE token = %s;
                    """,
                    (action, safe_token),
                )
                return {
                    "ok": True,
                    "already_processed": False,
                    "action": action,
                    "entry_id": entry_id,
                }

            response_json = entry_row[0]
            if isinstance(response_json, str):
                try:
                    response_json = json.loads(response_json)
                except Exception:
                    response_json = {}
            if not isinstance(response_json, dict):
                response_json = {}

            if action == "like":
                response_json["feel_explanation"] = feel_explanation
                response_json["feel_feedback"] = "like"
            else:
                response_json.pop("feel_explanation", None)
                response_json["feel_feedback"] = "dislike"

            cursor.execute(
                """
                UPDATE bt_3_webapp_dictionary_queries
                SET response_json = %s
                WHERE id = %s
                  AND user_id = %s;
                """,
                (json.dumps(response_json, ensure_ascii=False), entry_id, safe_user_id),
            )
            cursor.execute(
                """
                UPDATE bt_3_flashcard_feel_feedback_queue
                SET consumed_at = NOW(),
                    feedback_action = %s
                WHERE token = %s;
                """,
                (action, safe_token),
            )
    return {
        "ok": True,
        "already_processed": False,
        "action": action,
        "entry_id": entry_id,
    }


def get_dictionary_cache(word_ru: str) -> dict | None:
    if not word_ru:
        return None
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT response_json
                FROM bt_3_dictionary_cache
                WHERE word_ru = %s;
            """, (word_ru,))
            row = cursor.fetchone()
            if not row:
                return None
            response_json = row[0]
            if isinstance(response_json, str):
                try:
                    return json.loads(response_json)
                except json.JSONDecodeError:
                    return None
            return response_json


def upsert_dictionary_cache(word_ru: str, response_json: dict) -> None:
    if not word_ru:
        return
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO bt_3_dictionary_cache (word_ru, response_json, updated_at)
                VALUES (%s, %s, NOW())
                ON CONFLICT (word_ru) DO UPDATE
                SET response_json = EXCLUDED.response_json,
                    updated_at = NOW();
            """, (
                word_ru,
                json.dumps(response_json, ensure_ascii=False),
            ))


def get_dictionary_lookup_cache(cache_key: str, ttl_seconds: int | None = None) -> dict | None:
    cache_key_value = str(cache_key or "").strip()
    if not cache_key_value:
        return None
    ttl_value = int(ttl_seconds or 0)
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            if ttl_value > 0:
                cursor.execute(
                    """
                    SELECT response_json
                    FROM bt_3_dictionary_lookup_cache
                    WHERE cache_key = %s
                      AND updated_at >= NOW() - (%s * INTERVAL '1 second');
                    """,
                    (cache_key_value, ttl_value),
                )
            else:
                cursor.execute(
                    """
                    SELECT response_json
                    FROM bt_3_dictionary_lookup_cache
                    WHERE cache_key = %s;
                    """,
                    (cache_key_value,),
                )
            row = cursor.fetchone()
            if not row:
                return None
            response_json = row[0]
            if isinstance(response_json, str):
                try:
                    response_json = json.loads(response_json)
                except Exception:
                    return None
            if not isinstance(response_json, dict):
                return None
            try:
                cursor.execute(
                    """
                    UPDATE bt_3_dictionary_lookup_cache
                    SET hit_count = hit_count + 1
                    WHERE cache_key = %s;
                    """,
                    (cache_key_value,),
                )
            except Exception:
                pass
            return dict(response_json)


def upsert_dictionary_lookup_cache(
    *,
    cache_key: str,
    source_lang: str,
    target_lang: str,
    query_source_lang: str,
    query_target_lang: str,
    lookup_lang: str,
    normalized_word: str,
    response_json: dict,
) -> None:
    cache_key_value = str(cache_key or "").strip()
    if not cache_key_value or not isinstance(response_json, dict):
        return
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_dictionary_lookup_cache (
                    cache_key,
                    source_lang,
                    target_lang,
                    query_source_lang,
                    query_target_lang,
                    lookup_lang,
                    normalized_word,
                    response_json,
                    hit_count,
                    created_at,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 0, NOW(), NOW())
                ON CONFLICT (cache_key) DO UPDATE
                SET source_lang = EXCLUDED.source_lang,
                    target_lang = EXCLUDED.target_lang,
                    query_source_lang = EXCLUDED.query_source_lang,
                    query_target_lang = EXCLUDED.query_target_lang,
                    lookup_lang = EXCLUDED.lookup_lang,
                    normalized_word = EXCLUDED.normalized_word,
                    response_json = EXCLUDED.response_json,
                    updated_at = NOW();
                """,
                (
                    cache_key_value,
                    str(source_lang or "").strip().lower(),
                    str(target_lang or "").strip().lower(),
                    str(query_source_lang or "").strip().lower(),
                    str(query_target_lang or "").strip().lower(),
                    str(lookup_lang or "").strip().lower(),
                    str(normalized_word or "").strip(),
                    Json(response_json),
                ),
            )


def get_youtube_transcript_cache(video_id: str) -> dict | None:
    if not video_id:
        return None
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT items, language, is_generated, translations, updated_at
                FROM bt_3_youtube_transcripts
                WHERE video_id = %s;
            """, (video_id,))
            row = cursor.fetchone()
            if not row:
                return None
            items, language, is_generated, translations, updated_at = row
            if isinstance(items, str):
                try:
                    items = json.loads(items)
                except Exception:
                    items = []
            if isinstance(translations, str):
                try:
                    translations = json.loads(translations)
                except Exception:
                    translations = {}
            return {
                "items": items or [],
                "language": language,
                "is_generated": is_generated,
                "translations": translations or {},
                "updated_at": updated_at,
            }


def upsert_youtube_transcript_cache(
    video_id: str,
    items: list,
    language: str | None,
    is_generated: bool | None,
    translations: dict | None = None,
) -> None:
    if not video_id:
        return
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO bt_3_youtube_transcripts (video_id, items, language, is_generated, translations, updated_at)
                VALUES (%s, %s, %s, %s, %s, NOW())
                ON CONFLICT (video_id) DO UPDATE
                SET items = EXCLUDED.items,
                    language = EXCLUDED.language,
                    is_generated = EXCLUDED.is_generated,
                    translations = COALESCE(EXCLUDED.translations, bt_3_youtube_transcripts.translations),
                    updated_at = NOW();
            """, (
                video_id,
                json.dumps(items, ensure_ascii=False),
                language,
                is_generated,
                json.dumps(translations, ensure_ascii=False) if translations is not None else None,
            ))


def purge_old_youtube_transcripts(days: int = 7) -> None:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                DELETE FROM bt_3_youtube_transcripts
                WHERE updated_at < NOW() - (%s || ' days')::interval;
            """, (days,))


def upsert_youtube_translations(video_id: str, translations: dict) -> None:
    if not video_id or not translations:
        return
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO bt_3_youtube_transcripts (video_id, items, translations, updated_at)
                VALUES (%s, '[]'::jsonb, %s, NOW())
                ON CONFLICT (video_id) DO UPDATE
                SET translations = COALESCE(bt_3_youtube_transcripts.translations, '{}'::jsonb) || EXCLUDED.translations,
                    updated_at = NOW();
            """, (video_id, json.dumps(translations, ensure_ascii=False)))


def _youtube_watch_state_row_to_dict(row) -> dict | None:
    if not row:
        return None
    return {
        "user_id": int(row[0]),
        "video_id": str(row[1] or "").strip(),
        "input_text": str(row[2] or "").strip(),
        "current_time_seconds": max(0, int(row[3] or 0)),
        "last_opened_at": row[4].isoformat() if row[4] else None,
        "created_at": row[5].isoformat() if row[5] else None,
        "updated_at": row[6].isoformat() if row[6] else None,
    }


def get_youtube_watch_state(user_id: int, video_id: str) -> dict | None:
    normalized_video_id = str(video_id or "").strip()
    if not normalized_video_id:
        return None
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT user_id, video_id, input_text, current_time_seconds, last_opened_at, created_at, updated_at
                FROM bt_3_youtube_watch_state
                WHERE user_id = %s
                  AND video_id = %s
                LIMIT 1;
                """,
                (int(user_id), normalized_video_id),
            )
            row = cursor.fetchone()
    return _youtube_watch_state_row_to_dict(row)


def get_latest_youtube_watch_state(user_id: int) -> dict | None:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT user_id, video_id, input_text, current_time_seconds, last_opened_at, created_at, updated_at
                FROM bt_3_youtube_watch_state
                WHERE user_id = %s
                ORDER BY updated_at DESC
                LIMIT 1;
                """,
                (int(user_id),),
            )
            row = cursor.fetchone()
    return _youtube_watch_state_row_to_dict(row)


def upsert_youtube_watch_state(
    *,
    user_id: int,
    video_id: str,
    current_time_seconds: int | float,
    input_text: str | None = None,
) -> dict | None:
    normalized_video_id = str(video_id or "").strip()
    if not normalized_video_id:
        return None
    resolved_input = str(input_text or "").strip()
    safe_seconds = max(0, int(float(current_time_seconds or 0)))
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_youtube_watch_state (
                    user_id,
                    video_id,
                    input_text,
                    current_time_seconds,
                    last_opened_at,
                    created_at,
                    updated_at
                )
                VALUES (%s, %s, NULLIF(%s, ''), %s, NOW(), NOW(), NOW())
                ON CONFLICT (user_id, video_id) DO UPDATE
                SET
                    input_text = COALESCE(NULLIF(EXCLUDED.input_text, ''), bt_3_youtube_watch_state.input_text),
                    current_time_seconds = GREATEST(EXCLUDED.current_time_seconds, 0),
                    last_opened_at = NOW(),
                    updated_at = NOW()
                RETURNING user_id, video_id, input_text, current_time_seconds, last_opened_at, created_at, updated_at;
                """,
                (
                    int(user_id),
                    normalized_video_id,
                    resolved_input,
                    safe_seconds,
                ),
            )
            row = cursor.fetchone()
    return _youtube_watch_state_row_to_dict(row)


def _normalize_translation_draft_map(drafts) -> dict[str, str]:
    if not isinstance(drafts, dict):
        return {}
    normalized: dict[str, str] = {}
    for raw_key, raw_value in drafts.items():
        try:
            sentence_id = int(raw_key)
        except Exception:
            continue
        if sentence_id <= 0:
            continue
        text = str(raw_value if raw_value is not None else "")
        if text == "":
            continue
        normalized[str(sentence_id)] = text[:10000]
    return normalized


def _translation_draft_state_row_to_dict(row) -> dict | None:
    if not row:
        return None
    payload = row[2] if isinstance(row[2], dict) else {}
    return {
        "user_id": int(row[0]),
        "source_session_id": str(row[1] or "").strip(),
        "drafts": _normalize_translation_draft_map(payload),
        "created_at": row[3].isoformat() if row[3] else None,
        "updated_at": row[4].isoformat() if row[4] else None,
    }


def get_translation_draft_state(user_id: int, source_session_id: str) -> dict | None:
    normalized_session_id = str(source_session_id or "").strip()
    if not normalized_session_id:
        return None
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT user_id, source_session_id, drafts_json, created_at, updated_at
                FROM bt_3_translation_draft_state
                WHERE user_id = %s
                  AND source_session_id = %s
                LIMIT 1;
                """,
                (int(user_id), normalized_session_id),
            )
            row = cursor.fetchone()
    return _translation_draft_state_row_to_dict(row)


def delete_translation_draft_state(*, user_id: int, source_session_id: str) -> bool:
    normalized_session_id = str(source_session_id or "").strip()
    if not normalized_session_id:
        return False
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                DELETE FROM bt_3_translation_draft_state
                WHERE user_id = %s
                  AND source_session_id = %s;
                """,
                (int(user_id), normalized_session_id),
            )
            deleted = int(cursor.rowcount or 0) > 0
    return deleted


def upsert_translation_draft_state(
    *,
    user_id: int,
    source_session_id: str,
    drafts: dict | None,
) -> dict | None:
    normalized_session_id = str(source_session_id or "").strip()
    if not normalized_session_id:
        return None
    normalized_drafts = _normalize_translation_draft_map(drafts or {})
    if not normalized_drafts:
        delete_translation_draft_state(user_id=int(user_id), source_session_id=normalized_session_id)
        return None
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_translation_draft_state (
                    user_id,
                    source_session_id,
                    drafts_json,
                    created_at,
                    updated_at
                )
                VALUES (%s, %s, %s, NOW(), NOW())
                ON CONFLICT (user_id, source_session_id) DO UPDATE
                SET
                    drafts_json = EXCLUDED.drafts_json,
                    updated_at = NOW()
                RETURNING user_id, source_session_id, drafts_json, created_at, updated_at;
                """,
                (
                    int(user_id),
                    normalized_session_id,
                    Json(normalized_drafts),
                ),
            )
            row = cursor.fetchone()
    return _translation_draft_state_row_to_dict(row)


def delete_translation_draft_state_entries(
    *,
    user_id: int,
    source_session_id: str,
    sentence_ids: list[int] | tuple[int, ...] | set[int],
) -> dict | None:
    normalized_session_id = str(source_session_id or "").strip()
    if not normalized_session_id:
        return None
    removable_ids = {
        str(int(item))
        for item in list(sentence_ids or [])
        if item is not None and int(item) > 0
    }
    if not removable_ids:
        return get_translation_draft_state(int(user_id), normalized_session_id)
    current = get_translation_draft_state(int(user_id), normalized_session_id)
    if not current:
        return None
    next_drafts = {
        key: value
        for key, value in (current.get("drafts") or {}).items()
        if key not in removable_ids
    }
    return upsert_translation_draft_state(
        user_id=int(user_id),
        source_session_id=normalized_session_id,
        drafts=next_drafts,
    )


def get_tts_chunk_cache(cache_key: str) -> list[str] | None:
    if not cache_key:
        return None
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT chunks
                FROM bt_3_tts_chunk_cache
                WHERE cache_key = %s;
                """,
                (cache_key,),
            )
            row = cursor.fetchone()
            if not row:
                return None
            cursor.execute(
                """
                UPDATE bt_3_tts_chunk_cache
                SET hit_count = hit_count + 1,
                    updated_at = NOW()
                WHERE cache_key = %s;
                """,
                (cache_key,),
            )
            chunks = row[0]
            if isinstance(chunks, str):
                try:
                    chunks = json.loads(chunks)
                except Exception:
                    chunks = []
            if not isinstance(chunks, list):
                return None
            return [str(item).strip() for item in chunks if str(item).strip()]


def upsert_tts_chunk_cache(
    cache_key: str,
    language: str,
    source_text: str,
    chunks: list[str],
) -> None:
    if not cache_key or not source_text or not chunks:
        return
    normalized_chunks = [str(item).strip() for item in chunks if str(item).strip()]
    if not normalized_chunks:
        return
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_tts_chunk_cache (
                    cache_key,
                    language,
                    source_text,
                    chunks,
                    hit_count,
                    created_at,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, 1, NOW(), NOW())
                ON CONFLICT (cache_key) DO UPDATE
                SET language = EXCLUDED.language,
                    source_text = EXCLUDED.source_text,
                    chunks = EXCLUDED.chunks,
                    hit_count = bt_3_tts_chunk_cache.hit_count + 1,
                    updated_at = NOW();
                """,
                (
                    cache_key,
                    language,
                    source_text,
                    json.dumps(normalized_chunks, ensure_ascii=False),
                ),
            )


def get_tts_audio_cache(cache_key: str) -> bytes | None:
    if not cache_key:
        return None
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT audio_mp3
                FROM bt_3_tts_audio_cache
                WHERE cache_key = %s;
                """,
                (cache_key,),
            )
            row = cursor.fetchone()
            if not row:
                return None
            cursor.execute(
                """
                UPDATE bt_3_tts_audio_cache
                SET hit_count = hit_count + 1,
                    updated_at = NOW()
                WHERE cache_key = %s;
                """,
                (cache_key,),
            )
            payload = row[0]
            if payload is None:
                return None
            if isinstance(payload, memoryview):
                return payload.tobytes()
            return bytes(payload)


def upsert_tts_audio_cache(
    cache_key: str,
    language: str,
    voice: str,
    speed: float,
    source_text: str,
    audio_mp3: bytes,
) -> None:
    if not cache_key or not source_text or not audio_mp3:
        return
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_tts_audio_cache (
                    cache_key,
                    language,
                    voice,
                    speed,
                    source_text,
                    audio_mp3,
                    hit_count,
                    created_at,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, 1, NOW(), NOW())
                ON CONFLICT (cache_key) DO UPDATE
                SET language = EXCLUDED.language,
                    voice = EXCLUDED.voice,
                    speed = EXCLUDED.speed,
                    source_text = EXCLUDED.source_text,
                    audio_mp3 = EXCLUDED.audio_mp3,
                    hit_count = bt_3_tts_audio_cache.hit_count + 1,
                    updated_at = NOW();
                """,
                (
                    cache_key,
                    language,
                    voice,
                    speed,
                    source_text,
                    Binary(audio_mp3),
                ),
            )


def delete_stale_tts_db_cache(*, older_than_days: int) -> dict[str, int]:
    safe_older_than_days = max(1, int(older_than_days or 1))
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                DELETE FROM bt_3_tts_audio_cache
                WHERE updated_at < NOW() - (%s || ' days')::interval;
                """,
                (safe_older_than_days,),
            )
            deleted_audio = int(cursor.rowcount or 0)
            cursor.execute(
                """
                DELETE FROM bt_3_tts_chunk_cache
                WHERE updated_at < NOW() - (%s || ' days')::interval;
                """,
                (safe_older_than_days,),
            )
            deleted_chunks = int(cursor.rowcount or 0)
    return {
        "audio_rows": deleted_audio,
        "chunk_rows": deleted_chunks,
        "total_rows": deleted_audio + deleted_chunks,
    }


def _map_tts_object_cache_row(row: tuple) -> dict:
    return {
        "cache_key": str(row[0] or ""),
        "status": str(row[1] or "pending"),
        "language": str(row[2] or "").strip() or None,
        "voice": str(row[3] or "").strip() or None,
        "speed": float(row[4]) if row[4] is not None else None,
        "source_text": str(row[5] or "").strip() or None,
        "object_key": str(row[6] or "").strip() or None,
        "url": str(row[7] or "").strip() or None,
        "size_bytes": int(row[8]) if row[8] is not None else None,
        "error_code": str(row[9] or "").strip() or None,
        "error_msg": str(row[10] or "").strip() or None,
        "created_at": row[11].isoformat() if row[11] else None,
        "updated_at": row[12].isoformat() if row[12] else None,
        "last_hit_at": row[13].isoformat() if row[13] else None,
    }


def _map_tts_admin_monitor_event_row(row: tuple) -> dict:
    created_at = row[7]
    return {
        "id": int(row[0] or 0),
        "kind": str(row[1] or "").strip().lower() or "unknown",
        "status": str(row[2] or "").strip().lower() or "unknown",
        "source": str(row[3] or "").strip().lower() or "unknown",
        "count": max(0, int(row[4] or 0)),
        "chars": max(0, int(row[5] or 0)),
        "duration_ms": int(row[6]) if row[6] is not None else None,
        "meta": row[8] if isinstance(row[8], dict) else {},
        "created_at": created_at.isoformat() if created_at else None,
        "ts": float(created_at.timestamp()) if created_at else 0.0,
    }


def record_tts_admin_monitor_event(
    *,
    kind: str,
    status: str,
    source: str = "",
    count: int = 1,
    chars: int = 0,
    duration_ms: int | None = None,
    meta: dict | None = None,
) -> None:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_tts_admin_monitor_events (
                    kind,
                    status,
                    source,
                    count,
                    chars,
                    duration_ms,
                    meta,
                    created_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW());
                """,
                (
                    str(kind or "").strip().lower() or "unknown",
                    str(status or "").strip().lower() or "unknown",
                    str(source or "").strip().lower() or "unknown",
                    max(0, int(count or 0)),
                    max(0, int(chars or 0)),
                    int(duration_ms) if duration_ms is not None else None,
                    Json(meta if isinstance(meta, dict) else {}),
                ),
            )


def list_tts_admin_monitor_events_since(*, window_seconds: int) -> list[dict]:
    safe_window_seconds = max(1, int(window_seconds or 1))
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    id,
                    kind,
                    status,
                    source,
                    count,
                    chars,
                    duration_ms,
                    created_at,
                    meta
                FROM bt_3_tts_admin_monitor_events
                WHERE created_at >= NOW() - (%s || ' seconds')::interval
                ORDER BY created_at ASC, id ASC;
                """,
                (safe_window_seconds,),
            )
            rows = cursor.fetchall() or []
    return [_map_tts_admin_monitor_event_row(row) for row in rows]


def delete_old_tts_admin_monitor_events(*, older_than_seconds: int) -> int:
    safe_older_than_seconds = max(60, int(older_than_seconds or 60))
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                DELETE FROM bt_3_tts_admin_monitor_events
                WHERE created_at < NOW() - (%s || ' seconds')::interval;
                """,
                (safe_older_than_seconds,),
            )
            deleted = int(cursor.rowcount or 0)
    return deleted


def get_tts_object_cache(cache_key: str, *, touch_hit: bool = True) -> dict | None:
    if not cache_key:
        return None
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    cache_key,
                    status,
                    language,
                    voice,
                    speed,
                    source_text,
                    object_key,
                    url,
                    size_bytes,
                    error_code,
                    error_msg,
                    created_at,
                    updated_at,
                    last_hit_at
                FROM bt_3_tts_object_cache
                WHERE cache_key = %s
                LIMIT 1;
                """,
                (str(cache_key),),
            )
            row = cursor.fetchone()
            if not row:
                return None
            if touch_hit:
                cursor.execute(
                    """
                    UPDATE bt_3_tts_object_cache
                    SET last_hit_at = NOW(), updated_at = NOW()
                    WHERE cache_key = %s;
                    """,
                    (str(cache_key),),
                )
    return _map_tts_object_cache_row(row)


def try_create_tts_object_cache_pending(
    *,
    cache_key: str,
    language: str | None = None,
    voice: str | None = None,
    speed: float | None = None,
    source_text: str | None = None,
    object_key: str | None = None,
) -> bool:
    if not cache_key:
        return False
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_tts_object_cache (
                    cache_key,
                    status,
                    language,
                    voice,
                    speed,
                    source_text,
                    object_key,
                    created_at,
                    updated_at
                )
                VALUES (%s, 'pending', %s, %s, %s, %s, %s, NOW(), NOW())
                ON CONFLICT (cache_key) DO NOTHING;
                """,
                (
                    str(cache_key),
                    str(language or "").strip() or None,
                    str(voice or "").strip() or None,
                    float(speed) if speed is not None else None,
                    str(source_text or "").strip() or None,
                    str(object_key or "").strip() or None,
                ),
            )
            created = cursor.rowcount > 0
    return bool(created)


def mark_tts_object_cache_ready(
    *,
    cache_key: str,
    object_key: str,
    url: str,
    size_bytes: int | None = None,
    language: str | None = None,
    voice: str | None = None,
    speed: float | None = None,
    source_text: str | None = None,
) -> dict | None:
    if not cache_key or not object_key or not url:
        return None
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_tts_object_cache (
                    cache_key,
                    status,
                    language,
                    voice,
                    speed,
                    source_text,
                    object_key,
                    url,
                    size_bytes,
                    error_code,
                    error_msg,
                    created_at,
                    updated_at,
                    last_hit_at
                )
                VALUES (%s, 'ready', %s, %s, %s, %s, %s, %s, %s, NULL, NULL, NOW(), NOW(), NOW())
                ON CONFLICT (cache_key) DO UPDATE
                SET
                    status = 'ready',
                    language = COALESCE(EXCLUDED.language, bt_3_tts_object_cache.language),
                    voice = COALESCE(EXCLUDED.voice, bt_3_tts_object_cache.voice),
                    speed = COALESCE(EXCLUDED.speed, bt_3_tts_object_cache.speed),
                    source_text = COALESCE(EXCLUDED.source_text, bt_3_tts_object_cache.source_text),
                    object_key = EXCLUDED.object_key,
                    url = EXCLUDED.url,
                    size_bytes = COALESCE(EXCLUDED.size_bytes, bt_3_tts_object_cache.size_bytes),
                    error_code = NULL,
                    error_msg = NULL,
                    updated_at = NOW(),
                    last_hit_at = NOW()
                RETURNING
                    cache_key,
                    status,
                    language,
                    voice,
                    speed,
                    source_text,
                    object_key,
                    url,
                    size_bytes,
                    error_code,
                    error_msg,
                    created_at,
                    updated_at,
                    last_hit_at;
                """,
                (
                    str(cache_key),
                    str(language or "").strip() or None,
                    str(voice or "").strip() or None,
                    float(speed) if speed is not None else None,
                    str(source_text or "").strip() or None,
                    str(object_key).strip(),
                    str(url).strip(),
                    int(size_bytes) if size_bytes is not None else None,
                ),
            )
            row = cursor.fetchone()
    return _map_tts_object_cache_row(row) if row else None


def mark_tts_object_cache_failed(
    *,
    cache_key: str,
    error_code: str | None = None,
    error_msg: str | None = None,
    language: str | None = None,
    voice: str | None = None,
    speed: float | None = None,
    source_text: str | None = None,
    object_key: str | None = None,
) -> dict | None:
    if not cache_key:
        return None
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_tts_object_cache (
                    cache_key,
                    status,
                    language,
                    voice,
                    speed,
                    source_text,
                    object_key,
                    error_code,
                    error_msg,
                    created_at,
                    updated_at
                )
                VALUES (%s, 'failed', %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                ON CONFLICT (cache_key) DO UPDATE
                SET
                    status = 'failed',
                    language = COALESCE(EXCLUDED.language, bt_3_tts_object_cache.language),
                    voice = COALESCE(EXCLUDED.voice, bt_3_tts_object_cache.voice),
                    speed = COALESCE(EXCLUDED.speed, bt_3_tts_object_cache.speed),
                    source_text = COALESCE(EXCLUDED.source_text, bt_3_tts_object_cache.source_text),
                    object_key = COALESCE(EXCLUDED.object_key, bt_3_tts_object_cache.object_key),
                    error_code = COALESCE(EXCLUDED.error_code, bt_3_tts_object_cache.error_code),
                    error_msg = COALESCE(EXCLUDED.error_msg, bt_3_tts_object_cache.error_msg),
                    updated_at = NOW()
                RETURNING
                    cache_key,
                    status,
                    language,
                    voice,
                    speed,
                    source_text,
                    object_key,
                    url,
                    size_bytes,
                    error_code,
                    error_msg,
                    created_at,
                    updated_at,
                    last_hit_at;
                """,
                (
                    str(cache_key),
                    str(language or "").strip() or None,
                    str(voice or "").strip() or None,
                    float(speed) if speed is not None else None,
                    str(source_text or "").strip() or None,
                    str(object_key or "").strip() or None,
                    str(error_code or "").strip() or None,
                    str(error_msg or "").strip() or None,
                ),
            )
            row = cursor.fetchone()
    return _map_tts_object_cache_row(row) if row else None


def get_tts_object_meta(cache_key: str, *, touch_hit: bool = True) -> dict | None:
    return get_tts_object_cache(cache_key, touch_hit=touch_hit)


def create_tts_object_pending(
    *,
    cache_key: str,
    language: str | None = None,
    voice: str | None = None,
    speed: float | None = None,
    source_text: str | None = None,
    object_key: str | None = None,
) -> bool:
    return try_create_tts_object_cache_pending(
        cache_key=cache_key,
        language=language,
        voice=voice,
        speed=speed,
        source_text=source_text,
        object_key=object_key,
    )


def requeue_tts_object_pending(
    *,
    cache_key: str,
    language: str | None = None,
    voice: str | None = None,
    speed: float | None = None,
    source_text: str | None = None,
    object_key: str | None = None,
) -> bool:
    if not cache_key:
        return False
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE bt_3_tts_object_cache
                SET
                    status = 'pending',
                    language = COALESCE(%s, language),
                    voice = COALESCE(%s, voice),
                    speed = COALESCE(%s, speed),
                    source_text = COALESCE(%s, source_text),
                    object_key = COALESCE(%s, object_key),
                    error_code = NULL,
                    error_msg = NULL,
                    updated_at = NOW()
                WHERE cache_key = %s
                  AND status = 'failed';
                """,
                (
                    str(language or "").strip() or None,
                    str(voice or "").strip() or None,
                    float(speed) if speed is not None else None,
                    str(source_text or "").strip() or None,
                    str(object_key or "").strip() or None,
                    str(cache_key),
                ),
            )
            claimed = cursor.rowcount > 0
    return bool(claimed)


def list_stale_pending_tts_objects(
    *,
    limit: int = 100,
    older_than_minutes: int = 2,
) -> list[dict]:
    safe_limit = max(1, min(1000, int(limit or 100)))
    safe_age_minutes = max(1, min(24 * 60, int(older_than_minutes or 2)))
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    cache_key,
                    status,
                    language,
                    voice,
                    speed,
                    source_text,
                    object_key,
                    url,
                    size_bytes,
                    error_code,
                    error_msg,
                    created_at,
                    updated_at,
                    last_hit_at
                FROM bt_3_tts_object_cache
                WHERE status = 'pending'
                  AND updated_at <= NOW() - (%s || ' minutes')::interval
                ORDER BY updated_at ASC
                LIMIT %s;
                """,
                (safe_age_minutes, safe_limit),
            )
            rows = cursor.fetchall() or []
    return [_map_tts_object_cache_row(row) for row in rows]


def list_stale_ready_tts_objects(
    *,
    limit: int = 500,
    older_than_days: int = 60,
) -> list[dict]:
    safe_limit = max(1, min(5000, int(limit or 500)))
    safe_age_days = max(1, int(older_than_days or 60))
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    cache_key,
                    status,
                    language,
                    voice,
                    speed,
                    source_text,
                    object_key,
                    url,
                    size_bytes,
                    error_code,
                    error_msg,
                    created_at,
                    updated_at,
                    last_hit_at
                FROM bt_3_tts_object_cache
                WHERE status = 'ready'
                  AND object_key IS NOT NULL
                  AND COALESCE(last_hit_at, updated_at, created_at) <= NOW() - (%s || ' days')::interval
                ORDER BY COALESCE(last_hit_at, updated_at, created_at) ASC
                LIMIT %s;
                """,
                (safe_age_days, safe_limit),
            )
            rows = cursor.fetchall() or []
    return [_map_tts_object_cache_row(row) for row in rows]


def delete_tts_object_cache_entry(*, cache_key: str) -> int:
    safe_cache_key = str(cache_key or "").strip()
    if not safe_cache_key:
        return 0
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                DELETE FROM bt_3_tts_object_cache
                WHERE cache_key = %s;
                """,
                (safe_cache_key,),
            )
            deleted = int(cursor.rowcount or 0)
    return deleted


def mark_tts_object_ready(
    *,
    cache_key: str,
    object_key: str,
    url: str,
    size_bytes: int | None = None,
    language: str | None = None,
    voice: str | None = None,
    speed: float | None = None,
    source_text: str | None = None,
) -> dict | None:
    return mark_tts_object_cache_ready(
        cache_key=cache_key,
        object_key=object_key,
        url=url,
        size_bytes=size_bytes,
        language=language,
        voice=voice,
        speed=speed,
        source_text=source_text,
    )


def mark_tts_object_failed(
    *,
    cache_key: str,
    error_code: str | None = None,
    error_msg: str | None = None,
    language: str | None = None,
    voice: str | None = None,
    speed: float | None = None,
    source_text: str | None = None,
    object_key: str | None = None,
) -> dict | None:
    return mark_tts_object_cache_failed(
        cache_key=cache_key,
        error_code=error_code,
        error_msg=error_msg,
        language=language,
        voice=voice,
        speed=speed,
        source_text=source_text,
        object_key=object_key,
    )


def record_flashcard_answer(user_id: int, entry_id: int, is_correct: bool) -> None:
    if not user_id or not entry_id:
        return
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO bt_3_flashcard_stats (user_id, entry_id, correct_count, wrong_count, last_result, updated_at)
                VALUES (%s, %s, %s, %s, %s, NOW())
                ON CONFLICT (user_id, entry_id) DO UPDATE
                SET correct_count = bt_3_flashcard_stats.correct_count + %s,
                    wrong_count = bt_3_flashcard_stats.wrong_count + %s,
                    last_result = EXCLUDED.last_result,
                    updated_at = NOW();
            """, (
                user_id,
                entry_id,
                1 if is_correct else 0,
                0 if is_correct else 1,
                is_correct,
                1 if is_correct else 0,
                0 if is_correct else 1,
            ))
            mark_flashcards_seen(user_id=user_id, entry_ids=[entry_id], cursor=cursor)


def mark_flashcards_seen(
    *,
    user_id: int,
    entry_ids: list[int] | tuple[int, ...],
    seen_at: datetime | None = None,
    cursor=None,
) -> int:
    if not user_id:
        return 0
    normalized_ids: list[int] = []
    seen_ids: set[int] = set()
    for raw_id in entry_ids or []:
        try:
            value = int(raw_id)
        except Exception:
            continue
        if value <= 0 or value in seen_ids:
            continue
        seen_ids.add(value)
        normalized_ids.append(value)
    if not normalized_ids:
        return 0

    normalized_seen_at = seen_at or datetime.now(timezone.utc)
    if normalized_seen_at.tzinfo is None:
        normalized_seen_at = normalized_seen_at.replace(tzinfo=timezone.utc)

    def _insert(cur) -> int:
        execute_values(
            cur,
            """
            INSERT INTO bt_3_flashcard_seen (user_id, entry_id, seen_at)
            VALUES %s;
            """,
            [
                (int(user_id), int(entry_id), normalized_seen_at)
                for entry_id in normalized_ids
            ],
        )
        return len(normalized_ids)

    if cursor is not None:
        return _insert(cursor)
    with get_db_connection_context() as conn:
        with conn.cursor() as own_cursor:
            return _insert(own_cursor)


def get_flashcard_set(
    user_id: int,
    set_size: int = 15,
    wrong_size: int = 5,
    folder_mode: str = "all",
    folder_id: int | None = None,
    source_lang: str | None = None,
    target_lang: str | None = None,
    randomize_pool: bool = False,
    exclude_recent_seen: bool = True,
    wrong_source: str = "legacy",
    diagnostics: dict | None = None,
) -> list[dict]:
    if not user_id:
        return []
    debug_info = diagnostics if isinstance(diagnostics, dict) else None
    wrong_ids: list[int] = []
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            language_filter_sql_q, language_params = _build_language_pair_filter(
                source_lang,
                target_lang,
                table_alias="q",
            )
            normalized_wrong_source = str(wrong_source or "legacy").strip().lower()
            if normalized_wrong_source == "fsrs_review_log":
                wrong_where = (
                    "l.user_id = %s "
                    "AND l.rating = 1 "
                    "AND COALESCE(q.response_json->>'sentence_origin', '') <> 'gpt_seed'"
                )
                wrong_params = [user_id]
                if language_filter_sql_q:
                    wrong_where += language_filter_sql_q
                    wrong_params.extend(language_params)
                if folder_mode == "folder" and folder_id is not None:
                    wrong_where += " AND q.folder_id = %s"
                    wrong_params.append(folder_id)
                elif folder_mode == "none":
                    wrong_where += " AND q.folder_id IS NULL"
                wrong_params.append(wrong_size)
                cursor.execute(f"""
                    WITH latest_review AS (
                        SELECT
                            l.card_id,
                            l.rating,
                            l.reviewed_at,
                            ROW_NUMBER() OVER (
                                PARTITION BY l.card_id
                                ORDER BY l.reviewed_at DESC
                            ) AS rn
                        FROM bt_3_card_review_log l
                        JOIN bt_3_webapp_dictionary_queries q ON q.id = l.card_id
                        WHERE {wrong_where}
                    )
                    SELECT card_id
                    FROM latest_review
                    WHERE rn = 1
                    ORDER BY reviewed_at DESC
                    LIMIT %s;
                """, wrong_params)
                wrong_ids = [row[0] for row in cursor.fetchall()]
            else:
                wrong_where = (
                    "s.user_id = %s "
                    "AND s.last_result = FALSE "
                    "AND COALESCE(q.response_json->>'sentence_origin', '') <> 'gpt_seed'"
                )
                wrong_params = [user_id]
                if language_filter_sql_q:
                    wrong_where += language_filter_sql_q
                    wrong_params.extend(language_params)
                if folder_mode == "folder" and folder_id is not None:
                    wrong_where += " AND q.folder_id = %s"
                    wrong_params.append(folder_id)
                elif folder_mode == "none":
                    wrong_where += " AND q.folder_id IS NULL"
                wrong_params.append(wrong_size)
                cursor.execute(f"""
                    SELECT s.entry_id
                    FROM bt_3_flashcard_stats s
                    JOIN bt_3_webapp_dictionary_queries q ON q.id = s.entry_id
                    WHERE {wrong_where}
                    ORDER BY s.updated_at DESC
                    LIMIT %s;
                """, wrong_params)
                wrong_ids = [row[0] for row in cursor.fetchall()]
            if debug_info is not None:
                debug_info["wrong_source"] = normalized_wrong_source
                debug_info["wrong_ids_initial"] = len(wrong_ids)

            if normalized_wrong_source != "fsrs_review_log" and len(wrong_ids) < wrong_size:
                extra_where = (
                    "s.user_id = %s "
                    "AND s.entry_id <> ALL(%s::bigint[]) "
                    "AND COALESCE(q.response_json->>'sentence_origin', '') <> 'gpt_seed'"
                )
                extra_params = [user_id, wrong_ids or [0]]
                if language_filter_sql_q:
                    extra_where += language_filter_sql_q
                    extra_params.extend(language_params)
                if folder_mode == "folder" and folder_id is not None:
                    extra_where += " AND q.folder_id = %s"
                    extra_params.append(folder_id)
                elif folder_mode == "none":
                    extra_where += " AND q.folder_id IS NULL"
                extra_params.append(wrong_size - len(wrong_ids))
                cursor.execute(f"""
                    SELECT s.entry_id
                    FROM bt_3_flashcard_stats s
                    JOIN bt_3_webapp_dictionary_queries q ON q.id = s.entry_id
                    WHERE {extra_where}
                    ORDER BY (s.wrong_count - s.correct_count) DESC, s.updated_at DESC
                    LIMIT %s;
                """, extra_params)
                wrong_ids.extend([row[0] for row in cursor.fetchall()])
            if debug_info is not None:
                debug_info["wrong_ids_final"] = len(wrong_ids)

            base_where = (
                "user_id = %s "
                "AND id <> ALL(%s::bigint[]) "
                "AND COALESCE(response_json->>'sentence_origin', '') <> 'gpt_seed'"
            )
            base_params = [user_id, wrong_ids or [0]]
            language_filter_sql, language_params_no_alias = _build_language_pair_filter(source_lang, target_lang)
            if language_filter_sql:
                base_where += language_filter_sql
                base_params.extend(language_params_no_alias)
            if folder_mode == "folder" and folder_id is not None:
                base_where += " AND folder_id = %s"
                base_params.append(folder_id)
            elif folder_mode == "none":
                base_where += " AND folder_id IS NULL"
            needed = max(set_size - len(wrong_ids), 0)
            random_rows = []
            if needed > 0:
                if randomize_pool and not exclude_recent_seen:
                    sample_cap = max(needed * 40, 2000)
                else:
                    sample_cap = max(needed * 12, 300)
                if debug_info is not None:
                    debug_info["needed_after_wrong_ids"] = needed
                    debug_info["sample_cap"] = sample_cap
                    debug_info["randomize_pool"] = bool(randomize_pool)
                    debug_info["exclude_recent_seen"] = bool(exclude_recent_seen)
                sample_order_sql = "ORDER BY RANDOM()" if randomize_pool else "ORDER BY created_at DESC"
                sample_where = base_where
                sample_params = list(base_params)
                if exclude_recent_seen:
                    sample_where += (
                        " AND id NOT IN ("
                        " SELECT entry_id"
                        " FROM bt_3_flashcard_seen"
                        " WHERE user_id = %s"
                        "   AND seen_at >= NOW() - (%s * INTERVAL '1 hour')"
                        " )"
                    )
                    sample_params.extend([user_id, FLASHCARD_RECENT_SEEN_HOURS])
                sample_params.append(sample_cap)
                cursor.execute(f"""
                    SELECT id, word_ru, translation_de, word_de, translation_ru, response_json
                    FROM bt_3_webapp_dictionary_queries
                    WHERE {sample_where}
                    {sample_order_sql}
                    LIMIT %s;
                """, sample_params)
                candidate_rows = list(cursor.fetchall())
                if debug_info is not None:
                    debug_info["sample_candidates"] = len(candidate_rows)
                random.shuffle(candidate_rows)
                random_rows = candidate_rows[:needed]
                if debug_info is not None:
                    debug_info["sample_selected"] = len(random_rows)

                if len(random_rows) < needed:
                    fallback_where = (
                        "user_id = %s "
                        "AND id <> ALL(%s::bigint[]) "
                        "AND COALESCE(response_json->>'sentence_origin', '') <> 'gpt_seed'"
                    )
                    fallback_params = [user_id, wrong_ids or [0]]
                    if language_filter_sql:
                        fallback_where += language_filter_sql
                        fallback_params.extend(language_params_no_alias)
                    if folder_mode == "folder" and folder_id is not None:
                        fallback_where += " AND folder_id = %s"
                        fallback_params.append(folder_id)
                    elif folder_mode == "none":
                        fallback_where += " AND folder_id IS NULL"
                    if randomize_pool and not exclude_recent_seen:
                        fallback_cap = max(needed * 80, 4000)
                    else:
                        fallback_cap = max(needed * 20, 600)
                    fallback_order_sql = "ORDER BY RANDOM()" if randomize_pool else "ORDER BY created_at DESC"
                    fallback_params.append(fallback_cap)
                    cursor.execute(f"""
                        SELECT id, word_ru, translation_de, word_de, translation_ru, response_json
                        FROM bt_3_webapp_dictionary_queries
                        WHERE {fallback_where}
                        {fallback_order_sql}
                        LIMIT %s;
                    """, fallback_params)
                    fallback_rows = list(cursor.fetchall())
                    if debug_info is not None:
                        debug_info["fallback_cap"] = fallback_cap
                        debug_info["fallback_candidates"] = len(fallback_rows)
                    existing_ids = {row[0] for row in random_rows}
                    for row in fallback_rows:
                        if row[0] in existing_ids:
                            continue
                        random_rows.append(row)
                        existing_ids.add(row[0])
                        if len(random_rows) >= needed:
                            break
                    if debug_info is not None:
                        debug_info["fallback_selected_total"] = len(random_rows)

            if wrong_ids:
                wrong_where = (
                    "user_id = %s "
                    "AND id = ANY(%s::bigint[]) "
                    "AND COALESCE(response_json->>'sentence_origin', '') <> 'gpt_seed'"
                )
                wrong_params = [user_id, wrong_ids]
                if language_filter_sql:
                    wrong_where += language_filter_sql
                    wrong_params.extend(language_params_no_alias)
                if folder_mode == "folder" and folder_id is not None:
                    wrong_where += " AND folder_id = %s"
                    wrong_params.append(folder_id)
                elif folder_mode == "none":
                    wrong_where += " AND folder_id IS NULL"
                cursor.execute(f"""
                    SELECT id, word_ru, translation_de, word_de, translation_ru, response_json
                    FROM bt_3_webapp_dictionary_queries
                    WHERE {wrong_where};
                """, wrong_params)
                wrong_rows = cursor.fetchall()
            else:
                wrong_rows = []

    rows = wrong_rows + random_rows
    items = []
    for row in rows:
        items.append({
            "id": row[0],
            "word_ru": row[1],
            "translation_de": row[2],
            "word_de": row[3],
            "translation_ru": row[4],
            "response_json": row[5],
        })
    if debug_info is not None:
        debug_info["wrong_rows_returned"] = len(wrong_rows)
        debug_info["random_rows_returned"] = len(random_rows)
        debug_info["returned_items"] = len(items)
    return items


def get_card_srs_state(user_id: int, card_id: int, cursor=None) -> dict | None:
    def _fetch(cur):
        cur.execute(
            """
            SELECT
                id,
                user_id,
                card_id,
                status,
                due_at,
                last_review_at,
                interval_days,
                reps,
                lapses,
                stability,
                difficulty,
                created_at,
                updated_at
            FROM bt_3_card_srs_state
            WHERE user_id = %s AND card_id = %s
            LIMIT 1;
            """,
            (int(user_id), int(card_id)),
        )
        row = cur.fetchone()
        if not row:
            return None
        return {
            "id": row[0],
            "user_id": row[1],
            "card_id": row[2],
            "status": row[3],
            "due_at": row[4],
            "last_review_at": row[5],
            "interval_days": row[6],
            "reps": row[7],
            "lapses": row[8],
            "stability": float(row[9] or 0.0),
            "difficulty": float(row[10] or 0.0),
            "created_at": row[11],
            "updated_at": row[12],
        }
    if cursor is not None:
        return _fetch(cursor)
    with get_db_connection_context() as conn:
        with conn.cursor() as own_cursor:
            return _fetch(own_cursor)


def upsert_card_srs_state(
    user_id: int,
    card_id: int,
    status: str,
    due_at: datetime,
    last_review_at: datetime | None,
    interval_days: int,
    reps: int,
    lapses: int,
    stability: float,
    difficulty: float,
    cursor=None,
) -> dict:
    def _upsert(cur):
        cur.execute(
            """
            INSERT INTO bt_3_card_srs_state (
                user_id,
                card_id,
                status,
                due_at,
                last_review_at,
                interval_days,
                reps,
                lapses,
                stability,
                difficulty,
                created_at,
                updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
            ON CONFLICT (user_id, card_id) DO UPDATE
            SET status = EXCLUDED.status,
                due_at = EXCLUDED.due_at,
                last_review_at = EXCLUDED.last_review_at,
                interval_days = EXCLUDED.interval_days,
                reps = EXCLUDED.reps,
                lapses = EXCLUDED.lapses,
                stability = EXCLUDED.stability,
                difficulty = EXCLUDED.difficulty,
                updated_at = NOW()
            RETURNING
                id,
                user_id,
                card_id,
                status,
                due_at,
                last_review_at,
                interval_days,
                reps,
                lapses,
                stability,
                difficulty,
                created_at,
                updated_at;
            """,
            (
                int(user_id),
                int(card_id),
                status,
                due_at,
                last_review_at,
                int(interval_days),
                int(reps),
                int(lapses),
                float(stability),
                float(difficulty),
            ),
        )
        row = cur.fetchone()
        return {
            "id": row[0],
            "user_id": row[1],
            "card_id": row[2],
            "status": row[3],
            "due_at": row[4],
            "last_review_at": row[5],
            "interval_days": row[6],
            "reps": row[7],
            "lapses": row[8],
            "stability": float(row[9] or 0.0),
            "difficulty": float(row[10] or 0.0),
            "created_at": row[11],
            "updated_at": row[12],
        }
    if cursor is not None:
        return _upsert(cursor)
    with get_db_connection_context() as conn:
        with conn.cursor() as own_cursor:
            return _upsert(own_cursor)


def count_due_srs_cards(
    user_id: int,
    now_utc: datetime | None = None,
    source_lang: str | None = None,
    target_lang: str | None = None,
    cursor=None,
) -> int:
    now_utc = now_utc or datetime.now(timezone.utc)
    def _count(cur):
        language_filter_sql, language_params = _build_language_pair_filter(
            source_lang,
            target_lang,
            table_alias="q",
        )
        cur.execute(
            f"""
            SELECT COUNT(*)
            FROM bt_3_card_srs_state s
            JOIN bt_3_webapp_dictionary_queries q
              ON q.id = s.card_id AND q.user_id = s.user_id
            WHERE s.user_id = %s
              AND s.status <> 'suspended'
              AND s.due_at <= %s
              AND COALESCE(q.response_json->>'sentence_origin', '') <> 'gpt_seed'
              {language_filter_sql};
            """,
            [int(user_id), now_utc, *language_params],
        )
        row = cur.fetchone()
        return int(row[0] if row else 0)
    if cursor is not None:
        return _count(cursor)
    with get_db_connection_context() as conn:
        with conn.cursor() as own_cursor:
            return _count(own_cursor)


def count_new_cards_introduced_today(
    user_id: int,
    now_utc: datetime | None = None,
    source_lang: str | None = None,
    target_lang: str | None = None,
    cursor=None,
) -> int:
    now_utc = now_utc or datetime.now(timezone.utc)
    day_start = datetime(now_utc.year, now_utc.month, now_utc.day, tzinfo=timezone.utc)
    day_end = day_start + timedelta(days=1)
    def _count(cur):
        language_filter_sql, language_params = _build_language_pair_filter(
            source_lang,
            target_lang,
            table_alias="q",
        )
        cur.execute(
            f"""
            SELECT COUNT(*)
            FROM bt_3_card_srs_state s
            JOIN bt_3_webapp_dictionary_queries q
              ON q.id = s.card_id AND q.user_id = s.user_id
            WHERE s.user_id = %s
              AND s.created_at >= %s
              AND s.created_at < %s
              AND COALESCE(q.response_json->>'sentence_origin', '') <> 'gpt_seed'
              {language_filter_sql};
            """,
            [int(user_id), day_start, day_end, *language_params],
        )
        row = cur.fetchone()
        return int(row[0] if row else 0)
    if cursor is not None:
        return _count(cursor)
    with get_db_connection_context() as conn:
        with conn.cursor() as own_cursor:
            return _count(own_cursor)


def has_available_new_srs_cards(
    user_id: int,
    source_lang: str | None = None,
    target_lang: str | None = None,
    cursor=None,
) -> bool:
    def _exists(cur):
        language_filter_sql, language_params = _build_language_pair_filter(
            source_lang,
            target_lang,
            table_alias="q",
        )
        cur.execute(
            f"""
            SELECT 1
            FROM bt_3_webapp_dictionary_queries q
            LEFT JOIN bt_3_card_srs_state s
              ON s.user_id = q.user_id AND s.card_id = q.id
            WHERE q.user_id = %s
              AND s.id IS NULL
              AND COALESCE(q.response_json->>'sentence_origin', '') <> 'gpt_seed'
              {language_filter_sql}
            LIMIT 1;
            """,
            [int(user_id), *language_params],
        )
        return cur.fetchone() is not None
    if cursor is not None:
        return _exists(cursor)
    with get_db_connection_context() as conn:
        with conn.cursor() as own_cursor:
            return _exists(own_cursor)


def count_available_new_srs_cards(
    user_id: int,
    source_lang: str | None = None,
    target_lang: str | None = None,
) -> int:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            language_filter_sql, language_params = _build_language_pair_filter(
                source_lang,
                target_lang,
                table_alias="q",
            )
            cursor.execute(
                f"""
                SELECT COUNT(*)
                FROM bt_3_webapp_dictionary_queries q
                LEFT JOIN bt_3_card_srs_state s
                  ON s.user_id = q.user_id AND s.card_id = q.id
                WHERE q.user_id = %s
                  AND s.id IS NULL
                  AND COALESCE(q.response_json->>'sentence_origin', '') <> 'gpt_seed'
                  {language_filter_sql};
                """,
                [int(user_id), *language_params],
            )
            row = cursor.fetchone()
            return int(row[0] if row else 0)


def get_next_due_srs_card(
    user_id: int,
    now_utc: datetime | None = None,
    source_lang: str | None = None,
    target_lang: str | None = None,
    cursor=None,
) -> dict | None:
    now_utc = now_utc or datetime.now(timezone.utc)
    def _fetch(cur):
        language_filter_sql, language_params = _build_language_pair_filter(
            source_lang,
            target_lang,
            table_alias="q",
        )
        cur.execute(
            f"""
            SELECT
                s.card_id,
                s.status,
                s.due_at,
                s.last_review_at,
                s.interval_days,
                s.reps,
                s.lapses,
                s.stability,
                s.difficulty,
                q.word_ru,
                q.translation_de,
                q.word_de,
                q.translation_ru,
                q.response_json
            FROM bt_3_card_srs_state s
            JOIN bt_3_webapp_dictionary_queries q
              ON q.id = s.card_id
             AND q.user_id = s.user_id
            WHERE s.user_id = %s
              AND s.status <> 'suspended'
              AND s.due_at <= %s
              AND COALESCE(q.response_json->>'sentence_origin', '') <> 'gpt_seed'
              {language_filter_sql}
            ORDER BY s.due_at ASC
            LIMIT 1;
            """,
            [int(user_id), now_utc, *language_params],
        )
        row = cur.fetchone()
        if not row:
            return None
        return {
            "card": {
                "id": row[0],
                "word_ru": row[9],
                "translation_de": row[10],
                "word_de": row[11],
                "translation_ru": row[12],
                "response_json": row[13],
            },
            "srs": {
                "status": row[1],
                "due_at": row[2],
                "last_review_at": row[3],
                "interval_days": int(row[4] or 0),
                "reps": int(row[5] or 0),
                "lapses": int(row[6] or 0),
                "stability": float(row[7] or 0.0),
                "difficulty": float(row[8] or 0.0),
            },
        }
    if cursor is not None:
        return _fetch(cursor)
    with get_db_connection_context() as conn:
        with conn.cursor() as own_cursor:
            return _fetch(own_cursor)


def get_next_new_srs_candidate(
    user_id: int,
    source_lang: str | None = None,
    target_lang: str | None = None,
    cursor=None,
) -> dict | None:
    def _fetch(cur):
        language_filter_sql, language_params = _build_language_pair_filter(
            source_lang,
            target_lang,
            table_alias="q",
        )
        cur.execute(
            f"""
            SELECT q.id, q.word_ru, q.translation_de, q.word_de, q.translation_ru, q.response_json
            FROM bt_3_webapp_dictionary_queries q
            LEFT JOIN bt_3_card_srs_state s
              ON s.user_id = q.user_id AND s.card_id = q.id
            WHERE q.user_id = %s
              AND s.id IS NULL
              AND COALESCE(q.response_json->>'sentence_origin', '') <> 'gpt_seed'
              {language_filter_sql}
            ORDER BY q.created_at ASC
            LIMIT 1;
            """,
            [int(user_id), *language_params],
        )
        row = cur.fetchone()
        if not row:
            return None
        return {
            "id": row[0],
            "word_ru": row[1],
            "translation_de": row[2],
            "word_de": row[3],
            "translation_ru": row[4],
            "response_json": row[5],
        }
    if cursor is not None:
        return _fetch(cursor)
    with get_db_connection_context() as conn:
        with conn.cursor() as own_cursor:
            return _fetch(own_cursor)


def ensure_new_srs_state(user_id: int, card_id: int, now_utc: datetime | None = None, cursor=None) -> dict:
    now_utc = now_utc or datetime.now(timezone.utc)
    state = get_card_srs_state(user_id=user_id, card_id=card_id, cursor=cursor)
    if state:
        return state
    return upsert_card_srs_state(
        user_id=user_id,
        card_id=card_id,
        status="new",
        due_at=now_utc,
        last_review_at=None,
        interval_days=0,
        reps=0,
        lapses=0,
        stability=0.0,
        difficulty=0.0,
        cursor=cursor,
    )


def get_dictionary_entry_for_user(user_id: int, card_id: int, cursor=None) -> dict | None:
    def _fetch(cur):
        cur.execute(
            """
            SELECT
                id,
                word_ru,
                translation_de,
                word_de,
                translation_ru,
                source_lang,
                target_lang,
                response_json
            FROM bt_3_webapp_dictionary_queries
            WHERE user_id = %s AND id = %s
            LIMIT 1;
            """,
            (int(user_id), int(card_id)),
        )
        row = cur.fetchone()
        if not row:
            return None
        return {
            "id": row[0],
            "word_ru": row[1],
            "translation_de": row[2],
            "word_de": row[3],
            "translation_ru": row[4],
            "source_lang": row[5],
            "target_lang": row[6],
            "response_json": row[7],
        }
    if cursor is not None:
        return _fetch(cursor)
    with get_db_connection_context() as conn:
        with conn.cursor() as own_cursor:
            return _fetch(own_cursor)


def insert_card_review_log(
    *,
    user_id: int,
    card_id: int,
    reviewed_at: datetime,
    rating: int,
    response_ms: int | None,
    scheduled_due_before: datetime | None,
    scheduled_due_after: datetime | None,
    stability_before: float | None,
    difficulty_before: float | None,
    stability_after: float | None,
    difficulty_after: float | None,
    interval_days_after: int | None,
    cursor=None,
) -> None:
    def _insert(cur):
        cur.execute(
            """
            INSERT INTO bt_3_card_review_log (
                user_id,
                card_id,
                reviewed_at,
                rating,
                response_ms,
                scheduled_due_before,
                scheduled_due_after,
                stability_before,
                difficulty_before,
                stability_after,
                difficulty_after,
                interval_days_after
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """,
            (
                int(user_id),
                int(card_id),
                reviewed_at,
                int(rating),
                int(response_ms) if response_ms is not None else None,
                scheduled_due_before,
                scheduled_due_after,
                float(stability_before) if stability_before is not None else None,
                float(difficulty_before) if difficulty_before is not None else None,
                float(stability_after) if stability_after is not None else None,
                float(difficulty_after) if difficulty_after is not None else None,
                int(interval_days_after) if interval_days_after is not None else None,
            ),
        )
    if cursor is not None:
        _insert(cursor)
        return
    with get_db_connection_context() as conn:
        with conn.cursor() as own_cursor:
            _insert(own_cursor)


def create_dictionary_folder(
    user_id: int,
    name: str,
    color: str,
    icon: str,
) -> dict:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO bt_3_dictionary_folders (user_id, name, color, icon)
                VALUES (%s, %s, %s, %s)
                RETURNING id, name, color, icon, created_at;
            """, (user_id, name, color, icon))
            row = cursor.fetchone()
            return {
                "id": row[0],
                "name": row[1],
                "color": row[2],
                "icon": row[3],
                "created_at": row[4].isoformat() if row[4] else None,
            }


def get_dictionary_folders(user_id: int) -> list[dict]:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT id, name, color, icon, created_at
                FROM bt_3_dictionary_folders
                WHERE user_id = %s
                ORDER BY created_at DESC;
            """, (user_id,))
            rows = cursor.fetchall()
            return [
                {
                    "id": row[0],
                    "name": row[1],
                    "color": row[2],
                    "icon": row[3],
                    "created_at": row[4].isoformat() if row[4] else None,
                }
                for row in rows
            ]


def get_or_create_dictionary_folder(
    user_id: int,
    name: str,
    color: str,
    icon: str,
) -> dict:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT id, name, color, icon, created_at
                FROM bt_3_dictionary_folders
                WHERE user_id = %s AND name = %s
                LIMIT 1;
                """,
                (user_id, name),
            )
            row = cursor.fetchone()
            if row:
                return {
                    "id": row[0],
                    "name": row[1],
                    "color": row[2],
                    "icon": row[3],
                    "created_at": row[4].isoformat() if row[4] else None,
                }
            cursor.execute(
                """
                INSERT INTO bt_3_dictionary_folders (user_id, name, color, icon)
                VALUES (%s, %s, %s, %s)
                RETURNING id, name, color, icon, created_at;
                """,
                (user_id, name, color, icon),
            )
            row = cursor.fetchone()
            return {
                "id": row[0],
                "name": row[1],
                "color": row[2],
                "icon": row[3],
                "created_at": row[4].isoformat() if row[4] else None,
            }


def record_telegram_system_message(
    chat_id: int,
    message_id: int,
    message_type: str = "text",
) -> None:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_telegram_system_messages (chat_id, message_id, message_type)
                VALUES (%s, %s, %s)
                ON CONFLICT (chat_id, message_id) DO NOTHING;
                """,
                (int(chat_id), int(message_id), (message_type or "text").strip()[:32]),
            )


def get_pending_telegram_system_messages(
    target_date: date,
    tz_name: str = "UTC",
    max_days_back: int = 2,
    limit: int = 5000,
    excluded_types: list[str] | None = None,
) -> list[dict]:
    max_days_back = max(0, int(max_days_back))
    excluded = [str(item or "").strip().lower() for item in (excluded_types or []) if str(item or "").strip()]
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            if excluded:
                cursor.execute(
                    """
                    SELECT id, chat_id, message_id, message_type, created_at
                    FROM bt_3_telegram_system_messages
                    WHERE deleted_at IS NULL
                      AND ((created_at AT TIME ZONE %s)::date BETWEEN %s AND %s)
                      AND LOWER(COALESCE(message_type, 'text')) <> ALL(%s)
                    ORDER BY created_at ASC
                    LIMIT %s;
                    """,
                    (
                        tz_name,
                        target_date - timedelta(days=max_days_back),
                        target_date,
                        excluded,
                        int(limit),
                    ),
                )
            else:
                cursor.execute(
                    """
                    SELECT id, chat_id, message_id, message_type, created_at
                    FROM bt_3_telegram_system_messages
                    WHERE deleted_at IS NULL
                      AND ((created_at AT TIME ZONE %s)::date BETWEEN %s AND %s)
                    ORDER BY created_at ASC
                    LIMIT %s;
                    """,
                    (
                        tz_name,
                        target_date - timedelta(days=max_days_back),
                        target_date,
                        int(limit),
                    ),
                )
            rows = cursor.fetchall()
    return [
        {
            "id": int(row[0]),
            "chat_id": int(row[1]),
            "message_id": int(row[2]),
            "message_type": row[3],
            "created_at": row[4].isoformat() if row[4] else None,
        }
        for row in rows
    ]


def mark_telegram_system_message_deleted(
    row_id: int,
    delete_error: str | None = None,
) -> None:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            if delete_error:
                cursor.execute(
                    """
                    UPDATE bt_3_telegram_system_messages
                    SET delete_error = %s
                    WHERE id = %s;
                    """,
                    (str(delete_error)[:500], int(row_id)),
                )
            else:
                cursor.execute(
                    """
                    UPDATE bt_3_telegram_system_messages
                    SET deleted_at = NOW(),
                        delete_error = NULL
                    WHERE id = %s;
                    """,
                    (int(row_id),),
                )


def update_telegram_system_message_type(
    *,
    chat_id: int,
    message_id: int,
    message_type: str,
) -> None:
    normalized_type = str(message_type or "").strip().lower()[:32] or "text"
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE bt_3_telegram_system_messages
                SET message_type = %s
                WHERE chat_id = %s
                  AND message_id = %s;
                """,
                (normalized_type, int(chat_id), int(message_id)),
            )


def has_admin_scheduler_run(
    *,
    job_key: str,
    run_period: str,
    target_chat_id: int,
) -> bool:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT 1
                FROM bt_3_admin_scheduler_runs
                WHERE job_key = %s
                  AND run_period = %s
                  AND target_chat_id = %s
                LIMIT 1;
                """,
                (
                    str(job_key or "").strip()[:80],
                    str(run_period or "").strip()[:32],
                    int(target_chat_id),
                ),
            )
            row = cursor.fetchone()
    return bool(row)


def mark_admin_scheduler_run(
    *,
    job_key: str,
    run_period: str,
    target_chat_id: int,
    metadata: dict | None = None,
) -> None:
    payload = metadata if isinstance(metadata, dict) else {}
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_admin_scheduler_runs (job_key, run_period, target_chat_id, metadata)
                VALUES (%s, %s, %s, %s::jsonb)
                ON CONFLICT (job_key, run_period, target_chat_id) DO NOTHING;
                """,
                (
                    str(job_key or "").strip()[:80],
                    str(run_period or "").strip()[:32],
                    int(target_chat_id),
                    Json(payload),
                ),
            )


def claim_scheduler_run_guard(
    *,
    job_key: str,
    run_period: str,
    target_scope: str = "global",
    metadata: dict | None = None,
) -> bool:
    payload = metadata if isinstance(metadata, dict) else {}
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_scheduler_run_guards (
                    job_key,
                    run_period,
                    target_scope,
                    status,
                    metadata,
                    claimed_at,
                    updated_at
                )
                VALUES (%s, %s, %s, 'running', %s::jsonb, NOW(), NOW())
                ON CONFLICT (job_key, run_period, target_scope) DO NOTHING
                RETURNING id;
                """,
                (
                    str(job_key or "").strip()[:80],
                    str(run_period or "").strip()[:32],
                    str(target_scope or "global").strip()[:80] or "global",
                    Json(payload),
                ),
            )
            row = cursor.fetchone()
    return bool(row)


def finish_scheduler_run_guard(
    *,
    job_key: str,
    run_period: str,
    target_scope: str = "global",
    status: str = "completed",
    metadata: dict | None = None,
) -> None:
    normalized_status = str(status or "completed").strip().lower()
    if normalized_status not in {"completed", "failed", "running"}:
        normalized_status = "completed"
    payload = metadata if isinstance(metadata, dict) else {}
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE bt_3_scheduler_run_guards
                SET
                    status = %s,
                    metadata = COALESCE(metadata, '{}'::jsonb) || %s::jsonb,
                    finished_at = CASE
                        WHEN %s = 'running' THEN finished_at
                        ELSE NOW()
                    END,
                    updated_at = NOW()
                WHERE job_key = %s
                  AND run_period = %s
                  AND target_scope = %s;
                """,
                (
                    normalized_status,
                    Json(payload),
                    normalized_status,
                    str(job_key or "").strip()[:80],
                    str(run_period or "").strip()[:32],
                    str(target_scope or "global").strip()[:80] or "global",
                ),
            )


def _map_semantic_benchmark_library_row(row: tuple | None) -> dict | None:
    if not row:
        return None
    return {
        "id": int(row[0]),
        "source_lang": str(row[1] or "ru"),
        "target_lang": str(row[2] or "de"),
        "source_sentence": str(row[3] or ""),
        "source_sentence_hash": str(row[4] or ""),
        "benchmark_json": row[5] if isinstance(row[5], dict) else {},
        "benchmark_status": str(row[6] or "pending"),
        "benchmark_confidence": str(row[7] or "") or None,
        "sentence_level_anchor": str(row[8] or "") or None,
        "prompt_version": str(row[9] or "") or None,
        "llm_model": str(row[10] or "") or None,
        "notes": str(row[11] or "") or None,
        "metadata": row[12] if isinstance(row[12], dict) else {},
        "approved_at": row[13].isoformat() if row[13] else None,
        "last_used_at": row[14].isoformat() if row[14] else None,
        "created_at": row[15].isoformat() if row[15] else None,
        "updated_at": row[16].isoformat() if row[16] else None,
    }


def get_semantic_benchmark_library_entry(
    *,
    source_sentence: str,
    source_lang: str = "ru",
    target_lang: str = "de",
) -> dict | None:
    sentence_text = " ".join(str(source_sentence or "").strip().split())
    if not sentence_text:
        return None
    sentence_hash = _semantic_benchmark_sentence_hash(
        sentence_text,
        source_lang=source_lang,
        target_lang=target_lang,
    )
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    id,
                    source_lang,
                    target_lang,
                    source_sentence,
                    source_sentence_hash,
                    benchmark_json,
                    benchmark_status,
                    benchmark_confidence,
                    sentence_level_anchor,
                    prompt_version,
                    llm_model,
                    notes,
                    metadata,
                    approved_at,
                    last_used_at,
                    created_at,
                    updated_at
                FROM bt_3_semantic_benchmark_library
                WHERE source_lang = %s
                  AND target_lang = %s
                  AND source_sentence_hash = %s
                LIMIT 1;
                """,
                (
                    str(source_lang or "ru").strip().lower(),
                    str(target_lang or "de").strip().lower(),
                    sentence_hash,
                ),
            )
            return _map_semantic_benchmark_library_row(cursor.fetchone())


def upsert_semantic_benchmark_library_entry(
    *,
    source_sentence: str,
    benchmark_json: dict | None,
    source_lang: str = "ru",
    target_lang: str = "de",
    benchmark_status: str = "ready",
    benchmark_confidence: str | None = None,
    sentence_level_anchor: str | None = None,
    prompt_version: str | None = None,
    llm_model: str | None = None,
    notes: str | None = None,
    metadata: dict | None = None,
    approved: bool = False,
) -> dict | None:
    sentence_text = " ".join(str(source_sentence or "").strip().split())
    if not sentence_text:
        return None
    status_value = str(benchmark_status or "ready").strip().lower() or "ready"
    confidence_value = str(benchmark_confidence or "").strip().lower() or None
    sentence_hash = _semantic_benchmark_sentence_hash(
        sentence_text,
        source_lang=source_lang,
        target_lang=target_lang,
    )
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_semantic_benchmark_library (
                    source_lang,
                    target_lang,
                    source_sentence,
                    source_sentence_hash,
                    benchmark_json,
                    benchmark_status,
                    benchmark_confidence,
                    sentence_level_anchor,
                    prompt_version,
                    llm_model,
                    notes,
                    metadata,
                    approved_at,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, NOW())
                ON CONFLICT (source_lang, target_lang, source_sentence_hash)
                DO UPDATE SET
                    source_sentence = EXCLUDED.source_sentence,
                    benchmark_json = EXCLUDED.benchmark_json,
                    benchmark_status = EXCLUDED.benchmark_status,
                    benchmark_confidence = EXCLUDED.benchmark_confidence,
                    sentence_level_anchor = EXCLUDED.sentence_level_anchor,
                    prompt_version = EXCLUDED.prompt_version,
                    llm_model = EXCLUDED.llm_model,
                    notes = EXCLUDED.notes,
                    metadata = EXCLUDED.metadata,
                    approved_at = COALESCE(EXCLUDED.approved_at, bt_3_semantic_benchmark_library.approved_at),
                    updated_at = NOW()
                RETURNING
                    id,
                    source_lang,
                    target_lang,
                    source_sentence,
                    source_sentence_hash,
                    benchmark_json,
                    benchmark_status,
                    benchmark_confidence,
                    sentence_level_anchor,
                    prompt_version,
                    llm_model,
                    notes,
                    metadata,
                    approved_at,
                    last_used_at,
                    created_at,
                    updated_at;
                """,
                (
                    str(source_lang or "ru").strip().lower(),
                    str(target_lang or "de").strip().lower(),
                    sentence_text,
                    sentence_hash,
                    Json(benchmark_json if isinstance(benchmark_json, dict) else {}),
                    status_value,
                    confidence_value,
                    str(sentence_level_anchor or "").strip() or None,
                    str(prompt_version or "").strip() or None,
                    str(llm_model or "").strip() or None,
                    str(notes or "").strip() or None,
                    Json(metadata if isinstance(metadata, dict) else {}),
                    datetime.now(timezone.utc) if approved else None,
                ),
            )
            return _map_semantic_benchmark_library_row(cursor.fetchone())


def touch_semantic_benchmark_library_entry(
    *,
    benchmark_id: int,
) -> None:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE bt_3_semantic_benchmark_library
                SET last_used_at = NOW(), updated_at = NOW()
                WHERE id = %s;
                """,
                (int(benchmark_id),),
            )


def enqueue_semantic_benchmark_candidate(
    *,
    source_sentence: str,
    source_lang: str = "ru",
    target_lang: str = "de",
    first_seen_at: datetime | None = None,
    last_seen_at: datetime | None = None,
    sample_count: int = 1,
    priority: float = 0.0,
    recent_source_session_ids: list[str] | None = None,
    recent_check_session_ids: list[int] | None = None,
    metadata: dict | None = None,
) -> dict | None:
    sentence_text = " ".join(str(source_sentence or "").strip().split())
    if not sentence_text:
        return None
    source_value = str(source_lang or "ru").strip().lower()
    target_value = str(target_lang or "de").strip().lower()
    sentence_hash = _semantic_benchmark_sentence_hash(
        sentence_text,
        source_lang=source_value,
        target_lang=target_value,
    )
    library_entry = get_semantic_benchmark_library_entry(
        source_sentence=sentence_text,
        source_lang=source_value,
        target_lang=target_value,
    )
    queue_status = "ready" if library_entry else "pending"
    benchmark_id = int(library_entry["id"]) if library_entry else None
    source_session_ids = [
        str(item).strip()
        for item in list(recent_source_session_ids or [])
        if str(item).strip()
    ][:10]
    check_session_ids = []
    for item in list(recent_check_session_ids or [])[:10]:
        try:
            check_session_ids.append(int(item))
        except Exception:
            continue
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_semantic_benchmark_queue (
                    source_lang,
                    target_lang,
                    source_sentence,
                    source_sentence_hash,
                    queue_status,
                    priority,
                    sample_count,
                    first_seen_at,
                    last_seen_at,
                    recent_source_session_ids,
                    recent_check_session_ids,
                    benchmark_id,
                    metadata,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s, %s::jsonb, NOW())
                ON CONFLICT (source_lang, target_lang, source_sentence_hash)
                DO UPDATE SET
                    source_sentence = EXCLUDED.source_sentence,
                    queue_status = CASE
                        WHEN bt_3_semantic_benchmark_queue.benchmark_id IS NOT NULL THEN 'ready'
                        ELSE EXCLUDED.queue_status
                    END,
                    priority = GREATEST(bt_3_semantic_benchmark_queue.priority, EXCLUDED.priority),
                    sample_count = GREATEST(bt_3_semantic_benchmark_queue.sample_count, EXCLUDED.sample_count),
                    first_seen_at = COALESCE(bt_3_semantic_benchmark_queue.first_seen_at, EXCLUDED.first_seen_at),
                    last_seen_at = GREATEST(
                        COALESCE(bt_3_semantic_benchmark_queue.last_seen_at, EXCLUDED.last_seen_at),
                        COALESCE(EXCLUDED.last_seen_at, bt_3_semantic_benchmark_queue.last_seen_at)
                    ),
                    recent_source_session_ids = EXCLUDED.recent_source_session_ids,
                    recent_check_session_ids = EXCLUDED.recent_check_session_ids,
                    benchmark_id = COALESCE(bt_3_semantic_benchmark_queue.benchmark_id, EXCLUDED.benchmark_id),
                    metadata = EXCLUDED.metadata,
                    updated_at = NOW()
                RETURNING
                    id,
                    source_lang,
                    target_lang,
                    source_sentence,
                    source_sentence_hash,
                    queue_status,
                    priority,
                    sample_count,
                    first_seen_at,
                    last_seen_at,
                    recent_source_session_ids,
                    recent_check_session_ids,
                    benchmark_id,
                    last_error,
                    metadata,
                    created_at,
                    updated_at;
                """,
                (
                    source_value,
                    target_value,
                    sentence_text,
                    sentence_hash,
                    queue_status,
                    float(priority or 0.0),
                    max(1, int(sample_count or 1)),
                    first_seen_at,
                    last_seen_at,
                    Json(source_session_ids),
                    Json(check_session_ids),
                    benchmark_id,
                    Json(metadata if isinstance(metadata, dict) else {}),
                ),
            )
            row = cursor.fetchone()
    if not row:
        return None
    return _map_semantic_benchmark_queue_row(row)


def _map_semantic_benchmark_queue_row(row: tuple | None) -> dict | None:
    if not row:
        return None
    return {
        "id": int(row[0]),
        "source_lang": str(row[1] or "ru"),
        "target_lang": str(row[2] or "de"),
        "source_sentence": str(row[3] or ""),
        "source_sentence_hash": str(row[4] or ""),
        "queue_status": str(row[5] or "pending"),
        "priority": float(row[6] or 0.0),
        "sample_count": int(row[7] or 0),
        "first_seen_at": row[8].isoformat() if row[8] else None,
        "last_seen_at": row[9].isoformat() if row[9] else None,
        "recent_source_session_ids": list(row[10] or []),
        "recent_check_session_ids": list(row[11] or []),
        "benchmark_id": int(row[12]) if row[12] is not None else None,
        "last_error": str(row[13] or "") or None,
        "metadata": row[14] if isinstance(row[14], dict) else {},
        "created_at": row[15].isoformat() if row[15] else None,
        "updated_at": row[16].isoformat() if row[16] else None,
    }


def list_semantic_benchmark_queue_candidates(
    *,
    queue_status: str = "pending",
    source_lang: str = "ru",
    target_lang: str = "de",
    limit: int = 25,
) -> list[dict]:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    id,
                    source_lang,
                    target_lang,
                    source_sentence,
                    source_sentence_hash,
                    queue_status,
                    priority,
                    sample_count,
                    first_seen_at,
                    last_seen_at,
                    recent_source_session_ids,
                    recent_check_session_ids,
                    benchmark_id,
                    last_error,
                    metadata,
                    created_at,
                    updated_at
                FROM bt_3_semantic_benchmark_queue
                WHERE source_lang = %s
                  AND target_lang = %s
                  AND queue_status = %s
                ORDER BY priority DESC, updated_at ASC, id ASC
                LIMIT %s;
                """,
                (
                    str(source_lang or "ru").strip().lower(),
                    str(target_lang or "de").strip().lower(),
                    str(queue_status or "pending").strip().lower(),
                    max(1, int(limit or 25)),
                ),
            )
            rows = cursor.fetchall() or []
    return [_map_semantic_benchmark_queue_row(row) for row in rows if row]


def update_semantic_benchmark_queue_item(
    *,
    queue_id: int,
    queue_status: str | None = None,
    benchmark_id: int | None = None,
    last_error: str | None = None,
    metadata: dict | None = None,
) -> dict | None:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE bt_3_semantic_benchmark_queue
                SET
                    queue_status = COALESCE(%s, queue_status),
                    benchmark_id = COALESCE(%s, benchmark_id),
                    last_error = %s,
                    metadata = CASE
                        WHEN %s::jsonb IS NULL THEN metadata
                        ELSE %s::jsonb
                    END,
                    updated_at = NOW()
                WHERE id = %s
                RETURNING
                    id,
                    source_lang,
                    target_lang,
                    source_sentence,
                    source_sentence_hash,
                    queue_status,
                    priority,
                    sample_count,
                    first_seen_at,
                    last_seen_at,
                    recent_source_session_ids,
                    recent_check_session_ids,
                    benchmark_id,
                    last_error,
                    metadata,
                    created_at,
                    updated_at;
                """,
                (
                    str(queue_status or "").strip().lower() or None,
                    int(benchmark_id) if benchmark_id is not None else None,
                    str(last_error or "").strip() or None,
                    Json(metadata) if isinstance(metadata, dict) else None,
                    Json(metadata) if isinstance(metadata, dict) else None,
                    int(queue_id),
                ),
            )
            return _map_semantic_benchmark_queue_row(cursor.fetchone())


def list_recent_unique_translation_check_sentences(
    *,
    period_start: datetime,
    period_end: datetime,
    source_lang: str = "ru",
    target_lang: str = "de",
    user_id: int | None = None,
    limit: int | None = None,
) -> list[dict]:
    params: list[Any] = [
        period_start,
        period_end,
        str(source_lang or "ru").strip().lower(),
        str(target_lang or "de").strip().lower(),
    ]
    user_filter_sql = ""
    if user_id is not None:
        user_filter_sql = " AND s.user_id = %s "
        params.append(int(user_id))
    limit_sql = ""
    if limit is not None:
        limit_sql = " LIMIT %s "
        params.append(max(1, int(limit)))
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT
                    i.original_text AS source_sentence,
                    MIN(COALESCE(i.created_at, s.created_at)) AS first_seen_at,
                    MAX(COALESCE(i.finished_at, i.updated_at, i.created_at, s.finished_at, s.created_at)) AS last_seen_at,
                    COUNT(*)::INT AS attempts_count,
                    ARRAY_REMOVE(ARRAY_AGG(DISTINCT s.source_session_id), NULL) AS source_session_ids,
                    ARRAY_REMOVE(ARRAY_AGG(DISTINCT s.id), NULL) AS check_session_ids
                FROM bt_3_translation_check_items i
                JOIN bt_3_translation_check_sessions s
                  ON s.id = i.check_session_id
                WHERE s.status = 'done'
                  AND s.created_at >= %s
                  AND s.created_at < %s
                  AND COALESCE(s.source_lang, 'ru') = %s
                  AND COALESCE(s.target_lang, 'de') = %s
                  {user_filter_sql}
                  AND COALESCE(NULLIF(BTRIM(i.original_text), ''), NULL) IS NOT NULL
                GROUP BY i.original_text
                ORDER BY attempts_count DESC, last_seen_at DESC, i.original_text ASC
                {limit_sql};
                """,
                params,
            )
            rows = cursor.fetchall() or []
    result: list[dict] = []
    for row in rows:
        source_sentence = " ".join(str(row[0] or "").strip().split())
        if not source_sentence:
            continue
        result.append(
            {
                "source_sentence": source_sentence,
                "source_lang": str(source_lang or "ru").strip().lower(),
                "target_lang": str(target_lang or "de").strip().lower(),
                "source_sentence_hash": _semantic_benchmark_sentence_hash(
                    source_sentence,
                    source_lang=source_lang,
                    target_lang=target_lang,
                ),
                "first_seen_at": row[1].isoformat() if row[1] else None,
                "last_seen_at": row[2].isoformat() if row[2] else None,
                "attempts_count": int(row[3] or 0),
                "source_session_ids": [str(item) for item in list(row[4] or []) if str(item or "").strip()],
                "check_session_ids": [int(item) for item in list(row[5] or []) if item is not None],
            }
        )
    return result


def create_semantic_audit_run(
    *,
    run_scope: str,
    source_lang: str = "ru",
    target_lang: str = "de",
    run_key: str | None = None,
    period_start: date | None = None,
    period_end: date | None = None,
    sample_size: int = 0,
    benchmark_case_count: int = 0,
    metadata: dict | None = None,
    delivery_chat_id: int | None = None,
) -> dict | None:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            normalized_run_key = str(run_key or "").strip() or None
            common_params = (
                str(run_scope or "").strip(),
                str(source_lang or "ru").strip().lower(),
                str(target_lang or "de").strip().lower(),
                period_start,
                period_end,
                max(0, int(sample_size or 0)),
                max(0, int(benchmark_case_count or 0)),
                int(delivery_chat_id) if delivery_chat_id is not None else None,
                Json(metadata if isinstance(metadata, dict) else {}),
            )
            if normalized_run_key:
                cursor.execute(
                    """
                    SELECT id
                    FROM bt_3_semantic_audit_runs
                    WHERE run_key = %s
                    LIMIT 1;
                    """,
                    (normalized_run_key,),
                )
                existing_row = cursor.fetchone()
                if existing_row:
                    cursor.execute(
                        """
                        UPDATE bt_3_semantic_audit_runs
                        SET
                            run_scope = %s,
                            source_lang = %s,
                            target_lang = %s,
                            period_start = %s,
                            period_end = %s,
                            sample_size = %s,
                            benchmark_case_count = %s,
                            run_status = 'running',
                            delivery_chat_id = %s,
                            delivery_status = 'pending',
                            metadata = %s::jsonb,
                            started_at = NOW(),
                            completed_at = NULL,
                            updated_at = NOW(),
                            last_error = NULL
                        WHERE id = %s
                        RETURNING
                            id,
                            run_key,
                            run_scope,
                            source_lang,
                            target_lang,
                            period_start,
                            period_end,
                            sample_size,
                            benchmark_case_count,
                            run_status,
                            delivery_chat_id,
                            delivery_status,
                            metrics_json,
                            summary_json,
                            summary_markdown,
                            last_error,
                            metadata,
                            started_at,
                            completed_at,
                            created_at,
                            updated_at;
                        """,
                        (*common_params, int(existing_row[0])),
                    )
                else:
                    cursor.execute(
                        """
                        INSERT INTO bt_3_semantic_audit_runs (
                            run_key,
                            run_scope,
                            source_lang,
                            target_lang,
                            period_start,
                            period_end,
                            sample_size,
                            benchmark_case_count,
                            run_status,
                            delivery_chat_id,
                            delivery_status,
                            metadata,
                            started_at,
                            updated_at
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'running', %s, 'pending', %s::jsonb, NOW(), NOW())
                        RETURNING
                            id,
                            run_key,
                            run_scope,
                            source_lang,
                            target_lang,
                            period_start,
                            period_end,
                            sample_size,
                            benchmark_case_count,
                            run_status,
                            delivery_chat_id,
                            delivery_status,
                            metrics_json,
                            summary_json,
                            summary_markdown,
                            last_error,
                            metadata,
                            started_at,
                            completed_at,
                            created_at,
                            updated_at;
                        """,
                        (normalized_run_key, *common_params),
                    )
            else:
                cursor.execute(
                    """
                    INSERT INTO bt_3_semantic_audit_runs (
                        run_key,
                        run_scope,
                        source_lang,
                        target_lang,
                        period_start,
                        period_end,
                        sample_size,
                        benchmark_case_count,
                        run_status,
                        delivery_chat_id,
                        delivery_status,
                        metadata,
                        started_at,
                        updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'running', %s, 'pending', %s::jsonb, NOW(), NOW())
                    RETURNING
                        id,
                        run_key,
                        run_scope,
                        source_lang,
                        target_lang,
                        period_start,
                        period_end,
                        sample_size,
                        benchmark_case_count,
                        run_status,
                        delivery_chat_id,
                        delivery_status,
                        metrics_json,
                        summary_json,
                        summary_markdown,
                        last_error,
                        metadata,
                        started_at,
                        completed_at,
                        created_at,
                        updated_at;
                    """,
                    (None, *common_params),
                )
            row = cursor.fetchone()
    if not row:
        return None
    return {
        "id": int(row[0]),
        "run_key": str(row[1] or "") or None,
        "run_scope": str(row[2] or ""),
        "source_lang": str(row[3] or "ru"),
        "target_lang": str(row[4] or "de"),
        "period_start": row[5].isoformat() if row[5] else None,
        "period_end": row[6].isoformat() if row[6] else None,
        "sample_size": int(row[7] or 0),
        "benchmark_case_count": int(row[8] or 0),
        "run_status": str(row[9] or "queued"),
        "delivery_chat_id": int(row[10]) if row[10] is not None else None,
        "delivery_status": str(row[11] or "pending"),
        "metrics_json": row[12] if isinstance(row[12], dict) else {},
        "summary_json": row[13] if isinstance(row[13], dict) else {},
        "summary_markdown": str(row[14] or "") or None,
        "last_error": str(row[15] or "") or None,
        "metadata": row[16] if isinstance(row[16], dict) else {},
        "started_at": row[17].isoformat() if row[17] else None,
        "completed_at": row[18].isoformat() if row[18] else None,
        "created_at": row[19].isoformat() if row[19] else None,
        "updated_at": row[20].isoformat() if row[20] else None,
    }


def finalize_semantic_audit_run(
    *,
    audit_run_id: int,
    run_status: str,
    metrics_json: dict | None = None,
    summary_json: dict | None = None,
    summary_markdown: str | None = None,
    delivery_status: str | None = None,
    last_error: str | None = None,
) -> None:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE bt_3_semantic_audit_runs
                SET
                    run_status = %s,
                    metrics_json = %s::jsonb,
                    summary_json = %s::jsonb,
                    summary_markdown = %s,
                    delivery_status = COALESCE(%s, delivery_status),
                    last_error = %s,
                    completed_at = CASE WHEN %s IN ('done', 'failed') THEN NOW() ELSE completed_at END,
                    updated_at = NOW()
                WHERE id = %s;
                """,
                (
                    str(run_status or "done").strip().lower(),
                    Json(metrics_json if isinstance(metrics_json, dict) else {}),
                    Json(summary_json if isinstance(summary_json, dict) else {}),
                    str(summary_markdown or "").strip() or None,
                    str(delivery_status or "").strip().lower() or None,
                    str(last_error or "").strip() or None,
                    str(run_status or "done").strip().lower(),
                    int(audit_run_id),
                ),
            )


def update_semantic_audit_run_delivery(
    *,
    audit_run_id: int,
    delivery_status: str,
    last_error: str | None = None,
) -> None:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE bt_3_semantic_audit_runs
                SET
                    delivery_status = %s,
                    last_error = CASE
                        WHEN %s IS NULL OR %s = '' THEN last_error
                        ELSE %s
                    END,
                    updated_at = NOW()
                WHERE id = %s;
                """,
                (
                    str(delivery_status or "pending").strip().lower(),
                    str(last_error or "").strip() or None,
                    str(last_error or "").strip() or None,
                    str(last_error or "").strip() or None,
                    int(audit_run_id),
                ),
            )


def list_recent_semantic_audit_runs(
    *,
    run_scope: str | None = None,
    source_lang: str = "ru",
    target_lang: str = "de",
    limit: int = 8,
) -> list[dict[str, Any]]:
    params: list[Any] = [
        str(source_lang or "ru").strip().lower(),
        str(target_lang or "de").strip().lower(),
    ]
    scope_sql = ""
    if str(run_scope or "").strip():
        scope_sql = " AND run_scope = %s "
        params.append(str(run_scope).strip().lower())
    params.append(max(1, int(limit or 8)))
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT
                    id,
                    run_key,
                    run_scope,
                    source_lang,
                    target_lang,
                    period_start,
                    period_end,
                    sample_size,
                    benchmark_case_count,
                    run_status,
                    delivery_chat_id,
                    delivery_status,
                    metrics_json,
                    summary_json,
                    summary_markdown,
                    last_error,
                    metadata,
                    started_at,
                    completed_at,
                    created_at,
                    updated_at
                FROM bt_3_semantic_audit_runs
                WHERE source_lang = %s
                  AND target_lang = %s
                  {scope_sql}
                ORDER BY created_at DESC, id DESC
                LIMIT %s;
                """,
                params,
            )
            rows = cursor.fetchall() or []
    result: list[dict[str, Any]] = []
    for row in rows:
        result.append(
            {
                "id": int(row[0]),
                "run_key": str(row[1] or "") or None,
                "run_scope": str(row[2] or ""),
                "source_lang": str(row[3] or "ru"),
                "target_lang": str(row[4] or "de"),
                "period_start": row[5].isoformat() if row[5] else None,
                "period_end": row[6].isoformat() if row[6] else None,
                "sample_size": int(row[7] or 0),
                "benchmark_case_count": int(row[8] or 0),
                "run_status": str(row[9] or ""),
                "delivery_chat_id": int(row[10]) if row[10] is not None else None,
                "delivery_status": str(row[11] or ""),
                "metrics_json": row[12] if isinstance(row[12], dict) else {},
                "summary_json": row[13] if isinstance(row[13], dict) else {},
                "summary_markdown": str(row[14] or "") or None,
                "last_error": str(row[15] or "") or None,
                "metadata": row[16] if isinstance(row[16], dict) else {},
                "started_at": row[17].isoformat() if row[17] else None,
                "completed_at": row[18].isoformat() if row[18] else None,
                "created_at": row[19].isoformat() if row[19] else None,
                "updated_at": row[20].isoformat() if row[20] else None,
            }
        )
    return result


def replace_semantic_audit_case_results(
    *,
    audit_run_id: int,
    case_results: list[dict[str, Any]],
    source_lang: str = "ru",
    target_lang: str = "de",
) -> int:
    normalized_rows: list[tuple[Any, ...]] = []
    for item in list(case_results or []):
        if not isinstance(item, dict):
            continue
        source_sentence = " ".join(str(item.get("source_sentence") or "").strip().split())
        if not source_sentence:
            continue
        source_session_id = str(item.get("source_session_id") or "").strip() or None
        sentence_id_value = item.get("sentence_id")
        check_session_id_value = item.get("check_session_id")
        try:
            sentence_id = int(sentence_id_value) if sentence_id_value is not None else None
        except Exception:
            sentence_id = None
        try:
            check_session_id = int(check_session_id_value) if check_session_id_value is not None else None
        except Exception:
            check_session_id = None
        normalized_rows.append(
            (
                int(audit_run_id),
                str(item.get("case_id") or "").strip() or None,
                str(source_lang or "ru").strip().lower(),
                str(target_lang or "de").strip().lower(),
                source_sentence,
                _semantic_benchmark_sentence_hash(
                    source_sentence,
                    source_lang=source_lang,
                    target_lang=target_lang,
                ),
                source_session_id,
                check_session_id,
                sentence_id,
                Json(item.get("benchmark_json") if isinstance(item.get("benchmark_json"), dict) else {}),
                str(item.get("expected_tested_primary") or "").strip() or None,
                Json(list(item.get("expected_tested_secondary") or [])),
                str(item.get("expected_outcome_type") or "").strip() or None,
                str(item.get("actual_tested_primary") or "").strip() or None,
                Json(list(item.get("actual_tested_secondary") or [])),
                Json(list(item.get("actual_errored_skills") or [])),
                str(item.get("actual_outcome_type") or "").strip() or None,
                bool(item.get("primary_match")) if item.get("primary_match") is not None else None,
                float(item.get("secondary_skill_overlap") or 0.0) if item.get("secondary_skill_overlap") is not None else None,
                bool(item.get("outcome_match")) if item.get("outcome_match") is not None else None,
                str(item.get("classification") or "").strip() or None,
                Json(item.get("metadata") if isinstance(item.get("metadata"), dict) else {}),
            )
        )
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                DELETE FROM bt_3_semantic_audit_case_results
                WHERE audit_run_id = %s;
                """,
                (int(audit_run_id),),
            )
            if not normalized_rows:
                return 0
            execute_values(
                cursor,
                """
                INSERT INTO bt_3_semantic_audit_case_results (
                    audit_run_id,
                    case_id,
                    source_lang,
                    target_lang,
                    source_sentence,
                    source_sentence_hash,
                    source_session_id,
                    check_session_id,
                    sentence_id,
                    benchmark_json,
                    expected_tested_primary,
                    expected_tested_secondary,
                    expected_outcome_type,
                    actual_tested_primary,
                    actual_tested_secondary,
                    actual_errored_skills,
                    actual_outcome_type,
                    primary_match,
                    secondary_skill_overlap,
                    outcome_match,
                    classification,
                    metadata
                )
                VALUES %s
                """,
                normalized_rows,
            )
    return len(normalized_rows)


def create_support_message(
    *,
    user_id: int,
    from_role: str,
    message_text: str,
    attachment_url: str | None = None,
    attachment_kind: str | None = None,
    attachment_mime_type: str | None = None,
    attachment_file_name: str | None = None,
    admin_telegram_id: int | None = None,
    telegram_chat_id: int | None = None,
    telegram_message_id: int | None = None,
    reply_to_id: int | None = None,
    is_read_by_user: bool | None = None,
) -> dict:
    normalized_role = str(from_role or "").strip().lower()
    if normalized_role not in {"user", "admin", "system"}:
        raise ValueError("from_role must be one of: user, admin, system")
    text = str(message_text or "").strip()
    normalized_attachment_url = str(attachment_url or "").strip() or None
    normalized_attachment_kind = str(attachment_kind or "").strip().lower() or None
    normalized_attachment_mime_type = str(attachment_mime_type or "").strip().lower() or None
    normalized_attachment_file_name = str(attachment_file_name or "").strip() or None
    if normalized_attachment_kind and normalized_attachment_kind not in {"image"}:
        raise ValueError("attachment_kind must be one of: image")
    if not text and not normalized_attachment_url:
        raise ValueError("message_text or attachment_url is required")
    if is_read_by_user is None:
        is_read_by_user = normalized_role != "admin"

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_support_messages (
                    user_id,
                    from_role,
                    message_text,
                    attachment_url,
                    attachment_kind,
                    attachment_mime_type,
                    attachment_file_name,
                    admin_telegram_id,
                    telegram_chat_id,
                    telegram_message_id,
                    reply_to_id,
                    is_read_by_user
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING
                    id, user_id, from_role, message_text,
                    attachment_url, attachment_kind, attachment_mime_type, attachment_file_name,
                    admin_telegram_id, telegram_chat_id, telegram_message_id, reply_to_id, is_read_by_user, created_at;
                """,
                (
                    int(user_id),
                    normalized_role,
                    text,
                    normalized_attachment_url,
                    normalized_attachment_kind,
                    normalized_attachment_mime_type,
                    normalized_attachment_file_name,
                    int(admin_telegram_id) if admin_telegram_id is not None else None,
                    int(telegram_chat_id) if telegram_chat_id is not None else None,
                    int(telegram_message_id) if telegram_message_id is not None else None,
                    int(reply_to_id) if reply_to_id is not None else None,
                    bool(is_read_by_user),
                ),
            )
            row = cursor.fetchone()
    return {
        "id": int(row[0]),
        "user_id": int(row[1]),
        "from_role": str(row[2] or ""),
        "message_text": str(row[3] or ""),
        "attachment_url": str(row[4] or "").strip() or None,
        "attachment_kind": str(row[5] or "").strip() or None,
        "attachment_mime_type": str(row[6] or "").strip() or None,
        "attachment_file_name": str(row[7] or "").strip() or None,
        "admin_telegram_id": int(row[8]) if row[8] is not None else None,
        "telegram_chat_id": int(row[9]) if row[9] is not None else None,
        "telegram_message_id": int(row[10]) if row[10] is not None else None,
        "reply_to_id": int(row[11]) if row[11] is not None else None,
        "is_read_by_user": bool(row[12]),
        "created_at": row[13].isoformat() if row[13] else None,
    }


def list_support_messages_for_user(*, user_id: int, limit: int = 100) -> list[dict]:
    safe_limit = max(1, min(int(limit or 100), 500))
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    id, user_id, from_role, message_text,
                    attachment_url, attachment_kind, attachment_mime_type, attachment_file_name,
                    admin_telegram_id, telegram_chat_id, telegram_message_id, reply_to_id, is_read_by_user, created_at
                FROM bt_3_support_messages
                WHERE user_id = %s
                ORDER BY created_at ASC
                LIMIT %s;
                """,
                (int(user_id), safe_limit),
            )
            rows = cursor.fetchall()
    return [
        {
            "id": int(row[0]),
            "user_id": int(row[1]),
            "from_role": str(row[2] or ""),
            "message_text": str(row[3] or ""),
            "attachment_url": str(row[4] or "").strip() or None,
            "attachment_kind": str(row[5] or "").strip() or None,
            "attachment_mime_type": str(row[6] or "").strip() or None,
            "attachment_file_name": str(row[7] or "").strip() or None,
            "admin_telegram_id": int(row[8]) if row[8] is not None else None,
            "telegram_chat_id": int(row[9]) if row[9] is not None else None,
            "telegram_message_id": int(row[10]) if row[10] is not None else None,
            "reply_to_id": int(row[11]) if row[11] is not None else None,
            "is_read_by_user": bool(row[12]),
            "created_at": row[13].isoformat() if row[13] else None,
        }
        for row in rows
    ]


def count_unread_support_messages_for_user(*, user_id: int) -> int:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM bt_3_support_messages
                WHERE user_id = %s
                  AND from_role = 'admin'
                  AND is_read_by_user = FALSE;
                """,
                (int(user_id),),
            )
            row = cursor.fetchone()
    return int(row[0] or 0) if row else 0


def mark_support_messages_read_for_user(*, user_id: int) -> int:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE bt_3_support_messages
                SET is_read_by_user = TRUE
                WHERE user_id = %s
                  AND from_role = 'admin'
                  AND is_read_by_user = FALSE;
                """,
                (int(user_id),),
            )
            affected = cursor.rowcount
    return int(affected or 0)


def get_support_message_by_telegram_ref(*, telegram_chat_id: int, telegram_message_id: int) -> dict | None:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    id, user_id, from_role, message_text,
                    attachment_url, attachment_kind, attachment_mime_type, attachment_file_name,
                    admin_telegram_id, telegram_chat_id, telegram_message_id, reply_to_id, is_read_by_user, created_at
                FROM bt_3_support_messages
                WHERE telegram_chat_id = %s
                  AND telegram_message_id = %s
                ORDER BY created_at DESC
                LIMIT 1;
                """,
                (int(telegram_chat_id), int(telegram_message_id)),
            )
            row = cursor.fetchone()
    if not row:
        return None
    return {
        "id": int(row[0]),
        "user_id": int(row[1]),
        "from_role": str(row[2] or ""),
        "message_text": str(row[3] or ""),
        "attachment_url": str(row[4] or "").strip() or None,
        "attachment_kind": str(row[5] or "").strip() or None,
        "attachment_mime_type": str(row[6] or "").strip() or None,
        "attachment_file_name": str(row[7] or "").strip() or None,
        "admin_telegram_id": int(row[8]) if row[8] is not None else None,
        "telegram_chat_id": int(row[9]) if row[9] is not None else None,
        "telegram_message_id": int(row[10]) if row[10] is not None else None,
        "reply_to_id": int(row[11]) if row[11] is not None else None,
        "is_read_by_user": bool(row[12]),
        "created_at": row[13].isoformat() if row[13] else None,
    }


def _map_daily_plan_item(row: tuple) -> dict:
    return {
        "id": int(row[0]),
        "plan_id": int(row[1]),
        "order_index": int(row[2] or 0),
        "task_type": row[3],
        "title": row[4],
        "estimated_minutes": int(row[5] or 0),
        "payload": row[6] if isinstance(row[6], dict) else {},
        "status": row[7] or "todo",
        "completed_at": row[8].isoformat() if row[8] else None,
    }


def _week_bounds(anchor_date: date | None = None) -> tuple[date, date]:
    current = anchor_date or date.today()
    week_start = current - timedelta(days=current.weekday())
    return week_start, week_start + timedelta(days=6)


def _period_bounds(period: str, anchor_date: date | None = None) -> tuple[date, date]:
    current = anchor_date or date.today()
    normalized = str(period or "week").strip().lower()
    if normalized == "week":
        return _week_bounds(current)
    if normalized == "month":
        start = current.replace(day=1)
        end = date(current.year, current.month, monthrange(current.year, current.month)[1])
        return start, end
    if normalized == "quarter":
        quarter_index = (current.month - 1) // 3
        start_month = quarter_index * 3 + 1
        end_month = start_month + 2
        start = date(current.year, start_month, 1)
        end = date(current.year, end_month, monthrange(current.year, end_month)[1])
        return start, end
    if normalized == "half-year":
        if current.month <= 6:
            return date(current.year, 1, 1), date(current.year, 6, 30)
        return date(current.year, 7, 1), date(current.year, 12, 31)
    if normalized == "year":
        return date(current.year, 1, 1), date(current.year, 12, 31)
    return _week_bounds(current)


def get_weekly_goals(
    *,
    user_id: int,
    source_lang: str,
    target_lang: str,
    week_start: date | None = None,
) -> dict | None:
    normalized_source = str(source_lang or "ru").strip().lower() or "ru"
    normalized_target = str(target_lang or "de").strip().lower() or "de"
    resolved_week_start, week_end = _week_bounds(week_start)
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT translations_goal, learned_words_goal, updated_at
                     , agent_minutes_goal, reading_minutes_goal
                FROM bt_3_weekly_goals
                WHERE user_id = %s
                  AND source_lang = %s
                  AND target_lang = %s
                  AND week_start = %s
                LIMIT 1;
                """,
                (int(user_id), normalized_source, normalized_target, resolved_week_start),
            )
            row = cursor.fetchone()
    if not row:
        return None
    return {
        "week_start": resolved_week_start.isoformat(),
        "week_end": week_end.isoformat(),
        "source_lang": normalized_source,
        "target_lang": normalized_target,
        "translations_goal": max(0, int(row[0] or 0)),
        "learned_words_goal": max(0, int(row[1] or 0)),
        "agent_minutes_goal": max(0, int(row[3] or 0)),
        "reading_minutes_goal": max(0, int(row[4] or 0)),
        "updated_at": row[2].isoformat() if row[2] else None,
    }


def upsert_weekly_goals(
    *,
    user_id: int,
    source_lang: str,
    target_lang: str,
    translations_goal: int,
    learned_words_goal: int,
    agent_minutes_goal: int,
    reading_minutes_goal: int,
    week_start: date | None = None,
) -> dict:
    normalized_source = str(source_lang or "ru").strip().lower() or "ru"
    normalized_target = str(target_lang or "de").strip().lower() or "de"
    resolved_week_start, week_end = _week_bounds(week_start)
    translations_goal = max(0, int(translations_goal or 0))
    learned_words_goal = max(0, int(learned_words_goal or 0))
    agent_minutes_goal = max(0, int(agent_minutes_goal or 0))
    reading_minutes_goal = max(0, int(reading_minutes_goal or 0))

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_weekly_goals (
                    user_id,
                    source_lang,
                    target_lang,
                    week_start,
                    translations_goal,
                    learned_words_goal,
                    agent_minutes_goal,
                    reading_minutes_goal,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (user_id, source_lang, target_lang, week_start) DO UPDATE
                SET
                    translations_goal = EXCLUDED.translations_goal,
                    learned_words_goal = EXCLUDED.learned_words_goal,
                    agent_minutes_goal = EXCLUDED.agent_minutes_goal,
                    reading_minutes_goal = EXCLUDED.reading_minutes_goal,
                    updated_at = NOW()
                RETURNING translations_goal, learned_words_goal, agent_minutes_goal, reading_minutes_goal, updated_at;
                """,
                (
                    int(user_id),
                    normalized_source,
                    normalized_target,
                    resolved_week_start,
                    translations_goal,
                    learned_words_goal,
                    agent_minutes_goal,
                    reading_minutes_goal,
                ),
            )
            saved = cursor.fetchone()

    return {
        "week_start": resolved_week_start.isoformat(),
        "week_end": week_end.isoformat(),
        "source_lang": normalized_source,
        "target_lang": normalized_target,
        "translations_goal": max(0, int(saved[0] if saved else translations_goal)),
        "learned_words_goal": max(0, int(saved[1] if saved else learned_words_goal)),
        "agent_minutes_goal": max(0, int(saved[2] if saved else agent_minutes_goal)),
        "reading_minutes_goal": max(0, int(saved[3] if saved else reading_minutes_goal)),
        "updated_at": saved[4].isoformat() if saved and saved[4] else None,
    }


def get_plan_progress(
    *,
    user_id: int,
    source_lang: str,
    target_lang: str,
    mature_interval_days: int = 21,
    period: str = "week",
    week_start: date | None = None,
    as_of_date: date | None = None,
) -> dict:
    normalized_source = str(source_lang or "ru").strip().lower() or "ru"
    normalized_target = str(target_lang or "de").strip().lower() or "de"
    normalized_period = str(period or "week").strip().lower()
    if normalized_period == "week":
        resolved_start, resolved_end = _week_bounds(week_start or as_of_date)
    else:
        resolved_start, resolved_end = _period_bounds(normalized_period, as_of_date)
    effective_start = _resolve_progress_window_start(
        int(user_id),
        source_lang=normalized_source,
        target_lang=normalized_target,
        start_date=resolved_start,
    )
    has_overlap = effective_start <= resolved_end
    report_start = effective_start if has_overlap else resolved_end
    today = as_of_date or date.today()
    effective_end = min(resolved_end, max(report_start, today))
    days_total = max(1, (resolved_end - report_start).days + 1)
    days_elapsed = max(1, (effective_end - report_start).days + 1)
    mature_threshold = max(1, int(mature_interval_days or 21))

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT COALESCE(SUM(i.estimated_minutes), 0)
                FROM bt_3_daily_plans p
                JOIN bt_3_daily_plan_items i ON i.plan_id = p.id
                WHERE p.user_id = %s
                  AND p.plan_date BETWEEN %s AND %s
                  AND LOWER(COALESCE(i.task_type, '')) IN ('video', 'youtube');
                """,
                (
                    int(user_id),
                    effective_start,
                    resolved_end,
                ),
            )
            video_goal_row = cursor.fetchone()
            youtube_minutes_goal = max(0, int(video_goal_row[0] or 0)) if video_goal_row else 0

            cursor.execute(
                """
                SELECT COUNT(*)
                FROM bt_3_translations t
                JOIN bt_3_daily_sentences ds ON ds.id = t.sentence_id
                WHERE t.user_id = %s
                  AND COALESCE(t.source_lang, 'ru') = %s
                  AND COALESCE(t.target_lang, 'de') = %s
                  AND ds.date BETWEEN %s AND %s;
                """,
                (
                    int(user_id),
                    normalized_source,
                    normalized_target,
                    effective_start,
                    effective_end,
                ),
            )
            translation_row = cursor.fetchone()
            translations_actual = max(0, int(translation_row[0] or 0)) if translation_row else 0

            language_filter_sql, language_params = _build_language_pair_filter(
                normalized_source,
                normalized_target,
                table_alias="q",
            )
            cursor.execute(
                f"""
                WITH first_learned AS (
                    SELECT
                        l.card_id,
                        MIN(l.reviewed_at) AS learned_at
                    FROM bt_3_card_review_log l
                    JOIN bt_3_webapp_dictionary_queries q
                      ON q.id = l.card_id
                     AND q.user_id = l.user_id
                    WHERE l.user_id = %s
                      AND COALESCE(l.interval_days_after, 0) >= %s
                      {language_filter_sql}
                    GROUP BY l.card_id
                )
                SELECT COUNT(*)
                FROM first_learned
                WHERE (learned_at AT TIME ZONE 'UTC')::date BETWEEN %s AND %s;
                """,
                [
                    int(user_id),
                    mature_threshold,
                    *language_params,
                    effective_start,
                    effective_end,
                ],
            )
            learned_row = cursor.fetchone()
            learned_words_actual = max(0, int(learned_row[0] or 0)) if learned_row else 0

            period_start_dt = datetime.combine(effective_start, dt_time.min, tzinfo=timezone.utc)
            end_exclusive_dt = datetime.combine(effective_end + timedelta(days=1), dt_time.min, tzinfo=timezone.utc)
            now_utc = datetime.now(timezone.utc)
            cap_dt = min(end_exclusive_dt, now_utc)
            if cap_dt > period_start_dt:
                cursor.execute(
                    """
                    SELECT COALESCE(
                        SUM(
                            GREATEST(
                                0,
                                EXTRACT(
                                    EPOCH FROM (
                                        LEAST(
                                            COALESCE(
                                                ended_at,
                                                CASE
                                                    WHEN duration_seconds IS NOT NULL
                                                        THEN started_at + (GREATEST(duration_seconds, 0) * INTERVAL '1 second')
                                                    ELSE started_at
                                                END
                                            ),
                                            %s
                                        )
                                        - GREATEST(started_at, %s)
                                    )
                                )
                            )
                        ),
                        0
                    )
                    FROM bt_3_agent_voice_sessions
                    WHERE user_id = %s
                      AND source_lang = %s
                      AND target_lang = %s
                      AND started_at < %s
                      AND COALESCE(
                          ended_at,
                          CASE
                              WHEN duration_seconds IS NOT NULL
                                  THEN started_at + (GREATEST(duration_seconds, 0) * INTERVAL '1 second')
                              ELSE started_at
                          END
                      ) > %s;
                    """,
                    (
                        cap_dt,
                        period_start_dt,
                        int(user_id),
                        normalized_source,
                        normalized_target,
                        cap_dt,
                        period_start_dt,
                    ),
                )
                agent_row = cursor.fetchone()
                agent_minutes_actual = float(agent_row[0] or 0.0) / 60.0 if agent_row else 0.0
            else:
                agent_minutes_actual = 0.0

            if cap_dt > period_start_dt:
                cursor.execute(
                    """
                    SELECT COALESCE(
                        SUM(
                            GREATEST(
                                0,
                                EXTRACT(
                                    EPOCH FROM (
                                        LEAST(
                                            CASE
                                                WHEN duration_seconds IS NOT NULL
                                                    THEN started_at + (LEAST(GREATEST(duration_seconds, 0), %s) * INTERVAL '1 second')
                                                WHEN ended_at IS NOT NULL
                                                    THEN LEAST(
                                                        ended_at,
                                                        started_at + (%s * INTERVAL '1 second')
                                                    )
                                                ELSE started_at
                                            END,
                                            %s
                                        )
                                        - GREATEST(started_at, %s)
                                    )
                                )
                            )
                        ),
                        0
                    )
                    FROM bt_3_reader_sessions
                    WHERE user_id = %s
                      AND source_lang = %s
                      AND target_lang = %s
                      AND started_at < %s
                      AND CASE
                          WHEN duration_seconds IS NOT NULL
                              THEN started_at + (LEAST(GREATEST(duration_seconds, 0), %s) * INTERVAL '1 second')
                          WHEN ended_at IS NOT NULL
                              THEN LEAST(
                                  ended_at,
                                  started_at + (%s * INTERVAL '1 second')
                              )
                          ELSE started_at
                      END > %s;
                    """,
                    (
                        int(READER_SESSION_AUTOCLOSE_MAX_SECONDS),
                        int(READER_SESSION_AUTOCLOSE_MAX_SECONDS),
                        cap_dt,
                        period_start_dt,
                        int(user_id),
                        normalized_source,
                        normalized_target,
                        cap_dt,
                        int(READER_SESSION_AUTOCLOSE_MAX_SECONDS),
                        int(READER_SESSION_AUTOCLOSE_MAX_SECONDS),
                        period_start_dt,
                    ),
                )
                reading_row = cursor.fetchone()
                reading_minutes_actual = float(reading_row[0] or 0.0) / 60.0 if reading_row else 0.0
            else:
                reading_minutes_actual = 0.0

            cursor.execute(
                """
                SELECT COALESCE(
                    SUM(
                        GREATEST(
                            0,
                            CASE
                                WHEN COALESCE(NULLIF(i.payload ->> 'timer_seconds', ''), '') <> ''
                                    THEN (i.payload ->> 'timer_seconds')::numeric / 60.0
                                WHEN LOWER(COALESCE(i.status, '')) = 'done'
                                    THEN COALESCE(
                                        NULLIF(i.payload ->> 'duration_sec', '')::numeric / 60.0,
                                        NULLIF(i.estimated_minutes, 0)::numeric,
                                        0
                                    )
                                ELSE 0
                            END
                        )
                    ),
                    0
                )
                FROM bt_3_daily_plans p
                JOIN bt_3_daily_plan_items i ON i.plan_id = p.id
                WHERE p.user_id = %s
                  AND p.plan_date BETWEEN %s AND %s
                  AND LOWER(COALESCE(i.task_type, '')) IN ('video', 'youtube');
                """,
                (
                    int(user_id),
                    effective_start,
                    effective_end,
                ),
            )
            youtube_row = cursor.fetchone()
            youtube_minutes_actual = float(youtube_row[0] or 0.0) if youtube_row else 0.0

    prorated_goals = _list_prorated_weekly_goals(
        user_id=int(user_id),
        source_lang=normalized_source,
        target_lang=normalized_target,
        start_date=effective_start,
        end_date=resolved_end,
    )
    translations_goal = int(prorated_goals.get("translations_goal") or 0)
    learned_words_goal = int(prorated_goals.get("learned_words_goal") or 0)
    agent_minutes_goal = int(prorated_goals.get("agent_minutes_goal") or 0)
    reading_minutes_goal = int(prorated_goals.get("reading_minutes_goal") or 0)

    def _metric(goal: int, actual: float) -> dict:
        return _build_plan_metric(
            goal,
            actual,
            days_elapsed=days_elapsed,
            days_total=days_total,
        )

    return {
        "period": normalized_period,
        "start_date": report_start.isoformat(),
        "end_date": resolved_end.isoformat(),
        "as_of_date": effective_end.isoformat(),
        "days_elapsed": days_elapsed,
        "days_total": days_total,
        "source_lang": normalized_source,
        "target_lang": normalized_target,
        "metrics": {
            "translations": _metric(translations_goal, translations_actual),
            "learned_words": _metric(learned_words_goal, learned_words_actual),
            "agent_minutes": _metric(agent_minutes_goal, agent_minutes_actual),
            "reading_minutes": _metric(reading_minutes_goal, reading_minutes_actual),
            "youtube_minutes": _metric(youtube_minutes_goal, youtube_minutes_actual),
        },
    }


def _build_plan_metric(
    goal: int,
    actual: float,
    *,
    days_elapsed: int,
    days_total: int,
) -> dict:
    safe_goal = max(0, int(goal or 0))
    safe_actual = max(0.0, float(actual or 0.0))
    safe_days_elapsed = max(1, int(days_elapsed or 1))
    safe_days_total = max(1, int(days_total or 1))
    raw_forecast = (safe_actual / float(safe_days_elapsed)) * float(safe_days_total)
    # Once the user has already cleared the goal, showing a pace-based extrapolation
    # creates misleading spikes (for example, one long reading session on Monday).
    forecast = safe_actual if safe_goal > 0 and safe_actual >= float(safe_goal) else raw_forecast
    expected_to_date = (safe_goal / float(safe_days_total)) * float(safe_days_elapsed)
    completion = (safe_actual / float(safe_goal) * 100.0) if safe_goal > 0 else 0.0
    return {
        "goal": safe_goal,
        "actual": round(safe_actual, 2),
        "forecast": round(forecast, 2),
        "completion_percent": round(completion, 1),
        "delta_vs_goal": round(safe_actual - safe_goal, 2),
        "forecast_delta_vs_goal": round(forecast - safe_goal, 2),
        "expected_to_date": round(expected_to_date, 2),
        "delta_vs_expected": round(safe_actual - expected_to_date, 2),
    }


def get_weekly_plan_progress(
    *,
    user_id: int,
    source_lang: str,
    target_lang: str,
    mature_interval_days: int = 21,
    week_start: date | None = None,
    as_of_date: date | None = None,
) -> dict:
    result = get_plan_progress(
        user_id=user_id,
        source_lang=source_lang,
        target_lang=target_lang,
        mature_interval_days=mature_interval_days,
        period="week",
        week_start=week_start,
        as_of_date=as_of_date,
    )
    return {
        **result,
        "week_start": result.get("start_date"),
        "week_end": result.get("end_date"),
    }


def start_agent_voice_session(
    *,
    user_id: int,
    source_lang: str = "ru",
    target_lang: str = "de",
    scenario_id: int | None = None,
    prep_pack_id: int | None = None,
    topic_mode: str | None = None,
    custom_topic_text: str | None = None,
    started_at: datetime | None = None,
) -> dict:
    normalized_source = str(source_lang or "ru").strip().lower() or "ru"
    normalized_target = str(target_lang or "de").strip().lower() or "de"
    normalized_topic_mode = str(topic_mode or "").strip().lower() or None
    normalized_custom_topic_text = str(custom_topic_text or "").strip() or None
    started = started_at or datetime.now(timezone.utc)
    if started.tzinfo is None:
        started = started.replace(tzinfo=timezone.utc)
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE bt_3_agent_voice_sessions
                SET
                    ended_at = %s,
                    duration_seconds = GREATEST(0, EXTRACT(EPOCH FROM (%s - started_at))::INT),
                    updated_at = NOW()
                WHERE user_id = %s
                  AND source_lang = %s
                  AND target_lang = %s
                  AND ended_at IS NULL;
                """,
                (started, started, int(user_id), normalized_source, normalized_target),
            )
            cursor.execute(
                """
                INSERT INTO bt_3_agent_voice_sessions (
                    user_id,
                    source_lang,
                    target_lang,
                    scenario_id,
                    prep_pack_id,
                    topic_mode,
                    custom_topic_text,
                    started_at,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                RETURNING id, started_at;
                """,
                (
                    int(user_id),
                    normalized_source,
                    normalized_target,
                    int(scenario_id) if scenario_id is not None else None,
                    int(prep_pack_id) if prep_pack_id is not None else None,
                    normalized_topic_mode,
                    normalized_custom_topic_text,
                    started,
                ),
            )
            row = cursor.fetchone()
    return {
        "session_id": int(row[0]),
        "started_at": row[1].isoformat() if row and row[1] else started.isoformat(),
        "scenario_id": int(scenario_id) if scenario_id is not None else None,
        "prep_pack_id": int(prep_pack_id) if prep_pack_id is not None else None,
        "topic_mode": normalized_topic_mode,
        "custom_topic_text": normalized_custom_topic_text,
    }


def _agent_voice_session_row_to_dict(row: tuple | None) -> dict | None:
    if not row:
        return None
    return {
        "session_id": int(row[0]),
        "user_id": int(row[1]),
        "source_lang": str(row[2] or "ru"),
        "target_lang": str(row[3] or "de"),
        "scenario_id": int(row[4]) if row[4] is not None else None,
        "prep_pack_id": int(row[5]) if row[5] is not None else None,
        "topic_mode": str(row[6] or "").strip().lower() or None,
        "custom_topic_text": str(row[7] or "").strip() or None,
        "started_at": row[8].isoformat() if row[8] else None,
        "ended_at": row[9].isoformat() if row[9] else None,
        "duration_seconds": int(row[10] or 0) if row[10] is not None else None,
        "created_at": row[11].isoformat() if row[11] else None,
        "updated_at": row[12].isoformat() if row[12] else None,
    }


def _agent_voice_transcript_segment_row_to_dict(row: tuple | None) -> dict | None:
    if not row:
        return None
    return {
        "id": int(row[0]),
        "session_id": int(row[1]),
        "seq_no": int(row[2]),
        "speaker": str(row[3] or "").strip().lower() or "unknown",
        "text": str(row[4] or ""),
        "metadata": dict(row[5] or {}) if isinstance(row[5], dict) else None,
        "created_at": row[6].isoformat() if row[6] else None,
    }


def _voice_scenario_row_to_dict(row: tuple | None) -> dict | None:
    if not row:
        return None
    return {
        "scenario_id": int(row[0]),
        "slug": str(row[1] or "").strip(),
        "title": str(row[2] or "").strip(),
        "topic": str(row[3] or "").strip(),
        "level": str(row[4] or "mixed").strip() or "mixed",
        "system_prompt": str(row[5] or "").strip() or None,
        "is_active": bool(row[6]),
        "created_at": row[7].isoformat() if row[7] else None,
        "updated_at": row[8].isoformat() if row[8] else None,
    }


def _voice_prep_pack_row_to_dict(row: tuple | None) -> dict | None:
    if not row:
        return None
    raw_vocab = row[4] if isinstance(row[4], list) else []
    raw_expressions = row[5] if isinstance(row[5], list) else []
    return {
        "prep_pack_id": int(row[0]),
        "user_id": int(row[1]),
        "scenario_id": int(row[2]) if row[2] is not None else None,
        "custom_topic_text": str(row[3] or "").strip() or None,
        "target_vocab": [str(item).strip() for item in raw_vocab if str(item).strip()],
        "target_expressions": [str(item).strip() for item in raw_expressions if str(item).strip()],
        "created_at": row[6].isoformat() if row[6] else None,
        "updated_at": row[7].isoformat() if row[7] else None,
    }


def _voice_session_assessment_row_to_dict(row: tuple | None) -> dict | None:
    if not row:
        return None
    raw_used = row[9] if isinstance(row[9], list) else []
    raw_missed = row[10] if isinstance(row[10], list) else []
    raw_bridge_notes = row[15] if isinstance(row[15], dict) else {}
    return {
        "assessment_id": int(row[0]),
        "session_id": int(row[1]),
        "summary": str(row[2] or "").strip() or None,
        "strict_feedback": str(row[3] or "").strip() or None,
        "lexical_range_note": str(row[4] or "").strip() or None,
        "grammar_control_note": str(row[5] or "").strip() or None,
        "fluency_note": str(row[6] or "").strip() or None,
        "coherence_relevance_note": str(row[7] or "").strip() or None,
        "self_correction_note": str(row[8] or "").strip() or None,
        "target_vocab_used": [str(item).strip() for item in raw_used if str(item).strip()],
        "target_vocab_missed": [str(item).strip() for item in raw_missed if str(item).strip()],
        "recommended_next_focus": str(row[11] or "").strip() or None,
        "created_at": row[12].isoformat() if row[12] else None,
        "updated_at": row[13].isoformat() if row[13] else None,
        "skill_bridge_status": str(row[14] or "").strip().lower() or "pending",
        "skill_bridge_notes": raw_bridge_notes if isinstance(raw_bridge_notes, dict) else {},
        "skill_bridge_updated_at": row[16].isoformat() if row[16] else None,
    }


def create_voice_scenario(
    *,
    slug: str,
    title: str,
    topic: str,
    level: str | None = None,
    system_prompt: str | None = None,
    is_active: bool = True,
) -> dict | None:
    normalized_slug = str(slug or "").strip()
    normalized_title = str(title or "").strip()
    normalized_topic = str(topic or "").strip()
    if not normalized_slug or not normalized_title or not normalized_topic:
        return None
    normalized_level = str(level or "mixed").strip().lower() or "mixed"
    normalized_prompt = str(system_prompt or "").strip() or None
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_voice_scenarios (
                    slug,
                    title,
                    topic,
                    level,
                    system_prompt,
                    is_active,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, NOW())
                RETURNING id, slug, title, topic, level, system_prompt, is_active, created_at, updated_at;
                """,
                (
                    normalized_slug,
                    normalized_title,
                    normalized_topic,
                    normalized_level,
                    normalized_prompt,
                    bool(is_active),
                ),
            )
            return _voice_scenario_row_to_dict(cursor.fetchone())


def get_voice_scenario(scenario_id: int) -> dict | None:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    id,
                    slug,
                    title,
                    topic,
                    level,
                    system_prompt,
                    is_active,
                    created_at,
                    updated_at
                FROM bt_3_voice_scenarios
                WHERE id = %s
                LIMIT 1;
                """,
                (int(scenario_id),),
            )
            return _voice_scenario_row_to_dict(cursor.fetchone())


def create_voice_prep_pack(
    *,
    user_id: int,
    scenario_id: int | None = None,
    custom_topic_text: str | None = None,
    target_vocab: list[str] | None = None,
    target_expressions: list[str] | None = None,
) -> dict | None:
    normalized_custom_topic_text = str(custom_topic_text or "").strip() or None
    normalized_vocab = [
        str(item).strip()
        for item in (target_vocab or [])
        if str(item).strip()
    ]
    normalized_expressions = [
        str(item).strip()
        for item in (target_expressions or [])
        if str(item).strip()
    ]
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_voice_prep_packs (
                    user_id,
                    scenario_id,
                    custom_topic_text,
                    target_vocab,
                    target_expressions,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, NOW())
                RETURNING
                    id,
                    user_id,
                    scenario_id,
                    custom_topic_text,
                    target_vocab,
                    target_expressions,
                    created_at,
                    updated_at;
                """,
                (
                    int(user_id),
                    int(scenario_id) if scenario_id is not None else None,
                    normalized_custom_topic_text,
                    Json(normalized_vocab),
                    Json(normalized_expressions),
                ),
            )
            return _voice_prep_pack_row_to_dict(cursor.fetchone())


def get_voice_prep_pack(prep_pack_id: int) -> dict | None:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    id,
                    user_id,
                    scenario_id,
                    custom_topic_text,
                    target_vocab,
                    target_expressions,
                    created_at,
                    updated_at
                FROM bt_3_voice_prep_packs
                WHERE id = %s
                LIMIT 1;
                """,
                (int(prep_pack_id),),
            )
            return _voice_prep_pack_row_to_dict(cursor.fetchone())


def get_agent_voice_session(session_id: int) -> dict | None:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    id,
                    user_id,
                    source_lang,
                    target_lang,
                    scenario_id,
                    prep_pack_id,
                    topic_mode,
                    custom_topic_text,
                    started_at,
                    ended_at,
                    duration_seconds,
                    created_at,
                    updated_at
                FROM bt_3_agent_voice_sessions
                WHERE id = %s
                LIMIT 1;
                """,
                (int(session_id),),
            )
            return _agent_voice_session_row_to_dict(cursor.fetchone())


def get_latest_active_agent_voice_session(
    *,
    user_id: int,
) -> dict | None:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    id,
                    user_id,
                    source_lang,
                    target_lang,
                    scenario_id,
                    prep_pack_id,
                    topic_mode,
                    custom_topic_text,
                    started_at,
                    ended_at,
                    duration_seconds,
                    created_at,
                    updated_at
                FROM bt_3_agent_voice_sessions
                WHERE user_id = %s
                  AND ended_at IS NULL
                ORDER BY started_at DESC, id DESC
                LIMIT 1;
                """,
                (int(user_id),),
            )
            return _agent_voice_session_row_to_dict(cursor.fetchone())


def get_agent_voice_session_context(session_id: int) -> dict | None:
    session = get_agent_voice_session(int(session_id))
    if not session:
        return None
    scenario = None
    if session.get("scenario_id") is not None:
        scenario = get_voice_scenario(int(session["scenario_id"]))
    prep_pack = None
    if session.get("prep_pack_id") is not None:
        prep_pack = get_voice_prep_pack(int(session["prep_pack_id"]))
    return {
        "session": session,
        "scenario": scenario,
        "prep_pack": prep_pack,
    }


def append_agent_voice_transcript_segment(
    *,
    session_id: int,
    speaker: str,
    text: str,
    metadata: dict | None = None,
) -> dict | None:
    normalized_text = str(text or "").strip()
    if not normalized_text:
        return None
    normalized_speaker = str(speaker or "").strip().lower() or "unknown"
    safe_metadata = dict(metadata) if isinstance(metadata, dict) else None
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                WITH locked_session AS (
                    SELECT id
                    FROM bt_3_agent_voice_sessions
                    WHERE id = %s
                    FOR UPDATE
                ),
                next_seq AS (
                    SELECT COALESCE(MAX(seq_no), 0) + 1 AS seq_no
                    FROM bt_3_agent_voice_transcript_segments
                    WHERE session_id = %s
                )
                INSERT INTO bt_3_agent_voice_transcript_segments (
                    session_id,
                    seq_no,
                    speaker,
                    text,
                    metadata
                )
                SELECT
                    %s,
                    next_seq.seq_no,
                    %s,
                    %s,
                    %s
                FROM locked_session, next_seq
                RETURNING id, session_id, seq_no, speaker, text, metadata, created_at;
                """,
                (
                    int(session_id),
                    int(session_id),
                    int(session_id),
                    normalized_speaker,
                    normalized_text,
                    Json(safe_metadata) if safe_metadata is not None else None,
                ),
            )
            return _agent_voice_transcript_segment_row_to_dict(cursor.fetchone())


def fetch_agent_voice_transcript_segments(
    *,
    session_id: int,
) -> list[dict]:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    id,
                    session_id,
                    seq_no,
                    speaker,
                    text,
                    metadata,
                    created_at
                FROM bt_3_agent_voice_transcript_segments
                WHERE session_id = %s
                ORDER BY seq_no ASC, id ASC;
                """,
                (int(session_id),),
            )
            return [
                item
                for item in (
                    _agent_voice_transcript_segment_row_to_dict(row)
                    for row in (cursor.fetchall() or [])
                )
                if item
            ]


def upsert_voice_session_assessment(
    *,
    session_id: int,
    summary: str | None = None,
    strict_feedback: str | None = None,
    lexical_range_note: str | None = None,
    grammar_control_note: str | None = None,
    fluency_note: str | None = None,
    coherence_relevance_note: str | None = None,
    self_correction_note: str | None = None,
    target_vocab_used: list[str] | None = None,
    target_vocab_missed: list[str] | None = None,
    recommended_next_focus: str | None = None,
) -> dict | None:
    normalized_used = [
        str(item).strip()
        for item in (target_vocab_used or [])
        if str(item).strip()
    ]
    normalized_missed = [
        str(item).strip()
        for item in (target_vocab_missed or [])
        if str(item).strip()
    ]
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_voice_session_assessments (
                    session_id,
                    summary,
                    strict_feedback,
                    lexical_range_note,
                    grammar_control_note,
                    fluency_note,
                    coherence_relevance_note,
                    self_correction_note,
                    target_vocab_used,
                    target_vocab_missed,
                    recommended_next_focus,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (session_id) DO UPDATE
                SET
                    summary = EXCLUDED.summary,
                    strict_feedback = EXCLUDED.strict_feedback,
                    lexical_range_note = EXCLUDED.lexical_range_note,
                    grammar_control_note = EXCLUDED.grammar_control_note,
                    fluency_note = EXCLUDED.fluency_note,
                    coherence_relevance_note = EXCLUDED.coherence_relevance_note,
                    self_correction_note = EXCLUDED.self_correction_note,
                    target_vocab_used = EXCLUDED.target_vocab_used,
                    target_vocab_missed = EXCLUDED.target_vocab_missed,
                    recommended_next_focus = EXCLUDED.recommended_next_focus,
                    updated_at = NOW()
                RETURNING
                    id,
                    session_id,
                    summary,
                    strict_feedback,
                    lexical_range_note,
                    grammar_control_note,
                    fluency_note,
                    coherence_relevance_note,
                    self_correction_note,
                    target_vocab_used,
                    target_vocab_missed,
                    recommended_next_focus,
                    created_at,
                    updated_at,
                    skill_bridge_status,
                    skill_bridge_notes,
                    skill_bridge_updated_at;
                """,
                (
                    int(session_id),
                    str(summary or "").strip() or None,
                    str(strict_feedback or "").strip() or None,
                    str(lexical_range_note or "").strip() or None,
                    str(grammar_control_note or "").strip() or None,
                    str(fluency_note or "").strip() or None,
                    str(coherence_relevance_note or "").strip() or None,
                    str(self_correction_note or "").strip() or None,
                    Json(normalized_used),
                    Json(normalized_missed),
                    str(recommended_next_focus or "").strip() or None,
                ),
            )
            return _voice_session_assessment_row_to_dict(cursor.fetchone())


def get_voice_session_assessment(session_id: int) -> dict | None:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    id,
                    session_id,
                    summary,
                    strict_feedback,
                    lexical_range_note,
                    grammar_control_note,
                    fluency_note,
                    coherence_relevance_note,
                    self_correction_note,
                    target_vocab_used,
                    target_vocab_missed,
                    recommended_next_focus,
                    created_at,
                    updated_at,
                    skill_bridge_status,
                    skill_bridge_notes,
                    skill_bridge_updated_at
                FROM bt_3_voice_session_assessments
                WHERE session_id = %s
                LIMIT 1;
                """,
                (int(session_id),),
            )
            return _voice_session_assessment_row_to_dict(cursor.fetchone())


def claim_voice_session_assessment_for_skill_bridge(session_id: int) -> dict | None:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE bt_3_voice_session_assessments
                SET
                    skill_bridge_status = 'in_progress',
                    skill_bridge_updated_at = NOW()
                WHERE session_id = %s
                  AND COALESCE(skill_bridge_status, 'pending') IN ('pending', 'failed')
                RETURNING
                    id,
                    session_id,
                    summary,
                    strict_feedback,
                    lexical_range_note,
                    grammar_control_note,
                    fluency_note,
                    coherence_relevance_note,
                    self_correction_note,
                    target_vocab_used,
                    target_vocab_missed,
                    recommended_next_focus,
                    created_at,
                    updated_at,
                    skill_bridge_status,
                    skill_bridge_notes,
                    skill_bridge_updated_at;
                """,
                (int(session_id),),
            )
            return _voice_session_assessment_row_to_dict(cursor.fetchone())


def set_voice_session_assessment_skill_bridge_status(
    session_id: int,
    *,
    status: str,
    notes: dict | None = None,
) -> dict | None:
    normalized_status = str(status or "").strip().lower()
    if normalized_status not in {"pending", "in_progress", "applied", "skipped", "failed"}:
        raise ValueError("invalid skill bridge status")
    safe_notes = dict(notes or {})
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE bt_3_voice_session_assessments
                SET
                    skill_bridge_status = %s,
                    skill_bridge_notes = %s,
                    skill_bridge_updated_at = NOW()
                WHERE session_id = %s
                RETURNING
                    id,
                    session_id,
                    summary,
                    strict_feedback,
                    lexical_range_note,
                    grammar_control_note,
                    fluency_note,
                    coherence_relevance_note,
                    self_correction_note,
                    target_vocab_used,
                    target_vocab_missed,
                    recommended_next_focus,
                    created_at,
                    updated_at,
                    skill_bridge_status,
                    skill_bridge_notes,
                    skill_bridge_updated_at;
                """,
                (
                    normalized_status,
                    Json(safe_notes),
                    int(session_id),
                ),
            )
            return _voice_session_assessment_row_to_dict(cursor.fetchone())


def finish_agent_voice_session(
    *,
    user_id: int,
    session_id: int | None = None,
    source_lang: str = "ru",
    target_lang: str = "de",
    ended_at: datetime | None = None,
) -> dict | None:
    normalized_source = str(source_lang or "ru").strip().lower() or "ru"
    normalized_target = str(target_lang or "de").strip().lower() or "de"
    ended = ended_at or datetime.now(timezone.utc)
    if ended.tzinfo is None:
        ended = ended.replace(tzinfo=timezone.utc)

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            if session_id is not None:
                cursor.execute(
                    """
                    UPDATE bt_3_agent_voice_sessions
                    SET
                        ended_at = COALESCE(ended_at, %s),
                        duration_seconds = CASE
                            WHEN ended_at IS NOT NULL THEN duration_seconds
                            ELSE GREATEST(0, EXTRACT(EPOCH FROM (%s - started_at))::INT)
                        END,
                        updated_at = NOW()
                    WHERE id = %s
                      AND user_id = %s
                    RETURNING id, started_at, ended_at, duration_seconds, source_lang, target_lang;
                    """,
                    (ended, ended, int(session_id), int(user_id)),
                )
            else:
                cursor.execute(
                    """
                    WITH latest AS (
                        SELECT id
                        FROM bt_3_agent_voice_sessions
                        WHERE user_id = %s
                          AND source_lang = %s
                          AND target_lang = %s
                          AND ended_at IS NULL
                        ORDER BY started_at DESC
                        LIMIT 1
                    )
                    UPDATE bt_3_agent_voice_sessions s
                    SET
                        ended_at = %s,
                        duration_seconds = GREATEST(0, EXTRACT(EPOCH FROM (%s - s.started_at))::INT),
                        updated_at = NOW()
                    FROM latest
                    WHERE s.id = latest.id
                    RETURNING s.id, s.started_at, s.ended_at, s.duration_seconds, s.source_lang, s.target_lang;
                    """,
                    (int(user_id), normalized_source, normalized_target, ended, ended),
                )
            row = cursor.fetchone()
    if not row:
        return None
    duration_seconds = int(row[3] or 0)
    return {
        "session_id": int(row[0]),
        "started_at": row[1].isoformat() if row[1] else None,
        "ended_at": row[2].isoformat() if row[2] else ended.isoformat(),
        "duration_seconds": duration_seconds,
        "duration_minutes": round(duration_seconds / 60.0, 2),
        "source_lang": str(row[4] or normalized_source),
        "target_lang": str(row[5] or normalized_target),
    }


def start_reader_session(
    *,
    user_id: int,
    source_lang: str = "ru",
    target_lang: str = "de",
    started_at: datetime | None = None,
) -> dict:
    normalized_source = str(source_lang or "ru").strip().lower() or "ru"
    normalized_target = str(target_lang or "de").strip().lower() or "de"
    started = started_at or datetime.now(timezone.utc)
    if started.tzinfo is None:
        started = started.replace(tzinfo=timezone.utc)
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE bt_3_reader_sessions
                SET
                    ended_at = %s,
                    duration_seconds = GREATEST(
                        0,
                        LEAST(
                            CASE
                                WHEN duration_seconds IS NOT NULL THEN GREATEST(duration_seconds, 0)
                                ELSE EXTRACT(EPOCH FROM (%s - started_at))::INT
                            END,
                            %s
                        )
                    ),
                    updated_at = NOW()
                WHERE user_id = %s
                  AND source_lang = %s
                  AND target_lang = %s
                  AND ended_at IS NULL;
                """,
                (
                    started,
                    started,
                    int(READER_SESSION_AUTOCLOSE_MAX_SECONDS),
                    int(user_id),
                    normalized_source,
                    normalized_target,
                ),
            )
            cursor.execute(
                """
                INSERT INTO bt_3_reader_sessions (
                    user_id,
                    source_lang,
                    target_lang,
                    started_at,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, NOW())
                RETURNING id, started_at;
                """,
                (int(user_id), normalized_source, normalized_target, started),
            )
            row = cursor.fetchone()
    return {
        "session_id": int(row[0]),
        "started_at": row[1].isoformat() if row and row[1] else started.isoformat(),
    }


def finish_reader_session(
    *,
    user_id: int,
    session_id: int | None = None,
    source_lang: str = "ru",
    target_lang: str = "de",
    ended_at: datetime | None = None,
    duration_seconds: int | None = None,
) -> dict | None:
    normalized_source = str(source_lang or "ru").strip().lower() or "ru"
    normalized_target = str(target_lang or "de").strip().lower() or "de"
    ended = ended_at or datetime.now(timezone.utc)
    if ended.tzinfo is None:
        ended = ended.replace(tzinfo=timezone.utc)
    normalized_duration_seconds = None
    if duration_seconds is not None:
        normalized_duration_seconds = max(0, min(int(duration_seconds), int(READER_SESSION_AUTOCLOSE_MAX_SECONDS)))

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            if session_id is not None:
                cursor.execute(
                    """
                    UPDATE bt_3_reader_sessions
                    SET
                        ended_at = COALESCE(ended_at, %s),
                        duration_seconds = CASE
                            WHEN ended_at IS NOT NULL THEN duration_seconds
                            ELSE GREATEST(
                                0,
                                LEAST(
                                    COALESCE(%s, EXTRACT(EPOCH FROM (%s - started_at))::INT),
                                    %s
                                )
                            )
                        END,
                        updated_at = NOW()
                    WHERE id = %s
                      AND user_id = %s
                    RETURNING id, started_at, ended_at, duration_seconds, source_lang, target_lang;
                    """,
                    (
                        ended,
                        normalized_duration_seconds,
                        ended,
                        int(READER_SESSION_AUTOCLOSE_MAX_SECONDS),
                        int(session_id),
                        int(user_id),
                    ),
                )
            else:
                cursor.execute(
                    """
                    WITH latest AS (
                        SELECT id
                        FROM bt_3_reader_sessions
                        WHERE user_id = %s
                          AND source_lang = %s
                          AND target_lang = %s
                          AND ended_at IS NULL
                        ORDER BY started_at DESC
                        LIMIT 1
                    )
                    UPDATE bt_3_reader_sessions s
                    SET
                        ended_at = %s,
                        duration_seconds = GREATEST(
                            0,
                            LEAST(
                                COALESCE(%s, EXTRACT(EPOCH FROM (%s - s.started_at))::INT),
                                %s
                            )
                        ),
                        updated_at = NOW()
                    FROM latest
                    WHERE s.id = latest.id
                    RETURNING s.id, s.started_at, s.ended_at, s.duration_seconds, s.source_lang, s.target_lang;
                    """,
                    (
                        int(user_id),
                        normalized_source,
                        normalized_target,
                        ended,
                        normalized_duration_seconds,
                        ended,
                        int(READER_SESSION_AUTOCLOSE_MAX_SECONDS),
                    ),
                )
            row = cursor.fetchone()
    if not row:
        return None
    duration_seconds = int(row[3] or 0)
    return {
        "session_id": int(row[0]),
        "started_at": row[1].isoformat() if row[1] else None,
        "ended_at": row[2].isoformat() if row[2] else ended.isoformat(),
        "duration_seconds": duration_seconds,
        "duration_minutes": round(duration_seconds / 60.0, 2),
        "source_lang": str(row[4] or normalized_source),
        "target_lang": str(row[5] or normalized_target),
    }


def touch_reader_session(
    *,
    user_id: int,
    session_id: int,
    duration_seconds: int,
) -> dict | None:
    normalized_duration_seconds = max(0, min(int(duration_seconds or 0), int(READER_SESSION_AUTOCLOSE_MAX_SECONDS)))
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE bt_3_reader_sessions
                SET
                    duration_seconds = %s,
                    updated_at = NOW()
                WHERE id = %s
                  AND user_id = %s
                  AND ended_at IS NULL
                RETURNING id, started_at, ended_at, duration_seconds, source_lang, target_lang;
                """,
                (
                    normalized_duration_seconds,
                    int(session_id),
                    int(user_id),
                ),
            )
            row = cursor.fetchone()
    if not row:
        return None
    return {
        "session_id": int(row[0]),
        "started_at": row[1].isoformat() if row[1] else None,
        "ended_at": row[2].isoformat() if row[2] else None,
        "duration_seconds": int(row[3] or 0),
        "duration_minutes": round(int(row[3] or 0) / 60.0, 2),
        "source_lang": str(row[4] or ""),
        "target_lang": str(row[5] or ""),
    }


def _reader_library_row_to_dict(row: tuple, *, include_content: bool = False) -> dict:
    payload = {
        "id": int(row[0]),
        "user_id": int(row[1]),
        "source_lang": str(row[2] or "ru"),
        "target_lang": str(row[3] or "de"),
        "title": str(row[4] or "Untitled"),
        "source_type": str(row[5] or "text"),
        "source_url": row[6] if row[6] else None,
        "text_hash": str(row[7] or ""),
        "total_chars": max(0, int(row[8] or 0)),
        "progress_percent": round(max(0.0, min(100.0, float(row[9] or 0.0))), 2),
        "bookmark_percent": round(max(0.0, min(100.0, float(row[10] or 0.0))), 2),
        "reading_mode": str(row[11] or "vertical"),
        "is_archived": bool(row[12]),
        "archived_at": row[13].isoformat() if row[13] else None,
        "last_opened_at": row[14].isoformat() if row[14] else None,
        "created_at": row[15].isoformat() if row[15] else None,
        "updated_at": row[16].isoformat() if row[16] else None,
    }
    if include_content:
        payload["content_text"] = str(row[17] or "")
        payload["content_pages"] = row[18] if isinstance(row[18], list) else []
    return payload


def upsert_reader_library_document(
    *,
    user_id: int,
    source_lang: str,
    target_lang: str,
    title: str,
    source_type: str,
    source_url: str | None,
    content_text: str,
    content_pages: list | None = None,
) -> dict:
    normalized_source = str(source_lang or "ru").strip().lower() or "ru"
    normalized_target = str(target_lang or "de").strip().lower() or "de"
    resolved_title = str(title or "Untitled").strip() or "Untitled"
    resolved_source_type = str(source_type or "text").strip().lower() or "text"
    resolved_source_url = str(source_url or "").strip() or None
    resolved_content = str(content_text or "").strip()
    resolved_pages = content_pages if isinstance(content_pages, list) else []
    text_hash = hashlib.sha256(resolved_content.encode("utf-8")).hexdigest()
    total_chars = len(resolved_content)

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_reader_library (
                    user_id,
                    source_lang,
                    target_lang,
                    title,
                    source_type,
                    source_url,
                    text_hash,
                    content_text,
                    content_pages,
                    total_chars,
                    last_opened_at,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, NOW(), NOW())
                ON CONFLICT (user_id, source_lang, target_lang, text_hash) DO UPDATE
                SET
                    title = EXCLUDED.title,
                    source_type = EXCLUDED.source_type,
                    source_url = COALESCE(EXCLUDED.source_url, bt_3_reader_library.source_url),
                    content_text = EXCLUDED.content_text,
                    content_pages = EXCLUDED.content_pages,
                    total_chars = EXCLUDED.total_chars,
                    is_archived = FALSE,
                    archived_at = NULL,
                    last_opened_at = NOW(),
                    updated_at = NOW()
                RETURNING
                    id, user_id, source_lang, target_lang, title, source_type, source_url,
                    text_hash, total_chars, progress_percent, bookmark_percent, reading_mode,
                    is_archived, archived_at, last_opened_at, created_at, updated_at, content_text, content_pages;
                """,
                (
                    int(user_id),
                    normalized_source,
                    normalized_target,
                    resolved_title,
                    resolved_source_type,
                    resolved_source_url,
                    text_hash,
                    resolved_content,
                    json.dumps(resolved_pages, ensure_ascii=False),
                    total_chars,
                ),
            )
            row = cursor.fetchone()
    return _reader_library_row_to_dict(row, include_content=True)


def list_reader_library_documents(
    *,
    user_id: int,
    source_lang: str,
    target_lang: str,
    limit: int = 100,
    include_archived: bool = False,
) -> list[dict]:
    normalized_source = str(source_lang or "ru").strip().lower() or "ru"
    normalized_target = str(target_lang or "de").strip().lower() or "de"
    safe_limit = max(1, min(300, int(limit or 100)))
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    id, user_id, source_lang, target_lang, title, source_type, source_url,
                    text_hash, total_chars, progress_percent, bookmark_percent, reading_mode,
                    is_archived, archived_at, last_opened_at, created_at, updated_at
                FROM bt_3_reader_library
                WHERE user_id = %s
                  AND source_lang = %s
                  AND target_lang = %s
                  AND (%s OR COALESCE(is_archived, FALSE) = FALSE)
                ORDER BY COALESCE(last_opened_at, updated_at, created_at) DESC
                LIMIT %s;
                """,
                (int(user_id), normalized_source, normalized_target, bool(include_archived), safe_limit),
            )
            rows = cursor.fetchall()
    return [_reader_library_row_to_dict(row, include_content=False) for row in rows]


def get_reader_library_document(
    *,
    user_id: int,
    document_id: int,
    source_lang: str,
    target_lang: str,
) -> dict | None:
    normalized_source = str(source_lang or "ru").strip().lower() or "ru"
    normalized_target = str(target_lang or "de").strip().lower() or "de"
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    id, user_id, source_lang, target_lang, title, source_type, source_url,
                    text_hash, total_chars, progress_percent, bookmark_percent, reading_mode,
                    is_archived, archived_at, last_opened_at, created_at, updated_at, content_text
                    , content_pages
                FROM bt_3_reader_library
                WHERE id = %s
                  AND user_id = %s
                  AND source_lang = %s
                  AND target_lang = %s
                LIMIT 1;
                """,
                (int(document_id), int(user_id), normalized_source, normalized_target),
            )
            row = cursor.fetchone()
            if not row:
                return None
            cursor.execute(
                """
                UPDATE bt_3_reader_library
                SET last_opened_at = NOW(), updated_at = NOW()
                WHERE id = %s;
                """,
                (int(document_id),),
            )
    return _reader_library_row_to_dict(row, include_content=True)


def update_reader_library_state(
    *,
    user_id: int,
    document_id: int,
    source_lang: str,
    target_lang: str,
    progress_percent: float | None = None,
    bookmark_percent: float | None = None,
    reading_mode: str | None = None,
) -> dict | None:
    normalized_source = str(source_lang or "ru").strip().lower() or "ru"
    normalized_target = str(target_lang or "de").strip().lower() or "de"
    resolved_progress = None if progress_percent is None else max(0.0, min(100.0, float(progress_percent)))
    resolved_bookmark = None if bookmark_percent is None else max(0.0, min(100.0, float(bookmark_percent)))
    resolved_mode = str(reading_mode or "").strip().lower()
    if resolved_mode not in {"", "vertical", "horizontal"}:
        resolved_mode = ""

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE bt_3_reader_library
                SET
                    progress_percent = COALESCE(%s, progress_percent),
                    bookmark_percent = COALESCE(%s, bookmark_percent),
                    reading_mode = COALESCE(NULLIF(%s, ''), reading_mode),
                    last_opened_at = NOW(),
                    updated_at = NOW()
                WHERE id = %s
                  AND user_id = %s
                  AND source_lang = %s
                  AND target_lang = %s
                RETURNING
                    id, user_id, source_lang, target_lang, title, source_type, source_url,
                    text_hash, total_chars, progress_percent, bookmark_percent, reading_mode,
                    is_archived, archived_at, last_opened_at, created_at, updated_at;
                """,
                (
                    resolved_progress,
                    resolved_bookmark,
                    resolved_mode,
                    int(document_id),
                    int(user_id),
                    normalized_source,
                    normalized_target,
                ),
            )
            row = cursor.fetchone()
    if not row:
        return None
    return _reader_library_row_to_dict(row, include_content=False)


def rename_reader_library_document(
    *,
    user_id: int,
    document_id: int,
    source_lang: str,
    target_lang: str,
    title: str,
) -> dict | None:
    normalized_source = str(source_lang or "ru").strip().lower() or "ru"
    normalized_target = str(target_lang or "de").strip().lower() or "de"
    resolved_title = str(title or "").strip()
    if not resolved_title:
        return None
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE bt_3_reader_library
                SET title = %s, updated_at = NOW()
                WHERE id = %s
                  AND user_id = %s
                  AND source_lang = %s
                  AND target_lang = %s
                RETURNING
                    id, user_id, source_lang, target_lang, title, source_type, source_url,
                    text_hash, total_chars, progress_percent, bookmark_percent, reading_mode,
                    is_archived, archived_at, last_opened_at, created_at, updated_at;
                """,
                (resolved_title, int(document_id), int(user_id), normalized_source, normalized_target),
            )
            row = cursor.fetchone()
    if not row:
        return None
    return _reader_library_row_to_dict(row, include_content=False)


def archive_reader_library_document(
    *,
    user_id: int,
    document_id: int,
    source_lang: str,
    target_lang: str,
    archived: bool = True,
) -> dict | None:
    normalized_source = str(source_lang or "ru").strip().lower() or "ru"
    normalized_target = str(target_lang or "de").strip().lower() or "de"
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE bt_3_reader_library
                SET
                    is_archived = %s,
                    archived_at = CASE WHEN %s THEN NOW() ELSE NULL END,
                    updated_at = NOW()
                WHERE id = %s
                  AND user_id = %s
                  AND source_lang = %s
                  AND target_lang = %s
                RETURNING
                    id, user_id, source_lang, target_lang, title, source_type, source_url,
                    text_hash, total_chars, progress_percent, bookmark_percent, reading_mode,
                    is_archived, archived_at, last_opened_at, created_at, updated_at;
                """,
                (bool(archived), bool(archived), int(document_id), int(user_id), normalized_source, normalized_target),
            )
            row = cursor.fetchone()
    if not row:
        return None
    return _reader_library_row_to_dict(row, include_content=False)


def delete_reader_library_document(
    *,
    user_id: int,
    document_id: int,
    source_lang: str,
    target_lang: str,
) -> bool:
    normalized_source = str(source_lang or "ru").strip().lower() or "ru"
    normalized_target = str(target_lang or "de").strip().lower() or "de"
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                DELETE FROM bt_3_reader_library
                WHERE id = %s
                  AND user_id = %s
                  AND source_lang = %s
                  AND target_lang = %s;
                """,
                (int(document_id), int(user_id), normalized_source, normalized_target),
            )
            deleted = cursor.rowcount > 0
    return bool(deleted)


def get_daily_plan(user_id: int, plan_date: date) -> dict | None:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT id, user_id, plan_date, total_minutes, created_at
                FROM bt_3_daily_plans
                WHERE user_id = %s AND plan_date = %s
                LIMIT 1;
                """,
                (int(user_id), plan_date),
            )
            row = cursor.fetchone()
            if not row:
                return None

            plan_id = int(row[0])
            cursor.execute(
                """
                SELECT
                    id,
                    plan_id,
                    order_index,
                    task_type,
                    title,
                    estimated_minutes,
                    payload,
                    status,
                    completed_at
                FROM bt_3_daily_plan_items
                WHERE plan_id = %s
                ORDER BY order_index ASC, id ASC;
                """,
                (plan_id,),
            )
            items = [_map_daily_plan_item(item_row) for item_row in cursor.fetchall()]

    return {
        "id": plan_id,
        "user_id": int(row[1]),
        "plan_date": row[2].isoformat() if row[2] else None,
        "total_minutes": int(row[3] or 0),
        "created_at": row[4].isoformat() if row[4] else None,
        "items": items,
    }


def get_daily_plan_item(*, user_id: int, item_id: int) -> dict | None:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    i.id,
                    i.plan_id,
                    i.order_index,
                    i.task_type,
                    i.title,
                    i.estimated_minutes,
                    i.payload,
                    i.status,
                    i.completed_at
                FROM bt_3_daily_plan_items i
                JOIN bt_3_daily_plans p ON p.id = i.plan_id
                WHERE i.id = %s
                  AND p.user_id = %s
                LIMIT 1;
                """,
                (int(item_id), int(user_id)),
            )
            row = cursor.fetchone()
    if not row:
        return None
    return _map_daily_plan_item(row)


def create_daily_plan(
    user_id: int,
    plan_date: date,
    total_minutes: int,
    items: list[dict],
) -> dict:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_daily_plans (user_id, plan_date, total_minutes)
                VALUES (%s, %s, %s)
                ON CONFLICT (user_id, plan_date) DO UPDATE
                SET total_minutes = EXCLUDED.total_minutes
                RETURNING id;
                """,
                (int(user_id), plan_date, max(0, int(total_minutes))),
            )
            plan_id = int(cursor.fetchone()[0])

            cursor.execute(
                """
                DELETE FROM bt_3_daily_plan_items
                WHERE plan_id = %s;
                """,
                (plan_id,),
            )

            for index, item in enumerate(items):
                payload = item.get("payload") if isinstance(item, dict) else {}
                if not isinstance(payload, dict):
                    payload = {}
                cursor.execute(
                    """
                    INSERT INTO bt_3_daily_plan_items (
                        plan_id,
                        order_index,
                        task_type,
                        title,
                        estimated_minutes,
                        payload,
                        status
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s);
                    """,
                    (
                        plan_id,
                        int(item.get("order_index", index)),
                        str(item.get("task_type") or "task"),
                        str(item.get("title") or "Задача"),
                        max(0, int(item.get("estimated_minutes", 0))),
                        json.dumps(payload, ensure_ascii=False),
                        str(item.get("status") or "todo"),
                    ),
                )

    return get_daily_plan(user_id=user_id, plan_date=plan_date) or {
        "id": plan_id,
        "user_id": int(user_id),
        "plan_date": plan_date.isoformat(),
        "total_minutes": max(0, int(total_minutes)),
        "created_at": None,
        "items": [],
    }


def delete_daily_plans_from_date(
    *,
    user_id: int,
    from_date: date,
) -> int:
    if not isinstance(from_date, date):
        raise ValueError("from_date must be a date")
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                DELETE FROM bt_3_daily_plans
                WHERE user_id = %s
                  AND plan_date >= %s;
                """,
                (int(user_id), from_date),
            )
            return int(cursor.rowcount or 0)


def update_daily_plan_item_status(
    *,
    user_id: int,
    item_id: int,
    status: str,
) -> dict | None:
    normalized = str(status or "").strip().lower()
    if normalized not in {"todo", "doing", "done", "skipped"}:
        raise ValueError("status must be one of: todo, doing, done, skipped")

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE bt_3_daily_plan_items i
                SET
                    payload = CASE
                        WHEN %s = 'done' THEN
                            COALESCE(i.payload, '{}'::jsonb)
                            || jsonb_build_object(
                                'timer_running', false,
                                'timer_paused', false,
                                'timer_started_at', NULL,
                                'timer_updated_at', NOW() AT TIME ZONE 'UTC'
                            )
                        ELSE COALESCE(i.payload, '{}'::jsonb)
                    END,
                    status = %s,
                    completed_at = CASE
                        WHEN %s = 'done' THEN NOW()
                        ELSE NULL
                    END
                FROM bt_3_daily_plans p
                WHERE i.id = %s
                  AND i.plan_id = p.id
                  AND p.user_id = %s
                RETURNING
                    i.id,
                    i.plan_id,
                    i.order_index,
                    i.task_type,
                    i.title,
                    i.estimated_minutes,
                    i.payload,
                    i.status,
                    i.completed_at;
                """,
                (normalized, normalized, normalized, int(item_id), int(user_id)),
            )
            row = cursor.fetchone()
            if not row:
                return None
            return _map_daily_plan_item(row)


def update_daily_plan_item_payload(
    *,
    user_id: int,
    item_id: int,
    payload_updates: dict,
) -> dict | None:
    updates = payload_updates if isinstance(payload_updates, dict) else {}
    if not updates:
        return None

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE bt_3_daily_plan_items i
                SET payload = COALESCE(i.payload, '{}'::jsonb) || %s::jsonb
                FROM bt_3_daily_plans p
                WHERE i.id = %s
                  AND i.plan_id = p.id
                  AND p.user_id = %s
                RETURNING
                    i.id,
                    i.plan_id,
                    i.order_index,
                    i.task_type,
                    i.title,
                    i.estimated_minutes,
                    i.payload,
                    i.status,
                    i.completed_at;
                """,
                (json.dumps(updates, ensure_ascii=False), int(item_id), int(user_id)),
            )
            row = cursor.fetchone()
            if not row:
                return None
            return _map_daily_plan_item(row)


def update_daily_plan_item_timer(
    *,
    user_id: int,
    item_id: int,
    action: str,
    elapsed_seconds: int | None = None,
    running: bool | None = None,
    event_at: datetime | None = None,
) -> dict | None:
    normalized_action = str(action or "").strip().lower()
    if normalized_action not in {"start", "pause", "resume", "sync"}:
        raise ValueError("action must be one of: start, pause, resume, sync")

    def _parse_iso_dt(raw: object) -> datetime | None:
        text = str(raw or "").strip()
        if not text:
            return None
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except Exception:
            return None

    now_utc = event_at or datetime.now(timezone.utc)
    if now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=timezone.utc)
    else:
        now_utc = now_utc.astimezone(timezone.utc)
    safe_elapsed_override = None if elapsed_seconds is None else max(0, int(elapsed_seconds))

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    i.id,
                    i.plan_id,
                    i.order_index,
                    i.task_type,
                    i.title,
                    i.estimated_minutes,
                    i.payload,
                    i.status,
                    i.completed_at
                FROM bt_3_daily_plan_items i
                JOIN bt_3_daily_plans p ON p.id = i.plan_id
                WHERE i.id = %s
                  AND p.user_id = %s
                LIMIT 1;
                """,
                (int(item_id), int(user_id)),
            )
            row = cursor.fetchone()
            if not row:
                return None

            current_item = _map_daily_plan_item(row)
            current_payload = current_item["payload"] if isinstance(current_item["payload"], dict) else {}
            task_type = str(current_item.get("task_type") or "").strip().lower()
            is_translation_task = task_type == "translation"
            estimated_minutes = max(0, int(current_item.get("estimated_minutes") or 0))
            goal_seconds = estimated_minutes * 60

            stored_elapsed = max(0, int(current_payload.get("timer_seconds") or 0))
            stored_started_at = _parse_iso_dt(current_payload.get("timer_started_at"))
            stored_running = bool(current_payload.get("timer_running")) and stored_started_at is not None
            live_elapsed = stored_elapsed
            if stored_running and stored_started_at:
                live_elapsed += max(0, int((now_utc - stored_started_at).total_seconds()))

            authoritative_elapsed = live_elapsed if safe_elapsed_override is None else safe_elapsed_override

            next_elapsed = max(0, int(authoritative_elapsed))
            next_running = stored_running
            next_paused = bool(current_payload.get("timer_paused")) and not stored_running
            next_started_at = stored_started_at if stored_running else None
            next_status = str(current_item.get("status") or "todo").lower()

            if normalized_action == "start":
                if next_status != "done":
                    next_status = "doing"
                    next_running = True
                    next_paused = False
                    next_started_at = now_utc
            elif normalized_action == "resume":
                if next_status != "done":
                    next_status = "doing"
                    next_running = True
                    next_started_at = now_utc
                    next_paused = False
            elif normalized_action == "pause":
                next_running = False
                next_paused = next_status != "done"
                next_started_at = None
            elif normalized_action == "sync":
                if running is not None:
                    if bool(running) and next_status != "done":
                        next_running = True
                        next_paused = False
                        next_started_at = now_utc
                    else:
                        next_running = False
                        next_paused = next_status != "done"
                        next_started_at = None

            progress_percent = 0.0
            if goal_seconds > 0:
                progress_percent = min(100.0, (float(next_elapsed) / float(goal_seconds)) * 100.0)
            elif next_elapsed > 0:
                progress_percent = 100.0

            if not is_translation_task and goal_seconds > 0 and next_elapsed >= goal_seconds:
                next_status = "done"
                next_running = False
                next_paused = False
                next_started_at = None
            elif next_status != "done" and next_elapsed > 0 and next_status in {"todo", "skipped"}:
                next_status = "doing"

            payload_next = {
                **current_payload,
                "timer_seconds": int(next_elapsed),
                "timer_goal_seconds": int(goal_seconds),
                "timer_progress_percent": round(progress_percent, 2),
                "timer_running": bool(next_running),
                "timer_paused": bool(next_paused),
                "timer_started_at": next_started_at.isoformat() if next_started_at else None,
                "timer_updated_at": now_utc.isoformat(),
            }

            cursor.execute(
                """
                UPDATE bt_3_daily_plan_items i
                SET
                    payload = %s::jsonb,
                    status = %s,
                    completed_at = CASE
                        WHEN %s = 'done' THEN NOW()
                        ELSE NULL
                    END
                FROM bt_3_daily_plans p
                WHERE i.id = %s
                  AND i.plan_id = p.id
                  AND p.user_id = %s
                RETURNING
                    i.id,
                    i.plan_id,
                    i.order_index,
                    i.task_type,
                    i.title,
                    i.estimated_minutes,
                    i.payload,
                    i.status,
                    i.completed_at;
                """,
                (
                    json.dumps(payload_next, ensure_ascii=False),
                    next_status,
                    next_status,
                    int(item_id),
                    int(user_id),
                ),
            )
            saved = cursor.fetchone()
            if not saved:
                return None
            return _map_daily_plan_item(saved)


def _normalize_focus_part(value: str | None) -> str:
    return " ".join(str(value or "").strip().lower().split())


def _build_video_focus_key(
    *,
    source_lang: str,
    target_lang: str,
    skill_id: str | None,
    main_category: str | None,
    sub_category: str | None,
) -> str:
    src = _normalize_focus_part(source_lang) or "ru"
    tgt = _normalize_focus_part(target_lang) or "de"
    skill = _normalize_focus_part(skill_id) or "-"
    main = _normalize_focus_part(main_category) or "-"
    sub = _normalize_focus_part(sub_category) or "-"
    return f"{src}|{tgt}|{skill}|{main}|{sub}"


def _map_video_recommendation_row(row: tuple) -> dict:
    return {
        "id": int(row[0]),
        "source_lang": str(row[1] or "ru"),
        "target_lang": str(row[2] or "de"),
        "focus_key": str(row[3] or ""),
        "skill_id": str(row[4] or "") or None,
        "main_category": str(row[5] or "") or None,
        "sub_category": str(row[6] or "") or None,
        "search_query": str(row[7] or "") or None,
        "video_id": str(row[8] or ""),
        "video_url": str(row[9] or "") or None,
        "video_title": str(row[10] or "") or None,
        "like_count": int(row[11] or 0),
        "dislike_count": int(row[12] or 0),
        "score": int(row[13] or 0),
        "is_active": bool(row[14]),
        "updated_at": row[15].isoformat() if row[15] else None,
        "last_selected_at": row[16].isoformat() if row[16] else None,
    }


def get_best_video_recommendation_for_focus(
    *,
    source_lang: str,
    target_lang: str,
    skill_id: str | None,
    main_category: str | None,
    sub_category: str | None,
) -> dict | None:
    focus_key = _build_video_focus_key(
        source_lang=source_lang,
        target_lang=target_lang,
        skill_id=skill_id,
        main_category=main_category,
        sub_category=sub_category,
    )
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    id, source_lang, target_lang, focus_key, skill_id, main_category, sub_category,
                    search_query, video_id, video_url, video_title, like_count, dislike_count,
                    score, is_active, updated_at, last_selected_at
                FROM bt_3_video_recommendations
                WHERE focus_key = %s
                  AND is_active = TRUE
                ORDER BY score DESC, like_count DESC, updated_at DESC
                LIMIT 1;
                """,
                (focus_key,),
            )
            row = cursor.fetchone()
            if not row:
                return None
            cursor.execute(
                """
                UPDATE bt_3_video_recommendations
                SET last_selected_at = NOW(), updated_at = NOW()
                WHERE id = %s;
                """,
                (int(row[0]),),
            )
    return _map_video_recommendation_row(row)


def upsert_video_recommendation(
    *,
    source_lang: str,
    target_lang: str,
    skill_id: str | None,
    main_category: str | None,
    sub_category: str | None,
    search_query: str | None,
    video_id: str,
    video_url: str | None,
    video_title: str | None,
) -> dict | None:
    resolved_video_id = str(video_id or "").strip()
    if not resolved_video_id:
        return None
    normalized_source = str(source_lang or "ru").strip().lower() or "ru"
    normalized_target = str(target_lang or "de").strip().lower() or "de"
    focus_key = _build_video_focus_key(
        source_lang=normalized_source,
        target_lang=normalized_target,
        skill_id=skill_id,
        main_category=main_category,
        sub_category=sub_category,
    )
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_video_recommendations (
                    source_lang,
                    target_lang,
                    focus_key,
                    skill_id,
                    main_category,
                    sub_category,
                    search_query,
                    video_id,
                    video_url,
                    video_title,
                    is_active,
                    last_selected_at,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, TRUE, NOW(), NOW())
                ON CONFLICT (focus_key, video_id) DO UPDATE
                SET
                    source_lang = EXCLUDED.source_lang,
                    target_lang = EXCLUDED.target_lang,
                    skill_id = COALESCE(EXCLUDED.skill_id, bt_3_video_recommendations.skill_id),
                    main_category = COALESCE(EXCLUDED.main_category, bt_3_video_recommendations.main_category),
                    sub_category = COALESCE(EXCLUDED.sub_category, bt_3_video_recommendations.sub_category),
                    search_query = COALESCE(EXCLUDED.search_query, bt_3_video_recommendations.search_query),
                    video_url = COALESCE(EXCLUDED.video_url, bt_3_video_recommendations.video_url),
                    video_title = COALESCE(EXCLUDED.video_title, bt_3_video_recommendations.video_title),
                    is_active = TRUE,
                    last_selected_at = NOW(),
                    updated_at = NOW()
                RETURNING
                    id, source_lang, target_lang, focus_key, skill_id, main_category, sub_category,
                    search_query, video_id, video_url, video_title, like_count, dislike_count,
                    score, is_active, updated_at, last_selected_at;
                """,
                (
                    normalized_source,
                    normalized_target,
                    focus_key,
                    str(skill_id or "").strip() or None,
                    str(main_category or "").strip() or None,
                    str(sub_category or "").strip() or None,
                    str(search_query or "").strip() or None,
                    resolved_video_id,
                    str(video_url or "").strip() or None,
                    str(video_title or "").strip() or None,
                ),
            )
            row = cursor.fetchone()
    return _map_video_recommendation_row(row) if row else None


def get_video_recommendation_by_id(recommendation_id: int) -> dict | None:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    id, source_lang, target_lang, focus_key, skill_id, main_category, sub_category,
                    search_query, video_id, video_url, video_title, like_count, dislike_count,
                    score, is_active, updated_at, last_selected_at
                FROM bt_3_video_recommendations
                WHERE id = %s
                LIMIT 1;
                """,
                (int(recommendation_id),),
            )
            row = cursor.fetchone()
    return _map_video_recommendation_row(row) if row else None


def vote_video_recommendation(
    *,
    user_id: int,
    recommendation_id: int,
    vote: int,
) -> dict | None:
    normalized_vote = 1 if int(vote) >= 0 else -1
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_video_recommendation_votes (
                    recommendation_id, user_id, vote, updated_at
                )
                VALUES (%s, %s, %s, NOW())
                ON CONFLICT (recommendation_id, user_id) DO UPDATE
                SET vote = EXCLUDED.vote, updated_at = NOW();
                """,
                (int(recommendation_id), int(user_id), int(normalized_vote)),
            )
            cursor.execute(
                """
                WITH agg AS (
                    SELECT
                        recommendation_id,
                        COUNT(*) FILTER (WHERE vote = 1) AS like_count,
                        COUNT(*) FILTER (WHERE vote = -1) AS dislike_count
                    FROM bt_3_video_recommendation_votes
                    WHERE recommendation_id = %s
                    GROUP BY recommendation_id
                )
                UPDATE bt_3_video_recommendations r
                SET
                    like_count = COALESCE(agg.like_count, 0),
                    dislike_count = COALESCE(agg.dislike_count, 0),
                    score = COALESCE(agg.like_count, 0) - COALESCE(agg.dislike_count, 0),
                    is_active = (COALESCE(agg.like_count, 0) - COALESCE(agg.dislike_count, 0)) >= 0,
                    updated_at = NOW()
                FROM agg
                WHERE r.id = agg.recommendation_id
                  AND r.id = %s
                RETURNING
                    r.id, r.source_lang, r.target_lang, r.focus_key, r.skill_id, r.main_category, r.sub_category,
                    r.search_query, r.video_id, r.video_url, r.video_title, r.like_count, r.dislike_count,
                    r.score, r.is_active, r.updated_at, r.last_selected_at;
                """,
                (int(recommendation_id), int(recommendation_id)),
            )
            row = cursor.fetchone()
            if not row:
                return None
    result = _map_video_recommendation_row(row)
    result["user_vote"] = int(normalized_vote)
    return result


def consume_today_regenerate_limit(
    *,
    user_id: int,
    limit_date: date,
) -> dict:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_today_regenerate_limits (user_id, limit_date)
                VALUES (%s, %s)
                ON CONFLICT (user_id, limit_date) DO NOTHING
                RETURNING consumed_at;
                """,
                (int(user_id), limit_date),
            )
            row = cursor.fetchone()
            if row:
                return {
                    "allowed": True,
                    "consumed_at": row[0].isoformat() if row[0] else None,
                }

            cursor.execute(
                """
                SELECT consumed_at
                FROM bt_3_today_regenerate_limits
                WHERE user_id = %s AND limit_date = %s
                LIMIT 1;
                """,
                (int(user_id), limit_date),
            )
            existing = cursor.fetchone()
    return {
        "allowed": False,
        "consumed_at": existing[0].isoformat() if existing and existing[0] else None,
    }


def list_top_weak_topics(
    *,
    user_id: int,
    lookback_days: int = 7,
    source_lang: str | None = None,
    target_lang: str | None = None,
    limit: int = 5,
) -> list[dict]:
    lookback_days = max(1, int(lookback_days))
    limit = max(1, min(int(limit or 5), 20))
    normalized_source = str(source_lang or "ru").strip().lower() or "ru"
    normalized_target = str(target_lang or "de").strip().lower() or "de"
    cutoff_date = get_user_progress_reset_date(
        user_id=int(user_id),
        source_lang=normalized_source,
        target_lang=normalized_target,
    )
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        COALESCE(NULLIF(dm.main_category, ''), 'Other mistake') AS main_category,
                        COALESCE(NULLIF(dm.sub_category, ''), 'Unclassified mistake') AS sub_category,
                        SUM(COALESCE(dm.mistake_count, 1)) AS total_mistakes
                    FROM bt_3_detailed_mistakes dm
                    JOIN bt_3_daily_sentences ds
                      ON ds.id_for_mistake_table = dm.sentence_id
                     AND ds.user_id = dm.user_id
                    WHERE dm.user_id = %s
                      AND COALESCE(ds.source_lang, 'ru') = COALESCE(%s, 'ru')
                      AND COALESCE(ds.target_lang, 'de') = COALESCE(%s, 'de')
                      AND LOWER(COALESCE(NULLIF(dm.sub_category, ''), 'Unclassified mistake')) NOT IN ('unclassified mistake', 'unclassified mistakes')
                      AND COALESCE(dm.last_seen, dm.added_data, NOW()) >= NOW() - (%s::text || ' days')::interval
                      AND (%s::date IS NULL OR COALESCE(dm.last_seen, dm.added_data, NOW())::date >= %s::date)
                    GROUP BY 1, 2
                    ORDER BY total_mistakes DESC, main_category ASC, sub_category ASC
                    LIMIT %s;
                    """,
                    (int(user_id), normalized_source, normalized_target, lookback_days, cutoff_date, cutoff_date, limit),
                )
                rows = cursor.fetchall()
    except Exception:
        return []

    topics: list[dict] = []
    for row in rows or []:
        if not row:
            continue
        topics.append(
            {
                "main_category": row[0],
                "sub_category": row[1],
                "mistakes": int(row[2] or 0),
            }
        )
    return topics


def get_top_weak_topic(
    *,
    user_id: int,
    lookback_days: int = 7,
    source_lang: str | None = None,
    target_lang: str | None = None,
) -> dict | None:
    topics = list_top_weak_topics(
        user_id=user_id,
        lookback_days=lookback_days,
        source_lang=source_lang,
        target_lang=target_lang,
        limit=1,
    )
    if not topics:
        return None
    return topics[0]


def get_weak_topic_sentences(
    *,
    user_id: int,
    main_category: str,
    sub_category: str,
    lookback_days: int = 7,
    limit: int = 5,
    source_lang: str | None = None,
    target_lang: str | None = None,
) -> list[str]:
    if not main_category and not sub_category:
        return []
    lookback_days = max(1, int(lookback_days))
    limit = max(1, min(int(limit), 20))
    normalized_source = str(source_lang or "ru").strip().lower() or "ru"
    normalized_target = str(target_lang or "de").strip().lower() or "de"
    cutoff_date = get_user_progress_reset_date(
        user_id=int(user_id),
        source_lang=normalized_source,
        target_lang=normalized_target,
    )
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT dm.sentence
                    FROM bt_3_detailed_mistakes dm
                    JOIN bt_3_daily_sentences ds
                      ON ds.id_for_mistake_table = dm.sentence_id
                     AND ds.user_id = dm.user_id
                    WHERE dm.user_id = %s
                      AND COALESCE(ds.source_lang, 'ru') = COALESCE(%s, 'ru')
                      AND COALESCE(ds.target_lang, 'de') = COALESCE(%s, 'de')
                      AND COALESCE(NULLIF(dm.main_category, ''), 'Other mistake') = %s
                      AND COALESCE(NULLIF(dm.sub_category, ''), 'Unclassified mistake') = %s
                      AND COALESCE(dm.last_seen, dm.added_data, NOW()) >= NOW() - (%s::text || ' days')::interval
                      AND (%s::date IS NULL OR COALESCE(dm.last_seen, dm.added_data, NOW())::date >= %s::date)
                      AND dm.sentence IS NOT NULL
                      AND dm.sentence <> ''
                    ORDER BY COALESCE(dm.last_seen, dm.added_data, NOW()) DESC, COALESCE(dm.mistake_count, 1) DESC
                    LIMIT %s;
                    """,
                    (
                        int(user_id),
                        normalized_source,
                        normalized_target,
                        main_category,
                        sub_category,
                        lookback_days,
                        cutoff_date,
                        cutoff_date,
                        limit,
                    ),
                )
                rows = cursor.fetchall()
    except Exception:
        return []
    return [str(row[0]).strip() for row in rows if row and str(row[0]).strip()]


def list_recent_started_video_topics(
    *,
    user_id: int,
    lookback_days: int = 7,
    limit: int = 20,
) -> list[dict]:
    safe_lookback_days = max(1, int(lookback_days or 7))
    safe_limit = max(1, min(int(limit or 20), 100))
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT DISTINCT ON (
                        COALESCE(NULLIF(i.payload ->> 'skill_id', ''), ''),
                        COALESCE(NULLIF(i.payload ->> 'main_category', ''), ''),
                        COALESCE(NULLIF(i.payload ->> 'sub_category', ''), '')
                    )
                        NULLIF(i.payload ->> 'skill_id', '') AS skill_id,
                        NULLIF(i.payload ->> 'main_category', '') AS main_category,
                        NULLIF(i.payload ->> 'sub_category', '') AS sub_category,
                        COALESCE(i.completed_at, p.created_at) AS last_used_at
                    FROM bt_3_daily_plan_items i
                    JOIN bt_3_daily_plans p ON p.id = i.plan_id
                    WHERE p.user_id = %s
                      AND p.plan_date >= (CURRENT_DATE - ((%s::int - 1) * INTERVAL '1 day'))::date
                      AND LOWER(COALESCE(i.task_type, '')) IN ('video', 'youtube')
                      AND (
                        LOWER(COALESCE(i.status, '')) IN ('doing', 'done')
                        OR COALESCE(NULLIF(i.payload ->> 'timer_seconds', '')::int, 0) > 0
                        OR COALESCE(NULLIF(i.payload ->> 'video_user_vote', '')::int, 0) <> 0
                      )
                    ORDER BY
                        COALESCE(NULLIF(i.payload ->> 'skill_id', ''), ''),
                        COALESCE(NULLIF(i.payload ->> 'main_category', ''), ''),
                        COALESCE(NULLIF(i.payload ->> 'sub_category', ''), ''),
                        COALESCE(i.completed_at, p.created_at) DESC
                    LIMIT %s;
                    """,
                    (int(user_id), safe_lookback_days, safe_limit),
                )
                rows = cursor.fetchall()
    except Exception:
        return []

    topics: list[dict] = []
    for row in rows or []:
        if not row:
            continue
        skill_id = str(row[0] or "").strip() or None
        main_category = str(row[1] or "").strip() or None
        sub_category = str(row[2] or "").strip() or None
        if not skill_id and not main_category and not sub_category:
            continue
        topics.append(
            {
                "skill_id": skill_id,
                "main_category": main_category,
                "sub_category": sub_category,
                "last_used_at": row[3].isoformat() if row[3] else None,
            }
        )
    return topics


def get_recent_mistake_examples_for_topic(
    *,
    user_id: int,
    main_category: str,
    sub_category: str,
    lookback_days: int = 14,
    limit: int = 5,
    source_lang: str | None = None,
    target_lang: str | None = None,
) -> list[dict]:
    if not main_category and not sub_category:
        return []
    lookback_days = max(1, int(lookback_days))
    limit = max(1, min(int(limit), 20))
    normalized_source = str(source_lang or "ru").strip().lower() or "ru"
    normalized_target = str(target_lang or "de").strip().lower() or "de"
    cutoff_date = get_user_progress_reset_date(
        user_id=int(user_id),
        source_lang=normalized_source,
        target_lang=normalized_target,
    )
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        COALESCE(ds.sentence, '') AS source_sentence,
                        COALESCE(latest_tr.user_translation, '') AS user_translation,
                        COALESCE(latest_tr.feedback, '') AS feedback
                    FROM bt_3_detailed_mistakes dm
                    JOIN bt_3_daily_sentences ds
                      ON ds.id_for_mistake_table = dm.sentence_id
                     AND ds.user_id = dm.user_id
                    LEFT JOIN LATERAL (
                        SELECT tr.user_translation, tr.feedback
                        FROM bt_3_translations tr
                        WHERE tr.user_id = dm.user_id
                          AND tr.id_for_mistake_table = dm.sentence_id
                          AND COALESCE(tr.source_lang, 'ru') = COALESCE(%s, 'ru')
                          AND COALESCE(tr.target_lang, 'de') = COALESCE(%s, 'de')
                          AND (%s::date IS NULL OR tr.timestamp::date >= %s::date)
                        ORDER BY tr.timestamp DESC
                        LIMIT 1
                    ) latest_tr ON TRUE
                    WHERE dm.user_id = %s
                      AND COALESCE(ds.source_lang, 'ru') = COALESCE(%s, 'ru')
                      AND COALESCE(ds.target_lang, 'de') = COALESCE(%s, 'de')
                      AND COALESCE(NULLIF(dm.main_category, ''), 'Other mistake') = %s
                      AND COALESCE(NULLIF(dm.sub_category, ''), 'Unclassified mistake') = %s
                      AND COALESCE(dm.last_seen, dm.added_data, NOW()) >= NOW() - (%s::text || ' days')::interval
                      AND (%s::date IS NULL OR COALESCE(dm.last_seen, dm.added_data, NOW())::date >= %s::date)
                    ORDER BY COALESCE(dm.last_seen, dm.added_data, NOW()) DESC, COALESCE(dm.mistake_count, 1) DESC
                    LIMIT %s;
                    """,
                    (
                        normalized_source,
                        normalized_target,
                        cutoff_date,
                        cutoff_date,
                        int(user_id),
                        normalized_source,
                        normalized_target,
                        main_category,
                        sub_category,
                        lookback_days,
                        cutoff_date,
                        cutoff_date,
                        limit,
                    ),
                )
                rows = cursor.fetchall()
    except Exception:
        return []
    result: list[dict] = []
    for row in rows:
        source_sentence = str(row[0] or "").strip()
        user_translation = str(row[1] or "").strip()
        feedback = str(row[2] or "").strip()
        if not source_sentence and not user_translation:
            continue
        result.append(
            {
                "source_sentence": source_sentence,
                "user_translation": user_translation,
                "feedback": feedback,
            }
        )
    return result


def get_lowest_mastery_skill(
    user_id: int,
    *,
    source_lang: str | None = None,
    target_lang: str | None = None,
) -> dict | None:
    normalized_source = str(source_lang or "ru").strip().lower() or "ru"
    normalized_target = str(target_lang or "de").strip().lower() or "de"
    reset_date = get_user_progress_reset_date(
        user_id=int(user_id),
        source_lang=normalized_source,
        target_lang=normalized_target,
    )
    if reset_date is not None:
        state_map = _get_skill_state_v2_snapshot_since_date(
            user_id=int(user_id),
            source_lang=normalized_source,
            target_lang=normalized_target,
            reset_date=reset_date,
        )
        if not state_map:
            return None
        try:
            with get_db_connection_context() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT skill_id, title, category
                        FROM bt_3_skills
                        WHERE language_code = %s
                          AND COALESCE(is_active, TRUE) = TRUE
                          AND LOWER(COALESCE(skill_id, '')) NOT IN ('other_unclassified', 'en_other_unclassified', 'es_other_unclassified', 'it_other_unclassified')
                          AND LOWER(COALESCE(skill_id, '')) NOT LIKE '%%unclassified%%'
                          AND LOWER(COALESCE(title, '')) NOT LIKE '%%unclassified%%';
                        """,
                        (normalized_target,),
                    )
                    skill_rows = cursor.fetchall() or []
        except Exception:
            return None
        candidates: list[dict[str, Any]] = []
        for skill_id_raw, title_raw, category_raw in skill_rows:
            skill_id = str(skill_id_raw or "").strip()
            state = state_map.get(skill_id)
            if not skill_id or not state or int(state.get("total_events") or 0) <= 0:
                continue
            candidates.append(
                {
                    "skill_id": skill_id,
                    "skill_title": str(title_raw or ""),
                    "skill_category": str(category_raw or ""),
                    "mastery": float(state.get("mastery") or 0.0),
                    "total_events": int(state.get("total_events") or 0),
                    "updated_at": state.get("last_practiced_at"),
                }
            )
        if not candidates:
            return None
        candidates.sort(key=lambda item: (float(item.get("mastery") or 0.0), -int(item.get("total_events") or 0)))
        return candidates[0]
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        s.skill_id,
                        k.title,
                        k.category,
                        s.mastery,
                        s.total_events,
                        s.updated_at
                    FROM bt_3_user_skill_state s
                    JOIN bt_3_skills k ON k.skill_id = s.skill_id
                    WHERE s.user_id = %s
                      AND s.source_lang = COALESCE(%s, 'ru')
                      AND s.target_lang = COALESCE(%s, 'de')
                      AND k.language_code = COALESCE(%s, 'de')
                      AND COALESCE(k.is_active, TRUE) = TRUE
                      AND LOWER(COALESCE(k.skill_id, '')) NOT IN ('other_unclassified', 'en_other_unclassified', 'es_other_unclassified', 'it_other_unclassified')
                      AND LOWER(COALESCE(k.skill_id, '')) NOT LIKE '%%unclassified%%'
                      AND LOWER(COALESCE(k.title, '')) NOT LIKE '%%unclassified%%'
                    ORDER BY s.mastery ASC, s.total_events DESC, s.updated_at DESC
                    LIMIT 1;
                    """,
                    (
                        int(user_id),
                        normalized_source,
                        normalized_target,
                        normalized_target,
                    ),
                )
                row = cursor.fetchone()
    except Exception:
        return None

    if not row:
        return None
    return {
        "skill_id": str(row[0]),
        "skill_title": str(row[1] or ""),
        "skill_category": str(row[2] or ""),
        "mastery": float(row[3] or 0.0),
        "total_events": int(row[4] or 0),
        "updated_at": row[5].isoformat() if row[5] else None,
    }


def get_top_error_topic_for_skill(
    *,
    user_id: int,
    skill_id: str,
    lookback_days: int = 7,
    source_lang: str | None = None,
    target_lang: str | None = None,
) -> dict | None:
    if not skill_id:
        return None
    lookback_days = max(1, int(lookback_days))
    normalized_skill_id = str(skill_id).strip()
    normalized_skill_key = _normalize_skill_error_label(normalized_skill_id)
    if normalized_skill_key in EXCLUDED_UNCLASSIFIED_SKILL_IDS or "unclassified" in normalized_skill_key:
        return None
    normalized_source_lang = str(source_lang or "ru").strip().lower() or "ru"
    normalized_target_lang = str(target_lang or "de").strip().lower() or "de"
    cutoff_date = get_user_progress_reset_date(
        user_id=int(user_id),
        source_lang=normalized_source_lang,
        target_lang=normalized_target_lang,
    )
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        COALESCE(NULLIF(dm.main_category, ''), 'Other mistake') AS main_category,
                        COALESCE(NULLIF(dm.sub_category, ''), 'Unclassified mistake') AS sub_category,
                        SUM(COALESCE(dm.mistake_count, 1)) AS total_mistakes,
                        MAX(m.weight) AS map_weight
                    FROM bt_3_detailed_mistakes dm
                    JOIN bt_3_daily_sentences ds
                      ON ds.id_for_mistake_table = dm.sentence_id
                     AND ds.user_id = dm.user_id
                    JOIN bt_3_error_skill_map m
                      ON m.error_category = COALESCE(NULLIF(dm.main_category, ''), 'Other mistake')
                     AND m.error_subcategory = COALESCE(NULLIF(dm.sub_category, ''), 'Unclassified mistake')
                    WHERE dm.user_id = %s
                      AND m.language_code = %s
                      AND COALESCE(ds.source_lang, 'ru') = COALESCE(%s, 'ru')
                      AND COALESCE(ds.target_lang, 'de') = COALESCE(%s, 'de')
                      AND m.skill_id = %s
                      AND LOWER(COALESCE(NULLIF(dm.sub_category, ''), 'Unclassified mistake')) NOT IN ('unclassified mistake', 'unclassified mistakes')
                      AND COALESCE(dm.last_seen, dm.added_data, NOW()) >= NOW() - (%s::text || ' days')::interval
                      AND (%s::date IS NULL OR COALESCE(dm.last_seen, dm.added_data, NOW())::date >= %s::date)
                    GROUP BY 1, 2
                    ORDER BY total_mistakes DESC, map_weight DESC, main_category ASC, sub_category ASC
                    LIMIT 1;
                    """,
                    (
                        int(user_id),
                        normalized_target_lang,
                        normalized_source_lang,
                        normalized_target_lang,
                        normalized_skill_id,
                        lookback_days,
                        cutoff_date,
                        cutoff_date,
                    ),
                )
                row = cursor.fetchone()
                if row:
                    return {
                        "main_category": str(row[0] or ""),
                        "sub_category": str(row[1] or ""),
                        "mistakes": int(row[2] or 0),
                        "map_weight": float(row[3] or 1.0),
                    }

                cursor.execute(
                    """
                    SELECT error_category, error_subcategory, weight
                    FROM bt_3_error_skill_map
                    WHERE skill_id = %s
                      AND language_code = %s
                      AND LOWER(COALESCE(error_subcategory, '')) NOT IN ('unclassified mistake', 'unclassified mistakes')
                    ORDER BY weight DESC, error_category ASC, error_subcategory ASC
                    LIMIT 1;
                    """,
                    (normalized_skill_id, normalized_target_lang),
                )
                fallback = cursor.fetchone()
    except Exception:
        return None

    if not fallback:
        return None
    return {
        "main_category": str(fallback[0] or ""),
        "sub_category": str(fallback[1] or ""),
        "mistakes": 0,
        "map_weight": float(fallback[2] or 1.0),
    }


def list_default_topics_for_user(
    *,
    user_id: int,
    target_language: str,
    limit: int = 100,
) -> list[dict]:
    normalized_target = str(target_language or "de").strip().lower() or "de"
    safe_limit = max(1, min(int(limit or 100), 300))
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT topic_name, error_category, error_subcategory, skill_id, created_at
                FROM bt_3_default_topics
                WHERE user_id = %s
                  AND target_language = %s
                ORDER BY created_at DESC
                LIMIT %s;
                """,
                (int(user_id), normalized_target, safe_limit),
            )
            rows = cursor.fetchall()
    return [
        {
            "topic_name": str(row[0] or ""),
            "error_category": str(row[1] or "") or None,
            "error_subcategory": str(row[2] or "") or None,
            "skill_id": str(row[3] or "") or None,
            "created_at": row[4].isoformat() if row[4] else None,
        }
        for row in rows
    ]


def add_default_topic_for_user(
    *,
    user_id: int,
    target_language: str,
    topic_name: str,
    error_category: str | None = None,
    error_subcategory: str | None = None,
    skill_id: str | None = None,
) -> dict | None:
    normalized_target = str(target_language or "de").strip().lower() or "de"
    resolved_topic = str(topic_name or "").strip()
    if not resolved_topic:
        return None
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_default_topics (
                    user_id,
                    target_language,
                    topic_name,
                    error_category,
                    error_subcategory,
                    skill_id
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (user_id, target_language, topic_name) DO UPDATE
                SET
                    error_category = COALESCE(EXCLUDED.error_category, bt_3_default_topics.error_category),
                    error_subcategory = COALESCE(EXCLUDED.error_subcategory, bt_3_default_topics.error_subcategory),
                    skill_id = COALESCE(EXCLUDED.skill_id, bt_3_default_topics.skill_id)
                RETURNING topic_name, error_category, error_subcategory, skill_id, created_at;
                """,
                (
                    int(user_id),
                    normalized_target,
                    resolved_topic,
                    str(error_category or "").strip() or None,
                    str(error_subcategory or "").strip() or None,
                    str(skill_id or "").strip() or None,
                ),
            )
            row = cursor.fetchone()
    if not row:
        return None
    return {
        "topic_name": str(row[0] or ""),
        "error_category": str(row[1] or "") or None,
        "error_subcategory": str(row[2] or "") or None,
        "skill_id": str(row[3] or "") or None,
        "created_at": row[4].isoformat() if row[4] else None,
    }


def _normalize_billing_currency(value: str | None) -> str:
    normalized = str(value or "USD").strip().upper()
    if not normalized:
        return "USD"
    return normalized[:12]


def _normalize_billing_status(value: str | None) -> str:
    normalized = str(value or "estimated").strip().lower()
    return normalized if normalized in {"estimated", "final"} else "estimated"


def _normalize_allocation_method(value: str | None) -> str:
    normalized = str(value or "equal").strip().lower()
    return normalized if normalized in {"equal", "weighted"} else "equal"


def _billing_period_bounds(period: str, as_of_date: date | None = None) -> tuple[date, date]:
    normalized = str(period or "month").strip().lower()
    if normalized == "all":
        return date(1970, 1, 1), (as_of_date or date.today())
    if normalized == "day":
        anchor = as_of_date or date.today()
        return anchor, anchor
    if normalized == "half_year":
        normalized = "half-year"
    if normalized not in {"week", "month", "quarter", "half-year", "year"}:
        normalized = "month"
    return _period_bounds(normalized, as_of_date)


def _month_period_start(as_of: date | datetime | None = None, tz: str = TRIAL_POLICY_TZ) -> date:
    if isinstance(as_of, datetime):
        local_dt = _to_aware_datetime(as_of).astimezone(_resolve_timezone(tz))
        base_date = local_dt.date()
    elif isinstance(as_of, date):
        base_date = as_of
    else:
        base_date = datetime.now(timezone.utc).astimezone(_resolve_timezone(tz)).date()
    return base_date.replace(day=1)


def _prorate_fixed_cost_amount(
    *,
    amount: float,
    item_start: date | None,
    item_end: date | None,
    range_start: date,
    range_end: date,
) -> tuple[float, float, int, int]:
    try:
        amount_value = float(amount or 0.0)
    except Exception:
        amount_value = 0.0
    if amount_value <= 0 or not item_start or not item_end or item_end < item_start:
        return 0.0, 0.0, 0, 0
    overlap_start = max(item_start, range_start)
    overlap_end = min(item_end, range_end)
    if overlap_end < overlap_start:
        return 0.0, 0.0, 0, 0
    period_days = (item_end - item_start).days + 1
    overlap_days = (overlap_end - overlap_start).days + 1
    if period_days <= 0 or overlap_days <= 0:
        return 0.0, 0.0, 0, 0
    ratio = overlap_days / period_days
    return amount_value * ratio, ratio, overlap_days, period_days


def _provider_budget_default_base_limit(provider: str) -> int:
    normalized = str(provider or "").strip().lower()
    if normalized == "google_tts":
        return int(GOOGLE_TTS_MONTHLY_BASE_LIMIT_CHARS)
    if normalized == "google_translate":
        return int(GOOGLE_TRANSLATE_MONTHLY_BASE_LIMIT_CHARS)
    if normalized == "deepl_free":
        return int(DEEPL_MONTHLY_BASE_LIMIT_CHARS)
    if normalized == "azure_translator":
        return int(AZURE_TRANSLATOR_MONTHLY_BASE_LIMIT_CHARS)
    if normalized == "perplexity":
        return int(PERPLEXITY_MONTHLY_BASE_LIMIT_REQUESTS)
    if normalized == "cloudflare_r2_class_a":
        return int(CLOUDFLARE_R2_CLASS_A_MONTHLY_BASE_LIMIT_OPS)
    if normalized == "cloudflare_r2_class_b":
        return int(CLOUDFLARE_R2_CLASS_B_MONTHLY_BASE_LIMIT_OPS)
    if normalized == "cloudflare_r2_storage":
        if CLOUDFLARE_R2_STORAGE_MONTHLY_BASE_LIMIT_GB > 0:
            return int(CLOUDFLARE_R2_STORAGE_MONTHLY_BASE_LIMIT_GB * 1024)
        return int(CLOUDFLARE_R2_STORAGE_MONTHLY_BASE_LIMIT_MB)
    if normalized == "stripe":
        return int(STRIPE_MONTHLY_BASE_LIMIT_PAYMENTS)
    return 0


def _row_get(row: Any, index: int, default: Any = None) -> Any:
    if row is None:
        return default
    try:
        return row[index]
    except Exception:
        return default


def _provider_budget_row_to_dict(row) -> dict | None:
    if not row:
        return None
    base_limit = int(_row_get(row, 2, 0) or 0)
    extra_limit = int(_row_get(row, 3, 0) or 0)
    return {
        "provider": str(_row_get(row, 0, "") or ""),
        "period_month": _row_get(row, 1).isoformat() if _row_get(row, 1) else None,
        "base_limit_units": base_limit,
        "extra_limit_units": extra_limit,
        "effective_limit_units": max(0, base_limit + extra_limit),
        "is_blocked": bool(_row_get(row, 4)),
        "block_reason": str(_row_get(row, 5, "") or "").strip() or None,
        "notified_thresholds": _row_get(row, 6, {}) if isinstance(_row_get(row, 6, {}), dict) else {},
        "metadata": _row_get(row, 7, {}) if isinstance(_row_get(row, 7, {}), dict) else {},
        "created_at": _row_get(row, 8).isoformat() if _row_get(row, 8) else None,
        "updated_at": _row_get(row, 9).isoformat() if _row_get(row, 9) else None,
    }


def _billing_event_row_to_dict(row: tuple) -> dict:
    return {
        "id": int(row[0]),
        "idempotency_key": str(row[1] or ""),
        "user_id": int(row[2]) if row[2] is not None else None,
        "source_lang": str(row[3] or "") or None,
        "target_lang": str(row[4] or "") or None,
        "action_type": str(row[5] or ""),
        "provider": str(row[6] or ""),
        "units_type": str(row[7] or ""),
        "units_value": float(row[8] or 0.0),
        "price_snapshot_id": int(row[9]) if row[9] is not None else None,
        "cost_amount": float(row[10] or 0.0),
        "currency": str(row[11] or "USD"),
        "status": str(row[12] or "estimated"),
        "metadata": row[13] if isinstance(row[13], dict) else {},
        "event_time": row[14].isoformat() if row[14] else None,
        "created_at": row[15].isoformat() if row[15] else None,
    }


def upsert_billing_price_snapshot(
    *,
    provider: str,
    sku: str,
    unit: str,
    price_per_unit: float,
    currency: str = "USD",
    valid_from: datetime | None = None,
    source: str = "manual",
    raw_payload: dict | None = None,
) -> dict | None:
    provider_value = str(provider or "").strip().lower()
    sku_value = str(sku or "").strip()
    unit_value = str(unit or "").strip().lower()
    if not provider_value or not sku_value or not unit_value:
        return None
    try:
        price_value = max(0.0, float(price_per_unit or 0.0))
    except Exception:
        price_value = 0.0
    currency_value = _normalize_billing_currency(currency)
    source_value = str(source or "manual").strip() or "manual"
    valid_from_value = valid_from or datetime.now(timezone.utc)
    payload_value = Json(raw_payload) if isinstance(raw_payload, dict) else None
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_billing_price_snapshots (
                    provider,
                    sku,
                    unit,
                    price_per_unit,
                    currency,
                    valid_from,
                    source,
                    raw_payload
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (provider, sku, unit, valid_from) DO UPDATE
                SET
                    price_per_unit = EXCLUDED.price_per_unit,
                    currency = EXCLUDED.currency,
                    source = EXCLUDED.source,
                    raw_payload = COALESCE(EXCLUDED.raw_payload, bt_3_billing_price_snapshots.raw_payload)
                RETURNING
                    id, provider, sku, unit, price_per_unit, currency, valid_from, source, raw_payload, created_at;
                """,
                (
                    provider_value,
                    sku_value,
                    unit_value,
                    price_value,
                    currency_value,
                    valid_from_value,
                    source_value,
                    payload_value,
                ),
            )
            row = cursor.fetchone()
    if not row:
        return None
    return {
        "id": int(row[0]),
        "provider": str(row[1] or ""),
        "sku": str(row[2] or ""),
        "unit": str(row[3] or ""),
        "price_per_unit": float(row[4] or 0.0),
        "currency": str(row[5] or "USD"),
        "valid_from": row[6].isoformat() if row[6] else None,
        "source": str(row[7] or "manual"),
        "raw_payload": row[8] if isinstance(row[8], dict) else None,
        "created_at": row[9].isoformat() if row[9] else None,
    }


def get_effective_billing_price_snapshot(
    *,
    provider: str,
    sku: str,
    unit: str,
    currency: str = "USD",
    as_of: datetime | None = None,
) -> dict | None:
    provider_value = str(provider or "").strip().lower()
    sku_value = str(sku or "").strip()
    unit_value = str(unit or "").strip().lower()
    if not provider_value or not sku_value or not unit_value:
        return None
    currency_value = _normalize_billing_currency(currency)
    as_of_value = as_of or datetime.now(timezone.utc)
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT id, provider, sku, unit, price_per_unit, currency, valid_from, source, raw_payload, created_at
                FROM bt_3_billing_price_snapshots
                WHERE provider = %s
                  AND sku = %s
                  AND unit = %s
                  AND currency = %s
                  AND valid_from <= %s
                ORDER BY valid_from DESC
                LIMIT 1;
                """,
                (
                    provider_value,
                    sku_value,
                    unit_value,
                    currency_value,
                    as_of_value,
                ),
            )
            row = cursor.fetchone()
            if not row:
                cursor.execute(
                    """
                    SELECT id, provider, sku, unit, price_per_unit, currency, valid_from, source, raw_payload, created_at
                    FROM bt_3_billing_price_snapshots
                    WHERE provider = %s
                      AND sku = %s
                      AND unit = %s
                      AND valid_from <= %s
                    ORDER BY valid_from DESC
                    LIMIT 1;
                    """,
                    (
                        provider_value,
                        sku_value,
                        unit_value,
                        as_of_value,
                    ),
                )
                row = cursor.fetchone()
    if not row:
        return None
    return {
        "id": int(row[0]),
        "provider": str(row[1] or ""),
        "sku": str(row[2] or ""),
        "unit": str(row[3] or ""),
        "price_per_unit": float(row[4] or 0.0),
        "currency": str(row[5] or "USD"),
        "valid_from": row[6].isoformat() if row[6] else None,
        "source": str(row[7] or "manual"),
        "raw_payload": row[8] if isinstance(row[8], dict) else None,
        "created_at": row[9].isoformat() if row[9] else None,
    }


def log_billing_event(
    *,
    idempotency_key: str,
    user_id: int | None,
    action_type: str,
    provider: str,
    units_type: str,
    units_value: float,
    source_lang: str | None = None,
    target_lang: str | None = None,
    price_snapshot_id: int | None = None,
    price_provider: str | None = None,
    price_sku: str | None = None,
    price_unit: str | None = None,
    currency: str = "USD",
    status: str = "estimated",
    metadata: dict | None = None,
    event_time: datetime | None = None,
    cost_amount: float | None = None,
) -> dict | None:
    key = str(idempotency_key or "").strip()
    if not key:
        raise ValueError("idempotency_key is required")
    action_value = str(action_type or "").strip()
    provider_value = str(provider or "").strip().lower()
    unit_type_value = str(units_type or "").strip().lower()
    if not action_value or not provider_value or not unit_type_value:
        return None
    try:
        units_value_number = max(0.0, float(units_value or 0.0))
    except Exception:
        units_value_number = 0.0
    event_time_value = event_time or datetime.now(timezone.utc)
    currency_value = _normalize_billing_currency(currency)
    status_value = _normalize_billing_status(status)
    metadata_value = metadata if isinstance(metadata, dict) else {}

    resolved_snapshot_id = int(price_snapshot_id) if price_snapshot_id else None
    resolved_currency = currency_value
    resolved_cost = float(cost_amount) if cost_amount is not None else None

    if resolved_snapshot_id:
        with get_db_connection_context() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT price_per_unit, currency
                    FROM bt_3_billing_price_snapshots
                    WHERE id = %s
                    LIMIT 1;
                    """,
                    (resolved_snapshot_id,),
                )
                row = cursor.fetchone()
        if row:
            resolved_currency = str(row[1] or resolved_currency)
            if resolved_cost is None:
                resolved_cost = units_value_number * float(row[0] or 0.0)
    elif price_provider and price_sku and price_unit:
        snapshot = get_effective_billing_price_snapshot(
            provider=str(price_provider),
            sku=str(price_sku),
            unit=str(price_unit),
            currency=currency_value,
            as_of=event_time_value,
        )
        if snapshot:
            resolved_snapshot_id = int(snapshot["id"])
            resolved_currency = str(snapshot.get("currency") or resolved_currency)
            if resolved_cost is None:
                resolved_cost = units_value_number * float(snapshot.get("price_per_unit") or 0.0)

    if resolved_cost is None:
        resolved_cost = 0.0
    resolved_cost = max(0.0, float(resolved_cost))
    resolved_currency = _normalize_billing_currency(resolved_currency)
    metadata_payload = Json(metadata_value)

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_billing_events (
                    idempotency_key,
                    user_id,
                    source_lang,
                    target_lang,
                    action_type,
                    provider,
                    units_type,
                    units_value,
                    price_snapshot_id,
                    cost_amount,
                    currency,
                    status,
                    metadata,
                    event_time
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (idempotency_key) DO NOTHING
                RETURNING
                    id, idempotency_key, user_id, source_lang, target_lang, action_type, provider,
                    units_type, units_value, price_snapshot_id, cost_amount, currency, status,
                    metadata, event_time, created_at;
                """,
                (
                    key,
                    int(user_id) if user_id is not None else None,
                    str(source_lang or "").strip().lower() or None,
                    str(target_lang or "").strip().lower() or None,
                    action_value,
                    provider_value,
                    unit_type_value,
                    units_value_number,
                    resolved_snapshot_id,
                    resolved_cost,
                    resolved_currency,
                    status_value,
                    metadata_payload,
                    event_time_value,
                ),
            )
            row = cursor.fetchone()
            if not row:
                cursor.execute(
                    """
                    SELECT
                        id, idempotency_key, user_id, source_lang, target_lang, action_type, provider,
                        units_type, units_value, price_snapshot_id, cost_amount, currency, status,
                        metadata, event_time, created_at
                    FROM bt_3_billing_events
                    WHERE idempotency_key = %s
                    LIMIT 1;
                    """,
                    (key,),
                )
                row = cursor.fetchone()
    return _billing_event_row_to_dict(row) if row else None


def upsert_billing_fixed_cost(
    *,
    category: str,
    provider: str,
    amount: float,
    currency: str = "USD",
    period_start: date,
    period_end: date,
    allocation_method_default: str = "equal",
    metadata: dict | None = None,
) -> dict | None:
    category_value = str(category or "").strip().lower()
    provider_value = str(provider or "infra").strip().lower() or "infra"
    if not category_value:
        return None
    try:
        amount_value = max(0.0, float(amount or 0.0))
    except Exception:
        amount_value = 0.0
    currency_value = _normalize_billing_currency(currency)
    allocation_value = _normalize_allocation_method(allocation_method_default)
    metadata_value = metadata if isinstance(metadata, dict) else {}
    metadata_payload = Json(metadata_value)
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_billing_fixed_costs (
                    category,
                    provider,
                    amount,
                    currency,
                    period_start,
                    period_end,
                    allocation_method_default,
                    metadata,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (category, provider, period_start, period_end, currency) DO UPDATE
                SET
                    amount = EXCLUDED.amount,
                    allocation_method_default = EXCLUDED.allocation_method_default,
                    metadata = EXCLUDED.metadata,
                    updated_at = NOW()
                RETURNING id, category, provider, amount, currency, period_start, period_end,
                          allocation_method_default, metadata, created_at, updated_at;
                """,
                (
                    category_value,
                    provider_value,
                    amount_value,
                    currency_value,
                    period_start,
                    period_end,
                    allocation_value,
                    metadata_payload,
                ),
            )
            row = cursor.fetchone()
    if not row:
        return None
    return {
        "id": int(row[0]),
        "category": str(row[1] or ""),
        "provider": str(row[2] or ""),
        "amount": float(row[3] or 0.0),
        "currency": str(row[4] or "USD"),
        "period_start": row[5].isoformat() if row[5] else None,
        "period_end": row[6].isoformat() if row[6] else None,
        "allocation_method_default": str(row[7] or "equal"),
        "metadata": row[8] if isinstance(row[8], dict) else {},
        "created_at": row[9].isoformat() if row[9] else None,
        "updated_at": row[10].isoformat() if row[10] else None,
    }


def get_or_create_provider_budget_control(
    *,
    provider: str,
    period_month: date | datetime | None = None,
    tz: str = TRIAL_POLICY_TZ,
) -> dict | None:
    provider_value = str(provider or "").strip().lower()
    if not provider_value:
        return None
    period_value = _month_period_start(period_month, tz=tz)
    base_limit_units = _provider_budget_default_base_limit(provider_value)
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_provider_budget_controls (
                    provider,
                    period_month,
                    base_limit_units,
                    extra_limit_units,
                    is_blocked,
                    block_reason,
                    notified_thresholds,
                    metadata,
                    updated_at
                )
                VALUES (%s, %s, %s, 0, FALSE, NULL, '{}'::jsonb, '{}'::jsonb, NOW())
                ON CONFLICT (provider, period_month) DO UPDATE
                SET
                    base_limit_units = GREATEST(bt_3_provider_budget_controls.base_limit_units, EXCLUDED.base_limit_units),
                    updated_at = NOW()
                RETURNING
                    provider,
                    period_month,
                    base_limit_units,
                    extra_limit_units,
                    is_blocked,
                    block_reason,
                    notified_thresholds,
                    metadata,
                    created_at,
                    updated_at;
                """,
                (provider_value, period_value, int(base_limit_units)),
            )
            row = cursor.fetchone()
    return _provider_budget_row_to_dict(row)


def get_provider_budget_month_usage(
    *,
    provider: str,
    units_type: str = "chars",
    period_month: date | datetime | None = None,
    tz: str = TRIAL_POLICY_TZ,
) -> float:
    provider_value = str(provider or "").strip().lower()
    units_type_value = str(units_type or "").strip().lower() or "chars"
    if not provider_value:
        return 0.0
    period_start = _month_period_start(period_month, tz=tz)
    if period_start.month == 12:
        period_end = date(period_start.year + 1, 1, 1)
    else:
        period_end = date(period_start.year, period_start.month + 1, 1)
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT COALESCE(SUM(units_value), 0)
                FROM bt_3_billing_events
                WHERE provider = %s
                  AND units_type = %s
                  AND event_time >= %s
                  AND event_time < %s
                  AND COALESCE(metadata->>'cached', 'false') <> 'true';
                """,
                (
                    provider_value,
                    units_type_value,
                    datetime.combine(period_start, dt_time.min, tzinfo=timezone.utc),
                    datetime.combine(period_end, dt_time.min, tzinfo=timezone.utc),
                ),
            )
            row = cursor.fetchone()
    return float((row or [0])[0] or 0.0)


def get_provider_monthly_budget_status(
    *,
    provider: str,
    units_type: str,
    unit_label: str | None = None,
    period_month: date | datetime | None = None,
    tz: str = TRIAL_POLICY_TZ,
) -> dict | None:
    provider_value = str(provider or "").strip().lower()
    units_type_value = str(units_type or "").strip().lower()
    if not provider_value or not units_type_value:
        return None
    control = get_or_create_provider_budget_control(
        provider=provider_value,
        period_month=period_month,
        tz=tz,
    )
    if not control:
        return None
    used_units = get_provider_budget_month_usage(
        provider=provider_value,
        units_type=units_type_value,
        period_month=period_month,
        tz=tz,
    )
    effective_limit = float(control.get("effective_limit_units") or 0.0)
    usage_ratio = (used_units / effective_limit) if effective_limit > 0 else 0.0
    result = dict(control)
    result["used_units"] = float(round(used_units, 3))
    result["remaining_units"] = max(0.0, float(round(effective_limit - used_units, 3)))
    result["usage_ratio"] = max(0.0, usage_ratio)
    result["unit"] = str(unit_label or units_type_value).strip() or units_type_value
    result["units_type"] = units_type_value
    return result


def get_google_tts_monthly_budget_status(
    *,
    period_month: date | datetime | None = None,
    tz: str = TRIAL_POLICY_TZ,
) -> dict | None:
    return get_provider_monthly_budget_status(
        provider="google_tts",
        units_type="chars",
        unit_label="chars",
        period_month=period_month,
        tz=tz,
    )


def get_google_translate_monthly_budget_status(
    *,
    period_month: date | datetime | None = None,
    tz: str = TRIAL_POLICY_TZ,
) -> dict | None:
    return get_provider_monthly_budget_status(
        provider="google_translate",
        units_type="chars",
        unit_label="chars",
        period_month=period_month,
        tz=tz,
    )


def set_provider_budget_extra_limit(
    *,
    provider: str,
    extra_limit_units: int,
    period_month: date | datetime | None = None,
    tz: str = TRIAL_POLICY_TZ,
    metadata: dict | None = None,
) -> dict | None:
    provider_value = str(provider or "").strip().lower()
    if not provider_value:
        return None
    base = get_or_create_provider_budget_control(provider=provider_value, period_month=period_month, tz=tz)
    if not base:
        return None
    extra_value = max(0, int(extra_limit_units or 0))
    metadata_value = metadata if isinstance(metadata, dict) else {}
    period_value = _month_period_start(period_month, tz=tz)
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE bt_3_provider_budget_controls
                SET
                    extra_limit_units = %s,
                    metadata = COALESCE(metadata, '{}'::jsonb) || %s::jsonb,
                    updated_at = NOW()
                WHERE provider = %s
                  AND period_month = %s
                RETURNING
                    provider,
                    period_month,
                    base_limit_units,
                    extra_limit_units,
                    is_blocked,
                    block_reason,
                    notified_thresholds,
                    metadata,
                    created_at,
                    updated_at;
                """,
                (extra_value, Json(metadata_value), provider_value, period_value),
            )
            row = cursor.fetchone()
    return _provider_budget_row_to_dict(row)


def set_provider_budget_block_state(
    *,
    provider: str,
    is_blocked: bool,
    block_reason: str | None = None,
    period_month: date | datetime | None = None,
    tz: str = TRIAL_POLICY_TZ,
) -> dict | None:
    provider_value = str(provider or "").strip().lower()
    if not provider_value:
        return None
    base = get_or_create_provider_budget_control(provider=provider_value, period_month=period_month, tz=tz)
    if not base:
        return None
    period_value = _month_period_start(period_month, tz=tz)
    reason_value = str(block_reason or "").strip() or None
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE bt_3_provider_budget_controls
                SET
                    is_blocked = %s,
                    block_reason = %s,
                    updated_at = NOW()
                WHERE provider = %s
                  AND period_month = %s
                RETURNING
                    provider,
                    period_month,
                    base_limit_units,
                    extra_limit_units,
                    is_blocked,
                    block_reason,
                    notified_thresholds,
                    metadata,
                    created_at,
                    updated_at;
                """,
                (bool(is_blocked), reason_value, provider_value, period_value),
            )
            row = cursor.fetchone()
    return _provider_budget_row_to_dict(row)


def mark_provider_budget_threshold_notified(
    *,
    provider: str,
    threshold_percent: int | float,
    period_month: date | datetime | None = None,
    tz: str = TRIAL_POLICY_TZ,
    metadata: dict | None = None,
) -> dict | None:
    provider_value = str(provider or "").strip().lower()
    if not provider_value:
        return None
    base = get_or_create_provider_budget_control(provider=provider_value, period_month=period_month, tz=tz)
    if not base:
        return None
    try:
        threshold_value = int(round(float(threshold_percent)))
    except Exception:
        threshold_value = 0
    if threshold_value <= 0:
        return base
    period_value = _month_period_start(period_month, tz=tz)
    metadata_value = metadata if isinstance(metadata, dict) else {}
    threshold_key = str(threshold_value)
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE bt_3_provider_budget_controls
                SET
                    notified_thresholds = COALESCE(notified_thresholds, '{}'::jsonb) || jsonb_build_object(%s, NOW()::text),
                    metadata = COALESCE(metadata, '{}'::jsonb) || %s::jsonb,
                    updated_at = NOW()
                WHERE provider = %s
                  AND period_month = %s
                RETURNING
                    provider,
                    period_month,
                    base_limit_units,
                    extra_limit_units,
                    is_blocked,
                    block_reason,
                    notified_thresholds,
                    metadata,
                    created_at,
                    updated_at;
                """,
                (threshold_key, Json(metadata_value), provider_value, period_value),
            )
            row = cursor.fetchone()
    return _provider_budget_row_to_dict(row)


def get_user_billing_summary(
    *,
    user_id: int,
    period: str = "month",
    allocation_method: str = "equal",
    source_lang: str | None = None,
    target_lang: str | None = None,
    currency: str = "USD",
    as_of_date: date | None = None,
) -> dict:
    currency_value = _normalize_billing_currency(currency)
    period_start, period_end = _billing_period_bounds(period, as_of_date)
    allocation = _normalize_allocation_method(allocation_method)
    source_value = str(source_lang or "").strip().lower() or None
    target_value = str(target_lang or "").strip().lower() or None

    language_sql = ""
    language_params: list = []
    if source_value:
        language_sql += " AND COALESCE(source_lang, '') = %s"
        language_params.append(source_value)
    if target_value:
        language_sql += " AND COALESCE(target_lang, '') = %s"
        language_params.append(target_value)

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT
                    COALESCE(SUM(CASE WHEN provider = 'youtube_api' THEN 0 ELSE cost_amount END), 0) AS total_cost,
                    COALESCE(SUM(units_value), 0) AS total_units,
                    COUNT(*) AS events_count,
                    COALESCE(SUM(CASE WHEN status = 'final' AND provider <> 'youtube_api' THEN cost_amount ELSE 0 END), 0) AS final_cost,
                    COALESCE(SUM(CASE WHEN status = 'estimated' AND provider <> 'youtube_api' THEN cost_amount ELSE 0 END), 0) AS estimated_cost,
                    COALESCE(SUM(CASE WHEN COALESCE(metadata->>'pricing_state', '') = 'missing_snapshot' THEN units_value ELSE 0 END), 0) AS unpriced_units,
                    COALESCE(SUM(CASE WHEN COALESCE(metadata->>'pricing_state', '') = 'missing_snapshot' THEN 1 ELSE 0 END), 0) AS unpriced_events
                FROM bt_3_billing_events
                WHERE user_id = %s
                  AND currency = %s
                  AND (event_time AT TIME ZONE 'UTC')::date BETWEEN %s AND %s
                  {language_sql};
                """,
                [int(user_id), currency_value, period_start, period_end, *language_params],
            )
            user_totals_row = cursor.fetchone() or (0, 0, 0, 0, 0, 0, 0)
            user_variable_cost = float(user_totals_row[0] or 0.0)
            user_units = float(user_totals_row[1] or 0.0)
            user_events = int(user_totals_row[2] or 0)
            user_final_cost = float(user_totals_row[3] or 0.0)
            user_estimated_cost = float(user_totals_row[4] or 0.0)
            user_unpriced_units = float(user_totals_row[5] or 0.0)
            user_unpriced_events = int(user_totals_row[6] or 0)

            cursor.execute(
                """
                SELECT
                    COALESCE(SUM(amount), 0) AS fixed_cost_total
                FROM bt_3_billing_fixed_costs
                WHERE currency = %s
                  AND period_end >= %s
                  AND period_start <= %s;
                """,
                (currency_value, period_start, period_end),
            )
            fixed_total = float((cursor.fetchone() or [0])[0] or 0.0)

            cursor.execute(
                """
                SELECT COUNT(DISTINCT user_id)
                FROM bt_3_billing_events
                WHERE user_id IS NOT NULL
                  AND currency = %s
                  AND (event_time AT TIME ZONE 'UTC')::date BETWEEN %s AND %s;
                """,
                (currency_value, period_start, period_end),
            )
            active_users = int((cursor.fetchone() or [0])[0] or 0)

            cursor.execute(
                """
                SELECT COALESCE(SUM(units_value), 0)
                FROM bt_3_billing_events
                WHERE user_id IS NOT NULL
                  AND currency = %s
                  AND (event_time AT TIME ZONE 'UTC')::date BETWEEN %s AND %s;
                """,
                (currency_value, period_start, period_end),
            )
            all_units = float((cursor.fetchone() or [0])[0] or 0.0)

            user_is_active = user_events > 0
            fixed_allocated = 0.0
            if allocation == "weighted":
                if all_units > 0 and user_units > 0:
                    fixed_allocated = fixed_total * (user_units / all_units)
                elif active_users > 0 and user_is_active:
                    fixed_allocated = fixed_total / active_users
            else:
                if active_users > 0 and user_is_active:
                    fixed_allocated = fixed_total / active_users

            cursor.execute(
                f"""
                SELECT provider, SUM(CASE WHEN provider = 'youtube_api' THEN 0 ELSE cost_amount END) AS cost_total, SUM(units_value) AS units_total, COUNT(*) AS events_count
                FROM bt_3_billing_events
                WHERE user_id = %s
                  AND currency = %s
                  AND (event_time AT TIME ZONE 'UTC')::date BETWEEN %s AND %s
                  {language_sql}
                GROUP BY provider
                ORDER BY cost_total DESC, units_total DESC
                LIMIT 12;
                """,
                [int(user_id), currency_value, period_start, period_end, *language_params],
            )
            providers = [
                {
                    "provider": str(row[0] or ""),
                    "cost": float(row[1] or 0.0),
                    "units": float(row[2] or 0.0),
                    "events": int(row[3] or 0),
                }
                for row in (cursor.fetchall() or [])
            ]

            cursor.execute(
                f"""
                SELECT action_type, SUM(CASE WHEN provider = 'youtube_api' THEN 0 ELSE cost_amount END) AS cost_total, SUM(units_value) AS units_total, COUNT(*) AS events_count
                FROM bt_3_billing_events
                WHERE user_id = %s
                  AND currency = %s
                  AND (event_time AT TIME ZONE 'UTC')::date BETWEEN %s AND %s
                  {language_sql}
                GROUP BY action_type
                ORDER BY cost_total DESC, units_total DESC
                LIMIT 20;
                """,
                [int(user_id), currency_value, period_start, period_end, *language_params],
            )
            actions = [
                {
                    "action_type": str(row[0] or ""),
                    "cost": float(row[1] or 0.0),
                    "units": float(row[2] or 0.0),
                    "events": int(row[3] or 0),
                }
                for row in (cursor.fetchall() or [])
            ]

            cursor.execute(
                """
                SELECT category, provider, amount, period_start, period_end, allocation_method_default
                FROM bt_3_billing_fixed_costs
                WHERE currency = %s
                  AND period_end >= %s
                  AND period_start <= %s
                ORDER BY period_start DESC, category ASC;
                """,
                (currency_value, period_start, period_end),
            )
            fixed_items = [
                {
                    "category": str(row[0] or ""),
                    "provider": str(row[1] or ""),
                    "amount": float(row[2] or 0.0),
                    "period_start": row[3].isoformat() if row[3] else None,
                    "period_end": row[4].isoformat() if row[4] else None,
                    "allocation_method_default": str(row[5] or "equal"),
                }
                for row in (cursor.fetchall() or [])
            ]

            cursor.execute(
                """
                SELECT COALESCE(SUM(CASE WHEN provider = 'youtube_api' THEN 0 ELSE cost_amount END), 0), COUNT(*)
                FROM bt_3_billing_events
                WHERE user_id IS NOT NULL
                  AND currency = %s
                  AND (event_time AT TIME ZONE 'UTC')::date BETWEEN %s AND %s;
                """,
                (currency_value, period_start, period_end),
            )
            all_variable_cost_row = cursor.fetchone() or (0, 0)
            all_variable_cost = float(all_variable_cost_row[0] or 0.0)
            all_events = int(all_variable_cost_row[1] or 0)

    total_cost = user_variable_cost + fixed_allocated
    avg_per_event = total_cost / user_events if user_events > 0 else 0.0
    avg_per_active_user = (all_variable_cost + fixed_total) / active_users if active_users > 0 else 0.0
    return {
        "period": str(period or "month"),
        "range": {
            "start_date": period_start.isoformat(),
            "end_date": period_end.isoformat(),
        },
        "allocation_method": allocation,
        "currency": currency_value,
        "totals": {
            "user_variable_cost": round(user_variable_cost, 6),
            "user_fixed_allocated_cost": round(fixed_allocated, 6),
            "user_total_cost": round(total_cost, 6),
            "user_final_cost": round(user_final_cost, 6),
            "user_estimated_cost": round(user_estimated_cost, 6),
            "user_unpriced_events": user_unpriced_events,
            "user_unpriced_units": round(user_unpriced_units, 6),
            "user_events_count": user_events,
            "user_units_total": round(user_units, 6),
            "avg_cost_per_user_event": round(avg_per_event, 6),
            "period_fixed_cost_total": round(fixed_total, 6),
            "period_variable_cost_total": round(all_variable_cost, 6),
            "period_events_total": all_events,
            "period_active_users": active_users,
            "period_avg_cost_per_active_user": round(avg_per_active_user, 6),
        },
        "breakdown": {
            "by_provider": providers,
            "by_action_type": actions,
            "fixed_costs": fixed_items,
        },
    }


def _get_billing_all_time_bounds(currency: str = "USD") -> tuple[date, date]:
    currency_value = _normalize_billing_currency(currency)
    today = date.today()
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                WITH dates AS (
                    SELECT
                        MIN((event_time AT TIME ZONE 'UTC')::date) AS min_date,
                        MAX((event_time AT TIME ZONE 'UTC')::date) AS max_date
                    FROM bt_3_billing_events
                    WHERE currency = %s
                    UNION ALL
                    SELECT
                        MIN(period_start) AS min_date,
                        MAX(period_end) AS max_date
                    FROM bt_3_billing_fixed_costs
                    WHERE currency = %s
                )
                SELECT MIN(min_date), MAX(max_date)
                FROM dates;
                """,
                (currency_value, currency_value),
            )
            row = cursor.fetchone() or (None, None)
    return (_row_get(row, 0, today) or today, _row_get(row, 1, today) or today)


def _get_product_active_users_count(
    cursor,
    *,
    period_start: date,
    period_end: date,
    provider: str | None = None,
) -> int:
    provider_value = str(provider or "").strip().lower() or None

    translation_providers = {
        "google_translate",
        "deepl_free",
        "azure_translator",
        "mymemory",
        "libretranslate",
        "argos_offline",
    }
    voice_providers = {"livekit", "openai", "agent_tts"}
    reader_providers = {
        "google_tts",
        "cloudflare_r2_class_a",
        "cloudflare_r2_class_b",
        "cloudflare_r2_storage",
        "offline_tts",
        "app_internal",
    }

    def _count_from_union(union_sql: str, params: tuple | list) -> int:
        cursor.execute(
            f"""
            SELECT COUNT(DISTINCT user_id)
            FROM (
                {union_sql}
            ) AS product_users
            WHERE user_id IS NOT NULL;
            """,
            params,
        )
        return int(_row_get(cursor.fetchone(), 0, 0) or 0)

    translation_activity_union = """
        SELECT user_id
        FROM bt_3_translations
        WHERE user_id IS NOT NULL
          AND timestamp::date BETWEEN %s AND %s
        UNION
        SELECT user_id
        FROM bt_3_webapp_checks
        WHERE user_id IS NOT NULL
          AND created_at::date BETWEEN %s AND %s
        UNION
        SELECT user_id
        FROM bt_3_translation_check_sessions
        WHERE user_id IS NOT NULL
          AND created_at::date BETWEEN %s AND %s
    """

    voice_activity_union = """
        SELECT user_id
        FROM bt_3_agent_voice_sessions
        WHERE user_id IS NOT NULL
          AND COALESCE(ended_at, started_at, created_at)::date BETWEEN %s AND %s
    """

    reader_activity_union = """
        SELECT user_id
        FROM bt_3_reader_sessions
        WHERE user_id IS NOT NULL
          AND COALESCE(ended_at, started_at, created_at)::date BETWEEN %s AND %s
        UNION
        SELECT user_id
        FROM bt_3_reader_library
        WHERE user_id IS NOT NULL
          AND COALESCE(updated_at, created_at)::date BETWEEN %s AND %s
    """

    if provider_value in translation_providers:
        return _count_from_union(
            translation_activity_union,
            (period_start, period_end, period_start, period_end, period_start, period_end),
        )
    if provider_value in voice_providers:
        return _count_from_union(
            voice_activity_union,
            (period_start, period_end),
        )
    if provider_value in reader_providers:
        return _count_from_union(
            reader_activity_union,
            (period_start, period_end, period_start, period_end),
        )

    return _count_from_union(
        f"""
        {translation_activity_union}
        UNION
        {voice_activity_union}
        UNION
        {reader_activity_union}
        """,
        (
            period_start, period_end,
            period_start, period_end,
            period_start, period_end,
            period_start, period_end,
            period_start, period_end,
            period_start, period_end,
        ),
    )


def get_provider_active_users_count(
    *,
    period_start: date,
    period_end: date,
    provider: str | None = None,
) -> int:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            return _get_product_active_users_count(
                cursor,
                period_start=period_start,
                period_end=period_end,
                provider=provider,
            )


def get_global_billing_summary(
    *,
    period: str = "month",
    provider: str | None = None,
    currency: str = "USD",
    as_of_date: date | None = None,
) -> dict:
    currency_value = _normalize_billing_currency(currency)
    normalized_period = str(period or "month").strip().lower() or "month"
    if normalized_period == "half_year":
        normalized_period = "half-year"
    if normalized_period == "all":
        period_start, period_end = _get_billing_all_time_bounds(currency_value)
    else:
        period_start, period_end = _billing_period_bounds(normalized_period, as_of_date)

    provider_value = str(provider or "").strip().lower() or None
    event_provider_sql = " AND provider = %s" if provider_value else ""
    fixed_provider_sql = " AND COALESCE(provider, '') = %s" if provider_value else ""

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            totals_params = [currency_value, period_start, period_end]
            if provider_value:
                totals_params.append(provider_value)
            cursor.execute(
                f"""
                SELECT
                    COALESCE(SUM(CASE WHEN provider = 'youtube_api' THEN 0 ELSE cost_amount END), 0) AS total_cost,
                    COALESCE(SUM(units_value), 0) AS total_units,
                    COUNT(*) AS events_count,
                    COALESCE(SUM(CASE WHEN status = 'final' AND provider <> 'youtube_api' THEN cost_amount ELSE 0 END), 0) AS final_cost,
                    COALESCE(SUM(CASE WHEN status = 'estimated' AND provider <> 'youtube_api' THEN cost_amount ELSE 0 END), 0) AS estimated_cost,
                    COALESCE(SUM(CASE WHEN COALESCE(metadata->>'pricing_state', '') = 'missing_snapshot' THEN units_value ELSE 0 END), 0) AS unpriced_units,
                    COALESCE(SUM(CASE WHEN COALESCE(metadata->>'pricing_state', '') = 'missing_snapshot' THEN 1 ELSE 0 END), 0) AS unpriced_events
                FROM bt_3_billing_events
                WHERE currency = %s
                  AND (event_time AT TIME ZONE 'UTC')::date BETWEEN %s AND %s
                  {event_provider_sql};
                """,
                totals_params,
            )
            totals_row = cursor.fetchone() or (0, 0, 0, 0, 0, 0, 0)
            variable_cost_total = float(_row_get(totals_row, 0, 0.0) or 0.0)
            units_total = float(_row_get(totals_row, 1, 0.0) or 0.0)
            events_count = int(_row_get(totals_row, 2, 0) or 0)
            final_cost = float(_row_get(totals_row, 3, 0.0) or 0.0)
            estimated_cost = float(_row_get(totals_row, 4, 0.0) or 0.0)
            unpriced_units = float(_row_get(totals_row, 5, 0.0) or 0.0)
            unpriced_events = int(_row_get(totals_row, 6, 0) or 0)

            fixed_items_params = [currency_value, period_start, period_end]
            if provider_value:
                fixed_items_params.append(provider_value)
            cursor.execute(
                f"""
                SELECT category, provider, amount, period_start, period_end, allocation_method_default
                FROM bt_3_billing_fixed_costs
                WHERE currency = %s
                  AND period_end >= %s
                  AND period_start <= %s
                  {fixed_provider_sql}
                ORDER BY period_start DESC, category ASC;
                """,
                fixed_items_params,
            )
            fixed_rows = cursor.fetchall() or []
            fixed_items: list[dict] = []
            fixed_by_provider: dict[str, float] = {}
            fixed_cost_total = 0.0
            for row in fixed_rows:
                row_amount = float(_row_get(row, 2, 0.0) or 0.0)
                row_period_start = _row_get(row, 3)
                row_period_end = _row_get(row, 4)
                prorated_amount, proration_ratio, overlap_days, period_days = _prorate_fixed_cost_amount(
                    amount=row_amount,
                    item_start=row_period_start,
                    item_end=row_period_end,
                    range_start=period_start,
                    range_end=period_end,
                )
                provider_key = str(_row_get(row, 1, "") or "")
                fixed_cost_total += prorated_amount
                fixed_by_provider[provider_key] = fixed_by_provider.get(provider_key, 0.0) + prorated_amount
                fixed_items.append(
                    {
                        "category": str(_row_get(row, 0, "") or ""),
                        "provider": provider_key,
                        "amount": round(prorated_amount, 6),
                        "full_amount": round(row_amount, 6),
                        "period_start": row_period_start.isoformat() if row_period_start else None,
                        "period_end": row_period_end.isoformat() if row_period_end else None,
                        "allocation_method_default": str(_row_get(row, 5, "equal") or "equal"),
                        "proration_ratio": round(proration_ratio, 6),
                        "overlap_days": overlap_days,
                        "period_days": period_days,
                    }
                )

            active_users = _get_product_active_users_count(
                cursor,
                period_start=period_start,
                period_end=period_end,
                provider=provider_value,
            )

            provider_rows: list[dict] = []
            provider_units_map: dict[str, list[dict]] = {}
            if not provider_value:
                cursor.execute(
                    """
                    SELECT provider, SUM(CASE WHEN provider = 'youtube_api' THEN 0 ELSE cost_amount END) AS cost_total, SUM(units_value) AS units_total, COUNT(*) AS events_count
                    FROM bt_3_billing_events
                    WHERE currency = %s
                      AND (event_time AT TIME ZONE 'UTC')::date BETWEEN %s AND %s
                    GROUP BY provider
                    ORDER BY cost_total DESC, units_total DESC;
                    """,
                    (currency_value, period_start, period_end),
                )
                provider_rows = [
                    {
                        "provider": str(_row_get(row, 0, "") or ""),
                        "variable_cost": float(_row_get(row, 1, 0.0) or 0.0),
                        "units": float(_row_get(row, 2, 0.0) or 0.0),
                        "events": int(_row_get(row, 3, 0) or 0),
                    }
                    for row in (cursor.fetchall() or [])
                ]
                cursor.execute(
                    """
                    SELECT provider, units_type, SUM(units_value) AS units_total
                    FROM bt_3_billing_events
                    WHERE currency = %s
                      AND (event_time AT TIME ZONE 'UTC')::date BETWEEN %s AND %s
                    GROUP BY provider, units_type
                    ORDER BY provider ASC, units_total DESC;
                    """,
                    (currency_value, period_start, period_end),
                )
                for row in (cursor.fetchall() or []):
                    provider_key = str(_row_get(row, 0, "") or "")
                    provider_units_map.setdefault(provider_key, []).append({
                        "units_type": str(_row_get(row, 1, "") or ""),
                        "units": float(_row_get(row, 2, 0.0) or 0.0),
                    })
                provider_map = {str(item.get("provider") or ""): item for item in provider_rows}
                for provider_key, fixed_total in fixed_by_provider.items():
                    existing = provider_map.get(provider_key)
                    if existing:
                        existing["fixed_cost"] = fixed_total
                    else:
                        provider_map[provider_key] = {
                            "provider": provider_key,
                            "variable_cost": 0.0,
                            "fixed_cost": fixed_total,
                            "units": 0.0,
                            "events": 0,
                        }
                provider_rows = list(provider_map.values())
                for item in provider_rows:
                    variable_cost = float(item.get("variable_cost") or 0.0)
                    fixed_cost = float(item.get("fixed_cost") or 0.0)
                    provider_key = str(item.get("provider") or "")
                    item["fixed_cost"] = round(fixed_cost, 6)
                    item["total_cost"] = round(variable_cost + fixed_cost, 6)
                    item["units_by_type"] = provider_units_map.get(provider_key, [])
                provider_rows.sort(
                    key=lambda item: (
                        -float(item.get("total_cost") or 0.0),
                        -float(item.get("units") or 0.0),
                        str(item.get("provider") or ""),
                    )
                )
            else:
                cursor.execute(
                    """
                    SELECT units_type, SUM(units_value) AS units_total
                    FROM bt_3_billing_events
                    WHERE currency = %s
                      AND (event_time AT TIME ZONE 'UTC')::date BETWEEN %s AND %s
                      AND provider = %s
                    GROUP BY units_type
                    ORDER BY units_total DESC;
                    """,
                    (currency_value, period_start, period_end, provider_value),
                )
                provider_units_map[provider_value] = [
                    {
                        "units_type": str(_row_get(row, 0, "") or ""),
                        "units": float(_row_get(row, 1, 0.0) or 0.0),
                    }
                    for row in (cursor.fetchall() or [])
                ]
                provider_rows = [{
                    "provider": provider_value,
                    "variable_cost": round(variable_cost_total, 6),
                    "fixed_cost": round(fixed_cost_total, 6),
                    "total_cost": round(variable_cost_total + fixed_cost_total, 6),
                    "units": round(units_total, 6),
                    "events": events_count,
                    "units_by_type": provider_units_map.get(provider_value, []),
                }]

            actions_params = [currency_value, period_start, period_end]
            if provider_value:
                actions_params.append(provider_value)
            cursor.execute(
                f"""
                SELECT action_type, SUM(CASE WHEN provider = 'youtube_api' THEN 0 ELSE cost_amount END) AS cost_total, SUM(units_value) AS units_total, COUNT(*) AS events_count
                FROM bt_3_billing_events
                WHERE currency = %s
                  AND (event_time AT TIME ZONE 'UTC')::date BETWEEN %s AND %s
                  {event_provider_sql}
                GROUP BY action_type
                ORDER BY cost_total DESC, units_total DESC
                LIMIT 24;
                """,
                actions_params,
            )
            actions = [
                {
                    "action_type": str(_row_get(row, 0, "") or ""),
                    "cost": float(_row_get(row, 1, 0.0) or 0.0),
                    "units": float(_row_get(row, 2, 0.0) or 0.0),
                    "events": int(_row_get(row, 3, 0) or 0),
                }
                for row in (cursor.fetchall() or [])
            ]

            models_params = [currency_value, period_start, period_end]
            if provider_value:
                models_params.append(provider_value)
            cursor.execute(
                f"""
                SELECT
                    COALESCE(metadata->>'model', '') AS model,
                    SUM(CASE WHEN provider = 'youtube_api' THEN 0 ELSE cost_amount END) AS cost_total,
                    COUNT(*) AS events_count,
                    COALESCE(SUM(CASE WHEN units_type = 'tokens_in' THEN units_value ELSE 0 END), 0) AS tokens_in_total,
                    COALESCE(SUM(CASE WHEN units_type = 'tokens_out' THEN units_value ELSE 0 END), 0) AS tokens_out_total
                FROM bt_3_billing_events
                WHERE currency = %s
                  AND (event_time AT TIME ZONE 'UTC')::date BETWEEN %s AND %s
                  AND COALESCE(metadata->>'model', '') <> ''
                  {event_provider_sql}
                GROUP BY COALESCE(metadata->>'model', '')
                ORDER BY cost_total DESC, events_count DESC
                LIMIT 16;
                """,
                models_params,
            )
            models = [
                {
                    "model": str(_row_get(row, 0, "") or ""),
                    "cost": float(_row_get(row, 1, 0.0) or 0.0),
                    "events": int(_row_get(row, 2, 0) or 0),
                    "tokens_in": float(_row_get(row, 3, 0.0) or 0.0),
                    "tokens_out": float(_row_get(row, 4, 0.0) or 0.0),
                }
                for row in (cursor.fetchall() or [])
            ]

            units_params = [currency_value, period_start, period_end]
            if provider_value:
                units_params.append(provider_value)
            cursor.execute(
                f"""
                SELECT units_type, SUM(units_value) AS units_total, SUM(CASE WHEN provider = 'youtube_api' THEN 0 ELSE cost_amount END) AS cost_total, COUNT(*) AS events_count
                FROM bt_3_billing_events
                WHERE currency = %s
                  AND (event_time AT TIME ZONE 'UTC')::date BETWEEN %s AND %s
                  {event_provider_sql}
                GROUP BY units_type
                ORDER BY cost_total DESC, units_total DESC
                LIMIT 20;
                """,
                units_params,
            )
            units_by_type = [
                {
                    "units_type": str(_row_get(row, 0, "") or ""),
                    "units": float(_row_get(row, 1, 0.0) or 0.0),
                    "cost": float(_row_get(row, 2, 0.0) or 0.0),
                    "events": int(_row_get(row, 3, 0) or 0),
                }
                for row in (cursor.fetchall() or [])
            ]

            cursor.execute(
                """
                SELECT provider
                FROM (
                    SELECT DISTINCT provider
                    FROM bt_3_billing_events
                    WHERE COALESCE(provider, '') <> ''
                    UNION
                    SELECT DISTINCT provider
                    FROM bt_3_billing_fixed_costs
                    WHERE COALESCE(provider, '') <> ''
                ) catalog
                ORDER BY provider ASC;
                """
            )
            provider_catalog = [
                str(_row_get(row, 0, "") or "")
                for row in (cursor.fetchall() or [])
                if str(_row_get(row, 0, "") or "").strip()
            ]

    total_cost = variable_cost_total + fixed_cost_total
    avg_cost_per_event = total_cost / events_count if events_count > 0 else 0.0
    avg_variable_cost_per_active_user = variable_cost_total / active_users if active_users > 0 else 0.0
    avg_fixed_cost_per_active_user = fixed_cost_total / active_users if active_users > 0 else 0.0
    avg_cost_per_active_user = total_cost / active_users if active_users > 0 else 0.0
    avg_events_per_active_user = events_count / active_users if active_users > 0 else 0.0
    return {
        "scope": "global",
        "period": normalized_period,
        "range": {
            "start_date": period_start.isoformat(),
            "end_date": period_end.isoformat(),
        },
        "currency": currency_value,
        "provider_filter": provider_value or "all",
        "providers": provider_catalog,
        "totals": {
            "variable_cost_total": round(variable_cost_total, 6),
            "fixed_cost_total": round(fixed_cost_total, 6),
            "total_cost": round(total_cost, 6),
            "final_cost": round(final_cost, 6),
            "estimated_cost": round(estimated_cost, 6),
            "unpriced_events": unpriced_events,
            "unpriced_units": round(unpriced_units, 6),
            "events_count": events_count,
            "units_total": round(units_total, 6),
            "active_users": active_users,
            "avg_cost_per_event": round(avg_cost_per_event, 6),
            "avg_variable_cost_per_active_user": round(avg_variable_cost_per_active_user, 6),
            "avg_fixed_cost_per_active_user": round(avg_fixed_cost_per_active_user, 6),
            "avg_cost_per_active_user": round(avg_cost_per_active_user, 6),
            "avg_events_per_active_user": round(avg_events_per_active_user, 6),
        },
        "breakdown": {
            "by_provider": provider_rows,
            "by_action_type": actions,
            "by_model": models,
            "by_units_type": units_by_type,
            "fixed_costs": fixed_items,
        },
    }


def list_billing_plans(*, include_inactive: bool = False) -> list[dict]:
    where_sql = "" if include_inactive else "WHERE is_active = TRUE"
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT
                    plan_code,
                    name,
                    is_paid,
                    stripe_price_id,
                    daily_cost_cap_eur,
                    trial_days,
                    is_active,
                    created_at,
                    updated_at
                FROM plans
                {where_sql}
                ORDER BY is_paid ASC, plan_code ASC;
                """
            )
            rows = cursor.fetchall() or []
    return [
        {
            "plan_code": str(row[0] or ""),
            "name": str(row[1] or ""),
            "is_paid": bool(row[2]),
            "stripe_price_id": str(row[3] or "") or None,
            "daily_cost_cap_eur": float(row[4]) if row[4] is not None else None,
            "trial_days": int(row[5] or 0),
            "is_active": bool(row[6]),
            "created_at": row[7].isoformat() if row[7] else None,
            "updated_at": row[8].isoformat() if row[8] else None,
        }
        for row in rows
    ]


def get_billing_plan(plan_code: str) -> dict | None:
    code = str(plan_code or "").strip().lower()
    if not code:
        return None
    now_ts = time.time()
    with _BILLING_PLAN_CACHE_LOCK:
        cached = _BILLING_PLAN_CACHE.get(code)
        if cached is not None:
            cached_at_ts, cached_value = cached
            if now_ts - cached_at_ts <= float(BILLING_PLAN_CACHE_TTL_SEC):
                return dict(cached_value) if isinstance(cached_value, dict) else None
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    plan_code,
                    name,
                    is_paid,
                    stripe_price_id,
                    daily_cost_cap_eur,
                    trial_days,
                    is_active,
                    created_at,
                    updated_at
                FROM plans
                WHERE plan_code = %s
                LIMIT 1;
                """,
                (code,),
            )
            row = cursor.fetchone()
    plan = {
        "plan_code": str(row[0] or ""),
        "name": str(row[1] or ""),
        "is_paid": bool(row[2]),
        "stripe_price_id": str(row[3] or "") or None,
        "daily_cost_cap_eur": float(row[4]) if row[4] is not None else None,
        "trial_days": int(row[5] or 0),
        "is_active": bool(row[6]),
        "created_at": row[7].isoformat() if row[7] else None,
        "updated_at": row[8].isoformat() if row[8] else None,
    } if row else None
    with _BILLING_PLAN_CACHE_LOCK:
        _BILLING_PLAN_CACHE[code] = (now_ts, dict(plan) if isinstance(plan, dict) else None)
        if len(_BILLING_PLAN_CACHE) > 64:
            stale_keys = sorted(_BILLING_PLAN_CACHE.items(), key=lambda item: item[1][0])[:16]
            for stale_key, _value in stale_keys:
                _BILLING_PLAN_CACHE.pop(stale_key, None)
    return dict(plan) if isinstance(plan, dict) else None


def _subscription_row_to_dict(row) -> dict:
    return {
        "user_id": int(row[0]),
        "plan_code": str(row[1] or ""),
        "status": str(row[2] or ""),
        "trial_ends_at": row[3].isoformat() if row[3] else None,
        "current_period_end": row[4].isoformat() if row[4] else None,
        "stripe_customer_id": str(row[5] or "") or None,
        "stripe_subscription_id": str(row[6] or "") or None,
        "created_at": row[7].isoformat() if row[7] else None,
        "updated_at": row[8].isoformat() if row[8] else None,
    }


def get_user_subscription(user_id: int) -> dict | None:
    user_id_value = int(user_id)
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    user_id,
                    plan_code,
                    status,
                    trial_ends_at,
                    current_period_end,
                    stripe_customer_id,
                    stripe_subscription_id,
                    created_at,
                    updated_at
                FROM user_subscriptions
                WHERE user_id = %s
                LIMIT 1;
                """,
                (user_id_value,),
            )
            row = cursor.fetchone()
    return _subscription_row_to_dict(row) if row else None


def get_user_subscription_by_customer_id(stripe_customer_id: str) -> dict | None:
    customer_id_value = str(stripe_customer_id or "").strip()
    if not customer_id_value:
        return None
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    user_id,
                    plan_code,
                    status,
                    trial_ends_at,
                    current_period_end,
                    stripe_customer_id,
                    stripe_subscription_id,
                    created_at,
                    updated_at
                FROM user_subscriptions
                WHERE stripe_customer_id = %s
                ORDER BY updated_at DESC
                LIMIT 1;
                """,
                (customer_id_value,),
            )
            row = cursor.fetchone()
    return _subscription_row_to_dict(row) if row else None


def get_user_subscription_by_stripe_subscription_id(stripe_subscription_id: str) -> dict | None:
    subscription_id_value = str(stripe_subscription_id or "").strip()
    if not subscription_id_value:
        return None
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    user_id,
                    plan_code,
                    status,
                    trial_ends_at,
                    current_period_end,
                    stripe_customer_id,
                    stripe_subscription_id,
                    created_at,
                    updated_at
                FROM user_subscriptions
                WHERE stripe_subscription_id = %s
                ORDER BY updated_at DESC
                LIMIT 1;
                """,
                (subscription_id_value,),
            )
            row = cursor.fetchone()
    return _subscription_row_to_dict(row) if row else None


def _normalize_subscription_status(status: str | None) -> str:
    value = str(status or "").strip().lower() or "inactive"
    allowed = {"active", "inactive", "past_due", "canceled", "trialing"}
    if value not in allowed:
        raise ValueError(f"Unsupported subscription status: {value!r}")
    return value


def get_or_create_user_subscription(
    user_id: int,
    now_ts: datetime | None = None,
    tz: str = TRIAL_POLICY_TZ,
) -> dict:
    """Race-safe create-or-get via INSERT .. ON CONFLICT DO NOTHING + SELECT."""
    user_id_value = int(user_id)
    trial_ends_at = _compute_trial_ends_at(now_ts, trial_days=TRIAL_POLICY_DAYS, tz_name=tz)
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO user_subscriptions (
                    user_id,
                    plan_code,
                    status,
                    trial_ends_at,
                    updated_at
                )
                VALUES (%s, 'free', 'trialing', %s, NOW())
                ON CONFLICT (user_id) DO NOTHING
                RETURNING
                    user_id,
                    plan_code,
                    status,
                    trial_ends_at,
                    current_period_end,
                    stripe_customer_id,
                    stripe_subscription_id,
                    created_at,
                    updated_at;
                """,
                (user_id_value, trial_ends_at),
            )
            row = cursor.fetchone()
            if not row:
                cursor.execute(
                    """
                    SELECT
                        user_id,
                        plan_code,
                        status,
                        trial_ends_at,
                        current_period_end,
                        stripe_customer_id,
                        stripe_subscription_id,
                        created_at,
                        updated_at
                    FROM user_subscriptions
                    WHERE user_id = %s
                    LIMIT 1;
                    """,
                    (user_id_value,),
                )
                row = cursor.fetchone()
    if not row:
        raise RuntimeError("Failed to create or fetch user subscription")
    return _subscription_row_to_dict(row)


def bind_stripe_customer_to_user(user_id: int, stripe_customer_id: str, db_conn=None) -> dict:
    user_id_value = int(user_id)
    customer_id_value = str(stripe_customer_id or "").strip()
    if not customer_id_value:
        raise ValueError("stripe_customer_id is required")
    owns_conn = db_conn is None
    if owns_conn:
        with get_db_connection_context() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO user_subscriptions (
                        user_id,
                        plan_code,
                        status,
                        trial_ends_at,
                        stripe_customer_id,
                        updated_at
                    )
                    VALUES (%s, 'free', 'trialing', %s, %s, NOW())
                    ON CONFLICT (user_id) DO UPDATE
                    SET
                        stripe_customer_id = EXCLUDED.stripe_customer_id,
                        updated_at = NOW()
                    RETURNING
                        user_id,
                        plan_code,
                        status,
                        trial_ends_at,
                        current_period_end,
                        stripe_customer_id,
                        stripe_subscription_id,
                        created_at,
                        updated_at;
                    """,
                    (
                        user_id_value,
                        _compute_trial_ends_at(datetime.now(timezone.utc), trial_days=TRIAL_POLICY_DAYS, tz_name=TRIAL_POLICY_TZ),
                        customer_id_value,
                    ),
                )
                row = cursor.fetchone()
    else:
        with db_conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO user_subscriptions (
                    user_id,
                    plan_code,
                    status,
                    trial_ends_at,
                    stripe_customer_id,
                    updated_at
                )
                VALUES (%s, 'free', 'trialing', %s, %s, NOW())
                ON CONFLICT (user_id) DO UPDATE
                SET
                    stripe_customer_id = EXCLUDED.stripe_customer_id,
                    updated_at = NOW()
                RETURNING
                    user_id,
                    plan_code,
                    status,
                    trial_ends_at,
                    current_period_end,
                    stripe_customer_id,
                    stripe_subscription_id,
                    created_at,
                    updated_at;
                """,
                (
                    user_id_value,
                    _compute_trial_ends_at(datetime.now(timezone.utc), trial_days=TRIAL_POLICY_DAYS, tz_name=TRIAL_POLICY_TZ),
                    customer_id_value,
                ),
            )
            row = cursor.fetchone()
    if not row:
        raise RuntimeError("Failed to bind stripe customer to user subscription")
    return _subscription_row_to_dict(row)


def set_subscription_from_stripe(
    user_id: int,
    plan_code: str,
    stripe_customer_id: str | None,
    stripe_subscription_id: str | None,
    status: str,
    current_period_end: datetime | None,
    db_conn=None,
) -> dict:
    user_id_value = int(user_id)
    plan_code_value = str(plan_code or "pro").strip().lower() or "pro"
    status_value = _normalize_subscription_status(status)
    period_end_value = _to_aware_datetime(current_period_end) if current_period_end else None
    customer_id_value = str(stripe_customer_id or "").strip() or None
    subscription_id_value = str(stripe_subscription_id or "").strip() or None
    owns_conn = db_conn is None
    if owns_conn:
        with get_db_connection_context() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO user_subscriptions (
                        user_id,
                        plan_code,
                        status,
                        trial_ends_at,
                        current_period_end,
                        stripe_customer_id,
                        stripe_subscription_id,
                        updated_at
                    )
                    VALUES (%s, %s, %s, NULL, %s, %s, %s, NOW())
                    ON CONFLICT (user_id) DO UPDATE
                    SET
                        plan_code = EXCLUDED.plan_code,
                        status = EXCLUDED.status,
                        trial_ends_at = NULL,
                        current_period_end = EXCLUDED.current_period_end,
                        stripe_customer_id = COALESCE(EXCLUDED.stripe_customer_id, user_subscriptions.stripe_customer_id),
                        stripe_subscription_id = COALESCE(EXCLUDED.stripe_subscription_id, user_subscriptions.stripe_subscription_id),
                        updated_at = NOW()
                    RETURNING
                        user_id,
                        plan_code,
                        status,
                        trial_ends_at,
                        current_period_end,
                        stripe_customer_id,
                        stripe_subscription_id,
                        created_at,
                        updated_at;
                    """,
                    (
                        user_id_value,
                        plan_code_value,
                        status_value,
                        period_end_value,
                        customer_id_value,
                        subscription_id_value,
                    ),
                )
                row = cursor.fetchone()
    else:
        with db_conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO user_subscriptions (
                    user_id,
                    plan_code,
                    status,
                    trial_ends_at,
                    current_period_end,
                    stripe_customer_id,
                    stripe_subscription_id,
                    updated_at
                )
                    VALUES (%s, %s, %s, NULL, %s, %s, %s, NOW())
                    ON CONFLICT (user_id) DO UPDATE
                    SET
                        plan_code = EXCLUDED.plan_code,
                    status = EXCLUDED.status,
                    trial_ends_at = NULL,
                    current_period_end = EXCLUDED.current_period_end,
                    stripe_customer_id = COALESCE(EXCLUDED.stripe_customer_id, user_subscriptions.stripe_customer_id),
                    stripe_subscription_id = COALESCE(EXCLUDED.stripe_subscription_id, user_subscriptions.stripe_subscription_id),
                    updated_at = NOW()
                RETURNING
                    user_id,
                    plan_code,
                    status,
                    trial_ends_at,
                    current_period_end,
                    stripe_customer_id,
                    stripe_subscription_id,
                    created_at,
                    updated_at;
                """,
                    (
                        user_id_value,
                        plan_code_value,
                        status_value,
                        period_end_value,
                        customer_id_value,
                    subscription_id_value,
                ),
            )
            row = cursor.fetchone()
    if not row:
        raise RuntimeError("Failed to upsert Stripe subscription")
    return _subscription_row_to_dict(row)


def try_mark_stripe_event_processed(event_id: str, db_conn=None) -> bool:
    """Insert-first idempotency marker. True only for the first successful insert."""
    event_id_value = str(event_id or "").strip()
    if not event_id_value:
        return False
    if db_conn is None:
        with get_db_connection_context() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO stripe_events_processed (event_id)
                    VALUES (%s)
                    ON CONFLICT (event_id) DO NOTHING
                    RETURNING event_id;
                    """,
                    (event_id_value,),
                )
                inserted = cursor.fetchone()
    else:
        with db_conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO stripe_events_processed (event_id)
                VALUES (%s)
                ON CONFLICT (event_id) DO NOTHING
                RETURNING event_id;
                """,
                (event_id_value,),
            )
            inserted = cursor.fetchone()
    return bool(inserted)


def mark_stripe_event_processed(event_id: str, db_conn=None) -> bool:
    # Backward-compatible alias; use try_mark_stripe_event_processed in webhook flow.
    return try_mark_stripe_event_processed(event_id, db_conn=db_conn)


def is_stripe_event_processed(event_id: str) -> bool:
    event_id_value = str(event_id or "").strip()
    if not event_id_value:
        return False
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT 1
                FROM stripe_events_processed
                WHERE event_id = %s
                LIMIT 1;
                """,
                (event_id_value,),
            )
            row = cursor.fetchone()
    return bool(row)


def get_today_cost_eur(user_id: int, tz: str = TRIAL_POLICY_TZ) -> float:
    user_id_value = int(user_id)
    tz_name = str(tz or TRIAL_POLICY_TZ).strip() or TRIAL_POLICY_TZ
    tzinfo = _resolve_timezone(tz_name)
    day_local = datetime.now(timezone.utc).astimezone(tzinfo).date()

    # Always recompute from source-of-truth billing events before writing rollup.
    # This avoids stale totals when new events arrive after an earlier rollup read.
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT currency, COALESCE(SUM(cost_amount), 0) AS total_cost
                FROM bt_3_billing_events
                WHERE user_id = %s
                  AND (event_time AT TIME ZONE %s)::date = %s
                GROUP BY currency;
                """,
                (user_id_value, tz_name, day_local),
            )
            rows = cursor.fetchall() or []
            total_eur = 0.0
            for row in rows:
                currency = str(row[0] or "EUR").upper()
                amount = float(row[1] or 0.0)
                try:
                    total_eur += convert_cost_to_eur(amount, currency)
                except ValueError:
                    continue

            cursor.execute(
                """
                INSERT INTO daily_cost_rollup (
                    user_id,
                    day,
                    currency,
                    total_cost_eur,
                    updated_at
                )
                VALUES (%s, %s, 'EUR', %s, NOW())
                ON CONFLICT (user_id, day) DO UPDATE
                SET
                    currency = 'EUR',
                    total_cost_eur = EXCLUDED.total_cost_eur,
                    updated_at = NOW();
                """,
                (user_id_value, day_local, total_eur),
            )
    return float(total_eur)


def get_today_cost_eur_fast(
    user_id: int,
    tz: str = TRIAL_POLICY_TZ,
    *,
    max_age_sec: int = 90,
) -> float:
    user_id_value = int(user_id)
    tz_name = str(tz or TRIAL_POLICY_TZ).strip() or TRIAL_POLICY_TZ
    tzinfo = _resolve_timezone(tz_name)
    day_local = datetime.now(timezone.utc).astimezone(tzinfo).date()
    max_age_value = max(0, int(max_age_sec or 0))

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT total_cost_eur, updated_at
                FROM daily_cost_rollup
                WHERE user_id = %s
                  AND day = %s
                LIMIT 1;
                """,
                (user_id_value, day_local),
            )
            row = cursor.fetchone()

    if row:
        total_cost_eur = float(row[0] or 0.0)
        updated_at = _to_aware_datetime(row[1]) if row[1] else None
        if updated_at is not None:
            age_sec = max(0.0, (datetime.now(timezone.utc) - updated_at).total_seconds())
            if age_sec <= float(max_age_value):
                return total_cost_eur

    return get_today_cost_eur(user_id_value, tz=tz_name)


def _next_local_midnight_iso(now_ts_utc: datetime | None = None, tz: str = TRIAL_POLICY_TZ) -> str:
    tzinfo = _resolve_timezone(tz)
    now_utc = _to_aware_datetime(now_ts_utc)
    local_now = now_utc.astimezone(tzinfo)
    next_day = local_now.date() + timedelta(days=1)
    next_midnight_local = datetime.combine(next_day, dt_time.min, tzinfo=tzinfo)
    return next_midnight_local.isoformat()


def _next_local_month_start_iso(now_ts_utc: datetime | None = None, tz: str = TRIAL_POLICY_TZ) -> str:
    tzinfo = _resolve_timezone(tz)
    now_utc = _to_aware_datetime(now_ts_utc)
    local_now = now_utc.astimezone(tzinfo)
    if local_now.month == 12:
        next_month_date = date(local_now.year + 1, 1, 1)
    else:
        next_month_date = date(local_now.year, local_now.month + 1, 1)
    next_month_local = datetime.combine(next_month_date, dt_time.min, tzinfo=tzinfo)
    return next_month_local.isoformat()


def resolve_entitlement(
    user_id: int,
    now_ts_utc: datetime | None = None,
    tz: str = TRIAL_POLICY_TZ,
    subscription: dict | None = None,
) -> dict:
    now_utc = _to_aware_datetime(now_ts_utc)
    subscription_row = dict(subscription) if isinstance(subscription, dict) else get_user_subscription(int(user_id))
    if not subscription_row:
        subscription = {
            "user_id": int(user_id),
            "plan_code": "free",
            "status": "inactive",
            "trial_ends_at": None,
            "current_period_end": None,
            "stripe_customer_id": None,
            "stripe_subscription_id": None,
            "created_at": None,
            "updated_at": None,
        }
    else:
        subscription = subscription_row

    plan_code = str(subscription.get("plan_code") or "free").strip().lower() or "free"
    status = _normalize_subscription_status(subscription.get("status"))
    current_plan = get_billing_plan(plan_code) or {}
    if not current_plan and plan_code != "free":
        plan_code = "free"
        current_plan = get_billing_plan("free") or {}

    trial_ends_at_value = subscription.get("trial_ends_at")
    trial_ends_at_dt = None
    if trial_ends_at_value:
        try:
            trial_ends_at_dt = datetime.fromisoformat(str(trial_ends_at_value))
            trial_ends_at_dt = _to_aware_datetime(trial_ends_at_dt)
        except Exception:
            trial_ends_at_dt = None

    if bool(current_plan.get("is_paid")) and status in {"active", "trialing"}:
        effective_mode = "pro"
    elif status == "trialing" and trial_ends_at_dt is not None and now_utc < trial_ends_at_dt:
        effective_mode = "trial"
    else:
        effective_mode = "free"

    free_plan = get_billing_plan("free") or {}
    pro_plan = current_plan if bool(current_plan.get("is_paid")) else (get_billing_plan("pro") or {})
    if effective_mode == "pro":
        cap_eur = pro_plan.get("daily_cost_cap_eur")
    elif effective_mode == "trial":
        cap_eur = float(_env_decimal("TRIAL_DAILY_COST_CAP_EUR", "1.00"))
    else:
        cap_eur = free_plan.get("daily_cost_cap_eur")
        if cap_eur is None:
            fallback = _env_decimal("FREE_DAILY_COST_CAP_EUR", "0.50")
            cap_eur = float(fallback) if fallback is not None else None

    return {
        "user_id": int(user_id),
        "plan_code": plan_code,
        "plan_name": str(current_plan.get("name") or (free_plan.get("name") if plan_code == "free" else plan_code)),
        "status": status,
        "trial_ends_at": trial_ends_at_dt.isoformat() if trial_ends_at_dt else None,
        "effective_mode": effective_mode,
        "cap_eur": float(cap_eur) if cap_eur is not None else None,
        "reset_at": _next_local_midnight_iso(now_utc, tz=tz),
    }


def enforce_daily_cost_cap(
    user_id: int,
    now_ts_utc: datetime | None = None,
    tz: str = TRIAL_POLICY_TZ,
) -> dict | None:
    entitlement = resolve_entitlement(user_id=int(user_id), now_ts_utc=now_ts_utc, tz=tz)
    cap_eur = entitlement.get("cap_eur")
    if cap_eur is None:
        return None
    spent_eur = float(get_today_cost_eur(int(user_id), tz=tz))
    if spent_eur >= float(cap_eur):
        return {
            "error": "cost_cap_exceeded",
            "cap_eur": float(cap_eur),
            "spent_eur": float(round(spent_eur, 6)),
            "currency": "EUR",
            "reset_at": entitlement.get("reset_at"),
            "upgrade": {
                "available": True,
                "plan_code": "pro",
                "action": "checkout",
                "endpoint": "/api/billing/create-checkout-session",
            },
        }
    return None


def get_plan_limit(plan_code: str, feature_code: str, period: str = "day") -> dict | None:
    plan_value = str(plan_code or "").strip().lower()
    feature_value = str(feature_code or "").strip().lower()
    period_value = str(period or "day").strip().lower() or "day"
    if not plan_value or not feature_value:
        return None
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT plan_code, feature_code, limit_value, limit_unit, period, is_active
                FROM plan_limits
                WHERE plan_code = %s
                  AND feature_code = %s
                  AND period = %s
                  AND is_active = TRUE
                LIMIT 1;
                """,
                (plan_value, feature_value, period_value),
            )
            row = cursor.fetchone()
    if not row:
        return None
    return {
        "plan_code": str(row[0] or ""),
        "feature_code": str(row[1] or ""),
        "limit_value": float(row[2] or 0.0),
        "limit_unit": str(row[3] or ""),
        "period": str(row[4] or "day"),
        "is_active": bool(row[5]),
    }


def _get_feature_usage_today(user_id: int, feature_code: str, tz: str = TRIAL_POLICY_TZ) -> float:
    feature = str(feature_code or "").strip().lower()
    tz_name = str(tz or TRIAL_POLICY_TZ).strip() or TRIAL_POLICY_TZ
    day_local = datetime.now(timezone.utc).astimezone(_resolve_timezone(tz_name)).date()

    if feature == "youtube_fetch_daily":
        with get_db_connection_context() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM bt_3_billing_events
                    WHERE user_id = %s
                      AND action_type = 'youtube_transcript_fetch'
                      AND (event_time AT TIME ZONE %s)::date = %s;
                    """,
                    (int(user_id), tz_name, day_local),
                )
                row = cursor.fetchone()
        return float((row or [0])[0] or 0)

    if feature == "tts_chars_daily":
        with get_db_connection_context() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT COALESCE(SUM(units_value), 0)
                    FROM bt_3_billing_events
                    WHERE user_id = %s
                      AND action_type = 'webapp_tts_chars'
                      AND units_type = 'chars'
                      AND (event_time AT TIME ZONE %s)::date = %s;
                    """,
                    (int(user_id), tz_name, day_local),
                )
                row = cursor.fetchone()
        return float((row or [0])[0] or 0.0)

    if feature == "feel_word_daily":
        with get_db_connection_context() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM bt_3_billing_events
                    WHERE user_id = %s
                      AND action_type = 'flashcards_feel_request'
                      AND units_type = 'requests'
                      AND (event_time AT TIME ZONE %s)::date = %s;
                    """,
                    (int(user_id), tz_name, day_local),
                )
                row = cursor.fetchone()
        return float((row or [0])[0] or 0)

    if feature == "skill_training_daily":
        with get_db_connection_context() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM bt_3_billing_events
                    WHERE user_id = %s
                      AND action_type = 'theory_package_prepare'
                      AND (event_time AT TIME ZONE %s)::date = %s;
                    """,
                    (int(user_id), tz_name, day_local),
                )
                row = cursor.fetchone()
        return float((row or [0])[0] or 0)

    if feature == "translation_daily_sets":
        with get_db_connection_context() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT COUNT(DISTINCT ds.session_id)
                    FROM bt_3_daily_sentences ds
                    WHERE ds.user_id = %s
                      AND COALESCE(ds.shown_to_user, FALSE) = TRUE
                      AND (
                        COALESCE((ds.shown_to_user_at AT TIME ZONE %s)::date, ds.date) = %s
                      );
                    """,
                    (int(user_id), tz_name, day_local),
                )
                row = cursor.fetchone()
        return float((row or [0])[0] or 0)

    return 0.0


def enforce_feature_limit(
    user_id: int,
    feature_code: str,
    *,
    requested_units: float = 1.0,
    now_ts_utc: datetime | None = None,
    tz: str = TRIAL_POLICY_TZ,
) -> dict | None:
    entitlement = resolve_entitlement(user_id=int(user_id), now_ts_utc=now_ts_utc, tz=tz)
    effective_mode = str(entitlement.get("effective_mode") or "free")
    plan_code = str(entitlement.get("plan_code") or "free")

    # Product policy: trial has full features, except YouTube safety limit.
    if effective_mode == "trial" and feature_code != "youtube_fetch_daily":
        return None

    lookup_plan = plan_code
    if effective_mode == "trial" and feature_code == "youtube_fetch_daily":
        lookup_plan = "free"
    limit = get_plan_limit(lookup_plan, feature_code, period="day")
    if not limit:
        return None

    usage = _get_feature_usage_today(int(user_id), feature_code, tz=tz)
    requested = max(0.0, float(requested_units or 0.0))
    limit_value = float(limit.get("limit_value") or 0.0)
    if usage + requested > limit_value:
        used_value = float(round(usage, 6))
        limit_out = int(limit_value) if float(limit_value).is_integer() else float(limit_value)
        used_out = int(used_value) if float(used_value).is_integer() else float(used_value)
        return {
            "error": "feature_limit_exceeded",
            "feature": feature_code,
            "limit": limit_out,
            "used": used_out,
            "unit": str(limit.get("limit_unit") or "count"),
            "reset_at": entitlement.get("reset_at"),
            "upgrade": {
                "available": True,
                "plan_code": "pro",
                "action": "checkout",
                "endpoint": "/api/billing/create-checkout-session",
            },
        }
    return None


def get_user_action_month_usage(
    user_id: int,
    action_type: str,
    *,
    units_type: str = "chars",
    provider: str | None = None,
    period_month: date | datetime | None = None,
    tz: str = TRIAL_POLICY_TZ,
) -> float:
    action_value = str(action_type or "").strip().lower()
    units_type_value = str(units_type or "").strip().lower() or "chars"
    provider_value = str(provider or "").strip().lower() or None
    if not action_value:
        return 0.0
    period_start = _month_period_start(period_month, tz=tz)
    if period_start.month == 12:
        period_end = date(period_start.year + 1, 1, 1)
    else:
        period_end = date(period_start.year, period_start.month + 1, 1)

    provider_sql = ""
    provider_params: list = []
    if provider_value:
        provider_sql = " AND provider = %s"
        provider_params.append(provider_value)

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT COALESCE(SUM(units_value), 0)
                FROM bt_3_billing_events
                WHERE user_id = %s
                  AND action_type = %s
                  AND units_type = %s
                  {provider_sql}
                  AND event_time >= %s
                  AND event_time < %s;
                """,
                (
                    int(user_id),
                    action_value,
                    units_type_value,
                    *provider_params,
                    datetime.combine(period_start, dt_time.min, tzinfo=timezone.utc),
                    datetime.combine(period_end, dt_time.min, tzinfo=timezone.utc),
                ),
            )
            row = cursor.fetchone()
    return float((row or [0])[0] or 0.0)


def enforce_reader_audio_pro_monthly_limit(
    user_id: int,
    *,
    requested_units: float = 1.0,
    now_ts_utc: datetime | None = None,
    tz: str = TRIAL_POLICY_TZ,
) -> dict | None:
    used_units = float(
        get_user_action_month_usage(
            int(user_id),
            "reader_audio_tts",
            units_type="chars",
            provider="google_tts",
            period_month=now_ts_utc,
            tz=tz,
        )
    )
    requested = max(0.0, float(requested_units or 0.0))
    limit_value = float(READER_AUDIO_PRO_MONTHLY_LIMIT_CHARS)
    if used_units + requested > limit_value:
        used_out = int(round(used_units))
        limit_out = int(round(limit_value))
        remaining_out = max(0, limit_out - used_out)
        return {
            "error": "reader_audio_monthly_limit_exceeded",
            "feature": "reader_audio_tts_monthly",
            "limit": limit_out,
            "used": used_out,
            "requested": int(round(requested)),
            "remaining": remaining_out,
            "unit": "chars",
            "reset_at": _next_local_month_start_iso(now_ts_utc, tz=tz),
            "message": (
                f"Лимит Reader Audio на месяц исчерпан: {used_out} / {limit_out} chars. "
                f"Следующий сброс: {_next_local_month_start_iso(now_ts_utc, tz=tz)}"
            ),
        }
    return None


def get_today_reminder_settings(user_id: int) -> dict:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT enabled, timezone, reminder_hour, reminder_minute, updated_at
                FROM bt_3_today_reminder_settings
                WHERE user_id = %s
                LIMIT 1;
                """,
                (int(user_id),),
            )
            row = cursor.fetchone()
    if not row:
        return {
            "user_id": int(user_id),
            "enabled": False,
            "timezone": "Europe/Vienna",
            "reminder_hour": 7,
            "reminder_minute": 0,
            "updated_at": None,
        }
    return {
        "user_id": int(user_id),
        "enabled": bool(row[0]),
        "timezone": row[1] or "Europe/Vienna",
        "reminder_hour": int(row[2] or 7),
        "reminder_minute": int(row[3] or 0),
        "updated_at": row[4].isoformat() if row[4] else None,
    }


def upsert_today_reminder_settings(
    user_id: int,
    *,
    enabled: bool,
    timezone_name: str = "Europe/Vienna",
    reminder_hour: int = 7,
    reminder_minute: int = 0,
) -> dict:
    tz_name = (timezone_name or "Europe/Vienna").strip() or "Europe/Vienna"
    hour = max(0, min(int(reminder_hour), 23))
    minute = max(0, min(int(reminder_minute), 59))
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_today_reminder_settings (
                    user_id,
                    enabled,
                    timezone,
                    reminder_hour,
                    reminder_minute,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, NOW())
                ON CONFLICT (user_id) DO UPDATE
                SET
                    enabled = EXCLUDED.enabled,
                    timezone = EXCLUDED.timezone,
                    reminder_hour = EXCLUDED.reminder_hour,
                    reminder_minute = EXCLUDED.reminder_minute,
                    updated_at = NOW()
                RETURNING enabled, timezone, reminder_hour, reminder_minute, updated_at;
                """,
                (int(user_id), bool(enabled), tz_name, hour, minute),
            )
            row = cursor.fetchone()
    return {
        "user_id": int(user_id),
        "enabled": bool(row[0]),
        "timezone": row[1] or "Europe/Vienna",
        "reminder_hour": int(row[2] or 7),
        "reminder_minute": int(row[3] or 0),
        "updated_at": row[4].isoformat() if row[4] else None,
    }


def list_today_reminder_users(limit: int = 1000, offset: int = 0) -> list[dict]:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    s.user_id,
                    COALESCE(
                        NULLIF(BTRIM(u.username), ''),
                        NULLIF((
                            SELECT BTRIM(up.username)
                            FROM bt_3_user_progress up
                            WHERE up.user_id = s.user_id
                              AND BTRIM(COALESCE(up.username, '')) <> ''
                            ORDER BY up.start_time DESC NULLS LAST
                            LIMIT 1
                        ), ''),
                        NULLIF((
                            SELECT BTRIM(tr.username)
                            FROM bt_3_translations tr
                            WHERE tr.user_id = s.user_id
                              AND BTRIM(COALESCE(tr.username, '')) <> ''
                            ORDER BY tr.timestamp DESC NULLS LAST
                            LIMIT 1
                        ), ''),
                        NULLIF((
                            SELECT BTRIM(m.username)
                            FROM bt_3_messages m
                            WHERE m.user_id = s.user_id
                              AND BTRIM(COALESCE(m.username, '')) <> ''
                            ORDER BY m.timestamp DESC NULLS LAST
                            LIMIT 1
                        ), ''),
                        ''
                    ) AS username,
                    s.timezone,
                    s.reminder_hour,
                    s.reminder_minute
                FROM bt_3_today_reminder_settings s
                LEFT JOIN bt_3_allowed_users u ON u.user_id = s.user_id
                WHERE s.enabled = TRUE
                ORDER BY s.updated_at DESC
                LIMIT %s OFFSET %s;
                """,
                (max(1, min(int(limit), 5000)), max(0, int(offset))),
            )
            rows = cursor.fetchall()
    return [
        {
            "user_id": int(row[0]),
            "username": row[1] or None,
            "timezone": row[2] or "Europe/Vienna",
            "reminder_hour": int(row[3] or 7),
            "reminder_minute": int(row[4] or 0),
        }
        for row in rows
    ]


def get_audio_grammar_settings(user_id: int) -> dict:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT enabled, updated_at
                FROM bt_3_audio_grammar_settings
                WHERE user_id = %s
                LIMIT 1;
                """,
                (int(user_id),),
            )
            row = cursor.fetchone()
    if not row:
        return {
            "user_id": int(user_id),
            "enabled": False,
            "updated_at": None,
        }
    return {
        "user_id": int(user_id),
        "enabled": bool(row[0]),
        "updated_at": row[1].isoformat() if row[1] else None,
    }


def upsert_audio_grammar_settings(user_id: int, *, enabled: bool) -> dict:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_audio_grammar_settings (
                    user_id,
                    enabled,
                    updated_at
                )
                VALUES (%s, %s, NOW())
                ON CONFLICT (user_id) DO UPDATE
                SET
                    enabled = EXCLUDED.enabled,
                    updated_at = NOW()
                RETURNING enabled, updated_at;
                """,
                (int(user_id), bool(enabled)),
            )
            row = cursor.fetchone()
    return {
        "user_id": int(user_id),
        "enabled": bool(row[0]),
        "updated_at": row[1].isoformat() if row[1] else None,
    }


def get_tts_prewarm_settings() -> dict:
    default_limit = max(50, min(10000, int((os.getenv("TTS_PREWARM_PER_USER_CHAR_LIMIT") or "600").strip() or "600")))
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT per_user_char_limit, updated_by, updated_at
                FROM bt_3_tts_prewarm_settings
                WHERE settings_key = 'global'
                LIMIT 1;
                """
            )
            row = cursor.fetchone()
    if not row:
        return {
            "settings_key": "global",
            "per_user_char_limit": int(default_limit),
            "updated_by": None,
            "updated_at": None,
        }
    return {
        "settings_key": "global",
        "per_user_char_limit": max(50, int(row[0] or default_limit)),
        "updated_by": int(row[1]) if row[1] is not None else None,
        "updated_at": row[2].isoformat() if row[2] else None,
    }


def upsert_tts_prewarm_settings(*, per_user_char_limit: int, updated_by: int | None = None) -> dict:
    safe_limit = max(50, min(10000, int(per_user_char_limit or 0)))
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_tts_prewarm_settings (
                    settings_key,
                    per_user_char_limit,
                    updated_by,
                    updated_at
                )
                VALUES ('global', %s, %s, NOW())
                ON CONFLICT (settings_key) DO UPDATE
                SET
                    per_user_char_limit = EXCLUDED.per_user_char_limit,
                    updated_by = EXCLUDED.updated_by,
                    updated_at = NOW()
                RETURNING per_user_char_limit, updated_by, updated_at;
                """,
                (int(safe_limit), int(updated_by) if updated_by is not None else None),
            )
            row = cursor.fetchone()
    return {
        "settings_key": "global",
        "per_user_char_limit": int(row[0] or safe_limit),
        "updated_by": int(row[1]) if row[1] is not None else None,
        "updated_at": row[2].isoformat() if row[2] else None,
    }


def update_translation_audio_grammar_opt_in(user_id: int, translation_id: int, *, enabled: bool) -> dict:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE bt_3_translations
                SET audio_grammar_opt_in = %s
                WHERE id = %s AND user_id = %s
                RETURNING id, audio_grammar_opt_in, timestamp;
                """,
                (bool(enabled), int(translation_id), int(user_id)),
            )
            row = cursor.fetchone()
    if not row:
        raise ValueError("translation not found")
    return {
        "translation_id": int(row[0]),
        "enabled": bool(row[1]),
        "timestamp": row[2].isoformat() if row[2] else None,
    }


def has_youtube_proxy_subtitles_access(user_id: int) -> bool:
    if not user_id:
        return False
    if int(user_id) in get_admin_telegram_ids():
        return True

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT enabled
                FROM bt_3_youtube_proxy_subtitles_access
                WHERE user_id = %s
                LIMIT 1;
                """,
                (int(user_id),),
            )
            row = cursor.fetchone()
    return bool(row and bool(row[0]))


def upsert_youtube_proxy_subtitles_access(
    *,
    user_id: int,
    enabled: bool = True,
    granted_by: int | None = None,
    note: str | None = None,
) -> dict:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_youtube_proxy_subtitles_access (
                    user_id,
                    enabled,
                    granted_by,
                    note,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, NOW())
                ON CONFLICT (user_id) DO UPDATE
                SET
                    enabled = EXCLUDED.enabled,
                    granted_by = COALESCE(EXCLUDED.granted_by, bt_3_youtube_proxy_subtitles_access.granted_by),
                    note = COALESCE(EXCLUDED.note, bt_3_youtube_proxy_subtitles_access.note),
                    updated_at = NOW()
                RETURNING user_id, enabled, granted_by, note, created_at, updated_at;
                """,
                (int(user_id), bool(enabled), granted_by, note),
            )
            row = cursor.fetchone()
    return {
        "user_id": int(row[0]),
        "enabled": bool(row[1]),
        "granted_by": int(row[2]) if row[2] is not None else None,
        "note": row[3] if row[3] is not None else None,
        "created_at": row[4].isoformat() if row[4] else None,
        "updated_at": row[5].isoformat() if row[5] else None,
    }


def list_youtube_proxy_subtitles_access(
    *,
    limit: int = 200,
    offset: int = 0,
    enabled_only: bool = False,
) -> list[dict]:
    safe_limit = max(1, min(int(limit or 200), 1000))
    safe_offset = max(0, int(offset or 0))
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT user_id, enabled, granted_by, note, created_at, updated_at
                FROM bt_3_youtube_proxy_subtitles_access
                WHERE (%s = FALSE OR enabled = TRUE)
                ORDER BY updated_at DESC, user_id DESC
                LIMIT %s OFFSET %s;
                """,
                (bool(enabled_only), safe_limit, safe_offset),
            )
            rows = cursor.fetchall()
    return [
        {
            "user_id": int(row[0]),
            "enabled": bool(row[1]),
            "granted_by": int(row[2]) if row[2] is not None else None,
            "note": row[3] if row[3] is not None else None,
            "created_at": row[4].isoformat() if row[4] else None,
            "updated_at": row[5].isoformat() if row[5] else None,
        }
        for row in rows
    ]


def upsert_active_quiz(
    poll_id: str,
    *,
    chat_id: int,
    message_id: int | None,
    correct_option_id: int,
    options: list[str],
    correct_text: str | None = None,
    freeform_option: str | None = None,
    quiz_type: str | None = None,
    word_ru: str | None = None,
) -> None:
    payload_options = [str(option) for option in (options or [])]
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_active_quizzes (
                    poll_id,
                    chat_id,
                    message_id,
                    correct_option_id,
                    correct_text,
                    options,
                    freeform_option,
                    quiz_type,
                    word_ru,
                    created_at
                )
                VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, NOW())
                ON CONFLICT (poll_id) DO UPDATE
                SET
                    chat_id = EXCLUDED.chat_id,
                    message_id = EXCLUDED.message_id,
                    correct_option_id = EXCLUDED.correct_option_id,
                    correct_text = EXCLUDED.correct_text,
                    options = EXCLUDED.options,
                    freeform_option = EXCLUDED.freeform_option,
                    quiz_type = EXCLUDED.quiz_type,
                    word_ru = EXCLUDED.word_ru,
                    created_at = NOW();
                """,
                (
                    str(poll_id),
                    int(chat_id),
                    int(message_id) if message_id is not None else None,
                    int(correct_option_id),
                    (correct_text or None),
                    json.dumps(payload_options, ensure_ascii=False),
                    (freeform_option or None),
                    (quiz_type or None),
                    (word_ru or None),
                ),
            )


def get_active_quiz(poll_id: str) -> dict | None:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    poll_id,
                    chat_id,
                    message_id,
                    correct_option_id,
                    correct_text,
                    options,
                    freeform_option,
                    quiz_type,
                    word_ru,
                    created_at
                FROM bt_3_active_quizzes
                WHERE poll_id = %s
                LIMIT 1;
                """,
                (str(poll_id),),
            )
            row = cursor.fetchone()
    if not row:
        return None
    raw_options = row[5]
    if isinstance(raw_options, str):
        try:
            raw_options = json.loads(raw_options)
        except json.JSONDecodeError:
            raw_options = []
    options = [str(item) for item in (raw_options or [])]
    return {
        "poll_id": str(row[0]),
        "chat_id": int(row[1]),
        "message_id": int(row[2]) if row[2] is not None else None,
        "correct_option_id": int(row[3]),
        "correct_text": row[4] or "",
        "options": options,
        "freeform_option": row[6] or None,
        "quiz_type": row[7] or None,
        "word_ru": row[8] or None,
        "created_at": row[9].isoformat() if row[9] else None,
    }


def delete_active_quiz(poll_id: str) -> bool:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "DELETE FROM bt_3_active_quizzes WHERE poll_id = %s;",
                (str(poll_id),),
            )
            return cursor.rowcount > 0


def store_prepared_telegram_quiz(
    quiz_type: str,
    payload: dict,
    *,
    word_ru: str | None = None,
    source_lang: str = "ru",
    target_lang: str = "de",
) -> int:
    normalized_type = str(quiz_type or "").strip().lower() or "generated"
    payload_value = payload if isinstance(payload, dict) else {}
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_prepared_telegram_quizzes (
                    quiz_type,
                    word_ru,
                    payload,
                    source_lang,
                    target_lang,
                    prepared_at,
                    last_used_at,
                    use_count
                )
                VALUES (%s, %s, %s, %s, %s, NOW(), NULL, 0)
                RETURNING id;
                """,
                (
                    normalized_type,
                    (word_ru or None),
                    Json(payload_value),
                    str(source_lang or "ru").strip().lower() or "ru",
                    str(target_lang or "de").strip().lower() or "de",
                ),
            )
            row = cursor.fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def count_prepared_telegram_quizzes(
    quiz_type: str | None = None,
    *,
    source_lang: str = "ru",
    target_lang: str = "de",
) -> int:
    normalized_type = str(quiz_type or "").strip().lower()
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            if normalized_type:
                cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM bt_3_prepared_telegram_quizzes
                    WHERE source_lang = %s
                      AND target_lang = %s
                      AND quiz_type = %s;
                    """,
                    (
                        str(source_lang or "ru").strip().lower() or "ru",
                        str(target_lang or "de").strip().lower() or "de",
                        normalized_type,
                    ),
                )
            else:
                cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM bt_3_prepared_telegram_quizzes
                    WHERE source_lang = %s
                      AND target_lang = %s;
                    """,
                    (
                        str(source_lang or "ru").strip().lower() or "ru",
                        str(target_lang or "de").strip().lower() or "de",
                    ),
                )
            row = cursor.fetchone()
    return int(row[0] or 0) if row else 0


def claim_prepared_telegram_quiz(
    preferred_quiz_types: list[str] | tuple[str, ...] | None = None,
    *,
    source_lang: str = "ru",
    target_lang: str = "de",
) -> dict | None:
    preferred = [
        str(item or "").strip().lower()
        for item in (preferred_quiz_types or [])
        if str(item or "").strip()
    ]
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                WITH selected AS (
                    SELECT id
                    FROM bt_3_prepared_telegram_quizzes
                    WHERE source_lang = %s
                      AND target_lang = %s
                    ORDER BY
                        COALESCE(array_position(%s::text[], quiz_type), 2147483647),
                        COALESCE(last_used_at, to_timestamp(0)) ASC,
                        prepared_at ASC
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE bt_3_prepared_telegram_quizzes q
                SET
                    last_used_at = NOW(),
                    use_count = q.use_count + 1
                FROM selected
                WHERE q.id = selected.id
                RETURNING q.id, q.quiz_type, q.word_ru, q.payload, q.source_lang, q.target_lang, q.prepared_at, q.last_used_at, q.use_count;
                """,
                (
                    str(source_lang or "ru").strip().lower() or "ru",
                    str(target_lang or "de").strip().lower() or "de",
                    preferred,
                ),
            )
            row = cursor.fetchone()
    if not row:
        return None
    payload = row[3]
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except Exception:
            payload = {}
    return {
        "id": int(row[0]),
        "quiz_type": str(row[1] or ""),
        "word_ru": row[2] or None,
        "payload": payload if isinstance(payload, dict) else {},
        "source_lang": str(row[4] or "ru"),
        "target_lang": str(row[5] or "de"),
        "prepared_at": row[6].isoformat() if row[6] else None,
        "last_used_at": row[7].isoformat() if row[7] else None,
        "use_count": int(row[8] or 0),
    }


def create_image_quiz_template(
    *,
    user_id: int,
    source_dictionary_entry_id: int | None = None,
    canonical_entry_id: int | None = None,
    source_lang: str,
    target_lang: str,
    source_text: str | None = None,
    target_text: str | None = None,
    source_sentence: str | None = None,
    image_prompt: str | None = None,
    question_de: str | None = None,
    answer_options: list[str] | tuple[str, ...] | None = None,
    correct_option_index: int | None = None,
    explanation: str | None = None,
    provider_name: str | None = None,
    provider_meta: dict | None = None,
    image_object_key: str | None = None,
    image_url: str | None = None,
    generation_status: str = "pending",
    visual_status: str = "unknown",
    last_error: str | None = None,
    prepared_at: datetime | None = None,
) -> int:
    safe_options = _normalize_image_quiz_options(answer_options)
    safe_correct_index = int(correct_option_index) if correct_option_index is not None else None
    if safe_correct_index is not None and (safe_correct_index < 0 or safe_correct_index >= len(safe_options)):
        raise ValueError("correct_option_index is out of range for answer_options")
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_image_quiz_templates (
                    user_id,
                    source_dictionary_entry_id,
                    canonical_entry_id,
                    source_lang,
                    target_lang,
                    source_text,
                    target_text,
                    source_sentence,
                    image_prompt,
                    question_de,
                    answer_options,
                    correct_option_index,
                    explanation,
                    provider_name,
                    provider_meta,
                    image_object_key,
                    image_url,
                    generation_status,
                    visual_status,
                    last_error,
                    prepared_at,
                    created_at,
                    updated_at
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s, %s, NOW(), NOW()
                )
                RETURNING id;
                """,
                (
                    int(user_id),
                    int(source_dictionary_entry_id) if source_dictionary_entry_id is not None else None,
                    int(canonical_entry_id) if canonical_entry_id is not None else None,
                    str(source_lang or "").strip().lower(),
                    str(target_lang or "").strip().lower(),
                    str(source_text or "").strip(),
                    str(target_text or "").strip(),
                    str(source_sentence or "").strip(),
                    str(image_prompt or "").strip(),
                    str(question_de or "").strip(),
                    json.dumps(safe_options, ensure_ascii=False),
                    safe_correct_index,
                    str(explanation or "").strip() or None,
                    str(provider_name or "").strip() or None,
                    Json(provider_meta if isinstance(provider_meta, dict) else {}),
                    str(image_object_key or "").strip() or None,
                    str(image_url or "").strip() or None,
                    _normalize_image_quiz_generation_status(generation_status),
                    _normalize_image_quiz_visual_status(visual_status),
                    str(last_error or "").strip() or None,
                    prepared_at,
                ),
            )
            row = cursor.fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def get_image_quiz_template(template_id: int) -> dict | None:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    id,
                    user_id,
                    source_dictionary_entry_id,
                    canonical_entry_id,
                    source_lang,
                    target_lang,
                    source_text,
                    target_text,
                    source_sentence,
                    image_prompt,
                    question_de,
                    answer_options,
                    correct_option_index,
                    explanation,
                    provider_name,
                    provider_meta,
                    image_object_key,
                    image_url,
                    generation_status,
                    visual_status,
                    last_error,
                    prepared_at,
                    last_used_at,
                    use_count,
                    created_at,
                    updated_at
                FROM bt_3_image_quiz_templates
                WHERE id = %s
                LIMIT 1;
                """,
                (int(template_id),),
            )
            return _map_image_quiz_template_row(cursor.fetchone())


def update_image_quiz_template_status(
    template_id: int,
    *,
    generation_status: str | None = None,
    visual_status: str | None = None,
    source_sentence: str | None = None,
    image_object_key: str | None = None,
    image_url: str | None = None,
    provider_name: str | None = None,
    provider_meta: dict | None = None,
    image_prompt: str | None = None,
    question_de: str | None = None,
    answer_options: list[str] | tuple[str, ...] | None = None,
    correct_option_index: int | None = None,
    explanation: str | None = None,
    last_error: str | None = None,
    prepared_at: datetime | None = None,
) -> dict | None:
    safe_generation_status = (
        _normalize_image_quiz_generation_status(generation_status)
        if generation_status is not None
        else None
    )
    safe_visual_status = (
        _normalize_image_quiz_visual_status(visual_status)
        if visual_status is not None
        else None
    )
    safe_options = _normalize_image_quiz_options(answer_options) if answer_options is not None else None
    safe_correct_index = int(correct_option_index) if correct_option_index is not None else None
    if safe_options is not None and safe_correct_index is not None and (
        safe_correct_index < 0 or safe_correct_index >= len(safe_options)
    ):
        raise ValueError("correct_option_index is out of range for answer_options")
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE bt_3_image_quiz_templates
                SET
                    generation_status = COALESCE(%s, generation_status),
                    visual_status = COALESCE(%s, visual_status),
                    source_sentence = COALESCE(%s, source_sentence),
                    image_object_key = COALESCE(%s, image_object_key),
                    image_url = COALESCE(%s, image_url),
                    provider_name = COALESCE(%s, provider_name),
                    provider_meta = CASE
                        WHEN %s IS NULL THEN provider_meta
                        ELSE COALESCE(provider_meta, '{}'::jsonb) || %s::jsonb
                    END,
                    image_prompt = COALESCE(%s, image_prompt),
                    question_de = COALESCE(%s, question_de),
                    answer_options = CASE
                        WHEN %s IS NULL THEN answer_options
                        ELSE %s::jsonb
                    END,
                    correct_option_index = COALESCE(%s, correct_option_index),
                    explanation = COALESCE(%s, explanation),
                    last_error = CASE
                        WHEN %s IS NULL THEN last_error
                        ELSE %s
                    END,
                    prepared_at = CASE
                        WHEN %s IS NOT NULL THEN %s
                        WHEN COALESCE(%s, generation_status) = 'ready' AND prepared_at IS NULL THEN NOW()
                        ELSE prepared_at
                    END,
                    updated_at = NOW()
                WHERE id = %s
                RETURNING
                    id,
                    user_id,
                    source_dictionary_entry_id,
                    canonical_entry_id,
                    source_lang,
                    target_lang,
                    source_text,
                    target_text,
                    source_sentence,
                    image_prompt,
                    question_de,
                    answer_options,
                    correct_option_index,
                    explanation,
                    provider_name,
                    provider_meta,
                    image_object_key,
                    image_url,
                    generation_status,
                    visual_status,
                    last_error,
                    prepared_at,
                    last_used_at,
                    use_count,
                    created_at,
                    updated_at;
                """,
                (
                    safe_generation_status,
                    safe_visual_status,
                    str(source_sentence or "").strip() or None,
                    str(image_object_key or "").strip() or None,
                    str(image_url or "").strip() or None,
                    str(provider_name or "").strip() or None,
                    Json(provider_meta) if isinstance(provider_meta, dict) else None,
                    Json(provider_meta) if isinstance(provider_meta, dict) else None,
                    str(image_prompt or "").strip() or None,
                    str(question_de or "").strip() or None,
                    json.dumps(safe_options, ensure_ascii=False) if safe_options is not None else None,
                    json.dumps(safe_options, ensure_ascii=False) if safe_options is not None else None,
                    safe_correct_index,
                    str(explanation or "").strip() or None,
                    str(last_error or "").strip() if last_error is not None else None,
                    str(last_error or "").strip() if last_error is not None else None,
                    prepared_at,
                    prepared_at,
                    safe_generation_status,
                    int(template_id),
                ),
            )
            return _map_image_quiz_template_row(cursor.fetchone())


def mark_image_quiz_template_ready(
    template_id: int,
    *,
    source_sentence: str | None = None,
    image_object_key: str,
    image_url: str,
    provider_name: str | None = None,
    provider_meta: dict | None = None,
    image_prompt: str | None = None,
    question_de: str | None = None,
    answer_options: list[str] | tuple[str, ...] | None = None,
    correct_option_index: int | None = None,
    explanation: str | None = None,
) -> dict | None:
    return update_image_quiz_template_status(
        int(template_id),
        generation_status="ready",
        visual_status="valid",
        source_sentence=source_sentence,
        image_object_key=image_object_key,
        image_url=image_url,
        provider_name=provider_name,
        provider_meta=provider_meta,
        image_prompt=image_prompt,
        question_de=question_de,
        answer_options=answer_options,
        correct_option_index=correct_option_index,
        explanation=explanation,
        last_error="",
    )


def mark_image_quiz_template_failed(
    template_id: int,
    *,
    last_error: str | None = None,
    visual_status: str | None = None,
    provider_name: str | None = None,
    provider_meta: dict | None = None,
) -> dict | None:
    return update_image_quiz_template_status(
        int(template_id),
        generation_status="failed",
        visual_status=visual_status,
        provider_name=provider_name,
        provider_meta=provider_meta,
        last_error=last_error,
    )


def mark_image_quiz_template_visual_status(
    template_id: int,
    *,
    visual_status: str,
    provider_name: str | None = None,
    provider_meta: dict | None = None,
    last_error: str | None = None,
) -> dict | None:
    return update_image_quiz_template_status(
        int(template_id),
        visual_status=visual_status,
        provider_name=provider_name,
        provider_meta=provider_meta,
        last_error=last_error,
    )


def store_image_quiz_template_blueprint(
    template_id: int,
    *,
    source_sentence: str,
    image_prompt: str,
    question_de: str,
    answer_options: list[str] | tuple[str, ...],
    correct_option_index: int,
    explanation: str | None = None,
    provider_name: str | None = None,
    provider_meta: dict | None = None,
) -> dict | None:
    return update_image_quiz_template_status(
        int(template_id),
        generation_status="blueprint_ready",
        source_sentence=source_sentence,
        image_prompt=image_prompt,
        question_de=question_de,
        answer_options=answer_options,
        correct_option_index=correct_option_index,
        explanation=explanation,
        provider_name=provider_name,
        provider_meta=provider_meta,
        last_error="",
    )


def claim_image_quiz_template_candidate(
    *,
    user_id: int,
    source_lang: str,
    target_lang: str,
) -> dict | None:
    normalized_source_lang = str(source_lang or "").strip().lower() or "ru"
    normalized_target_lang = str(target_lang or "").strip().lower() or "de"
    usage_examples_count_expr = (
        "CASE WHEN jsonb_typeof(q.response_json->'usage_examples') = 'array' "
        "THEN jsonb_array_length(q.response_json->'usage_examples') ELSE 0 END"
    )
    sentence_like_expr = (
        "CASE WHEN "
        "COALESCE(NULLIF(q.response_json->>'source_text', ''), NULLIF(q.word_ru, ''), NULLIF(q.word_de, ''), '') ~ '[.!?]' "
        "OR COALESCE(NULLIF(q.response_json->>'target_text', ''), NULLIF(q.translation_de, ''), NULLIF(q.translation_ru, ''), '') ~ '[.!?]' "
        "THEN 1 ELSE 0 END"
    )
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT
                    q.id,
                    q.user_id,
                    q.canonical_entry_id,
                    q.word_ru,
                    q.translation_de,
                    q.word_de,
                    q.translation_ru,
                    q.source_lang,
                    q.target_lang,
                    q.response_json,
                    q.created_at
                FROM bt_3_webapp_dictionary_queries q
                WHERE q.user_id = %s
                  AND COALESCE(
                        NULLIF(q.source_lang, ''),
                        NULLIF(q.response_json->>'source_lang', ''),
                        NULLIF(q.response_json#>>'{{language_pair,source_lang}}', ''),
                        'ru'
                  ) = %s
                  AND COALESCE(
                        NULLIF(q.target_lang, ''),
                        NULLIF(q.response_json->>'target_lang', ''),
                        NULLIF(q.response_json#>>'{{language_pair,target_lang}}', ''),
                        'de'
                  ) = %s
                  AND q.response_json IS NOT NULL
                  AND NOT EXISTS (
                      SELECT 1
                      FROM bt_3_image_quiz_templates t
                      WHERE t.user_id = q.user_id
                        AND t.source_dictionary_entry_id = q.id
                  )
                ORDER BY
                    CASE
                        WHEN {usage_examples_count_expr} > 0 THEN 0
                        WHEN {sentence_like_expr} = 1 THEN 1
                        ELSE 2
                    END,
                    q.created_at DESC,
                    q.id DESC
                LIMIT 1
                FOR UPDATE SKIP LOCKED;
                """,
                (
                    int(user_id),
                    normalized_source_lang,
                    normalized_target_lang,
                ),
            )
            candidate_row = cursor.fetchone()
            candidate = _map_image_quiz_candidate_row(candidate_row)
            if candidate is None:
                return None
            cursor.execute(
                """
                INSERT INTO bt_3_image_quiz_templates (
                    user_id,
                    source_dictionary_entry_id,
                    canonical_entry_id,
                    source_lang,
                    target_lang,
                    source_text,
                    target_text,
                    generation_status,
                    visual_status,
                    created_at,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, 'pending', 'unknown', NOW(), NOW())
                RETURNING
                    id,
                    user_id,
                    source_dictionary_entry_id,
                    canonical_entry_id,
                    source_lang,
                    target_lang,
                    source_text,
                    target_text,
                    source_sentence,
                    image_prompt,
                    question_de,
                    answer_options,
                    correct_option_index,
                    explanation,
                    provider_name,
                    provider_meta,
                    image_object_key,
                    image_url,
                    generation_status,
                    visual_status,
                    last_error,
                    prepared_at,
                    last_used_at,
                    use_count,
                    created_at,
                    updated_at;
                """,
                (
                    int(user_id),
                    int(candidate["entry_id"]),
                    int(candidate["canonical_entry_id"]) if candidate.get("canonical_entry_id") is not None else None,
                    normalized_source_lang,
                    normalized_target_lang,
                    str(candidate.get("source_text") or "").strip(),
                    str(candidate.get("target_text") or "").strip(),
                ),
            )
            template = _map_image_quiz_template_row(cursor.fetchone())
            if template is None:
                return None
            return {
                "template": template,
                "candidate": candidate,
            }


def claim_next_ready_template(
    user_id: int,
    source_lang: str,
    target_lang: str,
) -> dict | None:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    t.id,
                    t.user_id,
                    t.source_dictionary_entry_id,
                    t.canonical_entry_id,
                    t.source_lang,
                    t.target_lang,
                    t.source_text,
                    t.target_text,
                    t.source_sentence,
                    t.image_prompt,
                    t.question_de,
                    t.answer_options,
                    t.correct_option_index,
                    t.explanation,
                    t.provider_name,
                    t.provider_meta,
                    t.image_object_key,
                    t.image_url,
                    t.generation_status,
                    t.visual_status,
                    t.last_error,
                    t.prepared_at,
                    t.last_used_at,
                    t.use_count,
                    t.created_at,
                    t.updated_at
                FROM bt_3_image_quiz_templates t
                WHERE t.user_id = %s
                  AND t.source_lang = %s
                  AND t.target_lang = %s
                  AND t.generation_status = 'ready'
                  AND t.visual_status = 'valid'
                  AND (t.last_used_at IS NULL OR t.last_used_at <= NOW() - (%s || ' hours')::interval)
                  AND NOT EXISTS (
                      SELECT 1
                      FROM bt_3_image_quiz_dispatches d
                      WHERE d.template_id = t.id
                        AND d.target_user_id = %s
                  )
                ORDER BY
                    COALESCE(t.last_used_at, to_timestamp(0)) ASC,
                    COALESCE(t.prepared_at, t.created_at) ASC,
                    t.id ASC
                LIMIT 1
                FOR UPDATE SKIP LOCKED;
                """,
                (
                    int(user_id),
                    str(source_lang or "").strip().lower(),
                    str(target_lang or "").strip().lower(),
                    str(max(1, int(IMAGE_QUIZ_TEMPLATE_REUSE_COOLDOWN_HOURS or 1))),
                    int(user_id),
                ),
            )
            return _map_image_quiz_template_row(cursor.fetchone())


def count_available_image_quiz_templates(
    *,
    user_id: int,
    source_lang: str,
    target_lang: str,
) -> int:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM bt_3_image_quiz_templates t
                WHERE t.user_id = %s
                  AND t.source_lang = %s
                  AND t.target_lang = %s
                  AND t.generation_status = 'ready'
                  AND t.visual_status = 'valid'
                  AND (t.last_used_at IS NULL OR t.last_used_at <= NOW() - (%s || ' hours')::interval)
                  AND NOT EXISTS (
                      SELECT 1
                      FROM bt_3_image_quiz_dispatches d
                      WHERE d.template_id = t.id
                        AND d.target_user_id = %s
                  );
                """,
                (
                    int(user_id),
                    str(source_lang or "").strip().lower(),
                    str(target_lang or "").strip().lower(),
                    str(max(1, int(IMAGE_QUIZ_TEMPLATE_REUSE_COOLDOWN_HOURS or 1))),
                    int(user_id),
                ),
            )
            row = cursor.fetchone()
            return int(row[0] or 0) if row else 0


def claim_next_ready_image_quiz_template(
    *,
    user_id: int,
    source_lang: str,
    target_lang: str,
) -> dict | None:
    return claim_next_ready_template(
        int(user_id),
        str(source_lang or "").strip().lower() or "ru",
        str(target_lang or "").strip().lower() or "de",
    )


def claim_next_blueprint_ready_template(
    *,
    user_id: int,
    source_lang: str,
    target_lang: str,
    provider_name: str | None = None,
    provider_meta: dict | None = None,
) -> dict | None:
    normalized_provider_name = str(provider_name or "").strip() or None
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                WITH selected AS (
                    SELECT t.id
                    FROM bt_3_image_quiz_templates t
                    WHERE t.user_id = %s
                      AND t.source_lang = %s
                      AND t.target_lang = %s
                      AND t.generation_status = 'blueprint_ready'
                      AND t.visual_status = 'valid'
                      AND COALESCE(NULLIF(t.image_prompt, ''), '') <> ''
                    ORDER BY
                        COALESCE(t.prepared_at, t.created_at) ASC,
                        t.id ASC
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE bt_3_image_quiz_templates t
                SET
                    generation_status = 'rendering',
                    provider_name = COALESCE(%s, t.provider_name),
                    provider_meta = CASE
                        WHEN %s IS NULL THEN t.provider_meta
                        ELSE COALESCE(t.provider_meta, '{}'::jsonb) || %s::jsonb
                    END,
                    last_error = NULL,
                    updated_at = NOW()
                FROM selected
                WHERE t.id = selected.id
                RETURNING
                    t.id,
                    t.user_id,
                    t.source_dictionary_entry_id,
                    t.canonical_entry_id,
                    t.source_lang,
                    t.target_lang,
                    t.source_text,
                    t.target_text,
                    t.source_sentence,
                    t.image_prompt,
                    t.question_de,
                    t.answer_options,
                    t.correct_option_index,
                    t.explanation,
                    t.provider_name,
                    t.provider_meta,
                    t.image_object_key,
                    t.image_url,
                    t.generation_status,
                    t.visual_status,
                    t.last_error,
                    t.prepared_at,
                    t.last_used_at,
                    t.use_count,
                    t.created_at,
                    t.updated_at;
                """,
                (
                    int(user_id),
                    str(source_lang or "").strip().lower(),
                    str(target_lang or "").strip().lower(),
                    normalized_provider_name,
                    Json(provider_meta) if isinstance(provider_meta, dict) else None,
                    Json(provider_meta) if isinstance(provider_meta, dict) else None,
                ),
            )
            return _map_image_quiz_template_row(cursor.fetchone())


def fail_stale_rendering_image_quiz_templates(
    *,
    user_id: int,
    source_lang: str,
    target_lang: str,
    stale_after_minutes: int,
    provider_name: str | None = None,
    provider_meta: dict | None = None,
    limit: int = 25,
) -> list[dict]:
    normalized_provider_name = str(provider_name or "").strip() or None
    safe_limit = max(1, min(int(limit or 25), 500))
    safe_stale_after_minutes = max(5, int(stale_after_minutes or 30))
    failure_reason = f"stale_rendering_timeout:{safe_stale_after_minutes}m"
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                WITH stale_templates AS (
                    SELECT t.id
                    FROM bt_3_image_quiz_templates t
                    WHERE t.user_id = %s
                      AND t.source_lang = %s
                      AND t.target_lang = %s
                      AND t.generation_status = 'rendering'
                      AND t.updated_at <= NOW() - (%s || ' minutes')::interval
                    ORDER BY t.updated_at ASC, t.id ASC
                    LIMIT %s
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE bt_3_image_quiz_templates t
                SET
                    generation_status = 'failed',
                    provider_name = COALESCE(%s, t.provider_name),
                    provider_meta = CASE
                        WHEN %s IS NULL THEN COALESCE(t.provider_meta, '{}'::jsonb)
                        ELSE COALESCE(t.provider_meta, '{}'::jsonb) || %s::jsonb
                    END,
                    last_error = %s,
                    updated_at = NOW()
                FROM stale_templates
                WHERE t.id = stale_templates.id
                RETURNING
                    t.id,
                    t.user_id,
                    t.source_dictionary_entry_id,
                    t.canonical_entry_id,
                    t.source_lang,
                    t.target_lang,
                    t.source_text,
                    t.target_text,
                    t.source_sentence,
                    t.image_prompt,
                    t.question_de,
                    t.answer_options,
                    t.correct_option_index,
                    t.explanation,
                    t.provider_name,
                    t.provider_meta,
                    t.image_object_key,
                    t.image_url,
                    t.generation_status,
                    t.visual_status,
                    t.last_error,
                    t.prepared_at,
                    t.last_used_at,
                    t.use_count,
                    t.created_at,
                    t.updated_at;
                """,
                (
                    int(user_id),
                    str(source_lang or "").strip().lower() or "ru",
                    str(target_lang or "").strip().lower() or "de",
                    str(safe_stale_after_minutes),
                    safe_limit,
                    normalized_provider_name,
                    Json(provider_meta) if isinstance(provider_meta, dict) else None,
                    Json(provider_meta) if isinstance(provider_meta, dict) else None,
                    failure_reason,
                ),
            )
            return [_map_image_quiz_template_row(row) for row in cursor.fetchall() or [] if row]


def create_image_quiz_dispatch(
    *,
    template_id: int,
    target_user_id: int,
    chat_id: int,
    message_id: int | None = None,
    delivery_scope: str = "private",
    delivery_slot: str | None = None,
    delivery_date_local: date | None = None,
    status: str = "claimed",
    sent_at: datetime | None = None,
) -> dict | None:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_image_quiz_dispatches (
                    template_id,
                    target_user_id,
                    chat_id,
                    message_id,
                    delivery_scope,
                    delivery_slot,
                    delivery_date_local,
                    status,
                    created_at,
                    sent_at,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW(), %s, NOW())
                RETURNING
                    id,
                    template_id,
                    target_user_id,
                    chat_id,
                    message_id,
                    delivery_scope,
                    delivery_slot,
                    delivery_date_local,
                    status,
                    created_at,
                    sent_at,
                    updated_at;
                """,
                (
                    int(template_id),
                    int(target_user_id),
                    int(chat_id),
                    int(message_id) if message_id is not None else None,
                    _normalize_image_quiz_delivery_scope(delivery_scope),
                    str(delivery_slot or "").strip() or None,
                    delivery_date_local,
                    _normalize_image_quiz_dispatch_status(status),
                    sent_at,
                ),
            )
            return _map_image_quiz_dispatch_row(cursor.fetchone())


def mark_image_quiz_dispatch_sent(
    dispatch_id: int,
    *,
    message_id: int,
    sent_at: datetime | None = None,
) -> dict | None:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                WITH updated_dispatch AS (
                    UPDATE bt_3_image_quiz_dispatches
                    SET
                        message_id = %s,
                        status = 'sent',
                        sent_at = COALESCE(%s, NOW()),
                        updated_at = NOW()
                    WHERE id = %s
                    RETURNING
                        id,
                        template_id,
                        target_user_id,
                        chat_id,
                        message_id,
                        delivery_scope,
                        delivery_slot,
                        delivery_date_local,
                        status,
                        created_at,
                        sent_at,
                        updated_at
                )
                UPDATE bt_3_image_quiz_templates t
                SET
                    last_used_at = COALESCE((SELECT sent_at FROM updated_dispatch), NOW()),
                    use_count = t.use_count + 1,
                    updated_at = NOW()
                FROM updated_dispatch
                WHERE t.id = updated_dispatch.template_id;
                """,
                (
                    int(message_id),
                    sent_at,
                    int(dispatch_id),
                ),
            )
            cursor.execute(
                """
                SELECT
                    id,
                    template_id,
                    target_user_id,
                    chat_id,
                    message_id,
                    delivery_scope,
                    delivery_slot,
                    delivery_date_local,
                    status,
                    created_at,
                    sent_at,
                    updated_at
                FROM bt_3_image_quiz_dispatches
                WHERE id = %s
                LIMIT 1;
                """,
                (int(dispatch_id),),
            )
            return _map_image_quiz_dispatch_row(cursor.fetchone())


def mark_image_quiz_dispatch_failed(dispatch_id: int) -> dict | None:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE bt_3_image_quiz_dispatches
                SET
                    status = 'failed',
                    updated_at = NOW()
                WHERE id = %s
                RETURNING
                    id,
                    template_id,
                    target_user_id,
                    chat_id,
                    message_id,
                    delivery_scope,
                    delivery_slot,
                    delivery_date_local,
                    status,
                    created_at,
                    sent_at,
                    updated_at;
                """,
                (int(dispatch_id),),
            )
            return _map_image_quiz_dispatch_row(cursor.fetchone())


def get_image_quiz_dispatch(dispatch_id: int) -> dict | None:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    id,
                    template_id,
                    target_user_id,
                    chat_id,
                    message_id,
                    delivery_scope,
                    delivery_slot,
                    delivery_date_local,
                    status,
                    created_at,
                    sent_at,
                    updated_at
                FROM bt_3_image_quiz_dispatches
                WHERE id = %s
                LIMIT 1;
                """,
                (int(dispatch_id),),
            )
            return _map_image_quiz_dispatch_row(cursor.fetchone())


def record_image_quiz_answer(
    *,
    dispatch_id: int,
    user_id: int,
    selected_option_index: int,
    selected_text: str | None = None,
    is_correct: bool,
) -> dict | None:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_image_quiz_answers (
                    dispatch_id,
                    user_id,
                    selected_option_index,
                    selected_text,
                    is_correct,
                    answered_at
                )
                VALUES (%s, %s, %s, %s, %s, NOW())
                ON CONFLICT (dispatch_id, user_id) DO NOTHING
                RETURNING
                    id,
                    dispatch_id,
                    user_id,
                    selected_option_index,
                    selected_text,
                    is_correct,
                    answered_at,
                    feedback_sent_at;
                """,
                (
                    int(dispatch_id),
                    int(user_id),
                    int(selected_option_index),
                    str(selected_text or "").strip() or None,
                    bool(is_correct),
                ),
            )
            row = cursor.fetchone()
            if row:
                answer = _map_image_quiz_answer_row(row)
                if answer is not None:
                    answer["created"] = True
                return answer
            cursor.execute(
                """
                SELECT
                    id,
                    dispatch_id,
                    user_id,
                    selected_option_index,
                    selected_text,
                    is_correct,
                    answered_at,
                    feedback_sent_at
                FROM bt_3_image_quiz_answers
                WHERE dispatch_id = %s
                  AND user_id = %s
                LIMIT 1;
                """,
                (int(dispatch_id), int(user_id)),
            )
            answer = _map_image_quiz_answer_row(cursor.fetchone())
            if answer is not None:
                answer["created"] = False
            return answer


def mark_image_quiz_answer_feedback_sent(dispatch_id: int, user_id: int) -> dict | None:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE bt_3_image_quiz_answers
                SET
                    feedback_sent_at = COALESCE(feedback_sent_at, NOW())
                WHERE dispatch_id = %s
                  AND user_id = %s
                RETURNING
                    id,
                    dispatch_id,
                    user_id,
                    selected_option_index,
                    selected_text,
                    is_correct,
                    answered_at,
                    feedback_sent_at;
                """,
                (int(dispatch_id), int(user_id)),
            )
            return _map_image_quiz_answer_row(cursor.fetchone())


def list_skills(category: str | None = None, language_code: str | None = None) -> list[dict]:
    lang = (language_code or "").strip().lower()
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            if category:
                if lang:
                    cursor.execute(
                        """
                        SELECT skill_id, title, category, is_active, language_code
                        FROM bt_3_skills
                        WHERE category = %s AND language_code = %s
                        ORDER BY skill_id;
                        """,
                        (category, lang),
                    )
                else:
                    cursor.execute(
                        """
                        SELECT skill_id, title, category, is_active, language_code
                        FROM bt_3_skills
                        WHERE category = %s
                        ORDER BY skill_id;
                        """,
                        (category,),
                    )
            else:
                if lang:
                    cursor.execute(
                        """
                        SELECT skill_id, title, category, is_active, language_code
                        FROM bt_3_skills
                        WHERE language_code = %s
                        ORDER BY category, skill_id;
                        """,
                        (lang,),
                    )
                else:
                    cursor.execute(
                        """
                        SELECT skill_id, title, category, is_active, language_code
                        FROM bt_3_skills
                        ORDER BY language_code, category, skill_id;
                        """
                    )
            rows = cursor.fetchall()
    return [
        {
            "skill_id": row[0],
            "title": row[1],
            "category": row[2],
            "is_active": bool(row[3]),
            "language_code": row[4] or "de",
        }
        for row in rows
    ]


def get_skill_by_id(skill_id: str, language_code: str | None = None) -> dict | None:
    normalized = str(skill_id or "").strip()
    if not normalized:
        return None
    lang = (language_code or "").strip().lower()
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            if lang:
                cursor.execute(
                    """
                    SELECT skill_id, title, category, is_active, language_code
                    FROM bt_3_skills
                    WHERE skill_id = %s AND language_code = %s
                    LIMIT 1;
                    """,
                    (normalized, lang),
                )
            else:
                cursor.execute(
                    """
                    SELECT skill_id, title, category, is_active, language_code
                    FROM bt_3_skills
                    WHERE skill_id = %s
                    LIMIT 1;
                    """,
                    (normalized,),
                )
            row = cursor.fetchone()
    if not row:
        return None
    return {
        "skill_id": row[0],
        "title": row[1],
        "category": row[2],
        "is_active": bool(row[3]),
        "language_code": row[4] or "de",
    }


def get_skill_mapping_for_error(
    error_category: str,
    error_subcategory: str | None,
    language_code: str | None = None,
) -> list[dict]:
    category = str(error_category or "").strip()
    subcategory = str(error_subcategory or "").strip()
    lang = (language_code or "de").strip().lower() or "de"
    if not category:
        return []
    if _is_unclassified_error(category, subcategory):
        return []

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT skill_id, weight
                FROM bt_3_error_skill_map
                WHERE language_code = %s
                  AND error_category = %s
                  AND error_subcategory = %s
                ORDER BY weight DESC, skill_id ASC;
                """,
                (lang, category, subcategory),
            )
            rows = cursor.fetchall()
            if rows:
                return [{"skill_id": row[0], "weight": float(row[1] or 1.0)} for row in rows]

            cursor.execute(
                """
                SELECT skill_id, weight
                FROM bt_3_error_skill_map
                WHERE language_code = %s
                  AND error_category = %s
                  AND LOWER(COALESCE(error_subcategory, '')) NOT IN ('unclassified mistake', 'unclassified mistakes')
                ORDER BY weight DESC, skill_id ASC
                LIMIT 3;
                """,
                (lang, category),
            )
            fallback_rows = cursor.fetchall()
            if fallback_rows:
                return [{"skill_id": row[0], "weight": float(row[1] or 1.0)} for row in fallback_rows]

    return []


def _clamp_mastery(value: float) -> float:
    return max(0.0, min(100.0, float(value)))


def apply_user_skill_event(
    *,
    user_id: int,
    skill_id: str,
    source_lang: str = "ru",
    target_lang: str = "de",
    event_type: str,
    base_delta: float,
    event_at: datetime | None = None,
) -> dict:
    normalized_event = str(event_type or "").strip().lower()
    if normalized_event not in {"success", "fail"}:
        raise ValueError("event_type must be success or fail")
    event_at = event_at or datetime.now(timezone.utc)
    if event_at.tzinfo is None:
        event_at = event_at.replace(tzinfo=timezone.utc)

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT mastery, success_streak, fail_streak, total_events, last_practiced_at
                FROM bt_3_user_skill_state
                WHERE user_id = %s
                  AND skill_id = %s
                  AND source_lang = %s
                  AND target_lang = %s
                LIMIT 1;
                """,
                (int(user_id), str(skill_id), source_lang or "ru", target_lang or "de"),
            )
            row = cursor.fetchone()

            mastery = float(row[0]) if row else 50.0
            success_streak = int(row[1]) if row else 0
            fail_streak = int(row[2]) if row else 0
            total_events = int(row[3]) if row else 0
            last_practiced_at = row[4] if row else None

            # Light decay if user did not practice this skill for a while.
            if isinstance(last_practiced_at, datetime):
                last_ts = last_practiced_at if last_practiced_at.tzinfo else last_practiced_at.replace(tzinfo=timezone.utc)
                days_idle = max(0, (event_at.date() - last_ts.date()).days)
                decay = min(8.0, days_idle * 0.15)
                mastery -= decay

            if normalized_event == "success":
                accel = min(success_streak, 5) * 0.2
                effective_delta = max(0.0, float(base_delta)) + accel
                success_streak += 1
                fail_streak = 0
            else:
                accel = min(fail_streak, 5) * 0.3
                effective_delta = min(0.0, float(base_delta)) - accel
                fail_streak += 1
                success_streak = 0

            mastery = _clamp_mastery(mastery + effective_delta)
            total_events += 1

            cursor.execute(
                """
                INSERT INTO bt_3_user_skill_state (
                    user_id,
                    skill_id,
                    source_lang,
                    target_lang,
                    mastery,
                    success_streak,
                    fail_streak,
                    total_events,
                    last_event_delta,
                    last_event_at,
                    last_practiced_at,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (user_id, skill_id, source_lang, target_lang) DO UPDATE
                SET
                    mastery = EXCLUDED.mastery,
                    success_streak = EXCLUDED.success_streak,
                    fail_streak = EXCLUDED.fail_streak,
                    total_events = EXCLUDED.total_events,
                    last_event_delta = EXCLUDED.last_event_delta,
                    last_event_at = EXCLUDED.last_event_at,
                    last_practiced_at = EXCLUDED.last_practiced_at,
                    updated_at = NOW()
                RETURNING mastery, success_streak, fail_streak, total_events, last_event_delta, last_practiced_at;
                """,
                (
                    int(user_id),
                    str(skill_id),
                    source_lang or "ru",
                    target_lang or "de",
                    mastery,
                    success_streak,
                    fail_streak,
                    total_events,
                    float(effective_delta),
                    event_at,
                    event_at,
                ),
            )
            saved = cursor.fetchone()

    return {
        "user_id": int(user_id),
        "skill_id": str(skill_id),
        "source_lang": source_lang or "ru",
        "target_lang": target_lang or "de",
        "mastery": float(saved[0] if saved else mastery),
        "success_streak": int(saved[1] if saved else success_streak),
        "fail_streak": int(saved[2] if saved else fail_streak),
        "total_events": int(saved[3] if saved else total_events),
        "last_event_delta": float(saved[4] if saved else 0.0),
        "last_practiced_at": saved[5].isoformat() if saved and saved[5] else None,
    }


def apply_skill_events_for_error(
    *,
    user_id: int,
    source_lang: str = "ru",
    target_lang: str = "de",
    error_category: str,
    error_subcategory: str | None,
    event_type: str,
    success_delta: float = 2.0,
    fail_delta: float = -3.0,
    event_at: datetime | None = None,
) -> list[dict]:
    mapping = get_skill_mapping_for_error(
        error_category,
        error_subcategory,
        language_code=target_lang or "de",
    )
    base = float(success_delta if str(event_type).lower() == "success" else fail_delta)
    results: list[dict] = []
    for item in mapping:
        skill_id = str(item.get("skill_id") or "").strip()
        weight = float(item.get("weight") or 1.0)
        if not skill_id:
            continue
        try:
            result = apply_user_skill_event(
                user_id=int(user_id),
                skill_id=skill_id,
                source_lang=source_lang or "ru",
                target_lang=target_lang or "de",
                event_type=event_type,
                base_delta=base * weight,
                event_at=event_at,
            )
            results.append(result)
        except Exception:
            continue
    return results


def get_skill_progress_report(
    *,
    user_id: int,
    lookback_days: int = 7,
    source_lang: str | None = None,
    target_lang: str | None = None,
) -> dict:
    window_days = max(1, min(int(lookback_days), 30))
    now_utc = datetime.now(timezone.utc)
    normalized_source_lang = str(source_lang or "ru").strip().lower() or "ru"
    normalized_target_lang = str(target_lang or "de").strip().lower() or "de"
    reset_date = get_user_progress_reset_date(
        user_id=int(user_id),
        source_lang=normalized_source_lang,
        target_lang=normalized_target_lang,
    )

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                WITH err_7d AS (
                    SELECT
                        m.skill_id,
                        SUM(COALESCE(dm.mistake_count, 1))::BIGINT AS errors_7d
                    FROM bt_3_detailed_mistakes dm
                    JOIN bt_3_daily_sentences ds
                      ON ds.id_for_mistake_table = dm.sentence_id
                     AND ds.user_id = dm.user_id
                    JOIN bt_3_error_skill_map m
                      ON m.error_category = COALESCE(NULLIF(dm.main_category, ''), 'Other mistake')
                     AND m.error_subcategory = COALESCE(NULLIF(dm.sub_category, ''), 'Unclassified mistake')
                    WHERE dm.user_id = %s
                      AND m.language_code = %s
                      AND COALESCE(ds.source_lang, 'ru') = COALESCE(%s, 'ru')
                      AND COALESCE(ds.target_lang, 'de') = COALESCE(%s, 'de')
                      AND LOWER(COALESCE(NULLIF(dm.sub_category, ''), 'Unclassified mistake')) NOT IN ('unclassified mistake', 'unclassified mistakes')
                      AND COALESCE(dm.last_seen, dm.added_data, NOW()) >= NOW() - (%s::text || ' days')::interval
                      AND (%s::date IS NULL OR COALESCE(dm.last_seen, dm.added_data, NOW())::date >= %s::date)
                    GROUP BY m.skill_id
                ),
                err_prev_7d AS (
                    SELECT
                        m.skill_id,
                        SUM(COALESCE(dm.mistake_count, 1))::BIGINT AS errors_prev_7d
                    FROM bt_3_detailed_mistakes dm
                    JOIN bt_3_daily_sentences ds
                      ON ds.id_for_mistake_table = dm.sentence_id
                     AND ds.user_id = dm.user_id
                    JOIN bt_3_error_skill_map m
                      ON m.error_category = COALESCE(NULLIF(dm.main_category, ''), 'Other mistake')
                     AND m.error_subcategory = COALESCE(NULLIF(dm.sub_category, ''), 'Unclassified mistake')
                    WHERE dm.user_id = %s
                      AND m.language_code = %s
                      AND COALESCE(ds.source_lang, 'ru') = COALESCE(%s, 'ru')
                      AND COALESCE(ds.target_lang, 'de') = COALESCE(%s, 'de')
                      AND LOWER(COALESCE(NULLIF(dm.sub_category, ''), 'Unclassified mistake')) NOT IN ('unclassified mistake', 'unclassified mistakes')
                      AND COALESCE(dm.last_seen, dm.added_data, NOW()) < NOW() - (%s::text || ' days')::interval
                      AND COALESCE(dm.last_seen, dm.added_data, NOW()) >= NOW() - ((%s * 2)::text || ' days')::interval
                      AND (%s::date IS NULL OR COALESCE(dm.last_seen, dm.added_data, NOW())::date >= %s::date)
                    GROUP BY m.skill_id
                )
                SELECT
                    k.skill_id,
                    k.title,
                    k.category,
                    COALESCE(e.errors_7d, 0) AS errors_7d,
                    COALESCE(p.errors_prev_7d, 0) AS errors_prev_7d
                FROM bt_3_skills k
                LEFT JOIN err_7d e ON e.skill_id = k.skill_id
                LEFT JOIN err_prev_7d p ON p.skill_id = k.skill_id
                WHERE k.is_active = TRUE
                  AND k.language_code = %s
                  AND LOWER(COALESCE(k.skill_id, '')) NOT IN ('other_unclassified', 'en_other_unclassified', 'es_other_unclassified', 'it_other_unclassified')
                  AND LOWER(COALESCE(k.skill_id, '')) NOT LIKE '%%unclassified%%'
                  AND LOWER(COALESCE(k.title, '')) NOT LIKE '%%unclassified%%'
                ORDER BY k.category ASC, k.title ASC, k.skill_id ASC;
                """,
                (
                    int(user_id),
                    normalized_target_lang,
                    normalized_source_lang,
                    normalized_target_lang,
                    window_days,
                    reset_date,
                    reset_date,
                    int(user_id),
                    normalized_target_lang,
                    normalized_source_lang,
                    normalized_target_lang,
                    window_days,
                    window_days,
                    reset_date,
                    reset_date,
                    normalized_target_lang,
                ),
            )
            rows = cursor.fetchall()
            state_map: dict[str, dict[str, Any]] = {}
            if reset_date is None:
                cursor.execute(
                    """
                    SELECT skill_id, mastery, total_events, last_practiced_at
                    FROM bt_3_user_skill_state
                    WHERE user_id = %s
                      AND source_lang = %s
                      AND target_lang = %s;
                    """,
                    (int(user_id), normalized_source_lang, normalized_target_lang),
                )
                state_rows = cursor.fetchall() or []
                for state_row in state_rows:
                    skill_id = str(state_row[0] or "").strip()
                    if not skill_id:
                        continue
                    state_map[skill_id] = {
                        "mastery": float(state_row[1]) if state_row[1] is not None else None,
                        "total_events": int(state_row[2] or 0),
                        "last_practiced_at": state_row[3].isoformat() if hasattr(state_row[3], "isoformat") else None,
                    }
            else:
                state_map = _get_skill_state_v2_snapshot_since_date(
                    user_id=int(user_id),
                    source_lang=normalized_source_lang,
                    target_lang=normalized_target_lang,
                    reset_date=reset_date,
                )

    skills: list[dict] = []
    groups_map: dict[str, list[dict]] = {}
    for row in rows:
        if not isinstance(row, (tuple, list)):
            continue
        row_values = list(row)
        if len(row_values) < 5:
            row_values.extend([None] * (5 - len(row_values)))
        skill_id_raw = row_values[0]
        title_raw = row_values[1]
        category_raw = row_values[2]
        errors_7d = int(row_values[3] or 0)
        errors_prev_7d = int(row_values[4] or 0)
        state = state_map.get(str(skill_id_raw or "").strip()) or {}
        mastery_raw = state.get("mastery")
        total_events = int(state.get("total_events") or 0)
        last_practiced_raw = state.get("last_practiced_at")
        has_data = total_events > 0 and mastery_raw is not None

        mastery: float | None = None
        if has_data:
            mastery = float(mastery_raw)

        if not has_data:
            trend = "none"
            zone = "unknown"
        else:
            if errors_7d < errors_prev_7d:
                trend = "up"
            elif errors_7d > errors_prev_7d:
                trend = "down"
            else:
                trend = "flat"
            if mastery < 40:
                zone = "weak"
            elif mastery < 70:
                zone = "growing"
            elif mastery < 90:
                zone = "confident"
            else:
                zone = "stable"

        skill = {
            "skill_id": str(skill_id_raw or ""),
            "name": str(title_raw or skill_id_raw or ""),
            "group": str(category_raw or "Other"),
            "mastery": round(mastery, 2) if mastery is not None else None,
            "errors_7d": errors_7d,
            "errors_prev_7d": errors_prev_7d,
            "trend": trend,
            "zone": zone,
            "confidence": round(min(1.0, total_events / 20.0), 3) if has_data else 0.0,
            "has_data": has_data,
            "total_events": total_events,
            "last_practiced_at": (
                last_practiced_raw.isoformat()
                if hasattr(last_practiced_raw, "isoformat")
                else (str(last_practiced_raw) if last_practiced_raw else None)
            ),
        }
        skills.append(skill)
        group_name = skill["group"]
        groups_map.setdefault(group_name, []).append(skill)

    skills_with_data = [item for item in skills if bool(item.get("has_data"))]
    top_weak = sorted(
        skills_with_data,
        key=lambda item: (
            float(item.get("mastery") or 0.0),
            -int(item.get("errors_7d") or 0),
            str(item.get("skill_id") or ""),
        ),
    )[:5]
    groups = [
        {"group": group_name, "skills": groups_map[group_name]}
        for group_name in sorted(groups_map.keys())
    ]
    return {
        "updated_at": now_utc.isoformat(),
        "period_days": window_days,
        "top_weak": top_weak,
        "groups": groups,
        "total_skills": len(skills),
        "skills_with_data": len(skills_with_data),
    }


def _get_latest_session_id(cursor, user_id: int) -> str | None:
    cursor.execute(
        """
        SELECT session_id
        FROM bt_3_user_progress
        WHERE user_id = %s AND completed = FALSE
        ORDER BY start_time DESC
        LIMIT 1;
        """,
        (user_id,),
    )
    row = cursor.fetchone()
    return row[0] if row else None


def get_latest_daily_sentences(user_id: int, limit: int = 7) -> list[dict]:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            latest_session_id = _get_latest_session_id(cursor, user_id)
            if not latest_session_id:
                return []

            cursor.execute("""
                SELECT id_for_mistake_table, sentence, unique_id
                FROM bt_3_daily_sentences
                WHERE user_id = %s AND session_id = %s
                ORDER BY unique_id ASC
                LIMIT %s;
            """, (user_id, latest_session_id, limit))
            rows = cursor.fetchall()
            return [
                {
                    "id_for_mistake_table": row[0],
                    "sentence": row[1],
                    "unique_id": row[2],
                    "source_session_id": str(latest_session_id),
                }
                for row in rows
            ]


def close_stale_open_translation_sessions_for_user(
    *,
    user_id: int,
    source_lang: str = "ru",
    target_lang: str = "de",
) -> int:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE bt_3_user_progress up
                SET
                    active_seconds = COALESCE(up.active_seconds, 0)
                        + CASE
                            WHEN COALESCE(up.active_running, FALSE) = TRUE
                             AND up.active_started_at IS NOT NULL
                                THEN GREATEST(
                                    0,
                                    EXTRACT(EPOCH FROM (NOW() - up.active_started_at))::BIGINT
                                )
                            ELSE 0
                        END,
                    active_started_at = NULL,
                    active_running = FALSE,
                    end_time = NOW(),
                    completed = TRUE
                WHERE up.user_id = %s
                  AND up.completed = FALSE
                  AND EXISTS (
                    SELECT 1
                    FROM bt_3_daily_sentences ds
                    WHERE ds.user_id = up.user_id
                      AND ds.session_id = up.session_id
                      AND COALESCE(ds.source_lang, 'ru') = %s
                      AND COALESCE(ds.target_lang, 'de') = %s
                      AND ds.date < CURRENT_DATE
                  )
                  AND NOT EXISTS (
                    SELECT 1
                    FROM bt_3_daily_sentences ds
                    WHERE ds.user_id = up.user_id
                      AND ds.session_id = up.session_id
                      AND COALESCE(ds.source_lang, 'ru') = %s
                      AND COALESCE(ds.target_lang, 'de') = %s
                      AND ds.date >= CURRENT_DATE
                  );
                """,
                (int(user_id), source_lang, target_lang, source_lang, target_lang),
            )
            return int(cursor.rowcount or 0)


def get_pending_daily_sentences(
    user_id: int,
    limit: int = 7,
    source_lang: str = "ru",
    target_lang: str = "de",
    close_stale_sessions: bool = True,
    session_id: str | int | None = None,
) -> list[dict]:
    try:
        safe_limit = int(limit or 7)
    except Exception:
        safe_limit = 7
    safe_limit = max(1, min(safe_limit, 7))
    if close_stale_sessions:
        close_stale_open_translation_sessions_for_user(
            user_id=int(user_id),
            source_lang=source_lang,
            target_lang=target_lang,
        )
    normalized_session_id = str(session_id or "").strip() or None
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            latest_session_id = normalized_session_id
            if latest_session_id:
                cursor.execute(
                    """
                    SELECT ds.id_for_mistake_table, ds.sentence, ds.unique_id
                    FROM bt_3_daily_sentences ds
                    LEFT JOIN bt_3_translations tr
                        ON tr.user_id = ds.user_id
                        AND tr.sentence_id = ds.id
                        AND tr.session_id = ds.session_id
                        AND COALESCE(tr.source_lang, 'ru') = %s
                        AND COALESCE(tr.target_lang, 'de') = %s
                    WHERE ds.user_id = %s
                      AND ds.session_id = %s
                      AND ds.date = CURRENT_DATE
                      AND COALESCE(ds.source_lang, 'ru') = %s
                      AND COALESCE(ds.target_lang, 'de') = %s
                      AND tr.id IS NULL
                    ORDER BY ds.unique_id ASC
                    LIMIT %s;
                    """,
                    (
                        source_lang,
                        target_lang,
                        int(user_id),
                        latest_session_id,
                        source_lang,
                        target_lang,
                        safe_limit,
                    ),
                )
                rows = cursor.fetchall()
                return [
                    {
                        "id_for_mistake_table": row[0],
                        "sentence": row[1],
                        "unique_id": row[2],
                        "source_session_id": str(latest_session_id),
                    }
                    for row in rows
                ]

            return []


def mark_translation_sentences_shown(
    *,
    user_id: int,
    source_session_id: str | int,
    sentence_ids: list[int] | tuple[int, ...] | set[int],
    source_lang: str = "ru",
    target_lang: str = "de",
) -> dict[str, int | str]:
    normalized_session_id = str(source_session_id or "").strip()
    if not normalized_session_id:
        return {"session_id": "", "updated": 0}
    normalized_sentence_ids = sorted(
        {
            int(item)
            for item in list(sentence_ids or [])
            if item is not None and str(item).strip() != "" and int(item) > 0
        }
    )
    if not normalized_sentence_ids:
        return {"session_id": normalized_session_id, "updated": 0}

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE bt_3_daily_sentences
                SET
                    shown_to_user = TRUE,
                    shown_to_user_at = COALESCE(shown_to_user_at, NOW())
                WHERE user_id = %s
                  AND session_id = %s
                  AND id_for_mistake_table = ANY(%s)
                  AND COALESCE(source_lang, 'ru') = %s
                  AND COALESCE(target_lang, 'de') = %s
                  AND shown_to_user = FALSE;
                """,
                (
                    int(user_id),
                    normalized_session_id,
                    normalized_sentence_ids,
                    source_lang,
                    target_lang,
                ),
            )
            updated = int(cursor.rowcount or 0)

    return {"session_id": normalized_session_id, "updated": updated}

# --- Новые функции для ассистента по продажам ---

async def get_client_by_identifier(identifier: str) -> dict | None:
    """
    Ищет клиента по system_id или номеру телефона.
    Возвращает словарь с данными клиента или None, если клиент не найден.
    """
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT id, first_name, last_name, system_id, phone_number, email, location, manager_contact, is_existing_client
                FROM clients
                WHERE system_id = %s OR phone_number = %s;
            """, (identifier, identifier)) # Поиск по обоим полям
            result = cursor.fetchone()
            if result:
                return {
                    "id": result[0],
                    "first_name": result[1],
                    "last_name": result[2],
                    "system_id": result[3],
                    "phone_number": result[4],
                    "email": result[5],
                    "location": result[6],
                    "manager_contact": result[7],
                    "is_existing_client": result[8]
                }
            return None

async def create_client(
    first_name: str,
    phone_number: str,
    last_name: str = None,
    system_id: str = None,
    email: str = None,
    location: str = None,
    manager_contact: str = None,
    is_existing_client: bool = False
) -> dict:
    """
    Создает новую запись клиента в базе данных.
    Возвращает словарь с данными нового клиента.
    """
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            # Используем ON CONFLICT для обновления, если клиент с таким system_id или phone_number уже существует
            # Это позволяет избежать дубликатов и обновить информацию, если она уже есть
            cursor.execute("""
                INSERT INTO clients (first_name, last_name, system_id, phone_number, email, location, manager_contact, is_existing_client)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (phone_number) DO UPDATE SET -- Конфликт по номеру телефона
                    first_name = EXCLUDED.first_name,
                    last_name = COALESCE(EXCLUDED.last_name, clients.last_name), -- Обновляем, только если новое значение не NULL
                    system_id = COALESCE(EXCLUDED.system_id, clients.system_id),
                    email = COALESCE(EXCLUDED.email, clients.email),
                    location = COALESCE(EXCLUDED.location, clients.location),
                    manager_contact = COALESCE(EXCLUDED.manager_contact, clients.manager_contact),
                    is_existing_client = EXCLUDED.is_existing_client
                RETURNING id, first_name, last_name, system_id, phone_number, email, location, manager_contact, is_existing_client;
            """, (first_name, last_name, system_id, phone_number, email, location, manager_contact, is_existing_client))
            
            result = cursor.fetchone()
            if result:
                return {
                    "id": result[0],
                    "first_name": result[1],
                    "last_name": result[2],
                    "system_id": result[3],
                    "phone_number": result[4],
                    "email": result[5],
                    "location": result[6],
                    "manager_contact": result[7],
                    "is_existing_client": result[8]
                }
            raise RuntimeError("Не удалось создать или обновить клиента")


async def get_new_products() -> list[dict]:
    """
    Возвращает список всех продуктов, помеченных как новинки.
    """
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT id, name, description, price
                FROM products
                WHERE is_new = TRUE;
            """)
            return [{
                "id": row[0],
                "name": row[1],
                "description": row[2],
                "price": float(row[3]) # Преобразуем Decimal в float для удобства
            } for row in cursor.fetchall()]

async def get_product_by_name(product_name: str) -> dict | None:
    """
    Ищет продукт по его названию (регистронезависимо).
    Возвращает словарь с данными продукта или None.
    """
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT id, name, description, price, available_quantity
                FROM products
                WHERE LOWER(name) = LOWER(%s);
            """, (product_name,))
            result = cursor.fetchone()
            if result:
                return {
                    "id": result[0],
                    "name": result[1],
                    "description": result[2],
                    "price": float(result[3]),
                    "available_quantity": result[4]
                }
            return None

async def record_order(
    client_id: int,
    products_with_quantity: list[dict], # Пример: [{"product_id": 1, "quantity": 2}, {"product_id": 4, "quantity": 1}]
    status: str = 'pending'
) -> dict:
    """
    Записывает новый заказ в базу данных.
    products_with_quantity: Список словарей, где каждый словарь содержит 'product_id' и 'quantity'.
    """
    total_amount = 0.0
    order_details_list = [] # Список для хранения деталей заказа для JSONB

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            # Сначала получаем цены продуктов и рассчитываем общую сумму
            for item in products_with_quantity:
                product_id = item["product_id"]
                quantity = item["quantity"]
                
                cursor.execute("SELECT name, price FROM products WHERE id = %s;", (product_id,))
                product_info = cursor.fetchone()
                
                if not product_info:
                    raise ValueError(f"Продукт с ID {product_id} не найден.")
                
                product_name, price_per_item = product_info
                item_total = float(price_per_item) * quantity
                total_amount += item_total
                
                order_details_list.append({
                    "product_id": product_id,
                    "product_name": product_name,
                    "quantity": quantity,
                    "price_per_item": float(price_per_item),
                    "item_total": item_total
                })
            
            # Вставляем новый заказ
            cursor.execute("""
                INSERT INTO orders (client_id, total_amount, order_details, status)
                VALUES (%s, %s, %s, %s)
                RETURNING id, client_id, order_date, status, total_amount, order_details;
            """, (client_id, total_amount, json.dumps(order_details_list), status)) # json.dumps для JSONB.  json.dumps означает "dump string" (выгрузить в строку)
            
            result = cursor.fetchone()
            if result:
                #Когда вы делаете запрос SELECT (в вашем случае через RETURNING), библиотека psycopg2 видит, что данные приходят из колонки типа JSONB.
                # Она автоматически выполняет обратное действие — десериализует данные. Она берет бинарные JSONB-данные из базы, 
                # преобразует их в текстовый JSON, а затем парсит этот текст, создавая из него родной для Python объект
                return {
                    "id": result[0],
                    "client_id": result[1],
                    "order_date": result[2],
                    "status": result[3],
                    "total_amount": float(result[4]),
                    "order_details": result[5] # JSONB возвращается как Python-словарь/список 
                }
            raise RuntimeError("Не удалось записать заказ")


async def get_manager_contact_by_location(location: str) -> str | None:
    """
    Получает контактные данные менеджера, отвечающего за указанную локацию.
    В реальной системе это может быть более сложная логика (таблица managers, зоны покрытия).
    Для простоты пока ищем среди клиентов, у которых указана эта локация и контакт менеджера.
    """
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            # Ищем первого клиента, у которого указана данная локация и есть контакт менеджера
            cursor.execute("""
                SELECT manager_contact
                FROM clients
                WHERE LOWER(location) = LOWER(%s) AND manager_contact IS NOT NULL
                LIMIT 1;
            """, (location,))
            result = cursor.fetchone()
            return result[0] if result else None
