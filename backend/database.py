import psycopg2
from psycopg2 import Binary
import os
from contextlib import contextmanager
import json
from datetime import datetime, timezone, date, timedelta
from pathlib import Path
from dotenv import load_dotenv

env_path = Path(__file__).parent / ".env"
load_dotenv(dotenv_path=env_path)

DATABASE_URL = os.getenv("DATABASE_URL_RAILWAY") #

# Добавим проверку, чтобы сразу видеть ошибку в логах, если адреса нет
if not DATABASE_URL:
    print("❌ ОШИБКА: DATABASE_URL_RAILWAY не найден в .env или переменных окружения!")
else:
    # Для безопасности печатаем только хост, скрывая пароль
    print(f"✅ database.py успешно загрузил URL (хост: {DATABASE_URL.split('@')[-1].split(':')[0]})")

@contextmanager
def get_db_connection_context(): #
    conn = psycopg2.connect(DATABASE_URL, sslmode='require', connect_timeout=8) #
    try:
        yield conn #
        conn.commit() #
    finally:
        conn.close() #

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
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
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
                CREATE TABLE IF NOT EXISTS bt_3_webapp_checks (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    username TEXT,
                    session_id TEXT,
                    original_text TEXT NOT NULL,
                    user_translation TEXT NOT NULL,
                    result TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
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
                CREATE TABLE IF NOT EXISTS bt_3_webapp_dictionary_queries (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    word_ru TEXT,
                    translation_de TEXT,
                    word_de TEXT,
                    translation_ru TEXT,
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
                ADD COLUMN IF NOT EXISTS is_learned BOOLEAN NOT NULL DEFAULT FALSE;
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
                CREATE TABLE IF NOT EXISTS bt_3_dictionary_cache (
                    word_ru TEXT PRIMARY KEY,
                    response_json JSONB NOT NULL,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
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
                    result
                )
                VALUES (%s, %s, %s, %s, %s, %s);
            """, (
                user_id,
                username,
                session_id,
                original_text,
                user_translation,
                result,
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
                    "created_at": row[5].isoformat() if row[5] else None,
                }
                for row in rows
            ]


def save_webapp_dictionary_query(
    user_id: int,
    word_ru: str | None,
    translation_de: str | None,
    word_de: str | None,
    translation_ru: str | None,
    response_json: dict,
    folder_id: int | None = None,
) -> None:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO bt_3_webapp_dictionary_queries (
                    user_id,
                    word_ru,
                    folder_id,
                    translation_de,
                    word_de,
                    translation_ru,
                    response_json
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s);
            """, (
                user_id,
                word_ru,
                folder_id,
                translation_de,
                word_de,
                translation_ru,
                json.dumps(response_json, ensure_ascii=False),
            ))


def get_webapp_dictionary_entries(
    user_id: int,
    limit: int = 100,
    folder_mode: str = "all",
    folder_id: int | None = None,
) -> list[dict]:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            where_clause = "WHERE user_id = %s"
            params = [user_id]
            if folder_mode == "folder" and folder_id is not None:
                where_clause += " AND folder_id = %s"
                params.append(folder_id)
            elif folder_mode == "none":
                where_clause += " AND folder_id IS NULL"
            params.append(limit)
            cursor.execute(f"""
                SELECT id, word_ru, translation_de, word_de, translation_ru, response_json, folder_id, created_at
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
            "response_json": row[5],
            "folder_id": row[6],
            "created_at": row[7].isoformat() if row[7] else None,
        })
    return items


def get_dictionary_entry_by_id(entry_id: int) -> dict | None:
    if not entry_id:
        return None
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT id, word_ru, translation_de, word_de, translation_ru, response_json, folder_id, created_at
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
                "response_json": row[5],
                "folder_id": row[6],
                "created_at": row[7].isoformat() if row[7] else None,
            }


def get_random_dictionary_entry(cooldown_days: int = 5) -> dict | None:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT
                    id,
                    user_id,
                    word_ru,
                    translation_de,
                    response_json
                FROM bt_3_webapp_dictionary_queries
                WHERE response_json IS NOT NULL
                  AND NOT EXISTS (
                      SELECT 1
                      FROM bt_3_quiz_history h
                      WHERE h.word_ru = bt_3_webapp_dictionary_queries.word_ru
                        AND h.asked_at >= NOW() - INTERVAL %s
                  )
                ORDER BY RANDOM()
                LIMIT 1;
            """, (f"{cooldown_days} days",))
            row = cursor.fetchone()
            if not row:
                cursor.execute("""
                    SELECT
                        id,
                        user_id,
                        word_ru,
                        translation_de,
                        response_json
                    FROM bt_3_webapp_dictionary_queries
                    WHERE response_json IS NOT NULL
                    ORDER BY RANDOM()
                    LIMIT 1;
                """)
                row = cursor.fetchone()
                if not row:
                    return None
            return {
                "id": row[0],
                "user_id": row[1],
                "word_ru": row[2],
                "translation_de": row[3],
                "response_json": row[4],
            }


def get_random_dictionary_entry_for_quiz_type(quiz_type: str, cooldown_days: int = 5) -> dict | None:
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
        "prefix": f"({prefixes_count_expr} > 0 OR {base_word_expr} IS NOT NULL)",
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
                    response_json
                FROM bt_3_webapp_dictionary_queries
                WHERE response_json IS NOT NULL
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
                (f"{cooldown_days} days",),
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
                        response_json
                    FROM bt_3_webapp_dictionary_queries
                    WHERE response_json IS NOT NULL
                      AND {extra_where}
                    ORDER BY RANDOM()
                    LIMIT 1;
                    """
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
    }


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
            cursor.execute("""
                INSERT INTO bt_3_flashcard_seen (user_id, entry_id, seen_at)
                VALUES (%s, %s, NOW());
            """, (user_id, entry_id))


def get_flashcard_set(
    user_id: int,
    set_size: int = 15,
    wrong_size: int = 5,
    folder_mode: str = "all",
    folder_id: int | None = None,
) -> list[dict]:
    if not user_id:
        return []
    wrong_ids: list[int] = []
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            wrong_where = "s.user_id = %s AND s.last_result = FALSE"
            wrong_params = [user_id]
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

            if len(wrong_ids) < wrong_size:
                extra_where = "s.user_id = %s AND s.entry_id <> ALL(%s::bigint[])"
                extra_params = [user_id, wrong_ids or [0]]
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

            base_where = "user_id = %s AND id <> ALL(%s::bigint[])"
            base_params = [user_id, wrong_ids or [0]]
            if folder_mode == "folder" and folder_id is not None:
                base_where += " AND folder_id = %s"
                base_params.append(folder_id)
            elif folder_mode == "none":
                base_where += " AND folder_id IS NULL"
            base_params.extend([user_id, max(set_size - len(wrong_ids), 0)])
            cursor.execute(f"""
                SELECT id, word_ru, translation_de, word_de, translation_ru, response_json
                FROM bt_3_webapp_dictionary_queries
                WHERE {base_where}
                  AND id NOT IN (
                      SELECT entry_id
                      FROM bt_3_flashcard_seen
                      WHERE user_id = %s
                        AND seen_at >= NOW() - INTERVAL '2 days'
                  )
                ORDER BY RANDOM()
                LIMIT %s;
            """, base_params)
            random_rows = cursor.fetchall()

            if len(random_rows) < max(set_size - len(wrong_ids), 0):
                fallback_where = "user_id = %s AND id <> ALL(%s::bigint[])"
                fallback_params = [user_id, wrong_ids or [0]]
                if folder_mode == "folder" and folder_id is not None:
                    fallback_where += " AND folder_id = %s"
                    fallback_params.append(folder_id)
                elif folder_mode == "none":
                    fallback_where += " AND folder_id IS NULL"
                fallback_params.append(max(set_size - len(wrong_ids), 0))
                cursor.execute(f"""
                SELECT id, word_ru, translation_de, word_de, translation_ru, response_json
                FROM bt_3_webapp_dictionary_queries
                WHERE {fallback_where}
                ORDER BY RANDOM()
                LIMIT %s;
            """, fallback_params)
                random_rows = cursor.fetchall()

            if wrong_ids:
                wrong_where = "user_id = %s AND id = ANY(%s::bigint[])"
                wrong_params = [user_id, wrong_ids]
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


def count_due_srs_cards(user_id: int, now_utc: datetime | None = None) -> int:
    now_utc = now_utc or datetime.now(timezone.utc)
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM bt_3_card_srs_state
                WHERE user_id = %s
                  AND status <> 'suspended'
                  AND due_at <= %s;
                """,
                (int(user_id), now_utc),
            )
            row = cursor.fetchone()
            return int(row[0] if row else 0)


def count_new_cards_introduced_today(user_id: int, now_utc: datetime | None = None) -> int:
    now_utc = now_utc or datetime.now(timezone.utc)
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM bt_3_card_srs_state
                WHERE user_id = %s
                  AND DATE(created_at AT TIME ZONE 'UTC') = DATE(%s AT TIME ZONE 'UTC');
                """,
                (int(user_id), now_utc),
            )
            row = cursor.fetchone()
            return int(row[0] if row else 0)


def get_next_due_srs_card(user_id: int, now_utc: datetime | None = None) -> dict | None:
    now_utc = now_utc or datetime.now(timezone.utc)
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    s.card_id,
                    s.status,
                    s.due_at,
                    s.interval_days,
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
                ORDER BY s.due_at ASC
                LIMIT 1;
                """,
                (int(user_id), now_utc),
            )
            row = cursor.fetchone()
            if not row:
                return None
            return {
                "card": {
                    "id": row[0],
                    "word_ru": row[6],
                    "translation_de": row[7],
                    "word_de": row[8],
                    "translation_ru": row[9],
                    "response_json": row[10],
                },
                "srs": {
                    "status": row[1],
                    "due_at": row[2],
                    "interval_days": int(row[3] or 0),
                    "stability": float(row[4] or 0.0),
                    "difficulty": float(row[5] or 0.0),
                },
            }


def get_next_new_srs_candidate(user_id: int) -> dict | None:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT q.id, q.word_ru, q.translation_de, q.word_de, q.translation_ru, q.response_json
                FROM bt_3_webapp_dictionary_queries q
                LEFT JOIN bt_3_card_srs_state s
                  ON s.user_id = q.user_id AND s.card_id = q.id
                WHERE q.user_id = %s
                  AND s.id IS NULL
                ORDER BY q.created_at ASC
                LIMIT 1;
                """,
                (int(user_id),),
            )
            row = cursor.fetchone()
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


def ensure_new_srs_state(user_id: int, card_id: int, now_utc: datetime | None = None) -> dict:
    now_utc = now_utc or datetime.now(timezone.utc)
    state = get_card_srs_state(user_id=user_id, card_id=card_id)
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
    )


def get_dictionary_entry_for_user(user_id: int, card_id: int, cursor=None) -> dict | None:
    def _fetch(cur):
        cur.execute(
            """
            SELECT id, word_ru, translation_de, word_de, translation_ru, response_json
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
            "response_json": row[5],
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
) -> list[dict]:
    max_days_back = max(0, int(max_days_back))
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
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
                }
                for row in rows
            ]


def get_pending_daily_sentences(user_id: int, limit: int = 7) -> list[dict]:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            latest_session_id = _get_latest_session_id(cursor, user_id)
            if not latest_session_id:
                return []

            cursor.execute("""
                SELECT ds.id_for_mistake_table, ds.sentence, ds.unique_id
                FROM bt_3_daily_sentences ds
                LEFT JOIN bt_3_translations tr
                    ON tr.user_id = ds.user_id
                    AND tr.sentence_id = ds.id
                    AND tr.session_id = %s
                WHERE ds.user_id = %s
                  AND ds.session_id = %s
                  AND tr.id IS NULL
                ORDER BY ds.unique_id ASC
                LIMIT %s;
            """, (latest_session_id, user_id, latest_session_id, limit))
            rows = cursor.fetchall()
            return [
                {
                    "id_for_mistake_table": row[0],
                    "sentence": row[1],
                    "unique_id": row[2],
                }
                for row in rows
            ]

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
