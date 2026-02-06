import psycopg2
import os
from contextlib import contextmanager
import json
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
