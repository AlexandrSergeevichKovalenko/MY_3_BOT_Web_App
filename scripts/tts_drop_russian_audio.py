# -*- coding: utf-8 -*-
"""Удалить накопленную русскую озвучку — файлы в хранилище и записи в базе.

Решение владельца 23.08.2026: «delete russian — we do not use them».

Почему это безопасно и почему это вообще понадобилось
─────────────────────────────────────────────────────
Прогрев при сохранении слова озвучивал «целевую сторону» словарного запроса, а запрос
почти всегда идёт de->ru — человек смотрит немецкое слово ради русского перевода
(3052 карточки против 240 обратных). Целевой стороной оказывался русский текст, и мы
за него платили. Замер 23.08.2026: 1612 готовых русских озвучек, 22,6 МБ; из них за
десять дней кто-то слушал три. Источник закрыт двумя правками — прогрев берёт
изучаемый язык, а синтез чужого языка отклоняется на входе.

Скрипт удаляет ТОЛЬКО записи с language='ru-RU'. Ничего не синтезирует и не трогает
ни немецкие озвучки, ни словарь, ни карточки.

    python3 scripts/tts_drop_russian_audio.py            # показать, что будет удалено
    python3 scripts/tts_drop_russian_audio.py --apply    # удалить

Порядок важен: сначала файл в хранилище, потом запись в базе. Если удаление файла не
удалось — запись НЕ трогаем, иначе файл останется в хранилище навсегда и его никто уже
не найдёт. Не удалённые считаются и печатаются числом.
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("BACKEND_RUNTIME_SIDE_EFFECTS_ENABLED", "0")
os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SKIP_BILLING_LEDGER_WRITES", "1")

from backend.database import get_db_connection_context  # noqa: E402
from backend.r2_storage import r2_delete_object  # noqa: E402

TARGET_LANGUAGE = "ru-RU"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="удалить (без флага — только показать)")
    parser.add_argument("--limit", type=int, default=0, help="ограничить число записей (0 — все)")
    args = parser.parse_args()

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT cache_key, object_key, COALESCE(size_bytes, 0), LEFT(COALESCE(source_text, ''), 40)
                FROM bt_3_tts_object_cache
                WHERE language = %s
                ORDER BY created_at;
                """,
                (TARGET_LANGUAGE,),
            )
            rows = cursor.fetchall() or []

    if args.limit > 0:
        rows = rows[: args.limit]

    total_bytes = sum(int(row[2] or 0) for row in rows)
    print(f"русских озвучек: {len(rows)}, объём {total_bytes / 1024 / 1024:.1f} МБ")
    for row in rows[:5]:
        print(f"   пример: «{row[3]}»")
    if not args.apply:
        print("\nэто показ. Удалить: --apply")
        return 0

    files_deleted = 0
    files_failed = 0
    rows_deleted = 0
    # Одно соединение на весь проход и коммит пачками: по соединению на запись полторы
    # тысячи раз — это полчаса на ровном месте (замерено 23.08.2026 на первом прогоне).
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            for index, (cache_key, object_key, _size, _text) in enumerate(rows, start=1):
                key = str(object_key or "").strip()
                if key:
                    try:
                        r2_delete_object(key)
                        files_deleted += 1
                    except Exception as error:
                        files_failed += 1
                        print(f"   не смог удалить файл {key}: {error}")
                        continue  # запись оставляем: иначе файл потеряется в хранилище
                cursor.execute(
                    "DELETE FROM bt_3_tts_object_cache WHERE cache_key = %s AND language = %s;",
                    (cache_key, TARGET_LANGUAGE),
                )
                rows_deleted += cursor.rowcount
                if index % 50 == 0:
                    conn.commit()
                    print(f"   удалено {rows_deleted} из {len(rows)}")
        conn.commit()

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) FROM bt_3_tts_object_cache WHERE language = %s;",
                (TARGET_LANGUAGE,),
            )
            left = cursor.fetchone()[0]

    print(f"\nудалено файлов: {files_deleted}, не удалось: {files_failed}")
    print(f"удалено записей: {rows_deleted}")
    print(f"осталось русских записей: {left}")
    return 0 if files_failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
