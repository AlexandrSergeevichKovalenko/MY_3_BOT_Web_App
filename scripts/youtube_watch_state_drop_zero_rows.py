# -*- coding: utf-8 -*-
"""Убрать из памяти просмотра строки с нулевой позицией (решение владельца 29.08.2026).

Зачем
─────
До 29.08.2026 приложение при КАЖДОМ запуске сообщало «я на нулевой секунде» по
последнему ролику: при восстановлении последнего видео youtubeId получал значение
ещё на главном экране, плеера не было, время равнялось нулю — и эффект «человек вне
раздела видео → дозапиши позицию» отправлял этот ноль на сервер. Upsert клал
присланное значение как есть, поэтому реальные 449 секунд молча заменялись нулём.

Замер живой базы 29.08.2026 (bt_3_youtube_watch_state):
    всего строк ................ 56   (и ровно ОДИН user_id — сохраняли только админу)
    позиция = 0 ................ 39
    переписывались позже ....... 51
    из них сейчас ноль ......... 37

Восстановить затёртые позиции неоткуда — истории значений таблица не хранит.
Владелец 29.08.2026: чистить. Строка с нулём не несёт никакого знания: «не начинал»
и «начинал, но всё стёрлось» выглядят в ней одинаково, а для продукта отсутствие
строки и строка с нулём означают ровно одно и то же — открыть ролик с начала.

Дыра, через которую эти строки попадали, закрыта тремя поясами и без них скрипт
пришлось бы гонять снова:
  • frontend/src/App.jsx — youtubeResumeValueIsWritable: клиент не пишет позицию,
    которую плеер ещё не назвал (блок «ИСПРАВЛЕНО 29.08.2026»);
  • backend/backend_server.py — страж skip_reason="zero_without_playback";
  • backend/tests/test_youtube_watch_position.py — 7 тестов, оба пояса проверены
    мутацией (сняли пояс → тесты покраснели).
Поэтому скрипт разовый: повторяться этой работе больше не из чего.

    python3 scripts/youtube_watch_state_drop_zero_rows.py            # только показать
    python3 scripts/youtube_watch_state_drop_zero_rows.py --apply    # удалить
"""

from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.database import get_db_connection_context  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="удалить (без флага — только показать)")
    args = parser.parse_args()

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                  COUNT(*)                                                AS всего,
                  COUNT(*) FILTER (WHERE current_time_seconds = 0)        AS нулевых,
                  COUNT(DISTINCT user_id)                                 AS людей
                FROM bt_3_youtube_watch_state;
                """
            )
            total, zeros, people = cursor.fetchone()
            print(f"В памяти просмотра: {total} строк, {people} человек.")
            print(f"Из них с нулевой позицией: {zeros}.")

            if not zeros:
                print("Нулевых строк нет — чистить нечего.")
                return 0

            if not args.apply:
                cursor.execute(
                    """
                    SELECT user_id, video_id, updated_at
                    FROM bt_3_youtube_watch_state
                    WHERE current_time_seconds = 0
                    ORDER BY updated_at DESC
                    LIMIT 10;
                    """
                )
                print("\nПервые 10 из тех, что будут удалены:")
                for user_id, video_id, updated_at in cursor.fetchall():
                    print(f"   {user_id}  {video_id}  {updated_at}")
                print(f"\nНичего не удалено. Чтобы удалить: --apply")
                return 0

            cursor.execute("DELETE FROM bt_3_youtube_watch_state WHERE current_time_seconds = 0;")
            deleted = cursor.rowcount
            conn.commit()

            cursor.execute(
                """
                SELECT COUNT(*), COUNT(*) FILTER (WHERE current_time_seconds = 0)
                FROM bt_3_youtube_watch_state;
                """
            )
            left_total, left_zeros = cursor.fetchone()
            print(f"\nУдалено строк: {deleted}.")
            print(f"Осталось: {left_total} строк, из них с нулём: {left_zeros}.")
            if left_zeros:
                print("⚠️  Нулевые строки остались — значит, кто-то их пишет прямо сейчас. Разбираться.")
                return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
