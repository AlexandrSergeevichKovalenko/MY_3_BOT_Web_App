#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Убрать из списка доступа строки, за которыми нет человека.

Решение владельца 14.09.2026: «убери эти три тестовые строки».

Повод. Батл #13 показал владельцу «🚫 Не дошло: 10». Три из десяти — строки 77, 777 и
987654321: телеграмных id такой формы не бывает, а живой `getChat` по каждой отвечает
«Chat not found», то есть за ними нет ни одного чата. В список доступа они попали
прогонами кода по боевой базе 28–30.08.2026 (пометка «self-serve access: приложение
(Mini App)») — то же самое, о чём написан комментарий у REAL_ALLOWED_USER_SQL.

Что делает скрипт. Показывает найденные строки целиком, удаляет РОВНО перечисленные
id и печатает состояние до/после. Удаление оставляет след: id, имя, пометка и дата —
в выводе и в логе.

Прогон:  python3 scripts/allowed_users_drop_test_rows.py --dry-run   # только показать
         python3 scripts/allowed_users_drop_test_rows.py --apply     # удалить
"""
from __future__ import annotations

import argparse
import logging
import sys

# Ровно те три строки, которые владелец решил убрать. Список НЕ вычисляется правилом:
# удаление доступа — решение человека, а не вывод регулярки. Появятся новые — их
# сначала увидит обещание `allowed_rows_are_real_people`, и решение снова примет владелец.
TEST_ROWS = (77, 777, 987654321)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="удалить (без флага — только показать)")
    ap.add_argument("--dry-run", action="store_true", help="только показать")
    args = ap.parse_args()

    from backend.database import (get_db_connection_context, count_allowed_users,
                                  count_allowed_rows_not_real_people,
                                  invalidate_telegram_user_allowed_cache)

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT user_id, username, note, created_at FROM bt_3_allowed_users "
                "WHERE user_id = ANY(%s) ORDER BY user_id;",
                (list(TEST_ROWS),),
            )
            найдено = cursor.fetchall() or []

    print(f"Живых людей в списке доступа сейчас: {count_allowed_users()}")
    print(f"Строк, которые НЕ настоящие люди:    {count_allowed_rows_not_real_people()}")
    print(f"\nИз них перечислено к удалению: {len(найдено)} из {len(TEST_ROWS)}")
    for uid, username, note, created in найдено:
        print(f"  • {uid} | {username or '—'} | {note or '—'} | {created}")
    отсутствуют = sorted(set(TEST_ROWS) - {int(r[0]) for r in найдено})
    if отсутствуют:
        print(f"  (уже нет в базе: {', '.join(str(u) for u in отсутствуют)})")

    if not args.apply or args.dry_run:
        print("\nСухой прогон. Чтобы удалить: --apply")
        return 0
    if not найдено:
        print("\nУдалять нечего.")
        return 0

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "DELETE FROM bt_3_allowed_users WHERE user_id = ANY(%s);",
                ([int(r[0]) for r in найдено],),
            )
            удалено = int(cursor.rowcount or 0)
        conn.commit()
    for uid, username, note, created in найдено:
        # След удаления: кого именно убрали и чем он был помечен.
        logging.warning("список доступа: удалена тестовая строка id=%s username=%s note=%s created=%s",
                        uid, username, note, created)
        invalidate_telegram_user_allowed_cache(int(uid))

    print(f"\nУдалено строк: {удалено}")
    print(f"Живых людей в списке доступа теперь: {count_allowed_users()}")
    print(f"Строк, которые НЕ настоящие люди:    {count_allowed_rows_not_real_people()}")
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    sys.exit(main())
