#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Убрать из списка доступа строки, за которыми нет человека.

Решение владельца 14.09.2026: «убери эти три тестовые строки» (77, 777, 987654321).
Решение владельца 16.09.2026: правило «это должен быть настоящий человек» поставлено
в САМУ запись — питоновский страж `refuse_if_not_a_person` плюс ограничение в таблице
(`bt_3_allowed_users_person_*`). Новых таких строк появиться больше не может ниоткуда:
ни из двери самостоятельного входа, ни из посева админов при старте бота, ни из ночного
впуска из очереди, ни из нагрузочного прогона, ни из пути, которого ещё нет.

Поэтому скрипт больше НЕ носит список id в себе. Он показывает ВСЕ строки, которые
правило считает не-людьми, вместе с пометкой и датой — по ним видно дверь:
    «self-serve access: …»          — самостоятельный вход (мини-апп / бот);
    «auto-seeded admin (startup)»   — посев админов при старте;
    «load_test_…»                   — нагрузочный прогон;
    «approved via …»                — решение админа руками.

Удаление по-прежнему делает ЧЕЛОВЕК: без `--apply` скрипт только показывает. Молчание
согласием не считается, и деплой ничего не чистит сам.

Прогон:  python3 scripts/allowed_users_drop_test_rows.py --dry-run   # только показать
         python3 scripts/allowed_users_drop_test_rows.py --apply     # удалить показанные
"""
from __future__ import annotations

import argparse
import logging
import sys


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="удалить (без флага — только показать)")
    ap.add_argument("--dry-run", action="store_true", help="только показать")
    args = ap.parse_args()

    from backend.database import (count_allowed_users, count_allowed_rows_not_real_people,
                                  list_allowed_rows_not_real_people,
                                  delete_allowed_rows_not_real_people)

    строки = list_allowed_rows_not_real_people()
    print(f"Живых людей в списке доступа сейчас: {count_allowed_users()}")
    print(f"Строк, которые НЕ настоящие люди:    {count_allowed_rows_not_real_people()}")
    if not строки:
        print("\nЧисто: строк без человека нет.")
        return 0

    print(f"\nНайдено строк без человека: {len(строки)}")
    for r in строки:
        print(f"  • {r['user_id']} | {r['username'] or '—'} | {r['note'] or '—'} "
              f"| создана {r['created_at']}")

    if not args.apply or args.dry_run:
        print("\nСухой прогон. Чтобы удалить показанные: --apply")
        return 0

    удалено = delete_allowed_rows_not_real_people([r["user_id"] for r in строки])
    print(f"\nУдалено строк: {удалено}")
    print(f"Живых людей в списке доступа теперь: {count_allowed_users()}")
    print(f"Строк, которые НЕ настоящие люди:    {count_allowed_rows_not_real_people()}")
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    sys.exit(main())
