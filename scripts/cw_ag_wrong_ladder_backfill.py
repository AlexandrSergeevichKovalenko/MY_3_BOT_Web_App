#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Достроить лестницу возврата тем сдачам, что легли ДО правила 24.08.2026.

Зачем
─────
До 24.08.2026 неверно сданный кроссворд или анаграмма не закрывались для человека
вообще: `next_state` возвращал пустой срок, и в памяти ротации оставалась строка с
`next_eligible_at IS NULL`. Держал такое задание только общий отдых карточки — а его
тем же решением сняли. Значит без этой уборки заваленное задание вернулось бы человеку
на следующий же день.

Правило то же, что в коде (`backend/task_rotation.WRONG_LADDER_DAYS`): 7 → 14 → 30 дней
от ДАТЫ СДАЧИ, ступень — по числу неверных сдач (`seen_count - correct_count`).
Считаем от `last_seen_at`, а не от «сейчас»: человек завалил задание тогда, а не сегодня.

Запуск:
    DATABASE_URL=... python3 scripts/cw_ag_wrong_ladder_backfill.py            # показать
    DATABASE_URL=... python3 scripts/cw_ag_wrong_ladder_backfill.py --apply    # записать
"""

import os
import sys
from datetime import timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.task_rotation import WRONG_LADDER_DAYS  # noqa: E402

KINDS = ("cw", "ag")


def _dsn() -> str:
    for key in ("DATABASE_URL", "DATABASE_PUBLIC_URL"):
        value = (os.getenv(key) or "").strip()
        if value:
            return value
    sys.exit("Нужен DATABASE_URL (или DATABASE_PUBLIC_URL).")


def main() -> int:
    import psycopg2

    apply = "--apply" in sys.argv
    conn = psycopg2.connect(_dsn(), connect_timeout=30)
    conn.set_session(readonly=not apply, autocommit=False)
    cur = conn.cursor()
    cur.execute(
        """SELECT id, user_id, kind, task_key, seen_count, correct_count, last_seen_at
           FROM bt_3_user_task_state
           WHERE kind = ANY(%s)
             AND retired_at IS NULL
             AND next_eligible_at IS NULL
             AND seen_count > correct_count
           ORDER BY last_seen_at;""",
        (list(KINDS),),
    )
    rows = cur.fetchall() or []
    print(f"Строк без срока возврата: {len(rows)}")
    updates = []
    for row_id, user_id, kind, task_key, seen, correct, last_seen in rows:
        wrong = max(1, int(seen or 0) - int(correct or 0))
        days = WRONG_LADDER_DAYS[min(wrong, len(WRONG_LADDER_DAYS)) - 1]
        due = last_seen + timedelta(days=int(days))
        updates.append((due, row_id))
        print(f"  {kind} {task_key[:24]}… чел.{user_id}: неверных {wrong} → "
              f"+{days} дн. от {last_seen:%Y-%m-%d} = {due:%Y-%m-%d}")
    if not updates:
        print("Достраивать нечего.")
        return 0
    if not apply:
        print("\nЭто показ. Записать: добавьте --apply")
        return 0
    cur.executemany(
        "UPDATE bt_3_user_task_state SET next_eligible_at = %s WHERE id = %s;", updates)
    conn.commit()
    print(f"\nЗаписано строк: {len(updates)}")
    cur.execute(
        """SELECT COUNT(*) FROM bt_3_user_task_state
           WHERE kind = ANY(%s) AND retired_at IS NULL AND next_eligible_at IS NULL
             AND seen_count > correct_count;""",
        (list(KINDS),),
    )
    left = int((cur.fetchone() or [0])[0])
    print(f"Осталось без срока: {left}")
    return 0 if left == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
