"""Погасить указатели «набор переводов не закончен», которые разошлись с базой.

Зачем. Плашку «5 из 7» на главном экране показывает не сама сессия, а указатель на неё.
До 30.08.2026 закрытие сессии гасило только быструю копию указателя (Redis), а долгая
(bt_3_user_api_snapshots / session_presence_card) оставалась со словом "active" навсегда.
Владелец 30.08 увидел приглашение доделать набор, закрытый ночным заданием сутки назад.

Систему починили: закрытие сессии теперь гасит указатель в обоих хранилищах, а ночное
задание метёт остатки и пишет владельцу, если их вдруг больше нуля. Этот скрипт нужен
ровно один раз — чтобы убрать то, что уже накопилось, не дожидаясь ближайшей ночи.
Запускать повторно безопасно: он трогает ТОЛЬКО карточки, расходящиеся с базой.

    python scripts/session_presence_markers_sweep.py --dry-run   # показать, ничего не менять
    python scripts/session_presence_markers_sweep.py             # погасить

Источник истины — сама сессия: строка bt_3_user_progress с completed = FALSE, у которой
есть предложения на СЕГОДНЯ. Карточка живого набора не пострадает.
"""
from __future__ import annotations

import argparse
import os
import sys

# Запуск как `python scripts/...` кладёт в путь папку scripts/, а не корень репозитория.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.database import (  # noqa: E402
    clear_stale_translation_session_presence_markers,
    get_db_connection_context,
)

FIND_SQL = """
SELECT s.user_id, s.payload->>'session_id', s.refreshed_at
FROM bt_3_user_api_snapshots s
WHERE s.snapshot_kind = 'session_presence_card'
  AND s.snapshot_key = 'current'
  AND s.payload->>'state' = 'active'
  AND NOT EXISTS (
    SELECT 1
    FROM bt_3_user_progress up
    WHERE up.user_id = s.user_id
      AND up.session_id::text = s.payload->>'session_id'
      AND up.completed = FALSE
      AND EXISTS (
        SELECT 1
        FROM bt_3_daily_sentences ds
        WHERE ds.user_id = up.user_id
          AND ds.session_id = up.session_id
          AND ds.date = CURRENT_DATE
      )
  )
ORDER BY s.refreshed_at;
"""


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="только показать, ничего не менять")
    args = parser.parse_args()

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(FIND_SQL)
            rows = cursor.fetchall()

    if not rows:
        print("Чисто: ни одного указателя, расходящегося с базой.")
        return 0

    print(f"Указателей, которые врут: {len(rows)}")
    for user_id, session_id, refreshed_at in rows:
        print(f"  • человек {user_id}, набор {session_id}, карточка записана {refreshed_at}")

    if args.dry_run:
        print("\n--dry-run: ничего не меняли.")
        return 0

    result = clear_stale_translation_session_presence_markers()
    print(
        f"\nПогашено: {result.get('cleared_snapshots')} "
        f"(быстрая копия не погасла у {result.get('redis_failures')})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
