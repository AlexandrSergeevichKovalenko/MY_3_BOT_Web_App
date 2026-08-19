# -*- coding: utf-8 -*-
"""Прогнать проверку правок по УЖЕ ОТКРЫТЫМ спорным фразам.

Проверка правки судьи («грамотен ли предложенный немецкий» и «сохранён ли смысл»)
появилась 19.08.2026. Вопросы, лежащие в очереди с прежних дней, судились без неё —
и без этого прогона владелец увидел бы у каждой правки честное, но бесполезное
«проверить не удалось». Лежащая очередь должна доехать до того же качества, что и
новая, сама, а не ждать, пока владелец нажмёт «Пересудить» 64 раза.

Судей ЗАНОВО НЕ СПРАШИВАЕМ: их вердикты остаются как есть. Спрашиваем только про уже
предложенные ими тексты, и спрашиваем ДВАЖДЫ: забраковать правку имеют право только
два совпавших голоса (см. _check_fix_twice — почему именно так).

    python3 scripts/phrase_review_check_existing_fixes.py           # показать, не писать
    python3 scripts/phrase_review_check_existing_fixes.py --apply   # проверить и записать
"""
from __future__ import annotations

import argparse
import os
import sys
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SKIP_BILLING_LEDGER_WRITES", "1")

from backend.database import (  # noqa: E402
    get_db_connection_context,
    update_phrase_review_judges,
)
from backend.phrase_night_check import _check_fix_twice  # noqa: E402

WORKERS = 6


def check_one(row: tuple) -> tuple:
    """(review_id, text, translation, judges) → (review_id, judges, что нашли)."""
    review_id, text, translation, judges = row
    found = []
    for number, judge in enumerate(judges, 1):
        if not isinstance(judge, dict):
            continue
        for field in ("corrected", "proposal"):
            fix = str(judge.get(field) or "").strip()
            if not fix:
                continue
            check = _check_fix_twice(text, translation or "", fix)
            judge[f"{field}_check"] = check
            if check.get("checked") and not (
                    check.get("grammar_ok") and check.get("meaning_kept")):
                bad = []
                if not check.get("grammar_ok"):
                    bad.append("немецкий неверен")
                if not check.get("meaning_kept"):
                    bad.append("смысл другой")
                found.append((number, fix, ", ".join(bad), str(check.get("why") or "")))
    return review_id, judges, found


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--limit", type=int, default=500)
    args = parser.parse_args()

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """SELECT id, text, translation, judges FROM bt_3_phrase_review
                    WHERE status = 'open' ORDER BY id LIMIT %s;""",
                (int(args.limit),),
            )
            rows = [(r[0], r[1], r[2], r[3] if isinstance(r[3], list) else [])
                    for r in cursor.fetchall()]

    fixes = sum(1 for _rid, _t, _tr, js in rows for j in js if isinstance(j, dict)
                for f in ("corrected", "proposal") if str(j.get(f) or "").strip())
    print(f"\nОТКРЫТЫХ ВОПРОСОВ: {len(rows)}, правок к проверке: {fixes}\n")
    if not args.apply:
        print("ВХОЛОСТУЮ. Проверить и записать: --apply\n")
        return 0

    rejected = 0
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        for review_id, judges, found in pool.map(check_one, rows):
            update_phrase_review_judges(review_id, judges)
            for number, fix, what, why in found:
                rejected += 1
                print(f"⛔ #{review_id} судья {number}: {what}")
                print(f"     предлагал: {fix!r}")
                print(f"     почему   : {why}")

    print(f"\nПРОВЕРЕНО ВОПРОСОВ: {len(rows)}. ОТКЛОНЕНО ПРАВОК: {rejected}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
