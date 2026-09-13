# -*- coding: utf-8 -*-
"""Разбор статей, где часть речи спорит с родом — руками, тем же кодом, что и ночью.

Правило и повод живут в `backend/pos_gender_conflict.py`. Здесь только вызов: копии
правила тут нет специально, иначе прогон руками и ночной проход разойдутся — это уже
случилось 13.09.2026 с правилом регистра (226 против 230), и разница оказалась
содержательной.

    python3 scripts/lex_units_fix_pos_gender_conflict.py            # показать разбор
    python3 scripts/lex_units_fix_pos_gender_conflict.py --apply    # применить
"""
from __future__ import annotations

import argparse
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")

from backend.database import get_db_connection_context                  # noqa: E402
from backend.german_reference_dwds import dwds_pos                      # noqa: E402
from backend.pos_gender_conflict import разобрать, разобрать_противоречия  # noqa: E402


def спросить_упрямо(слово: str) -> str | None:
    """DWDS с тремя попытками: одиночные соединения он обрывает примерно на половине
    запросов (замер 13.09.2026), и молчание сети нельзя путать с «слова нет»."""
    for попытка in range(3):
        ответ = dwds_pos(слово)
        if ответ is not None:
            return ответ
        time.sleep(0.7 * (попытка + 1))
    return None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--limit", type=int, default=100)
    args = ap.parse_args()

    if args.apply:
        итог = разобрать_противоречия(limit=args.limit, спросить=спросить_упрямо)
        print("итог:", итог)
    else:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute("""SELECT display, pos, gender FROM bt_3_lex_units
                               WHERE lang='de' AND pos IN ('verb','adjective','adverb')
                                 AND gender IS NOT NULL ORDER BY display LIMIT %s""",
                            (int(args.limit),))
                статьи = cur.fetchall() or []
        print(f"статей с противоречием: {len(статьи)}\n")
        for display, pos, gender in статьи:
            решение, написание, ответ = разобрать(display, спросить_упрямо)
            print(f"   {display:<18} {pos:<10} род={gender:<4} → {решение:<20} "
                  f"{написание or '':<18} {ответ}")
        print("\nсухой прогон; применить — ключ --apply")

    print("\nЭКРАН ПОСЛЕ:")
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute("""SELECT COUNT(*) FROM bt_3_lex_units
                           WHERE lang='de' AND pos IN ('verb','adjective','adverb')
                             AND gender IS NOT NULL""")
            осталось = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM bt_3_card_complaints WHERE status <> 'решена'")
            в_очереди = cur.fetchone()[0]
    print(f"   статей с противоречием: {осталось}")
    print(f"   ждут решения владельца (с кнопками): {в_очереди}")


if __name__ == "__main__":
    main()
