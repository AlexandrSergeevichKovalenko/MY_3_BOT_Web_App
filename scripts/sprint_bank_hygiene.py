#!/usr/bin/env python3
"""Прогнать накопленное в банке спринта через дверь приёма синонимов (та же функция,
что ночью в 03:10: backend.sprint_intake.hygiene_pass).

    python3 scripts/sprint_bank_hygiene.py --dry-run      # что изменится, базу не трогает
    python3 scripts/sprint_bank_hygiene.py --apply        # применить
    python3 scripts/sprint_bank_hygiene.py --dry-run --all   # и уже проверенные тоже (перемер обещания)

Нужен DATABASE_URL живой базы и доступ в сеть (de.wiktionary для непрокешированных слов).
Сухой прогон ТОЖЕ ходит в Wiktionary и пишет его кеш (bt_3_wiktionary_synonyms) — это
кеш источника, а не данные банка. Без выгрузки OpenThesaurus в базе прогон отказывается
работать: пустой словарь неотличим от «словарь не знает».
"""
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SKIP_BILLING_LEDGER_WRITES", "1")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--all", action="store_true", help="сухой перемер по всем записям, а не только непроверенным")
    ap.add_argument("--limit", type=int)
    ap.add_argument("--fix-examples", action="store_true",
                    help="дочистить дубли в trainer_json.correct_examples у всех записей")
    args = ap.parse_args()
    if not args.apply and not args.dry_run:
        ap.error("нужен --dry-run или --apply")
    from backend.synonym_sources import openthesaurus_loaded
    if not openthesaurus_loaded():
        print("⛔ bt_3_openthesaurus_synsets пуста — сперва python3 scripts/load_openthesaurus.py --apply")
        return 2
    from backend import sprint_intake
    if args.fix_examples:
        n = sprint_intake.dedup_examples_pass(apply=args.apply)
        print(f"записей с лишними примерами: {n}" + ("" if args.apply else " (сухой прогон)"))
        return 0
    if args.all:
        if args.apply:
            ap.error("--all только с --dry-run")
        # Перемер по всему банку: временно читаем все строки. Делается подменой запроса
        # внутри hygiene_pass через флаг — проще: снимаем фильтр SQL.
        from backend.database import get_db_connection_context
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT sprint_id, relation, wort, accepted FROM bt_3_sprint_bank "
                            "WHERE NOT retired ORDER BY wort")
                rows = cur.fetchall() or []
        changed = 0
        for sprint_id, relation, wort, accepted in rows:
            res = sprint_intake.clean_accepted(wort, relation, list(accepted or []))
            if res.kept != [dict(a) for a in (accepted or [])]:
                changed += 1
                print(f"ИЗМЕНИТСЯ {relation} «{wort}»: "
                      + ", ".join(f"{r.de} [{r.reason}]" for r in res.rejected))
        print(f"записей {len(rows)}, изменилось бы {changed}")
        return 0
    summary = sprint_intake.hygiene_pass(limit=args.limit, apply=args.apply)
    print("ИТОГ:", summary)
    if args.apply:
        sprint_intake.remember_last_stats("hygiene", summary)
    return 0


if __name__ == "__main__":
    sys.exit(main())
