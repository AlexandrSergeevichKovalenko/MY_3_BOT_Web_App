# -*- coding: utf-8 -*-
"""Указатели-словоформы: сверка со справочником и чистка доказанного мусора.

РУКАМИ ЭТО ЗАПУСКАТЬ НЕ НАДО — сверка идёт КАЖДУЮ НОЧЬ сама, сразу после прогрева
справочника (`background_jobs.run_verb_paradigm_warm_actor`), а раз в неделю владельцу
уходит строчка с числами. Скрипт остался для разбора руками: посмотреть классы,
показать примеры, прогнать внеочередной проход.

Вся логика и всё объяснение — в `backend/lex_form_index_sweep.py`. Здесь только CLI,
чтобы правило жило в одном месте и не разъезжалось между скриптом и ночной задачей.

    python3 scripts/dict_units_forms_confirm.py              # отчёт, база не меняется
    python3 scripts/dict_units_forms_confirm.py --apply      # снести классы A и B
    python3 scripts/dict_units_forms_confirm.py --list 40    # + примеры того, что снесётся
"""
from __future__ import annotations

import argparse
import os
import sys

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SKIP_BILLING_LEDGER_WRITES", "1")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.lex_form_index_sweep import (  # noqa: E402
    CLASS_ORDER,
    build_form_index_report_text,
    sweep_form_index,
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true",
                        help="снести указатели классов A и B (по умолчанию только отчёт)")
    parser.add_argument("--list", type=int, default=0, metavar="N",
                        help="показать N примеров того, что будет снесено")
    args = parser.parse_args()

    report = sweep_form_index(apply=args.apply, sample=args.list)

    print("глаголов в справочнике (документировано): %d" % report["reference_verbs"])
    print("указателей-словоформ у глаголов:          %d" % report["pointers"])
    print()
    for name in CLASS_ORDER:
        print("  %-56s %5d" % (name, report["classes"].get(name, 0)))
    print()
    print("  снести можно (доказано И объяснено): %d" % report["deletable"])
    if args.apply:
        print("  снято: %d" % report["removed"])
    else:
        print("\n(отчёт: в базе ничего не менялось; чистить — с флагом --apply)")
    for line in report.get("samples") or []:
        print("   " + line)

    print("\n─── строчка, которая уходит владельцу ───")
    print(build_form_index_report_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
