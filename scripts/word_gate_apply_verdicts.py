# -*- coding: utf-8 -*-
"""Довезти приговоры двери слова до данных — РУКАМИ. Сухой прогон по умолчанию.

Вся работа живёт в backend/word_gate_apply.py: её же зовёт ночной актёр, поэтому здесь
только разбор аргументов и печать. Логика применения в двух копиях жить не должна —
цена расхождения тут потеря чьей-то карточки.

    python3 scripts/word_gate_apply_verdicts.py            # сухой прогон
    python3 scripts/word_gate_apply_verdicts.py --apply    # записать
    python3 scripts/word_gate_apply_verdicts.py --быстро   # состав без переспроса двери
"""
from __future__ import annotations

import argparse
import os
import sys

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SKIP_BILLING_LEDGER_WRITES", "1")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.word_gate_apply import apply_pending_verdicts  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--быстро", action="store_true",
                        help="показать состав без переспроса двери (применять нельзя)")
    args = parser.parse_args()
    отчёт = apply_pending_verdicts(apply=args.apply, переспрашивать=not args.быстро,
                                   печатать=True)
    return 0 if отчёт.get("не удалось", 0) == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
