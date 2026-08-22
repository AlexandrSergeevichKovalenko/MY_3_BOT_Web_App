# -*- coding: utf-8 -*-
"""Показать владельцу состояние дверей: кто пишет немецкий текст в базу и есть ли защита.

╔══════════════════════════════════════════════════════════════════════════════════╗
║  ЭТО ПОКАЗ, А НЕ ИСТОЧНИК. Список мест и признак двери живут в                    ║
║  backend/write_doors.py — оттуда же их берёт ночная проверка целостности          ║
║  словаря, и число уходит владельцу утренним отчётом. Свой список здесь заводить   ║
║  НЕЛЬЗЯ: два списка разойдутся, и отчёт начнёт врать.                            ║
╚══════════════════════════════════════════════════════════════════════════════════╝

    python3 scripts/dict_write_doors_audit.py           # состояние в проде
    python3 scripts/dict_write_doors_audit.py --here    # в рабочем каталоге
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.write_doors import ROOT, inspect  # noqa: E402

MARKS = {"ok": "✅", "safe": "🟢", "open": "❌", "missing": "⁉️"}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--here", action="store_true",
                        help="смотреть рабочий каталог, а не то, что в проде")
    args = parser.parse_args()
    ref = None if args.here else "bot3_webapp/refactor/interface"
    if ref:
        subprocess.run(["git", "fetch", "-q", "bot3_webapp"], cwd=ROOT,
                       capture_output=True, timeout=180)

    print("\nисточник: " + ("рабочий каталог" if args.here else "код в проде"))
    print("\nМЕСТА, КОТОРЫЕ ПИШУТ НЕМЕЦКИЙ ТЕКСТ В БАЗУ\n")
    print(f"{'№':>3}  {'состояние':<26} что это")
    print("─" * 96)
    counts = {"ok": 0, "safe": 0, "open": 0, "missing": 0}
    for item in inspect(ref):
        counts[item["state"]] += 1
        state = {
            "ok": "дверь: " + " + ".join(item["doors"]),
            "safe": "проверено, неопасно",
            "open": "ДВЕРИ НЕТ",
            "missing": "НЕТ В КОДЕ",
        }[item["state"]]
        print(f'{item["number"]:>3}  {MARKS[item["state"]]} {state:<24} {item["human"]}')
    print("─" * 96)
    print(f'\nдверь стоит: {counts["ok"]}   проверено и неопасно: {counts["safe"]}   '
          f'ДВЕРИ НЕТ: {counts["open"]}'
          + (f'   нет в коде: {counts["missing"]}' if counts["missing"] else ""))
    if counts["open"] or counts["missing"]:
        print("\nКрасные строки — настоящая работа. Остальное закрыто и перепроверять не нужно.")
    else:
        print("\nОткрытых мест не осталось. Ночная проверка следит за этим сама —"
              "\nчисло приходит владельцу утренним отчётом.")
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
