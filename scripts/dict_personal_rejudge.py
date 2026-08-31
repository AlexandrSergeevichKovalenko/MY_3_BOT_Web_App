# -*- coding: utf-8 -*-
"""РАЗОВЫЙ ПЕРЕСУД отметок «текст человека» — чтобы у старых появилась кнопка.

ЗАЧЕМ. Отметки, поставленные до 31.08.2026, несут только претензию: поля «как надо» в
вопросе к панели тогда не было. Человеку они приходят без кнопки «да, правильно так» —
с тем самым дефектом, который в этот день и чинился. Выдумать вариант задним числом
нельзя, значит надо спросить заново.

Владелец 31.08.2026 на вопрос «прогнать их заново за ≈$2?» ответил «yes».

ПОЧЕМУ РАЗОВЫЙ, А НЕ НОЧНОЙ. Новые отметки уже рождаются с готовым вариантом
(`backend/phrase_panel.py`), и второй раз этот прогон не понадобится. Ставить его на
ночь значило бы платить вечно за работу, которой больше нет.

Логика целиком в `backend/phrase_panel.rejudge_personal` — здесь только ключи из
боевого окружения, потолок расхода и печать хода.

    python3 scripts/dict_personal_rejudge.py --limit 5        # проба, без записи
    python3 scripts/dict_personal_rejudge.py --apply          # весь пересуд
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SKIP_BILLING_LEDGER_WRITES", "1")

# Потолок на весь пересуд. Замер 31.08.2026: $0.0042 за карточку, карточек ~470,
# то есть ≈$2. Три доллара — с запасом; упрётся в них — прогон честно остановится,
# а непересуженные останутся как были.
BUDGET_USD = 3.0


def prod(var: str, service: str = "BACKEND_WEB(backend:server.py)") -> str:
    import subprocess
    out = subprocess.run(["railway", "variables", "--service", service, "--json"],
                         capture_output=True, text=True).stdout
    value = json.loads(out).get(var)
    if not value:
        raise RuntimeError(f"в боевом окружении нет {var}")
    return value


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--limit", type=int, default=1000)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--budget", type=float, default=BUDGET_USD)
    args = parser.parse_args()

    os.environ.setdefault("OPENAI_API_KEY", prod("OPENAI_API_KEY"))
    os.environ.setdefault("GEMINI_API_KEY", prod("GEMINI_API_KEY"))

    from backend import phrase_panel as pp

    начало = time.time()
    счёт = {"n": 0}

    def на_карточку(unit_id, display, verdict, why):
        счёт["n"] += 1
        print(f"   {str(display)[:40]:42} {verdict:26} {str(why)[:50]}")
        if счёт["n"] % 50 == 0:
            print(f"   … {счёт['n']}, {(time.time()-начало)/60:.0f} мин")

    отчёт = pp.rejudge_personal(limit=int(args.limit), budget_usd=float(args.budget),
                                workers=int(args.workers), apply=bool(args.apply),
                                on_card=на_карточку)
    if отчёт.get("пропущено"):
        print(f"\n⛔ ПРОГОН НЕ ШЁЛ: {отчёт['пропущено']}")
        return 1

    print("\n— ИТОГ")
    for имя in ("взято", "пересужено", "с готовым вариантом", "вопрос обновлён",
                "вопрос заведён", "снята претензия", "ушло владельцу", "наши примеры",
                pp.NOT_ASKED):
        if отчёт.get(имя):
            print(f"   {имя:22} {отчёт[имя]}")
    print(f"\n   потрачено: ${отчёт['потрачено']:.2f} из потолка ${args.budget:.2f},"
          f" время {(time.time()-начало)/60:.0f} мин")
    if отчёт.get("остановлено потолком"):
        print("\n   ⛔ ОСТАНОВЛЕНО ПОТОЛКОМ. Непересуженные остались как были.")
    if not args.apply:
        print("\n(холостой прогон: ничего не записано, нужен --apply)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
