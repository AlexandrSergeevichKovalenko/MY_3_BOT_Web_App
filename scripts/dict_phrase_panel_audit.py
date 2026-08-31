# -*- coding: utf-8 -*-
"""СПЛОШНОЙ ПРОХОД ПО ФРАЗАМ ПАНЕЛЬЮ ИЗ ТРЁХ ГОЛОСОВ — руками, по всей базе.

С 31.08.2026 это ТОНКАЯ ОБЁРТКА над `backend/phrase_panel.py`: там и правила
судейства, и цены, и потолок, и запись отметки. Ночью тот же код зовётся маленькой
порцией из планировщика (`bot_3._run_phrase_panel_night_safe`). Своей копии правил
здесь нет сознательно: две копии через полгода разойдутся, и одна станет неверной.

ЧТО ОСТАЛОСЬ ЗДЕСЬ И ЗАЧЕМ:
  • ключи из боевого окружения через `railway variables` — на своей машине их нет;
  • большой потолок расхода на весь проход (ночью он маленький);
  • печать прогресса и итога, чтобы прогон на несколько тысяч карточек было видно.

ПРОДОЛЖЕНИЕ С МЕСТА ОСТАНОВКИ. Проверенные карточки пропускаются по отметке, поэтому
прогон можно прерывать и запускать снова — он не начнёт сначала и не заплатит дважды.

    python3 scripts/dict_phrase_panel_audit.py --limit 20        # проба, без записи
    python3 scripts/dict_phrase_panel_audit.py --apply           # весь проход
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

# ⛔ ПОТОЛОК РАСХОДА НА ВЕСЬ ПРОХОД. Владелец 23.08.2026: «чтобы мы не превышали 15 евро
# затрат, это важно». Потолок в USD и ниже названной суммы: доллар дешевле евро, так что
# $15 гарантированно укладывается в €15, даже если курс качнётся. По достижении потолка
# прогон останавливается и больше ни одного платного запроса не делает; проверенные
# карточки помечены, поэтому следующий запуск продолжит с того же места.
BUDGET_USD = 15.0


def prod(var: str, service: str = "Postgres") -> str:
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
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--workers", type=int, default=6)
    parser.add_argument("--budget", type=float, default=BUDGET_USD,
                        help="потолок расхода в долларах; по умолчанию 15")
    args = parser.parse_args()

    сервис = "BACKEND_WEB(backend:server.py)"
    os.environ.setdefault("OPENAI_API_KEY", prod("OPENAI_API_KEY", сервис))
    os.environ.setdefault("GEMINI_API_KEY", prod("GEMINI_API_KEY", сервис))

    from backend import phrase_panel as pp

    осталось = pp.count_unchecked()
    сколько = int(args.limit) if args.limit else осталось
    print(f"карточек к проверке: {осталось} (уже проверенные пропущены)")
    print(f"берём в этот прогон: {сколько}\n")
    if сколько <= 0:
        return 0

    начало = time.time()
    показано = {"n": 0}

    def на_карточку(unit_id, display, verdict, why):
        показано["n"] += 1
        if verdict != pp.CLEAN:
            print(f"   {str(display)[:44]:46} {verdict:14} {str(why)[:60]}")
        if показано["n"] % 100 == 0:
            скорость = показано["n"] / max(time.time() - начало, 1)
            мин = (сколько - показано["n"]) / max(скорость, 0.001) / 60
            print(f"   … {показано['n']}/{сколько}, осталось ~{мин:.0f} мин")

    отчёт = pp.run_batch(limit=сколько, budget_usd=float(args.budget),
                         workers=int(args.workers), apply=bool(args.apply),
                         on_card=на_карточку)

    if отчёт.get("пропущено"):
        print(f"\n⛔ ПРОГОН НЕ ШЁЛ: {отчёт['пропущено']}")
        return 1

    print("\n— ИТОГ")
    for вердикт in (pp.CLEAN, pp.DEFECT, pp.HUMANS_OWN, pp.DISPUTED, pp.NOT_ASKED):
        число = int(отчёт.get(вердикт) or 0)
        if число:
            доля = 100 * число / max(отчёт["проверено"], 1)
            print(f"   {вердикт:16} {число:>6}  ({доля:.1f}%)")
    if отчёт.get("ушло владельцу"):
        print(f"\n   ушло владельцу вопросом: {отчёт['ушло владельцу']} "
              f"(экран «Спорные фразы», с именем поля и готовым вариантом)")
    print(f"\n   потрачено: ${отчёт['потрачено']:.2f} из потолка ${args.budget:.2f},"
          f" время {(time.time()-начало)/60:.0f} мин")
    print(f"   осталось непроверенных: {отчёт['осталось']}")
    if отчёт.get("остановлено потолком"):
        print("\n   ⛔ ПРОГОН ОСТАНОВЛЕН ПОТОЛКОМ РАСХОДА. Непроверенные карточки")
        print("      остались в остатке и ждут следующего запуска — ни одна из них")
        print("      не помечена как проверенная.")
    if not args.apply:
        print("\n(холостой прогон: отметки не записаны, нужен --apply)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
