# -*- coding: utf-8 -*-
"""Спросить справочник о спряжении ВСЕХ глаголов словаря. Долгий, возобновляемый.

Ночной прогрев (`warm_verb_paradigms`) берёт маленькую порцию и при первом молчании
справочника останавливается — так и нужно для регулярной работы. Этот скрипт делает
разовый полный обход: он не останавливается на лимите, а отступает и повторяет тот же
глагол.

ТЕМП. Замер 17.08.2026: при паузе 1.5 с справочник уходит в лимит примерно на десятом
запросе подряд. Поэтому база — 4 секунды, а на молчание идёт отступление
30 → 60 → 120 → 300 секунд. Жадность здесь уже стоила проекту 2737 ложных пометок
«страницы нет», и повторять это нельзя: молчание справочника НЕ записывается.

ВОЗОБНОВЛЯЕМОСТЬ. Спрашиваем только тех, кого ещё нет в кэше, поэтому скрипт можно
прерывать и запускать снова — он продолжит с того места, где остановился.

    python3 scripts/warm_verb_paradigms_all.py              # весь остаток
    python3 scripts/warm_verb_paradigms_all.py --limit 200  # только N глаголов
"""
from __future__ import annotations

import argparse
import os
import sys
import time

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SKIP_BILLING_LEDGER_WRITES", "1")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.german_verb_paradigms import (                       # noqa: E402
    ensure_german_verb_paradigm_schema,
    fetch_documented_tables,
    pending_paradigm_verbs,
    store_paradigm,
)

BASE_PAUSE_SEC = float((os.getenv("VERB_PARADIGM_FULL_PAUSE_SEC") or "4").strip() or "4")
BACKOFF_SEC = (30, 60, 120, 300)


def pending_verbs(limit: int | None) -> list[str]:
    """Глаголы, о которых ещё не спрашивали.

    Отбор ОДИН на все прогревы и живёт в справочнике
    (`backend.german_verb_paradigms.pending_paradigm_verbs`). Здесь была своя копия
    запроса — она брала кандидатов только из наших единиц, поэтому основы («gehen»,
    «stellen», «graben») не попадали в прогрев никогда. 01.09.2026 копия убрана.
    """
    return pending_paradigm_verbs(limit)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()

    ensure_german_verb_paradigm_schema()
    verbs = pending_verbs(args.limit or None)
    print("спросить осталось: %d глаголов" % len(verbs), flush=True)

    asked = documented = no_page = 0
    started = time.time()
    for index, verb in enumerate(verbs, 1):
        tables = None
        for attempt, wait in enumerate((0,) + BACKOFF_SEC):
            if wait:
                print("   справочник молчит — жду %d c (%s)" % (wait, verb), flush=True)
                time.sleep(wait)
            tables = fetch_documented_tables(verb)
            if tables is not None:
                break
        if tables is None:
            print("   справочник не отвечает после всех отступлений — останавливаюсь на «%s»" % verb,
                  flush=True)
            break
        store_paradigm(verb, tables)
        asked += 1
        if tables.get("praesens"):
            documented += 1
        else:
            no_page += 1
        if index % 25 == 0:
            spent = time.time() - started
            speed = index / spent * 3600 if spent else 0
            print("   %d/%d · подтверждено %d · страницы нет %d · %.0f глаголов в час"
                  % (index, len(verbs), documented, no_page, speed), flush=True)
        time.sleep(BASE_PAUSE_SEC)

    print()
    print("спрошено: %d · подтверждено справочником: %d · страницы нет: %d"
          % (asked, documented, no_page), flush=True)
    print("время: %.1f мин" % ((time.time() - started) / 60), flush=True)


if __name__ == "__main__":
    main()
