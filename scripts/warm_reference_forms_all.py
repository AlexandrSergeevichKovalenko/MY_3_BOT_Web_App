# -*- coding: utf-8 -*-
"""Спросить справочник о склонении и степенях сравнения ВСЕХ слов. Долгий, возобновляемый.

Брат-близнец `warm_verb_paradigms_all.py`, только для существительных, прилагательных и
наречий. Темп взят оттуда же и по той же причине.

ТЕМП. Замер 18.08.2026: при паузе 2.5 с справочник отвечал 429 на каждый второй запрос.
Поэтому база — 4 секунды, а на молчание идёт отступление 30 → 60 → 120 → 300 секунд.
Молчание справочника (429, сеть) НЕ записывается в кэш: иначе слово навсегда получит
ложную пометку «страницы нет». У глаголов такая жадность уже стоила 2737 ложных пометок.

ВОЗОБНОВЛЯЕМОСТЬ. Спрашиваем только тех, кого ещё нет в кэше, поэтому скрипт можно
прерывать и запускать снова.

МОДЕЛЬ. По умолчанию ВЫКЛЮЧЕНА: полный обход и так закроет основную массу справочником
и разбором составных слов. Включать `--model` осмысленно вторым проходом — по остатку,
когда видно, сколько его и сколько он будет стоить.

    python3 scripts/warm_reference_forms_all.py                 # весь остаток
    python3 scripts/warm_reference_forms_all.py --limit 200     # только N слов
    python3 scripts/warm_reference_forms_all.py --model         # добить остаток моделью
"""
from __future__ import annotations

import argparse
import os
import sys
import time

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SKIP_BILLING_LEDGER_WRITES", "1")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.database import get_db_connection_context                      # noqa: E402
from backend.german_reference_forms import (                                # noqa: E402
    adjective_degrees_for,
    clear_unresolved,
    ensure_german_reference_forms_schema,
    fetch_adjective_degrees,
    fetch_noun_declension,
    mark_unresolved,
    noun_declension_for,
    store_adjective_degrees,
    store_noun_declension,
    unresolved_count,
)

BASE_PAUSE_SEC = 4.0
BACKOFF_SEC = (30, 60, 120, 300)

SELECT_REMAINING = """
SELECT u.lemma, u.pos
  FROM bt_3_lex_units u
 WHERE u.pos IN ('noun','adjective','adverb')
   AND u.lemma IS NOT NULL AND u.lemma <> '' AND position(' ' in u.lemma) = 0
   AND NOT EXISTS (SELECT 1 FROM bt_3_german_noun_declensions d WHERE d.noun = lower(u.lemma))
   AND NOT EXISTS (SELECT 1 FROM bt_3_german_adjective_degrees a WHERE a.adjective = lower(u.lemma))
 ORDER BY u.pos, u.lemma
"""


def _remaining(limit: int | None) -> list[tuple[str, str]]:
    sql = SELECT_REMAINING + (f" LIMIT {int(limit)}" if limit else "")
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            return [(str(a), str(b)) for a, b in (cur.fetchall() or [])]


def _ask_with_backoff(word: str, pos: str):
    """Ответ справочника, с отступлением на молчание. None — так и не ответил."""
    fetch = fetch_noun_declension if pos == "noun" else fetch_adjective_degrees
    for wait in (0, *BACKOFF_SEC):
        if wait:
            print(f"    справочник молчит — жду {wait} с", flush=True)
            time.sleep(wait)
        result = fetch(word)
        if result is not None:
            return result
    return None


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--model", action="store_true",
                        help="добивать остаток моделью (платно, два спроса на слово)")
    args = parser.parse_args()

    ensure_german_reference_forms_schema()
    words = _remaining(args.limit or None)
    print(f"осталось спросить: {len(words)}")

    stats = {"справочник": 0, "страницы нет": 0, "композит": 0, "модель": 0,
             "не закрыто": 0, "справочник так и не ответил": 0}
    for i, (word, pos) in enumerate(words, 1):
        time.sleep(BASE_PAUSE_SEC)
        fetched = _ask_with_backoff(word, pos)
        if fetched is None:
            stats["справочник так и не ответил"] += 1
            print(f"[{i}/{len(words)}] {word}: пропущен, спросим в следующий раз", flush=True)
            continue
        if pos == "noun":
            store_noun_declension(word, fetched)
            resolved = noun_declension_for(word, allow_model=args.model)
        else:
            store_adjective_degrees(word, fetched)
            resolved = adjective_degrees_for(word, allow_model=args.model)
        if not fetched:
            stats["страницы нет"] += 1
        if not resolved:
            stats["не закрыто"] += 1
            mark_unresolved(word, pos, "ни справочник, ни композит, ни модель")
        else:
            source = str(resolved.get("source") or "")
            stats["композит" if source == "правило композита"
                  else "модель" if source == "модель" else "справочник"] += 1
            clear_unresolved(word)
        if i % 25 == 0:
            print(f"[{i}/{len(words)}] {stats}", flush=True)

    print()
    print("ИТОГ:", stats)
    print("в очереди на разбор владельцем:", unresolved_count())


if __name__ == "__main__":
    main()
