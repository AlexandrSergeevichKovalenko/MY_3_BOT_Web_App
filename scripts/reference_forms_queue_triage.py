# -*- coding: utf-8 -*-
"""Разобрать очередь «форм нет» на три кучи. Сухой прогон по умолчанию.

Зачем. В очередь на разбор владельцем попало 181 слово, но замер 19.08.2026 показал,
что вопросом к человеку является меньше половины:

    82  страницы в справочнике нет вовсе — «Bierhausschwätzer», «Beschimpfungen»
        (множественное), «aufenthaltsgenehmigung» (существительное со строчной);
    29  часть речи у НАС неверна — «ausstatten» и «ausreißen» это глаголы,
        «abgestumpft» причастие, «besonderer» склонённая форма;
    70  слово настоящее — но у большинства это НАРЕЧИЕ, у которого степеней
        сравнения не бывает, и справочник честно об этом говорит.

Спрашивать у владельца «какой артикль у глагола ausstatten» — впустую тратить его
время. Поэтому очередь разбирается по источнику, а не по нашим пометкам.

ЧТО ДЕЛАЕТ КАЖДАЯ КУЧА
    нет страницы / часть речи неверна → это дефект ЗАГОЛОВКА, а не пробел формы.
        Снимается с очереди владельца, причина переписывается на «негодный заголовок».
        Строка остаётся в таблице — решение хранится, повторно не всплывёт.
    слово есть, но степеней у него нет → это ОТВЕТ справочника, а не пустота.
        Записывается как «несравнимое» и уходит с очереди.
    остальное → настоящий вопрос, остаётся владельцу.

    python3 scripts/reference_forms_queue_triage.py            # сухой прогон
    python3 scripts/reference_forms_queue_triage.py --apply    # применить
"""
from __future__ import annotations

import argparse
import os
import re
import sys
import time
from collections import Counter

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SKIP_BILLING_LEDGER_WRITES", "1")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.database import get_db_connection_context            # noqa: E402
import backend.german_reference_forms as R                        # noqa: E402

BATCH = 40
POS_OK = {"adjective": {"Adjektiv", "Adverb"}, "adverb": {"Adjektiv", "Adverb"},
          "noun": {"Substantiv"}}


def _queue() -> list[tuple[str, str]]:
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT word, pos FROM bt_3_reference_forms_unresolved "
                        "WHERE NOT reviewed ORDER BY word")
            return [(str(a), str(b)) for a, b in (cur.fetchall() or [])]


def _classify(word: str, pos: str, text: str) -> tuple[str, str]:
    """(куча, пояснение)."""
    if not text:
        return "заголовок негоден", "страницы в справочнике нет"
    kinds = set(re.findall(r"\{\{Wortart\|([^|}]+)", text))
    if not (kinds & POS_OK.get(pos, set())):
        return "заголовок негоден", f"часть речи не {pos}, а {sorted(kinds)[:3]}"
    if (pos in ("adjective", "adverb") and "Adjektiv" not in kinds
            and not R._blocks(text, "Adjektiv")):
        # Страница есть, слово объявлено НАРЕЧИЕМ, таблицы степеней нет — у наречий
        # образа действия («allerdings», «ansonsten») её и не бывает. Это ОТВЕТ.
        #
        # Проверка вреда 19.08.2026 перед применением: если слово объявлено
        # ПРИЛАГАТЕЛЬНЫМ, а таблицы всё равно нет, молча звать его несравнимым нельзя —
        # это может быть пробел справочника. Таких нашлось одно («facile»), и оно
        # остаётся вопросом к владельцу.
        return "степеней не бывает", "справочник: это наречие, степеней у него нет"
    return "вопрос владельцу", "слово настоящее, форм справочник не дал"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    queue = _queue()
    print(f"в очереди сейчас: {len(queue)}")
    piles: dict[str, list[tuple[str, str, str]]] = {}
    silent = 0

    for i in range(0, len(queue), BATCH):
        chunk = queue[i:i + BATCH]
        sources = None
        for _ in range(4):
            sources = R.fetch_sources_bulk([R._reference_title(w, p) for w, p in chunk])
            if sources is not None:
                break
            time.sleep(8)
        if sources is None:
            silent += len(chunk)
            continue
        for word, pos in chunk:
            text = sources.get(R._reference_title(word, pos)) or sources.get(word) or ""
            pile, why = _classify(word, pos, text)
            piles.setdefault(pile, []).append((word, pos, why))
        time.sleep(2)

    if silent:
        print(f"⚠ справочник промолчал про {silent} слов — они остаются в очереди")
    print()
    for pile, items in sorted(piles.items(), key=lambda kv: -len(kv[1])):
        print(f"  {len(items):4}  {pile}")
        for word, _pos, why in items[:6]:
            print(f"        {word} — {why}")
    print()

    if not args.apply:
        print("Это СУХОЙ ПРОГОН. Ничего не изменено. Применить: --apply")
        return

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            for word, pos, why in piles.get("заголовок негоден", []):
                cur.execute(
                    "UPDATE bt_3_reference_forms_unresolved "
                    "SET reviewed = TRUE, reason = %s WHERE word = %s;",
                    (f"негодный заголовок: {why}", word),
                )
            for word, pos, why in piles.get("степеней не бывает", []):
                cur.execute(
                    "UPDATE bt_3_reference_forms_unresolved "
                    "SET reviewed = TRUE, reason = %s WHERE word = %s;",
                    ("справочник: степеней сравнения не бывает", word),
                )
        conn.commit()
    # Ответ «степеней не бывает» кладём в кэш форм — это знание, а не пустота.
    for word, pos, _why in piles.get("степеней не бывает", []):
        R.store_adjective_degrees(word, {
            "positive": word[:1].lower() + word[1:], "comparative": "", "superlative": "",
            "gradable": False, "source": "wiktionary-steigerung"})
    print("применено.")
    print(f"снято с очереди владельца: "
          f"{len(piles.get('заголовок негоден', [])) + len(piles.get('степеней не бывает', []))}")
    print(f"осталось вопросов владельцу: {len(piles.get('вопрос владельцу', []))}")


if __name__ == "__main__":
    main()
