# -*- coding: utf-8 -*-
"""Уборка накопленных разборов пар: строка «Конструкция» и строка «Приставка».

ЗАЧЕМ. 30.08.2026 владелец прислал с экрана «anbieten ↔ unterbreiten»:

    jdm. etw. anbieten + Dativ + Akkusativ · Dativ + Akkusativ
    etw. zum Verkauf anbieten + zum + Akkusativ + Akkusativ · Akkusativ + Akkusativ

Падеж печатался дважды, предлог приписывался второй раз, а «zum + Akkusativ» —
грамматическая ложь (zum = zu dem, это Dativ). Систему починили в тот же день
(`_word_diff_construction_view`), но ГОТОВЫЕ разборы лежат в `bt_3_word_diff_cards`
целиком, вместе со склеенными строками: новая сборка их не касается. Открыв старую
пару, человек увидит ту же кашу. Этот скрипт — вторая половина работы: он чинит то,
что уже лежит.

ЧТО ДЕЛАЕТ, по каждой карточке:
  1. расклеивает запись конструкции (`_word_diff_construction_unglue`) и собирает её
     заново тем же правилом, что и живая выдача, — разойтись они не могут;
  2. дописывает глаголам отделяемость приставки из справочника напечатанных форм;
     чего справочник не знает — остаётся «не знаем» и печатается числом.

Ничего не выдумывает: новых конструкций, падежей и приставок не появляется.

ЗАПУСК (по умолчанию — только показать, ничего не писать):
    railway run --service BACKEND_WEB python3 scripts/word_diff_rebuild_cards.py
    railway run --service BACKEND_WEB python3 scripts/word_diff_rebuild_cards.py --apply
"""
import json
import os
import sys

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SKIP_BILLING_LEDGER_WRITES", "1")
# Справочник спряжений в скриптах уборки включается явно — так же, как в остальных.
os.environ.setdefault("VERB_PARADIGM_LOOKUP", "1")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend import backend_server as B  # noqa: E402
from backend import german_grammar_tables as G  # noqa: E402
from psycopg2.extras import Json  # noqa: E402

from backend.database import get_db_connection_context  # noqa: E402

APPLY = "--apply" in sys.argv


def db():
    """Соединение с базой, с честными повторами.

    Проверено 30.08.2026: первый коннект к `zephyr.proxy.rlwy.net` из внешней сети
    таймаутится примерно через раз, второй проходит. Это ретрай сети, а не fallback:
    после трёх неудач скрипт падает и НИЧЕГО не делает вида, что отработал.
    """
    last = None
    for attempt in range(3):
        try:
            return get_db_connection_context()
        except Exception as exc:
            last = exc
            print(f"  … база не ответила (попытка {attempt + 1}/3): {str(exc)[:70]}")
    raise last


def rebuild_constructions(payload: dict) -> int:
    """Пересобрать записи конструкций. Возвращает число изменённых строк."""
    changed = 0
    for row in (payload.get("constructions") or []):
        if not isinstance(row, dict) or row.get("bare"):
            continue
        word = str(row.get("word") or "")
        case = str(row.get("case") or "")
        prep = str(row.get("preposition") or "")
        raw = B._word_diff_construction_unglue(row.get("pattern") or "", case, prep)
        text, note = B._word_diff_construction_view(word, raw, case, prep)
        if (row.get("pattern") != text or row.get("case_note") != note
                or row.get("case") != B._word_diff_case_text(case)):
            row["pattern"] = text
            row["case"] = B._word_diff_case_text(case)
            row["case_note"] = note
            changed += 1
    return changed


def rebuild_separability(payload: dict) -> tuple[int, int]:
    """Дописать отделяемость приставки. Возвращает (заполнено, осталось «не знаем»)."""
    filled = unknown = 0
    for row in (payload.get("usage") or []):
        if not isinstance(row, dict) or row.get("separability"):
            continue
        word = str(row.get("word") or "")
        # Ответ приходит только на документированный глагол: страницы Flexion с
        # личными формами у существительного не бывает. Поэтому отдельно спрашивать
        # часть речи не нужно — её уже сказал справочник.
        # Уборке РАЗРЕШЕНО достраивать источник: статьи слова нет в памяти —
        # спрашиваем её и запоминаем навсегда (`verb_readings`).
        answer = G.verb_prefix_separability(word, allow_network=True) or {}
        if answer:
            row["separability"] = answer
            filled += 1
        elif not G.leading_verb_prefix(word):
            # Приставки нет вовсе — строке про отделяемость взяться неоткуда, и это
            # не пробел: у «machen» такой строки быть не должно.
            continue
        else:
            unknown += 1
    return filled, unknown


def main() -> None:
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT pair_key, payload FROM bt_3_word_diff_cards ORDER BY pair_key;")
            rows = cur.fetchall()

    cards = touched = lines = prefixes = unknown_total = 0
    unknown_words: set[str] = set()
    updates: list[tuple[str, dict]] = []
    for pair_key, payload in rows:
        cards += 1
        if isinstance(payload, str):
            payload = json.loads(payload)
        if not isinstance(payload, dict):
            continue
        changed_lines = rebuild_constructions(payload)
        filled, unknown = rebuild_separability(payload)
        for row in (payload.get("usage") or []):
            if isinstance(row, dict) and not row.get("separability"):
                word = str(row.get("word") or "")
                if G.leading_verb_prefix(word):
                    unknown_words.add(word)
        lines += changed_lines
        prefixes += filled
        unknown_total += unknown
        if changed_lines or filled:
            touched += 1
            updates.append((pair_key, payload))
            if len(updates) <= 5:
                print(f"  {pair_key}: строк пересобрано {changed_lines}, приставок дописано {filled}")

    print(f"\nкарточек всего:            {cards}")
    print(f"карточек изменено:         {touched}")
    print(f"записей конструкций:       {lines}")
    print(f"приставок дописано:        {prefixes}")
    print(f"глаголов без ответа:       {unknown_total}"
          + (f"  ({', '.join(sorted(unknown_words))})" if unknown_words else ""))

    if not APPLY:
        print("\nЭто показ. Записать: добавьте --apply")
        return
    with db() as conn:
        with conn.cursor() as cur:
            for pair_key, payload in updates:
                # payload — JSONB, поэтому Json(), а не строка: иначе Postgres
                # откажется приводить text к jsonb и уборка встанет на первой карточке.
                cur.execute(
                    "UPDATE bt_3_word_diff_cards SET payload = %s WHERE pair_key = %s;",
                    (Json(payload), pair_key),
                )
        conn.commit()
    print(f"\nзаписано карточек: {len(updates)}")


if __name__ == "__main__":
    main()
