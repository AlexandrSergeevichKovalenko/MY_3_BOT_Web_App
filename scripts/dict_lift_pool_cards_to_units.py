# -*- coding: utf-8 -*-
"""Поднять на пустое слово разбор, который УЖЕ КУПЛЕН и лежит в общем кеше словаря.

ЗАЧЕМ. Слой общих слов появился 27.07.2026, а разборы покупались с февраля — полгода они
складывались в кеш словаря и в личные карточки. Когда слова переехали в слой, разборы за
ними не пошли: на слове пусто, а в кеше лежит полновесный разбор того же выражения.

Ночной добор этого не видит: он берёт слова без разбора и честно идёт ПОКУПАТЬ заново.
Замер 25.08.2026: так мы собирались второй раз оплатить 1 235 разборов, часть которых
куплена ещё в феврале.

    пустых слов и словосочетаний        1 815
    из них разбор уже есть в кеше       1 235   ← этот скрипт
    кандидат ровно один                 1 211
    кандидатов несколько                   24   ← берём самый полный
    остаётся модели                      ~580

⚠ СВЯЗЬ ИДЁТ ПО ТЕКСТУ, а не по ссылке, и это опаснее. Защиты три:
  1. текст сравнивается по голой форме — без артикля, регистра и лишних пробелов;
  2. при нескольких кандидатах берётся САМЫЙ ПОЛНЫЙ, а не первый попавшийся;
  3. запись идёт через дверь `save_unit_card`, а она разворачивает разбор лицом к языку
     слова и не пропускает размноженный текст. Половина кандидатов собрана со стороны
     «русский → немецкий», и без разворота они легли бы задом наперёд.

⚠ МОДЕЛЬ НЕ УЧАСТВУЕТ. Источник «переезд пула» не значится сочинённым, поэтому второй
голос не вызывается: проверять нечего, текст уже был проверен, когда его покупали.

    python3 scripts/dict_lift_pool_cards_to_units.py            # показать
    python3 scripts/dict_lift_pool_cards_to_units.py --apply    # перенести
"""
from __future__ import annotations

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SKIP_BILLING_LEDGER_WRITES", "1")

from backend.database import get_db_connection_context, card_content_score  # noqa: E402
from backend import lex_units                                               # noqa: E402

ИСТОЧНИК = "переезд пула"

# ⚠ ТОНКИЙ РАЗБОР ХУЖЕ ПУСТОГО, и это не фигура речи. Слово с разбором ночной добор
# больше НЕ БЕРЁТ (`units_needing_card`: card IS NULL). Перенеси мы разбор в 64 байта —
# слово перестанет считаться пустым и останется таким навсегда, а человек будет открывать
# карточку с одной строчкой.
#
# Планка та же, что у продукта на записи: считаем ЗАПОЛНЕННЫЕ блоки, а не длину текста.
# Три блока — это перевод плюс хоть что-то ещё; всё, что тоньше, оставляем модели.
МИНИМУМ_БЛОКОВ = 3

ЗАПРОС = """
    WITH пустые AS (
      SELECT id, display, kind,
             lower(regexp_replace(btrim(display), '^(der|die|das)\\s+', '', 'i')) AS ключ
      FROM bt_3_lex_units
      WHERE lang = 'de' AND card IS NULL AND kind IN ('word', 'collocation')
    ), пары AS (
      SELECT p.id AS unit_id, p.display, e.response_json,
             length(e.response_json::text) AS вес,
             row_number() OVER (PARTITION BY p.id ORDER BY length(e.response_json::text) DESC) AS место
      FROM пустые p
      JOIN bt_3_dictionary_entries e
        ON lower(btrim(COALESCE(e.word_de, e.source_text))) = p.ключ
       AND e.response_json IS NOT NULL
       AND COALESCE(e.source_lang, '') <> 'en'
    )
    SELECT unit_id, display, response_json, вес FROM пары WHERE место = 1 ORDER BY unit_id;
"""


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(ЗАПРОС)
            строки = cur.fetchall()
    if args.limit:
        строки = строки[: args.limit]

    print(f"пустых слов с готовым разбором в кеше: {len(строки)}\n")
    перенесено = отказано = тонких = 0
    for unit_id, display, разбор, вес in строки:
        if not isinstance(разбор, dict) or not разбор:
            отказано += 1
            continue
        блоков = card_content_score(разбор)
        if блоков < МИНИМУМ_БЛОКОВ:
            тонких += 1
            if not args.apply:
                print(f"   [{unit_id}] {str(display)[:44]:46} ТОНКИЙ ({блоков} блока) — оставляем модели")
            continue
        if not args.apply:
            print(f"   [{unit_id}] {str(display)[:44]:46} блоков {блоков}, вес {вес}")
            continue
        # Дверь сама развернёт лицом к языку слова и отвергнет размноженный текст.
        if lex_units.save_unit_card(int(unit_id), dict(разбор), source=ИСТОЧНИК):
            перенесено += 1
        else:
            отказано += 1
            print(f"   [{unit_id}] {str(display)[:44]:46} дверь не приняла")
        if args.apply and (перенесено + отказано) % 100 == 0:
            print(f"   … {перенесено + отказано}/{len(строки)}, перенесено {перенесено}")

    if args.apply:
        print(f"\n— ИТОГ\n   перенесено: {перенесено}\n   не принято дверью: {отказано}")
        print(f"   тонких, оставлены модели: {тонких}")
        print("   потрачено:  $0.00 — разбор был куплен раньше")
    else:
        print("\n(холостой прогон: ничего не записано, нужен --apply)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
