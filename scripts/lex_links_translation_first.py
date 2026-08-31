#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Первой связью СЛОВА ставим перевод, а не толкование.

ЗАЧЕМ. Строка «ПЕРЕВОД» в быстром словаре — это первая по рангу связь слова с русской
стороной. До 31.08.2026 связи строились из `card.meanings`, а список `card.translations`
смотрели, только если из значений ничего не осталось. У промпта это разные поля:
`translations[]` — эквиваленты, `meanings.*.value` — значение, и туда законно попадает
толкование. Так «Verschwörer» показывал «человек, участвующий в заговоре», хотя рядом в
той же карточке лежало «заговорщик».

Систему починили в `lex_units.card_link_values`: у СЛОВА переводы идут первыми, значения
следом; у ФРАЗЫ порядок прежний (там в `translations[]` лежат обрывки — замерено).
Этот скрипт разбирает то, что УЖЕ лежит в базе: перевешивает связи тем же кодом продукта.

    python3 scripts/lex_links_translation_first.py --dry-run   # посчитать и показать
    python3 scripts/lex_links_translation_first.py             # перевесить

Трогает ТОЛЬКО те единицы, у которых первая связь реально меняется: остальные не
переписываются вовсе. Своих переводов не выдумывает — берёт написанное в карточке.
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.database import get_db_connection_context  # noqa: E402
from backend.lex_units import (  # noqa: E402
    card_link_values,
    door_check,
    looks_like_example_not_translation,
    normalize_query,
    sync_unit_links_from_card,
)

ВЫБОРКА = """
    SELECT u.id, u.kind, u.display, u.card,
           (SELECT t.display
              FROM bt_3_lex_links l
              JOIN bt_3_lex_units t
                ON t.id = CASE WHEN l.from_unit = u.id THEN l.to_unit ELSE l.from_unit END
             WHERE (l.from_unit = u.id OR l.to_unit = u.id) AND t.lang = %s
             ORDER BY l.rank LIMIT 1) AS первая_связь
      FROM bt_3_lex_units u
     WHERE u.lang = %s
       AND u.kind = 'word'
       AND jsonb_typeof(u.card) = 'object'
     ORDER BY u.id;
"""


def проходит_заслоны(value: str, native_lang: str) -> bool:
    """Те же два заслона, что стоят в цикле записи связей (`sync_unit_links_from_card`)."""
    if looks_like_example_not_translation(value):
        return False
    if native_lang == "ru" and not any("Ѐ" <= ch <= "ӿ" for ch in value):
        return False
    return bool(door_check(value, native_lang))


def будущая_первая(card: dict, kind: str, native_lang: str) -> str:
    for item in card_link_values(card, kind=kind):
        if проходит_заслоны(item["value"], native_lang):
            return item["value"]
    return ""


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--lang", default="de")
    parser.add_argument("--native-lang", default="ru")
    parser.add_argument("--limit", type=int, default=0, help="0 — все")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--ids", default="", help="перевесить только эти единицы (через запятую)")
    args = parser.parse_args()

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(ВЫБОРКА, (args.native_lang, args.lang))
            строки = cur.fetchall() or []

    только = {int(x) for x in args.ids.replace(",", " ").split() if x.strip().isdigit()}
    к_правке = []
    for unit_id, kind, display, card, было in строки:
        if только and int(unit_id) not in только:
            continue
        if not isinstance(card, dict):
            continue
        станет = будущая_первая(card, str(kind or ""), args.native_lang)
        if not станет:
            continue
        if normalize_query(станет) == normalize_query(str(было or "")):
            continue
        к_правке.append((unit_id, display, str(было or ""), станет, card))

    if args.limit:
        к_правке = к_правке[: args.limit]

    if только:
        print(f"названо единиц: {len(только)}")
    print(f"единиц-слов с разбором: {len(строки)}")
    print(f"первая связь меняется у: {len(к_правке)}")
    for _i, display, было, станет, _c in к_правке[: (len(к_правке) if только else 30)]:
        print(f"   {display}: «{было}» → «{станет}»")
    if args.dry_run:
        print("сухой прогон: ничего не записано")
        return 0

    сделано, ошибок = 0, 0
    for unit_id, display, _было, _станет, card in к_правке:
        try:
            sync_unit_links_from_card(int(unit_id), card, native_lang=args.native_lang)
            сделано += 1
        except Exception as exc:  # noqa: BLE001 — считаем и показываем, а не глушим
            ошибок += 1
            print(f"  ! {display}: {exc}")
    print(f"перевешено: {сделано}")
    print(f"ошибок:     {ошибок}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
