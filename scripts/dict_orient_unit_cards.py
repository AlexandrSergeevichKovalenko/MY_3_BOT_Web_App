# -*- coding: utf-8 -*-
"""Развернуть разборы, лежащие лицом не в ту сторону. 466 слов, замер 21.08.2026.

СИМПТОМ, который увидел владелец: у «die Tonne» пример в карточке шёл «Эта машина может
увезти десять тонн.» → «Dieses Fahrzeug kann zehn Tonnen transportieren» — то есть в
немецком словаре слева стоял русский.

ПРИЧИНА ОДНА НА ВСЕ 466. Человек искал ПО-РУССКИ. Разбор собрался «русский → немецкий»
и лёг как есть на НЕМЕЦКОЕ слово. Выдача читает его как «немецкий → русский». Порчи в
данных нет — разбор просто лежит лицом не в ту сторону.

ЗАМЕР 21.08.2026 (правило отбора: немецкое поле обязано быть латиницей, русское —
кириллицей; то же правило, что у стражей от 22.07.2026):

    немецких слов с разбором        10 338
      развёрнут ВЕСЬ разбор              386
      зеркальны ТОЛЬКО примеры            80
      ВСЕГО                              466   ← чинит этот скрипт
    личных карточек людей           25 516, из них перевёрнутых 17
    общий пул                       17 326, из них перевёрнутых 88

Первый счёт дал 410 и был неполным: он смотрел только поле `word_source`, а у части
разборов заголовок лежит под вторым именем — `source_text`. Класс тот же, счёт полный.

Личные карточки и пул прикрыты стражами с 22.07.2026 и здесь НЕ трогаются: у них своя
дверь и свой бэкфилл, разворачивать их этим же скриптом — лезть в чужую починку.

ДЫРА ЗАКРЫТА В КОДЕ: `lex_units.save_unit_card` разворачивает разбор лицом к языку слова
на входе. Это единственная дверь, через которую разбор попадает на единицу.

    python3 scripts/dict_orient_unit_cards.py           # показать
    python3 scripts/dict_orient_unit_cards.py --apply   # развернуть
"""
from __future__ import annotations

import argparse
import json
import os
import sys

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SKIP_BILLING_LEDGER_WRITES", "1")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.database import get_db_connection_context  # noqa: E402
from backend.lex_units import (  # noqa: E402
    card_is_facing_away,
    orient_card_to_unit_language,
    orient_examples_to_unit_language,
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--list", type=int, default=12, help="сколько примеров показать")
    args = parser.parse_args()

    turned = 0
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            # Порциями по id: разбор — это JSONB, и одним запросом десять тысяч штук
            # через публичный прокси не долетают (обрыв по таймауту, прогон 21.08.2026).
            rows: list = []
            last_id = 0
            while True:
                # ТОЛЬКО НЕМЕЦКИЕ СЛОВА — ровно тот класс, что померен (410 из 10 335).
                # У русских единиц разбор тоже лежит «немецкий → русский», но это не тот
                # же дефект: карточку по русскому запросу выдача берёт с НЕМЕЦКОЙ стороны
                # связи (`_build_item`), и разбор на русской единице на экран не идёт.
                # Расширять правку на них без отдельного замера — менять то, чего не мерил.
                cursor.execute("""SELECT id, lang, display, card FROM bt_3_lex_units
                                   WHERE card IS NOT NULL AND lang = 'de' AND id > %s
                                   ORDER BY id LIMIT 400;""", (last_id,))
                batch = cursor.fetchall()
                if not batch:
                    break
                rows.extend(batch)
                last_id = batch[-1][0]

            facing_away = []
            for uid, lang, display, card in rows:
                whole = card_is_facing_away(card, lang)
                examples_only = (not whole
                                 and orient_examples_to_unit_language(card, lang) is not card)
                if whole or examples_only:
                    facing_away.append((uid, lang, display, card,
                                        "весь разбор" if whole else "только примеры"))

            whole_count = sum(1 for item in facing_away if item[4] == "весь разбор")
            print(f"\nслов с разбором: {len(rows)}")
            print(f"лежит лицом не в ту сторону: {len(facing_away)}"
                  f"  (весь разбор: {whole_count}, только примеры: {len(facing_away) - whole_count})\n")
            for uid, lang, display, card, what in facing_away[: args.list]:
                example = next((e for e in (card.get("usage_examples") or [])
                                if isinstance(e, dict)), {})
                print(f"  {uid:>6} {display!r}  — {what}")
                print(f"         было:   {card.get('word_source')!r} → {card.get('word_target')!r}")
                if example:
                    print(f"         пример: {example.get('source')!r} → {example.get('target')!r}")

            if not args.apply:
                print("\nСУХОЙ ПРОГОН. Ничего не изменено. Применить: --apply\n")
                return 0

            for uid, lang, display, card, _what in facing_away:
                fixed = orient_examples_to_unit_language(
                    orient_card_to_unit_language(card, lang), lang)
                if fixed is card:
                    continue
                cursor.execute(
                    "UPDATE bt_3_lex_units SET card = %s::jsonb, updated_at = NOW() "
                    "WHERE id = %s;",
                    (json.dumps(fixed, ensure_ascii=False), uid),
                )
                turned += 1
        conn.commit()

    print(f"\nразвёрнуто: {turned}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
