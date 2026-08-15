# -*- coding: utf-8 -*-
"""Поднять разбор из личной копии на ОБЩЕЕ СЛОВО — там, где у слова разбора нет вовсе.

Зачем
─────
После перевода схемы на «одно слово, люди подписаны» содержимое берётся с общего слова.
Но у части слов разбора нет вообще, и тогда единственный источник — старая копия в
карточке человека. Замер 14.08.2026 по ВСЕМ карточкам: таких 778, за ними 251 разное
слово, годное к подъёму. (Если считать только пришедшие стартовым импортом — 153 карточки
и 17 слов; это узкая выборка, полная картина шире.)

Снять у них копию нельзя — человек останется с пустой карточкой. Но и держать копию
незачем: содержимое уже есть, оно просто лежит не там. Поднимаем его на слово — и
дальше карточка читает общее, как все остальные.

Модель не вызывается. Разбор берётся из самой карточки, ничего не выдумывается.
Из нескольких копий одного слова берётся САМАЯ БОГАТАЯ (card_content_score).

Из 778 пропускаются два вида: 279, где заголовок карточки разошёлся со словом (поднимать
нельзя — слово получит разбор чужой фразы), и 103, где в карточке нет ни одного
содержательного блока (там нужен не подъём, а обогащение).

Разбор в этих копиях часто тощий. Это нормально: как только на слове
появится карточка, её подхватит ночной добор «тонких» разборов (units_with_thin_card
в backend/lex_units.py) и доведёт до полной.

    python3 scripts/dict_promote_orphan_cards_to_units.py            # сухой прогон
    python3 scripts/dict_promote_orphan_cards_to_units.py --apply    # записать
"""
from __future__ import annotations

import argparse
import os
import sys

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SKIP_BILLING_LEDGER_WRITES", "1")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.database import (                                       # noqa: E402
    get_db_connection_context,
    card_content_score,
    unit_card_is_about_the_same_word,
)
from backend.lex_units import save_unit_card_if_richer               # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT q.id, q.word_de, q.lex_unit_id, u.lemma_key, q.response_json
                FROM bt_3_webapp_dictionary_queries q
                JOIN bt_3_lex_units u ON u.id = q.lex_unit_id
                WHERE u.card IS NULL
                  AND (q.response_json ? 'usage_examples' OR q.response_json ? 'meanings')
                ORDER BY q.id;
                """
            )
            rows = cur.fetchall()

    # Лучшая копия на каждое слово.
    best: dict[int, tuple] = {}
    skipped_wrong_word = 0
    empty_cards = 0
    for entry_id, word_de, unit_id, lemma_key, payload in rows:
        if not unit_card_is_about_the_same_word(unit_lemma_key=lemma_key, card_word=word_de):
            # Заголовок карточки и слово разошлись — поднимать нельзя, иначе слово
            # получит разбор чужой фразы. Эти разбираем руками.
            skipped_wrong_word += 1
            continue
        score = card_content_score(payload)
        if score <= 0:
            # Поднимать нечего: в карточке нет ни одного содержательного блока.
            # Такому слову нужен не подъём, а обогащение.
            empty_cards += 1
            continue
        current = best.get(int(unit_id))
        if not current or score > current[0]:
            best[int(unit_id)] = (score, entry_id, word_de, payload)

    print("карточек без разбора на слове: %d" % len(rows))
    print("  из них заголовок расходится со словом (пропускаем): %d" % skipped_wrong_word)
    print("  из них в карточке нет содержимого — нужен не подъём, а обогащение: %d" % empty_cards)
    print("  РАЗНЫХ СЛОВ к подъёму: %d" % len(best))
    print()
    for unit_id, (score, entry_id, word_de, _payload) in sorted(best.items()):
        print("   слово %-8s ← карточка %-8s %-34s богатство %d"
              % (unit_id, entry_id, str(word_de)[:34], score))

    if not args.apply:
        print("\nСУХОЙ ПРОГОН. Ничего не записано. Для записи добавь --apply.")
        return

    written = 0
    for unit_id, (_score, _entry_id, _word_de, payload) in best.items():
        try:
            if save_unit_card_if_richer(int(unit_id), payload, source="подъём из карточки"):
                written += 1
        except Exception as exc:
            print("   слово %s не принял разбор: %s" % (unit_id, exc))
    print("\nСлов получило разбор: %d из %d" % (written, len(best)))
    print("Дальше их доведёт до полного ночной добор тонких разборов.")


if __name__ == "__main__":
    main()
