# -*- coding: utf-8 -*-
"""Заголовки в форме zu-инфинитива → словарная форма: «klarzukommen» → «klarkommen».

Зачем
─────
«zu» между отделяемой приставкой и основой — синтаксическая частица, а не часть слова.
Ни один немецкий глагол не имеет её в словарной форме. Но заголовок карточки брался из
ответа модели без единой проверки, и от него же наш движок достраивал парадигму —
печатая «ich klarzukomme, du klarzukommst», форм, которых в языке нет.

Причина закрыта на входе (backend_server.py, _normalize_saved_german_single_word) и в
самом движке таблиц (german_grammar_tables.py: от zu-инфинитива таблица не строится).
Этот скрипт разбирает накопленное.

Замер 14.08.2026: пять слов, 15 записей в трёх хранилищах. Правильных форм в базе нет
ни для одного — значит переименование не создаст дублей. Проверяется прямо в скрипте.

    python3 scripts/dict_fix_zu_infinitive_headwords.py            # сухой прогон
    python3 scripts/dict_fix_zu_infinitive_headwords.py --apply    # записать
"""
from __future__ import annotations

import argparse
import os
import sys

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SKIP_BILLING_LEDGER_WRITES", "1")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.database import get_db_connection_context            # noqa: E402
from backend.german_grammar_tables import (                       # noqa: E402
    looks_like_zu_infinitive,
    strip_zu_infinitive,
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, display, lemma, lemma_key FROM bt_3_lex_units "
                "WHERE lang = 'de' AND lemma ~ 'zu' ORDER BY id;"
            )
            units = [(i, d, l, k) for i, d, l, k in cur.fetchall() if looks_like_zu_infinitive(l)]

            cur.execute(
                "SELECT id, word_de FROM bt_3_webapp_dictionary_queries "
                "WHERE word_de ~ 'zu' ORDER BY id;"
            )
            cards = [(i, w) for i, w in cur.fetchall() if looks_like_zu_infinitive(w)]

            cur.execute(
                # Немецкая сторона лежит в РАЗНЫХ полях в зависимости от направления
                # запроса: при de→ru в source_text, при ru→de в target_text. Проверять
                # надо все три, иначе половина записей пула не находится.
                "SELECT id, response_json->>'word_de', response_json->>'source_text', "
                "       response_json->>'target_text' "
                "FROM bt_3_dictionary_entries WHERE response_json::text ~ 'zu';"
            )
            entries = []
            for i, w, s_txt, t_txt in cur.fetchall():
                german = next((x for x in (w, s_txt, t_txt) if x and looks_like_zu_infinitive(x)), None)
                if german:
                    entries.append((i, german, german))

        print("единиц словаря: %d · личных карточек: %d · записей пула: %d"
              % (len(units), len(cards), len(entries)))

        # Проверка на дубли: правильной формы в слое единиц быть не должно.
        collisions = []
        with conn.cursor() as cur:
            for unit_id, display, lemma, _key in units:
                fixed = strip_zu_infinitive(lemma)
                cur.execute(
                    "SELECT id FROM bt_3_lex_units WHERE lang = 'de' AND lemma_key = %s AND id <> %s;",
                    (fixed.casefold(), unit_id),
                )
                found = cur.fetchall()
                mark = "СТОЛКНЁТСЯ с %s" % [r[0] for r in found] if found else "чисто"
                if found:
                    collisions.append(unit_id)
                print("   единица %-7s %-16s → %-16s %s" % (unit_id, lemma[:16], fixed[:16], mark))
        for card_id, word in cards:
            print("   карточка %-7s %-16s → %s" % (card_id, word[:16], strip_zu_infinitive(word)))
        for entry_id, word, source in entries:
            base = word or source
            print("   пул      %-7s %-16s → %s" % (entry_id, str(base)[:16], strip_zu_infinitive(base)))

        if collisions:
            print("\n⚠ Есть столкновения — это не переименование, а слияние. Останавливаюсь.")
            return
        if not args.apply:
            print("\nСУХОЙ ПРОГОН. Ничего не записано. Для записи добавь --apply.")
            return

        with conn.cursor() as cur:
            for unit_id, display, lemma, _key in units:
                # zu-инфинитив всегда глагол, а глагол в словарной форме — со строчной.
                fixed = (strip_zu_infinitive(lemma) or lemma).lower()
                fixed_display = (strip_zu_infinitive(display) or display).lower()
                cur.execute(
                    "UPDATE bt_3_lex_units SET lemma = %s, lemma_key = %s, display = %s, "
                    "updated_at = NOW() WHERE id = %s;",
                    (fixed, fixed.casefold(), fixed_display, unit_id),
                )
                # Написание, по которому слово ищут, тоже должно указывать на словарную
                # форму — но СТАРОЕ оставляем: человек мог встретить именно zu-форму,
                # и по ней он должен слово находить.
                cur.execute(
                    "INSERT INTO bt_3_lex_surfaces (lang, surface_key, unit_id, match_kind) "
                    "VALUES ('de', %s, %s, 'exact') ON CONFLICT DO NOTHING;",
                    (fixed.casefold(), unit_id),
                )
                cur.execute(
                    "UPDATE bt_3_lex_surfaces SET match_kind = 'inflected' "
                    "WHERE unit_id = %s AND surface_key = %s;",
                    (unit_id, lemma.casefold()),
                )
            for card_id, word in cards:
                fixed = (strip_zu_infinitive(word) or word).lower()
                cur.execute(
                    "UPDATE bt_3_webapp_dictionary_queries "
                    "SET word_de = %s, "
                    "    response_json = jsonb_set("
                    "        jsonb_set(response_json, '{word_de}', to_jsonb(%s::text)),"
                    "        '{lemma_de}', to_jsonb(%s::text)), "
                    "    updated_at = NOW() "
                    "WHERE id = %s;",
                    (fixed, fixed, fixed, card_id),
                )
            for entry_id, word, source in entries:
                base = word or source
                fixed = (strip_zu_infinitive(base) or base).lower()
                cur.execute(
                    "UPDATE bt_3_dictionary_entries "
                    "SET response_json = jsonb_set(response_json, '{word_de}', to_jsonb(%s::text)) "
                    "WHERE id = %s;",
                    (fixed, entry_id),
                )
        conn.commit()
        print("\nЗаписано: единиц %d, карточек %d, записей пула %d"
              % (len(units), len(cards), len(entries)))


if __name__ == "__main__":
    main()
