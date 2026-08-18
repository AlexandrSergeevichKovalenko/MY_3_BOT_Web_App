# -*- coding: utf-8 -*-
"""Разбить склеенные слова и перепривязать карточки, показывавшие чужой разбор.

Решения владельца 18.08.2026, продолжение разбора словаря. Здесь то, что не сводится к
замене строки: слово надо переименовать И завести второе, а карточку — отцепить от
чужого слова и привязать к правильному.

1. «schwerfallen-leichtfallen» — два глагола, склеенных дефисом. У слова десять
   карточек, и все десять одинаковые: это запись стартового набора, розданная десяти
   разным людям («Трудно и легко даваться»). Собственный пример слова —
   «Diese Aufgabe fällt mir schwer» — то есть про schwerfallen. Поэтому слово и
   карточки становятся «schwerfallen» с переводом «даваться с трудом», а
   «leichtfallen» заводится отдельным словом словаря.

2. «zuspätkommen» слитно не существует: в немецком это три слова. Становится фразой
   «zu spät kommen».

3. Три карточки показывали разбор ЧУЖОГО слова — привязка нашла похожее написание:
       «die Eiche» (дуб)   → была привязана к глаголу «eichen» (калибровать)
       «die Glotze» (телик)→ была привязана к глаголу «glotzen» (пялиться)
       «Künftig» (впредь)  → была привязана к существительному «die Künftige»
   Правильных слов в справочнике не было вовсе — заводим и перепривязываем.

    python3 scripts/dict_split_and_rebind_18_08.py            # сухой прогон
    python3 scripts/dict_split_and_rebind_18_08.py --apply    # записать
"""
from __future__ import annotations

import argparse
import os
import sys

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SKIP_BILLING_LEDGER_WRITES", "1")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.database import (                                    # noqa: E402
    get_db_connection_context,
    spread_correction_everywhere,
)
from backend.lex_units import ensure_unit, normalize_query        # noqa: E402

# слово, которое переименовываем → (новое написание, вид, часть речи, новый русский)
RENAME = {
    "schwerfallen-leichtfallen": ("schwerfallen", "word", "verb", "даваться с трудом"),
    "zuspätkommen": ("zu spät kommen", "collocation", None, "опаздывать"),
}

# новые слова, которых в справочнике не было
CREATE = [
    ("leichtfallen", "word", "verb", "даваться легко",
     "вторая половина склеенного «schwerfallen-leichtfallen»"),
    ("die Eiche", "word", "noun", "дуб", "карточки показывали разбор глагола «eichen»"),
    ("die Glotze", "word", "noun", "телик", "карточки показывали разбор глагола «glotzen»"),
    ("künftig", "word", "adverb", "впредь", "карточка была привязана к «die Künftige»"),
]

# карточка (по написанию) → правильное слово
REBIND = {
    "die Eiche": "die Eiche",
    "die Glotze": "die Glotze",
    "Künftig": "künftig",
}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            print("ПЕРЕИМЕНОВАНИЕ:")
            for old, (new, kind, pos, russian) in sorted(RENAME.items()):
                cur.execute(
                    "SELECT id FROM bt_3_lex_units WHERE lang='de' AND lower(display)=lower(%s);",
                    (old,),
                )
                row = cur.fetchone()
                cur.execute(
                    "SELECT count(*) FROM bt_3_webapp_dictionary_queries "
                    "WHERE lower(BTRIM(word_de))=lower(%s) OR lower(BTRIM(translation_de))=lower(%s);",
                    (old, old),
                )
                cards = cur.fetchone()[0]
                print("   %-28s → %-18s карточек %d, русский → «%s»"
                      % (old, new, cards, russian))
                if not args.apply or not row:
                    continue
                unit_id = int(row[0])
                spread_correction_everywhere(cur, unit_id=unit_id, old_text=old, new_text=new)
                cur.execute(
                    "UPDATE bt_3_lex_units SET display=%s, lemma=%s, lemma_key=%s, kind=%s, "
                    "pos=COALESCE(%s, pos), updated_at=NOW() WHERE id=%s;",
                    (new, new, normalize_query(new), kind, pos, unit_id),
                )
                cur.execute(
                    "INSERT INTO bt_3_lex_surfaces (lang, surface_key, unit_id, match_kind) "
                    "VALUES ('de', %s, %s, 'exact') ON CONFLICT DO NOTHING;",
                    (normalize_query(new), unit_id),
                )
                # Русская сторона карточек должна сойтись с новым словом.
                cur.execute(
                    "UPDATE bt_3_webapp_dictionary_queries SET translation_ru=%s, word_ru=%s, "
                    "updated_at=NOW() WHERE lex_unit_id=%s;",
                    (russian, russian, unit_id),
                )
        if args.apply:
            conn.commit()

    print()
    print("ЗАВОДИМ НОВЫЕ СЛОВА:")
    created = {}
    for display, kind, pos, russian, why in CREATE:
        print("   %-14s %-6s %-8s «%s»   (%s)" % (display, kind, pos, russian, why))
        if not args.apply:
            continue
        unit_id = ensure_unit(display, "de")
        if not unit_id:
            print("      не удалось завести")
            continue
        created[display] = unit_id
        ru_id = ensure_unit(russian, "ru")
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE bt_3_lex_units SET pos=%s, kind=%s, updated_at=NOW() WHERE id=%s;",
                            (pos, kind, unit_id))
                if ru_id:
                    cur.execute(
                        "INSERT INTO bt_3_lex_links (from_unit, to_unit, rank) VALUES (%s,%s,1) "
                        "ON CONFLICT DO NOTHING;",
                        (unit_id, ru_id),
                    )
            conn.commit()
        print("      слово %s, перевод %s" % (unit_id, ru_id))

    print()
    print("ПЕРЕПРИВЯЗКА КАРТОЧЕК:")
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            for card_text, target in sorted(REBIND.items()):
                cur.execute(
                    "SELECT id FROM bt_3_lex_units WHERE lang='de' AND lower(display)=lower(%s);",
                    (target,),
                )
                row = cur.fetchone()
                cur.execute(
                    "SELECT q.id, u.display FROM bt_3_webapp_dictionary_queries q "
                    "LEFT JOIN bt_3_lex_units u ON u.id=q.lex_unit_id "
                    "WHERE lower(BTRIM(q.word_de))=lower(%s);",
                    (card_text,),
                )
                cards = cur.fetchall()
                for card_id, was in cards:
                    print("   карточка %-7s «%s» была на «%s» → «%s»"
                          % (card_id, card_text, was, target))
                    if args.apply and row:
                        cur.execute(
                            "UPDATE bt_3_webapp_dictionary_queries SET lex_unit_id=%s, "
                            "updated_at=NOW() WHERE id=%s;",
                            (int(row[0]), card_id),
                        )
        if args.apply:
            conn.commit()

    if not args.apply:
        print()
        print("СУХОЙ ПРОГОН. Ничего не записано. Для записи добавь --apply.")


if __name__ == "__main__":
    main()
