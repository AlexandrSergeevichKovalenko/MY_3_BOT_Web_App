# -*- coding: utf-8 -*-
"""Заголовок-глагол в спряжённой форме → словарная форма. И то, что глаголом не является.

Откуда список
─────────────
Полный обход справочника спряжений 18.08.2026 спросил про все 1322 глагола словаря.
У 86 страницы не нашлось — и, разбирая почему, стало видно, что это в основном НЕ пробел
справочника, а наш испорченный заголовок: причастие вместо инфинитива («begriffen»,
«durchgewunken»), спряжённая форма («klingt», «reicht»), опечатка («laueren»), старое
написание через ß, а иногда и вовсе не глагол («qualen» — это «die Qual»).

То есть обход дал точный список брака, который раньше приходилось искать глазами.

Каждая строка разобрана по СОБСТВЕННОМУ переводу слова, он написан рядом. Там, где
перевод не позволяет решить однозначно, слово оставлено владельцу (список в конце).

    python3 scripts/dict_fix_verb_headword_forms.py            # сухой прогон
    python3 scripts/dict_fix_verb_headword_forms.py --apply    # записать
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
from backend.lex_units import normalize_query                     # noqa: E402
# Слияние дубликатов берём готовое: та же механика, что чинила заголовки во
# множественном числе 15.08.2026 — переносит связи, написания и карточки и убирает
# дубликат. Своей копии не заводим, иначе две версии разойдутся.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from dict_fix_plural_headwords import merge_into                  # noqa: E402

# id → (что лежит, как правильно, часть речи, перевод слова — по нему и решали)
FIXES = {
    24746: ("darlege", "darlegen", "verb", "объяснить"),
    65: ("erklärt", "erklären", "verb", "объясняет"),
    25174: ("Gedieh", "gedeihen", "verb", "развивался"),
    41349: ("Klingt", "klingen", "verb", "звучит"),
    11529: ("mitkriege", "mitkriegen", "verb", "понять"),
    23656: ("mutmaße", "mutmaßen", "verb", "предполагать"),
    93: ("reicht", "reichen", "verb", "достаточно"),
    41219: ("wütete", "wüten", "verb", "бушевал"),
    26319: ("begriffen", "begreifen", "verb", "понимать"),
    9558: ("durchgewunken", "durchwinken", "verb", "пропущен без проверки"),
    41427: ("geschwommen", "schwimmen", "verb", "плавал"),
    13815: ("rausgenommen", "rausnehmen", "verb", "вытащенный"),
    25305: ("besagt", "besagen", "verb", "Это говорит о том, что"),
    # опечатка: «lauern» с одним «e»
    456: ("laueren", "lauern", "verb", "выслеживать"),
    # английское написание немецкого глагола «schustern» — перевод описывает именно его
    38773: ("schuster", "schustern", "verb", "делать небрежно, плохого качества"),
    # старое написание: после реформы «ss» после краткого гласного
    23791: ("mißbrauchen", "missbrauchen", "verb", "злоупотреблять"),
    24979: ("mißlingen", "misslingen", "verb", "не удаваться"),
    # НЕ ГЛАГОЛЫ. Перевод прямо это показывает.
    25957: ("verzeh", "der Verzehr", "noun", "потребление"),
    14931: ("qualen", "die Qual", "noun", "мучение"),
    23855: ("berufstätigen", "berufstätig", "adjective", "работающий"),
}

# Разобрать по переводу нельзя — решение за владельцем.
NEEDS_OWNER = {
    "Hätte": "перевод «Если бы да кабы» — это идиома, а не глагол «haben». "
             "Переименовать в «haben» значит потерять смысл.",
    "möchten": "форма от «mögen», но живёт как самостоятельный модальный глагол; "
               "справочник страницы не даёт.",
    "verzockt": "перевод «проигранный (в азартные игры)» — причастие в роли "
                "прилагательного, а не инфинитив «verzocken».",
    "bore": "английское слово с переводом «скучный человек»; в немецком словаре "
            "ему не место, но это сохранил человек.",
    "slay": "то же самое: английский сленг, перевод «круто выглядеть».",
}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    total = {"cards": 0, "places": 0, "pool": 0, "tasks_dropped": 0}
    renamed = merged = skipped = 0
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            for unit_id, (old, new, pos, meaning) in sorted(FIXES.items(), key=lambda kv: kv[1][0]):
                cur.execute("SELECT display FROM bt_3_lex_units WHERE id = %s;", (unit_id,))
                row = cur.fetchone()
                if not row:
                    print("   %-7s %-16s единицы нет — ПРОПУСК" % (unit_id, old))
                    skipped += 1
                    continue
                if str(row[0] or "").strip() != old:
                    print("   %-7s ожидали %r, лежит %r — ПРОПУСК" % (unit_id, old, row[0]))
                    skipped += 1
                    continue
                # Рядом уже живёт слово с таким написанием — сливать без решения
                # владельца нельзя; текст в карточках и пуле всё равно правим.
                cur.execute(
                    "SELECT id, display FROM bt_3_lex_units WHERE lang='de' AND lemma_key=%s "
                    "AND id <> %s LIMIT 1;",
                    (normalize_query(new), unit_id),
                )
                twin = cur.fetchone()
                print("   %-7s %-16s → %-18s %-10s (%s)%s"
                      % (unit_id, old, new, pos, meaning[:34],
                         "  ⚠ рядом уже есть %s — заголовок не трогаю" % twin[0] if twin else ""))
                if not args.apply:
                    continue
                report = spread_correction_everywhere(cur, unit_id=unit_id,
                                                      old_text=old, new_text=new)
                for name in total:
                    total[name] += report.get(name, 0)
                if twin:
                    # Правильное слово уже живёт рядом — переименование упёрлось бы в
                    # уникальный ключ. Сливаем: карточки, связи и написания переезжают
                    # на него, испорченный дубликат исчезает.
                    merge_into(cur, dead=unit_id, alive=int(twin[0]), lang="de")
                    merged += 1
                else:
                    cur.execute(
                        "UPDATE bt_3_lex_units SET display=%s, lemma=%s, lemma_key=%s, pos=%s, "
                        "pos_source=COALESCE(pos_source,'разбор владельцем'), updated_at=NOW() "
                        "WHERE id=%s;",
                        (new, new, normalize_query(new), pos, unit_id),
                    )
                    cur.execute(
                        "INSERT INTO bt_3_lex_surfaces (lang, surface_key, unit_id, match_kind) "
                        "VALUES ('de', %s, %s, 'exact') ON CONFLICT DO NOTHING;",
                        (normalize_query(new), unit_id),
                    )
                    renamed += 1
        if args.apply:
            conn.commit()

    print()
    print("НУЖНО РЕШЕНИЕ ВЛАДЕЛЬЦА:")
    for word, why in sorted(NEEDS_OWNER.items()):
        print("   %-12s %s" % (word, why))
    print()
    if args.apply:
        print("   переименовано слов:   %d" % renamed)
        print("   слито с существующим: %d" % merged)
        print("   карточек тронуто:     %d" % total["cards"])
        print("   мест внутри разборов: %d" % total["places"])
        print("   записей пула:         %d" % total["pool"])
        print("   пропущено:            %d" % skipped)
    else:
        print("СУХОЙ ПРОГОН. Ничего не записано. Для записи добавь --apply.")


if __name__ == "__main__":
    main()
