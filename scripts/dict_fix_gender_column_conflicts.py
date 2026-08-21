# -*- coding: utf-8 -*-
"""Род, который лежит ОТДЕЛЬНОЙ КОЛОНКОЙ и врёт на экране: «die Narr», «die Spritpreis».

ОТКУДА БЕРЁТСЯ. Артикль в написании есть не у всех слов: у части он лежит колонкой
`gender`, и выдача приклеивает его к заголовку сама (`lex_units._build_item`). Колонку
заполняют из разбора — а разбор нередко собран про МНОЖЕСТВЕННОЕ число: у слова
«Spritpreis» разбор озаглавлен «die Spritpreise», и его «die» уезжает на единственное.
У множественного артикль всегда die, родом слова он не является.

    на экране: die Narr · die Spritpreis · die Beliebtheitswert · die Elektrogerät

ПОЧЕМУ ЭТОГО НЕ ЛОВИЛ `dict_headword_article_audit.py`. Тот сверяет артикль ВНУТРИ
написания («der Hügel»), а здесь написание голое, а род лежит рядом. Замер 21.08.2026:
из 691 такого слова арбитр подтвердил 534, промолчал про 145 и возразил по 12.

КТО РЕШАЕТ. `backend.article_authority.authoritative_article` — тот же арбитр, что решает
род в игре с артиклями: Wiktionary, банк артиклей, правило композита по 19 тысячам родов
и честное «не знаю». Мы ничего не выводим сами.

ЧТО ЗДЕСЬ РУКАМИ. Список правок закрытый и выверен поштучно 21.08.2026, потому что
арбитр не всесилен: у «Dicke» род зависит от значения («der Dicke» толстяк / «die Dicke»
толщина, в разборе — толстяк, значит der и остаётся), а «Abflughall» — обрезанное
написание слова «die Abflughalle», и род ему править бессмысленно, пока не починено само
слово. Оба случая уходят владельцу, а не правятся молча.

ДЫРА ЗАКРЫТА В КОДЕ: `lex_units.fix_gender_conflicts_from_authority` идёт ночью и сам
чинит тот класс, у которого причина известна — род, взятый из разбора про ДРУГОЕ
написание. Остальные расхождения он считает и показывает в ночном отчёте.

    python3 scripts/dict_fix_gender_column_conflicts.py           # показать
    python3 scripts/dict_fix_gender_column_conflicts.py --apply   # починить
"""
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SKIP_BILLING_LEDGER_WRITES", "1")

from backend.database import get_db_connection_context  # noqa: E402

# Слово → род, подтверждённый арбитром и проверенный глазами по переводу карточки.
# Рядом — чем разбор сбил род, чтобы правка читалась без обращения к замеру.
CONFIRMED = {
    14152: ("der", "Narr",               "разбор был про «Narren» — множественное"),
    25057: ("der", "Arbeitseinstieg",    "der Einstieg, правило композита"),
    32628: ("der", "Spritpreis",         "разбор про «die Spritpreise» — множественное"),
    32631: ("der", "Nierenschmerz",      "разбор про «die Nierenschmerzen» — множественное"),
    32633: ("der", "Beliebtheitswert",   "разбор про «die Beliebtheitswerte» — множественное"),
    32639: ("das", "Menschenrecht",      "разбор про «die Menschenrechte» — множественное"),
    38552: ("das", "Hintergrundgeräusch","разбор про «Hintergrundgeräusche» — множественное"),
    41440: ("das", "Elektrogerät",       "разбор про «die Elektrogeräte» — множественное"),
    41524: ("die", "Luke",               "die Luke, wiktionary; перевод «люк, лаз» — то самое слово"),
    45083: ("der", "Volksvertreter",     "разбор про множественное, у него написание то же"),
}

# Сюда род НЕ правим — решает владелец. Причина у каждого своя и она не про род.
TO_OWNER = {
    32587: ("Abflughall",
            "обрезанное написание слова «die Abflughalle»: чинить надо заголовок, "
            "а не род. Арбитр отвечает про несуществующий «Hall» и говорит der."),
    44759: ("Dicke",
            "род зависит от значения: «der Dicke» толстяк / «die Dicke» толщина. "
            "В разборе — толстяк, значит нынешний der верен, а арбитр отвечает про толщину."),
}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="записать правки в базу")
    args = parser.parse_args()

    fixed = skipped = 0
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            print("\nПРАВИМ (род подтверждён арбитром и сверен по переводу):\n")
            for unit_id, (gender, word, why) in CONFIRMED.items():
                cursor.execute(
                    "SELECT display, gender, gender_source FROM bt_3_lex_units WHERE id = %s;",
                    (unit_id,),
                )
                row = cursor.fetchone()
                if not row:
                    print(f"  ⚠️ {unit_id}: слова больше нет — пропускаю")
                    skipped += 1
                    continue
                display, was, gsrc = row
                if str(display or "").strip() != word:
                    # Написание поменялось после замера — значит правка уже не про то
                    # слово, которое я смотрел. Молча применять нельзя.
                    print(f"  ⚠️ {unit_id}: написание стало {display!r}, ждали {word!r} — пропускаю")
                    skipped += 1
                    continue
                if was == gender:
                    print(f"  ✓ {unit_id} «{gender} {word}» — уже верно")
                    continue
                print(f"  {unit_id} «{was} {word}» → «{gender} {word}»   ({why}; было откуда: {gsrc})")
                if args.apply:
                    cursor.execute(
                        "UPDATE bt_3_lex_units SET gender = %s, gender_source = %s, "
                        "updated_at = NOW() WHERE id = %s;",
                        (gender, "арбитр рода", unit_id),
                    )
                    fixed += 1
        if args.apply:
            conn.commit()

    print("\nВЛАДЕЛЬЦУ (род не трогаем):\n")
    for unit_id, (word, why) in TO_OWNER.items():
        print(f"  {unit_id} «{word}» — {why}")

    print()
    print(f"починено: {fixed}, пропущено: {skipped}" if args.apply
          else "сухой прогон, в базу ничего не писалось")
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
