# -*- coding: utf-8 -*-
"""Обрезанные заголовки карточек: «Die Einbürgeru» → «die Einbürgerung».

Откуда
──────
Наследство лемматизатора spaCy: маленькая модель de_core_news_sm откусывала окончание
(Felge→Felg, beibringen→beibring). Из пути сохранения её вывели, но обрезки остались
заголовками личных карточек. Замер 17.08.2026: 20 карточек, 11 разных текстов.

Признак обрезка — жёсткий: заголовок карточки является НАЧАЛОМ заголовка её же слова в
справочнике, короче него не более чем на четыре буквы, и сам словом не является
(справочник родов его не знает). «die Eiche» при слове «eichen» под правило не попадает:
это настоящее слово, и там другая беда — карточка привязана к чужому слову.

Таблица явная, каждая строка просмотрена глазами: два случая из одиннадцати правилом
чинить нельзя, и они здесь названы отдельно.

    python3 scripts/dict_fix_truncated_card_headwords.py            # сухой прогон
    python3 scripts/dict_fix_truncated_card_headwords.py --apply    # записать
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

# что лежит в карточке → как правильно
FIXES = {
    "Die Einbürgeru": "die Einbürgerung",
    "Jahrela": "jahrelang",                      # наречие: со строчной
    "Betätige": "betätigen",
    "Verknüpfe": "verknüpfen",
    "Entwerte": "entwerten",
    "Die Aufenthaltsgenehmigu": "die Aufenthaltsgenehmigung",
    "Das Erbstü": "das Erbstück",
    "Erachte": "erachten",
    "Erlange": "erlangen",
    "Die Rivalitä": "die Rivalität",
}

# Правилом не чинятся — нужна не форма, а решение о том, ЧТО это за слово.
NEEDS_OWNER = {
    "Künftig": "слово в справочнике — «die Künftige» (существительное), а в карточке "
               "похоже на прилагательное «künftig». Это разные статьи, а не обрезок.",
    "die Eiche": "карточка привязана к слову «eichen» (калибровать). «die Eiche» — дуб, "
                 "другое слово: чинить надо привязку, а не заголовок.",
    "die Glotze": "то же самое: привязана к глаголу «glotzen».",
}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    total = {"cards": 0, "places": 0, "pool": 0, "tasks_dropped": 0}
    touched = 0
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            for old, new in sorted(FIXES.items()):
                cur.execute(
                    """
                    SELECT q.id, q.lex_unit_id FROM bt_3_webapp_dictionary_queries q
                     WHERE lower(BTRIM(q.word_de)) = lower(%s)
                        OR lower(BTRIM(q.translation_de)) = lower(%s);
                    """,
                    (old, old),
                )
                rows = cur.fetchall()
                print("   %-26s → %-26s карточек: %d" % (old, new, len(rows)))
                if not args.apply:
                    continue
                for _entry_id, unit_id in rows:
                    if not unit_id:
                        continue
                    report = spread_correction_everywhere(
                        cur, unit_id=int(unit_id), old_text=old, new_text=new)
                    for name in total:
                        total[name] += report.get(name, 0)
                    touched += 1
        if args.apply:
            conn.commit()

    print()
    print("НУЖНО РЕШЕНИЕ ВЛАДЕЛЬЦА — правилом не чинится:")
    for word, why in sorted(NEEDS_OWNER.items()):
        print("   %-14s %s" % (word, why))

    if args.apply:
        print()
        print("   карточек тронуто:     %d" % total["cards"])
        print("   мест внутри разборов: %d" % total["places"])
        print("   записей пула:         %d" % total["pool"])
        print("   заданий снесено:      %d" % total["tasks_dropped"])
    else:
        print()
        print("СУХОЙ ПРОГОН. Ничего не записано. Для записи добавь --apply.")


if __name__ == "__main__":
    main()
