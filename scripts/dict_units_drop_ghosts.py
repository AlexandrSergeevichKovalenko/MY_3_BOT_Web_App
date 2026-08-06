"""Убрать единицы-призраки — те, которые не может получить никто и никогда.

Призрак — это строка в слое слов, на которую не ссылается ничто: нет разбора, нет ни
одной связи (значит, нет перевода — а словарь не отдаёт слово без перевода), нет ни
одной карточки человека, нет разобранных значений и нет записи в таблице источников.
Такая строка не участвует ни в выдаче, ни в поиске, ни в очереди ночного обогатителя.
Это след массовой сборки 27 июля.

ОСТОРОЖНО, и это главное. Первый замер насчитал 765 «призраков» по двум признакам —
нет разбора и нет связей. Проверка остальных ссылок показала, что **572 из них держат
живую карточку человека**: удалить их значило бы наплодить висячие указатели, которые
мы только что чинили. Ещё 129 держит таблица источников — по ней ночной обогатитель
ранжирует очередь и по ней же личные карточки привязываются к словам.

Поэтому удаляем только то, на что не ссылается НИЧТО, и вдобавок оставляем в покое
одиночные слова (`kind='word'`): очередь ночного обогатителя — это ровно «слово без
разбора», и такая строка не мусор, а невыполненная работа.

По умолчанию НИЧЕГО НЕ ПИШЕТ. Запись — только с --apply.

    python scripts/dict_units_drop_ghosts.py           # вхолостую
    python scripts/dict_units_drop_ghosts.py --apply   # записать
"""

from __future__ import annotations

import argparse
import os
import sys

_here = os.path.dirname(os.path.abspath(globals().get("__file__", ".")))
sys.path.insert(0, os.path.join(_here, ".."))
sys.path.insert(0, os.path.join(_here, "..", "backend"))
sys.path.insert(0, "/app/backend")

from database import get_db_connection_context  # noqa: E402

UNREFERENCED = """
    FROM bt_3_lex_units u
    WHERE u.card IS NULL
      AND NOT EXISTS (SELECT 1 FROM bt_3_lex_links l WHERE l.from_unit = u.id OR l.to_unit = u.id)
      AND NOT EXISTS (SELECT 1 FROM bt_3_webapp_dictionary_queries q WHERE q.lex_unit_id = u.id)
      AND NOT EXISTS (SELECT 1 FROM bt_3_lex_senses s WHERE s.unit_id = u.id)
      AND NOT EXISTS (SELECT 1 FROM bt_3_lex_unit_sources o WHERE o.unit_id = u.id)
"""
KEEP_WORDS = " AND u.kind <> 'word' "


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute("""SELECT count(*) FROM bt_3_lex_units u WHERE u.card IS NULL
                           AND NOT EXISTS (SELECT 1 FROM bt_3_lex_links l
                                           WHERE l.from_unit = u.id OR l.to_unit = u.id);""")
            naive = cur.fetchone()[0]
            cur.execute("""SELECT count(*) FROM bt_3_lex_units u WHERE u.card IS NULL
                           AND NOT EXISTS (SELECT 1 FROM bt_3_lex_links l
                                           WHERE l.from_unit = u.id OR l.to_unit = u.id)
                           AND EXISTS (SELECT 1 FROM bt_3_webapp_dictionary_queries q
                                       WHERE q.lex_unit_id = u.id);""")
            hold_cards = cur.fetchone()[0]
            cur.execute("SELECT count(*) " + UNREFERENCED)
            unreferenced = cur.fetchone()[0]
            cur.execute("SELECT count(*) " + UNREFERENCED + " AND u.kind = 'word'")
            pending_words = cur.fetchone()[0]

            cur.execute("SELECT u.id, u.lang, u.kind, u.display " + UNREFERENCED + KEEP_WORDS + " ORDER BY u.id")
            doomed = cur.fetchall()

            print("«без разбора и без связей» (первый, наивный замер): %d" % naive)
            print("   из них ДЕРЖАТ карточку человека — не трогаем:     %d" % hold_cards)
            print("   из них держит таблица источников — не трогаем:    %d" % (naive - hold_cards - unreferenced))
            print("на них не ссылается ничто:                           %d" % unreferenced)
            print("   из них одиночные слова — это очередь обогатителя: %d (оставляем)" % pending_words)
            print("УБИРАЕМ: %d" % len(doomed))
            for uid, lang, kind, display in doomed[:15]:
                print("   %s %s %s  %r" % (uid, lang, kind, (display or "")[:60]))
            if len(doomed) > 15:
                print("   ... ещё %d" % (len(doomed) - 15))

            if not args.apply:
                print()
                print("ВХОЛОСТУЮ. Записать: --apply")
                return 0

            cur.execute(
                "DELETE FROM bt_3_lex_units u WHERE u.id = ANY(%s);",
                ([int(r[0]) for r in doomed],),
            )
            removed = cur.rowcount or 0
            conn.commit()
    print()
    print("УБРАНО ЕДИНИЦ: %d" % removed)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
