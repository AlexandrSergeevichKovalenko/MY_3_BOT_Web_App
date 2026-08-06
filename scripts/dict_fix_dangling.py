"""Убрать висячие указатели и мёртвые связи слоя слов.

Три хвоста, замер 06.08.2026:

1. **170 карточек указывают на несуществующую единицу.** Единицу когда-то убрали или
   слили, а указатель в карточке остался. Такая карточка не получает разбор с единицы —
   тот самый случай, когда слово разобрано и оплачено, а человек видит пустую карточку.
   Чиним: ищем правильную единицу по немецкой стороне карточки; не нашли — обнуляем
   указатель, и ближайшее сохранение или ночной проход поставят его заново.

2. **3 карточки указывают на несуществующую запись общего словаря.** То же самое,
   только указатель на общий словарь. Обнуляем.

3. **480 связей соединяют два слова ОДНОГО языка** (442 немецких, 38 русских):
   «die Pinnwand» → «die Pinnwand / das schwarze Brett», «снять трубку» → «взять
   телефонную трубку для ответа на звонок». Это синонимы и пояснения, попавшие в
   таблицу переводов при разборе. Вреда они не приносят — выдача переводов отбирает
   связи по языку и такие не показывает, — но это мёртвый груз, который путает замеры.
   Источник уже закрыт: заслон в `sync_unit_links_from_card` не даёт завести единицу
   чужого алфавита, свежих таких связей нет.

   Правилом базы это НЕ закрыть: `CHECK` не умеет заглядывать в другую таблицу, а
   вешать на каждую запись связи триггер ради 480 строк — плата больше пользы.

По умолчанию НИЧЕГО НЕ ПИШЕТ. Запись — только с --apply.

    python scripts/dict_fix_dangling.py           # вхолостую
    python scripts/dict_fix_dangling.py --apply   # записать
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
from lex_units import normalize_query  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT q.id, q.word_de FROM bt_3_webapp_dictionary_queries q
                   WHERE q.lex_unit_id IS NOT NULL
                     AND NOT EXISTS (SELECT 1 FROM bt_3_lex_units u WHERE u.id = q.lex_unit_id);"""
            )
            dangling_units = cur.fetchall()
            repoint, clear = [], []
            for entry_id, word_de in dangling_units:
                key = normalize_query(word_de or "")
                unit_id = None
                if key:
                    cur.execute(
                        """SELECT u.id FROM bt_3_lex_surfaces s
                           JOIN bt_3_lex_units u ON u.id = s.unit_id
                           WHERE s.lang = 'de' AND s.surface_key = %s
                           ORDER BY (u.card IS NULL), u.id LIMIT 1;""",
                        (key,),
                    )
                    row = cur.fetchone()
                    unit_id = int(row[0]) if row else None
                (repoint if unit_id else clear).append((entry_id, unit_id, word_de))

            cur.execute(
                """SELECT count(*) FROM bt_3_webapp_dictionary_queries q
                   WHERE q.canonical_entry_id IS NOT NULL
                     AND NOT EXISTS (SELECT 1 FROM bt_3_dictionary_entries e
                                     WHERE e.id = q.canonical_entry_id);"""
            )
            dangling_pool = cur.fetchone()[0]

            cur.execute(
                """SELECT count(*) FROM bt_3_lex_links l
                   JOIN bt_3_lex_units a ON a.id = l.from_unit
                   JOIN bt_3_lex_units b ON b.id = l.to_unit
                   WHERE a.lang = b.lang;"""
            )
            same_lang_links = cur.fetchone()[0]

            print("КАРТОЧКИ С ВИСЯЧИМ УКАЗАТЕЛЕМ НА ЕДИНИЦУ: %d" % len(dangling_units))
            print("   нашли правильную единицу: %d" % len(repoint))
            for entry_id, unit_id, word_de in repoint[:8]:
                print("      карточка %s → единица %s   %r" % (entry_id, unit_id, (word_de or "")[:45]))
            print("   единицы нет, обнуляем указатель: %d" % len(clear))
            for entry_id, _u, word_de in clear[:8]:
                print("      карточка %s   %r" % (entry_id, (word_de or "")[:45]))
            print("КАРТОЧКИ С ВИСЯЧИМ УКАЗАТЕЛЕМ НА СЛОВАРЬ: %d" % dangling_pool)
            print("СВЯЗИ ВНУТРИ ОДНОГО ЯЗЫКА (убираем): %d" % same_lang_links)

            if not args.apply:
                print()
                print("ВХОЛОСТУЮ. Записать: --apply")
                return 0

            for entry_id, unit_id, _w in repoint:
                cur.execute(
                    "UPDATE bt_3_webapp_dictionary_queries SET lex_unit_id = %s WHERE id = %s;",
                    (unit_id, entry_id),
                )
            for entry_id, _u, _w in clear:
                cur.execute(
                    "UPDATE bt_3_webapp_dictionary_queries SET lex_unit_id = NULL WHERE id = %s;",
                    (entry_id,),
                )
            cur.execute(
                """UPDATE bt_3_webapp_dictionary_queries q SET canonical_entry_id = NULL
                   WHERE q.canonical_entry_id IS NOT NULL
                     AND NOT EXISTS (SELECT 1 FROM bt_3_dictionary_entries e
                                     WHERE e.id = q.canonical_entry_id);"""
            )
            cur.execute(
                """DELETE FROM bt_3_lex_links l
                   USING bt_3_lex_units a, bt_3_lex_units b
                   WHERE a.id = l.from_unit AND b.id = l.to_unit AND a.lang = b.lang;"""
            )
            removed_links = cur.rowcount or 0
            conn.commit()
    print()
    print("ЗАПИСАНО: указателей на единицу переставлено %d, обнулено %d, "
          "указателей на словарь обнулено %d, мёртвых связей убрано %d"
          % (len(repoint), len(clear), dangling_pool, removed_links))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
