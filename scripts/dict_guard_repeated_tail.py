# -*- coding: utf-8 -*-
"""Замок В САМОЙ БАЗЕ: немецкий текст с размноженным хвостом записать нельзя.

╔══════════════════════════════════════════════════════════════════════════════════╗
║  ЗАКРЫТО 21.08.2026. Правило и виды порчи берутся из backend/mangled_text.py.     ║
║  Свой признак здесь заводить НЕЛЬЗЯ — именно так тема открывалась четыре раза.    ║
╚══════════════════════════════════════════════════════════════════════════════════╝

ПОЧЕМУ ЗАМОК В БАЗЕ, А НЕ ПРОВЕРКА В КОДЕ
─────────────────────────────────────────
Порча 16.08.2026 пришла НЕ из продукта: продуктовый путь так не умеет. Она пришла из
разового скрипта уборки, который ходит в базу напрямую и обходит любую питоновскую
проверку. Значит и замок должен стоять там, где мимо него не пройти, — на самой колонке.
Плохая запись теперь не ложится тихо, а падает с ошибкой.

ЧТО ЗАПЕРТО И ПОЧЕМУ ИМЕННО ЭТО
───────────────────────────────
Заперты только колонки, где текст ТОЧНО немецкий:

    bt_3_lex_units                  display, lemma
    bt_3_webapp_dictionary_queries  word_de, translation_de
    bt_3_dictionary_entries         word_de, translation_de
                                    source_text — только когда source_lang = 'de'
                                    target_text — только когда target_lang = 'de'

РУССКИЕ КОЛОНКИ НЕ ЗАПЕРТЫ, И ЭТО РЕШЕНИЕ, А НЕ НЕДОСМОТР. Человек имеет право написать
в своём переводе «нееееет» или «да да да да» — это не порча, это его текст. Замок на
русской колонке уронил бы ему сохранение, а мы не ломаем человеку работу ради своей
чистоты. В немецком же четыре одинаковые буквы подряд не встречаются вовсе (даже у
«Schifffahrt» их три), поэтому там замок безопасен.

Колонки пула `source_text`/`target_text` держат то одну сторону, то другую, поэтому
условие смотрит на пометку языка. Пометка есть у ВСЕХ 17 186 записей (проверено
21.08.2026), так что дыры «языка нет — значит можно» не возникает. Испорченная запись
`sterile Gazennnnnn` лежала именно в `source_text` при `source_lang = 'de'` — условный
замок её бы поймал.

ПЕРЕД УСТАНОВКОЙ СКРИПТ ПРОВЕРЯЕТ ЖИВЫЕ ДАННЫЕ. Есть хоть одна строка, которая не
прошла бы, — замок не ставится, строки показываются владельцу. Замер 21.08.2026:
0 нарушений на 15 351 + 25 522 + 17 186 строках.

ЧЕГО ЭТОТ ЗАМОК НЕ ЗАКРЫВАЕТ, И ЭТО НАДО ЗНАТЬ. Разбор карточки (`card`,
`response_json`) — это дерево значений, на него CHECK не ставится. Туда порча 16.08
тоже дошла: 15 разборов на словах и 143 карточки людей. Прикрыть тот путь может только
проверка на дне записи разбора (`lex_units.save_unit_card`) — это отдельная работа, и
она НЕ сделана. Не считать эту тему закрытой целиком.

СТАВИТСЯ БЕЗОПАСНО, И ЭТО НЕ ФОРМАЛЬНОСТЬ. Прямой `ADD CONSTRAINT` 21.08.2026 положил
словарь на 45 минут — не потому, что долго работал (сверка занимает секунды), а потому,
что монопольный замок встал в очередь за читающим запросом и заблокировал за собой всех.
Поэтому здесь `lock_timeout` + `NOT VALID` + отдельный `VALIDATE`; подробности в коде
у самой установки. Не сумели взять замок — скрипт честно падает, а не молчит.

    python3 scripts/dict_guard_repeated_tail.py            # проверить, ничего не менять
    python3 scripts/dict_guard_repeated_tail.py --apply    # поставить замки
"""
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SKIP_BILLING_LEDGER_WRITES", "1")

from backend.database import get_db_connection_context  # noqa: E402
from backend.mangled_text import SQL_MANGLED  # noqa: E402

# (таблица, имя замка, [(колонка, когда проверять или None), …])
LOCKS = (
    ("bt_3_lex_units", "bt_3_lex_units_no_repeated_tail",
     (("display", None), ("lemma", None))),
    ("bt_3_webapp_dictionary_queries", "bt_3_dict_queries_no_repeated_tail",
     (("word_de", None), ("translation_de", None))),
    ("bt_3_dictionary_entries", "bt_3_dict_entries_no_repeated_tail",
     (("word_de", None), ("translation_de", None),
      ("source_text", "source_lang = 'de'"), ("target_text", "target_lang = 'de'"))),
)


def _column_check(column: str, when: str | None) -> str:
    """Условие «эта колонка не испорчена» — по всем видам хвоста сразу."""
    clean = " AND ".join([f"{column} !~ %s"] * len(SQL_MANGLED))
    body = f"({column} IS NULL OR ({clean}))"
    return body if not when else f"(NOT ({when}) OR {body})"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    dirty = 0
    with get_db_connection_context() as conn:
        for table, _name, columns in LOCKS:
            for column, when in columns:
                with conn.cursor() as cursor:
                    where = " OR ".join([f"{column} ~ %s"] * len(SQL_MANGLED))
                    if when:
                        where = f"({when}) AND ({where})"
                    cursor.execute(
                        f"SELECT id, {column} FROM {table} WHERE {where} ORDER BY id LIMIT 20;",
                        tuple(SQL_MANGLED),
                    )
                    rows = cursor.fetchall()
                print(f"{'✅' if not rows else '❌'} {table}.{column}: {len(rows)}")
                for row_id, value in rows:
                    print(f"      {row_id} {value!r}")
                dirty += len(rows)

    if dirty:
        print("\nСначала уборка: python3 scripts/dict_undo_repeated_tail_damage.py --apply")
        print("Замок не ставится, пока есть строки, которые он завалит.\n")
        return 1
    if not args.apply:
        print("\nЖивые данные чистые — замки встанут. Поставить: --apply\n")
        return 0

    # ═══ СПОСОБ УСТАНОВКИ ВАЖЕН. ЭТО УЖЕ СТОИЛО НАМ 45 МИНУТ ПРОСТОЯ. ═══════════════
    #
    # 21.08.2026 замки ставились прямым `ADD CONSTRAINT` — и словарь встал на 45 минут.
    # Причина НЕ в длительности работы: сама сверка занимает секунды (замер 23.08.2026
    # по живой базе — 2,85 с + 2,34 с + 2,03 с на трёх таблицах). Причина в ОЧЕРЕДИ:
    # `ADD CONSTRAINT` берёт монопольный замок, встаёт за любым уже идущим читающим
    # запросом и блокирует за собой ВСЕХ, кто пришёл после. Одного долгого читателя
    # хватает, чтобы словарь встал целиком.
    #
    # Урок был записан в коммит 59d1c48d, но в сам скрипт НЕ ПОПАЛ: до 23.08.2026 здесь
    # стоял прямой ADD CONSTRAINT, то есть инструмент повторял бы поломку при каждом
    # запуске. Поэтому порядок теперь ЗДЕСЬ, в коде, а не в истории.
    #
    #   lock_timeout    не ждать очереди дольше трёх секунд — лучше честно упасть, чем
    #                   держать за собой словарь;
    #   NOT VALID       монопольный замок берётся на мгновение и без прохода по таблице;
    #   VALIDATE        отдельной транзакцией и СЛАБЫМ замком: читателям и пишущим не
    #                   мешает, хотя проход по таблице делает именно он.
    #
    # Не сумели взять замок — НЕ ПРОДОЛЖАЕМ и не делаем вид, что поставили. Возвращаем
    # ошибку и говорим, что повторить.
    import psycopg2                                                    # noqa: PLC0415

    поставлено, не_смогли = [], []
    with get_db_connection_context() as conn:
        for table, name, columns in LOCKS:
            checks, params = [], []
            for column, when in columns:
                checks.append(_column_check(column, when))
                params.extend(SQL_MANGLED)
            try:
                with conn.cursor() as cursor:
                    cursor.execute("SET LOCAL lock_timeout = '3s';")
                    # Замок мог стоять со старым, более узким правилом — снимаем и ставим
                    # заново, иначе новый вид хвоста останется незакрытым.
                    cursor.execute(f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {name};")
                    cursor.execute(
                        f"ALTER TABLE {table} ADD CONSTRAINT {name} "
                        f"CHECK ({' AND '.join(checks)}) NOT VALID;",
                        tuple(params),
                    )
                conn.commit()
            except psycopg2.errors.LockNotAvailable:
                conn.rollback()
                не_смогли.append((table, "не дали монопольный замок за 3 секунды"))
                continue
            except Exception as exc:
                conn.rollback()
                не_смогли.append((table, str(exc).strip().splitlines()[0]))
                continue
            # Сверка — ОТДЕЛЬНОЙ транзакцией. Иначе монопольный замок от ADD держался бы
            # всё время прохода по таблице, и мы вернулись бы ровно к тому, от чего ушли.
            try:
                with conn.cursor() as cursor:
                    cursor.execute("SET LOCAL lock_timeout = '3s';")
                    cursor.execute(f"ALTER TABLE {table} VALIDATE CONSTRAINT {name};")
                conn.commit()
            except Exception as exc:
                conn.rollback()
                # Замок УЖЕ стоит и уже не пускает новую порчу — не сверено только то,
                # что накоплено раньше. Это разные вещи, и путать их нельзя.
                не_смогли.append(
                    (table, f"замок поставлен, но сверка накопленного не прошла: "
                            f"{str(exc).strip().splitlines()[0]}"))
                continue
            поставлено.append(table)
            print(f"🔒 {table}: {name}")

    if не_смогли:
        print("\n⚠ НЕ ЗАКРЫТО ПОЛНОСТЬЮ:\n")
        for table, почему in не_смогли:
            print(f"   {table}: {почему}")
        print("\nЭто НЕ отказ навсегда: значит, в этот момент по таблице шёл долгий")
        print("запрос. Повторите позже — скрипт можно запускать сколько угодно раз.\n")
        return 1

    print("\nЗамки поставлены. Немецкий текст с размноженным хвостом больше не запишется.\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
