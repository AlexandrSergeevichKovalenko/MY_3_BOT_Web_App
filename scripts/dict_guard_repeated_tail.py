# -*- coding: utf-8 -*-
"""Страж в САМОЙ БАЗЕ: заголовок с размноженным хвостом записать нельзя.

ЗАЧЕМ ИМЕННО В БАЗЕ, А НЕ В КОДЕ
────────────────────────────────
16.08.2026 одна транзакция размножила хвост у заголовков 15 фраз («Er erlag der
Versuchung......», «vorbei. vorbei. vorbei. …»). Механизм доказан: подстановка
`s.replace(старое, новое)`, применённая шесть раз к накапливающемуся результату.
Виновника в репозитории нет — его прогнали разовым скриптом и не закоммитили.

И в этом всё дело. Дефект пришёл НЕ из продукта: продуктовый путь такого не умеет.
Он пришёл из скрипта уборки, который ходит в базу напрямую и обходит любую питоновскую
проверку. Значит и страж должен стоять там, где мимо него не пройти, — на самой колонке.
Плохая запись теперь не «тихо ложится и ждёт, пока кто-то заметит», а падает с ошибкой.

ЧТО ИМЕННО ЗАПРЕЩЕНО
────────────────────
Хвост, повторённый ЧЕТЫРЕ и более раз подряд, в трёх видах: один и тот же знак препинания
в конце («......», «??????»), одно и то же слово через пробел («ist ist ist ist») и одна
и та же БУКВА («sterile Gazennnnnn»).

Третий вид добавлен 21.08.2026: первая версия стража его не знала и запись пропустила —
там шаг порчи был короче слова, «sterile Gaze» плюс «n», применённое шесть раз. Нашёл её
соседний агент, сверив базу по своему признаку. В немецком четыре одинаковые буквы подряд
не встречаются: даже у «Schifffahrt» их три.

Порог четыре, а не три, — потому что многоточие из трёх точек это законный заголовок:
«Es kommt darauf an...», «Wenn man bedenkt, dass...». Их в словаре два десятка, и они
не порча, а шаблон фразы с продолжением. Порог стоит между ними и нашей шестикратной
порчей. Перед установкой скрипт ПРОВЕРЯЕТ всю таблицу: есть хоть одна живая строка,
которая не прошла бы, — ограничение не ставится, а строки показываются владельцу.

    python3 scripts/dict_guard_repeated_tail.py            # проверить, ничего не менять
    python3 scripts/dict_guard_repeated_tail.py --apply    # поставить ограничение
"""
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SKIP_BILLING_LEDGER_WRITES", "1")

from backend.database import get_db_connection_context  # noqa: E402

CONSTRAINT = "bt_3_lex_units_no_repeated_tail"

# POSIX-регулярка Postgres. Три запрещённых вида хвоста:
#   ([.?!])\1{3,}$        — один и тот же знак четыре и более раз в конце
#   (\S+)( \1){3,}$       — одно и то же слово четыре и более раз в конце
#   (\w)\1{3,}$           — одна и та же БУКВА четыре и более раз в конце
#
# Третий вид добавлен 21.08.2026. Первая версия стража его не знала и пропустила
# «sterile Gazennnnnn» — ту же порчу, только шаг был короче слова: «sterile Gaze» плюс
# «n», применённое шесть раз. Запись нашёл соседний агент, сверив базу по своему
# признаку, — моя проверка искала повтор слова и знака и мимо неё прошла.
# В немецком четыре одинаковые буквы подряд не встречаются: даже у «Schifffahrt» их три.
# Три ОТДЕЛЬНЫХ правила, а не одно склеенное.
#
# Склеенное я уже написал и получил от Postgres «invalid backreference number»: в общей
# регулярке номера скобок сдвигаются, и обратная ссылка начинает указывать не туда.
# Три независимых правила считают свои скобки сами, и добавить четвёртое можно, ничего
# не пересчитывая.
BAD_PUNCT  = r"([.?!])\1{3,}[[:space:]]*$"                                  # «......», «??????»
BAD_WORD   = r"(^|[[:space:]])([^[:space:]]+)([[:space:]]+\2){3,}[[:space:]]*$"  # «ist ist ist ist»
BAD_LETTER = r"([[:alnum:]])\1{3,}[[:space:]]*$"                            # «Gazennnnnn»
BAD_ALL = (BAD_PUNCT, BAD_WORD, BAD_LETTER)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            where = " OR ".join("display ~ %s OR lemma ~ %s" for _ in BAD_ALL)
            cursor.execute(
                f"SELECT id, display, lemma FROM bt_3_lex_units WHERE {where} ORDER BY id;",
                tuple(x for rule in BAD_ALL for x in (rule, rule)),
            )
            offenders = cursor.fetchall()

    print(f"\nЖИВЫХ СТРОК, КОТОРЫЕ НЕ ПРОШЛИ БЫ СТРАЖА: {len(offenders)}")
    for unit_id, display, lemma in offenders[:30]:
        print(f"   {unit_id:>6} display={display!r}")
        if lemma != display:
            print(f"          lemma  ={lemma!r}")
    if offenders:
        print("\nСначала уборка: python3 scripts/dict_undo_repeated_tail_damage.py --apply")
        print("Страж не ставится, пока в таблице есть строки, которые он завалит.\n")
        return 1

    if not args.apply:
        print("\nТаблица чистая — страж встанет. Поставить: --apply\n")
        return 0

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            # Ограничение уже могло стоять со старым, более узким правилом —
            # снимаем и ставим заново, иначе новый вид хвоста останется незакрытым.
            cursor.execute(
                f"ALTER TABLE bt_3_lex_units DROP CONSTRAINT IF EXISTS {CONSTRAINT};")
            checks = " AND ".join(
                "display !~ %s AND (lemma IS NULL OR lemma !~ %s)" for _ in BAD_ALL)
            cursor.execute(
                f"ALTER TABLE bt_3_lex_units ADD CONSTRAINT {CONSTRAINT} CHECK ({checks});",
                tuple(x for rule in BAD_ALL for x in (rule, rule)),
            )
        conn.commit()
    print(f"\nСТРАЖ ПОСТАВЛЕН: {CONSTRAINT}. Запись с размноженным хвостом теперь падает.\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
