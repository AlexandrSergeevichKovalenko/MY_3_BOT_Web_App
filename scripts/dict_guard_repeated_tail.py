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
Хвост, повторённый ЧЕТЫРЕ и более раз подряд: один и тот же знак препинания в конце
(«......», «??????») или одно и то же слово через пробел («ist ist ist ist»).

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

# POSIX-регулярка Postgres. Два запрещённых вида хвоста:
#   ([.?!])\1{3,}$        — один и тот же знак четыре и более раз в конце
#   (\S+)( \1){3,}$       — одно и то же слово четыре и более раз в конце
BAD = r"(([.?!])\2{3,}[[:space:]]*$)|((^|[[:space:]])([^[:space:]]+)([[:space:]]+\5){3,}[[:space:]]*$)"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT id, display, lemma FROM bt_3_lex_units "
                "WHERE display ~ %s OR lemma ~ %s ORDER BY id;",
                (BAD, BAD),
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
            cursor.execute(
                "SELECT 1 FROM pg_constraint WHERE conname = %s;", (CONSTRAINT,))
            if cursor.fetchone():
                print(f"\nСтраж {CONSTRAINT} уже стоит.\n")
                return 0
            cursor.execute(
                f'ALTER TABLE bt_3_lex_units ADD CONSTRAINT {CONSTRAINT} '
                f"CHECK (display !~ %s AND (lemma IS NULL OR lemma !~ %s));",
                (BAD, BAD),
            )
        conn.commit()
    print(f"\nСТРАЖ ПОСТАВЛЕН: {CONSTRAINT}. Запись с размноженным хвостом теперь падает.\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
