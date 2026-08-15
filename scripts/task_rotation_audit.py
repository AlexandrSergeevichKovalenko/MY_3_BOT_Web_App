# -*- coding: utf-8 -*-
"""Замер личной ротации: сколько РАЗНЫХ заданий получили разные люди.

Зачем этот файл существует
──────────────────────────
До 15.08.2026 задание выбиралось ОДИН раз до цикла рассылки и уходило всем сразу,
поэтому «разных заданий на человека» равнялось ровно 1.0 — по устройству, а не по
случайности. Этот скрипт меряет то же самое число после правки, одним и тем же
правилом, чтобы «стало лучше» было числом, а не ощущением.

Скрипт ТОЛЬКО ЧИТАЕТ. Он ничего не чинит и ничего не удаляет.

    DATABASE_URL="$(railway variables --service Postgres --kv \
        | grep '^DATABASE_PUBLIC_URL=' | cut -d= -f2-)" \
      python3 scripts/task_rotation_audit.py
    python3 scripts/task_rotation_audit.py --days 30   # окно замера, по умолчанию 7

Как читать
──────────
    разных заданий на человека ≈ 1.0  → ротация НЕ работает, все получают одно и то же
    заметно больше 1.0                → у людей разные задания, ротация живая
"""

import argparse
import os
import sys

import psycopg2


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=7, help="окно замера в днях")
    args = ap.parse_args()

    dsn = (os.getenv("DATABASE_URL") or os.getenv("DATABASE_PUBLIC_URL") or "").strip()
    if not dsn:
        print("Нужен DATABASE_URL (публичный адрес живой базы).", file=sys.stderr)
        return 2
    if "sslmode" not in dsn:
        dsn += ("&" if "?" in dsn else "?") + "sslmode=require"

    conn = psycopg2.connect(dsn)
    conn.set_session(readonly=True, autocommit=True)
    with conn.cursor() as cur:
        # Таблица заводится при первом обращении к памяти ротации, поэтому до раскатки
        # её может не быть — это не ошибка замера, а «ещё не начиналось».
        cur.execute("SELECT to_regclass('public.bt_3_user_task_state');")
        if not (cur.fetchone() or [None])[0]:
            print("Память ротации ещё не заведена в этой базе: таблицы")
            print("bt_3_user_task_state нет. Значит правка не доехала до прода либо")
            print("после деплоя ещё никто не отвечал. Это не поломка.")
            return 0
        cur.execute(
            """SELECT kind,
                      COUNT(DISTINCT task_key)      AS tasks,
                      COUNT(DISTINCT user_id)       AS people,
                      COUNT(*)                      AS answers,
                      SUM(CASE WHEN retired_at IS NOT NULL THEN 1 ELSE 0 END) AS retired
               FROM bt_3_user_task_state
               WHERE last_seen_at > NOW() - (%s || ' days')::interval
               GROUP BY kind
               ORDER BY answers DESC;""",
            (int(args.days),),
        )
        rows = cur.fetchall()

    print(f"Замер личной ротации за {args.days} дн.\n")
    if not rows:
        print("В памяти ротации пусто: либо ещё не раскатано, либо за это окно никто")
        print("не отвечал. Это не поломка — до правки таблица не наполнялась вовсе.")
        return 0

    print("Вид задания                 разных заданий   людей   ответов   выброшено   "
          "разных заданий на человека")
    for kind, tasks, people, answers, retired in rows:
        per = (tasks / people) if people else 0.0
        print(f"{str(kind):<28}{int(tasks):>10}{int(people):>9}{int(answers):>10}"
              f"{int(retired or 0):>12}          {per:>6.1f}")

    print("\nЕсли последняя колонка около 1.0 — все получают одно и то же, ротация не")
    print("работает. Замер ДО правки (14.08.2026): ровно 1.0 по устройству рассылки.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
