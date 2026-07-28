# -*- coding: utf-8 -*-
"""Часть речи для русских слов — выводится из немецкой стороны, без сети и GPT.

Русский перевод немецкого глагола — глагол; перевод существительного —
существительное. Это бесплатное знание, которое у нас уже есть в связях.

Гадать не будем: часть речи ставится ТОЛЬКО если все связанные немецкие слова
согласны между собой. «ключ» может быть и существительным (der Schlüssel), и частью
глагольной пары — при разногласии оставляем пусто.

Запуск:
    DATABASE_URL=... python3 scripts/dict_units_ru_pos.py --dry-run
    DATABASE_URL=... python3 scripts/dict_units_ru_pos.py --apply
"""
from __future__ import annotations

import argparse
import os
import time

import psycopg2

SQL_CANDIDATES = """
SELECT ru.id, ru.display, MIN(de.pos) AS pos, COUNT(DISTINCT de.pos) AS variants
FROM bt_3_lex_links l
JOIN bt_3_lex_units de ON de.id = l.from_unit AND de.lang = 'de' AND de.pos IS NOT NULL
JOIN bt_3_lex_units ru ON ru.id = l.to_unit AND ru.lang <> 'de'
                      AND ru.kind = 'word' AND ru.pos IS NULL
WHERE l.rank < 900
GROUP BY ru.id, ru.display
HAVING COUNT(DISTINCT de.pos) = 1;
"""


def connect(dsn: str):
    last = None
    for attempt in range(6):
        try:
            return psycopg2.connect(dsn, connect_timeout=20)
        except Exception as exc:
            last = exc
            print("  переподключение %d/6: %s" % (attempt + 1, exc))
            time.sleep(5)
    raise SystemExit("база недоступна: %s" % last)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()
    if not args.dry_run and not args.apply:
        raise SystemExit("укажи --dry-run или --apply")
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise SystemExit("нужен DATABASE_URL")

    conn = connect(dsn)
    conn.autocommit = False
    cur = conn.cursor()
    cur.execute(SQL_CANDIDATES)
    rows = cur.fetchall()
    print("русских слов, где немецкая сторона согласна: %d" % len(rows))
    by_pos: dict[str, int] = {}
    for _id, _display, pos, _v in rows:
        by_pos[pos] = by_pos.get(pos, 0) + 1
    for pos, count in sorted(by_pos.items(), key=lambda x: -x[1]):
        print("   %-14s %d" % (pos, count))
    print()
    for _id, display, pos, _v in rows[:8]:
        print("   %-26s → %s" % (display[:26], pos))

    if not args.apply:
        conn.rollback()
        print("\n(--dry-run: в базу ничего не записано)")
        return 0
    cur.executemany(
        "UPDATE bt_3_lex_units SET pos = %s, pos_source = 'по немецкой стороне', "
        "updated_at = NOW() WHERE id = %s AND pos IS NULL;",
        [(pos, rid) for rid, _d, pos, _v in rows],
    )
    conn.commit()
    print("\nзаписано: часть речи проставлена у %d слов." % len(rows))
    cur.close()
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
