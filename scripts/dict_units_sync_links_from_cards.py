# -*- coding: utf-8 -*-
"""Разовый проход: перечитать переводы всех разобранных слов из их разбора.

Ночной добор теперь делает это сам сразу после разбора, но 1337 слов разобрались
раньше — по ним связи остались старыми, из текста банка. А разбор знает больше:
у «die Scheide» в связях было только «влагалище», хотя есть и «ножны»; у «betreffen»
стояло «касаться, относиться» одной строкой вместо двух значений.

Старые связи не удаляются — отодвигаются за значения разбора: они могли прийти из
живого сохранения человека.

Запуск:
    DATABASE_URL=... python3 scripts/dict_units_sync_links_from_cards.py --dry-run
    DATABASE_URL=... python3 scripts/dict_units_sync_links_from_cards.py --apply
"""
from __future__ import annotations

import argparse
import os
import sys
import time

import psycopg2

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from backend import lex_units  # noqa: E402


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
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()
    if not args.dry_run and not args.apply:
        raise SystemExit("укажи --dry-run или --apply")
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise SystemExit("нужен DATABASE_URL")

    conn = connect(dsn)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, display, card FROM bt_3_lex_units
        WHERE lang = 'de' AND kind = 'word' AND card IS NOT NULL
        ORDER BY id;
        """
    )
    rows = cur.fetchall()
    if args.limit:
        rows = rows[: args.limit]
    print("слов с разбором: %d" % len(rows))

    total_senses = touched = 0
    shown = 0
    for unit_id, display, card in rows:
        meanings = (card or {}).get("meanings") or {}
        values = []
        primary = meanings.get("primary")
        if isinstance(primary, dict) and str(primary.get("value") or "").strip():
            values.append(str(primary["value"]).strip())
        for item in (meanings.get("secondary") or []):
            if isinstance(item, dict) and str(item.get("value") or "").strip():
                values.append(str(item["value"]).strip())
        if len(values) < 1:
            continue
        if shown < 8:
            shown += 1
            print("   %-24s → %s" % (display[:24], " | ".join(values[:4])))
        if not args.apply:
            total_senses += len(values)
            touched += 1
            continue
        result = lex_units.sync_unit_links_from_card(unit_id, card, native_lang="ru")
        if result.get("senses"):
            touched += 1
            total_senses += int(result["senses"])

    print("\nслов затронуто: %d, значений разложено: %d" % (touched, total_senses))
    if not args.apply:
        print("(--dry-run: в базу ничего не записано)")
    cur.close()
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
