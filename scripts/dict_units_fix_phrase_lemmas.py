# -*- coding: utf-8 -*-
"""Починка фраз, потерявших первое слово при сборке слоя.

Сборщик снимал ведущий артикль у ВСЕГО подряд, а у фразы «Das kriegen wir hin» первое
слово — не артикль перед существительным, а часть предложения. В итоге человек видел
обрубок «kriegen wir hin» вместо своей сохранённой фразы. Пострадало 1727 единиц.

Здесь исходный текст восстанавливается из строки банка, на которую единица ссылается
(старый банк только читается). Если единица с полным текстом уже есть — единицы
сливаются, а не плодятся. Прежнее (обрезанное) написание остаётся указателем, поэтому
всё, что уже ссылалось на единицу, продолжает находиться.

Запуск:
    DATABASE_URL=... python3 scripts/dict_units_fix_phrase_lemmas.py --dry-run
    DATABASE_URL=... python3 scripts/dict_units_fix_phrase_lemmas.py --apply
"""
from __future__ import annotations

import argparse
import os
import re
import time

import psycopg2

SPACE_RE = re.compile(r"\s+")


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


def norm(text: str) -> str:
    return SPACE_RE.sub(" ", str(text or "").strip()).casefold()


def merge_into(cur, *, victim: int, keeper: int) -> None:
    cur.execute(
        """
        INSERT INTO bt_3_lex_surfaces (lang, surface_key, unit_id, match_kind)
        SELECT lang, surface_key, %s, match_kind FROM bt_3_lex_surfaces WHERE unit_id = %s
        ON CONFLICT (lang, surface_key, unit_id) DO NOTHING;
        """, (keeper, victim))
    for sql in (
        """INSERT INTO bt_3_lex_links (from_unit, to_unit, rank, source)
           SELECT %s, to_unit, rank, source FROM bt_3_lex_links WHERE from_unit = %s AND to_unit <> %s
           ON CONFLICT (from_unit, to_unit) DO UPDATE SET rank = LEAST(bt_3_lex_links.rank, EXCLUDED.rank);""",
        """INSERT INTO bt_3_lex_links (from_unit, to_unit, rank, source)
           SELECT from_unit, %s, rank, source FROM bt_3_lex_links WHERE to_unit = %s AND from_unit <> %s
           ON CONFLICT (from_unit, to_unit) DO UPDATE SET rank = LEAST(bt_3_lex_links.rank, EXCLUDED.rank);""",
    ):
        cur.execute(sql, (keeper, victim, keeper))
    cur.execute(
        """INSERT INTO bt_3_lex_unit_sources (unit_id, entry_id, side)
           SELECT %s, entry_id, side FROM bt_3_lex_unit_sources WHERE unit_id = %s
           ON CONFLICT DO NOTHING;""", (keeper, victim))
    # Личные карточки, указывавшие на поглощаемую единицу, переводим на оставшуюся —
    # иначе указатель повис бы в пустоту.
    cur.execute("UPDATE bt_3_webapp_dictionary_queries SET lex_unit_id = %s WHERE lex_unit_id = %s;",
                (keeper, victim))
    cur.execute("DELETE FROM bt_3_lex_units WHERE id = %s;", (victim,))


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

    # Настоящий текст берём из банка: он там лежит нетронутым.
    cur.execute(
        """
        SELECT u.id, u.lang, u.kind, u.lemma, u.lemma_key,
               MIN(CASE WHEN s.side = 'source' THEN e.source_text ELSE e.target_text END) AS full_text
        FROM bt_3_lex_units u
        JOIN bt_3_lex_unit_sources s ON s.unit_id = u.id
        JOIN bt_3_dictionary_entries e ON e.id = s.entry_id
        WHERE u.kind <> 'word'
          AND ((s.side = 'source' AND e.source_text ~* '^(der|die|das)\\s')
            OR (s.side = 'target' AND e.target_text ~* '^(der|die|das)\\s'))
        GROUP BY u.id, u.lang, u.kind, u.lemma, u.lemma_key;
        """
    )
    rows = cur.fetchall()
    print("фраз к починке: %d" % len(rows))

    cur.execute("SELECT lang, kind, lemma_key, id FROM bt_3_lex_units WHERE kind <> 'word';")
    existing = {(r[0], r[1], r[2]): r[3] for r in cur.fetchall()}

    fixed = merged = skipped = 0
    for unit_id, lang, kind, lemma, lemma_key, full_text in rows:
        full = SPACE_RE.sub(" ", str(full_text or "").strip())
        if not full or norm(full) == lemma_key:
            skipped += 1
            continue
        # Обрезанное написание оставляем указателем: по нему уже могли искать.
        target = existing.get((lang, kind, norm(full)))
        if fixed + merged < 5:
            print("   %-40r → %r%s" % (lemma[:40], full[:52], "  (сливаю в существующую)" if target else ""))
        if not args.apply:
            if target:
                merged += 1
            else:
                fixed += 1
            continue
        cur.execute(
            """INSERT INTO bt_3_lex_surfaces (lang, surface_key, unit_id, match_kind)
               VALUES (%s, %s, %s, 'exact') ON CONFLICT DO NOTHING;""",
            (lang, norm(full), unit_id))
        if target and target != unit_id:
            merge_into(cur, victim=unit_id, keeper=target)
            merged += 1
            continue
        cur.execute(
            "UPDATE bt_3_lex_units SET lemma = %s, lemma_key = %s, display = %s, updated_at = NOW() "
            "WHERE id = %s;",
            (full, norm(full), full, unit_id))
        existing[(lang, kind, norm(full))] = unit_id
        fixed += 1

    print("\nитог: исправлено %d, слито с существующими %d, пропущено %d" % (fixed, merged, skipped))
    if args.apply:
        conn.commit()
        print("записано.")
    else:
        conn.rollback()
        print("(--dry-run: в базу ничего не записано)")
    cur.close()
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
