#!/usr/bin/env python3
"""Загрузить текстовую выгрузку OpenThesaurus в `bt_3_openthesaurus_synsets`.

Источник: https://www.openthesaurus.de/export/OpenThesaurus-Textversion.zip (LGPL 2.1,
© Daniel Naber). Одна строка файла = одно гнездо синонимов, слова через «;», пометки в
скобках («(ugs.)», «(Hauptform)», «(sich)») — часть написания, ключ их срезает
(`synonym_sources.term_key`).

Запуск:
    python3 scripts/load_openthesaurus.py --dry-run          # скачать, разобрать, посчитать
    python3 scripts/load_openthesaurus.py --apply            # залить (таблица ПЕРЕЗАПИСЫВАЕТСЯ)
    python3 scripts/load_openthesaurus.py --apply --file x.txt   # из уже скачанного файла

Нужен DATABASE_URL (живая база — через `railway variables --service Postgres --kv`,
ключ DATABASE_PUBLIC_URL). Дата выгрузки (строка «# Automatically generated …» в шапке
файла) пишется в `bt_3_admin_kv` под ключом `openthesaurus_export_date`, чтобы отчёт мог
назвать, какой версией словаря подтверждались синонимы.

Образец пачечной вставки — `scripts/tatoeba_import.py` (execute_values, коммит на партию).
"""
from __future__ import annotations

import argparse
import io
import os
import re
import sys
import urllib.request
import zipfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

EXPORT_URL = "https://www.openthesaurus.de/export/OpenThesaurus-Textversion.zip"
CHUNK = 2000


def _download() -> str:
    req = urllib.request.Request(EXPORT_URL, headers={"User-Agent": "DeutschBot/1.0 (OpenThesaurus loader)"})
    with urllib.request.urlopen(req, timeout=120) as resp:
        blob = resp.read()
    with zipfile.ZipFile(io.BytesIO(blob)) as z:
        name = next(n for n in z.namelist() if n.endswith(".txt") and "LICENSE" not in n.upper())
        return z.read(name).decode("utf-8")


def parse(text: str) -> tuple[str, list[tuple[int, str, str]]]:
    """(дата выгрузки, [(synset_id, term, term_key)])."""
    from backend.synonym_sources import term_key
    export_date = ""
    rows: list[tuple[int, str, str]] = []
    sid = 0
    for line in text.splitlines():
        if line.startswith("#"):
            m = re.search(r"Automatically generated (\S+)", line)
            if m:
                export_date = m.group(1)
            continue
        terms = [t.strip() for t in line.split(";") if t.strip()]
        if len(terms) < 2:
            continue
        sid += 1
        seen: set[str] = set()
        for t in terms:
            if t in seen:
                continue
            seen.add(t)
            key = term_key(t)
            if key:
                rows.append((sid, t, key))
    return export_date, rows


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--file", help="уже скачанный openthesaurus.txt")
    args = ap.parse_args()
    if not args.apply and not args.dry_run:
        ap.error("нужен --dry-run или --apply")
    text = open(args.file, encoding="utf-8").read() if args.file else _download()
    export_date, rows = parse(text)
    synsets = len({r[0] for r in rows})
    print(f"выгрузка от {export_date or '?'}: гнёзд {synsets}, строк {len(rows)}, "
          f"разных ключей {len({r[2] for r in rows})}")
    if not args.apply:
        return 0
    from psycopg2.extras import execute_values
    from backend.database import admin_kv_set, get_db_connection_context
    from backend.synonym_sources import ensure_openthesaurus_schema
    ensure_openthesaurus_schema()
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE bt_3_openthesaurus_synsets")
            for i in range(0, len(rows), CHUNK):
                execute_values(cur, "INSERT INTO bt_3_openthesaurus_synsets (synset_id, term, term_key) VALUES %s",
                               rows[i:i + CHUNK])
        conn.commit()
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*), COUNT(DISTINCT synset_id) FROM bt_3_openthesaurus_synsets")
            n, s = cur.fetchone()
    admin_kv_set("openthesaurus_export_date", export_date or "unknown")
    print(f"в базе: строк {n}, гнёзд {s} (ожидалось {len(rows)} / {synsets})")
    return 0 if (n == len(rows) and s == synsets) else 1


if __name__ == "__main__":
    sys.exit(main())
