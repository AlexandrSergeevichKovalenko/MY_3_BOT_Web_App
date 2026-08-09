#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Загрузить пары «немецкое ↔ русское предложение» из Tatoeba в нашу базу.

    python scripts/tatoeba_import.py --dry-run      # посчитать, ничего не писать
    python scripts/tatoeba_import.py --apply        # записать
    python scripts/tatoeba_import.py --apply --limit 5000

Почему выгрузка, а не запрос к их сайту на лету. Пример нужен в момент показа карточки;
ходить за ним к чужому серверу — это и задержка на горячем пути, и зависимость от их
доступности. Выгрузка мала: файл пар весит 1,4 МБ, немецкие предложения 17 МБ, русские
15 МБ. Раз в месяц этого достаточно — корпус растёт медленно.

Замер 09.08.2026: 225 027 готовых пар, и для 64% наших немецких слов пример находится.

Лицензия CC BY 2.0 FR требует указания источника, поэтому автора предложения храним
рядом с текстом и показываем в карточке.
"""

from __future__ import annotations

import argparse
import bz2
import os
import sys
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

from backend.corpus_examples import (  # noqa: E402
    MAX_EXAMPLE_CHARS,
    MIN_EXAMPLE_CHARS,
    ensure_corpus_schema,
)

BASE = "https://downloads.tatoeba.org/exports/per_language"
LINKS_URL = f"{BASE}/deu/deu-rus_links.tsv.bz2"
DE_URL = f"{BASE}/deu/deu_sentences_detailed.tsv.bz2"
RU_URL = f"{BASE}/rus/rus_sentences.tsv.bz2"


def _download(url: str, dest: Path) -> Path:
    if dest.exists() and dest.stat().st_size > 0:
        print(f"  уже скачано: {dest.name}")
        return dest
    print(f"  качаю {url} …")
    with urllib.request.urlopen(url, timeout=300) as resp, dest.open("wb") as out:
        out.write(resp.read())
    return dest


def _read_bz2_tsv(path: Path):
    with bz2.open(path, "rt", encoding="utf-8", errors="ignore") as fh:
        for line in fh:
            yield line.rstrip("\n").split("\t")


def collect_pairs(workdir: Path, limit: int | None = None) -> list[tuple]:
    links = _download(LINKS_URL, workdir / "links.tsv.bz2")
    de_file = _download(DE_URL, workdir / "de.tsv.bz2")
    ru_file = _download(RU_URL, workdir / "ru.tsv.bz2")

    print("  читаю немецкие предложения…")
    de: dict[str, tuple[str, str, str]] = {}
    for parts in _read_bz2_tsv(de_file):
        if len(parts) >= 4:
            de[parts[0]] = (parts[2], parts[3], parts[4] if len(parts) > 4 else "")

    print("  читаю русские предложения…")
    ru: dict[str, str] = {}
    for parts in _read_bz2_tsv(ru_file):
        if len(parts) >= 3:
            ru[parts[0]] = parts[2]

    print("  сшиваю пары…")
    pairs: list[tuple] = []
    seen: set[str] = set()
    for parts in _read_bz2_tsv(links):
        if len(parts) < 2:
            continue
        de_id, ru_id = parts[0], parts[1]
        if de_id in seen or de_id not in de or ru_id not in ru:
            continue
        text_de, author, license_raw = de[de_id]
        text_ru = ru[ru_id]
        # Отсеиваем на входе то, что всё равно не покажем: слишком коротко ничему не
        # учит, слишком длинно не читают с телефона. Так база не носит лишнего.
        if not (MIN_EXAMPLE_CHARS <= len(text_de) <= MAX_EXAMPLE_CHARS):
            continue
        if not text_ru.strip():
            continue
        seen.add(de_id)
        license_value = license_raw.strip() if license_raw.strip() not in ("", "\\N") else "CC BY 2.0 FR"
        pairs.append((int(de_id), text_de, text_ru, author, license_value))
        if limit and len(pairs) >= limit:
            break
    return pairs


def write_pairs(pairs: list[tuple]) -> int:
    """Записать пары пакетами.

    Первый заход делал executemany — то есть отдельный запрос на КАЖДУЮ строку. Через
    внешний прокси это дало 15 строк в секунду: 9 000 за десять минут при 225 тысячах
    к загрузке, то есть четыре часа. Одна строка — один обмен с сервером, и платим мы
    не за работу базы, а за задержку сети.

    execute_values склеивает партию в ОДИН запрос. Партиями по 2 000, с фиксацией после
    каждой: обрыв связи на середине не должен обнулять уже загруженное, а повторный
    запуск дописывает недостающее (ON CONFLICT обновляет, а не плодит копии).
    """
    from psycopg2.extras import execute_values
    from backend.database import get_db_connection_context
    written = 0
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            CHUNK = 2000
            for i in range(0, len(pairs), CHUNK):
                chunk = pairs[i:i + CHUNK]
                execute_values(
                    cur,
                    """
                    INSERT INTO bt_3_corpus_examples
                        (source, source_id, text_de, text_ru, author, license)
                    VALUES %s
                    ON CONFLICT (source, source_id) DO UPDATE
                       SET text_de = EXCLUDED.text_de,
                           text_ru = EXCLUDED.text_ru,
                           author  = EXCLUDED.author,
                           license = EXCLUDED.license;
                    """,
                    [("tatoeba", *row) for row in chunk],
                    page_size=500,
                )
                conn.commit()
                written += len(chunk)
                print(f"    записано {written} / {len(pairs)}", flush=True)
    return written


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="записать в базу")
    ap.add_argument("--dry-run", action="store_true", help="только посчитать")
    ap.add_argument("--limit", type=int, default=0, help="взять не больше N пар")
    args = ap.parse_args()
    if not args.apply and not args.dry_run:
        ap.error("укажите --apply или --dry-run")

    workdir = Path(os.getenv("TATOEBA_WORKDIR") or "/tmp/tatoeba")
    workdir.mkdir(parents=True, exist_ok=True)

    pairs = collect_pairs(workdir, limit=args.limit or None)
    print(f"\nготовых пар после отбора: {len(pairs)}")
    if args.dry_run:
        for de_id, text_de, text_ru, author, _lic in pairs[:5]:
            print(f"  [{de_id}] {text_de}  —  {text_ru}   ({author})")
        print("\n(пробный прогон, в базу ничего не записано)")
        return 0

    ensure_corpus_schema()
    written = write_pairs(pairs)
    print(f"\nзаписано пар: {written}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
