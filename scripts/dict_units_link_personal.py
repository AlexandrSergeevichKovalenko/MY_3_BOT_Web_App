# -*- coding: utf-8 -*-
"""Переклейка личных карточек на единицы словаря.

В личной таблице bt_3_webapp_dictionary_queries появляется ОДНА новая пустая колонка
lex_unit_id — указатель на единицу. Больше в этой таблице не меняется ничего: текст,
перевод, разбор, папки, даты повторений и статистика ответов остаются как были.

Дубликаты НЕ склеиваются (решение владельца): если одно слово сохранено дважды, обе
карточки просто указывают на одну единицу. Прогресс тренировки (bt_3_flashcard_stats,
bt_3_flashcard_seen) привязан к личным карточкам и не трогается вовсе.

Порядок опознания единицы, от надёжного к запасному:
  1. по строке общего банка, из которой карточка выросла (bt_3_lex_unit_sources);
  2. по написанию — через указатели слоя;
  3. не опознали — оставляем пусто, карточка работает как раньше.

Запуск:
    DATABASE_URL=... python3 scripts/dict_units_link_personal.py --dry-run
    DATABASE_URL=... python3 scripts/dict_units_link_personal.py --apply
"""
from __future__ import annotations

import argparse
import collections
import os
import re
import time

import psycopg2
import psycopg2.extras

ARTICLE_RE = re.compile(r"^(der|die|das)\s+", re.I)
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


def strip_article(text: str) -> str:
    return ARTICLE_RE.sub("", str(text or "").strip()).strip()


def article_of(text: str) -> str:
    m = ARTICLE_RE.match(str(text or "").strip())
    return m.group(1).lower() if m else ""


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

    if args.apply:
        # Колонка добавляется пустой и необязательной: пока она не заполнена,
        # приложение работает ровно как раньше.
        cur.execute("ALTER TABLE bt_3_webapp_dictionary_queries "
                    "ADD COLUMN IF NOT EXISTS lex_unit_id BIGINT;")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_webapp_dict_queries_lex_unit "
                    "ON bt_3_webapp_dictionary_queries (lex_unit_id);")

    print("читаю слой…")
    cur.execute("SELECT unit_id, entry_id FROM bt_3_lex_unit_sources;")
    units_by_entry: dict[int, set] = collections.defaultdict(set)
    for unit_id, entry_id in cur.fetchall():
        units_by_entry[entry_id].add(unit_id)
    cur.execute("SELECT s.lang, s.surface_key, s.unit_id FROM bt_3_lex_surfaces s;")
    units_by_surface: dict[tuple, set] = collections.defaultdict(set)
    for lang, surface_key, unit_id in cur.fetchall():
        units_by_surface[(lang, surface_key)].add(unit_id)
    cur.execute("SELECT id, lang, kind, lemma_key, COALESCE(gender,''), display FROM bt_3_lex_units;")
    unit_info = {r[0]: {"lang": r[1], "kind": r[2], "lemma_key": r[3], "gender": r[4], "display": r[5]}
                 for r in cur.fetchall()}
    print("  единиц %d, указателей %d, ссылок на строки банка %d"
          % (len(unit_info), len(units_by_surface), len(units_by_entry)))

    cur.execute("""
        SELECT id, user_id, canonical_entry_id, word_de, word_ru, source_lang, target_lang
        FROM bt_3_webapp_dictionary_queries;
    """)
    rows = cur.fetchall()
    print("личных карточек: %d" % len(rows))

    def pick(candidates: set, german_text: str) -> int | None:
        """Из нескольких единиц выбираем ту, чей род совпадает с артиклем в карточке.
        Омографы («der Kiefer» / «die Kiefer») иначе развесить нечем — и гадать нельзя."""
        candidates = {c for c in candidates if c in unit_info and unit_info[c]["lang"] == "de"}
        if not candidates:
            return None
        if len(candidates) == 1:
            return next(iter(candidates))
        art = article_of(german_text)
        if art:
            same = [c for c in candidates if unit_info[c]["gender"] == art]
            if len(same) == 1:
                return same[0]
        return None

    decided: list[tuple] = []
    owners: dict[int, int] = {}
    by_entry = by_surface = ambiguous = missing = foreign = no_german = 0
    for qid, user_id, entry_id, word_de, word_ru, sl, tl in rows:
        if "de" not in {str(sl or "").lower(), str(tl or "").lower()}:
            foreign += 1  # английские и итальянские карточки — не наш случай
            continue
        german = word_de or ""
        if not german:
            no_german += 1
            continue
        unit_id = None
        if entry_id and entry_id in units_by_entry:
            unit_id = pick(units_by_entry[entry_id], german)
            if unit_id:
                by_entry += 1
        if unit_id is None:
            key = ("de", norm(strip_article(german)))
            found = units_by_surface.get(key) or set()
            unit_id = pick(found, german)
            if unit_id:
                by_surface += 1
            elif found:
                ambiguous += 1
            else:
                missing += 1
        if unit_id:
            decided.append((unit_id, qid))
            owners[qid] = user_id

    print("\nопознано:")
    print("  по строке банка:            %6d" % by_entry)
    print("  по написанию:               %6d" % by_surface)
    print("  неоднозначно (омографы):    %6d" % ambiguous)
    print("  единица не нашлась:         %6d" % missing)
    print("  без немецкого текста:       %6d" % no_german)
    print("  английские/итальянские:     %6d" % foreign)
    print("  ИТОГО получат указатель:    %6d из %d" % (len(decided), len(rows)))

    # Дубль — это когда ОДИН человек сохранил одно слово дважды. Одно слово у разных
    # людей дублем не является: карточки личные, единица общая.
    per_user = collections.Counter((owners[qid], unit_id) for unit_id, qid in decided)
    dup_groups = sum(1 for n in per_user.values() if n > 1)
    dup_cards = sum(n for n in per_user.values() if n > 1)
    print("  у одного человека одно слово дважды: %d случаев (%d карточек) — НЕ склеиваем"
          % (dup_groups, dup_cards))

    if not args.apply:
        conn.rollback()
        print("\n(--dry-run: в базу ничего не записано)")
        return 0

    psycopg2.extras.execute_values(
        cur,
        "UPDATE bt_3_webapp_dictionary_queries AS q SET lex_unit_id = v.unit_id "
        "FROM (VALUES %s) AS v(unit_id, qid) WHERE q.id = v.qid",
        decided, page_size=1000,
    )
    # Проверка «до/после»: ни одна карточка не должна пропасть или измениться,
    # кроме появления указателя.
    cur.execute("SELECT COUNT(*) FROM bt_3_webapp_dictionary_queries;")
    after = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM bt_3_webapp_dictionary_queries WHERE lex_unit_id IS NOT NULL;")
    linked = cur.fetchone()[0]
    if after != len(rows):
        conn.rollback()
        raise SystemExit("число карточек изменилось (%d → %d) — откат" % (len(rows), after))
    conn.commit()
    print("\nзаписано. карточек %d (было %d), из них с указателем %d" % (after, len(rows), linked))
    cur.close()
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
