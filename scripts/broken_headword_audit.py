# -*- coding: utf-8 -*-
"""Заголовки, у которых съеден конец слова: «die Scheib», «die Abflughall».

Тот же класс, что и «артикль леммы на форме» — заголовок карточки не равен словарной
форме, — но причина другая: не число, а обрезанное написание. Найдены при разборе
«das Probleme» 29.07.2026 в общем пуле (провайдер base_dict и GPT).

Признак, по которому такое опознаётся надёжно и без догадок:
  • у самой поверхности нет ни рода в справочнике, ни страницы в Wiktionary;
  • а если дописать к ней типичное окончание (-e, -en, -er, -ung), получается слово,
    род которого документирован.
«Scheib» + e = «die Scheibe», «Abflughall» + e = «die Abflughalle» — попадание.
Редкий, но настоящий композит так не срабатывает: у него нет документированного
двойника на одну букву длиннее.

Только отчёт. Автоматически переименовывать заголовки в личных карточках нельзя:
это чужие данные, решение принимает владелец.

Запуск:  python3 scripts/broken_headword_audit.py [--examples 40]
"""
from __future__ import annotations

import argparse
import os
import re
import sys
from collections import defaultdict

import psycopg2

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from backend.german_surface import UNKNOWN, german_surface  # noqa: E402

ARTICLE_RE = re.compile(r"^(der|die|das)\s+", re.I)
CYRILLIC_RE = re.compile(r"[А-Яа-яЁё]")
# Хвосты, которые чаще всего и теряются при обрезке. Порядок — от частого к редкому.
TAILS = ("e", "en", "er", "ung", "n", "el")

SOURCES = {
    "общий пул": """
        SELECT id, COALESCE(NULLIF(word_de, ''),
                            CASE WHEN source_lang = 'de' THEN source_text ELSE target_text END),
               COALESCE(response_json->>'provider', '')
        FROM bt_3_dictionary_entries WHERE source_lang = 'de' OR target_lang = 'de'
    """,
    "карточки людей": """
        SELECT id, COALESCE(NULLIF(word_de, ''),
                            CASE WHEN source_lang = 'de' THEN word_ru ELSE translation_de END),
               COALESCE(origin_process, '')
        FROM bt_3_webapp_dictionary_queries WHERE source_lang = 'de' OR target_lang = 'de'
    """,
}


def bare(text: str) -> str:
    return ARTICLE_RE.sub("", re.sub(r"\s+", " ", str(text or "").strip())).strip()


def load_pageless(conn) -> set[str]:
    """Поверхности, о которых Wiktionary ответил «страницы нет» (пометка ночного
    прогрева). Именно этот признак отделяет обрезок от настоящего слова: «Band»,
    «Erbe», «Haft» страницу имеют, просто их рода нет в нашем кэше — трогать их нельзя,
    хотя дописанное окончание тоже даёт настоящее слово («Bande», «Erben», «Haftung»)."""
    with conn.cursor() as cur:
        cur.execute("SELECT surface_key FROM bt_3_german_form_index "
                    "WHERE number_tag = 'unknown' AND source LIKE 'wiktionary_нет%%'")
        return {str(r[0]) for r in cur.fetchall() or []}


def truncated_original(surface: str, cache: dict, pageless: set[str]) -> str:
    """Слово, из которого получилась эта обрезанная поверхность, иначе ''."""
    if surface in cache:
        return cache[surface]
    cache[surface] = ""
    if surface.casefold() not in pageless:
        return ""                      # страница есть или ещё не спрашивали — не трогаем
    if german_surface(surface)["number"] != UNKNOWN:
        return ""                      # поверхность опознана — она не сломана
    for tail in TAILS:
        candidate = surface + tail
        if german_surface(candidate)["article"]:
            cache[surface] = candidate
            break
    return cache[surface]


def lowercase_words(surfaces: list[str]) -> set[str]:
    """Из списка — те, у кого есть страница в НИЖНЕМ регистре: «Fremd» страницы не
    имеет, а «fremd» имеет, потому что это прилагательное. Такой заголовок не обрезан,
    он записан с большой буквы как будто существительное — это другой дефект."""
    from backend.article_wiktionary_ref import _fetch_wikitext  # noqa: PLC2701 — свой же модуль
    found: set[str] = set()
    titles = sorted({s[:1].lower() + s[1:] for s in surfaces})
    for i in range(0, len(titles), 45):
        pages = _fetch_wikitext(titles[i:i + 45])
        found |= {t.casefold() for t, text in pages.items() if text}
    return found


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--examples", type=int, default=40)
    args = parser.parse_args()
    dsn = (os.getenv("DATABASE_URL_PGBOUNCER_RAILWAY") or os.getenv("DATABASE_URL") or "").strip()
    if not dsn:
        print("Нет DATABASE_URL", file=sys.stderr)
        return 2
    conn = psycopg2.connect(dsn, connect_timeout=20)
    cache: dict = {}
    pageless = load_pageless(conn)
    print("═══ Обрезанные заголовки ═══")
    print(f"поверхностей без страницы в Wiktionary: {len(pageless)}")
    hits: dict[str, list] = {}
    by_origin: dict[str, int] = defaultdict(int)
    for label, sql in SOURCES.items():
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall() or []
        found = []
        for row_id, raw, origin in rows:
            surface = bare(raw)
            if not surface or " " in surface or not surface[:1].isupper() or CYRILLIC_RE.search(surface):
                continue
            original = truncated_original(surface, cache, pageless)
            if original:
                found.append((row_id, surface, original, origin))
                by_origin[origin or "неизвестно"] += 1
        hits[label] = found

    # Разводим два разных дефекта: обрезанное написание и потерянную часть речи.
    all_surfaces = sorted({h[1] for found in hits.values() for h in found})
    lower_pages = lowercase_words(all_surfaces)
    total_cut = total_case = 0
    for label, found in hits.items():
        cut = [h for h in found if (h[1][:1].lower() + h[1][1:]).casefold() not in lower_pages]
        case = [h for h in found if h not in cut]
        total_cut += len(cut)
        total_case += len(case)
        print(f"\n{label}: обрезано {len(cut)}, часть речи потеряна {len(case)}")
        for row_id, surface, original, origin in cut[:args.examples]:
            print(f"  ✂ id={row_id} «{surface}» → похоже на «{original}»"
                  f"{f' (источник: {origin})' if origin else ''}")
        for row_id, surface, original, origin in case[:args.examples]:
            print(f"  Aa id={row_id} «{surface}» — это «{surface[:1].lower() + surface[1:]}», "
                  f"не существительное{f' (источник: {origin})' if origin else ''}")
    print(f"\nвсего: обрезано {total_cut}, часть речи потеряна {total_case}")
    print("⚠ деление приблизительное: «Scheib» попадает во вторую корзину, потому что у "
          "«scheib» есть страница (повелительное наклонение). Правки — глазами, не скопом.")
    if by_origin:
        print("по источникам: " + ", ".join(f"{k}={v}" for k, v in sorted(by_origin.items(),
                                                                          key=lambda kv: -kv[1])))
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
