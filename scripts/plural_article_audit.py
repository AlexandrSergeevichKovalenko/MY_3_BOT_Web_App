# -*- coding: utf-8 -*-
"""Замер: сколько форм слова живёт в наших данных как самостоятельные слова.

Находка владельца: «Проблемы» → карточка «das Probleme». «das» — артикль леммы
«das Problem», приклеенный к форме множественного числа. Этот скрипт отвечает на
вопрос «это один случай или класс» — и служит приёмкой починки: после уборки
данных колонка «der/das на форме» обязана стать нулевой.

Смотрит четыре места, где заголовок виден человеку:
  • bt_3_dictionary_entries      — общий пул (виден ВСЕМ)
  • bt_3_webapp_dictionary_queries — личные карточки
  • bt_3_lex_units               — слой единиц (форма не должна быть единицей)
  • bt_3_article_sprint_nouns    — банк игры der/die/das (у формы нет верного ответа)

Ничего не меняет — только читает и считает.

Запуск (в контейнере прода):
    python3 scripts/plural_article_audit.py [--examples 20] [--llm]

--llm дополнительно замеряет быстрый путь: спрашивает у модели артикль для
фиксированного набора форм множественного числа и считает долю ошибок.
"""
from __future__ import annotations

import argparse
import os
import re
import sys
from collections import Counter, defaultdict

import psycopg2

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from backend.german_surface import PL, german_surface  # noqa: E402

ARTICLE_RE = re.compile(r"^(der|die|das)\s+", re.I)
CYRILLIC_RE = re.compile(r"[А-Яа-яЁё]")

# Множественное число, где артикль может быть только «die». На этом наборе
# замеряется быстрый путь: 29.07.2026 модель ошиблась в 10 случаях из 20.
LLM_PROBE_WORDS = [
    "Probleme", "Bücher", "Kinder", "Frauen", "Häuser", "Autos", "Tische", "Blumen",
    "Männer", "Städte", "Länder", "Wörter", "Fehler", "Fragen", "Antworten", "Ideen",
    "Zimmer", "Freunde", "Schuhe", "Termine", "Straßen", "Zeitungen", "Gläser",
    "Bäume", "Briefe", "Zahlen", "Wände", "Ärzte", "Schüler", "Wege",
]

SOURCES = {
    "общий пул": """
        SELECT id, COALESCE(NULLIF(word_de, ''),
                            CASE WHEN source_lang = 'de' THEN source_text ELSE target_text END),
               response_json->>'article'
        FROM bt_3_dictionary_entries
        WHERE source_lang = 'de' OR target_lang = 'de'
    """,
    "карточки людей": """
        SELECT id, COALESCE(NULLIF(word_de, ''),
                            CASE WHEN source_lang = 'de' THEN word_ru ELSE translation_de END),
               response_json->>'article'
        FROM bt_3_webapp_dictionary_queries
        WHERE source_lang = 'de' OR target_lang = 'de'
    """,
    "слой единиц": "SELECT id, display, gender FROM bt_3_lex_units WHERE lang = 'de' AND kind = 'word'",
    "банк «Артикли»": "SELECT id, word, article FROM bt_3_article_sprint_nouns",
}


def split_article(text: str) -> tuple[str, str]:
    compact = re.sub(r"\s+", " ", str(text or "").strip())
    match = ARTICLE_RE.match(compact)
    if match:
        return match.group(1).lower(), compact[match.end():]
    return "", compact


def is_noun_surface(word: str) -> bool:
    return bool(word) and " " not in word and word[:1].isupper() and not CYRILLIC_RE.search(word)


def audit(dsn: str, examples: int) -> int:
    conn = psycopg2.connect(dsn, connect_timeout=20)
    verdict_cache: dict[str, dict] = {}
    totals: dict[str, Counter] = defaultdict(Counter)
    samples: dict[str, list] = defaultdict(list)
    wrong_total = 0

    for label, sql in SOURCES.items():
        with conn.cursor() as cursor:
            cursor.execute(sql)
            rows = cursor.fetchall() or []
        for row_id, raw, stored_article in rows:
            article, bare = split_article(raw)
            if not is_noun_surface(bare):
                continue
            totals[label]["существительных всего"] += 1
            verdict = verdict_cache.get(bare)
            if verdict is None:
                verdict = german_surface(bare)
                verdict_cache[bare] = verdict
            if verdict["number"] != PL:
                continue
            shown = (article or str(stored_article or "")).strip().lower()
            shown = shown if shown in ("der", "die", "das") else "—"
            # Догадка правила НЕ идёт в главное число: пока индекс форм не прогрет,
            # правило принимает за формы реальные леммы, которых нет в кэше родов
            # (der Stürmer, das Abkommen, der Laster). Такое чинить нельзя.
            if verdict["confidence"] != "high":
                totals[label]["кандидатов (нужен прогрев)"] += 1
                continue
            totals[label]["форм в заголовке"] += 1
            totals[label][f"  артикль {shown}"] += 1
            if shown in ("der", "das"):
                wrong_total += 1
                if len(samples[label]) < examples:
                    samples[label].append((row_id, shown, bare, verdict["lemma"], verdict["source"]))

    print("═══ Формы слова в заголовках карточек ═══\n")
    for label in SOURCES:
        counts = totals[label]
        print(f"{label}:")
        for key in ("существительных всего", "форм в заголовке",
                    "  артикль der", "  артикль das", "  артикль die", "  артикль —",
                    "кандидатов (нужен прогрев)"):
            if counts.get(key):
                print(f"    {key:26} {counts[key]}")
        for sample in samples[label]:
            print(f"      ✗ id={sample[0]} «{sample[1]} {sample[2]}» — форма от «{sample[3]}» ({sample[4]})")
        print()

    by_confidence = Counter(v["confidence"] for v in verdict_cache.values() if v["number"] == PL)
    print(f"Разных форм-заголовков: подтверждено справочником {by_confidence.get('high', 0)}, "
          f"кандидатов по правилу {by_confidence.get('low', 0)} "
          f"(среди них есть реальные леммы — их разберёт прогрев индекса форм)")
    print(f"ГЛАВНОЕ ЧИСЛО — артикль леммы на подтверждённой форме (der/das): {wrong_total}")
    conn.close()
    return wrong_total


def probe_llm() -> None:
    """Замер быстрого пути: у формы множественного артикль может быть только «die»."""
    from backend.openai_manager import run_quick_article
    wrong = []
    for word in LLM_PROBE_WORDS:
        answer = run_quick_article(word=word)
        if answer != "die":
            wrong.append(f"{word}→{answer or 'пусто'}")
    print(f"\n═══ Быстрый путь (модель) ═══\n"
          f"проверено форм множественного: {len(LLM_PROBE_WORDS)}, ошибок: {len(wrong)}")
    if wrong:
        print("  " + ", ".join(wrong))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--examples", type=int, default=20, help="сколько примеров печатать")
    parser.add_argument("--llm", action="store_true", help="дополнительно замерить быстрый путь")
    args = parser.parse_args()

    dsn = (os.getenv("DATABASE_URL_PGBOUNCER_RAILWAY") or os.getenv("DATABASE_URL") or "").strip()
    if not dsn:
        print("Нет DATABASE_URL", file=sys.stderr)
        return 2
    wrong_total = audit(dsn, args.examples)
    if args.llm:
        probe_llm()
    return 0 if wrong_total == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
