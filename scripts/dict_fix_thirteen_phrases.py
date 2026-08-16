# -*- coding: utf-8 -*-
"""Тринадцать фраз: правильный немецкий — везде, где он лежит.

Откуда взялись
──────────────
16.08.2026 проверка прочитала глазами все 105 уникальных правок владельца, принятых по
предложению судьи, и нашла тринадцать, где стало ХУЖЕ: пропал предлог («nagen an» →
«nagen»), сменился род («all dieser Kram» → «all dieses Kram»), перевернулся смысл
(«говорят, что развелись» → «они должны развестись»). Владелец разобрал каждую и принял
исправления 16.08.2026.

Что делает скрипт
─────────────────
Заменяет текст ВЕЗДЕ, где он лежит:
    личная карточка — колонки word_de, translation_de
    личная карточка — весь разбор рекурсивно, КРОМЕ полей-исключений (ниже)
    слово в справочнике — display, lemma, lemma_key + написание для поиска
    разбор на слове — рекурсивно
    общий пул bt_3_dictionary_entries — source_text и разбор

⚠ ПОЛЯ-ИСКЛЮЧЕНИЯ, их не трогаем никогда:
    original_query, raw_text  — это ИСТОРИЯ: чем человек искал. Переписать её значит
                                соврать, что он искал другое.
    corrected_form            — след прошлой правки, не текущий текст.
    pronunciation.*           — там разметка ударений «Ich HAbe SEInem RAT geFOLGT»,
                                она совпадает с текстом только по буквам; заменишь —
                                разметка умрёт, а ipa рядом останется от старого слова.
    translation_ru            — русская сторона. Немецкий туда писать нельзя, даже если
                                в конкретной карточке стороны когда-то перепутали.
    sentence_gap_v2           — заготовка задания. Там пропуск «___», и заменой строки
                                её не починить: вопрос и ответ разойдутся. Такие задания
                                СНОСИМ, чтобы собрались заново.

Проверено тремя независимыми прогонами до применения. Первая версия этого скрипта имела
безусловный jsonb_strip_nulls и удалила бы forms.plural у 778 карточек — поймано до
запуска.

    python3 scripts/dict_fix_thirteen_phrases.py            # сухой прогон
    python3 scripts/dict_fix_thirteen_phrases.py --apply    # записать
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SKIP_BILLING_LEDGER_WRITES", "1")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.database import get_db_connection_context      # noqa: E402
from backend.lex_units import normalize_query               # noqa: E402

_SPACE = re.compile(r"\s+")

# что лежит сейчас → как правильно. Разобрано владельцем 16.08.2026.
FIXES = {
    "Die Schuld hat mich lange genagt.": "Die Schuld hat lange an mir genagt",
    "Sie sollen sich scheiden lassen": "Sie sollen sich haben scheiden lassen",
    "Was sie nicht gegessen hat, aber am Seki Sippen tat sie ordentlich.":
        "Gegessen hat sie nichts, aber am Sekt genippt hat sie ordentlich.",
    "all dieses Kram": "all dieser Kram",
    "ihr ganz Talent unter Beweis stellen": "ihr ganzes Talent unter Beweis stellen",
    "Das Zylinder müsste geliefert worden sein": "Der Zylinder müsste geliefert worden sein",
    "Die Katze lauert die Maus": "Die Katze lauert auf die Maus",
    "der Unterhalt zahlen": "Unterhalt zahlen",
    "Die Kinder sind neugierig auf ihren Lehrer zu erfahren":
        "Die Kinder sind neugierig auf ihren Lehrer",
    "ein Stück zurück": "ein Stück rücken",
    "sich von etwas verabschieden": "sich etwas schenken",
    "sich kündigen von": "bei jemandem kündigen",
    "ist oft Gefahr im Verzug": "Es ist oft Gefahr im Verzug",
}

# Ключи, внутрь которых не заходим ни на каком уровне.
UNTOUCHED = {"original_query", "raw_text", "corrected_form", "pronunciation",
             "translation_ru", "sentence_gap_v2", "word_ru"}


def key_of(value) -> str:
    return _SPACE.sub(" ", str(value or "").strip()).casefold()


TABLE = {key_of(k): v for k, v in FIXES.items()}


def replace_deep(node, path: tuple = ()) -> tuple[object, int]:
    """Заменить известные фразы всюду, кроме полей-исключений."""
    if isinstance(node, str):
        fixed = TABLE.get(key_of(node))
        return (fixed, 1) if fixed and fixed != node else (node, 0)
    if isinstance(node, list):
        out, hits = [], 0
        for item in node:
            value, n = replace_deep(item, path)
            out.append(value)
            hits += n
        return out, hits
    if isinstance(node, dict):
        out, hits = {}, 0
        for name, item in node.items():
            if name in UNTOUCHED:
                out[name] = item
                continue
            value, n = replace_deep(item, path + (name,))
            out[name] = value
            hits += n
        return out, hits
    return node, 0


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    olds = list(FIXES.keys())
    report = {"карточки": 0, "мест в карточках": 0, "слова": 0, "разборы слов": 0,
              "пул": 0, "написания": 0, "задания снесены": 0}

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            # ── личные карточки ────────────────────────────────────────────────
            cur.execute(
                """
                SELECT id, word_de, translation_de, response_json
                FROM bt_3_webapp_dictionary_queries
                WHERE word_de = ANY(%s) OR translation_de = ANY(%s)
                   OR response_json::text ILIKE ANY(%s)
                ORDER BY id;
                """,
                (olds, olds, ["%" + o + "%" for o in olds]),
            )
            cards = cur.fetchall()
            for entry_id, word_de, translation_de, payload in cards:
                card = payload if isinstance(payload, dict) else {}
                fixed_card, hits = replace_deep(card)
                new_word = TABLE.get(key_of(word_de))
                new_tr = TABLE.get(key_of(translation_de))
                gap_stale = isinstance(card.get("sentence_gap_v2"), dict) and any(
                    o.casefold() in json.dumps(card["sentence_gap_v2"], ensure_ascii=False).casefold()
                    for o in olds
                )
                if not (hits or new_word or new_tr or gap_stale):
                    continue
                report["карточки"] += 1
                report["мест в карточках"] += hits + bool(new_word) + bool(new_tr)
                if gap_stale:
                    fixed_card.pop("sentence_gap_v2", None)
                    report["задания снесены"] += 1
                if args.apply:
                    cur.execute(
                        "UPDATE bt_3_webapp_dictionary_queries SET response_json = %s::jsonb, "
                        "word_de = COALESCE(%s, word_de), "
                        "translation_de = COALESCE(%s, translation_de), updated_at = NOW() "
                        "WHERE id = %s;",
                        (json.dumps(fixed_card, ensure_ascii=False), new_word, new_tr, entry_id),
                    )

            # ── слова справочника ──────────────────────────────────────────────
            cur.execute(
                "SELECT id, display, card FROM bt_3_lex_units WHERE lang='de' AND display = ANY(%s);",
                (olds,),
            )
            for unit_id, display, card in cur.fetchall():
                new_text = TABLE.get(key_of(display))
                if not new_text:
                    continue
                report["слова"] += 1
                if args.apply:
                    cur.execute(
                        "UPDATE bt_3_lex_units SET display=%s, lemma=%s, lemma_key=%s, "
                        "updated_at=NOW() WHERE id=%s;",
                        (new_text, new_text, normalize_query(new_text), unit_id),
                    )
                    cur.execute(
                        "INSERT INTO bt_3_lex_surfaces (lang, surface_key, unit_id, match_kind) "
                        "VALUES ('de', %s, %s, 'exact') ON CONFLICT DO NOTHING;",
                        (normalize_query(new_text), unit_id),
                    )
                    report["написания"] += 1

            # разбор на слове — рекурсивно, даже если само написание уже верное
            cur.execute(
                "SELECT id, card FROM bt_3_lex_units WHERE card IS NOT NULL "
                "AND card::text ILIKE ANY(%s);",
                (["%" + o + "%" for o in olds],),
            )
            for unit_id, card in cur.fetchall():
                fixed, hits = replace_deep(card if isinstance(card, dict) else {})
                if not hits:
                    continue
                report["разборы слов"] += 1
                if args.apply:
                    cur.execute(
                        "UPDATE bt_3_lex_units SET card=%s::jsonb, updated_at=NOW() WHERE id=%s;",
                        (json.dumps(fixed, ensure_ascii=False), unit_id),
                    )

            # ── общий пул ──────────────────────────────────────────────────────
            cur.execute(
                "SELECT id, source_text, response_json FROM bt_3_dictionary_entries "
                "WHERE source_text = ANY(%s) OR response_json::text ILIKE ANY(%s);",
                (olds, ["%" + o + "%" for o in olds]),
            )
            for pool_id, source_text, payload in cur.fetchall():
                fixed, hits = replace_deep(payload if isinstance(payload, dict) else {})
                new_source = TABLE.get(key_of(source_text))
                if not (hits or new_source):
                    continue
                report["пул"] += 1
                if args.apply:
                    cur.execute(
                        "UPDATE bt_3_dictionary_entries SET response_json=%s::jsonb, "
                        "source_text=COALESCE(%s, source_text), updated_at=NOW() WHERE id=%s;",
                        (json.dumps(fixed, ensure_ascii=False), new_source, pool_id),
                    )
        if args.apply:
            conn.commit()

    for name, count in report.items():
        print("   %-20s %d" % (name, count))
    if not args.apply:
        print("\nСУХОЙ ПРОГОН. Ничего не записано. Для записи добавь --apply.")


if __name__ == "__main__":
    main()
