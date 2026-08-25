#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Вычистить старое слово из карточек, исправленных на экране проверки слов.

ЗАЧЕМ. До 25.08.2026 правка с экрана проверки меняла две графы из восьми: word_de и
translation_ru. В строке оставались word_ru и translation_de со СТАРЫМ словом, весь
response_json (разбор про старое слово: значения, примеры, формы, род) и указатель на
запись общего пула, собранную вокруг обрубка. Тренажёр берёт текст карточки ИЗ РАЗБОРА
(frontend/src/App.jsx, resolveFlashcardTexts) — то есть человек исправлял слово и
продолжал бы учить старое.

Дыру закрыли в `word_confirm_digest._rewrite_card_to_new_word`; этот скрипт убирает то,
что успело накопиться ДО починки. Обе половины обязательны — одна без другой не считается.

ПОЧЕМУ РАЗБОР СТИРАЕТСЯ, А НЕ ПРАВИТСЯ. Исправленное слово — ДРУГОЕ слово: у
«Scheinwerfergla» перевод «стекло фары», у «Scheinwerfer» — «фара». Заменить внутри
разбора одно написание на другое значит оставить человеку значения, примеры и формы
чужого слова под новым заголовком.

ПУСТО НЕ ОСТАНЕТСЯ. Разбор живёт на слове (`bt_3_lex_units`), карточка берёт его по
указателю `lex_unit_id`. Обнулённый указатель подбирает `lex_units.attach_missing_entries`
(зовётся и ночью, и на чтении), а разбор дособирает ночной `_run_units_night_enrichment`.

ЗАПУСК. Сухой прогон по умолчанию, пишет только с `--apply`:
    railway run -s Postgres python3 scripts/word_audit_fix_stale_cards.py
    railway run -s Postgres python3 scripts/word_audit_fix_stale_cards.py --apply
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys

import psycopg2

БЕЗ_АРТИКЛЯ = re.compile(r"^(der|die|das)\s+", re.I)


def голое(текст) -> str:
    return БЕЗ_АРТИКЛЯ.sub("", str(текст or "").strip()).strip()


def то_же(левое, правое) -> bool:
    return голое(левое).casefold() == голое(правое).casefold()


def русское_из_разбора(разбор: dict) -> str:
    """Русская сторона, как её помнит СТАРЫЙ разбор. Нужна, чтобы отличить зеркальную
    графу (её переписываем) от текста, вписанного человеком (его не трогаем)."""
    for ключ in ("target_text", "translation_ru", "word_ru"):
        значение = str(разбор.get(ключ) or "").strip()
        if значение:
            return значение
    return ""


def немецкое_из_разбора(разбор: dict) -> str:
    for ключ in ("source_text", "word_de", "translation_de"):
        значение = str(разбор.get(ключ) or "").strip()
        if значение:
            return значение
    return ""


ВЫБОРКА = """
SELECT d.user_id, d.word, d.decision,
       q.id, q.word_de, q.word_ru, q.translation_de, q.translation_ru,
       q.canonical_entry_id, q.lex_unit_id, q.response_json
  FROM bt_3_word_confirm_digest d
  JOIN bt_3_webapp_dictionary_queries q ON q.user_id = d.user_id
 WHERE d.decision IN ('fixed', 'manual')
   -- Карточку ищем по СТАРОМУ слову внутри неё: заголовок уже переписан, и по нему
   -- решение с карточкой больше не сходится.
   AND (regexp_replace(COALESCE(q.response_json->>'word_de', ''), '^(der|die|das)[[:space:]]+', '', 'i') = d.word
     OR regexp_replace(COALESCE(q.response_json->>'source_text', ''), '^(der|die|das)[[:space:]]+', '', 'i') = d.word
     OR regexp_replace(COALESCE(q.translation_de, ''), '^(der|die|das)[[:space:]]+', '', 'i') = d.word)
 ORDER BY q.id;
"""


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="писать в базу (по умолчанию сухой прогон)")
    args = parser.parse_args()

    dsn = os.getenv("DATABASE_PUBLIC_URL") or os.getenv("DATABASE_URL")
    if not dsn:
        print("нет DATABASE_PUBLIC_URL / DATABASE_URL", file=sys.stderr)
        return 2

    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    итог = {"карточек": 0, "разборов стёрто": 0, "зеркал переписано": 0,
            "снято из пула": 0, "чужих граф не тронуто": 0}
    with conn, conn.cursor() as cur:
        cur.execute(ВЫБОРКА)
        строки = cur.fetchall() or []
        if not строки:
            print("нечего чистить: исправленных карточек со старым разбором нет")
            return 0

        пул_к_снятию: set[int] = set()
        for (user_id, старое, решение, card_id, word_de, word_ru, translation_de,
             translation_ru, canonical_id, lex_unit_id, разбор) in строки:
            разбор = разбор if isinstance(разбор, dict) else {}
            новое_de = str(word_de or "").strip()
            новое_ru = str(translation_ru or "").strip()

            # Зеркала переписываем ТОЛЬКО если в них лежит то, что помнит старый разбор.
            # Лежит что-то другое — это текст человека, он не наш.
            de_зеркало = новое_de if то_же(translation_de, старое) else None
            старое_ru = русское_из_разбора(разбор)
            ru_зеркало = новое_ru if (старое_ru and то_же(word_ru, старое_ru)
                                      and not то_же(word_ru, новое_ru)) else None
            if de_зеркало is None and str(translation_de or "").strip():
                итог["чужих граф не тронуто"] += 1
            if ru_зеркало is None and str(word_ru or "").strip() and not то_же(word_ru, новое_ru):
                итог["чужих граф не тронуто"] += 1

            print(f"\ncard {card_id} (человек {user_id}, решение «{решение}»)")
            print(f"   было в разборе : {немецкое_из_разбора(разбор)!r} / {старое_ru!r}")
            print(f"   стало в графах : {новое_de!r} / {новое_ru!r}")
            print(f"   разбор         : стереть ({len(json.dumps(разбор))} байт про старое слово)")
            if de_зеркало:
                print(f"   translation_de : {translation_de!r} → {de_зеркало!r}")
            if ru_зеркало:
                print(f"   word_ru        : {word_ru!r} → {ru_зеркало!r}")
            if lex_unit_id:
                print(f"   lex_unit_id    : {lex_unit_id} → NULL (ночь привяжет к новому слову)")

            # Запись пула снимаем, только если она вправду собрана вокруг старого слова.
            if canonical_id:
                cur.execute("SELECT source_text, target_text, word_de, translation_de "
                            "FROM bt_3_dictionary_entries WHERE id=%s;", (int(canonical_id),))
                запись = cur.fetchone()
                if запись and any(то_же(значение, старое) for значение in запись):
                    пул_к_снятию.add(int(canonical_id))
                    print(f"   пул {canonical_id}       : снять — {запись[0]!r} → {запись[1]!r}")
                else:
                    print(f"   пул {canonical_id}       : про другое слово, не трогаем")

            итог["карточек"] += 1
            итог["разборов стёрто"] += 1
            итог["зеркал переписано"] += bool(de_зеркало) + bool(ru_зеркало)

            if args.apply:
                cur.execute(
                    """UPDATE bt_3_webapp_dictionary_queries
                          SET translation_de = COALESCE(%s, translation_de),
                              word_ru        = COALESCE(%s, word_ru),
                              response_json  = NULL,
                              canonical_entry_id = NULL,
                              lex_unit_id    = NULL,
                              updated_at     = NOW()
                        WHERE id = %s;""",
                    (de_зеркало, ru_зеркало, int(card_id)))

        for pool_id in sorted(пул_к_снятию):
            итог["снято из пула"] += 1
            if args.apply:
                cur.execute("UPDATE bt_3_webapp_dictionary_queries SET canonical_entry_id=NULL "
                            "WHERE canonical_entry_id=%s;", (pool_id,))
                cur.execute("DELETE FROM bt_3_dictionary_entries WHERE id=%s;", (pool_id,))

        if not args.apply:
            conn.rollback()

    print("\n" + ("ЗАПИСАНО" if args.apply else "СУХОЙ ПРОГОН — в базу не писали"))
    for ключ, значение in итог.items():
        print(f"   {ключ}: {значение}")
    if not args.apply:
        print("\nповторить с --apply, чтобы применить")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
