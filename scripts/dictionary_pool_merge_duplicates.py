#!/usr/bin/env python3
"""Одно слово — одна карточка в общем пуле.

Ключ уникальности пула («пара языков + текст запроса + текст ответа») заводил новую
строку и на артикль («die Mündung» против «Mündung»), и на другую формулировку перевода
(«устье» против «устье, дуло»). Из-за этого человек видел РАЗНЫЙ перевод одного слова
в зависимости от того, набрал он артикль или нет.

Что делает проход:
  • собирает строки одного слова одного РОДА (разный род — разные слова, не сливаем:
    «der Kiefer» челюсть против «die Kiefer» сосна);
  • оставляет самую полную карточку победителем;
  • проигравшим проставляет `merged_into` в response_json — их перестают отдавать и
    добирать, но НИЧЕГО не удаляется, откат — снятием этого поля;
  • личные карточки людей перевешивает на победителя (там, где у человека ещё нет
    своей записи на победителе — иначе оставляет как есть, чтобы не тронуть тренировку).

Запуск:
    python scripts/dictionary_pool_merge_duplicates.py            # прогон вхолостую
    python scripts/dictionary_pool_merge_duplicates.py --apply    # запись
    python scripts/dictionary_pool_merge_duplicates.py --undo     # снять merged_into
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict

sys.path.insert(0, ".")

from backend.database import (  # noqa: E402
    get_db_connection_context,
    _dictionary_entry_article,
    _dictionary_text_is_single_word,
    _normalize_dictionary_headword_key,
)


def card_score(payload: dict | None) -> int:
    """Насколько карточка полна. Побеждает та, где человеку больше пользы."""
    payload = payload if isinstance(payload, dict) else {}
    score = 0
    for key, weight in (
        ("usage_examples", 3),
        ("dictionary_senses", 3),
        ("translations", 2),
        ("common_collocations", 2),
        ("government_patterns", 2),
        ("synonyms", 1),
        ("related_words", 1),
    ):
        value = payload.get(key)
        if isinstance(value, list):
            score += weight * min(len(value), 5)
    meanings = payload.get("meanings")
    if isinstance(meanings, dict):
        primary = meanings.get("primary")
        if isinstance(primary, dict) and str(primary.get("value") or "").strip():
            score += 5
        secondary = meanings.get("secondary")
        if isinstance(secondary, list):
            score += min(len(secondary), 5)
    for key in ("forms", "grammar_tables", "pronunciation", "word_formation"):
        if payload.get(key):
            score += 1
    return score


def headline_penalty(target_text: str | None) -> int:
    """Подпись под словом должна быть переводом, а не фразой. Без этого штрафа
    победителем становилась строка с самой полной карточкой, но с подписью «Ты соврал?»
    (живой случай «flunkern»), и человек видел под словом кусок примера."""
    text = str(target_text or "").strip()
    if not text:
        return -20
    penalty = 0
    if any(mark in text for mark in "?!"):
        penalty -= 12
    if text.endswith("."):
        penalty -= 8
    if len(text.split()) > 5:
        penalty -= 6
    return penalty


def load_rows(cursor) -> list[dict]:
    cursor.execute(
        """
        SELECT id, source_lang, target_lang, source_text, target_text,
               source_headword_norm, response_json, created_at
        FROM bt_3_dictionary_entries
        WHERE source_headword_norm IS NOT NULL
          AND COALESCE(response_json->>'merged_into', '') = ''
        ORDER BY id
        """
    )
    rows = []
    for row in cursor.fetchall() or []:
        payload = row[6] if isinstance(row[6], dict) else json.loads(row[6] or "{}")
        german_is_source = row[1] == "de"
        rows.append(
            {
                "id": int(row[0]),
                "source_lang": row[1],
                "target_lang": row[2],
                "source_text": row[3],
                "target_text": row[4],
                "headword": row[5] or _normalize_dictionary_headword_key(row[3]),
                "target_headword": _normalize_dictionary_headword_key(row[4]),
                "payload": payload,
                "article": _dictionary_entry_article(
                    row[3] if german_is_source else row[4], payload
                ),
            }
        )
    return rows


def build_groups(rows: list[dict]) -> list[list[dict]]:
    """Группы «одно слово одного рода». Строки без артикля пристёгиваем к роду только
    тогда, когда род в группе ОДИН — иначе непонятно, к какому слову они относятся.

    ⚠️ Слово — это НЕМЕЦКАЯ сторона. В de→ru она в запросе, и разные русские формулировки
    одного перевода сливаются. В ru→de запрос русский, и одно русское слово даёт разные
    немецкие («Молния» = der Blitz и der Reißverschluss) — там требуем совпадения обеих
    сторон, иначе слияние потеряло бы слово."""
    by_word: dict[tuple, list[dict]] = defaultdict(list)
    for row in rows:
        if not _dictionary_text_is_single_word(row["source_text"]):
            continue
        if row["source_lang"] == "de":
            key = (row["source_lang"], row["target_lang"], row["headword"], "")
        else:
            if not _dictionary_text_is_single_word(row["target_text"]):
                continue
            key = (row["source_lang"], row["target_lang"], row["headword"], row["target_headword"])
        by_word[key].append(row)

    groups: list[list[dict]] = []
    for members in by_word.values():
        if len(members) < 2:
            continue
        articles = {row["article"] for row in members if row["article"]}
        if len(articles) > 1:
            # омонимы разного рода: сливаем каждый род отдельно, безродные не трогаем
            for article in articles:
                same = [row for row in members if row["article"] == article]
                if len(same) > 1:
                    groups.append(same)
            continue
        groups.append(members)
    return groups


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="записать изменения")
    parser.add_argument("--undo", action="store_true", help="снять merged_into со всех строк")
    parser.add_argument("--limit-samples", type=int, default=15)
    args = parser.parse_args()

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            if args.undo:
                cursor.execute(
                    """
                    UPDATE bt_3_dictionary_entries
                    SET response_json = response_json - 'merged_into'
                    WHERE COALESCE(response_json->>'merged_into', '') <> ''
                    """
                )
                print(f"Снято merged_into: {cursor.rowcount}")
                conn.commit()
                return 0

            rows = load_rows(cursor)
            groups = build_groups(rows)

            merged_rows = 0
            repointed = 0
            collisions = 0
            samples = []
            for members in groups:
                winner = max(
                    members,
                    key=lambda row: (
                        card_score(row["payload"]) + headline_penalty(row["target_text"]),
                        -row["id"],
                    ),
                )
                losers = [row for row in members if row["id"] != winner["id"]]
                if not losers:
                    continue
                merged_rows += len(losers)
                if len(samples) < args.limit_samples:
                    samples.append((winner, losers))
                if not args.apply:
                    continue
                for loser in losers:
                    cursor.execute(
                        """
                        UPDATE bt_3_dictionary_entries
                        SET response_json = COALESCE(response_json, '{}'::jsonb)
                                            || jsonb_build_object('merged_into', %s::text),
                            updated_at = NOW()
                        WHERE id = %s
                        """,
                        (str(winner["id"]), loser["id"]),
                    )
                    # личные карточки переводим на победителя, но только если у человека
                    # ещё нет своей записи на нём — иначе уникальный индекс (user, canonical)
                    # и, главное, его тренировка пострадали бы
                    cursor.execute(
                        """
                        UPDATE bt_3_webapp_dictionary_queries q
                        SET canonical_entry_id = %s
                        WHERE q.canonical_entry_id = %s
                          AND NOT EXISTS (
                              SELECT 1 FROM bt_3_webapp_dictionary_queries other
                              WHERE other.user_id = q.user_id
                                AND other.canonical_entry_id = %s
                          )
                        """,
                        (winner["id"], loser["id"], winner["id"]),
                    )
                    repointed += cursor.rowcount
                    cursor.execute(
                        "SELECT COUNT(*) FROM bt_3_webapp_dictionary_queries WHERE canonical_entry_id = %s",
                        (loser["id"],),
                    )
                    collisions += int((cursor.fetchone() or [0])[0] or 0)

            if args.apply:
                conn.commit()

    print(f"{'ЗАПИСАНО' if args.apply else 'ПРОГОН ВХОЛОСТУЮ'}")
    print(f"  слов с несколькими карточками: {len(groups)}")
    print(f"  строк уходит в слитые:         {merged_rows}")
    if args.apply:
        print(f"  личных карточек перевешено:    {repointed}")
        print(f"  остались на старой записи:     {collisions} (у человека уже есть своя)")
    print("\n  примеры:")
    for winner, losers in samples:
        print(f"    остаётся #{winner['id']:>6} {winner['source_text']!r} → {winner['target_text']!r}")
        for loser in losers:
            print(f"       сливаем #{loser['id']:>6} {loser['source_text']!r} → {loser['target_text']!r}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
