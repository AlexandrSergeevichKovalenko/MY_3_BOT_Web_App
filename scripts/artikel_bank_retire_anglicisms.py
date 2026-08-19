# -*- coding: utf-8 -*-
"""Снять с ротации англицизмы, которых нет в ходовом немецком, — по всему банку.

ПОВОД. 19.08.2026 владелец сыграл спринт по теме «Computer & Geräte» и получил
экран из Upload, Backup, Controller, Export, Tab. Разбор нашёл три разные беды,
и их нельзя лечить одним движением:

  1. Слова НЕ выдуманы: der Export, der Alarm, der Controller, der Upload, der Hack
     есть в de.wiktionary, род совпадает. Выдуманы были только «die Sync» и
     «der SMS-Ton» — их чинит `artikel_bank_drop_unsourced.py`.
  2. Тема на треть состояла из английских слов. Учить на них немецкий род нечему:
     он держится на договорённости и часто спорен (der/das Tab).
  3. Игрок увидел 77 слов из 80 живых — то есть ВЕСЬ банк темы разом, вместе с
     хвостом. Это чинится в `article_sprint_sets` (порог темы дня), не здесь.

ЧТО ДЕЛАЕТ ЭТОТ СКРИПТ. Проходит весь живой банк, спрашивает у справочников
происхождение каждого слова (`backend/article_anglicism.py`: de.wiktionary Herkunft,
затем en.wiktionary про направление заимствования) и снимает с ротации те, что
одновременно:
    • названы справочником заимствованием из английского, И
    • лежат за 20 000 в частотном списке `bt_3_word_frequency` (или их там нет).

Граница 20 000 — решение владельца 19.08.2026. Замер того же дня: всё, что он
обвёл на экране, лежит на 24 119 (Account) — 46 639 (Upload); всё, что трогать
нельзя, — в первых 20 000 (der Bus 1603, der Film 673, der Sport 3244).

ЧЕГО СКРИПТ НЕ ДЕЛАЕТ. Не удаляет — только `retired = TRUE` (решение владельца
31.07.2026: ничего не удалять). Не трогает слова, о происхождении которых оба
справочника молчат: недоказанное не равно доказанному, такие СЧИТАЮТСЯ и попадают
в отчёт отдельной строкой. Не решает за владельца — список печатается целиком.

Слова заносятся в стоп-лист (`blacklist_article_words`), иначе ночной добор вернёт
их следующей же ночью.

Запуск:
    python -m scripts.artikel_bank_retire_anglicisms                 # отчёт
    python -m scripts.artikel_bank_retire_anglicisms --apply
    python -m scripts.artikel_bank_retire_anglicisms --theme computer_geraete
"""
from __future__ import annotations

import argparse
import sys


def live_rows(theme: str | None) -> list[dict]:
    """Живые слова банка: проверенные и не снятые."""
    from backend.database import get_db_connection_context
    sql = ("SELECT id, theme_key, word, article, meaning_ru FROM bt_3_article_sprint_nouns "
           "WHERE retired = FALSE AND verified = TRUE")
    params: list = []
    if theme:
        sql += " AND theme_key = %s"
        params.append(theme)
    sql += " ORDER BY theme_key, word"
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return [{"id": r[0], "theme_key": r[1], "word": r[2],
                     "article": r[3], "meaning_ru": r[4]} for r in cur.fetchall()]


def retire(ids: list[int], reason: str) -> int:
    """Снять с ротации по id — по СТРОКЕ, не по написанию.

    По написанию снимать нельзя: у двуродовых слов (der See / die See) одно
    написание держит две разные карточки, и снятие по слову убивает обе.
    На этом уже наступали 31.07.2026."""
    if not ids:
        return 0
    from backend.database import get_db_connection_context
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE bt_3_article_sprint_nouns "
                "SET retired = TRUE, retire_reason = %s, retire_reviewed = TRUE, updated_at = NOW() "
                "WHERE id = ANY(%s) AND retired = FALSE",
                (reason[:200], ids),
            )
            changed = cur.rowcount
        conn.commit()
    return int(changed or 0)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="применить (без него — только отчёт)")
    parser.add_argument("--theme", default="", help="только одна тема")
    args = parser.parse_args()

    from backend.article_anglicism import origin_of, everyday_ranks, ANGLICISM, UNKNOWN, TAIL_RANK

    rows = live_rows(args.theme.strip() or None)
    words = sorted({r["word"] for r in rows})
    print(f"живых слов в банке: {len(rows)} (уникальных написаний {len(words)})")
    print(f"спрашиваю справочники о происхождении… граница ходового языка: {TAIL_RANK}")

    origins = origin_of(words)
    ranks = everyday_ranks(words)

    to_retire: list[dict] = []
    unknown: list[str] = []
    for row in rows:
        verdict, basis = origins.get(row["word"], (UNKNOWN, "слово не проверялось"))
        if verdict == UNKNOWN:
            unknown.append(row["word"])
            continue
        if verdict != ANGLICISM:
            continue
        rank = ranks.get(row["word"])
        if rank is not None and rank <= TAIL_RANK:
            continue                      # англицизм, но ходовой — остаётся в игре
        to_retire.append({**row, "rank": rank, "basis": basis})

    print(f"\n=== К СНЯТИЮ: {len(to_retire)} ===")
    for item in to_retire:
        rank = item["rank"] if item["rank"] is not None else "нет в списке"
        print(f"  {item['theme_key']:22s} {item['article']} {item['word']:18s} "
              f"{(item['meaning_ru'] or '')[:26]:28s} ранг={rank}")

    # «Не знаем» — это ЧИСЛО в отчёте, а не тишина. Такие слова остаются в игре.
    print(f"\n=== происхождение неизвестно (остаются в игре, задача на источник): "
          f"{len(set(unknown))} ===")

    if not args.apply:
        print("\n(отчёт; чтобы применить — --apply)")
        return 0

    changed = retire([i["id"] for i in to_retire], "англицизм вне ходового языка")
    from backend.database import blacklist_article_words
    banned = blacklist_article_words(
        [(i["word"], "англицизм вне ходового языка", i["theme_key"]) for i in to_retire]
    )
    print(f"\nснято с ротации: {changed}; занесено в стоп-лист: {banned}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
