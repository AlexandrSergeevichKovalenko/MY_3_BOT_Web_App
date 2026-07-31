# -*- coding: utf-8 -*-
"""Вернуть двуродовые пары, схлопнутые дедупликацией по написанию.

Первая версия artikel_bank_dedupe считала дублем всё, что одинаково пишется, и выбросила
половину каждой пары: der See (озеро) остался, die See (море) снята; der Kiefer (челюсть)
остался, die Kiefer (сосна) снята. Это не копии, а разные слова — артикль у них решает
смысл, и ради них в игре сделан показ перевода.

Возвращаем не всё подряд: только те пары, где двуродовость подтверждает справочник
(article_authority). Если справочник знает у слова один род, то карточка с другим
артиклем — просто ошибка, и её место в снятых.

Обе карточки пары помечаются two_gender: без этого игра спросит «der/die/das?» по
написанию, на которое честного ответа нет.

Запуск:
    python -m scripts.artikel_bank_restore_two_gender
    python -m scripts.artikel_bank_restore_two_gender --apply
"""
from __future__ import annotations

import argparse
import sys


def find_pairs() -> list[dict]:
    """Снятые сегодня карточки, у которых артикль отличается от оставшейся в игре."""
    from backend.database import get_db_connection_context
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT r.id, r.word, r.article, r.meaning_ru, r.theme_key,
                       a.id, a.article, a.meaning_ru
                FROM bt_3_article_sprint_nouns r
                JOIN bt_3_article_sprint_nouns a
                  ON lower(a.word) = lower(r.word) AND NOT a.retired
                 AND lower(a.article) <> lower(r.article)
                WHERE r.retired AND r.updated_at::date = CURRENT_DATE
                ORDER BY r.word;
            """)
            return [{"id": r[0], "word": r[1], "article": (r[2] or "").lower(),
                     "ru": r[3] or "", "theme": r[4],
                     "twin_id": r[5], "twin_article": (r[6] or "").lower(), "twin_ru": r[7] or ""}
                    for r in cur.fetchall()]


def judge(pairs: list[dict]) -> tuple[list[dict], list[dict]]:
    """→ (возвращаем, оставляем снятыми). Решает справочник, а не написание."""
    from backend.article_authority import authoritative_article
    back, keep = [], []
    for p in pairs:
        article, source = authoritative_article(p["word"], allow_network=True)
        if article is None and "родовое" in source:
            p["why"] = "справочник: двуродовое"
            back.append(p)
        else:
            p["why"] = f"справочник знает один род ({article or source}) — вторая карточка ошибочна"
            keep.append(p)
    return back, keep


def apply(back: list[dict]) -> int:
    from backend.database import get_db_connection_context
    if not back:
        return 0
    ids = [p["id"] for p in back]
    twins = [p["twin_id"] for p in back]
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE bt_3_article_sprint_nouns "
                "SET retired = FALSE, retire_reviewed = TRUE, two_gender = TRUE, updated_at = NOW() "
                "WHERE id = ANY(%s);", (ids,))
            n = int(cur.rowcount or 0)
            # Вторая половина пары тоже двуродовая — без пометки игра спросит артикль
            # по написанию, а честного ответа на такой вопрос нет.
            cur.execute(
                "UPDATE bt_3_article_sprint_nouns SET two_gender = TRUE, updated_at = NOW() "
                "WHERE id = ANY(%s) AND NOT two_gender;", (twins,))
        conn.commit()
    return n


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    pairs = find_pairs()
    print(f"схлопнутых пар: {len(pairs)}")
    back, keep = judge(pairs)
    print(f"\nвозвращаем ({len(back)}):")
    for p in back:
        print(f"  {p['article']} {p['word']:14} «{p['ru'][:34]}»   рядом с {p['twin_article']} «{p['twin_ru'][:26]}»")
    print(f"\nостаются снятыми ({len(keep)}):")
    for p in keep:
        print(f"  {p['article']} {p['word']:14} «{p['ru'][:34]}»   {p['why']}")
    if not args.apply:
        print("\nэто был разбор без изменений. Применить: --apply")
        return 0
    print(f"\nвернули карточек: {apply(back)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
