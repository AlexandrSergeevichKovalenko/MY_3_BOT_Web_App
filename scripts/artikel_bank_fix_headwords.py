# -*- coding: utf-8 -*-
"""Заголовки банка артиклей: убрать формы слов и отклеить артикль от слова.

ПОВОД. 20.08.2026, разбор темы «Computer & Geräte» вывел на слова, о происхождении
которых справочник молчит, — и в этой куче нашёлся другой класс брака:
  • «die Die Feier» и «die Die Fete» — АРТИКЛЬ ВКЛЕЕН В ЗАГОЛОВОК. Карточка
    показывает ответ в самом вопросе. Обе добавлены через `/artikel_addwords`,
    то есть дыра в двери ручного добавления;
  • «die Bänder» (это мн. от das Band), «die Sorten» (от die Sorte) — ФОРМА СЛОВА
    вместо словарной формы. У множественного артикль всегда die, знать нечего,
    а человек видит «Band» и «Bänder» как два разных слова.

ИСТОЧНИК. `backend/article_headword.py`: de.wiktionary заводит формам словоизменения
отдельные страницы с пометкой `{{Wortart|Deklinierte Form}}` и ссылкой на исходное
слово. Это прямое утверждение справочника, а не признак по написанию. Законные
pluralia tantum (die Eltern, die Kosten, die Leute, die Ferien) имеют свою статью
`{{Wortart|Substantiv}}` и НЕ задеваются — проверено 20.08.2026.

ЧТО ДЕЛАЕТ СКРИПТ:
  • артикль вклеен → заголовок чинится (Die Feier → Feier), артикль строки
    сверяется со справочником; если такое слово в теме уже есть — строка снимается
    как дубль, чтобы не плодить вторую карточку;
  • форма слова → строка снимается с показа с причиной и ссылкой на исходное слово.
    Не удаляется: решение владельца 31.07.2026 ничего не удалять;
  • справочник промолчал → НЕ ТРОГАЕМ, считаем и печатаем числом.

Запуск:
    python -m scripts.artikel_bank_fix_headwords            # отчёт
    python -m scripts.artikel_bank_fix_headwords --apply
"""
from __future__ import annotations

import argparse
import sys


def live_rows() -> list[dict]:
    from backend.database import get_db_connection_context
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, theme_key, word, article, meaning_ru FROM bt_3_article_sprint_nouns "
                "WHERE retired = FALSE AND verified = TRUE ORDER BY theme_key, word;")
            return [{"id": r[0], "theme": r[1], "word": r[2],
                     "article": r[3], "meaning_ru": r[4]} for r in cur.fetchall()]


def retire(row_id: int, reason: str) -> None:
    from backend.database import get_db_connection_context
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE bt_3_article_sprint_nouns SET retired = TRUE, retire_reason = %s, "
                "retire_reviewed = TRUE, updated_at = NOW() WHERE id = %s;",
                (reason[:200], row_id))
        conn.commit()


def fix_glued(row: dict, clean: str) -> str:
    """Отклеить артикль. Артикль строки берём у справочника, а не у приклеенного слова.

    Приклеенный артикль — это то, что написал человек, а не то, что говорит источник:
    доверять ему нельзя ровно по тому же правилу, по которому мы не верим модели."""
    from backend.database import get_db_connection_context
    from backend.article_authority import authoritative_article
    verdict, basis = authoritative_article(clean, allow_network=True)
    if not verdict:
        retire(row["id"], f"артикль в заголовке, а род «{clean}» справочник не знает")
        return f"снято: род «{clean}» не подтверждён ({basis})"
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM bt_3_article_sprint_nouns WHERE theme_key = %s "
                "AND lower(word) = lower(%s) AND article = %s AND id <> %s;",
                (row["theme"], clean, verdict, row["id"]))
            twin = cur.fetchone()
            if twin:
                cur.execute(
                    "UPDATE bt_3_article_sprint_nouns SET retired = TRUE, "
                    "retire_reason = 'артикль в заголовке; слово уже есть в теме', "
                    "retire_reviewed = TRUE, updated_at = NOW() WHERE id = %s;", (row["id"],))
                conn.commit()
                return f"снято как дубль: «{clean}» в теме уже есть"
            cur.execute(
                "UPDATE bt_3_article_sprint_nouns SET word = %s, article = %s, source = %s, "
                "    mnemonic_ru = '', mnemonic_method = '', mnemonic_head = '', "
                "    audio_object_key = '', image_object_key = '', image_checked = FALSE, "
                "    updated_at = NOW() WHERE id = %s;",
                (clean, verdict, basis, row["id"]))
        conn.commit()
    # Мнемонику, картинку и озвучку стираем: их делали для «Die Feier», то есть
    # для другого написания. Пересоберутся штатными ночными задачами.
    return f"починено: «{row['word']}» → «{verdict} {clean}» ({basis})"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--limit", type=int, default=0, help="только первые N слов (для пробы)")
    args = parser.parse_args()

    from backend.article_headword import (
        headword_verdicts, DECLINED, GLUED_ARTICLE, UNKNOWN, LEMMA)

    rows = live_rows()
    if args.limit:
        rows = rows[:args.limit]
    words = sorted({r["word"] for r in rows})
    print(f"живых карточек: {len(rows)} (уникальных заголовков {len(words)})")
    print("спрашиваю de.wiktionary про каждый заголовок…\n")

    verdicts = headword_verdicts(words)
    counts = {LEMMA: 0, DECLINED: 0, GLUED_ARTICLE: 0, UNKNOWN: 0}
    glued: list[tuple[dict, str]] = []
    declined: list[tuple[dict, str]] = []
    for row in rows:
        verdict, lemma = verdicts.get(row["word"], (UNKNOWN, ""))
        counts[verdict] = counts.get(verdict, 0) + 1
        if verdict == GLUED_ARTICLE:
            glued.append((row, lemma))
        elif verdict == DECLINED:
            declined.append((row, lemma))

    print(f"=== АРТИКЛЬ ВКЛЕЕН В ЗАГОЛОВОК: {len(glued)} ===")
    for row, clean in glued:
        print(f"  {row['theme']:22s} «{row['article']} {row['word']}» → «{clean}»")
    print(f"\n=== ФОРМА СЛОВА ВМЕСТО СЛОВАРНОЙ: {len(declined)} ===")
    for row, lemma in declined:
        print(f"  {row['theme']:22s} {row['article']} {row['word']:20s} — форма от «{lemma or '?'}»")
    print(f"\n=== справочник промолчал (НЕ трогаем): {counts.get(UNKNOWN, 0)} ===")
    print(f"=== в порядке: {counts.get(LEMMA, 0)} ===")

    if not args.apply:
        print("\n(отчёт; чтобы применить — --apply)")
        return 0

    for row, clean in glued:
        print("  " + fix_glued(row, clean))
    for row, lemma in declined:
        retire(row["id"], f"форма слова, а не словарная форма (от «{lemma}»)" if lemma
               else "форма слова, а не словарная форма")
    print(f"\nотклеено артиклей: {len(glued)}; снято форм слова: {len(declined)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
