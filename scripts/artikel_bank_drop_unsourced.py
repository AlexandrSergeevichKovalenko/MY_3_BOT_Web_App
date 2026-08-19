# -*- coding: utf-8 -*-
"""Убрать из игры слова, чей род не подтверждает НИ ОДИН справочник.

ПОВОД. 19.08.2026 в теме «Computer & Geräte» нашлась карточка «die Sync ·
синхронизация». Статьи «Sync» в de.wiktionary нет вообще — ни рода, ни слова.
Откуда взялся артикль: `authoritative_article` отдаёт вердикт из НАШЕГО ЖЕ банка
с честной пометкой источника «банк артиклей», а приёмка `fill_theme` эту пометку
не читала и принимала вердикт как подтверждение. Слово, один раз попавшее в банк
от модели, дальше подтверждало себя само на каждом прогоне.

Сам модуль про эту ловушку знал — в нём с самого начала стоит комментарий
«банк не может подтверждать сам себя» и множество `_bank_sourced`. Не хватало
ровно одного: чтобы приёмка это множество читала. С 19.08.2026 читает.

ЧТО ДЕЛАЕТ СКРИПТ. Берёт живые слова, чей род в банке держится только на самом
банке, и переспрашивает справочники ЗАНОВО, банку не веря:
    1. de.wiktionary про само слово (живой запрос, ответ оседает в кэше);
    2. правило композита (голова составного слова).
Дальше:
    • справочник подтвердил → строке проставляется настоящий источник, слово
      остаётся в игре (замер 19.08.2026: так подтвердились 11 слов из 18 —
      die Konstante, das Saatgut, die Startbahn, der Schuldschein и другие);
    • справочник назвал ДРУГОЙ род → это дефект показа, слово снимается и едет
      к владельцу: менять род молча нельзя, у половины таких слов род несёт смысл;
    • не подтверждает ничто → слово уходит из игры (`verified = FALSE`) и
      попадает владельцу в разбор с кнопками. Не удаляется.

Почему не удаляем и не «чиним» родом от модели: у «das Bauteil», «die Samba»,
«die Beschäftigte» род зависит от смысла или от пола человека — это не пробел
в справочнике, а слово, которому в игре «der/die/das?» нужен показ смысла.
Выдумать им один род значило бы научить человека неверному.

Запуск:
    python -m scripts.artikel_bank_drop_unsourced           # отчёт
    python -m scripts.artikel_bank_drop_unsourced --apply
"""
from __future__ import annotations

import argparse
import sys


def bank_only_rows() -> list[dict]:
    """Живые строки, чей род известен только из нашего банка."""
    from backend.database import get_db_connection_context
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, theme_key, word, article, meaning_ru FROM bt_3_article_sprint_nouns "
                "WHERE retired = FALSE AND verified = TRUE AND source = 'банк артиклей' "
                "ORDER BY word;"
            )
            return [{"id": r[0], "theme_key": r[1], "word": r[2],
                     "article": r[3], "meaning_ru": r[4]} for r in cur.fetchall()]


def confirm(word: str) -> tuple[str | None, str]:
    """(род, источник) от справочников — БЕЗ участия нашего банка."""
    from backend.article_authority import _wiktionary_live, compound_article
    live = _wiktionary_live(word)
    if live:
        return live, "wiktionary-live"
    head = compound_article(word)
    if head:
        return head, "правило композита"
    return None, "справочник молчит"


def apply_confirmed(row_id: int, source: str) -> None:
    from backend.database import get_db_connection_context
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE bt_3_article_sprint_nouns SET source = %s, updated_at = NOW() WHERE id = %s;",
                (source, row_id),
            )
        conn.commit()


def apply_unsourced(row_id: int, reason: str) -> None:
    """Из игры — но НЕ из базы: слово поедет владельцу в разбор."""
    from backend.database import get_db_connection_context
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE bt_3_article_sprint_nouns "
                "SET verified = FALSE, retired = TRUE, retire_reason = %s, "
                "    retire_reviewed = FALSE, updated_at = NOW() "
                "WHERE id = %s;",
                (reason[:200], row_id),
            )
        conn.commit()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    rows = bank_only_rows()
    print(f"слов, чей род держится только на нашем банке: {len(rows)}\n")
    confirmed: list[tuple[dict, str]] = []
    mismatched: list[tuple[dict, str]] = []
    unsourced: list[dict] = []
    for row in rows:
        verdict, source = confirm(row["word"])
        if verdict is None:
            unsourced.append(row)
            print(f"  ✗ {row['word']:16s} {row['article']:4s} — не подтверждает ничто")
        elif verdict == row["article"]:
            confirmed.append((row, source))
            print(f"  ✓ {row['word']:16s} {row['article']:4s} — подтверждён ({source})")
        else:
            mismatched.append((row, verdict))
            print(f"  ! {row['word']:16s} {row['article']:4s} — справочник говорит «{verdict}»")

    print(f"\nподтверждено: {len(confirmed)}; расхождение: {len(mismatched)}; "
          f"не подтверждает ничто: {len(unsourced)}")
    if not args.apply:
        print("\n(отчёт; чтобы применить — --apply)")
        return 0

    for row, source in confirmed:
        apply_confirmed(row["id"], source)
    for row, verdict in mismatched:
        apply_unsourced(row["id"], f"справочник даёт «{verdict}», в банке «{row['article']}»")
    for row in unsourced:
        apply_unsourced(row["id"], "род не подтверждает ни один справочник")
    print(f"\nисточник проставлен: {len(confirmed)}; "
          f"убрано из игры в разбор владельцу: {len(mismatched) + len(unsourced)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
