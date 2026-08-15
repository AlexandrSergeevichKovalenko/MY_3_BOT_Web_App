# -*- coding: utf-8 -*-
"""Вернуть перевод, который владелец выбрал руками на экране спорных фраз.

Что случилось
─────────────
На экране разбора спорных фраз у каждого варианта показывались ОБЕ половины —
немецкая и русская (frontend/src/answer/PhraseReviewScreen.jsx:349). Владелец
нажимал на кнопку, где написан и перевод. А сохранение забирало из выбранного
варианта только немецкое: `new_text = variants[idx]["text"]` — поле "ru" не читалось
нигде во всём файле. Русский после этого заново придумывала модель в
rebuild_unit_breakdown и вставала ГЛАВНЫМ переводом, а выбор владельца оставался
вторым или пропадал.

Причина закрыта 14.08.2026 в backend/database.py (apply_phrase_review_decision берёт
обе половины, rebuild_unit_breakdown ставит перевод владельца первым). Этот скрипт
разбирает НАКОПЛЕННОЕ.

Как восстанавливается выбор
───────────────────────────
Запись разбора не хранит номер выбранного варианта. Но выбранный НЕМЕЦКИЙ стал
заголовком слова — по нему находится вариант судьи, а в нём лежит русская половина.
Это не догадка: сверяется точное совпадение текста.

Ничего не удаляет. Только поднимает перевод владельца на первое место.

    python3 scripts/phrase_review_restore_owner_translation.py            # сухой прогон
    python3 scripts/phrase_review_restore_owner_translation.py --apply    # записать
"""
from __future__ import annotations

import argparse
import os
import re
import sys

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SKIP_BILLING_LEDGER_WRITES", "1")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.database import get_db_connection_context, phrase_review_variants  # noqa: E402

SPACE_RE = re.compile(r"\s+")


def norm(text) -> str:
    return SPACE_RE.sub(" ", str(text or "").strip()).casefold()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    restore: list[tuple[int, str, str, str, str]] = []
    already_ok = 0
    no_match = 0
    no_ru = 0

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT r.id, r.unit_id, r.text, r.translation, r.judges, r.arbiter, u.lemma
                FROM bt_3_phrase_review r
                JOIN bt_3_lex_units u ON u.id = r.unit_id
                WHERE r.status IN ('accepted', 'replaced')
                ORDER BY r.id;
                """
            )
            rows = cur.fetchall()

            for review_id, unit_id, old_text, _translation, judges, arbiter, lemma in rows:
                variants = phrase_review_variants(judges, old_text, arbiter)
                # Выбранный владельцем вариант — тот, чей немецкий стал заголовком слова.
                chosen = next((v for v in variants if norm(v.get("text")) == norm(lemma)), None)
                if not chosen:
                    no_match += 1
                    continue
                owner_ru = str(chosen.get("ru") or "").strip()
                if not owner_ru:
                    no_ru += 1
                    continue
                # Что сейчас показывается ГЛАВНЫМ переводом.
                cur.execute(
                    """
                    SELECT t.display FROM bt_3_lex_links l
                    JOIN bt_3_lex_units t ON t.id = l.to_unit
                    WHERE l.from_unit = %s AND t.lang = 'ru' AND l.rank < 900
                    ORDER BY l.rank, t.id LIMIT 1;
                    """,
                    (int(unit_id),),
                )
                row = cur.fetchone()
                current_main = str(row[0] or "").strip() if row else ""
                if norm(current_main) == norm(owner_ru):
                    already_ok += 1
                    continue
                restore.append((int(unit_id), str(lemma), owner_ru, current_main, str(review_id)))

    print("решений владельца разобрано: %d" % len(rows))
    print("  перевод владельца уже главный:            %d" % already_ok)
    print("  вариант не опознан (заголовок изменился): %d" % no_match)
    print("  у выбранного варианта не было перевода:   %d" % no_ru)
    print("  НАДО ВЕРНУТЬ перевод владельца:           %d" % len(restore))
    print()
    for unit_id, lemma, owner_ru, current_main, review_id in restore:
        print("  единица %-7s %s" % (unit_id, lemma[:58]))
        print("      сейчас главный: %s" % (current_main[:70] or "— нет —"))
        print("      выбор владельца: %s" % owner_ru[:70])

    if not args.apply:
        print("\nСУХОЙ ПРОГОН. Ничего не записано. Для записи добавь --apply.")
        return

    from backend.lex_units import ensure_unit
    written = 0
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            for unit_id, _lemma, owner_ru, _current, _review_id in restore:
                owner_unit = ensure_unit(owner_ru, "ru")
                if not owner_unit:
                    continue
                cur.execute(
                    "UPDATE bt_3_lex_links SET rank = GREATEST(rank, 20) "
                    "WHERE from_unit = %s AND rank < 20;",
                    (unit_id,),
                )
                cur.execute(
                    """
                    INSERT INTO bt_3_lex_links (from_unit, to_unit, rank, source)
                    VALUES (%s, %s, 1, 'вычитка')
                    ON CONFLICT (from_unit, to_unit)
                    DO UPDATE SET rank = 1, source = 'вычитка', updated_at = NOW();
                    """,
                    (unit_id, int(owner_unit)),
                )
                written += 1
        conn.commit()
    print("\nВозвращено переводов владельца: %d" % written)


if __name__ == "__main__":
    main()
