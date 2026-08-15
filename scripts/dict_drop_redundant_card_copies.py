# -*- coding: utf-8 -*-
"""Снять с личных карточек копию разбора — там, где она больше не нужна.

Зачем
─────
Содержимое слова должно жить в одном месте — на общем слове. Тогда правка доходит до
всех сразу. Но исторически при подписке разбор копировался человеку в карточку, и эта
копия перекрывала общее: замер 14.08.2026 — 4 703 карточки показывали свой перевод,
разошедшийся с общим словом, и у ВСЕХ у них слово улучшали позже (в среднем на 105 дней).

Правило безопасности
────────────────────
Снимаем копию ТОЛЬКО если доказано, что человек после этого увидит РОВНО ТО ЖЕ.
Проверка честная: собираем карточку по боевому правилу показа (merge_unit_card_for_serve)
дважды — с копией и без неё — и сравниваем результат поблочно. Отличается хоть один
блок — не трогаем.

Это отсекает разом три опасных случая, каждый из которых замерен:
  • у слова разбора нет вовсе — копия единственный источник (382 карточки);
  • у человека блок богаче, чем на слове (например заполнено прошедшее время, а на
    слове пусто — 70 карточек);
  • человек правил поле своей рукой (bt_3_user_word_overrides) — такое не трогаем никогда.

Перед снятием делается ПОЛНЫЙ снимок в bt_3_card_content_removed: вернуть можно всё.

    python3 scripts/dict_drop_redundant_card_copies.py            # сухой прогон
    python3 scripts/dict_drop_redundant_card_copies.py --apply    # записать
    python3 scripts/dict_drop_redundant_card_copies.py --limit 50 # только первые N
"""
from __future__ import annotations

import argparse
import json
import os
import sys

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SKIP_BILLING_LEDGER_WRITES", "1")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.database import (                                        # noqa: E402
    get_db_connection_context,
    merge_unit_card_for_serve,
    strip_card_content_for_subscription,
    unit_card_is_about_the_same_word,
    get_user_word_overrides,
    CARD_CONTENT_KEYS,
    _coerce_json_object,
)


def content_view(payload: dict) -> str:
    """Только содержательные блоки, в устойчивом порядке — для сравнения «до/после»."""
    return json.dumps(
        {key: payload.get(key) for key in sorted(CARD_CONTENT_KEYS) if key in payload},
        ensure_ascii=False, sort_keys=True,
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT q.id, q.user_id, q.word_de, q.response_json, u.card, u.lemma_key, u.id
                FROM bt_3_webapp_dictionary_queries q
                JOIN bt_3_lex_units u ON u.id = q.lex_unit_id
                WHERE u.card IS NOT NULL
                  AND (q.response_json ?| %s)
                ORDER BY q.id;
                """,
                (list(CARD_CONTENT_KEYS),),
            )
            rows = cur.fetchall()

    overrides = get_user_word_overrides([r[0] for r in rows])

    safe, kept_edited, kept_wrong_word, kept_would_lose = [], 0, 0, 0
    for entry_id, user_id, word_de, payload, unit_card, lemma_key, unit_id in rows:
        card = _coerce_json_object(payload)
        unit = _coerce_json_object(unit_card)
        own = overrides.get(int(entry_id))
        if own:
            kept_edited += 1
            continue
        if not unit_card_is_about_the_same_word(unit_lemma_key=lemma_key, card_word=word_de):
            kept_wrong_word += 1
            continue
        before = content_view(merge_unit_card_for_serve(card, unit, own))
        after = content_view(merge_unit_card_for_serve(
            strip_card_content_for_subscription(card), unit, own))
        if before != after:
            kept_would_lose += 1
            continue
        safe.append((entry_id, user_id, word_de))

    print("карточек с копией разбора и живым словом: %d" % len(rows))
    print("  человек правил сам — НЕ ТРОГАЕМ:                %d" % kept_edited)
    print("  заголовок разошёлся со словом — НЕ ТРОГАЕМ:     %d" % kept_wrong_word)
    print("  человек увидел бы меньше — НЕ ТРОГАЕМ:          %d" % kept_would_lose)
    print("  КОПИЮ МОЖНО СНЯТЬ БЕЗ ПОТЕРЬ:                   %d" % len(safe))
    print()
    for entry_id, user_id, word_de in safe[:15]:
        print("   карточка %-8s человек %-12s %s" % (entry_id, user_id, str(word_de)[:40]))
    if len(safe) > 15:
        print("   … и ещё %d" % (len(safe) - 15))

    if not args.apply:
        print("\nСУХОЙ ПРОГОН. Ничего не записано. Для записи добавь --apply.")
        return

    targets = [x[0] for x in safe]
    if args.limit:
        targets = targets[: args.limit]

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS bt_3_card_content_removed (
                    id         BIGSERIAL PRIMARY KEY,
                    removed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    entry_id   BIGINT NOT NULL,
                    user_id    BIGINT NOT NULL,
                    word_de    TEXT,
                    response_json JSONB NOT NULL,
                    reason     TEXT
                );
                """
            )
            cur.execute(
                """
                INSERT INTO bt_3_card_content_removed (entry_id, user_id, word_de, response_json, reason)
                SELECT id, user_id, word_de, response_json,
                       'копия снята: показ не изменился, 14.08.2026'
                FROM bt_3_webapp_dictionary_queries WHERE id = ANY(%s);
                """,
                (targets,),
            )
            saved = cur.rowcount
            for entry_id in targets:
                cur.execute(
                    "SELECT response_json FROM bt_3_webapp_dictionary_queries WHERE id = %s;",
                    (entry_id,),
                )
                row = cur.fetchone()
                light = strip_card_content_for_subscription(_coerce_json_object(row[0]))
                cur.execute(
                    "UPDATE bt_3_webapp_dictionary_queries "
                    "SET response_json = %s::jsonb, updated_at = NOW() WHERE id = %s;",
                    (json.dumps(light, ensure_ascii=False), entry_id),
                )
        conn.commit()
    print("\nснимков сохранено: %d, копий снято: %d" % (saved, len(targets)))


if __name__ == "__main__":
    main()
