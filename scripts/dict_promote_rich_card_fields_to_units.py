# -*- coding: utf-8 -*-
"""Поднять на ОБЩЕЕ СЛОВО те поля, которые у личной копии богаче.

Зачем
─────
После перевода читателей на общее слово копии в личных карточках сняли там, где показ
не менялся: 4 634 карточки. Осталось 9 881 — у них копия БОГАЧЕ слова, и снять её
значило бы показать человеку меньше.

Это не долг, а очередь: содержимое написано и оплачено, оно просто лежит не в том
слое. Поднимаем его на слово — и копия становится не нужна, а заодно то же самое
видят все, кто это слово учит.

Замер 15.08.2026: словам есть что добавить у 2 548 слов. Чего не хватает чаще всего:
значения (могут дать 3 895 карточек), примеры (692), устойчивые сочетания (175).

Правила
───────
• Поле берётся, только если на слове его НЕТ или оно беднее (то же сравнение, что и
  при показе: _block_is_more_structured плюс длина списка).
• Из нескольких копий одного слова берётся самая полная — по каждому полю отдельно.
• Заголовок карточки должен быть про это слово (справочник написаний); иначе слово
  получит содержимое чужого. Пропущено таких: 79.
• Значения чистятся теми же стражами, что стоят на приёмке: свалки с номерами
  разрезаются, повторы отсеиваются. В общее слово мусор не кладём.
• Разбор слова целиком сохраняется в bt_3_unit_card_backup — откат возможен.

    python3 scripts/dict_promote_rich_card_fields_to_units.py            # сухой прогон
    python3 scripts/dict_promote_rich_card_fields_to_units.py --apply    # записать
"""
from __future__ import annotations

import argparse
import collections
import json
import os
import sys

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SKIP_BILLING_LEDGER_WRITES", "1")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import backend.database as db                                     # noqa: E402
from backend.lex_units import split_numbered_senses               # noqa: E402


def block_size(value) -> int:
    if isinstance(value, (list, dict)):
        return len(value)
    return len(str(value or ""))


def is_richer(mine, theirs) -> bool:
    """Стоит ли поднимать МОЁ поле на слово, где лежит ИХ."""
    if not db._card_block_is_filled(mine):
        return False
    if not db._card_block_is_filled(theirs):
        return True
    if db._block_is_more_structured(mine, theirs):
        return True
    if db._block_is_more_structured(theirs, mine):
        return False
    if isinstance(mine, list) and isinstance(theirs, list):
        return len(mine) > len(theirs)
    return False


def clean_for_the_shared_word(field: str, value):
    """Те же стражи, что на приёмке: свалки разрезаны, повторы сняты."""
    if field not in ("dictionary_senses", "translations") or not isinstance(value, list):
        return value
    unpacked = []
    for item in value:
        text = item.get("value") if isinstance(item, dict) else item
        pieces = split_numbered_senses(str(text or ""))
        for piece in pieces:
            if isinstance(item, dict):
                unpacked.append({**item, "value": piece})
            else:
                unpacked.append(piece)
    cleaned = db.dedupe_card_meanings({field: unpacked}).get(field) or []
    return cleaned


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--limit", type=int, default=0, help="ограничить число слов")
    args = parser.parse_args()

    with db.get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT q.id, q.word_de, q.response_json, u.id, u.card, u.lemma_key,
                       COALESCE(""" + db.UNIT_OWNS_CARD_SURFACE_SQL.format(q="q", u="u") + """, FALSE)
                FROM bt_3_webapp_dictionary_queries q
                JOIN bt_3_lex_units u ON u.id = q.lex_unit_id
                WHERE u.card IS NOT NULL AND (q.response_json ?| %s)
                ORDER BY q.id;
                """,
                (list(db.CARD_CONTENT_KEYS),),
            )
            rows = cur.fetchall()

    # unit_id -> {поле: (размер, значение, номер карточки)}
    best: dict[int, dict] = collections.defaultdict(dict)
    unit_cards: dict[int, dict] = {}
    skipped_wrong_word = 0
    for entry_id, word, payload, unit_id, unit_card, lemma, surface in rows:
        if not db.unit_card_is_about_the_same_word_sql(
            unit_lemma_key=lemma, card_word=word, surface_confirms=bool(surface)
        ):
            skipped_wrong_word += 1
            continue
        card = db._coerce_json_object(payload)
        unit = db._coerce_json_object(unit_card)
        unit_cards[int(unit_id)] = unit
        for field in db.CARD_CONTENT_KEYS:
            mine = clean_for_the_shared_word(field, card.get(field))
            if not is_richer(mine, unit.get(field)):
                continue
            size = block_size(mine)
            current = best[int(unit_id)].get(field)
            if not current or size > current[0]:
                best[int(unit_id)][field] = (size, mine, entry_id)

    targets = sorted(best.items())
    if args.limit:
        targets = targets[: args.limit]

    field_counts = collections.Counter()
    for _unit_id, gained in targets:
        for field in gained:
            field_counts[field] += 1

    print("карточек с копией и живым словом: %d" % len(rows))
    print("  заголовок не про это слово — пропуск: %d" % skipped_wrong_word)
    print("  СЛОВ получат содержимое:             %d" % len(targets))
    print()
    print("какие поля добавятся (в скольких словах):")
    for field, n in field_counts.most_common():
        print("   %-24s %5d" % (field, n))
    print()
    for unit_id, gained in targets[:10]:
        print("   слово %-8s ← %s" % (unit_id, ", ".join(sorted(gained))))

    if not args.apply:
        print("\nСУХОЙ ПРОГОН. Ничего не записано. Для записи добавь --apply.")
        return

    written = 0
    with db.get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS bt_3_unit_card_backup (
                    id         BIGSERIAL PRIMARY KEY,
                    saved_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    unit_id    BIGINT NOT NULL,
                    card       JSONB,
                    reason     TEXT
                );
                """
            )
            for unit_id, gained in targets:
                merged = dict(unit_cards.get(unit_id) or {})
                for field, (_size, value, _entry_id) in gained.items():
                    merged[field] = value
                cur.execute(
                    "INSERT INTO bt_3_unit_card_backup (unit_id, card, reason) "
                    "SELECT id, card, %s FROM bt_3_lex_units WHERE id = %s;",
                    ("до подъёма богатых полей с карточек, 15.08.2026", unit_id),
                )
                cur.execute(
                    "UPDATE bt_3_lex_units SET card = %s::jsonb, updated_at = NOW() WHERE id = %s;",
                    (json.dumps(merged, ensure_ascii=False), unit_id),
                )
                written += cur.rowcount
        conn.commit()
    print("\nСлов обновлено: %d" % written)
    print("Прежние разборы сохранены в bt_3_unit_card_backup — откат возможен.")


if __name__ == "__main__":
    main()
