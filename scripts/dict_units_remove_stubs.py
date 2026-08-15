# -*- coding: utf-8 -*-
"""Убрать из словаря обрубки — записи, которые словами не являются.

Решение владельца 14.08.2026. Это остатки старых сбоев: «Auftre» (от Auftreten),
«Aufrechtha», «Erwe», «Ic», «Lukra», «Umgekehr», плюс «C» и «CSDs». Они лежат в общем
словаре, занимают написания и могут вылезти в поиске.

Перед удалением снимается ПОЛНЫЙ снимок в bt_3_lex_units_removed: сама единица, её
написания, связи-переводы и значения. Без следа не удаляем ничего — правило появилось
после разбора ночной чистки дублей, которая два месяца стирала карточки людей, не
записывая, что именно исчезло.

Личные карточки НЕ страдают: внешний ключ fk_webapp_dictionary_lex_unit стоит на
SET NULL (проверено в pg_constraint), то есть карточка человека остаётся, у неё лишь
отвязывается ссылка на единицу.

    python3 scripts/dict_units_remove_stubs.py            # сухой прогон
    python3 scripts/dict_units_remove_stubs.py --apply    # удалить
"""
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.database import get_db_connection_context  # noqa: E402

# Поимённо, по решению владельца. Список НЕ вычисляется правилом: обрубок от настоящего
# редкого слова отличает человек, а не регулярка.
STUB_UNIT_IDS = [41251, 41337, 41506, 41490, 41329, 41530, 41526, 23390]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="удалить (без него — сухой прогон)")
    args = parser.parse_args()

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT u.id, u.display, u.lang, u.kind,
                       (SELECT count(*) FROM bt_3_lex_surfaces s WHERE s.unit_id = u.id),
                       (SELECT count(*) FROM bt_3_lex_links l WHERE l.from_unit = u.id OR l.to_unit = u.id),
                       (SELECT count(*) FROM bt_3_webapp_dictionary_queries q WHERE q.lex_unit_id = u.id)
                FROM bt_3_lex_units u WHERE u.id = ANY(%s) ORDER BY u.display;
                """,
                (STUB_UNIT_IDS,),
            )
            rows = cur.fetchall()

        print("найдено единиц: %d" % len(rows))
        print("%-7s %-16s %-5s %-8s %-8s %-8s" % ("id", "слово", "язык", "написан", "связей", "личных"))
        for r in rows:
            print("%-7s %-16s %-5s %-8s %-8s %-8s" % (r[0], r[1][:16], r[2], r[4], r[5], r[6]))

        if not args.apply:
            print("\nСУХОЙ ПРОГОН. Ничего не удалено. Для удаления добавь --apply.")
            return

        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS bt_3_lex_units_removed (
                    id BIGSERIAL PRIMARY KEY,
                    removed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    reason TEXT,
                    unit_id BIGINT,
                    lang TEXT, kind TEXT, lemma TEXT, lemma_key TEXT,
                    pos TEXT, gender TEXT, display TEXT, card JSONB,
                    surfaces JSONB, links JSONB, senses JSONB
                );
                """
            )
            cur.execute(
                """
                INSERT INTO bt_3_lex_units_removed (
                    reason, unit_id, lang, kind, lemma, lemma_key, pos, gender, display, card,
                    surfaces, links, senses
                )
                SELECT 'обрубок, решение владельца 14.08.2026',
                       u.id, u.lang, u.kind, u.lemma, u.lemma_key, u.pos, u.gender, u.display, u.card,
                       COALESCE((SELECT jsonb_agg(to_jsonb(s)) FROM bt_3_lex_surfaces s WHERE s.unit_id = u.id), '[]'::jsonb),
                       COALESCE((SELECT jsonb_agg(to_jsonb(l)) FROM bt_3_lex_links l
                                  WHERE l.from_unit = u.id OR l.to_unit = u.id), '[]'::jsonb),
                       COALESCE((SELECT jsonb_agg(to_jsonb(x)) FROM bt_3_lex_senses x WHERE x.unit_id = u.id), '[]'::jsonb)
                FROM bt_3_lex_units u WHERE u.id = ANY(%s);
                """,
                (STUB_UNIT_IDS,),
            )
            saved = cur.rowcount
            cur.execute("DELETE FROM bt_3_lex_units WHERE id = ANY(%s);", (STUB_UNIT_IDS,))
            removed = cur.rowcount
        conn.commit()
        print("\nснимков сохранено: %d, единиц удалено: %d" % (saved, removed))


if __name__ == "__main__":
    main()
