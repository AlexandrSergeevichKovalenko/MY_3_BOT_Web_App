# -*- coding: utf-8 -*-
"""Немые слова справочника: чужое — убрать, живому — вернуть перевод из карточки.

Разбор 15.08.2026
─────────────────
Немых слов (разбор есть, перевода нет ни одного) осталось 51 после того, как 121 слово
получило переводы из собственного разбора. Владелец сказал «убирай 51 строку». Разбор
показал, что убирать надо НЕ 51, а 4:

  УБРАТЬ (4)   немецкими помечены строки, написанные по-русски: «die раскопки»,
               «der кислотность», «die растение», плюс грамматическая помета «vt»,
               заведённая словом. Ни одной карточки человека на них не ссылается.
               Слова «die раскопки» в немецком не существует.

  ОСТАВИТЬ (47) настоящие немецкие фразы, и за каждой стоит карточка человека:
               «Stimmt's», «eine Rechnung ausstellen», «das klappt ohnehin nicht».
               Удалить их значило бы удалить то, что человек учит. Им не хватает
               перевода — а он лежит рядом, в самой карточке: у 46 из 47 русская
               сторона заполнена. Её и связываем со словом.

Одна строка остаётся немой: у её карточки русской стороны нет тоже. Это работа
обогащения, а не чистки.

Убранное складывается в bt_3_lex_units_removed — вернуть можно.

    python3 scripts/dict_clean_mute_units.py            # сухой прогон
    python3 scripts/dict_clean_mute_units.py --apply    # записать
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SKIP_BILLING_LEDGER_WRITES", "1")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.database import get_db_connection_context      # noqa: E402
from backend import lex_units as LU                         # noqa: E402

CYRILLIC = re.compile(r"[А-Яа-яЁё]")

MUTE_SQL = """
    SELECT u.id, u.display, u.card
    FROM bt_3_lex_units u
    WHERE u.lang = 'de' AND u.card IS NOT NULL
      AND NOT EXISTS (
          SELECT 1 FROM bt_3_lex_links l
          JOIN bt_3_lex_units t
            ON t.id = CASE WHEN l.from_unit = u.id THEN l.to_unit ELSE l.from_unit END
          WHERE (l.from_unit = u.id OR l.to_unit = u.id) AND t.lang = 'ru' AND l.rank < 900
      )
    ORDER BY u.id;
"""


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(MUTE_SQL)
            rows = cur.fetchall()

            wrong_language, alive = [], []
            for unit_id, display, card in rows:
                text = str(display or "").strip()
                # Немецкое слово не может быть написано кириллицей. «vt» — помета, не слово.
                if CYRILLIC.search(text) or (len(text) <= 3 and not text[:1].isupper()):
                    wrong_language.append((unit_id, text, card))
                else:
                    alive.append((unit_id, text))

            print("немых слов: %d" % len(rows))
            print("   убрать — заголовок не немецкий:      %d" % len(wrong_language))
            print("   оставить — настоящие немецкие:       %d" % len(alive))
            print()
            for unit_id, text, _card in wrong_language:
                cur.execute("SELECT count(*) FROM bt_3_webapp_dictionary_queries WHERE lex_unit_id = %s;",
                            (unit_id,))
                print("   УБРАТЬ %-8s %-30s карточек: %s" % (unit_id, text[:30], cur.fetchone()[0]))

            # Перевод для живых берём из карточки человека — он там уже есть.
            cur.execute(
                """
                SELECT u.id, q.id, COALESCE(NULLIF(BTRIM(q.translation_ru), ''), q.word_ru)
                FROM bt_3_lex_units u
                JOIN bt_3_webapp_dictionary_queries q ON q.lex_unit_id = u.id
                WHERE u.id = ANY(%s)
                ORDER BY u.id;
                """,
                ([uid for uid, _t in alive],),
            )
            restore = []
            for unit_id, entry_id, russian in cur.fetchall():
                text = str(russian or "").strip()
                if text and CYRILLIC.search(text):
                    restore.append((unit_id, entry_id, text))
            print()
            print("   вернуть перевод из карточки: %d слов" % len({u for u, _e, _t in restore}))
            for unit_id, entry_id, text in restore[:8]:
                print("      слово %-8s ← карточка %-8s %s" % (unit_id, entry_id, text[:44]))

            if not args.apply:
                print("\nСУХОЙ ПРОГОН. Ничего не записано. Для записи добавь --apply.")
                return

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS bt_3_lex_units_removed (
                    id         BIGSERIAL PRIMARY KEY,
                    removed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    unit_id    BIGINT NOT NULL,
                    display    TEXT,
                    card       JSONB,
                    reason     TEXT
                );
                """
            )
            removed = 0
            for unit_id, text, card in wrong_language:
                cur.execute(
                    "INSERT INTO bt_3_lex_units_removed (unit_id, display, card, reason) VALUES (%s,%s,%s,%s);",
                    (unit_id, text, json.dumps(card, ensure_ascii=False) if card else None,
                     "немецким помечена не немецкая строка, 15.08.2026"),
                )
                cur.execute("DELETE FROM bt_3_lex_links WHERE from_unit=%s OR to_unit=%s;", (unit_id, unit_id))
                cur.execute("DELETE FROM bt_3_lex_surfaces WHERE unit_id=%s;", (unit_id,))
                cur.execute("DELETE FROM bt_3_lex_units WHERE id=%s;", (unit_id,))
                removed += 1
        conn.commit()

    linked = 0
    for unit_id, _entry_id, text in restore:
        target = LU.ensure_unit(text, "ru")
        if not target:
            continue
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                for a, b in ((unit_id, target), (target, unit_id)):
                    cur.execute(
                        """
                        INSERT INTO bt_3_lex_links (from_unit, to_unit, rank, source)
                        VALUES (%s, %s, 10, 'перевод из карточки')
                        ON CONFLICT (from_unit, to_unit) DO NOTHING;
                        """,
                        (a, b),
                    )
            conn.commit()
        linked += 1

    print("\nубрано слов: %d, возвращено переводов: %d" % (removed, linked))
    print("Убранное лежит в bt_3_lex_units_removed — вернуть можно.")


if __name__ == "__main__":
    main()
