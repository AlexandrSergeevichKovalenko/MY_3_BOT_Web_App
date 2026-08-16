# -*- coding: utf-8 -*-
"""Прогнать ПРОДУКТОВЫЕ правила заголовка по всему, что уже лежит в базе.

Зачем отдельный скрипт
──────────────────────
Правила заголовка теперь стоят на двери записи (backend/database.py,
`_save_webapp_dictionary_query_returning_id_with_conn`):

    german_dictionary_headword     zu-инфинитив и неопределённый артикль
    _fix_plural_article_on_headword  «der Handschuhe» → «die Handschuhe»

Дверь закрывает будущее. Этот скрипт закрывает прошлое — и зовёт РОВНО ТЕ ЖЕ функции,
а не свою копию правила. Копия разошлась бы с продуктом через неделю, и уборка начала
бы чинить не то, что стережёт дверь.

Проходим все три хранилища словаря:
    bt_3_lex_units                 display (заголовок слова)
    bt_3_webapp_dictionary_queries word_de, translation_de
    bt_3_dictionary_entries        source_text

    python3 scripts/dict_apply_headword_rules_to_stored.py            # сухой прогон
    python3 scripts/dict_apply_headword_rules_to_stored.py --apply    # записать
"""
from __future__ import annotations

import argparse
import os
import sys

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SKIP_BILLING_LEDGER_WRITES", "1")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.database import (                                    # noqa: E402
    get_db_connection_context,
    _fix_plural_article_on_headword,
)
from backend.german_grammar_tables import german_dictionary_headword  # noqa: E402
from backend.lex_units import normalize_query                     # noqa: E402


def product_rule(value: str | None) -> str | None:
    """Ровно то, что делает дверь записи. Ни одной своей строчки правила."""
    if not value:
        return value
    return _fix_plural_article_on_headword(german_dictionary_headword(value))


# таблица, колонка с заголовком, как называть в отчёте
TARGETS = [
    ("bt_3_lex_units", "display", "слово в справочнике",
     "lang = 'de' AND display <> ''"),
    ("bt_3_webapp_dictionary_queries", "word_de", "карточка word_de",
     "word_de IS NOT NULL AND word_de <> '' AND word_de !~ '[А-Яа-яЁё]'"),
    ("bt_3_webapp_dictionary_queries", "translation_de", "карточка translation_de",
     "translation_de IS NOT NULL AND translation_de <> '' AND translation_de !~ '[А-Яа-яЁё]'"),
    ("bt_3_dictionary_entries", "source_text", "пул",
     "source_text IS NOT NULL AND source_text <> '' AND source_text !~ '[А-Яа-яЁё]'"),
]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    total = 0
    with get_db_connection_context() as conn:
        for table, column, title, where in TARGETS:
            with conn.cursor() as cur:
                cur.execute("SELECT id, %s FROM %s WHERE %s;" % (column, table, where))
                rows = cur.fetchall()
            changes = []
            for row_id, value in rows:
                fixed = product_rule(value)
                if fixed and fixed != value:
                    changes.append((row_id, value, fixed))
            print("%-28s просмотрено %-6d к правке %d" % (title, len(rows), len(changes)))
            for row_id, old, new in changes[:10]:
                print("      %-8s %-32s → %s" % (row_id, str(old)[:32], new))
            if len(changes) > 10:
                print("      … ещё %d" % (len(changes) - 10))
            total += len(changes)

            if args.apply and changes:
                with conn.cursor() as cur:
                    for row_id, _old, new in changes:
                        cur.execute(
                            "UPDATE %s SET %s = %%s, updated_at = NOW() WHERE id = %%s;"
                            % (table, column),
                            (new, row_id),
                        )
                        # У слова заголовок — это ещё и ключ поиска. Старое написание
                        # остаётся дверью, новое добавляем, иначе слово перестанет
                        # находиться по собственной новой форме.
                        if table == "bt_3_lex_units":
                            cur.execute(
                                "UPDATE bt_3_lex_units SET lemma = %s, lemma_key = %s "
                                "WHERE id = %s;",
                                (new, normalize_query(new), row_id),
                            )
                            cur.execute(
                                "INSERT INTO bt_3_lex_surfaces (lang, surface_key, unit_id, match_kind) "
                                "VALUES ('de', %s, %s, 'exact') ON CONFLICT DO NOTHING;",
                                (normalize_query(new), row_id),
                            )
                conn.commit()

    print()
    print("всего к правке: %d" % total)
    if not args.apply:
        print("СУХОЙ ПРОГОН. Ничего не записано. Для записи добавь --apply.")


if __name__ == "__main__":
    main()
