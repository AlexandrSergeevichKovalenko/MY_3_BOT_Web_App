# -*- coding: utf-8 -*-
"""Пересобрать ХРАНИМЫЕ таблицы спряжения по текущему заголовку.

Почему это понадобилось
───────────────────────
17.08.2026 я исправил движок таблиц: у отделяемого глагола приставка в личной форме
уходит в конец («ich komme klar»). Владелец открыл карточку — там по-прежнему
«ich klarzukomme». Я перед этим объявил, что таблицы нигде не хранятся и строятся на
выдаче. ЭТО БЫЛО НЕВЕРНО: я искал ключи `conjugation` и `praesens` на верхнем уровне
разбора, а таблица лежит глубже — `grammar_tables.conjugation`.

Хранимая таблица перекрывает движок, поэтому правка кода её не касается. Замер 17.08:

    bt_3_lex_units                   33 со спряжением
    bt_3_webapp_dictionary_queries    9
    bt_3_dictionary_entries          18

Как пересобираем
────────────────
Зовём боевую german_grammar_tables.build_verb_conjugation по ТЕКУЩЕМУ заголовку и
передаём ей неправильные формы из старой таблицы как `seed` — иначе сильный глагол
получил бы правильную по правилу, но неверную по языку форму («kommte» вместо «kam»).

⚠ Испорченную клетку в seed не берём. «klarzukommt» починить подстановкой нельзя:
снять приставку «klar» оставит «zukommt», то есть частицу внутри формы. Такие клетки
выбрасываем, и движок считает их сам — регулярно и верно. Признак порчи один и тот же
на весь проект: looks_like_zu_infinitive для склеенной формы и «zu» между приставкой и
основой в личной.

    python3 scripts/dict_rebuild_stored_conjugations.py            # сухой прогон
    python3 scripts/dict_rebuild_stored_conjugations.py --apply    # записать
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SKIP_BILLING_LEDGER_WRITES", "1")
# Таблицы пересобираем ИЗ СПРАВОЧНИКА — включаем его явно.
os.environ.setdefault("VERB_PARADIGM_LOOKUP", "1")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.database import get_db_connection_context            # noqa: E402
from backend.german_grammar_tables import (                       # noqa: E402
    build_verb_conjugation,
    split_separable_verb,
)

TARGETS = [
    ("bt_3_lex_units", "card", "display", "слово в справочнике"),
    ("bt_3_webapp_dictionary_queries", "response_json", "word_de", "личная карточка"),
    ("bt_3_dictionary_entries", "response_json", "source_text", "общий пул"),
]


def polluted(form: str, prefix: str) -> bool:
    """Форма с «zu» между приставкой и основой: «klarzukommt», «anzulehnst»."""
    text = str(form or "").strip().casefold()
    if not text or not prefix:
        return False
    return text.startswith(prefix.casefold() + "zu")


def seed_from_old(table: dict, prefix: str) -> dict:
    """Собрать seed из старой таблицы, выбросив испорченные клетки."""
    seed: dict = {}
    praesens = table.get("praesens") if isinstance(table.get("praesens"), dict) else {}
    imperativ = table.get("imperativ") if isinstance(table.get("imperativ"), dict) else {}
    praet = table.get("praeteritum") if isinstance(table.get("praeteritum"), dict) else {}
    konj = table.get("konjunktiv2") if isinstance(table.get("konjunktiv2"), dict) else {}

    for key, value in (
        ("present_2sg", praesens.get("du")),
        ("present_3sg", praesens.get("er/sie/es")),
        ("praeteritum", praet.get("ich")),
        ("konjunktiv2", konj.get("ich")),
        ("imperative_sg", imperativ.get("du")),
    ):
        text = str(value or "").strip()
        if text and not polluted(text, prefix):
            seed[key] = text

    aux = str(table.get("auxiliary") or "").strip()
    participle = str(table.get("partizip2") or "").strip()
    if aux and participle:
        seed["perfekt"] = f"{aux} {participle}"
    if table.get("is_separable") is not None:
        seed["is_separable"] = table["is_separable"]
    return seed


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    total = 0
    with get_db_connection_context() as conn:
        for table_name, column, head_column, title in TARGETS:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id, %s, %s FROM %s WHERE %s->'grammar_tables' ? 'conjugation';"
                    % (head_column, column, table_name, column)
                )
                rows = cur.fetchall()
            changes = []
            for row_id, headword, payload in rows:
                card = payload if isinstance(payload, dict) else {}
                tables = card.get("grammar_tables")
                if not isinstance(tables, dict):
                    continue
                old = tables.get("conjugation")
                if not isinstance(old, dict):
                    continue
                # Инфинитив берём из самой таблицы: он и был её основой. Колонка
                # заголовка для этого не годится — в пуле там встречается спряжённая
                # форма («lehne», «mitkriege»), и сборка от неё даёт «lehnee».
                # Не инфинитив ни там, ни там — не трогаем: выдумывать лемму нельзя.
                head = ""
                for candidate in (old.get("infinitive"), headword):
                    text = str(candidate or "").strip()
                    if text and " " not in text and text.casefold().endswith(("en", "eln", "ern")):
                        head = text
                        break
                if not head:
                    continue
                prefix, _base = split_separable_verb(head)
                fresh = build_verb_conjugation(word_de=head, seed=seed_from_old(old, prefix))
                if not fresh or fresh == old:
                    continue
                fixed = dict(card)
                fixed["grammar_tables"] = {**tables, "conjugation": fresh}
                changes.append((row_id, head, old, fresh, fixed))

            print("%-22s таблиц: %-4d к пересборке: %d" % (title, len(rows), len(changes)))
            for row_id, head, old, fresh, _fixed in changes[:6]:
                o = (old.get("praesens") or {}).get("ich")
                n = (fresh.get("praesens") or {}).get("ich")
                print("      %-8s %-22s ich: %-22s → %s" % (row_id, head[:22], o, n))
            if len(changes) > 6:
                print("      … ещё %d" % (len(changes) - 6))
            total += len(changes)

            if args.apply and changes:
                with conn.cursor() as cur:
                    for row_id, _head, _old, _fresh, fixed in changes:
                        cur.execute(
                            "UPDATE %s SET %s = %%s::jsonb, updated_at = NOW() WHERE id = %%s;"
                            % (table_name, column),
                            (json.dumps(fixed, ensure_ascii=False), row_id),
                        )
                conn.commit()

    print()
    print("всего пересобрано: %d" % total)
    if not args.apply:
        print("СУХОЙ ПРОГОН. Ничего не записано. Для записи добавь --apply.")


if __name__ == "__main__":
    main()
