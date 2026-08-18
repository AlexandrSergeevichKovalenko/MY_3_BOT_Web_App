# -*- coding: utf-8 -*-
"""Снести выдуманные грамматические таблицы из трёх хранилищ. Сухой прогон по умолчанию.

Зачем. До 18.08.2026 падежи и степени сравнения СЧИТАЛ КОД, и часть таких таблиц осела
в базе. Замер 18.08.2026: 445 записей — 369 склонений, 74 степени сравнения, 2 спряжения.
Таблицы почти нигде не хранятся (собираются в момент показа), поэтому накопленного мало.

Почему СНОСИМ, а не правим. Правильная таблица теперь собирается на выдаче из
справочника — движок сам её построит. Если оставить выдуманную, она будет и дальше
отдаваться как готовая, а её никто не перепроверит: `response_json->'grammar_tables'`
считается признаком «карточка не пустая», и ночной ремонт такую запись не подберёт.

    python3 scripts/purge_invented_grammar_tables.py              # сухой прогон
    python3 scripts/purge_invented_grammar_tables.py --apply      # применить
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SKIP_BILLING_LEDGER_WRITES", "1")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.database import get_db_connection_context  # noqa: E402

STORAGES = [
    ("справочник слов", "bt_3_lex_units", "card", "lemma", "id"),
    ("личные карточки", "bt_3_webapp_dictionary_queries", "response_json", "word_de", "id"),
    ("общий пул", "bt_3_dictionary_entries", "response_json", "word_de", "id"),
]


def _as_dict(value):
    if isinstance(value, dict):
        return value
    if isinstance(value, (str, bytes)):
        try:
            return json.loads(value)
        except Exception:
            return {}
    return {}


def _invented(payload: dict) -> str:
    """Метка дефекта или '' — таблица законная (пришла из справочника)."""
    for holder in (payload, payload.get("item") or {}):
        if not isinstance(holder, dict):
            continue
        tables = holder.get("grammar_tables")
        if not isinstance(tables, dict) or not tables:
            continue
        conj = tables.get("conjugation") or {}
        if isinstance(conj, dict) and conj.get("praesens"):
            if str(conj.get("source") or "") != "wiktionary-flexion":
                return "спряжение посчитано нами"
        comp = tables.get("comparison") or {}
        if isinstance(comp, dict) and (comp.get("comparative") or comp.get("superlative")):
            if not str(comp.get("source") or "").startswith("wiktionary"):
                return "степени сравнения посчитаны нами"
        decl = tables.get("declension") or {}
        if isinstance(decl, dict) and decl.get("rows"):
            if not str(decl.get("source") or "").startswith("wiktionary"):
                return "склонение посчитано нами"
    return ""


def _strip(payload: dict) -> dict:
    """Убрать только grammar_tables. Остальное содержимое карточки не трогаем."""
    out = dict(payload)
    out.pop("grammar_tables", None)
    item = out.get("item")
    if isinstance(item, dict):
        item = dict(item)
        item.pop("grammar_tables", None)
        out["item"] = item
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="применить, иначе сухой прогон")
    args = parser.parse_args()

    grand = Counter()
    for title, table, col, wordcol, idcol in STORAGES:
        print(f"\n=== {title} ({table}) ===")
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT {idcol}, {wordcol}, {col} FROM {table} "
                            f"WHERE {col} IS NOT NULL")
                rows = cur.fetchall() or []
            targets = []
            for row_id, word, payload in rows:
                data = _as_dict(payload)
                label = _invented(data)
                if label:
                    targets.append((row_id, str(word), label, _strip(data)))
            print(f"строк с выдуманной таблицей: {len(targets)}")
            for _rid, word, label, _new in targets[:5]:
                print(f"    было → станет: «{word}» — {label} → таблица снята, "
                      f"на выдаче соберётся из справочника")
            if not args.apply:
                grand[title] = len(targets)
                continue
            with conn.cursor() as cur:
                for row_id, _word, _label, new_payload in targets:
                    cur.execute(f"UPDATE {table} SET {col} = %s::jsonb WHERE {idcol} = %s",
                                (json.dumps(new_payload, ensure_ascii=False), row_id))
            conn.commit()
            print(f"снято: {len(targets)}")
            grand[title] = len(targets)

    print("\nИТОГО:", dict(grand), "| всего", sum(grand.values()))
    if not args.apply:
        print("Это был СУХОЙ ПРОГОН. Ничего не изменено. Применить: --apply")


if __name__ == "__main__":
    main()
