# -*- coding: utf-8 -*-
"""Старый текст исправленной фразы вычистить из карточки ВЕЗДЕ, где он есть.

Что случилось
─────────────
Владелец правит спорные фразы, правка меняет слово в справочнике. Личная карточка
при этом хранит СВОЮ копию текста — и не одну.

16.08.2026, три замера подряд, каждый следующий опровергал предыдущий:
    «текст лежит в word_de»                    → починили, владелец прислал скриншот
    «текст лежит в четырёх полях»               → проверка нашла ДЕВЯТНАДЦАТЬ
    «починим эти четыре»                        → осталось бы 769 карточек с обоими

Поэтому здесь НЕТ списка полей. Берём старый текст и вычищаем его из всего разбора
рекурсивно: строки, списки, вложенные объекты. Девятнадцать мест или тридцать —
неважно, правило одно.

Откуда известен старый текст
────────────────────────────
Из bt_3_card_headword_backup: туда сложили прежние заголовки, когда подтягивали их к
исправленному слову. Заменяем РОВНО его — сравнение по регистру и пробелам. Поле с
русским текстом при этом не трогается: оно старому немецкому заголовку не равно
(проверено: карточек, где русское поле совпало бы со старым немецким, — ноль).

⚠ ЧЕГО ЗДЕСЬ НЕТ И ПОЧЕМУ
jsonb_strip_nulls. В первой версии он стоял безусловно и молча удалил бы ключи у 816
карточек: forms.plural у 778, praeteritum у 671, perfekt у 640, article у 29. У 601
карточки блок форм схлопнулся бы в пустой. К цели скрипта это отношения не имело.
Поймано проверкой до запуска.

⚠ ЧЕГО ЭТОТ СКРИПТ НЕ ЧИНИТ
Заготовки заданий (sentence_gap_v2) у 5 карточек построены на старой кривой фразе.
Их надо ПЕРЕСОБРАТЬ, а не заменить строку: пропуски и ответ должны сойтись. Список
печатается в конце.

    python3 scripts/dict_replace_stale_phrase_everywhere.py            # сухой прогон
    python3 scripts/dict_replace_stale_phrase_everywhere.py --apply    # записать
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

_SPACE = re.compile(r"\s+")


def key_of(value) -> str:
    return _SPACE.sub(" ", str(value or "").strip()).casefold()


def replace_everywhere(node, old_key: str, new_text: str) -> tuple[object, int]:
    """Заменить строку, равную old_key, в любом месте дерева. Возвращает (дерево, сколько)."""
    if isinstance(node, str):
        return (new_text, 1) if key_of(node) == old_key else (node, 0)
    if isinstance(node, list):
        out, hits = [], 0
        for item in node:
            fixed, n = replace_everywhere(item, old_key, new_text)
            out.append(fixed)
            hits += n
        return out, hits
    if isinstance(node, dict):
        out, hits = {}, 0
        for name, item in node.items():
            fixed, n = replace_everywhere(item, old_key, new_text)
            out[name] = fixed
            hits += n
        return out, hits
    return node, 0


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT b.entry_id, b.word_de, q.word_de, q.translation_de, q.response_json
                FROM bt_3_card_headword_backup b
                JOIN bt_3_webapp_dictionary_queries q ON q.id = b.entry_id
                ORDER BY b.entry_id;
                """
            )
            rows = cur.fetchall()

    plan, tasks_to_rebuild = [], []
    total_hits = 0
    for entry_id, old_text, new_text, translation_de, payload in rows:
        old_key = key_of(old_text)
        if not old_key or key_of(new_text) == old_key:
            continue
        card = payload if isinstance(payload, dict) else {}
        fixed_card, hits = replace_everywhere(card, old_key, new_text)
        fix_column = bool(translation_de) and key_of(translation_de) == old_key
        if not hits and not fix_column:
            continue
        total_hits += hits + (1 if fix_column else 0)
        plan.append((entry_id, old_text, new_text, fixed_card if hits else None, fix_column, hits))
        gap = card.get("sentence_gap_v2")
        if isinstance(gap, dict) and old_key in json.dumps(gap, ensure_ascii=False).casefold():
            tasks_to_rebuild.append((entry_id, new_text))

    print("карточек к правке: %d, мест внутри них: %d" % (len(plan), total_hits))
    print()
    for entry_id, old_text, new_text, _card, fix_column, hits in plan[:8]:
        print("   карточка %-7s мест в разборе: %-3d колонка: %s" % (entry_id, hits, "да" if fix_column else "нет"))
        print("      было:  %s" % str(old_text)[:66])
        print("      стало: %s" % str(new_text)[:66])

    if not args.apply:
        print()
        print("ЗАГОТОВКИ ЗАДАНИЙ НА ПЕРЕСБОРКУ (строку менять нельзя, надо собирать заново): %d"
              % len(tasks_to_rebuild))
        for entry_id, text in tasks_to_rebuild:
            print("   карточка %-7s %s" % (entry_id, str(text)[:60]))
        print()
        print("СУХОЙ ПРОГОН. Ничего не записано. Для записи добавь --apply.")
        return

    done = 0
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            for entry_id, _old_text, new_text, fixed_card, fix_column, _hits in plan:
                if fixed_card is not None:
                    cur.execute(
                        "UPDATE bt_3_webapp_dictionary_queries SET response_json = %s::jsonb, "
                        "updated_at = NOW() WHERE id = %s;",
                        (json.dumps(fixed_card, ensure_ascii=False), entry_id),
                    )
                if fix_column:
                    cur.execute(
                        "UPDATE bt_3_webapp_dictionary_queries SET translation_de = %s, "
                        "updated_at = NOW() WHERE id = %s;",
                        (new_text, entry_id),
                    )
                done += 1
        conn.commit()

    print("\nкарточек поправлено: %d" % done)
    print("ЗАГОТОВКИ ЗАДАНИЙ НА ПЕРЕСБОРКУ: %d — %s"
          % (len(tasks_to_rebuild), ", ".join(str(t[0]) for t in tasks_to_rebuild)))


if __name__ == "__main__":
    main()
