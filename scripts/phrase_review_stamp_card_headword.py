# -*- coding: utf-8 -*-
"""Заголовок внутри разбора = заголовок слова, для решений владельца по спорным фразам.

ЧТО БЫЛО. Владелец правит фразу, разбор собирается заново — но модель отвечает СВОИМ
написанием: на «Der nie versiegende Zapfhahn von Geldquellen» возвращает «der nie
versiegende…», на «Die Zuschlagsstoffe» — «die Zuschlagsstoffe». А правка владельца
ровно в регистре и состояла: в немецком это грамматика. Разбор оставался про доправочный
вид фразы. Замер 20.08.2026: 8 решений из 119.

ПОЧЕМУ ЭТО НЕ ВИДНО НА ЭКРАНЕ И ВСЁ РАВНО ЧИНИТСЯ. Выдача перебивает заголовок текстом
самого слова (`lex_units._build_item` → `backend_server:7809`), поэтому человек видит
правильное. Но в хранилище лежит неверное написание, и любой следующий читатель разбора
получит его — чинится причина, а не экран.

ДЫРА ЗАКРЫТА В КОДЕ: `rebuild_unit_breakdown` (backend/database.py) записывает в разбор
тот текст, о котором спрашивал. Здесь — уборка того, что уже накопилось.

ЧЕГО ЭТОТ СКРИПТ НЕ ДЕЛАЕТ. Он трогает ТОЛЬКО слова, по которым владелец принимал решение
о спорной фразе. По всему словарю таких расхождений 384 из 5353 немецких слов с разбором
(замер 20.08.2026) — там причина другая (человек искал форму, модель ответила словарной
формой), и разворачивать на них правило приёмки нельзя без отдельного разбора.

    python3 scripts/phrase_review_stamp_card_headword.py           # показать
    python3 scripts/phrase_review_stamp_card_headword.py --apply   # починить
"""
from __future__ import annotations

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SKIP_BILLING_LEDGER_WRITES", "1")

from backend.database import get_db_connection_context  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="записать правки в базу")
    args = parser.parse_args()

    fixed = 0
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """SELECT r.id, u.id, u.display, u.card
                     FROM bt_3_phrase_review r
                     JOIN bt_3_lex_units u ON u.id = r.unit_id
                    WHERE r.status IN ('accepted', 'replaced')
                      AND u.card ? 'word_source'
                      AND btrim(u.card->>'word_source') <> btrim(u.display)
                    ORDER BY r.id;"""
            )
            rows = cursor.fetchall()
            print(f"\nразборов с чужим заголовком: {len(rows)}\n")
            for review_id, unit_id, display, card in rows:
                was = str((card or {}).get("word_source") or "")
                print(f"  #{review_id} слово {unit_id}")
                print(f"      было:  {was}")
                print(f"      станет: {display}")
                if not args.apply:
                    continue
                card = dict(card or {})
                card["word_source"] = str(display or "").strip()
                cursor.execute(
                    "UPDATE bt_3_lex_units SET card = %s WHERE id = %s;",
                    (json.dumps(card, ensure_ascii=False), unit_id),
                )
                fixed += 1
        if args.apply:
            conn.commit()

    print()
    print(f"починено: {fixed}" if args.apply else "сухой прогон, в базу ничего не писалось")
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
