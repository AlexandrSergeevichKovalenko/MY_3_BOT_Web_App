# -*- coding: utf-8 -*-
"""Фраза помечена существительным, и к ней приклеен артикль. Снять и то и другое.

ЧТО ВИДЕЛ ВЛАДЕЛЕЦ 22.08.2026. Запрос «Ноющая, тянущая боль в боку», в ответе над
заголовком «ziehender, dumpfer Schmerz in der Seite» отдельной строкой «der», а фраза
помечена «существительное». Несогласованно в обе стороны: при артикле окончания
прилагательных должны быть слабыми («der ziehende, dumpfe Schmerz»), без артикля —
сильными, как и написано. Человек заучивает смесь.

ОТКУДА. Модель разбирает существительное ВНУТРИ фразы («der Schmerz») и подписывает им
всю фразу. Машинка авторитетного артикля тут ни при чём — она давно работает только на
одном слове; артикль приходит полем разбора и на многословном не снимался никем.

ЗАМЕР 22.08.2026: 10 337 разборов, из них помечены существительным при многословном
заголовке — 518. У 208 заголовок начинается со СТРОЧНОЙ, там артикль встаёт перед
прилагательным или предлогом и виден как ошибка сразу; у 310 — с заглавной («Pfand
zurückgeben», «Blumen auf die Fensterbank stellen»), и это тоже не существительные.

ЧИНИМ ПОМЕТКУ, А НЕ ТОЛЬКО ЕЁ ПОСЛЕДСТВИЕ. Пока фраза считается существительным, артикль
будет приклеиваться снова — просто в другом месте.

ДЫРА ЗАКРЫТА В КОДЕ: `_apply_german_headword_normalization` (backend/backend_server.py)
снимает артикль и пометку у любого многословного немецкого. Прежняя защита ловила только
целое предложение.

    python3 scripts/dict_phrase_is_not_a_noun.py           # показать
    python3 scripts/dict_phrase_is_not_a_noun.py --apply   # починить
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

from backend.database import get_db_connection_context  # noqa: E402

_LEADING_ARTICLE = re.compile(r"^(?:der|die|das)\s+", re.I)


def bare(value: str) -> str:
    return _LEADING_ARTICLE.sub("", str(value or "").strip()).strip()


def is_multiword(value: str) -> bool:
    """Многословное — по тому же счёту, что и у продукта: артикль словом не считается."""
    return len(bare(value).split()) > 1


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--list", type=int, default=10)
    args = parser.parse_args()

    fixed = 0
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            rows: list = []
            last_id = 0
            while True:
                cursor.execute("""SELECT id, display, card FROM bt_3_lex_units
                                   WHERE lang='de' AND card IS NOT NULL AND id > %s
                                   ORDER BY id LIMIT 400;""", (last_id,))
                batch = cursor.fetchall()
                if not batch:
                    break
                rows.extend(batch)
                last_id = batch[-1][0]

            targets = [(uid, disp, card) for uid, disp, card in rows
                       if isinstance(card, dict)
                       and str(card.get("part_of_speech") or "").strip().lower() == "noun"
                       and is_multiword(disp)]
            lower = [t for t in targets if bare(t[1])[:1].islower()]

            print(f"\nразборов всего: {len(rows)}")
            print(f"«существительное» у многословного: {len(targets)}"
                  f"  (из них заголовок со строчной: {len(lower)})\n")
            for uid, disp, card in targets[: args.list]:
                print(f"  {uid:>6} {disp[:58]!r}  артикль={card.get('article')!r}")

            if not args.apply:
                print("\nСУХОЙ ПРОГОН. Ничего не изменено. Применить: --apply\n")
                return 0

            for uid, disp, card in targets:
                patched = dict(card)
                patched["part_of_speech"] = "phrase"
                if str(patched.get("article") or "").strip():
                    patched["article"] = ""
                cursor.execute(
                    "UPDATE bt_3_lex_units SET card = %s::jsonb, updated_at = NOW() "
                    "WHERE id = %s;",
                    (json.dumps(patched, ensure_ascii=False), uid))
                fixed += 1
            # Род у многословного тоже не имеет смысла: артикль на экран клеится из
            # колонки, а не только из разбора (`lex_units._build_item`).
            cursor.execute("""UPDATE bt_3_lex_units SET gender = NULL, updated_at = NOW()
                               WHERE lang='de' AND gender IS NOT NULL
                                 AND id = ANY(%s);""", ([t[0] for t in targets],))
            print(f"  снят род у многословных: {cursor.rowcount}")
        conn.commit()

    print(f"\nпочинено разборов: {fixed}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
