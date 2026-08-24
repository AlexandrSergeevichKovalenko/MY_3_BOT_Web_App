# -*- coding: utf-8 -*-
"""ТОЛЬКО ЧИТАЕТ. Состояние карточек Wo-Fragen: в очереди «работа над ошибками»
и в дневных наборах — до заслона 29.07.2026 и после него.

Зачем именно так: сырое число «столько-то кривых» ничего не значит, пока не видно,
РОДИЛИСЬ они до правила или после. До — значит правило работает и чинить надо
накопленное; после — значит дырявое само правило.

Замер 24.08.2026 (эталон, с ним и сравнивать):
    очередь ошибок:  до 29.07 — 80 карточек, кривых 2; после — 29, кривых 0
    дневные наборы:  до 29.07 — 610 заданий, кривых 2; после — 800, кривых 0
    цена проверки:   ~1 мкс на карточку
    ЛОЖНЫЙ СЛЕД: 27 карточек «вещь в вопросе о человеке» — НЕ дефект, в режиме
    человека подсказка это имя (David, Julia), существительным оно и не должно быть.

    python3 scripts/wofrage_review_audit.py

ПРОВЕРЕНО 24.08.2026, НЕ ПОДНИМАТЬ СНОВА: в двух ИЮЛЬСКИХ дневных наборах лежат
кривые задания (das Opfer). Это мёртвые данные: наборы выдаются только за свой день
(get_latest_daily_wofrage_set по дате), и к битвам эти два набора не привязаны —
проверено запросом, 0 битв. Достать их с экрана нельзя, чинить нечего.
"""
from __future__ import annotations

import datetime
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.database import get_db_connection_context          # noqa: E402
from backend.wofrage_generator import stored_item_problems      # noqa: E402

GUARD = datetime.date(2026, 7, 29)   # коммит ac09d3e5 «защита от человека в списке вещей»


def main() -> int:
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, payload, created_at FROM bt_3_aufgabe_mistakes "
                        "WHERE format='wofrage';")
            queue = [(r[0], r[1] if isinstance(r[1], dict) else json.loads(r[1] or "{}"),
                      r[2].date()) for r in cur.fetchall() or []]
            cur.execute("SELECT items_json, created_at FROM bt_3_wofrage_sprint_sets;")
            sets = [(r[0] if isinstance(r[0], list) else json.loads(r[0] or "[]"), r[1].date())
                    for r in cur.fetchall() or []]

    print("ОЧЕРЕДЬ «РАБОТА НАД ОШИБКАМИ»")
    for label, rows in (("до заслона ", [x for x in queue if x[2] < GUARD]),
                        ("после      ", [x for x in queue if x[2] >= GUARD])):
        bad = [(x[0], x[2], stored_item_problems(x[1])[0]) for x in rows if stored_item_problems(x[1])]
        print(f"  {label}: карточек {len(rows):4d}, кривых {len(bad)}")
        for b in bad:
            print(f"      #{b[0]} от {b[1]}: {b[2]}")

    print("ДНЕВНЫЕ НАБОРЫ")
    for label, rows in (("до заслона ", [x for x in sets if x[1] < GUARD]),
                        ("после      ", [x for x in sets if x[1] >= GUARD])):
        items = [it for x in rows for it in x[0]]
        bad = [it for it in items if stored_item_problems(it)]
        print(f"  {label}: наборов {len(rows):4d}, заданий {len(items):5d}, кривых {len(bad)}")
        for b in bad:
            print(f"      {b.get('obj')} → {b.get('a')}: {stored_item_problems(b)[0]}")

    every = [x[1] for x in queue] + [it for x in sets for it in x[0]]
    if every:
        t0 = time.perf_counter()
        for _ in range(20):
            for p in every:
                stored_item_problems(p)
        per = (time.perf_counter() - t0) / (20 * len(every))
        print(f"ЦЕНА ПРОВЕРКИ: {per * 1e6:.1f} мкс на карточку; "
              f"страница из 15 карточек — {per * 15 * 1000:.4f} мс")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
