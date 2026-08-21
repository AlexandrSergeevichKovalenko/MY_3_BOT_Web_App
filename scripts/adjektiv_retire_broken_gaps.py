# -*- coding: utf-8 -*-
"""Снять с выдачи задания на окончания, у которых пропуск не склеивается обратно.

ЗАЧЕМ. Замер 21.08.2026 по 2370 выданным заданиям нашёл класс: пропуск стоит не на
том слове, и подстановка ответа даёт неверную немецкую форму.

    показано: ohne ein[___] gutes Argument   ответ «e»
    выйдет:   ohne eine gutes Argument       а верно: ohne ein gutes Argument

Ученик получает «правильно» за неверную форму и узнать об этом ему неоткуда.

Дыра закрыта в коде дважды: страж на входе (`bot_3.py`, разбор ответа модели) и
второй рубеж на выдаче (`database.pick_adjektiv_payloads` → `adjektiv_gap_rebuilds`).
Этот скрипт делает вторую половину — убирает то, что уже лежит в банке.

ЧТО НЕ ТРОГАЕТСЯ. Задания, у которых кривой только раскрой на «до» и «после», а
окончание стоит на нужном слове: они чинятся сами по целой фразе
(`derive_adjektiv_split`) и выбрасывать их было бы отдельной потерей.

ЗАПУСК:
    python3 scripts/adjektiv_retire_broken_gaps.py           # показать
    python3 scripts/adjektiv_retire_broken_gaps.py --apply   # снять с выдачи
"""
from __future__ import annotations

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def main() -> int:
    apply = "--apply" in sys.argv
    from backend.database import adjektiv_gap_rebuilds, get_db_connection_context

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT aufgabe_id, payload FROM bt_3_aufgabe_bank "
                        "WHERE format='adjektiv' AND retired=FALSE;")
            rows = cur.fetchall() or []

    broken: list[tuple[str, str]] = []
    for aufgabe_id, payload in rows:
        data = json.loads(payload) if isinstance(payload, str) else (payload or {})
        for item in (data.get("items") or [data]):
            if not adjektiv_gap_rebuilds(item):
                shown = (f"{item.get('before')}[___]{item.get('after')}"
                         f"  ответ «{item.get('correct') or item.get('a')}»"
                         f"  верно: {item.get('full')}")
                broken.append((str(aufgabe_id), shown))
                break

    print(f"Живых заданий на окончания в банке: {len(rows)}")
    print(f"Не склеиваются обратно: {len(broken)}")
    for aufgabe_id, shown in broken:
        print(f"  {aufgabe_id}  {shown}")
    if not broken:
        return 0
    if not apply:
        print("\nЭто показ. Чтобы снять их с выдачи — добавь --apply")
        return 0

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE bt_3_aufgabe_bank SET retired=TRUE WHERE aufgabe_id = ANY(%s);",
                        ([b[0] for b in broken],))
            снято = cur.rowcount
        conn.commit()
    print(f"\nСнято с выдачи: {снято}")

    # Проверка ФАКТОМ, а не намерением: спрашиваем банк заново.
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM bt_3_aufgabe_bank "
                        "WHERE format='adjektiv' AND retired=FALSE;")
            осталось = cur.fetchone()[0]
    print(f"Живых заданий осталось: {осталось}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
