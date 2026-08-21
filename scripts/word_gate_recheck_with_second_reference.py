# -*- coding: utf-8 -*-
"""Пересмотреть приговоры, вынесенные ДО того, как у двери появился второй справочник.

ЗАЧЕМ. 21.08.2026 в дверь слова добавлен DWDS — второй печатный справочник. Но
вердикты, вынесенные раньше, лежат в кеше как окончательные, и дверь к ним больше не
возвращается. То есть слово «Vergleichbarkeit» так и осталось бы «не подтверждено» и
так и приходило бы человеку на проверку, хотя источник, который его знает, уже есть.

Чинить систему и не чистить накопленное — половина работы, которая не принимается.
Этот скрипт делает вторую половину: снимает старый приговор и спрашивает дверь заново.

ЧТО ПЕРЕСМАТРИВАЕТСЯ. Только «не подтверждено» — то есть «мы не нашли». Подтверждённое
и исправленное не трогаем: их вынес источник, который никуда не делся. «Не слово»
тоже остаётся: DWDS обрубков не знает, а лишний запрос к модели стоит денег.

ЗАПУСК:
    python3 scripts/word_gate_recheck_with_second_reference.py           # показать
    python3 scripts/word_gate_recheck_with_second_reference.py --apply   # переспросить
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

TARGET_STATUS = "не подтверждено"


def main() -> int:
    apply = "--apply" in sys.argv
    from backend.database import get_db_connection_context
    from backend.german_word_gate import check_word

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT asked, status, source FROM bt_3_word_check "
                        "WHERE status = %s ORDER BY asked;", (TARGET_STATUS,))
            rows = [(str(a), str(b), str(c)) for a, b, c in (cur.fetchall() or [])]

    print(f"Приговоров «{TARGET_STATUS}» в кеше: {len(rows)}")
    if not rows:
        return 0
    if not apply:
        for asked, _status, source in rows:
            print(f"  {asked:24} ← {source}")
        print("\nЭто показ. Чтобы переспросить дверь заново — добавь --apply")
        return 0

    changed, same = [], []
    for asked, _status, source in rows:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM bt_3_word_check WHERE asked=%s;", (asked,))
            conn.commit()
        verdict = check_word(asked, allow_network=True, allow_model=True)
        line = (asked, source, str(verdict.get("status")), str(verdict.get("source")))
        (changed if verdict.get("status") != TARGET_STATUS else same).append(line)

    print(f"\nПересмотрено: {len(rows)} · вердикт изменился у {len(changed)}")
    for asked, was, status, now in changed:
        print(f"  ✔ {asked:24} {was}  →  {status} ({now})")
    if same:
        print(f"\nОсталось неподтверждённым: {len(same)}")
        for asked, _was, _status, now in same:
            print(f"    {asked:24} ← {now}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
