# -*- coding: utf-8 -*-
"""Перечитать таблицы спряжения, в которые попал служебный текст со страницы.

ЧТО БЫЛО. Разбор страницы Flexion брал за форму ячейку-примечание. На «zeigen» в
прошедшем времени во всех шести лицах стояло «veraltet:» («устарело:») вместо «zeigte»,
у «besagen» половина клеток была прочерками. Таблица при этом помечалась «подтверждена
справочником», и отличить её снаружи было нечем — из неё строится раздел «Грамматика»
в карточке, то есть человек видел служебное слово вместо формы.

Замер 23.08.2026 по 1467 таблицам: 9 с подписью, 12 с прочерком, у шести сломан именно
Präteritum (bedienen, keilen, stammen, verpönen, zähmen, zeigen).

ДЫРА ЗАКРЫТА В РАЗБОРЕ (backend/german_verb_paradigms.py), тремя правилами:
  • подпись вида «veraltet:» уводит из соревнования ПОМЕЧЕННЫЙ ЕЮ вариант — иначе
    правило «берём самый длинный» отдавало победу устаревшей форме XIX века;
  • прочерк занимает место столбца и означает «формы нет» — клетка остаётся пустой,
    и таблица честно не собирается (так у безличного «besagen»);
  • разбор блока останавливается на заголовке следующего — иначе при пустом столбце
    он перелезал в Perfekt и заполнял конъюнктив формами «habe gezeigt».

Здесь — уборка накопленного: те же слова спрашиваются у справочника заново, уже
починенным разбором. Что после этого не соберётся, ляжет как «страницы нет» и уйдёт
ночью к модели с двойным подтверждением — обычным путём каскада.

    python3 scripts/verb_paradigms_refetch_broken.py           # показать
    python3 scripts/verb_paradigms_refetch_broken.py --apply   # перечитать
"""
from __future__ import annotations

import argparse
import os
import sys
import time

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SKIP_BILLING_LEDGER_WRITES", "1")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.database import get_db_connection_context  # noqa: E402
from backend.german_verb_paradigms import (  # noqa: E402
    _is_dash,
    _is_note,
    fetch_documented_tables,
    store_paradigm,
)

_BLOCKS = ("praesens", "praeteritum", "konjunktiv2", "imperativ")


def broken(tables) -> str:
    """Почему таблица негодна. Пусто — таблица чистая."""
    if not isinstance(tables, dict):
        return ""
    for block in _BLOCKS:
        for value in (tables.get(block) or {}).values():
            if _is_dash(value):
                return f"прочерк в «{block}»"
            if _is_note(value):
                return f"служебная подпись в «{block}»"
    return ""


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT verb, tables FROM bt_3_german_verb_paradigms "
                        "WHERE documented ORDER BY verb;")
            rows = cur.fetchall()

    targets = [(verb, why) for verb, tables in rows if (why := broken(tables))]
    print(f"\nтаблиц подтверждённых: {len(rows)}")
    print(f"со служебным текстом:  {len(targets)}\n")
    for verb, why in targets:
        print(f"      {verb:20} {why}")

    if not args.apply:
        print("\nСУХОЙ ПРОГОН. Перечитать: --apply\n")
        return 0

    fixed = emptied = silent = 0
    for index, (verb, _why) in enumerate(targets, 1):
        tables = fetch_documented_tables(verb)
        if tables is None:
            # Справочник молчит (лимит, сеть). Это НЕ «страницы нет»: старую строку
            # не трогаем, слово переспросится в следующий прогон.
            time.sleep(8.0)
            tables = fetch_documented_tables(verb)
        if tables is None:
            silent += 1
            print(f"  [{index}/{len(targets)}] ⚠️ {verb} — справочник молчит, оставлено")
            time.sleep(2.0)
            continue
        still = broken(tables)
        if still:
            # Разбор всё ещё тащит служебный текст — значит правило не покрыло случай.
            # Записывать такое нельзя: пусть лучше таблицы не будет.
            print(f"  [{index}/{len(targets)}] ⚠️ {verb} — {still}, НЕ записано")
            time.sleep(1.5)
            continue
        store_paradigm(verb, tables)
        if tables.get("praesens"):
            fixed += 1
            pr = (tables.get("praeteritum") or {}).get("er/sie/es") or "—"
            print(f"  [{index}/{len(targets)}] {verb:20} прошедшее: {pr}")
        else:
            emptied += 1
            print(f"  [{index}/{len(targets)}] {verb:20} форм на странице нет — "
                  f"таблица снята, ночью спросим модель")
        time.sleep(1.5)

    print(f"\nперечитано верно: {fixed}, снято как «форм нет»: {emptied}, "
          f"справочник молчал: {silent}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
