# -*- coding: utf-8 -*-
"""Вид записи разошёлся с написанием — и слово стало невидимкой для ночной работы.

ЧЕМ ЭТО ПЛОХО, а не просто некрасиво. `kind` решает, попадёт ли слово в ночной добор:
`units_needing_card` берёт ТОЛЬКО `kind = 'word'`. Слово, записанное «оборотом», разбор
не получит НИКОГДА — не завтра, а никогда. На экране у него навсегда останется пусто:
«die Rutsche» словарь просто не находил.

ОТКУДА БРАЛОСЬ. Вид записи считается по написанию при заведении. Дальше написание
меняли — снятием школьного хвоста «, -n», решением владельца по спорной фразе, разгоном
правки, — а вид пересчитать забывали, потому что переименование делали три разных места
своими руками. «der Simulator, -en» это два слова, значит «оборот»; «der Simulator» —
одно, значит «слово», а в базе так и осталось «оборот».

ДЫРА ЗАКРЫТА В КОДЕ: `lex_units.retitle_unit` меняет написание, лемму, ключ поиска и вид
ЗА ОДИН РАЗ, и решение по спорной фразе теперь зовёт её, а не пишет запрос своими руками.

ЧТО ЧИНИТ ЭТОТ СКРИПТ. Только по-настоящему вредный подкласс: слово числится оборотом
или предложением, по написанию это ОДНО слово, и разбора у него нет — то есть ровно те,
кого ночь не возьмёт. Остальные расхождения вида (замер 21.08.2026: всего 80, вредных 6)
не трогаем: у них разбор уже есть, и перекладывать их без нужды — риск без выгоды.

    python3 scripts/dict_fix_unit_kind_mismatch.py           # показать
    python3 scripts/dict_fix_unit_kind_mismatch.py --apply   # починить
"""
from __future__ import annotations

import argparse
import os
import sys

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SKIP_BILLING_LEDGER_WRITES", "1")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.database import get_db_connection_context  # noqa: E402
from backend.lex_units import _kind_for_text  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    fixed = 0
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""SELECT id, display, kind, card IS NOT NULL
                                FROM bt_3_lex_units WHERE lang='de' ORDER BY id;""")
            rows = cursor.fetchall()
            mismatched = [(uid, disp, kind, has_card) for uid, disp, kind, has_card in rows
                          if _kind_for_text(str(disp or "")) != kind]
            # Вредный подкладсс: ночь их не возьмёт, а разбора нет.
            stuck = [item for item in mismatched
                     if item[2] != "word" and _kind_for_text(str(item[1] or "")) == "word"
                     and not item[3]]

            print(f"\nнемецких единиц: {len(rows)}")
            print(f"вид записи не совпадает с написанием: {len(mismatched)}")
            print(f"ИЗ НИХ НЕВИДИМЫ ДЛЯ НОЧНОЙ РАБОТЫ (чиним): {len(stuck)}\n")
            for uid, display, kind, _has_card in stuck:
                print(f"  {uid:>6} {display!r}: {kind} → word")
                if args.apply:
                    cursor.execute(
                        "UPDATE bt_3_lex_units SET kind='word', updated_at=NOW() WHERE id=%s;",
                        (uid,),
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
