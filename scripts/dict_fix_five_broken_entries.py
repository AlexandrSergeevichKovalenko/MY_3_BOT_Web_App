# -*- coding: utf-8 -*-
"""Пять записей, где сломан сам перевод или само слово. Внести нормальную форму.

Откуда
──────
Всплыли при разборе одиночных переводов с заглавной (16.08.2026). Регистр им не
поможет — сломано другое. Карточек людей ни на одной из пяти нет, поэтому правим смело.

    «Ут» ← Morgen        обрубок слова, должно быть «утро»
    «Наезжать» ← Ramme   неверный перевод: Ramme — это таран, копёр
    «Черт» ← Scheise     в немецком опечатка (нужно «Scheiße»), в русском нет ё
    «Чё» ← Was?          просторечие как словарная статья, должно быть «что»
    «Гарт» ← der Hart    такого существительного нет; hart — прилагательное «твёрдый»

Как чиним
─────────
Если правильная форма уже есть отдельной единицей — сливаем в неё (связи, написания,
карточки переезжают, дубликат удаляется). Если нет — переименовываем, оставляя старое
написание дверью для поиска и добавляя новое.

Слияние — та же механика, что в dict_fix_plural_headwords.py: проверена 15.08.2026.

    python3 scripts/dict_fix_five_broken_entries.py            # сухой прогон
    python3 scripts/dict_fix_five_broken_entries.py --apply    # записать
"""
from __future__ import annotations

import argparse
import os
import sys

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SKIP_BILLING_LEDGER_WRITES", "1")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.database import get_db_connection_context      # noqa: E402
from backend import lex_units as LU                         # noqa: E402
# Механику слияния и переименования берём из соседнего скрипта — она проверена
# 15.08.2026 на заголовках во множественном числе. Копировать её сюда значило бы
# завести вторую версию, которая через неделю разойдётся с первой.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from dict_fix_plural_headwords import merge_into, rename_to            # noqa: E402

# единица → (что лежит, что должно быть, язык, почему)
FIXES = [
    (41168, "Ут", "утро", "ru", "обрубок слова: Morgen — это утро"),
    (29461, "Наезжать", "таран", "ru", "Ramme — таран, копёр; «наезжать» не отсюда"),
    (41586, "Черт", "чёрт", "ru", "в русском пропала ё"),
    (41585, "Scheise", "Scheiße", "de", "опечатка в немецком: эсцет"),
    (41509, "Чё", "что", "ru", "просторечие как словарная статья"),
    (33792, "Гарт", "твёрдый", "ru", "der Hart — не существительное; hart значит твёрдый"),
    (13489, "der Hart", "hart", "de", "существительного «der Hart» нет, есть прилагательное hart"),
]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    merged = renamed = skipped = 0
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            for unit_id, was, should, lang, why in FIXES:
                cur.execute("SELECT display FROM bt_3_lex_units WHERE id = %s;", (unit_id,))
                row = cur.fetchone()
                if not row:
                    print("   %-7s %-14s единицы нет — пропуск" % (unit_id, was))
                    skipped += 1
                    continue
                current = str(row[0] or "")
                if current != was:
                    print("   %-7s ожидали %r, лежит %r — ПРОПУСК" % (unit_id, was, current))
                    skipped += 1
                    continue
                cur.execute(
                    "SELECT id, display FROM bt_3_lex_units "
                    "WHERE lang = %s AND lemma_key = %s AND id <> %s ORDER BY id LIMIT 1;",
                    (lang, LU.normalize_query(should), unit_id),
                )
                twin = cur.fetchone()
                if twin:
                    print("   %-7s %-14s → слить с «%s» (%s)   %s"
                          % (unit_id, was, twin[1], twin[0], why))
                    if args.apply:
                        merge_into(cur, dead=unit_id, alive=int(twin[0]), lang=lang)
                    merged += 1
                else:
                    print("   %-7s %-14s → «%s»   %s" % (unit_id, was, should, why))
                    if args.apply:
                        rename_to(cur, unit_id=unit_id, old=was, new=should, lang=lang)
                    renamed += 1
            if args.apply:
                conn.commit()

    print()
    print("слито: %d, переименовано: %d, пропущено: %d" % (merged, renamed, skipped))
    if not args.apply:
        print("СУХОЙ ПРОГОН. Ничего не записано. Для записи добавь --apply.")


if __name__ == "__main__":
    main()
