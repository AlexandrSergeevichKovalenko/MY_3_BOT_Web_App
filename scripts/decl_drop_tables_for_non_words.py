# -*- coding: utf-8 -*-
"""Убрать из справочника склонений таблицы, заведённые на НЕСУЩЕСТВУЮЩИЕ слова.

ЧТО НАШЛОСЬ. 25.08.2026, при разборе «почему обрезки не чинятся», выяснилось, что в
`bt_3_german_noun_declensions` лежат таблицы для самих обрезков:

    abschiebu · erwer · kohlenmonoxi · sauerstoffko · scheinwerfergla · tärigkeiten
    painka · -künfte

Каждая подписана `source: модель` — то есть это ДОГАДКА модели, не печать справочника.
Слова при этом уже признаны несуществующими: их вердикт в `bt_3_word_check` — «не слово»,
подтверждён моделью повторно 25.08.2026.

ЧЕМ ЭТО ВРЕДНО, кроме мусора в источнике. Правило достройки обрезка ищет единственное
слово, начинающееся на огрызок. Пока в таблице лежит сам огрызок, он «достраивается» в
себя же, и правило не срабатывает НИКОГДА. То есть заражение источника не просто
занимало место — оно ломало починку.

ПОЧЕМУ ЭТО НЕ РАЗОВАЯ УБОРКА. Дверь слова с 25.08.2026 достраивает обрезки по печатной
части справочника и исключает из поиска строки с подписью «модель» — то есть новые такие
строки правилу больше не мешают. Но лежать им там всё равно незачем: справочник это
источник истины, и слово, которого нет в языке, из него уходит.

МАСШТАБ ИЗМЕРЕН, А НЕ ПРЕДПОЛОЖЕН. Всего таблиц 89 709:

    86 795  german-nouns      печатная выгрузка, чистая
     2 504  без подписи       наше чтение страниц справочника, старое
       284  модель            догадки; из них НА НЕСЛОВА — 8
       126  правило композита вывод из головы составного слова

То есть речь о восьми строках, а не о чистке четверти базы.

    python3 scripts/decl_drop_tables_for_non_words.py           # показать
    python3 scripts/decl_drop_tables_for_non_words.py --apply   # убрать
"""
from __future__ import annotations

import argparse
import os
import sys

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SKIP_BILLING_LEDGER_WRITES", "1")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.database import get_db_connection_context  # noqa: E402

ЗАПРОС = """
 SELECT d.noun, COALESCE(d.tables->>'source','—'), w.asked, w.source
   FROM bt_3_german_noun_declensions d
   JOIN bt_3_word_check w
     ON lower(w.asked) = lower(d.noun)
     OR lower(w.asked) = lower('die ' || d.noun)
     OR lower(w.asked) = lower('der ' || d.noun)
     OR lower(w.asked) = lower('das ' || d.noun)
  WHERE w.status = 'не слово'
  ORDER BY d.noun
"""


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    with get_db_connection_context() as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(ЗАПРОС)
            строки = cur.fetchall() or []

    print(f"Таблиц склонения, заведённых на «не слово»: {len(строки)}\n")
    от_модели = 0
    for слово, источник, спрошено, почему in строки:
        метка = "" if источник == "модель" else "  ⚠ НЕ от модели — разобрать глазами"
        от_модели += (источник == "модель")
        print(f"   {слово!r:26} источник таблицы: {источник:18} приговор: {почему}{метка}")

    if от_модели != len(строки):
        # Печатный источник не мог напечатать несуществующее слово. Расхождение означает,
        # что либо приговор неверен, либо подпись источника — и решать это удалением
        # нельзя. Останавливаемся и говорим словами.
        print("\n⛔ Есть таблицы НЕ от модели. Значит либо приговор «не слово» неверен,")
        print("   либо неверна подпись источника. Удалением это не решается — разбирать.")
        return 1

    if not args.apply:
        print("\nЭто показ. Чтобы убрать — добавь --apply")
        return 0

    убрано = 0
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            for слово, _источник, _спрошено, _почему in строки:
                cur.execute("DELETE FROM bt_3_german_noun_declensions WHERE noun = %s;",
                            (слово,))
                убрано += cur.rowcount
        conn.commit()

    # Проверка ФАКТОМ: спрашиваем базу заново тем же запросом.
    with get_db_connection_context() as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(ЗАПРОС)
            осталось = len(cur.fetchall() or [])
            cur.execute("SELECT count(*) FROM bt_3_german_noun_declensions;")
            всего = cur.fetchone()[0]
    print(f"\nубрано: {убрано} · осталось таких: {осталось} · таблиц в справочнике: {всего}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
