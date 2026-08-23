# -*- coding: utf-8 -*-
"""Заменить догадку модели выводом из источника там, где составное слово это позволяет.

ОТКУДА ЗАДАЧА. Составное слово склоняется как его последняя часть: «Umschaltsituation»
как «Situation». Правило документировано (Duden, grammis: склоняется только основное
слово). До 23.08.2026 применять его было почти не к чему — голов в базе было 2909.
После загрузки офлайн-таблицы их 89 704, и правило ожило.

ТОЧНОСТЬ ИЗМЕРЕНА, А НЕ ПРЕДПОЛОЖЕНА. Прогон по 6000 длинных слов, где верный ответ
известен из справочника:

    шов доказан            2522   род верно 2474 · неверно 12   (99,5%)
    шов не доказан         3468   молчим, ничего не выдумываем
                                  мн. верно 2262 · неверно 17   (99,3%)
                                  ген. верно 2139 · неверно  9  (99,6%)

ПОЧЕМУ «ДОКАЗАННЫЙ ШОВ», А НЕ ПРОСТО ОТРЕЗАТЬ ХВОСТ. Наивный разрез «самый длинный
хвост, который сам является словом» я написал первым — и он режет «Abart» как «A|bart»,
выдавая мужской род от «der Bart» вместо женского от «die Art». На 64 тысячах слов такой
разрез дал 2597 ошибок по роду. Правило доказанного шва (обе части — известные
однозначные слова) этой ошибки не делает.

ЧТО ДЕЛАЕТ ЭТОТ СКРИПТ. У 347 слов таблица склонения получена ОТ МОДЕЛИ — это догадка,
пусть и с двумя совпавшими ответами. Для 73 из них правило шва теперь может дать вывод
ИЗ ИСТОЧНИКА. Меняем догадку на вывод, но только при согласии двух:

    род от модели == род головы   → заменяем, источник становится «правило композита»
    роды спорят                   → НЕ ТРОГАЕМ, слово идёт владельцу

Спорных оказалось 4, и каждый — настоящая ловушка, а не придирка:

    abflughall     сломанный заголовок; от «der Hall» вышла бы чепуха
    vorsitzende    шов «vorsitz|ende» ложный, это субстантивированное прилагательное
    apfelmark      «Mark» двузначно: das Mark (мякоть) и die Mark (монета)
    watte-stäbchen модель считает слово множественным

ЗАПУСК:
    python3 scripts/compound_upgrade_model_tables.py           # показать
    python3 scripts/compound_upgrade_model_tables.py --apply   # заменить
"""
from __future__ import annotations

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _род(таблица: dict) -> str | None:
    return next((k for k in (таблица or {}) if k in ("m", "f", "n", "pl")), None)


def main() -> int:
    apply = "--apply" in sys.argv
    from backend.database import get_db_connection_context
    from backend.german_reference_forms import declension_from_compound

    with get_db_connection_context() as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SELECT noun, tables FROM bt_3_german_noun_declensions "
                        "WHERE tables->>'source' = 'модель' ORDER BY noun;")
            строки = cur.fetchall() or []

    заменить, спорят, нечем = [], [], 0
    for слово, сырое in строки:
        от_модели = сырое if isinstance(сырое, dict) else json.loads(сырое or "{}")
        от_шва = declension_from_compound(слово)
        if not от_шва:
            нечем += 1
            continue
        if _род(от_модели) == _род(от_шва):
            заменить.append((слово, от_шва))
        else:
            спорят.append((слово, от_шва.get("head"), _род(от_модели), _род(от_шва)))

    print(f"Таблиц от модели: {len(строки)}")
    print(f"  шов не доказан, оставляем как есть: {нечем}")
    print(f"  ЗАМЕНЯЕМ выводом из источника:      {len(заменить)}")
    for слово, т in заменить[:12]:
        print(f"      {слово:30} голова «{т.get('head')}»")
    print(f"  СПОРЯТ — не трогаю, владельцу:      {len(спорят)}")
    for слово, голова, рм, рш in спорят:
        print(f"      {слово:30} голова «{голова}» · модель {рм} / шов {рш}")
    if not apply:
        print("\nЭто показ. Чтобы заменить — добавь --apply")
        return 0

    сделано = 0
    for слово, таблица in заменить:
        таблица = {**таблица, "source": "правило композита"}
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE bt_3_german_noun_declensions "
                            "SET tables = %s::jsonb, checked_at = NOW() WHERE noun = %s;",
                            (json.dumps(таблица, ensure_ascii=False), слово))
                сделано += cur.rowcount
            conn.commit()
    print(f"\nЗаменено догадок выводом из источника: {сделано}")

    # Проверка ФАКТОМ: спрашиваем базу заново.
    with get_db_connection_context() as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SELECT COALESCE(tables->>'source','—'), count(*) "
                        "FROM bt_3_german_noun_declensions GROUP BY 1 ORDER BY 2 DESC;")
            print("\nТаблицы по источникам:")
            for источник, n in cur.fetchall():
                print(f"   {n:6}  {источник}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
