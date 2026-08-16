# -*- coding: utf-8 -*-
"""Одиночные русские переводы с заглавной: опустить обычные, имена собственные не трогать.

Откуда взялись
──────────────
Правило показа опускает первую букву у 1365 переводов из 1418, а эти 51 не трогает:
одно слово, и немецкая сторона не названа явно глаголом, прилагательным или наречием.
Значит среди них могут быть имена собственные, а правилом их от обычных слов не
отличить — проверка «а это существительное?» однажды уже написала «афины».

Поэтому таблица явная, каждая строка просмотрена глазами 16.08.2026.

Что делает скрипт
─────────────────
Меняет display у русской единицы. Слово остаётся тем же, меняется только первая буква.
Написание для поиска не трогаем: поиск и так регистронезависимый (surface_key хранится
в нижнем регистре).

    python3 scripts/dict_fix_capitalized_translations.py            # сухой прогон
    python3 scripts/dict_fix_capitalized_translations.py --apply    # записать
"""
from __future__ import annotations

import argparse
import os
import sys

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SKIP_BILLING_LEDGER_WRITES", "1")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.database import get_db_connection_context      # noqa: E402

# Обычные слова: заглавная лишняя. Ключ — номер русской единицы, значение — как было.
LOWERCASE = {
    41483: "Блестки", 41426: "Взрослые", 41241: "Всё", 29165: "Выключатель",
    41200: "Где", 41418: "Город", 41654: "Двери", 41295: "Дерево",
    41314: "Деятельности", 44101: "Договорились", 41312: "Задница", 41350: "Звучит",
    41287: "Книги", 41336: "Командировки", 41525: "Люк", 41293: "Машины",
    41424: "Мигранты", 44305: "Мышонок", 31938: "Ощущение", 29636: "Плата",
    41695: "Поезда", 44110: "Понятно", 29006: "Призвание", 29909: "Пробка",
    41291: "Проблема", 41274: "Проблемы", 29719: "Произношение", 41590: "Сегодня",
    42979: "Сзади", 32983: "Средневековье", 41430: "Суд", 44108: "Точно",
    29996: "Треск", 29624: "Укол", 41169: "Утёс", 41422: "Фильм",
    41276: "Формулы", 41441: "Электроприборы",
}

# Имена собственные: заглавная законна, НЕ ТРОГАТЬ.
KEEP = {
    33830: "Анхальт — область в Германии",
    41548: "Афины — город",
    41414: "Марокко — страна",
    42397: "Пихлер — фамилия",
    41434: "Санчес — фамилия",
    39869: "Пасха — праздник",
}

# Здесь дело не в регистре: сломан сам перевод или само слово. Регистр им не поможет.
BROKEN = {
    41168: "«Ут» ← Morgen — обрубок слова",
    29461: "«Наезжать» ← Ramme — неверный перевод, Ramme это таран",
    41586: "«Черт» ← Scheise — опечатка в немецком, и перевод грубее оригинала",
    41509: "«Чё» ← Was? — просторечие как словарная статья",
    33792: "«Гарт» ← der Hart — непонятно, что это за слово",
}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    changed = missing = drifted = 0
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            for unit_id, expected in sorted(LOWERCASE.items(), key=lambda kv: kv[1]):
                cur.execute("SELECT display FROM bt_3_lex_units WHERE id = %s AND lang = 'ru';",
                            (unit_id,))
                row = cur.fetchone()
                if not row:
                    print("   %-8s %-22s единицы уже нет" % (unit_id, expected))
                    missing += 1
                    continue
                current = str(row[0] or "")
                if current != expected:
                    # Текст поменялся с момента просмотра — вслепую не трогаем.
                    print("   %-8s ожидали %-20r лежит %r — ПРОПУСК" % (unit_id, expected, current))
                    drifted += 1
                    continue
                fixed = current[:1].lower() + current[1:]
                print("   %-8s %-22s → %s" % (unit_id, current, fixed))
                if args.apply:
                    cur.execute(
                        "UPDATE bt_3_lex_units SET display = %s, updated_at = NOW() WHERE id = %s;",
                        (fixed, unit_id),
                    )
                changed += 1
            if args.apply:
                conn.commit()

    print()
    print("опущено: %d, пропущено (текст изменился): %d, не найдено: %d" % (changed, drifted, missing))
    print()
    print("НЕ ТРОГАЕМ — имена собственные:")
    for unit_id, why in sorted(KEEP.items()):
        print("   %-8s %s" % (unit_id, why))
    print()
    print("НЕ РЕГИСТР — сломан сам перевод, отдельная работа:")
    for unit_id, why in sorted(BROKEN.items()):
        print("   %-8s %s" % (unit_id, why))
    if not args.apply:
        print()
        print("СУХОЙ ПРОГОН. Ничего не записано. Для записи добавь --apply.")


if __name__ == "__main__":
    main()
