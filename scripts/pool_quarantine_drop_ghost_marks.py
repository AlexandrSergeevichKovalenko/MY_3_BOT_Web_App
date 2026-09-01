# -*- coding: utf-8 -*-
"""Снять клеймо «карточка не собралась» с тех слов, у которых разбор ЕСТЬ.

ПОВОД, 01.09.2026. Владелец открыл разбор карантина: 31 слово, и все хорошие — die
Vorgehensweise, ausführlich, zeigen, ankommen, Verschwörer. Оказалось, что клеймо ставил
не ночной добор (в проде он с 27.07.2026 ходит в слой слов и эту метку не вешает вообще),
а ПРОГОН ТЕСТОВ на машине разработчика: рубильника слоя там нет, база боевая, и тест
ночного добора с пустым ответом модели брал из живой базы самое востребованное слово и
метил его. Дыру закрыли замком в `mark_pool_entry_enrich_failed`; этот скрипт убирает то,
что уже накопилось.

ПРАВИЛО ОТБОРА БЕРЁТСЯ ИЗ ПРОДУКТА, а не выдумывается здесь: «разбор есть» — это ровно
то, что покажет человеку `lex_units.lookup`, то есть написание → указатель (surfaces) →
слово с непустым `card`. Поэтому отчёт не может разойтись с экраном.

    python3 scripts/pool_quarantine_drop_ghost_marks.py           # только посмотреть
    python3 scripts/pool_quarantine_drop_ghost_marks.py --apply   # снять клеймо

Скрипт НИЧЕГО НЕ УДАЛЯЕТ: он лишь стирает две служебные метки внутри response_json
(`enrich_attempts`, `enrich_last_reason`). Сами записи и история возвратов остаются.
"""
import os
import sys

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.database import get_db_connection_context          # noqa: E402
from backend.lex_units import normalize_query                   # noqa: E402


def собрать() -> list[dict]:
    """Все строки пула с клеймом + есть ли у слова разбор в слое, откуда его показывают."""
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT id, source_text, target_text,
                       COALESCE(response_json->>'enrich_attempts', '0'),
                       COALESCE(response_json->>'enrich_last_reason', '')
                FROM bt_3_dictionary_entries
                WHERE response_json ? 'enrich_attempts'
                ORDER BY id
                """
            )
            строки = [{"id": r[0], "слово": r[1], "перевод": r[2],
                       "попыток": int(r[3] or 0), "причина": r[4]} for r in (cursor.fetchall() or [])]
            for строка in строки:
                cursor.execute(
                    """
                    SELECT u.display, length(u.card::text)
                    FROM bt_3_lex_surfaces s
                    JOIN bt_3_lex_units u ON u.id = s.unit_id
                    WHERE s.lang = 'de' AND s.surface_key = %s
                      AND u.card IS NOT NULL AND u.card::text <> '{}'
                    ORDER BY length(u.card::text) DESC
                    LIMIT 1
                    """,
                    (normalize_query(строка["слово"]),),
                )
                найдено = cursor.fetchone()
                строка["разбор"] = (найдено[0], int(найдено[1])) if найдено else None
    return строки


def снять(ids: list[int]) -> int:
    if not ids:
        return 0
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE bt_3_dictionary_entries
                SET response_json = (response_json - 'enrich_attempts') - 'enrich_last_reason'
                WHERE id = ANY(%s)
                """,
                (ids,),
            )
            снято = cursor.rowcount
        conn.commit()
    return int(снято or 0)


def main() -> None:
    применять = "--apply" in sys.argv
    строки = собрать()
    с_разбором = [s for s in строки if s["разбор"]]
    без_разбора = [s for s in строки if not s["разбор"]]

    print(f"Строк с клеймом «не собралась»: {len(строки)}")
    print(f"  ├─ разбор ЕСТЬ и человек его видит: {len(с_разбором)}  ← клеймо ложное")
    print(f"  └─ разбора правда нет:              {len(без_разбора)}")
    print()
    print("Разбора нет ни у одного из этих (клеймо остаётся, разбирать владельцу):")
    for s in sorted(без_разбора, key=lambda x: -x["попыток"]):
        print(f"   {s['попыток']}× {s['слово']!r} — {s['перевод']!r}")
    print()
    if not применять:
        print("Сухой прогон. Ничего не изменено. Снять клеймо: --apply")
        return
    снято = снять([s["id"] for s in с_разбором])
    print(f"Клеймо снято с {снято} строк.")


if __name__ == "__main__":
    main()
