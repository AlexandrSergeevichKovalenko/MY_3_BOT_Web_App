# -*- coding: utf-8 -*-
"""Снять клеймо «карточка не собралась» со ВСЕХ строк старого банка словаря.

ПОВОД, 01.09.2026 → 04.09.2026. Дважды владелец открывал «Карантин пула» и дважды видел
там хорошие слова (die Vorgehensweise, zeigen, halten, Pferde) с подписью «ночь не смогла
собрать разбор». Ночь их не пробовала ни разу: в проде она с 27.07.2026 ходит в слой слов
(журнал прогона: mode=units), а единственная функция, ставившая метку, жила в ветке по
старому банку, которая в проде не выполняется. Метки ставили прогоны тестов с машины
разработчика.

01.09 сняли 162 метки, а 6 оставили «потому что разбора нет». Это был неверный вывод:
происхождение метки у всех одно, значит ложны все. Правило теперь простое и без
исключений: ЛЮБАЯ метка старого банка — призрак. Экран карантина, его счётчик и сама
функция-клеймо удалены 04.09.2026; этот скрипт убирает то, что от них осталось в данных.

    python3 scripts/pool_quarantine_drop_ghost_marks.py           # только посмотреть
    python3 scripts/pool_quarantine_drop_ghost_marks.py --apply   # снять метки

Скрипт НИЧЕГО НЕ УДАЛЯЕТ: стирает служебные ключи внутри response_json
(`enrich_attempts`, `enrich_last_reason`, `quarantine_releases`, `quarantine_released_at`,
`quarantine_owner_keep`, `quarantine_owner_keep_at`). Записи остаются.

Обещание «меток в старом банке: 0» проверяется каждое утро — backend/fix_promises.py.
"""
import os
import sys

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.database import get_db_connection_context          # noqa: E402

КЛЮЧИ = ("enrich_attempts", "enrich_last_reason", "quarantine_releases",
         "quarantine_released_at", "quarantine_owner_keep", "quarantine_owner_keep_at")
_УСЛОВИЕ = " OR ".join(f"response_json ? '{k}'" for k in КЛЮЧИ)


def собрать() -> list[dict]:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT id, source_text, target_text,
                       COALESCE(response_json->>'enrich_attempts', '0'),
                       COALESCE(response_json->>'enrich_last_reason', '')
                FROM bt_3_dictionary_entries
                WHERE {_УСЛОВИЕ}
                ORDER BY id
                """
            )
            return [{"id": r[0], "слово": r[1], "перевод": r[2],
                     "попыток": int(r[3] or 0), "причина": r[4]}
                    for r in (cursor.fetchall() or [])]


def снять(ids: list[int]) -> int:
    if not ids:
        return 0
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "UPDATE bt_3_dictionary_entries SET response_json = response_json - %s::text[] "
                "WHERE id = ANY(%s)",
                (list(КЛЮЧИ), ids),
            )
            снято = cursor.rowcount
        conn.commit()
    return int(снято or 0)


def main() -> None:
    применять = "--apply" in sys.argv
    строки = собрать()
    print(f"Строк старого банка со следом карантина: {len(строки)} — все призрачные.")
    for s in sorted(строки, key=lambda x: -x["попыток"]):
        print(f"   {s['попыток']}× {s['слово']!r} — {s['перевод']!r} [{s['причина']}]")
    if not применять:
        print("\nСухой прогон. Ничего не изменено. Снять: --apply")
        return
    снято = снять([s["id"] for s in строки])
    print(f"\nМетки сняты с {снято} строк. Осталось со следом: {len(собрать())}")


if __name__ == "__main__":
    main()
