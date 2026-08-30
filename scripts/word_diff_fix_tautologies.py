# -*- coding: utf-8 -*-
"""Буквальные переводы примеров: замер и починка накопленного.

ПОВОД. Владелец 30.08.2026, с экрана «anbieten ↔ unterbreiten»:

    Die Firma unterbreitete ein neues Angebot.
    Компания предложила новое предложение.       ← по-русски так не говорят

    Wir möchten Ihnen ein Angebot anbieten.
    Мы хотели бы предложить вам предложение.     ← и так тоже

Немецкий верный, ломается русская сторона: перевод слово в слово дал однокоренной
плеоназм. Ученик читает русскую строку как образец смысла — и учится понимать немецкую
фразу неверно.

Система на будущее закрыта отдельно: правило стоит в промптах, а сторож
(`backend/russian_tautology.py`) считает такие переводы на приёме и пишет их в счётчик
«не смогли» под поводом «tautology», откуда они попадают в недельный отчёт. Этот скрипт
— вторая половина: то, что УЖЕ лежит в базе.

ДВА РЕЖИМА.
  Замер (по умолчанию) — бесплатный. Смотрит `bt_3_word_usage` и `bt_3_word_diff_cards`,
  печатает: сколько переводов подозрительны, в скольких записях, и показывает первые.
  Ничего не пишет и модель не зовёт.

  Починка (--apply) — просит модель переписать ТОЛЬКО помеченные строки
  (`example_translation_repair`, gpt-4.1-mini, одна короткая фраза на вызов) и
  записывает результат. Новый перевод проверяется тем же сторожем: если плеоназм
  остался, строка НЕ подменяется и уходит в отчёт скрипта — врать поверх вранья нельзя.

ЗАПУСК:
    railway run --service BACKEND_WEB python3 scripts/word_diff_fix_tautologies.py
    railway run --service BACKEND_WEB python3 scripts/word_diff_fix_tautologies.py --apply
    ... --apply --limit 50      # починить только первые 50 строк
"""
import asyncio
import os
import sys

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
# Ведомость расходов НЕ отключаем: с --apply этот скрипт делает настоящие обращения к
# модели за настоящие деньги, и они обязаны быть видны в учёте. Заглушка стоит только
# там, где вызовов нет (тесты, замеры).
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from psycopg2.extras import Json  # noqa: E402

from backend.database import get_db_connection_context  # noqa: E402
from backend.russian_tautology import looks_tautological, tautology_pairs  # noqa: E402

APPLY = "--apply" in sys.argv
LIMIT = None
if "--limit" in sys.argv:
    LIMIT = int(sys.argv[sys.argv.index("--limit") + 1])


def db():
    """Соединение с базой, с честными повторами.

    Проверено 30.08.2026: первый коннект к `zephyr.proxy.rlwy.net` из внешней сети
    таймаутится примерно через раз, второй проходит. Это ретрай сети, а не fallback:
    после трёх неудач скрипт падает и НИЧЕГО не делает вида, что отработал.
    """
    last = None
    for attempt in range(3):
        try:
            return get_db_connection_context()
        except Exception as exc:
            last = exc
            print(f"  … база не ответила (попытка {attempt + 1}/3): {str(exc)[:70]}")
    raise last


def spots(payload: dict) -> list[tuple[dict, str, str]]:
    """Места, где лежит пара «немецкий пример → русский перевод».

    Возвращает (запись, ключ немецкого, ключ перевода) — чинить надо ровно ту строку,
    которую человек видит, а лежат они в четырёх разных блоках.
    """
    out = []
    for row in (payload.get("constructions") or []):
        if isinstance(row, dict):
            out.append((row, "example_de", "example_ru"))
    for row in (payload.get("collocations") or []):
        if isinstance(row, dict):
            out.append((row, "phrase", "translation"))
    for row in (payload.get("examples") or []):
        if isinstance(row, dict):
            out.append((row, "de", "translation"))
            contrast = row.get("contrast")
            if isinstance(contrast, dict):
                out.append((contrast, "de", "translation"))
    return out


def repair(sentence: str, current: str) -> str:
    from backend.openai_manager import run_example_translation_repair
    try:
        return asyncio.run(run_example_translation_repair(sentence, current))
    except Exception as exc:
        print(f"    ! модель не ответила: {exc}")
        return ""


def process(table: str, name_column: str, payload_column: str) -> tuple[int, int, int, int]:
    """Пройти одну таблицу. (записей, помеченных строк, починено, осталось кривых).

    Адресуемся по `id`, а не по имени: в `bt_3_word_usage` одна и та же лемма живёт
    в нескольких строках (своя на каждую языковую пару), и UPDATE по lemma_key задел
    бы чужие.
    """
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT id, {name_column}, {payload_column} FROM {table};")
            rows = cur.fetchall()

    records = flagged = fixed = stubborn = 0
    updates: list[tuple[int, dict]] = []
    for row_id, key, payload in rows:
        if not isinstance(payload, dict):
            continue
        records += 1
        changed = False
        for row, de_key, ru_key in spots(payload):
            sentence = " ".join(str(row.get(de_key) or "").split())
            current = " ".join(str(row.get(ru_key) or "").split())
            if not current or not looks_tautological(current):
                continue
            flagged += 1
            if flagged <= 8:
                print(f"  [{table}] {key}\n      DE: {sentence}\n      RU: {current}"
                      f"   ← {tautology_pairs(current)}")
            if not APPLY or (LIMIT is not None and fixed >= LIMIT):
                continue
            fresh = repair(sentence, current)
            if not fresh or fresh == current:
                stubborn += 1
                continue
            if looks_tautological(fresh):
                # Второй заход дал тот же плеоназм. Подменять не будем: это либо
                # ложное срабатывание правила, либо модель не справилась — в обоих
                # случаях врать поверх вранья нельзя.
                stubborn += 1
                print(f"    ! осталось кривым: {fresh}")
                continue
            row[ru_key] = fresh
            fixed += 1
            changed = True
            print(f"    → {fresh}")
        if changed:
            updates.append((row_id, payload))

    if APPLY and updates:
        with db() as conn:
            with conn.cursor() as cur:
                for row_id, payload in updates:
                    cur.execute(
                        f"UPDATE {table} SET {payload_column} = %s WHERE id = %s;",
                        (Json(payload), row_id),
                    )
            conn.commit()
    return records, flagged, fixed, stubborn


def main() -> None:
    total = [0, 0, 0, 0]
    for table, name_column, payload_column in (
        ("bt_3_word_usage", "lemma", "payload"),
        ("bt_3_word_diff_cards", "pair_key", "payload"),
    ):
        print(f"\n── {table} ──")
        result = process(table, name_column, payload_column)
        print(f"  записей: {result[0]}, помечено строк: {result[1]}, "
              f"починено: {result[2]}, осталось кривыми: {result[3]}")
        total = [a + b for a, b in zip(total, result)]

    print(f"\nВСЕГО записей: {total[0]}")
    print(f"подозрительных переводов: {total[1]}")
    if APPLY:
        print(f"переписано: {total[2]}")
        print(f"осталось кривыми (модель не справилась или правило ошиблось): {total[3]}")
    else:
        print("\nЭто замер, ничего не записано и модель не звалась.")
        print("Переписать: добавьте --apply (можно с --limit N для пробы)")


if __name__ == "__main__":
    main()
