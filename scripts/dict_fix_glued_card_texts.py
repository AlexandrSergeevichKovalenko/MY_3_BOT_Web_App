# -*- coding: utf-8 -*-
"""Починка карточек, где ответ виден на лицевой стороне повторения.

ЧТО СЛОМАНО. В `response_json` личной карточки лежит склейка «немецкая фраза — русский
перевод», а колонки таблицы при этом чистые. Показ берёт РАЗБОР, а не колонку
(`_resolve_training_entry_texts_for_pair`, backend_server.py:20818), поэтому на лицевой
стороне человек видит и вопрос, и ответ сразу.

ЧТО ДЕЛАЕМ. Заменяем испорченное поле разбора на чистое значение ИЗ КОЛОНКИ — и только
тогда, когда чистое значение целиком содержится внутри испорченного. Это гарантия без
потерь: мы ничего не придумываем и не выбрасываем того, чего нет в колонке. Всё, что под
это правило не подошло, скрипт не трогает и печатает отдельным списком.

Плюс снимаем дубль «X - X» (одна и та же фраза, повторённая сама с собой) — там обе
половины дословно совпадают, поэтому удаление второй тоже без потерь.

Колонки таблицы НЕ меняются: они уже верные.
Перед записью старые значения складываются в файл — правку можно откатить.

Запуск:
    DATABASE_URL=... python3 scripts/dict_fix_glued_card_texts.py --dry-run
    DATABASE_URL=... python3 scripts/dict_fix_glued_card_texts.py --apply --backup путь.json
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys

import psycopg2
import psycopg2.extras

LAT = "[A-Za-z]"
CYR = "[А-Яа-яЁё]"
SEP = " (—|–|-) "
SPACE_RE = re.compile(r"\s+")

# Ключ разбора → колонка, из которой берётся чистое значение.
KEY_TO_COLUMN = {
    "word_ru": "word_ru",
    "translation_ru": "translation_ru",
    "word_de": "word_de",
    "translation_de": "translation_de",
}
DUP_SEPARATORS = (" - ", " — ", " – ")


def norm(text: str | None) -> str:
    return SPACE_RE.sub(" ", str(text or "").strip())


def undouble(text: str | None) -> str:
    """«X - X» → «X». Только когда половины совпадают дословно."""
    value = norm(text)
    for sep in DUP_SEPARATORS:
        if sep in value:
            left, _, right = value.partition(sep)
            if norm(left) and norm(left) == norm(right):
                return norm(left)
    return value


def clean_for_key(key: str, row: dict) -> str:
    """Чистое значение для ключа разбора: из колонки, со снятым дублем."""
    column = KEY_TO_COLUMN.get(key)
    if column:
        return undouble(row.get(column))
    # source_text / target_text — зависят от направления карточки.
    source_lang = norm(row.get("source_lang")).lower() or "ru"
    if key == "source_text":
        return undouble(row.get("word_de") if source_lang == "de" else row.get("word_ru"))
    return undouble(row.get("word_ru") if source_lang == "de" else row.get("word_de"))


def plan_for_row(row: dict) -> tuple[dict, list[str]]:
    """Возвращает (что заменить, что не поддалось)."""
    payload = row.get("response_json")
    payload = payload if isinstance(payload, dict) else {}
    changes: dict[str, str] = {}
    skipped: list[str] = []
    for key in ("source_text", "target_text", "word_ru", "word_de",
                "translation_ru", "translation_de"):
        current = norm(payload.get(key))
        if not current:
            continue
        clean = clean_for_key(key, row)
        if not clean or clean == current:
            continue
        # Гарантия без потерь: чистое значение целиком лежит внутри испорченного…
        if clean in current:
            changes[key] = clean
            continue
        # …либо испорченное — это дубль самого себя.
        collapsed = undouble(current)
        if collapsed != current:
            changes[key] = collapsed
            continue
        skipped.append(f"{key}: {current[:70]}")
    return changes, skipped


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="записать правки в базу")
    parser.add_argument("--dry-run", action="store_true", help="только показать (по умолчанию)")
    parser.add_argument("--backup", default="", help="файл для старых значений (обязателен с --apply)")
    args = parser.parse_args()
    apply = bool(args.apply) and not args.dry_run
    if apply and not args.backup:
        print("с --apply нужен --backup путь.json"); return 2

    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        print("нет DATABASE_URL"); return 2

    conn = psycopg2.connect(dsn)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        f"""
        SELECT id, user_id, source_lang, target_lang, origin_process,
               word_ru, word_de, translation_ru, translation_de, response_json
        FROM bt_3_webapp_dictionary_queries
        WHERE ((response_json->>'source_text') ~ %s AND (response_json->>'source_text') ~ %s
               AND NOT (COALESCE(word_ru,'') ~ %s AND COALESCE(word_ru,'') ~ %s)
               AND (response_json->>'source_text') ~ %s)
           OR (position(' - ' in COALESCE(word_de,'')) > 0
               AND btrim(split_part(word_de,' - ',1)) = btrim(split_part(word_de,' - ',2)))
        ORDER BY id;
        """,
        (LAT, CYR, LAT, CYR, SEP),
    )
    rows = cur.fetchall()
    print(f"кандидатов: {len(rows)}")

    backup: list[dict] = []
    fixed = 0
    untouched: list[tuple[int, list[str]]] = []
    for row in rows:
        changes, skipped = plan_for_row(row)
        if skipped:
            untouched.append((row["id"], skipped))
        if not changes:
            continue
        fixed += 1
        print(f"\n— карточка {row['id']} ({row.get('origin_process') or 'нет двери'})")
        for key, value in changes.items():
            was = norm((row["response_json"] or {}).get(key))
            print(f"    {key}\n      было : {was[:96]}\n      стало: {value[:96]}")
        if apply:
            backup.append({"id": row["id"], "response_json": row["response_json"]})
            payload = dict(row["response_json"] or {})
            payload.update(changes)
            cur.execute(
                "UPDATE bt_3_webapp_dictionary_queries SET response_json = %s WHERE id = %s;",
                (psycopg2.extras.Json(payload), row["id"]),
            )

    print(f"\nитого к починке: {fixed} из {len(rows)}")
    if untouched:
        print(f"\nНЕ ТРОГАЮ (чистое значение не подтверждается колонкой) — {len(untouched)} карточек:")
        for card_id, items in untouched[:40]:
            print(f"  {card_id}: " + "; ".join(items))

    if apply:
        with open(args.backup, "w", encoding="utf-8") as fh:
            json.dump(backup, fh, ensure_ascii=False, indent=1, default=str)
        conn.commit()
        print(f"\nзаписано. старые значения: {args.backup}")
    else:
        conn.rollback()
        print("\nсухой прогон — база не менялась")
    cur.close(); conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
