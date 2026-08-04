#!/usr/bin/env python3
"""Починка обрезанных слов и шести кривых подписей.

ПРОХОД 1 — обрезанные слова. Лемматизатор spaCy на сохранении откусывал окончание
(«beibringen» → «beibring», «Felge» → «Felg»), и обрубок уезжал в ТЕЛО карточки, которое
и показывает тренировка. Целое слово всё это время лежало рядом — в колонке той же
записи. Проход берёт его оттуда: ни GPT, ни догадок, только собственные данные записи.
Правим лишь случаи, где колонка ПРОДОЛЖАЕТ обрубок ровно на немецкое окончание
(-n/-en/-e/-er/-s/-innen), — всё прочее не наше дело.

ПРОХОД 2 — шесть подписей, согласованных с владельцем 04.08.2026: машинный перевод
поставил под словом то, чего слово не значит («der Tümpel» → «Прудок»).

Запуск:
    python scripts/dictionary_repair_headwords.py           # прогон вхолостую
    python scripts/dictionary_repair_headwords.py --apply   # запись
"""
from __future__ import annotations

import argparse
import json
import sys

sys.path.insert(0, ".")

from backend.database import get_db_connection_context, _normalize_dictionary_text_key  # noqa: E402

GERMAN_ENDINGS = ("innen", "en", "er", "e", "n", "s")

# слово → правильная подпись (пункт 2, согласовано с владельцем)
CAPTION_FIXES = {
    48099: "прудик",
    47591: "душный",
    44335: "непринуждённый",
    46908: "вертолёт",
    48132: "подчинять",
    46187: "высказывания",
}

TRUNCATED_SQL = """
SELECT id, {word_column}, response_json
FROM {table}
WHERE response_json IS NOT NULL
  AND NULLIF(TRIM(response_json->>'word_de'), '') IS NOT NULL
  AND NULLIF(TRIM({word_column}), '') IS NOT NULL
  AND lower(TRIM({word_column})) LIKE lower(TRIM(response_json->>'word_de')) || '%%'
  AND length(TRIM({word_column})) > length(TRIM(response_json->>'word_de'))
ORDER BY id;
"""


def cut_tail(full: str, chopped: str) -> str:
    return full.strip()[len(chopped.strip()):].strip().lower()


def repair_payload(payload: dict, chopped: str, full: str) -> tuple[dict, list[str]]:
    """Меняем обрубок на целое слово ВО ВСЕХ полях карточки, где он стоит один."""
    fixed = dict(payload)
    touched = []
    chopped_key = chopped.strip().casefold()
    for key in ("word_de", "source_text", "target_text", "translation_de", "lemma_de"):
        current = str(fixed.get(key) or "").strip()
        if current and current.casefold() == chopped_key:
            fixed[key] = full.strip()
            touched.append(key)
    return fixed, touched


def run_truncated(cursor, *, table: str, word_column: str, apply: bool) -> tuple[int, list]:
    cursor.execute(TRUNCATED_SQL.format(table=table, word_column=word_column))
    rows = cursor.fetchall() or []
    repaired = 0
    samples = []
    for row_id, full_word, payload in rows:
        payload = payload if isinstance(payload, dict) else json.loads(payload or "{}")
        chopped = str(payload.get("word_de") or "").strip()
        if not chopped or not full_word:
            continue
        tail = cut_tail(full_word, chopped)
        if tail not in GERMAN_ENDINGS:
            continue  # не окончание, а другое слово — не наш случай
        fixed, touched = repair_payload(payload, chopped, full_word)
        if not touched:
            continue
        repaired += 1
        if len(samples) < 12:
            samples.append((row_id, chopped, full_word.strip(), touched))
        if apply:
            cursor.execute(
                f"UPDATE {table} SET response_json = %s WHERE id = %s",
                (json.dumps(fixed, ensure_ascii=False), row_id),
            )
    return repaired, samples


def run_captions(cursor, *, apply: bool) -> list[str]:
    lines = []
    for entry_id, caption in CAPTION_FIXES.items():
        cursor.execute(
            "SELECT source_lang, target_lang, source_text, target_text, response_json"
            " FROM bt_3_dictionary_entries WHERE id = %s",
            (entry_id,),
        )
        row = cursor.fetchone()
        if not row:
            lines.append(f"   #{entry_id}: записи нет, пропускаю")
            continue
        source_lang, target_lang, source_text, old_caption, payload = row
        payload = payload if isinstance(payload, dict) else json.loads(payload or "{}")
        caption_norm = _normalize_dictionary_text_key(caption)
        cursor.execute(
            """
            SELECT id FROM bt_3_dictionary_entries
            WHERE source_lang = %s AND target_lang = %s
              AND source_text_norm = (SELECT source_text_norm FROM bt_3_dictionary_entries WHERE id = %s)
              AND target_text_norm = %s AND id <> %s
            """,
            (source_lang, target_lang, entry_id, caption_norm, entry_id),
        )
        if cursor.fetchone():
            lines.append(f"   #{entry_id}: такая подпись уже есть отдельной записью, пропускаю")
            continue
        lines.append(f"   #{entry_id} {source_text!r}: {old_caption!r} -> {caption!r}")
        if not apply:
            continue
        for key in ("target_text", "translation_ru", "word_ru"):
            if str(payload.get(key) or "").strip():
                payload[key] = caption
        cursor.execute(
            """
            UPDATE bt_3_dictionary_entries
            SET target_text = %s, target_text_norm = %s, target_headword_norm = %s,
                word_ru = %s, translation_ru = %s, response_json = %s, updated_at = NOW()
            WHERE id = %s
            """,
            (caption, caption_norm, caption_norm, caption, caption,
             json.dumps(payload, ensure_ascii=False), entry_id),
        )
        # личные карточки, где стоит та же старая подпись
        cursor.execute(
            """
            UPDATE bt_3_webapp_dictionary_queries
            SET word_ru = %s, translation_ru = %s
            WHERE canonical_entry_id = %s
              AND lower(TRIM(COALESCE(translation_ru, ''))) = lower(TRIM(%s))
            """,
            (caption, caption, entry_id, old_caption or ""),
        )
        if cursor.rowcount:
            lines.append(f"      личных карточек поправлено: {cursor.rowcount}")
    return lines


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            personal, personal_samples = run_truncated(
                cursor, table="bt_3_webapp_dictionary_queries", word_column="word_de", apply=args.apply
            )
            pool, pool_samples = run_truncated(
                cursor, table="bt_3_dictionary_entries", word_column="word_de", apply=args.apply
            )
            caption_lines = run_captions(cursor, apply=args.apply)
            if args.apply:
                conn.commit()

    print("ЗАПИСАНО" if args.apply else "ПРОГОН ВХОЛОСТУЮ")
    print(f"\n1. Обрезанные слова: личных карточек {personal}, строк общего словаря {pool}")
    for row_id, chopped, full, touched in personal_samples + pool_samples:
        print(f"   #{row_id} {chopped!r} -> {full!r} (поля: {', '.join(touched)})")
    print("\n2. Подписи:")
    for line in caption_lines:
        print(line)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
