# -*- coding: utf-8 -*-
"""Развести карточки, в которых лежит несколько предложений, — по одной на каждое.

Владелец 11.08.2026 просмотрел список глазами и решил по каждой записи отдельно.
Здесь только те пять, что он утвердил, и список зашит: правила, которое отличает
«Was hast du vor? Was ziehst du vor?» (три разных вопроса) от «Hast du Zeit? – Jein.»
(вопрос и ответ), у нас нет и не будет — это решает человек.

Главное, почему это чинится честно, а не наугад: у всех пяти РУССКАЯ сторона тоже
разбита на части, и в том же порядке. Значит перевод для каждого предложения уже
написан, и выдумывать его моделью не нужно:

    RU: Что тебя раздражает? | На что ты злишься?
    DE: Was ärgert dich?     | Worüber bist du verärgert?

Первая часть остаётся в ИСХОДНОЙ записи (id сохраняется — к ней привязаны единицы
слоя и чужие карточки повторения), остальные заводятся отдельными карточками.

Запуск:
    DATABASE_URL=... python3 scripts/dict_split_multi_phrase_cards.py
    DATABASE_URL=... python3 scripts/dict_split_multi_phrase_cards.py --apply
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys

import psycopg2

# id → что именно с ней делать. Опечатки правим здесь же: владелец заметил, что в
# третьем вопросе записи 21271 стоит «as» вместо «Was».
APPROVED: dict[int, dict] = {
    21271: {"fix": {"as schlägst du vor?": "Was schlägst du vor?"}},
    20825: {},
    19624: {},
    19230: {},
    19170: {},
}

# Границы предложений: точка с запятой ИЛИ конец предложения перед заглавной буквой.
_SPLIT_RE = re.compile(r"\s*;\s*|(?<=[.!?])\s+")


def split_parts(text: str) -> list[str]:
    parts = [p.strip() for p in _SPLIT_RE.split(str(text or "")) if p and p.strip()]
    return parts


def normalize_head(text: str) -> str:
    """Первая буква карточки — заглавная. «ich kann das aushalten» второй половиной
    записи стоял со строчной, а самостоятельной карточке так начинаться незачем."""
    value = str(text or "").strip()
    if value and value[0].islower():
        return value[0].upper() + value[1:]
    return value


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        print("Нужен DATABASE_URL", file=sys.stderr)
        sys.exit(1)
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    conn = psycopg2.connect(dsn, connect_timeout=25)
    conn.autocommit = False
    made = 0
    updated = 0

    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, source_lang, target_lang, source_text, target_text, response_json "
            "FROM bt_3_dictionary_entries WHERE id = ANY(%s) ORDER BY id;",
            (list(APPROVED.keys()),),
        )
        rows = cur.fetchall()
        found_ids = {row[0] for row in rows}
        for missing in set(APPROVED) - found_ids:
            print(f"[{missing}] записи нет — пропускаю")

        for entry_id, s_lang, t_lang, source_text, target_text, payload in rows:
            rules = APPROVED[entry_id]
            german_raw = str(target_text or "")
            for wrong, right in (rules.get("fix") or {}).items():
                german_raw = german_raw.replace(wrong, right)

            german = split_parts(german_raw)
            russian = split_parts(source_text)
            if len(german) < 2:
                print(f"[{entry_id}] делить нечего — уже одна фраза")
                continue
            if len(russian) < len(german):
                # Переводов меньше, чем фраз: выдумывать нечем, и молча ставить
                # общий перевод обеим карточкам нельзя — это и есть та болезнь.
                print(f"[{entry_id}] ПРОПУСК: {len(german)} фраз, а переводов {len(russian)}")
                continue

            # Заглавная нужна ОБЕИМ сторонам: «совершать посягательство» и «как ты
            # предлагаешь?» были серединой записи, а теперь это начало своей карточки.
            pairs = [(normalize_head(german[i]), normalize_head(russian[i]))
                     for i in range(len(german))]
            print(f"\n[{entry_id}]  было: {german_raw[:90]}")
            for index, (de, ru) in enumerate(pairs):
                mark = "остаётся в этой записи" if index == 0 else "новая карточка"
                print(f"   {mark}:  {de}")
                print(f"                    {ru}")
            if len(russian) > len(german):
                print(f"   (лишних переводов: {len(russian) - len(german)} — не пропали, "
                      f"просто немецкой пары для них нет)")

            if not args.apply:
                continue

            def payload_for(de: str, ru: str) -> dict:
                data = dict(payload) if isinstance(payload, dict) else {}
                data.update({
                    "source_text": ru, "target_text": de,
                    "word_ru": ru, "translation_ru": ru,
                    "word_de": de, "translation_de": de,
                })
                return data

            head_de, head_ru = pairs[0]
            cur.execute(
                """
                UPDATE bt_3_dictionary_entries
                SET source_text = %s, target_text = %s, word_ru = %s, translation_ru = %s,
                    word_de = %s, translation_de = %s, response_json = %s, updated_at = NOW()
                WHERE id = %s;
                """,
                (head_ru, head_de, head_ru, head_ru, head_de, head_de,
                 json.dumps(payload_for(head_de, head_ru), ensure_ascii=False), entry_id),
            )
            updated += 1

            from backend.database import upsert_dictionary_pool_entry
            for de, ru in pairs[1:]:
                try:
                    upsert_dictionary_pool_entry(
                        source_lang=s_lang, target_lang=t_lang,
                        source_text=ru, target_text=de,
                        word_ru=ru, translation_ru=ru,
                        word_de=de, translation_de=de,
                        response_json=payload_for(de, ru),
                    )
                    made += 1
                except Exception as exc:
                    print(f"   ! карточка не завелась ({de[:40]}): {exc}")

    if args.apply:
        conn.commit()
        print(f"\n✓ Обновлено записей: {updated}. Заведено новых карточек: {made}.")
    else:
        conn.rollback()
        print("\nЭто отчёт, база не изменена. Применить: --apply")
    conn.close()


if __name__ == "__main__":
    main()
