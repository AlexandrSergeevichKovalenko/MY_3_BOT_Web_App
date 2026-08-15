# -*- coding: utf-8 -*-
"""Разовый прогон: пересчитать ВИД записи новым правилом и вернуть стёртые значения.

Зачем
─────
До 14.08.2026 `_detect_dictionary_entry_kind` судила о записи по ИСХОДНОЙ стороне.
Когда человек ищет от русского, исходная сторона — русский перевод, а он почти всегда
многословный. Поэтому одиночное немецкое слово штатно объявлялось «фразой», и дальше
срабатывали два следствия:
  • артикль не приклеивался к заголовку («Прорыв водопроводной трубы» → `Rohrbruch`
    вместо `der Rohrbruch`);
  • разбивка по значениям стиралась перед записью.

Причина починена в самом детекторе, стирание снято. Этот скрипт разбирает НАКОПЛЕННОЕ.

Что делает
──────────
Для каждой личной карточки заново спрашивает продуктовые функции — не свои копии:
`_detect_dictionary_entry_kind` (новое правило) и `_build_dictionary_senses`. Если вид
изменился на «слово», записывает новый вид и собранные значения.

Значения НЕ выдумываются и НЕ запрашиваются у модели: `_build_dictionary_senses` —
чистая функция от блоков `meanings` / `translations`, которые всё это время лежали
в самой записи. Замер 13.08.2026: материал цел у 2949 карточек из 2961.

Ничего не удаляет. Только дописывает то, чего не хватало.

    python3 scripts/dict_fix_entry_kind_and_senses.py            # сухой прогон, по умолчанию
    python3 scripts/dict_fix_entry_kind_and_senses.py --apply    # записать
"""
from __future__ import annotations

import argparse
import json
import os
import sys

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SKIP_BILLING_LEDGER_WRITES", "1")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.database import get_db_connection_context                      # noqa: E402
from backend.backend_server import (                                        # noqa: E402
    _detect_dictionary_entry_kind,
    _build_dictionary_senses,
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="записать изменения (без него — сухой прогон)")
    parser.add_argument("--limit", type=int, default=0, help="ограничить число карточек (0 = все)")
    args = parser.parse_args()

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, user_id, word_de, word_ru, source_lang, target_lang, response_json
                FROM bt_3_webapp_dictionary_queries
                WHERE response_json->>'entry_kind' IN ('phrase', 'sentence')
                ORDER BY id;
                """
            )
            rows = cur.fetchall()

        print("карточек, помеченных фразой или предложением: %d" % len(rows))

        changed_kind = 0
        added_senses = 0
        updates: list[tuple] = []
        samples: list[str] = []

        for entry_id, user_id, word_de, word_ru, source_lang, target_lang, payload in rows:
            if not isinstance(payload, dict) or not payload:
                continue
            source_text = str(payload.get("source_text") or word_ru or word_de or "").strip()
            target_text = str(payload.get("target_text") or "").strip()
            if not source_text:
                continue

            fresh_kind = _detect_dictionary_entry_kind(
                source_text=source_text,
                target_text=target_text,
                source_lang=str(source_lang or payload.get("source_lang") or ""),
                target_lang=str(target_lang or payload.get("target_lang") or ""),
                response_json=payload,
            )
            if fresh_kind == str(payload.get("entry_kind") or ""):
                continue  # вид не изменился — трогать нечего

            new_payload = dict(payload)
            new_payload["entry_kind"] = fresh_kind
            changed_kind += 1

            senses = []
            if fresh_kind == "word":
                senses = _build_dictionary_senses(new_payload, target_text)
                if senses:
                    new_payload["dictionary_senses"] = senses
                    added_senses += 1

            if len(samples) < 15:
                samples.append("   %-26s %s → %s, значений: %d" % (
                    str(word_de or source_text)[:26], payload.get("entry_kind"), fresh_kind, len(senses)))
            updates.append((json.dumps(new_payload, ensure_ascii=False), int(entry_id)))
            if args.limit and len(updates) >= args.limit:
                break

        print("  вид записи изменится у: %d" % changed_kind)
        print("  из них вернутся значения у: %d" % added_senses)
        print("\n  примеры:")
        for line in samples:
            print(line)

        if not args.apply:
            print("\nСУХОЙ ПРОГОН. Ничего не записано. Для записи добавь --apply.")
            return

        with conn.cursor() as cur:
            for payload_json, entry_id in updates:
                cur.execute(
                    "UPDATE bt_3_webapp_dictionary_queries SET response_json = %s::jsonb WHERE id = %s;",
                    (payload_json, entry_id),
                )
        conn.commit()
        print("\nЗаписано карточек: %d" % len(updates))


if __name__ == "__main__":
    main()
