# -*- coding: utf-8 -*-
"""ХВОСТ ВТОРОЙ: 92 спорные карточки панели уходят владельцу — в ОДНУ существующую очередь.

ЧТО СЛУЧИЛОСЬ. Проход по 5 073 фразам тремя голосами дал 92 карточки, где голоса
разошлись и большинства нет. Машина по ним исчерпала себя: дальше решает человек.

ПОЧЕМУ НЕ НОВЫЙ КАНАЛ. У владельца уже шесть очередей с кнопками (артикли, слова без
справочника, формы, снятие с учёта, спорные фразы, разбор новостей). Седьмая означала бы
седьмое место, куда надо не забыть заглянуть. Спорные фразы уже разбираются экраном
`bt_3_phrase_review` — туда и кладём, тем же форматом судейских мнений.

⚠ ОДНА ОТКРЫТАЯ ЗАПИСЬ НА СЛОВО. В таблице стоит уникальный индекс по unit_id для
status='open': если фраза уже ждёт решения по другому поводу, второй вопрос про неё не
заводим — иначе владелец увидит одно и то же дважды и перестанет доверять очереди.

    python3 scripts/dict_panel_disputes_to_owner.py            # показать, не писать
    python3 scripts/dict_panel_disputes_to_owner.py --apply    # поставить в очередь
"""
from __future__ import annotations

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SKIP_BILLING_LEDGER_WRITES", "1")

from backend.database import get_db_connection_context      # noqa: E402

DISPUTED = "спорное"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT c.unit_id, u.display, u.card->>'translation_ru', c.reference
                FROM bt_3_field_checks c
                JOIN bt_3_lex_units u ON u.id = c.unit_id
                WHERE c.field = 'phrase_panel' AND c.verdict = %s
                  AND NOT EXISTS (SELECT 1 FROM bt_3_phrase_review r
                                  WHERE r.unit_id = c.unit_id AND r.status = 'open')
                ORDER BY c.unit_id;""", (DISPUTED,))
            rows = cur.fetchall()

            cur.execute("""SELECT count(*) FROM bt_3_field_checks c
                           WHERE c.field='phrase_panel' AND c.verdict=%s
                             AND EXISTS (SELECT 1 FROM bt_3_phrase_review r
                                         WHERE r.unit_id=c.unit_id AND r.status='open');""",
                        (DISPUTED,))
            already = cur.fetchone()[0]

    print(f"спорных к постановке: {len(rows)}")
    print(f"уже ждут решения по другому поводу (не дублируем): {already}\n")

    for unit_id, display, translation, why in rows[:12]:
        print(f"   [{unit_id}] {str(display)[:44]:46} {str(why or '')[:52]}")

    if not args.apply:
        print("\n(холостой прогон: ничего не записано, нужен --apply)")
        return 0

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            for unit_id, display, translation, why in rows:
                # Формат judges тот же, что у ночной проверки фраз: список мнений.
                # Здесь мнение одно — сводка панели, и в нём честно сказано, что
                # большинства не набралось.
                judges = [{"verdict": "doubt", "category": "панель из трёх голосов",
                           "corrected": "", "why": str(why or "голоса разошлись")[:400]}]
                cur.execute("""
                    INSERT INTO bt_3_phrase_review (unit_id, text, translation, judges, status)
                    VALUES (%s, %s, %s, %s::jsonb, 'open')
                    ON CONFLICT DO NOTHING;""",
                            (unit_id, str(display or "")[:500], str(translation or "")[:500],
                             json.dumps(judges, ensure_ascii=False)))
            conn.commit()
    print(f"\nпоставлено в очередь владельца: {len(rows)}")
    print("они придут в тот же разбор спорных фраз, отдельного канала не заводим")
    return 0


if __name__ == "__main__":
    sys.exit(main())
