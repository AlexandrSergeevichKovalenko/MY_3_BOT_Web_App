"""Облегчить подписные карточки, которые несут копию разбора вместо ссылки на слово.

Правило продукта: подписка — это ДОСТУП к слову, а не копия. Разбор живёт на слове,
общий для всех, и когда его дополняют — обновление видят сразу все подписчики. Копия
это правило ломает: человек навсегда остаётся с той версией разбора, которая была в
момент подписки.

Код это уже соблюдает: копию кладут только когда на слове разбора ЕЩЁ НЕТ (иначе
подписчик получил бы пустую карточку). Но 16 карточек, заведённых 05.08.2026, копию
несут, хотя у их слов разбор есть: он приехал на слово ПОЗЖЕ, чем прошла подписка.
Задним числом такие копии никто не убирал — этот проход убирает.

Что остаётся в карточке: опознавательные поля (заголовок, артикль, часть речи,
направление) и личные заметки. Разбор приезжает со слова при показе — ровно тот же
механизм, которым уже пользуются 12 тысяч карточек.

Трогаем ТОЛЬКО подписные (`origin_process = 'subscription'`) и только там, где у слова
разбор действительно есть. Своя карточка человека, купленная им самим, не трогается
никогда.

По умолчанию НИЧЕГО НЕ ПИШЕТ. Запись — только с --apply.

    python scripts/dict_subscription_drop_heavy_copies.py           # вхолостую
    python scripts/dict_subscription_drop_heavy_copies.py --apply   # записать
"""

from __future__ import annotations

import argparse
import json
import os
import sys

_here = os.path.dirname(os.path.abspath(globals().get("__file__", ".")))
sys.path.insert(0, os.path.join(_here, ".."))
sys.path.insert(0, os.path.join(_here, "..", "backend"))
sys.path.insert(0, "/app/backend")

from database import get_db_connection_context, strip_card_content_for_subscription  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT q.id, q.user_id, q.word_de, q.response_json,
                          (SELECT count(*) FROM jsonb_object_keys(q.response_json)) AS fields
                   FROM bt_3_webapp_dictionary_queries q
                   JOIN bt_3_lex_units u ON u.id = q.lex_unit_id
                   WHERE q.origin_process = 'subscription'
                     AND u.card IS NOT NULL
                     AND q.response_json IS NOT NULL
                   ORDER BY q.id;"""
            )
            rows = cur.fetchall()
            work = []
            for cid, user_id, word_de, payload, fields in rows:
                light = strip_card_content_for_subscription(payload if isinstance(payload, dict) else {})
                if len(light) < int(fields or 0):
                    work.append((cid, user_id, word_de, light, int(fields or 0)))

            print("ПОДПИСНЫХ КАРТОЧЕК С КОПИЕЙ РАЗБОРА: %d" % len(work))
            for cid, user_id, word_de, light, fields in work[:20]:
                print("   card=%s человек=%s %r: %d полей → %d"
                      % (cid, user_id, (word_de or "")[:35], fields, len(light)))

            if not args.apply:
                print()
                print("ВХОЛОСТУЮ. Записать: --apply")
                return 0

            for cid, _user_id, _word_de, light, _fields in work:
                cur.execute(
                    "UPDATE bt_3_webapp_dictionary_queries SET response_json = %s::jsonb WHERE id = %s;",
                    (json.dumps(light, ensure_ascii=False), cid),
                )
            conn.commit()
    print()
    print("ОБЛЕГЧЕНО КАРТОЧЕК: %d — теперь они читают общий разбор и получают обновления"
          % len(work))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
