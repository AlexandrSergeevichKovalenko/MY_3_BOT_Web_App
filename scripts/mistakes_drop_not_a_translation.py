"""Убирает из списков ошибок записи, которые ошибками не являются.

Замер 18.08.2026 по живой базе: у настоящих людей из 293 записей «тип ошибки не
определён» 270 стояли за ответами вроде «Оашв», «Чттч», «Ich weiß nicht» или текстом
по-русски — то есть человек вписал в поле перевода не перевод. Эти записи ложились в
список ошибок и возвращались ему как задания, вытесняя настоящие ошибки: у одного
человека из 59 накопленных ошибок 58 были такими, и его набор переставал содержать
новые предложения.

Правило отбора — то же самое, что теперь стоит стражем на входе
(`_log_translation_mistake_with_cursor` в backend/translation_workflow.py), и оба его
признака дала модель, а не наша арифметика:
  1) балл 0 — её вердикт «пусто или не по делу»;
  2) пара категорий «Other mistake / Unclassified mistake» — ни одного названного типа.
Порознь признаки не годятся: 68 записей с нулём оказались живыми обрывочными попытками
(«7. Auch wenn für uns schwierig war»), и типы ошибок модель у них назвала — такие
записи правило не трогает.

    python scripts/mistakes_drop_not_a_translation.py            # только показать
    python scripts/mistakes_drop_not_a_translation.py --apply    # удалить

Перед удалением всё, что уходит, выгружается в JSON рядом со скриптом — чтобы удаление
можно было разобрать потом, а не восстанавливать по памяти.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Живая база — zephyr; приложение читает эту же переменную (backend/database.py).
# Локальный DATABASE_URL указывает на старый мёртвый хост, брать его нельзя.
DSN_ENV = "DATABASE_URL_RAILWAY"

SELECT_DOOMED = """
SELECT d.id, d.user_id, d.sentence_id, d.score, d.mistake_count,
       COALESCE(d.first_seen, d.added_data) AS created,
       LEFT(d.sentence, 90) AS sentence
FROM bt_3_detailed_mistakes d
JOIN bt_3_user_identity u ON u.user_id = d.user_id
WHERE d.score = 0
  AND lower(COALESCE(NULLIF(d.main_category, ''), 'Other mistake'))
      IN ('other mistake', 'other mistakes')
  AND lower(COALESCE(NULLIF(d.sub_category, ''), 'Unclassified mistake'))
      IN ('unclassified mistake', 'unclassified mistakes')
ORDER BY d.user_id, created;
"""

# Контроль: сколько записей правило НЕ трогает, хотя балл у них тоже ноль.
SELECT_SPARED = """
SELECT COUNT(*)
FROM bt_3_detailed_mistakes d
JOIN bt_3_user_identity u ON u.user_id = d.user_id
WHERE d.score = 0
  AND lower(COALESCE(NULLIF(d.sub_category, ''), 'Unclassified mistake'))
      NOT IN ('unclassified mistake', 'unclassified mistakes');
"""

SELECT_TOTALS = """
SELECT COUNT(*) FILTER (WHERE TRUE) AS all_rows,
       COUNT(*) FILTER (WHERE lower(COALESCE(NULLIF(d.sub_category,''),'Unclassified mistake'))
             IN ('unclassified mistake','unclassified mistakes')) AS unclassified
FROM bt_3_detailed_mistakes d
JOIN bt_3_user_identity u ON u.user_id = d.user_id;
"""


def main(apply: bool = False) -> None:
    import psycopg2

    dsn = os.getenv(DSN_ENV)
    if not dsn:
        sys.exit(f"нет {DSN_ENV} в окружении")

    conn = psycopg2.connect(dsn)
    try:
        with conn.cursor() as cursor:
            cursor.execute(SELECT_TOTALS)
            all_rows, unclassified = cursor.fetchone()
            cursor.execute(SELECT_DOOMED)
            doomed = cursor.fetchall()
            cursor.execute(SELECT_SPARED)
            spared = cursor.fetchone()[0]

        print("── что нашлось у настоящих людей ──")
        print(f"   всего записей об ошибках      : {all_rows}")
        print(f"   из них «тип не определён»     : {unclassified}")
        print(f"   под правило подпадает         : {len(doomed)}")
        print(f"   пощажено (ноль, но тип назван): {spared}")

        by_user: dict[int, int] = {}
        for _id, user_id, *_rest in doomed:
            by_user[int(user_id)] = by_user.get(int(user_id), 0) + 1
        print("\n── по людям ──")
        for user_id, count in sorted(by_user.items(), key=lambda item: -item[1]):
            print(f"   user {user_id:<12} {count:>4}")

        print("\n── примеры (первые 8) ──")
        for _id, user_id, _sid, score, count, created, sentence in doomed[:8]:
            print(f"   user {user_id} · балл {score} · повторов {count} · {str(created)[:10]}")
            print(f"      {sentence}")

        if not doomed:
            print("\nудалять нечего.")
            return

        if not apply:
            print("\nЭто показ. Чтобы удалить: --apply")
            return

        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "data", f"dropped_not_a_translation_{stamp}.json"
        )
        os.makedirs(os.path.dirname(backup_path), exist_ok=True)
        with open(backup_path, "w", encoding="utf-8") as handle:
            json.dump(
                [
                    {
                        "id": row[0], "user_id": row[1], "sentence_id": row[2], "score": row[3],
                        "mistake_count": row[4], "created": str(row[5]), "sentence": row[6],
                    }
                    for row in doomed
                ],
                handle,
                ensure_ascii=False,
                indent=1,
            )
        print(f"\nрезервная копия: {backup_path}")

        doomed_ids = [int(row[0]) for row in doomed]
        with conn.cursor() as cursor:
            cursor.execute(
                "DELETE FROM bt_3_detailed_mistakes WHERE id = ANY(%s);",
                (doomed_ids,),
            )
            deleted = cursor.rowcount
        conn.commit()
        print(f"удалено записей: {deleted}")

        with conn.cursor() as cursor:
            cursor.execute(SELECT_TOTALS)
            all_rows_after, unclassified_after = cursor.fetchone()
        print("\n── после уборки ──")
        print(f"   всего записей об ошибках  : {all_rows_after}")
        print(f"   из них «тип не определён» : {unclassified_after}")
    finally:
        conn.close()


if __name__ == "__main__":
    main(apply="--apply" in sys.argv)
