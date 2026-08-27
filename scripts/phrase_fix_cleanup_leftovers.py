# -*- coding: utf-8 -*-
"""Снять копии СТАРОГО текста, оставшиеся после правок спорных фраз.

ПОВОД. Владелец правит спорную фразу: `retitle_unit` меняет написание и лемму,
`spread_correction_everywhere` разносит текст по карточкам — но ни одна из них не
трогает общий пул и кеш быстрого словаря. Замер 27.08.2026 по 50 правкам со сменой
текста: у 36 старый текст остался в пуле, у одной — в кеше.

ЧЕМ ЭТО ОПАСНО. Пул отдаёт готовый ответ ПО ТЕКСТУ. Пока там лежит «Auf etwas Verzicht
leisten» (неверный порядок слов) или «Die Prüfung wurde bestanden / abgelegt wurde»,
тот же кривой вариант может уехать человеку снова — из ярлыка, из бота, из повторного
сохранения. Мы уже признали этот текст неверным, а копия продолжает его раздавать.

ЧЕГО СКРИПТ НЕ ДЕЛАЕТ. Не трогает указатель поиска: у фразы старый текст это обычно
обрывок («Wie steht es um?»), и ключ по нему работает АЛИАСОМ — человек ищет то, что
сам сохранил криво, и попадает на исправленное. Это польза, а не мусор.

Новые хвосты больше не копятся: та же дочистка встроена в `apply_phrase_review_decision`
(backend/database.py). Скрипт нужен ровно один раз — на накопленное.

Запуск:  python3 scripts/phrase_fix_cleanup_leftovers.py [--применить]
Без «--применить» только показывает, что снял бы.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SECOND_VOICE_CHECK_DISABLED", "1")

from backend.database import get_db_connection_context  # noqa: E402
from backend.lex_units import normalize_query  # noqa: E402

ПРИМЕНИТЬ = "--применить" in sys.argv


def main() -> None:
    снято_пул = снято_кеш = 0
    тронуто = 0
    with get_db_connection_context() as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("""SELECT id, btrim(text), btrim(decided_text)
                             FROM bt_3_phrase_review
                            WHERE decided_text IS NOT NULL
                              AND btrim(decided_text) <> ''
                              AND btrim(decided_text) <> btrim(text)
                            ORDER BY id;""")
            правки = cur.fetchall()
            print(f"правок фраз со сменой текста: {len(правки)}")

            for pid, старый, новый in правки:
                ключ = normalize_query(старый)
                if not ключ:
                    continue
                граница = rf"\m{ключ}\M"
                cur.execute("""SELECT COUNT(*) FROM bt_3_dictionary_entries
                                WHERE source_text ~* %s OR target_text ~* %s;""",
                            (граница, граница))
                в_пуле = int(cur.fetchone()[0])
                cur.execute("""SELECT COUNT(*) FROM bt_3_dictionary_lookup_cache
                                WHERE response_json::text ~* %s;""", (граница,))
                в_кеше = int(cur.fetchone()[0])
                if not в_пуле and not в_кеше:
                    continue
                тронуто += 1
                print(f"   #{pid} «{старый[:50]}» → «{новый[:50]}»: "
                      f"пул {в_пуле}, кеш {в_кеше}")
                if ПРИМЕНИТЬ:
                    cur.execute("""DELETE FROM bt_3_dictionary_entries
                                    WHERE source_text ~* %s OR target_text ~* %s;""",
                                (граница, граница))
                    снято_пул += cur.rowcount or 0
                    cur.execute("""DELETE FROM bt_3_dictionary_lookup_cache
                                    WHERE response_json::text ~* %s;""", (граница,))
                    снято_кеш += cur.rowcount or 0

    print(f"\nправок с хвостами: {тронуто}")
    if ПРИМЕНИТЬ:
        print(f"снято из пула: {снято_пул}, из кеша: {снято_кеш}")
    else:
        print("это был показ. Чтобы снять — запустите с «--применить».")


if __name__ == "__main__":
    main()
