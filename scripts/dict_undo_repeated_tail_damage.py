# -*- coding: utf-8 -*-
"""Убрать размноженный хвост у заголовков: «Er erlag der Versuchung......» → «…Versuchung.»

ЧТО ПРОИЗОШЛО
─────────────
16.08.2026 в 18:16:15 одна транзакция переписала заголовки 15 фраз, размножив им хвост
ровно шесть раз:

    Er erlag der Versuchung......
    Das Anliegen an jemanden jemanden jemanden jemanden jemanden jemanden
    Kommen Sie bitte außerhalb der Arbeitszeiten vorbei. vorbei. vorbei. vorbei. vorbei. vorbei.

Механизм доказан на 5 случаях из 5, символ в символ: это подстановка `s.replace(старое,
новое)`, применённая ШЕСТЬ раз к накапливающемуся результату, где «новое» начинается со
«старого». Одна такая подстановка добавляет хвост, шесть — добавляют его шесть раз.

Скрипта в репозитории НЕТ: ни в рабочем дереве, ни в истории (искал и по содержимому).
Значит его прогнали разово и не закоммитили. Продуктовый код это повторить не может —
проверены все подстановки с двумя переменными в репозитории и все SQL REPLACE, ни одна
не трогает заголовок; правило заголовка (`german_dictionary_headword`) на этих текстах
не меняет ничего. Поэтому здесь только уборка; страж от повторения ставится отдельно, на
уровне базы, потому что скрипты пишут в неё в обход любых питоновских проверок.

КАК ВОССТАНАВЛИВАЕТСЯ ПРАВИЛЬНЫЙ ТЕКСТ
──────────────────────────────────────
Не на глаз и не «схлопнем повторы». Та транзакция НЕ ТРОНУЛА `lemma_key` — ключ поиска
остался от правильного текста. Он и есть свидетель: восстановленный вариант принимается
только если `normalize_query(восстановленное)` совпадает с сохранённым ключом. Не сошлось
— строку НЕ ТРОГАЕМ и показываем владельцу. Догадка вместо совпадения здесь запрещена.

    python3 scripts/dict_undo_repeated_tail_damage.py            # показать, не писать
    python3 scripts/dict_undo_repeated_tail_damage.py --apply    # починить
"""
from __future__ import annotations

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SKIP_BILLING_LEDGER_WRITES", "1")

from backend.database import get_db_connection_context  # noqa: E402
from backend.lex_units import normalize_query  # noqa: E402

# ПРИЗНАК ПОРЧИ И ВОССТАНОВЛЕНИЕ БЕРУТСЯ ИЗ ОБЩЕГО МОДУЛЯ, а не заводятся здесь.
#
# Свой признак у себя в файле — это и есть то, из-за чего тема открывалась ЧЕТЫРЕ раза:
# первый заход искал повтор слова и знака, второй заглянул в разбор, третий в карточки,
# четвёртый нашёл повтор БУКВЫ, который первые три пропускали. Каждый раз казалось, что
# порча выросла, а росло только правило. Нужен новый вид — добавляй в
# backend/mangled_text.py, и его сразу увидят и уборка, и замок в базе, и тест.
from backend.mangled_text import (                                # noqa: E402
    is_mangled,
    undo_candidates as candidates,
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    fixed, unsure = [], []
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT id, display, lemma, lemma_key FROM bt_3_lex_units WHERE lang = 'de';")
            for unit_id, display, lemma, key in cursor.fetchall():
                display = str(display or "")
                if not is_mangled(display):
                    continue
                # Свидетель — ключ поиска: его та транзакция не тронула.
                good = next((c for c in candidates(display)
                             if c != display and normalize_query(c) == str(key or "")), None)
                (fixed if good else unsure).append((unit_id, display, good, key))

    print("\nЗАГОЛОВКОВ С РАЗМНОЖЕННЫМ ХВОСТОМ: %d\n" % (len(fixed) + len(unsure)))
    for unit_id, display, good, _key in fixed:
        print(f"   {unit_id:>6} {display!r}\n          → {good!r}")
    if unsure:
        print("\n⚠ КЛЮЧ ПОИСКА НЕ ПОДТВЕРДИЛ — НЕ ТРОГАЮ, решать владельцу:\n")
        for unit_id, display, _good, key in unsure:
            print(f"   {unit_id:>6} {display!r}\n          ключ: {key!r}")

    if not args.apply:
        print("\nВХОЛОСТУЮ. Починить: --apply\n")
        return 0

    done = {"слов": 0, "карточек людей": 0, "разборов на слове": 0, "записей пула": 0}
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            for unit_id, display, good, _key in fixed:
                cursor.execute(
                    "UPDATE bt_3_lex_units SET display = %s, lemma = %s, updated_at = NOW() "
                    "WHERE id = %s;",
                    (good, good, unit_id),
                )
                done["слов"] += 1
                # Тот же изуродованный текст уехал внутрь разбора — и на слове, и в
                # карточках людей. Там он лежит строкой внутри JSON, поэтому меняем
                # его подстановкой по тексту JSON: строка длинная и своеобразная,
                # совпасть с чем-то посторонним она не может.
                cursor.execute("SELECT card FROM bt_3_lex_units WHERE id = %s;", (unit_id,))
                row = cursor.fetchone()
                if row and isinstance(row[0], dict):
                    raw = json.dumps(row[0], ensure_ascii=False)
                    if display in raw:
                        cursor.execute(
                            "UPDATE bt_3_lex_units SET card = %s::jsonb WHERE id = %s;",
                            (raw.replace(display, good), unit_id),
                        )
                        done["разборов на слове"] += 1
                cursor.execute(
                    "SELECT id, response_json FROM bt_3_webapp_dictionary_queries "
                    "WHERE response_json::text LIKE %s;",
                    ("%" + display + "%",),
                )
                for entry_id, payload in cursor.fetchall():
                    raw = json.dumps(payload, ensure_ascii=False)
                    cursor.execute(
                        "UPDATE bt_3_webapp_dictionary_queries "
                        "SET response_json = %s::jsonb, updated_at = NOW() WHERE id = %s;",
                        (raw.replace(display, good), entry_id),
                    )
                    done["карточек людей"] += 1
                # Общий пул: из него собирается карточка при поиске. Первая версия его не
                # чистила вовсе — а «sterile Gazennnnnn» лежало и там тоже.
                cursor.execute(
                    "SELECT id, source_text, response_json FROM bt_3_dictionary_entries "
                    "WHERE source_text = %s OR response_json::text LIKE %s;",
                    (display, "%" + display + "%"),
                )
                for entry_id, source_text, payload in cursor.fetchall():
                    raw = json.dumps(payload, ensure_ascii=False) if payload else ""
                    cursor.execute(
                        "UPDATE bt_3_dictionary_entries SET source_text = %s, "
                        "response_json = COALESCE(%s::jsonb, response_json), "
                        "updated_at = NOW() WHERE id = %s;",
                        (good if source_text == display else source_text,
                         raw.replace(display, good) if raw else None, entry_id),
                    )
                    done["записей пула"] += 1
        conn.commit()

    print("\nГОТОВО: %s" % done)
    if unsure:
        print("НЕ ТРОНУТО (ключ не подтвердил): %d — см. список выше" % len(unsure))
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
