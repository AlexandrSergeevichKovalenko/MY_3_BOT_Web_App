"""Развернуть единицы, у которых язык не совпадает с алфавитом текста.

Что сломано. Единица «Я вкладываю свои деньги надёжно» заведена как НЕМЕЦКАЯ, а
«anlegen» — как РУССКАЯ. Это не опечатка: пары заводились массовой сборкой 27 июля с
перепутанными сторонами. Замер 06.08.2026: 124 немецких единицы с русским текстом и
350 русских с немецким.

Чем это вредит. Разбор описывает НЕМЕЦКОЕ слово, и кладём мы его только на немецкую
единицу. Если немецкое слово числится русским — разбор к нему не приезжает никогда,
карточка остаётся тонкой, а на повторный запрос мы снова платим модели. Поиск по такой
единице тоже мимо: он ищет немецкое написание среди немецких.

Что делает скрипт. Ставит единице тот язык, на котором она реально написана. Текст,
ключ и связи не трогает — меняется только пометка языка (и та же пометка у написаний).

Написания приходится пересоздавать: составной внешний ключ смотрит на пару
(единица, язык), и просто поменять язык у родителя база не даст — сначала убираем
детей, потом меняем, потом кладём обратно с новым языком. Всё в одной сделке.

Пропускаем и показываем в отчёте:
  — текст, где есть оба алфавита или нет ни одного (какой язык — непонятно);
  — единицы, которые после разворота столкнутся с уже существующей: это дубликаты
    одной пары, сливать их надо осознанно.

По умолчанию НИЧЕГО НЕ ПИШЕТ. Запись — только с --apply.

    python scripts/dict_units_fix_language.py           # вхолостую
    python scripts/dict_units_fix_language.py --apply   # записать
"""

from __future__ import annotations

import argparse
import os
import sys

_here = os.path.dirname(os.path.abspath(globals().get("__file__", ".")))
sys.path.insert(0, os.path.join(_here, ".."))
sys.path.insert(0, os.path.join(_here, "..", "backend"))
sys.path.insert(0, "/app/backend")

from database import get_db_connection_context  # noqa: E402
from dictionary_intake import has_cyrillic, has_latin  # noqa: E402


def language_of(text: str) -> str:
    """Язык по алфавиту — только когда он однозначен."""
    latin, cyrillic = has_latin(text), has_cyrillic(text)
    if latin and not cyrillic:
        return "de"
    if cyrillic and not latin:
        return "ru"
    return ""


def collect(cur) -> tuple[list, list, list]:
    cur.execute(
        """SELECT id, lang, lemma_key, display FROM bt_3_lex_units
           WHERE (lang = 'de' AND display !~ '[A-Za-zÄÖÜäöüß]')
              OR (lang = 'ru' AND display !~ '[А-яЁё]')
              OR (lang = 'en' AND display !~ '[A-Za-z]')
              OR lang NOT IN ('de', 'ru', 'en')
           ORDER BY id;"""
    )
    flip, unclear, collision = [], [], []
    for uid, lang, key, display in cur.fetchall():
        real = language_of(display)
        if not real or real == lang:
            unclear.append((uid, lang, display))
            continue
        cur.execute(
            "SELECT id FROM bt_3_lex_units WHERE lang = %s AND lemma_key = %s AND id <> %s LIMIT 1;",
            (real, key, uid),
        )
        twin = cur.fetchone()
        if twin:
            collision.append((uid, lang, real, display, twin[0]))
            continue
        flip.append((uid, lang, real, display))
    return flip, unclear, collision


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            flip, unclear, collision = collect(cur)

            print("РАЗВОРАЧИВАЕМ: %d" % len(flip))
            for uid, lang, real, display in flip[:10]:
                print("   %s: %s → %s   %r" % (uid, lang, real, display[:60]))
            print("НЕПОНЯТНЫЙ ЯЗЫК (пропуск): %d" % len(unclear))
            for uid, lang, display in unclear[:10]:
                print("   %s (%s): %r" % (uid, lang, display[:60]))
            print("СТОЛКНУТСЯ С СУЩЕСТВУЮЩЕЙ (пропуск, сливать осознанно): %d" % len(collision))
            for uid, lang, real, display, twin in collision[:10]:
                print("   %s: %s → %s занято единицей %s   %r" % (uid, lang, real, twin, display[:50]))

            if not args.apply:
                print()
                print("ВХОЛОСТУЮ. Записать: --apply")
                return 0

            done = 0
            for uid, _lang, real, _display in flip:
                cur.execute(
                    "SELECT surface_key, match_kind FROM bt_3_lex_surfaces WHERE unit_id = %s;",
                    (uid,),
                )
                surfaces = cur.fetchall()
                # Порядок обязателен: составной ключ смотрит на пару (единица, язык).
                cur.execute("DELETE FROM bt_3_lex_surfaces WHERE unit_id = %s;", (uid,))
                cur.execute("UPDATE bt_3_lex_units SET lang = %s WHERE id = %s;", (real, uid))
                for surface_key, match_kind in surfaces:
                    cur.execute(
                        """INSERT INTO bt_3_lex_surfaces (lang, surface_key, unit_id, match_kind)
                           VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING;""",
                        (real, surface_key, uid, match_kind),
                    )
                done += 1
            conn.commit()
    print()
    print("РАЗВЁРНУТО ЕДИНИЦ: %d" % done)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
