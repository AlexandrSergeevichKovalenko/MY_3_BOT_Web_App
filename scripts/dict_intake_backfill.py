"""Снять с накопленных записей ту же грязь копипаста, которую теперь не пускает вход.

Что было. Текст попадал в словарь из шести мест, а механической чистки не было почти
нигде. Осело: невидимые знаки внутри слова, неразрывные пробелы, переводы строк,
нумерация из скопированного списка, типографские кавычки, повисшее тире, буквы-двойники
из чужого алфавита. Каждая такая строка — слово, которое не находится по своему же
написанию: для базы это другая строка, и человек снова платит за разбор того, что у нас
уже есть.

Дверь на входе закрыта 06.08.2026 (`backend/dictionary_intake.py`), новое так не
записывается. Этот проход разбирает накопленное — ровно той же функцией, никакой
отдельной логики: иначе чистка входа и чистка хвостов разойдутся, как это уже было с
шестью разными проверками.

Осторожность там, где текст участвует в ключе:
  карточки — ключей на тексте нет, правим прямо;
  общий словарь — чистая версия может СТОЛКНУТЬСЯ с уже существующей записью, такие
    пропускаем и показываем в отчёте: сливать записи надо осознанно;
  единицы — меняется ключ поиска, поэтому чистим и его, а старое написание ОСТАВЛЯЕМ
    рядом (алиас), чтобы уже сохранённые карточки не потеряли дом. Столкновения по
    ключу пропускаем.

По умолчанию НИЧЕГО НЕ ПИШЕТ: показывает отчёт. Запись — только с --apply.

    python scripts/dict_intake_backfill.py           # вхолостую
    python scripts/dict_intake_backfill.py --apply   # записать
"""

from __future__ import annotations

import argparse
import os
import sys

_here = os.path.dirname(os.path.abspath(globals().get("__file__", ".")))
sys.path.insert(0, os.path.join(_here, ".."))
sys.path.insert(0, os.path.join(_here, "..", "backend"))
sys.path.insert(0, "/app/backend")

from dictionary_intake import clean_text  # noqa: E402
from database import get_db_connection_context  # noqa: E402
from lex_units import normalize_query, text_matches_language  # noqa: E402

CARD_COLUMNS = ("word_de", "word_ru", "translation_de", "translation_ru")


def collect_cards(cur) -> list:
    cur.execute(
        "SELECT id, %s FROM bt_3_webapp_dictionary_queries" % ", ".join(CARD_COLUMNS)
    )
    out = []
    for row in cur.fetchall():
        entry_id, values = row[0], row[1:]
        cleaned = tuple(clean_text(v) if v else v for v in values)
        if cleaned != values:
            out.append((entry_id, values, cleaned))
    return out


def collect_pool(cur) -> tuple[list, list]:
    cur.execute(
        """SELECT id, source_lang, target_lang, source_text, target_text,
                  word_de, word_ru, translation_de, translation_ru
           FROM bt_3_dictionary_entries"""
    )
    rows = cur.fetchall()
    fix, collision = [], []
    for r in rows:
        pid, sl, tl = r[0], r[1], r[2]
        values = r[3:]
        cleaned = tuple(clean_text(v) if v else v for v in values)
        if cleaned == values:
            continue
        cur.execute(
            """SELECT id FROM bt_3_dictionary_entries
               WHERE source_lang = %s AND target_lang = %s
                 AND source_text = %s AND target_text = %s AND id <> %s
               LIMIT 1;""",
            (sl, tl, cleaned[0], cleaned[1], pid),
        )
        twin = cur.fetchone()
        (collision if twin else fix).append((pid, values, cleaned, twin[0] if twin else None))
    return fix, collision


def collect_units(cur) -> tuple[list, list, list]:
    cur.execute("SELECT id, lang, lemma, lemma_key, display FROM bt_3_lex_units")
    fix, collision, wrong_language = [], [], []
    for uid, lang, lemma, key, display in cur.fetchall():
        new_lemma = clean_text(lemma)
        new_display = clean_text(display)
        if new_lemma == lemma and new_display == display:
            continue
        if not text_matches_language(new_display, lang):
            # Немецкий текст под языком «ru» и наоборот — перепутанные стороны, отдельная
            # работа. Тронуть такую строку нельзя: правило базы её не пропустит, и весь
            # проход откатится. Показываем в отчёте и идём дальше.
            wrong_language.append((uid, lang, display))
            continue
        new_key = normalize_query(new_lemma)
        if not new_key or not new_lemma or not new_display:
            continue  # чистка опустошила бы единицу — не трогаем
        if new_key != key:
            cur.execute(
                "SELECT id FROM bt_3_lex_units WHERE lang = %s AND lemma_key = %s AND id <> %s LIMIT 1;",
                (lang, new_key, uid),
            )
            twin = cur.fetchone()
            if twin:
                collision.append((uid, lang, key, new_key, twin[0]))
                continue
        fix.append((uid, lang, lemma, new_lemma, key, new_key, display, new_display))
    return fix, collision, wrong_language


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="записать (по умолчанию — вхолостую)")
    parser.add_argument("--limit", type=int, default=0, help="ограничить число правок (для пробы)")
    args = parser.parse_args()

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cards = collect_cards(cur)
            pool_fix, pool_collision = collect_pool(cur)
            unit_fix, unit_collision, unit_wrong_lang = collect_units(cur)

            if args.limit:
                cards = cards[: args.limit]
                pool_fix = pool_fix[: args.limit]
                unit_fix = unit_fix[: args.limit]

            print("КАРТОЧКИ:      правим %d" % len(cards))
            for entry_id, old, new in cards[:5]:
                for o, n in zip(old, new):
                    if o != n:
                        print("   %s: %r → %r" % (entry_id, o[:60], n[:60]))
            print("ОБЩИЙ СЛОВАРЬ: правим %d, пропускаем как столкновение %d"
                  % (len(pool_fix), len(pool_collision)))
            for pid, old, new, twin in pool_collision:
                print("   пропуск %s: %r уже занято записью %s" % (pid, old[0][:50], twin))
            print("ЕДИНИЦЫ:       правим %d, пропускаем как столкновение %d"
                  % (len(unit_fix), len(unit_collision)))
            for uid, lang, key, new_key, twin in unit_collision:
                print("   пропуск %s (%s): ключ %r уже у единицы %s" % (uid, lang, new_key[:50], twin))
            print("               пропускаем как перепутанные стороны %d (отдельная работа)"
                  % len(unit_wrong_lang))
            for uid, lang, display in unit_wrong_lang[:5]:
                print("   пропуск %s: язык %s, а текст %r" % (uid, lang, display[:50]))

            if not args.apply:
                print()
                print("ВХОЛОСТУЮ. Записать: --apply")
                return 0

            for entry_id, _old, new in cards:
                cur.execute(
                    "UPDATE bt_3_webapp_dictionary_queries SET %s WHERE id = %%s;"
                    % ", ".join("%s = %%s" % c for c in CARD_COLUMNS),
                    (*new, entry_id),
                )
            for pid, _old, new, _twin in pool_fix:
                cur.execute(
                    """UPDATE bt_3_dictionary_entries
                       SET source_text = %s, target_text = %s,
                           word_de = %s, word_ru = %s,
                           translation_de = %s, translation_ru = %s
                       WHERE id = %s;""",
                    (*new, pid),
                )
            for uid, lang, _lemma, new_lemma, old_key, new_key, _display, new_display in unit_fix:
                cur.execute(
                    "UPDATE bt_3_lex_units SET lemma = %s, lemma_key = %s, display = %s WHERE id = %s;",
                    (new_lemma, new_key, new_display, uid),
                )
                # Новое написание добавляем, старое НЕ трогаем: по нему уже могли
                # сохраниться карточки, и убрать его — оставить их без дома.
                cur.execute(
                    """INSERT INTO bt_3_lex_surfaces (lang, surface_key, unit_id, match_kind)
                       VALUES (%s, %s, %s, 'exact')
                       ON CONFLICT DO NOTHING;""",
                    (lang, new_key, uid),
                )
            conn.commit()
    print()
    print("ЗАПИСАНО: карточек %d, записей словаря %d, единиц %d"
          % (len(cards), len(pool_fix), len(unit_fix)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
