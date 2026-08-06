"""Слить единицу с неверным языком в её правильного близнеца.

Что осталось после разворота. Разворот (`dict_units_fix_language.py`) починил те 295
единиц, у которых правильного близнеца ещё не было. Остальные 180 развернуть нельзя:
то же слово УЖЕ заведено правильно, и разворот сделал бы двух хозяев одному написанию —
на один запрос отвечали бы двое, а какой ответ придёт, зависело бы от порядка строк.

Замер 06.08.2026: у всех 180 есть правильный близнец, все несут связи (от 2 до 10),
личных карточек не держит ни одна. То есть это чистые дубликаты массовой сборки, и
правильное действие — перенести с них всё живое на близнеца и убрать.

Что переносится:
  разбор      — только если у близнеца его нет или он беднее (правило «понизить нельзя»);
  связи       — переставляются на близнеца; те, что стали бы повтором или петлёй, гибнут
                вместе с единицей (у близнеца такая связь уже есть);
  написания   — добавляются близнецу под его языком, ничего не удаляя;
  карточки    — указатель переставляется на близнеца (сейчас таких нет, но правило нужно).

По умолчанию НИЧЕГО НЕ ПИШЕТ. Запись — только с --apply.

    python scripts/dict_units_merge_wrong_language.py           # вхолостую
    python scripts/dict_units_merge_wrong_language.py --apply   # записать
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
import lex_units  # noqa: E402


def language_of(text: str) -> str:
    latin, cyrillic = has_latin(text), has_cyrillic(text)
    if latin and not cyrillic:
        return "de"
    if cyrillic and not latin:
        return "ru"
    return ""


def collect(cur) -> tuple[list, list]:
    cur.execute(
        """SELECT id, lang, lemma_key, display, card FROM bt_3_lex_units
           WHERE (lang = 'de' AND display !~ '[A-Za-zÄÖÜäöüß]')
              OR (lang = 'ru' AND display !~ '[А-яЁё]')
              OR (lang = 'en' AND display !~ '[A-Za-z]')
              OR lang NOT IN ('de', 'ru', 'en')
           ORDER BY id;"""
    )
    merge, orphan = [], []
    for uid, lang, key, display, card in cur.fetchall():
        real = language_of(display)
        if not real:
            orphan.append((uid, lang, display, "язык непонятен"))
            continue
        cur.execute(
            "SELECT id FROM bt_3_lex_units WHERE lang = %s AND lemma_key = %s AND id <> %s ORDER BY id LIMIT 1;",
            (real, key, uid),
        )
        twin = cur.fetchone()
        if not twin:
            orphan.append((uid, lang, display, "близнеца нет — это работа разворота"))
            continue
        merge.append((uid, lang, real, int(twin[0]), display, card if isinstance(card, dict) else None))
    return merge, orphan


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            merge, orphan = collect(cur)
            print("СЛИВАЕМ: %d" % len(merge))
            for uid, lang, real, twin, display, card in merge[:10]:
                print("   %s (%s) → %s (%s)%s   %r"
                      % (uid, lang, twin, real, "  + разбор" if card else "", display[:50]))
            print("НЕ СЛИВАЕМ: %d" % len(orphan))
            for uid, lang, display, why in orphan[:10]:
                print("   %s (%s): %s   %r" % (uid, lang, why, display[:50]))

            if not args.apply:
                print()
                print("ВХОЛОСТУЮ. Записать: --apply")
                return 0

            moved_cards = moved_links = moved_surfaces = 0
            for uid, _lang, real, twin, _display, card in merge:
                if card:
                    if lex_units.save_unit_card_if_richer(twin, card, source="слияние дубликата"):
                        moved_cards += 1
                cur.execute(
                    """UPDATE bt_3_lex_links l SET from_unit = %(twin)s
                       WHERE l.from_unit = %(uid)s AND l.to_unit <> %(twin)s
                         AND NOT EXISTS (SELECT 1 FROM bt_3_lex_links k
                                         WHERE k.from_unit = %(twin)s AND k.to_unit = l.to_unit);""",
                    {"twin": twin, "uid": uid},
                )
                moved_links += cur.rowcount or 0
                cur.execute(
                    """UPDATE bt_3_lex_links l SET to_unit = %(twin)s
                       WHERE l.to_unit = %(uid)s AND l.from_unit <> %(twin)s
                         AND NOT EXISTS (SELECT 1 FROM bt_3_lex_links k
                                         WHERE k.from_unit = l.from_unit AND k.to_unit = %(twin)s);""",
                    {"twin": twin, "uid": uid},
                )
                moved_links += cur.rowcount or 0
                cur.execute(
                    """INSERT INTO bt_3_lex_surfaces (lang, surface_key, unit_id, match_kind)
                       SELECT %s, s.surface_key, %s, s.match_kind
                       FROM bt_3_lex_surfaces s WHERE s.unit_id = %s
                       ON CONFLICT DO NOTHING;""",
                    (real, twin, uid),
                )
                moved_surfaces += cur.rowcount or 0
                cur.execute(
                    "UPDATE bt_3_webapp_dictionary_queries SET lex_unit_id = %s WHERE lex_unit_id = %s;",
                    (twin, uid),
                )
                cur.execute("DELETE FROM bt_3_lex_units WHERE id = %s;", (uid,))
            conn.commit()
    print()
    print("СЛИТО ЕДИНИЦ: %d (разборов перенесено %d, связей %d, написаний %d)"
          % (len(merge), moved_cards, moved_links, moved_surfaces))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
