"""Разделить склейку «вариант -> вариант», попавшую в одно поле общего словаря.

Что это. При переформулировке в читалке и в видео фраза и её замена склеивались стрелкой
и уезжали в словарь одной строкой: «Er schwadroniert ununterbrochen. -> Er plaudert
ununterbrochen.» Человек видит в поиске такую строку целиком — это не слово и не фраза.

На пути сохранения заслон стоит с 22.07.2026 (`_strip_rephrase_arrow_combo`), новые
такие строки не появляются. Этот проход убирает накопленное: 65 слов и 65 записей
старого словаря на замер 06.08.2026.

Берём ЛЕВУЮ часть — то, что человек написал сам; правая это предложенная замена, и
подменять ею оригинал мы права не имеем.

По умолчанию НИЧЕГО НЕ ПИШЕТ. Запись — только с --apply.
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
from dictionary_intake import clean_text, has_cyrillic, has_latin  # noqa: E402
from lex_units import normalize_query  # noqa: E402


def left_part(text: str) -> str:
    return clean_text(str(text or "").split("->", 1)[0])


def language_of(text: str) -> str:
    latin, cyrillic = has_latin(text), has_cyrillic(text)
    if latin and not cyrillic:
        return "de"
    if cyrillic and not latin:
        return "ru"
    return ""


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, lang, display FROM bt_3_lex_units WHERE display LIKE '%->%' ORDER BY id;"
            )
            units = []
            for uid, lang, display in cur.fetchall():
                fixed = left_part(display)
                if fixed and fixed != display:
                    units.append((uid, lang, display, fixed))

            cur.execute(
                """SELECT id, source_text, target_text FROM bt_3_dictionary_entries
                   WHERE source_text LIKE '%->%' OR target_text LIKE '%->%' ORDER BY id;"""
            )
            pool = []
            for eid, src, tgt in cur.fetchall():
                new_src = left_part(src) if "->" in (src or "") else src
                new_tgt = left_part(tgt) if "->" in (tgt or "") else tgt
                if (new_src, new_tgt) != (src, tgt) and new_src and new_tgt:
                    pool.append((eid, src, tgt, new_src, new_tgt))

            print("СЛОВА СО СКЛЕЙКОЙ: %d" % len(units))
            for uid, _l, was, now in units[:10]:
                print("   %s  %r → %r" % (uid, was[:62], now[:62]))
            print("ЗАПИСИ СТАРОГО СЛОВАРЯ: %d" % len(pool))
            for eid, was, _t, now, _n in pool[:10]:
                print("   %s  %r → %r" % (eid, was[:62], now[:62]))

            if not args.apply:
                print()
                print("ВХОЛОСТУЮ. Записать: --apply")
                return 0

            fixed_units = skipped = relanged = 0
            for uid, lang, _was, now in units:
                key = normalize_query(now)
                if not key:
                    continue
                # Под склейкой часто прячется перепутанный язык: строка вида
                # «немецкое -> русское» проходила проверку алфавита за счёт ПРАВОЙ
                # половины. Отрезали правую — осталось чистое немецкое под меткой
                # «русский», и правило базы такую запись не пропустит. Ставим язык
                # по алфавиту того, что реально осталось.
                real = language_of(now)
                if real and real != lang:
                    cur.execute(
                        "SELECT id FROM bt_3_lex_units WHERE lang=%s AND lemma_key=%s AND id<>%s LIMIT 1;",
                        (real, key, uid),
                    )
                    if cur.fetchone():
                        skipped += 1
                        continue
                    cur.execute(
                        "SELECT surface_key, match_kind FROM bt_3_lex_surfaces WHERE unit_id=%s;",
                        (uid,),
                    )
                    surfaces = cur.fetchall()
                    cur.execute("DELETE FROM bt_3_lex_surfaces WHERE unit_id=%s;", (uid,))
                    cur.execute("UPDATE bt_3_lex_units SET lang=%s WHERE id=%s;", (real, uid))
                    for surface_key, match_kind in surfaces:
                        cur.execute(
                            """INSERT INTO bt_3_lex_surfaces (lang, surface_key, unit_id, match_kind)
                               VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING;""",
                            (real, surface_key, uid, match_kind),
                        )
                    lang = real
                    relanged += 1
                cur.execute(
                    "SELECT id FROM bt_3_lex_units WHERE lang=%s AND lemma_key=%s AND id<>%s LIMIT 1;",
                    (lang, key, uid),
                )
                if cur.fetchone():
                    skipped += 1
                    continue
                cur.execute(
                    "UPDATE bt_3_lex_units SET display=%s, lemma=%s, lemma_key=%s WHERE id=%s;",
                    (now, now, key, uid),
                )
                cur.execute(
                    """INSERT INTO bt_3_lex_surfaces (lang, surface_key, unit_id, match_kind)
                       VALUES (%s, %s, %s, 'exact') ON CONFLICT DO NOTHING;""",
                    (lang, key, uid),
                )
                fixed_units += 1
            fixed_pool = 0
            for eid, _s, _t, new_src, new_tgt in pool:
                try:
                    cur.execute(
                        "UPDATE bt_3_dictionary_entries SET source_text=%s, target_text=%s WHERE id=%s;",
                        (new_src, new_tgt, eid),
                    )
                    fixed_pool += 1
                except Exception:
                    conn.rollback()
            conn.commit()
    print()
    print("ПОЧИНЕНО: слов %d (язык поправлен у %d, пропущено по столкновению %d), записей словаря %d"
          % (fixed_units, relanged, skipped, fixed_pool))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
