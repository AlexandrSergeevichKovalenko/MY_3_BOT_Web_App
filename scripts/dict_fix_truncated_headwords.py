"""Починить обкусанные заголовки слов: «die Rivalitä», «die Pfeif», «der Geizkrag».

Откуда. Приведение к начальной форме (spaCy) на части слов откусывало окончание, и в
слой попадало написание, которого в немецком нет. Человек видит его как заголовок
карточки. Замер 06.08.2026: у 54 слов написание в базе и заголовок внутри разбора
расходятся так, что одно является обрезком другого.

Почему нельзя починить правилом. Половина этих расхождений ЗАКОННА: «die Behörde» в
базе и «die Behörden» в разборе — это единственное и множественное число одного слова,
трогать нечего. А «die Rivalitä» и «die Rivalität» — обрезок и слово. Отличить одно от
другого может только знание языка, поэтому спрашиваем наш собственный корректор — тот
самый, который стоит на входе словаря. Он правит только настоящие ошибки написания и
молчит, когда всё в порядке.

Столкновения по ключу пропускаем: если исправленное написание уже занято другим словом,
их надо сливать осознанно.

По умолчанию НИЧЕГО НЕ ПИШЕТ и НИЧЕГО НЕ СПРАШИВАЕТ у модели. Запись — только с --apply.

    python scripts/dict_fix_truncated_headwords.py           # вхолостую (список кандидатов)
    python scripts/dict_fix_truncated_headwords.py --apply   # спросить корректор и записать
"""

from __future__ import annotations

import argparse
import os
import re
import sys

_here = os.path.dirname(os.path.abspath(globals().get("__file__", ".")))
sys.path.insert(0, os.path.join(_here, ".."))
sys.path.insert(0, os.path.join(_here, "..", "backend"))
sys.path.insert(0, "/app/backend")

from database import get_db_connection_context  # noqa: E402
from dictionary_intake import clean_text  # noqa: E402
import lex_units  # noqa: E402

WORD = re.compile(r"[^\W\d_]+", re.UNICODE)
ARTICLES = {"der", "die", "das", "den", "dem", "des"}


def body(text: str) -> str:
    words = WORD.findall(text or "")
    if words and words[0].lower() in ARTICLES:
        words = words[1:]
    return " ".join(words).lower()


def collect(cur) -> list:
    cur.execute(
        """SELECT id, lang, display, lemma_key, card->>'word_de'
           FROM bt_3_lex_units WHERE lang = 'de' AND card ? 'word_de' ORDER BY id;"""
    )
    out = []
    for uid, lang, display, key, headword in cur.fetchall():
        if not headword:
            continue
        a, b = body(display), body(headword)
        if not a or not b or a == b:
            continue
        if a.startswith(b) or b.startswith(a):
            out.append((uid, lang, display, key, headword))
    return out


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            candidates = collect(cur)
            print("КАНДИДАТЫ (написание — обрезок заголовка разбора или наоборот): %d"
                  % len(candidates))
            for uid, _lang, display, _key, headword in candidates[:20]:
                print("   %s: в базе %r | в разборе %r" % (uid, display[:45], headword[:45]))

            if not args.apply:
                print()
                print("ВХОЛОСТУЮ. Спросить корректор и записать: --apply")
                return 0

            from openai_manager import run_quick_correct

            fixed = untouched = collision = 0
            for uid, lang, display, key, _headword in candidates:
                try:
                    corrected = clean_text(run_quick_correct(text=display, source_lang="de"))
                except Exception as exc:
                    print("   %s: корректор не ответил (%s)" % (uid, type(exc).__name__))
                    continue
                if not corrected or corrected == display:
                    untouched += 1
                    continue
                new_key = lex_units.normalize_query(corrected)
                if not new_key:
                    untouched += 1
                    continue
                if new_key != key:
                    cur.execute(
                        "SELECT id FROM bt_3_lex_units WHERE lang = %s AND lemma_key = %s AND id <> %s LIMIT 1;",
                        (lang, new_key, uid),
                    )
                    if cur.fetchone():
                        collision += 1
                        print("   %s: %r → %r занято другим словом, пропуск"
                              % (uid, display[:40], corrected[:40]))
                        continue
                cur.execute(
                    "UPDATE bt_3_lex_units SET display = %s, lemma = %s, lemma_key = %s WHERE id = %s;",
                    (corrected, corrected, new_key, uid),
                )
                cur.execute(
                    """INSERT INTO bt_3_lex_surfaces (lang, surface_key, unit_id, match_kind)
                       VALUES (%s, %s, %s, 'exact') ON CONFLICT DO NOTHING;""",
                    (lang, new_key, uid),
                )
                # Карточки, которые держат это слово, показывают своё написание —
                # правим и его, иначе человек по-прежнему видит обрезок.
                cur.execute(
                    """UPDATE bt_3_webapp_dictionary_queries SET word_de = %s
                       WHERE lex_unit_id = %s AND word_de = %s;""",
                    (corrected, uid, display),
                )
                fixed += 1
                print("   %s: %r → %r" % (uid, display[:40], corrected[:40]))
                conn.commit()
            conn.commit()
    print()
    print("ИСПРАВЛЕНО: %d, корректор не нашёл ошибки: %d, пропущено по столкновению: %d"
          % (fixed, untouched, collision))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
