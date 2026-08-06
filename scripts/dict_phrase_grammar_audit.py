"""Проверить грамматику НЕМЕЦКИХ ФРАЗ и предложений в общем словаре.

Почему отдельно от слов. Артикль у слова сверяется со справочником: Wiktionary знает
род «Hoden» и «Verzeichnis». Для фразы справочника нет — «der Titanic rammen ein Eisberg
und beginnen zu sinken» ни в каком словаре не лежит. Грамматику предложения может
проверить только язык, поэтому здесь спрашиваем корректор — тот же, что стоит на входе
словаря (`run_quick_correct`).

Замер 06.08.2026: в общем словаре 9 164 немецкие фразы и предложения короче 120 знаков,
которые видит любой человек в поиске. Их правильность не проверялась НИ РАЗУ.

Осторожность. Корректор возвращает исправленную строку или молчит. Слепо применять его
ответ нельзя: на длинной фразе он иногда переписывает стиль, а не ошибку. Поэтому:
  • правку принимаем, только если изменилось НЕ БОЛЬШЕ трети знаков — большая
    переделка это уже не исправление ошибки, а чужой текст;
  • число слов должно совпасть: корректор не имеет права дописывать или выкидывать
    слова из фразы человека;
  • всё, что он предложил переписать сильнее, показываем в отчёте и НЕ трогаем.

По умолчанию НИЧЕГО НЕ ПИШЕТ и берёт выборку. Полный прогон — `--all`, запись — `--apply`.

    python scripts/dict_phrase_grammar_audit.py --sample 200     # проба
    python scripts/dict_phrase_grammar_audit.py --all            # весь словарь, вхолостую
    python scripts/dict_phrase_grammar_audit.py --all --apply    # починить
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor

_here = os.path.dirname(os.path.abspath(globals().get("__file__", ".")))
sys.path.insert(0, os.path.join(_here, ".."))
sys.path.insert(0, os.path.join(_here, "..", "backend"))
sys.path.insert(0, "/app/backend")

from database import get_db_connection_context  # noqa: E402

WORD = re.compile(r"[^\W\d_]+", re.UNICODE)
WORKERS = 8


def edit_ratio(a: str, b: str) -> float:
    """Доля изменённых знаков — грубо, по длине общего начала и конца."""
    a, b = a or "", b or ""
    if not a:
        return 1.0
    head = 0
    while head < min(len(a), len(b)) and a[head] == b[head]:
        head += 1
    tail = 0
    while (tail < min(len(a), len(b)) - head and a[-1 - tail] == b[-1 - tail]):
        tail += 1
    return (max(len(a), len(b)) - head - tail) / float(len(a))


def acceptable(original: str, corrected: str) -> bool:
    if not corrected or corrected == original:
        return False
    if len(WORD.findall(original)) != len(WORD.findall(corrected)):
        return False
    return edit_ratio(original, corrected) <= 0.33


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--all", action="store_true")
    parser.add_argument("--sample", type=int, default=200)
    args = parser.parse_args()

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT u.id, u.display FROM bt_3_lex_units u
                   WHERE u.lang = 'de' AND u.kind IN ('sentence', 'collocation')
                     AND length(u.display) <= 120
                     AND EXISTS (SELECT 1 FROM bt_3_lex_links l JOIN bt_3_lex_units v ON v.id = l.to_unit
                                 WHERE l.from_unit = u.id AND v.lang = 'ru')
                   ORDER BY %s LIMIT %s;"""
                % ("u.id" if args.all else "random()", 10 ** 9 if args.all else args.sample)
            )
            rows = cur.fetchall()

    print("ПРОВЕРЯЕМ ФРАЗ: %d" % len(rows))
    from openai_manager import run_quick_correct

    def check(row):
        uid, text = row
        try:
            return uid, text, run_quick_correct(text=text, source_lang="de")
        except Exception:
            return uid, text, ""

    results = []
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        for i, item in enumerate(pool.map(check, rows), 1):
            results.append(item)
            if i % 250 == 0:
                print("   проверено %d из %d" % (i, len(rows)))

    fixable = [(uid, t, c) for uid, t, c in results if acceptable(t, c)]
    heavy = [(uid, t, c) for uid, t, c in results
             if c and c != t and (uid, t, c) not in fixable]
    clean = len(results) - len(fixable) - len(heavy)

    print()
    print("КОРРЕКТОР НЕ НАШЁЛ ОШИБКИ:        %d (%.1f%%)" % (clean, 100.0 * clean / max(1, len(results))))
    print("МЕЛКАЯ ПРАВКА (принимаем):        %d" % len(fixable))
    print("ПЕРЕПИСАЛ СИЛЬНО (не трогаем):    %d" % len(heavy))
    print()
    for uid, t, c in fixable[:30]:
        print("   %s\n      было:  %r\n      стало: %r" % (uid, t[:70], c[:70]))
    if heavy:
        print()
        print("   -- не приняли, слишком большая переделка --")
        for uid, t, c in heavy[:15]:
            print("   %s\n      было:  %r\n      хотел: %r" % (uid, t[:70], c[:70]))

    if not args.apply:
        print()
        print("ВХОЛОСТУЮ. Починить: --apply (и --all для всего словаря)")
        return 0

    from lex_units import normalize_query
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            written = skipped = 0
            for uid, _text, corrected in fixable:
                key = normalize_query(corrected)
                if not key:
                    continue
                cur.execute(
                    "SELECT id FROM bt_3_lex_units WHERE lang='de' AND lemma_key=%s AND id<>%s LIMIT 1;",
                    (key, uid),
                )
                if cur.fetchone():
                    skipped += 1
                    continue
                cur.execute(
                    "UPDATE bt_3_lex_units SET display=%s, lemma=%s, lemma_key=%s WHERE id=%s;",
                    (corrected, corrected, key, uid),
                )
                cur.execute(
                    """INSERT INTO bt_3_lex_surfaces (lang, surface_key, unit_id, match_kind)
                       VALUES ('de', %s, %s, 'exact') ON CONFLICT DO NOTHING;""",
                    (key, uid),
                )
                written += 1
            conn.commit()
    print()
    print("ИСПРАВЛЕНО ФРАЗ: %d, пропущено по столкновению: %d" % (written, skipped))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
