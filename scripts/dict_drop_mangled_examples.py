"""Убрать «примеры», которые на самом деле — та же фраза, прогнанная через приведение
к начальной форме.

Что видит человек (скриншот владельца 06.08.2026). Карточка «die Titanic rammt ein
Eisberg und beginnt zu sinken», а под заголовком ПРИМЕРЫ стоит
«der Titanic rammen ein Eisberg und beginnen zu sinken» — та же фраза с неверным
артиклем и глаголами в неопределённой форме. Это не пример и не немецкий язык: это
след массовой сборки 27 июля, где приведение к начальной форме применили к КАЖДОМУ
слову предложения.

Как отличить урода от настоящего примера — это главное в скрипте. «Das Salz ist alle.»
и «Die Batterien sind alle.» тоже похожи, но это НАСТОЯЩИЙ пример: другое подлежащее.
Поэтому правило узкое: у урода то же число слов И каждое слово совпадает с исходным по
основе (первые три буквы), отличаются только окончания и артикль. Стоит ослабить — и
скрипт начнёт выбрасывать хорошие примеры.

Пример убираем целиком: подставить правильную форму мы не можем, а показывать неверный
немецкий человеку нельзя. Карточка без примера честнее карточки с враньём.

По умолчанию НИЧЕГО НЕ ПИШЕТ. Запись — только с --apply.

    python scripts/dict_drop_mangled_examples.py           # вхолостую
    python scripts/dict_drop_mangled_examples.py --apply   # записать
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys

_here = os.path.dirname(os.path.abspath(globals().get("__file__", ".")))
sys.path.insert(0, os.path.join(_here, ".."))
sys.path.insert(0, os.path.join(_here, "..", "backend"))
sys.path.insert(0, "/app/backend")

from database import get_db_connection_context  # noqa: E402

WORD = re.compile(r"[^\W\d_]+", re.UNICODE)
ARTICLES = {"der", "die", "das", "den", "dem", "des"}


def is_mangled(entry: str, example: str) -> bool:
    a = [w.lower() for w in WORD.findall(entry or "")]
    b = [w.lower() for w in WORD.findall(example or "")]
    if len(a) != len(b) or len(a) < 2 or a == b:
        return False
    differences = 0
    for i, (x, y) in enumerate(zip(a, b)):
        if x == y:
            continue
        differences += 1
        if i == 0 and x in ARTICLES and y in ARTICLES:
            continue  # сменился артикль — самый частый след приведения к начальной форме
        if x[:3] != y[:3]:
            return False  # разные слова → это настоящий пример, не урод
    return differences > 0


def clean_examples(entry: str, examples) -> tuple[list, list]:
    kept, dropped = [], []
    for item in examples if isinstance(examples, list) else []:
        source = (item or {}).get("source") if isinstance(item, dict) else None
        (dropped if source and is_mangled(entry, source) else kept).append(item)
    return kept, dropped


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT id, word_de, response_json FROM bt_3_webapp_dictionary_queries
                   WHERE jsonb_typeof(response_json->'usage_examples') = 'array'
                     AND COALESCE(word_de, '') <> '';"""
            )
            cards = []
            for cid, word_de, card in cur.fetchall():
                kept, dropped = clean_examples(word_de, (card or {}).get("usage_examples"))
                if dropped:
                    cards.append((cid, word_de, kept, dropped))

            cur.execute(
                """SELECT id, display, card FROM bt_3_lex_units
                   WHERE jsonb_typeof(card->'usage_examples') = 'array';"""
            )
            units = []
            for uid, display, card in cur.fetchall():
                kept, dropped = clean_examples(display, (card or {}).get("usage_examples"))
                if dropped:
                    units.append((uid, display, kept, dropped))

            print("КАРТОЧКИ: %d, примеров убираем %d"
                  % (len(cards), sum(len(c[3]) for c in cards)))
            for cid, word, _kept, dropped in cards[:15]:
                print("   card=%s\n      запись: %r\n      убираем: %r"
                      % (cid, (word or "")[:60], str(dropped[0].get("source"))[:60]))
            print("СЛОВА: %d, примеров убираем %d"
                  % (len(units), sum(len(u[3]) for u in units)))
            for uid, word, _kept, dropped in units[:15]:
                print("   unit=%s\n      слово: %r\n      убираем: %r"
                      % (uid, (word or "")[:60], str(dropped[0].get("source"))[:60]))

            if not args.apply:
                print()
                print("ВХОЛОСТУЮ. Записать: --apply")
                return 0

            for cid, _word, kept, _dropped in cards:
                cur.execute(
                    """UPDATE bt_3_webapp_dictionary_queries
                       SET response_json = jsonb_set(response_json, '{usage_examples}', %s::jsonb)
                       WHERE id = %s;""",
                    (json.dumps(kept, ensure_ascii=False), cid),
                )
            for uid, _word, kept, _dropped in units:
                cur.execute(
                    """UPDATE bt_3_lex_units
                       SET card = jsonb_set(card, '{usage_examples}', %s::jsonb)
                       WHERE id = %s;""",
                    (json.dumps(kept, ensure_ascii=False), uid),
                )
            conn.commit()
    print()
    print("ПОЧИНЕНО: карточек %d, слов %d" % (len(cards), len(units)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
