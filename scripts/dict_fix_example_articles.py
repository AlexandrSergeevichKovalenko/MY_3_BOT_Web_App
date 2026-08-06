"""Привести артикль в ПРИМЕРЕ в соответствие с заголовком карточки.

Что видел владелец: карточка «die Titanic», а под примерами — «Der Titanic sank 1912.»
Заголовок верный, врёт пример. Такой пример учит неправде, и это хуже, чем его отсутствие.

Почему нельзя чинить в лоб. «der Einwohner» → «Die Einwohner dieses Viertels sind sehr
freundlich.» выглядит так же, но это множественное число, и «die» там верно. Поэтому
правим ТОЛЬКО когда за парой «артикль + наше слово» стоит глагол в ЕДИНСТВЕННОМ числе
(«sank», «produziert», «lässt»). Множественное («tragen», «sind», «wackeln») не трогаем.

И только в начале предложения: там существительное стоит в именительном падеже, где
артикль обязан совпадать с родом. В середине фразы «mit der Hacke» — это дательный
падеж, и «der» у женского слова совершенно правильно. Кто правит по всей строке, ломает
хороший немецкий.

Заголовки к этому моменту уже сверены со справочником родов
(`dict_headword_article_audit.py`), поэтому равняем пример по заголовку, а не наоборот.

По умолчанию НИЧЕГО НЕ ПИШЕТ. Запись — только с --apply.

    python scripts/dict_fix_example_articles.py           # вхолостую
    python scripts/dict_fix_example_articles.py --apply   # записать
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
from article_authority import authoritative_article  # noqa: E402

HEAD_RE = re.compile(r"^\s*(der|die|das)\s+([A-ZÄÖÜ][\wÄÖÜäöüß-]*)", re.IGNORECASE)
PLURAL_VERBS = {"sind", "haben", "waren", "werden", "wurden", "hatten", "können",
                "müssen", "wollen", "sollen", "dürfen", "mögen"}

# Не глаголы: определения, предлоги, местоимения. Если сразу за словом стоит одно из
# них, значит подлежащее ещё не кончилось — «Die Einwohner DIESES Viertels sind…»,
# «Die Träger DES alten Gebäudes sind…». Число такого подлежащего по одному слову не
# определить, и трогать пример нельзя: он, скорее всего, верный.
NOT_A_VERB = {
    "der", "die", "das", "des", "dem", "den", "ein", "eine", "eines", "einem", "einen",
    "einer", "dieses", "dieser", "diesem", "diesen", "diese", "mein", "meine", "meines",
    "sein", "seine", "seines", "seiner", "ihr", "ihre", "ihres", "ihrer", "unser",
    "unsere", "vom", "von", "zum", "zur", "im", "in", "an", "auf", "aus", "bei", "mit",
    "nach", "über", "unter", "für", "und", "oder", "als", "wie", "am",
}


def not_a_verb(token: str) -> bool:
    return str(token or "").strip(".,;:!?").lower() in NOT_A_VERB


def looks_plural(verb: str) -> bool:
    v = str(verb or "").strip(".,;:!?").lower()
    if not v:
        return True          # не разобрали — не трогаем
    return v in PLURAL_VERBS or v.endswith("en") or v.endswith("n")


def fix_examples(headword: str, examples, *, confirmed: bool) -> tuple[list, list]:
    """Правим пример ТОЛЬКО когда заголовок подтверждён. Иначе бывает наоборот: врёт
    заголовок, а пример верен («Das Zylinder müsste…» при верном «der Zylinder»), и
    подгонка примера под заголовок делает хуже."""
    m = HEAD_RE.match(headword or "")
    if not m or not isinstance(examples, list) or not confirmed:
        return examples if isinstance(examples, list) else [], []
    article, noun = m.group(1).lower(), m.group(2)
    pattern = re.compile(r"^(\s*)(Der|Die|Das)(\s+" + re.escape(noun) + r"\s+)(\S+)")
    out, changed = [], []
    for item in examples:
        if not isinstance(item, dict):
            out.append(item)
            continue
        src = str(item.get("source") or "")
        hit = pattern.match(src)
        if (not hit or hit.group(2).lower() == article
                or not_a_verb(hit.group(4)) or looks_plural(hit.group(4))):
            out.append(item)
            continue
        fixed_article = article.capitalize()
        fixed = pattern.sub(lambda mm: mm.group(1) + fixed_article + mm.group(3) + mm.group(4), src, count=1)
        new_item = dict(item)
        new_item["source"] = fixed
        out.append(new_item)
        changed.append((src, fixed))
    return out, changed


def _confirmed(headword: str, own_article) -> bool:
    """Заголовку можно верить, если его подтвердил арбитр рода ИЛИ если сам разбор
    называет тот же артикль (у имён собственных вроде «die Titanic» справочника нет,
    но склонение в разборе своё и согласовано)."""
    m = HEAD_RE.match(headword or "")
    if not m:
        return False
    article, noun = m.group(1).lower(), m.group(2)
    right, basis = authoritative_article(noun, allow_network=False)
    if right and basis == "wiktionary":
        return right == article
    return str(own_article or "").strip().lower() == article


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT id, word_de, response_json->'usage_examples',
                          response_json->>'article'
                   FROM bt_3_webapp_dictionary_queries
                   WHERE jsonb_typeof(response_json->'usage_examples') = 'array'
                     AND word_de ~* '^(der|die|das) [A-ZÄÖÜ]';"""
            )
            cards = []
            for cid, word_de, examples, own_article in cur.fetchall():
                kept, changed = fix_examples(
                    word_de, examples, confirmed=_confirmed(word_de, own_article))
                if changed:
                    cards.append((cid, word_de, kept, changed))

            cur.execute(
                """SELECT id, display, card->'usage_examples', card->>'article'
                   FROM bt_3_lex_units
                   WHERE jsonb_typeof(card->'usage_examples') = 'array'
                     AND display ~* '^(der|die|das) [A-ZÄÖÜ]';"""
            )
            units = []
            for uid, display, examples, own_article in cur.fetchall():
                kept, changed = fix_examples(
                    display, examples, confirmed=_confirmed(display, own_article))
                if changed:
                    units.append((uid, display, kept, changed))

            print("КАРТОЧКИ: %d" % len(cards))
            for cid, word, _kept, changed in cards[:20]:
                print("   card=%s  заголовок %r\n      %r → %r"
                      % (cid, word[:34], changed[0][0][:52], changed[0][1][:52]))
            print("СЛОВА: %d" % len(units))
            for uid, word, _kept, changed in units[:20]:
                print("   unit=%s  слово %r\n      %r → %r"
                      % (uid, word[:34], changed[0][0][:52], changed[0][1][:52]))

            if not args.apply:
                print()
                print("ВХОЛОСТУЮ. Записать: --apply")
                return 0

            for cid, _w, kept, _c in cards:
                cur.execute(
                    """UPDATE bt_3_webapp_dictionary_queries
                       SET response_json = jsonb_set(response_json, '{usage_examples}', %s::jsonb)
                       WHERE id = %s;""",
                    (json.dumps(kept, ensure_ascii=False), cid),
                )
            for uid, _w, kept, _c in units:
                cur.execute(
                    """UPDATE bt_3_lex_units
                       SET card = jsonb_set(card, '{usage_examples}', %s::jsonb)
                       WHERE id = %s;""",
                    (json.dumps(kept, ensure_ascii=False), uid),
                )
            conn.commit()
    print()
    print("ИСПРАВЛЕНО: карточек %d, слов %d" % (len(cards), len(units)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
