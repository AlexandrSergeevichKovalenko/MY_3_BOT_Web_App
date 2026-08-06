"""Убрать из ОБЩЕГО словаря изуродованные двойники фраз и починить артикль в старом слое.

Что видел владелец. Поиск «Titan» показывает в разделе «В ОБЩЕМ СЛОВАРЕ» строку
«der Titanic rammen ein Eisberg und beginnen zu sinken» — ту же фразу, что и его
карточка, но прогнанную через приведение к начальной форме: артикль сменился на «der»,
глаголы встали в неопределённую форму. Такого немецкого не существует, а мы предлагаем
это другим людям как слово из общего словаря.

Раньше я починил ПРИМЕРЫ внутри разбора и решил, что закрыл вопрос. Это была половина
работы: сама изуродованная запись осталась лежать в слое слов и в старом словаре.

Как отличаем урода от законного соседа. Двух признаков сразу:
  1. рядом есть запись с ТЕМ ЖЕ переводом, у которой все слова те же по основе, а
     отличаются только окончания и артикль;
  2. на уродца не ссылается НИ ОДНА карточка человека, а на правильного — ссылается.
Второй признак и есть решающий: люди сохраняли себе правильную фразу, а не эту.
Без него правило симметрично и одинаково обвиняет обе стороны пары.

Отдельно: артикль в старом словаре. Там лежит «der Titanic», хотя в слое слов и в
карточке уже «die Titanic». Равняем старый слой по слою слов — он ведущий.

По умолчанию НИЧЕГО НЕ ПИШЕТ. Запись — только с --apply.

    python scripts/dict_drop_mangled_entries.py           # вхолостую
    python scripts/dict_drop_mangled_entries.py --apply   # записать
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
from article_authority import authoritative_article  # noqa: E402

WORD = re.compile(r"[^\W\d_]+", re.UNICODE)
ART = {"der", "die", "das", "den", "dem", "des"}


def is_mangled(good: str, suspect: str) -> bool:
    a = [w.lower() for w in WORD.findall(good or "")]
    b = [w.lower() for w in WORD.findall(suspect or "")]
    if len(a) != len(b) or len(a) < 2 or a == b:
        return False
    diff = 0
    for i, (x, y) in enumerate(zip(a, b)):
        if x == y:
            continue
        diff += 1
        if i == 0 and x in ART and y in ART:
            continue
        if x[:3] != y[:3]:
            return False
    return diff > 0


def collect_units(cur) -> list:
    cur.execute(
        """SELECT u.id, u.display,
                  (SELECT v.display FROM bt_3_lex_links l JOIN bt_3_lex_units v ON v.id = l.to_unit
                   WHERE l.from_unit = u.id AND v.lang = 'ru' ORDER BY l.rank LIMIT 1) AS translation,
                  (SELECT count(*) FROM bt_3_webapp_dictionary_queries q WHERE q.lex_unit_id = u.id) AS cards
           FROM bt_3_lex_units u WHERE u.lang = 'de';"""
    )
    by_translation: dict = {}
    for uid, display, translation, cards in cur.fetchall():
        if not translation:
            continue
        by_translation.setdefault(translation.strip().lower(), []).append((uid, display, int(cards or 0)))
    doomed = []
    for group in by_translation.values():
        if len(group) < 2:
            continue
        for good in group:
            for suspect in group:
                if suspect[0] == good[0]:
                    continue
                # уродца никто не сохранял, правильный — сохраняли
                if suspect[2] == 0 and good[2] > 0 and is_mangled(good[1], suspect[1]):
                    doomed.append((suspect, good))
    seen, uniq = set(), []
    for suspect, good in doomed:
        if suspect[0] in seen:
            continue
        seen.add(suspect[0])
        uniq.append((suspect, good))
    return uniq


def collect_pool_articles(cur) -> list:
    """Записи старого словаря, где артикль расходится со слоем слов.

    Три заслона, без которых этот проход ломает данные:
      • ДВУРОДОВЫЕ. «der Kiefer» челюсть и «die Kiefer» сосна — два разных слова с
        одинаковым написанием. Выравнивание вывернуло бы их друг в друга и уничтожило
        оба. Арбитр рода на таких словах молчит, и мы их пропускаем.
      • МНОЖЕСТВЕННОЕ. «die Reifen», «die Anführungszeichen» — верные заголовки при
        мужском и среднем единственном. Не трогаем ничего, где в словаре стоит «die»,
        а в слое — другой артикль: это почти всегда множественное.
      • НЕПОДТВЕРЖДЁННОЕ СЛОВО. Равнять словарь по слою можно, только если слой сам
        подтверждён справочником — или если слово справочнику неизвестно (имя
        собственное вроде «die Titanic»), но люди сохранили себе именно его.
    """
    cur.execute(
        """SELECT e.id, e.source_text, u.display,
                  (SELECT count(*) FROM bt_3_webapp_dictionary_queries q WHERE q.lex_unit_id = u.id)
           FROM bt_3_dictionary_entries e
           JOIN bt_3_lex_units u
             ON u.lang = 'de'
            AND lower(btrim(regexp_replace(u.display, '^(der|die|das)\\s+', '', 'i')))
              = lower(btrim(regexp_replace(e.source_text, '^(der|die|das)\\s+', '', 'i')))
           WHERE e.source_lang = 'de'
             AND e.source_text ~* '^(der|die|das) '
             AND u.display ~* '^(der|die|das) '
             AND lower(split_part(btrim(e.source_text), ' ', 1)) <> lower(split_part(btrim(u.display), ' ', 1));"""
    )
    rows = cur.fetchall()
    # Двуродовые заведены в словаре ОБЕИМИ строками намеренно: «der Gehalt» зарплата и
    # «das Gehalt» содержание, «der Kunde» клиент и «die Kunde» весть. Если для одного
    # написания есть две записи с разными артиклями — это пара, а не ошибка.
    seen_nouns: dict = {}
    for eid, text, display, cards in rows:
        key = text.strip().split(" ", 1)[-1].lower()
        seen_nouns.setdefault(key, set()).add(text.strip().split(" ", 1)[0].lower())
    out = []
    for eid, text, display, cards in rows:
        if len(seen_nouns.get(text.strip().split(" ", 1)[-1].lower(), set())) > 1:
            continue
        pool_article = text.strip().split(" ", 1)[0].lower()
        unit_article = display.strip().split(" ", 1)[0].lower()
        noun = display.strip().split(" ", 1)[1].split(" ")[0] if " " in display.strip() else ""
        if pool_article == "die" and unit_article != "die":
            continue                       # почти наверняка множественное число
        right, basis = authoritative_article(noun, allow_network=False)
        if right and basis == "wiktionary":
            if right != unit_article:
                continue                   # слой сам не подтверждён — равнять не по чему
        elif not (right is None and int(cards or 0) > 0):
            continue                       # справочник молчит и слово никто не сохранял
        out.append((eid, text, display))
    return out


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            doomed = collect_units(cur)
            pool = collect_pool_articles(cur)

            print("ИЗУРОДОВАННЫЕ ЗАПИСИ В ОБЩЕМ СЛОВАРЕ: %d" % len(doomed))
            for (uid, bad, _c), (gid, good, cards) in doomed[:25]:
                print("   убираем %s %r\n      остаётся %s %r (карточек %d)"
                      % (uid, bad[:58], gid, good[:58], cards))
            print("СТАРЫЙ СЛОВАРЬ, АРТИКЛЬ РАСХОДИТСЯ СО СЛОВОМ: %d" % len(pool))
            for eid, text, display in pool[:25]:
                print("   %s: %r → %r" % (eid, text[:48], display[:48]))

            if not args.apply:
                print()
                print("ВХОЛОСТУЮ. Записать: --apply")
                return 0

            for (uid, _bad, _c), _good in doomed:
                cur.execute("DELETE FROM bt_3_lex_units WHERE id = %s;", (uid,))
            for eid, _text, display in pool:
                cur.execute(
                    "UPDATE bt_3_dictionary_entries SET source_text = %s WHERE id = %s;",
                    (display, eid),
                )
            conn.commit()
    print()
    print("УБРАНО ЗАПИСЕЙ: %d, ПОПРАВЛЕНО АРТИКЛЕЙ В СТАРОМ СЛОВАРЕ: %d"
          % (len(doomed), len(pool)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
