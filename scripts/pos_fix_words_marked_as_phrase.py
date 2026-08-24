# -*- coding: utf-8 -*-
"""Одиночное слово, помеченное «фразой», получает свою настоящую часть речи.

ОТКУДА ЗАДАЧА. Бесплатный проход 24.08.2026 по 5323 одиночным словам: 5171 (97%)
справочник подтвердил, а 151 «не знает». Разбор этих 151 показал, что слова там
НАСТОЯЩИЕ — «lügen», «zustimmen», «gängig», «rutschen» — просто у них сбита часть речи:
они лежат как `phrase`, и потому не попадают ни в таблицу глаголов, ни в таблицу
прилагательных, ни в таблицу существительных. Не находится не слово, а его полка.

ПОЧЕМУ ИСТОЧНИК — СПРАВОЧНИК, А НЕ РАЗБОР КАРТОЧКИ. У самих карточек часть речи тоже
неверна: у «zustimmen», «rutschen», «dröge» в разборе стоит тот же `phrase`. Спрашивать
их бессмысленно — они и есть источник ошибки. Часть речи печатает de.wiktionary
(`{{Wortart|…}}`), её и берём через дверь слова, без модели и без денег.

ЧТО ПОКАЗАЛ ПРОГОН по 52 одиночным словам с пометкой «фраза»:

    37  глагол        abbestellen, abhalten, aufwickeln, ausrasten, besorgen…
     9  прилагательное dreist, dröge, gängig, kontaktfreudig, massig…
     2  наречие       soeben, zudem
     4  справочник части речи не назвал — НЕ ТРОГАЕМ

Последние четыре («entlang», «prägen», «reinlaufen», «vervollkommnen») остаются как
были: молчание источника — не разрешение решить за него.

ЗАПУСК:
    python3 scripts/pos_fix_words_marked_as_phrase.py           # показать
    python3 scripts/pos_fix_words_marked_as_phrase.py --apply   # применить
"""
from __future__ import annotations

import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

ОТБОР = """
 SELECT id, display FROM bt_3_lex_units
  WHERE lang='de' AND kind='word' AND display <> ''
    AND pos='phrase' AND display NOT LIKE '% %'
  ORDER BY display
"""


def main() -> int:
    apply = "--apply" in sys.argv
    from backend.database import get_db_connection_context
    from backend.german_word_gate import check_word

    with get_db_connection_context() as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(ОТБОР)
            слова = [(int(a), str(b)) for a, b in (cur.fetchall() or [])]

    print(f"одиночных слов с пометкой «фраза»: {len(слова)}")
    правки, молчит = [], []
    for uid, слово in слова:
        вердикт = check_word(слово, allow_network=True, allow_model=False)
        часть = str(вердикт.get("pos") or "").strip()
        (правки if часть else молчит).append((uid, слово, часть))
        # Справочник уходит в отказ по частоте, а отказ неотличим от «не знаю».
        time.sleep(3.5)

    по_части: dict[str, list[str]] = {}
    for _uid, слово, часть in правки:
        по_части.setdefault(часть, []).append(слово)
    for часть, список in sorted(по_части.items(), key=lambda x: -len(x[1])):
        print(f"  {len(список):3}  → {часть}")
        print(f"        {', '.join(список[:12])}")
    print(f"  {len(молчит):3}  справочник часть речи не назвал — НЕ ТРОГАЕМ")
    print(f"        {', '.join(с for _u, с, _ in молчит)}")

    if not apply:
        print("\nЭто показ. Чтобы применить — добавь --apply")
        return 0

    сделано = 0
    for uid, слово, часть in правки:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE bt_3_lex_units SET pos=%s, pos_source=%s, updated_at=NOW() "
                            "WHERE id=%s;", (часть, "справочник (часть речи)", uid))
                сделано += cur.rowcount
            conn.commit()
    print(f"\nЧасть речи поправлена у {сделано} слов")

    # Проверка ФАКТОМ: спрашиваем базу заново.
    with get_db_connection_context() as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(ОТБОР)
            осталось = len(cur.fetchall() or [])
    print(f"Осталось помеченных «фразой»: {осталось}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
