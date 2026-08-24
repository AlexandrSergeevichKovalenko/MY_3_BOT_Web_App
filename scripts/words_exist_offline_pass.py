# -*- coding: utf-8 -*-
"""Бесплатный проход: существует ли слово вообще. Без сети и без модели.

ЗАЧЕМ. Прогон 23–24.08.2026 проверил у карточек ПОЛЯ: род (2886), множественное (2886),
формы глаголов (1326), фразы через панель из трёх голосов (5073). Но вопрос «а есть ли
такое слово» не задавался ни разу — и потому у обрубка «Erwer» род честно проверили и
даже подтвердили множественное. Проверять род у несуществующего слова — всё равно что
проверять срок годности у пустой упаковки: всё сходится, а товара нет.

ПОЧЕМУ ЭТОТ ПРОХОД НИЧЕГО НЕ СТОИТ. С 23.08 у нас лежит офлайн-выгрузка на 89 704
существительных плюс свои таблицы спряжений и степеней сравнения. Ответ берётся с
диска: ни одного обращения к сети, ни одного цента.

ЧТО НА ВЫХОДЕ. Три кучи, и смешивать их нельзя:
    ЕСТЬ           справочник знает слово — вопрос закрыт навсегда, платить не за что
    НЕТ В ЛЕММАХ   слово не найдено, но найдено как ФОРМА другого слова — тогда дефект
                   в заголовке, а не в слове, и лечится переименованием
    НЕ ЗНАЕМ       справочник молчит — вот только это и уходит к модели, за деньги

ЗАПУСК:
    python3 scripts/words_exist_offline_pass.py
"""
from __future__ import annotations

import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

_АРТИКЛЬ = re.compile(r"^(der|die|das)\s+", re.I)


def main() -> int:
    from backend.database import get_db_connection_context

    with get_db_connection_context() as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("""SELECT id, display, pos FROM bt_3_lex_units
                            WHERE lang='de' AND kind='word' AND display<>'' ORDER BY id""")
            слова = [(int(a), str(b), str(c or "")) for a, b, c in (cur.fetchall() or [])]
            cur.execute("SELECT lower(noun) FROM bt_3_german_noun_declensions")
            существительные = {r[0] for r in cur.fetchall()}
            cur.execute("SELECT lower(verb) FROM bt_3_german_verb_paradigms")
            глаголы = {r[0] for r in cur.fetchall()}
            cur.execute("SELECT lower(adjective) FROM bt_3_german_adjective_degrees")
            прилагательные = {r[0] for r in cur.fetchall()}
            cur.execute("SELECT lower(surface), lemma FROM bt_3_german_form_index")
            указатель = dict(cur.fetchall())

    справочник = существительные | глаголы | прилагательные
    print(f"слов в словаре:        {len(слова)}")
    print(f"слов в справочнике:    {len(справочник)} "
          f"(сущ. {len(существительные)}, глаг. {len(глаголы)}, прил. {len(прилагательные)})")

    есть, форма_другого, не_знаем = [], [], []
    for uid, display, pos in слова:
        голое = _АРТИКЛЬ.sub("", display).strip()
        if not голое or " " in голое:
            continue
        low = голое.lower()
        if low in справочник:
            есть.append(голое)
        elif указатель.get(low) and указатель[low].lower() != low:
            форма_другого.append((голое, указатель[low]))
        else:
            не_знаем.append((uid, голое, pos))

    всего = len(есть) + len(форма_другого) + len(не_знаем)
    print()
    print(f"проверено одиночных слов: {всего}")
    print(f"  ЕСТЬ в справочнике:      {len(есть):5}  ({len(есть)*100//max(1,всего)}%) — платить не за что")
    print(f"  форма ДРУГОГО слова:     {len(форма_другого):5}  — дефект заголовка, лечится переименованием")
    print(f"  справочник НЕ ЗНАЕТ:     {len(не_знаем):5}  — только это к модели, за деньги")
    print()
    print("формы другого слова, примеры:")
    for г, л in форма_другого[:10]:
        print(f"    {г:26} это форма от «{л}»")
    print()
    print("справочник не знает, примеры:")
    for uid, г, pos in не_знаем[:15]:
        print(f"    {г:26} ({pos})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
