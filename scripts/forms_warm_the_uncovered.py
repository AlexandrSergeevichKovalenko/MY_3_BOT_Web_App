# -*- coding: utf-8 -*-
"""Догреть формы у слов, которым таблицы не досталось, и назвать остаток по классам.

ЗАЧЕМ. 23.08.2026 из фронта убран счётчик, который дорисовывал таблицу, когда сервер
её не давал. Замер по живой базе: без таблицы остаются 34 слова из 4971. На их месте
человек видит «уточняем формы — появятся в ближайшую ночь», и это обещание надо либо
выполнить, либо не давать.

ЧТО ВЫЯСНИЛОСЬ ПРИ ПРОВЕРКЕ. «34 сломанных заголовка» — снова не один класс:

    14 из 15 проверенных   СПРАВОЧНИК ИХ ЗНАЕТ. Заголовок целый, просто ночная
                           порция (120 слов за раз) до них ещё не дошла.
                           Чинить нечего — надо прогреть.
    остальные              заголовок вправду негоден: старое написание («entschluß»),
                           множественное строчными («zustimmungen»), неверная часть
                           речи («aber», «gehen» помечены существительными),
                           субстантивированные прилагательные («der Brave»).

Поэтому скрипт НЕ ПРАВИТ ЗАГОЛОВКИ. Он делает ровно одно: спрашивает справочник про
каждое слово и сохраняет то, что справочник напечатал. Всё, что не закрылось, выводится
разложенным по классам — это и есть настоящий список работ, а не сырое «34».

ЗАПУСК:
    python3 scripts/forms_warm_the_uncovered.py           # показать
    python3 scripts/forms_warm_the_uncovered.py --apply   # прогреть
"""
from __future__ import annotations

import os
import re
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

БЕЗ_ТАБЛИЦЫ = """
 SELECT u.id, u.pos, u.display FROM bt_3_lex_units u
  WHERE u.lang='de' AND u.display <> '' AND (
   (u.pos='noun' AND NOT EXISTS (SELECT 1 FROM bt_3_german_noun_declensions d
      WHERE lower(d.noun)=lower(regexp_replace(u.display,'^(der|die|das)[[:space:]]+','','i'))))
   OR (u.pos='adjective' AND NOT EXISTS (SELECT 1 FROM bt_3_german_adjective_degrees a
      WHERE lower(a.adjective)=lower(regexp_replace(u.display,'^(der|die|das)[[:space:]]+','','i'))))
   OR (u.pos='verb' AND NOT EXISTS (SELECT 1 FROM bt_3_german_verb_paradigms p
      WHERE lower(p.verb)=lower(u.display))))
  ORDER BY u.pos, u.display
"""

_АРТИКЛЬ = re.compile(r"^(der|die|das)\s+", re.I)


def _класс(display: str, pos: str) -> str:
    """К какому виду негодности относится заголовок, если справочник его не взял."""
    голое = _АРТИКЛЬ.sub("", display).strip()
    if " " in голое:
        return "это фраза, а не слово"
    if "ß" in голое:
        return "дореформенное написание"
    if pos == "noun" and голое[:1].islower():
        return "существительное со строчной буквы"
    if pos == "verb" and _АРТИКЛЬ.match(display):
        return "глагол записан с артиклем"
    if pos == "noun" and голое.lower() in ("aber", "wenn", "vier", "gehen", "schwimmen"):
        return "часть речи неверна"
    if pos == "adjective" and _АРТИКЛЬ.match(display):
        return "субстантивированное прилагательное"
    return "справочник не знает этого слова"


def main() -> int:
    apply = "--apply" in sys.argv
    from backend.database import get_db_connection_context
    from backend.german_reference_forms import adjective_degrees_for, noun_declension_for
    from backend.german_verb_paradigms import paradigm_for_verb as verb_paradigm_for

    with get_db_connection_context() as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(БЕЗ_ТАБЛИЦЫ)
            слова = [(int(a), str(b), str(c)) for a, b, c in (cur.fetchall() or [])]

    print(f"Слов без таблицы форм: {len(слова)}")
    if not apply:
        for uid, pos, disp in слова:
            print(f"  {uid:6} {pos:10} {disp}")
        print("\nЭто показ. Чтобы спросить справочник и сохранить ответ — добавь --apply")
        return 0

    закрыто, остаток = [], []
    for uid, pos, disp in слова:
        голое = _АРТИКЛЬ.sub("", disp).strip()
        таблица = None
        try:
            if pos == "noun":
                таблица = noun_declension_for(голое, allow_network=True)
            elif pos == "adjective":
                таблица = adjective_degrees_for(голое, allow_network=True)
            elif pos == "verb":
                таблица = verb_paradigm_for(голое, allow_network=True)
        except Exception as exc:
            print(f"  сбой на {disp!r}: {exc}")
        (закрыто if таблица else остаток).append((uid, pos, disp))
        # Справочник уходит в отказ по частоте, и отказ НЕОТЛИЧИМ от «слова нет».
        # На паузе 1,2 с прогон 23.08.2026 объявил «не знает» четыре слова, которые
        # справочник прекрасно знает («Ratte», «Scherbe», «Rivalität»,
        # «Verbindlichkeit»). Четыре секунды — проверенная пауза, на ней ложных
        # отказов не было.
        time.sleep(4.0)

    print(f"\nЗАКРЫТО справочником: {len(закрыто)}")
    for uid, pos, disp in закрыто:
        print(f"   {disp}")

    по_классам: dict[str, list[str]] = {}
    for uid, pos, disp in остаток:
        по_классам.setdefault(_класс(disp, pos), []).append(f"{disp} ({uid})")
    print(f"\nОСТАЛОСЬ: {len(остаток)} — и это НЕ один класс:")
    for имя, список in sorted(по_классам.items(), key=lambda x: -len(x[1])):
        print(f"   {len(список):3}  {имя}")
        for s in список:
            print(f"          {s}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
