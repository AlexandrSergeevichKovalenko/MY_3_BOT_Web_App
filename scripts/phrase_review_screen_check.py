# -*- coding: utf-8 -*-
"""Что человек ВИДИТ НА ЭКРАНЕ после решений владельца по спорным фразам.

Вопрос владельца 20.08.2026: «Я выбрал фразу — верно ли она сохраняется и верно ли
берётся русский перевод?»

Отличие от `phrase_review_landing_audit.py`, и ради него скрипт и заведён: тот смотрит,
ЧТО ЗАПИСАНО В БАЗЕ, и по всем 119 решениям говорил «чисто». А врала выдача: в базе
выбор владельца лежал первым (связь «вычитка», ранг 1), на экране показывался вторым —
56 случаев из 119, и ещё в 3 пропадал совсем. Поэтому здесь мы не читаем таблицы, а
спрашиваем словарь ТЕМ ЖЕ КОДОМ, которым его спрашивает приложение (`lex_units.lookup`),
и сверяем ответ с решением человека. Проверять надо путём экрана, иначе проверка врёт.

Скрипт ТОЛЬКО ЧИТАЕТ. Ничего не чинит и ничего не удаляет.

    python3 scripts/phrase_review_screen_check.py
    python3 scripts/phrase_review_screen_check.py --list 20   # + поимённо

ВЕРДИКТ 21.08.2026: после правки порядка выдачи (`lex_units._fetch_links`) и защиты
выбора владельца от правила «остаётся короткий» (`drop_nested_translations`) — расхождений
ноль. Числа до правки: немецкий 119/119 верно, русский 49 первым / 56 вторым / 3 пропали.
"""
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")

from backend.database import get_db_connection_context  # noqa: E402
from backend.lex_units import OWNER_CHOICE_SOURCE, lookup  # noqa: E402


def same_meaning(value: str) -> str:
    """Русские переводы сверяем БЕЗ регистра и концевого знака.

    В немецком регистр — грамматика, и там сравнение точное. В русском переводе
    заглавная в начале и точка в конце — оформление: выдача опускает заглавную у
    словарной статьи (`normalize_translation_case`), а `ensure_unit` переиспользует уже
    лежащую единицу «чтобы хоть как-то решить проблему холодных гостиных» вместо той же
    строки с запятой. Считать это подменой перевода — соврать: 20.08.2026 такое сравнение
    насчитало 8 подмен там, где их 2.
    """
    return str(value or "").strip().casefold().rstrip(".,!?;: ")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--list", type=int, default=0, help="сколько случаев показать поимённо")
    args = parser.parse_args()

    german_wrong: list[tuple] = []
    russian_second: list[tuple] = []
    russian_gone: list[tuple] = []
    no_owner_ru: list[tuple] = []
    owner_first = 0
    total = 0

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """SELECT r.id, r.unit_id, u.display
                     FROM bt_3_phrase_review r
                     JOIN bt_3_lex_units u ON u.id = r.unit_id
                    WHERE r.status IN ('accepted', 'replaced')
                    ORDER BY r.id;"""
            )
            decisions = cursor.fetchall()

            for review_id, unit_id, display in decisions:
                total += 1
                # 1. НЕМЕЦКИЙ. На экране обязан стоять текст, который выбрал владелец.
                item = lookup(display, source_lang="de", target_lang="ru")
                shown_de = str((item or {}).get("word_de")
                               or (item or {}).get("source_text") or "")
                if not item or shown_de.strip() != str(display).strip():
                    german_wrong.append((review_id, unit_id, display, shown_de))
                    continue

                # 2. РУССКИЙ. Выбор владельца — связь «вычитка» с рангом 1.
                cursor.execute(
                    """SELECT t.display FROM bt_3_lex_links l
                         JOIN bt_3_lex_units t ON t.id = l.to_unit
                        WHERE l.from_unit = %s AND l.source = %s AND l.rank = 1
                        LIMIT 1;""",
                    (unit_id, OWNER_CHOICE_SOURCE),
                )
                row = cursor.fetchone()
                if not row:
                    # Владелец вписал свой немецкий и русского не назвал — перевод
                    # собрала модель. С 21.08.2026 на экране разбора есть поле для
                    # русского, поэтому число должно перестать расти, а не обнулиться:
                    # прошлые решения так и останутся без его перевода.
                    no_owner_ru.append((review_id, unit_id, display))
                    continue
                owner_ru = row[0]
                shown_ru = [t.get("value") for t in ((item or {}).get("translations") or [])]
                if shown_ru and same_meaning(shown_ru[0]) == same_meaning(owner_ru):
                    owner_first += 1
                elif any(same_meaning(v) == same_meaning(owner_ru) for v in shown_ru):
                    russian_second.append((review_id, unit_id, display, owner_ru, shown_ru[0]))
                else:
                    russian_gone.append((review_id, unit_id, display, owner_ru, shown_ru))

    print(f"\nРЕШЕНИЙ ВЛАДЕЛЬЦА С ПРАВКОЙ ТЕКСТА: {total}\n")
    rows = [
        ("❌", "Немецкий на экране НЕ тот, что выбрал владелец", german_wrong),
        ("❌", "Русский владельца на экране ПРОПАЛ", russian_gone),
        ("❌", "Машинный перевод стоит ВЫШЕ выбора владельца", russian_second),
        ("ℹ️", "Владелец русский не выбирал — перевод собрала модель", no_owner_ru),
    ]
    print(f"✅ Перевод владельца показан ПЕРВЫМ: {owner_first}")
    for mark, title, hits in rows:
        print(f"{'✅' if not hits else mark} {title}: {len(hits)}")
        for item in hits[: args.list]:
            print(f"      {item}")
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
