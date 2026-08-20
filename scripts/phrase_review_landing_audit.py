# -*- coding: utf-8 -*-
"""Что РЕАЛЬНО легло в базу после решений владельца по спорным фразам.

Вопрос владельца 19.08.2026: «После того как я выберу корректную фразу — верно ли она
сохраняется и верно ли берётся русский перевод? Раньше под капотом происходила ерунда.»

Скрипт ТОЛЬКО ЧИТАЕТ. Ничего не чинит и ничего не удаляет.

Что проверяется по каждому закрытому вопросу (`accepted` / `replaced`):

  1. НЕМЕЦКИЙ ДОЕХАЛ.     `bt_3_lex_units.display` — это принятый вариант, а не старый
                          текст. Старый текст в `display` = решение не применилось.
  2. ВЫБОР УЗНАЁТСЯ.      Принятый текст совпадает с одним из вариантов судей (или это
                          свой текст владельца). Если не совпадает ни с чем — под капотом
                          записали НЕ ТО, что было на кнопке.
  3. РУССКИЙ ВЛАДЕЛЬЦА ГЛАВНЫЙ. Перевод, который стоял на кнопке рядом с вариантом,
                          лежит связью rank=1 с source='вычитка'. Замер 14.08.2026:
                          из 49 решений перевод владельца был заменён машинным в 30.
  4. КАРТОЧКА ПРО ЭТУ ЖЕ ФРАЗУ. В `card` нет старого текста и есть новый.
  5. ХВОСТОВ НЕТ.         Старого написания не осталось в карточках людей
                          (`bt_3_webapp_dictionary_queries`).

ЧЕГО ЗДЕСЬ СОЗНАТЕЛЬНО НЕ ПРОВЕРЯЕТСЯ. Старое написание в указателе `bt_3_lex_surfaces`
раньше числилось дефектом (87 из 89 решений) — и зря. Указатель для того и заведён, чтобы
одно слово находилось по РАЗНЫМ написаниям: `der Held` ищется и как «der held»,
`erklären` — как «erklärt», предложение с артиклем — и без него (`match_kind` = exact /
inflected / no_article, всего 57 138 написаний, из них 15 765 не совпадают с заголовком —
это норма, а не порча). Человек, сохранивший фразу криво, продолжает её находить по
своему написанию и видит уже исправленный заголовок. Удалить эти ключи значило бы
сломать поиск. Проверено и закрыто 20.08.2026. [[feedback_settle_it_once_in_code]]

    python3 scripts/phrase_review_landing_audit.py
    python3 scripts/phrase_review_landing_audit.py --list 10   # + примеры
"""
from __future__ import annotations

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")

from backend.database import (  # noqa: E402
    _CORRECTION_UNTOUCHED_KEYS,
    _phrase_same_text as _phrase_same_text_ru,
    get_db_connection_context,
    phrase_review_variants,
)


def same(a: str, b: str) -> bool:
    """ТОЧНОЕ сравнение — только пробелы по краям.

    Первая версия этого скрипта сравнивала без регистра и без концевой пунктуации, и
    поэтому объявила «не доехало» одиннадцать решений, где правка КАК РАЗ и состояла в
    регистре («Es ist mir Latte» → «latte») или в точке. Правило отбора нельзя
    придумывать себе отдельно от продукта. [[feedback_settle_it_once_in_code]]
    """
    return str(a or "").strip() == str(b or "").strip()


def _holds_value(node, needle: str) -> bool:
    """Лежит ли `needle` в дереве разбора ОТДЕЛЬНОЙ строкой, а не куском другой.

    Поля из `_CORRECTION_UNTOUCHED_KEYS` пропускаются: продукт хранит там старый текст
    НАМЕРЕННО. `corrected_form` — это след прошлой правки, `original_query` и `raw_text` —
    история, чем человек искал. Переписать их значило бы соврать, что он искал другое.
    Список берётся у самого продукта, чтобы отчёт не разошёлся с ним через неделю.
    """
    if isinstance(node, str):
        return same(node, needle)
    if isinstance(node, list):
        return any(_holds_value(item, needle) for item in node)
    if isinstance(node, dict):
        return any(_holds_value(value, needle) for name, value in node.items()
                   if name not in _CORRECTION_UNTOUCHED_KEYS)
    return False


def applied(variant_text: str) -> str:
    """Текст в том виде, в каком его кладёт в базу apply_phrase_review_decision:
    он прогоняет выбор владельца через clean_text, поэтому сравнивать надо с ним."""
    from backend.dictionary_intake import clean_text
    return str(clean_text(str(variant_text or "")) or "").strip()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--list", type=int, default=0, help="сколько примеров показать")
    args = parser.parse_args()

    findings: dict[str, list] = {
        "left_as_is": [],             # 1. фраза осталась как была
        "punctuation_only": [],       # 2а. правка была только в пунктуации
        "own_text": [],               # 2б. владелец вписал свой текст
        "owner_ru_missing": [],       # 3. русского владельца нет первым
        "owner_ru_unknown": [],       # 3b. на кнопке русского не было вовсе
        "card_stale": [],             # 4. карточка про старую фразу
        "user_card_stale": [],        # 5b. старый текст в карточке человека
        "unit_gone": [],              # слово удалено после решения
    }
    total = 0

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """SELECT id, unit_id, text, translation, judges, arbiter, status
                     FROM bt_3_phrase_review
                    WHERE status IN ('accepted', 'replaced')
                    ORDER BY id;"""
            )
            rows = cursor.fetchall()

            for review_id, unit_id, old_text, saved_ru, judges, arbiter, status in rows:
                total += 1
                judges = judges if isinstance(judges, list) else []
                arbiter = arbiter if isinstance(arbiter, dict) else None
                variants = phrase_review_variants(judges, old_text, arbiter)

                cursor.execute(
                    "SELECT display, card FROM bt_3_lex_units WHERE id = %s;", (unit_id,)
                )
                unit = cursor.fetchone()
                if not unit:
                    findings["unit_gone"].append((review_id, unit_id, old_text))
                    continue
                display, card = unit[0], unit[1]

                # 1. немецкий доехал?
                #
                # Совпадение с исходным текстом — ЕЩЁ НЕ ДЕФЕКТ. «Принять» можно и на
                # варианте, который в итоге не применился, потому что владелец решил
                # оставить фразу как есть, а верный немецкий уже стоял. Дефект — только
                # когда перевод при этом остался машинным; это проверяет пункт 3.
                if same(display, old_text):
                    findings["left_as_is"].append(
                        (review_id, unit_id, old_text))
                    continue

                # 2. в базе лежит то, что было на кнопке?
                match = next(
                    (v for v in variants if same(applied(v["text"]), display)), None)
                if match is None and status == "accepted":
                    # Кнопку сегодня не всегда можно восстановить, и это НЕ значит, что
                    # в базу легло не то. Две законные причины, разобранные 20.08.2026:
                    #   • правка была только в концевой пунктуации — такие варианты
                    #     `phrase_review_variants` теперь отсеивает как пустые придирки,
                    #     поэтому список кнопок сегодня пуст, а тогда кнопка была;
                    #   • владелец вписал СВОЙ текст, и он лучше всех кнопок
                    #     («Gegessen hat sie nichts…» против судейского «am Seki Sippen»).
                    # Дефектом это не является ни в том, ни в другом случае.
                    only_punctuation = _phrase_same_text_ru(display, old_text)
                    findings["punctuation_only" if only_punctuation else "own_text"].append(
                        (review_id, unit_id, old_text, display,
                         [v["text"] for v in variants]))

                # 3. русский владельца — главный?
                if status == "accepted":
                    button_ru = str((match or {}).get("ru") or "").strip()
                    if not button_ru:
                        findings["owner_ru_unknown"].append(
                            (review_id, unit_id, display, saved_ru))
                    else:
                        cursor.execute(
                            """SELECT u.display, l.rank, l.source
                                 FROM bt_3_lex_links l
                                 JOIN bt_3_lex_units u ON u.id = l.to_unit
                                WHERE l.from_unit = %s
                                ORDER BY l.rank LIMIT 1;""",
                            (unit_id,),
                        )
                        top = cursor.fetchone()
                        top_ru = str((top or ("",))[0] or "")
                        # Русский сравниваем БЕЗ регистра, в отличие от немецкого.
                        # В немецком регистр — грамматика («Es ist mir Latte» →
                        # «latte» и есть вся правка). В русском переводе заглавная
                        # в начале — оформление: ensure_unit переиспользует уже
                        # лежащую единицу «я умираю от жары», и это тот же перевод,
                        # а не подмена. Иначе отчёт насчитал 8 подмен там, где их 2.
                        if top_ru.strip().casefold() != button_ru.strip().casefold():
                            findings["owner_ru_missing"].append(
                                (review_id, unit_id, display, button_ru, top_ru,
                                 str((top or ("", 0, ""))[2] or "")))

                # 4. карточка про эту же фразу?
                #
                # Сравниваем ЗНАЧЕНИЯ, а не ищем подстроку. Подстрока давала ложь на
                # каждой правке пунктуации: «Er erlag der Versuchung» буквально входит
                # в «Er erlag der Versuchung.», и отчёт объявлял дефектом карточку,
                # которая на самом деле уже про новую фразу.
                if card and _holds_value(card, old_text):
                    findings["card_stale"].append((review_id, unit_id, old_text, display))

                # 5. хвосты старого написания в карточках людей
                cursor.execute(
                    """SELECT count(*) FROM bt_3_webapp_dictionary_queries
                        WHERE lex_unit_id = %s
                          AND (word_de = %s OR translation_de = %s);""",
                    (unit_id, old_text, old_text),
                )
                if (cursor.fetchone() or (0,))[0]:
                    findings["user_card_stale"].append((review_id, unit_id, old_text))

    print(f"\nРЕШЕНИЙ ВЛАДЕЛЬЦА РАЗОБРАНО: {total}\n")
    titles = {
        "left_as_is":         "Фраза осталась как была (владелец не менял немецкий)",
        "punctuation_only":   "Правка была только в концевой пунктуации",
        "own_text":           "Владелец вписал свой текст вместо вариантов судей",
        "owner_ru_missing":   "Русский владельца НЕ главный: сверху машинный",
        "owner_ru_unknown":   "На кнопке русского не было — перевод придумала модель",
        "card_stale":         "Карточка ещё про СТАРУЮ фразу",
        "user_card_stale":    "Старый текст остался в карточке человека",
        "unit_gone":          "Слова уже нет (удалено позже)",
    }
    # Три пункта — не дефекты, а СВЕДЕНИЯ: так владелец распорядился фразой. Кричать
    # о них красным значит приучить не читать отчёт. [[feedback_every_fact_needs_a_decision]]
    just_facts = {"left_as_is", "punctuation_only", "own_text", "owner_ru_unknown"}
    for key, title in titles.items():
        hits = findings[key]
        mark = ("ℹ️" if key in just_facts else "✅") if not hits or key in just_facts else "❌"
        if not hits:
            mark = "✅"
        print(f"{mark} {title}: {len(hits)}")
        for item in hits[: args.list]:
            print(f"      {item}")
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
