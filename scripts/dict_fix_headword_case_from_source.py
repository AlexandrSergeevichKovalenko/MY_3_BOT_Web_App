# -*- coding: utf-8 -*-
"""Регистр заголовка в словаре: глагол, прилагательное и наречие пишутся со строчной.

Повод и границы
───────────────
Владелец 13.09.2026: «fix it» — после того, как в анаграмму попало `Behaupten` вместо
`behaupten`. Замер того дня по журналу запросов: **233 слова** (252 записи) лежат одним
словом с заглавной буквы, хотя источник называет их глаголом, прилагательным или
наречием.

ЧЕСТНАЯ ГРАНИЦА, названная владельцу до правки: через общую формулу показа
(`compose_german_headword` → `german_headword_case`) на экран из этих 233 не доходит
с заглавной НИ ОДНО — продукт опускает регистр при отрисовке. Это проверено 25.08.2026
и перепроверено 13.09.2026 прогоном всех 233 через саму функцию показа. Значит это
уборка накопленного (шаг 5 замкнутого цикла), а не тушение пожара: опасность в том, что
следующий потребитель возьмёт поле СЫРЫМ — ровно так сломалась анаграмма.

Чем эта правка НЕ является
──────────────────────────
Здесь ничего не решается «на глаз». Регистр берётся из части речи, записанной в самом
разборе, а не из вида слова. Если записи об одном слове спорят (где-то noun или стоит
артикль) — слово не трогается: `das Aufstoßen` (отрыжка) законно пишется с заглавной.

Правка идёт ЧЕРЕЗ ДВЕРИ ПРОДУКТА, а не своим UPDATE:
  • `lex_units.retitle_unit` — написание, лемма, ключ поиска и вид записи одним местом;
  • `database.spread_correction_everywhere` — развоз правки по карточкам людей, пулу,
    разбору и (с 13.09.2026) по банкам игр.
Свой запрос тут поправил бы одну строку из восьми, и назавтра старое написание всплыло
бы из карточки человека.

    python3 scripts/dict_fix_headword_case_from_source.py            # показать, что будет
    python3 scripts/dict_fix_headword_case_from_source.py --apply    # применить
"""
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")

from backend.database import (                                        # noqa: E402
    compose_german_headword, get_db_connection_context, list_headword_case_offenders,
    spread_correction_everywhere,
)
from backend.german_grammar_tables import german_headword_case        # noqa: E402
from backend.lex_units import retitle_unit                            # noqa: E402

# Часть речи, при которой немецкое слово пишется со строчной. Список повторяет
# _LOWERCASE_POS правила показа (`german_grammar_tables.german_headword_case`) и нужен
# здесь только для последней строки отчёта — самому отбору он не судья.
СТРОЧНЫЕ = ("verb", "adjective", "adverb")


def кандидаты() -> list[tuple[str, str]]:
    """[(слово_как_лежит, часть_речи)]. Правило отбора живёт в продукте
    (`database.list_headword_case_offenders`) — здесь его копии нет, иначе уборка и
    утренняя проверка обещания разойдутся, как уже разошлись 13.09.2026."""
    return list_headword_case_offenders()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--show", type=int, default=15, help="сколько слов показать")
    args = ap.parse_args()

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            слова = кандидаты()
            план = []
            for слово, pos in слова:
                строчное = german_headword_case(слово, pos)
                if строчное == слово:
                    continue  # правило показа считает это написание верным — не трогаем
                план.append((слово, строчное, pos))

            print(f"слов в классе: {len(слова)}, к правке: {len(план)}")
            на_экране = sum(1 for с, _н, p in план
                            if compose_german_headword(с, pos=p)[:1].isupper())
            print(f"из них доходит до экрана С ЗАГЛАВНОЙ через формулу показа: {на_экране}")
            for слово, строчное, pos in план[:args.show]:
                print(f"   {слово:<24} → {строчное:<24} {pos}")

            if not args.apply:
                print("\nэто сухой прогон; чтобы применить — ключ --apply")
                return

            статей, развезено = 0, 0
            for слово, строчное, _pos in план:
                cur.execute("SELECT id, display FROM bt_3_lex_units "
                            "WHERE lang='de' AND LOWER(display)=LOWER(%s)", (слово,))
                строки = cur.fetchall() or []
                for unit_id, display in строки:
                    if str(display) != строчное:
                        retitle_unit(cur, int(unit_id), строчное)
                        статей += 1
                    отчёт = spread_correction_everywhere(
                        cur, unit_id=int(unit_id), old_text=слово, new_text=строчное)
                    развезено += int(отчёт.get("cards", 0)) + int(отчёт.get("pool", 0))
                # Записи журнала, не привязанные к статье: развоз их не видит, потому что
                # ищет по lex_unit_id. Правим те же поля, что правит дверь записи.
                cur.execute("""
                    UPDATE bt_3_webapp_dictionary_queries q
                       SET word_de = CASE WHEN LOWER(TRIM(COALESCE(q.word_de,''))) = LOWER(%s)
                                          THEN %s ELSE q.word_de END,
                           translation_de = CASE WHEN LOWER(TRIM(COALESCE(q.translation_de,''))) = LOWER(%s)
                                          THEN %s ELSE q.translation_de END,
                           response_json = q.response_json
                               || CASE WHEN q.response_json->>'word_de' = %s
                                       THEN jsonb_build_object('word_de', %s) ELSE '{}'::jsonb END
                               || CASE WHEN q.response_json->>'translation_de' = %s
                                       THEN jsonb_build_object('translation_de', %s) ELSE '{}'::jsonb END,
                           updated_at = NOW()
                     WHERE REGEXP_REPLACE(COALESCE(q.response_json->>'word_de',''),
                                          '^(der|die|das)\\s+','') = %s
                """, (слово, строчное, слово, строчное, слово, строчное,
                      слово, строчное, слово))

            print(f"\nстатей переименовано: {статей}; карточек и записей пула развезено: {развезено}")

    print("\nЭКРАН ПОСЛЕ:")
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            осталось = кандидаты()
            осталось = [(с, p) for с, p in осталось if german_headword_case(с, p) != с]
            print(f"   слов с неверным регистром в журнале: {len(осталось)}")
            cur.execute("""SELECT COUNT(*) FROM bt_3_lex_units
                           WHERE lang='de' AND pos = ANY(%s) AND display ~ '^[A-ZÄÖÜ][a-zäöüß]+$'""",
                        (list(СТРОЧНЫЕ),))
            print(f"   статей словаря с неверным регистром: {cur.fetchone()[0]}")


if __name__ == "__main__":
    main()
