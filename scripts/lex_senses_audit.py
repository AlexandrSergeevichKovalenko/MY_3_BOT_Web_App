# -*- coding: utf-8 -*-
"""Вычитка слоя ЗНАЧЕНИЙ (bt_3_lex_senses) — этап 0 задачи «четыре языка».

ТОЛЬКО ЧТЕНИЕ. Ни одна строка не правится.

Зачем. Мы собираемся вешать на значения переводы на английский, испанский и итальянский.
Прежде чем платить за это модели, надо знать, что за строки там лежат: сколько из них —
один смысл, разложенный по синонимичным переводам; сколько пояснений повторяется; где
ошибки содержания. Иначе брак размножится втрое.

Запуск:
    railway run -s Postgres bash -c 'DATABASE_URL="$DATABASE_PUBLIC_URL" \
        python3 scripts/lex_senses_audit.py'
"""
from __future__ import annotations

import os
import re
import sys

import psycopg2

DEMOTED_RANK = 900   # backend/lex_units.py:46 — ниже этого связь человеку не показывается
MAX_LINKS = 6        # backend/lex_units.py:41 — больше шести переводов в карточку не влезает

CYRILLIC = re.compile(r"[А-Яа-яЁё]")
LATIN = re.compile(r"[A-Za-zÄÖÜäöüß]")
TECH_WORDS = ("full_sentence", "primary", "secondary", "translations", "meanings",
              "null", "undefined", "value", "context")


def section(title: str) -> None:
    print("\n" + "=" * 78)
    print(title)
    print("=" * 78)


def rows(cur, sql, args=None, limit_print: int | None = None) -> list:
    cur.execute(sql, args or ())
    data = cur.fetchall()
    for i, row in enumerate(data):
        if limit_print is not None and i >= limit_print:
            print(f"    … ещё {len(data) - limit_print}")
            break
        print("   ", row)
    return data


def main() -> int:
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        print("нет DATABASE_URL"); return 2
    conn = psycopg2.connect(dsn)
    conn.set_session(readonly=True)
    cur = conn.cursor()

    section("1. СКОЛЬКО ИХ ВООБЩЕ")
    rows(cur, """
        SELECT u.lang, COUNT(DISTINCT s.unit_id) AS единиц, COUNT(*) AS значений
        FROM bt_3_lex_senses s JOIN bt_3_lex_units u ON u.id = s.unit_id
        GROUP BY u.lang ORDER BY 3 DESC;""")

    section("2. ДРОБЛЕНИЕ: сколько значений у одной единицы")
    rows(cur, """
        SELECT значений_у_единицы, COUNT(*) AS единиц
        FROM (SELECT unit_id, COUNT(*) AS значений_у_единицы
              FROM bt_3_lex_senses GROUP BY unit_id) t
        GROUP BY 1 ORDER BY 1;""")

    section("3. ДРОБЛЕНИЕ: значений в базе против смыслов в разборе карточки")
    print("   Код строит значение НА КАЖДЫЙ ПЕРЕВОД (sync_unit_links_from_card).")
    print("   Смыслов в карточке = meanings.primary + meanings.secondary.")
    rows(cur, """
        WITH x AS (
          SELECT u.id,
                 (CASE WHEN u.card->'meanings'->'primary' IS NOT NULL THEN 1 ELSE 0 END)
                 + COALESCE(jsonb_array_length(u.card->'meanings'->'secondary'), 0) AS смыслов,
                 (SELECT COUNT(*) FROM bt_3_lex_senses s WHERE s.unit_id = u.id) AS значений
          FROM bt_3_lex_units u
          WHERE u.lang='de' AND u.kind='word' AND u.card IS NOT NULL)
        SELECT смыслов, значений, COUNT(*) AS слов FROM x
        WHERE значений > 0 GROUP BY 1,2 ORDER BY 3 DESC;""", limit_print=25)

    section("4. СХЛОПЫВАНИЕ: сколько строк уйдёт, если считать одинаковое пояснение одним смыслом")
    rows(cur, """
        SELECT COUNT(*) AS значений_с_пояснением,
               COUNT(DISTINCT (unit_id::text || '|' || note)) AS останется_после_схлопывания
        FROM bt_3_lex_senses WHERE COALESCE(note,'') <> '';""")
    print("   Значения БЕЗ пояснения схлопывать нечем — их считаем отдельно:")
    rows(cur, "SELECT COUNT(*) FROM bt_3_lex_senses WHERE COALESCE(note,'') = '';")

    section("5. ВИДИМОСТЬ: что из этого вообще доходит до человека")
    print("   Карточка показывает СВЯЗИ (переводы), а не значения: _build_item собирает")
    print("   dictionary_senses из списка связей. Таблица значений человеку не видна —")
    print("   она только упорядочивает связи (ORDER BY sense_id IS NULL).")
    rows(cur, """
        SELECT COUNT(*) AS связей_всего,
               COUNT(*) FILTER (WHERE l.rank < %s) AS не_задвинутых,
               COUNT(*) FILTER (WHERE l.sense_id IS NOT NULL) AS с_привязкой_к_значению
        FROM bt_3_lex_links l;""", (DEMOTED_RANK,))

    section("6. ОШИБКА СОДЕРЖАНИЯ: техническое слово утекло в пояснение")
    like = " OR ".join(["note ILIKE %s"] * len(TECH_WORDS))
    rows(cur, f"""
        SELECT u.lemma, s.sense_no, s.note, s.source
        FROM bt_3_lex_senses s JOIN bt_3_lex_units u ON u.id = s.unit_id
        WHERE {like} ORDER BY u.lemma;""",
        tuple(f"%{w}%" for w in TECH_WORDS), limit_print=20)

    section("7. ОШИБКА СОДЕРЖАНИЯ: пояснение не на русском (немецкое определение вместо смысла)")
    rows(cur, """
        SELECT COUNT(*) FROM bt_3_lex_senses
        WHERE COALESCE(note,'') <> ''
          AND note !~ '[А-Яа-яЁё]' AND note ~ '[A-Za-zÄÖÜäöüß]';""")
    rows(cur, """
        SELECT u.lemma, s.sense_no, left(s.note, 60)
        FROM bt_3_lex_senses s JOIN bt_3_lex_units u ON u.id = s.unit_id
        WHERE COALESCE(s.note,'') <> ''
          AND s.note !~ '[А-Яа-яЁё]' AND s.note ~ '[A-Za-zÄÖÜäöüß]'
        LIMIT 12;""")

    section("8. ОШИБКА СВЯЗЕЙ: перевод ведёт в единицу ТОГО ЖЕ языка")
    rows(cur, """
        SELECT f.lang AS откуда, t.lang AS куда, COUNT(*) FROM bt_3_lex_links l
        JOIN bt_3_lex_units f ON f.id = l.from_unit
        JOIN bt_3_lex_units t ON t.id = l.to_unit
        GROUP BY 1,2 ORDER BY 3 DESC;""", limit_print=12)

    section("9. ОМОГРАФЫ: все семь, с их значениями и переводами")
    rows(cur, """
        SELECT u.id, u.lemma, u.gender, s.sense_no, left(COALESCE(s.note,'∅'), 45) AS пояснение,
               (SELECT string_agg(t.display, ' | ' ORDER BY l.rank)
                FROM bt_3_lex_links l JOIN bt_3_lex_units t ON t.id = l.to_unit
                WHERE l.sense_id = s.id AND t.lang <> u.lang) AS перевод,
               (SELECT min(l.rank) FROM bt_3_lex_links l WHERE l.sense_id = s.id) AS ранг
        FROM bt_3_lex_units u LEFT JOIN bt_3_lex_senses s ON s.unit_id = u.id
        WHERE u.lang='de' AND u.lemma_key IN (
          SELECT lemma_key FROM bt_3_lex_units WHERE lang='de'
          GROUP BY lemma_key HAVING COUNT(*) > 1)
        ORDER BY u.lemma, u.id, s.sense_no;""", limit_print=40)

    section("10. ПОДОЗРЕНИЕ НА ВЫДУМКУ: перевод значения не встречается в разборе карточки")
    print("   Разбор — источник правды по переводам. Если у значения перевод, которого в")
    print("   разборе нет, он взялся из старого банка или придуман переносом.")
    rows(cur, """
        WITH перевод_значения AS (
          SELECT s.id AS sense_id, u.id AS unit_id, u.lemma, u.card, t.display AS перевод
          FROM bt_3_lex_senses s
          JOIN bt_3_lex_units u ON u.id = s.unit_id AND u.lang='de' AND u.card IS NOT NULL
          JOIN bt_3_lex_links l ON l.sense_id = s.id
          JOIN bt_3_lex_units t ON t.id = l.to_unit AND t.lang='ru')
        SELECT COUNT(*) AS не_подтверждённых_разбором, COUNT(DISTINCT unit_id) AS слов
        FROM перевод_значения p
        WHERE NOT EXISTS (
          SELECT 1 FROM jsonb_array_elements(COALESCE(p.card->'translations','[]'::jsonb)) x
          WHERE position(lower(p.перевод) in lower(x->>'value')) > 0)
        AND NOT EXISTS (
          SELECT 1 FROM jsonb_array_elements(
            COALESCE(p.card->'meanings'->'secondary','[]'::jsonb)
            || COALESCE(jsonb_build_array(p.card->'meanings'->'primary'),'[]'::jsonb)) y
          WHERE position(lower(p.перевод) in lower(y->>'value')) > 0);""")
    print("   Примеры таких значений:")
    rows(cur, """
        WITH перевод_значения AS (
          SELECT s.id AS sense_id, u.id AS unit_id, u.lemma, u.gender, u.card, t.display AS перевод
          FROM bt_3_lex_senses s
          JOIN bt_3_lex_units u ON u.id = s.unit_id AND u.lang='de' AND u.card IS NOT NULL
          JOIN bt_3_lex_links l ON l.sense_id = s.id
          JOIN bt_3_lex_units t ON t.id = l.to_unit AND t.lang='ru')
        SELECT lemma, gender, перевод FROM перевод_значения p
        WHERE NOT EXISTS (
          SELECT 1 FROM jsonb_array_elements(COALESCE(p.card->'translations','[]'::jsonb)) x
          WHERE position(lower(p.перевод) in lower(x->>'value')) > 0)
        AND NOT EXISTS (
          SELECT 1 FROM jsonb_array_elements(
            COALESCE(p.card->'meanings'->'secondary','[]'::jsonb)
            || COALESCE(jsonb_build_array(p.card->'meanings'->'primary'),'[]'::jsonb)) y
          WHERE position(lower(p.перевод) in lower(y->>'value')) > 0)
        LIMIT 15;""")

    section("10б. ДУБЛИКАТЫ ЕДИНИЦ: существительное без рода заводит вторую строку")
    print("   Beschaffung и Kenntnisnahme попали в «омографы» именно так: одна строка")
    print("   с родом, вторая без. Ключ единицы включает род, поэтому пустой род = новая единица.")
    rows(cur, """
        SELECT COUNT(*) FILTER (WHERE gender IS NULL OR gender = '') AS без_рода,
               COUNT(*) FILTER (WHERE gender IS NOT NULL AND gender <> '') AS с_родом
        FROM bt_3_lex_units WHERE lang='de' AND pos='noun';""")
    rows(cur, """
        SELECT COUNT(*) FROM (
          SELECT lemma_key FROM bt_3_lex_units WHERE lang='de' AND pos='noun'
          GROUP BY lemma_key
          HAVING COUNT(*) FILTER (WHERE gender IS NULL OR gender='') > 0
             AND COUNT(*) FILTER (WHERE gender IS NOT NULL AND gender<>'') > 0) t;""")

    section("11. МАСШТАБ МОСТА: сколько значений надо перевести на три языка")
    rows(cur, """
        SELECT COUNT(*) AS значений_у_немецких_единиц,
               COUNT(DISTINCT s.unit_id) AS немецких_единиц
        FROM bt_3_lex_senses s JOIN bt_3_lex_units u ON u.id = s.unit_id
        WHERE u.lang = 'de';""")
    rows(cur, """
        SELECT COUNT(DISTINCT (s.unit_id::text || '|' || COALESCE(s.note,'∅'))) AS после_схлопывания
        FROM bt_3_lex_senses s JOIN bt_3_lex_units u ON u.id = s.unit_id
        WHERE u.lang = 'de';""")

    cur.close(); conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
