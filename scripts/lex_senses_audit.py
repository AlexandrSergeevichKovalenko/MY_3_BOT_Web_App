# -*- coding: utf-8 -*-
"""Сплошной разбор слоя единиц/значений/связей — этап 0 задачи «четыре языка».

ТОЛЬКО ЧТЕНИЕ. Ни одна строка не правится.

Правило этого разбора: ни одно утверждение не делается по одному счётчику. На каждый
класс — счёт, реальные строки и встречная проверка «а нет ли безобидного объяснения».
Знаменатель называется явно: «доля от немецких СЛОВ» и «доля от всех немецких единиц» —
разные числа, и путать их нельзя.

Источники записей (по коду):
  пул         — первичная сборка из старого банка (scripts/dict_units_build.py)
  разбор      — из карточки-разбора (backend/lex_units.py, sync_unit_links_from_card)
  разрез      — разрезание склеенных переводов (scripts/dict_units_split_senses.py)
  свалка      — исходная склейка, понижена в ранге тем же скриптом
  разведение  — разведение омографов по родам (scripts/dict_units_disambiguate.py)

Запуск:
    railway run -s Postgres bash -c 'DATABASE_URL="$DATABASE_PUBLIC_URL" \
        python3 scripts/lex_senses_audit.py'
"""
from __future__ import annotations

import os
import sys

import psycopg2

DEMOTED_RANK = 900   # backend/lex_units.py:46 — ниже этого связь человеку не показывается
MAX_LINKS = 6        # backend/lex_units.py:41 — больше шести переводов в карточку не влезает
HOMOGRAPHS = ("beschaffung", "gehalt", "kenntnisnahme", "kiefer",
              "kunde", "schild", "verdienst")


def head(n: str, title: str) -> None:
    print(f"\n{'=' * 78}\n{n}. {title}\n{'=' * 78}")


def sub(text: str) -> None:
    print(f"\n-- {text}")


def q(cur, sql, args=None, cap: int | None = None):
    cur.execute(sql, args or ())
    data = cur.fetchall()
    for i, row in enumerate(data):
        if cap is not None and i >= cap:
            print(f"    … ещё {len(data) - cap} строк")
            break
        print("   ", row)
    if not data:
        print("    (пусто)")
    return data


def main() -> int:
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        print("нет DATABASE_URL"); return 2
    conn = psycopg2.connect(dsn)
    conn.set_session(readonly=True)
    cur = conn.cursor()

    # ---------------------------------------------------------------- А. ИНВЕНТАРЬ
    head("А", "ИНВЕНТАРЬ: что вообще лежит в слое")

    sub("А1. единицы: язык × вид × есть ли разбор")
    q(cur, """
        SELECT lang, kind,
               COUNT(*) AS всего,
               COUNT(*) FILTER (WHERE card IS NOT NULL) AS с_разбором
        FROM bt_3_lex_units GROUP BY 1,2 ORDER BY 1, 3 DESC;""")

    sub("А2. значения и связи по источнику записи")
    q(cur, "SELECT source, COUNT(*) FROM bt_3_lex_senses GROUP BY 1 ORDER BY 2 DESC;")
    q(cur, "SELECT source, COUNT(*) FROM bt_3_lex_links GROUP BY 1 ORDER BY 2 DESC;")

    sub("А3. справочники решений (страж омографов) — заполнены ли")
    for table in ("bt_3_lex_form_rulings", "bt_3_lex_gloss_rulings"):
        cur.execute("SELECT to_regclass(%s);", (table,))
        if cur.fetchone()[0] is None:
            print(f"    {table}: таблицы нет")
            continue
        cur.execute(f"SELECT COUNT(*) FROM {table};")
        print(f"    {table}: строк {cur.fetchone()[0]}")
        cur.execute(f"""SELECT column_name FROM information_schema.columns
                        WHERE table_name = '{table}' ORDER BY ordinal_position;""")
        print("      колонки:", [r[0] for r in cur.fetchall()])

    # ------------------------------------------------------------- Б. ПОКРЫТИЕ
    head("Б", "ПОКРЫТИЕ: от какого знаменателя считать мост")
    print("   ВАЖНО: «единиц» и «слов» — разные числа. Предложения и сочетания значений")
    print("   почти не имеют по устройству, и требовать их от них бессмысленно.")

    sub("Б1. немецкие единицы по видам: сколько со значениями, сколько с переводом, сколько с разбором")
    q(cur, """
        SELECT u.kind,
               COUNT(*) AS единиц,
               COUNT(*) FILTER (WHERE EXISTS (SELECT 1 FROM bt_3_lex_senses s WHERE s.unit_id=u.id)) AS со_значениями,
               COUNT(*) FILTER (WHERE EXISTS (SELECT 1 FROM bt_3_lex_links l
                     JOIN bt_3_lex_units t ON t.id=l.to_unit
                     WHERE l.from_unit=u.id AND t.lang='ru')) AS с_русским_переводом,
               COUNT(*) FILTER (WHERE u.card IS NOT NULL) AS с_разбором
        FROM bt_3_lex_units u WHERE u.lang='de' GROUP BY 1 ORDER BY 2 DESC;""")

    sub("Б2. те же немецкие СЛОВА в долях (главный знаменатель для моста)")
    q(cur, """
        SELECT COUNT(*) AS слов,
               COUNT(*) FILTER (WHERE EXISTS (SELECT 1 FROM bt_3_lex_senses s WHERE s.unit_id=u.id)) AS со_значениями,
               ROUND(100.0 * COUNT(*) FILTER (WHERE EXISTS (SELECT 1 FROM bt_3_lex_senses s WHERE s.unit_id=u.id))
                     / NULLIF(COUNT(*),0), 1) AS процент
        FROM bt_3_lex_units u WHERE u.lang='de' AND u.kind='word';""")

    sub("Б3. единицы, до которых люди реально дотянулись (есть строка происхождения)")
    q(cur, """
        SELECT u.kind, COUNT(DISTINCT u.id) FROM bt_3_lex_units u
        JOIN bt_3_lex_unit_sources src ON src.unit_id = u.id
        WHERE u.lang='de' GROUP BY 1 ORDER BY 2 DESC;""")

    # ------------------------------------------------------- В. ЧТО ВИДИТ ЧЕЛОВЕК
    head("В", "ЧТО ВИДИТ ЧЕЛОВЕК")
    print("   Карточка строится из СВЯЗЕЙ: _build_item (lex_units.py:285) собирает")
    print("   dictionary_senses из списка переводов. Таблица значений наружу не отдаётся —")
    print("   она только задаёт порядок (ORDER BY l.sense_id IS NULL, l.rank).")

    sub("В1. связи: всего / проходят порог показа / привязаны к значению")
    q(cur, """
        SELECT COUNT(*) AS всего,
               COUNT(*) FILTER (WHERE rank < %s) AS проходят_порог,
               COUNT(*) FILTER (WHERE sense_id IS NOT NULL) AS с_привязкой
        FROM bt_3_lex_links;""", (DEMOTED_RANK,))

    sub("В2. немецкие слова, у которых человеку показать НЕЧЕГО (нет ни одного видимого перевода)")
    q(cur, """
        SELECT COUNT(*) FROM bt_3_lex_units u
        WHERE u.lang='de' AND u.kind='word'
          AND NOT EXISTS (SELECT 1 FROM bt_3_lex_links l JOIN bt_3_lex_units t ON t.id=l.to_unit
                          WHERE l.from_unit=u.id AND t.lang='ru' AND l.rank < %s
                            AND position('___' in t.display) = 0);""", (DEMOTED_RANK,))

    # --------------------------------------------------------- Г. ВХОД ДЛЯ МОСТА
    head("Г", "ВХОД ДЛЯ МОСТА: что мы реально отдадим модели")
    print("   Мост получает: немецкое слово + часть речи + пояснение значения + русский")
    print("   перевод этого значения. Печатаю РЕАЛЬНЫЕ строки, а не только счётчики.")

    sub("Г1. значения: сколько имеют годный вход (есть перевод) и сколько с пояснением")
    q(cur, """
        SELECT
          COUNT(*) AS значений,
          COUNT(*) FILTER (WHERE EXISTS (SELECT 1 FROM bt_3_lex_links l WHERE l.sense_id=s.id)) AS есть_перевод,
          COUNT(*) FILTER (WHERE COALESCE(s.note,'') <> '' AND s.note <> 'full_sentence') AS есть_пояснение,
          COUNT(*) FILTER (WHERE EXISTS (SELECT 1 FROM bt_3_lex_links l WHERE l.sense_id=s.id)
                             AND COALESCE(s.note,'') <> '' AND s.note <> 'full_sentence') AS есть_и_то_и_другое
        FROM bt_3_lex_senses s JOIN bt_3_lex_units u ON u.id=s.unit_id WHERE u.lang='de';""")

    sub("Г2. пятнадцать НАСТОЯЩИХ входов моста (как их увидит модель)")
    q(cur, """
        SELECT u.display AS слово, COALESCE(u.pos,'∅') AS часть_речи,
               left(COALESCE(s.note,'∅'), 55) AS пояснение,
               (SELECT string_agg(t.display, ', ' ORDER BY l.rank)
                FROM bt_3_lex_links l JOIN bt_3_lex_units t ON t.id=l.to_unit
                WHERE l.sense_id=s.id AND t.lang='ru') AS русский
        FROM bt_3_lex_senses s JOIN bt_3_lex_units u ON u.id=s.unit_id
        WHERE u.lang='de' AND u.kind='word' ORDER BY u.id LIMIT 15;""")

    sub("Г3. слова БЕЗ значений: есть ли у них чем кормить мост")
    q(cur, """
        SELECT u.display, COALESCE(u.pos,'∅'),
               (SELECT string_agg(t.display, ', ' ORDER BY l.rank)
                FROM bt_3_lex_links l JOIN bt_3_lex_units t ON t.id=l.to_unit
                WHERE l.from_unit=u.id AND t.lang='ru' AND l.rank < %s) AS русский
        FROM bt_3_lex_units u
        WHERE u.lang='de' AND u.kind='word'
          AND NOT EXISTS (SELECT 1 FROM bt_3_lex_senses s WHERE s.unit_id=u.id)
        LIMIT 12;""", (DEMOTED_RANK,))

    # ------------------------------------------------------------- Д. ДЕФЕКТЫ
    head("Д", "ДЕФЕКТЫ: счёт, строки, причина, встречная проверка")

    sub("Д1. пояснение = 'full_sentence'. Счёт, и есть ли у них перевод")
    q(cur, """
        SELECT COUNT(*) AS всего,
               COUNT(*) FILTER (WHERE EXISTS (SELECT 1 FROM bt_3_lex_links l WHERE l.sense_id=s.id)) AS с_переводом
        FROM bt_3_lex_senses s WHERE s.note = 'full_sentence';""")
    print("    встречная проверка: это вид единицы или сбой? смотрим kind этих единиц")
    q(cur, """
        SELECT u.kind, COUNT(*) FROM bt_3_lex_senses s JOIN bt_3_lex_units u ON u.id=s.unit_id
        WHERE s.note='full_sentence' GROUP BY 1 ORDER BY 2 DESC;""")
    print("    встречная проверка: лежит ли строка 'full_sentence' в самом разборе карточки")
    q(cur, """
        SELECT COUNT(*) FROM bt_3_lex_senses s JOIN bt_3_lex_units u ON u.id=s.unit_id
        WHERE s.note='full_sentence' AND position('full_sentence' in COALESCE(u.card::text,'')) > 0;""")
    print("    строки:")
    q(cur, """
        SELECT u.display, u.kind, s.sense_no,
               (SELECT string_agg(t.display, ', ') FROM bt_3_lex_links l JOIN bt_3_lex_units t ON t.id=l.to_unit
                WHERE l.sense_id=s.id AND t.lang='ru')
        FROM bt_3_lex_senses s JOIN bt_3_lex_units u ON u.id=s.unit_id
        WHERE s.note='full_sentence' ORDER BY u.display LIMIT 10;""")

    sub("Д2. значения без единой связи-перевода. Счёт и встречная проверка")
    q(cur, """
        SELECT COUNT(*) FROM bt_3_lex_senses s
        WHERE NOT EXISTS (SELECT 1 FROM bt_3_lex_links l WHERE l.sense_id=s.id);""")
    print("    встречная проверка: а у самой единицы переводы есть? (тогда потери нет)")
    q(cur, """
        SELECT COUNT(*) FILTER (WHERE EXISTS (SELECT 1 FROM bt_3_lex_links l2
                                              WHERE l2.from_unit=s.unit_id)) AS единица_с_переводами,
               COUNT(*) FILTER (WHERE NOT EXISTS (SELECT 1 FROM bt_3_lex_links l2
                                              WHERE l2.from_unit=s.unit_id)) AS единица_вообще_пустая
        FROM bt_3_lex_senses s
        WHERE NOT EXISTS (SELECT 1 FROM bt_3_lex_links l WHERE l.sense_id=s.id);""")

    sub("Д3. ВСЕ омографы целиком: единицы, значения, переводы, ранги, источники")
    q(cur, """
        SELECT u.id, u.display, COALESCE(u.gender,'∅') AS род, COALESCE(u.pos,'∅') AS часть_речи,
               u.card_source, s.sense_no, left(COALESCE(s.note,'∅'), 42) AS пояснение,
               (SELECT string_agg(t.display || '[' || l.rank || '/' || l.source || ']', ' ')
                FROM bt_3_lex_links l JOIN bt_3_lex_units t ON t.id=l.to_unit
                WHERE l.sense_id = s.id AND t.lang='ru') AS перевод
        FROM bt_3_lex_units u LEFT JOIN bt_3_lex_senses s ON s.unit_id=u.id
        WHERE u.lang='de' AND u.lemma_key = ANY(%s)
        ORDER BY u.lemma_key, u.id, s.sense_no;""", (list(HOMOGRAPHS),), cap=45)

    sub("Д4. что говорит справочник разведения про эти же слова")
    cur.execute("SELECT to_regclass('bt_3_lex_form_rulings');")
    if cur.fetchone()[0] is not None:
        q(cur, "SELECT * FROM bt_3_lex_form_rulings LIMIT 20;", cap=20)
    else:
        print("    таблицы bt_3_lex_form_rulings нет — страж не оставил решений")

    sub("Д5. переводы омографов ВНЕ значений (привязка sense_id пустая) — их человек тоже видит")
    q(cur, """
        SELECT u.display, COALESCE(u.gender,'∅'), t.display, l.rank, l.source
        FROM bt_3_lex_links l
        JOIN bt_3_lex_units u ON u.id=l.from_unit
        JOIN bt_3_lex_units t ON t.id=l.to_unit
        WHERE u.lang='de' AND t.lang='ru' AND l.sense_id IS NULL
          AND u.lemma_key = ANY(%s) AND l.rank < %s
        ORDER BY u.display, l.rank;""", (list(HOMOGRAPHS), DEMOTED_RANK), cap=25)

    sub("Д6. дубликат единицы: одно написание, одна строка без части речи")
    q(cur, """
        SELECT u.lemma_key,
               string_agg(u.id::text || ':' || COALESCE(u.pos,'∅') || '/' || COALESCE(u.gender,'∅')
                          || '/' || u.kind || '/' || COALESCE(u.card_source,'∅'), '   ') AS строки
        FROM bt_3_lex_units u WHERE u.lang='de' AND u.lemma_key IN (
          SELECT lemma_key FROM bt_3_lex_units WHERE lang='de' GROUP BY lemma_key
          HAVING COUNT(*) FILTER (WHERE pos IS NULL OR pos='') > 0
             AND COUNT(*) FILTER (WHERE pos IS NOT NULL AND pos <> '') > 0)
        GROUP BY 1;""", cap=15)

    sub("Д7. схлопывание по одинаковому пояснению — сколько строк уйдёт на самом деле")
    q(cur, """
        SELECT COUNT(*) AS с_пояснением,
               COUNT(DISTINCT (unit_id::text || '|' || note)) AS различных,
               COUNT(*) - COUNT(DISTINCT (unit_id::text || '|' || note)) AS схлопнется
        FROM bt_3_lex_senses WHERE COALESCE(note,'') <> '' AND note <> 'full_sentence';""")

    sub("Д8. значений против смыслов разбора (совпадает ли дробление)")
    q(cur, """
        WITH x AS (
          SELECT u.id,
                 (CASE WHEN u.card->'meanings'->'primary' IS NOT NULL THEN 1 ELSE 0 END)
                 + COALESCE(jsonb_array_length(u.card->'meanings'->'secondary'), 0) AS смыслов,
                 (SELECT COUNT(*) FROM bt_3_lex_senses s WHERE s.unit_id=u.id) AS значений
          FROM bt_3_lex_units u WHERE u.lang='de' AND u.kind='word' AND u.card IS NOT NULL)
        SELECT CASE WHEN значений = смыслов THEN 'совпадает'
                    WHEN значений > смыслов THEN 'значений больше'
                    ELSE 'значений меньше' END AS итог,
               COUNT(*) AS слов
        FROM x WHERE значений > 0 GROUP BY 1 ORDER BY 2 DESC;""")

    # ------------------------------------- Ж. СКЛЕЙКА ДВУХ ЯЗЫКОВ В ОДНОМ ПОЛЕ
    head("Ж", "СКЛЕЙКА: два языка в одном поле (найдено владельцем на живой карточке)")
    print("   Симптом: на лицевой стороне карточки повторения стоит")
    print("   «Der Verlag behält sich das Recht vor. — Издательство оставляет за собой право.»")
    print("   — ответ виден до нажатия «Show Answer». Значит в поле ОДНОГО языка лежат оба.")
    print("   Проверка механическая и однозначная: латиница И кириллица в одной строке.")

    LAT = "[A-Za-zÄÖÜäöüß]"
    CYR = "[А-Яа-яЁё]"
    # Русская сторона карточки — word_ru / translation_ru. Немецкая — word_de / translation_de.
    # Дефект: в РУССКОМ поле есть латиница И кириллица, то есть туда влезла немецкая фраза.
    RU_GLUED = f"(word_ru ~ '{LAT}' AND word_ru ~ '{CYR}')"
    DE_GLUED = f"(word_de ~ '{LAT}' AND word_de ~ '{CYR}')"

    sub("Ж1. личные карточки: в каком поле лежат оба алфавита")
    q(cur, f"""
        SELECT COUNT(*) AS карточек_всего,
               COUNT(*) FILTER (WHERE {RU_GLUED}) AS русское_поле_склеено,
               COUNT(*) FILTER (WHERE {DE_GLUED}) AS немецкое_поле_склеено,
               COUNT(*) FILTER (WHERE translation_ru ~ '{LAT}' AND translation_ru ~ '{CYR}') AS русский_перевод_склеен
        FROM bt_3_webapp_dictionary_queries;""")

    sub("Ж2. встречная проверка: сколько из них — просто немецкое слово в скобках/кавычках")
    print("    (это НЕ дефект: «право (das Recht)» — законная подсказка, а не склейка)")
    q(cur, f"""
        SELECT COUNT(*) FILTER (WHERE word_ru ~ '\\(' OR word_ru ~ '«') AS в_скобках_или_кавычках,
               COUNT(*) FILTER (WHERE word_ru !~ '\\(' AND word_ru !~ '«') AS без_скобок
        FROM bt_3_webapp_dictionary_queries WHERE {RU_GLUED};""")

    sub("Ж3. дефект в чистом виде: русское поле = «немецкое — русское» через разделитель")
    q(cur, f"""
        SELECT COUNT(*) FROM bt_3_webapp_dictionary_queries
        WHERE {RU_GLUED}
          AND (position(' — ' in word_ru) > 0 OR position(' – ' in word_ru) > 0
               OR position(' - ' in word_ru) > 0);""")

    sub("Ж4. строки: как это выглядит на самом деле")
    q(cur, f"""
        SELECT user_id, id, left(word_ru, 72) AS русское_поле, left(word_de, 34) AS немецкое_поле,
               COALESCE(source_lang,'∅') || '→' || COALESCE(target_lang,'∅') AS пара,
               COALESCE(origin_process,'∅') AS дверь
        FROM bt_3_webapp_dictionary_queries
        WHERE {RU_GLUED} ORDER BY id DESC LIMIT 15;""")

    sub("Ж5. откуда они пришли — по двери сохранения")
    q(cur, f"""
        SELECT COALESCE(origin_process,'∅') AS дверь, COUNT(*) FROM bt_3_webapp_dictionary_queries
        WHERE {RU_GLUED} GROUP BY 1 ORDER BY 2 DESC;""", cap=15)

    sub("Ж6. когда их сохранили")
    q(cur, f"""
        SELECT date_trunc('month', created_at)::date AS месяц, COUNT(*)
        FROM bt_3_webapp_dictionary_queries WHERE {RU_GLUED}
        GROUP BY 1 ORDER BY 1;""", cap=24)

    sub("Ж7. сколько из них уже в повторении (то есть человек их видит)")
    q(cur, f"""
        SELECT COUNT(*) FROM bt_3_webapp_dictionary_queries qq
        JOIN bt_3_card_srs_state s ON s.card_id = qq.id AND s.user_id = qq.user_id
        WHERE {RU_GLUED};""")

    sub("Ж7. та же склейка в общем слое единиц")
    q(cur, """
        SELECT lang, COUNT(*) FROM bt_3_lex_units
        WHERE display ~ '[A-Za-zÄÖÜäöüß]' AND display ~ '[А-Яа-яЁё]'
        GROUP BY 1 ORDER BY 2 DESC;""")
    q(cur, """
        SELECT lang, kind, left(display, 76) FROM bt_3_lex_units
        WHERE display ~ '[A-Za-zÄÖÜäöüß]' AND display ~ '[А-Яа-яЁё]' LIMIT 12;""")

    sub("Ж8. конкретная карточка со скриншота")
    q(cur, """
        SELECT id, user_id, COALESCE(source_lang,'∅') || '→' || COALESCE(target_lang,'∅') AS пара, origin_process, created_at::date,
               word_ru, word_de, translation_ru, translation_de
        FROM bt_3_webapp_dictionary_queries
        WHERE position('behält sich das Recht vor' in COALESCE(word_ru,'')) > 0
           OR position('behält sich das Recht vor' in COALESCE(word_de,'')) > 0
        LIMIT 5;""")

    # ------------------------------------------------------------- Е. МАСШТАБ
    head("Е", "МАСШТАБ РАБОТЫ МОСТА")
    q(cur, """
        SELECT
          (SELECT COUNT(*) FROM bt_3_lex_senses s JOIN bt_3_lex_units u ON u.id=s.unit_id
             WHERE u.lang='de'
               AND EXISTS (SELECT 1 FROM bt_3_lex_links l WHERE l.sense_id=s.id)) AS значений_готовых,
          (SELECT COUNT(*) FROM bt_3_lex_units u WHERE u.lang='de' AND u.kind='word'
             AND NOT EXISTS (SELECT 1 FROM bt_3_lex_senses s WHERE s.unit_id=u.id)
             AND EXISTS (SELECT 1 FROM bt_3_lex_links l JOIN bt_3_lex_units t ON t.id=l.to_unit
                         WHERE l.from_unit=u.id AND t.lang='ru')) AS слов_без_значений_но_с_переводом;""")

    cur.close(); conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
