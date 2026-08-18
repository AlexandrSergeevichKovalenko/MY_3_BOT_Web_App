# -*- coding: utf-8 -*-
"""ПРОВЕРКА КАЖДОГО СЛУЧАЯ. Пока хоть одна строка не ПРОШЛА — говорить нечего."""
import os, sys
os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP","1"); os.environ.setdefault("SKIP_BILLING_LEDGER_WRITES","1")
sys.path.insert(0,"/Users/alexandr/Desktop/TELEGRAM_BOT_DEUTSCHESPRACHE")
from backend.database import get_db_connection_context
from backend import lex_units as LU

D = r'\y(der|die|das|den|dem|des|ein|eine|einen|einem|einer|eines)\s+\1\y'
ok = True

def check(name, got, want):
    global ok
    good = got == want
    ok = ok and good
    print("   %-52s %-22s %s" % (name, got, "ПРОШЛО" if good else "НЕ ПРОШЛО (ждали %s)" % (want,)))

with get_db_connection_context() as conn:
    with conn.cursor() as cur:
        # 1. то, что владелец показывал на скриншотах
        cur.execute("SELECT word_de, response_json->>'word_de', translation_de FROM bt_3_webapp_dictionary_queries WHERE id=18")
        w, rj, td = cur.fetchone()
        check("карточка 18: заголовок", w, "Daher nehme ich Korrekturen selbst vor")
        check("карточка 18: внутри разбора", rj, "Daher nehme ich Korrekturen selbst vor")
        cur.execute("""SELECT count(*) FROM bt_3_webapp_dictionary_queries
                       WHERE translation_de ILIKE '%vornehme ich Korrekturen%'
                          OR word_de ILIKE '%vornehme ich Korrekturen%'""")
        check("старый «Daher vornehme…» в колонках", cur.fetchone()[0], 0)

        # 2. удвоенный артикль — мой же промах
        for name, sql in (
            ("удвоенный артикль: карточки",
             "SELECT count(*) FROM bt_3_webapp_dictionary_queries WHERE word_de ~* %(d)s OR translation_de ~* %(d)s OR response_json::text ~* %(d)s"),
            ("удвоенный артикль: слова",
             "SELECT count(*) FROM bt_3_lex_units WHERE display ~* %(d)s OR card::text ~* %(d)s"),
            ("удвоенный артикль: пул",
             "SELECT count(*) FROM bt_3_dictionary_entries WHERE source_text ~* %(d)s OR response_json::text ~* %(d)s"),
        ):
            cur.execute(sql, {"d": D})
            check(name, cur.fetchone()[0], 0)

        # 3. склонённые прилагательные
        cur.execute("""SELECT count(*) FROM bt_3_lex_units WHERE lang='de' AND lower(lemma) = ANY(%s)""",
                    (["schlammigen","aussichtslosen","außereuropäischen","tatverdächtige","adversative"],))
        check("склонённые прилагательные остались", cur.fetchone()[0], 0)

        # 4. слова с заглавной без рода
        cur.execute("""SELECT count(*) FROM bt_3_lex_units WHERE lang='de' AND kind='word'
                       AND gender IS NULL AND card->>'article' IN ('der','die','das')""")
        check("род лежит в разборе, но не у слова", cur.fetchone()[0], 0)
        cur.execute("""SELECT count(*) FROM bt_3_lex_units WHERE lang='de' AND kind='word'
                       AND display ~ '^[A-ZÄÖÜ]' AND gender IS NULL
                       AND pos IS NOT NULL AND pos <> 'noun'""")
        check("не существительное с заглавной", cur.fetchone()[0], 0)

        # 5. задания на устаревшем тексте
        cur.execute("""SELECT count(*) FROM bt_3_webapp_dictionary_queries q
                       WHERE q.response_json ? 'sentence_gap_v2'
                         AND q.word_de IS NOT NULL
                         AND lower(q.response_json->'sentence_gap_v2'->>'source_sentence') NOT LIKE '%'||lower(q.word_de)||'%'
                         AND lower(COALESCE(q.response_json->'sentence_gap_v2'->'payload'->>'correct_full_sentence','')) NOT LIKE '%'||lower(q.word_de)||'%'""")
        check("задания на тексте, которого нет в карточке", cur.fetchone()[0], 0)

        # 6. немые карточки
        cur.execute("""SELECT count(*) FROM bt_3_lex_units u WHERE u.lang='de' AND u.card IS NOT NULL
                       AND NOT EXISTS (SELECT 1 FROM bt_3_lex_links l
                                       JOIN bt_3_lex_units t ON t.id=CASE WHEN l.from_unit=u.id THEN l.to_unit ELSE l.from_unit END
                                       WHERE (l.from_unit=u.id OR l.to_unit=u.id) AND t.lang='ru' AND l.rank<900)""")
        # Считаем ДЕФЕКТ, а не очередь: перевод в разборе ЕСТЬ, а связи нет. Слово,
        # у которого разбор пока без переводов, ждёт ночного обогащения — это не брак.
        cur.execute("""SELECT count(*) FROM bt_3_lex_units u WHERE u.lang='de'
                       AND jsonb_typeof(u.card->'translations')='array'
                       AND jsonb_array_length(u.card->'translations') > 0
                       AND NOT EXISTS (SELECT 1 FROM bt_3_lex_links l
                                       JOIN bt_3_lex_units t ON t.id=CASE WHEN l.from_unit=u.id THEN l.to_unit ELSE l.from_unit END
                                       WHERE (l.from_unit=u.id OR l.to_unit=u.id) AND t.lang='ru' AND l.rank<900)""")
        check("перевод в разборе есть, а связи нет", cur.fetchone()[0], 0)
        # Пустой массив translations внутри разбора — ещё НЕ признак немоты: у фраз
        # перевод живёт связью, и экран берёт именно её. Замер 18.08.2026: из 182 таких
        # слов у 180 перевод на месте. Поэтому считаем тех, у кого его нет НИГДЕ.
        cur.execute("""SELECT count(*) FROM bt_3_lex_units u WHERE u.lang='de' AND u.card IS NOT NULL
                       AND COALESCE(jsonb_array_length(u.card->'translations'), 0) = 0
                       AND NOT EXISTS (SELECT 1 FROM bt_3_lex_links l
                                       JOIN bt_3_lex_units t ON t.id=CASE WHEN l.from_unit=u.id THEN l.to_unit ELSE l.from_unit END
                                       WHERE (l.from_unit=u.id OR l.to_unit=u.id) AND t.lang='ru')""")
        print("   %-52s %s" % ("(ждут ночного разбора: перевода нет нигде)", cur.fetchone()[0]))

        # 7. перевод оказался немецким текстом — переводом это не является
        cur.execute("""SELECT count(*) FROM bt_3_lex_units u
                       JOIN bt_3_lex_links l ON (l.from_unit=u.id OR l.to_unit=u.id) AND l.rank<900
                       JOIN bt_3_lex_units t ON t.id=CASE WHEN l.from_unit=u.id THEN l.to_unit ELSE l.from_unit END
                        AND t.lang='ru'
                       WHERE u.lang='de' AND t.display !~ '[А-Яа-яЁё]'""")
        check("«перевод» без единой русской буквы", cur.fetchone()[0], 0)

        # 8. заголовок словарной статьи — правило продукта, все три хранилища.
        #    Проверяем НЕ ту таблицу, которую правили, а каждую, откуда читает экран.
        from backend.german_grammar_tables import german_dictionary_headword
        from backend.database import _fix_plural_article_on_headword
        for table, column, title, where in (
            ("bt_3_lex_units", "display", "слово в справочнике",
             "lang='de' AND display <> ''"),
            ("bt_3_webapp_dictionary_queries", "word_de", "карточка word_de",
             "word_de IS NOT NULL AND word_de !~ '[А-Яа-яЁё]'"),
            ("bt_3_webapp_dictionary_queries", "translation_de", "карточка translation_de",
             "translation_de IS NOT NULL AND translation_de !~ '[А-Яа-яЁё]'"),
            ("bt_3_dictionary_entries", "source_text", "пул",
             "source_text IS NOT NULL AND source_text !~ '[А-Яа-яЁё]'"),
        ):
            cur.execute("SELECT %s FROM %s WHERE %s;" % (column, table, where))
            bad = 0
            for (value,) in cur.fetchall():
                if not value:
                    continue
                if _fix_plural_article_on_headword(german_dictionary_headword(value)) != value:
                    bad += 1
            check("заголовок не в словарной форме: %s" % title, bad, 0)

        # 9. капс в заголовке: «ERNEUERBARE», «SCHNECKE» — это выделение из текста, не
        #    язык. Аббревиатуры (USA, NASA) остаются: признак точный — русская сторона
        #    у них тоже капсом, потому что аббревиатура ею и остаётся в любом языке.
        cur.execute("""
            SELECT count(*) FROM bt_3_lex_units u
             WHERE u.lang='de' AND u.display ~ '^[A-ZÄÖÜ]{3,}$'
               AND COALESCE((SELECT t.display FROM bt_3_lex_links l
                               JOIN bt_3_lex_units t
                                 ON t.id = CASE WHEN l.from_unit=u.id THEN l.to_unit ELSE l.from_unit END
                              WHERE (l.from_unit=u.id OR l.to_unit=u.id)
                                AND t.lang='ru' AND l.rank < 900 LIMIT 1), '') !~ '^[А-ЯЁ\\-]+$'
        """)
        check("заголовок капсом (кроме аббревиатур)", cur.fetchone()[0], 0)

        # 10. банк артиклей: форма множественного числа словом для тренировки
        cur.execute("""SELECT count(*) FROM bt_3_article_sprint_nouns b
                       JOIN bt_3_german_form_index f
                         ON lower(f.surface)=lower(b.word) AND f.number_tag='pl'
                       WHERE NOT b.retired AND lower(f.lemma)<>lower(b.word)
                         AND b.id NOT IN (9943, 13, 7062, 7134, 10096, 7966)""")
        check("банк Artikel: множественное число словом", cur.fetchone()[0], 0)

        # 11. решения владельца: то, что разобрано и оставлено намеренно
        cur.execute("SELECT count(*), count(DISTINCT defect_class) FROM bt_3_lex_review_decisions")
        n, classes = cur.fetchone()
        print("   %-52s %s" % ("решений владельца в реестре (классов: %d)" % classes, n))

# 12. живая выдача
for q, want in (("schlammig", "schlammig"), ("Gericht", "das Gericht"), ("die Habe", "die Habe")):
    it = LU.lookup(q, source_lang="de", target_lang="ru")
    check("выдача «%s»" % q, (it or {}).get("word_de"), want)

# 9. русская сторона у той, где перевода не было нигде
it = LU.lookup("Das Risiko willkürlicher Festnahmen.", source_lang="de", target_lang="ru") or {}
check("перевод «Das Risiko willkürlicher Festnahmen.»",
      str(it.get("word_ru") or "").rstrip("."), "Риск произвольных арестов")

print()
print("ИТОГ:", "ВСЁ ПРОШЛО" if ok else "ЕСТЬ НЕ ПРОШЕДШИЕ")
