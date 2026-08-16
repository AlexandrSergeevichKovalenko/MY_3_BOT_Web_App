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
        print("   %-52s %s" % ("немых карточек (разбор есть, перевода нет)", cur.fetchone()[0]))

# 7. живая выдача
for q, want in (("schlammig", "schlammig"), ("Gericht", "das Gericht"), ("die Habe", "die Habe")):
    it = LU.lookup(q, source_lang="de", target_lang="ru")
    check("выдача «%s»" % q, (it or {}).get("word_de"), want)

print()
print("ИТОГ:", "ВСЁ ПРОШЛО" if ok else "ЕСТЬ НЕ ПРОШЕДШИЕ")
