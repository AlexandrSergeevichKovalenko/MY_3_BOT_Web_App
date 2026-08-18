# -*- coding: utf-8 -*-
"""Решения владельца 18.08.2026 по разбору словаря: и правки, и «оставить как есть».

Разбор шёл списком, слово за словом, с переводом каждого перед глазами. Ниже ровно то,
что решено, и почему. Решения «оставить» записываются в реестр
bt_3_lex_review_decisions — иначе следующая проверка принесёт те же слова снова, а
владелец прямо сказал: «чтобы ты потом опять не показывал, что это некорректные слова».

    python3 scripts/dict_apply_owner_decisions_18_08.py            # сухой прогон
    python3 scripts/dict_apply_owner_decisions_18_08.py --apply    # записать
"""
from __future__ import annotations

import argparse
import os
import sys

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SKIP_BILLING_LEDGER_WRITES", "1")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from backend.database import (                                    # noqa: E402
    get_db_connection_context,
    spread_correction_everywhere,
)
from backend.lex_review_decisions import (                        # noqa: E402
    ensure_lex_review_decisions_schema,
    record_decision,
)
from backend.lex_units import normalize_query                     # noqa: E402
from dict_fix_plural_headwords import merge_into                  # noqa: E402

# ── ПРАВИМ: заголовок не словарная форма. Решение владельца — привести к правильной ──
# что лежит → (как правильно, часть речи, почему)
RENAME = {
    "abgezweigt": ("abzweigen", "verb", "причастие от abzweigen, перевод «отклонился»"),
    "verketteten": ("verketten", "verb", "форма от verketten, перевод «связывать»"),
    "wuchsen": ("wachsen", "verb", "форма от wachsen, перевод «росли»"),
    "verfälzen": ("verfälschen", "verb", "опечатка, перевод «искажать»"),
    "zwitschen": ("zwitschern", "verb", "опечатка, перевод «щебетание»"),
    "nitpicker": ("der Erbsenzähler", "noun",
                  "английское слово; по-немецки «придира» — der Erbsenzähler, "
                  "род задокументирован справочником"),
    # Обрезок и школьная ошибка склонения — те же классы, что чинились раньше.
    "Die Verbindlichkeite": ("die Verbindlichkeiten", "noun",
                             "обрезок; перевод «обязательства» — множественное число"),
    "Der Verbündeter": ("der Verbündete", "noun",
                        "с артиклем слабое склонение: der Verbündete, не Verbündeter"),
    "Der Angestellter": ("der Angestellte", "noun", "то же слабое склонение"),
    "Leuchtest": ("leuchten", "verb", "спряжённая форма 2 лица, перевод «светишься»"),
    "des Umbruchs": ("der Umbruch", "noun", "родительный падеж в заголовке"),
    "der Schneebesen.": ("der Schneebesen", "noun", "лишняя точка"),
    "der Tortenheber.": ("der Tortenheber", "noun", "лишняя точка"),
    "die Suppenkelle.": ("die Suppenkelle", "noun", "лишняя точка"),
    "der Kochlöffel.": ("der Kochlöffel", "noun", "лишняя точка"),
}

# ── СНОСИМ: мусор без карточек ──────────────────────────────────────────────────
DELETE = {
    "wüen": "перевод «неизвестное_слово», карточек нет — мусор",
}

# ── ЗАВОДИМ ПРАВИЛЬНОЕ СЛОВО И ПЕРЕПРИВЯЗЫВАЕМ КАРТОЧКУ ─────────────────────────
# карточка → (правильное написание, род/часть речи, русский перевод, почему)
REBIND = {
    "die Eiche": ("die Eiche", "noun", "дуб",
                  "карточка была привязана к глаголу «eichen» (калибровать) — "
                  "человек видел разбор совершенно другого слова"),
    "die Glotze": ("die Glotze", "noun", "телик",
                   "то же: привязана к глаголу «glotzen» (пялиться)"),
    "Künftig": ("künftig", "adverb", "впредь",
                "привязана к существительному «die Künftige»; «künftig» — наречие"),
}

# ── РАЗБИВАЕМ СКЛЕЕННОЕ ─────────────────────────────────────────────────────────
SPLIT = {
    "schwerfallen-leichtfallen": (("schwerfallen", "leichtfallen"),
                                  "два разных глагола через дефис; у карточек 10 штук"),
    "zuspätkommen": (("zu spät kommen",),
                     "слитно не существует: в немецком это три слова"),
}

# ── ОСТАВЛЯЕМ КАК ЕСТЬ. Записываем решение, чтобы проверки больше не спрашивали ──
# слово → (класс дефекта, почему оставили)
KEEP = {
    "aspettiamo": ("не-немецкое слово", "сленг; человек сохранил сам, карточка рабочая"),
    "bore": ("не-немецкое слово", "сленг; человек сохранил сам, карточка рабочая"),
    "slay": ("не-немецкое слово", "сленг; человек сохранил сам, карточка рабочая"),
    "Hätte": ("заголовок не инфинитив", "идиома «Если бы да кабы», а не глагол haben"),
    "möchten": ("заголовок не инфинитив",
                "форма от mögen, но живёт как самостоятельный модальный глагол"),
    "verzockt": ("заголовок не инфинитив",
                 "причастие в роли прилагательного, перевод «проигранный»"),
    # Причастия, сохранённые человеком как отдельные слова. Владелец: «человек же их
    # как-то сохранил в такой форме, значит они ему в такой форме нужны».
    "umgewandelt": ("причастие отдельным словом", "сохранено человеком как прилагательное"),
    "verdeutlicht": ("причастие отдельным словом", "сохранено человеком как прилагательное"),
    "vererbt": ("причастие отдельным словом", "сохранено человеком как прилагательное"),
    "erfüllt": ("причастие отдельным словом", "сохранено человеком как прилагательное"),
    "abgezogen": ("причастие отдельным словом", "сохранено человеком как прилагательное"),
    "verbraucht": ("причастие отдельным словом", "сохранено человеком как прилагательное"),
    "geheftet": ("причастие отдельным словом", "сохранено человеком как прилагательное"),
    "zurückgetreten": ("причастие отдельным словом", "сохранено человеком как прилагательное"),
    "Umfasst": ("причастие отдельным словом", "сохранено человеком как прилагательное"),
    "abgereichert": ("причастие отдельным словом", "сохранено человеком как прилагательное"),
    # Глаголы на -tun: нормальные инфинитивы, мой фильтр их не знал.
    "abtun": ("заголовок не инфинитив", "это инфинитив: tun — глагол, фильтр его не знал"),
    "antun": ("заголовок не инфинитив", "то же"),
    "hineintun": ("заголовок не инфинитив", "то же"),
    "nachtun": ("заголовок не инфинитив", "то же"),
    "vertun": ("заголовок не инфинитив", "то же"),
}

# ── РОД ПО АНАЛОГИИ, а не по справочнику. Записываем ИМЕННО так ─────────────────
GENDER_BY_ANALOGY = {
    "Ragebait": ("der", "справочник страницы не даёт; родственные заимствования "
                        "«der Bait» и «der Content» задокументированы мужским родом"),
}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    if args.apply:
        ensure_lex_review_decisions_schema()

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            print("ПЕРЕИМЕНОВАНИЕ:")
            for old, (new, pos, why) in sorted(RENAME.items()):
                cur.execute(
                    "SELECT id FROM bt_3_lex_units WHERE lang='de' AND lower(display)=lower(%s);",
                    (old,),
                )
                units = [r[0] for r in cur.fetchall()]
                cur.execute(
                    "SELECT count(*) FROM bt_3_webapp_dictionary_queries "
                    "WHERE lower(BTRIM(word_de))=lower(%s) OR lower(BTRIM(translation_de))=lower(%s);",
                    (old, old),
                )
                cards = cur.fetchone()[0]
                print("   %-26s → %-24s слов %d, карточек %d   (%s)"
                      % (old, new, len(units), cards, why))
                if not args.apply:
                    continue
                for unit_id in units:
                    cur.execute(
                        "SELECT id FROM bt_3_lex_units WHERE lang='de' AND lemma_key=%s AND id<>%s LIMIT 1;",
                        (normalize_query(new), unit_id),
                    )
                    twin = cur.fetchone()
                    spread_correction_everywhere(cur, unit_id=unit_id, old_text=old, new_text=new)
                    if twin:
                        merge_into(cur, dead=unit_id, alive=int(twin[0]), lang="de")
                    else:
                        cur.execute(
                            "UPDATE bt_3_lex_units SET display=%s, lemma=%s, lemma_key=%s, pos=%s, "
                            "updated_at=NOW() WHERE id=%s;",
                            (new, new, normalize_query(new), pos, unit_id),
                        )
                        cur.execute(
                            "INSERT INTO bt_3_lex_surfaces (lang, surface_key, unit_id, match_kind) "
                            "VALUES ('de', %s, %s, 'exact') ON CONFLICT DO NOTHING;",
                            (normalize_query(new), unit_id),
                        )
                if not units and cards:
                    # Слова нет, а карточки с таким текстом есть — правим текст напрямую.
                    cur.execute(
                        "UPDATE bt_3_webapp_dictionary_queries SET word_de=%s, "
                        "translation_de=CASE WHEN translation_de IS NULL THEN NULL ELSE %s END, "
                        "updated_at=NOW() "
                        "WHERE lower(BTRIM(word_de))=lower(%s) OR lower(BTRIM(translation_de))=lower(%s);",
                        (new, new, old, old),
                    )

            print()
            print("СНОСИМ:")
            for word, why in sorted(DELETE.items()):
                cur.execute(
                    "SELECT u.id, (SELECT count(*) FROM bt_3_webapp_dictionary_queries q "
                    "WHERE q.lex_unit_id=u.id) FROM bt_3_lex_units u "
                    "WHERE u.lang='de' AND lower(u.display)=lower(%s);",
                    (word,),
                )
                for unit_id, cards in cur.fetchall():
                    print("   %-7s %-16s карточек %d   (%s)" % (unit_id, word, cards, why))
                    if args.apply and cards == 0:
                        cur.execute("DELETE FROM bt_3_lex_links WHERE from_unit=%s OR to_unit=%s;",
                                    (unit_id, unit_id))
                        cur.execute("DELETE FROM bt_3_lex_surfaces WHERE unit_id=%s;", (unit_id,))
                        cur.execute("DELETE FROM bt_3_lex_units WHERE id=%s;", (unit_id,))

            print()
            print("ОСТАВЛЯЕМ — записываем решение, чтобы проверки больше не спрашивали:")
            for word, (defect_class, why) in sorted(KEEP.items()):
                print("   %-18s [%s] %s" % (word, defect_class, why))
                if args.apply:
                    record_decision(word, defect_class, decision="оставить", reason=why)

            print()
            print("РОД ПО АНАЛОГИИ (справочник молчит):")
            for word, (article, why) in sorted(GENDER_BY_ANALOGY.items()):
                print("   %-14s → %s   (%s)" % (word, article, why))
                if args.apply:
                    cur.execute(
                        "UPDATE bt_3_lex_units SET gender=%s, "
                        "gender_source='аналогия, решение владельца 18.08.2026', updated_at=NOW() "
                        "WHERE lang='de' AND lower(display)=lower(%s) AND gender IS NULL;",
                        (article, word),
                    )
                    record_decision(word, "существительное без рода",
                                    decision="род по аналогии", reason=why)
        if args.apply:
            conn.commit()

    print()
    print("ОТДЕЛЬНО, РУЧНЫМИ ШАГАМИ (сложнее замены строки):")
    for word, (parts, why) in sorted(SPLIT.items()):
        print("   разбить %-26s → %s   (%s)" % (word, " + ".join(parts), why))
    for word, (correct, pos, ru, why) in sorted(REBIND.items()):
        print("   перепривязать %-12s → «%s» (%s, %s)   %s" % (word, correct, pos, ru, why))

    if not args.apply:
        print()
        print("СУХОЙ ПРОГОН. Ничего не записано. Для записи добавь --apply.")


if __name__ == "__main__":
    main()
