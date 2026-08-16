# -*- coding: utf-8 -*-
"""Слово без части речи и существительное без рода: закрыть тем, что уже известно.

Откуда взялись
──────────────
После разбора немых карточек 16.08.2026 остались две кучи:
    48 слов с заглавной буквы, у которых часть речи не проставлена;
     7 слов с меткой «существительное», но без рода.

Часть речи для слова — это не украшение: пока её нет, слово формально «неизвестно что».
От неё зависит регистр заголовка (в немецком с заглавной пишутся ТОЛЬКО существительные),
подбор рода и то, какие таблицы форм строятся.

Откуда берём ответ. Источники, а не догадки
───────────────────────────────────────────
    часть речи   из СОБСТВЕННОГО разбора слова (`card->>'part_of_speech'`). Разбор уже
                 собран моделью и проверен на выдаче; своей догадки не добавляем.
    регистр      german_grammar_tables.german_headword_case — правило продукта.
    род          article_authority.authoritative_article: справочник de.wiktionary,
                 живой запрос при промахе, правило композита. Молчит — оставляем без
                 рода, ничего не выдумываем.

Что НЕ чиним и почему это отдельная строка отчёта
─────────────────────────────────────────────────
    заголовок-форма   «Gedieh», «Klingt», «Hätte» — это прошедшее время и спряжённые
                      формы, а не словарная. Опустить регистр мало, нужна лемма, а
                      источника леммы для глагола у нас нет: лемматизатор уже выводили
                      из пути сохранения за откусывание окончаний. Печатаем списком.
    разбор молчит     part_of_speech = other / phrase / пусто. Часть речи неоткуда
                      взять — печатаем списком.

Опознание слова = лемма + часть речи + род, поэтому любая из этих правок МЕНЯЕТ ключ,
по которому слово находят. Если рядом уже живёт такое же слово с этим ключом, строку
пропускаем: сливать единицы без решения владельца нельзя.

    python3 scripts/dict_close_pos_and_gender_gaps.py            # сухой прогон
    python3 scripts/dict_close_pos_and_gender_gaps.py --apply    # записать
"""
from __future__ import annotations

import argparse
import os
import sys

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SKIP_BILLING_LEDGER_WRITES", "1")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.database import get_db_connection_context            # noqa: E402
from backend.article_authority import (                           # noqa: E402
    authoritative_article,
    compound_article,
)
from backend.german_grammar_tables import german_headword_case    # noqa: E402
from backend.lex_units import normalize_query                     # noqa: E402

# Части речи, которые разбор называет однозначно и которым можно верить.
KNOWN_POS = {"noun", "verb", "adjective", "adverb"}
VERB_ENDINGS = ("en", "eln", "ern")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    plan, not_lemma, no_source, gender_silent = [], [], [], []

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            # Русский перевод берётся вместе со словом: в русском с заглавной пишутся
            # ТОЛЬКО имена собственные, и это признак, которому можно верить без модели.
            # Имени собственному род проставлять нельзя: заголовок склеивается с
            # артиклем, и «Athen» стало бы «das Athen», «Marokko» — «das Marokko».
            cur.execute(
                """
                SELECT u.id, u.display, u.pos, u.gender, u.card->>'part_of_speech',
                       (SELECT t.display FROM bt_3_lex_links l
                          JOIN bt_3_lex_units t
                            ON t.id = CASE WHEN l.from_unit=u.id THEN l.to_unit ELSE l.from_unit END
                         WHERE (l.from_unit=u.id OR l.to_unit=u.id)
                           AND t.lang='ru' AND l.rank < 900
                         ORDER BY l.rank LIMIT 1)
                FROM bt_3_lex_units u
                WHERE u.lang='de' AND u.kind='word'
                  AND (u.pos IS NULL OR (u.pos='noun' AND u.gender IS NULL))
                ORDER BY u.display;
                """
            )
            rows = cur.fetchall()

    proper_nouns = []
    for unit_id, display, pos, gender, card_pos, russian in rows:
        said = str(card_pos or "").strip().lower()
        is_proper = bool(russian) and str(russian)[:1].isupper()
        if said not in KNOWN_POS:
            # Разбор молчит или врёт: «Kurzbefehl», «Stumm-Modus», «Bugfahrwerk»
            # помечены фразой, хотя это одно слово. Метку не чиним догадкой — берём
            # ПРАВИЛО КОМПОЗИТА: род немецкого сложного слова равен роду его последней
            # части, а род бывает только у существительного. Значит найденный род — это
            # ответ и про часть речи, полученный из источника.
            #
            # ⚠ Почему именно правило композита, а не общий вопрос справочнику: немецкий
            # субстантивирует что угодно, и de.wiktionary честно документирует «das Aber»,
            # «das Wenn», «der Schade». Спросив про написание с заглавной, я сделал бы
            # существительными союзы «aber», «wenn» и наречие «schade» — проверено на
            # сухом прогоне. У правила композита такой дыры нет по построению: служебное
            # слово короче восьми букв и головы не имеет.
            single_word = display and " " not in display.strip()
            if single_word and display[:1].isupper() and not is_proper:
                article = compound_article(display)
                if article:
                    plan.append((unit_id, display, display, pos, "noun", gender, article))
                    continue
            no_source.append((unit_id, display, said or "—"))
            continue

        # Глагол, чей заголовок не инфинитив, — это форма, а не словарная статья.
        if said == "verb" and not display.lower().endswith(VERB_ENDINGS):
            not_lemma.append((unit_id, display, said))
            continue

        new_pos = said
        new_display = german_headword_case(display, said)
        new_gender = gender
        is_proper = bool(russian) and str(russian)[:1].isupper()
        if said == "noun" and not gender and is_proper:
            proper_nouns.append((unit_id, display, russian))
        elif said == "noun" and not gender:
            article, source = authoritative_article(new_display, allow_network=True)
            if article:
                new_gender = article
            else:
                gender_silent.append((unit_id, display, source))
        if new_pos == pos and new_display == display and new_gender == gender:
            continue
        plan.append((unit_id, display, new_display, pos, new_pos, gender, new_gender))

    print("К ПРАВКЕ: %d" % len(plan))
    for unit_id, old, new, pos, new_pos, gender, new_gender in plan:
        parts = []
        if new != old:
            parts.append("«%s» → «%s»" % (old, new))
        if new_pos != pos:
            parts.append("часть речи: %s" % new_pos)
        if new_gender != gender:
            parts.append("род: %s" % new_gender)
        print("   %-7s %s" % (unit_id, "; ".join(parts)))

    print()
    print("НЕ ТРОГАЕМ — заголовок это форма, а не словарная статья (нужна лемма): %d" % len(not_lemma))
    for unit_id, display, said in not_lemma:
        print("   %-7s %-24s разбор: %s" % (unit_id, display, said))
    print()
    print("НЕ ТРОГАЕМ — разбор не называет часть речи: %d" % len(no_source))
    for unit_id, display, said in no_source[:60]:
        print("   %-7s %-24s разбор: %s" % (unit_id, display, said))
    print()
    print("СУЩЕСТВИТЕЛЬНЫЕ, ПРО КОТОРЫЕ СПРАВОЧНИК МОЛЧИТ: %d" % len(gender_silent))
    for unit_id, display, source in gender_silent:
        print("   %-7s %-24s %s" % (unit_id, display, source))
    print()
    print("ИМЕНА СОБСТВЕННЫЕ — род не ставим, артикль им не положен: %d" % len(proper_nouns))
    for unit_id, display, russian in proper_nouns:
        print("   %-7s %-24s перевод с заглавной: %s" % (unit_id, display, russian))

    if not args.apply:
        print()
        print("СУХОЙ ПРОГОН. Ничего не записано. Для записи добавь --apply.")
        return

    done = skipped = 0
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            for unit_id, old, new, _pos, new_pos, _gender, new_gender in plan:
                cur.execute(
                    """
                    SELECT 1 FROM bt_3_lex_units
                     WHERE lang='de' AND kind='word' AND lemma_key=%s
                       AND COALESCE(pos,'')=%s AND COALESCE(gender,'')=%s AND id<>%s
                     LIMIT 1;
                    """,
                    (normalize_query(new), new_pos or "", new_gender or "", unit_id),
                )
                if cur.fetchone():
                    print("   ⚠ %-7s «%s»: такое слово с этим ключом уже есть — ПРОПУСК"
                          % (unit_id, new))
                    skipped += 1
                    continue
                cur.execute(
                    """
                    UPDATE bt_3_lex_units
                       SET display=%s, lemma=%s, lemma_key=%s, pos=%s, gender=%s,
                           pos_source=COALESCE(pos_source,'card'),
                           gender_source=CASE WHEN %s IS NULL THEN gender_source
                                              ELSE COALESCE(gender_source,'справочник') END,
                           updated_at=NOW()
                     WHERE id=%s;
                    """,
                    (new, new, normalize_query(new), new_pos, new_gender,
                     new_gender, unit_id),
                )
                cur.execute(
                    "INSERT INTO bt_3_lex_surfaces (lang, surface_key, unit_id, match_kind) "
                    "VALUES ('de', %s, %s, 'exact') ON CONFLICT DO NOTHING;",
                    (normalize_query(new), unit_id),
                )
                done += 1
        conn.commit()

    print()
    print("проставлено: %d, пропущено из-за совпадения ключа: %d" % (done, skipped))


if __name__ == "__main__":
    main()
