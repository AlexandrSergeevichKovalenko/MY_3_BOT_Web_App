# -*- coding: utf-8 -*-
"""Заголовок слова: что с ним не так — ПО ВСЕМ трём хранилищам сразу.

Зачем этот замер существует
───────────────────────────
16.08.2026 владелец прислал три скриншота подряд: «schlammigen» вместо «schlammig»,
«klarzukommen» вместо «klarkommen», «die eine Pleite» вместо «die Pleite». Всё три
я к тому моменту «починил» — и отчитался. Проверка соврала, потому что смотрела в
ОДНО хранилище (слово в справочнике), а экран читает три:

    bt_3_lex_units                  слово в справочнике (общее для всех)
    bt_3_webapp_dictionary_queries  личная карточка: word_de, translation_de и разбор
    bt_3_dictionary_entries         общий пул (кеш ответов модели)

Фактическое состояние на момент замера:
    schlammigen   слово починено · карточка НЕТ (обе колонки) · пул НЕТ
    klarzukommen  word_de починено · translation_de НЕТ · пул НЕТ

⭐ ПРАВИЛО, РАДИ КОТОРОГО НАПИСАН ЭТОТ ФАЙЛ: заголовок правится во всех хранилищах
сразу, а проверяется тем же путём, каким его читает экран. Починка одного слоя — это
не починка, а видимость.

Что считается дефектом (каждое правило детерминированное, без модели)
────────────────────────────────────────────────────────────────────
    zu-инфинитив        «klarzukommen» — «zu» между приставкой и основой; словарной
                        формы с ней не существует, и наш движок печатает от неё
                        выдуманное спряжение. Правило: german_grammar_tables.
                        looks_like_zu_infinitive — то же, что в продукте.
    неопределённый      «eine Pleite» — артикль «ein/eine/…» внутри заголовка. Экран
    артикль             дописывает СВОЙ артикль по роду и получается «die eine Pleite».
                        В словарной статье неопределённого артикля не бывает.
    определённый        «die Fahne» как заголовок — сам по себе законен (так и
    артикль             показываем), поэтому НЕ дефект. Считаем отдельной строкой,
                        чтобы было видно, сколько их и что они не потерялись.
    множественное       «Handschuhe» отдельным словом при живом «Handschuh». Судим
    как заголовок       ТОЛЬКО по нашему же указателю форм bt_3_german_form_index
                        (страницы de.wiktionary «Nominativ Plural des Substantivs X»),
                        а не по окончанию: «Felge», «Auslage», «Kosten» окончанием
                        неотличимы от множественного.
    слои разошлись      одно и то же слово в карточке, в справочнике и в пуле написано
                        по-разному. Это и есть след «починил один слой».

Склонённые прилагательные («schlammigen») отдельным правилом НЕ ловятся: окончание
-en/-e/-er/-es носят и настоящие словарные формы («offen», «sauber», «leiser»), а
указателя форм для прилагательных у нас нет — там только существительные (4928 строк).
Их ловит строка «слои разошлись»: у такого слова карточка не совпадает со справочником.

    python3 scripts/dict_headword_defect_audit.py            # весь замер
    python3 scripts/dict_headword_defect_audit.py --list zu  # показать список по классу
"""
from __future__ import annotations

import argparse
import os
import re
import sys

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SKIP_BILLING_LEDGER_WRITES", "1")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.database import get_db_connection_context            # noqa: E402
from backend.german_grammar_tables import looks_like_zu_infinitive  # noqa: E402

INDEFINITE_RE = re.compile(r"^(?:ein|eine|einen|einem|einer|eines)\s+\S", re.I)
DEFINITE_RE = re.compile(r"^(?:der|die|das|den|dem|des)\s+\S", re.I)
_SPACE = re.compile(r"\s+")


def key_of(value) -> str:
    return _SPACE.sub(" ", str(value or "").strip()).casefold()


def collect() -> dict:
    """Собрать все немецкие заголовки из трёх хранилищ. Ничего не меняем."""
    rows = {"слово в справочнике": [], "карточка word_de": [],
            "карточка translation_de": [], "пул": []}
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, display FROM bt_3_lex_units WHERE lang='de' AND display <> '';")
            rows["слово в справочнике"] = cur.fetchall()
            cur.execute("""SELECT id, word_de FROM bt_3_webapp_dictionary_queries
                           WHERE word_de IS NOT NULL AND word_de <> '';""")
            rows["карточка word_de"] = cur.fetchall()
            # translation_de — вторая колонка немецкой стороны. Именно её показывает
            # экран разбора, и именно её я не тронул при первой починке.
            cur.execute("""SELECT id, translation_de FROM bt_3_webapp_dictionary_queries
                           WHERE translation_de IS NOT NULL AND translation_de <> ''
                             AND translation_de !~ '[А-Яа-яЁё]';""")
            rows["карточка translation_de"] = cur.fetchall()
            cur.execute("""SELECT id, source_text FROM bt_3_dictionary_entries
                           WHERE source_text IS NOT NULL AND source_text <> ''
                             AND source_text !~ '[А-Яа-яЁё]';""")
            rows["пул"] = cur.fetchall()

            cur.execute("SELECT lower(surface), lemma FROM bt_3_german_form_index WHERE number_tag='pl';")
            plurals = {k: v for k, v in cur.fetchall()}

            # слои разошлись: карточка ↔ её же слово в справочнике
            cur.execute("""
                SELECT q.id, q.word_de, q.translation_de, u.display
                FROM bt_3_webapp_dictionary_queries q
                JOIN bt_3_lex_units u ON u.id = q.lex_unit_id AND u.lang='de'
                WHERE u.kind = 'word'
                  AND (q.word_de IS NOT NULL OR q.translation_de IS NOT NULL);
            """)
            drift = cur.fetchall()
    return {"rows": rows, "plurals": plurals, "drift": drift}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--list", dest="show", default="",
                        help="zu | indefinite | plural | drift — показать список")
    args = parser.parse_args()

    data = collect()
    plurals = data["plurals"]
    found = {"zu": {}, "indefinite": {}, "definite": {}, "plural": {}}

    for layer, items in data["rows"].items():
        for row_id, text in items:
            surface = str(text or "").strip()
            if not surface:
                continue
            one_word = " " not in surface
            if one_word and looks_like_zu_infinitive(surface):
                found["zu"].setdefault(layer, []).append((row_id, surface))
            if INDEFINITE_RE.match(surface):
                found["indefinite"].setdefault(layer, []).append((row_id, surface))
            elif DEFINITE_RE.match(surface):
                found["definite"].setdefault(layer, []).append((row_id, surface))
            bare = DEFINITE_RE.sub("", surface).strip()
            lemma = plurals.get(bare.lower())
            if one_word and lemma and lemma.lower() != bare.lower():
                found["plural"].setdefault(layer, []).append((row_id, surface, lemma))

    drift = []
    for entry_id, word_de, translation_de, display in data["drift"]:
        shown = key_of(display)
        bad = [name for name, value in (("word_de", word_de), ("translation_de", translation_de))
               if value and key_of(value) != shown]
        if bad:
            drift.append((entry_id, word_de, translation_de, display, bad))

    print("ЗАГОЛОВКИ: что не так, по хранилищам")
    print()
    titles = {"zu": "zu-инфинитив вместо словарной формы",
              "indefinite": "неопределённый артикль внутри заголовка",
              "plural": "множественное число отдельным заголовком",
              "definite": "определённый артикль в заголовке (это НОРМА, для счёта)"}
    for name in ("zu", "indefinite", "plural", "definite"):
        total = sum(len(v) for v in found[name].values())
        print("   %-52s %d" % (titles[name], total))
        for layer, items in sorted(found[name].items()):
            print("      %-28s %d" % (layer, len(items)))
        if not total:
            print("      —")
    print()
    print("   %-52s %d" % ("карточка разошлась со своим словом в справочнике", len(drift)))

    if args.show:
        print()
        if args.show == "drift":
            for entry_id, word_de, translation_de, display, bad in drift[:80]:
                print("   карточка %-8s справочник: %-30s  карточка: %s / %s   (%s)"
                      % (entry_id, str(display)[:30], str(word_de)[:26],
                         str(translation_de)[:26], ", ".join(bad)))
            print("   … всего %d" % len(drift))
        else:
            for layer, items in sorted(found.get(args.show, {}).items()):
                print("   %s:" % layer)
                for item in items[:60]:
                    print("      %s" % (item,))


if __name__ == "__main__":
    main()
