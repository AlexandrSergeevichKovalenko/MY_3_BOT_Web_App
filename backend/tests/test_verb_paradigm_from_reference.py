# -*- coding: utf-8 -*-
"""Спряжение читается из напечатанной таблицы справочника, а не считается правилом.

Владелец 17.08.2026: «это лингвистическое приложение, тут не должно быть придуманного
от тебя ничего, только из справочников, только из таблиц».

Разбор шаблона основ этого не давал: у сильного глагола «du hältst», у слабого
«du arbeitest», у «lesen» — «du liest». Эти окончания из основ не выводятся, и первая
версия печатала «du hältest» и «er arbeit». Поэтому формы берутся из готовой таблицы
дословно. Здесь проверяется чтение этой таблицы — на сохранённых кусках настоящей
страницы, без обращения в сеть.
"""
from backend.german_verb_paradigms import documented_tables

# Куски настоящей отрендеренной страницы Flexion. Разметка убрана так же, как в модуле:
# каждая ячейка отдельной строкой.
def _page(cells):
    return "".join("<td>%s</td>" % c for c in cells)


HALTEN = _page([
    "Imperative", "Präsens Aktiv", "Präsens Vorgangspassiv",
    "2. Person Singular", "halt!", "halte!", "werde gehalten!",
    "2. Person Plural", "haltet!", "werdet gehalten!",
    "Präsens", "Person", "Aktiv", "Vorgangspassiv",
    "Indikativ", "Konjunktiv I", "Indikativ", "Konjunktiv I",
    "1. Person Singular", "ich halte", "ich halte", "ich werde gehalten", "ich werde gehalten",
    "2. Person Singular", "du hältst", "du haltest", "du wirst gehalten", "du werdest gehalten",
    "3. Person Singular", "er/sie/es", "hält", "er/sie/es", "halte", "er/sie/es", "wird gehalten",
    "1. Person Plural", "wir halten", "wir halten", "wir werden gehalten", "wir werden gehalten",
    "2. Person Plural", "ihr haltet", "ihr haltet", "ihr werdet gehalten", "ihr werdet gehalten",
    "3. Person Plural", "sie halten", "sie halten", "sie werden gehalten", "sie werden gehalten",
    "Präteritum", "Person", "Aktiv", "Indikativ", "Konjunktiv II",
    "1. Person Singular", "ich hielt", "ich hielte",
    "2. Person Singular", "du hieltest,", "du hieltst", "du hieltest",
    "3. Person Singular", "er/sie/es", "hielt", "er/sie/es", "hielte",
    "1. Person Plural", "wir hielten", "wir hielten",
    "2. Person Plural", "ihr hieltet", "ihr hieltet",
    "3. Person Plural", "sie hielten", "sie hielten",
])

KLARKOMMEN = _page([
    "Imperative", "Präsens Aktiv",
    "2. Person Singular", "komm klar!", "komme klar!",
    "2. Person Plural", "kommt klar!",
    "Präsens", "Person", "Aktiv", "Indikativ", "Konjunktiv I",
    "1. Person Singular", "ich komme klar", "ich komme klar",
    "2. Person Singular", "du kommst klar", "du kommest klar",
    "3. Person Singular", "er/sie/es", "kommt klar", "er/sie/es", "komme klar",
    "1. Person Plural", "wir kommen klar", "wir kommen klar",
    "2. Person Plural", "ihr kommt klar", "ihr kommet klar",
    "3. Person Plural", "sie kommen klar", "sie kommen klar",
    "Präteritum", "Person", "Aktiv", "Indikativ", "Konjunktiv II",
    "1. Person Singular", "ich kam klar", "ich käme klar",
    "2. Person Singular", "du kamst klar", "du kämest klar",
    "3. Person Singular", "er/sie/es", "kam klar", "er/sie/es", "käme klar",
    "1. Person Plural", "wir kamen klar", "wir kämen klar",
    "2. Person Plural", "ihr kamt klar", "ihr kämet klar",
    "3. Person Plural", "sie kamen klar", "sie kämen klar",
])


def test_strong_verb_forms_are_taken_verbatim():
    """«du hältst» — правилом эта форма не выводится, она читается из таблицы."""
    tables = documented_tables(HALTEN)
    assert tables["praesens"]["du"] == "hältst"
    assert tables["praesens"]["er/sie/es"] == "hält"
    assert tables["praesens"]["ihr"] == "haltet"


def test_separable_prefix_position_comes_from_the_reference():
    """Списка приставок здесь нет: в таблице напечатано «ich komme klar»."""
    tables = documented_tables(KLARKOMMEN)
    assert tables["praesens"]["ich"] == "komme klar"
    assert tables["praesens"]["wir"] == "kommen klar"
    assert tables["praeteritum"]["du"] == "kamst klar"


def test_imperative_is_not_overwritten_by_the_present_block():
    """Метки лиц повторяются ниже по странице; берём первое вхождение."""
    assert documented_tables(HALTEN)["imperativ"]["du"] == "halt"
    assert documented_tables(KLARKOMMEN)["imperativ"]["du"] == "komm klar"


def test_comma_variant_does_not_shift_the_column():
    """«du hieltest,» и «du hieltst» — два варианта ОДНОЙ клетки, а не два столбца."""
    tables = documented_tables(HALTEN)
    assert tables["praeteritum"]["du"] == "hieltest"
    assert tables["konjunktiv2"]["du"] == "hieltest"


def test_third_person_pronoun_cell_is_skipped():
    assert documented_tables(HALTEN)["praeteritum"]["er/sie/es"] == "hielt"


def test_incomplete_table_yields_nothing():
    """Половина строк — не таблица. Лучше не показать, чем показать с дырами."""
    partial = _page(["Präsens", "1. Person Singular", "ich halte"])
    assert documented_tables(partial).get("praesens") is None
