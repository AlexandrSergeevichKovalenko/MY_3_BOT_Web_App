# -*- coding: utf-8 -*-
"""У отделяемого глагола приставка в личной форме УХОДИТ В КОНЕЦ.

Владелец, 17.08.2026, на карточке «klarkommen»: таблица печатала «ich klarzukomme»,
а после исправления заголовка — «ich klarkomme». Ни того, ни другого в немецком нет:
правильно «ich komme klar».

Замер: из 1321 глагола справочника 435 отделяемых — то есть у каждого третьего глагола
таблица спряжения показывала несуществующие формы. Таблицы нигде не хранятся, они
строятся на выдаче, поэтому правило в движке исправляет всех сразу.
"""
import pytest

from backend.german_grammar_tables import (
    build_verb_conjugation,
    split_separable_verb,
)


@pytest.mark.parametrize("verb, prefix, base", [
    ("klarkommen", "klar", "kommen"),
    ("ankommen", "an", "kommen"),
    ("aufstehen", "auf", "stehen"),
    ("zusammenarbeiten", "zusammen", "arbeiten"),
    ("weitermachen", "weiter", "machen"),
])
def test_separable_prefix_is_recognised(verb, prefix, base):
    assert split_separable_verb(verb) == (prefix, base)


@pytest.mark.parametrize("verb", [
    # Неотделяемые: приставка остаётся на месте.
    "verstehen", "bekommen", "erklären", "gehören", "entstehen",
    # Отделяемость зависит от ЗНАЧЕНИЯ, а написание одно: «übersetzen» —
    # переводить (неотделяемая) и переправлять через реку (отделяемая). Молчим.
    "übersetzen", "umfahren", "durchschauen", "unterhalten", "wiederholen",
    # Не глаголы и слишком короткие основы.
    "machen", "gehen", "sein",
])
def test_left_joined_when_not_certainly_separable(verb):
    assert split_separable_verb(verb) == ("", verb)


def test_present_tense_puts_the_prefix_at_the_end():
    table = build_verb_conjugation(
        word_de="klarkommen",
        seed={"praeteritum": "kam klar", "perfekt": "ist klargekommen"},
    )
    assert table["praesens"]["ich"] == "komme klar"
    assert table["praesens"]["du"] == "kommst klar"
    assert table["praesens"]["wir"] == "kommen klar"


def test_past_tense_is_not_glued_letter_by_letter():
    """«kam klar» разворачивалось как «kam klarst», «kam klaren» — приклеиванием
    окончаний к целой строке вместе с приставкой."""
    table = build_verb_conjugation(
        word_de="klarkommen",
        seed={"praeteritum": "kam klar", "perfekt": "ist klargekommen"},
    )
    assert table["praeteritum"]["du"] == "kamst klar"
    assert table["praeteritum"]["wir"] == "kamen klar"


def test_imperative_and_polite_form():
    table = build_verb_conjugation(word_de="aufstehen", seed={"praeteritum": "stand auf"})
    assert table["imperativ"]["du"] == "steh auf"
    assert table["imperativ"]["Sie"] == "stehen Sie auf"


def test_participle_stays_joined():
    """В Perfekt приставка НЕ отделяется: «ist klargekommen»."""
    table = build_verb_conjugation(
        word_de="klarkommen", seed={"perfekt": "ist klargekommen"})
    assert table["perfekt"]["ich"] == "bin klargekommen"
    assert table["partizip2"] == "klargekommen"


def test_inseparable_verb_is_untouched():
    table = build_verb_conjugation(word_de="verstehen", seed={"praeteritum": "verstand"})
    assert table["praesens"]["ich"] == "verstehe"
    assert table["praeteritum"]["wir"] == "verstanden"


def test_glued_seed_form_is_repaired():
    """Разбор мог прислать уже склеенную форму — её тоже разбираем."""
    table = build_verb_conjugation(
        word_de="ankommen",
        seed={"present_2sg": "ankommst", "praeteritum": "ankam"},
    )
    assert table["praesens"]["du"] == "kommst an"
    assert table["praeteritum"]["ich"] == "kam an"


def test_razbor_may_veto_the_split():
    """Разбор прямо сказал «не отделяемый» — верим ему, а не списку приставок."""
    table = build_verb_conjugation(
        word_de="umfassen", seed={"is_separable": False, "praeteritum": "umfasste"})
    assert table["praesens"]["ich"] == "umfasse"


def test_zu_infinitive_still_builds_no_table():
    assert build_verb_conjugation(word_de="klarzukommen") is None
