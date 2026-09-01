# -*- coding: utf-8 -*-
"""Словоформа принадлежит СВОЕМУ глаголу, а не соседу по таблице.

Владелец 01.09.2026 нажал слово в фильме и увидел в строке «ПЕРЕВОД» слово «рыется»,
которого в русском языке нет. Разбор вывел на два дефекта, и второй был наш собственный:

    ging  → ausgehen  («выходить»), базового «gehen» в ответе не было вовсе
    gräbt → untergraben («подрывать») вместо graben («копать»)
    auf   → 34 глагола сразу, хотя это предлог, а не форма

Причина одна: ячейка парадигмы резалась на слова. У отделяемого глагола напечатано
«ging aus» — в указатели уезжали оба куска. На живой базе так натекло 5872 указателя,
из них 1061 доказанно чужих.

Здесь заперты ОБА правила, которыми это чинится. Оба читают источник — таблицу
Flexion с de.wiktionary.org — и ничего не достраивают сами.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from backend.german_grammar_tables import form_token_of_cell  # noqa: E402
from backend.german_verb_paradigms import whole_cell_forms     # noqa: E402

# Так таблица «ausgehen» напечатана в справочнике на самом деле (выборка 01.09.2026).
AUSGEHEN = {
    "praesens": {"ich": "gehe aus", "du": "gehst aus", "er/sie/es": "geht aus",
                 "wir": "gehen aus", "ihr": "geht aus", "sie/Sie": "gehen aus"},
    "praeteritum": {"ich": "ging aus", "du": "gingst aus", "er/sie/es": "ging aus",
                    "wir": "gingen aus", "ihr": "gingt aus", "sie/Sie": "gingen aus"},
    "perfekt": {"ich": "bin ausgegangen", "du": "bist ausgegangen",
                "er/sie/es": "ist ausgegangen"},
    "imperativ": {"du": "geh aus", "ihr": "geht aus"},
    "partizip2": "ausgegangen",
    "auxiliary": "sein",
}


def test_голая_основа_не_форма_приставочного_глагола():
    """«ging» — форма «gehen», а не «ausgehen». В таблице ausgehen стоит «ging aus»."""
    cells = whole_cell_forms(AUSGEHEN)
    assert "ging aus" in cells
    assert "ging" not in cells
    assert "geht" not in cells


def test_отделяемая_приставка_не_форма():
    """«aus» — приставка. Формой глагола она не является ни в одной ячейке."""
    assert "aus" not in whole_cell_forms(AUSGEHEN)


def test_причастие_остаётся_формой():
    """«ausgegangen» напечатано отдельной ячейкой — это настоящая форма, её не теряем."""
    cells = whole_cell_forms(AUSGEHEN)
    assert "ausgegangen" in cells
    assert "ist ausgegangen" in cells


def test_вспомогательный_глагол_не_форма():
    """Из «ist ausgegangen» нельзя вынуть «ist»: это связка, а не форма глагола."""
    assert "ist" not in whole_cell_forms(AUSGEHEN)


def test_ячейка_с_приставкой_не_даёт_словоформы():
    """Форма разнесена по словам — брать из неё отдельное слово НЕЛЬЗЯ."""
    assert form_token_of_cell("ging aus") == ""
    assert form_token_of_cell("geht aus") == ""
    assert form_token_of_cell("gehen aus") == ""


def test_служебное_слово_отбрасывается_а_форма_остаётся():
    """Артикль и вспомогательный глагол — часть ячейки, но не часть формы."""
    assert form_token_of_cell("des Studenten") == "studenten"
    assert form_token_of_cell("hat angefangen") == "angefangen"
    assert form_token_of_cell("ist ausgegangen") == "ausgegangen"
    assert form_token_of_cell("am ältesten") == "ältesten"


def test_одиночная_ячейка_остаётся_как_есть():
    assert form_token_of_cell("angefangen") == "angefangen"
    assert form_token_of_cell("  Häuser ") == "häuser"


def test_пустая_и_прочерк_не_формы():
    """Прочерк в таблице значит «формы нет», а не «форма — тире»."""
    assert form_token_of_cell("") == ""
    assert form_token_of_cell("—") == ""
    assert "—" not in whole_cell_forms({"praesens": {"ich": "—"}})


def test_служебные_ключи_таблицы_не_считаются_формами():
    """«sein» в поле auxiliary — это пометка о вспомогательном глаголе, а не форма."""
    cells = whole_cell_forms(AUSGEHEN)
    assert "sein" not in cells
