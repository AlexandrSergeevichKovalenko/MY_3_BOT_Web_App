"""Один перевод целиком внутри другого — человеку показываем ОДИН.

Замер 11.08.2026 (scripts/dict_defect_audit.py, п.4): у 1358 немецких слов из 4627
в списке лежит перевод, целиком сидящий внутри соседнего — «скобка» и «Скобка,
скрепка». Это не два значения, а одно плюс пересказ. У 338 слов такие пересказы
занимали все шесть мест, и настоящие другие значения не помещались.
"""
from backend.lex_units import drop_nested_translations as drop


def test_enumeration_containing_another_translation_goes_away():
    assert drop(["скобка", "зажим, клипса", "Скобка, скрепка", "зажим", "скрепка"]) == [
        "скобка", "зажим", "скрепка",
    ]


def test_glued_explanation_loses_to_the_plain_translation():
    assert drop(["нырять под воду, полностью погружаться под воду", "нырять"]) == ["нырять"]


def test_bracket_note_survives_and_the_bare_word_goes():
    """Помета в скобках человеку нужна: «деньги (разг.)» информативнее «денег»."""
    assert drop(["деньги", "пластилин", "деньги (разг.)"]) == ["пластилин", "деньги (разг.)"]


def test_trailing_dot_is_not_a_second_meaning():
    assert drop(["венчик", "венчик."]) == ["венчик"]


def test_different_meanings_are_all_kept():
    values = ["изменение", "перемена", "трансформация"]
    assert drop(values) == values


def test_enumeration_stays_whole_when_nothing_repeats_it():
    """Резать перечисления по запятой нельзя: получились бы обрывки."""
    values = ["зажим, клипса", "скрепка"]
    assert drop(values) == values


def test_case_and_spacing_do_not_make_a_new_meaning():
    assert drop(["перемена", "Перемена,  изменение", "изменение"]) == ["перемена", "изменение"]


def test_empty_list_is_survived():
    assert drop([]) == []
