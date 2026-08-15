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


# ── свалки с номерами значений ────────────────────────────────────────────────
# Замер 11.08.2026 (dict_defect_audit, п.2): 15 строк с цифрами доходят до карточек
# одиночных слов. Двенадцать из них — настоящие свалки, три — живые переводы, где
# цифра часть смысла. Тесты держат обе стороны: свалки режем, живое не трогаем.

from backend.lex_units import split_numbered_senses as split


def test_leading_number_is_not_part_of_the_translation():
    assert split("1 колоть") == ["колоть"]
    assert split("2 разгадать, отгадать") == ["разгадать, отгадать"]


def test_leading_number_glued_to_the_word():
    assert split("1.приём [оформление] на работу 2. место, должность; работа") == [
        "приём [оформление] на работу", "место, должность; работа",
    ]


def test_dump_is_cut_into_separate_meanings():
    assert split("Парировать, отражать, отбивать 2 предотвращать") == [
        "Парировать, отражать, отбивать", "предотвращать",
    ]
    assert split("поймать, схватить 2. поймать, застигнуть") == [
        "поймать, схватить", "поймать, застигнуть",
    ]


def test_digit_inside_a_living_translation_is_left_alone():
    """«Меню из 5 блюд» — цифра часть смысла, а не номер значения."""
    for value in ("Меню из 5 блюд",
                  "детская группа для малышей до 3 лет",
                  "Вы могли бы отложить куртку для меня до 16 часов?"):
        assert split(value) == [value]


def test_counted_noun_after_a_leading_digit_is_not_a_sense_number():
    assert split("2 недели") == ["2 недели"]
    assert split("5 блюд") == ["5 блюд"]


def test_plain_translation_survives_untouched():
    assert split("класть трубку") == ["класть трубку"]
    assert split("") == []
