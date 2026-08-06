"""Одна дверь для входящего текста: механическая чистка.

Замер живой базы 06.08.2026 — что реально пролезло с входа и осело в словаре:
невидимые знаки (15), переводы строк внутри слова (19), типографские кавычки (43),
нумерация из списка в начале (125), знак препинания в начале (51), двойная точка в
конце (74), незакрытая скобка (3). Каждый случай — слово, которое не находится по
своему же написанию: для базы это другая строка.

Отдельно — буквы-двойники: строка выглядит немецкой, а внутри русская «а». Таких
смешанных строк 254 в карточках и 303 в единицах, но БОЛЬШИНСТВО из них законные
(«налог на выбросы CO2»), поэтому правило работает по слову, а не по строке.
"""
from backend.dictionary_intake import clean_text, repair_homoglyphs, worth_language_check


def test_invisible_characters_are_removed():
    """Слово с нулевой шириной внутри выглядит нормальным и не находится никогда."""
    assert clean_text("Haus" + chr(0x200B)) == "Haus"
    assert clean_text(chr(0xFEFF) + "Wetter") == "Wetter"
    assert clean_text("Fahr" + chr(0x00AD) + "rad") == "Fahrrad"


def test_odd_spaces_become_ordinary_ones():
    assert clean_text("der" + chr(0x00A0) + "Hund") == "der Hund"
    assert clean_text("die\nGefahr") == "die Gefahr"
    assert clean_text("das\tHaus") == "das Haus"
    assert clean_text("zu   viel") == "zu viel"


def test_list_leftovers_are_stripped():
    assert clean_text("1. das Haus") == "das Haus"
    assert clean_text("• der Hund") == "der Hund"
    assert clean_text("— die Katze") == "die Katze"
    assert clean_text("2) aufstehen") == "aufstehen"


def test_dangling_tail_goes_but_sentence_end_stays():
    assert clean_text("Hallo meine Lieben —") == "Hallo meine Lieben"
    assert clean_text("der Hund:") == "der Hund"
    assert clean_text("Wie geht es dir?") == "Wie geht es dir?"
    assert clean_text("Das ist gut.") == "Das ist gut."


def test_wrapping_quotes_are_removed_inner_ones_stay():
    assert clean_text("«das Haus»") == "das Haus"
    assert clean_text("„Guten Tag“") == "Guten Tag"
    assert clean_text('das sogenannte "Haus" hier') == 'das sogenannte "Haus" hier'


def test_content_that_only_looks_like_junk_survives():
    """Три ловушки, которые холостой прогон по живой базе поймал 06.08.2026 —
    чистка портила содержание, приняв его за мусор копипаста."""
    # окончания множественного числа: тире здесь — содержание, а не маркер списка
    assert clean_text("-e") == "-e"
    assert clean_text("-n") == "-n"
    assert clean_text("-en") == "-en"
    # перечисление значений: первая цифра — часть содержания, а не нумерация из списка
    assert clean_text("1) белка; 2) круассан, рогалик") == "1) белка; 2) круассан, рогалик"
    # обрезанный копипаст оставляем как есть: обрезать хвост = потерять содержание
    assert clean_text("партнёр (в паре, романтических отношениях") == "партнёр (в паре, романтических отношениях"
    # дата: после нумерации обязана идти буква, иначе съедается первое число
    assert clean_text("10. 01. 24 товар прибыл на наш склад") == "10. 01. 24 товар прибыл на наш склад"


def test_ellipsis_and_space_before_punctuation():
    assert clean_text("Was …") == "Was ..."
    assert clean_text("Wirklich ?") == "Wirklich?"


def test_homoglyph_letters_come_home():
    """«Hаus» с русской «а» выглядит немецким словом и не находится никогда."""
    broken = "H" + chr(0x0430) + "us"          # русская «а» в немецком слове
    assert broken != "Haus"
    assert clean_text(broken) == "Haus"
    assert clean_text("до" + chr(0x006D)) == "дом"   # латинская «m» в русском слове
    assert repair_homoglyphs("H" + chr(0x0430) + "us und д" + chr(0x043E) + "m") == "Haus und дом"


def test_legitimately_mixed_text_is_left_alone():
    """Эти строки живут в базе законно — ломать их нельзя."""
    assert clean_text("налог на выбросы CO2") == "налог на выбросы CO2"
    assert clean_text("die CO2 Abgabe") == "die CO2 Abgabe"
    assert clean_text("Auto-Ремонт") == "Auto-Ремонт"


def test_cleaning_is_idempotent():
    """Дверь стоит на дне записи, текст проходит её не один раз."""
    for raw in ("1. «das Haus» —", "der" + chr(0x00A0) + "Hund", "Was …", "H" + chr(0x0430) + "us"):
        once = clean_text(raw)
        assert clean_text(once) == once, raw


def test_ordinary_text_is_untouched():
    for raw in ("das Haus", "sich freuen auf + Akkusativ", "Wie geht es dir?",
                "auf jeden Fall", "новорождённый", "Das ist nicht mein Bier."):
        assert clean_text(raw) == raw, raw


def test_empty_input_never_explodes():
    for raw in (None, "", "   ", chr(0x200B), "\n\t"):
        assert clean_text(raw) == ""


def test_language_check_is_skipped_where_it_would_be_wasted():
    assert worth_language_check("das Neugeborenes", "de")
    assert worth_language_check("новорождённый", "ru")
    assert not worth_language_check("", "de")
    assert not worth_language_check("123", "de")
    assert not worth_language_check("https://example.com/x", "de")
    assert not worth_language_check("mail@example.com", "de")
    assert not worth_language_check("house", "en"), "модель сторожит только de/ru"
    assert not worth_language_check("x" * 250, "de"), "длинный текст — не словарное слово"
