"""Слово обязано быть написано своим алфавитом — и это проверяет БАЗА, а не код.

Откуда взялась беда. Русский текст, заведённый как немецкая единица («Ты ужасно
воняешь.» вместо «Du stinkst furchtbar.»), — не опечатка, а перепутанные стороны.
Карточка после этого указывает на чужое слово, разбор к ней не приезжает, поиск её не
находит. Замер 05.08.2026: 124 немецких единицы с русским текстом и 350 русских с
немецким.

Почему код это пропустил. Проверки жили на ОДНОМ пути — там, где слово сохраняет
человек. Массовая сборка 27 июля и разовые прогоны (включая мои собственные) писали в
таблицу напрямую, мимо этих проверок. Каждый новый скрипт заводит свои проверки или не
заводит никаких — так дырка открывается заново с каждой задачей.

Поэтому правило переехало в саму базу: ограничение `chk_lex_units_script_matches_lang`
на таблице единиц. Мимо него не пройдёт ни скрипт, ни ветка, ни будущий агент. Функция
ниже — то же правило в коде: чтобы отказать понятно и заранее, а не ловить ошибку записи.
"""
import re
import pathlib

from backend.lex_units import text_matches_language


def test_german_word_must_have_latin_letters():
    assert text_matches_language("Du stinkst furchtbar.", "de")
    assert text_matches_language("der Rüpel", "de")
    assert not text_matches_language("Ты ужасно воняешь.", "de")
    assert not text_matches_language("присоединить зарядку к розетке", "de")


def test_russian_word_must_have_cyrillic():
    assert text_matches_language("мочалка для умывания", "ru")
    assert not text_matches_language("einen Fusselrasierer benutzen", "ru")
    assert not text_matches_language("anlegen", "ru")


def test_mixed_text_passes_for_both():
    """«налог на выбросы CO2» и «die CO2 Abgabe» — живые записи, ломать их нельзя."""
    assert text_matches_language("налог на выбросы CO2", "ru")
    assert text_matches_language("die CO2 Abgabe", "de")


def test_unknown_language_is_not_our_business():
    assert text_matches_language("gobsmacked", "en")
    assert text_matches_language("qualsiasi", "it")


def test_empty_is_refused():
    for value in (None, "", "   "):
        assert not text_matches_language(value, "de"), value


def test_the_database_carries_the_same_rule():
    """Главная защита — в схеме. Если ограничение уйдёт из файла, скрипты снова смогут
    писать перепутанные стороны, и мы вернёмся к чистке задним числом."""
    schema = pathlib.Path(__file__).resolve().parents[1] / "lex_units_schema.sql"
    text = schema.read_text(encoding="utf-8")
    assert "chk_lex_units_script_matches_lang" in text, "запрет пропал из схемы"
    assert re.search(r"lang\s*<>\s*'de'\s*OR\s*display\s*~", text), "правило для немецкого потерялось"
    assert re.search(r"lang\s*<>\s*'ru'\s*OR\s*display\s*~", text), "правило для русского потерялось"
