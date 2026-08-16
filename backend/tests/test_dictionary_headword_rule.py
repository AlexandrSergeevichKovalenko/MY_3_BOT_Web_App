# -*- coding: utf-8 -*-
"""Заголовок словарной статьи: одно правило, и оно не должно тихо исчезнуть.

Три класса дефекта, которые владелец нашёл на своих карточках 16.08.2026 — все три
были «починены» скриптом раньше и вернулись через живую дверь. Скрипт чинит прошлое,
тест держит будущее.
"""
import pytest

from backend.german_grammar_tables import german_dictionary_headword


@pytest.mark.parametrize("stored, expected", [
    # zu-инфинитив: словарной формы с «zu» не бывает, а от заголовка строится спряжение
    ("klarzukommen", "klarkommen"),
    ("anzulehnen", "anlehnen"),
    ("umzukrempeln", "umkrempeln"),
    ("auszulaugen", "auslaugen"),
    # неопределённый артикль у одиночного существительного
    ("eine Pleite", "Pleite"),
    ("die eine Pleite", "die Pleite"),
    ("ein Funke", "Funke"),
    ("ein 50-Euro-Schein", "50-Euro-Schein"),
])
def test_headword_is_normalised(stored, expected):
    assert german_dictionary_headword(stored) == expected


@pytest.mark.parametrize("stored", [
    # ФРАЗА: артикль принадлежит ей, снять его — испортить пример.
    "eine Pressekonferenz abhalten",
    "ein Gesetz aushebeln",
    "eine Kamera um den Hals tragen",
    # определённый артикль — это и есть наш формат заголовка
    "die Fahne",
    "das Vermögen",
    # «hinzu-» — приставка, а не zu-инфинитив
    "hinzufügen",
    "dazugeben",
    # обычные слова и не-немецкий текст трогать нечего
    "schlammig",
    "Понос",
    "",
    # мусор не превращаем в заголовок «n»
    "Einer n",
])
def test_headword_left_alone(stored):
    assert german_dictionary_headword(stored) == stored


def test_rule_runs_on_every_card_write():
    """Правило стоит в той единственной функции, через которую пишется любая карточка.

    Раньше оно жило выше по течению, в веб-слое, и колонка translation_de его обходила —
    именно её показывает крупным шрифтом экран разбора."""
    import inspect
    from backend import database

    source = inspect.getsource(database._save_webapp_dictionary_query_returning_id_with_conn)
    assert "german_dictionary_headword(word_de)" in source
    assert "german_dictionary_headword(translation_de)" in source
