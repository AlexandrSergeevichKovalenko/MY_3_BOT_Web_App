# -*- coding: utf-8 -*-
"""Кроссворд загадывает слова из живой речи, а не выдуманные.

Разбор банка 31.07 (72 кроссворда): в 31 % стояло слово, которого в немецком нет
(TUGENDHAFTIG, DEONTOLIGIE, MIVVERKEHR, обрубок VERSUCHSAN), и только 8 % слов
входили в первые 2000 самых частых. Проверка тут сторожит обе границы.
"""
import pytest

from backend.answer_eval import _summarize_crossword
from backend.crossword_generator import _accept_word_entry, _select_hidden_words
from backend.crossword_word_gate import check_word, is_everyday, normalize_word


NONSENSE = [
    "TUGENDHAFTIG",   # есть tugendhaft, такого слова нет
    "DEONTOLIGIE",    # опечатка
    "ETIMOLOGIE",     # опечатка (Etymologie)
    "AMPLELICHT",
    "MIVVERKEHR",
    "TIERWOEHNDE",
    "VERSUCHSAN",     # обрубок
    "VEROEFFENT",     # обрубок
    "RECHNUNGSEN",    # обрубок
    "ILLUSTRAT",      # обрубок
    "PARKPLATZES",    # родительный падеж
    "NIECE",          # английское
    "BAGGAGE",        # английское
    "EPIKURATION",
    "SYMBOLIKERIN",
]

EVERYDAY = [
    "KÜHLSCHRANK", "BAHNHOF", "RECHNUNG", "SCHLÜSSEL", "FRÜHSTÜCK",
    "REGENSCHIRM", "HALTESTELLE", "ZAHNBÜRSTE", "NACHBARIN", "FÜHRERSCHEIN",
]


@pytest.mark.parametrize("word", NONSENSE)
def test_nonsense_words_never_reach_a_puzzle(word):
    ok, reason = check_word(normalize_word(word))
    assert not ok, f"{word} прошло приёмку"
    assert reason


@pytest.mark.parametrize("word", EVERYDAY)
def test_everyday_words_pass(word):
    normalized = normalize_word(word)
    ok, reason = check_word(normalized)
    assert ok, f"{word} отклонено: {reason}"


def test_spelling_is_repaired_from_the_dictionary():
    # Модель пишет умляуты то транслитом, то теряет их вовсе — в сетку должно лечь
    # написание, которое подтверждает словарь.
    assert normalize_word("REISEGEPAECK") == "REISEGEPÄCK"
    assert normalize_word("KUNSTLER") == "KÜNSTLER"
    assert normalize_word("ARCHAEOLOGIE") == "ARCHÄOLOGIE"
    # Сетка заглавная, а заглавная ß в немецком записывается как SS.
    assert normalize_word("Straßenbahn") == "STRASSENBAHN"
    assert normalize_word("Fußgänger") == "FUSSGÄNGER"


def test_accepted_entry_carries_a_translation_not_a_clue():
    entry, reason = _accept_word_entry({
        "word": "kuehlschrank",
        "clue_de": "Hier bleiben Milch und Butter kalt",
        "clue_ru": "Здесь молоко и масло остаются холодными",
        "translation_ru": "холодильник",
    })
    assert entry, reason
    assert entry["word"] == "KÜHLSCHRANK"
    assert entry["translation_ru"] == "холодильник"


def test_entry_without_clues_is_refused():
    entry, reason = _accept_word_entry({"word": "KÜHLSCHRANK", "clue_de": "", "clue_ru": ""})
    assert entry is None and reason


def test_hidden_words_are_the_ones_people_actually_say():
    # Сетка: ходовое слово и редкое пересекаются. Набирать руками просим ходовое.
    words = [
        {"word": "MIETE", "direction": "across", "row": 0, "col": 0, "number": 1},
        {"word": "MUSEUMSAMT", "direction": "down", "row": 0, "col": 0, "number": 2},
        {"word": "TERMIN", "direction": "down", "row": 0, "col": 4, "number": 3},
        {"word": "KAUTION", "direction": "across", "row": 4, "col": 0, "number": 4},
    ]
    result = _select_hidden_words(words, hidden_count=2)
    hidden = [w["word"] for w in result if w["hidden"]]
    assert len(hidden) == 2
    for word in hidden:
        assert is_everyday(word), f"загадали неходовое слово {word}"


def test_saved_word_gets_the_translation_and_falls_back_to_the_clue():
    summary = _summarize_crossword([
        {"number": 1, "direction": "across", "correct": "KÜHLSCHRANK", "user_answer": "",
         "is_correct": False, "clue_de": "", "clue_ru": "Здесь продукты остаются холодными",
         "translation_ru": "холодильник"},
        {"number": 2, "direction": "down", "correct": "MIETE", "user_answer": "",
         "is_correct": False, "clue_de": "", "clue_ru": "Плата за квартиру каждый месяц",
         "translation_ru": ""},
    ], already_answered=False)
    targets = {item["source"]: item["target"] for item in summary["saveable_words"]}
    assert targets["KÜHLSCHRANK"] == "холодильник"
    # У старых кроссвордов перевода в банке нет — подсказка остаётся запасным вариантом.
    assert targets["MIETE"] == "Плата за квартиру каждый месяц"
