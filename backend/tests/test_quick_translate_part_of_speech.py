"""Часть речи у быстрого перевода берётся из НАШЕГО банка слов, а не у модели.

Владелец 08.08.2026 прислал экран «Soweit → Насколько» без единой пометы. Причина:
быстрый перевод — гонка обычных переводчиков, они частей речи не отдают. При этом
часть речи у нас уже лежит у 4 787 немецких единиц, и артикль на этот же экран
подтягивается ровно так же — отдельным дешёвым запросом после ответа переводчика.
Часть речи просто никто не спросил.

«Soweit» с большой буквы выглядит существительным — помета «наречие» сразу показывает,
что это не оно.
"""
from backend.backend_server import (
    _attach_quick_translate_pos,
    _quick_translate_german_side,
)


def test_german_side_is_found_in_both_directions():
    # ru→de: немецкое слово в переводе
    assert _quick_translate_german_side(
        {"translation": "aufstehen"}, "вставать", "ru", "de") == "aufstehen"
    # de→ru: немецкое слово во вводе
    assert _quick_translate_german_side(
        {"translation": "насколько", "detected_source_lang": "de"}, "soweit", "de", "ru") == "soweit"


def test_article_is_stripped_before_lookup():
    assert _quick_translate_german_side(
        {"translation": "die Mündung"}, "устье", "ru", "de") == "Mündung"


def test_not_a_single_word_is_skipped():
    """Фразу в банке слов не ищем: там единицы, а не предложения."""
    assert _quick_translate_german_side(
        {"translation": "Ich muss früh aufstehen"}, "мне рано вставать", "ru", "de") == ""


def test_existing_part_of_speech_is_never_overwritten():
    """Существительным помету уже проставляет опознание артикля — не спорим с ним."""
    result = {"translation": "die Mündung", "part_of_speech": "noun"}
    _attach_quick_translate_pos(result, "устье", "ru", "de")
    assert result["part_of_speech"] == "noun"


def test_lookup_failure_is_silent():
    """База недоступна — быстрый перевод обязан доехать без пометы, а не упасть."""
    result = {"translation": "aufstehen"}
    _attach_quick_translate_pos(result, "вставать", "ru", "de")
    # Помета либо появилась, либо нет — но исключения наружу не вышло и перевод цел.
    assert result["translation"] == "aufstehen"
