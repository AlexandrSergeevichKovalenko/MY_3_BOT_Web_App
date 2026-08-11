"""Кэш словаря обязан уметь признать себя неправым.

11.08.2026. Человек набрал «Blad» — настоящее прилагательное «blad» (толстый).
Вычитка приняла его за опечатку частого «Blatt», и разбор про ЛИСТ лёг в ОБЩИЙ кэш
под ключом «blad». Саму вычитку починили в тот же день, но отравленная запись
продолжала раздаваться всем, кто спросит это слово: общий ключ живёт годами.

А на экране это выглядело так: страховка карточку отбрасывала, ошибку никто не
показывал, и кнопка «Подробный разбор» просто не давала ничего.

Проверка узкая намеренно: спросили ОДНО немецкое слово, и оно есть в выверенном
словаре — значит заголовок карточки должен быть им же. Словоформы и фразы не
трогаем: «wuchsen» законно ведёт на «wachsen».
"""
from unittest.mock import patch

from backend.backend_server import _cached_card_is_about


def card(word_de):
    return {"item": {"word_de": word_de}}


def test_a_card_about_another_word_is_refused():
    with patch("backend.dictionary_entries.known_in_base_dictionary", return_value=True):
        assert _cached_card_is_about(card("Blatt"), "blad", "de") is False


def test_a_card_about_the_asked_word_passes():
    with patch("backend.dictionary_entries.known_in_base_dictionary", return_value=True):
        assert _cached_card_is_about(card("blad"), "blad", "de") is True


def test_the_article_does_not_count_as_a_difference():
    with patch("backend.dictionary_entries.known_in_base_dictionary", return_value=True):
        assert _cached_card_is_about(card("die Kiefer"), "Kiefer", "de") is True
        assert _cached_card_is_about(card("Kiefer"), "die Kiefer", "de") is True


def test_case_does_not_count_as_a_difference():
    with patch("backend.dictionary_entries.known_in_base_dictionary", return_value=True):
        assert _cached_card_is_about(card("Blad"), "blad", "de") is True


def test_a_word_we_do_not_know_is_never_judged():
    """Незнакомое слово мы описывать не можем, значит и спорить с карточкой не вправе:
    «wuchsen» законно ведёт на «wachsen», и объявить это подменой было бы хуже болезни."""
    with patch("backend.dictionary_entries.known_in_base_dictionary", return_value=False):
        assert _cached_card_is_about(card("wachsen"), "wuchsen", "de") is True


def test_only_german_lookups_are_judged():
    with patch("backend.dictionary_entries.known_in_base_dictionary", return_value=True) as probe:
        assert _cached_card_is_about(card("Blatt"), "толстый", "ru") is True
        probe.assert_not_called()


def test_a_card_without_a_german_headword_is_left_alone():
    with patch("backend.dictionary_entries.known_in_base_dictionary", return_value=True):
        assert _cached_card_is_about(card(""), "blad", "de") is True
        assert _cached_card_is_about({"item": None}, "blad", "de") is True
        assert _cached_card_is_about(None, "blad", "de") is True
