"""Вычитка не переписывает слово, которое есть в выверенном словаре.

Живой случай 11.08.2026. Человек набрал «Blad» — настоящее немецкое прилагательное
«blad» (толстый). В общем пуле его не было, поэтому слово ушло в корректор, тот счёл
его опечаткой частого «Blatt», и человек получил заголовок «толстый», а под ним
разбор про лист, страницу и die Seite.

Правило, которое это чинит, у самой функции было записано в комментарии: «слово,
которое у нас уже есть, верное по определению». Проверялось оно только по ПУЛУ, а
пул — не весь наш словарь.

И вето даёт ТОЛЬКО внешний выверенный словарь, не наши единицы: в них живут наши же
прошлые ошибки и двери для опечаток («Bestürtz» ведёт на «bestürzt»). Дай им право
вето — и они объявят себя правильными навсегда.
"""
from unittest.mock import patch

from backend.backend_server import _dictionary_layer_knows


def test_a_word_from_the_curated_dictionary_is_never_corrected():
    with patch("backend.dictionary_entries.known_in_base_dictionary", return_value=True):
        assert _dictionary_layer_knows("blad", source_lang="de", target_lang="ru") is True


def test_a_word_we_do_not_have_is_left_to_the_proofreader():
    with patch("backend.dictionary_entries.known_in_base_dictionary", return_value=False):
        assert _dictionary_layer_knows("Wohnungg", source_lang="de", target_lang="ru") is False


def test_only_german_gets_the_veto():
    """Выверенный словарь у нас немецкий. Русскую сторону он не описывает, и молчать
    за неё не должен."""
    with patch("backend.dictionary_entries.known_in_base_dictionary", return_value=True) as probe:
        assert _dictionary_layer_knows("толстый", source_lang="ru", target_lang="de") is False
        probe.assert_not_called()


def test_a_silent_dictionary_never_blocks_the_proofreader():
    """Справочник не ответил — вычитка работает как раньше. Отказ базы не должен
    превращаться в «слово верное»."""
    with patch("backend.dictionary_entries.known_in_base_dictionary", side_effect=RuntimeError("база молчит")):
        assert _dictionary_layer_knows("blad", source_lang="de", target_lang="ru") is False


def test_a_phrase_with_an_article_still_goes_to_the_proofreader():
    """«das Neugeborenes» неверно АРТИКЛЕМ, а само написание «Neugeborenes» словарь
    знает. Сверять такую строку с однословным словарём — значит выбросить то
    единственное, что в ней сломано. Согласование проверяет вычитка."""
    with patch("backend.dictionary_entries.known_in_base_dictionary", return_value=True) as probe:
        assert _dictionary_layer_knows("das Neugeborenes", source_lang="de", target_lang="ru") is False
        probe.assert_not_called()


def test_links_and_numbers_never_reach_the_dictionary():
    """До базы такое не доходит вовсе — незачем ходить туда за ссылкой."""
    with patch("backend.dictionary_entries.known_in_base_dictionary", return_value=True) as probe:
        for junk in ("https://example.com", "2026", "", "   "):
            assert _dictionary_layer_knows(junk, source_lang="de", target_lang="ru") is False
        probe.assert_not_called()
