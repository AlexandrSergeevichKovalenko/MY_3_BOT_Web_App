"""Одно слово — один ключ, независимо от регистра.

Первое слово в строке человек пишет с большой буквы. Для нас это был ДРУГОЙ запрос:
свой ключ кеша, свой поход к переводчику, свой ответ и своя запись в общем пуле.
Замер живой базы 11.08.2026: 35 слов лежали в пуле парами «Слово»/«слово» с разными
переводами, среди них «толстый» → dick и «Толстый» → die Dicke.

У фраз регистр не трогаем: там заглавная буква может стоять за именем собственным.
"""
from backend.backend_server import _build_quick_translate_cache_key


def key(text, source="ru", target="de"):
    return _build_quick_translate_cache_key(text=text, source_lang=source, target_lang=target)


def test_capital_letter_does_not_make_it_another_word():
    assert key("Толстый") == key("толстый")
    assert key("Haus", source="de", target="ru") == key("haus", source="de", target="ru")


def test_different_words_still_have_different_keys():
    assert key("толстый") != key("тонкий")


def test_language_pair_still_separates_keys():
    assert key("Haus", source="de", target="ru") != key("Haus", source="ru", target="de")


def test_phrases_keep_their_case():
    """«Мой друг Толстый» и «мой друг толстый» — не одно и то же: в первом имя."""
    assert key("Мой друг Толстый") != key("мой друг толстый")
