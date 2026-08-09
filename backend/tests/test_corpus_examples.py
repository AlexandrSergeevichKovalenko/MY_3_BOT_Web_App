"""Живые примеры из корпуса: отбор и границы длины.

Владелец с самого начала сомневался, что у Яндекса примеры от модели — «они как будто
из книжки». Он был прав: это корпуса. Замер 09.08.2026 перед постройкой: в Tatoeba
225 027 готовых пар «немецкое ↔ русское», и для 64% наших немецких слов пример
находится (считано немецким стеммером самого Postgres — тем же, которым идёт поиск).
Оставшейся трети пример по-прежнему даёт модель, и это не сбой.
"""
from backend.corpus_examples import (
    CORPUS_SCHEMA_SQL,
    MAX_EXAMPLE_CHARS,
    MIN_EXAMPLE_CHARS,
    examples_for_word,
)


def test_empty_word_asks_nothing():
    """Пустой запрос не должен идти в базу вовсе."""
    assert examples_for_word("") == []
    assert examples_for_word("   ") == []
    assert examples_for_word(None) == []


def test_length_window_is_sane():
    """Слишком короткое («Ja.») ничему не учит, слишком длинное не читают с телефона."""
    assert MIN_EXAMPLE_CHARS >= 8
    assert MAX_EXAMPLE_CHARS <= 160
    assert MIN_EXAMPLE_CHARS < MAX_EXAMPLE_CHARS


def test_search_uses_german_dictionary():
    """Поиск обязан идти немецким словарём Postgres.

    Без него «erschöpfen» не найдёт предложение с «erschöpft», и треть попаданий
    теряется: замер дал 53% простым совпадением против 64% со стеммером."""
    import inspect
    from backend import corpus_examples
    src = inspect.getsource(corpus_examples.examples_for_word)
    assert "to_tsvector('german'" in src, "поиск идёт без немецкого словаря"
    assert "plainto_tsquery('german'" in src, "запрос строится без немецкого словаря"


def test_index_matches_the_query():
    """Индекс обязан быть построен ТЕМ ЖЕ выражением, что и поиск, иначе он не
    применится и запрос пойдёт перебором по всей таблице."""
    assert "GIN (to_tsvector('german', text_de))" in CORPUS_SCHEMA_SQL


def test_repeat_import_updates_instead_of_duplicating():
    assert "UNIQUE (source, source_id)" in CORPUS_SCHEMA_SQL


def test_license_is_carried_with_the_sentence():
    """CC BY 2.0 FR требует указания источника — значит автор обязан храниться рядом."""
    assert "author" in CORPUS_SCHEMA_SQL
    assert "license" in CORPUS_SCHEMA_SQL
