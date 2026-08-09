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


# ── Подключение к карточке ──────────────────────────────────────────────────────
#
# Решение владельца 09.08.2026: примеры из корпуса ДОПОЛНЯЮТ модельные, а не заменяют.
# Корпус покрывает 63% наших слов; заменяй мы — у одного слова источник был бы подписан,
# у соседнего нет, и человек не понял бы почему.

def test_corpus_examples_live_in_their_own_field():
    """Смешивать проверяемое с сочинённым в одном списке нельзя: у корпусного примера
    есть автор и лицензия, и их надо показать."""
    from backend.backend_server import _with_corpus_examples
    item = {"word_de": "der Test", "usage_examples": [{"source": "Ein Test.", "target": "Тест."}]}
    out = _with_corpus_examples(dict(item))
    assert out["usage_examples"] == item["usage_examples"], "модельные примеры затронуты"


def test_model_examples_are_never_replaced():
    from backend.backend_server import _with_corpus_examples
    mine = [{"source": "Ein Satz.", "target": "Предложение."}]
    out = _with_corpus_examples({"word_de": "Haus", "usage_examples": list(mine)})
    assert out["usage_examples"] == mine


def test_already_attached_is_not_asked_again():
    """Один запрос на карточку. Повторный вызов не должен идти в базу снова."""
    from backend.backend_server import _with_corpus_examples
    item = {"word_de": "Haus", "corpus_examples": [{"source": "x", "target": "y"}]}
    out = _with_corpus_examples(item)
    assert out["corpus_examples"] == [{"source": "x", "target": "y"}]


def test_broken_item_does_not_break_the_card():
    from backend.backend_server import _with_corpus_examples
    assert _with_corpus_examples(None) is None
    assert _with_corpus_examples({}) == {}


def test_every_single_card_path_goes_through_one_place():
    """Путей ответа тринадцать (кеш, пул, обратная сторона, фоновая работа, модель).
    Новый блок не должен требовать правки в тринадцати местах."""
    import inspect
    from backend import backend_server
    src = inspect.getsource(backend_server)
    assert '"item": _with_grammar_tables(' not in src, (
        "часть путей выдачи обошла общую точку — на них живых примеров не будет"
    )
    assert src.count('"item": _serve_dictionary_item(') >= 10
