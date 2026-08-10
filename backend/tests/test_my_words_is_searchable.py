"""«Мои слова» должны искаться и догружаться, иначе это не список, а выборка.

Владелец 10.08.2026 открыл закладку в словаре с рабочего стола: сто строк, поля поиска
нет, конец списка — и всё. При пятнадцати тысячах сохранённых слов пользоваться этим
нельзя, и он справедливо спросил, как.

Во внутреннем словаре поиск был давно; в отдельном — нет, хотя интерфейс мы
договорились держать одинаковым.
"""
import inspect

from backend import database


def test_entries_query_supports_search_and_offset():
    sig = inspect.signature(database.get_webapp_dictionary_entries)
    assert "search" in sig.parameters, "выдача не умеет искать — закладка останется бесполезной"
    assert "offset" in sig.parameters, "без смещения список упрётся в первую сотню"


def _search_block() -> str:
    src = inspect.getsource(database.get_webapp_dictionary_entries)
    start = src.index('needle = str(')
    return src[start:start + 600]


def test_search_looks_at_both_sides_of_the_pair():
    """Человек помнит слово то по-немецки, то по-русски. Заставлять его угадывать
    сторону — значит заставлять искать дважды."""
    block = _search_block()
    for column in ("word_de", "word_ru", "translation_de", "translation_ru"):
        assert f"{column} ILIKE" in block, f"поиск не смотрит в {column}"


def test_search_is_not_glued_into_sql():
    """Строку поиска подставляем параметром, а не склейкой: иначе кавычка в запросе
    человека ломает выдачу, а то и больше."""
    block = _search_block()
    assert block.count("ILIKE %s") == 4, "поиск идёт не через параметры"
    assert "params.extend" in block, "значение не передаётся параметром"


def test_endpoint_passes_search_through():
    from backend import backend_server
    src = inspect.getsource(backend_server)
    start = src.index('@app.route("/api/webapp/dictionary/cards"')
    body = src[start:src.index("@app.route(", start + 20)]
    assert "search=search" in body, "эндпоинт не передаёт поиск в выдачу"
    assert "offset=" in body, "эндпоинт не передаёт смещение"
