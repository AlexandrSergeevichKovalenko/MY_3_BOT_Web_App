"""Род не берётся у разбора, который собран про ДРУГОЕ написание слова.

Замер 21.08.2026. Артикль в написании есть не у всех слов: у 691 он лежит колонкой
`gender`, и выдача приклеивает его к заголовку сама. Колонку заполняли из разбора, а
разбор нередко собран про множественное число — «Spritpreis» получал «die» от заголовка
«die Spritpreise». На экране выходило «die Spritpreis», «die Narr», «die Elektrogerät».

Правило: молча правится только этот класс — разбор про другое написание ПЛЮС возражение
арбитра рода. Совпало написание — разбор про это самое слово, и его артикль не
перебивается: там род зависит от значения («der Dicke» толстяк / «die Dicke» толщина),
а это решение владельца, а не правила.
"""
from backend import lex_units


class _Cursor:
    """Подставная база: одна выборка слов, потом собираем UPDATE'ы."""

    def __init__(self, rows):
        self._rows = rows
        self.updates = []

    def execute(self, sql, params=None):
        if sql.strip().upper().startswith("UPDATE"):
            self.updates.append(params)

    def fetchall(self):
        return self._rows

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _Conn:
    def __init__(self, cursor):
        self._cursor = cursor
        self.committed = False

    def cursor(self):
        return self._cursor

    def commit(self):
        self.committed = True

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def _run(monkeypatch, rows, authority):
    cursor = _Cursor(rows)
    monkeypatch.setattr(lex_units, "get_db_connection_context", lambda **kw: _Conn(cursor))
    import backend.article_authority as authority_module
    monkeypatch.setattr(authority_module, "authoritative_article",
                        lambda word, **kw: authority.get(word, (None, "нет данных")))
    return lex_units.fix_gender_conflicts_from_authority(), cursor


def test_plural_card_gender_is_corrected(monkeypatch):
    # Разбор озаглавлен множественным «die Spritpreise», слово — единственное.
    rows = [(1, "Spritpreis", "spritpreis", "die", {"word_de": "die Spritpreise"})]
    report, cursor = _run(monkeypatch, rows, {"spritpreis": ("der", "правило композита")})
    assert report["fixed"] == 1
    assert report["doubts"] == 0
    assert cursor.updates == [("der", "арбитр рода", 1)]


def test_card_about_the_same_word_is_left_to_the_owner(monkeypatch):
    # «der Dicke» толстяк против «die Dicke» толщина: разбор про ЭТО слово, и решает
    # значение, а не арбитр. Молча не правим — считаем и показываем владельцу.
    rows = [(2, "Dicke", "dicke", "der", {"word_de": "die Dicke"})]
    report, cursor = _run(monkeypatch, rows, {"dicke": ("die", "wiktionary")})
    assert report["fixed"] == 0
    assert report["doubts"] == 1
    assert cursor.updates == []
    assert report["doubt_samples"][0]["word"] == "Dicke"


def test_silent_authority_is_not_evidence(monkeypatch):
    # Арбитр не знает — значит не знаем и мы. Догадка вместо источника запрещена.
    rows = [(3, "Zwirbelding", "zwirbelding", "die", {"word_de": "die Zwirbeldinge"})]
    report, cursor = _run(monkeypatch, rows, {})
    assert report["fixed"] == 0
    assert report["doubts"] == 0
    assert cursor.updates == []


def test_agreeing_gender_is_untouched(monkeypatch):
    rows = [(4, "Hügel", "hügel", "der", {"word_de": "die Hügel"})]
    report, cursor = _run(monkeypatch, rows, {"hügel": ("der", "wiktionary")})
    assert report["fixed"] == 0
    assert cursor.updates == []


def test_card_headword_is_read_from_any_of_the_known_fields():
    assert lex_units._card_headword({"word_de": "die Narren"}) == "die Narren"
    assert lex_units._card_headword({"word_source": "Narren"}) == "Narren"
    assert lex_units._card_headword({"source_text": "Narren"}) == "Narren"
    assert lex_units._card_headword({}) == ""
    assert lex_units._card_headword(None) == ""
