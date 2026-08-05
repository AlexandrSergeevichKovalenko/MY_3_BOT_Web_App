"""Разбор с общей единицы ДОПОЛНЯЕТ личную карточку, но никогда её не обедняет.

Правило одно: человек не может увидеть меньше, чем видел вчера. Поэтому с единицы
берётся блок, только когда своего блока нет или он беднее. Замена целиком была бы
проще, но у 0.9% карточек указатель ведёт на соседнее слово (мусорные единицы старой
массовой сборки) — и замена показала бы разбор чужого слова. Такой случай отсекает
сверка заголовка.
"""
from backend.database import (
    merge_unit_card_for_serve,
    unit_card_is_about_the_same_word,
)

CARD = {
    "word_de": "der Wandel",
    "source_text": "der Wandel",
    "usage_examples": [{"de": "Der Wandel kommt."}],
    "meanings": {},
}

UNIT = {
    "word_de": "der Wandel",
    "usage_examples": [{"de": "a"}, {"de": "b"}, {"de": "c"}],
    "meanings": {"primary": {"value": "перемена"}},
    "memory_tip": "как «вандал», только про перемены",
    "forms": {"plural": "die Wandel"},
}


def test_missing_blocks_are_taken_from_the_unit():
    merged = merge_unit_card_for_serve(CARD, UNIT)
    assert merged["meanings"] == UNIT["meanings"]
    assert merged["memory_tip"] == UNIT["memory_tip"]
    assert merged["forms"] == UNIT["forms"]


def test_richer_block_wins_poorer_one():
    merged = merge_unit_card_for_serve(CARD, UNIT)
    assert len(merged["usage_examples"]) == 3


def test_personal_block_survives_when_it_is_richer():
    card = dict(CARD, usage_examples=[{"de": "1"}, {"de": "2"}, {"de": "3"}, {"de": "4"}])
    merged = merge_unit_card_for_serve(card, UNIT)
    assert len(merged["usage_examples"]) == 4


def test_direction_and_headword_stay_from_the_card():
    """Направление и заголовок задаёт карточка человека — единица их не переписывает."""
    card = dict(CARD, source_text="Wandel", target_text="перемена", language_pair={"code": "de-ru"})
    merged = merge_unit_card_for_serve(card, dict(UNIT, source_text="ЧУЖОЕ", language_pair={"code": "ru-de"}))
    assert merged["source_text"] == "Wandel"
    assert merged["target_text"] == "перемена"
    assert merged["language_pair"] == {"code": "de-ru"}


def test_empty_unit_changes_nothing():
    for value in (None, {}, "разбор", []):
        assert merge_unit_card_for_serve(CARD, value) == CARD


def test_empty_card_is_filled_from_the_unit():
    merged = merge_unit_card_for_serve({}, UNIT)
    assert merged["memory_tip"] == UNIT["memory_tip"]
    assert len(merged["usage_examples"]) == 3


def test_nothing_new_means_the_card_object_is_returned_as_is():
    assert merge_unit_card_for_serve(CARD, {"word_de": "der Wandel"}) == CARD


# ── сверка заголовка ──────────────────────────────────────────────────────────

def test_same_word_passes_with_and_without_article():
    assert unit_card_is_about_the_same_word(unit_lemma_key="wandel", card_word="der Wandel")
    assert unit_card_is_about_the_same_word(unit_lemma_key="wandel", card_word="Wandel")
    assert unit_card_is_about_the_same_word(unit_lemma_key="der wandel", card_word="Wandel")


def test_foreign_word_is_refused():
    """Живой случай: карточка «einen Fusselrasierer benutzen» указывает на единицу
    «использовать машинку для удаления катышков». Чужой разбор показывать нельзя."""
    assert not unit_card_is_about_the_same_word(
        unit_lemma_key="использовать машинку для удаления катышков",
        card_word="einen Fusselrasierer benutzen",
    )
    assert not unit_card_is_about_the_same_word(unit_lemma_key="laueren", card_word="Lauern")


def test_missing_sides_are_refused():
    assert not unit_card_is_about_the_same_word(unit_lemma_key="wandel", card_word=None)
    assert not unit_card_is_about_the_same_word(unit_lemma_key=None, card_word="Wandel")
    assert not unit_card_is_about_the_same_word(unit_lemma_key="", card_word="")


# ── пакетная надстройка для тренажёров и повторений ───────────────────────────

def _fake_units(rows):
    """База, отдающая связку карточка → разбор единицы → личные заметки."""
    import contextlib

    class Cursor:
        def execute(self, *_a, **_k):
            pass

        def fetchall(self):
            return rows

        def __enter__(self):
            return self

        def __exit__(self, *_a):
            return False

    class Conn:
        def cursor(self):
            return Cursor()

        def commit(self):
            pass

    @contextlib.contextmanager
    def ctx():
        yield Conn()

    return ctx


def test_batch_fills_cards_from_their_units(monkeypatch):
    from backend import database
    monkeypatch.setattr(database, "get_db_connection_context",
                        _fake_units([(1, "der Wandel", UNIT, "wandel", None)]))
    items = [{"id": 1, "response_json": dict(CARD)}, {"id": 2, "response_json": dict(CARD)}]
    database.attach_unit_content_to_cards(items)
    assert items[0]["response_json"]["memory_tip"] == UNIT["memory_tip"]
    assert "memory_tip" not in items[1]["response_json"], "у карточки без единицы ничего не меняется"


def test_batch_refuses_a_unit_about_another_word(monkeypatch):
    from backend import database
    monkeypatch.setattr(database, "get_db_connection_context",
                        _fake_units([(1, "einen Fusselrasierer benutzen", UNIT,
                                      "использовать машинку для удаления катышков", None)]))
    items = [{"id": 1, "response_json": dict(CARD)}]
    database.attach_unit_content_to_cards(items)
    assert items[0]["response_json"] == CARD


def test_batch_survives_junk_input(monkeypatch):
    from backend import database
    monkeypatch.setattr(database, "get_db_connection_context", _fake_units([]))
    assert database.attach_unit_content_to_cards([]) == []
    assert database.attach_unit_content_to_cards(None) is None
    weird = [{"id": None}, "строка", {"нет ключа": 1}]
    assert database.attach_unit_content_to_cards(weird) is weird


def test_batch_brings_personal_notes_along(monkeypatch):
    """Заметки едут той же строкой, что и разбор, — но ложатся ОТДЕЛЬНЫМ полем.
    Внутрь разбора им нельзя: разбор общий и обновляется, заметка личная и нет."""
    from backend import database
    notes = [{"label": "Моё", "text": "не путать с wandern"}]
    monkeypatch.setattr(database, "get_db_connection_context",
                        _fake_units([(1, "der Wandel", UNIT, "wandel", notes)]))
    items = [{"id": 1, "response_json": dict(CARD)}]
    database.attach_unit_content_to_cards(items)
    assert items[0]["user_notes"] == notes
    assert "user_notes" not in items[0]["response_json"]


def test_card_without_a_unit_still_gets_its_notes(monkeypatch):
    """У слова может не быть единицы — заметка всё равно должна доехать."""
    from backend import database
    notes = [{"label": "", "text": "личное"}]
    monkeypatch.setattr(database, "get_db_connection_context",
                        _fake_units([(1, "der Wandel", None, None, notes)]))
    items = [{"id": 1, "response_json": dict(CARD)}]
    database.attach_unit_content_to_cards(items)
    assert items[0]["user_notes"] == notes
    assert items[0]["response_json"] == CARD
