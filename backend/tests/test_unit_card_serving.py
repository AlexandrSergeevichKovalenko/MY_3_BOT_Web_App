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


def test_examples_from_the_unit_come_first():
    """ИЗМЕНЕНО 22.08.2026: примеры больше не ВЫБИРАЮТСЯ, а складываются.

    Раньше здесь стояло «побеждает список побогаче», и проверялось это длиной. На
    пересобранном слове правило дало обратный эффект: у владельца в карточке «die Hose
    anhaben» с 02.06 лежали два примера про брюки, на слове — два свежих про идиому, и
    старые побеждали, потому что их было не меньше. Значение на экране уже поменялось на
    «быть главным», а примеры остались про одежду.

    Замысел теста не изменился — человек не должен увидеть меньше, чем видел вчера. Он и
    не видит: свой пример остаётся, просто ниже свежего.
    """
    merged = merge_unit_card_for_serve(CARD, UNIT)
    shown = [item.get("de") for item in merged["usage_examples"]]
    assert shown[: len(UNIT["usage_examples"])] == [i["de"] for i in UNIT["usage_examples"]]
    assert CARD["usage_examples"][0]["de"] in shown, "личный пример пропал"


def test_personal_examples_survive_even_when_there_are_more_of_them():
    card = dict(CARD, usage_examples=[{"de": "1"}, {"de": "2"}, {"de": "3"}, {"de": "4"}])
    merged = merge_unit_card_for_serve(card, UNIT)
    shown = [item.get("de") for item in merged["usage_examples"]]
    # Свежие с общего слова сверху, все четыре своих на месте — ничего не потеряно.
    assert shown[:3] == ["a", "b", "c"]
    assert {"1", "2", "3", "4"} <= set(shown)


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
                        _fake_units([(1, "der Wandel", UNIT, "wandel", None, False)]))
    items = [{"id": 1, "response_json": dict(CARD)}, {"id": 2, "response_json": dict(CARD)}]
    database.attach_unit_content_to_cards(items)
    assert items[0]["response_json"]["memory_tip"] == UNIT["memory_tip"]
    assert "memory_tip" not in items[1]["response_json"], "у карточки без единицы ничего не меняется"


def test_batch_refuses_a_unit_about_another_word(monkeypatch):
    from backend import database
    monkeypatch.setattr(database, "get_db_connection_context",
                        _fake_units([(1, "einen Fusselrasierer benutzen", UNIT,
                                      "использовать машинку для удаления катышков", None, False)]))
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
                        _fake_units([(1, "der Wandel", UNIT, "wandel", notes, False)]))
    items = [{"id": 1, "response_json": dict(CARD)}]
    database.attach_unit_content_to_cards(items)
    assert items[0]["user_notes"] == notes
    assert "user_notes" not in items[0]["response_json"]


def test_card_without_a_unit_still_gets_its_notes(monkeypatch):
    """У слова может не быть единицы — заметка всё равно должна доехать."""
    from backend import database
    notes = [{"label": "", "text": "личное"}]
    monkeypatch.setattr(database, "get_db_connection_context",
                        _fake_units([(1, "der Wandel", None, None, notes, False)]))
    items = [{"id": 1, "response_json": dict(CARD)}]
    database.attach_unit_content_to_cards(items)
    assert items[0]["user_notes"] == notes
    assert items[0]["response_json"] == CARD


# ── Синонимы: блок, который мост тихо не переносил ──────────────────────────────
#
# Замер 08.08.2026: секции разбора 1–4 доезжали до личной карточки в 14 489 случаях
# из 24 554, а пятая — та, где лежат синонимы, — всего в 670. На самих единицах она
# была у 4 098 из 9 711. Модель их присылала, мы за них платили, они сохранялись на
# слове — и не показывались человеку, потому что в списке переносимых блоков их не
# было. Причём соседний блок «чем отличаются синонимы» там был: карточка объясняла
# разницу между словами, которых на экране нет.

UNIT_WITH_SYNONYMS = {
    "word_de": "aufstehen",
    "synonyms": ["sich erheben", "hochkommen"],
    "antonyms": ["sich hinlegen"],
    "related_words": [{"word": "der Aufstand", "gloss": "восстание"}],
    "when_to_use": "о подъёме с постели и о том, что окно стоит открытым",
}


def test_synonyms_reach_the_card():
    card = {"word_de": "aufstehen", "source_text": "aufstehen"}
    merged = merge_unit_card_for_serve(card, UNIT_WITH_SYNONYMS)
    assert merged["synonyms"] == ["sich erheben", "hochkommen"], (
        "синонимы не доехали с единицы до карточки — человек снова увидит пустоту"
    )
    assert merged["antonyms"] == ["sich hinlegen"]
    assert merged["related_words"][0]["word"] == "der Aufstand"
    assert merged["when_to_use"]


def test_own_synonyms_are_not_replaced_by_a_poorer_list():
    """Правило прежнее: дополняем, но никогда не обедняем."""
    card = {
        "word_de": "aufstehen",
        "synonyms": ["sich erheben", "hochkommen", "sich aufrichten"],
    }
    merged = merge_unit_card_for_serve(card, {"word_de": "aufstehen", "synonyms": ["x"]})
    assert merged["synonyms"] == ["sich erheben", "hochkommen", "sich aufrichten"], (
        "свой список синонимов подменён более коротким с единицы"
    )


def test_richness_counts_synonyms():
    """Карточка с синонимами считается более полной — иначе ночной добор её не заметит."""
    from backend.database import card_content_score
    without = card_content_score({"meanings": {"primary": {"value": "вставать"}}})
    with_syn = card_content_score({
        "meanings": {"primary": {"value": "вставать"}},
        "synonyms": ["sich erheben"],
    })
    assert with_syn > without, "синонимы не влияют на оценку полноты разбора"


def test_surface_index_confirms_a_form_of_the_same_word(monkeypatch):
    """«Die Strümpfe» и «Strumpf» — одно слово, просто множественное число.

    Сравнение по буквам это не признаёт, и до 14.08.2026 разбор с общего слова таким
    карточкам не показывался вовсе — 1077 штук из 24 908. Теперь решает справочник
    форм bt_3_lex_surfaces: подтвердил — показываем."""
    from backend import database
    monkeypatch.setattr(database, "get_db_connection_context",
                        _fake_units([(1, "Die Strümpfe", UNIT, "strumpf", None, True)]))
    items = [{"id": 1, "response_json": {"word_de": "Die Strümpfe"}}]
    database.attach_unit_content_to_cards(items)
    assert items[0]["response_json"].get("usage_examples"), "разбор с общего слова не приехал"


def test_without_surface_confirmation_letters_still_decide(monkeypatch):
    """Справочник промолчал — работает прежнее правило сравнения по буквам."""
    from backend import database
    monkeypatch.setattr(database, "get_db_connection_context",
                        _fake_units([(1, "Die Strümpfe", UNIT, "strumpf", None, False)]))
    items = [{"id": 1, "response_json": {"word_de": "Die Strümpfe"}}]
    database.attach_unit_content_to_cards(items)
    assert not items[0]["response_json"].get("usage_examples")


# ── быстрый словарь читает ОБЩЕЕ слово, а не только личную копию ───────────────
# Экран «Мои слова» и быстрый словарь ходят в get_webapp_dictionary_entries. До
# 15.08.2026 эта выборка отдавала личную копию как есть, и уточнение общего слова до
# человека не доходило: ночная раздача копий существовала ровно затем, чтобы это
# обойти. Тест держит дверь — если слияние из выборки уберут, он покраснеет.

def _fake_dictionary_db(card_rows, unit_rows):
    """База, отвечающая по-разному на два запроса: список карточек и разбор их слов."""
    import contextlib

    class Cursor:
        def __init__(self):
            self._rows = []

        def execute(self, sql, *_a, **_k):
            text = sql if isinstance(sql, str) else str(sql)
            self._rows = unit_rows if "bt_3_lex_units" in text else card_rows

        def fetchall(self):
            return self._rows

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


def test_quick_dictionary_shows_the_shared_word(monkeypatch):
    from backend import database

    card_row = (
        7, "перемена", "der Wandel", "der Wandel", "перемена", "de", "ru",
        "manual", None, dict(CARD), None, None,
    )
    unit_row = (7, "der Wandel", UNIT, "wandel", None, False)
    monkeypatch.setattr(database, "get_db_connection_context",
                        _fake_dictionary_db([card_row], [unit_row]))
    monkeypatch.setattr(database, "get_user_word_overrides", lambda _ids, cursor=None: {})

    items = database.get_webapp_dictionary_entries(user_id=1, limit=10)

    assert len(items) == 1
    card = items[0]["response_json"]
    assert card["memory_tip"] == UNIT["memory_tip"], "подсказка с общего слова не доехала"
    assert card["forms"] == UNIT["forms"], "формы с общего слова не доехали"
    assert card["word_de"] == "der Wandel", "заголовок остаётся личным"
