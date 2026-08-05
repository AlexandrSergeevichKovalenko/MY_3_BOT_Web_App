"""Спросили по-русски — разбор берём с немецкой стороны.

Разбор описывает НЕМЕЦКОЕ слово: формы, род, управление, примеры. На русской единице
его нет и быть не должно — она про перевод, а не про грамматику. Замер 05.08.2026:
21 534 русских единицы, разбор лежит у 158.

Из-за этого запрос «чёткое направление» возвращал карточку без разбора, словарь считал
слово незнакомым и шёл к модели — за тем, что у нас уже разобрано и оплачено. При этом
тот же запрос по-немецки отдавал семь блоков.

Замер после правки: русских единиц со связью на немецкое слово с готовым разбором —
15 374. Столько запросов по-русски теперь обслуживается из своей базы вместо 158.
"""
from backend.lex_units import _build_item

GERMAN_CARD = {
    "usage_examples": [{"de": "Eine klare Zielrichtung fehlt."}],
    "meanings": {"primary": {"value": "направление"}},
    "forms": {"plural": "die Zielrichtungen"},
    "memory_tip": "Ziel + Richtung",
}

DE_UNIT = {"id": 1, "lang": "de", "kind": "word", "display": "die Zielrichtung",
           "lemma": "Zielrichtung", "pos": "noun", "gender": "die", "card": GERMAN_CARD}
RU_UNIT = {"id": 2, "lang": "ru", "kind": "word", "display": "направление",
           "lemma": "направление", "pos": None, "gender": None, "card": None}


def _link(unit, rank=10):
    return dict(unit, rank=rank)


def test_asking_in_russian_returns_the_german_breakdown():
    item = _build_item(RU_UNIT, [_link(DE_UNIT)], source_lang="ru", target_lang="de")
    assert item["__lex_has_card"] is True
    assert item["usage_examples"] == GERMAN_CARD["usage_examples"]
    assert item["forms"] == GERMAN_CARD["forms"]
    assert item["memory_tip"] == GERMAN_CARD["memory_tip"]


def test_asking_in_german_is_unchanged():
    item = _build_item(DE_UNIT, [_link(RU_UNIT)], source_lang="de", target_lang="ru")
    assert item["__lex_has_card"] is True
    assert item["forms"] == GERMAN_CARD["forms"]


def test_headword_stays_on_the_right_side():
    """Разбор берём у немца, но кто спросил — тот и слева."""
    ru = _build_item(RU_UNIT, [_link(DE_UNIT)], source_lang="ru", target_lang="de")
    assert ru["source_text"] == "направление"
    assert ru["target_text"] == "die Zielrichtung"

    de = _build_item(DE_UNIT, [_link(RU_UNIT)], source_lang="de", target_lang="ru")
    assert de["source_text"] == "die Zielrichtung"
    assert de["target_text"] == "направление"


def test_no_german_card_means_no_card_at_all():
    """Нечего брать — честно говорим, что разбора нет, и обычный путь идёт дальше."""
    bare_de = dict(DE_UNIT, card=None)
    item = _build_item(RU_UNIT, [_link(bare_de)], source_lang="ru", target_lang="de")
    assert item["__lex_has_card"] is False


def test_own_card_wins_over_the_linked_one():
    """Если у русской единицы разбор всё-таки есть — он и показывается."""
    own = {"memory_tip": "своя подсказка"}
    ru_with_card = dict(RU_UNIT, card=own)
    item = _build_item(ru_with_card, [_link(DE_UNIT)], source_lang="ru", target_lang="de")
    assert item["memory_tip"] == "своя подсказка"
