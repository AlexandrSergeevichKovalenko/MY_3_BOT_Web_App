"""Часть речи и род берутся из артикля в разборе — без модели и без догадок.

Настоящая дыра в слое единиц — не род, а ЧАСТЬ РЕЧИ: род требуется только
существительным, и пока слово формально «неизвестно что», артикль к нему не показывают.
Так «Ausgabe» и «Käsefuß» висели без артикля, хотя «die» и «der» лежали в их разборе.

Правило узкое намеренно: артикль в разборе И заглавная первая буква. Заглавная буква
сама по себе НЕ признак существительного — в банке лежат «Hineingehen» и «Nahtlos»,
глагол и прилагательное с большой буквы (грабли слоя единиц, 27.07.2026).
"""
from backend.lex_units import _gender_from_card


def test_article_gives_the_gender():
    assert _gender_from_card({"article": "der"}) == "der"
    assert _gender_from_card({"article": "die"}) == "die"
    assert _gender_from_card({"article": "das"}) == "das"


def test_case_and_padding_do_not_break_it():
    assert _gender_from_card({"article": " Die "}) == "die"
    assert _gender_from_card({"article": "DAS"}) == "das"


def test_anything_that_is_not_a_definite_article_is_refused():
    """Неопределённый артикль рода не задаёт («ein» — и der, и das), пустое значение
    и мусор тоже. Молчаливая догадка тут дороже пропуска."""
    for value in ("ein", "eine", "the", "", None, "der die", "l'"):
        assert _gender_from_card({"article": value}) == "", value


def test_missing_or_broken_card_is_not_a_crash():
    assert _gender_from_card(None) == ""
    assert _gender_from_card({}) == ""
    assert _gender_from_card("der Hund") == ""
    assert _gender_from_card({"forms": {"plural": "Hunde"}}) == ""
