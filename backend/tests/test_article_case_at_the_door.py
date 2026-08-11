"""Артикль ложится в базу со строчной — на входе, а не разовой чисткой потом.

11.08.2026 в общем пуле нашлось 1 384 заголовка вида «Die Enkelin», «Der Held».
Само слово и род верные, испорчен только регистр артикля: в немецком он пишется со
строчной, с заглавной — только когда открывает предложение. В карточке он ничего не
открывает, и «Die Kundgebung» читается как обрывок фразы.

Накопленное починили разом. Но владелец спросил главное: «а дальше как будет?» —
и без правила на входе оно копилось бы заново, потому что модель возвращает
заголовок с заглавной регулярно. Правило стоит на самом дне записи в общий пул,
через которое проходят ВСЕ пути: сохранение из мини-аппа, из бота, быстрый перевод
и разовые прогоны.
"""
from backend.dictionary_intake import clean_all, lower_leading_article


def test_article_of_a_headword_goes_lowercase():
    assert lower_leading_article("Die Enkelin") == "die Enkelin"
    assert lower_leading_article("Der Held") == "der Held"
    assert lower_leading_article("Das Weinstüberl") == "das Weinstüberl"


def test_a_correct_headword_is_left_alone():
    assert lower_leading_article("die Enkelin") == "die Enkelin"
    assert lower_leading_article("das Haus") == "das Haus"


def test_a_sentence_keeps_its_capital_letter():
    """«Die Kosten waren höher als erwartet.» начинается с большой буквы ЗАКОННО.
    Первая версия проверки собиралась «починить» 2 450 таких строк."""
    for sentence in (
        "Die Kosten waren höher als erwartet.",
        "Der Zug fährt gleich ab.",
        "Das Unternehmen hat sich etabliert, um neue Kunden anzulocken.",
    ):
        assert lower_leading_article(sentence) == sentence


def test_hyphenated_headwords_still_work():
    assert lower_leading_article("Die Blau-Weiße") == "die Blau-Weiße"


def test_russian_text_is_never_touched():
    for value in ("Дом", "Постепенно", "Что тебя раздражает?", ""):
        assert lower_leading_article(value) == value


def test_the_door_applies_it_to_every_field():
    """Через clean_all проходит КАЖДАЯ запись в общий пул, поэтому правило стоит там,
    а не в одном из путей сохранения."""
    cleaned = clean_all("Die Enkelin", "внучка", None, "Der Held", "герой", "Die Kosten waren höher.")
    assert cleaned == ("die Enkelin", "внучка", "", "der Held", "герой", "Die Kosten waren höher.")


def test_it_is_idempotent():
    once = lower_leading_article("Die Enkelin")
    assert lower_leading_article(once) == once
