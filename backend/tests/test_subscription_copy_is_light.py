"""Подписка на слово — это отметка, а не копия слова.

Раньше каждая подписная строка несла внутри полную фотокопию разбора. За 9 067 строками
«Быстрого старта» стоит всего 1 005 разных слов — то есть каждое скопировано девять раз,
9,75 МБ одного и того же.

Читать эту копию давно некому: с 04–05.08.2026 карточка берёт разбор с общей единицы при
показе. Копия только занимала место и жила своей жизнью.

Опознавательные поля остаются: артикль, часть речи, стороны и направление нужны самой
строке — по ним рисуется заголовок, и весят они ничего.

Одно исключение: если на единице разбора ещё нет, копируем как раньше. Пустая карточка
у человека хуже лишней копии.
"""
from backend.database import (
    CARD_CONTENT_KEYS,
    CARD_IDENTITY_KEYS,
    strip_card_content_for_subscription,
)

FULL_CARD = {
    "word_de": "der Wandel",
    "word_ru": "перемена",
    "translation_ru": "перемена, изменение",
    "source_lang": "de",
    "target_lang": "ru",
    "language_pair": {"code": "de-ru"},
    "article": "der",
    "part_of_speech": "noun",
    "usage_examples": [{"de": "Der Wandel kommt."}],
    "meanings": {"primary": {"value": "перемена"}},
    "forms": {"plural": "die Wandel"},
    "government_patterns": ["Wandel + Genitiv"],
    "memory_tip": "как «вандал», только про перемены",
    "raw_text": "сырой ответ модели " * 40,
}


def test_identity_survives():
    """Заголовок, род и направление остаются — по ним строка сама себя показывает."""
    light = strip_card_content_for_subscription(FULL_CARD)
    assert light["word_de"] == "der Wandel"
    assert light["article"] == "der"
    assert light["part_of_speech"] == "noun"
    assert light["language_pair"] == {"code": "de-ru"}
    assert light["source_lang"] == "de" and light["target_lang"] == "ru"


def test_the_breakdown_itself_is_dropped():
    light = strip_card_content_for_subscription(FULL_CARD)
    for key in CARD_CONTENT_KEYS:
        assert key not in light, key


def test_service_junk_is_dropped_too():
    """Сырой ответ модели весит больше всего разбора и не нужен никому."""
    assert "raw_text" not in strip_card_content_for_subscription(FULL_CARD)


def test_it_gets_much_lighter():
    before = len(str(FULL_CARD))
    after = len(str(strip_card_content_for_subscription(FULL_CARD)))
    assert after < before / 2, f"было {before}, стало {after}"


def test_nothing_outside_the_agreed_list_slips_through():
    light = strip_card_content_for_subscription(FULL_CARD)
    assert set(light) <= set(CARD_IDENTITY_KEYS)


def test_junk_input_is_not_a_crash():
    for value in (None, "карточка", 42, []):
        assert strip_card_content_for_subscription(value) == {}, value


def test_empty_card_stays_empty():
    assert strip_card_content_for_subscription({}) == {}
