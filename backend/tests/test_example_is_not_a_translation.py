"""Пример употребления не должен становиться переводом слова.

Разбор слова возвращает список значений, и каждое значение заводится как перевод.
Но в этот список попадали и примеры: «abbestellen → Ваша подписка на рассылку успешно
отменена», «rutschen → Йо, ты можешь кататься?». Они получали высший приоритет и
вставали первыми, а настоящий перевод лежал ниже и человек его не видел.

Признак — знак конца ИЛИ заглавная буква при трёх и более словах. Не длина: длинное
толкование со строчной буквы («ясли, учреждение по уходу за детьми») — это и есть
перевод, и трогать его нельзя. Первая версия правила отбирала по длине и хотела
заменить «tüchtig → прилежный, усердный, старательно выполняющий работу» на «большой,
сильный» — то есть испортить.

Порог в три слова: «Точка зрения» — обычный перевод, просто с заглавной. Цена порога
известна и принята: короткий пример «Колокол прозвонил» проскочит. Пропустить дешевле,
чем спрятать хороший перевод.
"""
from backend.lex_units import looks_like_example_not_translation as is_example


def test_sentences_are_examples():
    for text in (
        "Ваша подписка на рассылку успешно отменена",
        "Йо, ты можешь кататься?",
        "Проект завершён?",
        "Я позвонил ему, чтобы согласиться встретиться",
        "Он пьет много вина",
        "Эта неделя не считается, это наша запасная неделя",
    ):
        assert is_example(text), text


def test_translations_are_not_examples():
    for text in (
        "отменять (заказ, подписку)",
        "скользить",
        "буферная неделя",
        "ясли, учреждение по уходу за совсем маленькими детьми",
        "прилежный, усердный, хорошо и старательно выполняющий работу",
        "медицинское устройство для внутривенного введения жидкости",
        "человек, который часто забывает спортивную сумку",
    ):
        assert not is_example(text), text


def test_capitalized_short_translation_survives():
    """«Точка зрения» — перевод, а не предложение. Ради него и стоит порог в три слова."""
    assert not is_example("Точка зрения")
    assert not is_example("Одноразовый стакан")


def test_capital_with_three_words_is_an_example():
    assert is_example("Моя рубашка помята")
    assert is_example("Я все испортил")


def test_final_punctuation_wins_regardless_of_length():
    assert is_example("Готово.")
    assert is_example("Правда?")


def test_broken_input_is_not_a_crash():
    for value in (None, "", "   ", 0, []):
        assert is_example(value) is False, value


# ── перевод обязан быть по-русски ─────────────────────────────────────────────

def test_german_paraphrase_is_not_a_russian_translation():
    """«Du stinkst furchtbar.» → «Du stinkst fürchterlich» человеку ничего не объясняет.
    Такая связь заводилась из разбора и вставала первой; теперь она не заводится вовсе.
    Замер 05.08.2026: связей «на русскую сторону» без единой русской буквы — 228."""
    from backend.lex_units import _CYRILLIC_ANY_RE
    for german_only in ("Du stinkst fürchterlich", "das Waschbecken", "einen Fusselrasierer benutzen"):
        assert not _CYRILLIC_ANY_RE.search(german_only), german_only
    for russian in ("раковина", "Одноразовый стакан", "мыть (кого-либо) тряпочкой"):
        assert _CYRILLIC_ANY_RE.search(russian), russian
