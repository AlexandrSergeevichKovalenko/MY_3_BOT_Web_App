"""Слой словарных статей: регистр ничего не решает, статьи не склеиваются.

Живой случай 11.08.2026. Человек набрал «Толстый» с большой буквы — так пишут первое
слово в строке. Машинный переводчик вернул «Der Dicke», а часть речи мы вывели из
ЗАГЛАВНОЙ БУКВЫ его ответа и выдали карточку про толстяка вместо прилагательного.
Тот же запрос с маленькой буквы давал верное «dick».

Здесь заперты правила, которые это чинят. Всё считается без базы и без сети — это
проверка самих правил, а не данных.
"""
from backend.dictionary_entries import (
    _dedupe_words,
    _display_of,
    _entry,
    _fold_genderless_nouns,
    _key_of,
)


def test_leading_article_never_doubles_in_the_headword():
    """Наши единицы отдают заголовок уже с артиклем. Без снятия получалось
    «die die Kiefer», а поиск частотности искал слово «das haus» и не находил."""
    entry = _entry("die Kiefer", pos="noun", translations=["сосна"])
    assert entry["headword"] == "Kiefer"
    assert entry["gender"] == "die"
    assert entry["display"] == "die Kiefer"


def test_gender_from_the_headword_does_not_override_a_known_one():
    entry = _entry("der Kiefer", pos="noun", gender="der", translations=["челюсть"])
    assert (entry["headword"], entry["gender"]) == ("Kiefer", "der")


def test_noun_without_a_known_gender_is_printed_bare():
    """Выдуманный артикль хуже отсутствующего — тот же контракт, что у german_surface."""
    assert _display_of("Soweit", "noun", "") == "Soweit"
    assert _display_of("dick", "adjective", "") == "dick"


def test_translations_lose_case_only_duplicates():
    assert _dedupe_words(["сосна", "Сосна", " сосна ", "челюсть"]) == ["сосна", "челюсть"]


def test_same_spelling_different_part_of_speech_stays_two_entries():
    """«essen» — глагол «есть», «Essen» — существительное «еда». Это два слова, и
    ровно этот выбор человек обязан увидеть."""
    verb = _entry("essen", pos="verb", translations=["есть"])
    noun = _entry("Essen", pos="noun", gender="das", translations=["еда"])
    assert _key_of(verb) != _key_of(noun)


def test_same_spelling_different_gender_stays_two_entries():
    """«der Kiefer» (челюсть) и «die Kiefer» (сосна) — разные слова. Склеить их в одну
    строку означало бы повторить ошибку «die Dicke» с примерами про «der Dicke»."""
    jaw = _entry("Kiefer", pos="noun", gender="der", translations=["челюсть"])
    pine = _entry("Kiefer", pos="noun", gender="die", translations=["сосна"])
    assert _key_of(jaw) != _key_of(pine)


def test_genderless_noun_fills_an_empty_gendered_entry():
    known = _entry("Haus", pos="noun", gender="das", translations=[], source="units")
    blob = _entry("Haus", pos="noun", translations=["дом", "здание"], source="freedict")
    out = _fold_genderless_nouns([known, blob])
    assert [e["display"] for e in out] == ["das Haus"]
    assert out[0]["translations"] == ["дом", "здание"]


def test_genderless_noun_never_pollutes_an_entry_that_already_has_senses():
    """«группа» — это «die Band», а не «das Band». Приписать чужое значение к
    известному роду нельзя: это ровно тот дефект, из-за которого разбирали случай."""
    known = _entry("Band", pos="noun", gender="das", translations=["лента"], source="units")
    blob = _entry("Band", pos="noun", translations=["группа", "том"], source="freedict")
    out = _fold_genderless_nouns([known, blob])
    assert [e["display"] for e in out] == ["das Band"]
    assert out[0]["translations"] == ["лента"]


def test_genderless_noun_is_dropped_when_the_spelling_has_real_homographs():
    jaw = _entry("Kiefer", pos="noun", gender="der", translations=["челюсть"], source="units")
    pine = _entry("Kiefer", pos="noun", gender="die", translations=["сосна"], source="units")
    blob = _entry("Kiefer", pos="noun", translations=["челюсть", "сосна"], source="freedict")
    out = _fold_genderless_nouns([jaw, pine, blob])
    assert sorted(e["display"] for e in out) == ["der Kiefer", "die Kiefer"]


def test_a_word_we_do_not_know_is_left_alone():
    """Безродное существительное без родовых соседей остаётся в списке: молчать про
    род можно, выбрасывать слово — нет."""
    lone = _entry("Zuhause", pos="noun", translations=["дом"], source="freedict")
    assert [e["headword"] for e in _fold_genderless_nouns([lone])] == ["Zuhause"]
