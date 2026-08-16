"""Спрягаемый глагол пишется со строчной — всегда, кроме начала предложения.

Заголовок в базе может стоять с заглавной: «Aufwachen», «Hineingehen» — это
субстантивированный инфинитив, в таком виде он и приходит из сохранения. Движок
приклеивал окончания как есть, и человек читал «ich Gehe», «ich Hineingehe».

Замер 15.08.2026: карточек с заглавным заголовком и неименной частью речи — 357
(210 глаголов, 118 прилагательных, 29 наречий).
"""
from backend.german_grammar_tables import build_verb_conjugation, build_adjective_comparison


def test_capitalized_infinitive_is_conjugated_in_lowercase():
    table = build_verb_conjugation(word_de="Gehen")
    assert table["praesens"]["ich"] == "gehe"
    assert table["praesens"]["du"] == "gehst"


def test_separable_verb_too():
    table = build_verb_conjugation(word_de="Hineingehen")
    assert table["praesens"]["ich"] == "hineingehe"


def test_lowercase_infinitive_is_unchanged():
    assert build_verb_conjugation(word_de="gehen")["praesens"]["ich"] == "gehe"


def test_adjective_degrees_are_lowercase():
    table = build_adjective_comparison(word_de="Nahtlos")
    assert table["positive"] == "nahtlos"
    assert table["comparative"] == "nahtloser"
    assert table["superlative"] == "am nahtlosesten"


def test_zu_infinitive_still_has_no_table():
    """Правило регистра не отменяет прежнего заслона."""
    assert build_verb_conjugation(word_de="klarzukommen") is None


def test_declined_adjective_still_has_no_table():
    assert build_adjective_comparison(word_de="schlammigen") is None


# ── заголовок слова ───────────────────────────────────────────────────────────

from backend.german_grammar_tables import german_headword_case as headword


def test_verb_and_adjective_headwords_are_lowercase():
    assert headword("Abbuchen", "verb") == "abbuchen"
    assert headword("Akut", "adjective") == "akut"
    assert headword("Nahtlos", "adjective") == "nahtlos"


def test_noun_keeps_its_capital():
    assert headword("Haus", "noun") == "Haus"
    assert headword("Wehe", "noun") == "Wehe"


def test_unknown_part_of_speech_is_not_touched():
    """Пустая часть речи — не разрешение: под ней прячутся имена собственные.

    Тот же урок, что с русскими переводами, где проверка «а это существительное?»
    написала «афины»."""
    assert headword("Berlin", "") == "Berlin"
    assert headword("Aufwachen", None) == "Aufwachen"


def test_already_lowercase_is_untouched():
    assert headword("gehen", "verb") == "gehen"
    assert headword("", "verb") == ""


# ── страж на сохранении ───────────────────────────────────────────────────────

def test_save_lowercases_a_verb_headword():
    """Без заслона на входе каждое новое слово ложилось бы криво снова.

    Правило показа чинит уже накопленное, но новое обязано ложиться правильно сразу.
    """
    import backend.backend_server as bs
    payload = bs._prepare_dictionary_response_json_for_save(
        response_json={"part_of_speech": "verb", "word_de": "Abbuchen",
                       "translation_de": "Abbuchen"},
        source_text="списывать", target_text="Abbuchen",
        source_lang="ru", target_lang="de",
        word_ru="списывать", word_de="Abbuchen",
        translation_de="Abbuchen", translation_ru="списывать",
    )
    assert payload["word_de"] == "abbuchen"
    assert payload["translation_de"] == "abbuchen"


def test_save_keeps_a_noun_capital():
    import backend.backend_server as bs
    payload = bs._prepare_dictionary_response_json_for_save(
        response_json={"part_of_speech": "noun", "word_de": "Haus", "article": "das"},
        source_text="дом", target_text="Haus",
        source_lang="ru", target_lang="de",
        word_ru="дом", word_de="Haus", translation_de="Haus", translation_ru="дом",
    )
    assert payload["word_de"].endswith("Haus"), "существительное остаётся с заглавной"


def test_save_does_not_touch_a_phrase():
    """У фразы заглавная законна — это начало предложения."""
    import backend.backend_server as bs
    payload = bs._prepare_dictionary_response_json_for_save(
        response_json={"part_of_speech": "verb", "word_de": "Ich gehe nach Hause"},
        source_text="я иду домой", target_text="Ich gehe nach Hause",
        source_lang="ru", target_lang="de",
        word_ru="я иду домой", word_de="Ich gehe nach Hause",
        translation_de="Ich gehe nach Hause", translation_ru="я иду домой",
    )
    assert payload["word_de"].startswith("Ich")
