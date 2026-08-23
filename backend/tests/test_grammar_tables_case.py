"""Спрягаемый глагол пишется со строчной — всегда, кроме начала предложения.

Заголовок в базе может стоять с заглавной: «Aufwachen», «Hineingehen» — это
субстантивированный инфинитив, в таком виде он и приходит из сохранения. Движок
приклеивал окончания как есть, и человек читал «ich Gehe», «ich Hineingehe».

Замер 15.08.2026: карточек с заглавным заголовком и неименной частью речи — 357
(210 глаголов, 118 прилагательных, 29 наречий).
"""
from backend.german_grammar_tables import build_verb_conjugation, build_adjective_comparison


def _reference(monkeypatch, table):
    """Подставной справочник: он же ведёт учёт, о каком написании его спросили."""
    import backend.german_grammar_tables as G
    asked: list[str] = []

    def fake(infinitive):
        asked.append(infinitive)
        return dict(table) if table else None

    monkeypatch.setattr(G, "_documented_conjugation", fake)
    return asked


def test_capitalized_infinitive_is_asked_about_in_lowercase(monkeypatch):
    """Ожидание обновлено 23.08.2026: формы больше не считает код.

    Тест писался про РЕГИСТР, а формы брал такие, какие тогда дописывал движок. Счёт от
    основы удалён целиком (решение владельца 23.08.2026): на не-глаголе он давал «ich
    boree», «ich aspettiamoe». Теперь таблица приходит напечатанной из
    `german_verb_paradigms`, а правило регистра проверяется по тому, О ЧЁМ спрошен
    справочник: он знает строчное «gehen», а не «Gehen».
    """
    asked = _reference(monkeypatch, {"praesens": {"ich": "gehe", "du": "gehst"}})
    table = build_verb_conjugation(word_de="Gehen")
    assert asked == ["gehen"], "справочник обязан спрашиваться о слове со строчной"
    assert table["praesens"]["ich"] == "gehe"


def test_separable_verb_too(monkeypatch):
    asked = _reference(monkeypatch, {"praesens": {"ich": "gehe hinein"}})
    table = build_verb_conjugation(word_de="Hineingehen")
    assert asked == ["hineingehen"]
    assert table["praesens"]["ich"] == "gehe hinein"


def test_lowercase_infinitive_is_unchanged(monkeypatch):
    asked = _reference(monkeypatch, {"praesens": {"ich": "gehe"}})
    build_verb_conjugation(word_de="gehen")
    assert asked == ["gehen"]


def test_verb_without_reference_has_no_invented_table(monkeypatch):
    """Справочник молчит — таблицы нет. Раньше здесь появлялось спряжение не-глагола."""
    _reference(monkeypatch, None)
    assert build_verb_conjugation(word_de="Besagt", seed={"present_3sg": "besagt"}) is None


def test_adjective_degrees_are_lowercase(monkeypatch):
    """Ожидание обновлено 18.08.2026: степени сравнения больше не считает код.

    Тест писался про РЕГИСТР, а формы брал такие, какие тогда дописывал движок
    («nahtloser», «am nahtlosesten»). Дописывание удалено: оно давало «gut → guter»,
    «alt → am altesten», «hoch → hocher» — формы, которых в языке нет. Теперь степени
    приходят из напечатанной таблицы справочника (`german_reference_forms`).

    Правило регистра проверяется здесь по-прежнему: заголовок приходит с заглавной
    («Nahtlos»), а справочник обязан спрашиваться о строчном слове, и в ответе
    положительная степень стоит со строчной."""
    asked: list[str] = []

    def fake_reference(word):
        asked.append(word)
        return {"positive": "nahtlos", "comparative": "nahtloser",
                "superlative": "am nahtlosesten", "source": "wiktionary-steigerung"}

    import backend.german_grammar_tables as G
    monkeypatch.setattr(G, "_documented_degrees", fake_reference)

    table = build_adjective_comparison(word_de="Nahtlos")
    assert asked == ["nahtlos"], "справочник обязан спрашиваться о слове со строчной"
    assert table["positive"] == "nahtlos"
    assert table["source"] == "wiktionary-steigerung"


def test_adjective_without_reference_has_no_invented_table(monkeypatch):
    """Справочник молчит — таблицы нет. Раньше здесь появлялась выдуманная лесенка."""
    import backend.german_grammar_tables as G
    monkeypatch.setattr(G, "_documented_degrees", lambda word: None)
    assert build_adjective_comparison(word_de="gut") is None


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
