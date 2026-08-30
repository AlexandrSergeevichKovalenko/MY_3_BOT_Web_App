"""Строка «Конструкция» и строка «Приставка» на экране сравнения слов.

Оба дефекта принёс владелец 30.08.2026, скриншотами пары «anbieten ↔ unterbreiten».

ЧТО БЫЛО НАПЕЧАТАНО НА ЭКРАНЕ:

    jdm. etw. anbieten + Dativ + Akkusativ · Dativ + Akkusativ
    etw. zum Verkauf anbieten + zum + Akkusativ + Akkusativ · Akkusativ + Akkusativ
    unterbreiten + Akkusativ · Akkusativ

Три класса вранья в трёх строках:

1. ПАДЕЖ ДВАЖДЫ. Бэкенд вклеивал падеж в саму запись, фронт печатал рядом ещё и поле
   `case` через «·». Это происходило на КАЖДОЙ строке с падежом, всегда.
2. ПРЕДЛОГ ДВАЖДЫ И НЕ ТОТ ПАДЕЖ. Предлог приписывался в хвост, даже когда он уже
   стоял внутри образца («zum Verkauf»), и падеж прирастал к нему: «zum + Akkusativ».
   Это грамматическая ложь — zum = zu dem, это Dativ. Akkusativ относился к «etw.»,
   а наша арифметика пришила его к предлогу. Правило ноль: немецкую форму мы не
   выводим из другой формы своим счётом.
3. ОТДЕЛЯЕМОЙ ПРИСТАВКИ НЕ БЫЛО ВОВСЕ. У глагола показывалась возвратность и не
   показывалось то, чем «anbieten» и «unterbreiten» как раз и различаются в речи.
   По написанию это не решается: «unterbringen» приставку отделяет, «unterbreiten»
   нет, приставка у обоих «unter-». Ответ читается из напечатанных форм справочника.
"""
import pytest

from backend import backend_server
from backend import german_grammar_tables


def view(word, pattern, case="", preposition=""):
    return backend_server._word_diff_construction_view(word, pattern, case, preposition)


# ── 1. Падеж печатается РОВНО ОДИН РАЗ ──────────────────────────────────────────

def test_rich_pattern_keeps_the_case_out_of_the_text():
    """«jdm. etw. anbieten» — образец уже читается, падеж уходит подписью, и только туда."""
    text, note = view("anbieten", "jdm. etw. anbieten", "Dativ + Akkusativ")
    assert text == "jdm. etw. anbieten"
    assert note == "Dativ + Akkusativ"
    # Ровно один раз: ни в записи, ни дважды в подписи.
    assert "Dativ" not in text and note.count("Dativ") == 1


def test_bare_pattern_becomes_a_formula_without_a_second_case():
    """«unterbreiten» голое — вот здесь формула читается, и подписи уже не будет.

    Подпись «unterbreiten · Akkusativ» владелец забраковал ещё 26.08.2026
    («непонятно, что такое слово, а потом стоит · Nominativ»)."""
    text, note = view("unterbreiten", "unterbreiten", "Akkusativ")
    assert text == "unterbreiten + Akkusativ"
    assert note == ""


def test_repeated_case_from_the_model_is_collapsed():
    """«Akkusativ + Akkusativ» — повтор модели, а не два разных падежа."""
    assert backend_server._word_diff_case_text("Akkusativ + Akkusativ") == "Akkusativ"
    assert backend_server._word_diff_case_text("Dativ + Akkusativ") == "Dativ + Akkusativ"


# ── 2. Предлог не приписывается второй раз, падеж не липнет к нему ──────────────

def test_preposition_already_inside_the_pattern_is_not_appended_again():
    """«etw. zum Verkauf anbieten»: zum уже в обороте — «+ zum + Akkusativ» это ложь."""
    text, note = view("anbieten", "etw. zum Verkauf anbieten", "Akkusativ + Akkusativ", "zum")
    assert text == "etw. zum Verkauf anbieten"
    assert note == "Akkusativ"
    assert "zum + " not in text and "+ zum" not in text


def test_preposition_in_the_middle_of_the_pattern_is_not_appended_again():
    """Прежняя правка снимала предлог только с КОНЦА образца — в середине не видела."""
    text, _note = view("anbieten", "etw. zum Preis von ... anbieten", "Akkusativ", "zum")
    assert text == "etw. zum Preis von ... anbieten"


def test_real_government_still_reads_as_a_formula():
    """Настоящее предложное управление остаётся формулой: «warten auf + Akkusativ»."""
    text, note = view("warten", "warten auf", "Akkusativ", "auf")
    assert text == "warten auf + Akkusativ"
    assert note == ""


def test_preposition_named_but_missing_from_the_pattern_is_added():
    """Источник назвал предлог, образец его не содержит — только тогда дописываем."""
    text, note = view("laufen", "laufen", "Dativ", "zu")
    assert text == "laufen + zu + Dativ"
    assert note == ""


# ── 3. Отделяемая приставка — из напечатанных форм, не из написания ─────────────

# Так это напечатано в статьях de.wiktionary. У «unterbreiten» и «umfahren» блока ДВА:
# одним написанием записаны два разных глагола, и статья разводит их пометами.
READINGS = {
    "anbieten": [{"present": "bietet an", "partizip2": "angeboten", "label": ""}],
    "verkaufen": [{"present": "verkauft", "partizip2": "verkauft", "label": ""}],
    "beginnen": [{"present": "beginnt", "partizip2": "begonnen", "label": ""}],
    "ernten": [{"present": "erntet", "partizip2": "geerntet", "label": ""}],
    "machen": [{"present": "macht", "partizip2": "gemacht", "label": ""}],
    "unterbreiten": [
        {"present": "breitet unter", "partizip2": "untergebreitet", "label": "trennbar"},
        {"present": "unterbreitet", "partizip2": "unterbreitet", "label": "untrennbar"},
    ],
    "unterbringen": [{"present": "bringt unter", "partizip2": "untergebracht", "label": ""}],
}


@pytest.fixture
def reference(monkeypatch):
    from backend import german_verb_paradigms
    monkeypatch.setattr(german_verb_paradigms, "verb_readings",
                        lambda verb, allow_network=False: READINGS.get(verb))


def test_separable_prefix_is_read_from_the_present_tense(reference):
    """«bietet an» — два слова, приставка уехала. Это и есть ответ, напечатанный."""
    answer = german_grammar_tables.verb_prefix_separability("anbieten")
    assert answer["value"] == "separable"
    assert answer["prefix"] == "an"
    assert answer["example"] == "er bietet … an"
    assert answer["partizip2"] == "angeboten"


def test_two_verbs_under_one_spelling_are_both_shown(reference):
    """«unterbreiten» — «подстилать» (отделяемый) И «предлагать» (неотделяемый).

    ┌─ ПРОВЕРЕНО 30.08.2026. НЕ ЧИНИТЬ ЭТО «ОДНИМ ВЕРДИКТОМ». ────────────────────┐
    │ Страница «Flexion:unterbreiten» отдаёт ТОЛЬКО отделяемый глагол — «breitet  │
    │ unter», «untergebreitet». Второй страницы у написания нет (поиск intitle    │
    │ дал одну). Поверив ей, мы написали бы владельцу «приставка отделяемая» под  │
    │ карточкой глагола «предлагать» — уверенно и неверно.                        │
    │ Одного ответа здесь не существует. Выбирать за человека запрещено, поэтому  │
    │ показываем оба прочтения с их формами.                                      │
    └─────────────────────────────────────────────────────────────────────────────┘"""
    answer = german_grammar_tables.verb_prefix_separability("unterbreiten")
    assert answer["value"] == "both_readings"
    assert [r["value"] for r in answer["readings"]] == ["separable", "inseparable"]
    assert [r["example"] for r in answer["readings"]] == ["er breitet … unter", "er unterbreitet"]


def test_same_prefix_opposite_verdicts(reference):
    """Написание «unter-» одно, поведение разное — потому и спрашиваем справочник."""
    assert german_grammar_tables.verb_prefix_separability("unterbringen")["value"] == "separable"
    assert german_grammar_tables.verb_prefix_separability("verkaufen")["value"] == "inseparable"


def test_inseparable_needs_a_participle_without_ge(reference):
    """«begonnen» без «ge-» подтверждает приставку; «geerntet» с «ge-» — опровергает.

    «ernten» — не «er» + «nten». Молчание здесь честнее выдуманной морфологии."""
    assert german_grammar_tables.verb_prefix_separability("beginnen")["value"] == "inseparable"
    assert german_grammar_tables.verb_prefix_separability("ernten") == {}


def test_verb_without_a_prefix_gets_no_line(reference):
    """У «machen» приставки нет — строка про отделяемость была бы враньём о слове."""
    assert german_grammar_tables.verb_prefix_separability("machen") == {}


def test_reference_silent_means_we_do_not_know(monkeypatch):
    """Справочник не ответил — {} («не знаем»), а не догадка по написанию.

    Этот случай считается в `_word_diff_gaps` как «нет отделяемости: слово»."""
    from backend import german_verb_paradigms
    monkeypatch.setattr(german_verb_paradigms, "verb_readings",
                        lambda verb, allow_network=False: None)
    assert german_grammar_tables.verb_prefix_separability("anbieten") == {}


def test_article_label_outranks_the_form(reference):
    """Помета статьи «trennbar»/«untrennbar» — прямой ответ, форма лишь подтверждает."""
    verdict = german_grammar_tables._separability_verdict(
        "unter", {"present": "unterbreitet", "partizip2": "unterbreitet", "label": "untrennbar"})
    assert verdict["value"] == "inseparable"


def test_gaps_count_verbs_without_a_separability_answer():
    """Владелец обязан видеть числом, скольким глаголам справочник ещё не отвечал."""
    diff = {
        "comparable": {"value": "partial"},
        "verdict": [{"word": "anbieten", "line": "…"}],
        "examples": [{"word": "anbieten"}],
        "collocations": [{"word": "anbieten"}],
        "constructions": [{"word": "anbieten"}],
        "usage": [{"word": "anbieten", "pos": "verb", "separability": {}}],
    }
    assert "нет отделяемости: anbieten" in backend_server._word_diff_gaps(diff, ["anbieten"])

    diff["usage"] = [{"word": "anbieten", "pos": "verb",
                      "separability": {"value": "separable", "prefix": "an"}}]
    assert backend_server._word_diff_gaps(diff, ["anbieten"]) == []


# ── 4. Уборка накопленного: расклеить старую запись и собрать заново ────────────
#
# Готовые разборы лежат в `bt_3_word_diff_cards` целиком. Починка сборки их не
# касается: открыв старую пару, человек увидел бы ту же кашу. Расклейка снимает
# нашу же приписку с конца строки — и дальше идёт по тому же правилу, что и живая
# выдача (scripts/word_diff_rebuild_cards.py).

@pytest.mark.parametrize("stored, case, prep, expect_text, expect_note", [
    # Ровно те три строки, что владелец прислал с экрана 30.08.2026.
    ("jdm. etw. anbieten + Dativ + Akkusativ", "Dativ + Akkusativ", "",
     "jdm. etw. anbieten", "Dativ + Akkusativ"),
    ("etw. zum Verkauf anbieten + zum + Akkusativ + Akkusativ", "Akkusativ + Akkusativ", "zum",
     "etw. zum Verkauf anbieten", "Akkusativ"),
    ("etw. zum Preis von ... anbieten + zum + Akkusativ", "Akkusativ", "zum",
     "etw. zum Preis von ... anbieten", "Akkusativ"),
])
def test_stored_card_is_ungled_and_rebuilt(stored, case, prep, expect_text, expect_note):
    raw = backend_server._word_diff_construction_unglue(stored, case, prep)
    text, note = view("anbieten", raw, case, prep)
    assert (text, note) == (expect_text, expect_note)


def test_unglue_keeps_a_single_preposition():
    """«laufen + zu + Dativ»: предлог здесь один и он часть управления — не снимаем."""
    raw = backend_server._word_diff_construction_unglue("laufen + zu + Dativ", "Dativ", "zu")
    assert raw == "laufen + zu"
    assert view("laufen", raw, "Dativ", "zu") == ("laufen + zu + Dativ", "")


def test_rebuilding_twice_changes_nothing():
    """Уборку можно запустить повторно: второй прогон обязан ничего не менять."""
    text, note = view("anbieten", "jdm. etw. anbieten", "Dativ + Akkusativ")
    again_raw = backend_server._word_diff_construction_unglue(text, "Dativ + Akkusativ", "")
    assert view("anbieten", again_raw, "Dativ + Akkusativ") == (text, note)
