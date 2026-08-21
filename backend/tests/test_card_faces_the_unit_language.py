"""Разбор ложится на слово ЛИЦОМ К ЕГО ЯЗЫКУ. Иначе в немецком словаре слева русский.

Владелец увидел это на «die Tonne»: пример в карточке шёл «Эта машина может увезти
десять тонн.» → «Dieses Fahrzeug kann zehn Tonnen transportieren».

Причина одна на весь класс: человек искал ПО-РУССКИ, разбор собрался «русский →
немецкий» и лёг как есть на НЕМЕЦКОЕ слово. Порчи в данных нет — разбор лежит лицом не в
ту сторону, а выдача читает его как «немецкий → русский».

Замер 21.08.2026 по немецким словам: 10 338 с разбором, перевёрнутых 466 — у 386
развёрнут весь разбор, у 80 заголовок верный и зеркальны только примеры. Поэтому правил
ДВА, и второе не является частью первого: развернуть весь разбор там, где зеркальны лишь
примеры, значило бы испортить верный заголовок.
"""
from backend.lex_units import (
    card_is_facing_away,
    orient_card_to_unit_language,
    orient_examples_to_unit_language,
)

RU_TO_DE_CARD = {
    "word_source": "Диаграмма",
    "word_target": "das Diagramm",
    "source_lang": "ru",
    "target_lang": "de",
    "language_pair": {"code": "ru-de", "source_lang": "ru", "target_lang": "de"},
    "translations": [{"value": "das Diagramm", "is_primary": True}],
    "usage_examples": [{"source": "Диаграмма показывает рост.",
                        "target": "Das Diagramm zeigt das Wachstum."}],
    "article": "das",
}


class TestWholeCardIsTurned:
    def test_a_russian_faced_card_on_a_german_word_is_recognised(self):
        assert card_is_facing_away(RU_TO_DE_CARD, "de")

    def test_turning_puts_german_on_the_left(self):
        turned = orient_card_to_unit_language(RU_TO_DE_CARD, "de")
        assert turned["word_source"] == "das Diagramm"
        assert turned["word_target"] == "Диаграмма"
        assert turned["source_lang"] == "de" and turned["target_lang"] == "ru"
        assert turned["language_pair"]["code"] == "de-ru"
        assert turned["usage_examples"][0]["source"] == "Das Diagramm zeigt das Wachstum."
        assert turned["usage_examples"][0]["target"] == "Диаграмма показывает рост."

    def test_the_translation_becomes_the_native_side(self):
        # Список значений хранил НЕМЕЦКИЕ слова — это же был перевод русского запроса.
        # Держать их под видом русских значений нельзя.
        turned = orient_card_to_unit_language(RU_TO_DE_CARD, "de")
        assert turned["translations"] == [
            {"value": "Диаграмма", "context": "", "is_primary": True}
        ]

    def test_a_correct_card_is_returned_untouched(self):
        good = {"word_source": "das Diagramm", "word_target": "диаграмма",
                "usage_examples": [{"source": "Das Diagramm zeigt.", "target": "Диаграмма показывает."}]}
        assert orient_card_to_unit_language(good, "de") is good

    def test_one_sided_evidence_is_not_enough(self):
        # Обе стороны по-немецки — это не перевёрнутый разбор, это разбор одноязычный.
        both_german = {"word_source": "das Diagramm", "word_target": "die Grafik"}
        assert not card_is_facing_away(both_german, "de")
        # И наоборот: пустая вторая сторона уликой не считается.
        assert not card_is_facing_away({"word_source": "Диаграмма"}, "de")


class TestOnlyExamplesAreMirrored:
    def test_examples_are_turned_without_touching_a_correct_headword(self):
        card = {
            "word_source": "Wir werden höhere Kosten haben.",
            "word_target": "У нас будут более высокие расходы.",
            "usage_examples": [{"source": "Компания понесла большие затраты.",
                                "target": "Das Unternehmen hat hohe Kosten getragen."}],
        }
        assert not card_is_facing_away(card, "de"), "заголовок здесь верный"
        fixed = orient_examples_to_unit_language(card, "de")
        assert fixed["word_source"] == card["word_source"], "заголовок трогать нельзя"
        assert fixed["usage_examples"][0]["source"] == "Das Unternehmen hat hohe Kosten getragen."
        assert fixed["usage_examples"][0]["target"] == "Компания понесла большие затраты."

    def test_each_example_is_judged_on_its_own(self):
        card = {"usage_examples": [
            {"source": "Das ist gut.", "target": "Это хорошо."},
            {"source": "Это плохо.", "target": "Das ist schlecht."},
        ]}
        fixed = orient_examples_to_unit_language(card, "de")
        assert fixed["usage_examples"][0]["source"] == "Das ist gut."
        assert fixed["usage_examples"][1]["source"] == "Das ist schlecht."

    def test_correct_examples_are_returned_untouched(self):
        card = {"usage_examples": [{"source": "Das ist gut.", "target": "Это хорошо."}]}
        assert orient_examples_to_unit_language(card, "de") is card

    def test_a_card_without_examples_is_untouched(self):
        card = {"word_source": "das Haus"}
        assert orient_examples_to_unit_language(card, "de") is card
