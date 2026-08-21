"""Написание решает СПРАВОЧНИК, а не две модели, согласившиеся друг с другом.

Повод, 21.08.2026. Судья предложил верную правку:

    Er war froh, dass er das Schwein losgeworden war

Проверка правок забраковала её со словами «пишется раздельно: los geworden». Оба её
независимых голоса ошиблись ОДИНАКОВО, поэтому правило «бракуем только при единогласии»
не спасло, и верная правка потеряла кнопку «Принять».

`losgeworden` напечатано в таблице `loswerden` на de.wiktionary — слитно. Спорить с
моделью нечем, со справочником есть чем: если слово, к которому придрались, напечатано
в источнике, придирка снимается.

А если справочника нет? Тогда он ДОСТРАИВАЕТСЯ, а не заменяется догадкой (CLAUDE.md,
правило ноль). Модель называет, на какую страницу смотреть, страница скачивается и
сохраняется, и ответ признаётся ТОЛЬКО если форма на ней напечатана. Модель здесь
указатель, а не источник: ошиблась — подтверждения просто не будет.
"""
import unittest
from unittest.mock import patch

from backend.phrase_night_check import _disputed_words, _reference_confirms_the_wording


class DisputedWordsTests(unittest.TestCase):
    """Какие именно слова проверка хочет заменить. Сравнение списков, не разбор языка."""

    def test_one_word_apart(self):
        self.assertEqual(
            _disputed_words("dass er das Schwein losgeworden war",
                            "dass er das Schwein los geworden war"),
            ["losgeworden"])

    def test_punctuation_at_the_edges_is_not_the_dispute(self):
        self.assertEqual(_disputed_words("Er erlag der Versuchung.",
                                         "Er erlag der Versuchung"), [])

    def test_repeated_word_is_counted_once_per_occurrence(self):
        self.assertEqual(_disputed_words("und und weiter", "und weiter"), ["und"])

    def test_nothing_to_compare_when_the_texts_match(self):
        self.assertEqual(_disputed_words("Ich gehe", "Ich gehe"), [])


class ReferenceBeatsTheCheckerTests(unittest.TestCase):
    def test_documented_form_clears_the_complaint(self):
        with patch("backend.german_verb_paradigms.confirm_form_growing_the_reference",
                   return_value="loswerden") as reference:
            confirmed = _reference_confirms_the_wording(
                "dass er das Schwein losgeworden war",
                ["dass er das Schwein los geworden war"])
        self.assertEqual(confirmed, "loswerden")
        reference.assert_called_once()
        self.assertEqual(reference.call_args.args[0], "losgeworden")

    def test_unknown_form_leaves_the_verdict_alone(self):
        """Справочник промолчал — придирка остаётся в силе. Молчание не согласие."""
        with patch("backend.german_verb_paradigms.confirm_form_growing_the_reference",
                   return_value=""):
            self.assertEqual(
                _reference_confirms_the_wording("Steck das Portemonnaie in den Taschen.",
                                                ["Steck das Portemonnaie in die Taschen."]),
                "")

    def test_half_confirmed_is_not_confirmed(self):
        """Подтвердилось одно слово из двух — снимать придирку целиком нельзя."""
        with patch("backend.german_verb_paradigms.confirm_form_growing_the_reference",
                   side_effect=lambda word, **_kw: "loswerden" if word == "losgeworden" else ""):
            self.assertEqual(
                _reference_confirms_the_wording("er losgeworden wäre",
                                                ["er los geworden waere"]),
                "")

    def test_a_full_rewrite_is_not_a_spelling_dispute(self):
        """Проверка переписала всё целиком — это спор не про написание, справочник молчит."""
        with patch("backend.german_verb_paradigms.confirm_form_growing_the_reference",
                   return_value="loswerden") as reference:
            confirmed = _reference_confirms_the_wording(
                "Ein voellig anderer Satz mit vielen Woertern hier",
                ["Kurz"])
        self.assertEqual(confirmed, "")
        reference.assert_not_called()

    def test_reference_is_never_asked_when_the_checker_gave_no_text(self):
        with patch("backend.german_verb_paradigms.confirm_form_growing_the_reference",
                   return_value="loswerden") as reference:
            self.assertEqual(_reference_confirms_the_wording("Ich gehe", ["", "  "]), "")
        reference.assert_not_called()


class ReferenceGrowsInsteadOfGuessingTests(unittest.TestCase):
    def test_the_page_confirms_the_form_and_the_table_is_stored(self):
        """Модель назвала страницу — форма на ней есть — таблица сохранена навсегда."""
        from backend import german_verb_paradigms as ref
        table = {"partizip2": "aufgeblieben", "praesens": {"ich": "bleibe auf"}}
        with patch.object(ref, "form_is_documented", return_value=""), \
             patch("backend.openai_manager.run_infinitive_of_form", return_value=["aufbleiben"]), \
             patch.object(ref, "load_paradigm", return_value=None), \
             patch.object(ref, "fetch_documented_tables", return_value=table), \
             patch.object(ref, "store_paradigm") as store:
            got = ref.confirm_form_growing_the_reference("aufgeblieben")
        self.assertEqual(got, "aufbleiben")
        store.assert_called_once_with("aufbleiben", table)

    def test_a_wrong_page_confirms_nothing(self):
        """Модель ошиблась страницей — формы там нет — подтверждения нет.

        Это и есть защита: выдумать форму через указатель невозможно, потому что
        последнее слово всегда за напечатанной таблицей.
        """
        from backend import german_verb_paradigms as ref
        with patch.object(ref, "form_is_documented", return_value=""), \
             patch("backend.openai_manager.run_infinitive_of_form", return_value=["gehen"]), \
             patch.object(ref, "load_paradigm", return_value={"partizip2": "gegangen"}), \
             patch.object(ref, "fetch_documented_tables") as fetch:
            got = ref.confirm_form_growing_the_reference("losgeworden")
        self.assertEqual(got, "")
        fetch.assert_not_called()

    def test_a_substring_is_not_a_printed_form(self):
        """«geworden» лежит внутри «losgeworden» — но это НЕ то, что напечатано."""
        from backend.german_verb_paradigms import _printed_words
        words = _printed_words({"perfekt": {"ich": "bin losgeworden"},
                                "partizip2": "losgeworden"})
        self.assertIn("losgeworden", words)
        self.assertIn("bin", words)
        self.assertNotIn("geworden", words)


if __name__ == "__main__":
    unittest.main()
