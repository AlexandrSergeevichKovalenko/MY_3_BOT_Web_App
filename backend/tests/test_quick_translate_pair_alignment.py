"""«Быстрый перевод»: первый вариант — та фраза, которую попросили, с её переводом.

Находка 30.07.2026. Запрос в личке боту: «Er übt eine Erwerbstätigkeit als Lehrer aus».
На экране:

    1. DE: eine Erwerbstätigkeit ausüben
       RU: Он работает учителем.            ← перевод НЕ этой фразы
    2. DE: eine Erwerbstätigkeit als Lehrer ausüben
       RU: работать учителем

Исходной фразы в списке нет вообще, а первая пара собрана из двух разных мест.
Резал наш код, не модель: `_coerce_sentence_lookup_payload` считал ЛЮБОЕ
расхождение ответа модели с запросом исправлением опечатки. Модель вернула
словарную форму («eine Erwerbstätigkeit ausüben») — её положили слева как
«исправленный запрос», а справа встал перевод исходного предложения.

Правило: слева и справа — половинки ОДНОЙ пары. Исправление опечатки — это та же
фраза с мелкими правками; словарная форма вместо фразы — отдельный вариант.
"""

import asyncio
import unittest
from unittest.mock import patch

import bot_3


class TypoCorrectionDetectionTests(unittest.TestCase):
    def test_lemma_instead_of_sentence_is_not_a_correction(self):
        self.assertFalse(bot_3._is_dictionary_typo_correction(
            "Er übt eine Erwerbstätigkeit als Lehrer aus",
            "eine Erwerbstätigkeit ausüben",
        ))

    def test_shortened_collocation_is_not_a_correction(self):
        self.assertFalse(bot_3._is_dictionary_typo_correction(
            "Franz ist in Rente gegangen",
            "in Rente gehen",
        ))

    def test_real_typo_is_a_correction(self):
        self.assertTrue(bot_3._is_dictionary_typo_correction(
            "Er übt eine Erwerbstätigket als Lehrer aus",
            "Er übt eine Erwerbstätigkeit als Lehrer aus",
        ))

    def test_case_and_punctuation_only_is_a_correction(self):
        self.assertTrue(bot_3._is_dictionary_typo_correction(
            "er übt eine erwerbstätigkeit als lehrer aus.",
            "Er übt eine Erwerbstätigkeit als Lehrer aus",
        ))


class QuickTranslateFirstVariantTests(unittest.TestCase):
    QUERY = "Er übt eine Erwerbstätigkeit als Lehrer aus"
    SENTENCE_RU = "Он работает учителем."

    def _coerce(self, model_payload):
        async def _fake_translate(lines, source_lang, target_lang):
            return [self.SENTENCE_RU]

        with patch.object(bot_3, "run_translate_subtitles_multilang", _fake_translate):
            return asyncio.run(bot_3._coerce_sentence_lookup_payload(
                model_payload, self.QUERY, "de", "ru",
            ))

    def test_requested_phrase_stays_the_headword(self):
        """Модель нормализовала фразу в словарную форму — запрос от этого не меняется."""
        lookup = self._coerce({
            "word_source": "eine Erwerbstätigkeit ausüben",
            "word_target": "заниматься трудовой деятельностью",
        })
        self.assertEqual(lookup["word_source"], self.QUERY)
        self.assertEqual(lookup["word_target"], self.SENTENCE_RU)
        self.assertFalse(lookup.get("correction_applied"))
        self.assertIsNone(lookup.get("corrected_form"))

    def test_typo_is_still_corrected(self):
        typo_query = "Er übt eine Erwerbstätigket als Lehrer aus"

        async def _fake_translate(lines, source_lang, target_lang):
            return [self.SENTENCE_RU]

        with patch.object(bot_3, "run_translate_subtitles_multilang", _fake_translate):
            lookup = asyncio.run(bot_3._coerce_sentence_lookup_payload(
                {"word_source": self.QUERY, "word_target": self.SENTENCE_RU},
                typo_query, "de", "ru",
            ))
        self.assertEqual(lookup["word_source"], self.QUERY)
        self.assertTrue(lookup["correction_applied"])
        self.assertEqual(lookup["corrected_form"], self.QUERY)

    def test_first_save_variant_is_the_query_second_is_a_collocation(self):
        lookup = self._coerce({
            "word_source": "eine Erwerbstätigkeit ausüben",
            "word_target": "заниматься трудовой деятельностью",
            "save_worthy_options": [
                # Модель по промпту первым отдаёт сам запрос — он дедупится с вариантом 1.
                {"source": self.QUERY, "target": self.SENTENCE_RU},
                {"source": "eine Erwerbstätigkeit als Lehrer ausüben", "target": "работать учителем"},
            ],
        })
        options = bot_3._build_fast_dictionary_save_options(
            {
                "source_lang": "de",
                "target_lang": "ru",
                "source_text": lookup["word_source"],
                "lookup": lookup,
                "original_query": self.QUERY,
            },
            max_options=2,
        )
        self.assertEqual(options[0]["source"], self.QUERY)
        self.assertEqual(options[0]["target"], self.SENTENCE_RU)
        self.assertTrue(options[0]["is_original"])
        self.assertEqual(len(options), 2)
        self.assertEqual(options[1]["source"], "eine Erwerbstätigkeit als Lehrer ausüben")
        self.assertEqual(options[1]["target"], "работать учителем")


class SecondVariantIsAPhraseTests(unittest.TestCase):
    """Второй вариант — слово в живой речи, а не та же лемма отдельной карточкой.

    Живой вызов модели 30.07.2026 на «Er übt eine Erwerbstätigkeit als Lehrer aus»
    вернул save_worthy_options в таком порядке:
        1) die Erwerbstätigkeit            (kind: base)      ← одинокое слово
        2) eine Erwerbstätigkeit ausüben   (kind: collocation)
        3) eine Erwerbstätigkeit als Lehrer ausüben (kind: phrase)
    Мы брали строго по порядку — и вторым вариантом показывали «die
    Erwerbstätigkeit / трудовая деятельность». Рядом с запросом это пустая карточка.
    """

    QUERY = "Er übt eine Erwerbstätigkeit als Lehrer aus"

    def _options(self, save_worthy, max_options=2):
        lookup = {
            "word_source": self.QUERY,
            "word_target": "Он работает учителем.",
            "save_worthy_options": save_worthy,
        }
        return bot_3._build_fast_dictionary_save_options(
            {"source_lang": "de", "target_lang": "ru",
             "source_text": self.QUERY, "lookup": lookup, "original_query": self.QUERY},
            max_options=max_options,
        )

    def test_bare_lemma_loses_to_a_collocation(self):
        options = self._options([
            {"source": "die Erwerbstätigkeit", "target": "трудовая деятельность", "kind": "base"},
            {"source": "eine Erwerbstätigkeit ausüben", "target": "заниматься трудовой деятельностью"},
            {"source": "eine Erwerbstätigkeit als Lehrer ausüben", "target": "работать учителем"},
        ])
        self.assertEqual(options[0]["source"], self.QUERY)
        self.assertEqual(options[1]["source"], "eine Erwerbstätigkeit ausüben")

    def test_single_word_is_still_offered_when_there_is_no_phrase(self):
        """Фраз нет — лучше слово, чем один вариант на весь экран."""
        options = self._options([
            {"source": "die Erwerbstätigkeit", "target": "трудовая деятельность", "kind": "base"},
        ])
        self.assertEqual(len(options), 2)
        self.assertEqual(options[1]["source"], "die Erwerbstätigkeit")

    def test_phrase_detection(self):
        self.assertFalse(bot_3._looks_like_save_phrase("die Erwerbstätigkeit"))
        self.assertFalse(bot_3._looks_like_save_phrase("Erwerbstätigkeit"))
        self.assertTrue(bot_3._looks_like_save_phrase("eine Erwerbstätigkeit ausüben"))
        self.assertTrue(bot_3._looks_like_save_phrase("in Rente gehen"))


class DefaultOptionPairTests(unittest.TestCase):
    """Подмена «шумной» конструкции меняет пару целиком, а не одну половинку."""

    def test_noisy_source_swaps_both_halves(self):
        option = bot_3._resolve_default_dictionary_option({
            "source_text": "sich um ... kümmern",
            "lookup": {
                "word_source": "sich um ... kümmern",
                "word_target": "заботиться о чём-то",
                "save_worthy_options": [
                    {"source": "sich um die Kinder kümmern", "target": "заботиться о детях"},
                ],
            },
        })
        self.assertEqual(option["source"], "sich um die Kinder kümmern")
        self.assertEqual(option["target"], "заботиться о детях")

    def test_half_option_leaves_the_original_pair_intact(self):
        """У варианта нет перевода — тогда не трогаем ничего: иначе слева одно, справа другое."""
        option = bot_3._resolve_default_dictionary_option({
            "source_text": "sich um ... kümmern",
            "lookup": {
                "word_source": "sich um ... kümmern",
                "word_target": "заботиться о чём-то",
                "save_worthy_options": [
                    {"source": "sich um die Kinder kümmern", "target": ""},
                ],
            },
        })
        self.assertEqual(option["source"], "sich um ... kümmern")
        self.assertEqual(option["target"], "заботиться о чём-то")


if __name__ == "__main__":
    unittest.main()
