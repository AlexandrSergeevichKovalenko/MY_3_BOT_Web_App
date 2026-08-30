"""Разбор выделенного слова в видео и читалке — это ОБЩАЯ карточка словаря.

Повод, 30.08.2026. У попапа выделения был свой, урезанный путь к модели: промпт
`dictionary_assistant_multilang_reader`, склейка ответа в текст с эмодзи на бэкенде и
парсер примеров регуляркой на фронте. На экране это дало сразу четыре дефекта —
немецкое слово в строке «Перевод», служебная метка «Примеры:», примеры дважды и
«типичные сочетания», нарезанные окном ±1 слово вокруг искомого («Das war»).

Эти тесты держат ДВЕ вещи:
  1) второй разбор не вернётся — ни функцией, ни промптом, ни веткой эндпоинта;
  2) стражи примеров, которые раньше стояли только на пути попапа, теперь работают на
     ОБЩЕЙ выдаче карточки, то есть во всём словаре.
"""

import unittest
from contextlib import ExitStack
from unittest.mock import patch

import backend.backend_server as server
import backend.openai_manager as openai_manager


class SecondBreakdownIsGoneTests(unittest.TestCase):
    """Второго разбора одного слова в приложении быть не должно."""

    def test_reader_prompt_is_gone(self):
        self.assertNotIn("dictionary_assistant_multilang_reader", openai_manager.system_message)

    def test_reader_lookup_function_is_gone(self):
        self.assertFalse(hasattr(openai_manager, "run_dictionary_lookup_multilang_reader"))

    def test_selection_text_formatter_is_gone(self):
        self.assertFalse(hasattr(server, "_format_selection_dictionary_explanation"))

    def test_explain_no_longer_serves_selection_context(self):
        """Ветка mode=selection_context убрана: без перевода эндпоинт честно отказывает.

        Раньше именно этот режим пускали БЕЗ user_translation — на нём и держался
        отдельный путь попапа."""
        client = server.app.test_client()
        with ExitStack() as stack:
            stack.enter_context(patch.object(server, "WEBAPP_SINGLE_INSTANCE_GUARD_ENABLED", False))
            stack.enter_context(patch.object(server, "_telegram_hash_is_valid", return_value=True))
            stack.enter_context(
                patch.object(server, "_parse_telegram_init_data", return_value={"user": {"id": 77}})
            )
            response = client.post(
                "/api/webapp/explain",
                json={
                    "initData": "signed",
                    "mode": "selection_context",
                    "original_text": "millionenfach",
                },
            )
        self.assertEqual(response.status_code, 400)


class ExampleGuardsOnServedCardTests(unittest.TestCase):
    """Стражи примеров на карточке, которая уходит человеку."""

    @staticmethod
    def _card(examples):
        return {
            "source_text": "millionenfach",
            "target_text": "в миллионы раз",
            "language_pair": {"source_lang": "ru", "target_lang": "de"},
            "usage_examples": examples,
        }

    def test_untranslated_headword_in_native_half_is_dropped(self):
        """«разные Auffassungen» — модель перевела всё, кроме того, что спросили."""
        card = self._card([{
            "source": "Es gibt verschiedene Auffassungen.",
            "target": "Есть разные Auffassungen.",
        }])
        card["source_text"] = "Auffassung"
        card["target_text"] = "взгляд"
        out = server.sanitize_dictionary_item_examples(card, native_lang="ru")
        example = out["usage_examples"][0]
        self.assertEqual(example["source"], "Es gibt verschiedene Auffassungen.")
        self.assertEqual(example["target"], "")

    def test_half_translated_native_sentence_is_dropped(self):
        """«На дороге внезапно rast ein LKW» — перевод брошен на середине."""
        card = self._card([{
            "source": "Auf der Straße rast ein LKW.",
            "target": "На дороге внезапно rast ein LKW.",
        }])
        out = server.sanitize_dictionary_item_examples(card, native_lang="ru")
        example = out["usage_examples"][0]
        self.assertEqual(example["source"], "Auf der Straße rast ein LKW.")
        self.assertEqual(example["target"], "")

    def test_cyrillic_inside_learning_sentence_drops_the_example(self):
        """Немецкое предложение с вклинившейся кириллицей читать нельзя."""
        card = self._card([
            {"source": "Ich sehe dich auch так.", "target": "Я тоже тебя вижу."},
            {"source": "Das Haus ist groß.", "target": "Дом большой."},
        ])
        out = server.sanitize_dictionary_item_examples(card, native_lang="ru")
        self.assertEqual(len(out["usage_examples"]), 1)
        self.assertEqual(out["usage_examples"][0]["source"], "Das Haus ist groß.")

    def test_clean_example_is_left_alone(self):
        card = self._card([{
            "source": "Dieses Video wurde millionenfach angesehen.",
            "target": "Это видео посмотрели миллионы раз.",
        }])
        out = server.sanitize_dictionary_item_examples(card, native_lang="ru")
        self.assertEqual(out["usage_examples"][0]["target"], "Это видео посмотрели миллионы раз.")

    def test_loanword_headword_is_not_stripped_from_translation(self):
        """Netflix и по-русски Netflix — это не «непереведённое слово»."""
        card = {
            "source_text": "Netflix",
            "target_text": "Netflix",
            "language_pair": {"source_lang": "ru", "target_lang": "de"},
            "usage_examples": [{
                "source": "Ich schaue Netflix jeden Abend.",
                "target": "Я смотрю Netflix каждый вечер.",
            }],
        }
        out = server.sanitize_dictionary_item_examples(card, native_lang="ru")
        self.assertEqual(out["usage_examples"][0]["target"], "Я смотрю Netflix каждый вечер.")

    def test_sides_are_found_by_script_not_by_field_names(self):
        """Модель иногда меняет source и target местами — стражи это переживают."""
        card = self._card([{
            "source": "Есть разные Auffassungen.",
            "target": "Es gibt verschiedene Auffassungen.",
        }])
        card["source_text"] = "Auffassung"
        card["target_text"] = "взгляд"
        out = server.sanitize_dictionary_item_examples(card, native_lang="ru")
        example = out["usage_examples"][0]
        self.assertEqual(example["source"], "")
        self.assertEqual(example["target"], "Es gibt verschiedene Auffassungen.")

    def test_non_cyrillic_native_language_is_left_alone(self):
        """Родной язык не кириллический — стороны различить нечем, и мы не гадаем."""
        card = {
            "source_text": "millionenfach",
            "target_text": "a million times",
            "language_pair": {"source_lang": "en", "target_lang": "de"},
            "usage_examples": [{
                "source": "Dieses Video wurde millionenfach angesehen.",
                "target": "This video was viewed millionenfach.",
            }],
        }
        out = server.sanitize_dictionary_item_examples(card, native_lang="en")
        self.assertEqual(out["usage_examples"][0]["target"], "This video was viewed millionenfach.")

    def test_meanings_examples_are_guarded_too(self):
        card = self._card([])
        card["source_text"] = "Auffassung"
        card["target_text"] = "взгляд"
        card["meanings"] = {
            "primary": {
                "example_source": "Es gibt verschiedene Auffassungen.",
                "example_target": "Есть разные Auffassungen.",
            },
            "secondary": [{
                "example_source": "Das Haus ist groß.",
                "example_target": "Дом большой.",
            }],
        }
        out = server.sanitize_dictionary_item_examples(card, native_lang="ru")
        self.assertEqual(out["meanings"]["primary"]["example_target"], "")
        self.assertEqual(out["meanings"]["secondary"][0]["example_target"], "Дом большой.")


class ServedCardRunsTheGuardsTests(unittest.TestCase):
    """Страж обязан стоять в ЕДИНСТВЕННОЙ точке выдачи, а не у одного вызывающего."""

    def test_serve_dictionary_item_applies_example_guards(self):
        card = {
            "source_text": "Auffassung",
            "target_text": "взгляд",
            "language_pair": {"source_lang": "ru", "target_lang": "de"},
            "usage_examples": [{
                "source": "Es gibt verschiedene Auffassungen.",
                "target": "Есть разные Auffassungen.",
            }],
        }
        served = server._serve_dictionary_item(card)
        self.assertEqual(served["usage_examples"][0]["target"], "")


if __name__ == "__main__":
    unittest.main()
