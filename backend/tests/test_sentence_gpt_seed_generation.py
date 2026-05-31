import unittest
from unittest.mock import patch

import backend.backend_server as server


class SentenceGptSeedGenerationTests(unittest.TestCase):
    def test_sentence_gpt_seed_generation_persists_separable_prefix_quiz(self):
        quiz = {
            "quiz_type": "separable_prefix_verb_gap",
            "level": "B1-B2",
            "topic": "work",
            "sentence_with_gap": "Ich ___ mein Geld in Immobilien.",
            "correct_full_sentence": "Ich lege mein Geld in Immobilien an.",
            "translation_ru": "Я вкладываю свои деньги в недвижимость.",
            "options": ["anlegen", "ausgeben", "umlegen", "vorlegen"],
            "correct_index": 1,
            "correct_infinitive": "anlegen",
            "prefix": "an",
            "base_verb": "legen",
            "explanation_de": "Anlegen bedeutet, Geld sinnvoll zu investieren.",
        }

        with patch.object(server, "_get_separable_prefix_quiz_item_with_retry", return_value=quiz), \
             patch.object(server, "save_webapp_dictionary_query_returning_id", return_value=321) as save_mock:
            entries = server._ensure_sentence_gpt_seed_entries(
                user_id=117649764,
                source_lang="ru",
                target_lang="de",
                existing_entries=[],
                max_generate_per_call=1,
            )

        self.assertEqual(len(entries), 1)
        entry = entries[0]
        response_json = entry["response_json"]
        self.assertEqual(response_json["quiz_type"], "separable_prefix_verb_gap")
        self.assertEqual(response_json["correct_infinitive"], "anlegen")
        self.assertEqual(response_json["target_text"], "anlegen")
        self.assertEqual(entry["target_text"], "anlegen")
        save_mock.assert_called_once()
        self.assertEqual(save_mock.call_args.kwargs["translation_de"], "anlegen")
        self.assertEqual(save_mock.call_args.kwargs["word_de"], "anlegen")


if __name__ == "__main__":
    unittest.main()
