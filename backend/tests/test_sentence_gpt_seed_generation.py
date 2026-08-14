import unittest
from unittest.mock import patch

import backend.backend_server as server


def _valid_quiz() -> dict:
    """Задание нового формата: два пропуска — спрягаемая форма и приставка.
    Именно так отделяемый глагол и стоит в предложении."""
    return {
        "quiz_type": "separable_prefix_verb_gap",
        "level": "B1-B2",
        "topic": "work",
        "sentence_with_gap": "Ich ___ mein Geld in Immobilien ___.",
        "correct_full_sentence": "Ich lege mein Geld in Immobilien an.",
        "translation_ru": "Я вкладываю свои деньги в недвижимость.",
        "options": ["anlegen", "ausgeben", "umlegen", "vorlegen"],
        "correct_index": 1,
        "correct_infinitive": "anlegen",
        "verb_form": "lege",
        "prefix": "an",
        "base_verb": "legen",
        "explanation_de": "Anlegen bedeutet, Geld sinnvoll zu investieren.",
    }


class SentenceGptSeedGenerationTests(unittest.TestCase):
    def test_sentence_gpt_seed_generation_persists_separable_prefix_quiz(self):
        quiz = server._validate_separable_prefix_quiz_item(_valid_quiz())

        # Судью живости обязательно глушим: тест не должен ходить в OpenAI.
        with patch.object(server, "_get_separable_prefix_quiz_item_with_retry", return_value=quiz), \
             patch.object(server, "separable_sentence_sounds_native", return_value=True), \
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
        self.assertEqual(response_json["verb_form"], "lege")
        self.assertEqual(response_json["target_text"], "anlegen")
        self.assertEqual(entry["target_text"], "anlegen")
        save_mock.assert_called_once()
        self.assertEqual(save_mock.call_args.kwargs["translation_de"], "anlegen")
        self.assertEqual(save_mock.call_args.kwargs["word_de"], "anlegen")


class UnnaturalSentenceIsNotSavedTests(unittest.TestCase):
    """Форма может сойтись, а предложение всё равно быть мёртвым: «Er steht jeden
    Morgen pünktlich zur Arbeit auf» так не говорят. Такое в базу не попадает."""

    def test_unnatural_sentence_never_reaches_the_pool(self):
        quiz = server._validate_separable_prefix_quiz_item(_valid_quiz())
        with patch.object(server, "_get_separable_prefix_quiz_item_with_retry", return_value=quiz), \
             patch.object(server, "separable_sentence_sounds_native", return_value=False), \
             patch.object(server, "save_webapp_dictionary_query_returning_id") as save_mock:
            entries = server._ensure_sentence_gpt_seed_entries(
                user_id=117649764,
                source_lang="ru",
                target_lang="de",
                existing_entries=[],
                max_generate_per_call=1,
            )
        self.assertEqual(entries, [])
        save_mock.assert_not_called()

    def test_judge_without_api_key_rejects(self):
        """Судья недоступен — предложение считается негодным. Пустой банк лучше
        банка, который учит несуществующему немецкому."""
        with patch.object(server, "OPENAI_API_KEY", ""):
            self.assertFalse(server.separable_sentence_sounds_native(
                "Er nimmt die neuen Aufgaben sofort an.",
                "Он сразу принимает новые задачи.", "annehmen"))


class SeparablePrefixGapGuardTests(unittest.TestCase):
    """Страж, которого не было до 14.08.2026: подставь спрягаемую форму в первый
    пропуск и приставку во второй — обязано выйти правильное предложение."""

    def test_valid_item_passes(self):
        payload = server._validate_separable_prefix_quiz_item(_valid_quiz())
        self.assertEqual(payload["sentence_with_gap"], "Ich ___ mein Geld in Immobilien ___.")
        self.assertEqual(payload["verb_form"], "lege")

    def test_single_gap_with_hidden_prefix_is_rejected(self):
        """Ровно тот брак, что уехал в прод: один пропуск, а в ответе инфинитив.
        «Ich anlegen mein Geld in Immobilien» — такого немецкого не бывает."""
        broken = _valid_quiz()
        broken["sentence_with_gap"] = "Ich ___ mein Geld in Immobilien."
        with self.assertRaises(ValueError):
            server._validate_separable_prefix_quiz_item(broken)

    def test_gap_that_does_not_reconstruct_is_rejected(self):
        broken = _valid_quiz()
        broken["correct_full_sentence"] = "Ich lege heute mein Geld in Immobilien an."
        with self.assertRaises(ValueError):
            server._validate_separable_prefix_quiz_item(broken)

    def test_infinitive_in_verb_slot_is_rejected(self):
        broken = _valid_quiz()
        broken["verb_form"] = "anlegen"
        broken["correct_full_sentence"] = "Ich anlegen mein Geld in Immobilien an."
        with self.assertRaises(ValueError):
            server._validate_separable_prefix_quiz_item(broken)

    def test_prefix_must_close_the_frame(self):
        broken = _valid_quiz()
        broken["sentence_with_gap"] = "Ich ___ ___ mein Geld in Immobilien."
        broken["correct_full_sentence"] = "Ich lege an mein Geld in Immobilien."
        with self.assertRaises(ValueError):
            server._validate_separable_prefix_quiz_item(broken)

    def test_legacy_stored_entry_is_not_sound(self):
        """Запись 25504 из прода — как она лежит в базе сегодня."""
        legacy = {
            "quiz_type": "separable_prefix_verb_gap",
            "sentence_with_gap": "Er ___ die neuen Aufgaben sofort.",
            "correct_full_sentence": "Er nimmt die neuen Aufgaben sofort an.",
            "correct_infinitive": "annehmen",
            "prefix": "an",
        }
        self.assertFalse(server.separable_gap_entry_is_sound(legacy))

    def test_new_stored_entry_is_sound(self):
        self.assertTrue(server.separable_gap_entry_is_sound({
            "quiz_type": "separable_prefix_verb_gap",
            "sentence_with_gap": "Er ___ die neuen Aufgaben sofort ___.",
            "correct_full_sentence": "Er nimmt die neuen Aufgaben sofort an.",
            "correct_infinitive": "annehmen",
            "verb_form": "nimmt",
            "prefix": "an",
        }))


class GapReconstructionRuleTests(unittest.TestCase):
    def test_single_filler(self):
        self.assertTrue(server.gap_reconstructs_sentence(
            "Er ___ die neuen Aufgaben sofort an.", "nimmt",
            "Er nimmt die neuen Aufgaben sofort an.",
        ))

    def test_two_fillers(self):
        self.assertTrue(server.gap_reconstructs_sentence(
            "Er ___ die neuen Aufgaben sofort ___.", ["nimmt", "an"],
            "Er nimmt die neuen Aufgaben sofort an.",
        ))

    def test_filler_count_must_match_gap_count(self):
        self.assertFalse(server.gap_reconstructs_sentence(
            "Er ___ die neuen Aufgaben sofort ___.", ["nimmt"],
            "Er nimmt die neuen Aufgaben sofort an.",
        ))

    def test_empty_filler_rejected(self):
        self.assertFalse(server.gap_reconstructs_sentence(
            "Er ___ die neuen Aufgaben sofort ___.", ["nimmt", ""],
            "Er nimmt die neuen Aufgaben sofort an.",
        ))


if __name__ == "__main__":
    unittest.main()
