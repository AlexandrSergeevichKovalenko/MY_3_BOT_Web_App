import unittest

from backend.openai_manager import system_message


class OpenAIManagerPromptingTests(unittest.TestCase):
    def test_quiz_followup_prompt_prefers_natural_collocations(self) -> None:
        prompt = system_message["quiz_followup_question"]
        self.assertIn("Prefer a natural collocation over a generic example sentence", prompt)
        self.assertIn("Never output broken fragments, dictionary-style fragments, or artificial textbook phrases.", prompt)
        self.assertIn("Avoid trivial toy examples unless they are the only natural option.", prompt)

    def test_private_question_prompt_prefers_natural_collocations(self) -> None:
        prompt = system_message["language_learning_private_question"]
        self.assertIn("Prefer a natural collocation over a generic example sentence", prompt)
        self.assertIn("If possible, make at least one save_variant a compact collocation", prompt)

    def test_detailed_prompt_mentions_natural_collocations(self) -> None:
        prompt = system_message["language_learning_private_question_detailed"]
        self.assertIn("Prefer natural collocations and characteristic real usage", prompt)
        self.assertIn("Never output broken fragments, literal translations, or awkward artificial phrases.", prompt)


if __name__ == "__main__":
    unittest.main()
