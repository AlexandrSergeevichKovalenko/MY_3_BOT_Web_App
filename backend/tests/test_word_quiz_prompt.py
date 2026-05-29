import unittest

from backend.openai_manager import system_message


class WordQuizPromptTests(unittest.TestCase):
    def test_translation_quiz_distractors_must_be_grammar_errors_not_paraphrases(self):
        prompt = system_message["generate_word_quiz"]

        self.assertIn("wrong options MUST be minimally changed versions", prompt)
        self.assertIn("must NOT be correct paraphrases", prompt)
        self.assertIn("grammatical and semantically acceptable", prompt)
        self.assertIn("Trotz dem in der Bestätigung genannten Datum", prompt)
        self.assertIn("Ungeachtet des Datums", prompt)


if __name__ == "__main__":
    unittest.main()
