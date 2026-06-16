import unittest

from backend import listening_evaluator


class ListeningEvaluatorTests(unittest.TestCase):
    def test_fragmentary_answers_are_rejected(self):
        self.assertTrue(listening_evaluator._is_fragmentary_listening_answer("55 Tage"))
        self.assertTrue(listening_evaluator._is_fragmentary_listening_answer("14 Tage"))
        self.assertTrue(listening_evaluator._is_fragmentary_listening_answer("am Vortag"))

    def test_full_sentence_like_answers_are_allowed(self):
        self.assertFalse(
            listening_evaluator._is_fragmentary_listening_answer(
                "Die Bestellung muss bis 18 Uhr am Vortag aufgegeben werden."
            )
        )


if __name__ == "__main__":
    unittest.main()
