import unittest

from backend.image_quiz_utils import (
    build_image_quiz_feedback_alert,
    build_image_quiz_feedback_payload,
    normalize_image_quiz_option_text,
)


class ImageQuizUtilsTests(unittest.TestCase):
    def test_build_payload_accepts_valid_german_options(self):
        payload = build_image_quiz_feedback_payload(
            {
                "answer_options": [
                    "Ein Schiff legt an",
                    "Ein Schiff fährt weg",
                    "Ein Schiff sinkt",
                    "Ein Schiff wird repariert",
                ],
                "correct_option_index": 0,
                "source_text": "корабль причаливает",
                "question_de": "Was zeigt das Bild?",
                "explanation": "Das Schiff ist am Kai und kommt an.",
            }
        )

        self.assertIsNotNone(payload)
        self.assertEqual(payload["correct_option_id"], 0)
        self.assertEqual(payload["correct_text"], "Ein Schiff legt an")
        self.assertEqual(payload["word_ru"], "корабль причаливает")

    def test_build_payload_rejects_cyrillic_option(self):
        payload = build_image_quiz_feedback_payload(
            {
                "answer_options": [
                    "использовать машинку для удаления катышков",
                    "einen Pullover waschen",
                    "einen Pullover bügeln",
                    "einen Pullover nähen",
                ],
                "correct_option_index": 1,
            }
        )

        self.assertIsNone(payload)

    def test_build_payload_rejects_duplicate_options_after_normalization(self):
        payload = build_image_quiz_feedback_payload(
            {
                "answer_options": [
                    "Ein Schiff fährt weg",
                    "  Ein   Schiff   fährt   weg  ",
                    "Ein Schiff sinkt",
                    "Ein Schiff wird repariert",
                ],
                "correct_option_index": 0,
            }
        )

        self.assertIsNone(payload)

    def test_feedback_alert_for_wrong_answer_includes_correct_option(self):
        alert = build_image_quiz_feedback_alert(
            is_correct=False,
            correct_text="Ein Schiff legt an",
            answer_accepted=True,
        )

        self.assertIn("Неверно", alert)
        self.assertIn("Ein Schiff legt an", alert)

    def test_feedback_alert_marks_duplicate_answer(self):
        alert = build_image_quiz_feedback_alert(
            is_correct=True,
            correct_text="Ein Schiff legt an",
            answer_accepted=False,
        )

        self.assertIn("Ответ уже был принят", alert)
        self.assertIn("Верно", alert)

    def test_option_normalization_collapses_whitespace(self):
        self.assertEqual(
            normalize_image_quiz_option_text("  Ein   Schiff   legt   an  "),
            "Ein Schiff legt an",
        )


if __name__ == "__main__":
    unittest.main()
