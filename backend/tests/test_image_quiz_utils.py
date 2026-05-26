import unittest

from backend.image_quiz_utils import (
    build_image_quiz_feedback_alert,
    build_image_quiz_feedback_payload,
    normalize_image_quiz_option_text,
    validate_ready_image_quiz_template,
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

    def test_build_payload_rejects_when_correct_slot_mismatches_expected_answer(self):
        payload = build_image_quiz_feedback_payload(
            {
                "source_lang": "ru",
                "target_lang": "de",
                "source_text": "оцепенеть",
                "target_text": "in Erstarrung verfallen",
                "answer_options": [
                    "Starker Regen",
                    "Schnee bedeckt den Boden",
                    "Gefrorenes Wasser",
                    "Fließendes Wasser",
                ],
                "correct_option_index": 2,
                "question_de": "Was zeigt das Bild?",
                "explanation": "Nur gefrorenes Wasser passt zur Szene.",
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

    def test_validate_ready_template_rejects_non_visual_relation_answer(self):
        error = validate_ready_image_quiz_template(
            {
                "source_lang": "ru",
                "target_lang": "de",
                "source_text": "вместо Хайнца",
                "target_text": "Anstelle von Heinz",
                "question_de": "Welche Situation ist hier dargestellt?",
                "answer_options": [
                    "Anna sitzt am Tisch.",
                    "Heinz steht am Tisch.",
                    "Anna und Heinz stehen zusammen am Tisch.",
                    "Anstelle von Heinz",
                ],
                "correct_option_index": 3,
            }
        )

        self.assertEqual(error, "non_visual_relation_answer")

    def test_validate_ready_template_rejects_mixed_answer_shapes(self):
        error = validate_ready_image_quiz_template(
            {
                "source_lang": "ru",
                "target_lang": "de",
                "source_text": "Анна стоит у стола",
                "target_text": "Anna steht am Tisch.",
                "question_de": "Welche Situation ist hier dargestellt?",
                "answer_options": [
                    "Anna steht am Tisch.",
                    "Heinz sitzt am Tisch.",
                    "Zusammen mit Heinz",
                    "Anna und Heinz lachen.",
                ],
                "correct_option_index": 0,
            }
        )

        self.assertEqual(error, "mixed_answer_shapes")

    def test_validate_ready_template_rejects_generic_question(self):
        error = validate_ready_image_quiz_template(
            {
                "source_lang": "ru",
                "target_lang": "de",
                "source_text": "корабль причаливает",
                "target_text": "Ein Schiff legt an",
                "question_de": "Was zeigt das Bild?",
                "answer_options": [
                    "Ein Schiff legt an",
                    "Ein Schiff fährt davon",
                    "Ein Schiff sinkt heute",
                    "Ein Schiff wird repariert",
                ],
                "correct_option_index": 0,
            }
        )

        self.assertEqual(error, "generic_question")


if __name__ == "__main__":
    unittest.main()
