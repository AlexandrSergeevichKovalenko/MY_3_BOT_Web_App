import unittest

from backend.background_jobs import validate_visual_riddle_blueprint


def _valid_blueprint() -> dict:
    return {
        "quiz_type": "SITUATIONAL_REBUS",
        "difficulty": "B1",
        "target_language": "German",
        "target_skill": "vocabulary",
        "target_word_or_phrase": "etwas aus den Augen verlieren",
        "title": "Не теряй из виду",
        "telegram_caption": "🟡 B1 Идиома в картинке",
        "question_text": "Welcher deutsche Ausdruck beschreibt diese Situation genau?",
        "image_prompt": (
            "Detailed illustration, soft watercolor style, warm pastel tones, no text, "
            "showing a person losing sight of an important target in a busy office scene."
        ),
        "answers": [
            {"id": "A", "text": "etwas aus den Augen verlieren", "is_correct": True},
            {"id": "B", "text": "etwas im Blick behalten", "is_correct": False},
            {"id": "C", "text": "etwas vor Augen haben", "is_correct": False},
            {"id": "D", "text": "den Überblick behalten", "is_correct": False},
        ],
        "correct_answer_id": "A",
        "short_explanation": "Правильная идиома описывает ситуацию, когда важная цель исчезает из внимания.",
        "language_explanation": "Так говорят, когда перестают следить за важной темой или целью.",
    }


class VisualRiddleValidationTests(unittest.TestCase):
    def test_rejects_wrong_statement_task(self):
        payload = _valid_blueprint()
        payload["question_text"] = "Welche Aussage ist grammatisch falsch?"

        with self.assertRaisesRegex(ValueError, "blueprint_wrong_statement_task_unsupported"):
            validate_visual_riddle_blueprint(payload)

    def test_rejects_statement_answers_with_quotes(self):
        payload = _valid_blueprint()
        payload["answers"][1]["text"] = "Der Kollege sagt: 'Ich habe das Ziel aus den Augen gelassen.'"

        with self.assertRaisesRegex(ValueError, "blueprint_answer_is_statement_not_answer"):
            validate_visual_riddle_blueprint(payload)

    def test_rejects_explanation_that_mentions_option_letter(self):
        payload = _valid_blueprint()
        payload["short_explanation"] = "Вариант B ошибочен: правильная идиома другая."

        with self.assertRaisesRegex(ValueError, "blueprint_short_explanation_mentions_option_letter"):
            validate_visual_riddle_blueprint(payload)


if __name__ == "__main__":
    unittest.main()
