import unittest

from backend.background_jobs import validate_visual_riddle_blueprint
from backend.rebus_bank import validate_rebus_entry_consistency
from backend.rebus_generator import _validate_replenishment_entry


def _base_payload(**overrides):
    payload = {
        "quiz_type": "VISUAL_WORD_REBUS",
        "difficulty": "A2",
        "target_skill": "vocabulary",
        "target_word_or_phrase": "Handschuh",
        "compound_parts": ["Hand", "Schuh"],
        "question_text": "Diese zwei Bilder ergeben zusammen ein deutsches Wort. Welches Wort ist das?",
        "image_prompt": (
            "Detailed illustration, soft watercolor style, warm pastel tones, no text: "
            "on the left a clear human hand, on the right a single leather shoe."
        ),
        "answers": [
            {"id": "A", "text": "Handschuh", "is_correct": True},
            {"id": "B", "text": "Hausschuh", "is_correct": False},
            {"id": "C", "text": "Handtuch", "is_correct": False},
            {"id": "D", "text": "Handtasche", "is_correct": False},
        ],
        "correct_answer_id": "A",
        "short_explanation": "Hand plus Schuh ergibt Handschuh.",
    }
    payload.update(overrides)
    return payload


class VisualRiddleValidationTests(unittest.TestCase):
    def test_visual_word_rebus_requires_real_compound_parts(self):
        with self.assertRaisesRegex(ValueError, "part_not_in_target"):
            validate_visual_riddle_blueprint(
                _base_payload(
                    target_word_or_phrase="Fernseher",
                    compound_parts=["Mann", "Fernseher"],
                    answers=[
                        {"id": "A", "text": "Fernbus", "is_correct": False},
                        {"id": "B", "text": "Fernseher", "is_correct": True},
                        {"id": "C", "text": "Fernglas", "is_correct": False},
                        {"id": "D", "text": "Fernbedienung", "is_correct": False},
                    ],
                    correct_answer_id="B",
                )
            )

    def test_visual_word_rebus_accepts_valid_compound_parts(self):
        result = validate_visual_riddle_blueprint(_base_payload())
        self.assertEqual(result["compound_parts"], ["Hand", "Schuh"])

    def test_rebus_bank_rejects_opaque_zuckerhut(self):
        err = validate_rebus_entry_consistency(
            {
                "id": "zuckerhut_001",
                "compound": "Zuckerhut",
                "parts": [
                    {"word": "Zucker", "meaning_ru": "сахар"},
                    {"word": "Hut", "meaning_ru": "шляпа"},
                ],
            }
        )
        self.assertIn("blocked opaque compound", err or "")

    def test_gpt_replenishment_rejects_opaque_zuckerhut(self):
        err = _validate_replenishment_entry(
            {
                "id": "zuckerhut_001",
                "compound": "Zuckerhut",
                "article": "der",
                "parts": [
                    {"word": "Zucker", "meaning_ru": "сахар"},
                    {"word": "Hut", "meaning_ru": "шляпа"},
                ],
                "dalle_prompts": {
                    "Zucker": "A bowl of white sugar, no text",
                    "Hut": "A classic wide-brimmed hat, no text",
                },
                "wrong_options": ["Zuckerstange", "Strohhut", "Zuckerbrot"],
            },
            existing_set=set(),
        )
        self.assertIn("blocked opaque compound", err or "")


if __name__ == "__main__":
    unittest.main()
