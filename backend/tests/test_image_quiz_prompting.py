import unittest

from backend.background_jobs import (
    _compose_image_quiz_render_prompt,
    _sanitize_image_quiz_blueprint,
    _select_image_quiz_visual_style,
)


class ImageQuizPromptingTests(unittest.TestCase):
    def test_visual_style_selection_is_deterministic(self):
        style_a = _select_image_quiz_visual_style(
            template_id=42,
            correct_answer="eine Festnahme",
            source_sentence="Die Polizei legt dem Mann Handschellen an.",
        )
        style_b = _select_image_quiz_visual_style(
            template_id=42,
            correct_answer="eine Festnahme",
            source_sentence="Die Polizei legt dem Mann Handschellen an.",
        )

        self.assertEqual(style_a["key"], style_b["key"])
        self.assertEqual(style_a["label"], style_b["label"])

    def test_sanitize_blueprint_keeps_optional_scene_fields(self):
        payload = _sanitize_image_quiz_blueprint(
            {
                "source_sentence": "Die Ärztin untersucht den Arm des Patienten.",
                "image_prompt": "A doctor closely examines a patient's injured arm in a bright clinic room.",
                "scene_core": "Medical examination in a clinic",
                "must_show": ["doctor", "patient arm", "close inspection"],
                "must_not_show": ["operating room", "x-ray screen"],
                "camera_framing": "medium shot focused on the arm and doctor",
                "key_disambiguator": "The doctor is inspecting rather than treating or operating.",
                "question_de": "Welche Handlung sieht man in dieser Szene?",
                "answer_options": [
                    "eine Untersuchung",
                    "eine Operation",
                    "eine Entlassung",
                    "eine Überweisung",
                ],
                "correct_option_index": 0,
                "explanation": "Nur die Untersuchung passt zur sichtbaren Handlung.",
            },
            expected_correct_answer="eine Untersuchung",
            answer_language="de",
        )

        self.assertEqual(payload["scene_core"], "Medical examination in a clinic")
        self.assertIn("doctor", payload["must_show"])
        self.assertIn("operating room", payload["must_not_show"])
        self.assertEqual(payload["camera_framing"], "medium shot focused on the arm and doctor")

    def test_render_prompt_includes_style_and_disambiguation_constraints(self):
        prompt = _compose_image_quiz_render_prompt(
            source_sentence="Ein Mann wischt den Küchentisch mit einem gelben Tuch ab.",
            correct_answer="abwischen",
            answer_language="de",
            visual_style={
                "key": "clean_editorial",
                "label": "clean editorial illustration",
                "guidance": "clean editorial illustration, crisp shapes, highly legible objects and actions",
            },
            blueprint={
                "image_prompt": "A man wipes a kitchen table with a yellow cloth after spilling juice.",
                "scene_core": "Cleaning a table in a kitchen",
                "must_show": ["yellow cloth touching table", "wet spill", "wiping motion"],
                "must_not_show": ["vacuum cleaner", "dishwasher", "multiple rooms"],
                "camera_framing": "medium shot centered on the table and hands",
                "key_disambiguator": "The cloth is actively wiping the tabletop, not washing dishes or sweeping.",
            },
        )

        self.assertIn("Visual style: clean editorial illustration.", prompt)
        self.assertIn("Target answer: abwischen.", prompt)
        self.assertIn("Must show: yellow cloth touching table; wet spill; wiping motion.", prompt)
        self.assertIn("Must not show: vacuum cleaner; dishwasher; multiple rooms.", prompt)
        self.assertIn("Key disambiguator: The cloth is actively wiping the tabletop", prompt)


if __name__ == "__main__":
    unittest.main()
