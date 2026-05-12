import unittest

import backend.backend_server as server


class FlashcardFeelKeyboardTests(unittest.TestCase):
    def test_flashcard_feel_reply_markup_includes_feedback_and_followup(self):
        markup = server._build_flashcard_feel_reply_markup("abc123")

        self.assertEqual(
            markup,
            {
                "inline_keyboard": [
                    [
                        {"text": "👍 Like", "callback_data": "feelfb:abc123:like"},
                        {"text": "👎 Dislike", "callback_data": "feelfb:abc123:dislike"},
                    ],
                    [
                        {"text": "❓ Задать вопрос", "callback_data": "langgpt:continue"},
                    ],
                ]
            },
        )


if __name__ == "__main__":
    unittest.main()
