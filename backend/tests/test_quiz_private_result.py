import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import bot_3 as bot


class _FakeBot:
    def __init__(self) -> None:
        self.messages: list[dict] = []

    async def send_message(self, **kwargs):
        self.messages.append(kwargs)
        return SimpleNamespace(message_id=123)


class QuizPrivateResultTests(unittest.IsolatedAsyncioTestCase):
    async def test_anagram_private_result_uses_question_hint_as_ru_translation(self):
        fake_bot = _FakeBot()
        context = SimpleNamespace(bot=fake_bot)

        with patch.object(bot, "_translate_quiz_text_to_ru", new=AsyncMock(side_effect=AssertionError("should not translate anagram result"))), \
             patch.object(bot, "_build_quiz_result_keyboard", return_value=None), \
             patch.object(bot, "_store_pending_quiz_phrase_request", return_value="phrase-key"), \
             patch.object(bot, "_store_pending_quiz_question_request", return_value="question-key"):
            sent = await bot._send_quiz_result_private(
                context=context,
                user_id=111,
                quiz_data={
                    "quiz_type": "anagram",
                    "correct_text": "Abstrich",
                    "word_ru": "Сокращение",
                    "options": ["🇦 🇧 🇹 🇷 🇸 🇨 🇮 🇭"],
                    "correct_option_id": 0,
                },
                is_correct=True,
                selected_text="🇦 🇧 🇹 🇷 🇸 🇨 🇮 🇭",
            )

        self.assertTrue(sent)
        self.assertEqual(len(fake_bot.messages), 1)
        text = fake_bot.messages[0]["text"]
        self.assertIn("Правильный вариант (DE):</b> Abstrich", text)
        self.assertIn("Перевод (RU):</b> Сокращение", text)
        self.assertNotIn("Мазок", text)

    def test_visual_riddle_explanation_with_option_letter_is_not_display_safe(self):
        self.assertFalse(bot._is_visual_riddle_explanation_display_safe("Вариант B ошибочен: ..."))
        self.assertTrue(
            bot._is_visual_riddle_explanation_display_safe(
                "Правильная идиома описывает потерю фокуса."
            )
        )


if __name__ == "__main__":
    unittest.main()
