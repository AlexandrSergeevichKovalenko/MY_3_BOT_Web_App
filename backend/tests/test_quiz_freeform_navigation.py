import unittest

import bot_3


class QuizFreeformNavigationTests(unittest.TestCase):
    def test_navigation_buttons_are_not_treated_as_freeform_answers(self):
        self.assertTrue(bot_3._is_quiz_freeform_navigation_text(bot_3.ARTIKEL_LEARN_BUTTON_TEXT))
        self.assertTrue(bot_3._is_quiz_freeform_navigation_text(bot_3.LANGUAGE_TUTOR_BUTTON_TEXT))
        self.assertTrue(bot_3._is_quiz_freeform_navigation_text("📌 Выбрать тему"))
        self.assertFalse(bot_3._is_quiz_freeform_navigation_text("Учить артикли"))


if __name__ == "__main__":
    unittest.main()
