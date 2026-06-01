import unittest

import backend.backend_server as server
import bot_3


class ShortcutOnboardingCopyTests(unittest.TestCase):
    def test_shortcut_code_message_uses_24h_copy(self):
        text = server._build_shortcut_onboarding_code_text(pairing_code="69G6ZW")
        self.assertIn("📱 Connect Shortcut", text)
        self.assertIn("Скопируйте только код ниже:", text)
        self.assertIn("69G6ZW", text)
        self.assertIn("24", text)
        self.assertNotIn("10 минут", text)

    def test_shortcut_instructions_are_plain_language(self):
        text = server._build_shortcut_onboarding_instructions()
        self.assertIn("Как пользоваться:", text)
        self.assertIn("Один раз подключите Shortcut", text)
        self.assertIn("Код нужен только при первом запуске", text)
        self.assertIn("Можно переслать сюда немецкий текст", text)
        self.assertIn("🇩🇪➡️🇷🇺 Быстрый перевод", text)
        self.assertNotIn("install_token", text)
        self.assertNotIn("POST", text)

    def test_start_copy_explains_next_steps(self):
        text = bot_3._build_private_start_onboarding_text()
        self.assertIn("Что умеет бот:", text)
        self.assertIn("Нажмите «📱 Connect Shortcut»", text)
        self.assertIn("Код нужен только при первом запуске", text)
        self.assertIn("Если слов много", text)


if __name__ == "__main__":
    unittest.main()
