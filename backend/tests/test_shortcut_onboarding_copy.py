"""Onboarding copy and keyboard for the iPhone Shortcut.

These tests deliberately do NOT pin whole sentences — copy gets polished, and a test that
breaks on every wording change teaches people to ignore it. They pin what must not slip:
plain language instead of developer jargon, and the actions a newcomer has to find.
"""

import unittest
from unittest.mock import patch

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

    def test_shortcut_instructions_speak_human(self):
        text = server._build_shortcut_onboarding_instructions()
        # What the user must learn from this message.
        self.assertIn("код", text.lower())
        self.assertIn("Shortcut", text)
        self.assertRegex(text, r"перв\w+ запуск")       # the code is needed only once
        # What must never leak into it.
        for jargon in ("install_token", "POST", "endpoint", "API", "JSON"):
            self.assertNotIn(jargon, text)

    def test_start_copy_explains_what_the_bot_does_and_what_to_press(self):
        text = bot_3._build_private_start_onboarding_text()
        self.assertIn("Что умеет бот:", text)
        self.assertIn("Shortcut", text)
        self.assertIn("словар", text.lower())
        for jargon in ("install_token", "POST", "endpoint", "webhook"):
            self.assertNotIn(jargon, text)

    def test_install_keyboard_offers_both_shortcuts_and_pairing(self):
        """The shortcut is TWO commands now (a collector and the nightly processor), so the
        keyboard must offer both installs plus the one-code pairing."""
        with patch.object(bot_3, "_shortcut_collector_install_url", return_value="https://icloud.test/collector"), \
             patch.object(bot_3, "_shortcut_processor_install_url", return_value="https://icloud.test/processor"):
            markup = bot_3._build_shortcut_install_keyboard()

        buttons = [button for row in markup.inline_keyboard for button in row]
        self.assertEqual(buttons[0].text, bot_3.SHORTCUT_COLLECTOR_INSTALL_BUTTON_TEXT)
        self.assertEqual(buttons[0].url, "https://icloud.test/collector")
        self.assertEqual(buttons[1].text, bot_3.SHORTCUT_PROCESSOR_INSTALL_BUTTON_TEXT)
        self.assertEqual(buttons[1].url, "https://icloud.test/processor")
        self.assertEqual(buttons[2].callback_data, "shortcut:connect")

    def test_no_keyboard_when_nothing_is_installable(self):
        """Without install links the keyboard would offer only «подключить» — a dead end
        for someone who has nothing installed yet. Better to show no keyboard at all."""
        with patch.object(bot_3, "_shortcut_collector_install_url", return_value=""), \
             patch.object(bot_3, "_shortcut_processor_install_url", return_value=""):
            self.assertIsNone(bot_3._build_shortcut_install_keyboard())


if __name__ == "__main__":
    unittest.main()
