import unittest
from unittest.mock import patch

import backend.backend_server as server


class FlashcardsFreeLimitUxTests(unittest.TestCase):
    @staticmethod
    def _entitlement(mode: str) -> tuple[dict, dict]:
        return (
            {
                "effective_mode": mode,
                "reset_at": "2026-06-04T00:00:00+02:00",
            },
            {},
        )

    def test_free_flashcards_daily_word_limit_is_10(self):
        with patch.object(server, "_resolve_user_entitlement", return_value=self._entitlement("free")), \
             patch.object(server, "_sum_billing_units_today", return_value=0.0):
            state = server._check_flashcards_words_daily_limit(
                user_id=77,
                mode="fsrs",
                requested_words=15,
            )

        self.assertEqual(server.FREE_FLASHCARDS_WORDS_DAILY_PER_MODE, 10)
        self.assertEqual(state["allowed_words"], 10)
        self.assertEqual(state["limit_words"], 10)
        self.assertEqual(state["effective_mode"], "free")

    def test_trial_flashcards_limit_is_unchanged(self):
        with patch.object(server, "_resolve_user_entitlement", return_value=self._entitlement("trial")), \
             patch.object(server, "_sum_billing_units_today") as usage_mock:
            state = server._check_flashcards_words_daily_limit(
                user_id=77,
                mode="fsrs",
                requested_words=15,
            )

        self.assertEqual(state["allowed_words"], 15)
        self.assertIsNone(state["limit_words"])
        self.assertEqual(state["effective_mode"], "trial")
        usage_mock.assert_not_called()

    def test_pro_flashcards_limit_is_unchanged(self):
        with patch.object(server, "_resolve_user_entitlement", return_value=self._entitlement("pro")), \
             patch.object(server, "_sum_billing_units_today") as usage_mock:
            state = server._check_flashcards_words_daily_limit(
                user_id=77,
                mode="fsrs",
                requested_words=15,
            )

        self.assertEqual(state["allowed_words"], 15)
        self.assertIsNone(state["limit_words"])
        self.assertEqual(state["effective_mode"], "pro")
        usage_mock.assert_not_called()

    def test_limit_message_is_user_facing_without_internal_feature_key(self):
        with patch.object(server, "_resolve_user_entitlement", return_value=self._entitlement("free")), \
             patch.object(server, "_sum_billing_units_today", return_value=10.0):
            state = server._check_flashcards_words_daily_limit(
                user_id=77,
                mode="fsrs",
                requested_words=1,
            )

        error = state["error"]
        self.assertEqual(error["error"], "feature_limit_exceeded")
        self.assertEqual(error["error_code"], "flashcards_daily_words_limit_exceeded")
        self.assertEqual(error["title"], "Лимит повторения слов на сегодня достигнут")
        self.assertIn("до 10 слов в день", error["message"])
        self.assertIn("Завтра лимит автоматически обновится", error["message"])
        self.assertNotIn("flashcards_fsrs_words_daily", error["message"])
        self.assertNotIn("flashcards_", error["message"])
        self.assertNotIn("fsrs", error["message"])
        self.assertNotIn("2026-06-04T00:00:00+02:00", error["message"])
        self.assertNotIn("feature", error)
        self.assertNotIn("used", error)
        self.assertNotIn("unit", error)
        self.assertNotIn("mode", error)


if __name__ == "__main__":
    unittest.main()
