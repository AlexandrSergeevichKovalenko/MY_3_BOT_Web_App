import unittest
from unittest.mock import patch

from backend import admin_economics


class AdminEconomicsFormattingTests(unittest.TestCase):
    def test_report_format_contains_required_sections(self):
        payload = {
            "day": "2026-06-06",
            "tz_name": "Europe/Vienna",
            "user_stats": {
                "active_free_users": 10,
                "active_pro_users": 2,
                "active_trial_users": 1,
                "new_users_today": 3,
                "total_active_users": 13,
            },
            "openai_stats": {
                "total_openai_requests": 25,
                "lookup_requests": 12,
                "explain_requests": 4,
                "story_requests": 2,
                "shortcut_split_requests": 3,
                "estimated_cache_hit_ratio": 0.5,
                "estimated_db_cache_hit_ratio": 0.2,
                "openai_requests_avoided_by_cache": 12,
            },
            "limit_utilization": [
                {
                    "feature_code": "dictionary_lookup_daily",
                    "title": "Словарные запросы",
                    "limit_value": 30,
                    "period": "day",
                    "users_who_used": 6,
                    "average_usage": 4.5,
                    "median_usage": 3,
                    "p95_usage": 20,
                    "max_usage": 30,
                    "blocked_user_count": 1,
                }
            ],
            "gpt_helper_usage": {
                "explain": 4,
                "explain_question": 2,
                "collocations": 5,
                "story": 2,
                "reader_gpt": 0,
                "youtube_gpt": 1,
            },
            "top_consumers": {
                "lookup": [{"user_id": 77, "usage": 30}],
                "shortcut": [],
                "save": [],
            },
            "trend_7d": {
                "dictionary_lookup_daily": {
                    "avg_usage_7d": 5,
                    "max_usage_7d": 30,
                    "blocked_users_7d": 2,
                }
            },
        }

        text = admin_economics.format_admin_economics_report(payload)

        self.assertIn("📊 Admin Economics", text)
        self.assertIn("👥 Users", text)
        self.assertIn("🤖 OpenAI", text)
        self.assertIn("📏 Limits", text)
        self.assertIn("🧠 GPT Helpers", text)
        self.assertIn("🔥 Top Consumers", text)
        self.assertIn("dictionary_lookup_daily", text)

    def test_limits_keyboard_contains_preview_callbacks_for_all_limits(self):
        with patch(
            "backend.admin_economics.list_admin_configurable_limits",
            return_value=[
                {"feature_code": "dictionary_lookup_daily"},
                {"feature_code": "ask_gpt_daily"},
            ],
        ):
            keyboard = admin_economics.build_admin_economics_limits_keyboard()

        callbacks = [
            button["callback_data"]
            for row in keyboard["inline_keyboard"]
            for button in row
            if str(button.get("callback_data", "")).startswith("admecon:preview:")
        ]
        self.assertIn("admecon:preview:dictionary_lookup_daily:-10", callbacks)
        self.assertIn("admecon:preview:dictionary_lookup_daily:10", callbacks)
        self.assertIn("admecon:preview:ask_gpt_daily:-1", callbacks)
        self.assertIn("admecon:preview:ask_gpt_daily:1", callbacks)

    def test_limit_preview_requires_apply_button(self):
        preview = {
            "token": "abc",
            "feature_code": "dictionary_lookup_daily",
            "old_value": 30,
            "new_value": 35,
            "period": "day",
        }

        text = admin_economics.format_admin_limit_preview(preview)
        keyboard = admin_economics.build_admin_limit_preview_keyboard("abc")

        self.assertIn("Limit Change Preview", text)
        self.assertIn("Current", text)
        self.assertIn("Proposed", text)
        callbacks = [button["callback_data"] for row in keyboard["inline_keyboard"] for button in row]
        self.assertEqual(callbacks, ["admecon:apply:abc", "admecon:cancel:abc"])


if __name__ == "__main__":
    unittest.main()
