import unittest
from contextlib import contextmanager
from unittest.mock import patch

from backend import admin_economics


class _FakeCursor:
    def __init__(self, rows_by_call):
        self.rows_by_call = list(rows_by_call)
        self.executed = []
        self._index = -1
        self._last_query = ""
        self._last_params = ()

    def execute(self, query, params=None):
        self.executed.append((query, params))
        self._last_query = str(query or "")
        self._last_params = tuple(params or ())
        self._index += 1

    def fetchall(self):
        if 0 <= self._index < len(self.rows_by_call):
            rows = self.rows_by_call[self._index]
            if "user_id < %s" in self._last_query and self._last_params:
                threshold = 0
                for value in self._last_params:
                    try:
                        candidate = int(value or 0)
                    except Exception:
                        continue
                    if candidate >= 1_000_000_000:
                        threshold = candidate
                        break
                filtered = []
                for row in rows:
                    try:
                        user_id = int(row[0])
                    except Exception:
                        continue
                    if user_id < threshold:
                        filtered.append(row)
                return filtered
            return rows
        return []

    def fetchone(self):
        rows = self.fetchall()
        return rows[0] if rows else None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _FakeConn:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


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
            "cost_breakdown": {
                "library_cost": 0.0931,
                "library_requests": 814,
                "user_cost": 0.1446,
                "user_requests": 46,
                "total_cost": 0.2377,
                "by_provider": [
                    {"provider": "openai", "label": "OpenAI", "cost": 0.2335, "requests": 858},
                    {"provider": "google_tts", "label": "TTS-аудио", "cost": 0.0042, "requests": 12},
                ],
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

        self.assertIn("📊 Экономика", text)
        self.assertIn("👥 Активны", text)
        self.assertIn("💰 Затраты", text)
        self.assertIn("🏭 Контент (мы)", text)
        self.assertIn("👤 Пользователи", text)
        self.assertIn("TTS-аудио", text)
        self.assertIn("🤖 OpenAI", text)
        self.assertIn("🚦 Лимиты", text)
        self.assertIn("🧠 GPT-хелперы", text)
        self.assertIn("🔥 Топ потребители", text)
        self.assertIn("Словарные запросы", text)

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

    def test_active_users_exclude_synthetic_ids(self):
        fake_cursor = _FakeCursor([
            [(1450575292,), (9937001856,), (9100000001,)],
            [(3,)],
        ])

        @contextmanager
        def fake_db():
            yield _FakeConn(fake_cursor)

        with patch("backend.admin_economics.get_db_connection_context", side_effect=fake_db):
            active_ids = admin_economics._fetch_active_user_ids(
                target_day=admin_economics.date(2026, 6, 14),
                tz_name="Europe/Vienna",
            )
            user_stats = admin_economics._user_stats(
                target_day=admin_economics.date(2026, 6, 14),
                tz_name="Europe/Vienna",
            )

        self.assertEqual(active_ids, {1450575292})
        self.assertEqual(user_stats["total_active_users"], 1)
        self.assertEqual(user_stats["new_users_today"], 0)

    def test_openai_by_user_passes_synthetic_floor_to_sql(self):
        fake_cursor = _FakeCursor([
            [(1450575292, 7, 70, 0.42), (9937001856, 99, 990, 9.9)],
        ])

        @contextmanager
        def fake_db():
            yield _FakeConn(fake_cursor)

        with patch("backend.admin_economics.get_db_connection_context", side_effect=fake_db):
            rows = admin_economics._openai_by_user(
                target_day=admin_economics.date(2026, 6, 14),
                tz_name="Europe/Vienna",
                limit=15,
            )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["user_id"], 1450575292)
        executed_query, executed_params = fake_cursor.executed[0]
        self.assertIn("user_id < %s", executed_query)
        self.assertEqual(executed_params[0], admin_economics.SYNTHETIC_TELEGRAM_USER_ID_MIN)


if __name__ == "__main__":
    unittest.main()
