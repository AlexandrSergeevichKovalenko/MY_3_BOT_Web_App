import unittest
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import bot_3


class _Cursor:
    def __init__(self, rows):
        self._rows = list(rows)

    def execute(self, *_args, **_kwargs):
        return None

    def fetchall(self):
        return list(self._rows)

    def fetchone(self):
        return ("Test User",)

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


class _Connection:
    def __init__(self, rows):
        self._cursor = _Cursor(rows)

    def cursor(self):
        return self._cursor

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


class WeeklyYoutubeRecommendationGateTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def _context():
        return SimpleNamespace(bot=SimpleNamespace())

    def _patch_user_query(self, user_id=77):
        return patch.object(bot_3, "get_db_connection", return_value=_Connection([(int(user_id),)]))

    async def test_free_user_skipped_before_expensive_work(self):
        with self._patch_user_query(77), \
             patch.object(bot_3, "_is_synthetic_telegram_user_id", return_value=False), \
             patch.object(bot_3, "resolve_entitlement", return_value={"effective_mode": "free"}), \
             patch.object(bot_3, "_resolve_user_delivery_chat_id", AsyncMock()) as delivery_mock, \
             patch.object(bot_3, "rate_mistakes", AsyncMock()) as rate_mock, \
             patch.object(bot_3, "llm_execute", AsyncMock()) as llm_mock, \
             patch.object(bot_3, "search_youtube_videous") as search_mock, \
             patch.object(bot_3.logging, "info") as info_mock:
            await bot_3.send_me_analytics_and_recommend_me(self._context())

        delivery_mock.assert_not_called()
        rate_mock.assert_not_called()
        llm_mock.assert_not_called()
        search_mock.assert_not_called()
        self.assertTrue(
            any("weekly_youtube_recommendation_skipped_free" in str(call.args[0]) for call in info_mock.call_args_list)
        )

    async def test_pro_user_flow_reaches_existing_generation_path(self):
        with self._patch_user_query(77), \
             patch.object(bot_3, "_is_synthetic_telegram_user_id", return_value=False), \
             patch.object(bot_3, "resolve_entitlement", return_value={"effective_mode": "pro"}), \
             patch.object(bot_3, "_resolve_user_delivery_chat_id", AsyncMock(return_value=77)), \
             patch.object(bot_3, "rate_mistakes", AsyncMock(return_value=(7, 3, "Grammar", 3, "Konjunktiv I", ""))) as rate_mock, \
             patch.object(bot_3, "_get_weekly_recommendation_topics", return_value=[{"main_category": "Grammar", "sub_category": "Konjunktiv I", "mistakes": 3}]), \
             patch.object(bot_3, "llm_execute", AsyncMock(return_value="Konjunktiv I")) as llm_mock, \
             patch.object(bot_3, "search_youtube_videous", return_value=["<a href=\"https://example.test\">Video</a>"]) as search_mock, \
             patch.object(bot_3, "_send_analytics_message_with_fallback", AsyncMock()) as send_mock, \
             patch.object(bot_3.asyncio, "sleep", AsyncMock()):
            await bot_3.send_me_analytics_and_recommend_me(self._context())

        rate_mock.assert_called_once()
        llm_mock.assert_called_once()
        search_mock.assert_called_once()
        send_mock.assert_called_once()


if __name__ == "__main__":
    unittest.main()
