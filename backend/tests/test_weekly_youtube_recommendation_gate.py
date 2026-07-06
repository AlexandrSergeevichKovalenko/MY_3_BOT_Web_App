import unittest
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import bot_3
from backend import grammar_video_catalog


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
             patch.object(bot_3, "_select_weekly_video_for_user") as select_mock, \
             patch.object(bot_3, "record_video_send") as record_mock, \
             patch.object(bot_3.logging, "info") as info_mock:
            await bot_3.send_me_analytics_and_recommend_me(self._context())

        delivery_mock.assert_not_called()
        rate_mock.assert_not_called()
        select_mock.assert_not_called()
        record_mock.assert_not_called()
        self.assertTrue(
            any("weekly_youtube_recommendation_skipped_free" in str(call.args[0]) for call in info_mock.call_args_list)
        )

    async def test_pro_user_flow_selects_pool_video_and_records_send(self):
        selection = {
            "videos": [{"video_id": "abc123", "title": "Konjunktiv II erklärt", "url": "https://youtu.be/abc123"}],
            "topic_key": "konjunktiv_2",
            "topic_display": grammar_video_catalog.topic_display("konjunktiv_2"),
            "source": "pool",
        }
        with self._patch_user_query(77), \
             patch.object(bot_3, "_is_synthetic_telegram_user_id", return_value=False), \
             patch.object(bot_3, "resolve_entitlement", return_value={"effective_mode": "pro"}), \
             patch.object(bot_3, "_resolve_user_delivery_chat_id", AsyncMock(return_value=77)), \
             patch.object(bot_3, "rate_mistakes", AsyncMock(return_value=(7, 3, "Moods", 3, "Subjunctive 2 (Konjunktiv II)", ""))) as rate_mock, \
             patch.object(bot_3, "_get_weekly_recommendation_topics", return_value=[{"main_category": "Moods", "sub_category": "Subjunctive 2 (Konjunktiv II)", "mistakes": 3}]), \
             patch.object(bot_3, "_select_weekly_video_for_user", return_value=selection) as select_mock, \
             patch.object(bot_3, "record_video_send") as record_mock, \
             patch.object(bot_3, "_send_analytics_message_with_fallback", AsyncMock()) as send_mock, \
             patch.object(bot_3.asyncio, "sleep", AsyncMock()):
            await bot_3.send_me_analytics_and_recommend_me(self._context())

        rate_mock.assert_called_once()
        select_mock.assert_called_once()
        record_mock.assert_called_once_with(user_id=77, video_id="abc123", topic_key="konjunktiv_2")
        send_mock.assert_called_once()
        # The recommendation body carries the catalog topic label and the video link.
        sent_text = send_mock.call_args.kwargs["text"]
        self.assertIn("youtu.be/abc123", sent_text)
        self.assertIn("Konjunktiv", sent_text)


class GrammarVideoCatalogTests(unittest.TestCase):
    def test_conditionals_maps_to_german_topic_not_english(self):
        # The screenshot bug: "Conditionals (wenn/falls)" produced English
        # "if clauses" videos. It must map to a German Konditionalsatz topic.
        key = grammar_video_catalog.match_topic_key("Clauses & Sentence Types", "Conditionals (wenn/falls)")
        self.assertEqual(key, "konditionalsatz")
        query = grammar_video_catalog.get_topic(key)["query"].lower()
        self.assertIn("deutsch", query)
        self.assertIn("konditional", query)
        self.assertNotIn("english", query)
        self.assertNotIn("if clause", query)

    def test_every_query_is_german(self):
        for key, entry in grammar_video_catalog.GRAMMAR_VIDEO_TOPICS.items():
            query = entry["query"].lower()
            self.assertIn("deutsch", query, f"{key} query is not German-anchored: {query!r}")
            self.assertNotIn("english", query, f"{key} query mentions english: {query!r}")

    def test_unknown_and_other_mistake_return_none(self):
        self.assertIsNone(grammar_video_catalog.match_topic_key("Other mistake", "Unclassified mistake"))
        self.assertIsNone(grammar_video_catalog.match_topic_key("", ""))

    def test_category_fallback_when_subcategory_unknown(self):
        # Unknown subcategory but known main category -> main-category topic.
        self.assertEqual(grammar_video_catalog.match_topic_key("Verbs", "totally unknown sub"), "verb_konjugation")


if __name__ == "__main__":
    unittest.main()
