import unittest
from types import SimpleNamespace
from unittest.mock import patch

import bot_3


class _FakeRedis:
    def __init__(self):
        self.values = {}
        self.deleted = []

    def get(self, key):
        return self.values.get(key)

    def setex(self, key, ttl, value):
        self.values[key] = value

    def delete(self, key):
        self.deleted.append(key)
        self.values.pop(key, None)


class PrivateDictionaryBatchFastButtonTests(unittest.TestCase):
    def setUp(self):
        self._orig_pending = dict(bot_3.pending_dictionary_lookup_requests)
        bot_3.pending_dictionary_lookup_requests.clear()

    def tearDown(self):
        bot_3.pending_dictionary_lookup_requests.clear()
        bot_3.pending_dictionary_lookup_requests.update(self._orig_pending)

    def test_private_keyboard_includes_batch_fast_button(self):
        markup = bot_3._build_private_language_tutor_reply_keyboard()
        labels = [
            str(getattr(button, "text", "") or "")
            for row in getattr(markup, "keyboard", []) or []
            for button in row or []
        ]
        self.assertIn(bot_3.DICTIONARY_BATCH_FAST_BUTTON_TEXT, labels)

    def test_open_private_chat_keyboard_uses_direct_bot_link(self):
        context = SimpleNamespace(bot=SimpleNamespace(username="TestDeutschBot"))

        markup = bot_3.asyncio.run(bot_3._build_open_private_chat_keyboard(context, start="quiz"))
        button = markup.inline_keyboard[0][0]

        self.assertEqual(button.text, "💬 Открыть личку с ботом")
        self.assertEqual(button.url, "https://t.me/TestDeutschBot?start=quiz")

    def test_pending_dictionary_lookup_keys_filtered_by_user(self):
        bot_3.pending_dictionary_lookup_requests["k1"] = {"user_id": 11, "text": "eins"}
        bot_3.pending_dictionary_lookup_requests["k2"] = {"user_id": 22, "text": "zwei"}
        bot_3.pending_dictionary_lookup_requests["k3"] = {"user_id": 11, "text": "drei"}

        self.assertEqual(
            bot_3._list_pending_dictionary_lookup_request_keys_for_user(11),
            ["k1", "k3"],
        )
        self.assertEqual(
            bot_3._list_pending_dictionary_lookup_request_keys_for_user(22),
            ["k2"],
        )

    def test_shortcut_pending_is_promoted_to_primary_redis_queue(self):
        redis = _FakeRedis()
        redis.values["dict_pending_user:11"] = "[]"
        redis.values["dict_pending_shortcut:11"] = (
            '[{"key": "sc1", "user_id": 11, "text": "eins"}]'
        )

        with patch("backend.job_queue.get_redis_client", return_value=redis):
            self.assertEqual(
                bot_3._list_pending_dictionary_lookup_request_keys_for_user(11),
                ["sc1"],
            )

        self.assertIn("dict_pending_shortcut:11", redis.deleted)
        self.assertIn('"key": "sc1"', redis.values["dict_pending_user:11"])
        self.assertNotIn("dict_pending_shortcut:11", redis.values)

    def test_listing_merges_in_memory_and_shortcut_pending(self):
        redis = _FakeRedis()
        redis.values["dict_pending_shortcut:11"] = (
            '[{"key": "sc2", "user_id": 11, "text": "zwei"}]'
        )
        bot_3.pending_dictionary_lookup_requests["k1"] = {"user_id": 11, "text": "eins"}

        with patch("backend.job_queue.get_redis_client", return_value=redis):
            self.assertEqual(
                bot_3._list_pending_dictionary_lookup_request_keys_for_user(11),
                ["k1", "sc2"],
            )


if __name__ == "__main__":
    unittest.main()
