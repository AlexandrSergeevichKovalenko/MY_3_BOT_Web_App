import unittest

import bot_3


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


if __name__ == "__main__":
    unittest.main()
