import unittest
from unittest.mock import AsyncMock, patch

import bot_3


class _FakeUser:
    def __init__(self, user_id: int):
        self.id = user_id


class _FakeChat:
    def __init__(self, chat_type: str = "private"):
        self.type = chat_type


class _FakeMessage:
    def __init__(self, *, text: str = "finden und erfinden", user_id: int = 77):
        self.text = text
        self.from_user = _FakeUser(user_id)
        self.chat_id = user_id
        self.reply_text = AsyncMock()


class _FakeUpdate:
    def __init__(self, *, message: _FakeMessage, chat_type: str = "private"):
        self.message = message
        self.effective_chat = _FakeChat(chat_type)


class ShortcutForwardedMessageFreeLimitTests(unittest.IsolatedAsyncioTestCase):
    async def test_free_forwarded_message_increments_usage_when_accepted(self):
        message = _FakeMessage(user_id=77)
        update = _FakeUpdate(message=message)

        with patch.object(bot_3, "resolve_entitlement", return_value={"effective_mode": "free", "plan_code": "free"}), \
             patch.object(bot_3, "get_free_feature_limit_metadata", return_value={"free_limit": 15}), \
             patch.object(bot_3, "get_free_feature_usage_today", return_value=0.0), \
             patch.object(bot_3, "_start_shortcut_lookup_enqueue_runner", return_value="job-123") as enqueue_mock, \
             patch.object(bot_3, "increment_free_feature_usage") as increment_mock:
            await bot_3.handle_forwarded_message_lookup(update, None)

        message.reply_text.assert_awaited_once_with("🔍", quote=True)
        enqueue_mock.assert_called_once_with(user_id=77, text="finden und erfinden", origin="forwarded")
        increment_mock.assert_called_once()
        self.assertEqual(increment_mock.call_args.kwargs["feature_key"], "shortcut_forwarded_message_daily")

    async def test_free_forwarded_message_sixteenth_request_is_blocked_before_enqueue(self):
        message = _FakeMessage(user_id=77)
        update = _FakeUpdate(message=message)

        with patch.object(bot_3, "resolve_entitlement", return_value={"effective_mode": "free", "plan_code": "free"}), \
             patch.object(bot_3, "get_free_feature_limit_metadata", return_value={"free_limit": 15}), \
             patch.object(bot_3, "get_free_feature_usage_today", return_value=15.0), \
             patch.object(bot_3, "_start_shortcut_lookup_enqueue_runner") as enqueue_mock, \
             patch.object(bot_3, "increment_free_feature_usage") as increment_mock:
            await bot_3.handle_forwarded_message_lookup(update, None)

        message.reply_text.assert_awaited_once_with(bot_3.SHORTCUT_FORWARDED_MESSAGE_LIMIT_MESSAGE, quote=True)
        enqueue_mock.assert_not_called()
        increment_mock.assert_not_called()

    async def test_pro_forwarded_message_is_not_blocked_by_free_limit(self):
        message = _FakeMessage(user_id=77)
        update = _FakeUpdate(message=message)

        with patch.object(bot_3, "resolve_entitlement", return_value={"effective_mode": "pro", "plan_code": "pro"}), \
             patch.object(bot_3, "get_free_feature_usage_today") as usage_mock, \
             patch.object(bot_3, "_start_shortcut_lookup_enqueue_runner", return_value="job-456") as enqueue_mock, \
             patch.object(bot_3, "increment_free_feature_usage") as increment_mock:
            await bot_3.handle_forwarded_message_lookup(update, None)

        message.reply_text.assert_awaited_once_with("🔍", quote=True)
        enqueue_mock.assert_called_once_with(user_id=77, text="finden und erfinden", origin="forwarded")
        usage_mock.assert_not_called()
        increment_mock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
