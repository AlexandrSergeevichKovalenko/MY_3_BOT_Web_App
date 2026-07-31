"""Ночная чистка чата должна забирать и сообщения самого пользователя.

Бот удаляет ночью свои служебные сообщения, но в личке остаётся столбик из нажатий
меню («⚙️ Настройки», «▶️ Следующее задание») и команд (/start) — их пишет в чат сам
пользователь, поэтому раньше они не попадали в чистку. Теперь попадают.

Граница: набранный руками текст (перевод, слово, вопрос учителю) НИКОГДА не удаляем —
это содержимое пользователя, а не навигационный мусор.
"""

import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import bot_3


def _message(text, *, chat_type="private", is_bot=False, entities=()):
    return SimpleNamespace(
        chat=SimpleNamespace(type=chat_type),
        chat_id=555,
        message_id=42,
        from_user=SimpleNamespace(id=555, is_bot=is_bot),
        text=text,
        entities=entities,
    )


def _command_entity(offset=0):
    return SimpleNamespace(type="bot_command", offset=offset)


class DisposableUserMessageTests(unittest.TestCase):
    def test_menu_taps_are_disposable(self):
        for label in (
            bot_3.SETTINGS_BUTTON_TEXT,
            bot_3.NEXT_TASK_BUTTON_TEXT,
            bot_3.INTERACTIVE_BUTTON_TEXT,
            bot_3.BATTLES_BUTTON_TEXT,
            bot_3.DICTIONARY_BATCH_FAST_BUTTON_TEXT,
            bot_3.HOWTO_GUIDE_BUTTON_TEXT,
        ):
            self.assertTrue(
                bot_3._is_disposable_user_message(_message(label)),
                f"нажатие «{label}» должно уходить в ночную чистку",
            )

    def test_commands_are_disposable(self):
        for text in ("/start", "/streak", "/start ref_123"):
            self.assertTrue(
                bot_3._is_disposable_user_message(_message(text, entities=(_command_entity(),))),
                f"команда «{text}» должна уходить в ночную чистку",
            )

    def test_typed_content_survives(self):
        for text in (
            "Ich habe gestern einen Film gesehen",
            "Sehenswürdigkeit",
            "почему здесь Dativ?",
            "20/30 — это сколько?",
        ):
            self.assertFalse(
                bot_3._is_disposable_user_message(_message(text)),
                f"набранный текст «{text}» удалять нельзя",
            )

    def test_slash_inside_text_is_not_a_command(self):
        # Без entity bot_command это обычный текст, а не команда.
        self.assertFalse(bot_3._is_disposable_user_message(_message("/ Sonderzeichen")))

    def test_groups_and_bots_are_untouched(self):
        self.assertFalse(
            bot_3._is_disposable_user_message(
                _message(bot_3.SETTINGS_BUTTON_TEXT, chat_type="supergroup")
            )
        )
        self.assertFalse(
            bot_3._is_disposable_user_message(_message(bot_3.SETTINGS_BUTTON_TEXT, is_bot=True))
        )

    def test_broken_message_is_ignored(self):
        self.assertFalse(bot_3._is_disposable_user_message(None))


class TrackUserMenuMessageTests(unittest.IsolatedAsyncioTestCase):
    async def test_tap_is_registered_for_the_nightly_purge(self):
        update = SimpleNamespace(effective_message=_message(bot_3.SETTINGS_BUTTON_TEXT))
        tracker = AsyncMock()
        with patch.object(bot_3, "_track_telegram_message_async", tracker):
            await bot_3._track_user_menu_message(update, SimpleNamespace())
        tracker.assert_awaited_once()
        self.assertIs(tracker.await_args.args[0], update.effective_message)
        self.assertEqual(tracker.await_args.args[1], bot_3.USER_MENU_MESSAGE_TYPE)

    async def test_typed_text_is_not_registered(self):
        update = SimpleNamespace(effective_message=_message("Guten Morgen"))
        tracker = AsyncMock()
        with patch.object(bot_3, "_track_telegram_message_async", tracker):
            await bot_3._track_user_menu_message(update, SimpleNamespace())
        tracker.assert_not_awaited()


class NightlyPurgeTypeTests(unittest.TestCase):
    def test_user_menu_type_is_not_preserved(self):
        # Тип должен оставаться удаляемым — иначе чистка снова пройдёт мимо.
        self.assertNotIn(bot_3.USER_MENU_MESSAGE_TYPE, bot_3.ALWAYS_PRESERVE_MESSAGE_TYPES)


if __name__ == "__main__":
    unittest.main()
