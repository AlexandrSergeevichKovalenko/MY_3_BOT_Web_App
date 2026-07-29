"""Возвращение пользователя и приход изменений доступа в процесс бота.

Возврат — СОБЫТИЕ ОТДЕЛЬНОЕ от «новый пользователь»: удаление чата в Telegram не
забирает доступ (список доступа наш, Telegram до него не дотягивается), поэтому
вернувшийся человек не должен считаться новым — иначе каждая переустановка читалась
бы как рост.
"""

import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import bot_3


class AbsenceDurationTests(unittest.TestCase):
    def test_reads_like_a_human(self):
        now = datetime.now(timezone.utc)
        cases = {
            None: "неизвестно сколько",
            now - timedelta(seconds=90): "1 мин.",
            now - timedelta(hours=5): "5 ч.",
            now - timedelta(days=1, hours=1): "1 день",
            now - timedelta(days=3): "3 дня",
            now - timedelta(days=12): "12 дней",
        }
        for blocked_at, expected in cases.items():
            self.assertEqual(bot_3._format_absence_duration(blocked_at), expected)


class ReturnNoticeTests(unittest.IsolatedAsyncioTestCase):
    def _context(self):
        return SimpleNamespace(bot=SimpleNamespace(send_message=AsyncMock()))

    async def test_admin_is_told_who_returned_and_for_how_long(self):
        ctx = self._context()
        user = SimpleNamespace(id=555, username="wanderer", first_name="Ольга", last_name=None)
        with patch.object(bot_3, "get_admin_telegram_ids", Mock(return_value=[117649764])):
            await bot_3._notify_admins_user_returned(
                ctx,
                user=user,
                user_id=555,
                blocked_at=datetime.now(timezone.utc) - timedelta(days=3),
            )
        ctx.bot.send_message.assert_awaited_once()
        text = ctx.bot.send_message.await_args.kwargs["text"]
        self.assertIn("↩️", text)
        self.assertIn("555", text)
        self.assertIn("3 дня", text)
        # Возврат не должен маскироваться под подключение нового человека.
        self.assertNotIn("Новый пользователь", text)

    async def test_no_admins_configured_is_not_an_error(self):
        ctx = self._context()
        with patch.object(bot_3, "get_admin_telegram_ids", Mock(return_value=[])):
            await bot_3._notify_admins_user_returned(ctx, user=None, user_id=555, blocked_at=None)
        ctx.bot.send_message.assert_not_awaited()


class CacheInvalidationFeedJobTests(unittest.IsolatedAsyncioTestCase):
    """Решение «допущен ли пользователь» живёт в памяти процесса сутки, поэтому выдача
    или отзыв доступа снаружи доходили до бота только после истечения кэша."""

    class _FakeRedis:
        def __init__(self, members):
            self.members = members
            self.queried = []

        def zrangebyscore(self, key, low, high):
            self.queried.append((key, low, high))
            return self.members

    async def test_allowlist_events_drop_the_local_cache(self):
        bot_3._CACHE_INVALIDATION_FEED_SEEN_UNTIL["ts"] = 0.0
        redis = self._FakeRedis(["allowlist:4242:1785.0:abc", "billing:99:1785.0:def"])
        dropped = Mock()
        with patch("backend.job_queue.get_redis_client", Mock(return_value=redis)), \
             patch("backend.database.invalidate_telegram_user_allowed_cache", dropped):
            await bot_3._cache_invalidation_feed_job(SimpleNamespace())
        # Только события списка доступа: тарифный кэш живёт в веб-части, не здесь.
        dropped.assert_called_once_with(4242)
        self.assertGreater(bot_3._CACHE_INVALIDATION_FEED_SEEN_UNTIL["ts"], 0)

    async def test_no_redis_is_survivable(self):
        with patch("backend.job_queue.get_redis_client", Mock(return_value=None)):
            await bot_3._cache_invalidation_feed_job(SimpleNamespace())  # must not raise

    async def test_broken_feed_entry_does_not_kill_the_job(self):
        bot_3._CACHE_INVALIDATION_FEED_SEEN_UNTIL["ts"] = 0.0
        redis = self._FakeRedis(["мусор", "allowlist:не_число:1:x", "allowlist:7:1:y"])
        dropped = Mock()
        with patch("backend.job_queue.get_redis_client", Mock(return_value=redis)), \
             patch("backend.database.invalidate_telegram_user_allowed_cache", dropped):
            await bot_3._cache_invalidation_feed_job(SimpleNamespace())
        dropped.assert_called_once_with(7)


if __name__ == "__main__":
    unittest.main()
