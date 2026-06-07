"""Focused tests for the enforce_user_access allow-list cache optimization.

Covers:
  * allowed user: cache hit avoids a second DB call
  * cache miss loads from DB (and caches the result)
  * revoked user is denied after invalidation (no stale access)
  * admin user bypasses the DB entirely
  * is_telegram_user_allowed_async: cache hit / admin -> no thread; miss -> to_thread (off event loop)
  * enforce_user_access uses the async (non-blocking) check, not the sync DB lookup
  * denied user still stops the handler (ApplicationHandlerStop)
  * private-chat path does not trigger the group-context DB write
"""

import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import backend.database as database


# --- Fake DB connection context that counts DB lookups ---

class _FakeCursor:
    def __init__(self, fetch_result, executed):
        self._fetch = fetch_result
        self._executed = executed

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self._executed.append((sql, params))

    def fetchone(self):
        return self._fetch


class _FakeConn:
    def __init__(self, fetch_result, executed):
        self._fetch = fetch_result
        self._executed = executed

    def cursor(self):
        return _FakeCursor(self._fetch, self._executed)


class _FakeCtx:
    """Mimics get_db_connection_context() as a context manager."""

    def __init__(self, fetch_result, executed):
        self._fetch = fetch_result
        self._executed = executed

    def __enter__(self):
        return _FakeConn(self._fetch, self._executed)

    def __exit__(self, *exc):
        return False


def _fake_ctx_factory(fetch_result, executed):
    return lambda *a, **k: _FakeCtx(fetch_result, executed)


class AllowListCacheTests(unittest.TestCase):
    def setUp(self):
        database.invalidate_telegram_user_allowed_cache()
        self._ttl_patch = patch.object(database, "TELEGRAM_ALLOWED_USER_CACHE_TTL_SEC", 90)
        self._ttl_patch.start()
        # No admins unless a test sets them.
        self._admin_patch = patch.object(database, "get_admin_telegram_ids", return_value=set())
        self._admin_patch.start()

    def tearDown(self):
        self._ttl_patch.stop()
        self._admin_patch.stop()
        database.invalidate_telegram_user_allowed_cache()

    def test_allowed_user_cache_hit_avoids_db(self):
        executed = []
        with patch.object(database, "get_db_connection_context", _fake_ctx_factory([1], executed)):
            self.assertTrue(database.is_telegram_user_allowed(555))   # miss -> DB
            self.assertTrue(database.is_telegram_user_allowed(555))   # hit -> no DB
            self.assertTrue(database.is_telegram_user_allowed(555))
        self.assertEqual(len(executed), 1, "second/third lookups must be served from cache")

    def test_cache_miss_loads_from_db(self):
        executed = []
        with patch.object(database, "get_db_connection_context", _fake_ctx_factory([1], executed)):
            self.assertTrue(database.is_telegram_user_allowed(777))
        self.assertEqual(len(executed), 1)
        self.assertIn("bt_3_allowed_users", executed[0][0])

    def test_denied_user_is_cached_too(self):
        executed = []
        with patch.object(database, "get_db_connection_context", _fake_ctx_factory(None, executed)):
            self.assertFalse(database.is_telegram_user_allowed(888))  # miss -> DB -> denied
            self.assertFalse(database.is_telegram_user_allowed(888))  # cached denial
        self.assertEqual(len(executed), 1)

    def test_revoked_user_denied_after_invalidation(self):
        allowed_exec = []
        with patch.object(database, "get_db_connection_context", _fake_ctx_factory([1], allowed_exec)):
            self.assertTrue(database.is_telegram_user_allowed(999))   # cached True

        database.invalidate_telegram_user_allowed_cache(999)          # e.g. revoke_telegram_user

        denied_exec = []
        with patch.object(database, "get_db_connection_context", _fake_ctx_factory(None, denied_exec)):
            self.assertFalse(database.is_telegram_user_allowed(999))  # re-reads DB -> denied
        self.assertEqual(len(denied_exec), 1, "after invalidation the next lookup must hit DB")

    def test_admin_bypasses_db(self):
        def _boom(*a, **k):
            raise AssertionError("admin must not hit the database")

        with patch.object(database, "get_admin_telegram_ids", return_value={4242}):
            with patch.object(database, "get_db_connection_context", _boom):
                self.assertTrue(database.is_telegram_user_allowed(4242))


class AllowListAsyncTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        database.invalidate_telegram_user_allowed_cache()
        self._ttl_patch = patch.object(database, "TELEGRAM_ALLOWED_USER_CACHE_TTL_SEC", 90)
        self._ttl_patch.start()
        self._admin_patch = patch.object(database, "get_admin_telegram_ids", return_value=set())
        self._admin_patch.start()

    def tearDown(self):
        self._ttl_patch.stop()
        self._admin_patch.stop()
        database.invalidate_telegram_user_allowed_cache()

    async def test_async_cache_hit_does_not_use_thread_or_db(self):
        database._allowed_user_cache_put(321, True)
        with patch.object(database.asyncio, "to_thread", side_effect=AssertionError("must not offload on cache hit")):
            self.assertTrue(await database.is_telegram_user_allowed_async(321))

    async def test_async_admin_does_not_use_thread_or_db(self):
        with patch.object(database, "get_admin_telegram_ids", return_value={7}):
            with patch.object(database.asyncio, "to_thread", side_effect=AssertionError("admin must not offload")):
                self.assertTrue(await database.is_telegram_user_allowed_async(7))

    async def test_async_miss_offloads_to_thread(self):
        # Cache empty -> must dispatch the blocking lookup via asyncio.to_thread,
        # never run it synchronously on the event loop.
        to_thread_mock = AsyncMock(return_value=True)
        with patch.object(database.asyncio, "to_thread", to_thread_mock):
            self.assertTrue(await database.is_telegram_user_allowed_async(654))
        to_thread_mock.assert_awaited_once()
        self.assertIs(to_thread_mock.await_args.args[0], database.is_telegram_user_allowed)


def _make_update(*, user_id=111, chat_type="private", text="hi"):
    message = SimpleNamespace(text=text, reply_text=AsyncMock())
    return SimpleNamespace(
        effective_user=SimpleNamespace(id=user_id, username="tester"),
        effective_chat=SimpleNamespace(id=-1, type=chat_type, title="T"),
        effective_message=message,
        callback_query=None,
        poll_answer=None,
        my_chat_member=None,
    )


class EnforceUserAccessTests(unittest.IsolatedAsyncioTestCase):
    async def test_enforce_uses_async_check_not_sync_db(self):
        import bot_3
        ctx = SimpleNamespace()
        update = _make_update(user_id=111)
        with patch.object(bot_3, "_register_group_context_from_update", AsyncMock()), \
             patch.object(bot_3, "is_telegram_user_allowed_async", AsyncMock(return_value=True)) as allowed_async, \
             patch.object(bot_3, "is_telegram_user_allowed",
                          Mock(side_effect=AssertionError("sync DB lookup must not run on the event loop"))):
            result = await bot_3.enforce_user_access(update, ctx)
        self.assertIsNone(result)
        allowed_async.assert_awaited_once()

    async def test_denied_user_stops_handler(self):
        import bot_3
        from telegram.ext import ApplicationHandlerStop
        ctx = SimpleNamespace()
        update = _make_update(user_id=222, text="just a message")
        with patch.object(bot_3, "_register_group_context_from_update", AsyncMock()), \
             patch.object(bot_3, "is_telegram_user_allowed_async", AsyncMock(return_value=False)):
            with self.assertRaises(ApplicationHandlerStop):
                await bot_3.enforce_user_access(update, ctx)
        update.effective_message.reply_text.assert_awaited_once()

    async def test_private_chat_does_not_trigger_group_context_write(self):
        import bot_3
        update = _make_update(user_id=333, chat_type="private")
        with patch.object(bot_3, "upsert_webapp_group_context", Mock()) as upsert, \
             patch.object(bot_3.asyncio, "to_thread", AsyncMock()) as to_thread:
            await bot_3._register_group_context_from_update(update)
        upsert.assert_not_called()
        to_thread.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
