"""Focused tests for the Phase-2 synthetic load dispatcher.

Proves the safety contract and the dispatch mechanics WITHOUT touching the
production DB/Redis, Telegram, or any paid provider:
  * refuses to run without SYNTHETIC_LOAD_MODE=1
  * refuses a production DB host
  * refuses a production Redis host
  * the stub bot never calls Telegram (records via metrics counters)
  * synthetic users are allowed via the in-memory cache only (no DB)
  * fake providers are active in synthetic mode
  * the dispatcher can feed a small number of updates into a mocked application
"""

import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import scripts.synthetic_load_dispatch as disp
import backend.synthetic_load as synth


class SafetyGuardTests(unittest.TestCase):
    def test_refuses_without_synthetic_mode(self):
        with patch.dict("os.environ", {}, clear=False):
            import os
            os.environ.pop("SYNTHETIC_LOAD_MODE", None)
            with patch.object(disp, "_active_db_host", return_value="localhost"):
                with self.assertRaises(disp.SyntheticSafetyError) as ctx:
                    disp.assert_safe_to_dispatch()
            self.assertIn("SYNTHETIC_LOAD_MODE", str(ctx.exception))

    def test_refuses_production_db_host(self):
        with patch.dict("os.environ", {"SYNTHETIC_LOAD_MODE": "1"}):
            with patch.object(disp, "_active_db_host", return_value="centerbeam.proxy.rlwy.net"):
                with self.assertRaises(disp.SyntheticSafetyError) as ctx:
                    disp.assert_safe_to_dispatch()
            self.assertIn("PRODUCTION", str(ctx.exception).upper())

    def test_refuses_pgbouncer_production_db_host(self):
        with patch.dict("os.environ", {"SYNTHETIC_LOAD_MODE": "1"}):
            with patch.object(disp, "_active_db_host", return_value="kodama.proxy.rlwy.net"):
                with self.assertRaises(disp.SyntheticSafetyError):
                    disp.assert_safe_to_dispatch()

    def test_refuses_production_redis_host(self):
        with patch.dict("os.environ", {"SYNTHETIC_LOAD_MODE": "1"}):
            with patch.object(disp, "_active_db_host", return_value="localhost"), \
                 patch.object(disp, "_active_redis_host", return_value="kodama.proxy.rlwy.net"):
                with self.assertRaises(disp.SyntheticSafetyError):
                    disp.assert_safe_to_dispatch()

    def test_passes_for_local_hosts(self):
        with patch.dict("os.environ", {"SYNTHETIC_LOAD_MODE": "1"}):
            with patch.object(disp, "_active_db_host", return_value="127.0.0.1"), \
                 patch.object(disp, "_active_redis_host", return_value="localhost"):
                safety = disp.assert_safe_to_dispatch()
        self.assertTrue(safety["synthetic_mode"])

    def test_host_classification(self):
        self.assertTrue(disp._is_production_host("centerbeam.proxy.rlwy.net"))
        self.assertTrue(disp._is_production_host("anything.railway.internal"))
        self.assertFalse(disp._is_production_host("localhost"))
        self.assertTrue(disp._is_local_or_synthetic_host("127.0.0.1"))
        self.assertTrue(disp._is_local_or_synthetic_host("synthetic_postgres"))
        self.assertFalse(disp._is_local_or_synthetic_host("centerbeam.proxy.rlwy.net"))


class AllowListSeedTests(unittest.TestCase):
    def test_seed_allows_users_via_cache_only(self):
        import backend.database as database
        database.invalidate_telegram_user_allowed_cache()
        with patch.object(database, "TELEGRAM_ALLOWED_USER_CACHE_TTL_SEC", 90), \
             patch.object(database, "get_admin_telegram_ids", return_value=set()):
            seeded = disp.seed_allowed_users([900001, 900002])
            self.assertEqual(seeded, 2)
            # DB must NOT be touched — a cache hit answers the allow check.
            with patch.object(database, "get_db_connection_context",
                              side_effect=AssertionError("seeded user must not hit DB")):
                self.assertTrue(database.is_telegram_user_allowed(900001))
                self.assertTrue(database.is_telegram_user_allowed(900002))
        database.invalidate_telegram_user_allowed_cache()


class FakeProvidersActiveTests(unittest.TestCase):
    def test_synthetic_mode_activates_fake_openai(self):
        with patch.dict("os.environ", {"SYNTHETIC_LOAD_MODE": "1"}):
            self.assertTrue(synth.is_synthetic_load_mode())
            client = synth.build_async_openai_client(api_key="x")
            self.assertIsInstance(client, synth._SyntheticNode)


class StubBotTests(unittest.IsolatedAsyncioTestCase):
    async def test_stub_bot_does_not_call_telegram(self):
        from backend import load_metrics
        load_metrics.reset()
        bot = disp.build_stub_bot()
        msg = await bot.send_message(chat_id=1, text="hi")     # no network
        await bot.edit_message_text(chat_id=1, message_id=0, text="x")
        await bot.answer_callback_query(callback_query_id="cb")
        me = await bot.get_me()                                 # no network
        self.assertEqual(me.username, "synthetic_bot")
        self.assertIsNotNone(msg)
        snap = load_metrics.snapshot()["telegram"]
        self.assertEqual(snap["send"], 1)
        self.assertEqual(snap["edit"], 1)
        self.assertEqual(snap["callback_answer"], 1)


class BuildUpdateTests(unittest.TestCase):
    def test_build_update_returns_telegram_update(self):
        from telegram import Update
        from scripts.synthetic_load_runner import _make_synthetic_update
        bot = disp.build_stub_bot()
        msg_update = disp.build_update(_make_synthetic_update(900010, "message", 1), bot)
        cb_update = disp.build_update(_make_synthetic_update(900010, "callback", 2), bot)
        self.assertIsInstance(msg_update, Update)
        self.assertIsInstance(cb_update, Update)
        self.assertEqual(msg_update.effective_user.id, 900010)
        self.assertIsNotNone(cb_update.callback_query)


class DispatchLoopTests(unittest.IsolatedAsyncioTestCase):
    async def test_dispatch_into_mocked_application(self):
        calls = []

        class _MockApp:
            bot = None  # -> dispatcher feeds raw payloads, no Update construction

            async def process_update(self, update):
                calls.append(update)

        result = await disp.run_dispatch(
            users=10, rate=50.0, duration=0.2,
            application=_MockApp(), enforce_guards=False,
        )
        self.assertEqual(result["generated"], 10)        # 50/s * 0.2s
        self.assertEqual(result["dispatched"], 10)
        self.assertEqual(result["errors"], 0)
        self.assertEqual(len(calls), 10)
        self.assertIn("handler_latency", result["metrics"])
        self.assertGreaterEqual(result["seeded_allowed_users"], 1)


if __name__ == "__main__":
    unittest.main()
