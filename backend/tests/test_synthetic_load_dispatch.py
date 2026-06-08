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


class StagingSafetyGuardTests(unittest.TestCase):
    """Railway staging safety guards: arming + allow-list + hard prod deny."""

    def _clear(self):
        import os
        for k in ("SYNTHETIC_STAGING_ARMED", "SYNTHETIC_ALLOWED_HOST",
                  "SYNTHETIC_EXTRA_PRODUCTION_HOSTS"):
            os.environ.pop(k, None)

    def test_local_run_without_staging_armed_passes(self):
        with patch.dict("os.environ", {"SYNTHETIC_LOAD_MODE": "1"}):
            self._clear()
            with patch.object(disp, "_active_db_host", return_value="127.0.0.1"), \
                 patch.object(disp, "_active_redis_host", return_value="localhost"):
                safety = disp.assert_safe_to_dispatch()
        self.assertEqual(safety["target"], "local")
        self.assertFalse(safety["staging_armed"])

    def test_nonlocal_host_requires_staging_armed(self):
        with patch.dict("os.environ", {"SYNTHETIC_LOAD_MODE": "1"}):
            self._clear()  # not armed
            with patch.object(disp, "_active_db_host", return_value="staging-pg.example.com"):
                with self.assertRaises(disp.SyntheticSafetyError) as ctx:
                    disp.assert_safe_to_dispatch()
        self.assertIn("SYNTHETIC_STAGING_ARMED", str(ctx.exception))

    def test_nonlocal_host_requires_allowed_host_match(self):
        # Armed but no allow-list set -> refuse.
        with patch.dict("os.environ", {"SYNTHETIC_LOAD_MODE": "1", "SYNTHETIC_STAGING_ARMED": "1"}):
            disp.os.environ.pop("SYNTHETIC_ALLOWED_HOST", None)
            with patch.object(disp, "_active_db_host", return_value="staging-pg.example.com"):
                with self.assertRaises(disp.SyntheticSafetyError) as ctx:
                    disp.assert_safe_to_dispatch()
        self.assertIn("SYNTHETIC_ALLOWED_HOST", str(ctx.exception))
        # Armed + allow-list set but MISMATCHED host -> refuse.
        with patch.dict("os.environ", {"SYNTHETIC_LOAD_MODE": "1", "SYNTHETIC_STAGING_ARMED": "1",
                                       "SYNTHETIC_ALLOWED_HOST": "other-host.example.com"}):
            with patch.object(disp, "_active_db_host", return_value="staging-pg.example.com"):
                with self.assertRaises(disp.SyntheticSafetyError) as ctx:
                    disp.assert_safe_to_dispatch()
        self.assertIn("SYNTHETIC_ALLOWED_HOST", str(ctx.exception))

    def test_nonlocal_host_passes_when_armed_and_allowed(self):
        with patch.dict("os.environ", {"SYNTHETIC_LOAD_MODE": "1", "SYNTHETIC_STAGING_ARMED": "1",
                                       "SYNTHETIC_ALLOWED_HOST": "staging-pg.example.com"}):
            with patch.object(disp, "_active_db_host", return_value="staging-pg.example.com"), \
                 patch.object(disp, "_active_redis_host", return_value="staging-pg.example.com"):
                safety = disp.assert_safe_to_dispatch()
        self.assertEqual(safety["target"], "staging")
        self.assertTrue(safety["staging_armed"])
        self.assertEqual(safety["allowed_host"], "staging-pg.example.com")

    def test_railway_staging_suffix_allowed_when_armed_and_allowed(self):
        # A *.proxy.rlwy.net host that is NOT a known prod host may run as a staging
        # target only when armed + explicitly allow-listed.
        host = "bot3-staging.proxy.rlwy.net"
        with patch.dict("os.environ", {"SYNTHETIC_LOAD_MODE": "1", "SYNTHETIC_STAGING_ARMED": "1",
                                       "SYNTHETIC_ALLOWED_HOST": host}):
            with patch.object(disp, "_active_db_host", return_value=host), \
                 patch.object(disp, "_active_redis_host", return_value=""):
                safety = disp.assert_safe_to_dispatch()
        self.assertEqual(safety["target"], "staging")

    def test_railway_staging_suffix_refused_without_arming(self):
        host = "bot3-staging.proxy.rlwy.net"
        with patch.dict("os.environ", {"SYNTHETIC_LOAD_MODE": "1", "SYNTHETIC_ALLOWED_HOST": host}):
            disp.os.environ.pop("SYNTHETIC_STAGING_ARMED", None)
            with patch.object(disp, "_active_db_host", return_value=host):
                with self.assertRaises(disp.SyntheticSafetyError) as ctx:
                    disp.assert_safe_to_dispatch()
        self.assertIn("SYNTHETIC_STAGING_ARMED", str(ctx.exception))

    def test_known_production_db_refused_even_if_armed(self):
        host = "centerbeam.proxy.rlwy.net"
        with patch.dict("os.environ", {"SYNTHETIC_LOAD_MODE": "1", "SYNTHETIC_STAGING_ARMED": "1",
                                       "SYNTHETIC_ALLOWED_HOST": host}):
            with patch.object(disp, "_active_db_host", return_value=host):
                with self.assertRaises(disp.SyntheticSafetyError) as ctx:
                    disp.assert_safe_to_dispatch()
        self.assertIn("KNOWN PRODUCTION", str(ctx.exception).upper())

    def test_known_production_redis_refused_even_if_armed(self):
        with patch.dict("os.environ", {"SYNTHETIC_LOAD_MODE": "1", "SYNTHETIC_STAGING_ARMED": "1",
                                       "SYNTHETIC_ALLOWED_HOST": "kodama.proxy.rlwy.net"}):
            with patch.object(disp, "_active_db_host", return_value="localhost"), \
                 patch.object(disp, "_active_redis_host", return_value="kodama.proxy.rlwy.net"):
                with self.assertRaises(disp.SyntheticSafetyError) as ctx:
                    disp.assert_safe_to_dispatch()
        self.assertIn("KNOWN PRODUCTION", str(ctx.exception).upper())

    def test_extra_production_hosts_are_refused_even_if_allowed(self):
        host = "extra-prod.example.com"
        with patch.dict("os.environ", {"SYNTHETIC_LOAD_MODE": "1", "SYNTHETIC_STAGING_ARMED": "1",
                                       "SYNTHETIC_ALLOWED_HOST": host,
                                       "SYNTHETIC_EXTRA_PRODUCTION_HOSTS": "a.example.com, extra-prod.example.com"}):
            with patch.object(disp, "_active_db_host", return_value=host):
                with self.assertRaises(disp.SyntheticSafetyError) as ctx:
                    disp.assert_safe_to_dispatch()
        self.assertIn("KNOWN PRODUCTION", str(ctx.exception).upper())


class SyntheticUserNamespaceTests(unittest.TestCase):
    """Generated synthetic user IDs must live in the reserved >= 900e9 namespace
    so they can never collide with a real Telegram user ID."""

    def test_reserved_base_is_correct(self):
        from scripts.synthetic_load_runner import SYNTHETIC_USER_ID_BASE
        self.assertGreaterEqual(SYNTHETIC_USER_ID_BASE, 900_000_000_000)

    def test_all_generated_user_ids_in_reserved_namespace(self):
        from scripts.synthetic_load_runner import generate_updates, SYNTHETIC_USER_ID_BASE
        updates = list(generate_updates(user_count=250, rate_per_sec=50, duration_sec=4))
        self.assertTrue(updates)
        ids = []
        for u in updates:
            uid = (u.get("message", {}).get("from", {}).get("id")
                   or u.get("callback_query", {}).get("from", {}).get("id"))
            self.assertIsNotNone(uid)
            ids.append(int(uid))
        self.assertTrue(all(i >= SYNTHETIC_USER_ID_BASE for i in ids),
                        f"min generated id {min(ids)} < base {SYNTHETIC_USER_ID_BASE}")

    def test_no_collision_with_plausible_real_telegram_ids(self):
        # Real Telegram user IDs are far below the reserved base (the production
        # synthetic floor itself is ~9.1e9). No generated id may fall below 900e9.
        from scripts.synthetic_load_runner import generate_updates, is_synthetic_user_id
        plausible_real_ids = [1, 12345, 7_000_000_000, 9_100_000_000]  # all < 900e9
        for rid in plausible_real_ids:
            self.assertFalse(is_synthetic_user_id(rid))
        for u in generate_updates(user_count=100, rate_per_sec=20, duration_sec=2):
            uid = (u.get("message", {}).get("from", {}).get("id")
                   or u.get("callback_query", {}).get("from", {}).get("id"))
            self.assertTrue(is_synthetic_user_id(int(uid)))
            self.assertNotIn(int(uid), plausible_real_ids)


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
        self.assertEqual(result["mode"], "sequential")


class _SleepyApp:
    """Mock app whose process_update sleeps (to force/observe overlap) and can
    fail selected update_ids. Never touches Telegram or any provider."""
    bot = None

    def __init__(self, delay: float = 0.05, fail_ids: set | None = None):
        self.delay = delay
        self.fail_ids = fail_ids or set()
        self.calls = []
        self._inflight = 0
        self.concurrent_peak = 0

    async def process_update(self, update):
        import asyncio as _asyncio
        self._inflight += 1
        self.concurrent_peak = max(self.concurrent_peak, self._inflight)
        try:
            await _asyncio.sleep(self.delay)
            if isinstance(update, dict) and update.get("update_id") in self.fail_ids:
                raise RuntimeError("synthetic task failure")
            self.calls.append(update)
        finally:
            self._inflight -= 1


class ConcurrentDispatchTests(unittest.IsolatedAsyncioTestCase):
    async def test_default_mode_is_sequential(self):
        app = _SleepyApp(delay=0.02)
        result = await disp.run_dispatch(
            users=20, rate=200.0, duration=0.1,
            application=app, enforce_guards=False,
        )
        self.assertEqual(result["mode"], "sequential")
        self.assertEqual(app.concurrent_peak, 1)          # never overlaps
        self.assertLessEqual(result["max_in_flight_observed"], 1)

    async def test_concurrent_mode_overlaps(self):
        app = _SleepyApp(delay=0.05)
        result = await disp.run_dispatch(
            users=20, rate=500.0, duration=0.1,
            application=app, enforce_guards=False,
            concurrent=True, max_in_flight=32,
        )
        self.assertEqual(result["mode"], "concurrent")
        self.assertGreater(app.concurrent_peak, 1)        # tasks ran in parallel
        self.assertGreater(result["max_in_flight_observed"], 1)
        self.assertEqual(result["completed_tasks"], result["scheduled_tasks"])

    async def test_max_in_flight_cap_respected(self):
        app = _SleepyApp(delay=0.05)
        result = await disp.run_dispatch(
            users=20, rate=500.0, duration=0.1,
            application=app, enforce_guards=False,
            concurrent=True, max_in_flight=4,
        )
        self.assertLessEqual(app.concurrent_peak, 4)
        self.assertLessEqual(result["max_in_flight_observed"], 4)
        self.assertGreater(result["backpressure_events"], 0)   # cap was hit

    async def test_task_errors_collected(self):
        # Fail a few specific update_ids; errors must be collected, not lost.
        payloads = list(disp.generate_updates(20, 500.0, 0.1))
        fail_ids = {p["update_id"] for p in payloads[:3]}
        app = _SleepyApp(delay=0.01, fail_ids=fail_ids)
        result = await disp.run_dispatch(
            users=20, rate=500.0, duration=0.1,
            application=app, enforce_guards=False,
            concurrent=True, max_in_flight=8,
        )
        self.assertGreater(result["task_errors"], 0)
        self.assertEqual(result["errors"], result["task_errors"])
        self.assertEqual(result["dispatched"], result["scheduled_tasks"])

    async def test_pending_tasks_awaited_on_shutdown(self):
        app = _SleepyApp(delay=0.05)
        result = await disp.run_dispatch(
            users=20, rate=500.0, duration=0.1,
            application=app, enforce_guards=False,
            concurrent=True, max_in_flight=16,
        )
        # all scheduled tasks completed before returning (no orphaned in-flight work)
        self.assertEqual(result["completed_tasks"], result["scheduled_tasks"])
        self.assertGreater(result["completed_tasks"], 0)

    async def test_guards_run_before_concurrent_dispatch(self):
        import os
        with patch.dict("os.environ", {}, clear=False):
            os.environ.pop("SYNTHETIC_LOAD_MODE", None)
            with patch.object(disp, "_active_db_host", return_value="localhost"):
                with self.assertRaises(disp.SyntheticSafetyError):
                    await disp.run_dispatch(
                        users=5, rate=50.0, duration=0.05,
                        application=_SleepyApp(), enforce_guards=True,
                        concurrent=True, max_in_flight=4,
                    )

    async def test_no_telegram_or_providers_touched_in_concurrent(self):
        from backend import load_metrics
        app = _SleepyApp(delay=0.01)
        await disp.run_dispatch(
            users=20, rate=500.0, duration=0.1,
            application=app, enforce_guards=False,
            concurrent=True, max_in_flight=8,
        )
        tg = load_metrics.snapshot()["telegram"]
        # mock app's process_update never calls the stub bot -> zero Telegram ops
        self.assertEqual(tg["send"], 0)
        self.assertEqual(tg["edit"], 0)
        self.assertEqual(tg["callback_answer"], 0)


if __name__ == "__main__":
    unittest.main()
