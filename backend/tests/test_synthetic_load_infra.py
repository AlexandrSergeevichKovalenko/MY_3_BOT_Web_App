"""Focused tests for the synthetic load-testing infrastructure (Phase 1).

Proves:
  * synthetic mode is off by default and gated by SYNTHETIC_LOAD_MODE
  * in synthetic mode the OpenAI/YouTube/TTS providers never build real clients
    and return schema-valid deterministic payloads
  * synthetic mode does NOT touch DB/Redis/billing (only paid APIs are faked)
  * the metrics foundation records and snapshots correctly
  * the load runner generates synthetic updates without Telegram traffic
"""

import asyncio
import unittest
from unittest.mock import patch

import backend.synthetic_load as synth
import backend.load_metrics as metrics


class SyntheticModeFlagTests(unittest.TestCase):
    def test_off_by_default(self):
        with patch.dict("os.environ", {}, clear=False):
            import os
            os.environ.pop("SYNTHETIC_LOAD_MODE", None)
            self.assertFalse(synth.is_synthetic_load_mode())

    def test_on_when_enabled(self):
        for val in ("1", "true", "YES", "on"):
            with patch.dict("os.environ", {"SYNTHETIC_LOAD_MODE": val}):
                self.assertTrue(synth.is_synthetic_load_mode())


class ClientFactoryTests(unittest.TestCase):
    def test_real_async_client_when_off(self):
        with patch.dict("os.environ", {"SYNTHETIC_LOAD_MODE": "0"}):
            c = synth.build_async_openai_client(api_key="x")
        self.assertEqual(type(c).__name__, "AsyncOpenAI")

    def test_synthetic_async_client_when_on(self):
        with patch.dict("os.environ", {"SYNTHETIC_LOAD_MODE": "1"}):
            c = synth.build_async_openai_client(api_key="x")
        self.assertIsInstance(c, synth._SyntheticNode)

    def test_synthetic_sync_client_when_on(self):
        with patch.dict("os.environ", {"SYNTHETIC_LOAD_MODE": "1"}):
            c = synth.build_sync_openai_client(api_key="x")
        self.assertIsInstance(c, synth._SyntheticNode)


class FakeProviderPayloadTests(unittest.IsolatedAsyncioTestCase):
    async def test_chat_completion_schema_and_determinism(self):
        with patch.dict("os.environ", {"SYNTHETIC_LOAD_MODE": "1"}):
            client = synth.build_async_openai_client(api_key="x")
            msgs = [{"role": "user", "content": "Hallo Welt"}]
            r1 = await client.chat.completions.create(model="gpt-4.1", messages=msgs)
            r2 = await client.chat.completions.create(model="gpt-4.1", messages=msgs)
        # schema-valid OpenAI envelope
        self.assertIsInstance(r1.choices[0].message.content, str)
        self.assertEqual(r1.choices[0].finish_reason, "stop")
        self.assertIsInstance(r1.usage.prompt_tokens, int)
        self.assertIsInstance(r1.usage.completion_tokens, int)
        self.assertEqual(r1.usage.total_tokens, r1.usage.prompt_tokens + r1.usage.completion_tokens)
        # deterministic for identical input
        self.assertEqual(r1.choices[0].message.content, r2.choices[0].message.content)

    def test_sync_image_payload(self):
        with patch.dict("os.environ", {"SYNTHETIC_LOAD_MODE": "1"}):
            client = synth.build_sync_openai_client(api_key="x")
            resp = client.images.generate(model="gpt-image-1", prompt="a cat")
        self.assertIsInstance(resp.data[0].b64_json, str)
        self.assertTrue(resp.data[0].b64_json)

    def test_youtube_fixture_shape(self):
        fx = synth.fake_youtube_transcript(video_id="abc", lang="de")
        self.assertTrue(fx["success"])
        self.assertEqual(fx["source"], "synthetic")
        self.assertIsInstance(fx["items"], list)
        self.assertIn("text", fx["items"][0])

    def test_tts_payload_is_bytes(self):
        payload = synth.fake_tts_mp3_bytes(text="hallo")
        self.assertIsInstance(payload, bytes)
        self.assertTrue(payload)

    def test_youtube_guard_returns_none_when_off(self):
        with patch.dict("os.environ", {"SYNTHETIC_LOAD_MODE": "0"}):
            self.assertIsNone(synth.synthetic_youtube_transcript_or_none("abc", "de"))
        with patch.dict("os.environ", {"SYNTHETIC_LOAD_MODE": "1"}):
            self.assertIsNotNone(synth.synthetic_youtube_transcript_or_none("abc", "de"))


class InfraNotMockedTests(unittest.TestCase):
    def test_synthetic_layer_does_not_touch_db_redis_billing(self):
        # The synthetic layer only fakes paid external providers. It must NOT expose
        # or override any DB / Redis / billing symbol.
        for forbidden in (
            "get_db_connection_context", "reserve_free_feature_usage",
            "increment_free_feature_usage", "get_redis_client", "log_billing_event",
        ):
            self.assertFalse(hasattr(synth, forbidden),
                             f"synthetic_load must not wrap {forbidden}")

    def test_db_and_billing_callables_unchanged_in_synthetic_mode(self):
        import backend.database as database
        before_db = database.get_db_connection_context
        before_reserve = database.reserve_free_feature_usage
        with patch.dict("os.environ", {"SYNTHETIC_LOAD_MODE": "1"}):
            self.assertTrue(synth.is_synthetic_load_mode())
            # importing/using the synthetic layer leaves real infra functions intact
            self.assertIs(database.get_db_connection_context, before_db)
            self.assertIs(database.reserve_free_feature_usage, before_reserve)


class MetricsTests(unittest.TestCase):
    def setUp(self):
        metrics.reset()

    def test_handler_latency_context_manager(self):
        with metrics.handler_latency("translation"):
            pass
        snap = metrics.snapshot()["handler_latency"]
        self.assertEqual(snap["translation"]["count"], 1)
        self.assertGreaterEqual(snap["translation"]["max_ms"], 0.0)

    def test_telegram_counters(self):
        metrics.incr_telegram("send", 3)
        metrics.incr_telegram("edit")
        metrics.incr_telegram("weird_kind")  # -> "other"
        snap = metrics.snapshot()["telegram"]
        self.assertEqual(snap["send"], 3)
        self.assertEqual(snap["edit"], 1)
        self.assertEqual(snap["other"], 1)

    def test_event_loop_lag_records(self):
        metrics.record_event_loop_lag(12.5)
        metrics.record_event_loop_lag(4.0)
        lag = metrics.snapshot()["event_loop_lag"]
        self.assertEqual(lag["max_ms"], 12.5)
        self.assertEqual(lag["samples"], 2)

    def test_event_loop_lag_sampler_runs(self):
        async def _run():
            task = asyncio.create_task(metrics.event_loop_lag_sampler(interval_sec=0.05))
            await asyncio.sleep(0.13)
            task.cancel()
        asyncio.run(_run())
        self.assertGreaterEqual(metrics.snapshot()["event_loop_lag"]["samples"], 1)

    def test_queue_depth_gauge_with_fake_redis(self):
        class _FakeRedis:
            def llen(self, key):
                return 7 if key == "dramatiq:tts_generation" else 0
        depths = metrics.sample_queue_depths(["tts_generation", "youtube_transcript"], redis_client=_FakeRedis())
        self.assertEqual(depths["tts_generation"], 7)
        self.assertEqual(depths["youtube_transcript"], 0)
        self.assertEqual(metrics.snapshot()["queue_depths"]["tts_generation"], 7)


class LoadRunnerTests(unittest.TestCase):
    def test_generate_updates_count_and_no_telegram(self):
        from scripts.synthetic_load_runner import run_plan, generate_updates
        updates = list(generate_updates(user_count=100, rate_per_sec=5, duration_sec=2))
        self.assertEqual(len(updates), 10)  # 5/s * 2s
        # synthetic payloads are plain dicts (no telegram.Bot / network)
        self.assertTrue(all(isinstance(u, dict) for u in updates))
        self.assertTrue(any("callback_query" in u for u in updates))

    def test_run_plan_is_plan_only_by_default(self):
        from scripts.synthetic_load_runner import run_plan
        plan = run_plan(user_count=50, rate_per_sec=2, duration_sec=1)
        self.assertEqual(plan["dispatch"], "plan-only")
        self.assertEqual(plan["dispatched"], 0)
        self.assertEqual(sum(plan["cohorts"].values()), 50)


if __name__ == "__main__":
    unittest.main()
