import asyncio
from contextlib import contextmanager
import unittest
from unittest.mock import AsyncMock, Mock, patch

import backend.backend_server as server
import backend.translation_workflow as workflow


class _FakeRedis:
    def __init__(self):
        self.set_calls = []
        self.deleted = []

    def set(self, key, value, ex=None, nx=None):
        self.set_calls.append((key, value, ex, nx))
        return True

    def delete(self, key):
        self.deleted.append(key)


class _FakeCursor:
    def close(self):
        return None


class _InventoryDemandCursor:
    def __init__(self):
        self._execute_count = 0

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, *_args, **_kwargs):
        self._execute_count += 1

    def fetchall(self):
        if self._execute_count == 1:
            return []
        if self._execute_count == 2:
            return [("focus_1", "b1", 30)]
        return []


class _InventoryDemandConnection:
    def __init__(self):
        self._cursor = _InventoryDemandCursor()

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def cursor(self):
        return self._cursor


class _FakeConnection:
    def __init__(self):
        self._cursor = _FakeCursor()
        self.commits = 0

    def cursor(self):
        return self._cursor

    def commit(self):
        self.commits += 1

    def close(self):
        return None


class TranslationFocusPoolRefillTests(unittest.TestCase):
    @staticmethod
    @contextmanager
    def _fake_db_scope(*_args, **_kwargs):
        yield None

    def test_refill_candidates_include_all_underfilled_focuses(self):
        payloads = {
            f"focus_{idx}": {
                "key": f"focus_{idx}",
                "label": f"Focus {idx}",
                "kind": "preset",
                "_pool_levels": ["b1"],
            }
            for idx in range(1, 9)
        }
        targets = {
            (f"focus_{idx}", "b1"): 12
            for idx in range(1, 9)
        }
        low_watermarks = {
            (f"focus_{idx}", "b1"): 8
            for idx in range(1, 9)
        }
        pool_rows = [
            {"focus_key": f"focus_{idx}", "level": "b1", "ready_count": 2, "focus_label": f"Focus {idx}"}
            for idx in range(1, 8)
        ] + [
            {"focus_key": "focus_8", "level": "b1", "ready_count": 12, "focus_label": "Focus 8"}
        ]
        readiness_rows = [
            {"focus_key": f"focus_{idx}", "sessions_started": idx, "ready_zero_starts": 0, "background_fill_required_count": 0, "fill_underfilled_count": 0, "fill_failed_count": 0}
            for idx in range(1, 9)
        ]

        with patch.object(server, "_all_translation_focus_pool_payloads", return_value=payloads), \
             patch.object(server, "_build_translation_focus_pool_bucket_targets", return_value=(low_watermarks, targets)), \
             patch.object(server, "get_translation_focus_pool_bucket_counts", return_value=pool_rows), \
             patch.object(server, "get_translation_readiness_bucket_rollup", return_value=readiness_rows):
            candidates = server._list_translation_focus_pool_refill_candidates()

        candidate_keys = [str(item.get("key") or "") for item in candidates]
        self.assertEqual(candidate_keys, [f"focus_{idx}" for idx in range(7, 0, -1)])

    def test_refill_candidates_pin_focus_to_hottest_underfilled_level(self):
        payloads = {
            "focus_1": {
                "key": "focus_1",
                "label": "Focus 1",
                "kind": "preset",
            },
        }
        targets = {
            ("focus_1", "b1"): 12,
            ("focus_1", "b2"): 12,
            ("focus_1", "c1"): 12,
        }
        low_watermarks = {
            ("focus_1", "b1"): 8,
            ("focus_1", "b2"): 8,
            ("focus_1", "c1"): 8,
        }
        pool_rows = [
            {"focus_key": "focus_1", "level": "b1", "ready_count": 2, "focus_label": "Focus 1"},
            {"focus_key": "focus_1", "level": "b2", "ready_count": 7, "focus_label": "Focus 1"},
            {"focus_key": "focus_1", "level": "c1", "ready_count": 10, "focus_label": "Focus 1"},
        ]
        readiness_rows = [
            {"focus_key": "focus_1", "level": "b1", "sessions_started": 9, "ready_zero_starts": 3, "background_fill_required_count": 2, "fill_underfilled_count": 0, "fill_failed_count": 0},
            {"focus_key": "focus_1", "level": "b2", "sessions_started": 2, "ready_zero_starts": 0, "background_fill_required_count": 0, "fill_underfilled_count": 0, "fill_failed_count": 0},
            {"focus_key": "focus_1", "level": "c1", "sessions_started": 1, "ready_zero_starts": 0, "background_fill_required_count": 0, "fill_underfilled_count": 0, "fill_failed_count": 0},
        ]

        with patch.object(server, "TRANSLATION_FOCUS_POOL_PREWARM_HOT_LEVEL_LIMIT", 1), \
             patch.object(server, "_all_translation_focus_pool_payloads", return_value=payloads), \
             patch.object(server, "_build_translation_focus_pool_bucket_targets", return_value=(low_watermarks, targets)), \
             patch.object(server, "get_translation_focus_pool_bucket_counts", return_value=pool_rows), \
             patch.object(server, "get_translation_readiness_bucket_rollup", return_value=readiness_rows):
            candidates = server._list_translation_focus_pool_refill_candidates()

        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0].get("_pool_levels"), ["b1"])

    def test_refill_candidates_use_inventory_demand_when_readiness_is_empty(self):
        payloads = {
            "focus_1": {
                "key": "focus_1",
                "label": "Focus 1",
                "kind": "preset",
            },
        }
        pool_rows = [
            {"focus_key": "focus_1", "level": "b1", "ready_count": 30, "focus_label": "Focus 1"},
            {"focus_key": "focus_1", "level": "b2", "ready_count": 30, "focus_label": "Focus 1"},
            {"focus_key": "focus_1", "level": "c1", "ready_count": 30, "focus_label": "Focus 1"},
        ]

        with patch.object(server, "_all_translation_focus_pool_payloads", return_value=payloads), \
             patch.object(server, "get_translation_readiness_bucket_rollup", return_value=[]), \
             patch.object(server, "get_translation_focus_pool_bucket_counts", return_value=pool_rows), \
             patch.object(server, "get_db_connection_context", return_value=_InventoryDemandConnection()), \
             patch.object(server, "_load_translation_focus_pool_history_by_bucket", return_value={}), \
             patch.object(server, "_load_translation_readiness_by_bucket", return_value={}):
            candidates = server._list_translation_focus_pool_refill_candidates()

        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0].get("key"), "focus_1")
        self.assertEqual(candidates[0].get("_pool_levels"), ["b1"])
        self.assertGreater(int(candidates[0].get("_demand_score") or 0), 0)

    def test_inventory_deficit_can_trigger_daytime_refill_without_background_fill(self):
        fake_redis = _FakeRedis()
        fake_thread = Mock()
        log_calls = []

        def fake_log_flow_observation(flow, stage, **fields):
            log_calls.append((flow, stage, fields))

        with patch.object(server, "_log_flow_observation", side_effect=fake_log_flow_observation), \
             patch.object(server, "_get_translation_focus_pool_payload_by_key", return_value={"key": "focus_1", "kind": "preset"}), \
             patch.object(server, "_build_translation_focus_pool_bucket_targets", return_value=({("focus_1", "b1"): 12}, {("focus_1", "b1"): 24})), \
             patch.object(server, "get_redis_client", return_value=fake_redis), \
             patch.object(server, "can_enqueue_background_jobs", return_value=False), \
             patch.object(server.threading, "Thread", return_value=fake_thread):
            result = server._maybe_trigger_translation_focus_pool_deficit_refill(
                source_lang="ru",
                target_lang="de",
                focus_key="focus_1",
                level="b1",
                readiness_diagnostics={"pool_ready_before": 3},
                background_fill_required=False,
            )

        self.assertTrue(result["triggered"])
        self.assertEqual(result["trigger_reason"], "inventory_deficit")
        self.assertEqual(len(fake_redis.set_calls), 1)
        fake_thread.start.assert_called_once()
        trigger_stages = [stage for flow, stage, _fields in log_calls if flow == "translation_focus_pool_refill"]
        self.assertIn("trigger_evaluated", trigger_stages)
        self.assertIn("trigger_enqueue_start", trigger_stages)
        self.assertIn("trigger_finish", trigger_stages)

    def test_refill_dispatch_emits_stage_logs(self):
        log_calls = []

        def fake_log_flow_observation(flow, stage, **fields):
            log_calls.append((flow, stage, fields))

        async def fake_prewarm(**_kwargs):
            return {
                "ok": True,
                "focuses": 1,
                "levels": 1,
                "generated": 1,
                "upserted": 1,
                "bucket_results": [{"focus_key": "focus_1", "level": "b1"}],
                "elapsed_ms": 11,
            }

        with patch.object(server, "_log_flow_observation", side_effect=fake_log_flow_observation), \
             patch.object(server, "TRANSLATION_FOCUS_POOL_PREWARM_ENABLED", True), \
             patch.object(server, "_should_run_sentence_prewarm_now", return_value=True), \
             patch.object(server, "_list_translation_focus_pool_refill_candidates", return_value=[{"key": "focus_1", "kind": "preset"}]), \
             patch.object(server, "_build_translation_focus_pool_bucket_targets", return_value=({("focus_1", "b1"): 8}, {("focus_1", "b1"): 12})), \
             patch.object(server, "_upsert_translation_focus_pool_daily_snapshot_from_inventory", return_value=1), \
             patch.object(server, "prewarm_shared_translation_sentence_pool", side_effect=fake_prewarm):
            result = server._dispatch_translation_focus_pool_refill(force=True, tz_name="Europe/Vienna")

        self.assertTrue(result["ok"])
        stages = [stage for flow, stage, _fields in log_calls if flow == "translation_focus_pool_refill"]
        self.assertIn("dispatch_start", stages)
        self.assertIn("candidate_selection", stages)
        self.assertIn("target_buckets_computed", stages)
        self.assertIn("generation_finished", stages)
        self.assertIn("snapshot_persisted", stages)
        self.assertIn("dispatch_finish", stages)

    def test_prewarm_budget_consumes_upserted_inventory_growth(self):
        fake_conn = _FakeConnection()
        focus = {
            "key": "focus_1",
            "label": "Focus 1",
            "prompt_topic": "Focus 1",
            "kind": "preset",
            "_pool_levels": ["b1"],
        }
        generated_batches = AsyncMock(side_effect=[
            [
                {"sentence": "Satz eins."},
                {"sentence": "Satz zwei."},
            ],
            [
                {"sentence": "Satz drei."},
            ],
        ])
        log_calls = []

        def fake_log_flow_observation(flow, stage, **fields):
            log_calls.append((flow, stage, fields))

        with patch.object(workflow, "_log_flow_observation", side_effect=fake_log_flow_observation), \
             patch.object(workflow, "db_acquire_scope", self._fake_db_scope), \
             patch.object(workflow, "get_db_connection", return_value=fake_conn), \
             patch.object(workflow, "_load_skill_catalog_with_cursor", return_value=[]), \
             patch.object(workflow, "_count_shared_sentence_pool_entries_with_cursor", side_effect=[0, 1, 2]), \
             patch.object(workflow, "_list_shared_sentence_pool_sentence_keys_with_cursor", return_value=set()), \
             patch.object(workflow, "_generate_legacy_sentence_entries_with_profiles", generated_batches), \
             patch.object(workflow, "_filter_sentence_entries_for_session", side_effect=lambda entries, **_kwargs: entries), \
             patch.object(workflow, "_upsert_shared_sentence_pool_entries_with_cursor", side_effect=[1, 1]):
            result = asyncio.run(
                workflow.prewarm_shared_translation_sentence_pool(
                    focuses=[focus],
                    levels=["b1"],
                    source_lang="ru",
                    target_lang="de",
                    target_ready_per_bucket=2,
                    max_generate_per_bucket=2,
                )
            )

        self.assertEqual(result["upserted"], 2)
        self.assertEqual(result["bucket_results"][0]["ready_after"], 2)
        self.assertEqual(result["bucket_results"][0]["generation_attempts"], 2)
        workflow_stages = [stage for flow, stage, _fields in log_calls if flow == "translation_focus_pool_refill"]
        self.assertIn("start", workflow_stages)
        self.assertIn("bucket_start", workflow_stages)
        self.assertIn("bucket_generation_attempt", workflow_stages)
        self.assertIn("bucket_generation_result", workflow_stages)
        self.assertIn("bucket_finish", workflow_stages)
        self.assertIn("finish", workflow_stages)

    def test_admin_report_scheduler_runs_refill_before_report(self):
        calls = []

        def fake_refill(*, force=False, tz_name=None):
            calls.append(("refill", bool(force), tz_name))
            return {"ok": True, "skipped": False}

        def fake_report(*, force=False):
            calls.append(("report", bool(force)))
            return {"ok": True}

        with patch.object(server, "TRANSLATION_FOCUS_POOL_PREWARM_ENABLED", True), \
             patch.object(server, "TRANSLATION_FOCUS_POOL_REFILL_TZ", "Europe/Vienna"), \
             patch.object(server, "_dispatch_translation_focus_pool_refill", side_effect=fake_refill), \
             patch.object(server, "_send_translation_focus_pool_admin_report", side_effect=fake_report):
            server._run_translation_focus_pool_admin_report_scheduler_job()

        self.assertEqual(calls[0], ("refill", False, "Europe/Vienna"))
        self.assertEqual(calls[1], ("report", False))


if __name__ == "__main__":
    unittest.main()
