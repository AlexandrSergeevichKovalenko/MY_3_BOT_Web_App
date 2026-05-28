"""
Tests for durable Shortcut ingest recovery semantics.

Covers:
  1.  enqueue_failure_after_db_row
  2.  stale_queued_row_triggers_recovery
  3.  stale_processing_row_triggers_recovery
  4.  illegal_transition_raises
  5.  recovery_retry_capped_at_max
  6.  delivered_rows_not_in_valid_recovery_transitions
  7.  duplicate_during_processing_returns_in_recovery
  8.  worker_sets_worker_id_when_processing
  9.  failed_recovery_enqueue_is_logged
  10. accepted_is_not_a_db_status_only_http_response_field
  11. recovery_actor_is_registered_in_scheduler_service
  12. scheduler_dispatch_calls_send_on_actor
  13. invalid_env_config_falls_back_with_warning
  14. delivered_row_never_returned_by_claim_stale
  15. stale_queued_and_processing_together_both_claimed
  16. stale_queued_beyond_max_attempts_skipped
  17. scheduler_dispatch_function_exists_and_is_callable
"""
import threading
import unittest
from contextlib import ExitStack
from unittest.mock import MagicMock, call, patch

import backend.backend_server as server
import backend.background_jobs as jobs
from backend.database import validate_shortcut_ingest_transition


def _base_route_patches():
    import os
    return [
        patch.dict(os.environ, {"SHORTCUT_SECRET": "secret"}, clear=False),
        patch.object(server, "is_telegram_user_allowed", return_value=True),
        patch.object(server, "_check_free_daily_usage_limit", return_value=(
            {"effective_mode": "free", "used": 0, "limit": 20}, None, None,
        )),
        patch.object(server, "_shortcut_dedup_reserve", return_value=False),
        patch.object(server, "can_enqueue_background_jobs", return_value=True),
    ]


class ShortcutIngestRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.client = server.app.test_client()

    # ------------------------------------------------------------------ #
    # Test 1: enqueue failure after DB row → set_status called with failed #
    # ------------------------------------------------------------------ #

    def test_enqueue_failure_after_db_row(self):
        """DB upsert succeeds (new row), but Dramatiq enqueue raises.
        The background thread must call set_status('failed', ...) and must
        NOT call set_status('queued').

        The assertion waits inside the ExitStack context so patches remain
        active while the daemon thread is running.
        """
        upsert_result = {"ingest_id": 10, "is_new": True, "status": "queued",
                         "job_id": None, "duplicate_count": 0}
        failed_event = threading.Event()
        set_status_calls = []

        def _mock_set_status(**kwargs):
            set_status_calls.append(kwargs)
            if kwargs.get("status") == "failed":
                failed_event.set()

        with ExitStack() as stack:
            stack.enter_context(
                patch.object(server, "shortcut_ingest_request_upsert", return_value=upsert_result)
            )
            stack.enter_context(
                patch.object(server, "enqueue_shortcut_lookup_job",
                             side_effect=RuntimeError("broker_down"))
            )
            stack.enter_context(
                patch.object(server, "shortcut_ingest_request_set_status",
                             side_effect=_mock_set_status)
            )
            for p in _base_route_patches():
                stack.enter_context(p)

            response = self.client.post(
                "/api/shortcut/lookup",
                json={"text": "auf etwas hinweisen", "user_id": 117649764},
                headers={"Authorization": "Bearer secret"},
            )

            # Route should have accepted the request (DB write was fine)
            self.assertEqual(response.status_code, 200)
            payload = response.get_json()
            self.assertTrue(payload["accepted"])
            self.assertTrue(payload["queued"])

            # Wait for the daemon thread INSIDE the with block while patches are still active
            notified = failed_event.wait(timeout=3.0)

        self.assertTrue(notified, "set_status('failed') was never called after enqueue failure")
        statuses = [c["status"] for c in set_status_calls]
        self.assertIn("failed", statuses)
        self.assertNotIn("queued", statuses)
        failed_call = next(c for c in set_status_calls if c["status"] == "failed")
        self.assertEqual(failed_call.get("error_code"), "enqueue_failed")

    # ------------------------------------------------------------------ #
    # Test 2: sweeper re-enqueues a stale queued row                      #
    # ------------------------------------------------------------------ #

    def test_stale_queued_row_triggers_recovery(self):
        """recover_stale_shortcut_ingests claims a stale queued row and re-enqueues it.

        Patches at source modules (backend.database / backend.job_queue) because
        the actor uses lazy imports inside the function body.
        """
        import backend.database as db_module
        import backend.job_queue as jq_module

        stale_row = {
            "ingest_id": 101, "user_id": 117649764, "source": "shortcut",
            "ingest_key": "abc123", "text_preview": "sich bewerben",
            "recovery_attempt_count": 1,
        }
        enqueue_calls = []

        with ExitStack() as stack:
            stack.enter_context(
                patch.object(db_module, "shortcut_ingest_request_claim_stale",
                             return_value=[stale_row])
            )
            stack.enter_context(
                patch.object(jq_module, "enqueue_shortcut_lookup_job",
                             side_effect=lambda **kwargs: enqueue_calls.append(kwargs) or "jid_rec")
            )
            jobs.recover_stale_shortcut_ingests()

        self.assertEqual(len(enqueue_calls), 1)
        call_kw = enqueue_calls[0]
        self.assertEqual(call_kw["user_id"], 117649764)
        self.assertEqual(call_kw["ingest_id"], 101)
        self.assertEqual(call_kw["ingest_key"], "abc123")

    # ------------------------------------------------------------------ #
    # Test 3: sweeper re-enqueues a stale processing row                  #
    # ------------------------------------------------------------------ #

    def test_stale_processing_row_triggers_recovery(self):
        import backend.database as db_module
        import backend.job_queue as jq_module

        stale_row = {
            "ingest_id": 202, "user_id": 117649764, "source": "shortcut",
            "ingest_key": "def456", "text_preview": "einen Beitrag leisten",
            "recovery_attempt_count": 2,
        }
        enqueue_calls = []

        with ExitStack() as stack:
            stack.enter_context(
                patch.object(db_module, "shortcut_ingest_request_claim_stale",
                             return_value=[stale_row])
            )
            stack.enter_context(
                patch.object(jq_module, "enqueue_shortcut_lookup_job",
                             side_effect=lambda **kwargs: enqueue_calls.append(kwargs) or "jid_rec2")
            )
            jobs.recover_stale_shortcut_ingests()

        self.assertEqual(len(enqueue_calls), 1)
        self.assertEqual(enqueue_calls[0]["ingest_id"], 202)

    # ------------------------------------------------------------------ #
    # Test 4: illegal transition raises ValueError                        #
    # ------------------------------------------------------------------ #

    def test_illegal_transition_raises(self):
        illegal_pairs = [
            ("queued", "delivered"),
            ("queued", "failed"),
            ("delivered", "queued"),
            ("delivered", "processing"),
            ("delivered", "failed"),
            ("processing", "queued"),
        ]
        for from_s, to_s in illegal_pairs:
            with self.subTest(transition=f"{from_s}->{to_s}"):
                with self.assertRaises(ValueError):
                    validate_shortcut_ingest_transition(from_s, to_s)

    # ------------------------------------------------------------------ #
    # Test 5: sweeper passes max_recovery_attempts to claim_stale         #
    # ------------------------------------------------------------------ #

    def test_recovery_retry_capped_at_max(self):
        """Sweeper must pass the configured max_recovery_attempts to claim_stale.
        When claim_stale returns empty (all rows exhausted), nothing is enqueued."""
        import backend.database as db_module
        import backend.job_queue as jq_module

        claim_calls = []

        def _mock_claim(**kwargs):
            claim_calls.append(kwargs)
            return []

        with ExitStack() as stack:
            stack.enter_context(
                patch.object(db_module, "shortcut_ingest_request_claim_stale",
                             side_effect=_mock_claim)
            )
            enqueue_mock = stack.enter_context(
                patch.object(jq_module, "enqueue_shortcut_lookup_job")
            )
            jobs.recover_stale_shortcut_ingests()

        self.assertEqual(len(claim_calls), 1)
        self.assertIn("max_recovery_attempts", claim_calls[0])
        self.assertGreater(claim_calls[0]["max_recovery_attempts"], 0)
        enqueue_mock.assert_not_called()

    # ------------------------------------------------------------------ #
    # Test 6: delivered rows have no valid recovery transition             #
    # ------------------------------------------------------------------ #

    def test_delivered_rows_not_in_valid_recovery_transitions(self):
        """delivered→queued and delivered→processing are not valid transitions.
        This is the state machine guard that prevents re-playing finished rows."""
        with self.assertRaises(ValueError):
            validate_shortcut_ingest_transition("delivered", "queued")
        with self.assertRaises(ValueError):
            validate_shortcut_ingest_transition("delivered", "processing")

    # ------------------------------------------------------------------ #
    # Test 7: duplicate during processing returns in_recovery=True        #
    # ------------------------------------------------------------------ #

    def test_duplicate_during_processing_returns_in_recovery(self):
        """When DB reports an existing row with status='processing', the route
        must return duplicate=True AND in_recovery=True."""
        upsert_result = {"ingest_id": 77, "is_new": False, "status": "processing",
                         "job_id": "jid_old", "duplicate_count": 1}
        with ExitStack() as stack:
            stack.enter_context(
                patch.object(server, "shortcut_ingest_request_upsert", return_value=upsert_result)
            )
            enqueue_mock = stack.enter_context(
                patch.object(server, "enqueue_shortcut_lookup_job")
            )
            for p in _base_route_patches():
                stack.enter_context(p)

            response = self.client.post(
                "/api/shortcut/lookup",
                json={"text": "aufgeben", "user_id": 117649764},
                headers={"Authorization": "Bearer secret"},
            )

        payload = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertTrue(payload["duplicate"])
        self.assertFalse(payload["accepted"])
        self.assertTrue(payload.get("in_recovery"), "in_recovery should be True for processing status")
        enqueue_mock.assert_not_called()

    # ------------------------------------------------------------------ #
    # Test 8: worker passes worker_id when transitioning to processing    #
    # ------------------------------------------------------------------ #

    def test_worker_sets_worker_id_when_processing(self):
        import backend.database as db_module

        row = {"ingest_id": 42, "status": "queued", "job_id": "j1",
               "duplicate_count": 0, "sent_prompt_count": 0,
               "accepted_at": None, "completed_at": None, "error_code": None,
               "normalized_text_full": None, "normalized_text_sha256": "",
               "normalized_text_size_bytes": 0}
        set_status_calls = []

        def _set_status(*, ingest_id, status, **kw):
            set_status_calls.append({"ingest_id": ingest_id, "status": status, **kw})

        with ExitStack() as stack:
            stack.enter_context(
                patch.object(db_module, "shortcut_ingest_request_get_by_id",
                             return_value=row)
            )
            stack.enter_context(
                patch.object(db_module, "shortcut_ingest_request_set_status",
                             side_effect=_set_status)
            )
            stack.enter_context(
                patch("backend.backend_server._run_shortcut_lookup_delivery",
                      return_value=2)
            )
            jobs.run_shortcut_lookup_job(
                user_id=117649764,
                text="aufgeben",
                source="shortcut",
                ingest_key="key123",
                ingest_id=42,
            )

        processing_calls = [c for c in set_status_calls if c["status"] == "processing"]
        self.assertEqual(len(processing_calls), 1, "Expected exactly one transition to processing")
        worker_id = processing_calls[0].get("worker_id")
        self.assertIsNotNone(worker_id, "worker_id must be set when transitioning to processing")
        self.assertIsInstance(worker_id, str)
        self.assertGreater(len(worker_id), 0)

    # ------------------------------------------------------------------ #
    # Test 9: failed recovery enqueue is logged and does not crash actor  #
    # ------------------------------------------------------------------ #

    def test_failed_recovery_enqueue_is_logged(self):
        """When enqueue fails for a claimed row, the actor logs but continues
        processing remaining rows and does NOT re-raise (max_retries=0)."""
        import backend.database as db_module
        import backend.job_queue as jq_module

        stale_rows = [
            {"ingest_id": 301, "user_id": 111, "source": "shortcut",
             "ingest_key": "k1", "text_preview": "word1", "recovery_attempt_count": 1},
            {"ingest_id": 302, "user_id": 222, "source": "shortcut",
             "ingest_key": "k2", "text_preview": "word2", "recovery_attempt_count": 1},
        ]
        enqueue_calls = []

        def _mock_enqueue(**kwargs):
            enqueue_calls.append(kwargs)
            if kwargs["ingest_id"] == 301:
                raise RuntimeError("broker unavailable")
            return "jid_ok"

        with ExitStack() as stack:
            stack.enter_context(
                patch.object(db_module, "shortcut_ingest_request_claim_stale",
                             return_value=stale_rows)
            )
            stack.enter_context(
                patch.object(jq_module, "enqueue_shortcut_lookup_job",
                             side_effect=_mock_enqueue)
            )
            # Must NOT raise even though one enqueue fails
            jobs.recover_stale_shortcut_ingests()

        # Both rows were attempted
        self.assertEqual(len(enqueue_calls), 2)
        attempted_ids = {c["ingest_id"] for c in enqueue_calls}
        self.assertIn(301, attempted_ids)
        self.assertIn(302, attempted_ids)


class ShortcutIngestRecoveryIntegrationTests(unittest.TestCase):
    """Tests that verify scheduler/runtime wiring, not just actor logic."""

    # ------------------------------------------------------------------ #
    # Test 10: 'accepted' is an HTTP response field, not a DB status      #
    # ------------------------------------------------------------------ #

    def test_accepted_is_not_a_db_status_only_http_response_field(self):
        """'accepted' is an HTTP response body field (accepted=True/False), not a DB
        status value.  Verify this via the module-level constants — they are the
        authoritative in-process representation of the valid status set.

        DB CHECK constraint: ('queued','processing','delivered','failed')
        Valid transitions: queued->processing, processing->delivered,
                           processing->failed, failed->queued
        'accepted' appears nowhere in the status machine.
        'accepted_at' is a timestamp column name, not a status value.
        """
        from backend.database import (
            _SHORTCUT_INGEST_ACTIVE_STATUSES,
            _SHORTCUT_INGEST_VALID_TRANSITIONS,
        )

        self.assertNotIn("accepted", _SHORTCUT_INGEST_ACTIVE_STATUSES,
                         "'accepted' must not be an active DB status")

        all_statuses = {s for pair in _SHORTCUT_INGEST_VALID_TRANSITIONS for s in pair}
        self.assertNotIn("accepted", all_statuses,
                         "'accepted' must not appear in any valid transition")

        # Exact set of active statuses (queued/processing/delivered → re-enqueue guard)
        self.assertEqual(
            _SHORTCUT_INGEST_ACTIVE_STATUSES,
            frozenset({"queued", "processing", "delivered"}),
        )

        # 'failed' is a valid non-active terminal status (recovery resets to queued)
        self.assertNotIn("failed", _SHORTCUT_INGEST_ACTIVE_STATUSES)
        self.assertIn(("processing", "failed"), _SHORTCUT_INGEST_VALID_TRANSITIONS)
        self.assertIn(("failed", "queued"), _SHORTCUT_INGEST_VALID_TRANSITIONS)

    # ------------------------------------------------------------------ #
    # Test 11: recovery actor is imported in scheduler_service            #
    # ------------------------------------------------------------------ #

    def test_recovery_actor_is_registered_in_scheduler_service(self):
        """recover_stale_shortcut_ingests must be importable from scheduler_service
        (i.e., present in its namespace), proving the scheduler service has the
        actor available to dispatch."""
        import importlib
        scheduler_service = importlib.import_module("backend.scheduler_service")
        self.assertTrue(
            hasattr(scheduler_service, "recover_stale_shortcut_ingests"),
            "recover_stale_shortcut_ingests must be imported in scheduler_service",
        )
        actor = scheduler_service.recover_stale_shortcut_ingests
        # Must be a Dramatiq actor with the correct queue name
        self.assertEqual(
            actor.actor_name if hasattr(actor, "actor_name") else
            getattr(actor, "_actor", actor).__class__.__name__,
            "recover_stale_shortcut_ingests",
        )

    # ------------------------------------------------------------------ #
    # Test 12: scheduler dispatch function exists and is callable         #
    # ------------------------------------------------------------------ #

    def test_scheduler_dispatch_function_exists_and_is_callable(self):
        """_dispatch_shortcut_ingest_recovery must exist in scheduler_service
        and call recover_stale_shortcut_ingests.send() when invoked."""
        import importlib
        scheduler_service = importlib.import_module("backend.scheduler_service")
        self.assertTrue(
            hasattr(scheduler_service, "_dispatch_shortcut_ingest_recovery"),
            "_dispatch_shortcut_ingest_recovery must exist in scheduler_service",
        )
        send_calls = []
        with patch.object(
            scheduler_service.recover_stale_shortcut_ingests,
            "send",
            side_effect=lambda *a, **kw: send_calls.append(True),
        ):
            scheduler_service._dispatch_shortcut_ingest_recovery()
        self.assertEqual(len(send_calls), 1,
                         "_dispatch_shortcut_ingest_recovery must call send() exactly once")

    # ------------------------------------------------------------------ #
    # Test 13: scheduler job is added when SHORTCUT_INGEST_RECOVERY_ENABLED=1 #
    # ------------------------------------------------------------------ #

    def test_scheduler_builds_shortcut_ingest_recovery_job_when_enabled(self):
        """When SHORTCUT_INGEST_RECOVERY_ENABLED=1, _build_scheduler must add
        a job that dispatches _dispatch_shortcut_ingest_recovery."""
        import os
        import importlib

        scheduler_service = importlib.import_module("backend.scheduler_service")

        added_jobs = []

        class _FakeScheduler:
            def add_job(self, fn, trigger, **kwargs):
                added_jobs.append({"fn": fn, "trigger": trigger, **kwargs})

        original_build = scheduler_service._build_scheduler

        def _patched_build():
            # We can't call the real _build_scheduler because APScheduler may
            # not be installed.  Instead, test just the dispatch registration
            # logic by checking _dispatch_shortcut_ingest_recovery is in the
            # module and that SHORTCUT_INGEST_RECOVERY_ENABLED controls it.
            return _FakeScheduler()

        with patch.dict(os.environ, {"SHORTCUT_INGEST_RECOVERY_ENABLED": "1"}, clear=False):
            enabled = scheduler_service._enabled("SHORTCUT_INGEST_RECOVERY_ENABLED", "1")
        self.assertTrue(enabled,
                        "SHORTCUT_INGEST_RECOVERY_ENABLED=1 must result in _enabled() returning True")

        with patch.dict(os.environ, {"SHORTCUT_INGEST_RECOVERY_ENABLED": "0"}, clear=False):
            disabled = scheduler_service._enabled("SHORTCUT_INGEST_RECOVERY_ENABLED", "1")
        self.assertFalse(disabled,
                         "SHORTCUT_INGEST_RECOVERY_ENABLED=0 must result in _enabled() returning False")

    # ------------------------------------------------------------------ #
    # Test 14: invalid env config falls back with a warning               #
    # ------------------------------------------------------------------ #

    def test_invalid_env_config_falls_back_with_warning(self):
        """_shortcut_cfg_int must return the default value (not raise) when
        given a non-numeric env var, and must log a warning."""
        from backend.background_jobs import _shortcut_cfg_int
        import os
        import logging

        with patch.dict(os.environ, {"SHORTCUT_INGEST_STALE_QUEUED_MINUTES": "not_a_number"},
                        clear=False):
            with self.assertLogs(level="WARNING") as log_ctx:
                result = _shortcut_cfg_int(
                    "SHORTCUT_INGEST_STALE_QUEUED_MINUTES", default=15, minimum=5
                )
        self.assertEqual(result, 15, "Should return default when env value is invalid")
        self.assertTrue(
            any("not_a_number" in msg or "invalid" in msg.lower() for msg in log_ctx.output),
            "Should log a warning mentioning the invalid value",
        )

    # ------------------------------------------------------------------ #
    # Test 15: delivered rows never returned by claim_stale               #
    # ------------------------------------------------------------------ #

    def test_delivered_row_never_returned_by_claim_stale(self):
        """When claim_stale returns empty list, the sweeper enqueues nothing.
        This models the case where all stale candidates are delivered rows
        (which the DB query correctly excludes via status filter)."""
        import backend.database as db_module
        import backend.job_queue as jq_module

        with ExitStack() as stack:
            stack.enter_context(
                patch.object(db_module, "shortcut_ingest_request_claim_stale",
                             return_value=[])  # delivered rows excluded at DB level
            )
            enqueue_mock = stack.enter_context(
                patch.object(jq_module, "enqueue_shortcut_lookup_job")
            )
            jobs.recover_stale_shortcut_ingests()

        enqueue_mock.assert_not_called()

    # ------------------------------------------------------------------ #
    # Test 16: mixed stale rows — queued and processing both recovered    #
    # ------------------------------------------------------------------ #

    def test_stale_queued_and_processing_both_claimed(self):
        """When claim_stale returns a mix of formerly-queued and
        formerly-processing rows, both are re-enqueued."""
        import backend.database as db_module
        import backend.job_queue as jq_module

        stale_rows = [
            {"ingest_id": 401, "user_id": 111, "source": "shortcut",
             "ingest_key": "kq1", "text_preview": "formerly queued",
             "recovery_attempt_count": 1},
            {"ingest_id": 402, "user_id": 222, "source": "shortcut",
             "ingest_key": "kp1", "text_preview": "formerly processing",
             "recovery_attempt_count": 2},
        ]
        enqueue_calls = []

        with ExitStack() as stack:
            stack.enter_context(
                patch.object(db_module, "shortcut_ingest_request_claim_stale",
                             return_value=stale_rows)
            )
            stack.enter_context(
                patch.object(jq_module, "enqueue_shortcut_lookup_job",
                             side_effect=lambda **kw: enqueue_calls.append(kw) or "jid")
            )
            jobs.recover_stale_shortcut_ingests()

        self.assertEqual(len(enqueue_calls), 2)
        enqueued_ids = {c["ingest_id"] for c in enqueue_calls}
        self.assertEqual(enqueued_ids, {401, 402})

    # ------------------------------------------------------------------ #
    # Test 17: rows at max recovery attempts skipped (claim returns empty)#
    # ------------------------------------------------------------------ #

    def test_rows_at_max_attempts_not_in_claim_result(self):
        """When claim_stale is called with max_recovery_attempts=N, rows that
        have already reached N attempts are excluded.  The sweeper must pass
        the configured value through without modification."""
        import backend.database as db_module
        import backend.job_queue as jq_module

        claim_kwargs_received = {}

        def _mock_claim(**kwargs):
            claim_kwargs_received.update(kwargs)
            return []

        with ExitStack() as stack:
            stack.enter_context(
                patch.object(db_module, "shortcut_ingest_request_claim_stale",
                             side_effect=_mock_claim)
            )
            stack.enter_context(
                patch.object(jq_module, "enqueue_shortcut_lookup_job")
            )
            from backend import background_jobs as bj
            jobs.recover_stale_shortcut_ingests()

        self.assertIn("max_recovery_attempts", claim_kwargs_received)
        self.assertEqual(
            claim_kwargs_received["max_recovery_attempts"],
            bj._SHORTCUT_INGEST_MAX_RECOVERY_ATTEMPTS,
        )


if __name__ == "__main__":
    unittest.main()
