"""
Tests for durable Shortcut ingest idempotency (PART 6 of the implementation spec).

Each test uses only mocks — no real DB or Redis required.
"""
import os
import unittest
from contextlib import ExitStack
from unittest.mock import patch

import backend.backend_server as server
import backend.background_jobs as jobs


def _route_patches(*, dedup_reserve=False, jobs_available=True):
    """Return list of patch objects shared by route tests."""
    return [
        patch.dict(os.environ, {"SHORTCUT_SECRET": "secret"}, clear=False),
        patch.object(server, "is_telegram_user_allowed", return_value=True),
        patch.object(server, "_check_free_daily_usage_limit", return_value=(
            {"effective_mode": "free", "used": 0, "limit": 20}, None, None,
        )),
        patch.object(server, "_shortcut_dedup_reserve", return_value=dedup_reserve),
        patch.object(server, "can_enqueue_background_jobs", return_value=jobs_available),
    ]


class ShortcutIngestIdempotencyTests(unittest.TestCase):
    def setUp(self):
        self.client = server.app.test_client()

    # ------------------------------------------------------------------ #
    # Test 1: first payload creates durable row and enqueues              #
    # ------------------------------------------------------------------ #

    def test_first_payload_creates_ingest_row_and_enqueues(self):
        upsert_result = {"ingest_id": 42, "is_new": True, "status": "queued",
                         "job_id": None, "duplicate_count": 0}
        with ExitStack() as stack:
            upsert_mock = stack.enter_context(
                patch.object(server, "shortcut_ingest_request_upsert", return_value=upsert_result)
            )
            stack.enter_context(
                patch.object(server, "shortcut_ingest_request_set_status")
            )
            stack.enter_context(
                patch.object(server, "enqueue_shortcut_lookup_job", return_value="jid1")
            )
            for p in _route_patches():
                stack.enter_context(p)

            response = self.client.post(
                "/api/shortcut/lookup",
                json={"text": "sich abfinden mit", "user_id": 117649764},
                headers={"Authorization": "Bearer secret"},
            )

        payload = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertTrue(payload["accepted"])
        self.assertTrue(payload["queued"])
        self.assertFalse(payload["duplicate"])
        self.assertEqual(payload["ingest_id"], 42)
        upsert_mock.assert_called_once()
        call_kwargs = upsert_mock.call_args.kwargs
        self.assertEqual(call_kwargs["user_id"], 117649764)
        self.assertEqual(call_kwargs["source"], "shortcut")
        self.assertIsNotNone(call_kwargs["ingest_key"])

    # ------------------------------------------------------------------ #
    # Test 2: same payload returns duplicate=True and does NOT enqueue    #
    # ------------------------------------------------------------------ #

    def test_repeated_payload_returns_duplicate_no_enqueue(self):
        upsert_result = {"ingest_id": 42, "is_new": False, "status": "delivered",
                         "job_id": "jid1", "duplicate_count": 1}
        with ExitStack() as stack:
            stack.enter_context(
                patch.object(server, "shortcut_ingest_request_upsert", return_value=upsert_result)
            )
            enqueue_mock = stack.enter_context(
                patch.object(server, "enqueue_shortcut_lookup_job")
            )
            for p in _route_patches():
                stack.enter_context(p)

            response = self.client.post(
                "/api/shortcut/lookup",
                json={"text": "sich abfinden mit", "user_id": 117649764},
                headers={"Authorization": "Bearer secret"},
            )

        payload = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertFalse(payload["accepted"])
        self.assertTrue(payload["duplicate"])
        self.assertEqual(payload["ingest_id"], 42)
        enqueue_mock.assert_not_called()

    # ------------------------------------------------------------------ #
    # Test 3: duplicate after Redis TTL — DB blocks re-enqueue            #
    # ------------------------------------------------------------------ #

    def test_duplicate_after_redis_ttl_still_blocked_by_db(self):
        """
        Simulate Redis TTL expiry: _shortcut_dedup_reserve returns False (no Redis hit),
        but DB upsert returns is_new=False with delivered status.
        Must NOT re-enqueue.
        """
        upsert_result = {"ingest_id": 77, "is_new": False, "status": "delivered",
                         "job_id": "jid2", "duplicate_count": 2}
        with ExitStack() as stack:
            stack.enter_context(
                patch.object(server, "shortcut_ingest_request_upsert", return_value=upsert_result)
            )
            enqueue_mock = stack.enter_context(
                patch.object(server, "enqueue_shortcut_lookup_job")
            )
            # dedup_reserve=False simulates Redis TTL expiry (key not cached)
            for p in _route_patches(dedup_reserve=False):
                stack.enter_context(p)

            response = self.client.post(
                "/api/shortcut/lookup",
                json={"text": "sich abfinden mit", "user_id": 117649764},
                headers={"Authorization": "Bearer secret"},
            )

        payload = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertTrue(payload["duplicate"])
        enqueue_mock.assert_not_called()

    # ------------------------------------------------------------------ #
    # Test 4: duplicate with processing status also blocked               #
    # ------------------------------------------------------------------ #

    def test_duplicate_processing_status_blocks_enqueue(self):
        upsert_result = {"ingest_id": 55, "is_new": False, "status": "processing",
                         "job_id": "jid3", "duplicate_count": 1}
        with ExitStack() as stack:
            stack.enter_context(
                patch.object(server, "shortcut_ingest_request_upsert", return_value=upsert_result)
            )
            enqueue_mock = stack.enter_context(
                patch.object(server, "enqueue_shortcut_lookup_job")
            )
            for p in _route_patches():
                stack.enter_context(p)

            response = self.client.post(
                "/api/shortcut/lookup",
                json={"text": "sich abfinden mit", "user_id": 117649764},
                headers={"Authorization": "Bearer secret"},
            )

        self.assertTrue(response.get_json()["duplicate"])
        enqueue_mock.assert_not_called()

    # ------------------------------------------------------------------ #
    # Test 5: worker transitions queued→processing then →delivered        #
    # ------------------------------------------------------------------ #

    def test_worker_transitions_queued_to_processing_then_delivered(self):
        import backend.database as db_module

        row = {"ingest_id": 42, "status": "queued", "job_id": "j1",
               "duplicate_count": 0, "sent_prompt_count": 0,
               "accepted_at": None, "completed_at": None, "error_code": None,
               "normalized_text_full": None, "normalized_text_sha256": "",
               "normalized_text_size_bytes": 0}

        set_status_calls = []

        def _set_status(*, ingest_id, status, **_kw):
            set_status_calls.append(status)

        with ExitStack() as stack:
            # Worker now calls get_by_id; patch it instead of get_by_key
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
                      return_value=3)
            )
            jobs.run_shortcut_lookup_job(
                user_id=117649764,
                text="sich abfinden mit",
                source="shortcut",
                ingest_key="abc123",
                ingest_id=42,
            )

        self.assertIn("processing", set_status_calls)
        self.assertIn("delivered", set_status_calls)
        self.assertLess(
            set_status_calls.index("processing"),
            set_status_calls.index("delivered"),
        )

    # ------------------------------------------------------------------ #
    # Test 6: worker delivery failure records failed status               #
    # ------------------------------------------------------------------ #

    def test_worker_delivery_failure_sets_failed_status(self):
        import backend.database as db_module

        row = {"ingest_id": 42, "status": "queued", "job_id": "j1",
               "duplicate_count": 0, "sent_prompt_count": 0,
               "accepted_at": None, "completed_at": None, "error_code": None,
               "normalized_text_full": None, "normalized_text_sha256": "",
               "normalized_text_size_bytes": 0}

        set_status_calls = []

        def _set_status(*, ingest_id, status, **_kw):
            set_status_calls.append(status)

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
                      side_effect=RuntimeError("Telegram unavailable"))
            )
            with self.assertRaises(RuntimeError):
                jobs.run_shortcut_lookup_job(
                    user_id=117649764,
                    text="sich abfinden mit",
                    source="shortcut",
                    ingest_key="abc123",
                    ingest_id=42,
                )

        self.assertIn("processing", set_status_calls)
        self.assertIn("failed", set_status_calls)
        self.assertNotIn("delivered", set_status_calls)

    # ------------------------------------------------------------------ #
    # Test 7: DB upsert failure returns 503 and does not enqueue          #
    # ------------------------------------------------------------------ #

    def test_db_upsert_failure_returns_503_and_does_not_enqueue(self):
        with ExitStack() as stack:
            stack.enter_context(
                patch.object(server, "shortcut_ingest_request_upsert",
                             side_effect=RuntimeError("DB connection failed"))
            )
            enqueue_mock = stack.enter_context(
                patch.object(server, "enqueue_shortcut_lookup_job")
            )
            for p in _route_patches():
                stack.enter_context(p)

            response = self.client.post(
                "/api/shortcut/lookup",
                json={"text": "sich abfinden mit", "user_id": 117649764},
                headers={"Authorization": "Bearer secret"},
            )

        self.assertEqual(response.status_code, 503)
        self.assertIn("error", response.get_json())
        enqueue_mock.assert_not_called()

    # ------------------------------------------------------------------ #
    # Test 8: worker skips already-delivered row (no Telegram re-send)    #
    # ------------------------------------------------------------------ #

    def test_worker_skips_already_delivered_row(self):
        import backend.database as db_module

        row = {"ingest_id": 42, "status": "delivered", "job_id": "j1",
               "duplicate_count": 1, "sent_prompt_count": 2,
               "accepted_at": None, "completed_at": None, "error_code": None,
               "normalized_text_full": None, "normalized_text_sha256": "",
               "normalized_text_size_bytes": 0}

        with ExitStack() as stack:
            stack.enter_context(
                patch.object(db_module, "shortcut_ingest_request_get_by_id",
                             return_value=row)
            )
            set_status_mock = stack.enter_context(
                patch.object(db_module, "shortcut_ingest_request_set_status")
            )
            delivery_mock = stack.enter_context(
                patch("backend.backend_server._run_shortcut_lookup_delivery")
            )
            jobs.run_shortcut_lookup_job(
                user_id=117649764,
                text="sich abfinden mit",
                source="shortcut",
                ingest_key="abc123",
                ingest_id=42,
            )

        delivery_mock.assert_not_called()
        for c in set_status_mock.call_args_list:
            self.assertNotEqual(c.kwargs.get("status"), "delivered")


if __name__ == "__main__":
    unittest.main()
