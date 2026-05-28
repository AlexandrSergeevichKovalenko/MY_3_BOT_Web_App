"""
Tests for durable Shortcut ingest full payload persistence (PART 1-7 of the spec).

Covers:
  1.  test_full_normalized_payload_persisted
  2.  test_worker_uses_db_payload_not_queue_text
  3.  test_recovery_uses_db_payload
  4.  test_payload_integrity_mismatch_fails_explicitly
  5.  test_oversized_payload_blocked_explicitly
  6.  test_payload_survives_queue_text_loss
  7.  test_text_preview_truncation_no_longer_affects_recovery
  8.  test_queue_payload_can_omit_full_text_safely
  9.  test_retention_config_parsing
"""
import hashlib
import os
import unittest
from contextlib import ExitStack
from unittest.mock import patch, MagicMock

import backend.backend_server as server
import backend.background_jobs as jobs
import backend.database as db_module
from backend.database import (
    _shortcut_db_cfg_int,
    _SHORTCUT_INGEST_MAX_TEXT_BYTES,
    shortcut_ingest_request_upsert,
)


def _make_db_row(
    *,
    ingest_id=42,
    status="queued",
    normalized_text_full=None,
    normalized_text_sha256="",
    normalized_text_size_bytes=0,
):
    """Return a minimal dict that mimics what get_by_id returns."""
    return {
        "ingest_id": ingest_id,
        "status": status,
        "job_id": None,
        "duplicate_count": 0,
        "sent_prompt_count": 0,
        "accepted_at": None,
        "completed_at": None,
        "error_code": None,
        "queued_at": None,
        "processing_started_at": None,
        "last_worker_heartbeat_at": None,
        "recovery_attempt_count": 0,
        "worker_id": None,
        "enqueue_attempt_count": 0,
        "normalized_text_full": normalized_text_full,
        "normalized_text_sha256": normalized_text_sha256,
        "normalized_text_size_bytes": normalized_text_size_bytes,
    }


def _route_patches(*, dedup_reserve=False, jobs_available=True):
    return [
        patch.dict(os.environ, {"SHORTCUT_SECRET": "secret"}, clear=False),
        patch.object(server, "is_telegram_user_allowed", return_value=True),
        patch.object(server, "_check_free_daily_usage_limit", return_value=(
            {"effective_mode": "free", "used": 0, "limit": 20}, None, None,
        )),
        patch.object(server, "_shortcut_dedup_reserve", return_value=dedup_reserve),
        patch.object(server, "can_enqueue_background_jobs", return_value=jobs_available),
    ]


class ShortcutPayloadPersistenceTests(unittest.TestCase):
    def setUp(self):
        self.client = server.app.test_client()

    # ------------------------------------------------------------------ #
    # Test 1: upsert is called with normalized_text_full                  #
    # ------------------------------------------------------------------ #

    def test_full_normalized_payload_persisted(self):
        """Route must pass normalized_text_full to upsert, not just text_preview."""
        upsert_result = {"ingest_id": 42, "is_new": True, "status": "queued",
                         "job_id": None, "duplicate_count": 0}
        with ExitStack() as stack:
            upsert_mock = stack.enter_context(
                patch.object(server, "shortcut_ingest_request_upsert",
                             return_value=upsert_result)
            )
            stack.enter_context(patch.object(server, "shortcut_ingest_request_set_status"))
            stack.enter_context(
                patch.object(server, "enqueue_shortcut_lookup_job", return_value="jid1")
            )
            for p in _route_patches():
                stack.enter_context(p)

            long_text = "sich abfinden mit " * 20  # >200 chars, tests truncation issue
            response = self.client.post(
                "/api/shortcut/lookup",
                json={"text": long_text, "user_id": 117649764},
                headers={"Authorization": "Bearer secret"},
            )

        self.assertEqual(response.status_code, 200)
        upsert_mock.assert_called_once()
        kwargs = upsert_mock.call_args.kwargs
        # normalized_text_full must be present and be the full (non-truncated) text
        self.assertIn("normalized_text_full", kwargs)
        stored_full = kwargs["normalized_text_full"]
        self.assertGreater(len(stored_full), 200,
                           "normalized_text_full must carry the full text, not just 200 chars")
        # text_preview is still present for display purposes
        self.assertIn("text_preview", kwargs)

    # ------------------------------------------------------------------ #
    # Test 2: worker uses DB payload, not queue text                      #
    # ------------------------------------------------------------------ #

    def test_worker_uses_db_payload_not_queue_text(self):
        """Worker must deliver the DB-stored full text, ignoring the (truncated) queue text."""
        full_text = "ich freue mich darauf " * 30   # long text
        sha256 = hashlib.sha256(full_text.encode("utf-8")).hexdigest()
        row = _make_db_row(
            ingest_id=99,
            status="queued",
            normalized_text_full=full_text,
            normalized_text_sha256=sha256,
            normalized_text_size_bytes=len(full_text.encode("utf-8")),
        )

        delivery_calls = []

        with ExitStack() as stack:
            stack.enter_context(
                patch.object(db_module, "shortcut_ingest_request_get_by_id", return_value=row)
            )
            stack.enter_context(
                patch.object(db_module, "shortcut_ingest_request_set_status")
            )
            stack.enter_context(
                patch("backend.backend_server._run_shortcut_lookup_delivery",
                      side_effect=lambda **kw: delivery_calls.append(kw) or 1)
            )

            jobs.run_shortcut_lookup_job(
                user_id=117649764,
                text="short truncated queue text",  # this should NOT be used
                source="shortcut",
                ingest_key="abc123",
                ingest_id=99,
            )

        self.assertEqual(len(delivery_calls), 1)
        self.assertEqual(delivery_calls[0]["text"], full_text,
                         "Worker must use DB full text, not the truncated queue text")

    # ------------------------------------------------------------------ #
    # Test 3: recovery sweeper enqueues truncated text, but worker        #
    # uses DB payload                                                     #
    # ------------------------------------------------------------------ #

    def test_recovery_uses_db_payload(self):
        """
        Sweeper sends text_preview (truncated ≤200 chars) in queue,
        but worker must load DB payload and use the full text.
        """
        full_text = "das kommt darauf an " * 50   # full text, >200 chars
        truncated_preview = full_text[:200]
        sha256 = hashlib.sha256(full_text.encode("utf-8")).hexdigest()

        row = _make_db_row(
            ingest_id=55,
            status="queued",
            normalized_text_full=full_text,
            normalized_text_sha256=sha256,
            normalized_text_size_bytes=len(full_text.encode("utf-8")),
        )

        delivery_calls = []

        with ExitStack() as stack:
            stack.enter_context(
                patch.object(db_module, "shortcut_ingest_request_get_by_id", return_value=row)
            )
            stack.enter_context(
                patch.object(db_module, "shortcut_ingest_request_set_status")
            )
            stack.enter_context(
                patch("backend.backend_server._run_shortcut_lookup_delivery",
                      side_effect=lambda **kw: delivery_calls.append(kw) or 1)
            )

            # Simulate recovery: sweeper passes truncated text_preview
            jobs.run_shortcut_lookup_job(
                user_id=117649764,
                text=truncated_preview,   # truncated text from recovery row
                source="shortcut",
                ingest_key="abc123",
                ingest_id=55,
            )

        self.assertEqual(len(delivery_calls), 1)
        self.assertEqual(delivery_calls[0]["text"], full_text,
                         "Worker must use DB full text even when recovery enqueued truncated text")

    # ------------------------------------------------------------------ #
    # Test 4: integrity mismatch → status=failed, no delivery             #
    # ------------------------------------------------------------------ #

    def test_payload_integrity_mismatch_fails_explicitly(self):
        """When stored sha256 does not match, worker must set status=failed and NOT deliver."""
        real_text = "guten morgen"
        wrong_sha256 = "aaaaaaaa" + "0" * 56   # wrong hash (64 hex chars total)
        row = _make_db_row(
            ingest_id=77,
            status="queued",
            normalized_text_full=real_text,
            normalized_text_sha256=wrong_sha256,
            normalized_text_size_bytes=len(real_text.encode("utf-8")),
        )

        set_status_calls = []

        with ExitStack() as stack:
            stack.enter_context(
                patch.object(db_module, "shortcut_ingest_request_get_by_id", return_value=row)
            )
            stack.enter_context(
                patch.object(db_module, "shortcut_ingest_request_set_status",
                             side_effect=lambda **kw: set_status_calls.append(kw))
            )
            delivery_mock = stack.enter_context(
                patch("backend.backend_server._run_shortcut_lookup_delivery")
            )

            jobs.run_shortcut_lookup_job(
                user_id=117649764,
                text="guten morgen",
                source="shortcut",
                ingest_key="abc123",
                ingest_id=77,
            )

        delivery_mock.assert_not_called()
        failed_calls = [c for c in set_status_calls if c.get("status") == "failed"]
        self.assertTrue(failed_calls, "set_status must be called with status=failed on integrity failure")
        self.assertEqual(
            failed_calls[0].get("error_code"), "payload_integrity_failed",
            "error_code must be 'payload_integrity_failed'",
        )

    # ------------------------------------------------------------------ #
    # Test 5: oversized payload blocked with explicit ValueError           #
    # ------------------------------------------------------------------ #

    def test_oversized_payload_blocked_explicitly(self):
        """upsert must raise ValueError when normalized_text_full exceeds max bytes."""
        oversized = "x" * (_SHORTCUT_INGEST_MAX_TEXT_BYTES + 1)

        with self.assertRaises(ValueError) as ctx:
            with patch("backend.database.get_db_connection_context") as mock_conn_ctx:
                # Ensure DB is never actually called
                shortcut_ingest_request_upsert(
                    user_id=1,
                    source="shortcut",
                    ingest_key="k1",
                    text_preview="preview",
                    normalized_text_full=oversized,
                )

        self.assertIn("shortcut_ingest_payload_too_large", str(ctx.exception))
        # DB must never be called for an oversized payload
        mock_conn_ctx.assert_not_called()

    # ------------------------------------------------------------------ #
    # Test 6: payload survives queue text loss                            #
    # ------------------------------------------------------------------ #

    def test_payload_survives_queue_text_loss(self):
        """
        Even if queue text is empty or completely wrong, delivery must use DB payload.
        """
        full_text = "es liegt an dir " * 20
        sha256 = hashlib.sha256(full_text.encode("utf-8")).hexdigest()
        row = _make_db_row(
            ingest_id=88,
            status="queued",
            normalized_text_full=full_text,
            normalized_text_sha256=sha256,
            normalized_text_size_bytes=len(full_text.encode("utf-8")),
        )

        delivery_calls = []

        with ExitStack() as stack:
            stack.enter_context(
                patch.object(db_module, "shortcut_ingest_request_get_by_id", return_value=row)
            )
            stack.enter_context(
                patch.object(db_module, "shortcut_ingest_request_set_status")
            )
            stack.enter_context(
                patch("backend.backend_server._run_shortcut_lookup_delivery",
                      side_effect=lambda **kw: delivery_calls.append(kw) or 1)
            )

            # Queue text is completely different (simulates loss/corruption)
            jobs.run_shortcut_lookup_job(
                user_id=117649764,
                text="WRONG TEXT ENTIRELY",
                source="shortcut",
                ingest_key="abc123",
                ingest_id=88,
            )

        self.assertEqual(len(delivery_calls), 1)
        self.assertEqual(delivery_calls[0]["text"], full_text,
                         "Delivery must use DB payload even when queue text is wrong")

    # ------------------------------------------------------------------ #
    # Test 7: truncated queue text ignored when full text is in DB        #
    # ------------------------------------------------------------------ #

    def test_text_preview_truncation_no_longer_affects_recovery(self):
        """Worker ignores ≤200 char queue text when full text is in DB."""
        full_text = "aufpassen auf " * 30
        preview = full_text[:200]
        sha256 = hashlib.sha256(full_text.encode("utf-8")).hexdigest()
        row = _make_db_row(
            ingest_id=101,
            status="queued",
            normalized_text_full=full_text,
            normalized_text_sha256=sha256,
            normalized_text_size_bytes=len(full_text.encode("utf-8")),
        )

        delivery_calls = []

        with ExitStack() as stack:
            stack.enter_context(
                patch.object(db_module, "shortcut_ingest_request_get_by_id", return_value=row)
            )
            stack.enter_context(
                patch.object(db_module, "shortcut_ingest_request_set_status")
            )
            stack.enter_context(
                patch("backend.backend_server._run_shortcut_lookup_delivery",
                      side_effect=lambda **kw: delivery_calls.append(kw) or 1)
            )

            jobs.run_shortcut_lookup_job(
                user_id=117649764,
                text=preview,  # only first 200 chars in queue (recovery path)
                source="shortcut",
                ingest_key="abc123",
                ingest_id=101,
            )

        self.assertEqual(len(delivery_calls), 1)
        self.assertGreater(
            len(delivery_calls[0]["text"]), 200,
            "Delivery text must be full DB payload, not the 200-char preview",
        )
        self.assertEqual(delivery_calls[0]["text"], full_text)

    # ------------------------------------------------------------------ #
    # Test 8: recovery sends truncated text in queue, worker uses DB      #
    # ------------------------------------------------------------------ #

    def test_queue_payload_can_omit_full_text_safely(self):
        """
        Sweeper may send only text_preview in the queue message.
        Worker must load the full text from DB and deliver correctly.
        """
        full_text = "ich bin gespannt " * 40
        truncated = full_text[:200]
        sha256 = hashlib.sha256(full_text.encode("utf-8")).hexdigest()
        row = _make_db_row(
            ingest_id=202,
            status="queued",
            normalized_text_full=full_text,
            normalized_text_sha256=sha256,
            normalized_text_size_bytes=len(full_text.encode("utf-8")),
        )

        delivery_calls = []

        with ExitStack() as stack:
            stack.enter_context(
                patch.object(db_module, "shortcut_ingest_request_get_by_id", return_value=row)
            )
            stack.enter_context(
                patch.object(db_module, "shortcut_ingest_request_set_status")
            )
            stack.enter_context(
                patch("backend.backend_server._run_shortcut_lookup_delivery",
                      side_effect=lambda **kw: delivery_calls.append(kw) or 1)
            )

            jobs.run_shortcut_lookup_job(
                user_id=117649764,
                text=truncated,
                source="shortcut",
                ingest_key="abc123",
                ingest_id=202,
            )

        self.assertEqual(len(delivery_calls), 1)
        self.assertEqual(delivery_calls[0]["text"], full_text)

    # ------------------------------------------------------------------ #
    # Test 9: retention config parsing is safe                            #
    # ------------------------------------------------------------------ #

    def test_retention_config_parsing(self):
        """_shortcut_db_cfg_int returns default on invalid env var; valid values preserved."""
        # Invalid string → default
        with patch.dict(os.environ, {"SHORTCUT_INGEST_RETENTION_DAYS": "not_a_number"}):
            val = _shortcut_db_cfg_int("SHORTCUT_INGEST_RETENTION_DAYS", default=30, minimum=7)
        self.assertEqual(val, 30, "Should return default on invalid env var")

        # Below minimum → clamped
        with patch.dict(os.environ, {"SHORTCUT_INGEST_RETENTION_DAYS": "3"}):
            val = _shortcut_db_cfg_int("SHORTCUT_INGEST_RETENTION_DAYS", default=30, minimum=7)
        self.assertEqual(val, 7, "Should clamp to minimum")

        # Valid value above minimum → preserved
        with patch.dict(os.environ, {"SHORTCUT_INGEST_RETENTION_DAYS": "60"}):
            val = _shortcut_db_cfg_int("SHORTCUT_INGEST_RETENTION_DAYS", default=30, minimum=7)
        self.assertEqual(val, 60, "Should return the configured value")

        # Empty string → default
        with patch.dict(os.environ, {"SHORTCUT_INGEST_RETENTION_DAYS": ""}):
            val = _shortcut_db_cfg_int("SHORTCUT_INGEST_RETENTION_DAYS", default=30, minimum=7)
        self.assertEqual(val, 30, "Should return default for empty env var")

        # MAX_TEXT_BYTES: invalid → default 50000
        with patch.dict(os.environ, {"SHORTCUT_INGEST_MAX_TEXT_BYTES": "abc"}):
            val = _shortcut_db_cfg_int("SHORTCUT_INGEST_MAX_TEXT_BYTES", default=50000, minimum=1000)
        self.assertEqual(val, 50000)

        # MAX_TEXT_BYTES: valid → used
        with patch.dict(os.environ, {"SHORTCUT_INGEST_MAX_TEXT_BYTES": "10000"}):
            val = _shortcut_db_cfg_int("SHORTCUT_INGEST_MAX_TEXT_BYTES", default=50000, minimum=1000)
        self.assertEqual(val, 10000)


if __name__ == "__main__":
    unittest.main()
