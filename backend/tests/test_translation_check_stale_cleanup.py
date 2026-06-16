import unittest
from contextlib import contextmanager
from datetime import datetime, timezone
from unittest.mock import Mock
from unittest.mock import call
from unittest.mock import patch

from backend.scheduler_jobs_core import run_translation_check_stale_cleanup_job
from backend.translation_check_worker_schedule import count_active_translation_check_sessions


class _DummyCursor:
    def __init__(self, responses):
        self._responses = list(responses)
        self.executed = []

    def execute(self, query, params=None):
        self.executed.append((query, params))

    def fetchone(self):
        if not self._responses:
            return None
        return self._responses.pop(0)

    def fetchall(self):
        if not self._responses:
            return []
        next_item = self._responses.pop(0)
        return next_item if isinstance(next_item, list) else []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _DummyConnection:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def _db_context(cursor):
    @contextmanager
    def _context():
        yield _DummyConnection(cursor)

    return _context


class TranslationCheckStaleCleanupTests(unittest.TestCase):
    def test_count_active_translation_check_sessions_uses_fresh_activity_filter(self) -> None:
        cursor = _DummyCursor([
            (
                2,
                1,
                3,
                4,
                datetime(2026, 6, 14, 7, 8, tzinfo=timezone.utc),
                datetime(2026, 6, 14, 7, 9, tzinfo=timezone.utc),
            )
        ])

        with patch("backend.database.get_translation_check_stale_session_max_age_minutes", return_value=60), patch(
            "backend.database.get_db_connection_context",
            _db_context(cursor),
        ):
            result = count_active_translation_check_sessions()

        self.assertEqual(result["queued_sessions"], 2)
        self.assertEqual(result["running_sessions"], 1)
        self.assertEqual(result["pending_sessions"], 3)
        self.assertEqual(result["stale_sessions"], 4)
        self.assertEqual(len(cursor.executed), 1)
        sql_text = cursor.executed[0][0]
        self.assertIn("COALESCE(heartbeat_at, started_at, dispatched_at, created_at)", sql_text)
        self.assertIn("is_fresh", sql_text)

    def test_cleanup_job_clears_state_after_db_cleanup(self) -> None:
        cleanup_result = {
            "stale_minutes": 60,
            "session_count": 2,
            "item_updates": 14,
            "session_ids": [2242, 2259],
            "cleanup_reason": "translation_check_session_stale_cleanup",
        }
        with patch(
            "backend.scheduler_jobs_core.cleanup_stale_translation_check_sessions",
            return_value=cleanup_result,
        ) as cleanup_mock, patch(
            "backend.scheduler_jobs_core.clear_translation_check_session_state"
        ) as clear_mock:
            run_translation_check_stale_cleanup_job()

        cleanup_mock.assert_called_once_with(
            stale_minutes=60,
            limit=100,
            cleanup_reason="translation_check_session_stale_cleanup",
        )
        clear_mock.assert_has_calls([call(2242), call(2259)])
        self.assertEqual(clear_mock.call_count, 2)

