import unittest
from unittest.mock import patch

from backend import database


class _FakeCursor:
    def __init__(self, *, fetchone_result=None, rowcount=0):
        self.fetchone_result = fetchone_result
        self.rowcount = rowcount
        self.executed: list[tuple[str, tuple | list | None]] = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchone(self):
        return self.fetchone_result


class _FakeConnection:
    def __init__(self, cursor: _FakeCursor):
        self._cursor = cursor

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def cursor(self):
        return self._cursor


class Phase1SnapshotLazyEnsureTests(unittest.TestCase):
    def setUp(self) -> None:
        self._orig_done = database._ENSURE_PHASE1_PROJECTION_SCHEMA_DONE
        database._ENSURE_PHASE1_PROJECTION_SCHEMA_DONE = False
        self.addCleanup(self._restore_done_flag)

    def _restore_done_flag(self) -> None:
        database._ENSURE_PHASE1_PROJECTION_SCHEMA_DONE = self._orig_done

    def test_get_user_api_snapshot_triggers_lazy_ensure_once(self) -> None:
        cursor = _FakeCursor(fetchone_result=None)
        conn = _FakeConnection(cursor)
        calls: list[str] = []

        def _fake_ensure():
            calls.append("ensure")
            database._ENSURE_PHASE1_PROJECTION_SCHEMA_DONE = True

        with patch("backend.database.ensure_phase1_projection_schema", side_effect=_fake_ensure), patch(
            "backend.database.get_db_connection_context",
            return_value=conn,
        ):
            database.get_user_api_snapshot(user_id=1, snapshot_kind="today_plan", snapshot_key="today")
            database.get_user_api_snapshot(user_id=1, snapshot_kind="today_plan", snapshot_key="today")

        self.assertEqual(calls, ["ensure"])
        self.assertEqual(len(cursor.executed), 2)

    def test_upsert_user_api_snapshot_triggers_lazy_ensure_once(self) -> None:
        row = (1, "today_plan", "today", "ru", "de", {"ok": True}, {}, None, None, None, None)
        cursor = _FakeCursor(fetchone_result=row)
        conn = _FakeConnection(cursor)
        calls: list[str] = []

        def _fake_ensure():
            calls.append("ensure")
            database._ENSURE_PHASE1_PROJECTION_SCHEMA_DONE = True

        with patch("backend.database.ensure_phase1_projection_schema", side_effect=_fake_ensure), patch(
            "backend.database.get_db_connection_context",
            return_value=conn,
        ):
            database.upsert_user_api_snapshot(
                user_id=1,
                snapshot_kind="today_plan",
                snapshot_key="today",
                payload={"ok": True},
            )
            database.upsert_user_api_snapshot(
                user_id=1,
                snapshot_kind="today_plan",
                snapshot_key="today",
                payload={"ok": True},
            )

        self.assertEqual(calls, ["ensure"])
        self.assertEqual(len(cursor.executed), 2)

    def test_mark_user_api_snapshots_stale_triggers_lazy_ensure_once(self) -> None:
        cursor = _FakeCursor(rowcount=3)
        conn = _FakeConnection(cursor)
        calls: list[str] = []

        def _fake_ensure():
            calls.append("ensure")
            database._ENSURE_PHASE1_PROJECTION_SCHEMA_DONE = True

        with patch("backend.database.ensure_phase1_projection_schema", side_effect=_fake_ensure), patch(
            "backend.database.get_db_connection_context",
            return_value=conn,
        ):
            first = database.mark_user_api_snapshots_stale(user_id=1, snapshot_kind="today_plan")
            second = database.mark_user_api_snapshots_stale(user_id=1, snapshot_kind="today_plan")

        self.assertEqual(calls, ["ensure"])
        self.assertEqual(first, 3)
        self.assertEqual(second, 3)
        self.assertEqual(len(cursor.executed), 2)

    def test_existing_projection_job_helper_still_calls_projection_ensure(self) -> None:
        row = (
            9,
            "refresh",
            "today_card",
            1,
            None,
            None,
            None,
            "live",
            "projection_materialization_live",
            "pending",
            0,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        cursor = _FakeCursor(fetchone_result=row)
        conn = _FakeConnection(cursor)

        with patch("backend.database.ensure_phase1_projection_schema") as ensure_mock, patch(
            "backend.database.get_db_connection_context",
            return_value=conn,
        ):
            database.upsert_projection_job(
                job_kind="refresh",
                projection_kind="today_card",
                user_id=1,
            )

        ensure_mock.assert_called_once()


if __name__ == "__main__":
    unittest.main()
