"""Focused tests for PgBouncer rollout readiness.

Covers:
  * connection-target normalization accepts the documented aliases
  * missing PgBouncer URL surfaces a selection error (no silent fallback)
  * sanitized URL metadata / rollout status never leak credentials or DSNs
  * the read-only smoke helper writes no user data
"""

import unittest

import backend.database as database
from backend.db_pgbouncer_smoke import run_pgbouncer_smoke_test


class TargetNormalizationTests(unittest.TestCase):
    def test_aliases_map_to_pgbouncer_transaction(self):
        for raw in [
            "pgbouncer",
            "pgbouncer_transaction",
            "transaction",
            "transaction_pool",
            "  PgBouncer_Transaction  ",
            "TRANSACTION",
        ]:
            self.assertEqual(
                database._normalize_db_connection_target(raw),
                "pgbouncer_transaction",
                msg=f"alias {raw!r} should normalize to pgbouncer_transaction",
            )

    def test_unknown_or_empty_maps_to_direct(self):
        for raw in ["", None, "direct", "foo", "session"]:
            self.assertEqual(database._normalize_db_connection_target(raw), "direct")


class UrlSelectionTests(unittest.TestCase):
    def test_pgbouncer_target_without_url_reports_error(self):
        url, source, error = database._compute_database_url_selection(
            "pgbouncer_transaction", "postgresql://direct/db", ""
        )
        self.assertEqual(url, "postgresql://direct/db")  # no silent change
        self.assertEqual(source, "DATABASE_URL_RAILWAY")
        self.assertIsNotNone(error)
        self.assertIn("DATABASE_URL_PGBOUNCER", error)

    def test_pgbouncer_target_with_url_selects_pgbouncer(self):
        url, source, error = database._compute_database_url_selection(
            "pgbouncer_transaction", "postgresql://direct/db", "postgresql://pgb/db"
        )
        self.assertEqual(url, "postgresql://pgb/db")
        self.assertEqual(source, "DATABASE_URL_PGBOUNCER")
        self.assertIsNone(error)

    def test_direct_target_uses_direct_url(self):
        url, source, error = database._compute_database_url_selection(
            "direct", "postgresql://direct/db", "postgresql://pgb/db"
        )
        self.assertEqual(url, "postgresql://direct/db")
        self.assertEqual(source, "DATABASE_URL_RAILWAY")
        self.assertIsNone(error)


class SanitizationTests(unittest.TestCase):
    def test_safe_parse_strips_credentials(self):
        dsn = "postgresql://dbuser:SUPERSECRET@shard.proxy.rlwy.net:5432/app?sslmode=require"
        meta = database._safe_parse_database_url(dsn)
        flat = " ".join(str(v) for v in meta.values())
        self.assertNotIn("SUPERSECRET", flat)
        self.assertNotIn("dbuser", flat)
        self.assertEqual(meta["host"], "shard.proxy.rlwy.net")
        self.assertEqual(meta["port"], 5432)
        self.assertEqual(meta["endpoint_kind"], "railway_tcp_proxy")
        self.assertEqual(meta["sslmode"], "require")
        # Only sanitized keys are exposed.
        self.assertEqual(
            set(meta.keys()),
            {"configured", "host", "port", "scheme", "sslmode", "endpoint_kind"},
        )

    def test_rollout_status_exposes_no_dsn(self):
        status = database.pgbouncer_rollout_status()
        self.assertIn("db_connection_target", status)
        self.assertIn("db_pool_minconn", status)
        self.assertIn("db_pool_maxconn", status)
        for value in status.values():
            if isinstance(value, str):
                self.assertNotIn("://", value, msg="rollout status must not contain a DSN")


# --- Recording fakes for the no-write smoke-test assertion ---

_FORBIDDEN_SQL = ("insert", "update", "delete", "drop", "truncate", "alter", "create", "merge", "copy")


class _RecordingCursor:
    def __init__(self, log):
        self._log = log
        self._last = ""

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self._log.append(str(sql))
        self._last = str(sql).strip().lower()

    def fetchone(self):
        if self._last.startswith("select 1"):
            return [1]
        if self._last.startswith("show statement_timeout"):
            return ["5s"]
        return [None]


class _RecordingConnection:
    def __init__(self, sql_log, events):
        self._sql_log = sql_log
        self._events = events

    def cursor(self):
        return _RecordingCursor(self._sql_log)

    def commit(self):
        self._events.append("commit")

    def rollback(self):
        self._events.append("rollback")


class _RecordingContextFactory:
    """Mimics get_db_connection_context: commit on success, rollback+reraise on error."""

    def __init__(self):
        self.sql_log = []
        self.events = []

    def __call__(self):
        return self

    def __enter__(self):
        return _RecordingConnection(self.sql_log, self.events)

    def __exit__(self, exc_type, exc, tb):
        if exc_type is None:
            self.events.append("commit")
        else:
            self.events.append("rollback")
        return False  # never suppress


class SmokeNoWriteTests(unittest.TestCase):
    def _fake_status(self):
        return {
            "db_connection_target": "direct",
            "database_url_source": "DATABASE_URL_RAILWAY",
            "db_pool_minconn": 4,
            "db_pool_maxconn": 16,
        }

    def test_smoke_runs_and_passes_against_recording_fake(self):
        factory = _RecordingContextFactory()
        result = run_pgbouncer_smoke_test(
            connection_context_factory=factory,
            rollout_status_provider=self._fake_status,
        )
        self.assertTrue(result["ok"], msg=result)
        names = {c["name"] for c in result["checks"]}
        self.assertEqual(
            names,
            {
                "select_1",
                "transaction_commit",
                "transaction_rollback",
                "advisory_xact_lock",
                "set_local_statement_timeout",
                "savepoint",
                "target_report",
            },
        )

    def test_smoke_issues_no_destructive_sql(self):
        factory = _RecordingContextFactory()
        run_pgbouncer_smoke_test(
            connection_context_factory=factory,
            rollout_status_provider=self._fake_status,
        )
        for sql in factory.sql_log:
            lowered = sql.lower()
            for bad in _FORBIDDEN_SQL:
                self.assertNotIn(bad, lowered, msg=f"smoke issued forbidden SQL: {sql!r}")
            self.assertNotIn("bt_3_", lowered, msg=f"smoke touched an app table: {sql!r}")

    def test_smoke_rollback_check_triggers_rollback(self):
        factory = _RecordingContextFactory()
        run_pgbouncer_smoke_test(
            connection_context_factory=factory,
            rollout_status_provider=self._fake_status,
        )
        self.assertIn("rollback", factory.events)
        self.assertIn("commit", factory.events)


if __name__ == "__main__":
    unittest.main()
