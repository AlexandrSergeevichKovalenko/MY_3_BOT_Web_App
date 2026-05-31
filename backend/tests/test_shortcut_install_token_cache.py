import unittest
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from backend import database


class _FakeCursor:
    def __init__(self, fetchone_results=None):
        self.fetchone_results = list(fetchone_results or [])
        self.executed: list[tuple[str, tuple | list | None]] = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchone(self):
        if not self.fetchone_results:
            return None
        return self.fetchone_results.pop(0)

    def fetchall(self):
        return []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _FakeConnection:
    def __init__(self, cursor: _FakeCursor):
        self._cursor = cursor
        self.commit_count = 0
        self.rollback_count = 0

    def cursor(self):
        return self._cursor

    def commit(self):
        self.commit_count += 1

    def rollback(self):
        self.rollback_count += 1

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def _db_context(connection):
    @contextmanager
    def _context():
        yield connection

    return _context


class ShortcutInstallTokenCacheTests(unittest.TestCase):
    def test_resolve_shortcut_install_token_prefers_cache(self):
        cached_payload = {
            "installation_id": 11,
            "user_id": 117649764,
            "source_pairing_code_id": 7,
            "is_active": True,
        }

        with patch("backend.database._shortcut_cache_get_installation", return_value=cached_payload), patch(
            "backend.database.get_db_connection_context"
        ) as db_context_mock:
            result = database.resolve_shortcut_install_token(
                install_token="install-token-value",
                request_id="req-1",
                remote_ip="127.0.0.1",
            )

        self.assertIsNotNone(result)
        self.assertEqual(result["installation_id"], 11)
        self.assertEqual(result["user_id"], 117649764)
        db_context_mock.assert_not_called()

    def test_resolve_shortcut_install_token_caches_db_result_on_miss(self):
        cursor = _FakeCursor(
            fetchone_results=[
                (11, 117649764, 7, datetime.now(timezone.utc), None, None, True),
            ]
        )
        connection = _FakeConnection(cursor)

        with patch("backend.database._shortcut_cache_get_installation", return_value=None), patch(
            "backend.database._shortcut_cache_set_installation"
        ) as cache_set_mock, patch("backend.database.get_db_connection_context", _db_context(connection)):
            result = database.resolve_shortcut_install_token(
                install_token="install-token-value",
                request_id="req-2",
                remote_ip="127.0.0.1",
            )

        self.assertIsNotNone(result)
        cache_set_mock.assert_called_once()
        _, payload = cache_set_mock.call_args.args
        self.assertEqual(payload["installation_id"], 11)
        self.assertEqual(payload["user_id"], 117649764)
        self.assertEqual(payload["source_pairing_code_id"], 7)

    def test_link_shortcut_installation_populates_cache_after_commit(self):
        future_expiry = datetime.now(timezone.utc) + timedelta(hours=1)
        cursor = _FakeCursor(
            fetchone_results=[
                (7, 117649764, future_expiry, None, None, True),
                (11, datetime.now(timezone.utc), None, None, True),
            ]
        )
        connection = _FakeConnection(cursor)

        with patch("backend.database.ensure_shortcut_tables", return_value=None), patch(
            "backend.database._shortcut_cache_set_installation"
        ) as cache_set_mock, patch("backend.database.get_db_connection_context", _db_context(connection)), patch(
            "backend.database.secrets.token_urlsafe", return_value="install-token-value"
        ):
            result = database.link_shortcut_installation(pairing_code="MAB3VV")

        self.assertEqual(result["status"], "linked")
        self.assertEqual(result["install_token"], "install-token-value")
        cache_set_mock.assert_called_once()
        _, payload = cache_set_mock.call_args.args
        self.assertEqual(payload["installation_id"], 11)
        self.assertEqual(payload["user_id"], 117649764)
        self.assertEqual(payload["source_pairing_code_id"], 7)
        self.assertEqual(connection.commit_count, 1)

    def test_revoke_shortcut_installation_clears_cache(self):
        cursor = _FakeCursor(fetchone_results=[(11,)])
        connection = _FakeConnection(cursor)
        token_hash = database._shortcut_install_token_hash("install-token-value")

        with patch("backend.database.ensure_shortcut_tables", return_value=None), patch(
            "backend.database._shortcut_cache_delete_installation"
        ) as cache_delete_mock, patch("backend.database.get_db_connection_context", _db_context(connection)):
            result = database.revoke_shortcut_installation(install_token="install-token-value")

        self.assertTrue(result)
        cache_delete_mock.assert_called_once_with(token_hash)
        self.assertEqual(connection.commit_count, 1)


if __name__ == "__main__":
    unittest.main()
