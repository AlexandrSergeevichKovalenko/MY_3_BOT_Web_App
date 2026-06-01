import unittest
from contextlib import contextmanager
from unittest.mock import patch

from backend.database import bulk_delete_vocabulary_entries


class _DummyCursor:
    def __init__(self, fetchall_result=None):
        self.fetchall_result = list(fetchall_result or [])
        self.executed = []

    def execute(self, query, params=None):
        self.executed.append((query, params))

    def fetchall(self):
        return list(self.fetchall_result)

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


class VocabularyBulkDeleteTests(unittest.TestCase):
    def test_bulk_delete_removes_found_entries(self):
        cursor = _DummyCursor(fetchall_result=[(101,), (202,)])
        with patch("backend.database.get_db_connection_context", _db_context(cursor)):
            deleted = bulk_delete_vocabulary_entries(user_id=42, entry_ids=[101, 202, 303])

        self.assertEqual(deleted, 2)
        executed_sql = " ".join(str(stmt[0]) for stmt in cursor.executed)
        self.assertIn("DELETE FROM bt_3_card_srs_state", executed_sql)
        self.assertIn("DELETE FROM bt_3_card_review_log", executed_sql)
        self.assertIn("DELETE FROM bt_3_webapp_dictionary_queries", executed_sql)

    def test_bulk_delete_returns_zero_for_missing_entries(self):
        cursor = _DummyCursor(fetchall_result=[])
        with patch("backend.database.get_db_connection_context", _db_context(cursor)):
            deleted = bulk_delete_vocabulary_entries(user_id=42, entry_ids=[101, 202])

        self.assertEqual(deleted, 0)
        self.assertEqual(len(cursor.executed), 1)


if __name__ == "__main__":
    unittest.main()
