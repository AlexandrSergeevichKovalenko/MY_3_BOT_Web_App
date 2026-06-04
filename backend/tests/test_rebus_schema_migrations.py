import unittest

import backend.database as database


class RebusSchemaMigrationTests(unittest.TestCase):
    def test_rute_fix_uses_component_generation_status_column(self):
        class DummyCursor:
            def __init__(self):
                self.queries = []

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def execute(self, query, params=None):
                self.queries.append((query, params))

            def fetchone(self):
                return None

        class DummyConnection:
            def __init__(self):
                self.cursor_obj = DummyCursor()
                self.committed = False

            def cursor(self):
                return self.cursor_obj

            def commit(self):
                self.committed = True

        conn = DummyConnection()

        database._run_rebus_rute_fix_migration(conn)

        component_updates = [
            query
            for query, _params in conn.cursor_obj.queries
            if "UPDATE bt_3_rebus_component_images" in query
        ]
        self.assertEqual(len(component_updates), 1)
        self.assertIn("generation_status = 'pending'", component_updates[0])
        self.assertNotIn("image_status", component_updates[0])
        self.assertTrue(conn.committed)
