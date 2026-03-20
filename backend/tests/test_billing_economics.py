import unittest
from contextlib import contextmanager
from datetime import date, datetime, timezone
from unittest.mock import patch

from psycopg2.extras import Json

from backend.database import _get_product_active_users_count, get_global_billing_summary, log_billing_event


class _DummyCursor:
    def __init__(self, responses):
        self._responses = list(responses)
        self.executed = []

    def execute(self, query, params=None):
        self.executed.append((query, params))

    def fetchone(self):
        if not self._responses:
            return None
        next_item = self._responses.pop(0)
        if isinstance(next_item, list):
            raise AssertionError("Expected fetchone response, got fetchall payload")
        return next_item

    def fetchall(self):
        if not self._responses:
            return []
        next_item = self._responses.pop(0)
        if isinstance(next_item, list):
            return next_item
        raise AssertionError("Expected fetchall payload, got fetchone response")

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


class BillingEconomicsTests(unittest.TestCase):
    def test_log_billing_event_wraps_metadata_in_json_payload(self):
        cursor = _DummyCursor([
            (
                11,
                "ev_test",
                77,
                "ru",
                "de",
                "webapp_tts_chars",
                "google_tts",
                "chars",
                12.0,
                None,
                0.0,
                "USD",
                "estimated",
                {"source": "test"},
                datetime(2026, 3, 20, tzinfo=timezone.utc),
                datetime(2026, 3, 20, tzinfo=timezone.utc),
            )
        ])

        with patch("backend.database.get_db_connection_context", _db_context(cursor)):
            item = log_billing_event(
                idempotency_key="ev_test",
                user_id=77,
                action_type="webapp_tts_chars",
                provider="google_tts",
                units_type="chars",
                units_value=12,
                metadata={"source": "test"},
            )

        self.assertIsNotNone(item)
        _, params = cursor.executed[0]
        self.assertIsInstance(params[12], Json)
        self.assertEqual(params[12].adapted, {"source": "test"})

    def test_global_summary_all_providers_tolerates_short_rows(self):
        cursor = _DummyCursor([
            (0, 0, 0, 0, 0, 0, 0),
            (0,),
            [
                ("google_tts",),
                ("google_translate", 1.25),
            ],
            [
                ("google_tts", "chars"),
            ],
            [
                ("google_tts",),
            ],
            [
                ("webapp_tts_chars",),
            ],
            [
                ("gpt-5-mini",),
            ],
            [
                ("chars",),
            ],
            [
                ("infra", "google_tts"),
            ],
            [
                ("google_tts",),
                ("google_translate",),
            ],
        ])

        with patch("backend.database.get_db_connection_context", _db_context(cursor)):
            with patch("backend.database._get_product_active_users_count", return_value=0):
                summary = get_global_billing_summary(
                    period="month",
                    provider=None,
                    currency="USD",
                    as_of_date=date(2026, 3, 20),
                )

        self.assertEqual(summary["provider_filter"], "all")
        self.assertEqual(summary["breakdown"]["by_provider"][0]["provider"], "google_translate")
        self.assertEqual(summary["breakdown"]["by_provider"][0]["variable_cost"], 1.25)
        self.assertEqual(summary["breakdown"]["by_provider"][1]["provider"], "google_tts")
        self.assertEqual(summary["breakdown"]["by_provider"][1]["variable_cost"], 0.0)
        self.assertEqual(summary["breakdown"]["by_provider"][1]["fixed_cost"], 0.0)
        self.assertEqual(summary["breakdown"]["by_action_type"][0]["events"], 0)
        self.assertEqual(summary["breakdown"]["by_model"][0]["tokens_in"], 0.0)
        self.assertEqual(summary["breakdown"]["fixed_costs"][0]["amount"], 0.0)

    def test_global_summary_tolerates_short_totals_row(self):
        cursor = _DummyCursor([
            (None, None),
            (0,),
            (0,),
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
        ])

        with patch("backend.database.get_db_connection_context", _db_context(cursor)):
            with patch("backend.database._get_product_active_users_count", return_value=0):
                summary = get_global_billing_summary(
                    period="all",
                    provider=None,
                    currency="USD",
                    as_of_date=date(2026, 3, 20),
                )

        self.assertEqual(summary["totals"]["variable_cost_total"], 0.0)
        self.assertEqual(summary["totals"]["events_count"], 0)
        self.assertEqual(summary["totals"]["unpriced_events"], 0)

    def test_product_active_users_count_treats_mymemory_as_translation_provider(self):
        cursor = _DummyCursor([
            (7,),
        ])

        count = _get_product_active_users_count(
            cursor,
            period_start=date(2026, 3, 1),
            period_end=date(2026, 3, 20),
            provider="mymemory",
        )

        self.assertEqual(count, 7)
        self.assertEqual(len(cursor.executed), 1)
        _, params = cursor.executed[0]
        self.assertEqual(len(params), 6)

    def test_product_active_users_count_unknown_provider_uses_full_union_param_set(self):
        cursor = _DummyCursor([
            (3,),
        ])

        count = _get_product_active_users_count(
            cursor,
            period_start=date(2026, 3, 1),
            period_end=date(2026, 3, 20),
            provider="stripe",
        )

        self.assertEqual(count, 3)
        self.assertEqual(len(cursor.executed), 1)
        _, params = cursor.executed[0]
        self.assertEqual(len(params), 12)


if __name__ == "__main__":
    unittest.main()
