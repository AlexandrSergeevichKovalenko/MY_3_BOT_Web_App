import unittest
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from unittest.mock import patch

from psycopg2.extras import Json

from backend.database import (
    FREE_FEATURE_LIMITS,
    _billing_period_bounds,
    _get_feature_usage_today,
    _get_product_active_users_count,
    _prorate_fixed_cost_amount,
    build_free_limit_error,
    enforce_feature_limit,
    get_free_feature_limit_metadata,
    get_free_feature_usage_today,
    get_global_billing_summary,
    increment_free_feature_usage,
    log_billing_event,
    resolve_entitlement,
)


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
    def test_billing_period_bounds_day_uses_single_day(self):
        start, end = _billing_period_bounds("day", date(2026, 3, 21))
        self.assertEqual(start, date(2026, 3, 21))
        self.assertEqual(end, date(2026, 3, 21))

    def test_prorate_fixed_cost_amount_for_single_day_inside_month(self):
        prorated, ratio, overlap_days, period_days = _prorate_fixed_cost_amount(
            amount=31.0,
            item_start=date(2026, 3, 1),
            item_end=date(2026, 3, 31),
            range_start=date(2026, 3, 21),
            range_end=date(2026, 3, 21),
        )
        self.assertEqual(prorated, 1.0)
        self.assertAlmostEqual(ratio, 1 / 31)
        self.assertEqual(overlap_days, 1)
        self.assertEqual(period_days, 31)

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
            [],
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
        self.assertEqual(summary["breakdown"]["fixed_costs"], [])

    def test_global_summary_tolerates_short_totals_row(self):
        cursor = _DummyCursor([
            (None, None),
            (None, None),
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
        self.assertEqual(summary["totals"]["avg_events_per_active_user"], 0.0)
        self.assertEqual(summary["totals"]["avg_variable_cost_per_active_user"], 0.0)
        self.assertEqual(summary["totals"]["avg_fixed_cost_per_active_user"], 0.0)
        self.assertEqual(summary["totals"]["avg_cost_per_active_user"], 0.0)

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

    def test_feature_usage_today_counts_distinct_translation_sets(self):
        cursor = _DummyCursor([
            (1,),
        ])

        with patch("backend.database.get_db_connection_context", _db_context(cursor)):
            usage = _get_feature_usage_today(
                user_id=77,
                feature_code="translation_daily_sets",
                tz="Europe/Vienna",
            )

        self.assertEqual(usage, 1.0)
        self.assertEqual(len(cursor.executed), 1)
        query, params = cursor.executed[0]
        self.assertIn("COUNT(DISTINCT ds.session_id)", query)
        self.assertEqual(params[0], 77)

    def test_enforce_feature_limit_blocks_second_free_translation_set(self):
        with patch("backend.database.resolve_entitlement", return_value={
            "plan_code": "free",
            "effective_mode": "free",
            "reset_at": "2026-03-23T00:00:00+01:00",
        }):
            with patch("backend.database.get_plan_limit", return_value={
                "plan_code": "free",
                "feature_code": "translation_daily_sets",
                "limit_value": 1,
                "limit_unit": "count",
                "period": "day",
                "is_active": True,
            }):
                with patch("backend.database._get_feature_usage_today", return_value=1.0):
                    result = enforce_feature_limit(
                        user_id=77,
                        feature_code="translation_daily_sets",
                        requested_units=1.0,
                        tz="Europe/Vienna",
                    )

        self.assertIsNotNone(result)
        self.assertEqual(result["error"], "feature_limit_exceeded")
        self.assertEqual(result["feature"], "translation_daily_sets")
        self.assertEqual(result["limit"], 1)
        self.assertEqual(result["used"], 1)

    def test_enforce_feature_limit_allows_pro_translation_set_rerequest(self):
        with patch("backend.database.resolve_entitlement", return_value={
            "plan_code": "pro",
            "effective_mode": "pro",
            "reset_at": "2026-03-23T00:00:00+01:00",
        }):
            with patch("backend.database.get_plan_limit", return_value=None):
                with patch("backend.database._get_feature_usage_today", return_value=5.0):
                    result = enforce_feature_limit(
                        user_id=77,
                        feature_code="translation_daily_sets",
                        requested_units=1.0,
                        tz="Europe/Vienna",
                    )

        self.assertIsNone(result)

    def test_free_feature_metadata_exists_for_expected_keys(self):
        expected = {
            "translation_daily_sets",
            "dictionary_lookup_save_daily",
            "dictionary_openai_explanation_daily",
            "fsrs_card_review_daily",
            "shortcut_ingest_save_daily",
            "ask_gpt_daily",
        }

        self.assertTrue(expected.issubset(set(FREE_FEATURE_LIMITS)))
        for feature_key in expected:
            meta = get_free_feature_limit_metadata(feature_key)
            self.assertIsNotNone(meta)
            self.assertIn("title", meta)
            self.assertIn("free_limit", meta)
            self.assertEqual(meta["reset_policy"], "daily_europe_vienna")

    def test_get_free_feature_usage_today_uses_europe_vienna_day_boundary(self):
        cursor = _DummyCursor([
            (3.0,),
        ])

        usage = get_free_feature_usage_today(
            user_id=77,
            feature_key="ask_gpt_daily",
            now_ts_utc=datetime(2026, 3, 30, 22, 30, tzinfo=timezone.utc),
            tz="Europe/Vienna",
            cursor=cursor,
        )

        self.assertEqual(usage, 3.0)
        self.assertEqual(len(cursor.executed), 1)
        query, params = cursor.executed[0]
        self.assertIn("bt_3_billing_events", query)
        self.assertEqual(params, (77, "ask_gpt_daily", "Europe/Vienna", date(2026, 3, 31)))

    def test_increment_free_feature_usage_writes_one_event(self):
        event_time = datetime(2026, 3, 20, 12, 0, tzinfo=timezone.utc)
        cursor = _DummyCursor([
            (
                42,
                "free_usage:test",
                77,
                "ru",
                "de",
                "ask_gpt_daily",
                "app_internal",
                "requests",
                1.0,
                None,
                0.0,
                "USD",
                "estimated",
                {"source": "test"},
                event_time,
                event_time,
            )
        ])

        event = increment_free_feature_usage(
            user_id=77,
            feature_key="ask_gpt_daily",
            idempotency_key="free_usage:test",
            source_lang="ru",
            target_lang="de",
            metadata={"source": "test"},
            event_time=event_time,
            cursor=cursor,
        )

        self.assertEqual(event["id"], 42)
        self.assertEqual(event["action_type"], "ask_gpt_daily")
        self.assertEqual(event["provider"], "app_internal")
        self.assertEqual(event["units_type"], "requests")
        self.assertEqual(event["units_value"], 1.0)
        self.assertEqual(len(cursor.executed), 1)
        query, params = cursor.executed[0]
        self.assertIn("INSERT INTO bt_3_billing_events", query)
        self.assertEqual(params[0], "free_usage:test")
        self.assertEqual(params[1], 77)
        self.assertEqual(params[4], "ask_gpt_daily")
        self.assertEqual(params[5], "app_internal")
        self.assertEqual(params[6], "requests")
        self.assertEqual(params[7], 1.0)
        self.assertEqual(params[11], "estimated")
        self.assertIsInstance(params[12], Json)
        self.assertEqual(params[12].adapted, {"source": "test"})

    def test_build_free_limit_error_payload(self):
        payload = build_free_limit_error(
            "ask_gpt_daily",
            used=5.0,
            limit=5.0,
            reset_at="2026-03-23T00:00:00+01:00",
        )

        self.assertEqual(payload["ok"], False)
        self.assertEqual(payload["error"], "free_limit_exceeded")
        self.assertEqual(payload["feature"], "ask_gpt_daily")
        self.assertEqual(payload["feature_title"], "Спросить GPT")
        self.assertEqual(payload["limit"], 5)
        self.assertEqual(payload["used"], 5)
        self.assertEqual(payload["reset_at"], "2026-03-23T00:00:00+01:00")
        self.assertIn("message", payload)

    def test_resolve_entitlement_preserves_free_trial_pro_and_source(self):
        now = datetime(2026, 3, 20, 12, 0, tzinfo=timezone.utc)
        plans = {
            "free": {"plan_code": "free", "name": "Free", "is_paid": False, "daily_cost_cap_eur": 0.5},
            "trial": {"plan_code": "trial", "name": "Trial", "is_paid": False, "daily_cost_cap_eur": 0.5},
            "pro": {"plan_code": "pro", "name": "Pro", "is_paid": True, "daily_cost_cap_eur": 5.0},
        }

        def _plan(code):
            return plans.get(code)

        with patch("backend.database.get_billing_plan", side_effect=_plan):
            free = resolve_entitlement(user_id=77, now_ts_utc=now, subscription={})
            trial = resolve_entitlement(
                user_id=77,
                now_ts_utc=now,
                subscription={
                    "user_id": 77,
                    "plan_code": "trial",
                    "status": "trialing",
                    "trial_ends_at": (now + timedelta(days=1)).isoformat(),
                },
            )
            pro = resolve_entitlement(
                user_id=77,
                now_ts_utc=now,
                subscription={
                    "user_id": 77,
                    "plan_code": "pro",
                    "status": "active",
                    "trial_ends_at": None,
                },
            )

        self.assertEqual(free["effective_mode"], "free")
        self.assertEqual(free["source_of_entitlement"], "free_default")
        self.assertIn("reset_at", free)
        self.assertEqual(trial["effective_mode"], "trial")
        self.assertEqual(trial["source_of_entitlement"], "explicit_trial_subscription")
        self.assertEqual(pro["effective_mode"], "pro")
        self.assertEqual(pro["source_of_entitlement"], "paid_subscription")


if __name__ == "__main__":
    unittest.main()
