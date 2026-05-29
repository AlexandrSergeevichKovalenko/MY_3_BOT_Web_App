from contextlib import contextmanager
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

import backend.backend_server as server
import backend.database as database
import backend.free_usage_lifecycle as lifecycle
import backend.translation_workflow as workflow


class BillingSubscriptionSyncTests(unittest.TestCase):
    @staticmethod
    @contextmanager
    def _fake_db_scope(*_args, **_kwargs):
        yield []

    @staticmethod
    @contextmanager
    def _fake_db_connection(*_args, **_kwargs):
        class _Cursor:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def execute(self, *_args, **_kwargs):
                return None

            def fetchone(self):
                return None

        class _Connection:
            def cursor(self):
                return _Cursor()

        yield _Connection()

    @staticmethod
    def _recording_db_context():
        class _Cursor:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def execute(self, *_args, **_kwargs):
                return None

            def fetchone(self):
                return None

        class _Connection:
            def __init__(self):
                self.committed = False
                self.rolled_back = False
                self.cursor_obj = _Cursor()

            def cursor(self):
                return self.cursor_obj

            def commit(self):
                self.committed = True

            def rollback(self):
                self.rolled_back = True

        conn = _Connection()

        @contextmanager
        def _context():
            try:
                yield conn
                conn.commit()
            except Exception:
                conn.rollback()
                raise

        return conn, _context

    def test_sync_user_subscription_prefers_live_active_subscription(self):
        local_subscription = {
            "user_id": 42,
            "plan_code": "free",
            "status": "canceled",
            "trial_ends_at": None,
            "current_period_end": None,
            "stripe_customer_id": "cus_live",
            "stripe_subscription_id": "sub_old",
            "created_at": None,
            "updated_at": None,
        }
        old_subscription = {
            "id": "sub_old",
            "customer": "cus_live",
            "status": "canceled",
            "cancel_at_period_end": True,
            "current_period_end": 1710000000,
            "created": 1700000000,
            "metadata": {"plan_code": "free"},
        }
        active_subscription = {
            "id": "sub_new",
            "customer": "cus_live",
            "status": "active",
            "cancel_at_period_end": False,
            "current_period_end": 1720000000,
            "created": 1715000000,
            "metadata": {"plan_code": "pro"},
        }
        fake_stripe = SimpleNamespace(
            Subscription=SimpleNamespace(
                list=Mock(return_value={"data": [old_subscription, active_subscription]}),
                retrieve=Mock(),
            )
        )
        updated_subscription = {
            **local_subscription,
            "plan_code": "pro",
            "status": "active",
            "stripe_subscription_id": "sub_new",
        }

        with patch.object(server, "STRIPE_SECRET_KEY", "sk_test"), \
             patch.object(server, "stripe", fake_stripe), \
             patch.object(server, "_upsert_subscription_from_stripe_payload", return_value=updated_subscription) as upsert_mock, \
             patch.object(server, "_invalidate_billing_front_caches_for_user") as invalidate_mock:
            result = server._sync_user_subscription_from_live_stripe(
                user_id=42,
                subscription=local_subscription,
            )

        self.assertEqual(result["plan_code"], "pro")
        self.assertEqual(result["status"], "active")
        self.assertEqual(result["stripe_subscription_id"], "sub_new")
        upsert_mock.assert_called_once_with(
            user_id=42,
            plan_code="pro",
            stripe_customer_id="cus_live",
            stripe_subscription_id="sub_new",
            stripe_status="active",
            current_period_end_ts=1720000000,
        )
        invalidate_mock.assert_called_once_with(42)

    def test_sync_user_subscription_skips_live_lookup_for_active_paid_row(self):
        local_subscription = {
            "user_id": 43,
            "plan_code": "pro",
            "status": "active",
            "trial_ends_at": None,
            "current_period_end": None,
            "stripe_customer_id": "cus_live",
            "stripe_subscription_id": "sub_live",
            "created_at": None,
            "updated_at": None,
        }
        fake_stripe = SimpleNamespace(
            Subscription=SimpleNamespace(
                list=Mock(),
                retrieve=Mock(),
            )
        )

        with patch.object(server, "STRIPE_SECRET_KEY", "sk_test"), \
             patch.object(server, "stripe", fake_stripe):
            result = server._sync_user_subscription_from_live_stripe(
                user_id=43,
                subscription=local_subscription,
            )

        self.assertEqual(result, local_subscription)
        fake_stripe.Subscription.list.assert_not_called()
        fake_stripe.Subscription.retrieve.assert_not_called()

    def test_extract_plan_code_from_attr_only_payload_uses_metadata(self):
        class StripeLikeObject:
            def __init__(self, **kwargs):
                for key, value in kwargs.items():
                    setattr(self, key, value)

        payload = StripeLikeObject(
            metadata=StripeLikeObject(plan_code="pro"),
            items=StripeLikeObject(data=[]),
        )

        result = server._extract_plan_code_from_stripe_payload(payload, default="free")

        self.assertEqual(result, "pro")

    def test_extract_plan_code_from_attr_only_payload_falls_back_to_price_id(self):
        class StripeLikeObject:
            def __init__(self, **kwargs):
                for key, value in kwargs.items():
                    setattr(self, key, value)

        payload = StripeLikeObject(
            metadata=StripeLikeObject(),
            items=StripeLikeObject(
                data=[
                    StripeLikeObject(
                        price=StripeLikeObject(id="price_pro"),
                    )
                ]
            ),
        )

        with patch.object(server, "STRIPE_PRICE_ID_PRO", "price_pro"), \
             patch.object(server, "STRIPE_PRICE_ID_SUPPORT_COFFEE", ""), \
             patch.object(server, "STRIPE_PRICE_ID_SUPPORT_CHEESECAKE", ""):
            result = server._extract_plan_code_from_stripe_payload(payload, default="free")

        self.assertEqual(result, "pro")

    def test_sync_user_subscription_from_attr_only_list_object_updates_to_pro(self):
        class StripeLikeObject:
            def __init__(self, **kwargs):
                for key, value in kwargs.items():
                    setattr(self, key, value)

        local_subscription = {
            "user_id": 77,
            "plan_code": "free",
            "status": "inactive",
            "trial_ends_at": None,
            "current_period_end": None,
            "stripe_customer_id": "cus_attr",
            "stripe_subscription_id": "sub_old",
            "created_at": None,
            "updated_at": None,
        }
        listed = StripeLikeObject(
            data=[
                StripeLikeObject(
                    id="sub_live",
                    customer="cus_attr",
                    status="active",
                    cancel_at_period_end=False,
                    current_period_end=1720000000,
                    created=1715000000,
                    metadata=StripeLikeObject(plan_code="pro"),
                )
            ]
        )
        fake_stripe = SimpleNamespace(
            Subscription=SimpleNamespace(
                list=Mock(return_value=listed),
                retrieve=Mock(),
            )
        )
        updated_subscription = {
            **local_subscription,
            "plan_code": "pro",
            "status": "active",
            "stripe_subscription_id": "sub_live",
        }

        with patch.object(server, "STRIPE_SECRET_KEY", "sk_test"), \
             patch.object(server, "stripe", fake_stripe), \
             patch.object(server, "_upsert_subscription_from_stripe_payload", return_value=updated_subscription) as upsert_mock, \
             patch.object(server, "_invalidate_billing_front_caches_for_user"):
            result = server._sync_user_subscription_from_live_stripe(
                user_id=77,
                subscription=local_subscription,
            )

        self.assertEqual(result["plan_code"], "pro")
        self.assertEqual(result["status"], "active")
        upsert_mock.assert_called_once_with(
            user_id=77,
            plan_code="pro",
            stripe_customer_id="cus_attr",
            stripe_subscription_id="sub_live",
            stripe_status="active",
            current_period_end_ts=1720000000,
        )

    def test_build_billing_status_response_payload_reports_pro_effective_mode(self):
        subscription = {
            "stripe_customer_id": "cus_live",
            "stripe_subscription_id": "sub_live",
            "current_period_end": 1720000000,
        }
        entitlement = {
            "plan_code": "pro",
            "plan_name": "Pro",
            "status": "active",
            "effective_mode": "pro",
            "trial_ends_at": None,
            "cap_eur": None,
            "reset_at": "2026-05-04T00:00:00+02:00",
        }

        with patch.object(server, "_sync_user_subscription_from_live_stripe", return_value=subscription), \
             patch.object(server, "_resolve_user_entitlement", return_value=(entitlement, subscription)), \
             patch.object(server, "get_today_cost_eur_fast", return_value=0.0):
            payload, meta = server._build_billing_status_response_payload(user_id=77)

        self.assertEqual(payload["effective_mode"], "pro")
        self.assertEqual(payload["status"], "active")
        self.assertFalse(payload["upgrade"]["available"])
        self.assertTrue(payload["manage"]["available"])
        self.assertEqual(meta["effective_mode"], "pro")

    def test_skills_paid_feature_gate_blocks_free_user(self):
        entitlement = {
            "effective_mode": "free",
            "reset_at": "2026-05-28T00:00:00+02:00",
        }
        with patch.object(server, "_resolve_user_entitlement", return_value=(entitlement, {})), \
             patch.object(server, "_log_flow_observation"):
            payload, status = server._block_free_paid_feature(
                user_id=77,
                feature="skills",
                feature_title="Карта навыков",
                flow="skill_progress",
                stage="skill_progress_completed",
            )

        self.assertEqual(status, 402)
        self.assertEqual(payload["error"], "paid_feature_required")
        self.assertEqual(payload["feature"], "skills")
        self.assertEqual(payload["effective_mode"], "free")

    def test_skills_paid_feature_gate_allows_paid_user(self):
        entitlement = {
            "effective_mode": "pro",
            "reset_at": "2026-05-28T00:00:00+02:00",
        }
        with patch.object(server, "_resolve_user_entitlement", return_value=(entitlement, {})), \
             patch.object(server, "_log_flow_observation"):
            payload, status = server._block_free_paid_feature(
                user_id=77,
                feature="skills",
                feature_title="Карта навыков",
                flow="skill_progress",
                stage="skill_progress_completed",
            )

        self.assertIsNone(payload)
        self.assertIsNone(status)

    def test_paid_feature_registry_lookup_success(self):
        entry, missing = server._paid_feature_registry_entry_for_path("/api/progress/skills")

        self.assertIsNone(missing)
        self.assertIsNotNone(entry)
        self.assertEqual(entry["feature_key"], "skills")
        self.assertEqual(entry["feature_title"], "Карта навыков")

    def test_paid_feature_registry_missing_expected_route_fails_explicit(self):
        registry_without_today = {
            key: value
            for key, value in server._PAID_FEATURE_GATE_REGISTRY.items()
            if key != "today"
        }

        entry, missing = server._paid_feature_registry_entry_for_path(
            "/api/today",
            registry=registry_without_today,
        )

        self.assertIsNone(entry)
        self.assertEqual(missing, "today")

    def test_free_user_blocked_from_today_endpoint(self):
        client = server.app.test_client()
        entitlement = {
            "effective_mode": "free",
            "reset_at": "2026-05-28T00:00:00+02:00",
        }
        with patch.object(server, "_get_authenticated_user_from_request_init_data", return_value=(77, "u", None)), \
             patch.object(server, "_resolve_user_entitlement", return_value=(entitlement, {})), \
             patch.object(server, "_log_flow_observation"):
            response = client.get("/api/today?initData=fake")

        self.assertEqual(response.status_code, 402)
        data = response.get_json()
        self.assertEqual(data["error"], "paid_feature_required")
        self.assertEqual(data["feature"], "today")
        self.assertEqual(data["message"], "Эта функция доступна по подписке.")

    def test_paid_user_allowed_for_today_endpoint(self):
        client = server.app.test_client()
        entitlement = {
            "effective_mode": "pro",
            "reset_at": "2026-05-28T00:00:00+02:00",
        }
        payload = {"date": "2026-05-27", "items": [], "total_minutes": 0}
        with patch.object(server, "_get_authenticated_user_from_request_init_data", return_value=(77, "u", None)), \
             patch.object(server, "_resolve_user_entitlement", return_value=(entitlement, {})), \
             patch.object(server, "_load_today_card_projection_with_source", return_value=({"projection_status": "ready"}, "test")), \
             patch.object(server, "_build_today_card_response_payload", return_value=payload), \
             patch.object(server, "_log_flow_observation"):
            response = client.get("/api/today?initData=fake")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["date"], "2026-05-27")

    def test_free_user_blocked_from_weekly_plan_endpoint(self):
        client = server.app.test_client()
        entitlement = {
            "effective_mode": "free",
            "reset_at": "2026-05-28T00:00:00+02:00",
        }
        with patch.object(server, "_get_authenticated_user_from_request_init_data", return_value=(77, "u", None)), \
             patch.object(server, "_resolve_user_entitlement", return_value=(entitlement, {})), \
             patch.object(server, "_log_flow_observation"):
            response = client.get("/api/progress/weekly-plan?initData=fake")

        self.assertEqual(response.status_code, 402)
        data = response.get_json()
        self.assertEqual(data["error"], "paid_feature_required")
        self.assertEqual(data["feature"], "weekly_plan")
        self.assertEqual(data["message"], "Эта функция доступна по подписке.")

    def test_paid_user_allowed_for_weekly_plan_endpoint(self):
        client = server.app.test_client()
        entitlement = {
            "effective_mode": "pro",
            "reset_at": "2026-05-28T00:00:00+02:00",
        }
        payload = {"ok": True, "week": {"start_date": "2026-05-25"}, "plan": {}}
        with patch.object(server, "_get_authenticated_user_from_request_init_data", return_value=(77, "u", None)), \
             patch.object(server, "_resolve_user_entitlement", return_value=(entitlement, {})), \
             patch.object(server, "db_acquire_scope", self._fake_db_scope), \
             patch.object(server, "_get_weekly_plan_response_cached", return_value=(payload, {"cache_hit": True})), \
             patch.object(server, "_log_flow_observation"):
            response = client.get("/api/progress/weekly-plan?initData=fake")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["ok"], True)

    def test_entitlement_resolution_failure_does_not_allow_today_endpoint(self):
        client = server.app.test_client()
        with patch.object(server, "_get_authenticated_user_from_request_init_data", return_value=(77, "u", None)), \
             patch.object(server, "_resolve_user_entitlement", side_effect=RuntimeError("boom")), \
             patch.object(server, "_log_flow_observation"):
            response = client.get("/api/today?initData=fake")

        self.assertEqual(response.status_code, 503)
        data = response.get_json()
        self.assertEqual(data["error"], "entitlement_resolution_failed")
        self.assertEqual(data["feature"], "today")

    def test_pure_dictionary_lookup_does_not_increment_save_limit(self):
        client = server.app.test_client()
        cached_payload = {
            "item": {"source_text": "Haus", "target_text": "дом"},
            "direction": "de-ru",
        }
        with patch.object(server, "WEBAPP_SINGLE_INSTANCE_GUARD_ENABLED", False), \
             patch.object(server, "_telegram_hash_is_valid", return_value=True), \
             patch.object(server, "_parse_telegram_init_data", return_value={"user": {"id": 77}}), \
             patch.object(server, "_resolve_webapp_user_allowed", return_value=(True, "test")), \
             patch.object(server, "_get_user_language_pair", return_value=("ru", "de", None)), \
             patch.object(server, "_resolve_dictionary_query_languages", return_value=("de", "ru")), \
             patch.object(server, "_get_cached_dictionary_lookup_with_tier", return_value=(cached_payload, "test")), \
             patch.object(server, "_increment_free_daily_usage") as increment_mock:
            response = client.post(
                "/api/webapp/dictionary",
                json={"initData": "fake", "word": "Haus", "lookup_lang": "de"},
            )

        self.assertEqual(response.status_code, 200)
        increment_mock.assert_not_called()

    def test_openai_dictionary_lookup_increments_explanation_limit_only(self):
        client = server.app.test_client()
        entitlement = {
            "effective_mode": "free",
            "reset_at": "2026-05-28T00:00:00+02:00",
        }
        core_payload = {
            "usage": {},
            "gateway_path": "test",
            "direction": "de-ru",
            "item": {"source_text": "Haus", "target_text": "дом"},
            "raw": {},
        }
        with patch.object(server, "WEBAPP_SINGLE_INSTANCE_GUARD_ENABLED", False), \
             patch.object(server, "_telegram_hash_is_valid", return_value=True), \
             patch.object(server, "_parse_telegram_init_data", return_value={"user": {"id": 77}}), \
             patch.object(server, "_resolve_webapp_user_allowed", return_value=(True, "test")), \
             patch.object(server, "_get_user_language_pair", return_value=("ru", "de", None)), \
             patch.object(server, "_resolve_dictionary_query_languages", return_value=("de", "ru")), \
             patch.object(server, "_get_cached_dictionary_lookup_with_tier", return_value=(None, "none")), \
             patch.object(server, "_get_dictionary_enrichment_job_for_cache_keys", return_value=None), \
             patch.object(server, "_resolve_user_entitlement", return_value=(entitlement, {})), \
             patch.object(server, "get_free_feature_usage_today", return_value=0.0), \
             patch.object(server, "_run_dictionary_core_lookup_sync", return_value=core_payload), \
             patch.object(server, "_create_dictionary_enrichment_job"), \
             patch.object(server, "_start_dictionary_enrichment_runner"), \
             patch.object(server, "_billing_log_event_safe"), \
             patch.object(server, "_billing_log_openai_usage"), \
             patch.object(server, "_increment_free_daily_usage", return_value=(None, None)) as increment_mock:
            response = client.post(
                "/api/webapp/dictionary",
                json={"initData": "fake", "word": "Haus", "lookup_lang": "de"},
            )

        self.assertEqual(response.status_code, 200)
        features = [call.kwargs.get("feature") for call in increment_mock.call_args_list]
        self.assertEqual(features, ["dictionary_openai_explanation_daily"])

    def test_free_dictionary_lookup_allowed_under_limit(self):
        entitlement = {
            "effective_mode": "free",
            "reset_at": "2026-05-28T00:00:00+02:00",
        }
        with patch.object(server, "_resolve_user_entitlement", return_value=(entitlement, {})), \
             patch.object(server, "get_free_feature_usage_today", return_value=19.0):
            state, payload, status = server._check_free_daily_usage_limit(
                user_id=77,
                feature="dictionary_lookup_save_daily",
                route="/api/webapp/dictionary",
            )

        self.assertIsNone(payload)
        self.assertIsNone(status)
        self.assertEqual(state["effective_mode"], "free")
        self.assertEqual(state["used"], 19.0)
        self.assertEqual(state["limit"], 20.0)

    def test_free_dictionary_lookup_blocked_at_limit(self):
        entitlement = {
            "effective_mode": "free",
            "reset_at": "2026-05-28T00:00:00+02:00",
        }
        with patch.object(server, "_resolve_user_entitlement", return_value=(entitlement, {})), \
             patch.object(server, "get_free_feature_usage_today", return_value=20.0):
            state, payload, status = server._check_free_daily_usage_limit(
                user_id=77,
                feature="dictionary_lookup_save_daily",
                route="/api/webapp/dictionary",
            )

        self.assertIsNone(state)
        self.assertEqual(status, 429)
        self.assertEqual(payload["error"], "free_limit_exceeded")
        self.assertEqual(payload["feature"], "dictionary_lookup_save_daily")

    def test_free_dictionary_openai_explanation_blocked_at_limit(self):
        entitlement = {
            "effective_mode": "free",
            "reset_at": "2026-05-28T00:00:00+02:00",
        }
        with patch.object(server, "_resolve_user_entitlement", return_value=(entitlement, {})), \
             patch.object(server, "get_free_feature_usage_today", return_value=5.0):
            state, payload, status = server._check_free_daily_usage_limit(
                user_id=77,
                feature="dictionary_openai_explanation_daily",
                route="/api/webapp/dictionary",
            )

        self.assertIsNone(state)
        self.assertEqual(status, 429)
        self.assertEqual(payload["error"], "free_limit_exceeded")
        self.assertEqual(payload["feature"], "dictionary_openai_explanation_daily")

    def test_paid_user_bypasses_dictionary_free_limits(self):
        entitlement = {
            "effective_mode": "pro",
            "reset_at": "2026-05-28T00:00:00+02:00",
        }
        with patch.object(server, "_resolve_user_entitlement", return_value=(entitlement, {})), \
             patch.object(server, "get_free_feature_usage_today") as usage_mock:
            state, payload, status = server._check_free_daily_usage_limit(
                user_id=77,
                feature="dictionary_lookup_save_daily",
                route="/api/webapp/dictionary",
            )

        self.assertIsNone(payload)
        self.assertIsNone(status)
        self.assertTrue(state["skip_increment"])
        usage_mock.assert_not_called()

    def test_failed_dictionary_save_does_not_increment_usage(self):
        conn, db_context = self._recording_db_context()
        client = server.app.test_client()
        usage_state = {
            "feature": "dictionary_lookup_save_daily",
            "feature_title": "Словарь",
            "effective_mode": "free",
            "operation_kind": "count_new_dictionary_item",
            "used": 0,
            "limit": 20,
            "skip_increment": False,
        }

        with patch.object(server, "WEBAPP_SINGLE_INSTANCE_GUARD_ENABLED", False), \
             patch.object(server, "_telegram_hash_is_valid", return_value=True), \
             patch.object(server, "_parse_telegram_init_data", return_value={"user": {"id": 77}}), \
             patch.object(server, "_resolve_webapp_user_allowed", return_value=(True, "test")), \
             patch.object(server, "_get_user_language_pair", return_value=("ru", "de", None)), \
             patch.object(server, "db_acquire_scope", self._fake_db_scope), \
             patch.object(server, "get_db_connection_context", db_context), \
             patch.object(server, "get_or_create_dictionary_folder", return_value={"id": 1}), \
             patch.object(server, "begin_free_usage_lifecycle_tx", return_value=(usage_state, None, None)), \
             patch.object(server, "save_webapp_dictionary_query_returning_result_with_cursor", side_effect=RuntimeError("save failed")), \
             patch.object(server, "finish_free_usage_lifecycle_success_tx") as finish_mock, \
             patch.object(server, "_start_saved_dictionary_entry_enrichment") as enrichment_mock, \
             patch.object(server, "_enqueue_dictionary_entry_tts_prewarm") as tts_mock:
            response = client.post(
                "/api/webapp/dictionary/save",
                json={
                    "initData": "fake",
                    "source_text": "Haus",
                    "target_text": "дом",
                    "source_lang": "de",
                    "target_lang": "ru",
                },
            )

        self.assertEqual(response.status_code, 500)
        finish_mock.assert_not_called()
        enrichment_mock.assert_not_called()
        tts_mock.assert_not_called()
        self.assertFalse(conn.committed)
        self.assertTrue(conn.rolled_back)

    def test_successful_new_dictionary_save_increments_save_limit(self):
        conn, db_context = self._recording_db_context()
        client = server.app.test_client()
        usage_state = {
            "feature": "dictionary_lookup_save_daily",
            "feature_title": "Словарь",
            "effective_mode": "free",
            "operation_kind": "count_new_dictionary_item",
            "used": 0,
            "limit": 20,
            "skip_increment": False,
        }

        with patch.object(server, "WEBAPP_SINGLE_INSTANCE_GUARD_ENABLED", False), \
             patch.object(server, "_telegram_hash_is_valid", return_value=True), \
             patch.object(server, "_parse_telegram_init_data", return_value={"user": {"id": 77}}), \
             patch.object(server, "_resolve_webapp_user_allowed", return_value=(True, "test")), \
             patch.object(server, "_get_user_language_pair", return_value=("ru", "de", None)), \
             patch.object(server, "db_acquire_scope", self._fake_db_scope), \
             patch.object(server, "get_db_connection_context", db_context), \
             patch.object(server, "get_or_create_dictionary_folder", return_value={"id": 1}), \
             patch.object(server, "begin_free_usage_lifecycle_tx", return_value=(usage_state, None, None)) as begin_mock, \
             patch.object(server, "save_webapp_dictionary_query_returning_result_with_cursor", return_value={"entry_id": 123, "inserted": True}) as save_mock, \
             patch.object(server, "finish_free_usage_lifecycle_success_tx") as finish_mock, \
             patch.object(server, "_start_saved_dictionary_entry_enrichment"), \
             patch.object(server, "_enqueue_dictionary_entry_tts_prewarm"):
            response = client.post(
                "/api/webapp/dictionary/save",
                json={
                    "initData": "fake",
                    "source_text": "Haus",
                    "target_text": "дом",
                    "source_lang": "de",
                    "target_lang": "ru",
                },
            )

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.get_json()["created"])
        begin_mock.assert_called_once()
        self.assertEqual(begin_mock.call_args.kwargs["lifecycle_key"], "dictionary_save_new_item")
        save_mock.assert_called_once()
        finish_mock.assert_called_once()
        self.assertEqual(finish_mock.call_args.kwargs["idempotency_seed"], "save:123")
        self.assertTrue(conn.committed)
        self.assertFalse(conn.rolled_back)

    def test_existing_dictionary_save_does_not_increment_save_limit(self):
        conn, db_context = self._recording_db_context()
        client = server.app.test_client()
        usage_state = {
            "feature": "dictionary_lookup_save_daily",
            "feature_title": "Словарь",
            "effective_mode": "free",
            "operation_kind": "count_new_dictionary_item",
            "used": 0,
            "limit": 20,
            "skip_increment": False,
        }

        with patch.object(server, "WEBAPP_SINGLE_INSTANCE_GUARD_ENABLED", False), \
             patch.object(server, "_telegram_hash_is_valid", return_value=True), \
             patch.object(server, "_parse_telegram_init_data", return_value={"user": {"id": 77}}), \
             patch.object(server, "_resolve_webapp_user_allowed", return_value=(True, "test")), \
             patch.object(server, "_get_user_language_pair", return_value=("ru", "de", None)), \
             patch.object(server, "db_acquire_scope", self._fake_db_scope), \
             patch.object(server, "get_db_connection_context", db_context), \
             patch.object(server, "get_or_create_dictionary_folder", return_value={"id": 1}), \
             patch.object(server, "begin_free_usage_lifecycle_tx", return_value=(usage_state, None, None)), \
             patch.object(server, "save_webapp_dictionary_query_returning_result_with_cursor", return_value={"entry_id": 123, "inserted": False}), \
             patch.object(server, "finish_free_usage_lifecycle_success_tx") as finish_mock, \
             patch.object(server, "_start_saved_dictionary_entry_enrichment"), \
             patch.object(server, "_enqueue_dictionary_entry_tts_prewarm"):
            response = client.post(
                "/api/webapp/dictionary/save",
                json={
                    "initData": "fake",
                    "source_text": "Haus",
                    "target_text": "дом",
                    "source_lang": "de",
                    "target_lang": "ru",
                },
            )

        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.get_json()["created"])
        finish_mock.assert_not_called()
        self.assertTrue(conn.committed)
        self.assertFalse(conn.rolled_back)

    def test_free_dictionary_save_blocked_at_limit(self):
        conn, db_context = self._recording_db_context()
        client = server.app.test_client()
        limit_payload = {
            "ok": False,
            "error": "free_limit_exceeded",
            "feature": "dictionary_lookup_save_daily",
            "feature_title": "Словарь",
            "limit": 20,
            "used": 20,
            "reset_at": "2026-05-28T00:00:00+02:00",
            "message": "limit",
        }
        with patch.object(server, "WEBAPP_SINGLE_INSTANCE_GUARD_ENABLED", False), \
             patch.object(server, "_telegram_hash_is_valid", return_value=True), \
             patch.object(server, "_parse_telegram_init_data", return_value={"user": {"id": 77}}), \
             patch.object(server, "_resolve_webapp_user_allowed", return_value=(True, "test")), \
             patch.object(server, "db_acquire_scope", self._fake_db_scope), \
             patch.object(server, "get_db_connection_context", db_context), \
             patch.object(server, "begin_free_usage_lifecycle_tx", return_value=(None, limit_payload, 429)), \
             patch.object(server, "save_webapp_dictionary_query_returning_result_with_cursor") as save_mock:
            response = client.post(
                "/api/webapp/dictionary/save",
                json={"initData": "fake", "source_text": "Haus"},
            )

        self.assertEqual(response.status_code, 429)
        self.assertEqual(response.get_json()["error"], "free_limit_exceeded")
        save_mock.assert_not_called()
        self.assertTrue(conn.committed)
        self.assertFalse(conn.rolled_back)

    def test_paid_dictionary_save_bypasses_save_limit(self):
        conn, db_context = self._recording_db_context()
        client = server.app.test_client()
        usage_state = {
            "feature": "dictionary_lookup_save_daily",
            "feature_title": "Словарь",
            "effective_mode": "pro",
            "operation_kind": "count_new_dictionary_item",
            "used": None,
            "limit": 20,
            "skip_increment": True,
        }

        with patch.object(server, "WEBAPP_SINGLE_INSTANCE_GUARD_ENABLED", False), \
             patch.object(server, "_telegram_hash_is_valid", return_value=True), \
             patch.object(server, "_parse_telegram_init_data", return_value={"user": {"id": 77}}), \
             patch.object(server, "_resolve_webapp_user_allowed", return_value=(True, "test")), \
             patch.object(server, "_get_user_language_pair", return_value=("ru", "de", None)), \
             patch.object(server, "db_acquire_scope", self._fake_db_scope), \
             patch.object(server, "get_db_connection_context", db_context), \
             patch.object(server, "get_or_create_dictionary_folder", return_value={"id": 1}), \
             patch.object(server, "begin_free_usage_lifecycle_tx", return_value=(usage_state, None, None)), \
             patch.object(server, "save_webapp_dictionary_query_returning_result_with_cursor", return_value={"entry_id": 123, "inserted": True}), \
             patch.object(server, "finish_free_usage_lifecycle_success_tx", wraps=server.finish_free_usage_lifecycle_success_tx) as finish_mock, \
             patch.object(server, "_start_saved_dictionary_entry_enrichment"), \
             patch.object(server, "_enqueue_dictionary_entry_tts_prewarm"):
            response = client.post(
                "/api/webapp/dictionary/save",
                json={
                    "initData": "fake",
                    "source_text": "Haus",
                    "target_text": "дом",
                    "source_lang": "de",
                    "target_lang": "ru",
                },
        )

        self.assertEqual(response.status_code, 200)
        finish_mock.assert_called_once()
        self.assertTrue(conn.committed)
        self.assertFalse(conn.rolled_back)

    def test_dictionary_save_usage_increment_failure_rolls_back_save(self):
        conn, db_context = self._recording_db_context()
        client = server.app.test_client()
        usage_state = {
            "feature": "dictionary_lookup_save_daily",
            "feature_title": "Словарь",
            "effective_mode": "free",
            "operation_kind": "count_new_dictionary_item",
            "used": 0,
            "limit": 20,
            "skip_increment": False,
        }
        error_payload = {
            "ok": False,
            "error": "free_usage_state_unavailable",
            "feature": "dictionary_lookup_save_daily",
            "feature_title": "Словарь",
            "message": "Не удалось проверить лимит бесплатного тарифа.",
        }
        with patch.object(server, "WEBAPP_SINGLE_INSTANCE_GUARD_ENABLED", False), \
             patch.object(server, "_telegram_hash_is_valid", return_value=True), \
             patch.object(server, "_parse_telegram_init_data", return_value={"user": {"id": 77}}), \
             patch.object(server, "_resolve_webapp_user_allowed", return_value=(True, "test")), \
             patch.object(server, "_get_user_language_pair", return_value=("ru", "de", None)), \
             patch.object(server, "db_acquire_scope", self._fake_db_scope), \
             patch.object(server, "get_db_connection_context", db_context), \
             patch.object(server, "get_or_create_dictionary_folder", return_value={"id": 1}), \
             patch.object(server, "begin_free_usage_lifecycle_tx", return_value=(usage_state, None, None)), \
             patch.object(server, "save_webapp_dictionary_query_returning_result_with_cursor", return_value={"entry_id": 123, "inserted": True}) as save_mock, \
             patch.object(server, "finish_free_usage_lifecycle_success_tx", side_effect=server.FreeUsageLifecycleAbort(error_payload, 503)), \
             patch.object(server, "_start_saved_dictionary_entry_enrichment") as enrichment_mock, \
             patch.object(server, "_enqueue_dictionary_entry_tts_prewarm") as tts_mock:
            response = client.post(
                "/api/webapp/dictionary/save",
                json={
                    "initData": "fake",
                    "source_text": "Haus",
                    "target_text": "дом",
                    "source_lang": "de",
                    "target_lang": "ru",
                },
            )

        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.get_json()["error"], "free_usage_state_unavailable")
        save_mock.assert_called_once()
        enrichment_mock.assert_not_called()
        tts_mock.assert_not_called()
        self.assertFalse(conn.committed)
        self.assertTrue(conn.rolled_back)

    def test_dictionary_save_missing_lifecycle_config_rolls_back_before_save(self):
        conn, db_context = self._recording_db_context()
        client = server.app.test_client()
        error_payload = {
            "ok": False,
            "error": "free_usage_state_unavailable",
            "feature": "dictionary_lookup_save_daily",
            "feature_title": "Словарь",
            "message": "Не удалось проверить лимит бесплатного тарифа.",
        }
        with patch.object(server, "WEBAPP_SINGLE_INSTANCE_GUARD_ENABLED", False), \
             patch.object(server, "_telegram_hash_is_valid", return_value=True), \
             patch.object(server, "_parse_telegram_init_data", return_value={"user": {"id": 77}}), \
             patch.object(server, "_resolve_webapp_user_allowed", return_value=(True, "test")), \
             patch.object(server, "_get_user_language_pair", return_value=("ru", "de", None)), \
             patch.object(server, "db_acquire_scope", self._fake_db_scope), \
             patch.object(server, "get_db_connection_context", db_context), \
             patch.object(server, "begin_free_usage_lifecycle_tx", return_value=(None, error_payload, 503)), \
             patch.object(server, "save_webapp_dictionary_query_returning_result_with_cursor") as save_mock:
            response = client.post(
                "/api/webapp/dictionary/save",
                json={
                    "initData": "fake",
                    "source_text": "Haus",
                    "target_text": "дом",
                    "source_lang": "de",
                    "target_lang": "ru",
                },
            )

        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.get_json()["error"], "free_usage_state_unavailable")
        save_mock.assert_not_called()
        self.assertFalse(conn.committed)
        self.assertTrue(conn.rolled_back)

    def test_existing_non_route_dictionary_save_helper_still_works(self):
        with patch.object(database, "get_db_connection_context", self._fake_db_connection), \
             patch.object(database, "_save_webapp_dictionary_query_returning_id_with_conn", return_value=(321, True)) as lower_mock:
            result = database.save_webapp_dictionary_query_returning_result(
                user_id=77,
                word_ru=None,
                translation_de=None,
                word_de="Haus",
                translation_ru="дом",
                response_json={"word_de": "Haus", "translation_ru": "дом"},
                folder_id=1,
                source_lang="de",
                target_lang="ru",
                origin_process="test",
                origin_meta={"endpoint": "test"},
            )

        self.assertEqual(result, {"entry_id": 321, "inserted": True})
        lower_mock.assert_called_once()

    def test_missing_feature_metadata_fails_explicit(self):
        state, payload, status = server._check_free_daily_usage_limit(
            user_id=77,
            feature="unknown_feature_daily",
            route="/api/webapp/dictionary",
        )

        self.assertIsNone(state)
        self.assertEqual(status, 503)
        self.assertEqual(payload["error"], "free_usage_state_unavailable")

    def test_entitlement_resolution_failure_does_not_allow_dictionary_limit(self):
        with patch.object(server, "_resolve_user_entitlement", side_effect=RuntimeError("boom")):
            state, payload, status = server._check_free_daily_usage_limit(
                user_id=77,
                feature="dictionary_lookup_save_daily",
                route="/api/webapp/dictionary",
            )

        self.assertIsNone(state)
        self.assertEqual(status, 503)
        self.assertEqual(payload["error"], "free_usage_state_unavailable")

    def test_lifecycle_successful_operation_increments(self):
        with patch.object(lifecycle, "resolve_entitlement", return_value={"effective_mode": "free", "reset_at": "2026-05-28T00:00:00+02:00"}), \
             patch.object(lifecycle, "get_free_feature_usage_today", return_value=0.0), \
             patch.object(lifecycle, "increment_free_feature_usage", return_value={"units_value": 1.0}) as increment_mock:
            result, payload, status, _state, outcome = lifecycle.run_free_usage_lifecycle(
                lifecycle_key="fsrs_card_review",
                user_id=77,
                route="/api/cards/review",
                operation=lambda: {"ok": True, "card_id": 123},
                classify_success=lambda value: {"success": True, "count_usage": True, "object_id": value["card_id"]},
                idempotency_seed=lambda value: f"review:{value['card_id']}",
            )

        self.assertEqual(result["card_id"], 123)
        self.assertIsNone(payload)
        self.assertIsNone(status)
        self.assertTrue(outcome["count_usage"])
        increment_mock.assert_called_once()

    def test_lifecycle_duplicate_operation_skips_increment(self):
        with patch.object(lifecycle, "resolve_entitlement", return_value={"effective_mode": "free", "reset_at": "2026-05-28T00:00:00+02:00"}), \
             patch.object(lifecycle, "get_free_feature_usage_today", return_value=19.0), \
             patch.object(lifecycle, "increment_free_feature_usage") as increment_mock:
            result, payload, status, _state, outcome = lifecycle.run_free_usage_lifecycle(
                lifecycle_key="fsrs_card_review",
                user_id=77,
                route="/api/cards/review",
                operation=lambda: {"ok": True, "card_id": 123, "duplicate": True},
                classify_success=lambda value: {
                    "success": True,
                    "count_usage": False,
                    "object_id": value["card_id"],
                    "skip_reason": "skip_duplicate_review",
                },
                idempotency_seed="review:dupe",
            )

        self.assertTrue(result["duplicate"])
        self.assertIsNone(payload)
        self.assertIsNone(status)
        self.assertFalse(outcome["count_usage"])
        increment_mock.assert_not_called()

    def test_lifecycle_failed_operation_skips_increment(self):
        with patch.object(lifecycle, "resolve_entitlement", return_value={"effective_mode": "free", "reset_at": "2026-05-28T00:00:00+02:00"}), \
             patch.object(lifecycle, "get_free_feature_usage_today", return_value=0.0), \
             patch.object(lifecycle, "increment_free_feature_usage") as increment_mock:
            result, payload, status, _state, outcome = lifecycle.run_free_usage_lifecycle(
                lifecycle_key="fsrs_card_review",
                user_id=77,
                route="/api/cards/review",
                operation=lambda: {"ok": False, "card_id": 123},
                classify_success=lambda value: {"success": False, "count_usage": False, "object_id": value["card_id"]},
                idempotency_seed="review:failed",
            )

        self.assertFalse(result["ok"])
        self.assertIsNone(payload)
        self.assertIsNone(status)
        self.assertFalse(outcome["success"])
        increment_mock.assert_not_called()

    def test_lifecycle_increment_storage_failure_returns_explicit_error(self):
        with patch.object(lifecycle, "resolve_entitlement", return_value={"effective_mode": "free", "reset_at": "2026-05-28T00:00:00+02:00"}), \
             patch.object(lifecycle, "get_free_feature_usage_today", return_value=0.0), \
             patch.object(lifecycle, "increment_free_feature_usage", side_effect=RuntimeError("storage down")):
            _result, payload, status, _state, _outcome = lifecycle.run_free_usage_lifecycle(
                lifecycle_key="fsrs_card_review",
                user_id=77,
                route="/api/cards/review",
                operation=lambda: {"ok": True, "card_id": 123},
                classify_success=lambda value: {"success": True, "count_usage": True, "object_id": value["card_id"]},
                idempotency_seed="review:increment-fails",
            )

        self.assertEqual(status, 503)
        self.assertEqual(payload["error"], "free_usage_state_unavailable")

    def test_lifecycle_missing_config_fails_explicit(self):
        result, payload, status, state, outcome = lifecycle.run_free_usage_lifecycle(
            lifecycle_key="missing_lifecycle",
            user_id=77,
            route="/api/cards/review",
            operation=lambda: {"ok": True},
            classify_success=lambda value: {"success": True, "count_usage": True},
            idempotency_seed="missing",
        )

        self.assertIsNone(result)
        self.assertIsNone(state)
        self.assertIsNone(outcome)
        self.assertEqual(status, 503)
        self.assertEqual(payload["error"], "free_usage_state_unavailable")

    def test_fsrs_free_user_blocked_after_twenty_reviews(self):
        with patch.object(lifecycle, "resolve_entitlement", return_value={"effective_mode": "free", "reset_at": "2026-05-28T00:00:00+02:00"}), \
             patch.object(lifecycle, "get_free_feature_usage_today", return_value=20.0), \
             patch.object(lifecycle, "increment_free_feature_usage") as increment_mock:
            result, payload, status, _state, _outcome = lifecycle.run_free_usage_lifecycle(
                lifecycle_key="fsrs_card_review",
                user_id=77,
                route="/api/cards/review",
                operation=lambda: {"ok": True},
                classify_success=lambda value: {"success": True, "count_usage": True},
                idempotency_seed="review:block",
            )

        self.assertIsNone(result)
        self.assertEqual(status, 429)
        self.assertEqual(payload["error"], "free_limit_exceeded")
        self.assertEqual(payload["feature"], "fsrs_card_review_daily")
        increment_mock.assert_not_called()

    def test_fsrs_paid_user_bypasses_limit(self):
        with patch.object(lifecycle, "resolve_entitlement", return_value={"effective_mode": "pro", "reset_at": "2026-05-28T00:00:00+02:00"}), \
             patch.object(lifecycle, "get_free_feature_usage_today") as usage_mock, \
             patch.object(lifecycle, "increment_free_feature_usage") as increment_mock:
            result, payload, status, _state, _outcome = lifecycle.run_free_usage_lifecycle(
                lifecycle_key="fsrs_card_review",
                user_id=77,
                route="/api/cards/review",
                operation=lambda: {"ok": True, "card_id": 123},
                classify_success=lambda value: {"success": True, "count_usage": True, "object_id": value["card_id"]},
                idempotency_seed="review:paid",
            )

        self.assertEqual(result["card_id"], 123)
        self.assertIsNone(payload)
        self.assertIsNone(status)
        usage_mock.assert_not_called()
        increment_mock.assert_not_called()

    def test_fsrs_review_endpoint_requires_review_id(self):
        client = server.app.test_client()
        with patch.object(server, "_extract_webapp_user_from_init_data", return_value=(77, "test")), \
             patch.object(server, "_is_webapp_user_allowed", return_value=True), \
             patch.object(server, "begin_free_usage_lifecycle_tx") as lifecycle_mock:
            response = client.post(
                "/api/cards/review",
                json={"initData": "signed", "card_id": 123, "rating": "GOOD"},
            )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.get_json()["error"], "review_id_required")
        lifecycle_mock.assert_not_called()

    def test_fsrs_review_endpoint_limit_block_skips_operation(self):
        client = server.app.test_client()
        limit_payload = {
            "ok": False,
            "error": "free_limit_exceeded",
            "feature": "fsrs_card_review_daily",
            "feature_title": "Тренировка карточек",
            "limit": 20,
            "used": 20,
            "reset_at": "2026-05-28T00:00:00+02:00",
            "message": "limit",
        }
        with patch.object(server, "_extract_webapp_user_from_init_data", return_value=(77, "test")), \
             patch.object(server, "_is_webapp_user_allowed", return_value=True), \
             patch.object(server, "_get_user_language_pair", return_value=("ru", "de", None)), \
             patch.object(server, "_resolve_flashcards_manual_selection", return_value=None), \
             patch.object(server, "db_acquire_scope", self._fake_db_scope), \
             patch.object(server, "get_db_connection_context", self._fake_db_connection), \
             patch.object(server, "get_dictionary_entry_for_user", return_value={"id": 123}), \
             patch.object(server, "get_card_review_log_by_review_id", return_value=None), \
             patch.object(server, "begin_free_usage_lifecycle_tx", return_value=(None, limit_payload, 429)) as lifecycle_mock, \
             patch.object(server, "upsert_card_srs_state") as persist_mock:
            response = client.post(
                "/api/cards/review",
                json={"initData": "signed", "card_id": 123, "review_id": "rv-1", "rating": "GOOD"},
            )

        self.assertEqual(response.status_code, 429)
        self.assertEqual(response.get_json()["feature"], "fsrs_card_review_daily")
        self.assertEqual(lifecycle_mock.call_args.kwargs["lifecycle_key"], "fsrs_card_review")
        persist_mock.assert_not_called()

    def test_fsrs_review_success_commits_business_and_usage_together(self):
        conn, db_context = self._recording_db_context()
        client = server.app.test_client()
        usage_state = {
            "feature": "fsrs_card_review_daily",
            "feature_title": "Тренировка карточек",
            "effective_mode": "free",
            "operation_kind": "count_successful_fsrs_review",
            "used": 0,
            "limit": 20,
            "skip_increment": False,
        }
        with patch.object(server, "_extract_webapp_user_from_init_data", return_value=(77, "test")), \
             patch.object(server, "_is_webapp_user_allowed", return_value=True), \
             patch.object(server, "_get_user_language_pair", return_value=("ru", "de", None)), \
             patch.object(server, "_resolve_flashcards_manual_selection", return_value=None), \
             patch.object(server, "db_acquire_scope", self._fake_db_scope), \
             patch.object(server, "get_db_connection_context", db_context), \
             patch.object(server, "get_dictionary_entry_for_user", return_value={"id": 123}), \
             patch.object(server, "get_card_review_log_by_review_id", return_value=None), \
             patch.object(server, "get_card_srs_state", return_value=None), \
             patch.object(server, "upsert_card_srs_state", return_value={"status": "review", "interval_days": 1, "due_at": None, "stability": 1.0, "difficulty": 1.0}) as persist_mock, \
             patch.object(server, "insert_card_review_log", return_value={"inserted": True, "review_log_id": 9}), \
             patch.object(server, "_build_next_srs_payload", return_value={}), \
             patch.object(server, "begin_free_usage_lifecycle_tx", return_value=(usage_state, None, None)), \
             patch.object(server, "finish_free_usage_lifecycle_success_tx") as finish_mock, \
             patch.object(server, "_mark_today_plan_snapshot_stale"):
            response = client.post(
                "/api/cards/review",
                json={"initData": "signed", "card_id": 123, "review_id": "rv-success", "rating": "GOOD"},
            )

        self.assertEqual(response.status_code, 200)
        persist_mock.assert_called_once()
        finish_mock.assert_called_once()
        self.assertTrue(conn.committed)
        self.assertFalse(conn.rolled_back)

    def test_fsrs_review_business_failure_rolls_back_without_usage_increment(self):
        conn, db_context = self._recording_db_context()
        client = server.app.test_client()
        usage_state = {
            "feature": "fsrs_card_review_daily",
            "feature_title": "Тренировка карточек",
            "effective_mode": "free",
            "operation_kind": "count_successful_fsrs_review",
            "used": 0,
            "limit": 20,
            "skip_increment": False,
        }
        with patch.object(server, "_extract_webapp_user_from_init_data", return_value=(77, "test")), \
             patch.object(server, "_is_webapp_user_allowed", return_value=True), \
             patch.object(server, "_get_user_language_pair", return_value=("ru", "de", None)), \
             patch.object(server, "_resolve_flashcards_manual_selection", return_value=None), \
             patch.object(server, "db_acquire_scope", self._fake_db_scope), \
             patch.object(server, "get_db_connection_context", db_context), \
             patch.object(server, "get_dictionary_entry_for_user", return_value={"id": 123}), \
             patch.object(server, "get_card_review_log_by_review_id", return_value=None), \
             patch.object(server, "get_card_srs_state", return_value=None), \
             patch.object(server, "begin_free_usage_lifecycle_tx", return_value=(usage_state, None, None)), \
             patch.object(server, "upsert_card_srs_state", side_effect=RuntimeError("persist failed")), \
             patch.object(server, "finish_free_usage_lifecycle_success_tx") as finish_mock:
            response = client.post(
                "/api/cards/review",
                json={"initData": "signed", "card_id": 123, "review_id": "rv-business-fails", "rating": "GOOD"},
            )

        self.assertEqual(response.status_code, 500)
        finish_mock.assert_not_called()
        self.assertFalse(conn.committed)
        self.assertTrue(conn.rolled_back)

    def test_fsrs_review_usage_increment_failure_rolls_back_business_mutation(self):
        conn, db_context = self._recording_db_context()
        client = server.app.test_client()
        usage_state = {
            "feature": "fsrs_card_review_daily",
            "feature_title": "Тренировка карточек",
            "effective_mode": "free",
            "operation_kind": "count_successful_fsrs_review",
            "used": 0,
            "limit": 20,
            "skip_increment": False,
        }
        error_payload = {
            "ok": False,
            "error": "free_usage_state_unavailable",
            "feature": "fsrs_card_review_daily",
            "feature_title": "Тренировка карточек",
            "message": "Не удалось проверить лимит бесплатного тарифа.",
        }
        with patch.object(server, "_extract_webapp_user_from_init_data", return_value=(77, "test")), \
             patch.object(server, "_is_webapp_user_allowed", return_value=True), \
             patch.object(server, "_get_user_language_pair", return_value=("ru", "de", None)), \
             patch.object(server, "_resolve_flashcards_manual_selection", return_value=None), \
             patch.object(server, "db_acquire_scope", self._fake_db_scope), \
             patch.object(server, "get_db_connection_context", db_context), \
             patch.object(server, "get_dictionary_entry_for_user", return_value={"id": 123}), \
             patch.object(server, "get_card_review_log_by_review_id", return_value=None), \
             patch.object(server, "get_card_srs_state", return_value=None), \
             patch.object(server, "upsert_card_srs_state", return_value={"status": "review", "interval_days": 1, "due_at": None, "stability": 1.0, "difficulty": 1.0}) as persist_mock, \
             patch.object(server, "insert_card_review_log", return_value={"inserted": True, "review_log_id": 9}), \
             patch.object(server, "begin_free_usage_lifecycle_tx", return_value=(usage_state, None, None)), \
             patch.object(server, "finish_free_usage_lifecycle_success_tx", side_effect=server.FreeUsageLifecycleAbort(error_payload, 503)):
            response = client.post(
                "/api/cards/review",
                json={"initData": "signed", "card_id": 123, "review_id": "rv-increment-fails", "rating": "GOOD"},
            )

        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.get_json()["error"], "free_usage_state_unavailable")
        persist_mock.assert_called_once()
        self.assertFalse(conn.committed)
        self.assertTrue(conn.rolled_back)

    def test_fsrs_review_duplicate_noop_skips_usage_increment(self):
        conn, db_context = self._recording_db_context()
        client = server.app.test_client()
        usage_state = {
            "feature": "fsrs_card_review_daily",
            "feature_title": "Тренировка карточек",
            "effective_mode": "free",
            "operation_kind": "count_successful_fsrs_review",
            "used": 20,
            "limit": 20,
            "skip_increment": False,
        }
        with patch.object(server, "_extract_webapp_user_from_init_data", return_value=(77, "test")), \
             patch.object(server, "_is_webapp_user_allowed", return_value=True), \
             patch.object(server, "_get_user_language_pair", return_value=("ru", "de", None)), \
             patch.object(server, "_resolve_flashcards_manual_selection", return_value=None), \
             patch.object(server, "db_acquire_scope", self._fake_db_scope), \
             patch.object(server, "get_db_connection_context", db_context), \
             patch.object(server, "get_dictionary_entry_for_user", return_value={"id": 123}), \
             patch.object(server, "get_card_review_log_by_review_id", return_value={"id": 9}), \
             patch.object(server, "get_card_srs_state", return_value={"status": "review", "interval_days": 1, "due_at": None, "stability": 1.0, "difficulty": 1.0}), \
             patch.object(server, "_build_next_srs_payload", return_value={}), \
             patch.object(server, "begin_free_usage_lifecycle_tx", return_value=(usage_state, None, None)), \
             patch.object(server, "finish_free_usage_lifecycle_success_tx") as finish_mock, \
             patch.object(server, "_mark_today_plan_snapshot_stale"):
            response = client.post(
                "/api/cards/review",
                json={"initData": "signed", "card_id": 123, "review_id": "rv-dupe", "rating": "GOOD"},
            )

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.get_json()["duplicate"])
        finish_mock.assert_not_called()

    def test_fsrs_review_paid_user_commits_without_free_usage_event(self):
        conn, db_context = self._recording_db_context()
        client = server.app.test_client()
        usage_state = {
            "feature": "fsrs_card_review_daily",
            "feature_title": "Тренировка карточек",
            "effective_mode": "pro",
            "operation_kind": "count_successful_fsrs_review",
            "used": None,
            "limit": 20,
            "skip_increment": True,
        }
        with patch.object(server, "_extract_webapp_user_from_init_data", return_value=(77, "test")), \
             patch.object(server, "_is_webapp_user_allowed", return_value=True), \
             patch.object(server, "_get_user_language_pair", return_value=("ru", "de", None)), \
             patch.object(server, "_resolve_flashcards_manual_selection", return_value=None), \
             patch.object(server, "db_acquire_scope", self._fake_db_scope), \
             patch.object(server, "get_db_connection_context", db_context), \
             patch.object(server, "get_dictionary_entry_for_user", return_value={"id": 123}), \
             patch.object(server, "get_card_review_log_by_review_id", return_value=None), \
             patch.object(server, "get_card_srs_state", return_value=None), \
             patch.object(server, "upsert_card_srs_state", return_value={"status": "review", "interval_days": 1, "due_at": None, "stability": 1.0, "difficulty": 1.0}), \
             patch.object(server, "insert_card_review_log", return_value={"inserted": True, "review_log_id": 9}), \
             patch.object(server, "_build_next_srs_payload", return_value={}), \
             patch.object(server, "begin_free_usage_lifecycle_tx", return_value=(usage_state, None, None)), \
             patch.object(server, "_mark_today_plan_snapshot_stale"):
            response = client.post(
                "/api/cards/review",
                json={"initData": "signed", "card_id": 123, "review_id": "rv-paid", "rating": "GOOD"},
            )

        self.assertEqual(response.status_code, 200)
        self.assertTrue(conn.committed)
        self.assertFalse(conn.rolled_back)

    def test_fsrs_review_missing_config_rolls_back_before_mutation(self):
        conn, db_context = self._recording_db_context()
        client = server.app.test_client()
        error_payload = {
            "ok": False,
            "error": "free_usage_state_unavailable",
            "feature": "fsrs_card_review_daily",
            "feature_title": "Тренировка карточек",
            "message": "Не удалось проверить лимит бесплатного тарифа.",
        }
        with patch.object(server, "_extract_webapp_user_from_init_data", return_value=(77, "test")), \
             patch.object(server, "_is_webapp_user_allowed", return_value=True), \
             patch.object(server, "_get_user_language_pair", return_value=("ru", "de", None)), \
             patch.object(server, "_resolve_flashcards_manual_selection", return_value=None), \
             patch.object(server, "db_acquire_scope", self._fake_db_scope), \
             patch.object(server, "get_db_connection_context", db_context), \
             patch.object(server, "get_dictionary_entry_for_user", return_value={"id": 123}), \
             patch.object(server, "get_card_review_log_by_review_id", return_value=None), \
             patch.object(server, "begin_free_usage_lifecycle_tx", return_value=(None, error_payload, 503)), \
             patch.object(server, "upsert_card_srs_state") as persist_mock:
            response = client.post(
                "/api/cards/review",
                json={"initData": "signed", "card_id": 123, "review_id": "rv-missing-config", "rating": "GOOD"},
            )

        self.assertEqual(response.status_code, 503)
        persist_mock.assert_not_called()
        self.assertFalse(conn.committed)
        self.assertTrue(conn.rolled_back)

    def test_fsrs_next_fetch_does_not_use_free_usage_lifecycle(self):
        client = server.app.test_client()
        with patch.object(server, "_extract_webapp_user_from_init_data", return_value=(77, "test")), \
             patch.object(server, "_is_webapp_user_allowed", return_value=True), \
             patch.object(server, "_get_user_language_pair", return_value=("ru", "de", None)), \
             patch.object(server, "_resolve_flashcards_manual_selection", return_value=None), \
             patch.object(server, "db_acquire_scope", self._fake_db_scope), \
             patch.object(server, "get_db_connection_context", self._fake_db_connection), \
             patch.object(server, "_build_next_srs_payload", return_value={"card": {"id": 123}}), \
             patch.object(server, "run_free_usage_lifecycle") as lifecycle_mock:
            response = client.get("/api/cards/next?initData=signed")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["card"]["id"], 123)
        lifecycle_mock.assert_not_called()

    def test_free_user_can_start_first_translation_session(self):
        entitlement = {
            "effective_mode": "free",
            "reset_at": "2026-05-28T00:00:00+02:00",
        }
        with patch.object(workflow, "begin_free_usage_lifecycle", return_value=({
            "effective_mode": "free",
            "used": 0.0,
            "limit": 1.0,
            "skip_increment": False,
        }, None, None)):
            state, payload = workflow._check_translation_daily_session_limit(
                user_id=77,
                route="/api/webapp/start",
            )

        self.assertIsNone(payload)
        self.assertEqual(state["effective_mode"], "free")
        self.assertEqual(state["used"], 0.0)

    def test_free_user_blocked_on_second_translation_session_same_vienna_day(self):
        entitlement = {
            "effective_mode": "free",
            "reset_at": "2026-05-28T00:00:00+02:00",
        }
        with patch.object(workflow, "begin_free_usage_lifecycle", return_value=(None, {
            "error": "free_limit_exceeded",
            "feature": "translation_daily_sets",
        }, 429)):
            state, payload = workflow._check_translation_daily_session_limit(
                user_id=77,
                route="/api/webapp/start",
            )

        self.assertIsNone(state)
        self.assertEqual(payload["error"], "free_limit_exceeded")
        self.assertEqual(payload["feature"], "translation_daily_sets")

    def test_paid_user_bypasses_translation_limit(self):
        entitlement = {
            "effective_mode": "pro",
            "reset_at": "2026-05-28T00:00:00+02:00",
        }
        with patch.object(workflow, "begin_free_usage_lifecycle", return_value=({
            "effective_mode": "pro",
            "skip_increment": True,
        }, None, None)) as begin_mock:
            state, payload = workflow._check_translation_daily_session_limit(
                user_id=77,
                route="/api/webapp/start",
            )

        self.assertIsNone(payload)
        self.assertTrue(state["skip_increment"])
        begin_mock.assert_called_once()

    def test_translation_increment_after_created_session(self):
        state = {"effective_mode": "free", "used": 0.0, "limit": 1.0}
        with patch.object(workflow, "finish_free_usage_lifecycle_success", return_value=(None, None)) as increment_mock:
            payload = workflow._increment_translation_daily_session_usage(
                user_id=77,
                usage_state=state,
                session_id=123456,
                route="/api/webapp/start",
                source_lang="ru",
                target_lang="de",
            )

        self.assertIsNone(payload)
        increment_mock.assert_called_once()
        self.assertEqual(increment_mock.call_args.kwargs["idempotency_seed"], "session:123456")

    def test_failed_translation_start_does_not_increment(self):
        client = server.app.test_client()
        workflow_mock = AsyncMock(side_effect=RuntimeError("start failed"))
        with patch.object(server, "WEBAPP_SINGLE_INSTANCE_GUARD_ENABLED", False), \
             patch.object(server, "_telegram_hash_is_valid", return_value=True), \
             patch.object(server, "_parse_telegram_init_data", return_value={"user": {"id": 55, "first_name": "Iryna"}}), \
             patch.object(server, "_resolve_webapp_user_allowed", return_value=(True, "test")), \
             patch.object(server, "_try_acquire_shared_idempotency", return_value="token"), \
             patch.object(server, "_release_shared_idempotency"), \
             patch.object(server, "_clear_recent_finish_no_active_session"), \
             patch.object(server, "db_acquire_scope", self._fake_db_scope), \
             patch.object(server, "_get_user_language_pair_for_webapp_request", return_value=("ru", "de", None, "request")), \
             patch.object(server, "resolve_webapp_focus", return_value={"kind": "catalog", "key": "v2_main", "prompt_topic": "V2"}), \
             patch.object(server, "_refresh_subscription_before_translation_start"), \
             patch.object(server, "start_translation_session_webapp", workflow_mock), \
             patch.object(workflow, "finish_free_usage_lifecycle_success") as increment_mock, \
             patch.object(server, "_log_flow_observation"), \
             patch.object(server, "summarize_db_acquire_events", return_value={}):
            response = client.post(
                "/api/webapp/start",
                json={"initData": "signed", "topic": "V2", "level": "c1"},
            )

        self.assertEqual(response.status_code, 500)
        increment_mock.assert_not_called()

    def test_returning_existing_translation_session_does_not_increment(self):
        result = {
            "session_id": 123456,
            "created": False,
            "blocked": True,
            "items": [],
            "ready_count": 7,
            "expected_total": 7,
            "remaining_count": 0,
            "generation_in_progress": False,
            "phase_metrics": {},
        }
        client = server.app.test_client()
        workflow_mock = AsyncMock(return_value=result)
        with patch.object(server, "WEBAPP_SINGLE_INSTANCE_GUARD_ENABLED", False), \
             patch.object(server, "_telegram_hash_is_valid", return_value=True), \
             patch.object(server, "_parse_telegram_init_data", return_value={"user": {"id": 55, "first_name": "Iryna"}}), \
             patch.object(server, "_resolve_webapp_user_allowed", return_value=(True, "test")), \
             patch.object(server, "_try_acquire_shared_idempotency", return_value="token"), \
             patch.object(server, "_release_shared_idempotency"), \
             patch.object(server, "_clear_recent_finish_no_active_session"), \
             patch.object(server, "db_acquire_scope", self._fake_db_scope), \
             patch.object(server, "_get_user_language_pair_for_webapp_request", return_value=("ru", "de", None, "request")), \
             patch.object(server, "resolve_webapp_focus", return_value={"kind": "catalog", "key": "v2_main", "prompt_topic": "V2"}), \
             patch.object(server, "_refresh_subscription_before_translation_start"), \
             patch.object(server, "start_translation_session_webapp", workflow_mock), \
             patch.object(server, "_build_translation_session_start_payload", return_value=result), \
             patch.object(server, "_build_language_pair_payload", return_value={"source": "ru", "target": "de"}), \
             patch.object(server, "_maybe_trigger_translation_focus_pool_deficit_refill", return_value={"triggered": False}), \
             patch.object(server, "_estimate_json_payload_size_bytes", return_value=0), \
             patch.object(workflow, "finish_free_usage_lifecycle_success") as increment_mock, \
             patch.object(server, "_log_flow_observation"), \
             patch.object(server, "summarize_db_acquire_events", return_value={}), \
             patch.object(server, "_write_session_presence_projection_active"):
            response = client.post(
                "/api/webapp/start",
                json={"initData": "signed", "topic": "V2", "level": "c1"},
            )

        self.assertEqual(response.status_code, 200)
        increment_mock.assert_not_called()

    def test_translation_session_polling_does_not_increment(self):
        client = server.app.test_client()
        projection = {"type": "none", "session_id": None, "source_lang": "ru", "target_lang": "de"}
        with patch.object(server, "WEBAPP_SINGLE_INSTANCE_GUARD_ENABLED", False), \
             patch.object(server, "_telegram_hash_is_valid", return_value=True), \
             patch.object(server, "_parse_telegram_init_data", return_value={"user": {"id": 77}}), \
             patch.object(server, "_resolve_webapp_user_allowed", return_value=(True, "test")), \
             patch.object(server, "_load_session_presence_projection_with_source", return_value=(projection, "test")), \
             patch.object(server, "_build_session_presence_response_payload", return_value={"type": "none"}), \
             patch.object(server, "_estimate_json_payload_size_bytes", return_value=0), \
             patch.object(workflow, "finish_free_usage_lifecycle_success") as increment_mock, \
             patch.object(server, "_log_flow_observation"):
            response = client.post("/api/webapp/session", json={"initData": "signed"})

        self.assertEqual(response.status_code, 200)
        increment_mock.assert_not_called()

    def test_webapp_start_refreshes_subscription_before_workflow_limit_check(self):
        client = server.app.test_client()
        workflow_mock = AsyncMock(return_value={
            "session_id": 123456,
            "items": [],
            "ready_count": 0,
            "expected_total": 7,
            "remaining_count": 7,
            "generation_in_progress": False,
            "generation_status": "idle",
            "phase_metrics": {},
        })

        with patch.object(server, "WEBAPP_SINGLE_INSTANCE_GUARD_ENABLED", False), \
             patch.object(server, "_telegram_hash_is_valid", return_value=True), \
             patch.object(server, "_parse_telegram_init_data", return_value={"user": {"id": 55, "first_name": "Iryna"}}), \
             patch.object(server, "_resolve_webapp_user_allowed", return_value=(True, "test")), \
             patch.object(server, "_try_acquire_shared_idempotency", return_value="token"), \
             patch.object(server, "_release_shared_idempotency"), \
             patch.object(server, "_clear_recent_finish_no_active_session"), \
             patch.object(server, "db_acquire_scope", self._fake_db_scope), \
             patch.object(server, "_get_user_language_pair_for_webapp_request", return_value=("ru", "de", None, "request")), \
             patch.object(server, "resolve_webapp_focus", return_value={"kind": "catalog", "key": "v2_main", "prompt_topic": "V2"}), \
             patch.object(server, "_refresh_subscription_before_translation_start") as refresh_mock, \
             patch.object(server, "start_translation_session_webapp", workflow_mock), \
             patch.object(server, "_build_translation_session_start_payload", return_value={
                 "session_id": 123456,
                 "items": [],
                 "ready_count": 0,
                 "expected_total": 7,
                 "remaining_count": 7,
                 "generation_in_progress": False,
                 "generation_status": "idle",
             }), \
             patch.object(server, "_build_language_pair_payload", return_value={"source": "ru", "target": "de"}), \
             patch.object(server, "_maybe_trigger_translation_focus_pool_deficit_refill", return_value={"triggered": False}), \
             patch.object(server, "_estimate_json_payload_size_bytes", return_value=0), \
             patch.object(server, "_log_flow_observation"), \
             patch.object(server, "summarize_db_acquire_events", return_value={}), \
             patch.object(server, "_write_session_presence_projection_active"):
            response = client.post(
                "/api/webapp/start",
                json={
                    "initData": "signed",
                    "topic": "V2",
                    "level": "c1",
                },
            )

        self.assertEqual(response.status_code, 200)
        refresh_mock.assert_called_once_with(55)
        workflow_mock.assert_called_once()


if __name__ == "__main__":
    unittest.main()
