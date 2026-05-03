from contextlib import contextmanager
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

import backend.backend_server as server


class BillingSubscriptionSyncTests(unittest.TestCase):
    @staticmethod
    @contextmanager
    def _fake_db_scope(*_args, **_kwargs):
        yield []

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
