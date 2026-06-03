import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import backend.backend_server as server


class EntitlementBootstrapPathTests(unittest.TestCase):
    def test_stripe_customer_bootstrap_uses_free_subscription_row(self):
        bootstrap_subscription = {
            "user_id": 77,
            "plan_code": "free",
            "status": "inactive",
            "trial_ends_at": None,
            "stripe_customer_id": None,
        }
        stripe_customer = SimpleNamespace(id="cus_test_123", deleted=False)
        create_mock = MagicMock(return_value=stripe_customer)
        fake_stripe = SimpleNamespace(Customer=SimpleNamespace(create=create_mock))

        with patch.object(server, "get_or_create_user_subscription", return_value=bootstrap_subscription) as bootstrap_mock, \
             patch.object(server, "stripe", fake_stripe), \
             patch.object(server, "bind_stripe_customer_to_user", return_value={
                 **bootstrap_subscription,
                 "stripe_customer_id": "cus_test_123",
             }) as bind_mock:
            customer_id = server._get_or_create_stripe_customer_id(77, username="Iryna")

        self.assertEqual(customer_id, "cus_test_123")
        bootstrap_mock.assert_called_once()
        self.assertEqual(bootstrap_mock.return_value["status"], "inactive")
        self.assertIsNone(bootstrap_mock.return_value["trial_ends_at"])
        create_mock.assert_called_once()
        bind_mock.assert_called_once_with(77, "cus_test_123")


if __name__ == "__main__":
    unittest.main()
