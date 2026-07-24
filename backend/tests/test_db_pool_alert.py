"""DB pool starvation alert: fires ONLY on genuine, sustained starvation (fallback_direct /
hard failure repeated), never on the benign retry-then-success path or a single blip."""
import unittest
from unittest import mock

from backend import database as db


class DbPoolStarvationAlertTests(unittest.TestCase):
    def setUp(self):
        db._DB_POOL_STARVATION_EVENTS.clear()
        db._DB_POOL_ALERT_LAST_SENT_TS = 0.0

    def _fire(self, source="fallback_direct", success=True):
        db._maybe_alert_db_pool_saturation(
            context_label="test", connection_source=source, success=success, pool_used_count=3,
        )

    def test_benign_pool_success_never_counts_or_alerts(self):
        # The common case: got a connection from the pool (even after a brief retry). Must be
        # completely ignored — this was the false-positive that alarmed the user.
        for _ in range(20):
            self._fire(source="pool", success=True)
        self.assertEqual(len(db._DB_POOL_STARVATION_EVENTS), 0)
        self.assertEqual(db._DB_POOL_ALERT_LAST_SENT_TS, 0.0)

    def test_single_fallback_does_not_alert(self):
        self._fire(source="fallback_direct", success=True)
        self.assertEqual(len(db._DB_POOL_STARVATION_EVENTS), 1)
        self.assertEqual(db._DB_POOL_ALERT_LAST_SENT_TS, 0.0)

    def test_hard_failure_counts_as_starvation(self):
        self._fire(source="pool", success=False)
        self.assertEqual(len(db._DB_POOL_STARVATION_EVENTS), 1)

    def test_sustained_starvation_alerts_once_then_clears(self):
        with mock.patch("backend.telegram_notify._send_private_message"), \
             mock.patch.object(db, "get_admin_telegram_ids", return_value=set()):
            for _ in range(db.DB_POOL_SATURATION_ALERT_MIN_EVENTS):
                self._fire(source="fallback_direct", success=True)
        # Crossed the threshold → decided to send (timestamp set) and window reset.
        self.assertGreater(db._DB_POOL_ALERT_LAST_SENT_TS, 0.0)
        self.assertEqual(len(db._DB_POOL_STARVATION_EVENTS), 0)


if __name__ == "__main__":
    unittest.main()
