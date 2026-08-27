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


class DbPoolStarvationMessageTests(unittest.TestCase):
    """Письмо обязано отличать «запрос упал» от «ушёл мимо пула», а не звать оба
    «резервным путём». Разбор 27.08.2026: DB_POOL_ALLOW_DIRECT_FALLBACK выключен во
    всех сервисах, поэтому в проде срабатывала ровно ветка падения — а письмо при
    этом писало «Это НЕ падение»."""

    @staticmethod
    def _events(kinds, label="unspecified"):
        return [(1000.0 + index, kind, label) for index, kind in enumerate(kinds)]

    def test_failures_are_called_a_failure_not_a_fallback(self):
        text = db._build_db_pool_starvation_message(self._events(["failed"] * 5))
        self.assertIn("🛑", text)
        self.assertIn("упали", text)
        self.assertIn("человек на экране увидел ошибку", text)
        self.assertNotIn("резервн", text.lower())

    def test_old_reassuring_line_is_gone(self):
        for kinds in (["failed"] * 5, ["fallback"] * 5):
            text = db._build_db_pool_starvation_message(self._events(kinds))
            self.assertNotIn("Это НЕ падение", text)

    def test_fallback_only_keeps_the_calm_heading(self):
        text = db._build_db_pool_starvation_message(self._events(["fallback"] * 5))
        self.assertIn("⚠️", text)
        self.assertNotIn("🛑", text)
        self.assertIn("мимо него прямым", text)

    def test_mixed_window_counts_both_kinds_separately(self):
        text = db._build_db_pool_starvation_message(
            self._events(["failed", "failed", "fallback", "fallback", "fallback"])
        )
        self.assertIn("2 запрос(ов)", text)
        self.assertIn("3 запрос(ов)", text)

    def test_unlabeled_culprit_says_not_recorded_not_unspecified(self):
        text = db._build_db_pool_starvation_message(self._events(["failed"] * 5))
        self.assertIn("НЕ ЗАПИСАНО", text)
        self.assertNotIn("unspecified", text)

    def test_named_culprits_are_listed_with_counts(self):
        events = (
            self._events(["failed"] * 3, label="http:/api/dictionary/search")
            + self._events(["failed"] * 2, label="bot:message")
        )
        text = db._build_db_pool_starvation_message(events)
        self.assertIn("http:/api/dictionary/search — 3", text)
        self.assertIn("bot:message — 2", text)
        self.assertNotIn("НЕ ЗАПИСАНО", text)

    def test_named_and_unnamed_are_both_reported(self):
        events = self._events(["failed"] * 3, label="bot:message") + self._events(["failed"] * 2)
        text = db._build_db_pool_starvation_message(events)
        self.assertIn("bot:message — 3", text)
        self.assertIn("ещё 2 без метки", text)


if __name__ == "__main__":
    unittest.main()
