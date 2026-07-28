"""Cross-process cache invalidation.

Both the billing decision cache and the allow-list hot cache live in PROCESS memory.
With one web worker a local drop is enough; the moment the service is scaled wider, a
paid upgrade (or a /deny) would stay invisible on every other worker until its TTL ran
out. These tests pin the feed that carries such events between processes.

Covers:
  * an invalidation is announced on the shared feed (payment path)
  * a reader applies a «billing» event to ITS OWN cache, without re-announcing it
  * a reader applies an «allowlist» event (both the decision and the self-serve memo)
  * repeated invalidations for the same user are distinct events (not swallowed by ZADD)
  * no Redis → local drop still happens, nothing raises
"""

import unittest
from unittest.mock import patch

import backend.backend_server as server
import backend.database as database


class _FakeRedis:
    """Just enough of a sorted set for the feed."""

    def __init__(self):
        self.zset: dict[str, float] = {}
        self.deleted: list[str] = []

    def zadd(self, key, mapping):
        self.zset.update(mapping)

    def zremrangebyscore(self, key, low, high):
        for member, score in list(self.zset.items()):
            if low <= score <= high:
                self.zset.pop(member, None)

    def zrangebyscore(self, key, low, high):
        items = sorted(self.zset.items(), key=lambda kv: kv[1])
        return [m for m, score in items if score >= float(low)]

    def delete(self, key):
        self.deleted.append(key)

    def get(self, key):
        return None

    def set(self, *_args, **_kwargs):
        return True


class CacheInvalidationFeedTests(unittest.TestCase):
    def setUp(self):
        self.redis = _FakeRedis()

    def test_payment_announces_invalidation_on_the_feed(self):
        with patch("backend.job_queue.get_redis_client", return_value=self.redis):
            server._invalidate_billing_guard_cache_for_user(4242)
        members = list(self.redis.zset)
        self.assertEqual(len(members), 1)
        self.assertTrue(members[0].startswith("billing:4242:"))

    def test_reader_applies_billing_event_to_its_own_cache(self):
        # Simulate "another worker" holding a stale free-tier decision for this user.
        with server._BILLING_GUARD_DECISION_CACHE_LOCK:
            server._BILLING_GUARD_DECISION_CACHE["4242|/api/webapp/story/submit"] = (
                ({"error": "paid_feature_required"}, 402),
                9e18,
            )
        server._apply_cache_invalidation_event("billing", 4242)
        with server._BILLING_GUARD_DECISION_CACHE_LOCK:
            self.assertNotIn("4242|/api/webapp/story/submit", server._BILLING_GUARD_DECISION_CACHE)

    def test_reader_does_not_republish_what_it_consumed(self):
        """Applying an event must NOT announce it again — otherwise two workers keep
        handing the same event back and forth forever."""
        with server._BILLING_GUARD_DECISION_CACHE_LOCK:
            server._BILLING_GUARD_DECISION_CACHE["4343|/api/token"] = ((None, None), 9e18)
        with patch("backend.job_queue.get_redis_client", return_value=self.redis):
            server._apply_cache_invalidation_event("billing", 4343)
        self.assertEqual(self.redis.zset, {})

    def test_reader_applies_allowlist_event(self):
        server._cache_webapp_allowlist(4444, True)
        server._remember_self_serve_attempt(4444)
        with patch.object(server, "invalidate_telegram_user_allowed_cache", lambda uid: None):
            server._apply_cache_invalidation_event("allowlist", 4444)
        self.assertIsNone(
            server._HOTPATH_ALLOWLIST_CACHE.get(server._allowlist_cache_key(4444), allow_stale=True)
        )
        self.assertIsNone(
            server._HOTPATH_ALLOWLIST_CACHE.get(
                server._self_serve_attempt_memo_key(4444), allow_stale=True
            )
        )

    def test_repeated_events_for_same_user_are_not_swallowed(self):
        with patch("backend.job_queue.get_redis_client", return_value=self.redis):
            database.publish_cache_invalidation("billing", 55)
            database.publish_cache_invalidation("billing", 55)
        self.assertEqual(len(self.redis.zset), 2)

    def test_without_redis_local_drop_still_happens(self):
        with server._BILLING_GUARD_DECISION_CACHE_LOCK:
            server._BILLING_GUARD_DECISION_CACHE["66|/api/token"] = ((None, None), 9e18)
        with patch("backend.job_queue.get_redis_client", return_value=None):
            server._invalidate_billing_guard_cache_for_user(66)  # must not raise
        with server._BILLING_GUARD_DECISION_CACHE_LOCK:
            self.assertNotIn("66|/api/token", server._BILLING_GUARD_DECISION_CACHE)


if __name__ == "__main__":
    unittest.main()
