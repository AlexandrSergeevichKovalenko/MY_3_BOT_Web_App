import json
import time
import unittest
from unittest.mock import patch

import bot_3


class _FakeRedis:
    def __init__(self):
        self.hashes = {}

    def scan_iter(self, match="*", count=100):
        import fnmatch
        for k in list(self.hashes.keys()):
            if fnmatch.fnmatch(k, match):
                yield k

    def hgetall(self, key):
        return dict(self.hashes.get(key, {}))

    def hdel(self, key, *fields):
        h = self.hashes.get(key, {})
        for f in fields:
            h.pop(f, None)
        return True


class NightlyStalePendingCleanupTests(unittest.TestCase):
    def setUp(self):
        bot_3.pending_dictionary_lookup_requests.clear()

    def test_purge_drops_only_stale(self):
        now = time.time()
        fresh = {"user_id": 1, "text": "begegnen", "created_at": now - 60}
        stale = {"user_id": 1, "text": "(batch x 10000)", "created_at": now - bot_3._DICT_PENDING_MAX_AGE_SEC - 100}
        bot_3.pending_dictionary_lookup_requests["fresh"] = fresh
        bot_3.pending_dictionary_lookup_requests["stale"] = stale

        redis = _FakeRedis()
        redis.hashes["dict_pending_user_hash:1"] = {
            "fresh": json.dumps(fresh),
            "stale": json.dumps(stale),
        }
        with patch("backend.job_queue.get_redis_client", return_value=redis):
            removed = bot_3._purge_stale_pending_all_users()

        # memory: stale gone, fresh kept
        self.assertIn("fresh", bot_3.pending_dictionary_lookup_requests)
        self.assertNotIn("stale", bot_3.pending_dictionary_lookup_requests)
        # redis hash: stale field gone, fresh kept
        h = redis.hashes["dict_pending_user_hash:1"]
        self.assertIn("fresh", h)
        self.assertNotIn("stale", h)
        self.assertGreaterEqual(removed, 2)  # 1 memory + 1 redis field


if __name__ == "__main__":
    unittest.main()
