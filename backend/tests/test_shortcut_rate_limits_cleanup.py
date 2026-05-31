import os
import unittest
from unittest.mock import patch

import backend.backend_server as server


class _FakeRedis:
    def __init__(self):
        self.values: dict[str, int] = {}
        self.ttls: dict[str, int] = {}

    def eval(self, script, numkeys, key, window_seconds):
        current = int(self.values.get(key, 0)) + 1
        self.values[key] = current
        if current == 1:
            self.ttls[key] = int(window_seconds)
        return [current, self.ttls.get(key, int(window_seconds))]

    def get(self, key):
        return self.values.get(key)

    def ttl(self, key):
        if key not in self.values:
            return -2
        return self.ttls.get(key, 600)

    def incr(self, key):
        current = int(self.values.get(key, 0)) + 1
        self.values[key] = current
        return current

    def expire(self, key, window_seconds):
        self.ttls[key] = int(window_seconds)
        return True

    def delete(self, key):
        self.values.pop(key, None)
        self.ttls.pop(key, None)
        return 1


class ShortcutRateLimitAndCleanupTests(unittest.TestCase):
    def setUp(self):
        self.client = server.app.test_client()
        self.redis = _FakeRedis()

    def test_pairing_code_issuance_is_limited_per_user(self):
        with patch.dict(os.environ, {"SHORTCUT_BOT_SECRET": "adminsecret"}, clear=False), \
             patch.object(server, "get_redis_client", return_value=self.redis), \
             patch.object(server, "is_telegram_user_allowed", return_value=True), \
             patch.object(
                 server,
                 "create_shortcut_pairing_code",
                 return_value={
                     "pairing_code_id": 7,
                     "user_id": 117649764,
                     "pairing_code": "A7F4K2",
                     "created_at": None,
                     "expires_at": None,
                     "expires_in": 600,
                 },
             ) as create_mock:
            responses = [
                self.client.post(
                    "/api/shortcut/pairing-code",
                    json={"user_id": 117649764},
                    headers={"Authorization": "Bearer adminsecret"},
                )
                for _ in range(4)
            ]

        self.assertEqual([resp.status_code for resp in responses[:3]], [200, 200, 200])
        self.assertEqual(responses[3].status_code, 429)
        self.assertEqual(responses[3].get_json()["error"], "shortcut_rate_limited")
        self.assertEqual(responses[3].headers.get("Retry-After"), "600")
        self.assertEqual(create_mock.call_count, 3)

    def test_link_endpoint_rate_limits_by_ip(self):
        with patch.object(server, "_SHORTCUT_LINK_IP_LIMIT", 1), \
             patch.object(server, "_shortcut_request_ip", return_value="test-ip"), \
             patch.object(server, "get_redis_client", return_value=self.redis), \
             patch.object(
                 server,
                 "link_shortcut_installation",
                 return_value={
                     "status": "linked",
                     "pairing_code_id": 7,
                     "installation_id": 11,
                     "user_id": 117649764,
                     "install_token": "install-token-value",
                     "created_at": None,
                     "expires_at": None,
                 },
             ) as link_mock:
            first = self.client.post("/api/shortcut/link", json={"pairing_code": "A7F4K2"})
            second = self.client.post("/api/shortcut/link", json={"pairing_code": "A7F4K2"})

        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 429)
        self.assertEqual(second.get_json()["error"], "shortcut_rate_limited")
        self.assertEqual(second.headers.get("Retry-After"), "600")
        self.assertEqual(link_mock.call_count, 1)

    def test_link_endpoint_rate_limits_invalid_attempts_per_pairing_code(self):
        with patch.object(server, "_shortcut_request_ip", return_value="test-ip"), \
             patch.object(server, "get_redis_client", return_value=self.redis), \
             patch.object(
                 server,
                 "link_shortcut_installation",
                 return_value={"status": "invalid"},
             ) as link_mock:
            responses = [
                self.client.post("/api/shortcut/link", json={"pairing_code": "A7F4K2"})
                for _ in range(6)
            ]

        self.assertEqual([resp.status_code for resp in responses[:5]], [400, 400, 400, 400, 400])
        self.assertEqual(responses[5].status_code, 429)
        self.assertEqual(responses[5].get_json()["error"], "pairing_code_rate_limited")
        self.assertEqual(responses[5].headers.get("Retry-After"), "600")
        self.assertEqual(link_mock.call_count, 5)

    def test_shortcut_pairing_code_cleanup_job_calls_purge(self):
        with patch.object(
            server,
            "purge_expired_shortcut_pairing_codes",
            return_value={
                "expired_deleted": 2,
                "consumed_deleted": 5,
                "expired_retention_seconds": 86400,
                "consumed_retention_seconds": 2592000,
                "expired_cutoff": None,
                "consumed_cutoff": None,
            },
        ) as purge_mock:
            server._run_shortcut_pairing_code_cleanup_job()

        purge_mock.assert_called_once_with(
            expired_retention_seconds=server._SHORTCUT_PAIRING_CODE_CLEANUP_AFTER_EXPIRED_SECONDS,
            consumed_retention_seconds=server._SHORTCUT_PAIRING_CODE_CLEANUP_AFTER_CONSUMED_SECONDS,
        )


if __name__ == "__main__":
    unittest.main()
