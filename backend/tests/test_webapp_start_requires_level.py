import unittest
from unittest.mock import patch

import backend.backend_server as server


class WebappStartRequiresLevelTests(unittest.TestCase):
    def setUp(self):
        self.client = server.app.test_client()

    def test_missing_level_returns_400(self):
        with patch.object(server, "_telegram_hash_is_valid", return_value=True), \
             patch.object(server, "_parse_telegram_init_data", return_value={"user": {"id": 55, "first_name": "Iryna"}}), \
             patch.object(server, "_resolve_webapp_user_allowed", return_value=(True, "test")):
            response = self.client.post(
                "/api/webapp/start",
                json={
                    "initData": "signed",
                    "topic": "V2",
                },
            )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.get_json()["error"], "level обязателен")

    def test_invalid_level_returns_400(self):
        with patch.object(server, "_telegram_hash_is_valid", return_value=True), \
             patch.object(server, "_parse_telegram_init_data", return_value={"user": {"id": 55, "first_name": "Iryna"}}), \
             patch.object(server, "_resolve_webapp_user_allowed", return_value=(True, "test")):
            response = self.client.post(
                "/api/webapp/start",
                json={
                    "initData": "signed",
                    "topic": "V2",
                    "level": "b3",
                },
            )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.get_json()["error"], "Некорректный level")


if __name__ == "__main__":
    unittest.main()
