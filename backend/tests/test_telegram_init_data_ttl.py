import time
import unittest

import backend.backend_server as server


class TelegramInitDataTtlTests(unittest.TestCase):
    def setUp(self):
        self.original_token = server.TELEGRAM_Deutsch_BOT_TOKEN
        self.original_ttl = server.TELEGRAM_WEBAPP_INIT_TTL_SECONDS
        server.TELEGRAM_Deutsch_BOT_TOKEN = "test-bot-token"
        server.TELEGRAM_WEBAPP_INIT_TTL_SECONDS = 60

    def tearDown(self):
        server.TELEGRAM_Deutsch_BOT_TOKEN = self.original_token
        server.TELEGRAM_WEBAPP_INIT_TTL_SECONDS = self.original_ttl

    def test_signed_init_data_is_valid_while_fresh(self):
        init_data = server._build_signed_init_data_for_user(
            {"id": 123456, "first_name": "Test"},
            auth_date=int(time.time()) - 30,
        )

        self.assertTrue(server._telegram_hash_is_valid(init_data))

    def test_signed_init_data_is_rejected_after_ttl(self):
        init_data = server._build_signed_init_data_for_user(
            {"id": 123456, "first_name": "Test"},
            auth_date=int(time.time()) - 120,
        )

        self.assertFalse(server._telegram_init_data_auth_date_is_fresh(init_data, max_age_seconds=60))
        self.assertFalse(server._telegram_hash_is_valid(init_data))


if __name__ == "__main__":
    unittest.main()
