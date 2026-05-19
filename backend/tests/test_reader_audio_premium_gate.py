import unittest
from unittest.mock import patch

import backend.backend_server as server


class ReaderAudioPremiumGateTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = server.app.test_client()

    def test_reader_audio_export_requires_premium(self):
        with patch.object(server, "_telegram_hash_is_valid", return_value=True), \
             patch.object(server, "_parse_telegram_init_data", return_value={"user": {"id": 55}}), \
             patch.object(server, "_resolve_webapp_user_allowed", return_value=(True, "test")), \
             patch.object(server, "_get_user_language_pair", return_value=("ru", "de", {})), \
             patch.object(server, "_resolve_user_entitlement", return_value=({"effective_mode": "free"}, None)):
            response = self.client.post(
                "/api/webapp/reader/audio",
                json={
                    "initData": "valid",
                    "document_id": 17,
                },
            )

        self.assertEqual(response.status_code, 403)
        payload = response.get_json()
        self.assertEqual(payload["error_code"], "reader_audio_premium_required")

    def test_reader_audio_page_requires_premium(self):
        with patch.object(server, "_telegram_hash_is_valid", return_value=True), \
             patch.object(server, "_parse_telegram_init_data", return_value={"user": {"id": 55}}), \
             patch.object(server, "_resolve_webapp_user_allowed", return_value=(True, "test")), \
             patch.object(server, "_get_user_language_pair", return_value=("ru", "de", {})), \
             patch.object(server, "_resolve_user_entitlement", return_value=({"effective_mode": "free"}, None)):
            response = self.client.post(
                "/api/webapp/reader/audio/page",
                json={
                    "initData": "valid",
                    "document_id": 17,
                    "page": 1,
                },
            )

        self.assertEqual(response.status_code, 403)
        payload = response.get_json()
        self.assertEqual(payload["error_code"], "reader_audio_premium_required")


if __name__ == "__main__":
    unittest.main()
