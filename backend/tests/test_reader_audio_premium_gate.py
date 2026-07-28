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

    # Per-page audio is NOT a Pro feature any more — it is a per-book paid add-on. Own
    # books need an unlock; public-domain «Классика» is voiced for everyone at no cost.
    OWN_BOOK = {
        "id": 17,
        "total_pages": 3,
        "total_chars": 5000,
        "content_pages": [{"page_number": 1, "text": "Es war einmal ein Haus."}],
    }

    def _post_page(self):
        return self.client.post(
            "/api/webapp/reader/audio/page",
            json={"initData": "valid", "document_id": 17, "page": 1},
        )

    def test_reader_audio_page_requires_a_paid_unlock_for_own_books(self):
        with patch.object(server, "_telegram_hash_is_valid", return_value=True), \
             patch.object(server, "_parse_telegram_init_data", return_value={"user": {"id": 55}}), \
             patch.object(server, "_resolve_webapp_user_allowed", return_value=(True, "test")), \
             patch.object(server, "_get_user_language_pair", return_value=("ru", "de", {})), \
             patch.object(server, "_resolve_reader_document_for_user", return_value=(self.OWN_BOOK, False)), \
             patch.object(server, "is_book_audio_unlocked", return_value=False), \
             patch.object(server, "get_book_audio_unlock_coverage", return_value=0), \
             patch.object(server, "get_audio_wallet_balance_minor", return_value=0):
            response = self._post_page()

        self.assertEqual(response.status_code, 402)
        payload = response.get_json()
        self.assertEqual(payload["error_code"], "audio_unlock_required")
        # The refusal must carry what it costs and what the user has — a price tag, not a wall.
        self.assertIn("price_minor", payload["unlock"])
        self.assertIn("balance_minor", payload["unlock"])

    def test_public_domain_book_is_never_gated(self):
        """«Классика» is free to read AND to listen to — the gate must not fire at all."""
        with patch.object(server, "_telegram_hash_is_valid", return_value=True), \
             patch.object(server, "_parse_telegram_init_data", return_value={"user": {"id": 55}}), \
             patch.object(server, "_resolve_webapp_user_allowed", return_value=(True, "test")), \
             patch.object(server, "_get_user_language_pair", return_value=("ru", "de", {})), \
             patch.object(server, "_resolve_reader_document_for_user", return_value=(self.OWN_BOOK, True)), \
             patch.object(server, "is_book_audio_unlocked", return_value=False) as unlock_mock:
            response = self._post_page()

        self.assertNotEqual(response.status_code, 402)
        unlock_mock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
