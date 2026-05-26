import unittest
from unittest.mock import patch

import backend.backend_server as server


class ReaderAudioPrefetchTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = server.app.test_client()

    def test_prefetch_only_falls_back_to_sync_generation_when_async_unavailable(self):
        document = {
            "id": 17,
            "title": "Test book",
            "content_pages": [
                {"page_number": 1, "text": "Das ist nur ein kurzer Testsatz."},
            ],
        }
        ready_payload = {
            "audio_url": "https://cdn.example.test/reader/p1.mp3",
            "mime": "audio/mpeg",
            "duration_ms": 1800,
            "word_timings": [{"wid": 0, "start_ms": 0, "end_ms": 400, "char_start": 0, "char_end": 2}],
            "voice": "de-DE-Standard-A",
            "rate": 1.0,
            "cached": False,
        }

        with patch.object(server, "_telegram_hash_is_valid", return_value=True), \
             patch.object(server, "_parse_telegram_init_data", return_value={"user": {"id": 55}}), \
             patch.object(server, "_resolve_webapp_user_allowed", return_value=(True, "test")), \
             patch.object(server, "_get_user_language_pair", return_value=("ru", "de", {})), \
             patch.object(server, "_resolve_user_entitlement", return_value=({"effective_mode": "pro"}, None)), \
             patch.object(server, "get_reader_library_document", return_value=document), \
             patch.object(server, "get_cached_reader_audio_page", return_value=None), \
             patch.object(server, "is_reader_audio_page_async_enabled", return_value=False), \
             patch.object(server, "can_enqueue_background_jobs", return_value=False), \
             patch.object(server, "enforce_reader_audio_pro_monthly_limit", return_value=None), \
             patch.object(server, "acquire_reader_audio_singleflight_slot", return_value=(True, object())), \
             patch.object(server, "release_reader_audio_singleflight_slot"), \
             patch.object(server, "_generate_and_cache_reader_audio_page", return_value=ready_payload) as generate_mock:
            response = self.client.post(
                "/api/webapp/reader/audio/page",
                json={
                    "initData": "valid",
                    "document_id": 17,
                    "page": 1,
                    "prefetch_only": True,
                },
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["audio_url"], ready_payload["audio_url"])
        generate_mock.assert_called_once()


if __name__ == "__main__":
    unittest.main()
