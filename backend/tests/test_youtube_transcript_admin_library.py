import unittest
from contextlib import ExitStack, contextmanager
from unittest.mock import patch

import backend.backend_server as server


@contextmanager
def _fake_db_scope(_name):
    yield []


class YoutubeTranscriptAdminLibraryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = server.app.test_client()

    def _post_transcript(self, user_id: int, extra_patches, *, patch_billing_guard: bool = True):
        base_patches = [
            patch.object(server, "WEBAPP_SINGLE_INSTANCE_GUARD_ENABLED", False),
            patch.object(server, "_telegram_hash_is_valid", return_value=True),
            patch.object(server, "_parse_telegram_init_data", return_value={"user": {"id": user_id}}),
            patch.object(server, "_resolve_webapp_user_allowed", return_value=(True, "test")),
            patch.object(server, "db_acquire_scope", _fake_db_scope),
            patch.object(server, "_get_user_language_pair", return_value=("ru", "de", {})),
            patch.object(server, "has_youtube_proxy_subtitles_access", return_value=True),
            patch.object(server, "_log_flow_observation"),
            patch.object(server, "summarize_db_acquire_events", return_value={}),
            patch.object(server, "_estimate_json_payload_size_bytes", return_value=100),
            patch.object(server, "_billing_log_event_safe"),
        ]
        if patch_billing_guard:
            base_patches.append(patch.object(server, "_apply_billing_guard", return_value=(None, None)))
        with ExitStack() as stack:
            for patcher in base_patches + list(extra_patches):
                stack.enter_context(patcher)
            return self.client.post(
                "/api/webapp/youtube/transcript",
                json={
                    "initData": "signed",
                    "videoId": "video-123",
                    "lang": "de",
                },
            )

    def test_admin_cache_miss_creates_transcript(self):
        fetched = {
            "items": [{"text": "Hallo", "start": 0, "duration": 1}],
            "language": "de",
            "is_generated": False,
            "translations": {},
            "source": "test",
        }
        upsert_patcher = patch.object(server, "upsert_youtube_transcript_cache")
        fetch_patcher = patch.object(server, "_fetch_youtube_transcript", return_value=fetched)

        with upsert_patcher as upsert_mock, fetch_patcher as fetch_mock:
            response = self._post_transcript(
                117649764,
                [
                    patch.object(server, "_load_cached_youtube_transcript_data", return_value=(None, None, 0)),
                    patch.object(server, "is_youtube_transcript_async_enabled", return_value=False),
                ],
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["items"], fetched["items"])
        fetch_mock.assert_called_once_with("video-123", lang="de", allow_proxy=True)
        upsert_mock.assert_called_once()

    def test_admin_cache_hit_still_works(self):
        cached = {
            "items": [{"text": "Cached", "start": 0, "duration": 1}],
            "language": "de",
            "is_generated": False,
            "translations": {},
            "source": "db",
        }
        with patch.object(server, "_fetch_youtube_transcript") as fetch_mock, \
             patch.object(server, "upsert_youtube_transcript_cache") as upsert_mock:
            response = self._post_transcript(
                117649764,
                [
                    patch.object(server, "_load_cached_youtube_transcript_data", return_value=(cached, "db_cache", 1)),
                ],
            )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["cached_db"])
        self.assertEqual(payload["items"], cached["items"])
        fetch_mock.assert_not_called()
        upsert_mock.assert_not_called()

    def test_non_admin_cache_hit_works(self):
        cached = {
            "items": [{"text": "Shared", "start": 0, "duration": 1}],
            "language": "de",
            "is_generated": False,
            "translations": {},
            "source": "db",
        }
        with patch.object(server, "_fetch_youtube_transcript") as fetch_mock, \
             patch.object(server, "upsert_youtube_transcript_cache") as upsert_mock:
            response = self._post_transcript(
                55,
                [
                    patch.object(server, "_load_cached_youtube_transcript_data", return_value=(cached, "db_cache", 1)),
                ],
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["items"], cached["items"])
        fetch_mock.assert_not_called()
        upsert_mock.assert_not_called()

    def test_non_admin_cache_hit_bypasses_fetch_limit_guard(self):
        cached = {
            "items": [{"text": "Shared", "start": 0, "duration": 1}],
            "language": "de",
            "is_generated": False,
            "translations": {},
            "source": "db",
        }
        with patch.object(server, "_load_cached_youtube_transcript_data", return_value=(cached, "db_cache", 1)), \
             patch.object(server, "_fetch_youtube_transcript") as fetch_mock, \
             patch.object(server, "enforce_feature_limit", return_value={"error": "limit"}):
            response = self._post_transcript(
                55,
                [],
                patch_billing_guard=False,
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["items"], cached["items"])
        fetch_mock.assert_not_called()

    def test_non_admin_cache_miss_cannot_create_transcript(self):
        with patch.object(server, "_fetch_youtube_transcript") as fetch_mock, \
             patch.object(server, "upsert_youtube_transcript_cache") as upsert_mock:
            response = self._post_transcript(
                55,
                [
                    patch.object(server, "_load_cached_youtube_transcript_data", return_value=(None, None, 0)),
                    patch.object(server, "is_youtube_transcript_async_enabled", return_value=False),
                ],
            )

        self.assertEqual(response.status_code, 403)
        payload = response.get_json()
        self.assertEqual(payload["error_code"], "youtube_transcript_not_in_library")
        fetch_mock.assert_not_called()
        upsert_mock.assert_not_called()

    def test_non_admin_cache_miss_does_not_enqueue_transcript_job(self):
        with patch.object(server, "enqueue_youtube_transcript_job") as enqueue_mock, \
             patch.object(server, "_fetch_youtube_transcript") as fetch_mock:
            response = self._post_transcript(
                55,
                [
                    patch.object(server, "_load_cached_youtube_transcript_data", return_value=(None, None, 0)),
                    patch.object(server, "is_youtube_transcript_async_enabled", return_value=True),
                ],
            )

        self.assertEqual(response.status_code, 403)
        self.assertEqual(response.get_json()["error_code"], "youtube_transcript_not_in_library")
        enqueue_mock.assert_not_called()
        fetch_mock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
