import unittest
from contextlib import ExitStack, contextmanager
from unittest.mock import AsyncMock, patch

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

    def _post_manual_transcript(self, user_id: int, extra_patches):
        base_patches = [
            patch.object(server, "WEBAPP_SINGLE_INSTANCE_GUARD_ENABLED", False),
            patch.object(server, "_telegram_hash_is_valid", return_value=True),
            patch.object(server, "_parse_telegram_init_data", return_value={"user": {"id": user_id}}),
            patch.object(server, "_resolve_webapp_user_allowed", return_value=(True, "test")),
        ]
        with ExitStack() as stack:
            for patcher in base_patches + list(extra_patches):
                stack.enter_context(patcher)
            return self.client.post(
                "/api/webapp/youtube/manual",
                json={
                    "initData": "signed",
                    "videoId": "video-123",
                    "language": "de",
                    "items": [
                        {"text": "Hallo", "start": 0, "duration": 1},
                        {"text": "Welt", "start": 1, "duration": 1},
                    ],
                },
            )

    def _post_translate(self, user_id: int, extra_patches, *, lines=None, patch_billing_guard: bool = True):
        base_patches = [
            patch.object(server, "WEBAPP_SINGLE_INSTANCE_GUARD_ENABLED", False),
            patch.object(server, "_telegram_hash_is_valid", return_value=True),
            patch.object(server, "_parse_telegram_init_data", return_value={"user": {"id": user_id}}),
            patch.object(server, "_resolve_webapp_user_allowed", return_value=(True, "test")),
            patch.object(server, "db_acquire_scope", _fake_db_scope),
            patch.object(server, "_get_user_language_pair_for_webapp_request", return_value=("ru", "de", {}, "test")),
            patch.object(server, "_log_flow_observation"),
            patch.object(server, "summarize_db_acquire_events", return_value={}),
            patch.object(server, "_estimate_json_payload_size_bytes", return_value=100),
            patch.object(server, "_billing_log_openai_usage"),
        ]
        if patch_billing_guard:
            base_patches.append(patch.object(server, "_apply_billing_guard", return_value=(None, None)))
        with ExitStack() as stack:
            for patcher in base_patches + list(extra_patches):
                stack.enter_context(patcher)
            return self.client.post(
                "/api/webapp/youtube/translate",
                json={
                    "initData": "signed",
                    "videoId": "video-123",
                    "start_index": 0,
                    "lines": lines if lines is not None else ["Hallo"],
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
                    patch.object(server, "resolve_entitlement", return_value={"effective_mode": "free"}),
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
             patch.object(server, "resolve_entitlement", return_value={"effective_mode": "free"}), \
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

    def test_free_transcript_payload_excludes_cached_ru_translations(self):
        cached = {
            "items": [{"text": "Shared", "start": 0, "duration": 1}],
            "language": "de",
            "is_generated": False,
            "translations": {"ru:0": "Привет"},
            "source": "db",
        }
        with patch.object(server, "_fetch_youtube_transcript") as fetch_mock, \
             patch.object(server, "upsert_youtube_transcript_cache") as upsert_mock:
            response = self._post_transcript(
                55,
                [
                    patch.object(server, "_load_cached_youtube_transcript_data", return_value=(cached, "db_cache", 1)),
                    patch.object(server, "resolve_entitlement", return_value={"effective_mode": "free"}),
                ],
            )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["items"], cached["items"])
        self.assertEqual(payload["translations"], {})
        fetch_mock.assert_not_called()
        upsert_mock.assert_not_called()

    def test_pro_transcript_payload_includes_cached_ru_translations(self):
        cached = {
            "items": [{"text": "Shared", "start": 0, "duration": 1}],
            "language": "de",
            "is_generated": False,
            "translations": {"ru:0": "Привет"},
            "source": "db",
        }
        response = self._post_transcript(
            77,
            [
                patch.object(server, "_load_cached_youtube_transcript_data", return_value=(cached, "db_cache", 1)),
                patch.object(server, "resolve_entitlement", return_value={"effective_mode": "pro"}),
            ],
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["translations"], {"0": "Привет"})

    def test_admin_transcript_payload_includes_cached_ru_translations(self):
        cached = {
            "items": [{"text": "Shared", "start": 0, "duration": 1}],
            "language": "de",
            "is_generated": False,
            "translations": {"ru:0": "Привет"},
            "source": "db",
        }
        with patch.object(server, "resolve_entitlement", return_value={"effective_mode": "free"}) as entitlement_mock:
            response = self._post_transcript(
                117649764,
                [
                    patch.object(server, "_load_cached_youtube_transcript_data", return_value=(cached, "db_cache", 1)),
                ],
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["translations"], {"0": "Привет"})
        entitlement_mock.assert_not_called()

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

    def test_admin_can_manually_save_transcript(self):
        with patch.object(server, "upsert_youtube_transcript_cache") as upsert_mock, \
             patch.object(server, "_yt_cache_put") as memory_cache_mock:
            response = self._post_manual_transcript(117649764, [])

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json(), {"ok": True})
        upsert_mock.assert_called_once()
        memory_cache_mock.assert_called_once()

    def test_admin_can_overwrite_existing_manual_transcript(self):
        with patch.object(server, "upsert_youtube_transcript_cache") as upsert_mock, \
             patch.object(server, "_yt_cache_put") as memory_cache_mock:
            response = self._post_manual_transcript(117649764, [])

        self.assertEqual(response.status_code, 200)
        upsert_mock.assert_called_once()
        args = upsert_mock.call_args.args
        self.assertEqual(args[0], "video-123")
        self.assertEqual(args[1][0]["text"], "Hallo")
        self.assertEqual(args[2], "de")
        self.assertFalse(args[3])
        memory_cache_mock.assert_called_once()

    def test_non_admin_cannot_manually_create_transcript(self):
        with patch.object(server, "upsert_youtube_transcript_cache") as upsert_mock, \
             patch.object(server, "_yt_cache_put") as memory_cache_mock:
            response = self._post_manual_transcript(55, [])

        self.assertEqual(response.status_code, 403)
        self.assertEqual(response.get_json()["error_code"], "youtube_manual_transcript_admin_required")
        upsert_mock.assert_not_called()
        memory_cache_mock.assert_not_called()

    def test_non_admin_cannot_manually_overwrite_transcript(self):
        with patch.object(server, "upsert_youtube_transcript_cache") as upsert_mock, \
             patch.object(server, "_yt_cache_put") as memory_cache_mock:
            response = self._post_manual_transcript(55, [])

        self.assertEqual(response.status_code, 403)
        self.assertEqual(response.get_json()["error_code"], "youtube_manual_transcript_admin_required")
        upsert_mock.assert_not_called()
        memory_cache_mock.assert_not_called()

    def test_movies_catalog_read_behavior_remains_unchanged(self):
        class FakeCursor:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def execute(self, *_args, **_kwargs):
                return None

            def fetchall(self):
                return [("video-123", "de", False, None, 2)]

        class FakeConnection:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def cursor(self):
                return FakeCursor()

        with patch.object(server, "WEBAPP_SINGLE_INSTANCE_GUARD_ENABLED", False), \
             patch.object(server, "_telegram_hash_is_valid", return_value=True), \
             patch.object(server, "_parse_telegram_init_data", return_value={"user": {"id": 55}}), \
             patch.object(server, "_resolve_webapp_user_allowed", return_value=(True, "test")), \
             patch.object(server, "_get_user_language_pair", return_value=("ru", "de", {})), \
             patch.object(server, "db_acquire_scope", _fake_db_scope), \
             patch.object(server, "get_db_connection_context", return_value=FakeConnection()), \
             patch.object(server, "_get_youtube_oembed", return_value={"title": "Video", "author_name": "Author", "thumbnail_url": "thumb"}), \
             patch.object(server, "_log_flow_observation"), \
             patch.object(server, "_estimate_json_payload_size_bytes", return_value=100), \
             patch.object(server, "upsert_youtube_transcript_cache") as upsert_mock:
            response = self.client.post(
                "/api/webapp/youtube/catalog",
                json={"initData": "signed", "limit": 60},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["items"][0]["video_id"], "video-123")
        upsert_mock.assert_not_called()

    def test_free_cannot_read_cached_ru_translations(self):
        cached = {"translations": {"ru:0": "Привет"}, "language": "de"}
        with patch.object(server, "get_youtube_transcript_cache", return_value=cached) as cache_mock, \
             patch.object(server, "resolve_entitlement", return_value={"effective_mode": "free"}), \
             patch.object(server, "run_translate_subtitles_ru", new=AsyncMock()) as translate_mock, \
             patch.object(server, "upsert_youtube_translations") as upsert_mock:
            response = self._post_translate(55, [])

        self.assertEqual(response.status_code, 403)
        self.assertEqual(response.get_json()["error_code"], "youtube_translation_pro_required")
        cache_mock.assert_not_called()
        translate_mock.assert_not_called()
        upsert_mock.assert_not_called()

    def test_free_cached_ru_translation_returns_pro_required_before_cost_cap(self):
        cached = {"translations": {"ru:0": "Привет"}, "language": "de"}
        with patch.object(server, "get_youtube_transcript_cache", return_value=cached), \
             patch.object(server, "resolve_entitlement", return_value={"effective_mode": "free"}), \
             patch.object(server, "enforce_daily_cost_cap", return_value={"error": "cap"}), \
             patch.object(server, "run_translate_subtitles_ru", new=AsyncMock()) as translate_mock, \
             patch.object(server, "upsert_youtube_translations") as upsert_mock:
            response = self._post_translate(55, [], patch_billing_guard=False)

        self.assertEqual(response.status_code, 403)
        self.assertEqual(response.get_json()["error_code"], "youtube_translation_pro_required")
        translate_mock.assert_not_called()
        upsert_mock.assert_not_called()

    def test_free_cannot_generate_missing_ru_translations(self):
        cached = {"translations": {}, "language": "de"}
        with patch.object(server, "get_youtube_transcript_cache", return_value=cached), \
             patch.object(server, "resolve_entitlement", return_value={"effective_mode": "free"}), \
             patch.object(server, "run_translate_subtitles_ru", new=AsyncMock()) as translate_mock, \
             patch.object(server, "upsert_youtube_translations") as upsert_mock:
            response = self._post_translate(55, [])

        self.assertEqual(response.status_code, 403)
        self.assertEqual(response.get_json()["error_code"], "youtube_translation_pro_required")
        translate_mock.assert_not_called()
        upsert_mock.assert_not_called()

    def test_free_missing_ru_translation_does_not_trigger_openai(self):
        cached = {"translations": {}, "language": "de"}
        with patch.object(server, "get_youtube_transcript_cache", return_value=cached), \
             patch.object(server, "resolve_entitlement", return_value={"effective_mode": "free"}), \
             patch.object(server, "run_translate_subtitles_ru", new=AsyncMock()) as translate_mock:
            response = self._post_translate(55, [])

        self.assertEqual(response.status_code, 403)
        translate_mock.assert_not_called()

    def test_free_missing_ru_translation_does_not_write_translations(self):
        cached = {"translations": {}, "language": "de"}
        with patch.object(server, "get_youtube_transcript_cache", return_value=cached), \
             patch.object(server, "resolve_entitlement", return_value={"effective_mode": "free"}), \
             patch.object(server, "upsert_youtube_translations") as upsert_mock:
            response = self._post_translate(55, [])

        self.assertEqual(response.status_code, 403)
        upsert_mock.assert_not_called()

    def test_pro_can_generate_missing_ru_translations(self):
        cached = {"translations": {}, "language": "de"}
        with patch.object(server, "get_youtube_transcript_cache", return_value=cached), \
             patch.object(server, "resolve_entitlement", return_value={"effective_mode": "pro"}), \
             patch.object(server, "run_translate_subtitles_ru", new=AsyncMock(return_value=["Привет"])) as translate_mock, \
             patch.object(server, "get_last_llm_usage", return_value={"total_tokens": 12}), \
             patch.object(server, "upsert_youtube_translations") as upsert_mock:
            response = self._post_translate(77, [])

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["translations"], ["Привет"])
        translate_mock.assert_called_once_with(["Hallo"])
        upsert_mock.assert_called_once()
        self.assertEqual(upsert_mock.call_args.args[0], "video-123")
        self.assertEqual(upsert_mock.call_args.args[1]["ru:0"], "Привет")

    def test_pro_can_reuse_cached_ru_translations(self):
        cached = {"translations": {"ru:0": "Привет"}, "language": "de"}
        with patch.object(server, "get_youtube_transcript_cache", return_value=cached), \
             patch.object(server, "resolve_entitlement", return_value={"effective_mode": "pro"}), \
             patch.object(server, "run_translate_subtitles_ru", new=AsyncMock()) as translate_mock, \
             patch.object(server, "upsert_youtube_translations") as upsert_mock:
            response = self._post_translate(77, [])

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["translations"], ["Привет"])
        translate_mock.assert_not_called()
        upsert_mock.assert_not_called()

    def test_admin_can_read_cached_ru_translations(self):
        cached = {"translations": {"ru:0": "Привет"}, "language": "de"}
        with patch.object(server, "get_youtube_transcript_cache", return_value=cached), \
             patch.object(server, "resolve_entitlement", return_value={"effective_mode": "free"}) as entitlement_mock, \
             patch.object(server, "run_translate_subtitles_ru", new=AsyncMock()) as translate_mock, \
             patch.object(server, "upsert_youtube_translations") as upsert_mock:
            response = self._post_translate(117649764, [])

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["translations"], ["Привет"])
        entitlement_mock.assert_not_called()
        translate_mock.assert_not_called()
        upsert_mock.assert_not_called()

    def test_admin_can_generate_missing_ru_translations(self):
        cached = {"translations": {}, "language": "de"}
        with patch.object(server, "get_youtube_transcript_cache", return_value=cached), \
             patch.object(server, "resolve_entitlement", return_value={"effective_mode": "free"}) as entitlement_mock, \
             patch.object(server, "run_translate_subtitles_ru", new=AsyncMock(return_value=["Привет"])) as translate_mock, \
             patch.object(server, "get_last_llm_usage", return_value={"total_tokens": 12}), \
             patch.object(server, "upsert_youtube_translations") as upsert_mock:
            response = self._post_translate(117649764, [])

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["translations"], ["Привет"])
        entitlement_mock.assert_not_called()
        translate_mock.assert_called_once_with(["Hallo"])
        upsert_mock.assert_called_once()

    def test_existing_translation_cache_reuse_remains_global_by_video_id(self):
        cached = {"translations": {"ru:0": "Привет"}, "language": "de"}
        with patch.object(server, "get_youtube_transcript_cache", return_value=cached) as cache_mock, \
             patch.object(server, "resolve_entitlement", return_value={"effective_mode": "pro"}), \
             patch.object(server, "run_translate_subtitles_ru", new=AsyncMock()) as translate_mock, \
             patch.object(server, "upsert_youtube_translations") as upsert_mock:
            response = self._post_translate(88, [])

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["translations"], ["Привет"])
        cache_mock.assert_called_with("video-123")
        translate_mock.assert_not_called()
        upsert_mock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
