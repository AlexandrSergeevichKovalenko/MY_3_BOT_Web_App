from contextlib import ExitStack
import unittest
from unittest.mock import patch

import backend.backend_server as server


class DictionarySaveFreeLimitTests(unittest.TestCase):
    def setUp(self):
        self.client = server.app.test_client()

    def _post_save(self):
        return self.client.post(
            "/api/webapp/dictionary/save",
            json={
                "initData": "signed",
                "source_text": "Haus",
                "target_text": "дом",
                "source_lang": "de",
                "target_lang": "ru",
                "response_json": {
                    "source_text": "Haus",
                    "target_text": "дом",
                    "source_lang": "de",
                    "target_lang": "ru",
                },
            },
        )

    def _base_patches(self, *, mode="free", existing_entry_id=None, usage=0.0, save_result=(123, True), save_error=None):
        stack = ExitStack()
        stack.enter_context(patch.object(server, "WEBAPP_SINGLE_INSTANCE_GUARD_ENABLED", False))
        stack.enter_context(patch.object(server, "_telegram_hash_is_valid", return_value=True))
        stack.enter_context(
            patch.object(
                server,
                "_parse_telegram_init_data",
                return_value={"user": {"id": 77, "first_name": "User"}},
            )
        )
        stack.enter_context(patch.object(server, "_resolve_webapp_user_allowed", return_value=(True, "test")))
        stack.enter_context(patch.object(server, "_get_user_language_pair", return_value=("de", "ru", None)))
        stack.enter_context(patch.object(server, "get_or_create_dictionary_folder", return_value={"id": 5}))
        stack.enter_context(
            patch.object(server, "resolve_entitlement", return_value={"effective_mode": mode, "plan_code": mode})
        )
        existing_mock = stack.enter_context(
            patch.object(server, "get_existing_user_dictionary_entry_id_for_save", return_value=existing_entry_id)
        )
        limit_meta_mock = stack.enter_context(
            patch.object(server, "get_free_feature_limit_metadata", return_value={"free_limit": 20})
        )
        usage_mock = stack.enter_context(
            patch.object(server, "get_free_feature_usage_today", return_value=usage)
        )
        increment_mock = stack.enter_context(patch.object(server, "increment_free_feature_usage"))
        if save_error is None:
            save_mock = stack.enter_context(
                patch.object(server, "_save_dictionary_entry_with_inserted_schema_retry", return_value=save_result)
            )
        else:
            save_mock = stack.enter_context(
                patch.object(server, "_save_dictionary_entry_with_inserted_schema_retry", side_effect=save_error)
            )
        enrichment_mock = stack.enter_context(patch.object(server, "_start_saved_dictionary_entry_enrichment"))
        tts_mock = stack.enter_context(patch.object(server, "_enqueue_dictionary_entry_tts_prewarm"))
        stack.enter_context(patch.object(server, "_build_language_pair_payload", return_value={"source": "de", "target": "ru"}))
        return stack, {
            "existing": existing_mock,
            "limit_meta": limit_meta_mock,
            "usage": usage_mock,
            "increment": increment_mock,
            "save": save_mock,
            "enrichment": enrichment_mock,
            "tts": tts_mock,
        }

    def test_free_new_save_increments_dictionary_save_usage(self):
        stack, mocks = self._base_patches(mode="free", existing_entry_id=None, usage=0.0, save_result=(123, True))
        with stack:
            response = self._post_save()

        self.assertEqual(response.status_code, 200)
        mocks["usage"].assert_called_once()
        mocks["increment"].assert_called_once()
        self.assertEqual(mocks["increment"].call_args.kwargs["feature_key"], "dictionary_lookup_save_daily")
        mocks["enrichment"].assert_called_once()
        mocks["tts"].assert_called_once()

    def test_free_duplicate_save_does_not_increment_usage(self):
        stack, mocks = self._base_patches(mode="free", existing_entry_id=123, usage=20.0, save_result=(123, False))
        with stack:
            response = self._post_save()

        self.assertEqual(response.status_code, 200)
        mocks["usage"].assert_not_called()
        mocks["increment"].assert_not_called()

    def test_free_twenty_first_new_save_is_blocked(self):
        stack, mocks = self._base_patches(mode="free", existing_entry_id=None, usage=20.0, save_result=(123, True))
        with stack:
            response = self._post_save()

        self.assertEqual(response.status_code, 429)
        payload = response.get_json()
        self.assertEqual(payload["error"], "free_limit_exceeded")
        self.assertEqual(payload["feature"], "dictionary_lookup_save_daily")
        mocks["save"].assert_not_called()

    def test_failed_save_does_not_increment_usage(self):
        stack, mocks = self._base_patches(
            mode="free",
            existing_entry_id=None,
            usage=0.0,
            save_error=RuntimeError("insert failed"),
        )
        with stack:
            response = self._post_save()

        self.assertEqual(response.status_code, 500)
        mocks["increment"].assert_not_called()

    def test_blocked_save_does_not_trigger_enrichment_or_tts_prewarm(self):
        stack, mocks = self._base_patches(mode="free", existing_entry_id=None, usage=20.0, save_result=(123, True))
        with stack:
            response = self._post_save()

        self.assertEqual(response.status_code, 429)
        mocks["enrichment"].assert_not_called()
        mocks["tts"].assert_not_called()

    def test_pro_save_is_not_blocked_by_free_limit(self):
        stack, mocks = self._base_patches(mode="pro", existing_entry_id=None, usage=20.0, save_result=(123, True))
        with stack:
            response = self._post_save()

        self.assertEqual(response.status_code, 200)
        mocks["usage"].assert_not_called()
        mocks["increment"].assert_not_called()
        mocks["save"].assert_called_once()


if __name__ == "__main__":
    unittest.main()
