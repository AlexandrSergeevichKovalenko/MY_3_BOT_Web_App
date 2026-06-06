from contextlib import ExitStack
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

import backend.backend_server as server
import bot_3


class DictionaryLookupFreeLimitWebappTests(unittest.TestCase):
    def setUp(self):
        self.client = server.app.test_client()

    def _post_lookup(self):
        return self.client.post(
            "/api/webapp/dictionary",
            json={"initData": "signed", "word": "Haus", "lookup_lang": "de"},
        )

    def _base_patches(self, *, mode="free", usage=0.0):
        stack = ExitStack()
        stack.enter_context(patch.object(server, "WEBAPP_SINGLE_INSTANCE_GUARD_ENABLED", False))
        stack.enter_context(patch.object(server, "_telegram_hash_is_valid", return_value=True))
        stack.enter_context(
            patch.object(server, "_parse_telegram_init_data", return_value={"user": {"id": 77}})
        )
        stack.enter_context(patch.object(server, "_resolve_webapp_user_allowed", return_value=(True, "test")))
        stack.enter_context(patch.object(server, "enforce_daily_cost_cap", return_value=None))
        stack.enter_context(patch.object(server, "_get_user_language_pair", return_value=("de", "ru", None)))
        stack.enter_context(patch.object(server, "_build_language_pair_payload", return_value={"source": "de", "target": "ru"}))
        stack.enter_context(patch.object(server, "resolve_entitlement", return_value={"effective_mode": mode, "plan_code": mode}))
        stack.enter_context(patch.object(server, "get_free_feature_limit_metadata", return_value={"free_limit": 30}))
        usage_mock = stack.enter_context(patch.object(server, "get_free_feature_usage_today", return_value=usage))
        increment_mock = stack.enter_context(patch.object(server, "increment_free_feature_usage"))
        stack.enter_context(patch.object(server, "DICTIONARY_COALESCE_ENABLED", False))
        stack.enter_context(patch.object(server, "_get_dictionary_enrichment_job_for_cache_keys", return_value=None))
        stack.enter_context(patch.object(server, "_billing_log_event_safe"))
        stack.enter_context(patch.object(server, "_billing_log_openai_usage"))
        return stack, {"usage": usage_mock, "increment": increment_mock}

    def test_mini_app_memory_cache_hit_does_not_consume_lookup_limit(self):
        stack, mocks = self._base_patches(mode="free", usage=30.0)
        cached_payload = {"item": {"source_text": "Haus", "target_text": "дом"}, "direction": "de-ru"}
        with stack, \
             patch.object(server, "_get_cached_dictionary_lookup_with_tier", return_value=(cached_payload, "memory")), \
             patch.object(server, "_run_dictionary_core_lookup_sync") as core_mock:
            response = self._post_lookup()

        self.assertEqual(response.status_code, 200)
        mocks["usage"].assert_not_called()
        mocks["increment"].assert_not_called()
        core_mock.assert_not_called()

    def test_mini_app_db_cache_hit_does_not_consume_lookup_limit(self):
        stack, mocks = self._base_patches(mode="free", usage=30.0)
        cached_payload = {"item": {"source_text": "laufen", "target_text": "бежать"}, "direction": "de-ru"}
        with stack, \
             patch.object(server, "_get_cached_dictionary_lookup_with_tier", return_value=(cached_payload, "db")), \
             patch.object(server, "_run_dictionary_core_lookup_sync") as core_mock:
            response = self._post_lookup()

        self.assertEqual(response.status_code, 200)
        mocks["usage"].assert_not_called()
        mocks["increment"].assert_not_called()
        core_mock.assert_not_called()

    def test_mini_app_openai_miss_consumes_lookup_limit(self):
        stack, mocks = self._base_patches(mode="free", usage=0.0)
        core_payload = {
            "item": {"source_text": "Haus", "target_text": "дом"},
            "direction": "de-ru",
            "raw": {},
            "usage": {},
            "gateway_path": "test",
        }
        with stack, \
             patch.object(server, "_get_cached_dictionary_lookup_with_tier", return_value=(None, "none")), \
             patch.object(server, "_run_dictionary_core_lookup_sync", return_value=core_payload), \
             patch.object(server, "_create_dictionary_enrichment_job"), \
             patch.object(server, "_start_dictionary_enrichment_runner"), \
             patch.object(server, "_set_cached_dictionary_lookup_all"):
            response = self._post_lookup()

        self.assertEqual(response.status_code, 200)
        mocks["usage"].assert_called_once()
        mocks["increment"].assert_called_once()
        self.assertEqual(mocks["increment"].call_args.kwargs["feature_key"], "dictionary_lookup_daily")

    def test_free_thirty_first_mini_app_lookup_is_blocked_before_openai(self):
        stack, mocks = self._base_patches(mode="free", usage=30.0)
        with stack, \
             patch.object(server, "_get_cached_dictionary_lookup_with_tier", return_value=(None, "none")), \
             patch.object(server, "_run_dictionary_core_lookup_sync") as core_mock:
            response = self._post_lookup()

        self.assertEqual(response.status_code, 429)
        payload = response.get_json()
        self.assertEqual(payload["feature"], "dictionary_lookup_daily")
        core_mock.assert_not_called()
        mocks["increment"].assert_not_called()

    def test_pro_mini_app_lookup_is_not_blocked_by_free_limit(self):
        stack, mocks = self._base_patches(mode="pro", usage=30.0)
        core_payload = {
            "item": {"source_text": "Haus", "target_text": "дом"},
            "direction": "de-ru",
            "raw": {},
            "usage": {},
            "gateway_path": "test",
        }
        with stack, \
             patch.object(server, "_get_cached_dictionary_lookup_with_tier", return_value=(None, "none")), \
             patch.object(server, "_run_dictionary_core_lookup_sync", return_value=core_payload), \
             patch.object(server, "_create_dictionary_enrichment_job"), \
             patch.object(server, "_start_dictionary_enrichment_runner"), \
             patch.object(server, "_set_cached_dictionary_lookup_all"):
            response = self._post_lookup()

        self.assertEqual(response.status_code, 200)
        mocks["usage"].assert_not_called()
        mocks["increment"].assert_not_called()


class DictionaryLookupFreeLimitTelegramTests(unittest.TestCase):
    def _patch_lookup_limit(self, *, mode="free", usage=0.0):
        stack = ExitStack()
        stack.enter_context(patch.object(bot_3, "resolve_entitlement", return_value={"effective_mode": mode, "plan_code": mode}))
        stack.enter_context(patch.object(bot_3, "get_free_feature_limit_metadata", return_value={"free_limit": 30}))
        usage_mock = stack.enter_context(patch.object(bot_3, "get_free_feature_usage_today", return_value=usage))
        increment_mock = stack.enter_context(patch.object(bot_3, "increment_free_feature_usage"))
        stack.enter_context(patch.object(bot_3, "_resolve_private_dictionary_save_folder", return_value={"folder_id": None, "name": "GENERAL", "icon": "📁"}))
        stack.enter_context(patch.object(bot_3, "_store_pending_dictionary_card", return_value="card-1"))
        stack.enter_context(patch.object(bot_3, "_store_pending_quiz_question_request", return_value="question-1"))
        stack.enter_context(patch.object(bot_3, "_store_pending_dictionary_save_options", return_value="option-1"))
        return stack, {"usage": usage_mock, "increment": increment_mock}

    def _lookup_payload(self):
        return {
            "word_source": "Haus",
            "word_target": "дом",
            "source_lang": "de",
            "target_lang": "ru",
            "save_worthy_options": [{"source": "Haus", "target": "дом"}],
        }

    def test_telegram_quick_lookup_consumes_usage(self):
        stack, mocks = self._patch_lookup_limit(mode="free", usage=0.0)
        with stack, \
             patch.object(bot_3, "_run_dictionary_lookup_for_pair", new=AsyncMock(return_value=self._lookup_payload())), \
             patch.object(bot_3, "_generate_dictionary_save_options", new=AsyncMock(side_effect=AssertionError("full path"))):
            bot_3.asyncio.run(
                bot_3._prepare_dictionary_lookup_response(
                    user_id=77,
                    lookup_input="Haus",
                    source_lang="de",
                    target_lang="ru",
                    fast_options=True,
                    request_key="direct-quick",
                    lookup_origin="telegram_direct",
                )
            )

        mocks["increment"].assert_called_once()
        self.assertEqual(mocks["increment"].call_args.kwargs["feature_key"], "dictionary_lookup_daily")

    def test_telegram_full_lookup_consumes_usage_once(self):
        stack, mocks = self._patch_lookup_limit(mode="free", usage=0.0)
        with stack, \
             patch.object(bot_3, "_run_dictionary_lookup_for_pair", new=AsyncMock(return_value=self._lookup_payload())), \
             patch.object(bot_3, "_generate_dictionary_save_options", new=AsyncMock(return_value=[{"source": "Haus", "target": "дом"}])):
            bot_3.asyncio.run(
                bot_3._prepare_dictionary_lookup_response(
                    user_id=77,
                    lookup_input="Haus",
                    source_lang="de",
                    target_lang="ru",
                    fast_options=False,
                    request_key="direct-full",
                    lookup_origin="telegram_direct",
                )
            )

        mocks["increment"].assert_called_once()

    def test_free_thirty_first_telegram_lookup_is_blocked_before_openai(self):
        stack, mocks = self._patch_lookup_limit(mode="free", usage=30.0)
        with stack, \
             patch.object(bot_3, "_run_dictionary_lookup_for_pair", new=AsyncMock(return_value=self._lookup_payload())) as lookup_mock:
            with self.assertRaises(bot_3.DictionaryLookupDailyLimitExceeded):
                bot_3.asyncio.run(
                    bot_3._prepare_dictionary_lookup_response(
                        user_id=77,
                        lookup_input="Haus",
                        source_lang="de",
                        target_lang="ru",
                        fast_options=True,
                        request_key="blocked",
                        lookup_origin="telegram_direct",
                    )
                )

        lookup_mock.assert_not_awaited()
        mocks["increment"].assert_not_called()

    def test_pro_telegram_lookup_is_not_limited(self):
        stack, mocks = self._patch_lookup_limit(mode="pro", usage=30.0)
        with stack, \
             patch.object(bot_3, "_run_dictionary_lookup_for_pair", new=AsyncMock(return_value=self._lookup_payload())):
            bot_3.asyncio.run(
                bot_3._prepare_dictionary_lookup_response(
                    user_id=77,
                    lookup_input="Haus",
                    source_lang="de",
                    target_lang="ru",
                    fast_options=True,
                    request_key="pro",
                    lookup_origin="telegram_direct",
                )
            )

        mocks["usage"].assert_not_called()
        mocks["increment"].assert_not_called()

    def test_shortcut_lookup_click_consumes_usage_with_origin(self):
        stack, mocks = self._patch_lookup_limit(mode="free", usage=0.0)
        with stack, \
             patch.object(bot_3, "_run_dictionary_lookup_for_pair", new=AsyncMock(return_value=self._lookup_payload())):
            bot_3.asyncio.run(
                bot_3._prepare_dictionary_lookup_response(
                    user_id=77,
                    lookup_input="laufen",
                    source_lang="de",
                    target_lang="ru",
                    fast_options=True,
                    request_key="shortcut-click",
                    lookup_origin="shortcut_delivery",
                )
            )

        self.assertEqual(mocks["increment"].call_args.kwargs["metadata"]["origin"], "shortcut_delivery")

    def test_forwarded_lookup_click_consumes_usage_with_origin(self):
        stack, mocks = self._patch_lookup_limit(mode="free", usage=0.0)
        with stack, \
             patch.object(bot_3, "_run_dictionary_lookup_for_pair", new=AsyncMock(return_value=self._lookup_payload())):
            bot_3.asyncio.run(
                bot_3._prepare_dictionary_lookup_response(
                    user_id=77,
                    lookup_input="sich freuen",
                    source_lang="de",
                    target_lang="ru",
                    fast_options=True,
                    request_key="forwarded-click",
                    lookup_origin="forwarded_delivery",
                )
            )

        self.assertEqual(mocks["increment"].call_args.kwargs["metadata"]["origin"], "forwarded_delivery")

    def test_blocked_send_uses_friendly_telegram_message(self):
        class FakeMessage:
            def __init__(self):
                self.replies = []

            async def reply_text(self, text, **_kwargs):
                self.replies.append(text)
                return SimpleNamespace(chat_id=77, message_id=1)

        message = FakeMessage()
        with patch.object(bot_3, "_prepare_dictionary_lookup_response", new=AsyncMock(side_effect=bot_3.DictionaryLookupDailyLimitExceeded())):
            bot_3.asyncio.run(
                bot_3._send_dictionary_lookup_quick_result(
                    message=message,
                    context=SimpleNamespace(),
                    user_id=77,
                    lookup_input="Haus",
                    source_lang="de",
                    target_lang="ru",
                )
            )

        self.assertEqual(message.replies, [bot_3.DICTIONARY_LOOKUP_DAILY_LIMIT_MESSAGE])


if __name__ == "__main__":
    unittest.main()
