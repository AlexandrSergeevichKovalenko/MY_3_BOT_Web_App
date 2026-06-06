from contextlib import ExitStack
import unittest
from unittest.mock import patch

import bot_3


class BotDictionarySaveFreeLimitTests(unittest.TestCase):
    def _payload(self, *, flow: str = "dictionary_select") -> dict:
        return {
            "user_id": 77,
            "source_lang": "de",
            "target_lang": "ru",
            "lookup": {
                "word_source": "Haus",
                "word_target": "дом",
                "source_lang": "de",
                "target_lang": "ru",
                "origin_meta": {"flow": flow},
            },
        }

    def _chosen(self) -> dict:
        return {"source": "Haus", "target": "дом"}

    def _base_patches(self, *, mode="free", existing_entry_id=None, usage=0.0, save_result=(123, True), save_error=None):
        stack = ExitStack()
        stack.enter_context(
            patch.object(bot_3, "resolve_entitlement", return_value={"effective_mode": mode, "plan_code": mode})
        )
        existing_mock = stack.enter_context(
            patch.object(bot_3, "get_existing_user_dictionary_entry_id_for_save", return_value=existing_entry_id)
        )
        limit_meta_mock = stack.enter_context(
            patch.object(bot_3, "get_free_feature_limit_metadata", return_value={"free_limit": 20})
        )
        usage_mock = stack.enter_context(
            patch.object(bot_3, "get_free_feature_usage_today", return_value=usage)
        )
        increment_mock = stack.enter_context(patch.object(bot_3, "increment_free_feature_usage"))
        stack.enter_context(patch.object(bot_3, "_resolve_private_dictionary_save_folder", return_value={"folder_id": 5}))
        if save_error is None:
            save_mock = stack.enter_context(
                patch.object(bot_3, "save_webapp_dictionary_query_returning_id_with_inserted", return_value=save_result)
            )
        else:
            save_mock = stack.enter_context(
                patch.object(bot_3, "save_webapp_dictionary_query_returning_id_with_inserted", side_effect=save_error)
            )
        return stack, {
            "existing": existing_mock,
            "limit_meta": limit_meta_mock,
            "usage": usage_mock,
            "increment": increment_mock,
            "save": save_mock,
        }

    def test_free_telegram_new_save_increments_dictionary_save_usage(self):
        stack, mocks = self._base_patches(mode="free", existing_entry_id=None, usage=0.0, save_result=(123, True))
        with stack:
            ok, msg, entry_id, _already_tagged = bot_3._save_dictionary_option_for_user(
                self._payload(),
                self._chosen(),
                user_id=77,
            )

        self.assertTrue(ok, msg)
        self.assertEqual(entry_id, 123)
        mocks["usage"].assert_called_once()
        mocks["increment"].assert_called_once()
        self.assertEqual(mocks["increment"].call_args.kwargs["feature_key"], "dictionary_lookup_save_daily")

    def test_free_duplicate_telegram_save_does_not_increment(self):
        stack, mocks = self._base_patches(mode="free", existing_entry_id=123, usage=20.0, save_result=(123, False))
        with stack:
            ok, msg, entry_id, _already_tagged = bot_3._save_dictionary_option_for_user(
                self._payload(),
                self._chosen(),
                user_id=77,
            )

        self.assertTrue(ok, msg)
        self.assertEqual(entry_id, 123)
        mocks["usage"].assert_not_called()
        mocks["increment"].assert_not_called()

    def test_free_twenty_first_telegram_origin_new_save_is_blocked(self):
        stack, mocks = self._base_patches(mode="free", existing_entry_id=None, usage=20.0, save_result=(123, True))
        with stack:
            ok, msg, entry_id, _already_tagged = bot_3._save_dictionary_option_for_user(
                self._payload(),
                self._chosen(),
                user_id=77,
            )

        self.assertFalse(ok)
        self.assertEqual(entry_id, 0)
        self.assertIn("лимит сохранения слов", msg)
        mocks["save"].assert_not_called()
        mocks["increment"].assert_not_called()

    def test_failed_telegram_save_does_not_increment(self):
        stack, mocks = self._base_patches(
            mode="free",
            existing_entry_id=None,
            usage=0.0,
            save_error=RuntimeError("insert failed"),
        )
        with stack:
            ok, msg, entry_id, _already_tagged = bot_3._save_dictionary_option_for_user(
                self._payload(),
                self._chosen(),
                user_id=77,
            )

        self.assertFalse(ok)
        self.assertEqual(entry_id, 0)
        self.assertIn("Ошибка сохранения", msg)
        mocks["increment"].assert_not_called()

    def test_blocked_telegram_save_does_not_reach_insert_or_downstream_side_effects(self):
        stack, mocks = self._base_patches(mode="free", existing_entry_id=None, usage=20.0, save_result=(123, True))
        with stack:
            ok, _msg, entry_id, _already_tagged = bot_3._save_dictionary_option_for_user(
                self._payload(),
                self._chosen(),
                user_id=77,
            )

        self.assertFalse(ok)
        self.assertEqual(entry_id, 0)
        mocks["save"].assert_not_called()

    def test_shortcut_origin_save_path_uses_same_limit(self):
        stack, mocks = self._base_patches(mode="free", existing_entry_id=None, usage=20.0, save_result=(123, True))
        with stack:
            ok, msg, _entry_id, _already_tagged = bot_3._save_dictionary_option_for_user(
                self._payload(flow="shortcut_delivery"),
                self._chosen(),
                user_id=77,
            )

        self.assertFalse(ok)
        self.assertIn("Лимит обновится завтра", msg)
        mocks["save"].assert_not_called()

    def test_forwarded_message_origin_save_path_uses_same_limit(self):
        stack, mocks = self._base_patches(mode="free", existing_entry_id=None, usage=20.0, save_result=(123, True))
        with stack:
            ok, msg, _entry_id, _already_tagged = bot_3._save_dictionary_option_for_user(
                self._payload(flow="forwarded_message"),
                self._chosen(),
                user_id=77,
            )

        self.assertFalse(ok)
        self.assertIn("до 20 новых слов", msg)
        mocks["save"].assert_not_called()

    def test_pro_telegram_save_is_not_blocked(self):
        stack, mocks = self._base_patches(mode="pro", existing_entry_id=None, usage=20.0, save_result=(123, True))
        with stack:
            ok, msg, entry_id, _already_tagged = bot_3._save_dictionary_option_for_user(
                self._payload(),
                self._chosen(),
                user_id=77,
            )

        self.assertTrue(ok, msg)
        self.assertEqual(entry_id, 123)
        mocks["usage"].assert_not_called()
        mocks["increment"].assert_not_called()
        mocks["save"].assert_called_once()


if __name__ == "__main__":
    unittest.main()
