import json
import os
import unittest
from contextlib import contextmanager
from unittest.mock import patch

import backend.backend_server as server
import bot_3 as bot


class ShortcutLookupSplitTests(unittest.TestCase):
    def setUp(self):
        self.client = server.app.test_client()

    @staticmethod
    @contextmanager
    def _fake_db_scope(*_args, **_kwargs):
        yield []

    @staticmethod
    def _recording_db_context():
        class _Cursor:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def execute(self, *_args, **_kwargs):
                return None

            def fetchone(self):
                return None

        class _Connection:
            def __init__(self):
                self.committed = False
                self.rolled_back = False
                self.cursor_obj = _Cursor()

            def cursor(self):
                return self.cursor_obj

            def commit(self):
                self.committed = True

            def rollback(self):
                self.rolled_back = True

        conn = _Connection()

        @contextmanager
        def _context():
            try:
                yield conn
                conn.commit()
            except Exception:
                conn.rollback()
                raise

        return conn, _context

    def test_normalize_unit_text_cleans_pedagogical_grammar_noise(self):
        self.assertEqual(
            server._shortcut_normalize_unit_text("  erinnert... an  +Akkusativ "),
            "erinnert an + Akkusativ",
        )
        self.assertEqual(
            server._shortcut_normalize_unit_text("ist… ähnlich +  Dativ"),
            "ist ähnlich + Dativ",
        )

    def test_extract_blocks_normalizes_wrapper_quotes(self):
        raw = json.dumps(
            {
                "blocks": [
                    {"term": '"Hat mich gefreut!"', "content": '"Hat mich gefreut!"'},
                    {"term": "1. Man sieht sich!", "content": "1. Man sieht sich!"},
                ]
            },
            ensure_ascii=False,
        )

        blocks = server._shortcut_extract_blocks_from_json(raw, ' "Hat mich gefreut!" 1. Man sieht sich! ')

        self.assertEqual(
            blocks,
            [
                ("Hat mich gefreut!", "Hat mich gefreut!"),
                ("Man sieht sich!", "Man sieht sich!"),
            ],
        )

    def test_validate_coverage_allows_short_clean_units_from_long_noisy_text(self):
        original = (
            "Прощание. Финальные фразы — чтобы уйти красиво. "
            'Ich muss dann mal wieder. (Мне уже пора.) '
            "Klassischer, höflicher Abschluss eines Gesprächs. "
            "Hat mich gefreut! (Рад был пообщаться!)"
        )

        self.assertTrue(
            server._shortcut_validate_coverage(
                [
                    ("Ich muss dann mal wieder.", "Ich muss dann mal wieder."),
                    ("Hat mich gefreut!", "Hat mich gefreut!"),
                ],
                original,
            )
        )

    def test_shortcut_lookup_sends_one_clean_request_per_block(self):
        with patch.dict(os.environ, {"SHORTCUT_SECRET": "secret"}, clear=False), \
             patch.object(server, "is_telegram_user_allowed", return_value=True), \
             patch.object(server, "_check_free_daily_usage_limit", return_value=({
                 "effective_mode": "free",
                 "used": 0,
                 "limit": 20,
             }, None, None)), \
             patch.object(server, "can_enqueue_background_jobs", return_value=True), \
             patch.object(server, "_start_shortcut_lookup_enqueue_runner", return_value=("req123", 1, False, "queued", "job123")) as enqueue_mock:
            response = self.client.post(
                "/api/shortcut/lookup",
                json={"text": "noisy input", "user_id": 117649764},
                headers={"Authorization": "Bearer secret"},
            )

        payload = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertTrue(payload["accepted"])
        self.assertTrue(payload["queued"])
        self.assertFalse(payload["completed"])
        self.assertNotIn("saved_count", payload)
        enqueue_mock.assert_called_once()
        self.assertEqual(enqueue_mock.call_args.kwargs["source"], "shortcut")

    def test_shortcut_delivery_uses_deterministic_shortcut_request_keys(self):
        with patch.object(
                 server,
                 "_shortcut_split_blocks",
                 return_value=[
                     ("Hat mich gefreut!", "Hat mich gefreut!"),
                     ("Man sieht sich!", "Man sieht sich!"),
                 ],
             ), \
             patch.object(server, "_send_private_message") as send_mock, \
             patch.object(server, "time") as time_mock:
            time_mock.sleep.return_value = None
            sent_first = server._run_shortcut_lookup_delivery(
                user_id=117649764,
                text="noisy input",
                source="shortcut",
                ingest_key="ingestabc",
            )
            first_callbacks = [
                send_mock.call_args_list[idx].kwargs["reply_markup"]["inline_keyboard"][0][0]["callback_data"]
                for idx in range(send_mock.call_count)
            ]
            send_mock.reset_mock()
            sent_second = server._run_shortcut_lookup_delivery(
                user_id=117649764,
                text="noisy input",
                source="shortcut",
                ingest_key="ingestabc",
            )

        second_callbacks = [
            send_mock.call_args_list[idx].kwargs["reply_markup"]["inline_keyboard"][0][0]["callback_data"]
            for idx in range(send_mock.call_count)
        ]
        self.assertEqual(sent_first, 2)
        self.assertEqual(sent_second, 2)
        self.assertEqual(first_callbacks, second_callbacks)
        self.assertTrue(all(item.startswith("dictpair:sc") for item in first_callbacks))

    def test_shortcut_lookup_blocks_free_user_over_limit(self):
        limit_payload = {
            "ok": False,
            "error": "free_limit_exceeded",
            "feature": "shortcut_ingest_save_daily",
            "feature_title": "Shortcut сохранение слов",
            "limit": 20,
            "used": 20,
            "reset_at": "2026-05-29T00:00:00+02:00",
            "message": "limit",
        }
        with patch.dict(os.environ, {"SHORTCUT_SECRET": "secret"}, clear=False), \
             patch.object(server, "is_telegram_user_allowed", return_value=True), \
             patch.object(server, "_check_free_daily_usage_limit", return_value=(None, limit_payload, 429)), \
             patch.object(server, "_start_shortcut_lookup_enqueue_runner") as enqueue_mock:
            response = self.client.post(
                "/api/shortcut/lookup",
                json={"text": "noisy input", "user_id": 117649764},
                headers={"Authorization": "Bearer secret"},
            )

        self.assertEqual(response.status_code, 429)
        self.assertEqual(response.get_json()["feature"], "shortcut_ingest_save_daily")
        enqueue_mock.assert_not_called()

    def test_shortcut_lookup_paid_user_bypasses_precheck_limit(self):
        with patch.dict(os.environ, {"SHORTCUT_SECRET": "secret"}, clear=False), \
             patch.object(server, "is_telegram_user_allowed", return_value=True), \
             patch.object(server, "_check_free_daily_usage_limit", return_value=({
                 "effective_mode": "pro",
                 "skip_increment": True,
             }, None, None)), \
             patch.object(server, "can_enqueue_background_jobs", return_value=True), \
             patch.object(server, "_start_shortcut_lookup_enqueue_runner", return_value=("req123", 1, False, "queued", "job123")):
            response = self.client.post(
                "/api/shortcut/lookup",
                json={"text": "noisy input", "user_id": 117649764},
                headers={"Authorization": "Bearer secret"},
            )

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.get_json()["accepted"])

    def test_shortcut_duplicate_enqueue_response_does_not_claim_completion(self):
        with patch.dict(os.environ, {"SHORTCUT_SECRET": "secret"}, clear=False), \
             patch.object(server, "is_telegram_user_allowed", return_value=True), \
             patch.object(server, "_check_free_daily_usage_limit", return_value=({
                 "effective_mode": "free",
                 "used": 0,
                 "limit": 20,
             }, None, None)), \
             patch.object(server, "can_enqueue_background_jobs", return_value=True), \
             patch.object(server, "_start_shortcut_lookup_enqueue_runner", return_value=("req123", 1, True, "processing", "job123")) as enqueue_mock:
            response = self.client.post(
                "/api/shortcut/lookup",
                json={"text": "noisy input", "user_id": 117649764},
                headers={"Authorization": "Bearer secret"},
            )

        payload = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertTrue(payload["accepted"])
        self.assertFalse(payload["queued"])
        self.assertFalse(payload["completed"])
        self.assertTrue(payload["duplicate"])
        self.assertEqual(payload["status"], "processing")
        enqueue_mock.assert_called_once()

    def test_shortcut_pure_extraction_without_save_does_not_increment(self):
        with patch.object(
                 server,
                 "_shortcut_split_blocks",
                 return_value=[("Hat mich gefreut!", "Hat mich gefreut!")],
             ), \
             patch.object(server, "_send_private_message"), \
             patch.object(bot, "finish_free_usage_lifecycle_success_tx") as finish_mock:
            sent = server._run_shortcut_lookup_delivery(
                user_id=117649764,
                text="Hat mich gefreut!",
                source="shortcut",
                ingest_key="ingestabc",
            )

        self.assertEqual(sent, 1)
        finish_mock.assert_not_called()

    def _shortcut_payload(self):
        return {
            "source_lang": "de",
            "target_lang": "ru",
            "direction": "de-ru",
            "lookup": {"word_source": "Haus", "word_target": "дом"},
            "folder_id": 1,
            "origin_process": "shortcut_ingest",
            "origin_meta": {"source": "shortcut", "flow": "shortcut_ingest", "request_key": "scabc"},
        }

    def test_shortcut_new_saved_item_increments_usage(self):
        conn, db_context = self._recording_db_context()
        usage_state = {
            "feature": "shortcut_ingest_save_daily",
            "feature_title": "Shortcut сохранение слов",
            "effective_mode": "free",
            "operation_kind": "count_new_shortcut_ingest_item",
            "used": 0,
            "limit": 20,
            "skip_increment": False,
        }
        with patch.object(bot, "db_acquire_scope", self._fake_db_scope), \
             patch.object(bot, "get_db_connection_context", db_context), \
             patch.object(bot, "begin_free_usage_lifecycle_tx", return_value=(usage_state, None, None)), \
             patch.object(bot, "save_webapp_dictionary_query_returning_result_with_cursor", return_value={"entry_id": 321, "inserted": True}), \
             patch.object(bot, "finish_free_usage_lifecycle_success_tx") as finish_mock:
            ok, msg = bot._save_dictionary_option_for_user(
                payload=self._shortcut_payload(),
                chosen={"source": "Haus", "target": "дом"},
                user_id=117649764,
            )

        self.assertTrue(ok, msg)
        finish_mock.assert_called_once()
        self.assertTrue(conn.committed)
        self.assertFalse(conn.rolled_back)

    def test_shortcut_duplicate_saved_item_does_not_increment(self):
        conn, db_context = self._recording_db_context()
        usage_state = {
            "feature": "shortcut_ingest_save_daily",
            "feature_title": "Shortcut сохранение слов",
            "effective_mode": "free",
            "operation_kind": "count_new_shortcut_ingest_item",
            "used": 0,
            "limit": 20,
            "skip_increment": False,
        }
        with patch.object(bot, "db_acquire_scope", self._fake_db_scope), \
             patch.object(bot, "get_db_connection_context", db_context), \
             patch.object(bot, "begin_free_usage_lifecycle_tx", return_value=(usage_state, None, None)), \
             patch.object(bot, "save_webapp_dictionary_query_returning_result_with_cursor", return_value={"entry_id": 321, "inserted": False}), \
             patch.object(bot, "finish_free_usage_lifecycle_success_tx") as finish_mock:
            ok, msg = bot._save_dictionary_option_for_user(
                payload=self._shortcut_payload(),
                chosen={"source": "Haus", "target": "дом"},
                user_id=117649764,
            )

        self.assertTrue(ok, msg)
        finish_mock.assert_not_called()
        self.assertTrue(conn.committed)
        self.assertFalse(conn.rolled_back)

    def test_shortcut_paid_user_save_bypasses_usage_increment(self):
        conn, db_context = self._recording_db_context()
        usage_state = {
            "feature": "shortcut_ingest_save_daily",
            "feature_title": "Shortcut сохранение слов",
            "effective_mode": "pro",
            "operation_kind": "count_new_shortcut_ingest_item",
            "used": None,
            "limit": 20,
            "skip_increment": True,
        }
        with patch.object(bot, "db_acquire_scope", self._fake_db_scope), \
             patch.object(bot, "get_db_connection_context", db_context), \
             patch.object(bot, "begin_free_usage_lifecycle_tx", return_value=(usage_state, None, None)), \
             patch.object(bot, "save_webapp_dictionary_query_returning_result_with_cursor", return_value={"entry_id": 321, "inserted": True}), \
             patch.object(bot, "finish_free_usage_lifecycle_success_tx", wraps=bot.finish_free_usage_lifecycle_success_tx) as finish_mock:
            ok, msg = bot._save_dictionary_option_for_user(
                payload=self._shortcut_payload(),
                chosen={"source": "Haus", "target": "дом"},
                user_id=117649764,
            )

        self.assertTrue(ok, msg)
        finish_mock.assert_called_once()
        self.assertTrue(conn.committed)
        self.assertFalse(conn.rolled_back)

    def test_shortcut_repeated_same_idempotency_key_does_not_double_increment(self):
        self.assertEqual(
            server._shortcut_callback_request_key(
                user_id=117649764,
                ingest_key="same-ingest",
                lookup_text="Haus",
                index=0,
                source="shortcut",
            ),
            server._shortcut_callback_request_key(
                user_id=117649764,
                ingest_key="same-ingest",
                lookup_text="Haus",
                index=0,
                source="shortcut",
            ),
        )
        conn, db_context = self._recording_db_context()
        usage_state = {
            "feature": "shortcut_ingest_save_daily",
            "feature_title": "Shortcut сохранение слов",
            "effective_mode": "free",
            "operation_kind": "count_new_shortcut_ingest_item",
            "used": 0,
            "limit": 20,
            "skip_increment": False,
        }
        with patch.object(bot, "db_acquire_scope", self._fake_db_scope), \
             patch.object(bot, "get_db_connection_context", db_context), \
             patch.object(bot, "begin_free_usage_lifecycle_tx", return_value=(usage_state, None, None)), \
             patch.object(bot, "save_webapp_dictionary_query_returning_result_with_cursor", return_value={"entry_id": 321, "inserted": False}), \
             patch.object(bot, "finish_free_usage_lifecycle_success_tx") as finish_mock:
            ok, msg = bot._save_dictionary_option_for_user(
                payload=self._shortcut_payload(),
                chosen={"source": "Haus", "target": "дом"},
                user_id=117649764,
            )

        self.assertTrue(ok, msg)
        finish_mock.assert_not_called()

    def test_shortcut_failed_save_does_not_increment(self):
        conn, db_context = self._recording_db_context()
        usage_state = {
            "feature": "shortcut_ingest_save_daily",
            "feature_title": "Shortcut сохранение слов",
            "effective_mode": "free",
            "operation_kind": "count_new_shortcut_ingest_item",
            "used": 0,
            "limit": 20,
            "skip_increment": False,
        }
        with patch.object(bot, "db_acquire_scope", self._fake_db_scope), \
             patch.object(bot, "get_db_connection_context", db_context), \
             patch.object(bot, "begin_free_usage_lifecycle_tx", return_value=(usage_state, None, None)), \
             patch.object(bot, "save_webapp_dictionary_query_returning_result_with_cursor", side_effect=RuntimeError("save failed")), \
             patch.object(bot, "finish_free_usage_lifecycle_success_tx") as finish_mock:
            ok, msg = bot._save_dictionary_option_for_user(
                payload=self._shortcut_payload(),
                chosen={"source": "Haus", "target": "дом"},
                user_id=117649764,
            )

        self.assertFalse(ok)
        self.assertIn("Ошибка сохранения", msg)
        finish_mock.assert_not_called()
        self.assertFalse(conn.committed)
        self.assertTrue(conn.rolled_back)

    def test_shortcut_missing_usage_state_fails_explicit(self):
        conn, db_context = self._recording_db_context()
        error_payload = {
            "ok": False,
            "error": "free_usage_state_unavailable",
            "feature": "shortcut_ingest_save_daily",
            "feature_title": "Shortcut сохранение слов",
            "message": "Не удалось проверить лимит бесплатного тарифа.",
        }
        with patch.object(bot, "db_acquire_scope", self._fake_db_scope), \
             patch.object(bot, "get_db_connection_context", db_context), \
             patch.object(bot, "begin_free_usage_lifecycle_tx", return_value=(None, error_payload, 503)), \
             patch.object(bot, "save_webapp_dictionary_query_returning_result_with_cursor") as save_mock:
            ok, msg = bot._save_dictionary_option_for_user(
                payload=self._shortcut_payload(),
                chosen={"source": "Haus", "target": "дом"},
                user_id=117649764,
            )

        self.assertFalse(ok)
        self.assertEqual(msg, "Не удалось проверить лимит бесплатного тарифа.")
        save_mock.assert_not_called()
        self.assertFalse(conn.committed)
        self.assertTrue(conn.rolled_back)

    def test_shortcut_lookup_sends_clean_requests_from_delivery(self):
        with patch.object(server, "_shortcut_split_blocks", return_value=[
                 ("Hat mich gefreut!", "Hat mich gefreut!"),
                 ("Man sieht sich!", "Man sieht sich!"),
             ]), \
             patch.object(
                 server,
                 "_send_private_message",
             ) as send_mock, \
             patch.object(server, "time") as time_mock:
            time_mock.sleep.return_value = None
            sent = server._run_shortcut_lookup_delivery(
                user_id=117649764,
                text="noisy input",
                source="shortcut",
                ingest_key="ingestabc",
            )

        self.assertEqual(sent, 2)
        self.assertEqual(send_mock.call_count, 2)
        self.assertEqual(
            send_mock.call_args_list[0].args[1],
            "Запрос: Hat mich gefreut!\n\nВыберите языковую пару для перевода:",
        )
        self.assertEqual(
            send_mock.call_args_list[1].args[1],
            "Запрос: Man sieht sich!\n\nВыберите языковую пару для перевода:",
        )
