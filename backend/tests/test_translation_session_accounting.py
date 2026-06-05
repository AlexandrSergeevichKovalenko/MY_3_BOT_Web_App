from contextlib import ExitStack, contextmanager
import unittest
from unittest.mock import AsyncMock, patch

import backend.backend_server as server


class TranslationSessionAccountingTests(unittest.TestCase):
    @staticmethod
    @contextmanager
    def _fake_db_scope(*_args, **_kwargs):
        yield []

    def setUp(self):
        self.client = server.app.test_client()

    def _start_route_patches(self, workflow_mock, start_payload):
        return (
            patch.object(server, "WEBAPP_SINGLE_INSTANCE_GUARD_ENABLED", False),
            patch.object(server, "_telegram_hash_is_valid", return_value=True),
            patch.object(
                server,
                "_parse_telegram_init_data",
                return_value={"user": {"id": 55, "first_name": "Iryna"}},
            ),
            patch.object(server, "_resolve_webapp_user_allowed", return_value=(True, "test")),
            patch.object(server, "_try_acquire_shared_idempotency", return_value="token"),
            patch.object(server, "_release_shared_idempotency"),
            patch.object(server, "_clear_recent_finish_no_active_session"),
            patch.object(server, "db_acquire_scope", self._fake_db_scope),
            patch.object(
                server,
                "_get_user_language_pair_for_webapp_request",
                return_value=("ru", "de", None, "request"),
            ),
            patch.object(
                server,
                "resolve_webapp_focus",
                return_value={"kind": "catalog", "key": "v2_main", "prompt_topic": "V2"},
            ),
            patch.object(server, "_refresh_subscription_before_translation_start"),
            patch.object(server, "start_translation_session_webapp", workflow_mock),
            patch.object(server, "_build_translation_session_start_payload", return_value=start_payload),
            patch.object(server, "_build_language_pair_payload", return_value={"source": "ru", "target": "de"}),
            patch.object(server, "_maybe_trigger_translation_focus_pool_deficit_refill", return_value={"triggered": False}),
            patch.object(server, "_estimate_json_payload_size_bytes", return_value=0),
            patch.object(server, "_log_flow_observation"),
            patch.object(server, "_log_compact_trace"),
            patch.object(server, "summarize_db_acquire_events", return_value={}),
            patch.object(server, "_write_session_presence_projection_active"),
        )

    def test_start_route_marks_ready_items_shown_for_free_accounting(self):
        workflow_mock = AsyncMock(return_value={
            "session_id": 123456,
            "items": [],
            "ready_count": 7,
            "expected_total": 7,
            "remaining_count": 0,
            "generation_in_progress": False,
            "generation_status": "ready",
            "phase_metrics": {},
        })
        ready_items = [
            {
                "id_for_mistake_table": 101,
                "sentence": "Ich lerne Deutsch.",
                "unique_id": 1,
                "source_session_id": "123456",
            },
            {
                "id_for_mistake_table": 102,
                "sentence": "Du lernst Deutsch.",
                "unique_id": 2,
                "source_session_id": "123456",
            },
        ]
        start_payload = {
            "session_id": 123456,
            "items": ready_items,
            "ready_count": 2,
            "expected_total": 2,
            "remaining_count": 0,
            "generation_in_progress": False,
            "generation_status": "ready",
        }

        with ExitStack() as stack:
            for patcher in self._start_route_patches(workflow_mock, start_payload):
                stack.enter_context(patcher)
            mark_mock = stack.enter_context(
                patch.object(server, "_mark_translation_sentences_delivered", return_value={"updated": 2})
            )
            response = self.client.post(
                "/api/webapp/start",
                json={"initData": "signed", "topic": "V2", "level": "c1"},
            )

        self.assertEqual(response.status_code, 200)
        mark_mock.assert_called_once_with(
            user_id=55,
            source_lang="ru",
            target_lang="de",
            items=ready_items,
        )

    def test_second_free_session_limit_error_still_blocks_start(self):
        workflow_mock = AsyncMock(return_value={
            "error": "feature_limit_exceeded",
            "feature": "translation_daily_sets",
            "limit": 1,
            "used": 1,
            "unit": "count",
            "phase_metrics": {},
        })
        with ExitStack() as stack:
            for patcher in self._start_route_patches(workflow_mock, {}):
                stack.enter_context(patcher)
            mark_mock = stack.enter_context(patch.object(server, "_mark_translation_sentences_delivered"))
            response = self.client.post(
                "/api/webapp/start",
                json={"initData": "signed", "topic": "V2", "level": "c1"},
            )

        self.assertEqual(response.status_code, 429)
        self.assertEqual(response.get_json()["feature"], "translation_daily_sets")
        mark_mock.assert_not_called()

    def test_sentences_route_still_marks_delivered_items_shown(self):
        items = [
            {
                "id_for_mistake_table": 201,
                "sentence": "Wir sprechen Deutsch.",
                "unique_id": 1,
                "source_session_id": "654321",
            }
        ]
        with patch.object(server, "WEBAPP_SINGLE_INSTANCE_GUARD_ENABLED", False), \
             patch.object(server, "_telegram_hash_is_valid", return_value=True), \
             patch.object(
                 server,
                 "_parse_telegram_init_data",
                 return_value={"user": {"id": 55, "first_name": "Iryna"}},
             ), \
             patch.object(server, "_resolve_webapp_user_allowed", return_value=(True, "test")), \
             patch.object(server, "db_acquire_scope", self._fake_db_scope), \
             patch.object(
                 server,
                 "_get_user_language_pair_for_webapp_request",
                 return_value=("ru", "de", None, "request"),
             ), \
             patch.object(server, "get_pending_daily_sentences", return_value=items), \
             patch.object(server, "_mark_translation_sentences_delivered", return_value={"updated": 1}) as mark_mock, \
             patch.object(server, "is_translation_sentence_fill_async_enabled", return_value=False), \
             patch.object(server, "_estimate_json_payload_size_bytes", return_value=0), \
             patch.object(server, "_log_flow_observation"), \
             patch.object(server, "summarize_db_acquire_events", return_value={}):
            response = self.client.post(
                "/api/webapp/sentences",
                json={"initData": "signed", "session_id": "654321", "limit": 7},
            )

        self.assertEqual(response.status_code, 200)
        mark_mock.assert_called_once_with(
            user_id=55,
            source_lang="ru",
            target_lang="de",
            items=items,
        )


if __name__ == "__main__":
    unittest.main()
