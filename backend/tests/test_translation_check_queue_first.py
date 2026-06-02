import unittest
from unittest.mock import patch

import backend.backend_server as server


class TranslationCheckQueueFirstTests(unittest.TestCase):
    def test_queue_first_start_stages_payload_and_dispatches(self) -> None:
        payload = {
            "session_id": "source-session-1",
            "translations": [
                {"item_order": 0, "id_for_mistake_table": 11, "translation": "Hallo"},
                {"item_order": 1, "id_for_mistake_table": 12, "translation": "Welt"},
            ],
            "send_private_grammar_text": True,
            "original_text": "orig",
            "user_translation": "user",
        }
        with server.app.app_context():
            with patch.object(server, "_get_user_language_pair_for_webapp_request", return_value=("ru", "de", {}, "default")), \
                 patch.object(server, "_load_user_translation_sentence_map", return_value=("source-session-1", {
                     11: {"sentence_number": 1, "id_for_mistake_table": 11, "sentence_id": 101, "original_text": "orig1"},
                     12: {"sentence_number": 2, "id_for_mistake_table": 12, "sentence_id": 102, "original_text": "orig2"},
                 })), \
                 patch.object(server, "_get_matching_active_translation_check_session", return_value=(None, [])), \
                 patch.object(server, "create_translation_check_session", return_value={"id": 77, "total_items": 2, "source_session_id": "source-session-1"}) as create_mock, \
                 patch.object(server, "set_translation_check_staging_payload", return_value={"updated_at_ms": 1}) as stage_mock, \
                 patch.object(server, "set_translation_check_state"), \
                 patch.object(server, "set_translation_check_dispatch_state"), \
                 patch.object(server, "set_translation_check_completion_state"), \
                 patch.object(server, "set_translation_check_poll_hint_state"), \
                 patch.object(server, "_write_translation_check_read_models"), \
                 patch.object(server, "_remember_translation_check_accepted_at"), \
                 patch.object(server, "_dispatch_translation_check_runner_async", return_value={"job_id": "job-123"}), \
                 patch.object(server, "get_translation_check_session_runtime", return_value={"dispatched_job_id": "job-123"}), \
                 patch.object(server, "_build_translation_check_payload", return_value={"check_session": {"id": 77}, "progress": {"total": 2}}):
                response, status_code = server._start_webapp_translation_check_queue_first(
                    payload=payload,
                    request_id="req-1",
                    correlation_id="corr-1",
                    started_perf=0.0,
                    init_data="init-data",
                    translations=payload["translations"],
                    send_private_grammar_text=True,
                    original_text="orig",
                    user_translation="user",
                    user_id=123,
                    username="Alex",
                )

        self.assertEqual(status_code, 200)
        self.assertEqual(response.get_json()["check_session"]["id"], 77)
        create_call = create_mock.call_args
        self.assertEqual(len(create_call.kwargs["items"]), 2)
        self.assertEqual(create_call.kwargs["total_items"], 2)
        self.assertTrue(create_call.kwargs["materialize_items"])
        stage_mock.assert_called_once()

    def test_queue_first_start_continues_when_staging_fails_after_db_items(self) -> None:
        payload = {
            "session_id": "source-session-1",
            "translations": [
                {"item_order": 0, "id_for_mistake_table": 11, "translation": "Hallo"},
            ],
        }
        with server.app.app_context():
            with patch.object(server, "_get_user_language_pair_for_webapp_request", return_value=("ru", "de", {}, "default")), \
                 patch.object(server, "_load_user_translation_sentence_map", return_value=("source-session-1", {
                     11: {"sentence_number": 1, "id_for_mistake_table": 11, "sentence_id": 101, "original_text": "orig1"},
                 })), \
                 patch.object(server, "_get_matching_active_translation_check_session", return_value=(None, [])), \
                 patch.object(server, "create_translation_check_session", return_value={"id": 77, "total_items": 1, "source_session_id": "source-session-1"}), \
                 patch.object(server, "set_translation_check_staging_payload", return_value=None), \
                 patch.object(server, "set_translation_check_state"), \
                 patch.object(server, "set_translation_check_dispatch_state"), \
                 patch.object(server, "set_translation_check_completion_state"), \
                 patch.object(server, "set_translation_check_poll_hint_state"), \
                 patch.object(server, "_write_translation_check_read_models"), \
                 patch.object(server, "_remember_translation_check_accepted_at"), \
                 patch.object(server, "_dispatch_translation_check_runner_async", return_value={"job_id": "job-123"}), \
                 patch.object(server, "get_translation_check_session_runtime", return_value={"dispatched_job_id": "job-123"}), \
                 patch.object(server, "_build_translation_check_payload", return_value={"check_session": {"id": 77}, "progress": {"total": 1}}):
                response, status_code = server._start_webapp_translation_check_queue_first(
                    payload=payload,
                    request_id="req-1",
                    correlation_id="corr-1",
                    started_perf=0.0,
                    init_data="init-data",
                    translations=payload["translations"],
                    send_private_grammar_text=False,
                    original_text=None,
                    user_translation=None,
                    user_id=123,
                    username="Alex",
                )

        self.assertEqual(status_code, 200)
        self.assertEqual(response.get_json()["check_session"]["id"], 77)

    def test_worker_hydrates_items_from_stage_payload(self) -> None:
        session = {
            "id": 77,
            "user_id": 123,
            "source_session_id": "source-session-1",
            "source_lang": "ru",
            "target_lang": "de",
            "total_items": 2,
        }
        stage_payload = {
            "translations": [
                {"item_order": 0, "sentence_id_for_mistake_table": 11, "translation": "Hallo"},
                {"item_order": 1, "sentence_id_for_mistake_table": 12, "translation": "Welt"},
            ]
        }
        hydrated_items = [
            {"item_order": 0, "id_for_mistake_table": 11, "translation": "Hallo", "original_text": "orig1"},
            {"item_order": 1, "id_for_mistake_table": 12, "translation": "Welt", "original_text": "orig2"},
        ]
        with server.app.app_context():
            with patch.object(server, "list_translation_check_items", side_effect=[[], hydrated_items]), \
                 patch.object(server, "get_translation_check_staging_payload", return_value=stage_payload), \
                 patch.object(server, "_load_user_translation_sentence_map", return_value=("source-session-1", {11: 101, 12: 102})), \
                 patch.object(server, "_normalize_translation_check_entries", return_value=hydrated_items), \
                 patch.object(server, "insert_translation_check_items", return_value=len(hydrated_items)) as insert_mock, \
                 patch.object(server, "clear_translation_check_staging_payload") as clear_mock, \
                 patch.object(server, "update_translation_check_session_total_items", return_value=session) as update_total_mock:
                result = server._hydrate_translation_check_session_items_from_stage(
                    session_id=77,
                    session=session,
                    request_id="req-1",
                    correlation_id="corr-1",
                )

        self.assertEqual(result, hydrated_items)
        insert_mock.assert_called_once_with(session_id=77, items=hydrated_items)
        clear_mock.assert_called_once_with(77)
        update_total_mock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
