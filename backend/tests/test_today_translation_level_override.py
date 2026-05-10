import unittest
from unittest.mock import AsyncMock, patch

import backend.backend_server as server


class TodayTranslationLevelOverrideTests(unittest.TestCase):
    def setUp(self):
        self.client = server.app.test_client()

    def _post_start(self, *, level=None):
        payload = {}
        if level is not None:
            payload["level"] = level
        return self.client.post("/api/today/items/42/translation/start", json=payload)

    def test_explicit_user_level_overrides_recommended_harder_level(self):
        workflow_mock = AsyncMock(return_value={"session_id": 987654, "expected_total": 7, "count": 0})
        item = {
            "task_type": "translation",
            "payload": {
                "mode": "weakest_topic",
                "sentences": 7,
                "level": "c1",
                "recommended_level": "c1",
                "recommended_topic_label": "Advanced clauses",
                "recommended_custom_focus": "",
                "recommended_reason_examples": [
                    "Хотя комиссия уже несколько месяцев обсуждает реформу, окончательное решение всё ещё откладывается.",
                ],
                "examples": [
                    "Несмотря на многочисленные возражения, проект был утверждён после длительного обсуждения.",
                ],
            },
        }

        with patch.object(server, "_get_authenticated_user_from_request_init_data", return_value=(77, "alex", None)), \
             patch.object(server, "_get_user_language_pair", return_value=("ru", "de", None)), \
             patch.object(server, "get_daily_plan_item", return_value=item), \
             patch.object(server, "resolve_webapp_focus", return_value={"kind": "preset", "prompt_topic": "Advanced clauses"}), \
             patch.object(server, "_refresh_subscription_before_translation_start"), \
             patch.object(server, "start_translation_session_webapp", workflow_mock), \
             patch.object(server, "_build_translation_session_start_payload", return_value={"session_id": 987654, "expected_total": 7, "count": 0}), \
             patch.object(server, "_count_translation_session_completed_sentences", return_value=0), \
             patch.object(server, "update_daily_plan_item_payload", return_value=item), \
             patch.object(server, "update_daily_plan_item_status", return_value=item), \
             patch.object(server, "_mark_today_plan_snapshot_stale"), \
             patch.object(server, "_schedule_today_plan_snapshot_refresh"), \
             patch.object(server, "_build_language_pair_payload", return_value={"source": "ru", "target": "de"}):
            response = self._post_start(level="b1")

        self.assertEqual(response.status_code, 200)
        workflow_mock.assert_called_once()
        self.assertEqual(workflow_mock.await_args.kwargs["level"], "b1")
        self.assertEqual(response.get_json()["practice"]["level"], "b1")

    def test_missing_user_level_falls_back_to_recommended_level(self):
        workflow_mock = AsyncMock(return_value={"session_id": 123456, "expected_total": 7, "count": 0})
        item = {
            "task_type": "translation",
            "payload": {
                "mode": "weakest_topic",
                "sentences": 7,
                "level": "c1",
                "recommended_level": "b2",
                "recommended_topic_label": "Reported speech",
                "recommended_custom_focus": "",
                "recommended_reason_examples": [],
                "examples": [],
            },
        }

        with patch.object(server, "_get_authenticated_user_from_request_init_data", return_value=(77, "alex", None)), \
             patch.object(server, "_get_user_language_pair", return_value=("ru", "de", None)), \
             patch.object(server, "get_daily_plan_item", return_value=item), \
             patch.object(server, "resolve_webapp_focus", return_value={"kind": "preset", "prompt_topic": "Reported speech"}), \
             patch.object(server, "_refresh_subscription_before_translation_start"), \
             patch.object(server, "start_translation_session_webapp", workflow_mock), \
             patch.object(server, "_build_translation_session_start_payload", return_value={"session_id": 123456, "expected_total": 7, "count": 0}), \
             patch.object(server, "_count_translation_session_completed_sentences", return_value=0), \
             patch.object(server, "update_daily_plan_item_payload", return_value=item), \
             patch.object(server, "update_daily_plan_item_status", return_value=item), \
             patch.object(server, "_mark_today_plan_snapshot_stale"), \
             patch.object(server, "_schedule_today_plan_snapshot_refresh"), \
             patch.object(server, "_build_language_pair_payload", return_value={"source": "ru", "target": "de"}):
            response = self._post_start()

        self.assertEqual(response.status_code, 200)
        workflow_mock.assert_called_once()
        self.assertEqual(workflow_mock.await_args.kwargs["level"], "b2")
        self.assertEqual(response.get_json()["practice"]["level"], "b2")


if __name__ == "__main__":
    unittest.main()
