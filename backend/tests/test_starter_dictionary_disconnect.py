import unittest
from unittest.mock import patch

import backend.backend_server as server


class StarterDictionaryDisconnectTests(unittest.TestCase):
    def setUp(self):
        self.client = server.app.test_client()

    def test_disconnect_removes_starter_snapshot_and_returns_offer(self):
        offer = {
            "enabled": True,
            "can_reconnect": True,
            "can_disconnect": False,
            "state": {"decision_status": "declined", "import_status": "idle"},
            "template_total": 1000,
            "suggested_count": 1000,
        }
        with patch.object(server, "_get_authenticated_user_from_request_init_data", return_value=(117649764, "alex", None)), \
             patch.object(server, "_get_user_language_pair", return_value=("ru", "de", {"has_profile": True})), \
             patch.object(server, "get_starter_dictionary_state", return_value={"last_imported_count": 25, "last_imported_at": None}), \
             patch.object(server, "count_dictionary_entries_for_language_pair", return_value=1000), \
             patch.object(server, "delete_starter_dictionary_snapshot", return_value={"deleted_count": 25}) as delete_mock, \
             patch.object(server, "upsert_starter_dictionary_state", return_value={"decision_status": "declined", "import_status": "idle"}) as upsert_mock, \
             patch.object(server, "_build_starter_dictionary_offer", return_value=offer), \
             patch.object(server, "STARTER_DICTIONARY_ENABLED", True), \
             patch.object(server, "STARTER_DICTIONARY_SOURCE_USER_ID", 42), \
             patch.object(server, "STARTER_DICTIONARY_TEMPLATE_VERSION", "v1"):
            with server.app.test_request_context(
                "/api/webapp/starter-dictionary/apply",
                method="POST",
                json={"initData": "stub", "action": "disconnect"},
            ):
                response = server.webapp_starter_dictionary_apply()

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["action"], "disconnected")
        self.assertEqual(payload["disconnect_result"]["deleted_count"], 25)
        delete_mock.assert_called_once_with(user_id=117649764, source_lang="ru", target_lang="de")
        self.assertTrue(upsert_mock.called)


if __name__ == "__main__":
    unittest.main()
