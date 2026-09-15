"""«Расширить до всего словаря» обязано доходить до сервера, а не глотаться.

Повод 15.09.2026. Подписка включается на ОБА размера, разница только в потолке
(subscription_limit: None = весь словарь, 1000 = быстрый старт). В /apply стоял
короткий ответ «уже подключено», который отсекал ЛЮБОЙ повторный запрос:
`already_accepted and user_pair_total > 0 and not force_reimport`. Человек на быстром
старте жал «🔓 Подключить весь словарь», получал ok:true — и потолок оставался 1000.

Коварство в том, что ломалось не сразу: условие ждёт user_pair_total > 0, а подписка
отдаёт слова постепенно. У новичка кнопка работала, через неделю переставала.
"""
import unittest
from unittest.mock import patch

import backend.backend_server as server


def _call(action, full, state, user_pair_total):
    offer = {"enabled": True, "state": state, "template_total": 17539, "suggested_count": 1000}
    with patch.object(server, "_authenticate_webapp_request", return_value=(313002147, "u", None)), \
         patch.object(server, "_get_user_language_pair", return_value=("ru", "de", {"has_profile": True})), \
         patch.object(server, "get_starter_dictionary_state", return_value=state), \
         patch.object(server, "count_dictionary_entries_for_language_pair", return_value=user_pair_total), \
         patch.object(server, "set_starter_dictionary_subscription") as sub_mock, \
         patch.object(server, "upsert_starter_dictionary_state", return_value=state), \
         patch.object(server, "_build_starter_dictionary_offer", return_value=offer), \
         patch.object(server, "STARTER_DICTIONARY_ENABLED", True), \
         patch.object(server, "STARTER_DICTIONARY_SOURCE_USER_ID", 42), \
         patch.object(server, "STARTER_DICTIONARY_IMPORT_LIMIT", 1000), \
         patch.object(server, "STARTER_DICTIONARY_TEMPLATE_VERSION", "v1"):
        with server.app.test_request_context(
            "/api/webapp/starter-dictionary/apply",
            method="POST",
            json={"initData": "stub", "action": action, "full": full},
        ):
            result = server.webapp_starter_dictionary_apply()
    response, status = result if isinstance(result, tuple) else (result, 200)
    return status, response.get_json(), sub_mock


QUICK = {"decision_status": "accepted", "import_status": "idle", "active_job_id": None,
         "live_subscription": True, "subscription_limit": 1000,
         "last_imported_count": 0, "last_imported_at": None}
FULL = dict(QUICK, subscription_limit=None)


class StarterDictionaryUpgradeTests(unittest.TestCase):
    def test_upgrade_from_quick_to_full_is_not_swallowed(self):
        """ГЛАВНОЕ: сидит на быстром старте, просит весь словарь — потолок обязан сняться."""
        status, payload, sub_mock = _call("accept", True, QUICK, user_pair_total=640)
        self.assertEqual(status, 200)
        self.assertNotIn("already_connected", payload,
                         "запрос на ДРУГОЙ размер снова глотается коротким ответом")
        sub_mock.assert_called_once_with(313002147, True, subscription_limit=None)

    def test_same_size_still_short_circuits(self):
        """Просит РОВНО ТО ЖЕ — короткий ответ остаётся: лишней записи в базу не делаем."""
        status, payload, sub_mock = _call("accept", True, FULL, user_pair_total=640)
        self.assertEqual(status, 200)
        self.assertTrue(payload.get("already_connected"))
        sub_mock.assert_not_called()

    def test_quick_asked_again_short_circuits(self):
        status, payload, sub_mock = _call("accept", False, QUICK, user_pair_total=640)
        self.assertTrue(payload.get("already_connected"))
        sub_mock.assert_not_called()

    def test_full_user_asking_for_quick_goes_through(self):
        """Сужение тоже проходит: это другой размер, а не повтор."""
        status, payload, sub_mock = _call("accept", False, FULL, user_pair_total=640)
        self.assertNotIn("already_connected", payload)
        sub_mock.assert_called_once_with(313002147, True, subscription_limit=1000)


if __name__ == "__main__":
    unittest.main()
