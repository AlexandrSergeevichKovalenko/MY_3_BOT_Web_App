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
        # The endpoint authenticates via _authenticate_webapp_request (initData OR a durable
        # app/dict token — the standalone home-screen app has no initData), not the
        # initData-only helper it used before.
        with patch.object(server, "_authenticate_webapp_request", return_value=(117649764, "alex", None)), \
             patch.object(server, "_get_user_language_pair", return_value=("ru", "de", {"has_profile": True})), \
             patch.object(server, "get_starter_dictionary_state", return_value={"last_imported_count": 25, "last_imported_at": None}), \
             patch.object(server, "count_dictionary_entries_for_language_pair", return_value=1000), \
             patch.object(server, "remove_untouched_subscription_words", return_value=25) as delete_mock, \
             patch.object(server, "set_starter_dictionary_subscription") as unsubscribe_mock, \
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
                result = server.webapp_starter_dictionary_apply()

        # The view returns a (body, status) tuple on some branches and a bare response on
        # others — Flask accepts both, so the test must too, and still assert the status.
        response, status = result if isinstance(result, tuple) else (result, 200)

        self.assertEqual(status, 200)
        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["action"], "disconnected")
        self.assertEqual(payload["disconnect_result"]["deleted_count"], 25)
        # Удаляем НЕТРОНУТОЕ у пользователя, а не «строки старого копирования по паре»:
        # у подписчика таких строк нет вообще, и прежняя чистка не находила ничего.
        delete_mock.assert_called_once_with(117649764)
        # Отключение обязано снимать подписку, иначе слова автора продолжат приходить.
        unsubscribe_mock.assert_called_once_with(117649764, False)
        self.assertTrue(upsert_mock.called)


class UntouchedRemovalRuleTests(unittest.TestCase):
    """Что именно удаляется при отключении словаря.

    Правило владельца: «не учил — значит не нужно», но всё, чего человек касался,
    остаётся: там его история, на ней держится аналитика.

    Первая версия правила защищала ещё и «лежит в папке» — и провалилась: импорт САМ
    раскладывал слова по смысловым папкам, поэтому под защиту попали все до одного
    (999 из 999 у первого же проверенного человека), и отключение не удаляло ничего.
    """

    def test_rule_protects_what_the_person_touched_but_not_folders(self):
        import inspect
        import backend.database as db
        sql = inspect.getsource(db.remove_untouched_subscription_words)
        # Признаки интереса: прогресс, журнал ответов, отметка «выучено», правка записи.
        self.assertIn("bt_3_card_srs_state", sql)
        self.assertIn("bt_3_card_review_log", sql)
        self.assertIn("is_learned", sql)
        self.assertIn("updated_at", sql)
        # А вот папка признаком быть НЕ должна — её проставлял импорт.
        self.assertNotIn("folder_id IS NULL", sql)

    def test_rule_touches_only_words_that_came_from_the_author(self):
        import inspect
        import backend.database as db
        sql = inspect.getsource(db.remove_untouched_subscription_words)
        self.assertIn("origin_process = 'subscription'", sql)
        self.assertIn("starter_dictionary_snapshot", sql)


if __name__ == "__main__":
    unittest.main()
