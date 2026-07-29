"""Какой словарь показан подключённым в настройках.

Состояний ровно три: ничего / базовый / полный. Экран обязан показывать ТО, что человек
выбрал. Раньше состояние вычислялось из числа скопированных слов («полный» — если
скопировано больше двух лимитов), а полный вариант вообще ничего не копирует, поэтому
«полный» не показывался никогда и переключатель отскакивал на «базовый».
"""

import unittest
from unittest.mock import patch

import backend.backend_server as server


class SettingsDictionaryTierTests(unittest.TestCase):
    def _tier(self, state):
        with patch.object(server, "_telegram_hash_is_valid", return_value=True), \
             patch.object(server, "_parse_telegram_init_data", return_value={"user": {"id": 77}}), \
             patch.object(server, "_resolve_webapp_user_allowed", return_value=(True, "test")), \
             patch.object(server, "_authenticate_webapp_request", return_value=(77, "tester", None)), \
             patch.object(server, "get_starter_dictionary_state", return_value=state), \
             patch.object(server, "get_shortcut_autosave_enabled", return_value=False), \
             patch.object(server, "is_article_battle_available", return_value=False), \
             patch.object(server, "get_user_prefs", return_value={}), \
             patch.object(server, "_resolve_user_entitlement", return_value=({"effective_mode": "free"}, None)), \
             patch.object(server, "get_admin_telegram_ids", return_value=[]), \
             patch.object(server, "_get_user_language_pair", return_value=("ru", "de", {})), \
             patch.object(server, "count_dictionary_entries_for_language_pair", return_value=14222):
            response = server.app.test_client().post(
                "/api/webapp/settings", json={"initData": "valid"}
            )
        self.assertEqual(response.status_code, 200)
        return response.get_json()

    def test_full_dictionary_shows_as_full(self):
        """Подписка включена — значит подключён полный, сколько бы слов ни было скопировано."""
        payload = self._tier({
            "decision_status": "accepted",
            "live_subscription": True,
            "last_imported_count": 1000,
        })
        self.assertEqual(payload["dict_tier"], "full")
        # И число называется, а не остаётся загадкой.
        self.assertEqual(payload["dict_full_total"], 14222)

    def test_capped_subscription_shows_as_quick_start(self):
        """Оба варианта — подписка, различает их ПОТОЛОК. Пока экран смотрел только на
        флаг подписки, он показывал «Весь словарь» даже после переключения на урезанный:
        человек выходил из настроек, возвращался — и видел свой выбор отменённым."""
        payload = self._tier({
            "decision_status": "accepted",
            "live_subscription": True,
            "subscription_limit": 1000,
            "last_imported_count": 1000,
        })
        self.assertEqual(payload["dict_tier"], "base")

    def test_uncapped_subscription_shows_as_full(self):
        payload = self._tier({
            "decision_status": "accepted",
            "live_subscription": True,
            "subscription_limit": None,
            "last_imported_count": 0,
        })
        self.assertEqual(payload["dict_tier"], "full")

    def test_quick_start_shows_as_base(self):
        payload = self._tier({
            "decision_status": "accepted",
            "live_subscription": False,
            "last_imported_count": 1000,
        })
        self.assertEqual(payload["dict_tier"], "base")
        self.assertEqual(payload["dict_base_total"], server.STARTER_DICTIONARY_IMPORT_LIMIT)

    def test_nothing_connected(self):
        payload = self._tier({
            "decision_status": "declined",
            "live_subscription": False,
            "last_imported_count": 0,
        })
        self.assertEqual(payload["dict_tier"], "none")

    def test_import_limit_is_bounded_but_never_silently(self):
        """Верхняя граница копирования нужна — строки вставляются ПО ОДНОЙ в одной
        транзакции под блокировкой. Недопустимо другое: срезать значение молча, как было
        раньше (ставишь 9000, получаешь 5000, и никакого следа)."""
        from backend.database import import_starter_dictionary_snapshot  # noqa: F401
        import inspect
        import backend.database as db
        source = inspect.getsource(db.import_starter_dictionary_snapshot)
        self.assertIn("safe_limit = min(requested_limit", source)
        self.assertIn("logging.warning", source)


if __name__ == "__main__":
    unittest.main()
