# -*- coding: utf-8 -*-
"""Замок бесплатного месяца на быстром переводе.

Владелец 04.09.2026 на запертом тестовом аккаунте открыл быстрый словарь с иконки на
экране и получил переводы. Причина: первый запрос словаря — /api/translate/quick — лежит
вне /api/webapp/* и шёл мимо общей двери. Здесь стережётся, что все три адреса быстрого
перевода отвечают запертому 402 с reason=free_month_over, а незапертому — как раньше.
"""
import os
import unittest
from unittest.mock import patch

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")

import backend.backend_server as server  # noqa: E402

UID = 8546091375


class БыстрыйПеревод(unittest.TestCase):

    def setUp(self):
        self.client = server.app.test_client()

    def _post(self, path, body, locked):
        with patch.object(server, "_resolve_webapp_user_id", return_value=UID), \
             patch.object(server, "_dict_user_has_left_bot", return_value=False), \
             patch.object(server, "is_access_locked", return_value=locked):
            return self.client.post(path, json=body)

    def test_перевод_запертому_402(self):
        r = self._post("/api/translate/quick", {"text": "Разговор", "target_lang": "de", "source_lang": "ru"}, True)
        self.assertEqual(r.status_code, 402)
        self.assertEqual(r.get_json()["reason"], "free_month_over")

    def test_артикль_и_исправление_запертому_402(self):
        r = self._post("/api/translate/quick/article", {"text": "Rede", "target_lang": "de"}, True)
        self.assertEqual(r.status_code, 402)
        r = self._post("/api/translate/quick/correct", {"text": "Ich habe Durst", "target_lang": "de"}, True)
        self.assertEqual(r.status_code, 402)

    def test_незапертого_не_трогаем(self):
        with patch.object(server, "_build_quick_translate_cache_key", side_effect=RuntimeError("дальше не идём")):
            r = self._post("/api/translate/quick", {"text": "Разговор", "target_lang": "de", "source_lang": "ru"}, False)
        self.assertNotEqual(r.status_code, 402)


if __name__ == "__main__":
    unittest.main()
