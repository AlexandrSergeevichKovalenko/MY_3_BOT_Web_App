# -*- coding: utf-8 -*-
"""Замок бесплатного месяца: боковые входы, идущие мимо двери /api/webapp/*.

Найдено 04.09.2026 по дороге: ярлык «Ночной переводчик» (/api/shortcut/lookup,
/api/shortcut/run-check) и пересылка сообщений боту (handle_forwarded_message_lookup)
не проходят через enforce_webapp_access и handle_user_message. Здесь стережётся, что
запертому они тоже отвечают отказом с предложением тарифа.
"""
import asyncio
import os
import unittest
from unittest.mock import patch

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")

import backend.backend_server as server  # noqa: E402
import bot_3  # noqa: E402

UID = 8546091375


class Ярлык(unittest.TestCase):

    def setUp(self):
        self.client = server.app.test_client()

    def test_run_check_запертому_отказ(self):
        with patch.object(server, "resolve_shortcut_install_token",
                          return_value={"user_id": UID, "installation_id": 5}), \
             patch.object(server, "is_telegram_user_allowed", return_value=True), \
             patch.object(server, "is_access_locked", return_value=True):
            r = self.client.post("/api/shortcut/run-check", json={"install_token": "t", "text": "x"})
        self.assertEqual(r.status_code, 402)
        body = r.get_json()
        self.assertFalse(body["allowed"])
        self.assertEqual(body["reason"], "access_locked")
        self.assertIn("Лайт", body["message"])

    def test_lookup_запертому_отказ(self):
        with patch.object(server, "resolve_shortcut_install_token",
                          return_value={"user_id": UID, "installation_id": 5}), \
             patch.object(server, "is_telegram_user_allowed", return_value=True), \
             patch.object(server, "is_access_locked", return_value=True), \
             patch.object(server, "_shortcut_run_gate") as gate:
            payload, status = server._shortcut_lookup_from_install_token(
                install_token="t", text="Guten Morgen", request_id="r", remote_ip="1.1.1.1")
        self.assertEqual(status, 402)
        self.assertEqual(payload["reason"], "free_month_over")
        gate.assert_not_called()


class Пересылка(unittest.TestCase):

    def test_пересланное_запертому_отказ_с_кнопками(self):
        sent = []
        ran = []

        class _User:
            id = UID

        class _Msg:
            text = "Ich habe Durst."
            caption = None
            from_user = _User()

            async def reply_text(self, text, **kw):
                sent.append((text, kw))

        class _Chat:
            type = "private"

        class _Upd:
            message = _Msg()
            effective_message = message
            effective_chat = _Chat()

        async def _split(*a, **k):
            ran.append(1)

        bot_3._ACCESS_LOCK_CACHE.clear()
        with patch.object(bot_3, "is_access_locked", return_value=True), \
             patch.object(bot_3, "_run_shortcut_text_split", _split), \
             patch.dict(os.environ, {"TELEGRAM_BOT_USERNAME": "Ich_Deutsch_bot"}):
            asyncio.run(bot_3.handle_forwarded_message_lookup(_Upd(), None))
        bot_3._ACCESS_LOCK_CACHE.clear()
        self.assertEqual(ran, [])
        self.assertEqual(len(sent), 1)
        self.assertIn("Бесплатный месяц закончился", sent[0][0])


if __name__ == "__main__":
    unittest.main()
