# -*- coding: utf-8 -*-
"""Замок бесплатного месяца: три двери и ничего сверх них.

ПОВОД. Владелец 04.09.2026: после 30 дней без подписки человек видит только окно с двумя
тарифами и настройки; задания в Telegram прекращаются. Стратегия —
docs/tasks/light_tier_strategy.md §5.

ЧТО СТЕРЕЖЁТСЯ:
  1. Дверь веба — одна (enforce_webapp_access): запертому 402 с reason=free_month_over
     на любой путь, КРОМЕ оплаты, настроек, онбординга и bootstrap.
  2. Право не прочиталось — не замок. Ошибка в лог, человек проходит.
  3. Администратор не запирается никогда.
  4. Ответ бота запертому — две кнопки: buylight и buypro.
  5. Обещание locked_users_got_learning_content зарегистрировано и ждёт 0.
"""
import asyncio
import os
import unittest
from unittest.mock import patch

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")

import backend.backend_server as server  # noqa: E402
from backend import database as db  # noqa: E402
from backend import fix_promises  # noqa: E402
import bot_3  # noqa: E402

UID = 8546091375


class ДверьВеба(unittest.TestCase):

    def setUp(self):
        self.client = server.app.test_client()

    def _door(self, locked, path):
        with patch.object(server, "_telegram_hash_is_valid", return_value=True), \
             patch.object(server, "_parse_telegram_init_data", return_value={"user": {"id": UID}}), \
             patch.object(server, "_resolve_webapp_user_allowed", return_value=(True, "test")), \
             patch.object(server, "_maybe_persist_display_name"), \
             patch.object(server, "is_access_locked", side_effect=locked):
            return self.client.post(path, json={"initData": "x"})

    def test_запертому_402_на_обычный_путь(self):
        r = self._door(lambda uid: True, "/api/webapp/nonexistent-path-for-door-test")
        self.assertEqual(r.status_code, 402)
        body = r.get_json()
        self.assertEqual(body["reason"], "free_month_over")
        self.assertEqual(body["light_stars"], 160)
        self.assertIn("Лайт", body["error"])

    def test_незапертый_проходит_дальше(self):
        r = self._door(lambda uid: False, "/api/webapp/nonexistent-path-for-door-test")
        self.assertNotEqual(r.status_code, 402)

    def test_оплата_настройки_онбординг_открыты_запертому(self):
        for path in ("/api/webapp/bootstrap", "/api/webapp/billing/stars_invoice",
                     "/api/webapp/settings", "/api/webapp/settings/preset",
                     "/api/webapp/onboarding/status", "/api/webapp/instance/claim"):
            self.assertFalse(server._access_lock_applies(path), path)
        for path in ("/api/webapp/dictionary/lookup", "/api/webapp/today", "/api/message",
                     "/api/webapp/reader/audio/page"):
            self.assertTrue(server._access_lock_applies(path), path)

    def test_право_не_прочиталось_это_не_замок(self):
        def boom(uid):
            raise RuntimeError("база лежит")
        r = self._door(boom, "/api/webapp/nonexistent-path-for-door-test")
        self.assertNotEqual(r.status_code, 402)


class ПризнакЗамка(unittest.TestCase):

    def test_заперт_когда_состояние_locked(self):
        with patch.object(db, "get_admin_telegram_ids", return_value=set()), \
             patch.object(db, "resolve_entitlement", return_value={"access_state": "locked"}):
            self.assertTrue(db.is_access_locked(UID))

    def test_бесплатный_месяц_и_неизвестно_не_замок(self):
        for state in ("free_month", "unknown", "light", "pro"):
            with patch.object(db, "get_admin_telegram_ids", return_value=set()), \
                 patch.object(db, "resolve_entitlement", return_value={"access_state": state}):
                self.assertFalse(db.is_access_locked(UID), state)

    def test_администратор_не_запирается(self):
        with patch.object(db, "get_admin_telegram_ids", return_value={UID}), \
             patch.object(db, "resolve_entitlement", return_value={"access_state": "locked"}) as ent:
            self.assertFalse(db.is_access_locked(UID))
        ent.assert_not_called()


class ОтветБота(unittest.TestCase):

    def test_две_кнопки_оплаты(self):
        with patch.dict(os.environ, {"TELEGRAM_BOT_USERNAME": "Ich_Deutsch_bot"}):
            kb = bot_3._access_locked_keyboard()
        urls = [b.url for row in kb.inline_keyboard for b in row]
        self.assertEqual(urls, ["https://t.me/Ich_Deutsch_bot?startapp=buylight",
                                "https://t.me/Ich_Deutsch_bot?startapp=buypro"])

    def test_памятка_замка_не_запирает_при_сбое(self):
        bot_3._ACCESS_LOCK_CACHE.clear()
        with patch.object(bot_3, "is_access_locked", side_effect=RuntimeError("база лежит")):
            self.assertFalse(bot_3._is_access_locked_cached(UID))
        with patch.object(bot_3, "is_access_locked", return_value=True):
            self.assertTrue(bot_3._is_access_locked_cached(UID))
        bot_3._ACCESS_LOCK_CACHE.clear()

    def test_сообщение_запертого_получает_отказ_с_кнопками(self):
        sent = []

        class _Msg:
            text = "▶️ Следующее задание"
            forward_origin = None

            class from_user:
                id = UID

            async def reply_text(self, text, **kw):
                sent.append((text, kw))

        class _Chat:
            type = "private"

        class _Upd:
            message = _Msg()
            effective_message = message
            effective_chat = _Chat()

        bot_3._ACCESS_LOCK_CACHE.clear()
        with patch.object(bot_3, "is_access_locked", return_value=True), \
             patch.object(bot_3, "_touch_access_period"), \
             patch.dict(os.environ, {"TELEGRAM_BOT_USERNAME": "Ich_Deutsch_bot"}):
            asyncio.run(bot_3.handle_user_message(_Upd(), None))
        bot_3._ACCESS_LOCK_CACHE.clear()
        self.assertEqual(len(sent), 1)
        self.assertIn("Бесплатный месяц закончился", sent[0][0])
        self.assertIsNotNone(sent[0][1].get("reply_markup"))


class Обещание(unittest.TestCase):

    def test_зарегистрировано_и_ждёт_ноль(self):
        p = fix_promises.by_key("locked_users_got_learning_content")
        self.assertIsNotNone(p)
        self.assertEqual(p.expected, 0)


if __name__ == "__main__":
    unittest.main()
