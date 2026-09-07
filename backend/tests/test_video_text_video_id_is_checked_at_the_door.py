# -*- coding: utf-8 -*-
"""Книга-видео заводится только с настоящим идентификатором ролика.

Книга помнит ролик через ссылку https://youtu.be/<id>, а источник слов из неё
собирается разбором этой ссылки (см. test_reader_saves_carry_the_source). Кривой id
дал бы книгу, слова из которой никогда не лягут под ролик. Закрыто 07.09.2026 по слову
владельца: одно правило на входе и на выходе.
"""
import os
import unittest
from unittest import mock

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")


class ИдентификаторРоликаПроверяетсяНаВходе(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from backend import backend_server as server
        cls.server = server
        cls.client = server.app.test_client()

    def test_the_door_and_the_parser_share_one_rule(self):
        for good in ("nLiOMhqDvC8", "abc_DEF-123", "A"):
            self.assertTrue(self.server._is_valid_video_id(good), good)
            self.assertEqual(self.server._video_text_video_id_from_source_url(
                self.server._video_text_source_url(good)), good)
        for bad in ("", "abc def", "abc/../x", "nLiOMhqDvC8?si=1", "пример", "a.b"):
            self.assertFalse(self.server._is_valid_video_id(bad), repr(bad))

    def test_both_endpoints_refuse_a_bad_id_before_touching_anything(self):
        s = self.server
        for path in ("/api/webapp/video/text/start", "/api/webapp/video/text/status"):
            # Общий страж входа проверяет подпись Telegram раньше эндпоинта — те же
            # заглушки, что в test_video_text_admin_has_no_weekly_slot.
            with mock.patch.object(s, "WEBAPP_SINGLE_INSTANCE_GUARD_ENABLED", False), \
                 mock.patch.object(s, "_telegram_hash_is_valid", return_value=True), \
                 mock.patch.object(s, "_parse_telegram_init_data", return_value={"user": {"id": 1}}), \
                 mock.patch.object(s, "_resolve_webapp_user_allowed", return_value=(True, "test")), \
                 mock.patch.object(s, "_video_text_resolve_user", return_value=1), \
                 mock.patch.object(s, "get_video_reader_text",
                                   side_effect=AssertionError("до базы дойти не должны")):
                r = self.client.post(path, json={"initData": "signed", "video_id": "abc def"})
            self.assertEqual(r.status_code, 400, path)
            self.assertIn("идентификатор ролика", r.get_json()["error"])


if __name__ == "__main__":
    unittest.main()
