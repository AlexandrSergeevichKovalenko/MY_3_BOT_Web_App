"""Текст из видео: недельный слот админа не касается.

Решение владельца 04.09.2026. Слот в один текст в неделю — про деньги: сборка идёт к
модели и стоит. Но админ ролики ПРОВЕРЯЕТ, и ему нужно пройти несколько подряд, а не
ждать семь дней между проверками. Поэтому у админа слот не спрашивается вовсе —
при этом расход считается как у всех (log_video_reader_text_request с was_new=True).

Тест держит обе стороны: обычному человеку слот по-прежнему закрывает дверь, админу —
нет. Без него первая же правка рядом молча вернёт админа в общую очередь.
"""
from contextlib import ExitStack
import unittest
from unittest.mock import patch

import backend.backend_server as server


ADMIN_ID = 117649764
NORMAL_ID = 555001


class VideoTextAdminHasNoWeeklySlotTests(unittest.TestCase):
    def setUp(self):
        self.client = server.app.test_client()

    def _patches(self, stack, *, user_id, slot_free):
        stack.enter_context(patch.object(server, "WEBAPP_SINGLE_INSTANCE_GUARD_ENABLED", False))
        # Общий страж входа (enforce_webapp_access) проверяет подпись Telegram раньше
        # самого эндпоинта — без этих заглушек до слота не доходит ни один запрос.
        stack.enter_context(patch.object(server, "_telegram_hash_is_valid", return_value=True))
        stack.enter_context(patch.object(
            server, "_parse_telegram_init_data", return_value={"user": {"id": user_id}}))
        stack.enter_context(patch.object(server, "_resolve_webapp_user_allowed", return_value=(True, "test")))
        stack.enter_context(patch.object(server, "_video_text_resolve_user", return_value=user_id))
        stack.enter_context(patch.object(
            server, "_resolve_user_entitlement",
            return_value=({"effective_mode": "pro"}, {}),
        ))
        stack.enter_context(patch.object(server, "_get_user_language_pair", return_value=("ru", "de", None)))
        stack.enter_context(patch.object(server, "get_video_reader_text", return_value=None))
        stack.enter_context(patch.object(server, "get_admin_telegram_ids", return_value={ADMIN_ID}))
        # Субтитры у ролика есть — иначе разговор о слоте вообще не доходит до слота.
        stack.enter_context(patch.object(
            server, "_load_cached_youtube_transcript_data",
            return_value=({"items": [{"text": "hallo welt", "start": 0.0}]}, "test", 0),
        ))
        return stack.enter_context(patch.object(
            server, "get_video_reader_text_slot",
            return_value={"free": slot_free, "next_at": "2026-09-11T00:00:00", "used_at": ""},
        ))

    def _start(self):
        return self.client.post(
            "/api/webapp/video/text/start",
            json={"initData": "signed", "video_id": "abc123", "title": "Ролик"},
        )

    def test_обычному_человеку_занятый_слот_закрывает_дверь(self):
        with ExitStack() as stack:
            self._patches(stack, user_id=NORMAL_ID, slot_free=False)
            response = self._start()
        self.assertEqual(response.status_code, 429)
        self.assertEqual(response.get_json().get("state"), "limit")

    def test_админа_занятый_слот_не_останавливает(self):
        with ExitStack() as stack:
            slot = self._patches(stack, user_id=ADMIN_ID, slot_free=False)
            response = self._start()
        data = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(data.get("state"), "confirm_needed",
                         msg=f"админ упёрся в лимит: {data}")
        # Слот у админа даже не спрашиваем: нечего мерить, ответ на решение не влияет.
        slot.assert_not_called()
        # На экране админу нельзя обещать «следующий через семь дней» — этого нет.
        self.assertTrue(data.get("unlimited"))

    def test_свободный_слот_у_обычного_человека_пропускает(self):
        with ExitStack() as stack:
            self._patches(stack, user_id=NORMAL_ID, slot_free=True)
            response = self._start()
        data = response.get_json()
        self.assertEqual(data.get("state"), "confirm_needed")
        self.assertFalse(data.get("unlimited"))
