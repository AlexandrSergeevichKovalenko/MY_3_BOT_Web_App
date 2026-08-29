"""Позиция просмотра видео помнится — и не стирается нулём (29.08.2026).

Владелец 29.08.2026: «Каждый раз я захожу и другие пользователи заходят и начинают
смотреть с самого начала. Мы уже делали это, но это не работает».

Механизм «продолжить с места» существовал целиком, но убивал себя двумя способами:

1. Сервер сохранял позицию ТОЛЬКО пользователю YOUTUBE_LIBRARY_ADMIN_USER_ID, а всем
   остальным отвечал {"ok": true, "state": null} — клиент считал это успехом, а памяти
   у человека не было никогда. Замер живой базы 29.08.2026: в bt_3_youtube_watch_state
   56 строк и РОВНО ОДИН user_id.

2. При запуске мини-аппа восстанавливался последний ролик, youtubeId получал значение
   ещё на главном экране (плеера нет, время = 0), и эффект «человек вне раздела видео →
   дозапиши позицию» отправлял ноль поверх реальной позиции. Замер: 51 строка из 56
   переписывалась позже, и в 37 из них после этого лежал ноль.

Здесь стережётся серверная половина починки. Клиентская (App.jsx не отправляет позицию,
пока плеер её не назвал) — в блоке «ИСПРАВЛЕНО 29.08.2026» у persistYoutubeResumeState,
и её сторожит test_client_never_sends_a_position_the_player_has_not_named.
"""

import io
import os
import re
import unittest
from unittest.mock import patch

import backend.backend_server as server


APP_JSX = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "frontend", "src", "App.jsx",
)


class YoutubeWatchPositionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = server.app.test_client()

    def _patches(self, user_id: int):
        return [
            patch.object(server, "_telegram_hash_is_valid", return_value=True),
            patch.object(server, "_parse_telegram_init_data", return_value={"user": {"id": user_id}}),
        ]

    def _save(self, *, user_id: int, seconds, playback_started=None):
        """Один запрос на сохранение. Возвращает (ответ, что дошло до базы или None)."""
        captured = {}

        def fake_upsert(**kwargs):
            captured.update(kwargs)
            return {"video_id": kwargs.get("video_id"), "current_time_seconds": kwargs.get("current_time_seconds")}

        body = {
            "initData": "valid",
            "videoId": "j40Qut-oFSw",
            "input": "https://youtu.be/j40Qut-oFSw",
            "current_time_seconds": seconds,
        }
        if playback_started is not None:
            body["playback_started"] = playback_started

        patches = self._patches(user_id) + [
            patch.object(server, "upsert_youtube_watch_state", side_effect=fake_upsert),
        ]
        for p in patches:
            p.start()
        try:
            response = self.client.post("/api/webapp/youtube/state", json=body)
        finally:
            for p in patches:
                p.stop()
        return response, (captured or None)

    def test_position_is_saved_for_an_ordinary_user(self):
        """Главное: обычный человек, не админ, получает свою позицию в базе.

        Здесь стоял `if user_id_int != YOUTUBE_LIBRARY_ADMIN_USER_ID: return ok, state=None`.
        Если тест покраснел — ограничение вернули, и все, кроме владельца, снова смотрят
        каждый ролик с начала после любой чистки кеша телефона.
        """
        ordinary_user = 987654321
        self.assertNotEqual(ordinary_user, server.YOUTUBE_LIBRARY_ADMIN_USER_ID)

        response, captured = self._save(user_id=ordinary_user, seconds=449, playback_started=True)

        self.assertEqual(response.status_code, 200)
        self.assertIsNotNone(captured, "позиция обычного пользователя не дошла до базы")
        self.assertEqual(captured.get("user_id"), ordinary_user)
        self.assertEqual(captured.get("current_time_seconds"), 449)

    def test_zero_without_playback_never_reaches_the_database(self):
        """Ноль без воспроизведения — это «мы ещё не знаем», а не «человек в начале».

        Именно такой запрос уходил при КАЖДОМ запуске приложения и стирал позицию.
        """
        response, captured = self._save(
            user_id=server.YOUTUBE_LIBRARY_ADMIN_USER_ID, seconds=0, playback_started=False,
        )

        self.assertEqual(response.status_code, 200)
        self.assertIsNone(captured, "ноль без воспроизведения дошёл до базы и стёр позицию")
        self.assertEqual(response.get_json().get("skipped"), "zero_without_playback")

    def test_zero_from_an_old_bundle_is_refused_too(self):
        """Старый закешированный бандл флага не знает — его ноль тоже не принимаем.

        Отсутствие поля и явное false — РАЗНЫЕ состояния (ловушка «старый бандл против
        нуля»), они различаются в логе, но оба не имеют права стереть позицию.
        """
        response, captured = self._save(
            user_id=server.YOUTUBE_LIBRARY_ADMIN_USER_ID, seconds=0, playback_started=None,
        )

        self.assertEqual(response.status_code, 200)
        self.assertIsNone(captured, "ноль от старого бандла дошёл до базы и стёр позицию")

    def test_deliberate_rewind_to_the_very_start_is_honoured(self):
        """А вот осознанная перемотка в самое начало обязана сохраниться.

        Иначе защита от нуля превратилась бы в «позиция не умеет ехать назад» —
        человек отмотал бы ролик в начало, вышел, вернулся и снова попал в середину.
        """
        response, captured = self._save(
            user_id=server.YOUTUBE_LIBRARY_ADMIN_USER_ID, seconds=0, playback_started=True,
        )

        self.assertEqual(response.status_code, 200)
        self.assertIsNotNone(captured, "перемотка в начало не сохранилась")
        self.assertEqual(captured.get("current_time_seconds"), 0)

    def test_real_position_is_saved_even_without_the_flag(self):
        """Настоящая позиция принимается всегда — флаг сторожит только ноль."""
        response, captured = self._save(
            user_id=server.YOUTUBE_LIBRARY_ADMIN_USER_ID, seconds=201, playback_started=None,
        )

        self.assertEqual(response.status_code, 200)
        self.assertIsNotNone(captured)
        self.assertEqual(captured.get("current_time_seconds"), 201)

    def test_client_never_sends_a_position_the_player_has_not_named(self):
        """Сторож на фронт: первый пояс защиты стоит в App.jsx, а не только на сервере.

        Без него приложение продолжало бы стирать позицию в localStorage телефона,
        даже если сервер её отстоял, — и человек с плохой связью снова смотрел бы
        всё заново.
        """
        source = io.open(APP_JSX, encoding="utf-8").read()

        self.assertIn(
            "const youtubeResumeValueIsWritable",
            source,
            "исчезла проверка «можно ли записывать это значение» — ноль снова затрёт позицию",
        )
        self.assertIn(
            "if (!youtubeResumeValueIsWritable(safeTime)) return;",
            source,
            "проверку перестали вызывать перед записью позиции",
        )
        # Проверка обязана стоять В ОБОИХ путях записи: локальный кеш и сервер.
        self.assertEqual(
            source.count("if (!youtubeResumeValueIsWritable(safeTime)) return;"),
            2,
            "проверка осталась только в одном из двух путей записи позиции",
        )
        self.assertIn(
            "playback_started: Boolean(youtubePlaybackStartedRef.current)",
            source,
            "клиент перестал сообщать серверу, было ли реальное воспроизведение",
        )
        # Флаг обязан забываться вместе с роликом. Иначе при смене видео через
        # cueVideoById он остаётся истинным от предыдущего, и ноль нового ролика
        # снова получает право стереть его сохранённую позицию.
        self.assertIn(
            "youtubePlaybackStartedRef.current = false;",
            source,
            "флаг «играли» перестал сбрасываться при смене ролика",
        )

    def test_client_remembers_more_than_one_video(self):
        """Сторож на фронт: кеш телефона помнит карту роликов, а не один последний.

        Раньше переключение на другое видео затирало позицию предыдущего насмерть.
        """
        source = io.open(APP_JSX, encoding="utf-8").read()

        self.assertIn("const readYoutubeResumeSecondsFor", source)
        self.assertIn("YOUTUBE_RESUME_MEMORY_LIMIT", source)
        self.assertTrue(
            re.search(r"byId:\s*nextMap", source),
            "карта позиций byId больше не записывается в localStorage",
        )


if __name__ == "__main__":
    unittest.main()
