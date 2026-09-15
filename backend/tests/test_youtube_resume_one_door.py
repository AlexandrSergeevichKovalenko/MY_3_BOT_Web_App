"""«Продолжить с места» — одна дверь вместо трёх (15.09.2026).

Владелец: «место ИНОГДА запоминается, а иногда нет. Бывают сбои, когда видео начинается
с начала». Замер живой базы: сохранение было ЖИВО (28 строк, 3 человека, нулей 0) —
ломалось ВОЗВРАЩЕНИЕ. Вернуть позицию умели три двери с разными правилами:

1. onReady — перемотка из памяти телефона. На холодном запуске не работала НИ РАЗУ:
   эффект-уборщик по пустому полю ввода стирал карту позиций раньше, чем ею успевали
   воспользоваться, и записывал туда ноль.
2. Ответ сервера — перемотка при условии `savedTime > youtubeCurrentTimeRef.current + 1`.
   Это число живёт дольше плеера и ролика: при возврате в раздел там лежала та же самая
   секунда того же ролика, и условие не выполнялось НИКОГДА.
3. Смена ролика через cueVideoById — не делалось ничего.

Отсюда «иногда»: закрыл мини-апп и открыл — работало; ушёл в другой раздел и вернулся —
нет. Теперь секунда решается ДО постройки плеера и передаётся плееру при рождении
(playerVars.start / cueVideoById(id, сек)) — штатный вход YouTube. Перематывать после
готовности нечего, весь класс гонок с опросом раз в 400 мс исчез.

Здесь стерегутся обе половины: серверная (длина ролика, журнал исходов) и клиентская
(по исходнику App.jsx — так же, как клиентская половина починки от 29.08.2026).
"""

import io
import os
import unittest
from unittest.mock import patch

import backend.backend_server as server


APP_JSX = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "frontend", "src", "App.jsx",
)


class ResumeServerSideTests(unittest.TestCase):
    """Сервер принимает длину ролика и исход восстановления."""

    def setUp(self) -> None:
        self.client = server.app.test_client()

    def _post(self, body: dict):
        captured = {}
        outcomes = []

        def fake_upsert(**kwargs):
            captured.update(kwargs)
            return {"video_id": kwargs.get("video_id"), "current_time_seconds": kwargs.get("current_time_seconds")}

        def fake_outcome(**kwargs):
            outcomes.append(kwargs)
            return True

        patches = [
            patch.object(server, "_telegram_hash_is_valid", return_value=True),
            patch.object(server, "_parse_telegram_init_data", return_value={"user": {"id": 555000111}}),
            patch.object(server, "upsert_youtube_watch_state", side_effect=fake_upsert),
            patch.object(server, "record_youtube_resume_outcome", side_effect=fake_outcome),
        ]
        for p in patches:
            p.start()
        try:
            response = self.client.post("/api/webapp/youtube/state", json=body)
        finally:
            for p in patches:
                p.stop()
        return response, captured, outcomes

    def _body(self, **extra):
        body = {
            "initData": "valid",
            "videoId": "wxXTLW1tbM0",
            "input": "https://youtu.be/wxXTLW1tbM0",
            "current_time_seconds": 1200,
            "playback_started": True,
        }
        body.update(extra)
        return body

    def test_duration_reaches_the_database(self):
        """Без длины нельзя отличить «остановился на 4526-й» от «досмотрел до конца»."""
        response, captured, _ = self._post(self._body(duration_seconds=4527))

        self.assertEqual(response.status_code, 200)
        self.assertEqual(captured.get("duration_seconds"), 4527)

    def test_missing_duration_is_not_a_zero(self):
        """Старый бандл длины не знает. Это «не сказали», а не «ролик нулевой длины»:
        ноль включил бы правило «досмотрено» на первой же секунде."""
        response, captured, _ = self._post(self._body())

        self.assertEqual(response.status_code, 200)
        self.assertIsNone(captured.get("duration_seconds"))

    def test_resume_outcome_is_recorded(self):
        response, _, outcomes = self._post(self._body(
            resume_outcome="restored", resume_saved_seconds=1200, resume_started_seconds=1200,
        ))

        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(outcomes), 1)
        self.assertEqual(outcomes[0].get("outcome"), "restored")
        self.assertEqual(outcomes[0].get("saved_seconds"), 1200)

    def test_no_outcome_means_no_record(self):
        """Исход сообщается ОДИН раз на открытие ролика; остальные сохранения молчат,
        и молчание не превращается в выдуманную строку журнала."""
        response, _, outcomes = self._post(self._body())

        self.assertEqual(response.status_code, 200)
        self.assertEqual(outcomes, [])

    def test_a_failing_journal_never_costs_the_position(self):
        """Побочная запись не имеет права утащить за собой главную: позиция сохранена,
        даже если журнал исходов упал."""
        captured = {}

        def fake_upsert(**kwargs):
            captured.update(kwargs)
            return {"video_id": kwargs.get("video_id"), "current_time_seconds": kwargs.get("current_time_seconds")}

        patches = [
            patch.object(server, "_telegram_hash_is_valid", return_value=True),
            patch.object(server, "_parse_telegram_init_data", return_value={"user": {"id": 555000111}}),
            patch.object(server, "upsert_youtube_watch_state", side_effect=fake_upsert),
            patch.object(server, "record_youtube_resume_outcome", side_effect=RuntimeError("журнал недоступен")),
        ]
        for p in patches:
            p.start()
        try:
            response = self.client.post("/api/webapp/youtube/state", json=self._body(resume_outcome="restored"))
        finally:
            for p in patches:
                p.stop()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(captured.get("current_time_seconds"), 1200)


class ResumeJournalTests(unittest.TestCase):
    """Журнал исходов не принимает выдуманных значений."""

    def test_unknown_outcome_is_refused(self):
        from backend.database import YOUTUBE_RESUME_OUTCOMES, record_youtube_resume_outcome

        self.assertEqual(
            set(YOUTUBE_RESUME_OUTCOMES),
            {"restored", "no_saved_position", "finished", "lookup_failed", "lost"},
        )
        # База тут не нужна: неизвестный исход обязан отсеяться ДО обращения к ней.
        self.assertFalse(record_youtube_resume_outcome(
            user_id=1, video_id="abc", outcome="что-нибудь",
        ))
        self.assertFalse(record_youtube_resume_outcome(
            user_id=1, video_id="", outcome="restored",
        ))


class ResumePromiseTests(unittest.TestCase):
    """Обещание зарегистрировано и меряет именно потерю места."""

    def test_promise_is_registered(self):
        from backend.fix_promises import PROMISES

        by_key = {p.key: p for p in PROMISES}
        self.assertIn("youtube_resume_lost", by_key)
        promise = by_key["youtube_resume_lost"]
        self.assertEqual(promise.expected, 0)
        self.assertEqual(promise.since, "15.09.2026")
        self.assertIsNotNone(promise.screen, "экран «после» обязан приходить сам")


class ResumeClientSideTests(unittest.TestCase):
    """Сторожа по исходнику App.jsx: дверь обязана остаться ОДНОЙ."""

    def setUp(self):
        self.source = io.open(APP_JSX, encoding="utf-8").read()

    def test_player_is_born_on_the_saved_second(self):
        self.assertTrue(
            "start: resumeStartSeconds," in self.source,
            "плеер перестал рождаться на сохранённой секунде (playerVars.start)",
        )
        self.assertTrue(
            "cueVideoById(youtubeId, resumeStartSeconds)" in self.source,
            "смена ролика снова не передаёт стартовую секунду",
        )

    def test_the_player_waits_until_the_second_is_decided(self):
        """'unknown' — это не ноль. Плеер, рождённый на нуле, пришлось бы перематывать
        после готовности, и гонка вернулась бы целиком."""
        self.assertTrue(
            "if (youtubeResumeStart.status !== 'ready' || youtubeResumeStart.videoId !== youtubeId) {" in self.source,
            "сняты ворота: плеер снова строится, не дождавшись стартовой секунды",
        )

    def test_the_second_door_stays_removed(self):
        """Перемотка по живому плееру после ответа сервера — это и была вторая дверь."""
        self.assertFalse(
            "savedTime > (youtubeCurrentTimeRef.current + 1)" in self.source,
            "вернулось сравнение сохранённой секунды с числом, живущим дольше плеера",
        )
        self.assertFalse(
            "youtubeResumeAppliedForVideoRef" in self.source,
            "вернулась пометка «этому ролику уже вернули», не знавшая о возврате в раздел",
        )
        self.assertTrue(
            "УДАЛЕНО 15.09.2026. ВТОРАЯ ДВЕРЬ ВОССТАНОВЛЕНИЯ. НЕ ВОЗВРАЩАТЬ." in self.source,
            "исчез вердикт в коде — следующий агент не узнает, что это уже разобрано",
        )

    def test_the_phone_map_survives_a_cold_start(self):
        """Уборщик по пустому полю ввода сносил карту 50 роликов на каждом запуске."""
        self.assertTrue("const forgetYoutubeLastVideoKeepingMap" in self.source,
                        "исчез точечный забыватель последнего ролика")
        self.assertFalse(
            "safeStorageRemove(youtubeResumeStorageKey);" in self.source,
            "карту позиций снова сносят целиком при пустом поле ввода",
        )

    def test_a_silent_network_failure_is_impossible(self):
        """«Сеть не ответила» и «позиции нет» — разные миры; молчать о первом нельзя."""
        self.assertTrue("'lookup_failed'" in self.source,
                        "исчез отдельный исход «не смогли спросить сервер»")
        self.assertTrue(
            "Не смогли вспомнить, где вы остановились" in self.source,
            "исчезла строка, которой мы объясняем человеку старт с начала",
        )

    def test_finished_video_starts_over_and_says_so(self):
        self.assertTrue("const YOUTUBE_WATCHED_SHARE = 0.95;" in self.source,
                        "исчез порог «досмотрено»")
        self.assertTrue(
            "Вы досмотрели этот ролик — начинаем сначала." in self.source,
            "правило «досмотрено» молчит — для человека это неотличимо от поломки",
        )

    def test_a_hanging_network_cannot_leave_the_player_unbuilt(self):
        """Ворота ждут решателя. Если бы он ждал сеть вечно, зависшая сеть означала бы
        «плеера нет вообще» — хуже исходного дефекта."""
        self.assertTrue(
            "controller.abort();" in self.source and "}, 4000);" in self.source,
            "снят предел ожидания ответа сервера — зависшая сеть оставит человека без плеера",
        )

    def test_duration_is_never_sent_as_zero(self):
        """Ноль длины включил бы «досмотрено» на первой секунде."""
        self.assertTrue(
            "...(youtubeDurationRef.current > 0" in self.source,
            "длина ролика снова уходит на сервер даже когда плеер её не назвал",
        )


if __name__ == "__main__":
    unittest.main()
