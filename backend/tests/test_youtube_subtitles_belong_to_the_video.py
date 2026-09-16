"""Субтитры на экране принадлежат тому ролику, который играет (16.09.2026).

Владелец: «Я открыл видео из папки фильмы, и у меня совершенно другие субтитры под ним
отображаются из предыдущего видео». На экране одновременно: НЕМЕЦКАЯ дорожка от
прошлого ролика (документалка про Hells Angels) и РУССКАЯ — уже от нового (стендап).

Почему так было. Запрос субтитров живёт долго: на 202 клиент опрашивает статус до 25 раз
примерно по 1.2 с, то есть около 30 секунд. Человек за это время переключает ролик.
Старый ответ возвращался ПОСЛЕ переключения и молча ложился в состояние — проверки
«а к тому ли ролику этот ответ» не было НИ В ОДНОМ месте записи (ни в
applyYoutubeTranscriptPayload, ни в ветках ошибок fetchTranscript).

Немецкое и русское расходились потому, что русский догружается отдельным запросом
translate_rows по НОМЕРАМ реплик ТЕКУЩЕГО ролика. Ярлыки строк у разных роликов
совпадают по форме, поэтому свежий перевод ложился на чужой немецкий текст — и выглядело
это как «субтитры не подтягиваются».

Правило теперь одно: любая запись в состояние субтитров после await сверяется с ролИком,
для которого она собиралась, и чужая НЕ принимается. Отброшенные считаются и уходят
числом в отчёт — тихо ронять и не считать нельзя.
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


class StaleSubtitleGuardsInClientTests(unittest.TestCase):
    def setUp(self):
        self.source = io.open(APP_JSX, encoding="utf-8").read()

    def test_every_write_names_its_video(self):
        self.assertTrue(
            "const youtubeSubtitlePayloadIsStale = (forVideoId, where) => {" in self.source,
            "исчезла проверка «ответ про тот ли ролик»",
        )
        self.assertTrue(
            "const applyYoutubeTranscriptPayload = (data, forVideoId) => {" in self.source,
            "приёмник субтитров снова не требует назвать ролик — чужой ответ пройдёт молча",
        )
        self.assertTrue(
            "if (youtubeSubtitlePayloadIsStale(forVideoId, 'applyYoutubeTranscriptPayload')) return;" in self.source,
            "приёмник субтитров перестал отбрасывать чужой ответ",
        )

    def test_the_request_pins_its_video_before_the_first_await(self):
        self.assertTrue(
            "const requestedVideoId = String(youtubeId).trim();" in self.source,
            "запрос субтитров перестал фиксировать ролик до первого await",
        )
        self.assertTrue(
            "applyYoutubeTranscriptPayload(data, requestedVideoId);" in self.source,
            "ответ субтитров применяется без указания ролика",
        )
        self.assertEqual(
            self.source.count("applyYoutubeTranscriptPayload(data, requestedVideoId);"),
            2,
            "один из двух путей (обычный ответ и ответ после опроса статуса) остался без проверки",
        )

    def test_error_paths_cannot_wipe_a_foreign_video(self):
        """«У ЭТОГО ролика субтитров нет» не имеет права стереть субтитры ДРУГОГО,
        который человек уже открыл."""
        self.assertTrue(
            "if (youtubeSubtitlePayloadIsStale(requestedVideoId, 'not_cached')) return;" in self.source,
            "ветка «субтитров нет в кеше» снова чистит экран чужого ролика",
        )
        self.assertTrue(
            "if (youtubeSubtitlePayloadIsStale(requestedVideoId, 'fetchTranscript:catch')) return;" in self.source,
            "ветка ошибки снова чистит экран чужого ролика",
        )

    def test_polling_stops_when_the_video_changes(self):
        """Иначе опрос крутится все ~30 секунд и в конце кладёт чужие субтитры."""
        self.assertTrue(
            "// Человек ушёл на другой ролик — спрашивать про этот больше незачем." in self.source,
            "опрос статуса субтитров снова не замечает смены ролика",
        )

    def test_no_answer_is_not_an_empty_answer(self):
        """Опрос прекращён — это «ответа нет», а не «субтитров нет». Пустой список сюда
        подставлять нельзя: он стёр бы то, что уже на экране."""
        self.assertTrue(
            "if (data == null) return;" in self.source,
            "отсутствие ответа снова превращается в пустые субтитры",
        )

    def test_the_russian_track_checks_the_video_too(self):
        """Ровно здесь свежий русский ложился на чужой немецкий текст."""
        self.assertTrue(
            "const translationVideoId = String(youtubeId).trim();" in self.source,
            "перевод строк перестал фиксировать ролик до запроса",
        )
        self.assertTrue(
            "if (youtubeSubtitlePayloadIsStale(translationVideoId, 'translate_rows')) return;" in self.source,
            "перевод строк снова применяется без проверки ролика",
        )

    def test_the_loading_flag_is_released_when_the_video_changes(self):
        """Самое коварное место. Флаг «идёт загрузка» — общий, а запросов бывает два:
        старый ещё летит, человек уже на новом ролике. Ворота автозагрузки смотрят на
        этот флаг и молча выходят, пока он поднят, — и субтитры нового видео не
        запрашиваются НИКОГДА. Кнопку «Загрузить субтитры» человек тоже не увидит:
        плашку прячет признак «субтитры есть», посчитанный по ЧУЖИМ данным.

        Без этой строки отбрасывание чужого ответа делает только хуже: старые субтитры
        с экрана уходят, а новые не приходят."""
        self.assertEqual(
            self.source.count("      setYoutubeTranscriptLoading(false);\n      setYoutubeTranscriptError('');"),
            2,
            "флаг загрузки субтитров перестал опускаться при смене ролика — "
            "автозагрузка нового видео залипнет навсегда",
        )

    def test_manual_paste_uses_the_same_single_door(self):
        """Ручная вставка админом (16.09.2026, решение владельца «do it»).

        Здесь текст записывался ПОД УСЛОВИЕМ, а предложения — безусловно: два состояния
        одного и того же текста по разным правилам. Ответ без текста — и на экране
        предложения из ответа поверх старого текста. Тот же класс, что наложение чужих
        субтитров, только без гонки.

        Сервер собирает текст и предложения ИЗ ОДНОГО списка и пустыми не отвечает
        (на пустых items — 400), поэтому ответ принимается целиком тем же приёмником,
        что и все остальные субтитры: он же сверяет ролик."""
        self.assertFalse(
            "if (Array.isArray(saved.items) && saved.items.length) setYoutubeTranscript(saved.items);" in self.source,
            "текст и предложения ручной вставки снова пишутся по разным правилам",
        )
        self.assertTrue(
            "applyYoutubeTranscriptPayload(saved, requestedVideoId);" in self.source,
            "ручная вставка снова не идёт через единственную дверь приёма субтитров",
        )

    def test_manual_paste_never_reports_success_it_did_not_see(self):
        """Тело ответа не прочиталось — сервер всё равно СОХРАНИЛ. Молчание оставило бы
        человека со старым текстом и мыслью, что вставка не сработала."""
        # Проверяем ТОЛЬКО тело saveManualTranscriptToDb: такая же конструкция есть в
        # других, не относящихся к делу местах приложения, и трогать их никто не просил.
        body = self.source.split("const saveManualTranscriptToDb = async (items) => {", 1)
        self.assertEqual(len(body), 2, "исчезла функция сохранения ручной вставки")
        body = body[1].split("\n  };", 1)[0]
        self.assertFalse(
            "response.json().catch(" in body,
            "вернулось молчаливое проглатывание неразобранного ответа ручной вставки",
        )
        self.assertTrue(
            "Субтитры сохранены, но панель не обновилась. Откройте ролик заново." in self.source,
            "исчезло honest-сообщение о сохранённых, но не показанных субтитрах",
        )

    def test_dropped_answers_are_counted_not_swallowed(self):
        self.assertTrue(
            "youtubeStaleSubtitleDropsRef.current += 1;" in self.source,
            "отброшенные чужие ответы перестали считаться — о гонке никто не узнает",
        )
        self.assertTrue(
            "{ stale_subtitle_drops: youtubeStaleSubtitleDropsRef.current }" in self.source,
            "счёт отброшенных перестал уходить на сервер",
        )


class StaleSubtitleCounterServerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = server.app.test_client()

    def test_counter_reaches_the_journal(self):
        recorded = []

        def fake_anomaly(**kwargs):
            recorded.append(kwargs)
            return True

        patches = [
            patch.object(server, "_telegram_hash_is_valid", return_value=True),
            patch.object(server, "_parse_telegram_init_data", return_value={"user": {"id": 777000333}}),
            patch.object(server, "upsert_youtube_watch_state", return_value={"video_id": "abc"}),
            patch.object(server, "record_youtube_client_anomaly", side_effect=fake_anomaly),
        ]
        for p in patches:
            p.start()
        try:
            response = self.client.post("/api/webapp/youtube/state", json={
                "initData": "valid",
                "videoId": "abc",
                "current_time_seconds": 30,
                "playback_started": True,
                "stale_subtitle_drops": 3,
            })
        finally:
            for p in patches:
                p.stop()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(recorded), 1)
        self.assertEqual(recorded[0].get("kind"), "stale_subtitle_payload")
        self.assertEqual(recorded[0].get("amount"), 3)

    def test_unknown_anomaly_kind_is_refused(self):
        from backend.database import record_youtube_client_anomaly

        self.assertFalse(record_youtube_client_anomaly(
            user_id=1, video_id="abc", kind="что-нибудь", amount=1,
        ))
        self.assertFalse(record_youtube_client_anomaly(
            user_id=1, video_id="abc", kind="stale_subtitle_payload", amount=0,
        ))

    def test_promise_is_registered(self):
        from backend.fix_promises import PROMISES

        by_key = {p.key: p for p in PROMISES}
        self.assertIn("youtube_stale_subtitles", by_key)
        self.assertEqual(by_key["youtube_stale_subtitles"].expected, 0)
        self.assertIsNotNone(by_key["youtube_stale_subtitles"].screen)


if __name__ == "__main__":
    unittest.main()
