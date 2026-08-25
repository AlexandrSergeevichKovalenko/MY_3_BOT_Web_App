"""Экран «🎬 Посмотреть видео» под тренажёрами: что он показывает и когда его нет.

Зачем эти тесты. Пул роликов темы (`bt_3_video_recommendations`) наполняют ДВА разных
источника, и они не равноценны:

  • администратор руками — /addvideo <тема> <ссылки>;
  • ночной автопрогрев по запросу YouTube — warm_grammar_video_pool(), он добирает тему
    до GRAMMAR_VIDEO_WARM_TARGET (по умолчанию 4).

Замер по живой базе 25.08.2026, тема adjektivdeklination: 8 активных роликов, из них
2 отобраны руками 16.07, остальные 6 нашёл автопрогрев 27.07 — и среди них
«GAST/TELC B1-Prüfung» и «B1 Deutsch komplett erklärt»: обзор экзамена и обзор всей
грамматики B1, к окончаниям прилагательных отношения не имеющие.

Экран обещает человеку «рекомендуем посмотреть эти видео». Показать под этим обещанием
машинную находку не по теме — тот же класс дефекта, что выдуманная грамматика: человек
идёт учить не то. Поэтому:

1. на экран попадают ТОЛЬКО отобранные человеком ролики (is_curated);
2. пометка «отобрано человеком» не снимается, когда тот же ролик позже находит автопоиск;
3. если отобранного нет ни одного — кнопки в сообщении бота нет вовсе: пустой экран с
   обещанием «рекомендуем эти видео» это обман, а не «пока пусто».
"""

import unittest
from unittest import mock

import bot_3
from backend import database


class TopicVideoButtonTests(unittest.TestCase):
    def setUp(self):
        bot_3._TOPIC_VIDEO_HAS_CACHE.clear()

    def tearDown(self):
        bot_3._TOPIC_VIDEO_HAS_CACHE.clear()

    def test_no_curated_videos_means_no_button(self):
        """Тема без отобранных роликов кнопку не получает."""
        with mock.patch.object(database, "list_curated_topic_videos", return_value=[]):
            self.assertEqual(bot_3._topic_video_button_row("adjektivdeklination"), [])

    def test_curated_videos_give_a_button_with_the_topic_deeplink(self):
        with mock.patch.object(
            database, "list_curated_topic_videos",
            return_value=[{"video_id": "VkyJbx5wr4Y", "video_url": "https://youtu.be/VkyJbx5wr4Y",
                           "video_title": "Adjektivdeklination"}],
        ):
            rows = bot_3._topic_video_button_row("adjektivdeklination")
        self.assertEqual(len(rows), 1)
        self.assertEqual(len(rows[0]), 1)
        self.assertIn("ans_gv_adjektivdeklination", rows[0][0].url)
        self.assertEqual(rows[0][0].text, bot_3.TOPIC_VIDEO_BUTTON_TEXT)

    def test_database_failure_does_not_look_like_an_empty_pool(self):
        """Сбой базы прячется в «кнопки нет», но обязан быть НАЗВАН в логе и не попасть
        в кэш — иначе десять минут все сообщения уходят без кнопки из-за одной осечки."""
        with mock.patch.object(database, "list_curated_topic_videos", side_effect=RuntimeError("db down")):
            with self.assertLogs(level="ERROR"):
                self.assertEqual(bot_3._topic_video_button_row("fragen"), [])
        self.assertNotIn("fragen", bot_3._TOPIC_VIDEO_HAS_CACHE)


class CuratedPoolQueryTests(unittest.TestCase):
    """Правила отбора живут в SQL, поэтому проверяем сам запрос: тест обязан покраснеть,
    если из него уберут условие is_curated или начнут сортировать по лайкам."""

    def test_reader_asks_only_for_curated_active_rows(self):
        import inspect
        src = inspect.getsource(database.list_curated_topic_videos)
        self.assertIn("is_curated = TRUE", src)
        self.assertIn("is_active = TRUE", src)
        self.assertIn("ORDER BY id", src)

    def test_upsert_never_demotes_a_hand_picked_video(self):
        import inspect
        src = inspect.getsource(database.upsert_video_recommendation)
        self.assertIn(
            "is_curated = bt_3_video_recommendations.is_curated OR EXCLUDED.is_curated", src)

    def test_pool_read_failure_is_not_reported_as_empty(self):
        """`except Exception: return []` здесь стоял и делал сбой базы неотличимым от
        «в пуле пусто» — ночной подбор теории молча решал, что роликов нет."""
        import inspect
        src = inspect.getsource(database.list_active_video_recommendations_for_focus)
        # Комментарии отбрасываем: в них слова «return []» стоят как раз в объяснении,
        # почему так делать нельзя, и тест не должен ловить собственное объяснение.
        code = "\n".join(
            line for line in src.split("\n") if not line.lstrip().startswith("#")
        )
        self.assertIn("raise", code)
        self.assertNotIn("return []", code)


if __name__ == "__main__":
    unittest.main()
