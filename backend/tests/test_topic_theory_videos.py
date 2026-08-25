"""Экран «🎬 Посмотреть видео» под тренажёрами: что он показывает и как туда не попадает чужое.

Решение владельца 25.08.2026: на экране показываются ВСЕ ролики темы — «пусть человек
листает и выберет то, которое ему нужно». Значит вся ответственность за качество лежит на
том, что в пуле темы лежит, и её нельзя оставить на «покажем только избранное».

Что было в живой базе (замер 25.08.2026, 370 активных строк, 74 темы). Пул наполняют
двое: администратор руками (/addvideo) и ночной автопрогрев поиском по YouTube. Поиск
почти на любой грамматический запрос возвращает одни и те же популярные обзоры, и они
осели во всех темах подряд:

    «Die komplette B2-Grammatik in 25 Minuten»  — 51 тема
    «GAST / TELC - B1-Prüfung»                  — 49 тем
    «B1 Deutsch komplett erklärt»               — 33 темы
    «DOPPELKONNEKTOREN»                         — 24 темы
    «100 Passiv-Sätze für B2 Deutsch»           — 20 тем

177 размещений, и ни одно — под собственной темой ролика. Человек, попросивший теорию по
окончаниям прилагательных, получал обзор экзамена TELC. Для языкового приложения это тот
же класс, что выдуманная грамматика: человек уходит учить не то.

Что эти тесты не дают вернуть:

1. Экран снова начинает что-то отбирать сам («три лучших») — тогда часть подходящих
   роликов человек не увидит, а решение было обратное.
2. Уборка перестаёт держаться: upsert снова ставит is_active = TRUE, и ночной автопрогрев
   возвращает выключенные ролики в темы следующей же ночью.
3. Страж на входе исчезает, и новый ролик-обзор снова расходится по полусотне тем.
4. Кнопка появляется у темы, где показывать нечего: пустой экран под обещанием
   «рекомендуем эти видео» — обман, а не «пока пусто».
5. Сбой базы снова выглядит как «в пуле пусто».
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
        with mock.patch.object(database, "list_topic_theory_videos", return_value=[]):
            self.assertEqual(bot_3._topic_video_button_row("adjektivdeklination"), [])

    def test_curated_videos_give_a_button_with_the_topic_deeplink(self):
        with mock.patch.object(
            database, "list_topic_theory_videos",
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
        with mock.patch.object(database, "list_topic_theory_videos", side_effect=RuntimeError("db down")):
            with self.assertLogs(level="ERROR"):
                self.assertEqual(bot_3._topic_video_button_row("fragen"), [])
        self.assertNotIn("fragen", bot_3._TOPIC_VIDEO_HAS_CACHE)


class CuratedPoolQueryTests(unittest.TestCase):
    """Правила отбора живут в SQL, поэтому проверяем сам запрос: тест обязан покраснеть,
    если из него уберут условие is_curated или начнут сортировать по лайкам."""

    def test_screen_shows_the_whole_topic_pool_own_picks_first(self):
        """Решение владельца: показываем все по теме. Отбора «лучших» на экране нет,
        свои (/addvideo) идут первыми."""
        import inspect
        src = inspect.getsource(database.list_topic_theory_videos)
        self.assertIn("is_active = TRUE", src)
        self.assertIn("ORDER BY is_curated DESC, id", src)
        self.assertNotIn("is_curated = TRUE", src)   # отбора по «своим» на экране нет

    def test_upsert_never_demotes_a_hand_picked_video(self):
        import inspect
        src = inspect.getsource(database.upsert_video_recommendation)
        self.assertIn(
            "is_curated = bt_3_video_recommendations.is_curated OR EXCLUDED.is_curated", src)

    def test_blocked_video_is_not_resurrected_by_the_nightly_warmer(self):
        """Главная защита уборки. `is_active = TRUE` здесь стояло всегда, поэтому любой
        выключенный ролик возвращался в тему следующей же ночью — уборка была бы
        бессмысленной работой."""
        import inspect
        src = inspect.getsource(database.upsert_video_recommendation)
        self.assertIn("is_active = NOT bt_3_video_recommendations.is_blocked", src)
        self.assertNotIn("is_active = TRUE,", src)


class GenericOverviewGuardTests(unittest.TestCase):
    """Страж на входе: ролик-обзор не становится «теорией по теме»."""

    def test_threshold_matches_the_measured_gap(self):
        """5 тем — потолок честно тематического ролика в живых данных, 20 — начало обзоров.
        Число меняется только вместе с новым замером, иначе граница едет на глаз."""
        self.assertEqual(bot_3._GENERIC_VIDEO_TOPIC_LIMIT, 6)

    def test_warmer_checks_before_it_stores(self):
        import inspect
        src = inspect.getsource(bot_3.warm_grammar_video_pool)
        self.assertIn("count_active_topics_for_video", src)
        self.assertIn("_GENERIC_VIDEO_TOPIC_LIMIT", src)
        # Проверка стоит ДО записи в пул, а не после.
        self.assertLess(src.index("count_active_topics_for_video"),
                        src.index("upsert_video_recommendation("))

    def test_cleanup_script_uses_the_same_threshold(self):
        """Уборка накопленного и приёмка нового обязаны считать по одному правилу —
        иначе ночью вернётся ровно то, что днём убрали."""
        import io as _io
        src = _io.open("scripts/video_pool_block_generic_overviews.py", encoding="utf-8").read()
        self.assertIn("TOPIC_LIMIT = 6", src)
        # Выключаем, а не удаляем: удалённая строка вернётся первой же находкой поиска.
        self.assertIn("is_blocked = TRUE", src)
        self.assertNotIn("DELETE FROM", src)

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


class EveryTopicSurfaceOffersTheVideoTests(unittest.TestCase):
    """Кнопка обязана быть в КАЖДОМ сообщении трёх тем, а не в тех, что вспомнились.

    Живой промах 25.08.2026: вставку привязали к строке «📚 Учить…», а в сообщении
    Artikel Sprint такой строки нет («⚡ Играть (2 минуты)» + «🎯 Своя тема»), и спринт
    артиклей остался без кнопки. Владелец заметил это раньше, чем я. Тест перебирает
    раскладки в исходнике, поэтому новая раскладка без кнопки красит прогон сразу.
    """

    TOPIC_LINKS = ("ans_as_0", "ans_al_0", "ans_ad_0", "ans_adl_0", "ans_wf_0", "ans_wfl_0")

    def test_every_keyboard_with_a_topic_game_also_offers_its_videos(self):
        import io as _io
        import re as _re
        src = _io.open("bot_3.py", encoding="utf-8").read()
        lines = src.split("\n")
        misses = []
        for i, line in enumerate(lines):
            if "InlineKeyboardMarkup([" not in line:
                continue
            block = []
            for j in range(i, min(i + 16, len(lines))):
                block.append(lines[j])
                if j > i and "])" in lines[j]:
                    break
            text = "\n".join(block)
            if not any(link in text for link in self.TOPIC_LINKS):
                continue
            if "_topic_video_button_row" not in text:
                buttons = _re.findall(r'InlineKeyboardButton\(\s*"([^"]+)"', text)
                misses.append("строка %d: %s" % (i + 1, " | ".join(buttons)))
        self.assertEqual(misses, [], "Раскладки трёх тем без кнопки «Посмотреть видео»:\n" + "\n".join(misses))


if __name__ == "__main__":
    unittest.main()
