"""Тема ведёт день при ЛЮБОМ своём размере. Нижнего порога у темы дня нет.

Решение владельца 19.08.2026, дословно:

    «даже если в теме есть 50 слов, то значит она может быть темой дня, неважно.
     Человеку нужно выучить все слова со всех тем, и ему совершенно всё равно,
     сколько в этой теме слов. Если их нету столько — зачем тужиться и придумывать
     что-то, чего не существует?»

Почему тест нужен именно здесь. В тот же день порог заводили ДВАЖДЫ:
  • `MIN_PLAYABLE = 60` — «ниже этого набор не выдаём»;
  • `MIN_THEME_FOR_DAILY = 140` — моя правка «тема должна быть непроходимой
    насквозь», сделанная как ответ на экран из 77 слов при 80 живых.

Оба порога — выдуманные числа, и оба воссоздают то самое давление «дорасти до
цифры», из-за которого генератор скрёб дно и набивал темы мусором. Плохим тот
экран делал МУСОР, а не размер темы. Мусор убирается стражем происхождения и
стражем подтверждения артикля; размер темы к делу не относится.

Единственная причина не выдать набор — в банке нет ни одного слова. Это не порог,
а арифметика.
"""
import datetime
import unittest
from unittest.mock import patch

import backend.article_sprint_sets as sets


def _words(n: int) -> list[dict]:
    # Разные написания, чтобы дедупликация по (слово, артикль) их не схлопнула.
    return [{"w": f"Wort{i}", "a": "der", "ru": f"слово {i}", "tg": False} for i in range(n)]


class DailySetTakesThemesOfAnySizeTests(unittest.TestCase):
    DAY = datetime.date(2026, 8, 20)

    def _build(self, *, theme_words: int, scheduled: str = "kueche_geschirr"):
        saved: dict = {}

        def _upsert(**kwargs):
            saved.update(kwargs)

        with patch.object(sets, "random"), \
             patch("backend.database.ensure_article_sprint_schema"), \
             patch("backend.database.get_article_sprint_theme_for_date", return_value=scheduled), \
             patch("backend.database.count_article_theme_verified", return_value=theme_words), \
             patch("backend.database.get_article_sprint_verified_sample",
                   side_effect=lambda key, size: _words(min(theme_words, size))), \
             patch("backend.database.upsert_article_sprint_set", side_effect=_upsert), \
             patch("backend.database.list_article_sprint_themes", return_value=[]), \
             patch("backend.article_sprint_generator.resolve_article",
                   side_effect=lambda word, article: article):
            return sets.build_daily_set(self.DAY), saved

    def test_a_small_theme_still_leads_the_day(self):
        # 50 честных слов — полноценная тема дня. Игра просто короче.
        result, saved = self._build(theme_words=50)
        self.assertEqual(result["status"], "ready")
        self.assertEqual(result["theme_key"], "kueche_geschirr")
        self.assertEqual(result["word_count"], 50)
        self.assertEqual(saved["theme_key"], "kueche_geschirr",
                         "маленькую тему подменять другой нельзя")

    def test_a_tiny_theme_is_not_swapped_either(self):
        result, _ = self._build(theme_words=7)
        self.assertEqual(result["status"], "ready")
        self.assertEqual(result["theme_key"], "kueche_geschirr")

    def test_a_big_theme_is_capped_by_what_a_player_can_get_through(self):
        # Потолок остаётся: замораживать больше, чем игрок осилит, незачем.
        result, _ = self._build(theme_words=400)
        self.assertEqual(result["word_count"], sets.DEFAULT_SET_SIZE)

    def test_the_set_is_never_topped_up_from_other_themes(self):
        # Долив из чужих тем когда-то положил медицинские слова под заголовок
        # «Technik & Computer». Короткий набор честнее чужого слова.
        _, saved = self._build(theme_words=30)
        self.assertEqual(len(saved["words"]), 30)

    def test_an_empty_bank_is_the_only_reason_to_refuse(self):
        result, _ = self._build(theme_words=0, scheduled="")
        self.assertEqual(result["status"], "insufficient")
        self.assertEqual(result["available"], 0)


class NoSizeThresholdConstantsTests(unittest.TestCase):
    """Порогов-констант в модуле быть не должно — иначе они снова начнут решать."""

    def test_the_module_has_no_minimum_size_constant(self):
        for gone in ("MIN_PLAYABLE", "MIN_THEME_FOR_DAILY"):
            self.assertFalse(
                hasattr(sets, gone),
                f"{gone} вернулся: тема дня снова обязана «дорасти до цифры»")

    def test_the_fallback_accepts_any_non_empty_theme(self):
        themes = [{"theme_key": "klein", "verified_count": 12, "active": True},
                  {"theme_key": "gross", "verified_count": 300, "active": True}]
        with patch("backend.database.list_article_sprint_themes", return_value=themes):
            picked = {sets._pick_fallback_theme(datetime.date(2026, 8, 20) +
                                                datetime.timedelta(days=d)) for d in range(2)}
        self.assertEqual(picked, {"klein", "gross"},
                         "маленькая тема обязана попадать в ротацию наравне с большой")

    def test_a_switched_off_theme_never_leads_a_day(self):
        # Погашенная тема (слитая с соседней или закрытая владельцем) не должна
        # вести день, даже если слова в ней ещё лежат. Отсутствие порога по размеру
        # не отменяет флага «активна».
        themes = [{"theme_key": "aus", "verified_count": 300, "active": False},
                  {"theme_key": "an", "verified_count": 40, "active": True}]
        with patch("backend.database.list_article_sprint_themes", return_value=themes):
            picked = {sets._pick_fallback_theme(datetime.date(2026, 8, 20) +
                                                datetime.timedelta(days=d)) for d in range(7)}
        self.assertEqual(picked, {"an"})


if __name__ == "__main__":
    unittest.main()
