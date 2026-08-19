"""Личная тренировка не перемешивает то, что база уже расставила по остыванию.

Вопрос владельца 19.08.2026: «если я второй раз выберу эту тему, мне опять покажут
те же 50 слов, которые я уже прошёл? человек не будет заниматься, если ему постоянно
одни и те же слова попадаются».

Замер показал, что механизм против этого есть, но его результат выбрасывался.
`get_article_sprint_verified_sample(..., user_id=...)` отдаёт слова в порядке
`ORDER BY cooling, random()`: недавно взятое верно уходит в КОНЕЦ очереди (сроки
10 / 30 / 90 дней — чем чаще человек отвечал верно, тем дольше слово не всплывает).
Внутри каждой группы порядок и так случайный. А следующей строкой стоял
`random.shuffle`, который перемешивал всё заново.

Почему это решает дело, хотя в набор всё равно попадает вся тема: за две минуты
человек доходит примерно до 72-го слова, а в теме их, например, 118. Значит важен
не состав набора, а ПОРЯДОК — ровно то, что перемешивание и уничтожало.

Замер на живых данных владельца (тема «Кухня и посуда», 118 слов, 32 он уже брал
верно, 11 из них в окне остывания): из первых 72 слов знакомых было 17, после
перемешивания — 21. Чем больше человек выучил, тем больше эта строка отнимает.
"""
import datetime
import unittest
from unittest.mock import patch

import backend.article_sprint_sets as sets


class PracticeSetKeepsTheDatabaseOrderTests(unittest.TestCase):
    DAY = datetime.date(2026, 8, 20)
    USER = 117649764

    # Так отдаёт база: сначала не остывшие (новые и те, чей срок вышел),
    # в конце — недавно взятые верно.
    FRESH = [{"w": f"Neu{i}", "a": "die", "ru": "", "tg": False} for i in range(30)]
    COOLING = [{"w": f"Bekannt{i}", "a": "der", "ru": "", "tg": False} for i in range(10)]

    def _build(self):
        saved: dict = {}
        with patch("backend.database.get_article_sprint_verified_sample",
                   return_value=self.FRESH + self.COOLING), \
             patch("backend.database.upsert_article_sprint_set",
                   side_effect=lambda **kw: saved.update(kw)), \
             patch("backend.article_sprint_generator.resolve_article",
                   side_effect=lambda word, article: article):
            result = sets.build_practice_set("kueche_geschirr", self.USER, self.DAY)
        return result, saved

    def test_known_words_stay_at_the_end_of_the_queue(self):
        _, saved = self._build()
        order = [w["w"] for w in saved["words"]]
        first_known = min(order.index(w["w"]) for w in self.COOLING)
        last_fresh = max(order.index(w["w"]) for w in self.FRESH)
        self.assertGreater(
            first_known, last_fresh,
            "недавно выученные слова снова уехали в начало игры — перемешивание вернулось")

    def test_the_whole_theme_still_reaches_the_set(self):
        # Остывшие слова не выбрасываются, а именно уходят в конец: иначе у человека,
        # знающего половину темы, набор схлопнется вдвое.
        result, saved = self._build()
        self.assertEqual(result["status"], "ready")
        self.assertEqual(len(saved["words"]), len(self.FRESH) + len(self.COOLING))

    def test_the_personal_set_asks_the_database_for_this_user(self):
        # Без user_id остывание не считается вовсе — набор станет как общий.
        with patch("backend.database.get_article_sprint_verified_sample",
                   return_value=self.FRESH) as sample, \
             patch("backend.database.upsert_article_sprint_set"), \
             patch("backend.article_sprint_generator.resolve_article",
                   side_effect=lambda word, article: article):
            sets.build_practice_set("kueche_geschirr", self.USER, self.DAY)
        self.assertEqual(sample.call_args.kwargs.get("user_id"), self.USER)


if __name__ == "__main__":
    unittest.main()
