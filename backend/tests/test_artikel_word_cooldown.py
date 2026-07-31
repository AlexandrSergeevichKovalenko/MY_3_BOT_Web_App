"""Выученное слово не исчезает, а приходит реже — и только тому, кто его знает.

Раньше слово, на которое ответили трое и двое угадали, снималось с показа НАВСЕГДА и у
всех. Выбивало ровно лучшее: ходовые слова спрашивают чаще и отвечают на них лучше, —
так ушли der Junge, der Gürtel, die Kollegin. Заменить их нечем: темы и так недобраны.
Теперь ничего не снимается, а у знающего человека слово уходит в конец очереди подбора.
"""
import unittest
from contextlib import contextmanager
from unittest.mock import patch

import backend.database as db


class _Cursor:
    def __init__(self):
        self.sql = ""
        self.params: tuple = ()

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        self.sql = " ".join(str(sql).split())
        self.params = tuple(params or ())

    def fetchall(self):
        return []


class _Conn:
    def __init__(self, cur):
        self._cur = cur

    def cursor(self):
        return self._cur

    def commit(self):
        pass


class ArticleSampleCooldownTests(unittest.TestCase):
    def _sample(self, **kwargs):
        cur = _Cursor()

        @contextmanager
        def _ctx(*a, **k):
            yield _Conn(cur)

        with patch.object(db, "get_db_connection_context", _ctx):
            db.get_article_sprint_verified_sample("tiere", 5, **kwargs)
        return cur

    def test_every_placeholder_gets_a_value(self):
        # Порядок %s в готовом запросе и порядок params легко разъезжаются молча:
        # запрос отработает, но сроки остывания уедут в user_id. Считаем и то, и другое.
        cur = self._sample(user_id=42)
        self.assertEqual(cur.sql.count("%s"), len(cur.params))

    def test_cooldown_values_come_before_the_user(self):
        cur = self._sample(user_id=42)
        self.assertEqual(cur.params[:5], (90, 30, 10, 42, 42),
                         "сначала три срока из SELECT, потом два user_id из JOIN")
        self.assertEqual(cur.params[5], "tiere")
        self.assertEqual(cur.params[-1], 5, "последним идёт размер набора")

    def test_the_longest_cooldown_matches_the_most_hits(self):
        # Ветки CASE идут «3+ раза / 2 раза / иначе», поэтому и сроки — от большего.
        self.assertEqual(tuple(reversed(db.ARTICLE_COOLDOWN_DAYS)), (90, 30, 10))

    def test_known_words_go_last_but_stay_available(self):
        cur = self._sample(user_id=42)
        self.assertIn("ORDER BY cooling, random()", cur.sql,
                      "остывшее уходит в конец очереди, а не выбрасывается")
        self.assertNotIn("cooling = 0", cur.sql,
                         "фильтра быть не должно: в маленькой теме набор иначе не соберётся")

    def test_answers_from_both_games_count(self):
        # Выученное в тренажёре не должно сыпаться в спринте, и наоборот.
        cur = self._sample(user_id=42)
        self.assertIn("bt_3_article_sprint_word_answers", cur.sql)
        self.assertIn("bt_3_article_learn_answers", cur.sql)

    def test_a_shared_set_has_no_personal_cooldown(self):
        # Набор дня и битвы одни на всех — «кто играет» там неизвестно.
        cur = self._sample()
        self.assertIn("ORDER BY random()", cur.sql)
        self.assertNotIn("bt_3_article_learn_answers", cur.sql)
        self.assertEqual(cur.sql.count("%s"), len(cur.params))

    def test_excluded_words_still_work_with_cooldown(self):
        cur = self._sample(user_id=42, exclude_words=["Hund"])
        self.assertEqual(cur.sql.count("%s"), len(cur.params))
        self.assertIn(["hund"], list(cur.params))


class RotationLeavesArticleWordsAloneTests(unittest.TestCase):
    def test_the_bank_is_never_shrunk_by_the_crowd(self):
        import asyncio
        import bot_3

        called = []
        with patch.object(db, "mastered_article_sprint_words",
                          lambda **k: called.append("asked") or []), \
                patch.object(db, "retire_article_sprint_nouns_by_words",
                             lambda w: called.append("retired") or 0):
            retired, added = asyncio.new_event_loop().run_until_complete(
                bot_3._rotate_article_words_domain(None, min_answerers=3))
        self.assertEqual((retired, added), (0, 0))
        self.assertEqual(called, [], "слова артиклей ротация не трогает вовсе")


if __name__ == "__main__":
    unittest.main()
