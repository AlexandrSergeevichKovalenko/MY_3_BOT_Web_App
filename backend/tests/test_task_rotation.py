"""Правило личной ротации: каждому своё задание из общего банка.

Замер 14.08.2026: сегодня одно задание выбирается ОДИН раз до цикла рассылки и уходит
всем сразу, поэтому «сколько разных заданий получили разные люди» равно единице по
определению. Здесь проверяется само правило — без базы, чтобы прогон был быстрым и не
зависел от живых данных.
"""

from datetime import datetime, timedelta, timezone
import unittest

from backend.task_rotation import (
    LADDER_DAYS, WRONG_LADDER_DAYS, next_state, order_candidates,
)

NOW = datetime(2026, 8, 15, 12, 0, tzinfo=timezone.utc)


class LadderTests(unittest.TestCase):
    """Решение владельца 14.08.2026: верно решённое возвращать через 90 дней, потом
    через 120, потом не показывать никогда."""

    def test_first_correct_answer_returns_in_90_days(self):
        st = next_state(seen_count=0, correct_count=0, is_correct=True, now=NOW)
        self.assertEqual(st["correct_count"], 1)
        self.assertEqual(st["next_eligible_at"], NOW + timedelta(days=LADDER_DAYS[0]))
        self.assertIsNone(st["retired_at"])

    def test_second_correct_answer_returns_in_120_days(self):
        st = next_state(seen_count=1, correct_count=1, is_correct=True, now=NOW)
        self.assertEqual(st["next_eligible_at"], NOW + timedelta(days=LADDER_DAYS[1]))
        self.assertIsNone(st["retired_at"])

    def test_third_correct_answer_retires_the_task_forever(self):
        st = next_state(seen_count=2, correct_count=2, is_correct=True, now=NOW)
        self.assertEqual(st["retired_at"], NOW)
        self.assertIsNone(st["next_eligible_at"])

    def test_wrong_answer_does_not_move_the_ladder(self):
        st = next_state(seen_count=1, correct_count=1, is_correct=False, now=NOW)
        self.assertEqual(st["correct_count"], 1)
        self.assertIsNone(st["retired_at"])
        self.assertIsNone(st["next_eligible_at"],
                          "заваленное возвращает очередь работы над ошибками, не ротация")

    def test_every_answer_counts_as_seen(self):
        st = next_state(seen_count=4, correct_count=1, is_correct=False, now=NOW)
        self.assertEqual(st["seen_count"], 5)


class WrongLadderTests(unittest.TestCase):
    """Заваленное задание возвращается через 7 дней, потом 14, потом 30.

    Решение владельца 24.08.2026. Лестница положена тем видам, у которых НЕТ своей
    очереди работы над ошибками: замер 24.08.2026 — в `bt_3_aufgabe_mistakes` нет ни
    одной строки кроссворда или анаграммы, и до этой правки неверно решённый кроссворд
    не закрывался для человека вообще.
    """

    def test_first_wrong_answer_returns_in_7_days(self):
        st = next_state(seen_count=0, correct_count=0, is_correct=False, now=NOW,
                        wrong_ladder=WRONG_LADDER_DAYS)
        self.assertEqual(st["next_eligible_at"], NOW + timedelta(days=7))
        self.assertIsNone(st["retired_at"])

    def test_second_wrong_answer_returns_in_14_days(self):
        st = next_state(seen_count=1, correct_count=0, is_correct=False, now=NOW,
                        wrong_ladder=WRONG_LADDER_DAYS)
        self.assertEqual(st["next_eligible_at"], NOW + timedelta(days=14))

    def test_third_wrong_answer_returns_in_30_days(self):
        st = next_state(seen_count=2, correct_count=0, is_correct=False, now=NOW,
                        wrong_ladder=WRONG_LADDER_DAYS)
        self.assertEqual(st["next_eligible_at"], NOW + timedelta(days=30))

    def test_further_wrong_answers_stay_at_30_days_and_never_retire(self):
        """Человек задание так и не решил — отнимать его насовсем неправильно."""
        st = next_state(seen_count=9, correct_count=0, is_correct=False, now=NOW,
                        wrong_ladder=WRONG_LADDER_DAYS)
        self.assertEqual(st["next_eligible_at"], NOW + timedelta(days=30))
        self.assertIsNone(st["retired_at"])

    def test_correct_answer_after_failures_switches_to_the_long_ladder(self):
        st = next_state(seen_count=3, correct_count=0, is_correct=True, now=NOW,
                        wrong_ladder=WRONG_LADDER_DAYS)
        self.assertEqual(st["next_eligible_at"], NOW + timedelta(days=LADDER_DAYS[0]))

    def test_kinds_without_the_ladder_keep_the_old_behaviour(self):
        """Задания пула ведут себя как раньше: их повтором заведует работа над ошибками,
        и два механизма на одно задание вешать нельзя."""
        st = next_state(seen_count=0, correct_count=0, is_correct=False, now=NOW)
        self.assertIsNone(st["next_eligible_at"])

    def test_only_crossword_and_anagram_got_the_ladder(self):
        """Карта видов — там же, где выдача: случайно раздать лестницу всем нельзя."""
        from backend.database import WRONG_LADDER_BY_KIND
        self.assertEqual(set(WRONG_LADDER_BY_KIND), {"cw", "ag"})


class OrderTests(unittest.TestCase):
    def _cands(self, *keys):
        return [{"task_key": k} for k in keys]

    def test_unseen_goes_first(self):
        state = {"a": {"seen_count": 1, "correct_count": 1,
                       "next_eligible_at": NOW - timedelta(days=1),
                       "retired_at": None, "last_seen_at": NOW - timedelta(days=100)}}
        out = order_candidates(self._cands("a", "b"), state, NOW)
        self.assertEqual([c["task_key"] for c in out], ["b", "a"])

    def test_not_yet_due_goes_last(self):
        state = {
            "a": {"seen_count": 1, "correct_count": 1,
                  "next_eligible_at": NOW + timedelta(days=30),   # срок не вышел
                  "retired_at": None, "last_seen_at": NOW - timedelta(days=60)},
            "b": {"seen_count": 1, "correct_count": 1,
                  "next_eligible_at": NOW - timedelta(days=1),    # срок вышел
                  "retired_at": None, "last_seen_at": NOW - timedelta(days=91)},
        }
        out = order_candidates(self._cands("a", "b"), state, NOW)
        self.assertEqual([c["task_key"] for c in out], ["b", "a"])

    def test_oldest_first_among_equals(self):
        state = {
            "a": {"seen_count": 1, "correct_count": 1, "next_eligible_at": None,
                  "retired_at": None, "last_seen_at": NOW - timedelta(days=10)},
            "b": {"seen_count": 1, "correct_count": 1, "next_eligible_at": None,
                  "retired_at": None, "last_seen_at": NOW - timedelta(days=200)},
        }
        out = order_candidates(self._cands("a", "b"), state, NOW)
        self.assertEqual([c["task_key"] for c in out], ["b", "a"])

    def test_retired_is_dropped(self):
        state = {"a": {"seen_count": 3, "correct_count": 3, "next_eligible_at": None,
                       "retired_at": NOW - timedelta(days=5),
                       "last_seen_at": NOW - timedelta(days=5)}}
        out = order_candidates(self._cands("a", "b"), state, NOW)
        self.assertEqual([c["task_key"] for c in out], ["b"])

    def test_never_leaves_the_person_empty_handed(self):
        """Всё выброшено — всё равно что-то выдаём: пустой экран человеку недопустим."""
        state = {"a": {"seen_count": 3, "correct_count": 3, "next_eligible_at": None,
                       "retired_at": NOW - timedelta(days=5),
                       "last_seen_at": NOW - timedelta(days=5)}}
        out = order_candidates(self._cands("a"), state, NOW)
        self.assertEqual([c["task_key"] for c in out], ["a"])

    def test_empty_bank_gives_empty_result(self):
        self.assertEqual(order_candidates([], {}, NOW), [])


if __name__ == "__main__":
    unittest.main()
