"""Group champion / quiz leaderboard must count Telegram-GROUP answers, not only the
Mini-App in-place path. Those group answers live in per-type tables with no timing, so:
  - compute_quiz_leaderboard credits base points but no speed bonus / gold for untimed rows;
  - get_leaderboard_rows_since merges timed challenge_results + untimed group rows.
Regression guard for group champions being empty/undercounted when members tap answers in
the group instead of opening the Mini-App. The per-type SQL (joins/dedup) is verified live."""
import unittest
from contextlib import contextmanager
from unittest.mock import patch

from backend.quiz_leaderboard import compute_quiz_leaderboard, get_leaderboard_rows_since
import backend.database as db


class UntimedScoringTests(unittest.TestCase):
    def test_untimed_group_answer_scores_base_points_but_no_gold(self):
        rows = [
            {"challenge_key": "rb:1", "user_id": 1, "name": "A", "is_correct": True, "time_ms": 3000},
            {"challenge_key": "rb:1", "user_id": 2, "name": "B", "is_correct": True, "time_ms": None},
            {"challenge_key": "au:9", "user_id": 2, "name": "B", "is_correct": True, "time_ms": None},
        ]
        lb = compute_quiz_leaderboard(rows)
        by = {l["user_id"]: l for l in lb["leaders"]}
        self.assertEqual((by[1]["points"], by[1]["golds"]), (15, 1))   # timed fastest → +10 +5 gold
        self.assertEqual((by[2]["points"], by[2]["golds"]), (20, 0))   # 2 untimed correct → +10 each
        self.assertEqual(by[2]["ctime_n"], 0)                          # untimed excluded from "fastest"
        self.assertEqual(lb["fastest"]["user_id"], 1)

    def test_all_timed_rows_behave_exactly_as_before(self):
        # Backward compat: with real timings, placement/gold unchanged.
        rows = [
            {"challenge_key": "rb:1", "user_id": 1, "name": "A", "is_correct": True, "time_ms": 5000},
            {"challenge_key": "rb:1", "user_id": 2, "name": "B", "is_correct": True, "time_ms": 2000},
        ]
        lb = compute_quiz_leaderboard(rows)
        by = {l["user_id"]: l for l in lb["leaders"]}
        self.assertEqual(by[2]["golds"], 1)   # B faster → gold
        self.assertEqual(by[1]["golds"], 0)
        self.assertEqual(by[2]["points"], 15)  # 1st: +10 +5
        self.assertEqual(by[1]["points"], 13)  # 2nd: +10 +3

    def test_get_leaderboard_rows_since_merges_timed_and_untimed(self):
        timed = [{"challenge_key": "rb:1", "user_id": 1, "name": "A", "is_correct": True, "time_ms": 3000}]
        untimed = [{"challenge_key": "cw:7", "user_id": 2, "name": "", "is_correct": True, "time_ms": None}]
        with patch.object(db, "get_challenge_results_since", return_value=timed), \
             patch.object(db, "get_group_untimed_answers_since", return_value=untimed):
            rows = get_leaderboard_rows_since(24)
        self.assertEqual(len(rows), 2)
        self.assertIn(None, [r["time_ms"] for r in rows])
        self.assertIn(3000, [r["time_ms"] for r in rows])

    def test_untimed_fetch_failure_is_non_fatal(self):
        timed = [{"challenge_key": "rb:1", "user_id": 1, "name": "A", "is_correct": True, "time_ms": 3000}]
        def _boom(*a, **k):
            raise RuntimeError("db down")
        with patch.object(db, "get_challenge_results_since", return_value=timed), \
             patch.object(db, "get_group_untimed_answers_since", _boom):
            rows = get_leaderboard_rows_since(24)   # must not raise
        self.assertEqual(rows, timed)               # falls back to timed rows only


class GroupUntimedQueryTests(unittest.TestCase):
    class _Cur:
        def __init__(self): self.sql = None; self.params = None
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def execute(self, sql, params=None): self.sql = sql; self.params = list(params or [])
        def fetchall(self): return []

    def _capture(self):
        cur = self._Cur()
        @contextmanager
        def _ctx(*a, **k):
            class C:
                def cursor(self_inner): return cur
            yield C()
        with patch.object(db, "get_db_connection_context", _ctx):
            db.get_group_untimed_answers_since(24)
        return cur

    def test_covers_challenge_typed_group_tables_and_dedups(self):
        cur = self._capture()
        for tbl in ("bt_3_rebus_answers", "bt_3_anagram_answers", "bt_3_aufgabe_answers",
                    "bt_3_quiz_freeform_answers", "bt_3_crossword_answers"):
            self.assertIn(tbl, cur.sql)
        self.assertIn("NOT EXISTS", cur.sql)                 # dedup vs challenge_results
        self.assertIn("bool_and(a.is_correct)", cur.sql)     # crossword folded to one puzzle
        self.assertEqual(len(cur.params), 10)                # 5 sources × (window + dedup window)


if __name__ == "__main__":
    unittest.main()
