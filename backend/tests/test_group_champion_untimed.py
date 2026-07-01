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

    def test_sprint_set_is_one_task_scored_by_completion(self):
        # Artikel/Adjektiv sets: ONE task each, fractional score {0,0.5,1} -> 0/5/10 points,
        # solved at >=0.5, untimed (no gold). Never per-word (that would let a 143-word set
        # dominate the champion).
        rows = [
            {"challenge_key": "ast:s1", "user_id": 1, "name": "A", "is_correct": True, "time_ms": None, "score": 1.0},
            {"challenge_key": "ast:s2", "user_id": 1, "name": "A", "is_correct": True, "time_ms": None, "score": 0.5},
            {"challenge_key": "ad:s3", "user_id": 1, "name": "A", "is_correct": False, "time_ms": None, "score": 0.0},
        ]
        lb = compute_quiz_leaderboard(rows)
        a = {l["user_id"]: l for l in lb["leaders"]}[1]
        self.assertEqual(a["answered"], 3)   # 3 sets = 3 tasks (not word counts)
        self.assertEqual(a["points"], 15)    # 10 + 5 + 0
        self.assertEqual(a["correct"], 2)    # the 1.0 and 0.5 sets solved; 0.0 not
        self.assertEqual(a["golds"], 0)      # untimed → no gold
        self.assertIsNone(lb["fastest"])     # no timed rows

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

    def test_get_leaderboard_rows_since_merges_all_three_sources(self):
        timed = [{"challenge_key": "rb:1", "user_id": 1, "name": "A", "is_correct": True, "time_ms": 3000}]
        untimed = [{"challenge_key": "cw:7", "user_id": 2, "name": "", "is_correct": True, "time_ms": None}]
        sprint = [{"challenge_key": "ast:s1", "user_id": 3, "name": "", "is_correct": True, "time_ms": None, "score": 1.0}]
        with patch.object(db, "get_challenge_results_since", return_value=timed), \
             patch.object(db, "get_group_untimed_answers_since", return_value=untimed), \
             patch.object(db, "get_sprint_leaderboard_rows_since", return_value=sprint):
            rows = get_leaderboard_rows_since(24)
        self.assertEqual(len(rows), 3)
        self.assertIn(3000, [r["time_ms"] for r in rows])         # timed
        self.assertIn("ast:s1", [r["challenge_key"] for r in rows])  # sprint

    def test_extra_source_failure_is_non_fatal(self):
        timed = [{"challenge_key": "rb:1", "user_id": 1, "name": "A", "is_correct": True, "time_ms": 3000}]
        def _boom(*a, **k):
            raise RuntimeError("db down")
        with patch.object(db, "get_challenge_results_since", return_value=timed), \
             patch.object(db, "get_group_untimed_answers_since", _boom), \
             patch.object(db, "get_sprint_leaderboard_rows_since", _boom):
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
