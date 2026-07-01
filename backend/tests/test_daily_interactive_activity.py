"""Daily «Итоги дня» digest counting: get_daily_interactive_activity() must aggregate
EVERY fresh interactive type from its own answer table (all channels), map rows to
{user_id: {category: [answered, correct]}}, and stay in sync with the digest's category
labels. Regression guard for the undercount where the digest saw only the Mini-App-fed
bt_3_challenge_results (screenshot: "1 ответил · Aufgabe" despite a full day of answers).

The per-table SQL is Postgres-specific (FILTER, ::int, INTERVAL) so it's verified live via
/admin_digest; here we mock the DB to lock the query shape + row mapping deterministically."""
import unittest
from contextlib import contextmanager
from unittest.mock import patch

import backend.database as db


class _FakeCursor:
    def __init__(self, rows):
        self._rows = rows
        self.executed_sql = None
        self.executed_params = None

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        self.executed_sql = sql
        self.executed_params = list(params or [])

    def fetchall(self):
        return self._rows


class _FakeConn:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor


class ActivityAggregationTests(unittest.TestCase):
    def _run(self, rows):
        cur = _FakeCursor(rows)

        @contextmanager
        def _fake_ctx(*a, **k):
            yield _FakeConn(cur)

        with patch.object(db, "get_db_connection_context", _fake_ctx):
            result = db.get_daily_interactive_activity(24)
        return result, cur

    def test_maps_rows_to_nested_dict(self):
        rows = [
            (111, "au", 3, 2),
            (111, "rb", 1, 1),
            (222, "poll", 5, 4),
        ]
        result, _ = self._run(rows)
        self.assertEqual(result[111]["au"], [3, 2])
        self.assertEqual(result[111]["rb"], [1, 1])
        self.assertEqual(result[222]["poll"], [5, 4])
        self.assertNotIn(222, {k: v for k, v in result.items() if "au" in v})

    def test_one_param_per_union_part_all_since_hours(self):
        _, cur = self._run([])
        # 10 per-item tables + 2 listening subqueries + 3 sprint/battle tables = 15 parts,
        # each parameterised by the same window → 15 identical params, 15 placeholders.
        self.assertEqual(len(cur.executed_params), 15)
        self.assertTrue(all(p == 24 for p in cur.executed_params))
        self.assertEqual(cur.executed_sql.replace("%%", "").count("%s"), 15)

    def test_covers_all_channel_independent_tables(self):
        # The whole point of the fix: read each type's OWN table (both channels), not
        # only challenge_results. Assert every interactive answer table is queried.
        _, cur = self._run([])
        for table in (
            "bt_3_article_quiz_answers", "bt_3_telegram_quiz_attempts", "bt_3_image_quiz_answers",
            "bt_3_visual_riddle_answers", "bt_3_rebus_answers", "bt_3_crossword_answers",
            "bt_3_anagram_answers", "bt_3_aufgabe_answers", "bt_3_numdict_answers",
            "bt_3_quiz_freeform_answers", "bt_3_listening_answers", "bt_3_sprint_results",
            "bt_3_article_sprint_results", "bt_3_adjektiv_sprint_results",
        ):
            self.assertIn(table, cur.executed_sql, f"{table} not aggregated")

    def test_excludes_review_and_practice_tables(self):
        # SRS/FSRS review + self-paced practice are out of scope by design.
        _, cur = self._run([])
        for excluded in ("bt_3_numdict_practice_answers", "bt_3_card_review_log", "bt_3_aufgabe_mistakes"):
            self.assertNotIn(excluded, cur.executed_sql, f"{excluded} must not be counted")

    def test_every_emitted_category_has_a_digest_label(self):
        # Guard against drift: if the DB function emits a category the digest can't label,
        # it silently vanishes from the card. Import lazily (bot_3 pulls telegram at import).
        import re
        src = __import__("inspect").getsource(db.get_daily_interactive_activity)
        emitted = set(re.findall(r"'([a-z]+)' AS category", src))
        from bot_3 import _DIGEST_CATEGORIES
        labelled = {c for c, _ in _DIGEST_CATEGORIES}
        self.assertTrue(emitted <= labelled, f"unlabelled categories: {emitted - labelled}")


if __name__ == "__main__":
    unittest.main()
