"""Anagram: the result screen must show the learner's own word, and the bank must
stop offering unplayable/repeat cards.

Live case that triggered this: `Zugehörigkeit` was assembled as `Zugehöregkiit` — two
letters swapped out of thirteen. The verdict screen showed only the correct word, so
the only available conclusion was "the tiles were wrong" (they were not: every one of
the 18 wrong answers in history used exactly the word's letters). The payload now
carries `user_answer` so the client can line the two words up letter by letter.

Pool side: the generator requires 8+ letters, but legacy cards (`Mohn` = 4 letters =
2 middle tiles) and duplicates (`Einwanderer` × 3) were still in the bank. Both are
gated where cards are CHOSEN — already delivered cards must stay openable.
"""
import unittest
from contextlib import contextmanager
from unittest.mock import patch

from backend.answer_eval import evaluate_anagram, load_anagram_task
from backend.database import (
    ANAGRAM_MIN_LETTERS, count_available_anagram_cards, create_anagram_card,
    pick_next_anagram,
)

CARD = {"card_id": "c1", "word": "Zugehörigkeit", "hint_ru": "Принадлежность",
        "scrambled": "Zergkhgeiöiut", "explanation": ""}
SWAPPED = "Zugehöregkiit"


class _DummyCursor:
    """Cursor with a scripted fetchone() queue (one entry per execute)."""

    def __init__(self, fetchone_results=None, fetchall_result=None):
        self._fetchone = list(fetchone_results or [])
        self.fetchall_result = list(fetchall_result or [])
        self.executed = []

    def execute(self, query, params=None):
        self.executed.append((query, params))

    def fetchone(self):
        return self._fetchone.pop(0) if self._fetchone else None

    def fetchall(self):
        return list(self.fetchall_result)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _DummyConnection:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor

    def commit(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def _db_context(cursor):
    @contextmanager
    def _context():
        yield _DummyConnection(cursor)

    return _context


class AnagramResultPayloadTests(unittest.TestCase):
    def test_wrong_answer_carries_the_word_the_learner_built(self):
        with patch("backend.database.get_anagram_dispatch_by_id", return_value=dict(CARD)), \
             patch("backend.database.get_anagram_answer", return_value=None), \
             patch("backend.database.record_anagram_answer") as rec:
            result = evaluate_anagram(dispatch_id=339, user_id=7, assembled=SWAPPED)

        self.assertFalse(result["is_correct"])
        self.assertEqual(result["correct_word"], "Zugehörigkeit")
        self.assertEqual(result["user_answer"], SWAPPED)
        rec.assert_called_once()
        # Both words are the same 13 letters — the client diff is a position compare.
        self.assertEqual(sorted(result["user_answer"].lower()),
                         sorted(result["correct_word"].lower()))

    def test_reopened_card_still_shows_the_stored_answer(self):
        stored = {"is_correct": False, "assembled": SWAPPED}
        with patch("backend.database.get_anagram_dispatch_by_id", return_value=dict(CARD)), \
             patch("backend.database.get_anagram_answer", return_value=stored):
            again = evaluate_anagram(dispatch_id=339, user_id=7, assembled="")
            meta = load_anagram_task(dispatch_id=339, user_id=7)

        self.assertEqual(again["user_answer"], SWAPPED)
        self.assertEqual(meta["result"]["user_answer"], SWAPPED)
        self.assertTrue(meta["result"]["already_answered"])

    def test_correct_answer_needs_no_diff_but_keeps_the_field(self):
        with patch("backend.database.get_anagram_dispatch_by_id", return_value=dict(CARD)), \
             patch("backend.database.get_anagram_answer", return_value=None), \
             patch("backend.database.record_anagram_answer"):
            result = evaluate_anagram(dispatch_id=339, user_id=7, assembled="Zugehörigkeit")

        self.assertTrue(result["is_correct"])
        self.assertEqual(result["user_answer"], "Zugehörigkeit")

    def test_delivered_short_card_stays_openable(self):
        # `Mohn` is gated out of the pool, but a July link to it must still open.
        short = dict(CARD, word="Mohn", scrambled="Mhon")
        with patch("backend.database.get_anagram_dispatch_by_id", return_value=short), \
             patch("backend.database.get_anagram_answer", return_value=None):
            meta = load_anagram_task(dispatch_id=264, user_id=7)

        self.assertIsNotNone(meta)
        self.assertEqual(meta["first_letter"], "M")
        self.assertEqual(meta["last_letter"], "n")


class AnagramPoolGateTests(unittest.TestCase):
    def test_picking_skips_words_shorter_than_the_minimum(self):
        cursor = _DummyCursor(fetchone_results=[None])
        with patch("backend.database.get_db_connection_context", _db_context(cursor)):
            self.assertIsNone(pick_next_anagram(cooldown_days=14))

        sql, params = cursor.executed[0]
        self.assertIn("REGEXP_REPLACE", sql)
        self.assertIn(ANAGRAM_MIN_LETTERS, params)
        self.assertGreaterEqual(ANAGRAM_MIN_LETTERS, 8)

    def test_availability_count_uses_the_same_gate(self):
        cursor = _DummyCursor(fetchone_results=[(3,)])
        with patch("backend.database.get_db_connection_context", _db_context(cursor)):
            self.assertEqual(count_available_anagram_cards(cooldown_days=14), 3)

        sql, params = cursor.executed[0]
        self.assertIn("REGEXP_REPLACE", sql)
        self.assertIn(ANAGRAM_MIN_LETTERS, params)


class AnagramDedupTests(unittest.TestCase):
    def test_word_already_in_the_bank_is_refused(self):
        # INSERT ... WHERE NOT EXISTS inserts nothing, and no row owns this card_id.
        cursor = _DummyCursor(fetchone_results=[None, None])
        with patch("backend.database.get_db_connection_context", _db_context(cursor)):
            created = create_anagram_card(card_id="new", word="Einwanderer",
                                          hint_ru="Иммигрант", scrambled="Ednaneriwr")

        self.assertFalse(created)
        sql = cursor.executed[0][0]
        self.assertIn("LOWER(word) = LOWER(", sql)
        self.assertIn("card_id <> ", sql)

    def test_fresh_word_is_inserted(self):
        cursor = _DummyCursor(fetchone_results=[("new",)])
        with patch("backend.database.get_db_connection_context", _db_context(cursor)):
            created = create_anagram_card(card_id="new", word="Zugehörigkeit",
                                          hint_ru="Принадлежность", scrambled="Zergkhgeiöiut")

        self.assertTrue(created)
        self.assertEqual(len(cursor.executed), 1)

    def test_resending_a_pooled_card_is_not_a_duplicate(self):
        # Delivery re-runs create for a card already in the bank under the SAME
        # card_id: ON CONFLICT skips the insert, but the card exists → keep sending.
        cursor = _DummyCursor(fetchone_results=[None, (1,)])
        with patch("backend.database.get_db_connection_context", _db_context(cursor)):
            created = create_anagram_card(card_id="c1", word="Zugehörigkeit",
                                          hint_ru="Принадлежность", scrambled="Zergkhgeiöiut")

        self.assertTrue(created)


if __name__ == "__main__":
    unittest.main()
