"""Разбор снятых слов в личке: спрашиваем только спорное, и спрашиваем один раз.

Чистка словника сняла 2 929 слов. Показывать весь список нельзя (2 784 из них вообще нет
в частотном списке — это и есть хлам), поэтому в очередь попадают только слова с рангом
лучше порога. Слово, про которое уже спросили, второй раз не приходит."""
import unittest
from contextlib import contextmanager
from unittest.mock import patch

import backend.database as db
from backend.article_retire_review import _word_text, _keyboard


class _FakeCursor:
    def __init__(self, rows):
        self._rows = rows
        self.sql_log: list[str] = []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        self.sql_log.append(" ".join(str(sql).split()))

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None


class _FakeConn:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor

    def commit(self):
        pass


class RetireReviewQueueTests(unittest.TestCase):
    ROWS = [
        (1, "Flur", "der", "коридор", "haus_wohnen"),              # ранг ~4 000 — спорное
        (2, "Schmetterlings-Tramete", "der", "губка", "pflanzen"),  # в списке нет — хлам
        (3, "Erwachsene", "die", "взрослая", "menschen"),           # ранг ~3 800 — спорное
    ]

    def _rows(self, rows=None):
        cur = _FakeCursor(self.ROWS if rows is None else rows)

        @contextmanager
        def _fake_ctx(*a, **k):
            yield _FakeConn(cur)

        with patch.object(db, "get_db_connection_context", _fake_ctx):
            out = db.list_retired_review_candidates(limit=10, max_rank=60000)
        return out, cur

    def test_only_words_that_look_common_are_asked_about(self):
        out, _ = self._rows()
        words = [r["word"] for r in out]
        self.assertIn("Flur", words)
        self.assertIn("Erwachsene", words)
        self.assertNotIn("Schmetterlings-Tramete", words,
                         "слова, которого нет в частотном списке, спрашивать незачем")

    def test_most_common_word_comes_first(self):
        out, _ = self._rows()
        self.assertEqual([r["rank"] for r in out], sorted(r["rank"] for r in out))

    def test_already_reviewed_words_are_not_asked_again(self):
        _, cur = self._rows()
        self.assertIn("NOT COALESCE(retire_reviewed, FALSE)", cur.sql_log[0])

    def test_the_same_word_in_two_themes_is_asked_once(self):
        out, _ = self._rows([
            (1, "Flur", "der", "коридор", "haus_wohnen"),
            (2, "Flur", "der", "коридор", "stadt_gebaeude"),
        ])
        self.assertEqual(len(out), 1)

    def test_restore_brings_the_word_back_in_every_theme(self):
        cur = _FakeCursor([("Flur", "der")])

        @contextmanager
        def _fake_ctx(*a, **k):
            yield _FakeConn(cur)

        with patch.object(db, "get_db_connection_context", _fake_ctx):
            res = db.restore_retired_article_noun(1)
        self.assertEqual(res, {"word": "Flur", "article": "der"})
        update = [s for s in cur.sql_log if s.startswith("UPDATE")][0]
        self.assertIn("lower(word) = lower(%s)", update, "вернуть надо во всех темах, не одну строку")
        self.assertIn("retired = FALSE", update)
        self.assertTrue([s for s in cur.sql_log if s.startswith("DELETE FROM bt_3_article_word_blacklist")],
                        "возвращённое слово надо снять со стоп-листа")

    def test_keep_puts_the_word_on_the_stop_list(self):
        cur = _FakeCursor([("Schmetterlings-Tramete",)])

        @contextmanager
        def _fake_ctx(*a, **k):
            yield _FakeConn(cur)

        recorded = []
        with patch.object(db, "get_db_connection_context", _fake_ctx), \
                patch.object(db, "blacklist_article_words", lambda items: recorded.extend(items)):
            res = db.keep_retired_article_noun(1)
        self.assertEqual(res, {"word": "Schmetterlings-Tramete"})
        self.assertEqual([w for w, _r, _t in recorded], ["Schmetterlings-Tramete"])


class RetireReviewCardTests(unittest.TestCase):
    def test_card_says_what_happened_and_why_it_is_being_asked(self):
        text = _word_text({"word": "Flur", "article": "der", "meaning_ru": "коридор", "rank": 4081},
                          index=1, total=10, left=145)
        self.assertIn("der Flur", text)
        self.assertIn("коридор", text)
        self.assertIn("убрано из игры", text)
        self.assertIn("4081", text)
        self.assertIn("1 из 10", text)

    def test_buttons_are_restore_and_junk(self):
        kb = _keyboard(7)["inline_keyboard"][0]
        self.assertEqual([b["callback_data"] for b in kb], ["artret:back:7", "artret:keep:7"])


if __name__ == "__main__":
    unittest.main()
