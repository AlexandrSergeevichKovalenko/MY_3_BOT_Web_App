"""«Работа над ошибками» не должна возвращать слова, снятые с показа в банке артиклей.

Очередь ошибок (bt_3_aufgabe_mistakes) хранит СВОЮ копию карточки, поэтому после чистки
словника (2 929 снятых слов, 29–30.07) в разбор ошибок продолжали приходить «Ranvier-
Schnürring», «Schmetterlings-Tramete», «Dachdeckergeselle» — слов этих в игре уже нет.
Здесь фиксируется форма запросов: и выдача карточек, и счётчики, и колода обучения
требуют ЖИВОЙ строки в bt_3_article_sprint_nouns, а сама выдача заодно чистит очередь.

SQL постгресовый (EXISTS + payload->>), поэтому проверяем форму запроса на моке БД."""
import unittest
from contextlib import contextmanager
from unittest.mock import patch

import backend.database as db


class _FakeCursor:
    def __init__(self, rows=(), one=(0,)):
        self._rows = list(rows)
        self._one = one
        self.sql_log: list[str] = []
        self.rowcount = 0

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        self.sql_log.append(" ".join(str(sql).split()))

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._one


class _FakeConn:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor

    def commit(self):
        pass


def _norm(sql: str) -> str:
    return " ".join(sql.split())


class ArtikelReviewSkipsRetiredWordsTests(unittest.TestCase):
    def _run(self, fn, *, rows=(), one=(0,)):
        cur = _FakeCursor(rows, one)

        @contextmanager
        def _fake_ctx(*a, **k):
            yield _FakeConn(cur)

        with patch.object(db, "get_db_connection_context", _fake_ctx), \
                patch.object(db, "ensure_aufgabe_mistakes_schema", lambda: None), \
                patch.object(db, "ensure_article_learn_schema", lambda: None):
            out = fn()
        return out, cur

    def _asserts_live_word_guard(self, sql: str, where: str):
        self.assertIn("bt_3_article_sprint_nouns", sql, where)
        self.assertIn("COALESCE(an.retired, FALSE) = FALSE", sql, where)

    def test_batch_query_requires_a_live_bank_row(self):
        _, cur = self._run(lambda: db.get_due_artikel_mistakes_batch(1, 20))
        card_sql = [s for s in cur.sql_log if "m.format = 'artikel'" in s and s.startswith("SELECT")]
        self.assertTrue(card_sql, "не нашёл запрос выдачи карточек")
        self._asserts_live_word_guard(card_sql[0], "выдача карточек разбора ошибок")

    def test_batch_load_purges_retired_rows(self):
        _, cur = self._run(lambda: db.get_due_artikel_mistakes_batch(1, 20))
        deletes = [s for s in cur.sql_log if s.startswith("DELETE FROM bt_3_aufgabe_mistakes")]
        self.assertTrue(deletes, "открытие разбора должно чистить снятые слова из очереди")
        self.assertIn("COALESCE(rn.retired, FALSE) = TRUE", deletes[0])
        self.assertIn("COALESCE(an.retired, FALSE) = FALSE", deletes[0])

    def test_purge_is_scoped_to_one_user_when_asked(self):
        _, cur = self._run(lambda: db.purge_retired_artikel_mistakes(42))
        self.assertIn("m.user_id = %s", cur.sql_log[0])
        _, cur_all = self._run(lambda: db.purge_retired_artikel_mistakes())
        self.assertNotIn("m.user_id", cur_all.sql_log[0])

    def test_counters_do_not_count_retired_words(self):
        # Счётчик «осталось N» должен совпадать с тем, что реально покажут.
        _, cur = self._run(lambda: db.count_due_mistakes(1, family="artikel"), one=(0,))
        self._asserts_live_word_guard(cur.sql_log[0], "счётчик count_due_mistakes")
        _, cur2 = self._run(lambda: db.count_due_mistakes_by_family(1), one=(0, 0, 0, 0))
        self._asserts_live_word_guard(cur2.sql_log[0], "счётчик по секциям")

    def test_next_due_mistake_skips_retired_words(self):
        _, cur = self._run(lambda: db.get_next_due_mistake(1, family="artikel"), rows=[])
        self._asserts_live_word_guard(cur.sql_log[0], "get_next_due_mistake")

    def test_learn_deck_review_skips_retired_words(self):
        _, cur = self._run(lambda: db.get_article_learn_review_words(1, 8), rows=[])
        self.assertIn("COALESCE(n.retired, FALSE) = FALSE", cur.sql_log[0])

    def test_guard_leaves_words_that_are_simply_absent_from_the_bank(self):
        # Снимаем только то, что мы САМИ пометили retired: «слова в банке нет вообще» —
        # не признак мусора, и терять такую карточку нельзя.
        sql = _norm(db._ARTIKEL_LIVE_WORD_SQL)
        self.assertIn("COALESCE(rn.retired, FALSE) = TRUE", sql)
        self.assertIn("AND NOT EXISTS", sql)


class RetiredWordMediaReclaimTests(unittest.TestCase):
    """Картинка и озвучка снятого слова — тоже мусор, но убирать его надо осторожно."""

    def _sweep(self, **kw):
        cur = _FakeCursor(rows=[], one=(0,))

        @contextmanager
        def _fake_ctx(*a, **k):
            yield _FakeConn(cur)

        with patch.object(db, "get_db_connection_context", _fake_ctx):
            res = db.reclaim_retired_pool_r2_orphans(**kw)
        return res, cur

    def test_sweep_covers_the_article_word_bank(self):
        res, cur = self._sweep(dry_run=True)
        self.assertIn("artikel_img", res["per_pool"])
        self.assertIn("artikel_audio", res["per_pool"])
        artikel = [s for s in cur.sql_log if "bt_3_article_sprint_nouns" in s]
        self.assertEqual(len(artikel), 2, "ждём по одному запросу на картинки и на озвучку")

    def test_sweep_waits_out_the_grace_period_and_spares_shared_files(self):
        _, cur = self._sweep(dry_run=True)
        for sql in [s for s in cur.sql_log if "bt_3_article_sprint_nouns" in s]:
            self.assertIn("r.updated_at < NOW() -", sql, "снятое слово должно отлежаться")
            self.assertIn("NOT EXISTS", sql, "файл живого слова трогать нельзя")

    def test_mnemonics_are_never_deleted(self):
        _, cur = self._sweep(dry_run=True)
        self.assertFalse([s for s in cur.sql_log if "mnemonic" in s],
                         "мнемонику не трогаем: текст бесплатен, а переписывать — деньги")


if __name__ == "__main__":
    unittest.main()
