"""Работа над ошибками выдаётся порцией на день, а не кучей.

Замер 15.08.2026 по живой базе: 363 задания в очереди, 356 просрочено, у одного
человека 188. За заход разбиралось 20-50 — то есть куча физически неразбираема и только
росла: выборка всегда отдаёт самые старые, поэтому хвост не достигался никогда.

Решение владельца: «давал бы короткими порциями по 30 ошибок за день, все ошибки
собираем, показываем самые старшие 30». Очередь при этом не режется — копится всё,
меняется только сколько видно за раз.
"""

import unittest
from unittest.mock import MagicMock, patch

import backend.database as db


def _fake_conn(one=None, rows=None):
    cur = MagicMock()
    cur.fetchone.return_value = one
    cur.fetchall.return_value = list(rows or [])
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    ctx = MagicMock()
    ctx.__enter__.return_value = conn
    return ctx, cur


class PortionSizeTests(unittest.TestCase):
    def test_portion_is_thirty(self):
        self.assertEqual(db.REVIEW_DAILY_PORTION, 30)

    def test_portion_takes_the_oldest(self):
        """Самые старые — иначе новые ошибки вечно оттесняли бы залежавшиеся."""
        self.assertIn("ORDER BY due_at ASC", db._DAILY_PORTION_SQL)
        self.assertIn(f"LIMIT {db.REVIEW_DAILY_PORTION}", db._DAILY_PORTION_SQL)


class CountersRespectThePortionTests(unittest.TestCase):
    def _sql_of(self, fn, **kw):
        ctx, cur = _fake_conn(one=[0, 0, 0, 0])
        with patch.object(db, "get_db_connection_context", return_value=ctx), \
             patch.object(db, "ensure_aufgabe_mistakes_schema"):
            fn(**kw)
        return " ".join(cur.execute.call_args[0][0].split()), cur.execute.call_args[0][1]

    def test_reminder_counter_shows_the_portion_not_the_pile(self):
        """Иначе человеку в личку приходит «у тебя 356 на повтор» — это не зовёт
        разбирать, это отпугивает."""
        sql, params = self._sql_of(db.count_due_mistakes, user_id=7)
        self.assertIn(f"LIMIT {db.REVIEW_DAILY_PORTION}", sql)
        self.assertEqual(list(params), [7, 7])

    def test_section_counters_respect_the_portion(self):
        sql, params = self._sql_of(db.count_due_mistakes_by_family, user_id=7)
        self.assertIn(f"LIMIT {db.REVIEW_DAILY_PORTION}", sql)
        self.assertEqual(list(params), [7, 7])


class BatchesRespectThePortionTests(unittest.TestCase):
    def test_wofrage_batch_is_inside_the_portion(self):
        ctx, cur = _fake_conn(rows=[])
        with patch.object(db, "get_db_connection_context", return_value=ctx), \
             patch.object(db, "ensure_aufgabe_mistakes_schema"):
            db.get_due_wofrage_mistakes_batch(7, 20)
        sql = " ".join(cur.execute.call_args[0][0].split())
        self.assertIn(f"LIMIT {db.REVIEW_DAILY_PORTION}", sql)
        self.assertEqual(list(cur.execute.call_args[0][1]), [7, 7, 20])

    def test_grammar_picker_is_inside_the_portion(self):
        ctx, cur = _fake_conn(rows=[])
        with patch.object(db, "get_db_connection_context", return_value=ctx), \
             patch.object(db, "ensure_aufgabe_mistakes_schema"):
            db.get_next_due_mistake(7)
        sql = " ".join(cur.execute.call_args[0][0].split())
        self.assertIn(f"LIMIT {db.REVIEW_DAILY_PORTION}", sql)
        self.assertEqual(list(cur.execute.call_args[0][1]), [7, 7])


if __name__ == "__main__":
    unittest.main()
