"""Работа над ошибками выдаётся порцией НА ДЕНЬ, а не кучей и не бесконечным окном.

Замер 15.08.2026 по живой базе: 363 задания в очереди, 356 просрочено, у одного
человека 188. За заход разбиралось 20-50 — то есть куча физически неразбираема и только
росла: выборка всегда отдаёт самые старые, поэтому хвост не достигался никогда.

Решение владельца: «давал бы короткими порциями по 30 ошибок за день, все ошибки
собираем, показываем самые старшие 30». Очередь при этом не режется — копится всё,
меняется только сколько видно за раз.

ДЕФЕКТ 16.08.2026 и его починка. Первая версия брала «30 самых старых из просроченных»
заново после КАЖДОГО ответа: разобранное уходило на завтра, а на его место вставало
31-е по старшинству. Владелец при очереди в 185 видел вечное «Weiter (30)» — счётчик не
уменьшался никогда, и дневного предела фактически не было (ограничена была надпись, а не
выдача). Замер в тот день: 185 / 128 / 32 в очереди у трёх человек, показано у всех 30.

Теперь порция считается ОТ ДНЯ: 30 минус разобранное сегодня (по местному дню
Europe/Vienna, той же границе суток, что назначает due_at). Дошёл до нуля — на сегодня
всё, следующие 30 завтра.
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

    def test_portion_limit_is_a_parameter_not_a_constant(self):
        """Ровно этим и был дефект: жёсткий LIMIT 30 доливал окно после каждого ответа,
        и счётчик у человека стоял на 30 при очереди в 185."""
        self.assertIn("LIMIT %s", db._DAILY_PORTION_SQL)
        self.assertNotIn(f"LIMIT {db.REVIEW_DAILY_PORTION}", db._DAILY_PORTION_SQL)


class PortionLeftTests(unittest.TestCase):
    def test_left_shrinks_with_every_review(self):
        for done, expect in ((0, 30), (1, 29), (29, 1), (30, 0)):
            with patch.object(db, "count_mistakes_reviewed_today", return_value=done):
                self.assertEqual(db.review_portion_left(7), expect, f"разобрано {done}")

    def test_left_never_goes_negative(self):
        """Разобрал больше порции (доехали отложенные ответы) — не «минус два», а ноль."""
        with patch.object(db, "count_mistakes_reviewed_today", return_value=32):
            self.assertEqual(db.review_portion_left(7), 0)

    def test_reviewed_today_counts_by_the_local_day(self):
        """Граница суток та же, по которой назначается due_at, — иначе порция
        обнулялась бы не тогда, когда для человека начинается новый день."""
        ctx, cur = _fake_conn(one=[3])
        with patch.object(db, "get_db_connection_context", return_value=ctx), \
             patch.object(db, "ensure_aufgabe_mistakes_schema"):
            self.assertEqual(db.count_mistakes_reviewed_today(7), 3)
        sql = " ".join(cur.execute.call_args[0][0].split())
        self.assertIn("last_review_at >=", sql)
        self.assertIn(db._REVIEW_TZ, sql)

    def test_reviewed_today_counts_items_not_answers(self):
        """Считаем СТРОКИ с сегодняшней отметкой, а не события: досылка одного и того же
        ответа из очереди браузера не должна съедать порцию дважды."""
        ctx, cur = _fake_conn(one=[3])
        with patch.object(db, "get_db_connection_context", return_value=ctx), \
             patch.object(db, "ensure_aufgabe_mistakes_schema"):
            db.count_mistakes_reviewed_today(7)
        sql = " ".join(cur.execute.call_args[0][0].split())
        self.assertIn("COUNT(*)", sql)
        self.assertNotIn("SUM(review_count)", sql)


class ExhaustedPortionTests(unittest.TestCase):
    """Порция добита — база больше не спрашивается вовсе, наружу идёт пусто."""

    def test_counter_is_zero(self):
        with patch.object(db, "review_portion_left", return_value=0), \
             patch.object(db, "get_db_connection_context") as conn:
            self.assertEqual(db.count_due_mistakes(7), 0)
            conn.assert_not_called()

    def test_sections_are_empty(self):
        with patch.object(db, "review_portion_left", return_value=0), \
             patch.object(db, "get_db_connection_context") as conn:
            self.assertEqual(db.count_due_mistakes_by_family(7),
                             {"artikel": 0, "wofrage": 0, "grammar": 0, "total": 0})
            conn.assert_not_called()

    def test_no_next_task_is_served(self):
        with patch.object(db, "review_portion_left", return_value=0), \
             patch.object(db, "get_db_connection_context") as conn:
            self.assertIsNone(db.get_next_due_mistake(7))
            conn.assert_not_called()

    def test_batches_are_empty(self):
        with patch.object(db, "review_portion_left", return_value=0), \
             patch.object(db, "get_db_connection_context") as conn:
            self.assertEqual(db.get_due_artikel_mistakes_batch(7, 20), [])
            self.assertEqual(db.get_due_wofrage_mistakes_batch(7, 20), [])
            conn.assert_not_called()


class CountersRespectThePortionTests(unittest.TestCase):
    def _sql_of(self, fn, left=12, **kw):
        ctx, cur = _fake_conn(one=[0, 0, 0, 0])
        with patch.object(db, "get_db_connection_context", return_value=ctx), \
             patch.object(db, "review_portion_left", return_value=left), \
             patch.object(db, "ensure_aufgabe_mistakes_schema"):
            fn(**kw)
        return " ".join(cur.execute.call_args[0][0].split()), cur.execute.call_args[0][1]

    def test_counter_asks_for_whats_left_today(self):
        sql, params = self._sql_of(db.count_due_mistakes, left=12, user_id=7)
        self.assertIn("LIMIT %s", sql)
        self.assertEqual(list(params), [7, 7, 12])

    def test_section_counters_respect_the_portion(self):
        sql, params = self._sql_of(db.count_due_mistakes_by_family, left=12, user_id=7)
        self.assertIn("LIMIT %s", sql)
        self.assertEqual(list(params), [7, 7, 12])

    def test_queue_size_ignores_the_portion(self):
        """Вся очередь нужна ровно для одного: отличить «разобрал всё» от «на сегодня
        всё, но куча ещё есть». Порцию она не применяет намеренно."""
        ctx, cur = _fake_conn(one=[185])
        with patch.object(db, "get_db_connection_context", return_value=ctx), \
             patch.object(db, "ensure_aufgabe_mistakes_schema"):
            self.assertEqual(db.count_due_mistakes_in_queue(7), 185)
        sql = " ".join(cur.execute.call_args[0][0].split())
        self.assertNotIn("LIMIT", sql)


class BatchesRespectThePortionTests(unittest.TestCase):
    def _sql_of(self, fn, left, *args):
        ctx, cur = _fake_conn(rows=[])
        with patch.object(db, "get_db_connection_context", return_value=ctx), \
             patch.object(db, "review_portion_left", return_value=left), \
             patch.object(db, "ensure_aufgabe_mistakes_schema"), \
             patch.object(db, "purge_retired_artikel_mistakes", return_value=0):
            fn(*args)
        return " ".join(cur.execute.call_args[0][0].split()), list(cur.execute.call_args[0][1])

    def test_wofrage_batch_is_inside_the_portion(self):
        sql, params = self._sql_of(db.get_due_wofrage_mistakes_batch, 12, 7, 20)
        self.assertIn("LIMIT %s", sql)
        self.assertEqual(params, [7, 7, 12, 12])   # страница не больше остатка дня

    def test_artikel_batch_is_inside_the_portion(self):
        sql, params = self._sql_of(db.get_due_artikel_mistakes_batch, 12, 7, 20)
        self.assertIn("LIMIT %s", sql)
        self.assertEqual(params, [7, 7, 12, 12])

    def test_batch_page_stays_within_its_own_limit(self):
        """Остаток дня больше страницы — страница остаётся страницей."""
        _sql, params = self._sql_of(db.get_due_wofrage_mistakes_batch, 30, 7, 20)
        self.assertEqual(params, [7, 7, 30, 20])

    def test_grammar_picker_is_inside_the_portion(self):
        ctx, cur = _fake_conn(rows=[])
        with patch.object(db, "get_db_connection_context", return_value=ctx), \
             patch.object(db, "review_portion_left", return_value=12), \
             patch.object(db, "ensure_aufgabe_mistakes_schema"):
            db.get_next_due_mistake(7)
        sql = " ".join(cur.execute.call_args[0][0].split())
        self.assertIn("LIMIT %s", sql)
        self.assertEqual(list(cur.execute.call_args[0][1]), [7, 7, 12])


class ReviewMarksTheDaySpentTests(unittest.TestCase):
    """Без отметки last_review_at порция не убывала бы вовсе — это и есть корень дефекта."""

    def _sqls_of_reschedule(self, is_correct, interval=1):
        ctx, cur = _fake_conn(one=[interval])
        with patch.object(db, "get_db_connection_context", return_value=ctx):
            db.reschedule_mistake(mistake_id=5, user_id=7, is_correct=is_correct)
        return [" ".join(c[0][0].split()) for c in cur.execute.call_args_list]

    def test_correct_answer_marks_the_review(self):
        self.assertTrue(any("last_review_at=NOW()" in s
                            for s in self._sqls_of_reschedule(True)))

    def test_wrong_answer_marks_the_review(self):
        """Ошибся — задание вернётся завтра, но порцию дня оно уже израсходовало."""
        self.assertTrue(any("last_review_at=NOW()" in s
                            for s in self._sqls_of_reschedule(False)))

    def test_mastered_answer_marks_the_review(self):
        """Последний интервал: карточка закрывается совсем — отметка всё равно нужна."""
        sqls = self._sqls_of_reschedule(True, interval=max(db._MISTAKE_INTERVALS))
        self.assertTrue(any("mastered=TRUE" in s and "last_review_at=NOW()" in s for s in sqls))

    def test_video_card_marks_the_review(self):
        ctx, cur = _fake_conn()
        with patch.object(db, "get_db_connection_context", return_value=ctx):
            db.consume_video_review(user_id=7, mistake_id=5)
        self.assertIn("last_review_at=NOW()", " ".join(cur.execute.call_args[0][0].split()))


class ReminderCallsForThePortionTests(unittest.TestCase):
    def test_reminder_counts_the_portion_not_the_pile(self):
        """Иначе человеку в личку приходит «у тебя 185 на повтор» — это не зовёт
        разбирать, это отпугивает. А кто порцию уже добил, не получает ничего."""
        ctx, cur = _fake_conn(rows=[(7, 30)])
        with patch.object(db, "get_db_connection_context", return_value=ctx), \
             patch.object(db, "ensure_aufgabe_mistakes_schema"):
            out = db.list_users_with_due_mistakes(min_count=1)
        self.assertEqual(out, [{"user_id": 7, "count": 30}])
        sql = " ".join(cur.execute.call_args[0][0].split())
        self.assertIn("last_review_at >=", sql)
        self.assertIn(f"{db.REVIEW_DAILY_PORTION} - COALESCE(d.done, 0)", sql)


if __name__ == "__main__":
    unittest.main()
