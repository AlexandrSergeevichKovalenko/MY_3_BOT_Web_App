"""Слой базы у ротации тонкий: он только хранит состояние, а решает правило.

Тест не ходит в базу — он проверяет, что слой зовёт правило из `backend/task_rotation.py`
и кладёт в SQL именно то, что правило вернуло. Живую базу трогать нельзя: в окружении
разработчика лежат боевые креденшелы (см. `backend/tests/conftest.py`).
"""

from datetime import datetime, timezone
import unittest
from unittest.mock import MagicMock, patch

import backend.database as db
from backend.task_rotation import LADDER_DAYS


def _fake_conn(rows=None):
    """Подделка подключения: курсор отдаёт заданные строки и запоминает запросы."""
    cur = MagicMock()
    cur.fetchall.return_value = list(rows or [])
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    ctx = MagicMock()
    ctx.__enter__.return_value = conn
    return ctx, cur


class RecordAnswerTests(unittest.TestCase):
    def _record(self, **kwargs):
        ctx, cur = _fake_conn(rows=[])
        with patch.object(db, "get_db_connection_context", return_value=ctx), \
             patch.object(db, "ensure_task_rotation_schema"):
            db.record_user_task_answer(**kwargs)
        return cur.execute.call_args_list

    def test_correct_answer_schedules_return_in_90_days(self):
        calls = self._record(user_id=1, kind="article_quiz", task_key="w42",
                             is_correct=True)
        params = calls[-1][0][1]
        self.assertIn(1, params)
        self.assertIn("article_quiz", params)
        due = [p for p in params if isinstance(p, datetime)]
        self.assertTrue(
            any(abs((d - datetime.now(timezone.utc)).days - LADDER_DAYS[0]) <= 1
                for d in due),
            f"верный ответ должен вернуть задание через {LADDER_DAYS[0]} дней: {due}")

    def test_wrong_answer_does_not_schedule_a_return(self):
        calls = self._record(user_id=1, kind="article_quiz", task_key="w42",
                             is_correct=False)
        params = calls[-1][0][1]
        self.assertFalse([p for p in params if isinstance(p, datetime)],
                         "неверный ответ лестницу не двигает — сроков быть не должно")

    def test_broken_database_never_raises(self):
        """Память служебная: если она упала, ответ человеку всё равно должен пройти."""
        with patch.object(db, "get_db_connection_context", side_effect=RuntimeError), \
             patch.object(db, "ensure_task_rotation_schema"):
            db.record_user_task_answer(user_id=1, kind="article_quiz", task_key="w1",
                                       is_correct=True)


class ReadStateTests(unittest.TestCase):
    def test_state_is_returned_keyed_by_task(self):
        now = datetime.now(timezone.utc)
        rows = [("w1", 2, 1, now, now, None)]
        ctx, cur = _fake_conn(rows=rows)
        with patch.object(db, "get_db_connection_context", return_value=ctx), \
             patch.object(db, "ensure_task_rotation_schema"):
            out = db.get_user_task_state(1, "article_quiz", ["w1", "w2"])
        self.assertEqual(set(out), {"w1"}, "чего в памяти нет — человек ещё не видел")
        self.assertEqual(out["w1"]["seen_count"], 2)
        self.assertEqual(out["w1"]["correct_count"], 1)

    def test_no_keys_means_no_query(self):
        ctx, cur = _fake_conn()
        with patch.object(db, "get_db_connection_context", return_value=ctx):
            self.assertEqual(db.get_user_task_state(1, "article_quiz", []), {})
        cur.execute.assert_not_called()


if __name__ == "__main__":
    unittest.main()
