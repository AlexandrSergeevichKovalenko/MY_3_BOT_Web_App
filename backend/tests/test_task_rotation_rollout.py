"""Личная ротация раскатывается на остальные интерактивы: ребус, кроссворд,
анаграмма, задания общего пула.

Все они сдают ответ через одно место (`/api/answer/submit`), и ключ там уже строится
по СОДЕРЖИМОМУ задания (`content_ranking_key` → `"<вид>:<id записи банка>"`), а не по
рассылке. Значит память ротации подключается ко всем разом, а на стороне выдачи нужно
лишь не предлагать человеку то, что для него сейчас закрыто.
"""

import unittest
from unittest.mock import MagicMock, patch

import backend.database as db


def _fake_conn(rows=None, one=None):
    cur = MagicMock()
    cur.fetchall.return_value = list(rows or [])
    cur.fetchone.return_value = one
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    ctx = MagicMock()
    ctx.__enter__.return_value = conn
    return ctx, cur


class BlockedIdsTests(unittest.TestCase):
    def test_prefix_of_the_kind_is_stripped(self):
        """В памяти ключ лежит как «rb:42», а банку нужен голый номер записи."""
        ctx, cur = _fake_conn(rows=[("rb:42",), ("rb:77",)])
        with patch.object(db, "get_db_connection_context", return_value=ctx), \
             patch.object(db, "ensure_task_rotation_schema"), \
             patch.object(db, "_task_rotation_writes_disabled", return_value=False):
            out = db.get_user_blocked_content_ids(1, "rb")
        self.assertEqual(out, ["42", "77"])

    def test_broken_database_blocks_nothing(self):
        """Память упала — человек получает задание по-старому, а не пустоту."""
        with patch.object(db, "get_db_connection_context", side_effect=RuntimeError), \
             patch.object(db, "ensure_task_rotation_schema"), \
             patch.object(db, "_task_rotation_writes_disabled", return_value=False):
            self.assertEqual(db.get_user_blocked_content_ids(1, "rb"), [])


class PickerExclusionTests(unittest.TestCase):
    """Отбор задания должен уметь пропускать то, что этому человеку сейчас закрыто."""

    def _sql_of(self, fn, **kwargs):
        ctx, cur = _fake_conn(one=None)
        with patch.object(db, "get_db_connection_context", return_value=ctx), \
             patch.object(db, "_task_rotation_writes_disabled", return_value=False):
            fn(**kwargs)
        sql, params = cur.execute.call_args[0][0], cur.execute.call_args[0][1]
        return " ".join(sql.split()), params

    def test_rebus_excludes_what_the_person_already_passed(self):
        sql, params = self._sql_of(db.pick_next_rebus, cooldown_days=15,
                                   exclude_ids=["42", "77"])
        self.assertIn("<> ALL(%s)", sql)
        self.assertIn(["42", "77"], list(params))

    def test_crossword_excludes_too(self):
        sql, params = self._sql_of(db.pick_next_crossword, exclude_ids=["7"])
        self.assertIn("<> ALL(%s)", sql)

    def test_anagram_excludes_too(self):
        sql, params = self._sql_of(db.pick_next_anagram, exclude_ids=["7"])
        self.assertIn("<> ALL(%s)", sql)

    def test_crossword_and_anagram_have_no_shared_rest_left(self):
        """Общий отдых карточки снят (решение владельца 24.08.2026).

        Проверяем ровно две вещи: условия «показан кому-то недавно — никому не давать»
        в отборе НЕТ, а `last_sent_at` остался — но только в сортировке, как порядок
        очереди. Без второй половины человек, который игру не открывает, получал бы
        каждый день буквально одну и ту же карточку.
        """
        for picker in (db.pick_next_crossword, db.pick_next_anagram):
            with self.subTest(picker=picker.__name__):
                sql, _ = self._sql_of(picker, exclude_ids=[])
                self.assertNotIn("last_sent_at <", sql)
                self.assertNotIn("last_sent_at IS NULL OR", sql)
                self.assertIn("ORDER BY last_sent_at NULLS FIRST", sql)

    def test_aufgabe_excludes_too(self):
        sql, params = self._sql_of(db.pick_next_aufgabe, cooldown_days=15,
                                   format="cloze", exclude_ids=["7"])
        self.assertIn("<> ALL(%s)", sql)

    def test_without_exclusions_the_query_stays_as_it_was(self):
        """Пустой список ничего не добавляет: старое поведение не трогаем."""
        sql, _ = self._sql_of(db.pick_next_rebus, cooldown_days=15, exclude_ids=[])
        self.assertNotIn("<> ALL(%s)", sql)


if __name__ == "__main__":
    unittest.main()
