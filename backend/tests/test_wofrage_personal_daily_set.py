"""Дневной набор Wo-Fragen стал личным, а зачёт остался общим.

Решение владельца 14.08.2026: «становится разным, зачёт по доле верных и времени».
Это возможно только потому, что задания Wo-Fragen собирает чистый код — личный набор
не стоит ни копейки, в отличие от игр с банком.

Главное, что здесь закреплено: наборы у людей РАЗНЫЕ, но в таблице результатов они
по-прежнему видят друг друга — иначе каждый оказался бы «первым из одного».
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


class GroupKeyTests(unittest.TestCase):
    def test_personal_set_knows_its_slot(self):
        self.assertEqual(db.wofrage_group_set_id("abc-123#u777"), "abc-123")

    def test_shared_set_is_its_own_group(self):
        self.assertEqual(db.wofrage_group_set_id("abc-123"), "abc-123")

    def test_battle_set_is_untouched(self):
        """У батлов набор общий на двоих — он не должен ни во что превращаться."""
        self.assertEqual(db.wofrage_group_set_id("battle-xyz"), "battle-xyz")


class PersonalSetTests(unittest.TestCase):
    def test_two_people_get_two_different_sets_in_one_slot(self):
        ctx, cur = _fake_conn(one=None)
        with patch.object(db, "get_db_connection_context", return_value=ctx), \
             patch.object(db, "ensure_wofrage_sprint_schema"), \
             patch.object(db, "pick_wofrage_payloads_for_user",
                          return_value=[{"s": "x", "opts": ["a"], "a": "a"}]):
            first = db.get_or_create_personal_wofrage_set("slot-1", 111)
            second = db.get_or_create_personal_wofrage_set("slot-1", 222)
        self.assertNotEqual(first, second)
        self.assertTrue(first.startswith("slot-1"))
        self.assertTrue(second.startswith("slot-1"))

    def test_existing_personal_set_is_reused_not_rebuilt(self):
        """Иначе человек, открывший карточку дважды, получал бы новые задания."""
        ctx, cur = _fake_conn(one=(1,))
        with patch.object(db, "get_db_connection_context", return_value=ctx), \
             patch.object(db, "ensure_wofrage_sprint_schema"), \
             patch.object(db, "pick_wofrage_payloads_for_user") as picker:
            out = db.get_or_create_personal_wofrage_set("slot-1", 111)
        self.assertEqual(out, "slot-1#u111")
        picker.assert_not_called()

    def test_empty_generator_falls_back_to_the_shared_set(self):
        ctx, _ = _fake_conn(one=None)
        with patch.object(db, "get_db_connection_context", return_value=ctx), \
             patch.object(db, "ensure_wofrage_sprint_schema"), \
             patch.object(db, "pick_wofrage_payloads_for_user", return_value=[]):
            self.assertIsNone(db.get_or_create_personal_wofrage_set("slot-1", 111))


class RankingTests(unittest.TestCase):
    def test_ranking_covers_the_whole_slot_not_one_persons_set(self):
        ctx, cur = _fake_conn(rows=[(111, "A", 9, 1000), (222, "B", 8, 900)])
        with patch.object(db, "get_db_connection_context", return_value=ctx):
            out = db.list_wofrage_sprint_results_ranked("slot-1#u111")
        params = cur.execute.call_args[0][1]
        self.assertIn("slot-1", params)
        self.assertIn("slot-1#u%", params)
        self.assertEqual(len(out), 2, "человек должен видеть остальных, а не себя одного")

    def test_order_is_by_correct_then_time(self):
        ctx, cur = _fake_conn(rows=[])
        with patch.object(db, "get_db_connection_context", return_value=ctx):
            db.list_wofrage_sprint_results_ranked("slot-1")
        sql = " ".join(cur.execute.call_args[0][0].split())
        self.assertIn("ORDER BY correct DESC, time_ms ASC", sql)


if __name__ == "__main__":
    unittest.main()
