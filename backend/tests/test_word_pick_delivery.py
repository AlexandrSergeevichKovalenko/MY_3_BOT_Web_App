# -*- coding: utf-8 -*-
"""«Слова со вчерашних тренировок»: набор дня, получатели, отметка прохода, норма."""
import os
import unittest
from datetime import date
from unittest import mock

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")

from backend import database as db  # noqa: E402


def _соединение_с_курсором(курсор):
    ctx = mock.MagicMock()
    ctx.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = курсор
    return ctx


class НаборДня(unittest.TestCase):

    def setUp(self):
        db._word_pick_schema_ready = True

    def test_получатели_это_все_у_кого_есть_отбор_на_день(self):
        курсор = mock.MagicMock()
        курсор.fetchall.return_value = [(7, 3), (9, 1)]
        with mock.patch.object(db, "get_db_connection_context", _соединение_с_курсором(курсор)):
            out = db.list_word_pick_recipients(date(2026, 9, 6))
        self.assertEqual(out, [{"user_id": 7, "count": 3}, {"user_id": 9, "count": 1}])
        sql = курсор.execute.call_args.args[0]
        self.assertIn("FROM bt_3_word_picks", sql)
        self.assertIn("for_day = %s", sql)

    def test_набор_дня_берёт_карточку_и_состояние_без_проверки_срока(self):
        """Срок карточки (due_at) НЕ фильтруется: вечером набор показывается снова,
        даже если утром поставили «Легко» (решение владельца 04.09.2026)."""
        курсор = mock.MagicMock()
        курсор.fetchall.return_value = [(
            99, "быстрый", None, "schnell", "быстрый", "de", "ru", {"word_de": "schnell"}, [],
            "new", None, None, 0, 0, 0, 0.0, 0.0, 0, None, None, "trainer_save",
        )]
        with mock.patch.object(db, "get_db_connection_context", _соединение_с_курсором(курсор)), \
             mock.patch.object(db, "attach_unit_content_to_cards", lambda items, **kw: None):
            out = db.list_word_pick_cards(user_id=7, for_day=date(2026, 9, 6))
        sql = курсор.execute.call_args.args[0]
        self.assertNotIn("due_at <=", sql)
        self.assertIn("LEFT JOIN bt_3_card_srs_state", sql)
        self.assertEqual(out[0]["card"]["id"], 99)
        self.assertEqual(out[0]["srs"]["status"], "new")
        self.assertIsNone(out[0]["am_rated_at"])

    def test_карточка_без_состояния_отдаёт_srs_none(self):
        курсор = mock.MagicMock()
        курсор.fetchall.return_value = [(
            99, "быстрый", None, "schnell", "быстрый", "de", "ru", {}, [],
            None, None, None, None, None, None, None, None, None, None, None, "trainer_save",
        )]
        with mock.patch.object(db, "get_db_connection_context", _соединение_с_курсором(курсор)), \
             mock.patch.object(db, "attach_unit_content_to_cards", lambda items, **kw: None):
            out = db.list_word_pick_cards(user_id=7, for_day=date(2026, 9, 6))
        self.assertIsNone(out[0]["srs"])

    def test_отметка_прохода_ставится_один_раз_и_только_в_свою_колонку(self):
        курсор = mock.MagicMock()
        db.mark_word_pick_rated(user_id=7, card_id=99, for_day=date(2026, 9, 6), slot="pm", cursor=курсор)
        sql = курсор.execute.call_args.args[0]
        self.assertIn("SET pm_rated_at = COALESCE(pm_rated_at, NOW())", sql)
        self.assertNotIn("am_rated_at", sql)
        with self.assertRaises(ValueError):
            db.mark_word_pick_rated(user_id=7, card_id=99, for_day=date(2026, 9, 6), slot="noon", cursor=курсор)


class СверхНормы(unittest.TestCase):

    def test_код_wp_исключён_из_нормы_везде_где_исключён_rv(self):
        self.assertEqual(tuple(db.INBOX_BONUS_KINDS), ("rv", "wp"))
        import inspect
        for fn in (db.get_inbox_delivery_stats_today, db.get_inbox_kinds_today):
            src = inspect.getsource(fn)
            self.assertNotIn("kind <> 'rv'", src, fn.__name__)
            self.assertIn("_INBOX_BONUS_KINDS_SQL", src, fn.__name__)
        self.assertIn("'wp'", db._INBOX_BONUS_KINDS_SQL)
        from backend.free_delivery_report import BONUS_INBOX_KINDS
        self.assertEqual(BONUS_INBOX_KINDS, {"rv", "wp"})
