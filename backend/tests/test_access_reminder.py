# -*- coding: utf-8 -*-
"""Напоминание запертым: каденс из таблицы, картинка, строка в отчёт, обещание.

Решение владельца 04.09.2026: раз в неделю 8 недель, дальше раз в месяц; оплатил — стоп.
Стратегия — docs/tasks/light_tier_strategy.md §7–8.
"""
import os
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")

from backend import database as db  # noqa: E402
from backend import fix_promises  # noqa: E402
import bot_3  # noqa: E402

NOW = datetime(2026, 10, 10, 9, 0, tzinfo=timezone.utc)
UID = 8546091375


def _conn(курсор):
    from unittest import mock
    ctx = mock.MagicMock()
    ctx.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = курсор
    return ctx


class Каденс(unittest.TestCase):

    def setUp(self):
        db._access_reminder_schema_ready = True

    def test_первые_восемь_раз_в_неделю_потом_раз_в_месяц(self):
        self.assertEqual(db.access_reminder_gap_days(1), 7)
        self.assertEqual(db.access_reminder_gap_days(7), 7)
        self.assertEqual(db.access_reminder_gap_days(8), 30)
        self.assertEqual(db.access_reminder_gap_days(20), 30)

    def _due(self, row):
        from unittest import mock
        курсор = mock.MagicMock()
        курсор.fetchone.return_value = row
        with patch.object(db, "get_db_connection_context", _conn(курсор)):
            return db.access_reminder_due(UID, NOW)

    def test_первое_сразу(self):
        self.assertEqual(self._due(None), (True, 1))

    def test_через_шесть_дней_рано_через_семь_пора(self):
        self.assertEqual(self._due((3, NOW - timedelta(days=6))), (False, 4))
        self.assertEqual(self._due((3, NOW - timedelta(days=7))), (True, 4))

    def test_после_восьмого_ждём_месяц(self):
        self.assertEqual(self._due((8, NOW - timedelta(days=20))), (False, 9))
        self.assertEqual(self._due((8, NOW - timedelta(days=30))), (True, 9))


class Картинка(unittest.TestCase):

    def test_рисуется_png(self):
        from backend.access_reminder_card import render_access_reminder_card
        png = render_access_reminder_card(light_stars=160, pro_stars=400)
        self.assertTrue(png.startswith(b"\x89PNG"))
        self.assertGreater(len(png), 10_000)


class СтрокаОтчёта(unittest.TestCase):

    def test_числа_и_оплаты(self):
        with patch.object(bot_3, "access_state_counts",
                          return_value={"pro": 1, "light": 2, "free_month": 10, "locked": 3, "unknown": 0}), \
             patch.object(bot_3, "count_star_payments_last_day", return_value={"light": 1, "pro": 0}):
            line = bot_3._access_state_line()
        self.assertIn("бесплатный месяц <b>10</b>", line)
        self.assertIn("заперто <b>3</b>", line)
        self.assertIn("Лайт 1 / Полный 0", line)
        self.assertNotIn("без начала", line)

    def test_без_начала_отсчёта_помечается(self):
        with patch.object(bot_3, "access_state_counts",
                          return_value={"pro": 0, "light": 0, "free_month": 0, "locked": 0, "unknown": 2}), \
             patch.object(bot_3, "count_star_payments_last_day", return_value={"light": 0, "pro": 0}):
            line = bot_3._access_state_line()
        self.assertIn("без начала отсчёта <b>2</b>", line)


class Обещание(unittest.TestCase):

    def test_зарегистрировано(self):
        p = fix_promises.by_key("access_reminders_over_cadence")
        self.assertIsNotNone(p)
        self.assertEqual(p.expected, 0)


if __name__ == "__main__":
    unittest.main()
