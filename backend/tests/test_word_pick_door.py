# -*- coding: utf-8 -*-
"""«Слова со вчерашних тренировок»: дверь записи отбора и чистые функции слотов.
Стратегия: docs/tasks/word_pick_review_strategy.md."""
import os
import unittest
from datetime import date, datetime
from zoneinfo import ZoneInfo

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")

from backend import word_pick  # noqa: E402

VIENNA = ZoneInfo("Europe/Vienna")


class Слоты(unittest.TestCase):

    def test_до_вечернего_слота_идёт_утренний_проход(self):
        self.assertEqual(word_pick.slot_now(datetime(2026, 9, 5, 7, 25, tzinfo=VIENNA)), "am")
        self.assertEqual(word_pick.slot_now(datetime(2026, 9, 5, 19, 34, tzinfo=VIENNA)), "am")

    def test_с_19_35_идёт_вечерний_проход(self):
        self.assertEqual(word_pick.slot_now(datetime(2026, 9, 5, 19, 35, tzinfo=VIENNA)), "pm")
        self.assertEqual(word_pick.slot_now(datetime(2026, 9, 5, 23, 59, tzinfo=VIENNA)), "pm")

    def test_день_из_ссылки_разбирается_строго(self):
        self.assertEqual(word_pick.parse_day("20260905"), date(2026, 9, 5))
        self.assertEqual(word_pick.parse_day("2026-09-05"), date(2026, 9, 5))
        for плохое in ("", None, "2026095", "abc", "20261305", 20260905.0):
            self.assertIsNone(word_pick.parse_day(плохое), плохое)

    def test_номер_строки_ведомости_различает_утро_и_вечер(self):
        self.assertEqual(word_pick.day_id(date(2026, 9, 5), "am"), 202609051)
        self.assertEqual(word_pick.day_id(date(2026, 9, 5), "pm"), 202609052)
        self.assertEqual(word_pick.deeplink_for(date(2026, 9, 5)), "ans_wp_20260905")
