# -*- coding: utf-8 -*-
"""«Слова со вчерашних тренировок»: оценка идёт в НАСТОЯЩЕЕ расписание (решение
владельца 04.09.2026), набор дня не режется сроком, чужая карточка не принимается."""
import os
import unittest

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")

from backend import backend_server as srv  # noqa: E402


class ИсточникОчереди(unittest.TestCase):

    def test_pick_разрешён_как_источник_очереди(self):
        self.assertEqual(srv._normalize_flashcards_queue_source("pick"), "pick")
        self.assertEqual(srv._normalize_flashcards_queue_source("PICK "), "pick")
        self.assertEqual(srv._normalize_flashcards_queue_source("чушь"), "system")

    def test_обработчик_оценки_знает_ветку_pick(self):
        import inspect
        src = inspect.getsource(srv.review_srs_card)
        self.assertIn('queue_source == "pick"', src)
        self.assertIn("word_pick_card_ids(", src)
        self.assertIn("mark_word_pick_rated(", src)
        # Оценка пишется тем же путём, что и в Space Rep: schedule_review → upsert → журнал.
        self.assertIn("schedule_review(", src)
        self.assertIn("insert_card_review_log(", src)

    def test_набор_дня_отдаёт_подсказки_интервалов_как_space_rep(self):
        import inspect
        src = inspect.getsource(srv.answer_word_pick_set)
        self.assertIn("_build_srs_review_preview(", src)
        self.assertIn('_inbox_mark_kind_done(int(user_id), "wp")', src)
        self.assertIn("parse_day(", src)
