"""Картиночный квиз артиклей: каждому своя картинка из общего банка.

До 15.08.2026 картинка выбиралась ОДИН раз до цикла рассылки и уходила всем сразу
(`bot_3.py`, `_send_scheduled_article_quiz`), а ответ человека писался только в свою
табличку `bt_3_article_quiz_answers` и на следующую выдачу никак не влиял. Банк на 452
готовые записи (замер 14.08.2026) позволяет каждому идти своим путём, не тратя на
генерацию ни копейки: разным людям МОЖНО дать одно и то же задание, нельзя только
повторять одному и тому же.
"""

from datetime import datetime, timedelta, timezone
import unittest
from unittest.mock import patch

import bot_3
import backend.database as db


class ArticleQuizWritesRotationTests(unittest.TestCase):
    def test_answer_lands_in_rotation_memory(self):
        with patch.object(bot_3, "record_user_task_answer") as rec:
            bot_3._remember_article_quiz_answer(user_id=7, word_id="42", is_correct=True)
        rec.assert_called_once()
        kwargs = rec.call_args.kwargs
        self.assertEqual(kwargs["user_id"], 7)
        self.assertEqual(kwargs["kind"], "article_quiz")
        self.assertEqual(kwargs["task_key"], "42")
        self.assertTrue(kwargs["is_correct"])

    def test_text_word_id_is_accepted(self):
        """15.08.2026: `int(word_id)` роняло КАЖДОЕ нажатие кнопки ещё до ответа
        человеку — в банке ключ текстовый («Rabe»), а не число."""
        with patch.object(bot_3, "record_user_task_answer") as rec:
            bot_3._remember_article_quiz_answer(user_id=7, word_id="Rabe",
                                                is_correct=False)
        self.assertEqual(rec.call_args.kwargs["task_key"], "Rabe")

    def test_failure_to_remember_never_breaks_the_answer(self):
        """Память служебная: упала — человек всё равно получает разбор своего ответа."""
        with patch.object(bot_3, "record_user_task_answer", side_effect=RuntimeError):
            bot_3._remember_article_quiz_answer(user_id=7, word_id=42, is_correct=False)


class PerUserPickTests(unittest.TestCase):
    def _bank(self, n=5):
        return [{"word_id": i, "word": f"w{i}", "image_object_key": f"k{i}"}
                for i in range(1, n + 1)]

    def test_two_people_get_different_pictures(self):
        """Главная проверка. До правки это число равно единице по определению."""
        now = datetime.now(timezone.utc)
        solved_by_first = {"1": {"seen_count": 1, "correct_count": 1,
                                 "last_seen_at": now,
                                 "next_eligible_at": now + timedelta(days=90),
                                 "retired_at": None}}
        with patch.object(db, "list_ready_article_quiz_entries",
                          return_value=self._bank()), \
             patch.object(db, "get_user_task_state",
                          side_effect=lambda u, k, keys: solved_by_first if u == 1 else {}):
            first = db.pick_article_quiz_for_user(1, cooldown_days=14, card_kind=None)
            second = db.pick_article_quiz_for_user(2, cooldown_days=14, card_kind=None)
        self.assertNotEqual(first["word_id"], 1,
                            "решённую картинку человеку сразу не возвращаем")
        self.assertEqual(second["word_id"], 1,
                         "второму человеку она ещё не показывалась — можно")

    def test_empty_bank_returns_none_not_crash(self):
        with patch.object(db, "list_ready_article_quiz_entries", return_value=[]), \
             patch.object(db, "get_user_task_state", return_value={}):
            self.assertIsNone(
                db.pick_article_quiz_for_user(1, cooldown_days=14, card_kind=None))

    def test_broken_personal_pick_falls_back_to_the_common_picture(self):
        """Личный отбор сломался — человек молча получает общую картинку, не ошибку."""
        with patch.object(db, "list_ready_article_quiz_entries",
                          side_effect=RuntimeError), \
             patch.object(db, "pick_next_article_quiz",
                          return_value={"word_id": 99}) as common:
            out = db.pick_article_quiz_for_user(1, cooldown_days=14, card_kind=None)
        self.assertEqual(out["word_id"], 99)
        common.assert_called_once()


if __name__ == "__main__":
    unittest.main()
