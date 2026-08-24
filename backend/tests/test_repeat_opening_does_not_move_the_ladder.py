"""Лестница возврата считает СДАЧИ, а не заходы в уже сданное задание.

Разбор 24.08.2026. Проверяющие устроены как анти-повтор: второй раз сдать то же задание
нельзя, возвращается сохранённый разбор с флагом `already_answered`. Но память ротации
писалась на КАЖДОМ таком заходе. Пока лестница двигалась только на верном ответе, это
почти не вредило; с 24.08.2026 у кроссвордов и анаграмм появилась лестница и для
неверного (7 → 14 → 30), и три открытия заваленного кроссворда увели бы его от человека
на месяц, а три открытия решённого — 90 → 120 → «никогда».

Рейтинг от этого защищён своим `ON CONFLICT DO NOTHING` (`record_challenge_result`),
а ротация — нет, поэтому сторож стоит у вызывающего.
"""

import unittest
from unittest.mock import patch

import backend.backend_server as server


class RepeatSubmitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = server.app.test_client()

    def _submit(self, *, already_answered: bool):
        verdict = {"total": 4, "correct_count": 4, "already_answered": already_answered}
        with patch.object(server, "_telegram_hash_is_valid", return_value=True), \
             patch.object(server, "_parse_telegram_init_data",
                          return_value={"user": {"id": 77, "first_name": "Alex"}}), \
             patch.object(server, "_is_webapp_user_allowed", return_value=True), \
             patch("backend.answer_eval.evaluate_crossword", return_value=verdict), \
             patch("backend.answer_eval.content_ranking_key", return_value="cw:c1"), \
             patch("backend.database.record_challenge_result"), \
             patch("backend.database.compute_challenge_ranking", return_value={}), \
             patch("backend.database.record_user_task_answer") as remember, \
             patch("backend.database.mark_interactive_inbox_answered", return_value=None), \
             patch("backend.database.enqueue_challenge_notification"):
            response = self.client.post("/api/answer/submit", json={
                "initData": "valid", "kind": "cw", "id": 1, "answer": "HAUS",
            })
        return response, remember

    def test_first_submission_is_remembered(self):
        response, remember = self._submit(already_answered=False)
        self.assertEqual(response.status_code, 200)
        remember.assert_called_once()
        self.assertEqual(remember.call_args.kwargs["kind"], "cw")
        self.assertEqual(remember.call_args.kwargs["task_key"], "cw:c1")

    def test_reopening_a_finished_task_does_not_touch_the_ladder(self):
        response, remember = self._submit(already_answered=True)
        self.assertEqual(response.status_code, 200)
        remember.assert_not_called()


if __name__ == "__main__":
    unittest.main()
