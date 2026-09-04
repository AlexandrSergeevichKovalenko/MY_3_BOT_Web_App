# -*- coding: utf-8 -*-
"""Прогон тестов НЕ имеет права делать ночную работу по живой базе.

ЧТО СЛУЧИЛОСЬ 28.08.2026. `run_phrase_night_check` в хвосте делает настоящую работу:
закрывает бесспорные вопросы, зовёт третьего судью (это деньги) и — с этого дня —
применяет решённые споры. Тест `test_sentences_get_no_breakdown` вызывает эту функцию
с подменёнными судьями, но хвост не подменяет. Хвост отработал по-настоящему: прогон в
22:20 UTC применил 64 правки в боевой базе. Итог совпал с тем, что владелец утвердил,
но сделал его ПРОГОН ТЕСТОВ, а не ночь, — и в следующий раз это могло быть не то, что
он утверждал.

Класс не новый: ровно так же локальный pytest оставил 1010 фантомных строк в ведомости
расходов (02.08.2026, см. `backend/tests/conftest.py`). Лечится тем же способом —
переменной, которую ставит conftest до импорта приложения, а прод не ставит никогда.
"""
import os
import unittest
from unittest import mock

from backend import phrase_night_check as ночь


class ХвостВыключенВТестах(unittest.TestCase):
    def test_conftest_sets_the_guard(self):
        """Ставится ДО импорта приложения, поэтому отказаться от него нельзя."""
        self.assertEqual(os.getenv("SKIP_NIGHT_SIDE_EFFECTS"), "1")

    def test_night_skips_the_real_work_under_the_guard(self):
        строка = {"unit_id": 777, "text": "фраза", "kind": "collocation",
                  "translation": "перевод"}
        with mock.patch.object(ночь, "pick_phrases_for_grammar_check", return_value=[строка]), \
             mock.patch.object(ночь, "_judge_once", return_value=[{"verdict": "ok"},
                                                                   {"verdict": "ok"}]), \
             mock.patch.object(ночь, "mark_phrase_checked"), \
             mock.patch.object(ночь, "count_phrases_left_for_grammar_check", return_value=0), \
             mock.patch.object(ночь, "count_open_phrase_reviews", return_value=0), \
             mock.patch.object(ночь, "settle_open_disputes") as судья, \
             mock.patch.object(ночь, "apply_settled_disputes") as применение, \
             mock.patch("backend.database.close_all_ok_phrase_reviews") as закрытие:
            отчёт = ночь.run_phrase_night_check(limit=1)
        судья.assert_not_called()
        применение.assert_not_called()
        закрытие.assert_not_called()
        self.assertTrue(отчёт.get("side_effects_skipped"))

    def test_production_does_the_work(self):
        """Запрет ровно один и снимается только отсутствием переменной."""
        строка = {"unit_id": 777, "text": "фраза", "kind": "collocation",
                  "translation": "перевод"}
        with mock.patch.dict(os.environ, {"SKIP_NIGHT_SIDE_EFFECTS": ""}), \
             mock.patch.object(ночь, "pick_phrases_for_grammar_check", return_value=[строка]), \
             mock.patch.object(ночь, "_judge_once", return_value=[{"verdict": "ok"},
                                                                   {"verdict": "ok"}]), \
             mock.patch.object(ночь, "mark_phrase_checked"), \
             mock.patch.object(ночь, "count_phrases_left_for_grammar_check", return_value=0), \
             mock.patch.object(ночь, "count_open_phrase_reviews", return_value=0), \
             mock.patch.object(ночь, "settle_open_disputes", return_value={}) as судья, \
             mock.patch.object(ночь, "apply_settled_disputes", return_value={}) as применение, \
             mock.patch("backend.database.close_all_ok_phrase_reviews", return_value=0):
            отчёт = ночь.run_phrase_night_check(limit=1)
        судья.assert_called_once()
        применение.assert_called_once()
        self.assertNotIn("side_effects_skipped", отчёт)


if __name__ == "__main__":
    unittest.main()
