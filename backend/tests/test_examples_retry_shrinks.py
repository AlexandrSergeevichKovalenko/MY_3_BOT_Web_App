# -*- coding: utf-8 -*-
"""Число непереписанных примеров обязано УМЕНЬШАТЬСЯ, а не стоять в отчёте вечно.

Владелец 24.08.2026, увидев строку «примеры к переписыванию: 111», спросил: «а как число
будет уменьшаться? есть ли механизм под капотом?» Механизма не было. Здесь заперто то,
что его делает механизмом, а не списком:

  • у карточки есть счётчик попыток — иначе первая неудача неотличима от третьей;
  • три неудачи подряд уводят карточку ВЛАДЕЛЬЦУ, а не крутят её ночь за ночью;
  • повтор просит у модели ДРУГОЙ вариант (температура выше нуля), иначе он бессмыслен;
  • «посчитать не удалось» и «ноль» — разные строки в отчёте.
"""
import os
import unittest
from unittest import mock

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SECOND_VOICE_CHECK_DISABLED", "1")

from backend import example_retry  # noqa: E402


class RetryRulesTests(unittest.TestCase):
    def test_three_attempts_then_the_owner_decides(self):
        """Машина обязана признать, что исчерпала себя, а не жечь деньги вечно."""
        self.assertEqual(example_retry.MAX_ATTEMPTS, 3)

    def test_retry_asks_for_a_different_variant(self):
        """Температура ноль дала бы тот же текст, который уже забраковали."""
        import inspect
        source = inspect.getsource(example_retry.retry_batch)
        self.assertIn("temperature=0.6", source)

    def test_escalation_writes_into_the_existing_owner_queue(self):
        cur = mock.Mock()
        example_retry._escalate(cur, 7, "die Hose anhaben", "быть главным", "не вышло")
        sqls = " ".join(str(call.args[0]) for call in cur.execute.call_args_list)
        self.assertIn("bt_3_phrase_review", sqls)      # та же очередь, не седьмой канал
        self.assertIn("bt_3_field_checks", sqls)       # и отметка снимается с «дефекта»

    def test_counter_says_do_not_know_instead_of_zero(self):
        """-1 значит «не смогли посчитать». Ноль значит «чисто». Путать нельзя."""
        with mock.patch("backend.database.get_db_connection_context",
                        side_effect=RuntimeError("база молчит")):
            self.assertEqual(example_retry.count_open_defects(), -1)


class ReportLineTests(unittest.TestCase):
    def _line(self, value):
        import importlib
        bot = importlib.import_module("bot_3")
        with mock.patch("backend.example_retry.count_open_defects", return_value=value):
            return bot._examples_retry_line()

    def test_zero_is_stated_out_loud(self):
        self.assertIn("не осталось", self._line(0))

    def test_number_is_shown_with_what_happens_next(self):
        line = self._line(111)
        self.assertIn("111", line)
        self.assertIn("Ночью", line)

    def test_unknown_is_not_reported_as_clean(self):
        line = self._line(-1)
        self.assertIn("не удалось", line)
        self.assertNotIn("не осталось", line)


if __name__ == "__main__":
    unittest.main()
