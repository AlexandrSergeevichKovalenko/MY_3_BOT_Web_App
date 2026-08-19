"""Счётчик «не знаю» должен быть виден владельцу, а не только в логах.

Правило ноль спрашивает: «где счётчик случаев „не знаю“ и как владелец увидит их
число?». До 19.08.2026 ответа не было — оба случая («ответ не был переводом» и «тип
ошибки назвать не удалось») жили только в логах Railway, и меряли их разовыми
запросами по живой базе.
"""

import unittest
from unittest.mock import patch

import backend.backend_server as server


class CheckQualityCaptionTests(unittest.TestCase):
    def _caption(self, summary_extra):
        return server._build_translation_focus_pool_admin_report_caption(
            rows=[],
            summary={"total_today": 10, "total_yesterday": 8, "delta_total": 2, **summary_extra},
            snapshot_date=__import__("datetime").date(2026, 8, 19),
            tz_name="Europe/Vienna",
        )

    def test_counters_appear_in_the_daily_digest(self):
        caption = self._caption({
            "check_quality": {
                "days": 7, "checked": 120, "not_a_translation": 9,
                "mistakes_total": 80, "type_unknown": 3, "type_unknown_pct": 3.8,
            }
        })
        self.assertIn("Разбор за 7 дн.", caption)
        self.assertIn("проверено 120", caption)
        self.assertIn("не переводов 9", caption)
        self.assertIn("тип не назван 3", caption)
        self.assertIn("3.8%", caption)

    def test_digest_survives_without_the_counters(self):
        """Замер не сошёлся — отчёт всё равно уходит, просто без этой строки."""
        caption = self._caption({})
        self.assertNotIn("Разбор за", caption)
        self.assertIn("Translation pool", caption)


class CheckQualityCountersQueryTests(unittest.TestCase):
    def test_share_is_computed_against_mistakes_not_against_checks(self):
        """Доля «тип не назван» считается от числа ЗАПИСЕЙ ОБ ОШИБКАХ.

        Считать её от числа проверенных переводов нельзя: у одного перевода бывает
        несколько записей об ошибках, и доля молча занизилась бы.
        """
        from backend import translation_workflow as tw

        class _Cursor:
            def __init__(self):
                self.answers = [(4, 100), (5,), (25,)]
            def execute(self, *args, **kwargs):
                self.current = self.answers.pop(0)
            def fetchone(self):
                return self.current
            def __enter__(self):
                return self
            def __exit__(self, *exc):
                return False

        class _Conn:
            def cursor(self):
                return _Cursor()
            def __enter__(self):
                return self
            def __exit__(self, *exc):
                return False

        with patch.object(tw, "get_db_connection", return_value=_Conn()), \
             patch.object(tw, "db_acquire_scope", lambda *a, **k: __import__("contextlib").nullcontext()):
            counters = tw.get_check_quality_counters(days=7)

        self.assertEqual(counters["not_a_translation"], 4)
        self.assertEqual(counters["checked"], 100)
        self.assertEqual(counters["type_unknown"], 5)
        self.assertEqual(counters["mistakes_total"], 25)
        self.assertEqual(counters["type_unknown_pct"], 20.0)


if __name__ == "__main__":
    unittest.main()
