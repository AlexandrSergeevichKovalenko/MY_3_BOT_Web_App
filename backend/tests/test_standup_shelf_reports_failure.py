"""Ночное пополнение полки стендапов обязано говорить, когда оно НЕ смогло.

Повод (29.08.2026). Полка стояла на 4 роликах из 30 с 21.08: каждую ночь работа
обходила каналы, упиралась в бюджет на первом же ролике без субтитров и добавляла
ноль. Сообщение владельцу уходило только при added > 0, поэтому семь ночей подряд
он не знал ничего. Рубрика выходит через день — запас кончился бы молча.

Молчим ТОЛЬКО когда полка полна. «Пробовали и не смогли» — всегда письмо.
"""
import unittest

from backend.standup_shelf import format_shelf_refill_report, refill_fell_short


class RefillFellShortTests(unittest.TestCase):
    def test_full_shelf_stays_silent(self):
        report = {"had_unused": 30, "target": 30, "added": 0,
                  "reason": "полка полна — в YouTube не ходили"}
        self.assertFalse(refill_fell_short(report))

    def test_nothing_added_on_a_half_empty_shelf_speaks_up(self):
        report = {"had_unused": 4, "target": 30, "added": 0, "now_unused": 4,
                  "swept": 3764, "attempted": 2, "no_transcript": 2, "budget_spent": True,
                  "budget_sec": 150}
        self.assertTrue(refill_fell_short(report))

    def test_no_candidates_at_all_speaks_up(self):
        # «кандидатов не получено (квота или сеть)» — ранний выход без пересчёта полки.
        report = {"had_unused": 4, "target": 30, "added": 0,
                  "reason": "кандидатов не получено (квота или сеть)"}
        self.assertTrue(refill_fell_short(report))

    def test_successful_refill_is_not_a_failure(self):
        report = {"had_unused": 4, "target": 30, "added": 7, "now_unused": 11}
        self.assertFalse(refill_fell_short(report))


class FailureTextTests(unittest.TestCase):
    def test_text_names_the_numbers_the_owner_needs(self):
        report = {"had_unused": 4, "target": 30, "added": 0, "now_unused": 4,
                  "swept": 3764, "attempted": 2, "no_transcript": 2, "short_transcript": 0,
                  "dur_skipped": 3116, "budget_spent": True, "budget_sec": 150}
        text = format_shelf_refill_report(report)
        self.assertIn("НЕ пополнилась", text)
        self.assertIn("4", text)          # сколько осталось
        self.assertIn("30", text)         # сколько нужно
        self.assertIn("3764", text)       # что обошли
        self.assertIn("без субтитров 2", text)
        self.assertIn("150", text)        # бюджет, в который упёрлись
        self.assertIn("/standup_shelf", text)

    def test_full_shelf_text_is_short_and_says_no_network(self):
        report = {"had_unused": 30, "target": 30, "added": 0,
                  "reason": "полка полна — в YouTube не ходили"}
        text = format_shelf_refill_report(report)
        self.assertIn("полка полна", text)
        self.assertNotIn("НЕ пополнилась", text)


if __name__ == "__main__":
    unittest.main()
