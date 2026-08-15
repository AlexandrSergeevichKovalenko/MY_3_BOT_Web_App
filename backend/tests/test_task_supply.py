"""Расчёт «на сколько дней хватает банка» и «сколько дозаказать ночью».

Главное, что здесь закреплено: при мелком расходе НЕ генерируем ничего. Владелец
14.08.2026: «зачем мне сейчас формировать под 2000 пользователей, это же глупо» —
и он прав, потому что банк зависит не от числа людей, а от расхода самого активного.
"""

import unittest

from backend.task_supply import (TARGET_SUPPLY_DAYS, percentile, shortfall,
                                 supply_days, verdict)


class PercentileTests(unittest.TestCase):
    def test_empty_is_zero(self):
        self.assertEqual(percentile([], 0.95), 0.0)

    def test_single_value(self):
        self.assertEqual(percentile([3.0], 0.95), 3.0)

    def test_one_outlier_does_not_drag_the_estimate(self):
        """Владелец гоняет тесты и расходует втрое больше всех — это не должно
        задирать потребность для остальных."""
        normal = [1.0] * 20
        self.assertLess(percentile(normal + [30.0], 0.95), 10.0)


class SupplyTests(unittest.TestCase):
    def test_days_are_available_over_rate(self):
        self.assertEqual(supply_days(60, 2.0), 30.0)

    def test_unused_game_never_runs_out(self):
        self.assertEqual(supply_days(5, 0), float("inf"))

    def test_deep_supply_orders_nothing(self):
        """Сегодня: 452 картинки, расход 0.3 в сутки — генератор обязан молчать."""
        self.assertEqual(shortfall(452, 0.3), 0)

    def test_thin_supply_orders_exactly_the_gap(self):
        # расход 2 в сутки, цель 30 дней = нужно 60, есть 20 → дозаказать 40
        self.assertEqual(shortfall(20, 2.0, target_days=30), 40)

    def test_unused_game_orders_nothing(self):
        self.assertEqual(shortfall(0, 0.0), 0)

    def test_target_is_a_month(self):
        self.assertEqual(TARGET_SUPPLY_DAYS, 30)


class VerdictTests(unittest.TestCase):
    def test_reads_like_a_human_sentence(self):
        self.assertIn("ДНО", verdict(3))
        self.assertIn("мало", verdict(20))
        self.assertEqual(verdict(45), "норма")
        self.assertEqual(verdict(400), "с запасом")
        self.assertEqual(verdict(float("inf")), "не расходуется")


if __name__ == "__main__":
    unittest.main()
