"""Расчёт «на сколько дней хватает банка» и «сколько дозаказать ночью».

Главное, что здесь закреплено: при мелком расходе НЕ генерируем ничего. Владелец
14.08.2026: «зачем мне сейчас формировать под 2000 пользователей, это же глупо» —
и он прав, потому что банк зависит не от числа людей, а от расхода самого активного.
"""

import unittest

from backend.task_supply import (FORECAST_MARGIN, TARGET_SUPPLY_DAYS,
                                 TOPUP_PER_NIGHT_CAP,
                                 percentile, plan_topups, shortfall, supply_days,
                                 verdict)


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

    def test_target_is_a_month_and_a_half(self):
        """Было 30 дней. Владелец поднял до 45 (20.08.2026) вместе с правилом
        «решённое человеку не возвращается никогда»: раньше задание после отдыха
        возвращалось в оборот, теперь расход идёт только в одну сторону."""
        self.assertEqual(TARGET_SUPPLY_DAYS, 45)


class TopupPlanTests(unittest.TestCase):
    """Заказ на ночь. Владелец 14.08.2026: «зачем мне сейчас формировать под 2000
    пользователей» — поэтому при здоровом запасе список заказа обязан быть ПУСТЫМ."""

    def _row(self, **kw):
        base = {"kind": "rb", "title": "Ребусы", "bank_total": 338, "order_now": 0}
        base.update(kw)
        return base

    def test_healthy_supply_orders_nothing_at_all(self):
        self.assertEqual(plan_topups([self._row(order_now=0)]), [])

    def test_shortfall_becomes_a_fill_up_to_target(self):
        """Пополнялки во всём проекте устроены как «дозаполни до N», а не «сделай N»."""
        plan = plan_topups([self._row(bank_total=20, order_now=10)])
        self.assertEqual(plan[0]["tonight"], 10)
        self.assertEqual(plan[0]["target_ready"], 30)
        self.assertEqual(plan[0]["deferred"], 0)

    def test_night_cap_defers_the_rest_instead_of_dropping_it(self):
        plan = plan_topups([self._row(bank_total=10, order_now=200)],
                           cap=TOPUP_PER_NIGHT_CAP)
        self.assertEqual(plan[0]["tonight"], TOPUP_PER_NIGHT_CAP)
        self.assertEqual(plan[0]["deferred"], 200 - TOPUP_PER_NIGHT_CAP)
        self.assertEqual(plan[0]["target_ready"], 10 + TOPUP_PER_NIGHT_CAP)

    def test_broken_measurement_orders_nothing(self):
        self.assertEqual(plan_topups([{"kind": "cw", "error": "замер не удался"}]), [])

    def test_biggest_need_goes_first(self):
        plan = plan_topups([self._row(kind="rb", order_now=3),
                            self._row(kind="cw", order_now=9)])
        self.assertEqual(plan[0]["kind"], "cw")


class VerdictTests(unittest.TestCase):
    def test_reads_like_a_human_sentence(self):
        self.assertIn("ДНО", verdict(3))
        self.assertIn("мало", verdict(20))
        self.assertEqual(verdict(45), "норма")
        # Подпись «мало» обязана называть ТУ ЖЕ границу, по которой она поставлена.
        # 23.08.2026 отчёт написал «хватит на 42 дн. (мало: меньше месяца)»: цель подняли
        # до 45, а текст остался от старой цели в 30 дней и стал враньём.
        self.assertIn(str(TARGET_SUPPLY_DAYS), verdict(TARGET_SUPPLY_DAYS - 1))
        self.assertNotIn("месяц", verdict(TARGET_SUPPLY_DAYS - 1))
        self.assertEqual(verdict(400), "с запасом")
        self.assertEqual(verdict(float("inf")), "не расходуется")




class ForecastMarginTests(unittest.TestCase):
    """Заказ работает на завтра, а замер показывает вчера. Решение владельца
    15.08.2026: считать средний расход живого человека и добавлять 20%."""

    def test_margin_is_twenty_percent(self):
        self.assertAlmostEqual(FORECAST_MARGIN, 1.20, places=2)

    def test_margin_orders_more_than_bare_measurement(self):
        bare = shortfall(20, 1.0)
        with_margin = shortfall(20, 1.0 * FORECAST_MARGIN)
        self.assertGreater(with_margin, bare)

    def test_margin_does_not_wake_a_healthy_bank(self):
        """Запас глубокий — 20% сверху всё равно не должны заставить нас платить."""
        self.assertEqual(shortfall(426, 0.39 * FORECAST_MARGIN), 0)



class BenchmarkIsTheHeaviestUserTests(unittest.TestCase):
    """Числа владельца 15.08.2026: один тратит 5 в сутки, другой 1. Среднее — 3.
    При банке 30 расчёт по среднему обещает 10 дней, а прожорливый упрётся на шестой:
    запас в 30 дней ему не обеспечен, хотя отчёт зелёный."""

    def test_average_underestimates_the_heavy_user(self):
        rates = [5.0, 1.0]
        avg = sum(rates) / len(rates)
        top = percentile(rates, 0.95)
        self.assertGreater(top, avg)
        self.assertLess(supply_days(30, top), supply_days(30, avg))
        self.assertLessEqual(supply_days(30, top), 6.5,
                             "по самому активному банк кончается на шестой день")

    def test_one_outlier_still_does_not_set_the_bar(self):
        """Владелец на тестах не должен задирать заказ для всех — потому не максимум."""
        rates = [1.0] * 20 + [40.0]
        self.assertLess(percentile(rates, 0.95), 40.0)

    def test_order_covers_the_heavy_user_for_the_whole_target(self):
        rates = [5.0, 1.0]
        per_day = percentile(rates, 0.95) * FORECAST_MARGIN
        need = shortfall(30, per_day)
        self.assertGreaterEqual((30 + need) / 5.0, TARGET_SUPPLY_DAYS,
                                "после заказа самому активному должно хватить на месяц")

if __name__ == "__main__":
    unittest.main()
