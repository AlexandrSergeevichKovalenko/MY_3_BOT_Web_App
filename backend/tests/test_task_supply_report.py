"""Отчёт о запасе заданий читает человек, а не программа.

Правило владельца: у каждого факта должно быть решение. Поэтому в отчёте не просто
число «осталось 12», а «дозаказать сегодня ночью столько-то» — либо явное «дозаказывать
нечего, деньги не тратим».
"""

import unittest
from unittest.mock import MagicMock, patch

import backend.database as db
from backend.task_supply_report import build_task_supply_report, task_supply_alerts


def _row(title="Ребусы", **kw):
    base = {"kind": "rb", "title": title, "bank_total": 338, "people": 3,
            "per_day": 1.0, "blocked_deepest": 300, "available": 38,
            "supply_days": 38.0, "order_now": 0}
    base.update(kw)
    return base


class ReportTextTests(unittest.TestCase):
    def test_thin_supply_says_what_to_order(self):
        text = build_task_supply_report([_row(supply_days=5.0, available=5,
                                              order_now=25)])
        self.assertIn("🔴", text)
        self.assertIn("дозаказать сегодня ночью", text)
        self.assertIn("25", text)

    def test_deep_supply_orders_nothing(self):
        text = build_task_supply_report([_row(supply_days=400.0, order_now=0)])
        self.assertNotIn("дозаказать сегодня ночью", text)
        self.assertIn("🟢", text)

    def test_unused_game_is_named_and_costs_nothing(self):
        text = build_task_supply_report([_row(title="Анаграммы", per_day=0.0,
                                              supply_days=float("inf"))])
        self.assertIn("Не выдавались", text)
        self.assertIn("Анаграммы", text)

    def test_every_live_game_is_listed_by_name(self):
        """15.08.2026: отчёт свернул пять живых игр в одну строку «не расходуются» и
        оставил на виду самую редкую. Владелец прочитал это как бред — и был прав."""
        text = build_task_supply_report([
            _row(title="Анаграммы", per_day=0.40, supply_days=195.0),
            _row(title="Аудирование", per_day=0.39, supply_days=77.0),
            _row(title="Кроссворды", per_day=0.37, supply_days=392.0),
            _row(title="Картиночный квиз артиклей", per_day=0.14, supply_days=3000.0),
        ])
        for name in ("Анаграммы", "Аудирование", "Кроссворды",
                     "Картиночный квиз артиклей"):
            self.assertIn(name, text)
        self.assertLess(text.index("Аудирование"),
                        text.index("Кроссворды"),
                        "первым идёт то, где раньше кончится")

    def test_broken_measurement_is_shown_not_hidden(self):
        text = build_task_supply_report([{"kind": "cw", "error": "замер не удался"}])
        self.assertIn("замер не удался", text)

    def test_empty_input_says_so_plainly(self):
        self.assertIn("пуста", build_task_supply_report([]))


class AlertTests(unittest.TestCase):
    def test_bottom_within_a_week_is_an_alert(self):
        self.assertTrue(task_supply_alerts([_row(supply_days=3.0, order_now=27)]))

    def test_healthy_supply_is_silent(self):
        self.assertEqual(task_supply_alerts([_row(supply_days=90.0)]), [])

    def test_unused_game_is_not_an_alert(self):
        self.assertEqual(
            task_supply_alerts([_row(per_day=0.0, supply_days=float("inf"))]), [])


class MeasureTests(unittest.TestCase):
    def test_unknown_kind_is_reported_not_crashed(self):
        out = db.measure_task_supply("нет-такого")
        self.assertIn("error", out)

    def test_measurement_uses_the_deepest_person_not_the_average(self):
        cur = MagicMock()
        # банк 100; трое: решают по 1/сутки, закрыто 10, 20 и 90 заданий
        cur.fetchone.return_value = [100]
        cur.fetchall.side_effect = [
            [(1, 1.0), (2, 1.0), (3, 1.0)],        # СДАЧИ — это и есть расход банка
            [(1, 2.0), (2, 2.0), (3, 2.0)],        # отправки — отдельным числом
            [(1, 10), (2, 20), (3, 90)],           # закрыто из памяти ротации
        ]
        conn = MagicMock()
        conn.cursor.return_value.__enter__.return_value = cur
        ctx = MagicMock()
        ctx.__enter__.return_value = conn
        with patch.object(db, "get_db_connection_context", return_value=ctx), \
             patch.object(db, "ensure_task_rotation_schema"), \
             patch.object(db, "_task_rotation_writes_disabled", return_value=False):
            out = db.measure_task_supply("rb")
        self.assertLess(out["available"], 50,
                        "запас надо считать по самому продвинутому, а не по среднему")
        self.assertGreater(out["order_now"], 0)


if __name__ == "__main__":
    unittest.main()
