import unittest

from backend.database import _build_plan_metric


class PlanProgressMetricTests(unittest.TestCase):
    def test_forecast_uses_current_actual_after_goal_is_met(self):
        metric = _build_plan_metric(30, 63, days_elapsed=1, days_total=7)

        self.assertEqual(metric["actual"], 63.0)
        self.assertEqual(metric["forecast"], 63.0)
        self.assertEqual(metric["forecast_delta_vs_goal"], 33.0)

    def test_forecast_keeps_pace_projection_while_goal_not_met(self):
        metric = _build_plan_metric(70, 20, days_elapsed=2, days_total=7)

        self.assertEqual(metric["forecast"], 70.0)
        self.assertEqual(metric["expected_to_date"], 20.0)
        self.assertEqual(metric["delta_vs_expected"], 0.0)


if __name__ == "__main__":
    unittest.main()
