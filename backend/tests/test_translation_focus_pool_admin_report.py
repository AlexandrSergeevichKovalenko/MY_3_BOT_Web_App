import unittest
from datetime import date

import backend.backend_server as server


class TranslationFocusPoolAdminReportTextTests(unittest.TestCase):
    def test_build_text_report_lists_theme_and_level_deltas(self):
        rows = [
            {
                "focus_key": "topic_b",
                "focus_label": "Nebensaetze",
                "level": "b2",
                "today_ready": 9,
                "yesterday_ready": 5,
                "delta": 4,
                "target_ready": 12,
                "low_watermark": 8,
                "is_candidate": True,
                "candidate_rank": 1,
                "demand_score": 20,
            },
            {
                "focus_key": "topic_a",
                "focus_label": "Praepositionen",
                "level": "b1",
                "today_ready": 6,
                "yesterday_ready": 6,
                "delta": 0,
                "target_ready": 10,
                "low_watermark": 7,
                "is_candidate": True,
                "candidate_rank": 0,
                "demand_score": 30,
            },
            {
                "focus_key": "topic_a",
                "focus_label": "Praepositionen",
                "level": "c1",
                "today_ready": 4,
                "yesterday_ready": 1,
                "delta": 3,
                "target_ready": 9,
                "low_watermark": 6,
                "is_candidate": True,
                "candidate_rank": 0,
                "demand_score": 30,
            },
        ]
        summary = {
            "source_lang": "ru",
            "target_lang": "de",
            "total_today": 19,
            "total_yesterday": 12,
            "delta_total": 7,
            "at_or_above_target": 0,
            "with_target": 3,
            "rows": 3,
            "missing_previous_snapshot": False,
            "readiness": {
                "lookback_days": 30,
                "sessions_started": 44,
                "ready_count_eq_0_pct": 0.25,
                "background_fill_required_rate": 0.5,
            },
            "refill_state": {
                "today_completed": False,
                "latest_completed_run_period": "2026-05-06",
                "latest_completed_finished_at": "2026-05-06 23:00:00+02:00",
            },
        }

        report = server._build_translation_focus_pool_admin_report_caption(
            rows=rows,
            summary=summary,
            snapshot_date=date(2026, 5, 7),
            tz_name="Europe/Vienna",
        )

        self.assertIn("📊 Translation pool · 2026-05-07 (Europe/Vienna)", report)
        self.assertIn("RU → DE  ·  Сегодня: 19  |  Вчера: 12  |  Δ +7", report)
        self.assertIn("Readiness: zero-ready 25%  ·  fill-required 50%", report)
        self.assertIn("Refill: today no · last 2026-05-06 · finished 2026-05-06 23:00:00+02:00", report)
        self.assertIn("🔴 Дефицит (топ тем):", report)
        self.assertIn("Praepositionen: 10/19  Δ +3  gap −9", report)
        self.assertIn("Nebensaetze: 9/12  Δ +4  gap −3", report)


if __name__ == "__main__":
    unittest.main()
