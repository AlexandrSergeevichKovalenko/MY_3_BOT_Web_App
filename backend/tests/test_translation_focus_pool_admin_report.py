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
        }

        report = server._build_translation_focus_pool_admin_report_text(
            rows=rows,
            summary=summary,
            snapshot_date=date(2026, 5, 7),
            tz_name="Europe/Vienna",
        )

        self.assertIn("Translation focus pool report", report)
        self.assertIn("Total ready now: 19 | Yesterday: 12 | Delta: +7", report)
        self.assertIn("Readiness 30d: sessions=44, zero-ready=25.0%, fill-required=50.0%", report)
        self.assertIn("1. Praepositionen | today 10 | yesterday 7 | delta +3 | gap 9", report)
        self.assertIn("B1: today 6 | yesterday 6 | delta +0 | low 7 | target 10", report)
        self.assertIn("C1: today 4 | yesterday 1 | delta +3 | low 6 | target 9", report)
        self.assertIn("2. Nebensaetze | today 9 | yesterday 5 | delta +4 | gap 3", report)
        self.assertIn("B2: today 9 | yesterday 5 | delta +4 | low 8 | target 12", report)


if __name__ == "__main__":
    unittest.main()
