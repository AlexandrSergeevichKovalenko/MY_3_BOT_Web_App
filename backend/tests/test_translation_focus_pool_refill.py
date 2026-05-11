import unittest
from unittest.mock import patch

import backend.backend_server as server


class TranslationFocusPoolRefillTests(unittest.TestCase):
    def test_refill_candidates_include_all_underfilled_focuses(self):
        payloads = {
            f"focus_{idx}": {
                "key": f"focus_{idx}",
                "label": f"Focus {idx}",
                "kind": "preset",
                "_pool_levels": ["b1"],
            }
            for idx in range(1, 9)
        }
        targets = {
            (f"focus_{idx}", "b1"): 12
            for idx in range(1, 9)
        }
        low_watermarks = {
            (f"focus_{idx}", "b1"): 8
            for idx in range(1, 9)
        }
        pool_rows = [
            {"focus_key": f"focus_{idx}", "level": "b1", "ready_count": 2, "focus_label": f"Focus {idx}"}
            for idx in range(1, 8)
        ] + [
            {"focus_key": "focus_8", "level": "b1", "ready_count": 12, "focus_label": "Focus 8"}
        ]
        readiness_rows = [
            {"focus_key": f"focus_{idx}", "sessions_started": idx, "ready_zero_starts": 0, "background_fill_required_count": 0, "fill_underfilled_count": 0, "fill_failed_count": 0}
            for idx in range(1, 9)
        ]

        with patch.object(server, "_all_translation_focus_pool_payloads", return_value=payloads), \
             patch.object(server, "_build_translation_focus_pool_bucket_targets", return_value=(low_watermarks, targets)), \
             patch.object(server, "get_translation_focus_pool_bucket_counts", return_value=pool_rows), \
             patch.object(server, "get_translation_readiness_bucket_rollup", return_value=readiness_rows):
            candidates = server._list_translation_focus_pool_refill_candidates()

        candidate_keys = [str(item.get("key") or "") for item in candidates]
        self.assertEqual(candidate_keys, [f"focus_{idx}" for idx in range(7, 0, -1)])

    def test_admin_report_scheduler_runs_refill_before_report(self):
        calls = []

        def fake_refill(*, force=False, tz_name=None):
            calls.append(("refill", bool(force), tz_name))
            return {"ok": True, "skipped": False}

        def fake_report(*, force=False):
            calls.append(("report", bool(force)))
            return {"ok": True}

        with patch.object(server, "TRANSLATION_FOCUS_POOL_PREWARM_ENABLED", True), \
             patch.object(server, "TRANSLATION_FOCUS_POOL_REFILL_TZ", "Europe/Vienna"), \
             patch.object(server, "_dispatch_translation_focus_pool_refill", side_effect=fake_refill), \
             patch.object(server, "_send_translation_focus_pool_admin_report", side_effect=fake_report):
            server._run_translation_focus_pool_admin_report_scheduler_job()

        self.assertEqual(calls[0], ("refill", False, "Europe/Vienna"))
        self.assertEqual(calls[1], ("report", False))


if __name__ == "__main__":
    unittest.main()
