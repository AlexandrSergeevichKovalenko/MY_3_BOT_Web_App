import unittest
from datetime import datetime, timezone
from unittest.mock import patch

import backend.backend_server as server


class SrsManualQueueTests(unittest.TestCase):
    def test_build_next_srs_payload_manual_mode_respects_due_at(self):
        now = datetime(2026, 5, 12, 8, 0, 0, tzinfo=timezone.utc)

        with patch.object(server, "get_next_due_srs_card", return_value=None) as due_mock, \
             patch.object(server, "count_new_cards_introduced_today", return_value=0), \
             patch.object(server, "get_next_new_srs_candidate", return_value=None):
            payload = server._build_next_srs_payload(
                user_id=55,
                source_lang="ru",
                target_lang="de",
                now_utc=now,
                queue_source="manual",
                allowed_card_ids=[11, 12, 13],
                include_queue_info=False,
            )

        self.assertEqual(payload["queue_source"], "manual")
        self.assertIsNone(payload["card"])
        due_mock.assert_called_once_with(
            user_id=55,
            now_utc=now,
            source_lang="ru",
            target_lang="de",
            allowed_card_ids=[11, 12, 13],
            bypass_due_at=False,
            cursor=None,
        )

    def test_compute_srs_queue_info_manual_mode_counts_real_due_cards_only(self):
        now = datetime(2026, 5, 12, 8, 0, 0, tzinfo=timezone.utc)

        with patch.object(server, "count_due_srs_cards", return_value=2) as due_count_mock, \
             patch.object(server, "count_new_cards_introduced_today", return_value=3), \
             patch.object(server, "count_due_cards_reviewed_today", return_value=1), \
             patch.object(server, "has_available_new_srs_cards", return_value=True):
            queue_info = server._compute_srs_queue_info(
                user_id=55,
                now_utc=now,
                source_lang="ru",
                target_lang="de",
                queue_source="manual",
                allowed_card_ids=[11, 12, 13, 14],
                cursor=None,
            )

        self.assertEqual(queue_info["queue_source"], "manual")
        self.assertEqual(queue_info["due_count_total"], 2)
        self.assertEqual(queue_info["due_limit_today"], 4)
        self.assertEqual(queue_info["new_remaining_today"], 1)
        due_count_mock.assert_called_once_with(
            user_id=55,
            now_utc=now,
            source_lang="ru",
            target_lang="de",
            allowed_card_ids=[11, 12, 13, 14],
            bypass_due_at=False,
            cursor=None,
        )


if __name__ == "__main__":
    unittest.main()
