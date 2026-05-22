import unittest
from unittest.mock import patch

import backend.backend_server as server


class TodayCardsProgressSyncTests(unittest.TestCase):
    def test_cards_due_task_uses_review_time_even_outside_today_timer(self):
        plan = {
            "plan_date": "2026-05-22",
            "items": [
                {
                    "id": 11,
                    "task_type": "cards",
                    "title": "Карточки: повторение",
                    "estimated_minutes": 15,
                    "status": "todo",
                    "payload": {
                        "mode": "fsrs_due",
                        "limit": 20,
                        "due_total": 20,
                        "timer_seconds": 0,
                    },
                }
            ],
        }
        payload_synced_item = {
            **plan["items"][0],
            "payload": {
                **plan["items"][0]["payload"],
                "timer_seconds": 901,
                "timer_goal_seconds": 900,
                "timer_progress_percent": 100.0,
            },
        }
        done_item = {
            **payload_synced_item,
            "status": "done",
        }

        with patch.object(server, "count_card_review_response_seconds_today", return_value=901), \
             patch.object(server, "count_due_srs_cards", return_value=17), \
             patch.object(server, "update_daily_plan_item_payload", return_value=payload_synced_item) as payload_mock, \
             patch.object(server, "update_daily_plan_item_status", return_value=done_item) as status_mock, \
             patch.object(server, "_announce_today_task_completion_to_group"):
            result = server._sync_today_plan_cards_progress(
                user_id=117649764,
                username="alex",
                plan=plan,
                source_lang="ru",
                target_lang="de",
                trigger="today_fetch",
            )

        self.assertIsInstance(result, dict)
        synced_item = (result.get("items") or [None])[0]
        self.assertEqual(synced_item["status"], "done")
        self.assertEqual(synced_item["payload"]["timer_seconds"], 901)
        self.assertEqual(synced_item["payload"]["timer_goal_seconds"], 900)
        self.assertEqual(synced_item["payload"]["timer_progress_percent"], 100.0)
        payload_mock.assert_called_once()
        status_mock.assert_called_once_with(user_id=117649764, item_id=11, status="done")


if __name__ == "__main__":
    unittest.main()
