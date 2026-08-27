import unittest
from datetime import datetime, timezone
from unittest.mock import patch

import backend.backend_server as server


class SrsManualQueueTests(unittest.TestCase):
    def test_build_next_srs_payload_manual_mode_respects_due_at(self):
        now = datetime(2026, 5, 12, 8, 0, 0, tzinfo=timezone.utc)

        with patch.object(server, "get_next_due_srs_card", return_value=None) as due_mock, \
             patch.object(server, "count_new_cards_introduced_today", return_value=0), \
             patch.object(server, "get_next_new_srs_candidate", return_value=None), \
             patch.object(server, "get_next_new_srs_candidate_with_subscription",
                          return_value=None):
            # Подменена была только «...candidate», а код зовёт «...candidate_with_subscription»
            # — и та уходила в живую базу. Тест выглядел изолированным и им не был
            # (замер 24.08.2026).
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

    def test_manual_selection_availability_names_asleep_cards_and_return_time(self):
        """Экран обязан назвать ВТОРОЕ число: сколько из отмеченного доступно сейчас.

        27.08.2026: владелец отметил 9 слов, тренажёр отдал 8, девятое спало до
        следующего вечера — и на экране об этом не было ни слова, только «Выбрано
        сейчас: 9». Этот тест держит оба числа рядом: если сводка снова начнёт
        показывать один лишь размер выборки, он покраснеет.
        """
        now = datetime(2026, 8, 27, 10, 0, 0, tzinfo=timezone.utc)

        with patch.object(server, "count_due_srs_cards", return_value=0), \
             patch.object(server, "count_new_cards_introduced_today", return_value=8), \
             patch.object(server, "count_due_cards_reviewed_today", return_value=0), \
             patch.object(server, "has_available_new_srs_cards", return_value=False), \
             patch.object(server, "describe_manual_selection_sleep", return_value={
                 "selected_total": 9,
                 "asleep_count": 9,
                 "next_due_at": "2026-08-27T22:08:04.990861+00:00",
                 "not_trainable_count": 0,
             }):
            availability = server._build_manual_selection_availability(
                user_id=117649764,
                source_lang="ru",
                target_lang="de",
                card_ids=[4952, 329507, 329508, 329511, 329512, 329513, 329522, 329529, 329530],
                now_utc=now,
                cursor=None,
            )

        self.assertEqual(availability["selected_total"], 9)
        # Все девять спят — очередь сейчас не отдаст ни одной, и экран говорит это прямо.
        self.assertEqual(availability["available_now"], 0)
        self.assertEqual(availability["asleep_count"], 9)
        self.assertEqual(availability["next_available_at"], "2026-08-27T22:08:04.990861+00:00")
        self.assertEqual(availability["not_trainable_count"], 0)

    def test_manual_selection_availability_matches_what_the_queue_will_serve(self):
        """`available_now` берётся из расчёта очереди, а не из собственной арифметики.

        Если завести здесь своё правило, экран и выдача разойдутся на первой же правке
        алгоритма — это ровно тот дефект, который чинится.
        """
        now = datetime(2026, 8, 27, 10, 0, 0, tzinfo=timezone.utc)

        with patch.object(server, "count_due_srs_cards", return_value=3), \
             patch.object(server, "count_new_cards_introduced_today", return_value=1), \
             patch.object(server, "count_due_cards_reviewed_today", return_value=0), \
             patch.object(server, "has_available_new_srs_cards", return_value=True), \
             patch.object(server, "describe_manual_selection_sleep", return_value={
                 "selected_total": 5,
                 "asleep_count": 1,
                 "next_due_at": "2026-08-28T19:41:49+00:00",
                 "not_trainable_count": 0,
             }):
            availability = server._build_manual_selection_availability(
                user_id=55,
                source_lang="ru",
                target_lang="de",
                card_ids=[11, 12, 13, 14, 15],
                now_utc=now,
                cursor=None,
            )

        # due_count = min(3, 5 - 0) = 3; new_remaining = 5 - 1 = 4 → очередь отдаст 7 показов.
        self.assertEqual(availability["available_now"], 7)
        self.assertEqual(availability["asleep_count"], 1)
        self.assertEqual(availability["next_available_at"], "2026-08-28T19:41:49+00:00")

    def test_manual_selection_availability_invents_nothing_for_empty_selection(self):
        """Пустая выборка — нули и None, а не выдуманная дата и не «—»."""
        availability = server._build_manual_selection_availability(
            user_id=55,
            source_lang="ru",
            target_lang="de",
            card_ids=[],
            now_utc=datetime(2026, 8, 27, 10, 0, 0, tzinfo=timezone.utc),
            cursor=None,
        )
        self.assertEqual(availability["selected_total"], 0)
        self.assertEqual(availability["available_now"], 0)
        self.assertEqual(availability["asleep_count"], 0)
        self.assertIsNone(availability["next_available_at"])


if __name__ == "__main__":
    unittest.main()
