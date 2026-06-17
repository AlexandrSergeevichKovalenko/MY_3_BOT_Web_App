import unittest

import bot_3


class QuizPollStatsTests(unittest.TestCase):
    def test_build_quiz_poll_stats_lines_formats_distribution(self):
        quiz_data = {
            "options": [
                "Zunächst möchten wir uns bei Ihnen für die Bestellung bedanken.",
                "Zunächst möchten wir uns bei Ihnen bedanken für die Bestellung.",
                "Zunächst möchten wir uns bei Ihnen für den Bestellung bedanken.",
                "Zunächst möchten wir uns bei Ihnen für die Bestellung bedanken.",
            ],
            "correct_option_id": 3,
        }
        poll_stats = {
            "total_answers": 10,
            "correct_answers": 7,
            "wrong_answers": 3,
            "freeform_answers": 2,
            "option_rows": [
                {"selected_option_index": 0, "votes": 1},
                {"selected_option_index": 1, "votes": 1},
                {"selected_option_index": 2, "votes": 1},
                {"selected_option_index": 3, "votes": 5},
            ],
        }

        lines = bot_3._build_quiz_poll_stats_lines(
            quiz_data=quiz_data,
            poll_stats=poll_stats,
            selected_index=3,
        )

        joined = "\n".join(lines)
        self.assertIn("📊 <b>Как отвечают все:</b>", joined)
        self.assertIn("1. Zunächst möchten wir uns bei Ihnen für die Bestellung bedanken. — 1 (10%)", joined)
        self.assertIn("✅ 🙋 4. Zunächst möchten wir uns bei Ihnen für die Bestellung bedanken. — 5 (50%)", joined)
        self.assertIn("✍️ Свой вариант — 2 (20%)", joined)
        self.assertIn("Верно: 7/10 (70%) · Неверно: 3/10 (30%)", joined)


if __name__ == "__main__":
    unittest.main()
