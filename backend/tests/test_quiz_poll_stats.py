"""Crowd stats under a quiz result.

The builder deliberately does NOT dump per-option vote counts any more — with 1–2
respondents that reads as noise. It states the success rate, and (only when the user
was wrong) how many others made the same mistake. These tests pin that contract.
"""

import unittest

import bot_3


QUIZ = {
    "options": [
        "Zunächst möchten wir uns bei Ihnen für die Bestellung bedanken.",
        "Zunächst möchten wir uns bei Ihnen bedanken für die Bestellung.",
        "Zunächst möchten wir uns bei Ihnen für den Bestellung bedanken.",
        "Zunächst möchten wir uns bei Ihnen für die Bestellung bedanken.",
    ],
    "correct_option_id": 3,
}


def _stats(**overrides):
    base = {
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
    base.update(overrides)
    return base


class QuizPollStatsTests(unittest.TestCase):
    def _build(self, *, stats=None, selected_index=3):
        return "\n".join(
            bot_3._build_quiz_poll_stats_lines(
                quiz_data=QUIZ,
                poll_stats=stats if stats is not None else _stats(),
                selected_index=selected_index,
            )
        )

    def test_shows_success_rate(self):
        self.assertIn("📊 <b>Правильно ответили:</b> 7 из 10 (70%)", self._build())

    def test_does_not_dump_per_option_votes(self):
        joined = self._build()
        for option in QUIZ["options"]:
            self.assertNotIn(f"{option} —", joined)

    def test_correct_answer_gets_no_mistake_line(self):
        self.assertNotIn("🙋", self._build(selected_index=3))

    def test_wrong_answer_reports_how_many_share_the_mistake(self):
        stats = _stats(option_rows=[
            {"selected_option_index": 1, "votes": 4},
            {"selected_option_index": 3, "votes": 6},
        ])
        joined = self._build(stats=stats, selected_index=1)
        self.assertIn("Так же, как ты, ошиблись ещё 3 из 10 (40%)", joined)

    def test_lonely_mistake_is_named_as_such(self):
        stats = _stats(option_rows=[
            {"selected_option_index": 1, "votes": 1},
            {"selected_option_index": 3, "votes": 9},
        ])
        joined = self._build(stats=stats, selected_index=1)
        self.assertIn("Так ошибся только ты", joined)

    def test_first_respondent_gets_no_percentages(self):
        stats = _stats(total_answers=1, correct_answers=1, wrong_answers=0, option_rows=[])
        joined = self._build(stats=stats, selected_index=3)
        self.assertIn("первым", joined)
        self.assertNotIn("%", joined)

    def test_no_answers_yet_produces_nothing(self):
        self.assertEqual(self._build(stats=_stats(total_answers=0)), "")


if __name__ == "__main__":
    unittest.main()
