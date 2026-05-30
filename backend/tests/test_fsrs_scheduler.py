from datetime import datetime, timezone
import unittest

from backend.srs.fsrs_scheduler import schedule_review, _LEARNING_STEPS


class FsrsSchedulerTests(unittest.TestCase):
    def test_new_card_good_sets_future_due_and_reps(self):
        now = datetime.now(timezone.utc)
        scheduled, _ = schedule_review(current_state=None, rating="GOOD", reviewed_at=now)
        self.assertGreaterEqual(scheduled.due_at, now)
        self.assertGreaterEqual(scheduled.reps, 1)

    def test_again_increments_lapses(self):
        now = datetime.now(timezone.utc)
        first, _ = schedule_review(current_state=None, rating="GOOD", reviewed_at=now)
        second, _ = schedule_review(
            current_state={
                "status": first.status,
                "due_at": first.due_at,
                "last_review_at": first.last_review_at,
                "interval_days": first.interval_days,
                "reps": first.reps,
                "lapses": first.lapses,
                "stability": first.stability,
                "difficulty": first.difficulty,
            },
            rating="AGAIN",
            reviewed_at=now,
        )
        self.assertGreaterEqual(second.lapses, first.lapses + 1)

    def test_good_chain_increases_interval(self):
        now = datetime.now(timezone.utc)
        first, _ = schedule_review(current_state=None, rating="GOOD", reviewed_at=now)
        second, _ = schedule_review(
            current_state={
                "status": first.status,
                "due_at": first.due_at,
                "last_review_at": first.last_review_at,
                "interval_days": first.interval_days,
                "reps": first.reps,
                "lapses": first.lapses,
                "stability": first.stability,
                "difficulty": first.difficulty,
            },
            rating="GOOD",
            reviewed_at=now,
        )
        self.assertGreaterEqual(second.interval_days, first.interval_days)


class LearningStepsTests(unittest.TestCase):
    def _minutes(self, result):
        now = datetime.now(timezone.utc)
        return (result.due_at - now).total_seconds() / 60

    def test_again_interval_at_least_10_minutes(self):
        now = datetime.now(timezone.utc)
        result, _ = schedule_review(current_state=None, rating="AGAIN", reviewed_at=now)
        minutes = (result.due_at - now).total_seconds() / 60
        self.assertGreaterEqual(minutes, 9)
        self.assertLessEqual(minutes, 12)

    def test_good_interval_is_one_day(self):
        now = datetime.now(timezone.utc)
        result, _ = schedule_review(current_state=None, rating="GOOD", reviewed_at=now)
        minutes = (result.due_at - now).total_seconds() / 60
        self.assertGreaterEqual(minutes, 1380)   # >=23h
        self.assertLessEqual(minutes, 1500)      # <=25h

    def test_learning_steps_config(self):
        # Verify the two configured steps: 10 min and 1 day
        self.assertEqual(len(_LEARNING_STEPS), 2)
        self.assertAlmostEqual(_LEARNING_STEPS[0].total_seconds(), 600, delta=1)
        self.assertAlmostEqual(_LEARNING_STEPS[1].total_seconds(), 86400, delta=1)


if __name__ == "__main__":
    unittest.main()

