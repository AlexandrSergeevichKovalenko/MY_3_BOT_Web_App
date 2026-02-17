from datetime import datetime, timezone
import unittest

from backend.srs.fsrs_scheduler import schedule_review


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


if __name__ == "__main__":
    unittest.main()

