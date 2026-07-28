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


class CounterOwnershipTests(unittest.TestCase):
    """reps/lapses are OUR columns, counted from OUR stored state.

    They used to be read back off the fsrs Card, which quietly stopped carrying them —
    884 reviewed cards in production sat at reps=0, lapses=0 for months. These tests fail
    the moment the counters start depending on the library again.
    """

    def _chain(self, ratings):
        now = datetime.now(timezone.utc)
        state, result = None, None
        for rating in ratings:
            result, _ = schedule_review(current_state=state, rating=rating, reviewed_at=now)
            state = {
                "status": result.status,
                "due_at": result.due_at,
                "last_review_at": result.last_review_at,
                "interval_days": result.interval_days,
                "reps": result.reps,
                "lapses": result.lapses,
                "stability": result.stability,
                "difficulty": result.difficulty,
                "step": result.step,
            }
        return result

    def test_reps_count_every_answer(self):
        self.assertEqual(self._chain(["GOOD"]).reps, 1)
        self.assertEqual(self._chain(["GOOD", "AGAIN", "HARD", "EASY"]).reps, 4)

    def test_lapses_count_only_again(self):
        self.assertEqual(self._chain(["GOOD", "HARD", "EASY"]).lapses, 0)
        self.assertEqual(self._chain(["GOOD", "AGAIN", "GOOD", "AGAIN"]).lapses, 2)

    def test_counters_continue_from_stored_state(self):
        """A card is loaded from the DB between sessions — counting must resume, not restart."""
        now = datetime.now(timezone.utc)
        result, _ = schedule_review(
            current_state={"status": "review", "due_at": now, "last_review_at": now,
                           "interval_days": 10, "reps": 17, "lapses": 4,
                           "stability": 12.0, "difficulty": 5.0, "step": 0},
            rating="AGAIN",
            reviewed_at=now,
        )
        self.assertEqual(result.reps, 18)
        self.assertEqual(result.lapses, 5)


class LearningStepsTests(unittest.TestCase):
    def _minutes(self, result):
        now = datetime.now(timezone.utc)
        return (result.due_at - now).total_seconds() / 60

    def test_again_comes_back_within_the_same_session(self):
        # Step 0 is ONE minute (product choice: a forgotten word must come back while the
        # user is still in the session, not ten minutes later when they have closed the app).
        now = datetime.now(timezone.utc)
        result, _ = schedule_review(current_state=None, rating="AGAIN", reviewed_at=now)
        minutes = (result.due_at - now).total_seconds() / 60
        self.assertGreater(minutes, 0)
        self.assertLessEqual(minutes, 5)

    def test_good_interval_is_one_day(self):
        now = datetime.now(timezone.utc)
        result, _ = schedule_review(current_state=None, rating="GOOD", reviewed_at=now)
        minutes = (result.due_at - now).total_seconds() / 60
        self.assertGreaterEqual(minutes, 1380)   # >=23h
        self.assertLessEqual(minutes, 1500)      # <=25h

    def test_learning_steps_config(self):
        # Two steps: 1 minute (same session) and 1 day. The first step was 10 minutes once;
        # it is a product decision, so this test pins the CURRENT setting rather than a
        # historical one — but still pins it, so nobody changes the rhythm by accident.
        self.assertEqual(len(_LEARNING_STEPS), 2)
        self.assertAlmostEqual(_LEARNING_STEPS[0].total_seconds(), 60, delta=1)
        self.assertAlmostEqual(_LEARNING_STEPS[1].total_seconds(), 86400, delta=1)


if __name__ == "__main__":
    unittest.main()

