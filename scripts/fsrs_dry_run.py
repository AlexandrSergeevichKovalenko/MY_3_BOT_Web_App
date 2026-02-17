from datetime import datetime, timezone, timedelta

from backend.srs.fsrs_scheduler import schedule_review


def main() -> None:
    now = datetime.now(timezone.utc)
    state = None
    ratings = ["GOOD"] * 10
    for idx, rating in enumerate(ratings, start=1):
        scheduled, _ = schedule_review(current_state=state, rating=rating, reviewed_at=now)
        print(
            f"{idx:02d}. rating={rating:<5} status={scheduled.status:<10} "
            f"interval_days={scheduled.interval_days:<4} due_at={scheduled.due_at.isoformat()}"
        )
        state = {
            "status": scheduled.status,
            "due_at": scheduled.due_at,
            "last_review_at": scheduled.last_review_at,
            "interval_days": scheduled.interval_days,
            "reps": scheduled.reps,
            "lapses": scheduled.lapses,
            "stability": scheduled.stability,
            "difficulty": scheduled.difficulty,
        }
        now = scheduled.due_at + timedelta(seconds=1)


if __name__ == "__main__":
    main()

