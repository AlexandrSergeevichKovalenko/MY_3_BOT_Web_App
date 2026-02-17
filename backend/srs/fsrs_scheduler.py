from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from fsrs import Card, Rating, Scheduler, State

MATURE_INTERVAL_DAYS = 21


@dataclass
class ScheduledResult:
    status: str
    due_at: datetime
    last_review_at: datetime
    interval_days: int
    reps: int
    lapses: int
    stability: float
    difficulty: float


def normalize_rating(raw_rating: int | str) -> tuple[Rating, int]:
    if isinstance(raw_rating, str):
        key = raw_rating.strip().upper()
        if key in {"1", "AGAIN"}:
            return Rating.Again, 1
        if key in {"2", "HARD"}:
            return Rating.Hard, 2
        if key in {"3", "GOOD"}:
            return Rating.Good, 3
        if key in {"4", "EASY"}:
            return Rating.Easy, 4
        raise ValueError(f"Unknown rating: {raw_rating}")

    value = int(raw_rating)
    if value == 1:
        return Rating.Again, 1
    if value == 2:
        return Rating.Hard, 2
    if value == 3:
        return Rating.Good, 3
    if value == 4:
        return Rating.Easy, 4
    raise ValueError(f"Rating must be 1..4, got {raw_rating}")


def _status_from_state(state: Any) -> str:
    name = getattr(state, "name", str(state)).lower()
    if "relearning" in name:
        return "relearning"
    if "learning" in name:
        return "learning"
    if "review" in name:
        return "review"
    return "new"


def _state_from_status(status: str) -> State:
    normalized = (status or "").strip().lower()
    if normalized == "review":
        return State.Review
    if normalized == "relearning":
        return State.Relearning
    if normalized == "learning":
        return State.Learning
    return State.New


def _build_fsrs_card(state: dict | None, now_utc: datetime) -> Card:
    card = Card()
    if not state:
        return card

    # We set fields explicitly so DB state survives process restarts.
    card.state = _state_from_status(state.get("status") or "new")
    due_at = state.get("due_at") or now_utc
    if due_at.tzinfo is None:
        due_at = due_at.replace(tzinfo=timezone.utc)
    card.due = due_at
    card.last_review = state.get("last_review_at")
    card.stability = float(state.get("stability") or 0.0) or None
    card.difficulty = float(state.get("difficulty") or 0.0) or None
    card.reps = int(state.get("reps") or 0)
    card.lapses = int(state.get("lapses") or 0)
    card.scheduled_days = int(state.get("interval_days") or 0)
    return card


def _interval_days(due_at: datetime, now_utc: datetime) -> int:
    delta = due_at - now_utc
    seconds = max(int(delta.total_seconds()), 0)
    return int(seconds // 86400)


def schedule_review(
    *,
    current_state: dict | None,
    rating: int | str,
    reviewed_at: datetime | None = None,
) -> tuple[ScheduledResult, int]:
    reviewed_at = reviewed_at or datetime.now(timezone.utc)
    if reviewed_at.tzinfo is None:
        reviewed_at = reviewed_at.replace(tzinfo=timezone.utc)

    rating_enum, rating_value = normalize_rating(rating)
    scheduler = Scheduler()
    card = _build_fsrs_card(current_state, reviewed_at)

    # FSRS official scheduler computes due/state/stability/difficulty.
    # For Again this naturally results in a short relearning step.
    reviewed_card, _review_log = scheduler.review_card(card, rating_enum)

    due_at = getattr(reviewed_card, "due", reviewed_at)
    if due_at.tzinfo is None:
        due_at = due_at.replace(tzinfo=timezone.utc)

    status = _status_from_state(getattr(reviewed_card, "state", State.New))
    interval_days = _interval_days(due_at, reviewed_at)

    result = ScheduledResult(
        status=status,
        due_at=due_at,
        last_review_at=reviewed_at,
        interval_days=interval_days,
        reps=int(getattr(reviewed_card, "reps", 0) or 0),
        lapses=int(getattr(reviewed_card, "lapses", 0) or 0),
        stability=float(getattr(reviewed_card, "stability", 0.0) or 0.0),
        difficulty=float(getattr(reviewed_card, "difficulty", 0.0) or 0.0),
    )
    return result, rating_value

