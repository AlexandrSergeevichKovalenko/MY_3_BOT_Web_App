"""Synthetic load-test entry point (Phase 1 scaffold).

Generates synthetic Telegram Update payloads for the configured user mix and
rate WITHOUT sending any Telegram API traffic and WITHOUT spending external API
budget (run with SYNTHETIC_LOAD_MODE=1 so OpenAI/TTS/YouTube are faked).

Phase 1 = infrastructure only. By default this prints the generation PLAN and a
sample of synthetic updates; it does NOT dispatch them into the bot. A caller may
pass an async `dispatch` coroutine to actually feed updates in a later phase.

Usage (planning only):
    SYNTHETIC_LOAD_MODE=1 python -m scripts.synthetic_load_runner --users 100 --rate 5 --duration 60
"""

import argparse
import itertools
import json
import time
from typing import Any, Callable, Iterator, Optional

# Reserved synthetic user-ID namespace. Generated synthetic Telegram user IDs
# start here so they can NEVER collide with a real Telegram user ID. 900 billion
# sits far above the largest plausible real Telegram ID and also above the
# production-side synthetic floor (SYNTHETIC_TELEGRAM_USER_ID_MIN, default
# 9_100_000_001 ≈ 9.1 billion), so any synthetic user is also recognised as
# synthetic by the backend. Do NOT lower this value.
SYNTHETIC_USER_ID_BASE = 900_000_000_000


def is_synthetic_user_id(user_id: int) -> bool:
    """True iff `user_id` is inside the reserved synthetic namespace."""
    try:
        return int(user_id) >= SYNTHETIC_USER_ID_BASE
    except (TypeError, ValueError):
        return False


# Synthetic user mix (matches the load-testing plan: A translator, B reviewer,
# C shortcut, D callback-heavy). Weights sum to 1.0.
USER_MIX = {
    "A_translator": {"weight": 0.40, "updates": ["message", "callback", "callback"]},
    "B_reviewer": {"weight": 0.25, "updates": ["callback", "callback", "message"]},
    "C_shortcut": {"weight": 0.20, "updates": ["forwarded", "message"]},
    "D_callback": {"weight": 0.15, "updates": ["callback", "callback", "callback"]},
}


def _assign_types(user_count: int) -> dict[str, int]:
    counts = {name: int(user_count * cfg["weight"]) for name, cfg in USER_MIX.items()}
    # Give any rounding remainder to the largest cohort.
    remainder = user_count - sum(counts.values())
    if remainder:
        counts["A_translator"] += remainder
    return counts


def _make_synthetic_update(user_id: int, update_type: str, seq: int) -> dict[str, Any]:
    """Build a lightweight synthetic Update payload (dict). No Telegram objects,
    no network. A later phase can convert these into telegram.Update via de_json."""
    chat = {"id": user_id, "type": "private"}
    user = {"id": user_id, "is_bot": False, "first_name": f"synthetic_{user_id}"}
    base = {"update_id": user_id * 1_000_000 + seq, "_synthetic_type": update_type}
    if update_type == "callback":
        return {**base, "callback_query": {
            "id": f"cb-{user_id}-{seq}", "from": user, "data": "noop:synthetic",
            "message": {"message_id": seq, "chat": chat}}}
    if update_type == "forwarded":
        return {**base, "message": {
            "message_id": seq, "from": user, "chat": chat,
            "text": "Synthetic forwarded text", "forward_origin": {"type": "user"}}}
    return {**base, "message": {
        "message_id": seq, "from": user, "chat": chat, "text": f"synthetic message {seq}"}}


def generate_updates(user_count: int, rate_per_sec: float, duration_sec: float) -> Iterator[dict[str, Any]]:
    """Yield synthetic update payloads pacing toward the requested global rate.

    This is a generator; it performs no I/O and no Telegram calls."""
    type_counts = _assign_types(user_count)
    user_ids: list[tuple[int, str]] = []
    next_id = SYNTHETIC_USER_ID_BASE  # reserved namespace: never collides with real IDs
    for cohort, n in type_counts.items():
        for _ in range(n):
            user_ids.append((next_id, cohort))
            next_id += 1
    if not user_ids:
        return
    total_updates = max(1, int(rate_per_sec * duration_sec))
    cohort_cycle = itertools.cycle(user_ids)
    for seq in range(total_updates):
        user_id, cohort = next(cohort_cycle)
        update_kind = USER_MIX[cohort]["updates"][seq % len(USER_MIX[cohort]["updates"])]
        yield _make_synthetic_update(user_id, update_kind, seq)


def run_plan(
    user_count: int,
    rate_per_sec: float,
    duration_sec: float,
    *,
    dispatch: Optional[Callable[[dict[str, Any]], Any]] = None,
    sample: int = 3,
) -> dict[str, Any]:
    """Return the generation plan. If `dispatch` is provided, feed updates to it
    (still no Telegram traffic — dispatch is the caller's concern). Default
    dispatch=None means planning only."""
    type_counts = _assign_types(user_count)
    plan = {
        "user_count": user_count,
        "rate_per_sec": rate_per_sec,
        "duration_sec": duration_sec,
        "expected_total_updates": int(rate_per_sec * duration_sec),
        "cohorts": type_counts,
        "dispatch": "live" if dispatch else "plan-only",
    }
    samples = []
    dispatched = 0
    for i, update in enumerate(generate_updates(user_count, rate_per_sec, duration_sec)):
        if i < sample:
            samples.append(update)
        if dispatch is not None:
            dispatch(update)
            dispatched += 1
    plan["sample_updates"] = samples
    plan["dispatched"] = dispatched
    return plan


def main() -> int:
    parser = argparse.ArgumentParser(description="Synthetic Telegram load-test scaffold (Phase 1)")
    parser.add_argument("--users", type=int, default=100)
    parser.add_argument("--rate", type=float, default=5.0, help="global updates/sec")
    parser.add_argument("--duration", type=float, default=60.0, help="seconds")
    parser.add_argument("--sample", type=int, default=3)
    args = parser.parse_args()

    started = time.time()
    plan = run_plan(args.users, args.rate, args.duration, dispatch=None, sample=args.sample)
    plan["planned_in_ms"] = round((time.time() - started) * 1000, 2)
    print(json.dumps(plan, indent=2, ensure_ascii=False))
    print("\nPhase 1: plan-only. No Telegram traffic, no external API calls, no load dispatched.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
