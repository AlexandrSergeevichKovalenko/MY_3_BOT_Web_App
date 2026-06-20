"""Deterministic, stateless weighted daily rotation for scheduled interactive sends.

Этап 0 of the schedule/tiering feature. Instead of firing EVERY (kind, slot) every
day (the old ~28/day firehose), we keep a fixed daily SEND BUDGET and rotate which
slots actually fire. Nothing is ever removed from generation — a slot that is "off"
today simply doesn't deliver today; it comes back another day. This caps daily
volume (group + default-DM alike) and lets generation throttle naturally (fewer
sends → pools stay fuller → nightly top-ups generate less).

Design
------
  * Some kinds are "always-on" (the 1×/2× types we never thin): they fire every day.
  * The rest form a "rotating pool" (the many aufgabe formats + the article-quiz
    slots); each day we pick `budget - len(always_on)` of them.
  * Selection is Efraimidis–Spirakis weighted reservoir sampling, seeded
    deterministically from the calendar day, so it is:
      - STATELESS (no DB; same answer in every process for a given day),
      - FAIR over time (each entry's airtime ∝ its weight, no starvation),
      - WEIGHTED (boring/low-value kinds get a smaller weight → appear less often),
      - non-repeating (the seed changes daily, so no fixed weekly pattern).

The engine is pure: it takes a catalog + a day ordinal and returns the set of
active slot keys. The bot builds the catalog from its existing slot constants and
gates each scheduled job on membership in that set.
"""

from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass


def slot_key(kind: str, hour: int, minute: int) -> str:
    """Stable identity for one (kind, slot) entry — used as the rotation seed and
    as the membership token the scheduler checks."""
    return f"{str(kind).strip()}:{int(hour):02d}:{int(minute):02d}"


@dataclass(frozen=True)
class SlotEntry:
    kind: str
    hour: int
    minute: int
    weight: float = 1.0      # rotating entries only; higher → chosen more often
    always_on: bool = False  # always-on entries ignore the budget and weight

    @property
    def key(self) -> str:
        return slot_key(self.kind, self.hour, self.minute)


def _deterministic_uniform(day_ordinal: int, key: str) -> float:
    """A stable pseudo-random uniform in (0, 1] for (day, entry). SHA-256 keeps the
    distribution clean and makes the result identical across processes/restarts."""
    digest = hashlib.sha256(f"{int(day_ordinal)}:{key}".encode("utf-8")).digest()
    n = int.from_bytes(digest, "big")
    span = 1 << (len(digest) * 8)
    # Map to (0, 1] so the log below is always finite.
    return (n + 1) / (span + 1)


def compute_active_slot_keys(
    entries: list[SlotEntry],
    day_ordinal: int,
    budget: int,
) -> set[str]:
    """Return the set of slot keys that should FIRE on `day_ordinal`.

    Always-on entries are always included. From the remaining "rotating" entries we
    pick `budget - len(always_on)` via weighted sampling. If the budget is at/below
    the always-on count, only always-on entries fire.
    """
    always_on = [e for e in entries if e.always_on]
    rotating = [e for e in entries if not e.always_on]

    active: set[str] = {e.key for e in always_on}

    rotating_budget = max(0, int(budget) - len(always_on))
    if rotating_budget >= len(rotating):
        # Budget covers everything — nothing to thin.
        active.update(e.key for e in rotating)
        return active
    if rotating_budget <= 0:
        return active

    # Efraimidis–Spirakis: sample without replacement weighted by `weight`.
    # key_val = -ln(u)/w  → larger weight yields a smaller key_val on average, so
    # taking the smallest `rotating_budget` keys favours higher-weight entries while
    # still rotating fairly as the daily seed changes.
    def sort_key(e: SlotEntry) -> float:
        w = max(1e-9, float(e.weight))
        u = _deterministic_uniform(day_ordinal, e.key)
        return -math.log(u) / w

    chosen = sorted(rotating, key=sort_key)[:rotating_budget]
    active.update(e.key for e in chosen)
    return active


# --------------------------------------------------------------------------- #
# Manual demo: prints the per-kind airtime and daily totals over a window so we
# can eyeball that the rotation holds the budget and spreads fairly.
# Run: python -m backend.global_rotation
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    from collections import Counter
    import datetime as _dt

    # Mirrors the live catalog: 12 always-on entries + 16 rotating (5 article-quiz,
    # 11 aufgabe). article-quiz is weighted slightly down (owner finds it boring).
    ARTICLE_QUIZ = [(9, 15), (10, 15), (13, 15), (17, 15), (18, 15)]
    AUFGABE = [
        (9, 30), (10, 30), (11, 30), (12, 0), (13, 30), (14, 30),
        (15, 30), (16, 0), (17, 30), (18, 30), (19, 30),
    ]
    ALWAYS_ON = [
        ("rebus", 12, 30), ("crossword", 11, 45), ("crossword", 17, 45),
        ("anagram", 12, 15), ("anagram", 19, 15), ("sprint", 14, 15),
        ("sprint", 20, 15), ("artikel_sprint", 19, 0),
        ("adjektiv_sprint", 10, 0), ("adjektiv_sprint", 15, 30),
        ("artikel_learn", 8, 0), ("listening", 18, 30),
    ]

    entries: list[SlotEntry] = []
    entries += [SlotEntry(k, h, m, always_on=True) for (k, h, m) in ALWAYS_ON]
    entries += [SlotEntry("article_quiz", h, m, weight=0.8) for (h, m) in ARTICLE_QUIZ]
    entries += [SlotEntry("aufgabe", h, m, weight=1.0) for (h, m) in AUFGABE]

    BUDGET = 20
    DAYS = 28
    base = _dt.date(2026, 6, 20).toordinal()

    totals: list[int] = []
    kind_days: Counter = Counter()
    quiz_per_day: list[int] = []
    aufg_per_day: list[int] = []
    for d in range(DAYS):
        active = compute_active_slot_keys(entries, base + d, BUDGET)
        totals.append(len(active))
        kinds = [k.rsplit(":", 2)[0] for k in active]
        kind_days.update(set(kinds))
        quiz_per_day.append(sum(1 for k in active if k.startswith("article_quiz")))
        aufg_per_day.append(sum(1 for k in active if k.startswith("aufgabe")))

    print(f"catalog: {len(entries)} entries  budget={BUDGET}")
    print(f"daily total sends over {DAYS}d: min={min(totals)} max={max(totals)} "
          f"avg={sum(totals)/len(totals):.1f}  (target {BUDGET})")
    print(f"article_quiz/day: avg={sum(quiz_per_day)/DAYS:.2f} (of 5 slots)")
    print(f"aufgabe/day:      avg={sum(aufg_per_day)/DAYS:.2f} (of 11 slots)")
    print(f"days each kind appeared (of {DAYS}):")
    for kind in sorted(kind_days):
        print(f"    {kind:18s} {kind_days[kind]:2d}/{DAYS}")
