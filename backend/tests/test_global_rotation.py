"""Tests for the deterministic daily send-rotation engine (Этап 0).

Mirrors the live catalog assembled in bot_3.py: 12 always-on entries + 16 rotating
(5 article-quiz @0.8, 11 aufgabe @1.0), daily budget 20.
"""

import datetime as dt

from backend.global_rotation import (
    SlotEntry,
    compute_active_slot_keys,
    slot_key,
    rank_slots,
)

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
BUDGET = 20


def _catalog() -> list[SlotEntry]:
    entries: list[SlotEntry] = [SlotEntry(k, h, m, always_on=True) for (k, h, m) in ALWAYS_ON]
    entries += [SlotEntry("article_quiz", h, m, weight=0.8) for (h, m) in ARTICLE_QUIZ]
    entries += [SlotEntry("aufgabe", h, m, weight=1.0) for (h, m) in AUFGABE]
    return entries


def _ordinals(n: int) -> range:
    base = dt.date(2026, 6, 20).toordinal()
    return range(base, base + n)


def test_catalog_shape():
    entries = _catalog()
    assert len(entries) == 28
    assert sum(1 for e in entries if e.always_on) == 12


def test_daily_total_equals_budget_exactly():
    entries = _catalog()
    for day in _ordinals(120):
        active = compute_active_slot_keys(entries, day, BUDGET)
        assert len(active) == BUDGET, f"day {day}: {len(active)} != {BUDGET}"


def test_always_on_fires_every_day():
    entries = _catalog()
    always_keys = {slot_key(k, h, m) for (k, h, m) in ALWAYS_ON}
    for day in _ordinals(120):
        active = compute_active_slot_keys(entries, day, BUDGET)
        assert always_keys <= active, f"day {day}: an always-on slot was thinned"


def test_deterministic():
    entries = _catalog()
    day = dt.date(2026, 6, 20).toordinal()
    assert compute_active_slot_keys(entries, day, BUDGET) == compute_active_slot_keys(entries, day, BUDGET)


def test_rotation_actually_rotates():
    """Over a window, a rotating slot is sometimes on and sometimes off (not stuck)."""
    entries = _catalog()
    sample = slot_key("article_quiz", 9, 15)
    states = {sample in compute_active_slot_keys(entries, day, BUDGET) for day in _ordinals(40)}
    assert states == {True, False}, "rotating slot never changed state over 40 days"


def test_every_rotating_entry_gets_airtime():
    entries = _catalog()
    seen: set[str] = set()
    for day in _ordinals(60):
        seen |= compute_active_slot_keys(entries, day, BUDGET)
    for (h, m) in ARTICLE_QUIZ:
        assert slot_key("article_quiz", h, m) in seen
    for (h, m) in AUFGABE:
        assert slot_key("aufgabe", h, m) in seen


def test_weighting_makes_aufgabe_more_frequent_than_quiz():
    entries = _catalog()
    quiz = aufg = 0
    days = 200
    for day in _ordinals(days):
        active = compute_active_slot_keys(entries, day, BUDGET)
        quiz += sum(1 for k in active if k.startswith("article_quiz"))
        aufg += sum(1 for k in active if k.startswith("aufgabe"))
    # 8 rotating picks/day → ~5.6 aufgabe + ~2.4 quiz on average.
    assert aufg / days > quiz / days
    assert 1.5 < quiz / days < 3.5
    assert 4.5 < aufg / days < 6.5


def test_budget_above_total_keeps_everything():
    entries = _catalog()
    active = compute_active_slot_keys(entries, _ordinals(1).start, 999)
    assert len(active) == len(entries)


def test_budget_at_or_below_always_on_keeps_only_always_on():
    entries = _catalog()
    active = compute_active_slot_keys(entries, _ordinals(1).start, 5)
    assert len(active) == 12
    assert all(":" in k for k in active)


# ── Tier ranking (Этап 1): nested per-tier subsets ──────────────────────────
FREE_N, OBYCHNO_N, FULL_N = 4, 12, 20


def _active_entries(entries, day):
    active = compute_active_slot_keys(entries, day, FULL_N)
    return [e for e in entries if e.key in active]


def test_rank_is_deterministic_and_total():
    entries = _catalog()
    for day in _ordinals(20):
        act = _active_entries(entries, day)
        r1 = rank_slots(act, day)
        r2 = rank_slots(act, day)
        assert [e.key for e in r1] == [e.key for e in r2]
        assert len(r1) == len(act) == FULL_N  # ranks every active slot once


def test_tiers_are_nested_free_subset_obychno_subset_full():
    entries = _catalog()
    for day in _ordinals(60):
        ranked = [e.key for e in rank_slots(_active_entries(entries, day), day)]
        free = set(ranked[:FREE_N])
        obychno = set(ranked[:OBYCHNO_N])
        full = set(ranked[:FULL_N])
        assert free <= obychno <= full
        assert len(free) == FREE_N and len(obychno) == OBYCHNO_N


def test_free_tier_rotates_types_over_time():
    """Free's 4/day should cover many different kinds across a window (breadth)."""
    entries = _catalog()
    kinds_seen = set()
    for day in _ordinals(21):
        ranked = rank_slots(_active_entries(entries, day), day)
        for e in ranked[:FREE_N]:
            kinds_seen.add(e.kind)
    # Over 3 weeks Free should have sampled well beyond a couple of types.
    assert len(kinds_seen) >= 6
