"""Global quiz leaderboard over interactive answers (content-keyed, so it aggregates ALL
bot users who answered the same task). Shared by the bot (champion poster/card) and the
web tier (Mini-App leaderboard endpoint).

Rows come from two sources (see get_leaderboard_rows_since): TIMED answers from
bt_3_challenge_results (the Mini-App in-place path, with time_ms), and UNTIMED answers
from each type's own table (answered in a Telegram GROUP via inline buttons — time_ms is
None). Untimed rows still earn base points and count toward activity/accuracy, but never a
speed bonus, a gold, or the "fastest" nomination — we don't know how fast they were.

Scoring: +10 per correct answer + place bonus on each task (🥇+5/🥈+3/🥉+1)."""


def compute_quiz_leaderboard(rows: list) -> dict:
    by_key: dict[str, list] = {}
    for r in rows:
        by_key.setdefault(r["challenge_key"], []).append(r)
    stats: dict[int, dict] = {}

    def st(uid: int, name: str) -> dict:
        s = stats.setdefault(uid, {"name": name or "Student", "points": 0, "answered": 0,
                                   "correct": 0, "golds": 0, "ctime_sum": 0, "ctime_n": 0})
        if name:
            s["name"] = name
        return s

    for _key, rs in by_key.items():
        # Speed placement (bonus + gold) ranks only TIMED correct answers. Untimed group
        # answers (time_ms is None) get the base +10 but no placement → pl = 99.
        timed_correct = sorted(
            [r for r in rs if r["is_correct"] and r.get("time_ms") is not None],
            key=lambda x: int(x["time_ms"]))
        place = {int(c["user_id"]): i + 1 for i, c in enumerate(timed_correct)}
        for r in rs:
            s = st(int(r["user_id"]), str(r["name"] or ""))
            s["answered"] += 1
            if r["is_correct"]:
                s["correct"] += 1
                timed = r.get("time_ms") is not None
                pl = place.get(int(r["user_id"]), 99) if timed else 99
                s["points"] += 10 + (5 if pl == 1 else 3 if pl == 2 else 1 if pl == 3 else 0)
                if pl == 1:
                    s["golds"] += 1
                if timed:  # only real timings feed the "fastest" nomination
                    s["ctime_sum"] += int(r["time_ms"] or 0)
                    s["ctime_n"] += 1

    total_tasks = len(by_key)
    # Prize / nomination eligibility: a player must have answered at least HALF of
    # the period's tasks to claim a prize place (podium) or win a nomination. This
    # stops someone who did 3 quick items from out-ranking someone who ground
    # through 25 (incl. slow crosswords that legitimately eat time). `min_for_prize`
    # = ceil(total_tasks / 2). Below the bar a player still appears in the list with
    # honest points, but is sorted under every eligible player and can't medal.
    min_for_prize = (total_tasks + 1) // 2 if total_tasks else 0

    leaders = []
    for uid, s in stats.items():
        eligible = s["answered"] >= min_for_prize and s["answered"] > 0
        leaders.append({"user_id": uid, "prize_eligible": eligible, **s})
    leaders.sort(key=lambda l: (0 if l["prize_eligible"] else 1,
                                -l["points"], -l["correct"], l["ctime_sum"]))

    fast_pool = [l for l in leaders if l["prize_eligible"] and l["ctime_n"] >= 1]
    acc_pool = [l for l in leaders if l["prize_eligible"] and l["answered"] >= 1]
    active_pool = [l for l in leaders if l["prize_eligible"]] or leaders
    return {
        "leaders": leaders,
        "total_players": len(leaders),
        "total_tasks": total_tasks,
        "min_for_prize": min_for_prize,
        "fastest": min(fast_pool, key=lambda l: l["ctime_sum"] / l["ctime_n"]) if fast_pool else None,
        "accurate": max(acc_pool, key=lambda l: (l["correct"] / l["answered"], l["answered"])) if acc_pool else None,
        "active": max(active_pool, key=lambda l: l["answered"]) if active_pool else None,
    }


def get_leaderboard_rows_since(since_hours: int) -> list:
    """All leaderboard-eligible interactive answers in the window: TIMED Mini-App answers
    (bt_3_challenge_results) + UNTIMED Telegram-group answers (per-type tables), deduped so
    a Mini-App answer is never double-counted. This is the single source for both the bot
    champion posters and the Mini-App leaderboard, so group play finally counts."""
    from backend.database import get_challenge_results_since, get_group_untimed_answers_since
    rows = list(get_challenge_results_since(int(since_hours)) or [])
    try:
        rows += get_group_untimed_answers_since(int(since_hours)) or []
    except Exception:
        import logging
        logging.warning("leaderboard: group untimed fetch failed", exc_info=True)
    return rows


def get_quiz_leaderboard(days: int = 7) -> dict:
    return compute_quiz_leaderboard(get_leaderboard_rows_since(int(days) * 24))
