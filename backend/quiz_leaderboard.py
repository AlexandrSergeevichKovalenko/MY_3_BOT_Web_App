"""Global quiz leaderboard over bt_3_challenge_results (content-keyed, so it
aggregates ALL bot users who answered the same task). Shared by the bot (champion
poster/card) and the web tier (Mini-App leaderboard endpoint).

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
        correct = sorted([r for r in rs if r["is_correct"]], key=lambda x: x["time_ms"])
        place = {c["user_id"]: i + 1 for i, c in enumerate(correct)}
        for r in rs:
            s = st(int(r["user_id"]), str(r["name"] or ""))
            s["answered"] += 1
            if r["is_correct"]:
                s["correct"] += 1
                pl = place.get(int(r["user_id"]), 99)
                s["points"] += 10 + (5 if pl == 1 else 3 if pl == 2 else 1 if pl == 3 else 0)
                if pl == 1:
                    s["golds"] += 1
                s["ctime_sum"] += int(r["time_ms"] or 0)
                s["ctime_n"] += 1

    leaders = [{"user_id": uid, **s} for uid, s in stats.items()]
    leaders.sort(key=lambda l: (-l["points"], -l["correct"], l["ctime_sum"]))
    fast_pool = [l for l in leaders if l["ctime_n"] >= 3]
    acc_pool = [l for l in leaders if l["answered"] >= 3]
    return {
        "leaders": leaders,
        "total_players": len(leaders),
        "total_tasks": len(by_key),
        "fastest": min(fast_pool, key=lambda l: l["ctime_sum"] / l["ctime_n"]) if fast_pool else None,
        "accurate": max(acc_pool, key=lambda l: (l["correct"] / l["answered"], l["answered"])) if acc_pool else None,
        "active": max(leaders, key=lambda l: l["answered"]) if leaders else None,
    }


def get_quiz_leaderboard(days: int = 7) -> dict:
    from backend.database import get_challenge_results_since
    rows = get_challenge_results_since(int(days) * 24)
    return compute_quiz_leaderboard(rows or [])
