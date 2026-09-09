"""Global quiz leaderboard over interactive answers (content-keyed, so it aggregates ALL
bot users who answered the same task). Shared by the bot (champion poster/card) and the
web tier (Mini-App leaderboard endpoint).

Rows come from three sources (see get_leaderboard_rows_since): TIMED answers from
bt_3_challenge_results (the Mini-App in-place path, with time_ms); UNTIMED answers from
each type's own table (answered in a Telegram GROUP via inline buttons — time_ms None); and
Artikel/Adjektiv SPRINT sets, each one task carrying a fractional `score` in {0,0.5,1}.
Untimed / sprint rows earn points + activity but never a speed bonus, a gold, or the
"fastest" nomination — we don't know how fast they were.

Scoring (владелец 09.09.2026, стратегия docs/tasks/champion_ranking_strategy.md):
каждый тип задания имеет ЦЕНУ (TASK_PRICE) — как у Duolingo/Memrise/Kahoot, где разные
задания приводятся к одной шкале ценой, а не воротами. Верный ответ даёт цену задания +
бонус за место по скорости (🥇+5/🥈+3/🥉+1, только у замеренных по времени). Набор спринта
даёт цену × {0, 0.5, 1} по проценту и считается решённым от 0.5.

┌─ ЗДЕСЬ БЫЛИ ВОРОТА «ПОЛОВИНА ЗАДАНИЙ» (min_for_prize). УБРАНЫ 09.09.2026. ────────────┐
│ Призовое место давалось только тому, кто ответил хотя бы на половину заданий периода;  │
│ остальные сортировались НИЖЕ независимо от очков. 08.09 это дало «Olga — 1 место,      │
│ 0 очков, 0/7 верно» над Oleg 45 и Aleksandr 35 (они ответили на 3 из 13). Ни у одного │
│ из лидеров (Duolingo, Kahoot, Memrise, Busuu) таких ворот нет: место — только очки,    │
│ а «три быстрых против 25 медленных» решает цена задания. Не возвращать.                │
└────────────────────────────────────────────────────────────────────────────────────────┘

Место: только за очки > 0; ноль — места нет (rank=None), на пьедестал и в номинации не
попадает (то же правило, что в недельном рейтинге 06.09). Равные очки — одно место
(1, 2, 2, 4)."""
import logging

# Цена задания по типу — ключ = первая часть challenge_key ('cw:123' → 'cw').
# ИСТОЧНИК (замер 09.09.2026, живая база, 60 дней, медиана времени ВЕРНОГО решения в
# bt_3_challenge_results, 3–6 человек на тип — замер тонкий, перемерить запросом ниже):
#   rb 8.7 с · ag 10.7 с · mc 12.6 с · nd 19.2 с · au 21.4 с · cw 60.5 с; qf, ls — ответов
#   для замера нет (ls: 3). Решение владельца 09.09.2026: три ступени 10 / 15 / 30;
#   qf и ls — как au; набор спринта (ast/ad/wf — десятки слов за раз) — как кроссворд.
#   SELECT split_part(challenge_key,':',1), percentile_cont(0.5) WITHIN GROUP (ORDER BY time_ms)
#   FROM bt_3_challenge_results WHERE is_correct AND time_ms>0 AND created_at>NOW()-interval '60 days'
#   GROUP BY 1;
TASK_PRICE = {
    "rb": 10, "ag": 10, "mc": 10,
    "au": 15, "nd": 15, "qf": 15, "ls": 15,
    "cw": 30,
    "ast": 30, "ad": 30, "wf": 30,
}
PLACE_BONUS = {1: 5, 2: 3, 3: 1}


def task_kind(challenge_key: str) -> str:
    return str(challenge_key or "").split(":", 1)[0].strip().lower()


def assign_ranks(leaders: list) -> list:
    """Место — только за очки > 0, равные очки делят место (1, 2, 2, 4). Список уже
    отсортирован по очкам; ноль получает rank=None и стоит внизу."""
    place = 0
    prev_points = None
    for i, l in enumerate(leaders):
        pts = int(l["points"])
        if pts <= 0:
            l["rank"] = None
            continue
        if pts != prev_points:
            place = i + 1
            prev_points = pts
        l["rank"] = place
    return leaders


def compute_quiz_leaderboard(rows: list) -> dict:
    by_key: dict[str, list] = {}
    unpriced = 0
    unpriced_kinds: set[str] = set()
    for r in rows:
        kind = task_kind(r["challenge_key"])
        if kind not in TASK_PRICE:
            # Тип без цены — не «10 по умолчанию», а ошибка каталога: строка не считается,
            # число уходит в результат, тест держит TASK_PRICE в согласии с типами.
            unpriced += 1
            unpriced_kinds.add(kind)
            continue
        by_key.setdefault(r["challenge_key"], []).append(r)
    if unpriced:
        logging.error("leaderboard: %s ответов типов без цены пропущены: %s", unpriced, sorted(unpriced_kinds))
    stats: dict[int, dict] = {}

    def st(uid: int, name: str) -> dict:
        s = stats.setdefault(uid, {"name": name or "Student", "points": 0, "answered": 0,
                                   "correct": 0, "golds": 0, "ctime_sum": 0, "ctime_n": 0})
        if name:
            s["name"] = name
        return s

    for key, rs in by_key.items():
        price = TASK_PRICE[task_kind(key)]
        # Speed placement (bonus + gold) ranks only TIMED correct answers. Untimed group
        # answers (time_ms is None) get the base price but no placement → pl = 99.
        timed_correct = sorted(
            [r for r in rs if r["is_correct"] and r.get("time_ms") is not None],
            key=lambda x: int(x["time_ms"]))
        place = {int(c["user_id"]): i + 1 for i, c in enumerate(timed_correct)}
        for r in rs:
            s = st(int(r["user_id"]), str(r["name"] or ""))
            s["answered"] += 1
            # Sprint/Battle sets carry a fractional `score` in {0, 0.5, 1}: ONE task worth
            # price × score, counted solved at >=0.5. Untimed → never a gold/speed bonus.
            score = r.get("score")
            if score is not None:
                if float(score) >= 0.5:
                    s["correct"] += 1
                s["points"] += round(price * float(score))
                continue
            if r["is_correct"]:
                s["correct"] += 1
                timed = r.get("time_ms") is not None
                pl = place.get(int(r["user_id"]), 99) if timed else 99
                s["points"] += price + PLACE_BONUS.get(pl, 0)
                if pl == 1:
                    s["golds"] += 1
                if timed:  # only real timings feed the "fastest" nomination
                    s["ctime_sum"] += int(r["time_ms"] or 0)
                    s["ctime_n"] += 1

    total_tasks = len(by_key)
    leaders = [{"user_id": uid, **s} for uid, s in stats.items()]
    # Очки → верные → суммарное время (быстрее выше) → user_id (устойчивость).
    leaders.sort(key=lambda l: (-l["points"], -l["correct"], l["ctime_sum"], l["user_id"]))
    assign_ranks(leaders)
    for l in leaders:
        l["prize_eligible"] = l["rank"] is not None   # прежнее имя поля для мини-аппа

    ranked = [l for l in leaders if l["rank"] is not None]
    fast_pool = [l for l in ranked if l["ctime_n"] >= 1]
    acc_pool = [l for l in ranked if l["correct"] >= 1]
    return {
        "leaders": leaders,
        "total_players": len(leaders),
        "total_tasks": total_tasks,
        "min_for_prize": 0,   # ворот больше нет; поле оставлено, чтобы старый клиент не упал
        "unpriced": unpriced,
        "fastest": min(fast_pool, key=lambda l: l["ctime_sum"] / l["ctime_n"]) if fast_pool else None,
        "accurate": max(acc_pool, key=lambda l: (l["correct"] / l["answered"], l["answered"])) if acc_pool else None,
        "active": max(ranked, key=lambda l: l["answered"]) if ranked else None,
    }


def get_leaderboard_rows_since(since_hours: int) -> list:
    """All leaderboard-eligible interactive answers in the window: TIMED Mini-App answers
    (bt_3_challenge_results) + UNTIMED Telegram-group answers (per-type tables), deduped so
    a Mini-App answer is never double-counted. This is the single source for both the bot
    champion posters and the Mini-App leaderboard, so group play finally counts."""
    from backend.database import (
        get_challenge_results_since, get_group_untimed_answers_since,
        get_sprint_leaderboard_rows_since,
    )
    rows = list(get_challenge_results_since(int(since_hours)) or [])
    try:
        rows += get_group_untimed_answers_since(int(since_hours)) or []
    except Exception:
        import logging
        logging.warning("leaderboard: group untimed fetch failed", exc_info=True)
    try:
        # Artikel/Adjektiv sprint sets: one task each, fractional {0,0.5,1} score.
        rows += get_sprint_leaderboard_rows_since(int(since_hours)) or []
    except Exception:
        import logging
        logging.warning("leaderboard: sprint rows fetch failed", exc_info=True)
    return rows


def get_quiz_leaderboard(days: int = 7) -> dict:
    rows = get_leaderboard_rows_since(int(days) * 24) or []
    try:  # exclude synthetic test/load users from the PUBLIC leaderboard
        from backend.database import SYNTHETIC_TELEGRAM_USER_ID_MIN
        rows = [r for r in rows if int(r.get("user_id") or 0) < SYNTHETIC_TELEGRAM_USER_ID_MIN]
    except Exception:
        pass
    lb = compute_quiz_leaderboard(rows)
    # Backfill display names for players whose points came only from nameless sources
    # (Telegram-GROUP inline answers land in per-type tables that store no user_name), so
    # they don't degrade to the "Student" placeholder on the podium. Nomination dicts
    # (fastest/accurate/active) reference the same leader objects, so mutating in place
    # fixes them too.
    leaders = lb.get("leaders") or []
    missing = [l["user_id"] for l in leaders if not str(l.get("name") or "").strip() or l.get("name") == "Student"]
    if missing:
        try:
            from backend.database import get_display_names_for_users
            names = get_display_names_for_users(missing)
        except Exception:
            import logging
            logging.warning("leaderboard: name backfill failed", exc_info=True)
            names = {}
        for l in leaders:
            resolved = names.get(int(l["user_id"]))
            if resolved:
                l["name"] = resolved
    return lb
