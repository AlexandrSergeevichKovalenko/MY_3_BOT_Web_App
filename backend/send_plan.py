"""
Daily send-plan timeline (plan vs fact) — single source for the Mini-App table
and the bot's scheduled link message.

The plan is rebuilt from the schedule below; the fact is reconciled from the
dispatch tables each call, so nothing is persisted. The schedule here MUST match
the slot times the bot actually schedules in bot_3.py.

fact_kind: slotHM → dispatch slot_hour == h*100+m (article/sprint);
           slotH  → dispatch slot_hour == h (rebus/crossword/anagram/aufgabe);
           createdH → local hour of created_at == h (visual-riddle / image-quiz);
           listening → any row today (once/day).
"""
from __future__ import annotations

_AUFGABE_SLOTS = [
    (9, 30, "Cloze"), (10, 30, "Satzbau"), (11, 30, "Wortbildung"), (12, 0, "Synonym"),
    (13, 30, "Transform"), (15, 30, "Error"), (16, 0, "Antonym"), (17, 30, "Hörlücke"),
    (18, 30, "Satzbau"), (19, 30, "Pin"),
]

PLAN_GROUPS = [
    {"emoji": "🇩🇪", "title": "Артикль-квиз",
     "slots": [(9, 15, ""), (10, 15, ""), (13, 15, ""), (17, 15, ""), (18, 15, "")],
     "table": "bt_3_article_quiz_dispatches", "kind": "slotHM"},
    {"emoji": "✏️", "title": "Aufgabe", "slots": _AUFGABE_SLOTS,
     "table": "bt_3_aufgabe_dispatches", "kind": "slotH"},
    {"emoji": "🧩", "title": "Ребус",
     "slots": [(h, 30, "") for h in range(8, 21)],
     "table": "bt_3_rebus_dispatches", "kind": "slotH"},
    {"emoji": "🔤", "title": "Кроссворд", "slots": [(11, 45, ""), (17, 45, "")],
     "table": "bt_3_crossword_dispatches", "kind": "slotH"},
    {"emoji": "🔀", "title": "Анаграмма", "slots": [(12, 15, ""), (19, 15, "")],
     "table": "bt_3_anagram_dispatches", "kind": "slotH"},
    {"emoji": "🎧", "title": "Аудирование", "slots": [(18, 30, "")],
     "table": "bt_3_listening_dispatches", "kind": "listening"},
    {"emoji": "🏃", "title": "Спринт", "slots": [(14, 15, "Синонимы"), (20, 15, "Антонимы")],
     "table": "bt_3_sprint_dispatches", "kind": "slotHM"},
    {"emoji": "🖼", "title": "Визуал-ребус", "slots": [(7, 30, ""), (12, 30, ""), (15, 30, "")],
     "table": "bt_3_visual_riddle_dispatches", "kind": "createdH"},
    {"emoji": "🎨", "title": "Картинка-квиз", "slots": [(9, 0, ""), (12, 0, ""), (18, 0, "")],
     "table": "bt_3_image_quiz_dispatches", "kind": "createdH"},
    {"emoji": "⚡", "title": "Artikel Sprint", "slots": [(19, 0, "")],
     "table": "bt_3_article_sprint_dispatches", "kind": "slotH"},
]

_GRACE_MIN = 8


def _image_quiz_enabled() -> bool:
    # Mirror bot_3._image_quiz_enabled — image_quiz is retired by default
    # (replaced by the Pin-Bild Aufgabe). Don't show its slots in the plan while
    # it's off, otherwise they sit forever as a phantom "not sent".
    import os
    return (os.getenv("IMAGE_QUIZ_ENABLED") or "false").strip().lower() in ("1", "true", "yes")


def _artikel_sprint_enabled() -> bool:
    import os
    return (os.getenv("ARTIKEL_SPRINT_ENABLED") or "1").strip().lower() in ("1", "true", "yes", "on")


def _plan_groups_active() -> list:
    groups = []
    image_quiz_on = _image_quiz_enabled()
    artikel_on = _artikel_sprint_enabled()
    for g in PLAN_GROUPS:
        if g["table"] == "bt_3_image_quiz_dispatches" and not image_quiz_on:
            continue
        if g["table"] == "bt_3_article_sprint_dispatches" and not artikel_on:
            continue
        groups.append(g)
    return groups


def _slot_status(kind: str, h: int, m: int, dispatched, now_minute: int) -> str:
    if kind == "listening":
        sent = bool(dispatched)
    elif kind == "slotHM":
        sent = (h * 100 + m) in dispatched
    else:  # slotH / createdH
        sent = h in dispatched
    if sent:
        return "sent"
    return "failed" if (h * 60 + m + _GRACE_MIN) < now_minute else "planned"


def build_plan_timeline(plan_date, now_minute: int) -> dict:
    """Flat, time-sorted plan/fact timeline. now_minute = local hour*60+minute."""
    from backend.database import (
        get_dispatched_slot_hours_today, get_dispatched_created_hours_today,
        listening_dispatched_today,
    )
    fact_cache: dict[str, object] = {}

    def _dispatched(group) -> object:
        table, kind = group["table"], group["kind"]
        ck = f"{table}:{kind}"
        if ck in fact_cache:
            return fact_cache[ck]
        try:
            if kind == "listening":
                val = bool(listening_dispatched_today(plan_date))
            elif kind == "createdH":
                val = get_dispatched_created_hours_today(table, plan_date)
            else:
                val = get_dispatched_slot_hours_today(table, plan_date)
        except Exception:
            val = set()
        fact_cache[ck] = val
        return val

    rows = []
    done = failed = 0
    for g in _plan_groups_active():
        dispatched = _dispatched(g)
        for (h, m, sub) in g["slots"]:
            st = _slot_status(g["kind"], h, m, dispatched, now_minute)
            done += (st == "sent")
            failed += (st == "failed")
            rows.append({
                "minute": h * 60 + m,
                "time": f"{h:02d}:{m:02d}",
                "emoji": g["emoji"],
                "title": g["title"],
                "sub": sub,
                "status": st,
            })
    rows.sort(key=lambda r: (r["minute"], r["title"]))
    total = len(rows)
    return {
        "date": plan_date.isoformat() if hasattr(plan_date, "isoformat") else str(plan_date),
        "rows": rows,
        "totals": {"sent": done, "failed": failed, "planned": total - done - failed, "total": total},
    }
