"""Daily "Лимиты и средние" admin report — per-tier («Полный доступ»/Free) picture of PERSONAL
daily cost vs the cost cap, so the owner sees the averages AND who is actually hitting
the limit (to react, not go blind). Mirrors provider_cost_truth.py: build_* text +
send_* DM with a run-guard against double-send.

"Personal" cost excludes DAILY_COST_CAP_EXCLUDED_ACTION_TYPES (shared-pool building) —
exactly the number the daily cap enforces — and excludes admins (they're cap-exempt).
"""
from __future__ import annotations

import logging
import os
import statistics
from datetime import date, datetime, timedelta, timezone
from typing import Any

import requests

JOB_KEY = "cap_health_daily"
_HTTP_TIMEOUT = 15


def _tz_name() -> str:
    return (os.getenv("CAP_HEALTH_REPORT_TZ")
            or os.getenv("TRIAL_POLICY_TZ")
            or "Europe/Vienna")


def _f(v: float) -> str:
    return f"{v:.2f}"


def _pct_of_cap(avg: float, cap: float | None) -> str:
    if not cap:
        return "—"
    return f"{round(100.0 * avg / cap)}% cap"


def _collect_personal_costs_eur(target_day: date, tz_name: str, *, days: int = 1) -> dict[int, float]:
    """{user_id: personal EUR cost} over the [target_day-(days-1) .. target_day] window,
    excluding shared-pool actions and NULL (system) rows — the cap-relevant number."""
    from backend.database import (
        get_db_connection_context,
        convert_cost_to_eur,
        DAILY_COST_CAP_EXCLUDED_ACTION_TYPES,
        DAILY_COST_CAP_EXCLUDED_PROVIDERS,
    )
    start = target_day - timedelta(days=max(1, days) - 1)
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT user_id, currency, COALESCE(SUM(cost_amount), 0) AS total
                FROM bt_3_billing_events
                WHERE user_id IS NOT NULL
                  AND (event_time AT TIME ZONE %s)::date BETWEEN %s AND %s
                  AND action_type <> ALL(%s)
                  AND provider <> ALL(%s)
                GROUP BY user_id, currency;
                """,
                (tz_name, start, target_day,
                 list(DAILY_COST_CAP_EXCLUDED_ACTION_TYPES),
                 list(DAILY_COST_CAP_EXCLUDED_PROVIDERS)),
            )
            rows = cursor.fetchall() or []
    out: dict[int, float] = {}
    for uid, currency, total in rows:
        try:
            eur = convert_cost_to_eur(float(total or 0.0), str(currency or "EUR").upper())
        except Exception:
            continue
        out[int(uid)] = out.get(int(uid), 0.0) + eur
    return out


def _tier_stats(costs: list[float], daily_cap: float | None, avgs_7d: list[float] | None = None,
                avg_cap: float | None = None, blocked_ids: list[int] | None = None,
                near_ids: list[int] | None = None) -> str:
    if not costs:
        return "   активных: 0"
    n = len(costs)
    mean = statistics.fmean(costs)
    median = statistics.median(costs)
    srt = sorted(costs)
    p90 = srt[min(n - 1, int(round(0.9 * (n - 1))))]
    mx = srt[-1]
    lines = [
        f"   активных: {n}",
        f"   среднее: {_f(mean)}€/день ({_pct_of_cap(mean, daily_cap)}) · медиана {_f(median)} · p90 {_f(p90)} · макс {_f(mx)}",
    ]
    if avgs_7d:
        lines.append(f"   среднее за 7д по тиру: {_f(statistics.fmean(avgs_7d))}€")
    blocked = blocked_ids or []
    near = near_ids or []
    tail = f"   ⚠️ упёрлись вчера: {len(blocked)}"
    if blocked:
        tail += " (id " + ", ".join(str(i) for i in blocked[:8]) + (", …" if len(blocked) > 8 else "") + ")"
    tail += f"   ·   близко (>80%): {len(near)}"
    lines.append(tail)
    return "\n".join(lines)


def build_cap_health_text(*, target_day: date | None = None, tz_name: str | None = None) -> str:
    from backend.database import (
        resolve_entitlement,
        get_admin_telegram_ids,
        get_billing_plan,
        _resolve_weekly_avg_cap_eur,
    )
    tz_name = tz_name or _tz_name()
    day = target_day or (datetime.now(timezone.utc).date() - timedelta(days=1))
    admins = {int(a) for a in (get_admin_telegram_ids() or [])}

    today = _collect_personal_costs_eur(day, tz_name, days=1)
    week = _collect_personal_costs_eur(day, tz_name, days=7)

    pro_plan = get_billing_plan("pro") or {}
    free_plan = get_billing_plan("free") or {}
    pro_daily = float(pro_plan.get("daily_cost_cap_eur")) if pro_plan.get("daily_cost_cap_eur") is not None else None
    free_daily = float(free_plan.get("daily_cost_cap_eur")) if free_plan.get("daily_cost_cap_eur") is not None else None
    # Pull the weekly-average cap from the SAME resolver the enforcement path uses, so the
    # report can never drift from the real cap (it used to hardcode 0.30 while enforcement
    # applied 0.12 → the report advertised a limit the system didn't actually use).
    pro_avg_cap = _resolve_weekly_avg_cap_eur({"effective_mode": "pro"})

    buckets: dict[str, dict[str, list]] = {
        "pro": {"today": [], "avg7": [], "blocked": [], "near": []},
        "free": {"today": [], "avg7": [], "blocked": [], "near": []},
    }
    # Union of users active in the 7-day window (so weekly-avg blocks show even with no spend yesterday).
    for uid in set(today) | set(week):
        if uid in admins:
            continue
        try:
            mode = str(resolve_entitlement(user_id=uid, tz=tz_name).get("effective_mode") or "free").lower()
        except Exception:
            mode = "free"
        tier = "pro" if mode == "pro" else "free"
        t_eur = float(today.get(uid, 0.0))
        avg7 = float(week.get(uid, 0.0)) / 7.0
        buckets[tier]["today"].append(t_eur)
        buckets[tier]["avg7"].append(avg7)
        daily_cap = pro_daily if tier == "pro" else free_daily
        avg_cap = pro_avg_cap if tier == "pro" else None
        hit = (daily_cap is not None and t_eur >= daily_cap) or (avg_cap is not None and avg7 >= avg_cap)
        near = (not hit) and (daily_cap is not None and t_eur >= 0.8 * daily_cap)
        if hit:
            buckets[tier]["blocked"].append(uid)
        elif near:
            buckets[tier]["near"].append(uid)

    total_blocked = len(buckets["pro"]["blocked"]) + len(buckets["free"]["blocked"])
    header = f"📊 Лимиты и средние — {day.strftime('%d.%m.%Y')} ({tz_name.split('/')[-1]})"
    parts = [
        header,
        "   (личные затраты юзеров, пул исключён; админы не в счёт)",
        "",
        f"🟣 Полный доступ   cap {_f(pro_daily) if pro_daily else '—'}/день · {_f(pro_avg_cap) if pro_avg_cap else '—'}/ср.7д",
        _tier_stats(buckets["pro"]["today"], pro_daily, buckets["pro"]["avg7"], pro_avg_cap,
                    buckets["pro"]["blocked"], buckets["pro"]["near"]),
        "",
        f"🟢 FREE  cap {_f(free_daily) if free_daily else '—'}/день",
        _tier_stats(buckets["free"]["today"], free_daily, None, None,
                    buckets["free"]["blocked"], buckets["free"]["near"]),
        "",
        ("✅ Итог: все под контролем" if total_blocked == 0
         else f"⚠️ Итог: {total_blocked} юзер(ов) упёрлись вчера — стоит глянуть"),
    ]
    return "\n".join(parts)


def _split_telegram_text(text: str, limit: int = 3900) -> list[str]:
    if len(text) <= limit:
        return [text]
    out, buf = [], ""
    for line in text.split("\n"):
        if len(buf) + len(line) + 1 > limit:
            out.append(buf)
            buf = ""
        buf += line + "\n"
    if buf:
        out.append(buf)
    return out


def send_cap_health_report(*, target_day: date | None = None, force: bool = False) -> dict[str, Any]:
    """Build and DM the cap-health report to all admins, run-guarded against double-send."""
    from backend.database import (
        get_admin_telegram_ids,
        claim_scheduler_run_guard,
        finish_scheduler_run_guard,
    )
    tz_name = _tz_name()
    day = target_day or (datetime.now(timezone.utc).date() - timedelta(days=1))
    run_period = day.isoformat()
    if not force and not claim_scheduler_run_guard(
        job_key=JOB_KEY, run_period=run_period, target_scope="global", metadata={"tz": tz_name},
    ):
        return {"ok": True, "skipped": True, "reason": "already_claimed", "day": run_period}
    try:
        token = os.getenv("TELEGRAM_Deutsch_BOT_TOKEN")
        admin_ids = sorted(int(a) for a in (get_admin_telegram_ids() or []) if int(a) > 0)
        if not token or not admin_ids:
            if not force:
                finish_scheduler_run_guard(job_key=JOB_KEY, run_period=run_period,
                                           target_scope="global", status="failed",
                                           metadata={"error": "no_token_or_admins"})
            return {"ok": False, "sent": 0, "error": "no_token_or_admins"}
        text = build_cap_health_text(target_day=day, tz_name=tz_name)
        sent = 0
        for uid in admin_ids:
            for part in _split_telegram_text(text):
                resp = requests.post(
                    f"https://api.telegram.org/bot{token}/sendMessage",
                    json={"chat_id": uid, "text": part, "disable_web_page_preview": True},
                    timeout=_HTTP_TIMEOUT,
                )
                if resp.status_code >= 400:
                    logging.warning("cap health DM failed uid=%s: %s", uid, resp.text[:200])
            sent += 1
        if not force:
            finish_scheduler_run_guard(job_key=JOB_KEY, run_period=run_period,
                                       target_scope="global", status="completed", metadata={"sent": sent})
        return {"ok": True, "sent": sent, "day": run_period}
    except Exception as exc:
        if not force:
            finish_scheduler_run_guard(job_key=JOB_KEY, run_period=run_period,
                                       target_scope="global", status="failed", metadata={"error": str(exc)})
        logging.exception("cap health report failed")
        return {"ok": False, "error": str(exc)}
