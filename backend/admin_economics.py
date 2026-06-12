from __future__ import annotations

import html
import json
import logging
import os
from datetime import date, datetime, timezone, timedelta
from statistics import median
from typing import Any
from zoneinfo import ZoneInfo

import requests
from psycopg2.extras import Json

from backend.database import (
    FREE_FEATURE_LIMITS,
    apply_admin_limit_change,
    cancel_admin_limit_change,
    claim_scheduler_run_guard,
    create_admin_limit_change_preview,
    ensure_admin_economics_schema,
    finish_scheduler_run_guard,
    get_admin_telegram_ids,
    get_db_connection_context,
    get_dict_dedup_report,
    list_admin_configurable_limits,
    resolve_entitlement,
)


ADMIN_ECONOMICS_TZ = "Europe/Vienna"
ADMIN_ECONOMICS_JOB_KEY = "admin_economics_daily_report"

_LIMIT_BUTTON_DELTAS: dict[str, tuple[int, ...]] = {
    "dictionary_lookup_daily": (-10, -5, 5, 10),
    "shortcut_forwarded_message_daily": (-5, 5),
    "dictionary_lookup_save_daily": (-5, 5),
    "translation_daily_sets": (-1, 1),
    "feel_word_daily": (-1, 1),
}


def _tz(tz_name: str = ADMIN_ECONOMICS_TZ) -> ZoneInfo:
    try:
        return ZoneInfo(str(tz_name or ADMIN_ECONOMICS_TZ))
    except Exception:
        return ZoneInfo(ADMIN_ECONOMICS_TZ)


def _target_day(target_day: date | None = None, tz_name: str = ADMIN_ECONOMICS_TZ) -> date:
    if target_day is not None:
        return target_day
    return datetime.now(timezone.utc).astimezone(_tz(tz_name)).date()


def _fmt_num(value: Any) -> str:
    try:
        number = float(value or 0)
    except Exception:
        return "0"
    if number.is_integer():
        return str(int(number))
    return f"{number:.2f}".rstrip("0").rstrip(".")


def _pct(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(float(v or 0.0) for v in values)
    if len(ordered) == 1:
        return ordered[0]
    rank = (len(ordered) - 1) * max(0.0, min(1.0, percentile))
    low = int(rank)
    high = min(len(ordered) - 1, low + 1)
    weight = rank - low
    return ordered[low] * (1 - weight) + ordered[high] * weight


def _day_bounds(target_day: date, tz_name: str = ADMIN_ECONOMICS_TZ) -> tuple[datetime, datetime]:
    tzinfo = _tz(tz_name)
    start_local = datetime.combine(target_day, datetime.min.time(), tzinfo=tzinfo)
    end_local = start_local + timedelta(days=1)
    return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc)


def _fetch_active_user_ids(target_day: date, tz_name: str) -> set[int]:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                WITH active AS (
                    -- billing_events is a cost/free-limit ledger: it misses PRO users
                    -- (free-limit rows are gated on effective_mode='free') and cache hits.
                    -- Source "active" from real activity tables so all plans are counted.
                    SELECT user_id
                    FROM bt_3_billing_events
                    WHERE user_id IS NOT NULL
                      AND (event_time AT TIME ZONE %s)::date = %s
                    UNION
                    SELECT user_id
                    FROM bt_3_daily_sentences
                    WHERE user_id IS NOT NULL
                      AND COALESCE(shown_to_user, FALSE) = TRUE
                      AND (shown_to_user_at AT TIME ZONE %s)::date = %s
                    UNION
                    SELECT user_id
                    FROM bt_3_webapp_dictionary_queries
                    WHERE user_id IS NOT NULL
                      AND (created_at AT TIME ZONE %s)::date = %s
                )
                SELECT DISTINCT user_id
                FROM active
                WHERE user_id IS NOT NULL;
                """,
                (tz_name, target_day, tz_name, target_day, tz_name, target_day),
            )
            return {int(row[0]) for row in cursor.fetchall() or [] if row and row[0] is not None}


def _user_stats(target_day: date, tz_name: str) -> dict[str, Any]:
    active_ids = _fetch_active_user_ids(target_day, tz_name)
    free_count = 0
    pro_count = 0
    trial_count = 0
    for user_id in active_ids:
        try:
            entitlement = resolve_entitlement(user_id=int(user_id), tz=tz_name)
            mode = str(entitlement.get("effective_mode") or "free").lower()
        except Exception:
            mode = "free"
        if mode == "pro":
            pro_count += 1
        elif mode == "trial":
            trial_count += 1
        else:
            free_count += 1
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM bt_3_allowed_users
                WHERE (created_at AT TIME ZONE %s)::date = %s;
                """,
                (tz_name, target_day),
            )
            row = cursor.fetchone()
    return {
        "active_free_users": free_count,
        "active_pro_users": pro_count,
        "active_trial_users": trial_count,
        "new_users_today": int((row or [0])[0] or 0),
        "total_active_users": len(active_ids),
    }


def _openai_stats(target_day: date, tz_name: str) -> dict[str, Any]:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    action_type,
                    COUNT(*) FILTER (WHERE provider = 'openai' AND units_type = 'requests') AS request_events,
                    COALESCE(SUM(units_value) FILTER (WHERE units_type IN ('tokens_in', 'tokens_out')), 0) AS tokens
                FROM bt_3_billing_events
                WHERE (event_time AT TIME ZONE %s)::date = %s
                  AND provider = 'openai'
                GROUP BY action_type
                ORDER BY action_type;
                """,
                (tz_name, target_day),
            )
            action_rows = cursor.fetchall() or []
            cursor.execute(
                """
                SELECT event_type, COUNT(*)
                FROM bt_3_limit_runtime_events
                WHERE feature_code = 'dictionary_lookup_daily'
                  AND (event_time AT TIME ZONE %s)::date = %s
                GROUP BY event_type;
                """,
                (tz_name, target_day),
            )
            cache_rows = cursor.fetchall() or []
    requests_by_action = {str(row[0] or ""): int(row[1] or 0) for row in action_rows}
    total_requests = sum(1 for _action, count in requests_by_action.items() for _ in range(max(0, int(count))))
    cache_counts = {str(row[0] or ""): int(row[1] or 0) for row in cache_rows}
    avoided = (
        cache_counts.get("cache_hit", 0)
        + cache_counts.get("db_cache_hit", 0)
        + cache_counts.get("memory_cache_hit", 0)
    )
    lookup_openai = requests_by_action.get("dictionary_lookup", 0)
    lookup_total_observed = lookup_openai + avoided
    return {
        "total_openai_requests": int(total_requests),
        "lookup_requests": int(lookup_openai),
        "explain_requests": int(sum(v for k, v in requests_by_action.items() if "explain" in k or "explanation" in k)),
        "story_requests": int(sum(v for k, v in requests_by_action.items() if "story" in k)),
        "shortcut_split_requests": int(sum(v for k, v in requests_by_action.items() if "shortcut" in k)),
        "cache_hits": int(cache_counts.get("cache_hit", 0) + cache_counts.get("memory_cache_hit", 0)),
        "db_cache_hits": int(cache_counts.get("db_cache_hit", 0)),
        "openai_requests_avoided_by_cache": int(avoided),
        "estimated_cache_hit_ratio": (float(avoided) / float(lookup_total_observed)) if lookup_total_observed else 0.0,
        "estimated_db_cache_hit_ratio": (float(cache_counts.get("db_cache_hit", 0)) / float(lookup_total_observed)) if lookup_total_observed else 0.0,
        "requests_by_action": requests_by_action,
    }


def _limit_usage_values(feature_code: str, target_day: date, tz_name: str) -> dict[int, float]:
    feature = str(feature_code or "").strip().lower()
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            if feature == "translation_daily_sets":
                cursor.execute(
                    """
                    SELECT user_id, COUNT(DISTINCT session_id)::float
                    FROM bt_3_daily_sentences
                    WHERE user_id IS NOT NULL
                      AND COALESCE(shown_to_user, FALSE) = TRUE
                      AND (shown_to_user_at AT TIME ZONE %s)::date = %s
                    GROUP BY user_id;
                    """,
                    (tz_name, target_day),
                )
            elif feature == "feel_word_daily":
                cursor.execute(
                    """
                    SELECT user_id, COUNT(*)::float
                    FROM bt_3_billing_events
                    WHERE user_id IS NOT NULL
                      AND action_type = 'flashcards_feel_request'
                      AND units_type = 'requests'
                      AND (event_time AT TIME ZONE %s)::date = %s
                    GROUP BY user_id;
                    """,
                    (tz_name, target_day),
                )
            else:
                cursor.execute(
                    """
                    SELECT user_id, COALESCE(SUM(units_value), 0)::float
                    FROM bt_3_billing_events
                    WHERE user_id IS NOT NULL
                      AND action_type = %s
                      AND units_type = 'requests'
                      AND (event_time AT TIME ZONE %s)::date = %s
                    GROUP BY user_id;
                    """,
                    (feature, tz_name, target_day),
                )
            rows = cursor.fetchall() or []
    return {int(row[0]): float(row[1] or 0.0) for row in rows if row and row[0] is not None}


def _blocked_users(feature_code: str, target_day: date, tz_name: str) -> int:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(DISTINCT user_id)
                FROM bt_3_limit_runtime_events
                WHERE feature_code = %s
                  AND event_type = 'blocked'
                  AND user_id IS NOT NULL
                  AND (event_time AT TIME ZONE %s)::date = %s;
                """,
                (str(feature_code or "").strip().lower(), tz_name, target_day),
            )
            row = cursor.fetchone()
    return int((row or [0])[0] or 0)


def _limit_utilization(target_day: date, tz_name: str) -> list[dict[str, Any]]:
    limits = list_admin_configurable_limits(plan_code="free")
    result = []
    for limit in limits:
        feature = str(limit.get("feature_code") or "").strip().lower()
        values_by_user = _limit_usage_values(feature, target_day, tz_name)
        values = list(values_by_user.values())
        avg_value = (sum(values) / len(values)) if values else 0.0
        result.append(
            {
                **limit,
                "users_who_used": len(values),
                "average_usage": avg_value,
                "median_usage": float(median(values)) if values else 0.0,
                "p95_usage": _pct(values, 0.95),
                "max_usage": max(values) if values else 0.0,
                "blocked_user_count": _blocked_users(feature, target_day, tz_name),
            }
        )
    return result


def _user_activity(target_day: date, tz_name: str, *, limit: int = 15) -> list[dict[str, Any]]:
    """Per-user activity from real activity tables (plan-independent, unlike billing_events).

    Counts dictionary lookups/saves and translations shown so PRO users — whose actions
    never hit the free-limit billing ledger — are still visible.
    """
    dict_counts: dict[int, int] = {}
    translation_counts: dict[int, int] = {}
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT user_id, COUNT(*)
                FROM bt_3_webapp_dictionary_queries
                WHERE user_id IS NOT NULL
                  AND (created_at AT TIME ZONE %s)::date = %s
                GROUP BY user_id;
                """,
                (tz_name, target_day),
            )
            for row in cursor.fetchall() or []:
                if row and row[0] is not None:
                    dict_counts[int(row[0])] = int(row[1] or 0)
            cursor.execute(
                """
                SELECT user_id, COUNT(*)
                FROM bt_3_daily_sentences
                WHERE user_id IS NOT NULL
                  AND COALESCE(shown_to_user, FALSE) = TRUE
                  AND (shown_to_user_at AT TIME ZONE %s)::date = %s
                GROUP BY user_id;
                """,
                (tz_name, target_day),
            )
            for row in cursor.fetchall() or []:
                if row and row[0] is not None:
                    translation_counts[int(row[0])] = int(row[1] or 0)

    result = []
    for user_id in set(dict_counts) | set(translation_counts):
        dict_actions = dict_counts.get(user_id, 0)
        translations = translation_counts.get(user_id, 0)
        try:
            entitlement = resolve_entitlement(user_id=int(user_id), tz=tz_name)
            plan = str(entitlement.get("effective_mode") or "free").lower()
        except Exception:
            plan = "free"
        result.append(
            {
                "user_id": int(user_id),
                "plan": plan,
                "dict_actions": dict_actions,
                "translations": translations,
                "total": dict_actions + translations,
            }
        )
    result.sort(key=lambda item: item["total"], reverse=True)
    return result[:limit]


def _openai_by_user(target_day: date, tz_name: str, *, limit: int = 15) -> list[dict[str, Any]]:
    """Per-user OpenAI usage + $-cost for the day (provider='openai' billing rows).
    Now that the bot tier logs usage with the acting user_id, this attributes
    requests/tokens/cost per user. Cost is in the billing currency (USD default);
    0 until price snapshots exist for the gateway model's input/output SKUs."""
    rows_out: list[dict[str, Any]] = []
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT user_id,
                       COUNT(*) FILTER (WHERE units_type = 'requests') AS requests,
                       COALESCE(SUM(units_value) FILTER (WHERE units_type IN ('tokens_in', 'tokens_out')), 0) AS tokens,
                       COALESCE(SUM(cost_amount), 0) AS cost
                FROM bt_3_billing_events
                WHERE provider = 'openai'
                  AND user_id IS NOT NULL
                  AND (event_time AT TIME ZONE %s)::date = %s
                GROUP BY user_id
                ORDER BY cost DESC, requests DESC
                LIMIT %s;
                """,
                (tz_name, target_day, int(limit)),
            )
            for row in cursor.fetchall() or []:
                if not row or row[0] is None:
                    continue
                try:
                    plan = str(resolve_entitlement(user_id=int(row[0]), tz=tz_name).get("effective_mode") or "free").lower()
                except Exception:
                    plan = "free"
                rows_out.append({
                    "user_id": int(row[0]), "plan": plan,
                    "requests": int(row[1] or 0), "tokens": int(row[2] or 0),
                    "cost": float(row[3] or 0.0),
                })
    return rows_out


def _gpt_helper_usage(target_day: date, tz_name: str) -> dict[str, int]:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT action_type, COUNT(*)
                FROM bt_3_billing_events
                WHERE provider = 'openai'
                  AND units_type = 'requests'
                  AND (event_time AT TIME ZONE %s)::date = %s
                GROUP BY action_type;
                """,
                (tz_name, target_day),
            )
            rows = cursor.fetchall() or []
    by_action = {str(row[0] or ""): int(row[1] or 0) for row in rows}
    return {
        "explain": sum(v for k, v in by_action.items() if "explain" in k or "explanation" in k),
        "explain_question": sum(v for k, v in by_action.items() if "private_question" in k or "ask_gpt" in k),
        "collocations": sum(v for k, v in by_action.items() if "collocation" in k),
        "story": sum(v for k, v in by_action.items() if "story" in k),
        "reader_gpt": sum(v for k, v in by_action.items() if "reader" in k),
        "youtube_gpt": sum(v for k, v in by_action.items() if "youtube" in k),
    }


def _top_consumers(target_day: date, tz_name: str) -> dict[str, list[dict[str, Any]]]:
    features = {
        "lookup": "dictionary_lookup_daily",
        "shortcut": "shortcut_forwarded_message_daily",
        "save": "dictionary_lookup_save_daily",
    }
    result = {}
    for key, feature in features.items():
        values = _limit_usage_values(feature, target_day, tz_name)
        result[key] = [
            {"user_id": int(user_id), "usage": usage}
            for user_id, usage in sorted(values.items(), key=lambda item: item[1], reverse=True)[:10]
        ]
    return result


def _trend_from_snapshots(target_day: date) -> dict[str, Any]:
    start_day = target_day - timedelta(days=6)
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT day, payload_json
                FROM bt_3_admin_economics_daily_snapshots
                WHERE day BETWEEN %s AND %s
                ORDER BY day ASC;
                """,
                (start_day, target_day),
            )
            rows = cursor.fetchall() or []
    by_feature: dict[str, dict[str, list[float]]] = {}
    for _day, payload in rows:
        if not isinstance(payload, dict):
            continue
        for item in payload.get("limit_utilization") or []:
            if not isinstance(item, dict):
                continue
            feature = str(item.get("feature_code") or "").strip().lower()
            if not feature:
                continue
            bucket = by_feature.setdefault(feature, {"average_usage": [], "max_usage": [], "blocked_user_count": []})
            bucket["average_usage"].append(float(item.get("average_usage") or 0.0))
            bucket["max_usage"].append(float(item.get("max_usage") or 0.0))
            bucket["blocked_user_count"].append(float(item.get("blocked_user_count") or 0.0))
    trend = {}
    for feature, values in by_feature.items():
        trend[feature] = {
            "days": len(values.get("average_usage") or []),
            "avg_usage_7d": sum(values["average_usage"]) / len(values["average_usage"]) if values["average_usage"] else 0.0,
            "max_usage_7d": max(values["max_usage"]) if values["max_usage"] else 0.0,
            "blocked_users_7d": sum(values["blocked_user_count"]) if values["blocked_user_count"] else 0.0,
        }
    return trend


def build_admin_economics_report_payload(
    *,
    target_day: date | None = None,
    tz_name: str = ADMIN_ECONOMICS_TZ,
    save_snapshot: bool = True,
) -> dict[str, Any]:
    ensure_admin_economics_schema()
    day = _target_day(target_day, tz_name)
    payload = {
        "day": day.isoformat(),
        "tz_name": tz_name,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "user_stats": _user_stats(day, tz_name),
        "user_activity": _user_activity(day, tz_name),
        "openai_stats": _openai_stats(day, tz_name),
        "openai_by_user": _openai_by_user(day, tz_name),
        "limit_utilization": _limit_utilization(day, tz_name),
        "gpt_helper_usage": _gpt_helper_usage(day, tz_name),
        "top_consumers": _top_consumers(day, tz_name),
    }
    if save_snapshot:
        with get_db_connection_context() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO bt_3_admin_economics_daily_snapshots (
                        day,
                        tz_name,
                        payload_json,
                        updated_at
                    )
                    VALUES (%s, %s, %s, NOW())
                    ON CONFLICT (day) DO UPDATE
                    SET
                        tz_name = EXCLUDED.tz_name,
                        payload_json = EXCLUDED.payload_json,
                        updated_at = NOW();
                    """,
                    (day, tz_name, Json(payload)),
                )
    payload["trend_7d"] = _trend_from_snapshots(day)
    return payload


def _num(value: Any) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def format_admin_economics_report(payload: dict[str, Any]) -> str:
    """Scan-friendly daily economics report.

    Design: surface signal (active users, cost, who hit limits), hide noise
    (all the all-zero limit rows are collapsed into a single tail line). Every
    number is still derivable; nothing is dropped, only zero rows are folded.
    """
    stats = payload.get("user_stats") or {}
    openai_stats = payload.get("openai_stats") or {}
    helpers = payload.get("gpt_helper_usage") or {}
    trend = payload.get("trend_7d") or {}

    L: list[str] = []

    # ── Header + users (one line) ────────────────────────────────────────────
    L.append(f"📊 Экономика · {payload.get('day')}  ({payload.get('tz_name') or ADMIN_ECONOMICS_TZ})")
    L.append("")
    L.append(
        f"👥 Активны: {_fmt_num(stats.get('total_active_users'))}  "
        f"(FREE {_fmt_num(stats.get('active_free_users'))} · "
        f"PRO {_fmt_num(stats.get('active_pro_users'))} · "
        f"TRIAL {_fmt_num(stats.get('active_trial_users'))})  ·  "
        f"+{_fmt_num(stats.get('new_users_today'))} новых"
    )

    # ── Activity by user (top first, medal for #1) ───────────────────────────
    activity = sorted(
        payload.get("user_activity") or [],
        key=lambda it: int(it.get("total") or 0), reverse=True,
    )
    L.append("")
    L.append("📈 Активность по пользователям")
    if not activity:
        L.append("   —")
    for i, it in enumerate(activity):
        medal = "🥇" if i == 0 else "  "
        d, t = int(it.get("dict_actions") or 0), int(it.get("translations") or 0)
        detail = " ".join(p for p in (f"📖{d}" if d else "", f"🔁{t}" if t else "") if p) or "—"
        L.append(
            f"{medal} {int(it.get('user_id') or 0)} {str(it.get('plan') or 'free').upper()} · "
            f"{_fmt_num(it.get('total'))} ({detail})"
        )

    # ── OpenAI (one line when silent; expand only when there were calls) ──────
    by_user = payload.get("openai_by_user") or []
    total_cost = sum(_num(it.get("cost")) for it in by_user)
    total_reqs = int(openai_stats.get("total_openai_requests") or 0)
    L.append("")
    if total_reqs == 0 and total_cost == 0:
        L.append("🤖 OpenAI: 0 запросов · $0.0000  (нет вызовов / всё из кэша)")
    else:
        L.append(f"🤖 OpenAI: {total_reqs} запросов · ${total_cost:.4f}")
        sub = []
        for key, lbl in (("lookup_requests", "lookup"), ("explain_requests", "explain"),
                         ("story_requests", "story"), ("shortcut_split_requests", "shortcut")):
            v = int(openai_stats.get(key) or 0)
            if v:
                sub.append(f"{lbl} {v}")
        if sub:
            L.append("   " + " · ".join(sub))
        hit = _num(openai_stats.get("estimated_cache_hit_ratio")) * 100
        avoided = int(openai_stats.get("openai_requests_avoided_by_cache") or 0)
        if hit or avoided:
            L.append(f"   кэш: hit {hit:.0f}% · сэкономлено {avoided} запросов")
        for it in sorted(by_user, key=lambda x: _num(x.get("cost")), reverse=True):
            if _num(it.get("cost")) <= 0 and int(it.get("requests") or 0) <= 0:
                continue
            L.append(
                f"   💸 {int(it.get('user_id') or 0)} {str(it.get('plan') or 'free').upper()} · "
                f"${_num(it.get('cost')):.4f} · {_fmt_num(it.get('requests'))} req"
            )

    # ── Limits: only the ones with activity; the rest folded into one line ────
    L.append("")
    L.append("🚦 Лимиты")
    limits = payload.get("limit_utilization") or []
    active_rows, idle = [], 0
    blocked_today = []
    for it in limits:
        feature = str(it.get("feature_code") or "")
        title = str(it.get("title") or feature)
        ft = trend.get(feature) or {}
        users = int(it.get("users_who_used") or 0)
        blocked = int(it.get("blocked_user_count") or 0)
        mx = int(it.get("max_usage") or 0)
        t_avg = _num(ft.get("avg_usage_7d"))
        t_blocked = int(ft.get("blocked_users_7d") or 0)
        has_activity = users or blocked or mx or t_avg or t_blocked
        if not has_activity:
            idle += 1
            continue
        if blocked:
            blocked_today.append(title)
        parts = []
        if users or mx:
            parts.append(f"сегодня {users} польз, max {mx}/{_fmt_num(it.get('limit_value'))}")
        if blocked:
            parts.append(f"{blocked} заблокир.")
        tail7 = []
        if t_avg:
            tail7.append(f"avg {t_avg:.1f}")
        if t_blocked:
            tail7.append(f"⛔{t_blocked}")
        if tail7:
            parts.append("7д: " + " ".join(tail7))
        icon = "⛔" if blocked else "•"
        active_rows.append(f" {icon} {title} — " + " · ".join(parts))
    if blocked_today:
        L.append(f"⛔ Сегодня упёрлись в лимит: {', '.join(blocked_today)}")
    elif not active_rows:
        L.append("   ✅ Никто не упирался в лимиты")
    L.extend(active_rows)
    if idle:
        L.append(f"   …ещё {idle} лимитов — без активности")

    # ── GPT helpers: only non-zero; one line when silent ─────────────────────
    helper_items = [
        ("Explain", helpers.get("explain")), ("Explain-Q", helpers.get("explain_question")),
        ("Kollokationen", helpers.get("collocations")), ("Story", helpers.get("story")),
        ("Reader", helpers.get("reader_gpt")), ("YouTube", helpers.get("youtube_gpt")),
    ]
    used = [f"{lbl} {int(v or 0)}" for lbl, v in helper_items if int(v or 0)]
    L.append("")
    L.append("🧠 GPT-хелперы: " + (" · ".join(used) if used else "без вызовов"))

    # ── Top consumers: skip empty buckets entirely ───────────────────────────
    top = payload.get("top_consumers") or {}
    top_lines = []
    for label, items in (("Lookup", top.get("lookup") or []), ("Shortcut", top.get("shortcut") or []), ("Save", top.get("save") or [])):
        if not items:
            continue
        top_lines.append(f"{label}: " + ", ".join(
            f"{int(it.get('user_id') or 0)}·{_fmt_num(it.get('usage'))}" for it in items[:5]))
    if top_lines:
        L.append("")
        L.append("🔥 Топ потребители")
        L.extend("   " + t for t in top_lines)

    return "\n".join(L).strip()


def build_admin_economics_limits_keyboard() -> dict[str, Any]:
    limits = list_admin_configurable_limits(plan_code="free")
    rows = []
    for limit in limits:
        feature = str(limit.get("feature_code") or "")
        deltas = _LIMIT_BUTTON_DELTAS.get(feature, (-1, 1))
        rows.append([{"text": feature[:32], "callback_data": f"admecon:noop:{feature[:20]}"}])
        row = []
        for delta in deltas:
            label = f"{delta:+d}"
            row.append({"text": label, "callback_data": f"admecon:preview:{feature}:{delta}"})
        rows.append(row)
    rows.append([{"text": "🔄 Refresh", "callback_data": "admecon:refresh"}])
    return {"inline_keyboard": rows}


def build_admin_limit_preview_keyboard(token: str) -> dict[str, Any]:
    token_value = str(token or "").strip()
    return {
        "inline_keyboard": [
            [
                {"text": "✅ Apply", "callback_data": f"admecon:apply:{token_value}"},
                {"text": "❌ Cancel", "callback_data": f"admecon:cancel:{token_value}"},
            ]
        ]
    }


def format_admin_limit_preview(preview: dict[str, Any]) -> str:
    return (
        "⚙️ Limit Change Preview\n\n"
        f"Limit:\n{preview.get('feature_code')}\n\n"
        f"Current:\n{_fmt_num(preview.get('old_value'))} / {preview.get('period')}\n\n"
        f"Proposed:\n{_fmt_num(preview.get('new_value'))} / {preview.get('period')}\n\n"
        "Apply this change?"
    )


def _send_telegram_message(
    *,
    user_id: int,
    text: str,
    reply_markup: dict[str, Any] | None = None,
) -> int | None:
    token = os.getenv("TELEGRAM_Deutsch_BOT_TOKEN")
    if not token:
        raise RuntimeError("TELEGRAM_Deutsch_BOT_TOKEN is not configured")
    response = requests.post(
        f"https://api.telegram.org/bot{token}/sendMessage",
        json={
            "chat_id": int(user_id),
            "text": text,
            "disable_web_page_preview": True,
            **({"reply_markup": reply_markup} if reply_markup else {}),
        },
        timeout=20,
    )
    if response.status_code >= 400:
        raise RuntimeError(f"Telegram API error: {response.text}")
    payload = response.json() if response.content else {}
    return (payload.get("result") or {}).get("message_id")


def _split_telegram_text(text: str, limit: int = 3800) -> list[str]:
    parts: list[str] = []
    buf = ""
    for line in str(text or "").splitlines():
        candidate = f"{buf}\n{line}" if buf else line
        if len(candidate) > limit and buf:
            parts.append(buf)
            buf = line
        else:
            buf = candidate
    if buf:
        parts.append(buf)
    return parts or [""]


def send_admin_economics_report(*, target_day: date | None = None, force: bool = False) -> dict[str, Any]:
    day = _target_day(target_day, ADMIN_ECONOMICS_TZ)
    run_period = day.isoformat()
    if not force and not claim_scheduler_run_guard(
        job_key=ADMIN_ECONOMICS_JOB_KEY,
        run_period=run_period,
        target_scope="global",
        metadata={"tz": ADMIN_ECONOMICS_TZ},
    ):
        return {"ok": True, "skipped": True, "reason": "already_claimed", "day": run_period}
    try:
        admin_ids = sorted(int(item) for item in get_admin_telegram_ids() if int(item) > 0)
        if not admin_ids:
            if not force:
                finish_scheduler_run_guard(
                    job_key=ADMIN_ECONOMICS_JOB_KEY,
                    run_period=run_period,
                    target_scope="global",
                    status="failed",
                    metadata={"error": "no_admin_ids"},
                )
            return {"ok": False, "sent": 0, "error": "no_admin_ids", "day": run_period}
        payload = build_admin_economics_report_payload(target_day=day, tz_name=ADMIN_ECONOMICS_TZ, save_snapshot=True)
        text = format_admin_economics_report(payload)
        keyboard = build_admin_economics_limits_keyboard()
        sent = 0
        for admin_id in admin_ids:
            parts = _split_telegram_text(text)
            for index, part in enumerate(parts):
                _send_telegram_message(
                    user_id=int(admin_id),
                    text=part,
                    reply_markup=keyboard if index == len(parts) - 1 else None,
                )
            sent += 1
        if not force:
            finish_scheduler_run_guard(
                job_key=ADMIN_ECONOMICS_JOB_KEY,
                run_period=run_period,
                target_scope="global",
                status="completed",
                metadata={"sent": sent},
            )
        return {"ok": True, "sent": sent, "day": run_period}
    except Exception as exc:
        if not force:
            finish_scheduler_run_guard(
                job_key=ADMIN_ECONOMICS_JOB_KEY,
                run_period=run_period,
                target_scope="global",
                status="failed",
                metadata={"error": str(exc)},
            )
        logging.exception("admin economics report failed")
        raise


DICT_DEDUP_REPORT_TZ = "Europe/Vienna"
DICT_DEDUP_REPORT_JOB_KEY = "dict_dedup_weekly_report"


def format_dict_dedup_weekly_report(report: dict[str, Any]) -> str:
    """Render the weekly duplicate-removal summary as a Telegram message (Russian)."""
    days = int(report.get("days") or 7)
    window_deleted = int(report.get("window_entries_deleted") or 0)
    window_groups = int(report.get("window_groups_found") or 0)
    window_runs = int(report.get("window_runs") or 0)
    window_active = int(report.get("window_active_runs") or 0)
    total_deleted = int(report.get("total_entries_deleted") or 0)
    last_run_raw = report.get("last_run_at")

    last_run_text = "—"
    if last_run_raw:
        try:
            dt = datetime.fromisoformat(str(last_run_raw))
            if dt.tzinfo is not None:
                dt = dt.astimezone(ZoneInfo(DICT_DEDUP_REPORT_TZ))
            last_run_text = dt.strftime("%d.%m.%Y %H:%M")
        except Exception:
            last_run_text = str(last_run_raw)

    lines = [
        "🧹 Чистка словаря от дубликатов — отчёт за неделю",
        "",
        f"📅 За последние {days} дн.:",
        f"   • удалено дубликатов: {window_deleted}",
        f"   • затронуто слов (групп): {window_groups}",
        f"   • прогонов джобы: {window_runs} (с удалениями: {window_active})",
        "",
        f"♾️ Всего удалено за всё время: {total_deleted}",
        f"🕒 Последний прогон: {last_run_text}",
    ]

    if window_runs == 0:
        lines += ["", "⚠️ За неделю не было ни одного прогона — проверь, что ночная джоба работает."]
    elif window_deleted == 0:
        lines += ["", "ℹ️ Джоба отработала, но дубликатов не нашла — это норма, если их просто нет."]

    return "\n".join(lines)


def send_dict_dedup_weekly_report(*, days: int = 7, force: bool = False) -> dict[str, Any]:
    """Build and DM the weekly duplicate-removal summary to all admins.

    Mirrors send_admin_economics_report: bot-side delivery, ISO-week run-guard, and
    force=True bypasses the guard so a stale claim can't block delivery.
    """
    now_local = datetime.now(ZoneInfo(DICT_DEDUP_REPORT_TZ))
    iso = now_local.isocalendar()
    run_period = f"{iso[0]}-W{int(iso[1]):02d}"
    if not force and not claim_scheduler_run_guard(
        job_key=DICT_DEDUP_REPORT_JOB_KEY,
        run_period=run_period,
        target_scope="global",
        metadata={"tz": DICT_DEDUP_REPORT_TZ},
    ):
        return {"ok": True, "skipped": True, "reason": "already_claimed", "week": run_period}
    try:
        admin_ids = sorted(int(item) for item in get_admin_telegram_ids() if int(item) > 0)
        if not admin_ids:
            if not force:
                finish_scheduler_run_guard(
                    job_key=DICT_DEDUP_REPORT_JOB_KEY,
                    run_period=run_period,
                    target_scope="global",
                    status="failed",
                    metadata={"error": "no_admin_ids"},
                )
            return {"ok": False, "sent": 0, "error": "no_admin_ids", "week": run_period}
        report = get_dict_dedup_report(days=days)
        text = format_dict_dedup_weekly_report(report)
        sent = 0
        for admin_id in admin_ids:
            for part in _split_telegram_text(text):
                _send_telegram_message(user_id=int(admin_id), text=part)
            sent += 1
        if not force:
            finish_scheduler_run_guard(
                job_key=DICT_DEDUP_REPORT_JOB_KEY,
                run_period=run_period,
                target_scope="global",
                status="completed",
                metadata={"sent": sent},
            )
        return {"ok": True, "sent": sent, "week": run_period, "report": report}
    except Exception as exc:
        if not force:
            finish_scheduler_run_guard(
                job_key=DICT_DEDUP_REPORT_JOB_KEY,
                run_period=run_period,
                target_scope="global",
                status="failed",
                metadata={"error": str(exc)},
            )
        logging.exception("dict dedup weekly report failed")
        raise
