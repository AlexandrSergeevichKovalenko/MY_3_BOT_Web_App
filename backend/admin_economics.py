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
                )
                SELECT DISTINCT user_id
                FROM active
                WHERE user_id IS NOT NULL;
                """,
                (tz_name, target_day, tz_name, target_day),
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
        "openai_stats": _openai_stats(day, tz_name),
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


def format_admin_economics_report(payload: dict[str, Any]) -> str:
    stats = payload.get("user_stats") or {}
    openai_stats = payload.get("openai_stats") or {}
    helpers = payload.get("gpt_helper_usage") or {}
    lines = [
        f"📊 Admin Economics — {payload.get('day')}",
        f"TZ: {payload.get('tz_name') or ADMIN_ECONOMICS_TZ}",
        "",
        "👥 Users",
        f"FREE active today: {_fmt_num(stats.get('active_free_users'))}",
        f"PRO active today: {_fmt_num(stats.get('active_pro_users'))}",
        f"TRIAL active today: {_fmt_num(stats.get('active_trial_users'))}",
        f"New users today: {_fmt_num(stats.get('new_users_today'))}",
        f"Total active users: {_fmt_num(stats.get('total_active_users'))}",
        "",
        "🤖 OpenAI",
        f"Total requests: {_fmt_num(openai_stats.get('total_openai_requests'))}",
        f"Lookup: {_fmt_num(openai_stats.get('lookup_requests'))}",
        f"Explain: {_fmt_num(openai_stats.get('explain_requests'))}",
        f"Story: {_fmt_num(openai_stats.get('story_requests'))}",
        f"Shortcut split: {_fmt_num(openai_stats.get('shortcut_split_requests'))}",
        f"Cache hit ratio: {_fmt_num(float(openai_stats.get('estimated_cache_hit_ratio') or 0) * 100)}%",
        f"DB cache hit ratio: {_fmt_num(float(openai_stats.get('estimated_db_cache_hit_ratio') or 0) * 100)}%",
        f"OpenAI avoided by cache: {_fmt_num(openai_stats.get('openai_requests_avoided_by_cache'))}",
        "",
        "📏 Limits",
    ]
    trend = payload.get("trend_7d") or {}
    for item in payload.get("limit_utilization") or []:
        feature = str(item.get("feature_code") or "")
        title = str(item.get("title") or feature)
        feature_trend = trend.get(feature) or {}
        lines.extend(
            [
                f"{title} ({feature}): {_fmt_num(item.get('limit_value'))}/{item.get('period') or 'day'}",
                (
                    f"users {_fmt_num(item.get('users_who_used'))} | avg {_fmt_num(item.get('average_usage'))} "
                    f"| med {_fmt_num(item.get('median_usage'))} | p95 {_fmt_num(item.get('p95_usage'))} "
                    f"| max {_fmt_num(item.get('max_usage'))} | blocked {_fmt_num(item.get('blocked_user_count'))}"
                ),
                (
                    f"7d avg {_fmt_num(feature_trend.get('avg_usage_7d'))} | "
                    f"7d max {_fmt_num(feature_trend.get('max_usage_7d'))} | "
                    f"7d blocked {_fmt_num(feature_trend.get('blocked_users_7d'))}"
                ),
            ]
        )
    lines.extend(
        [
            "",
            "🧠 GPT Helpers",
            f"Explain: {_fmt_num(helpers.get('explain'))}",
            f"Explain question: {_fmt_num(helpers.get('explain_question'))}",
            f"Collocations: {_fmt_num(helpers.get('collocations'))}",
            f"Story: {_fmt_num(helpers.get('story'))}",
            f"Reader GPT: {_fmt_num(helpers.get('reader_gpt'))}",
            f"YouTube GPT: {_fmt_num(helpers.get('youtube_gpt'))}",
            "",
            "🔥 Top Consumers",
        ]
    )
    top = payload.get("top_consumers") or {}
    for label, items in (("Lookup", top.get("lookup") or []), ("Shortcut", top.get("shortcut") or []), ("Save", top.get("save") or [])):
        lines.append(label + ":")
        if not items:
            lines.append("- none")
        for index, item in enumerate(items[:10], start=1):
            lines.append(f"{index}. {int(item.get('user_id') or 0)} — {_fmt_num(item.get('usage'))}")
    return "\n".join(lines).strip()


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
