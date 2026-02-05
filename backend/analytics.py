from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from calendar import monthrange
from typing import Any

from backend.database import get_db_connection_context


ALLOWED_PERIODS = {"day", "week", "month", "quarter", "half-year", "year", "all"}
ALLOWED_GRANULARITY = {"day", "week", "month", "quarter", "half-year", "year"}


@dataclass(frozen=True)
class PeriodBounds:
    start_date: date
    end_date: date


def _normalize_period(period: str | None) -> str:
    if not period:
        return "week"
    period = period.strip().lower()
    if period == "half_year":
        period = "half-year"
    if period not in ALLOWED_PERIODS:
        raise ValueError(f"Unsupported period: {period}")
    return period


def _normalize_granularity(granularity: str | None) -> str:
    if not granularity:
        return "day"
    granularity = granularity.strip().lower()
    if granularity == "half_year":
        granularity = "half-year"
    if granularity not in ALLOWED_GRANULARITY:
        raise ValueError(f"Unsupported granularity: {granularity}")
    return granularity


def get_period_bounds(period: str, today: date | None = None) -> PeriodBounds:
    period = _normalize_period(period)
    today = today or date.today()

    if period == "day":
        start = today
        end = today
    elif period == "week":
        start = today - timedelta(days=today.weekday())
        end = start + timedelta(days=6)
    elif period == "month":
        start = today.replace(day=1)
        end = date(today.year, today.month, monthrange(today.year, today.month)[1])
    elif period == "quarter":
        quarter_index = (today.month - 1) // 3
        start_month = quarter_index * 3 + 1
        end_month = start_month + 2
        start = date(today.year, start_month, 1)
        end = date(today.year, end_month, monthrange(today.year, end_month)[1])
    elif period == "half-year":
        if today.month <= 6:
            start = date(today.year, 1, 1)
            end = date(today.year, 6, 30)
        else:
            start = date(today.year, 7, 1)
            end = date(today.year, 12, 31)
    elif period == "year":
        start = date(today.year, 1, 1)
        end = date(today.year, 12, 31)
    elif period == "all":
        start = today
        end = today
    else:
        raise ValueError(f"Unsupported period: {period}")

    return PeriodBounds(start, end)


def get_all_time_bounds(user_id: int | None = None) -> PeriodBounds:
    where_user = "WHERE user_id = %s" if user_id else ""
    params = (user_id,) if user_id else ()
    sql = f"""
        WITH dates AS (
            SELECT MIN(date) AS min_date, MAX(date) AS max_date
            FROM bt_3_daily_sentences
            {where_user}
            UNION ALL
            SELECT MIN(timestamp::date) AS min_date, MAX(timestamp::date) AS max_date
            FROM bt_3_translations
            {where_user}
            UNION ALL
            SELECT MIN(start_time::date) AS min_date, MAX(start_time::date) AS max_date
            FROM bt_3_user_progress
            {where_user}
        )
        SELECT MIN(min_date) AS min_date, MAX(max_date) AS max_date
        FROM dates;
    """
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, params * 3)
            row = cursor.fetchone()
    min_date = row[0] if row else None
    max_date = row[1] if row else None
    today = date.today()
    return PeriodBounds(start_date=min_date or today, end_date=max_date or today)


def _period_start_expr(column: str, granularity: str) -> str:
    granularity = _normalize_granularity(granularity)
    if granularity == "half-year":
        return (
            f"make_date(EXTRACT(year FROM {column})::int, "
            f"CASE WHEN EXTRACT(month FROM {column}) <= 6 THEN 1 ELSE 7 END, 1)"
        )
    return f"date_trunc('{granularity}', {column})"


def _calculate_final_score(avg_score: float, avg_time_min: float, missed: int) -> float:
    return round(avg_score - avg_time_min * 1 - missed * 20, 2)


def _post_process_row(row: dict[str, Any]) -> dict[str, Any]:
    total = int(row.get("total_translations") or 0)
    success = int(row.get("successful_translations") or 0)
    total_time = float(row.get("total_time_min") or 0)
    assigned = int(row.get("assigned_sentences") or 0)
    avg_score = float(row.get("avg_score") or 0)
    avg_time = round(total_time / total, 2) if total > 0 else 0.0
    missed = max(0, assigned - total)
    missed_days = int(row.get("missed_days") or 0)
    success_rate = round((success / total) * 100, 1) if total > 0 else 0.0
    return {
        **row,
        "total_translations": total,
        "successful_translations": success,
        "unsuccessful_translations": max(0, total - success),
        "total_time_min": round(total_time, 2),
        "avg_time_min": avg_time,
        "assigned_sentences": assigned,
        "missed_sentences": missed,
        "missed_days": missed_days,
        "success_rate": success_rate,
        "avg_score": round(avg_score, 2),
        "final_score": _calculate_final_score(avg_score, avg_time, missed),
    }


def fetch_user_timeseries(
    user_id: int,
    start_date: date,
    end_date: date,
    granularity: str,
) -> list[dict[str, Any]]:
    granularity = _normalize_granularity(granularity)
    period_expr_t = _period_start_expr("t.timestamp", granularity)
    period_expr_p = _period_start_expr("p.start_time", granularity)
    period_expr_ds = _period_start_expr("ds.date", granularity)

    sql = f"""
        WITH base AS (
            SELECT
                t.user_id,
                t.id_for_mistake_table,
                t.score,
                t.timestamp,
                t.session_id,
                {period_expr_t} AS period_start,
                ROW_NUMBER() OVER (
                    PARTITION BY t.user_id, t.id_for_mistake_table
                    ORDER BY t.timestamp
                ) AS attempt_index,
                ROW_NUMBER() OVER (
                    PARTITION BY t.user_id, t.id_for_mistake_table, {period_expr_t}
                    ORDER BY t.timestamp DESC
                ) AS rn_period
            FROM bt_3_translations t
            WHERE t.user_id = %s AND t.timestamp::date BETWEEN %s AND %s
        ),
        filtered AS (
            SELECT *,
                CASE
                    WHEN attempt_index = 1 THEN (score >= 80)
                    ELSE (score >= 85)
                END AS is_success
            FROM base
            WHERE rn_period = 1
        ),
        translations_agg AS (
            SELECT
                period_start::date AS period_start,
                COUNT(*) AS total_translations,
                SUM(CASE WHEN is_success THEN 1 ELSE 0 END) AS successful_translations,
                SUM(CASE WHEN is_success AND attempt_index = 1 THEN 1 ELSE 0 END) AS success_on_1st_attempt,
                SUM(CASE WHEN is_success AND attempt_index = 2 THEN 1 ELSE 0 END) AS success_on_2nd_attempt,
                SUM(CASE WHEN is_success AND attempt_index >= 3 THEN 1 ELSE 0 END) AS success_on_3plus_attempt,
                AVG(score) AS avg_score
            FROM filtered
            GROUP BY period_start
        ),
        time_agg AS (
            SELECT
                {period_expr_p}::date AS period_start,
                SUM(EXTRACT(EPOCH FROM (end_time - start_time)) / 60) AS total_time_min,
                AVG(EXTRACT(EPOCH FROM (end_time - start_time)) / 60) AS avg_session_time_min
            FROM bt_3_user_progress p
            WHERE p.user_id = %s
                AND p.completed = TRUE
                AND p.start_time::date BETWEEN %s AND %s
            GROUP BY period_start
        ),
        assigned_agg AS (
            SELECT
                {period_expr_ds}::date AS period_start,
                COUNT(DISTINCT id_for_mistake_table) AS assigned_sentences
            FROM bt_3_daily_sentences ds
            WHERE ds.user_id = %s
                AND ds.date BETWEEN %s AND %s
            GROUP BY period_start
        )
        SELECT
            COALESCE(t.period_start, time_agg.period_start, assigned_agg.period_start) AS period_start,
            COALESCE(t.total_translations, 0) AS total_translations,
            COALESCE(t.successful_translations, 0) AS successful_translations,
            COALESCE(t.success_on_1st_attempt, 0) AS success_on_1st_attempt,
            COALESCE(t.success_on_2nd_attempt, 0) AS success_on_2nd_attempt,
            COALESCE(t.success_on_3plus_attempt, 0) AS success_on_3plus_attempt,
            COALESCE(t.avg_score, 0) AS avg_score,
            COALESCE(time_agg.total_time_min, 0) AS total_time_min,
            COALESCE(time_agg.avg_session_time_min, 0) AS avg_session_time_min,
            COALESCE(assigned_agg.assigned_sentences, 0) AS assigned_sentences
        FROM translations_agg t
        FULL OUTER JOIN time_agg
            ON time_agg.period_start = t.period_start
        FULL OUTER JOIN assigned_agg
            ON assigned_agg.period_start = COALESCE(t.period_start, time_agg.period_start)
        ORDER BY period_start;
    """

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                sql,
                (
                    user_id,
                    start_date,
                    end_date,
                    user_id,
                    start_date,
                    end_date,
                    user_id,
                    start_date,
                    end_date,
                ),
            )
            columns = [desc[0] for desc in cursor.description]
            rows = [dict(zip(columns, row)) for row in cursor.fetchall()]

    result = []
    for row in rows:
        row["period_start"] = row["period_start"].isoformat() if row["period_start"] else None
        result.append(_post_process_row(row))
    return result


def fetch_user_summary(
    user_id: int,
    start_date: date,
    end_date: date,
) -> dict[str, Any]:
    sql = """
        WITH base AS (
            SELECT
                t.user_id,
                t.id_for_mistake_table,
                t.score,
                t.timestamp,
                ROW_NUMBER() OVER (
                    PARTITION BY t.user_id, t.id_for_mistake_table
                    ORDER BY t.timestamp DESC
                ) AS rn_range,
                ROW_NUMBER() OVER (
                    PARTITION BY t.user_id, t.id_for_mistake_table
                    ORDER BY t.timestamp
                ) AS attempt_index
            FROM bt_3_translations t
            WHERE t.user_id = %s AND t.timestamp::date BETWEEN %s AND %s
        ),
        filtered AS (
            SELECT *,
                CASE
                    WHEN attempt_index = 1 THEN (score >= 80)
                    ELSE (score >= 85)
                END AS is_success
            FROM base
            WHERE rn_range = 1
        ),
        translations_agg AS (
            SELECT
                COUNT(*) AS total_translations,
                SUM(CASE WHEN is_success THEN 1 ELSE 0 END) AS successful_translations,
                AVG(score) AS avg_score
            FROM filtered
        ),
        time_agg AS (
            SELECT
                SUM(EXTRACT(EPOCH FROM (end_time - start_time)) / 60) AS total_time_min,
                AVG(EXTRACT(EPOCH FROM (end_time - start_time)) / 60) AS avg_session_time_min
            FROM bt_3_user_progress
            WHERE user_id = %s
                AND completed = TRUE
                AND start_time::date BETWEEN %s AND %s
        ),
        assigned_agg AS (
            SELECT
                COUNT(DISTINCT id_for_mistake_table) AS assigned_sentences
            FROM bt_3_daily_sentences
            WHERE user_id = %s AND date BETWEEN %s AND %s
        ),
        assigned_days AS (
            SELECT DISTINCT date
            FROM bt_3_daily_sentences
            WHERE user_id = %s AND date BETWEEN %s AND %s
        ),
        translated_days AS (
            SELECT DISTINCT timestamp::date AS date
            FROM bt_3_translations
            WHERE user_id = %s AND timestamp::date BETWEEN %s AND %s
        ),
        missed_days AS (
            SELECT COUNT(*) AS missed_days
            FROM assigned_days a
            LEFT JOIN translated_days t ON t.date = a.date
            WHERE t.date IS NULL
        )
        SELECT
            COALESCE(translations_agg.total_translations, 0) AS total_translations,
            COALESCE(translations_agg.successful_translations, 0) AS successful_translations,
            COALESCE(translations_agg.avg_score, 0) AS avg_score,
            COALESCE(time_agg.total_time_min, 0) AS total_time_min,
            COALESCE(time_agg.avg_session_time_min, 0) AS avg_session_time_min,
            COALESCE(assigned_agg.assigned_sentences, 0) AS assigned_sentences,
            COALESCE(missed_days.missed_days, 0) AS missed_days
        FROM translations_agg, time_agg, assigned_agg, missed_days;
    """

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                sql,
                (
                    user_id,
                    start_date,
                    end_date,
                    user_id,
                    start_date,
                    end_date,
                    user_id,
                    start_date,
                    end_date,
                    user_id,
                    start_date,
                    end_date,
                    user_id,
                    start_date,
                    end_date,
                ),
            )
            columns = [desc[0] for desc in cursor.description]
            row = cursor.fetchone()
            data = dict(zip(columns, row)) if row else {}

    return _post_process_row(data)


def fetch_comparison_leaderboard(
    start_date: date,
    end_date: date,
    limit: int = 10,
) -> list[dict[str, Any]]:
    sql = """
        WITH base AS (
            SELECT
                t.user_id,
                t.id_for_mistake_table,
                t.score,
                t.timestamp,
                ROW_NUMBER() OVER (
                    PARTITION BY t.user_id, t.id_for_mistake_table
                    ORDER BY t.timestamp DESC
                ) AS rn_range,
                ROW_NUMBER() OVER (
                    PARTITION BY t.user_id, t.id_for_mistake_table
                    ORDER BY t.timestamp
                ) AS attempt_index
            FROM bt_3_translations t
            WHERE t.timestamp::date BETWEEN %s AND %s
        ),
        filtered AS (
            SELECT *,
                CASE
                    WHEN attempt_index = 1 THEN (score >= 80)
                    ELSE (score >= 85)
                END AS is_success
            FROM base
            WHERE rn_range = 1
        ),
        translations_agg AS (
            SELECT
                user_id,
                COUNT(*) AS total_translations,
                SUM(CASE WHEN is_success THEN 1 ELSE 0 END) AS successful_translations,
                AVG(score) AS avg_score
            FROM filtered
            GROUP BY user_id
        ),
        time_agg AS (
            SELECT
                user_id,
                SUM(EXTRACT(EPOCH FROM (end_time - start_time)) / 60) AS total_time_min
            FROM bt_3_user_progress
            WHERE completed = TRUE
                AND start_time::date BETWEEN %s AND %s
            GROUP BY user_id
        ),
        assigned_agg AS (
            SELECT
                user_id,
                COUNT(DISTINCT id_for_mistake_table) AS assigned_sentences
            FROM bt_3_daily_sentences
            WHERE date BETWEEN %s AND %s
            GROUP BY user_id
        ),
        assigned_days AS (
            SELECT DISTINCT user_id, date
            FROM bt_3_daily_sentences
            WHERE date BETWEEN %s AND %s
        ),
        translated_days AS (
            SELECT DISTINCT user_id, timestamp::date AS date
            FROM bt_3_translations
            WHERE timestamp::date BETWEEN %s AND %s
        ),
        missed_days AS (
            SELECT
                a.user_id,
                COUNT(*) AS missed_days
            FROM assigned_days a
            LEFT JOIN translated_days t
                ON t.user_id = a.user_id AND t.date = a.date
            WHERE t.date IS NULL
            GROUP BY a.user_id
        ),
        latest_name AS (
            SELECT DISTINCT ON (user_id)
                user_id,
                username
            FROM bt_3_user_progress
            ORDER BY user_id, start_time DESC
        )
        SELECT
            t.user_id,
            COALESCE(latest_name.username, 'Unknown') AS username,
            COALESCE(t.total_translations, 0) AS total_translations,
            COALESCE(t.successful_translations, 0) AS successful_translations,
            COALESCE(t.avg_score, 0) AS avg_score,
            COALESCE(time_agg.total_time_min, 0) AS total_time_min,
            COALESCE(assigned_agg.assigned_sentences, 0) AS assigned_sentences,
            COALESCE(missed_days.missed_days, 0) AS missed_days
        FROM translations_agg t
        LEFT JOIN time_agg ON time_agg.user_id = t.user_id
        LEFT JOIN assigned_agg ON assigned_agg.user_id = t.user_id
        LEFT JOIN missed_days ON missed_days.user_id = t.user_id
        LEFT JOIN latest_name ON latest_name.user_id = t.user_id
        ORDER BY t.user_id;
    """

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                sql,
                (
                    start_date,
                    end_date,
                    start_date,
                    end_date,
                    start_date,
                    end_date,
                    start_date,
                    end_date,
                    start_date,
                    end_date,
                ),
            )
            columns = [desc[0] for desc in cursor.description]
            rows = [dict(zip(columns, row)) for row in cursor.fetchall()]

    processed = [_post_process_row(row) for row in rows]
    processed.sort(key=lambda item: item.get("final_score", 0), reverse=True)
    return processed[: max(1, limit)]
