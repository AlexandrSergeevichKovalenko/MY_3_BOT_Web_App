#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import statistics
from collections import Counter
from pathlib import Path
from typing import Any


TARGET_FLOW = "translation_check"
REQUEST_FLOWS = (
    "start_session",
    "progressive_fill",
    "sentences_ack",
    "check_start",
    "check_status",
    "session_completion",
    "post_finish_session",
    "post_finish_today",
    "post_finish_skills",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Summarize one staged translation load test run.")
    parser.add_argument("--stage-result", required=True)
    parser.add_argument("--logs", required=True)
    parser.add_argument("--db-before", required=True)
    parser.add_argument("--db-after", required=True)
    parser.add_argument("--skill-before", required=True)
    parser.add_argument("--skill-after", required=True)
    parser.add_argument("--out", default="")
    return parser.parse_args()


def load_json(path: str) -> Any:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    if len(values) == 1:
        return values[0]
    ordered = sorted(values)
    rank = (len(ordered) - 1) * (pct / 100.0)
    low = int(math.floor(rank))
    high = int(math.ceil(rank))
    if low == high:
        return ordered[low]
    weight = rank - low
    return ordered[low] * (1.0 - weight) + ordered[high] * weight


def summarize_numbers(values: list[float]) -> dict[str, float | int | None]:
    if not values:
        return {"count": 0, "avg": None, "p50": None, "p95": None, "p99": None}
    return {
        "count": len(values),
        "avg": sum(values) / len(values),
        "p50": statistics.median(values),
        "p95": percentile(values, 95),
        "p99": percentile(values, 99),
    }


def extract_json_payload(line: str) -> str | None:
    stripped = line.strip()
    if not stripped:
        return None
    if stripped.startswith("{") and stripped.endswith("}"):
        return stripped
    marker = "obs "
    marker_index = stripped.find(marker)
    if marker_index >= 0:
        candidate = stripped[marker_index + len(marker) :].strip()
        if candidate.startswith("{") and candidate.endswith("}"):
            return candidate
    first_brace = stripped.find("{")
    last_brace = stripped.rfind("}")
    if first_brace >= 0 and last_brace > first_brace:
        candidate = stripped[first_brace : last_brace + 1]
        if '"flow"' in candidate:
            return candidate
    return None


def parse_event(line: str) -> dict[str, Any] | None:
    payload = extract_json_payload(line)
    if not payload:
        return None
    try:
        parsed = json.loads(payload)
    except Exception:
        return None
    if not isinstance(parsed, dict):
        return None
    flow = str(parsed.get("flow") or "").strip()
    if flow == TARGET_FLOW:
        return parsed
    for key in ("message", "msg", "log"):
        nested = parsed.get(key)
        if not isinstance(nested, str):
            continue
        nested_payload = extract_json_payload(nested)
        if not nested_payload:
            continue
        try:
            nested_parsed = json.loads(nested_payload)
        except Exception:
            continue
        if isinstance(nested_parsed, dict) and str(nested_parsed.get("flow") or "").strip() == TARGET_FLOW:
            return nested_parsed
    return None


def to_int(value: Any) -> int | None:
    try:
        if value is None or value == "":
            return None
        return int(float(value))
    except Exception:
        return None


def to_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except Exception:
        return None


def summarize_request_records(records: list[dict[str, Any]]) -> dict[str, Any]:
    latencies = [float(item["latency_ms"]) for item in records]
    total = len(records)
    ok = sum(1 for item in records if bool(item.get("ok")))
    return {
        "total_requests": total,
        "success_rate": (ok / total) if total else None,
        "error_rate": ((total - ok) / total) if total else None,
        "requests_per_sec": None,
        **summarize_numbers(latencies),
    }


def summarize_session_records(records: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(records)
    success = sum(1 for item in records if bool(item.get("success")))
    failed = sum(1 for item in records if bool(item.get("failed")))
    timed_out = sum(1 for item in records if bool(item.get("timeout")))
    session_times = [float(item["session_time_ms"]) for item in records if item.get("session_time_ms") is not None]
    return {
        "total_sessions": total,
        "successful_sessions": success,
        "failed_sessions": failed,
        "timed_out_sessions": timed_out,
        "session_success_rate": (success / total) if total else None,
        "session_error_rate": ((total - success) / total) if total else None,
        "avg_session_time_ms": (sum(session_times) / len(session_times)) if session_times else None,
        "p95_session_time_ms": percentile(session_times, 95),
        "p99_session_time_ms": percentile(session_times, 99),
    }


def summarize_flow_records(records: list[dict[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for flow in REQUEST_FLOWS:
        flow_records = [item for item in records if str(item.get("flow") or "") == flow]
        latencies = [float(item["latency_ms"]) for item in flow_records]
        total = len(flow_records)
        errors = sum(1 for item in flow_records if not bool(item.get("ok")))
        result[flow] = {
            "count": total,
            "error_rate": (errors / total) if total else None,
            **summarize_numbers(latencies),
        }
    return result


def summarize_log_flow(events: list[dict[str, Any]], *, stage_name: str) -> dict[str, Any]:
    stage_counts = Counter(str(event.get("stage") or "unknown") for event in events)
    item_events = [event for event in events if str(event.get("stage") or "") == "item_processed"]
    status_events = [event for event in events if str(event.get("stage") or "") == "check_status_completed"]
    runner_finished = [event for event in events if str(event.get("stage") or "") == "runner_finished"]
    start_events = [event for event in events if str(event.get("stage") or "") == "check_start_completed"]
    terminal_outcomes = Counter(str(event.get("terminal_outcome") or "").strip().lower() for event in runner_finished)
    polling_counts = [to_float(event.get("status_polling_count")) for event in status_events]
    polling_counts = [value for value in polling_counts if value is not None]
    return {
        "stage": stage_name,
        "count_by_stage": dict(stage_counts),
        "check_start": summarize_numbers([to_float(event.get("duration_ms")) for event in start_events if to_float(event.get("duration_ms")) is not None]),
        "check_status": summarize_numbers([to_float(event.get("duration_ms")) for event in status_events if to_float(event.get("duration_ms")) is not None]),
        "item_processing": summarize_numbers([to_float(event.get("per_item_duration_ms")) for event in item_events if to_float(event.get("per_item_duration_ms")) is not None]),
        "runner_start_delay_ms": summarize_numbers([to_float(event.get("runner_start_delay_ms")) for event in events if to_float(event.get("runner_start_delay_ms")) is not None]),
        "db_update_duration_ms": summarize_numbers([to_float(event.get("db_update_duration_ms")) for event in events if to_float(event.get("db_update_duration_ms")) is not None]),
        "session_completion_duration_ms": summarize_numbers([to_float(event.get("session_completion_duration_ms")) for event in runner_finished if to_float(event.get("session_completion_duration_ms")) is not None]),
        "avg_polling_count": (sum(polling_counts) / len(polling_counts)) if polling_counts else None,
        "terminal_outcomes": dict(terminal_outcomes),
    }


def db_delta(before: dict[str, Any], after: dict[str, Any]) -> dict[str, int]:
    fields = (
        "translation_check_sessions",
        "translation_check_items",
        "skill_events_v2",
        "skill_state_v2_dirty",
    )
    return {
        field: int(after.get(field) or 0) - int(before.get(field) or 0)
        for field in fields
    }


def skill_delta(before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    before_status = before.get("status") or {}
    after_status = after.get("status") or {}
    before_dirty = ((before_status.get("dirty") or {}).get("total_keys"))
    after_dirty = ((after_status.get("dirty") or {}).get("total_keys"))
    before_errors = ((before_status.get("worker") or {}).get("error_count_total"))
    after_errors = ((after_status.get("worker") or {}).get("error_count_total"))
    return {
        "dirty_total_keys_before": before_dirty,
        "dirty_total_keys_after": after_dirty,
        "dirty_total_keys_delta": (int(after_dirty) - int(before_dirty)) if before_dirty is not None and after_dirty is not None else None,
        "worker_error_count_total_before": before_errors,
        "worker_error_count_total_after": after_errors,
        "worker_error_count_total_delta": (int(after_errors) - int(before_errors)) if before_errors is not None and after_errors is not None else None,
        "worker_last_duration_ms_after": ((after_status.get("worker") or {}).get("last_duration_ms")),
        "worker_last_keys_processed_after": ((after_status.get("worker") or {}).get("last_keys_processed")),
        "worker_last_events_processed_after": ((after_status.get("worker") or {}).get("last_events_processed")),
    }


def main() -> int:
    args = parse_args()
    stage_result = load_json(args.stage_result)
    db_before = load_json(args.db_before)
    db_after = load_json(args.db_after)
    skill_before = load_json(args.skill_before)
    skill_after = load_json(args.skill_after)

    request_records = list(stage_result.get("request_records") or [])
    session_records = list(stage_result.get("session_results") or [])
    stage_name = str(stage_result.get("stage") or "")
    stage_started_at = str(stage_result.get("started_at") or "")
    stage_finished_at = str(stage_result.get("finished_at") or "")
    user_count = int(stage_result.get("user_count") or 0)

    cohort_by_user: dict[int, str] = {}
    for item in session_records:
        user_id = to_int(item.get("user_id"))
        cohort = str(item.get("cohort") or "").strip().lower()
        if user_id is not None and cohort:
            cohort_by_user[user_id] = cohort
    for item in request_records:
        user_id = to_int(item.get("user_id"))
        cohort = str(item.get("cohort") or "").strip().lower()
        if user_id is not None and cohort and user_id not in cohort_by_user:
            cohort_by_user[user_id] = cohort
    stage_user_ids = set(cohort_by_user.keys())

    log_events: list[dict[str, Any]] = []
    for line in Path(args.logs).read_text(encoding="utf-8", errors="ignore").splitlines():
        parsed = parse_event(line)
        if not parsed:
            continue
        user_id = to_int(parsed.get("user_id"))
        if user_id is None or user_id not in stage_user_ids:
            continue
        log_events.append(parsed)

    cohorts = ("cold", "warm")
    request_summary: dict[str, Any] = {}
    session_summary: dict[str, Any] = {}
    flow_summary: dict[str, Any] = {}
    observability_summary: dict[str, Any] = {}

    # ISO parsing is not required for the current report. Duration is derived from request timestamps.
    stage_duration_sec = None
    if request_records:
        request_start_values = [to_float(item.get("start_ts")) for item in request_records if to_float(item.get("start_ts")) is not None]
        if request_start_values:
            min_ts = min(request_start_values)
            max_ts = max(request_start_values)
            stage_duration_sec = max(0.001, max_ts - min_ts)

    for cohort in cohorts:
        cohort_requests = [item for item in request_records if str(item.get("cohort") or "") == cohort]
        cohort_sessions = [item for item in session_records if str(item.get("cohort") or "") == cohort]
        cohort_users = {to_int(item.get("user_id")) for item in cohort_sessions}
        cohort_users.update({to_int(item.get("user_id")) for item in cohort_requests})
        cohort_users.discard(None)
        cohort_events = [event for event in log_events if to_int(event.get("user_id")) in cohort_users]
        request_summary[cohort] = summarize_request_records(cohort_requests)
        if stage_duration_sec is not None and cohort_requests:
            request_summary[cohort]["requests_per_sec"] = len(cohort_requests) / stage_duration_sec
        session_summary[cohort] = summarize_session_records(cohort_sessions)
        flow_summary[cohort] = summarize_flow_records(cohort_requests)
        obs = summarize_log_flow(cohort_events, stage_name=stage_name)
        flow_summary[cohort]["item_processing"] = {
            "count": int((obs.get("item_processing") or {}).get("count") or 0),
            "error_rate": None,
            **(obs.get("item_processing") or {}),
        }
        observability_summary[cohort] = obs

    output = {
        "stage": stage_name,
        "started_at": stage_started_at,
        "finished_at": stage_finished_at,
        "user_count": user_count,
        "request_summary": request_summary,
        "session_summary": session_summary,
        "flow_summary": flow_summary,
        "observability_summary": observability_summary,
        "db_before": db_before,
        "db_after": db_after,
        "db_delta": db_delta(db_before, db_after),
        "skill_before": skill_before,
        "skill_after": skill_after,
        "skill_delta": skill_delta(skill_before, skill_after),
    }

    if args.out:
        Path(args.out).write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(output, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
