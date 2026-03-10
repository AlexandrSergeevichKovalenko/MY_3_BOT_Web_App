#!/usr/bin/env python3
"""Analyze structured observability logs for tts and translation_check flows.

Expected input lines:
- `obs {"flow":"tts", ...}` (as emitted by backend logging)
- Raw JSON lines containing `"flow": "tts"` / `"translation_check"`
"""

from __future__ import annotations

import argparse
import json
import math
import statistics
import sys
from collections import Counter
from pathlib import Path
from typing import Any


TARGET_FLOWS = {"tts", "translation_check"}
TTS_ENDPOINT_STAGES = {
    "tts_url_completed",
    "tts_generate_completed",
    "tts_legacy_completed",
    "reader_audio_completed",
}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Analyze observability logs for tts and translation_check flows.",
    )
    parser.add_argument(
        "paths",
        nargs="*",
        help="Log file paths. If omitted, read from stdin.",
    )
    parser.add_argument(
        "--json-out",
        default="",
        help="Optional path to write machine-readable JSON summary.",
    )
    return parser.parse_args()


def _iter_lines(paths: list[str]):
    if not paths:
        for line in sys.stdin:
            yield line
        return

    for raw_path in paths:
        path = Path(raw_path)
        with path.open("r", encoding="utf-8", errors="ignore") as handle:
            for line in handle:
                yield line


def _extract_json_payload(line: str) -> str | None:
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


def _parse_event(line: str) -> dict[str, Any] | None:
    payload = _extract_json_payload(line)
    if not payload:
        return None
    try:
        parsed = json.loads(payload)
    except Exception:
        return None
    if not isinstance(parsed, dict):
        return None
    flow = str(parsed.get("flow") or "").strip()
    if flow not in TARGET_FLOWS:
        # Support wrappers like `railway logs --json` where app log line is nested.
        for key in ("message", "msg", "log"):
            nested = parsed.get(key)
            if not isinstance(nested, str):
                continue
            nested_payload = _extract_json_payload(nested)
            if not nested_payload:
                continue
            try:
                nested_parsed = json.loads(nested_payload)
            except Exception:
                continue
            if isinstance(nested_parsed, dict) and str(nested_parsed.get("flow") or "").strip() in TARGET_FLOWS:
                return nested_parsed
        return None
    return parsed


def _to_float(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return float(text)
        except Exception:
            return None
    return None


def _to_int(value: Any) -> int | None:
    as_float = _to_float(value)
    if as_float is None:
        return None
    try:
        return int(as_float)
    except Exception:
        return None


def _collect_numeric(events: list[dict[str, Any]], key: str) -> list[float]:
    values: list[float] = []
    for event in events:
        value = _to_float(event.get(key))
        if value is not None:
            values.append(value)
    return values


def _percentile(sorted_values: list[float], pct: float) -> float | None:
    if not sorted_values:
        return None
    if len(sorted_values) == 1:
        return sorted_values[0]
    rank = (len(sorted_values) - 1) * (pct / 100.0)
    low = int(math.floor(rank))
    high = int(math.ceil(rank))
    if low == high:
        return sorted_values[low]
    weight = rank - low
    return sorted_values[low] * (1.0 - weight) + sorted_values[high] * weight


def _stats(values: list[float]) -> dict[str, Any]:
    if not values:
        return {"count": 0, "avg": None, "median": None, "p95": None, "p99": None}
    sorted_values = sorted(values)
    return {
        "count": len(values),
        "avg": sum(values) / len(values),
        "median": statistics.median(sorted_values),
        "p95": _percentile(sorted_values, 95),
        "p99": _percentile(sorted_values, 99),
    }


def _format_num(value: Any) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, (int, float)):
        if abs(float(value) - round(float(value))) < 1e-9:
            return str(int(round(float(value))))
        return f"{float(value):.2f}"
    return str(value)


def _print_stats_line(label: str, stats: dict[str, Any]) -> None:
    print(
        f"- {label}: n={_format_num(stats.get('count'))} "
        f"avg={_format_num(stats.get('avg'))} "
        f"median={_format_num(stats.get('median'))} "
        f"p95={_format_num(stats.get('p95'))} "
        f"p99={_format_num(stats.get('p99'))}"
    )


def _build_tts_summary(events: list[dict[str, Any]]) -> dict[str, Any]:
    stage_counts = Counter(str(event.get("stage") or "unknown") for event in events)
    cache_hit_count = sum(1 for event in events if event.get("cache_hit") is True)
    cache_miss_count = sum(1 for event in events if event.get("cache_miss") is True)
    endpoint_events = [event for event in events if str(event.get("stage") or "") in TTS_ENDPOINT_STAGES]

    polling_values = _collect_numeric(events, "polling_attempt_count")
    cache_hit_endpoint_durations = _collect_numeric(
        [event for event in endpoint_events if event.get("cache_hit") is True],
        "duration_ms",
    )
    cache_miss_endpoint_durations = _collect_numeric(
        [event for event in endpoint_events if event.get("cache_miss") is True],
        "duration_ms",
    )

    return {
        "total_sample_count": len(events),
        "count_by_stage": dict(stage_counts),
        "cache_hit_count": cache_hit_count,
        "cache_miss_count": cache_miss_count,
        "duration_metrics": {
            "total_endpoint_duration_ms": _stats(_collect_numeric(endpoint_events, "duration_ms")),
            "db_lookup_duration_ms": _stats(_collect_numeric(events, "db_lookup_duration_ms")),
            "external_tts_provider_duration_ms": _stats(_collect_numeric(events, "external_tts_provider_duration_ms")),
            "storage_upload_duration_ms": _stats(_collect_numeric(events, "storage_upload_duration_ms")),
            "runner_start_delay_ms": _stats(_collect_numeric(events, "runner_start_delay_ms")),
        },
        "avg_polling_attempt_count": (sum(polling_values) / len(polling_values)) if polling_values else None,
        "cache_hit_endpoint_duration_ms": _stats(cache_hit_endpoint_durations),
        "cache_miss_endpoint_duration_ms": _stats(cache_miss_endpoint_durations),
    }


def _session_id_from_event(event: dict[str, Any]) -> int | None:
    for key in ("session_id", "check_id"):
        parsed = _to_int(event.get(key))
        if parsed is not None:
            return parsed
    return None


def _build_translation_summary(events: list[dict[str, Any]]) -> dict[str, Any]:
    stage_counts = Counter(str(event.get("stage") or "unknown") for event in events)
    session_ids: set[int] = set()
    items_by_session: dict[int, float] = {}
    terminal_outcome_counts: Counter[str] = Counter()

    for event in events:
        session_id = _session_id_from_event(event)
        if session_id is not None:
            session_ids.add(session_id)
            items_total = _to_float(event.get("items_total"))
            if items_total is not None:
                existing = items_by_session.get(session_id)
                if existing is None or items_total > existing:
                    items_by_session[session_id] = items_total
        if str(event.get("stage") or "") == "runner_finished":
            outcome = str(event.get("terminal_outcome") or "").strip().lower()
            if outcome in {"success", "partial", "error"}:
                terminal_outcome_counts[outcome] += 1

    polling_values = _collect_numeric(events, "status_polling_count")
    avg_items_per_session = (
        (sum(items_by_session.values()) / len(items_by_session))
        if items_by_session
        else None
    )

    start_events = [event for event in events if str(event.get("stage") or "") == "check_start_completed"]
    item_events = [event for event in events if str(event.get("stage") or "") == "item_processed"]

    return {
        "total_sessions_observed": len(session_ids),
        "total_item_processed_events": len(item_events),
        "count_by_stage": dict(stage_counts),
        "duration_metrics": {
            "start_endpoint_duration_ms": _stats(_collect_numeric(start_events, "duration_ms")),
            "runner_start_delay_ms": _stats(_collect_numeric(events, "runner_start_delay_ms")),
            "per_item_duration_ms": _stats(_collect_numeric(item_events, "per_item_duration_ms")),
            "db_update_duration_ms": _stats(_collect_numeric(events, "db_update_duration_ms")),
            "session_completion_duration_ms": _stats(_collect_numeric(events, "session_completion_duration_ms")),
        },
        "avg_items_per_session": avg_items_per_session,
        "avg_polling_count": (sum(polling_values) / len(polling_values)) if polling_values else None,
        "terminal_outcome_counts": {
            "success": int(terminal_outcome_counts.get("success", 0)),
            "partial": int(terminal_outcome_counts.get("partial", 0)),
            "error": int(terminal_outcome_counts.get("error", 0)),
        },
    }


def _print_tts_summary(summary: dict[str, Any]) -> None:
    print("=== TTS Flow Summary ===")
    print(f"- total sample count: {summary['total_sample_count']}")
    print(f"- count by stage: {summary['count_by_stage']}")
    print(f"- cache hit count: {summary['cache_hit_count']}")
    print(f"- cache miss count: {summary['cache_miss_count']}")
    _print_stats_line("total endpoint duration (ms)", summary["duration_metrics"]["total_endpoint_duration_ms"])
    _print_stats_line("db lookup duration (ms)", summary["duration_metrics"]["db_lookup_duration_ms"])
    _print_stats_line("external TTS provider duration (ms)", summary["duration_metrics"]["external_tts_provider_duration_ms"])
    _print_stats_line("storage/upload duration (ms)", summary["duration_metrics"]["storage_upload_duration_ms"])
    _print_stats_line("runner start delay (ms)", summary["duration_metrics"]["runner_start_delay_ms"])
    print(f"- avg polling attempt count: {_format_num(summary['avg_polling_attempt_count'])}")
    _print_stats_line("cache-hit endpoint duration (ms)", summary["cache_hit_endpoint_duration_ms"])
    _print_stats_line("cache-miss endpoint duration (ms)", summary["cache_miss_endpoint_duration_ms"])
    print()


def _print_translation_summary(summary: dict[str, Any]) -> None:
    print("=== Translation Check Flow Summary ===")
    print(f"- total sessions observed: {summary['total_sessions_observed']}")
    print(f"- total item_processed events: {summary['total_item_processed_events']}")
    print(f"- count by stage: {summary['count_by_stage']}")
    _print_stats_line("start endpoint duration (ms)", summary["duration_metrics"]["start_endpoint_duration_ms"])
    _print_stats_line("runner start delay (ms)", summary["duration_metrics"]["runner_start_delay_ms"])
    _print_stats_line("per-item duration (ms)", summary["duration_metrics"]["per_item_duration_ms"])
    _print_stats_line("DB update duration (ms)", summary["duration_metrics"]["db_update_duration_ms"])
    _print_stats_line("session completion duration (ms)", summary["duration_metrics"]["session_completion_duration_ms"])
    print(f"- avg items per session: {_format_num(summary['avg_items_per_session'])}")
    print(f"- avg polling count: {_format_num(summary['avg_polling_count'])}")
    print(f"- terminal outcomes: {summary['terminal_outcome_counts']}")
    print()


def main() -> int:
    args = _parse_args()
    all_events: list[dict[str, Any]] = []
    parsed_lines = 0
    for line in _iter_lines(args.paths):
        parsed_lines += 1
        event = _parse_event(line)
        if event:
            all_events.append(event)

    tts_events = [event for event in all_events if str(event.get("flow") or "") == "tts"]
    translation_events = [event for event in all_events if str(event.get("flow") or "") == "translation_check"]

    tts_summary = _build_tts_summary(tts_events)
    translation_summary = _build_translation_summary(translation_events)

    print(f"Parsed lines: {parsed_lines}")
    print(f"Matched observability events: {len(all_events)}")
    print()
    _print_tts_summary(tts_summary)
    _print_translation_summary(translation_summary)

    if args.json_out:
        output = {
            "meta": {
                "parsed_lines": parsed_lines,
                "matched_events": len(all_events),
            },
            "tts": tts_summary,
            "translation_check": translation_summary,
        }
        output_path = Path(args.json_out)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"Wrote JSON summary: {output_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
