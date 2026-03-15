#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.database import (  # noqa: E402
    create_semantic_audit_run,
    enqueue_semantic_benchmark_candidate,
    finalize_semantic_audit_run,
    get_semantic_benchmark_library_entry,
    list_recent_unique_translation_check_sentences,
    replace_semantic_audit_case_results,
    require_semantic_audit_tables,
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a periodic semantic audit from benchmark library + fresh production attempts.",
    )
    parser.add_argument("--source-lang", default="ru")
    parser.add_argument("--target-lang", default="de")
    parser.add_argument("--user-id", type=int, default=117649764)
    parser.add_argument("--all-users", action="store_true")
    parser.add_argument("--days-back", type=int, default=7)
    parser.add_argument("--period-start", default=None, help="ISO datetime in UTC, overrides --days-back")
    parser.add_argument("--period-end", default=None, help="ISO datetime in UTC, defaults to now")
    parser.add_argument("--limit", type=int, default=None, help="Optional cap for unique source sentences in the period")
    parser.add_argument("--min-attempts", type=int, default=1)
    parser.add_argument("--run-scope", default="weekly")
    parser.add_argument("--local-replay-targeting", action="store_true")
    parser.add_argument("--enqueue-missing", action="store_true")
    parser.add_argument("--output-json", default="semantic_weekly_audit_report.json")
    parser.add_argument("--output-md", default="")
    return parser.parse_args()


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = str(value).strip()
    if not normalized:
        return None
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _build_case_from_library_entry(entry: dict[str, Any]) -> dict[str, Any] | None:
    benchmark_json = dict(entry.get("benchmark_json") or {})
    source_sentence = str(entry.get("source_sentence") or benchmark_json.get("source_sentence") or "").strip()
    expected_primary = str(benchmark_json.get("expected_tested_primary") or "").strip()
    if not source_sentence or not expected_primary:
        return None
    secondaries = [
        str(item).strip()
        for item in list(benchmark_json.get("expected_tested_secondary") or [])
        if str(item).strip()
    ][:2]
    return {
        "case_id": str(benchmark_json.get("case_id") or entry.get("source_sentence_hash") or "").strip() or None,
        "source_sentence": source_sentence,
        "expected_tested_primary": expected_primary,
        "expected_tested_secondary": secondaries,
        "expected_errored_skills": [],
        "expected_outcome_type": "depends_on_attempt_context",
        "notes": str(benchmark_json.get("notes") or entry.get("notes") or "").strip() or None,
    }


def _derive_classification(
    *,
    primary_match: bool | None,
    outcome_match: bool | None,
    secondary_overlap: float | None,
) -> str:
    if primary_match and outcome_match and (secondary_overlap is None or secondary_overlap >= 0.5):
        return "clearly_correct"
    if primary_match is False:
        return "likely_incorrect"
    return "questionable"


def _flatten_case_results(per_case: list[dict[str, Any]]) -> list[dict[str, Any]]:
    flattened: list[dict[str, Any]] = []
    for case in list(per_case or []):
        source_sentence = str(case.get("source_sentence") or "").strip()
        if not source_sentence:
            continue
        expected_primary = str(case.get("expected_tested_primary") or "").strip() or None
        expected_secondaries = [
            str(item).strip()
            for item in list(case.get("expected_tested_secondary") or [])
            if str(item).strip()
        ]
        for attempt in list(case.get("attempts") or []):
            metadata = dict(attempt.get("metadata") or {})
            secondary_overlap = attempt.get("secondary_skill_overlap_score")
            try:
                secondary_overlap_value = float(secondary_overlap) if secondary_overlap is not None else None
            except Exception:
                secondary_overlap_value = None
            flattened.append(
                {
                    "case_id": str(case.get("case_id") or "").strip() or None,
                    "source_sentence": source_sentence,
                    "source_session_id": str(metadata.get("session_id") or "").strip() or None,
                    "check_session_id": None,
                    "sentence_id": metadata.get("sentence_id"),
                    "benchmark_json": {
                        "case_id": case.get("case_id"),
                        "source_sentence": source_sentence,
                        "expected_tested_primary": expected_primary,
                        "expected_tested_secondary": expected_secondaries,
                        "expected_outcome_type": case.get("expected_outcome_type"),
                        "notes": case.get("notes"),
                    },
                    "expected_tested_primary": expected_primary,
                    "expected_tested_secondary": expected_secondaries,
                    "expected_outcome_type": str(case.get("expected_outcome_type") or "").strip() or None,
                    "actual_tested_primary": str(attempt.get("actual_tested_primary") or "").strip() or None,
                    "actual_tested_secondary": [
                        str(item).strip()
                        for item in list(attempt.get("actual_tested_secondary") or [])
                        if str(item).strip()
                    ],
                    "actual_errored_skills": [
                        str(item).strip()
                        for item in list(attempt.get("actual_errored_skills") or [])
                        if str(item).strip()
                    ],
                    "actual_outcome_type": str(attempt.get("actual_outcome_type") or "").strip() or None,
                    "primary_match": attempt.get("primary_match"),
                    "secondary_skill_overlap": secondary_overlap_value,
                    "outcome_match": attempt.get("outcome_match"),
                    "classification": _derive_classification(
                        primary_match=attempt.get("primary_match"),
                        outcome_match=attempt.get("outcome_match"),
                        secondary_overlap=secondary_overlap_value,
                    ),
                    "metadata": {
                        **metadata,
                        "attempt_index": attempt.get("attempt_index"),
                        "expected_outcome_type_resolved": attempt.get("expected_outcome_type_resolved"),
                        "anchor_analysis": attempt.get("anchor_analysis") or {},
                    },
                }
            )
    return flattened


def main() -> int:
    args = _parse_args()
    require_semantic_audit_tables()

    output_json_path = Path(args.output_json)
    output_md_path = Path(args.output_md) if args.output_md else output_json_path.with_suffix(".md")
    benchmark_cases_path = output_json_path.with_name(output_json_path.stem + ".benchmark_cases.json")
    evaluator_json_path = output_json_path.with_name(output_json_path.stem + ".evaluator.json")
    evaluator_md_path = output_json_path.with_name(output_json_path.stem + ".evaluator.md")
    output_json_path.parent.mkdir(parents=True, exist_ok=True)

    period_end = _parse_datetime(args.period_end) or datetime.now(timezone.utc)
    period_start = _parse_datetime(args.period_start) or (period_end - timedelta(days=max(1, int(args.days_back or 7))))

    rows = list_recent_unique_translation_check_sentences(
        period_start=period_start,
        period_end=period_end,
        source_lang=args.source_lang,
        target_lang=args.target_lang,
        user_id=None if bool(args.all_users) else args.user_id,
        limit=args.limit,
    )
    eligible_rows = [
        row
        for row in rows
        if int(row.get("attempts_count") or 0) >= max(1, int(args.min_attempts or 1))
    ]

    benchmark_cases: list[dict[str, Any]] = []
    benchmarked_rows = 0
    missing_rows = 0
    source_session_ids: list[str] = []
    seen_session_ids: set[str] = set()
    for row in eligible_rows:
        source_sentence = str(row.get("source_sentence") or "").strip()
        if not source_sentence:
            continue
        library_entry = get_semantic_benchmark_library_entry(
            source_sentence=source_sentence,
            source_lang=args.source_lang,
            target_lang=args.target_lang,
        )
        case = _build_case_from_library_entry(library_entry) if library_entry else None
        if case:
            benchmark_cases.append(case)
            benchmarked_rows += 1
            for session_id in list(row.get("source_session_ids") or []):
                normalized_session_id = str(session_id).strip()
                if normalized_session_id and normalized_session_id not in seen_session_ids:
                    seen_session_ids.add(normalized_session_id)
                    source_session_ids.append(normalized_session_id)
            continue
        missing_rows += 1
        if args.enqueue_missing:
            enqueue_semantic_benchmark_candidate(
                source_sentence=source_sentence,
                source_lang=args.source_lang,
                target_lang=args.target_lang,
                first_seen_at=_parse_datetime(row.get("first_seen_at")),
                last_seen_at=_parse_datetime(row.get("last_seen_at")),
                sample_count=int(row.get("attempts_count") or 1),
                priority=float(row.get("attempts_count") or 0),
                recent_source_session_ids=list(row.get("source_session_ids") or []),
                recent_check_session_ids=list(row.get("check_session_ids") or []),
                metadata={
                    "build_source": "scripts.run_semantic_weekly_audit",
                    "period_start": period_start.isoformat(),
                    "period_end": period_end.isoformat(),
                    "user_id": None if bool(args.all_users) else args.user_id,
                },
            )

    user_scope_key = "all" if bool(args.all_users) else str(int(args.user_id))
    run_key = (
        f"{str(args.run_scope or 'weekly').strip().lower()}:"
        f"{args.source_lang}:{args.target_lang}:{user_scope_key}:"
        f"{period_start.date().isoformat()}:{period_end.date().isoformat()}"
    )
    audit_run = create_semantic_audit_run(
        run_scope=str(args.run_scope or "weekly").strip().lower(),
        source_lang=args.source_lang,
        target_lang=args.target_lang,
        run_key=run_key,
        period_start=period_start.date(),
        period_end=period_end.date(),
        sample_size=len(eligible_rows),
        benchmark_case_count=len(benchmark_cases),
        metadata={
            "user_id": None if bool(args.all_users) else args.user_id,
            "all_users": bool(args.all_users),
            "source_session_ids": source_session_ids,
            "enqueue_missing": bool(args.enqueue_missing),
            "local_replay_targeting": bool(args.local_replay_targeting),
        },
    )
    if not audit_run:
        raise RuntimeError("Failed to create semantic audit run")

    try:
        benchmark_cases_payload = {
            "defaults": {
                "mode": "db-backed",
                "language_pair": {
                    "source_lang": args.source_lang,
                    "target_lang": args.target_lang,
                },
                "notes": f"Auto-built semantic audit benchmark for {period_start.isoformat()}..{period_end.isoformat()}",
            },
            "cases": benchmark_cases,
        }
        benchmark_cases_path.write_text(
            json.dumps(benchmark_cases_payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        if benchmark_cases and source_session_ids:
            evaluator_cmd = [
                sys.executable,
                "scripts/evaluate_semantic_skill_benchmark.py",
                str(benchmark_cases_path),
                "--mode",
                "db-backed",
                "--user-id",
                str(int(args.user_id)),
                "--source-lang",
                str(args.source_lang),
                "--target-lang",
                str(args.target_lang),
                "--recent-limit",
                str(max(1, len(source_session_ids))),
                "--source-session-ids",
                ",".join(source_session_ids),
                "--output-json",
                str(evaluator_json_path),
                "--output-md",
                str(evaluator_md_path),
            ]
            if args.all_users:
                evaluator_cmd.append("--all-users")
            if args.local_replay_targeting:
                evaluator_cmd.append("--local-replay-targeting")
            subprocess.run(
                evaluator_cmd,
                check=True,
                cwd=str(REPO_ROOT),
                capture_output=True,
                text=True,
            )
            evaluator_result = json.loads(evaluator_json_path.read_text(encoding="utf-8"))
            evaluator_metrics = dict(evaluator_result.get("metrics") or {})
            evaluator_per_case = list(evaluator_result.get("per_case") or [])
            summary_markdown = evaluator_md_path.read_text(encoding="utf-8")
        else:
            evaluator_result = {
                "meta": {
                    "mode": "db-backed",
                    "input_file": str(benchmark_cases_path.resolve()),
                },
                "per_case": [],
                "metrics": {
                    "evaluated_attempt_count": 0,
                    "primary_skill_accuracy": None,
                    "secondary_skill_overlap": None,
                    "outcome_classification_accuracy": None,
                    "noise_primary_overpromotion_count": 0,
                    "noise_primary_overpromotion_rate": None,
                    "missed_sentence_level_anchor_count": 0,
                    "missed_sentence_level_anchor_rate": None,
                },
            }
            evaluator_metrics = dict(evaluator_result["metrics"])
            evaluator_per_case = []
            summary_markdown = (
                "# Semantic Weekly Audit\n\n"
                "No benchmark-covered sentences were available for this period.\n"
            )
            evaluator_json_path.write_text(json.dumps(evaluator_result, ensure_ascii=False, indent=2), encoding="utf-8")
            evaluator_md_path.write_text(summary_markdown, encoding="utf-8")

        case_result_count = replace_semantic_audit_case_results(
            audit_run_id=int(audit_run["id"]),
            case_results=_flatten_case_results(evaluator_per_case),
            source_lang=args.source_lang,
            target_lang=args.target_lang,
        )
        summary_json = {
            "audit_run_id": int(audit_run["id"]),
            "run_key": audit_run.get("run_key"),
            "period_start": period_start.isoformat(),
            "period_end": period_end.isoformat(),
            "eligible_sentence_count": len(eligible_rows),
            "benchmark_case_count": len(benchmark_cases),
            "benchmarked_rows": benchmarked_rows,
            "missing_benchmark_rows": missing_rows,
            "source_session_count": len(source_session_ids),
            "case_result_count": case_result_count,
            "artifacts": {
                "benchmark_cases_json": str(benchmark_cases_path.resolve()),
                "evaluator_json": str(evaluator_json_path.resolve()),
                "evaluator_md": str(evaluator_md_path.resolve()),
            },
        }
        final_output = {
            "audit_run": {
                **audit_run,
                "run_status": "done",
                "metrics_json": evaluator_metrics,
                "summary_json": summary_json,
                "summary_markdown": str(output_md_path.resolve()),
                "completed_at": datetime.now(timezone.utc).isoformat(),
            },
            "summary": summary_json,
            "metrics": evaluator_metrics,
            "evaluator_meta": evaluator_result.get("meta") or {},
        }
        output_json_path.write_text(json.dumps(final_output, ensure_ascii=False, indent=2), encoding="utf-8")
        output_md_path.write_text(summary_markdown, encoding="utf-8")
        finalize_semantic_audit_run(
            audit_run_id=int(audit_run["id"]),
            run_status="done",
            metrics_json=evaluator_metrics,
            summary_json=summary_json,
            summary_markdown=summary_markdown,
            delivery_status="pending",
            last_error=None,
        )
    except Exception as exc:
        finalize_semantic_audit_run(
            audit_run_id=int(audit_run["id"]),
            run_status="failed",
            metrics_json={},
            summary_json={
                "period_start": period_start.isoformat(),
                "period_end": period_end.isoformat(),
            },
            summary_markdown=None,
            delivery_status="pending",
            last_error=str(exc)[:1000],
        )
        raise

    print(output_json_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
