#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.database import (
    enqueue_semantic_benchmark_candidate,
    list_recent_unique_translation_check_sentences,
    require_semantic_audit_tables,
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build semantic benchmark queue from recent real translation-check sentences.",
    )
    parser.add_argument("--source-lang", default="ru")
    parser.add_argument("--target-lang", default="de")
    parser.add_argument("--user-id", type=int, default=None)
    parser.add_argument("--days-back", type=int, default=7)
    parser.add_argument("--period-start", default=None, help="ISO datetime in UTC, overrides --days-back")
    parser.add_argument("--period-end", default=None, help="ISO datetime in UTC, defaults to now")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--min-attempts", type=int, default=1)
    parser.add_argument("--output-json", default=None)
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


def main() -> int:
    args = _parse_args()
    require_semantic_audit_tables()

    period_end = _parse_datetime(args.period_end) or datetime.now(timezone.utc)
    period_start = _parse_datetime(args.period_start) or (period_end - timedelta(days=max(1, int(args.days_back or 7))))

    rows = list_recent_unique_translation_check_sentences(
        period_start=period_start,
        period_end=period_end,
        source_lang=args.source_lang,
        target_lang=args.target_lang,
        user_id=args.user_id,
        limit=args.limit,
    )

    filtered_rows = [
        row
        for row in rows
        if int(row.get("attempts_count") or 0) >= max(1, int(args.min_attempts or 1))
    ]

    queue_results = []
    for row in filtered_rows:
        queue_row = enqueue_semantic_benchmark_candidate(
            source_sentence=str(row.get("source_sentence") or ""),
            source_lang=str(row.get("source_lang") or args.source_lang),
            target_lang=str(row.get("target_lang") or args.target_lang),
            first_seen_at=_parse_datetime(row.get("first_seen_at")),
            last_seen_at=_parse_datetime(row.get("last_seen_at")),
            sample_count=int(row.get("attempts_count") or 1),
            priority=float(row.get("attempts_count") or 0),
            recent_source_session_ids=list(row.get("source_session_ids") or []),
            recent_check_session_ids=list(row.get("check_session_ids") or []),
            metadata={
                "build_source": "scripts.build_semantic_benchmark_queue",
                "period_start": period_start.isoformat(),
                "period_end": period_end.isoformat(),
                "user_id": args.user_id,
            },
        )
        queue_results.append(
            {
                **row,
                "queue": queue_row,
            }
        )

    summary = {
        "source_lang": args.source_lang,
        "target_lang": args.target_lang,
        "user_id": args.user_id,
        "period_start": period_start.isoformat(),
        "period_end": period_end.isoformat(),
        "unique_sentence_count": len(rows),
        "eligible_sentence_count": len(filtered_rows),
        "queued_pending_count": sum(
            1 for item in queue_results if str(((item.get("queue") or {}).get("queue_status") or "")).strip() == "pending"
        ),
        "queued_ready_count": sum(
            1 for item in queue_results if str(((item.get("queue") or {}).get("queue_status") or "")).strip() == "ready"
        ),
        "items": queue_results,
    }

    if args.output_json:
        output_path = Path(args.output_json)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
        print(output_path)
    else:
        print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
