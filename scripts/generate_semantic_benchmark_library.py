#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.database import (  # noqa: E402
    get_db_connection_context,
    list_semantic_benchmark_queue_candidates,
    require_semantic_audit_tables,
    update_semantic_benchmark_queue_item,
    upsert_semantic_benchmark_library_entry,
)
from backend.openai_manager import get_last_llm_usage, llm_execute  # noqa: E402
from backend.translation_workflow import _load_skill_catalog_with_cursor  # noqa: E402

PROMPT_VERSION = "semantic_benchmark_annotator_strict_v1"
TASK_NAME = "semantic_benchmark_annotator"
SYSTEM_INSTRUCTION_KEY = "semantic_benchmark_annotator_strict"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate semantic benchmark library entries from queued real production sentences.",
    )
    parser.add_argument("--source-lang", default="ru")
    parser.add_argument("--target-lang", default="de")
    parser.add_argument("--queue-status", default="pending")
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--case-id-prefix", default="auto")
    parser.add_argument("--output-json", default=None)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def _load_skill_catalog(target_lang: str) -> list[dict[str, Any]]:
    normalized_target_lang = str(target_lang or "de").strip().lower() or "de"
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            return _load_skill_catalog_with_cursor(
                cursor,
                target_lang=normalized_target_lang,
                authored_mastery_leaves_only=(normalized_target_lang == "de"),
            )


def _get_effective_model_name() -> str | None:
    task_env_suffix = TASK_NAME.upper()
    return (
        str(
            os.getenv(f"LLM_TASK_MODEL_{task_env_suffix}")
            or os.getenv(f"OPENAI_TASK_MODEL_{task_env_suffix}")
            or os.getenv("LLM_GATEWAY_MODEL")
            or os.getenv("OPENAI_MODEL")
            or ""
        ).strip()
        or None
    )


def _normalize_benchmark_payload(
    *,
    raw_payload: dict[str, Any],
    source_sentence: str,
    case_id: str,
    valid_skill_ids: set[str],
) -> dict[str, Any]:
    primary = str(raw_payload.get("expected_tested_primary") or "").strip()
    if not primary:
        raise ValueError("expected_tested_primary is missing")
    if primary not in valid_skill_ids:
        raise ValueError(f"expected_tested_primary is not in skill catalog: {primary}")

    secondaries: list[str] = []
    for item in list(raw_payload.get("expected_tested_secondary") or []):
        skill_id = str(item or "").strip()
        if not skill_id or skill_id == primary or skill_id in secondaries:
            continue
        if skill_id not in valid_skill_ids:
            raise ValueError(f"secondary skill is not in skill catalog: {skill_id}")
        secondaries.append(skill_id)
        if len(secondaries) >= 2:
            break

    confidence = str(raw_payload.get("benchmark_confidence") or "").strip().lower()
    if confidence not in {"high", "medium", "low"}:
        confidence = "medium"

    return {
        "case_id": str(raw_payload.get("case_id") or case_id).strip() or case_id,
        "source_sentence": str(raw_payload.get("source_sentence") or source_sentence).strip() or source_sentence,
        "expected_tested_primary": primary,
        "expected_tested_secondary": secondaries,
        "benchmark_confidence": confidence,
        "sentence_level_anchor": str(raw_payload.get("sentence_level_anchor") or "").strip() or None,
        "notes": str(raw_payload.get("notes") or "").strip() or None,
    }


async def _generate_single_benchmark(
    *,
    case_id: str,
    source_sentence: str,
    skill_catalog: list[dict[str, Any]],
    valid_skill_ids: set[str],
) -> tuple[dict[str, Any], dict[str, Any] | None, str]:
    user_message = json.dumps(
        {
            "case_id": case_id,
            "source_sentence": source_sentence,
            "skill_catalog": skill_catalog,
        },
        ensure_ascii=False,
    )
    raw_text = await llm_execute(
        task_name=TASK_NAME,
        system_instruction_key=SYSTEM_INSTRUCTION_KEY,
        user_message=user_message,
        poll_interval_seconds=1.0,
        responses_timeout_seconds=90.0,
        responses_only=True,
    )
    usage = get_last_llm_usage(reset=True)
    parsed = json.loads(raw_text)
    if not isinstance(parsed, dict):
        raise ValueError("benchmark annotator did not return a JSON object")
    normalized = _normalize_benchmark_payload(
        raw_payload=parsed,
        source_sentence=source_sentence,
        case_id=case_id,
        valid_skill_ids=valid_skill_ids,
    )
    return normalized, usage, raw_text


async def _run_generation(args: argparse.Namespace) -> dict[str, Any]:
    require_semantic_audit_tables()
    skill_catalog = _load_skill_catalog(args.target_lang)
    if not skill_catalog:
        raise RuntimeError(f"Skill catalog is empty for target_lang={args.target_lang!r}")
    valid_skill_ids = {
        str(item.get("skill_id") or "").strip()
        for item in skill_catalog
        if str(item.get("skill_id") or "").strip()
    }
    queue_items = list_semantic_benchmark_queue_candidates(
        queue_status=args.queue_status,
        source_lang=args.source_lang,
        target_lang=args.target_lang,
        limit=args.limit,
    )

    results: list[dict[str, Any]] = []
    for item in queue_items:
        queue_id = int(item["id"])
        source_sentence = str(item.get("source_sentence") or "").strip()
        case_id = f"{str(args.case_id_prefix or 'auto').strip() or 'auto'}_{queue_id}"
        if args.dry_run:
            results.append(
                {
                    "queue_id": queue_id,
                    "case_id": case_id,
                    "source_sentence": source_sentence,
                    "status": "dry_run",
                    "request_preview": {
                        "case_id": case_id,
                        "source_sentence": source_sentence,
                        "skill_catalog_count": len(skill_catalog),
                    },
                }
            )
            continue

        try:
            benchmark_json, usage, raw_text = await _generate_single_benchmark(
                case_id=case_id,
                source_sentence=source_sentence,
                skill_catalog=skill_catalog,
                valid_skill_ids=valid_skill_ids,
            )
            metadata = {
                "queue_id": queue_id,
                "queue_priority": item.get("priority"),
                "sample_count": item.get("sample_count"),
                "recent_source_session_ids": item.get("recent_source_session_ids") or [],
                "recent_check_session_ids": item.get("recent_check_session_ids") or [],
                "prompt_version": PROMPT_VERSION,
                "llm_usage": usage or {},
                "llm_raw_text": raw_text,
            }
            library_entry = upsert_semantic_benchmark_library_entry(
                source_sentence=source_sentence,
                benchmark_json=benchmark_json,
                source_lang=args.source_lang,
                target_lang=args.target_lang,
                benchmark_status="ready",
                benchmark_confidence=benchmark_json.get("benchmark_confidence"),
                sentence_level_anchor=benchmark_json.get("sentence_level_anchor"),
                prompt_version=PROMPT_VERSION,
                llm_model=(usage or {}).get("model") or _get_effective_model_name(),
                notes=benchmark_json.get("notes"),
                metadata=metadata,
                approved=False,
            )
            queue_row = update_semantic_benchmark_queue_item(
                queue_id=queue_id,
                queue_status="ready",
                benchmark_id=int(library_entry["id"]) if library_entry else None,
                last_error=None,
                metadata={
                    "prompt_version": PROMPT_VERSION,
                    "benchmark_id": int(library_entry["id"]) if library_entry else None,
                    "benchmark_confidence": benchmark_json.get("benchmark_confidence"),
                },
            )
            results.append(
                {
                    "queue_id": queue_id,
                    "case_id": case_id,
                    "source_sentence": source_sentence,
                    "status": "ready",
                    "benchmark_id": int(library_entry["id"]) if library_entry else None,
                    "benchmark_json": benchmark_json,
                    "queue": queue_row,
                    "llm_usage": usage or {},
                }
            )
        except Exception as exc:
            queue_row = update_semantic_benchmark_queue_item(
                queue_id=queue_id,
                queue_status="failed",
                last_error=str(exc)[:500],
                metadata={
                    "prompt_version": PROMPT_VERSION,
                    "failed_case_id": case_id,
                },
            )
            results.append(
                {
                    "queue_id": queue_id,
                    "case_id": case_id,
                    "source_sentence": source_sentence,
                    "status": "failed",
                    "error": str(exc),
                    "queue": queue_row,
                }
            )

    return {
        "source_lang": args.source_lang,
        "target_lang": args.target_lang,
        "queue_status": args.queue_status,
        "prompt_version": PROMPT_VERSION,
        "llm_model": _get_effective_model_name(),
        "skill_catalog_count": len(skill_catalog),
        "processed_count": len(results),
        "ready_count": sum(1 for item in results if item.get("status") == "ready"),
        "failed_count": sum(1 for item in results if item.get("status") == "failed"),
        "dry_run_count": sum(1 for item in results if item.get("status") == "dry_run"),
        "items": results,
    }


def main() -> int:
    args = _parse_args()
    summary = asyncio.run(_run_generation(args))
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
