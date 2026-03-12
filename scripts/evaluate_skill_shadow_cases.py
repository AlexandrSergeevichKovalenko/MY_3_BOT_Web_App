#!/usr/bin/env python3
import argparse
import asyncio
import json
import os
import sys
import time
from collections import Counter, defaultdict
from contextlib import contextmanager
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.openai_manager import get_last_llm_usage, run_check_translation_multilang  # noqa: E402
from backend.translation_workflow import (  # noqa: E402
    _build_errored_skill_details,
    _get_language_taxonomy,
    _normalize_category_pairs,
    _parse_translation_feedback_payload,
    _score_band_label,
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Evaluate a fixed set of translation cases against the current shadow-skill pipeline inputs.",
    )
    parser.add_argument(
        "input_file",
        nargs="?",
        default="scripts/skill_shadow_eval_cases.json",
        help="JSON array of cases. Defaults to scripts/skill_shadow_eval_cases.json",
    )
    parser.add_argument(
        "--model",
        default=os.getenv("LLM_TASK_MODEL_CHECK_TRANSLATION_MULTILANG", "gpt-4.1-2025-04-14"),
        help="Model to use for run_check_translation_multilang.",
    )
    parser.add_argument(
        "--source-lang",
        default="ru",
        help="Default source language.",
    )
    parser.add_argument(
        "--target-lang",
        default="de",
        help="Default target language.",
    )
    parser.add_argument(
        "--output",
        default="skill_shadow_eval_results.json",
        help="Machine-readable output JSON path.",
    )
    return parser.parse_args()


@contextmanager
def _temporary_env(name: str, value: str):
    previous = os.environ.get(name)
    os.environ[name] = value
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop(name, None)
        else:
            os.environ[name] = previous


def _load_cases(path: str) -> list[dict[str, Any]]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError("Input file must contain a JSON array.")
    cases: list[dict[str, Any]] = []
    for index, item in enumerate(payload, start=1):
        if not isinstance(item, dict):
            raise ValueError(f"Item #{index} is not an object.")
        original_text = str(item.get("original_text") or "").strip()
        user_translation = str(item.get("user_translation") or "").strip()
        if not original_text or not user_translation:
            raise ValueError(f"Item #{index} must include non-empty original_text and user_translation.")
        cases.append(item)
    return cases


async def _evaluate_case(
    *,
    item: dict[str, Any],
    model_name: str,
    default_source_lang: str,
    default_target_lang: str,
) -> dict[str, Any]:
    source_lang = str(item.get("source_lang") or default_source_lang).strip() or default_source_lang
    target_lang = str(item.get("target_lang") or default_target_lang).strip() or default_target_lang
    language_categories, language_subcategories, _ = _get_language_taxonomy(target_lang)

    started_at = time.perf_counter()
    with _temporary_env("LLM_TASK_MODEL_CHECK_TRANSLATION_MULTILANG", model_name):
        feedback = await run_check_translation_multilang(
            original_text=str(item["original_text"]).strip(),
            user_translation=str(item["user_translation"]).strip(),
            source_lang=source_lang,
            target_lang=target_lang,
            allowed_categories=language_categories,
            allowed_subcategories=language_subcategories,
        )
    elapsed_ms = (time.perf_counter() - started_at) * 1000.0
    usage = get_last_llm_usage(reset=True) or {}

    parsed = _parse_translation_feedback_payload(
        feedback,
        language_categories=language_categories,
        language_subcategories=language_subcategories,
    )
    normalized_pairs = _normalize_category_pairs(
        list(parsed["categories"]),
        list(parsed["subcategories"]),
        target_lang=target_lang,
        fallback_to_other=False,
    )
    errored_details = _build_errored_skill_details(
        error_pairs=normalized_pairs,
        target_lang=target_lang,
    )
    mapped_skills = [
        {
            "skill_id": skill_id,
            "map_weight": float(details.get("map_weight") or 0.0),
            "error_pairs": list(details.get("error_pairs") or []),
        }
        for skill_id, details in sorted(errored_details.items(), key=lambda item: item[0])
    ]
    score = int(parsed["score"]) if parsed.get("score") is not None else 0

    return {
        "id": item.get("id"),
        "set_name": item.get("set_name") or "default",
        "source_lang": source_lang,
        "target_lang": target_lang,
        "original_text": item["original_text"],
        "user_translation": item["user_translation"],
        "score": score,
        "score_band": _score_band_label(score),
        "correct_translation": parsed["correct_translation"],
        "categories": list(parsed["categories"]),
        "subcategories": list(parsed["subcategories"]),
        "normalized_error_pairs": [list(pair) for pair in normalized_pairs],
        "mapped_skills": mapped_skills,
        "mapped_skill_ids": [item["skill_id"] for item in mapped_skills],
        "feedback": feedback,
        "elapsed_ms": round(elapsed_ms, 3),
        "usage": usage,
    }


def _build_summary(results: list[dict[str, Any]]) -> dict[str, Any]:
    by_set: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in results:
        by_set[str(item.get("set_name") or "default")].append(item)

    sets_summary: dict[str, Any] = {}
    all_scores = [int(item["score"]) for item in results]
    all_skills = Counter(skill_id for item in results for skill_id in item.get("mapped_skill_ids", []))
    all_pairs = Counter(tuple(pair) for item in results for pair in item.get("normalized_error_pairs", []))

    for set_name, items in sorted(by_set.items()):
        scores = [int(item["score"]) for item in items]
        score_bands = Counter(str(item.get("score_band") or "") for item in items)
        mapped_skill_counts = Counter(skill_id for item in items for skill_id in item.get("mapped_skill_ids", []))
        sets_summary[set_name] = {
            "count": len(items),
            "avg_score": round(sum(scores) / len(scores), 2) if scores else None,
            "min_score": min(scores) if scores else None,
            "max_score": max(scores) if scores else None,
            "score_bands": dict(score_bands),
            "cases_with_no_mapped_skills": [
                str(item.get("id") or "")
                for item in items
                if not item.get("mapped_skill_ids")
            ],
            "top_mapped_skills": [
                {"skill_id": skill_id, "count": count}
                for skill_id, count in mapped_skill_counts.most_common(10)
            ],
        }

    return {
        "case_count": len(results),
        "avg_score": round(sum(all_scores) / len(all_scores), 2) if all_scores else None,
        "min_score": min(all_scores) if all_scores else None,
        "max_score": max(all_scores) if all_scores else None,
        "cases_with_no_mapped_skills": [
            str(item.get("id") or "")
            for item in results
            if not item.get("mapped_skill_ids")
        ],
        "top_mapped_skills": [
            {"skill_id": skill_id, "count": count}
            for skill_id, count in all_skills.most_common(15)
        ],
        "top_error_pairs": [
            {"pair": [pair[0], pair[1]], "count": count}
            for pair, count in all_pairs.most_common(15)
        ],
        "sets": sets_summary,
    }


def _print_console_summary(summary: dict[str, Any], results: list[dict[str, Any]]) -> None:
    print(f"Cases: {summary['case_count']}")
    print(f"Average score: {summary['avg_score']}")
    print(f"Cases without mapped skills: {', '.join(summary['cases_with_no_mapped_skills']) or 'none'}")
    for set_name, data in (summary.get("sets") or {}).items():
        print(f"\n[{set_name}] avg={data['avg_score']} min={data['min_score']} max={data['max_score']}")
        print(f"score bands: {data['score_bands']}")
        if data["cases_with_no_mapped_skills"]:
            print(f"no mapped skills: {', '.join(data['cases_with_no_mapped_skills'])}")
    print("\nPer case:")
    for item in results:
        print(
            f"- {item['id']}: score={item['score']} band={item['score_band']} "
            f"pairs={len(item['normalized_error_pairs'])} skills={', '.join(item['mapped_skill_ids']) or 'none'}"
        )


async def _run(args: argparse.Namespace) -> dict[str, Any]:
    cases = _load_cases(args.input_file)
    results: list[dict[str, Any]] = []
    for item in cases:
        results.append(
            await _evaluate_case(
                item=item,
                model_name=args.model,
                default_source_lang=args.source_lang,
                default_target_lang=args.target_lang,
            )
        )
    summary = _build_summary(results)
    return {
        "input_file": str(Path(args.input_file).resolve()),
        "model": args.model,
        "summary": summary,
        "results": results,
    }


def main() -> int:
    args = _parse_args()
    payload = asyncio.run(_run(args))
    output_path = Path(args.output)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    _print_console_summary(payload["summary"], payload["results"])
    print(f"\nSaved to {output_path.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
