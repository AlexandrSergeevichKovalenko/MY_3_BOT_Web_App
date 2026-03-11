#!/usr/bin/env python3
import argparse
import asyncio
import json
import os
import sys
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.openai_manager import get_last_llm_usage, run_check_translation_multilang
from backend.translation_workflow import _get_language_taxonomy, _parse_translation_feedback_payload


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare check_translation_multilang outputs for the same items across two models.",
    )
    parser.add_argument(
        "input_file",
        help="Path to a JSON file containing an array of test items.",
    )
    parser.add_argument(
        "--strong-model",
        default="gpt-4.1-2025-04-14",
        help="Model used for the stronger-model baseline path.",
    )
    parser.add_argument(
        "--mini-model",
        default="gpt-4.1-mini",
        help="Model used for the faster mini-model path.",
    )
    parser.add_argument(
        "--source-lang",
        default="ru",
        help="Default source language for items that do not specify source_lang.",
    )
    parser.add_argument(
        "--target-lang",
        default="de",
        help="Default target language for items that do not specify target_lang.",
    )
    parser.add_argument(
        "--output",
        default="translation_model_compare_results.json",
        help="Where to write the machine-readable comparison results.",
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
        raise ValueError("Input file must contain a JSON array of test items.")
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


def _normalize_text_set(values: list[str]) -> set[str]:
    return {str(value or "").strip().lower() for value in values if str(value or "").strip()}


def _jaccard(left: list[str], right: list[str]) -> float | None:
    left_set = _normalize_text_set(left)
    right_set = _normalize_text_set(right)
    if not left_set and not right_set:
        return 1.0
    union = left_set | right_set
    if not union:
        return None
    return len(left_set & right_set) / len(union)


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
    return {
        "model": model_name,
        "source_lang": source_lang,
        "target_lang": target_lang,
        "feedback": feedback,
        "feedback_chars": len(feedback),
        "score": parsed["score"],
        "score_str": parsed["score_str"],
        "correct_translation": parsed["correct_translation"],
        "categories": list(parsed["categories"]),
        "subcategories": list(parsed["subcategories"]),
        "category_count": len(parsed["categories"]),
        "subcategory_count": len(parsed["subcategories"]),
        "missing_score": not bool(parsed["score_str"]),
        "non_numeric_score": bool(parsed["score_str"]) and not str(parsed["score_str"]).isdigit(),
        "missing_correct_translation": not bool(parsed["correct_translation"]),
        "elapsed_ms": elapsed_ms,
        "usage": usage,
        "gateway": usage.get("gateway"),
        "usage_model": usage.get("model"),
        "prompt_tokens": usage.get("prompt_tokens"),
        "completion_tokens": usage.get("completion_tokens"),
        "total_tokens": usage.get("total_tokens"),
    }


async def _run_compare(args: argparse.Namespace) -> dict[str, Any]:
    cases = _load_cases(args.input_file)
    per_case: list[dict[str, Any]] = []

    for index, item in enumerate(cases, start=1):
        strong = await _evaluate_case(
            item=item,
            model_name=args.strong_model,
            default_source_lang=args.source_lang,
            default_target_lang=args.target_lang,
        )
        mini = await _evaluate_case(
            item=item,
            model_name=args.mini_model,
            default_source_lang=args.source_lang,
            default_target_lang=args.target_lang,
        )
        strong_score = strong.get("score")
        mini_score = mini.get("score")
        score_delta = None
        abs_score_delta = None
        if strong_score is not None and mini_score is not None:
            score_delta = int(mini_score) - int(strong_score)
            abs_score_delta = abs(score_delta)
        strong_elapsed_ms = float(strong.get("elapsed_ms") or 0.0)
        mini_elapsed_ms = float(mini.get("elapsed_ms") or 0.0)
        latency_delta_ms = mini_elapsed_ms - strong_elapsed_ms
        latency_percent_delta = (latency_delta_ms / strong_elapsed_ms * 100.0) if strong_elapsed_ms > 0 else None

        per_case.append(
            {
                "index": index,
                "case_id": item.get("id") or item.get("case_id") or index,
                "original_text": item["original_text"],
                "user_translation": item["user_translation"],
                "strong": strong,
                "mini": mini,
                "diff": {
                    "score_delta": score_delta,
                    "abs_score_delta": abs_score_delta,
                    "latency_delta_ms": latency_delta_ms,
                    "latency_percent_delta": latency_percent_delta,
                    "category_jaccard": _jaccard(strong["categories"], mini["categories"]),
                    "subcategory_jaccard": _jaccard(strong["subcategories"], mini["subcategories"]),
                    "strong_category_count": len(strong["categories"]),
                    "mini_category_count": len(mini["categories"]),
                    "strong_subcategory_count": len(strong["subcategories"]),
                    "mini_subcategory_count": len(mini["subcategories"]),
                    "correct_translation_equal": (
                        str(strong.get("correct_translation") or "").strip()
                        == str(mini.get("correct_translation") or "").strip()
                    ),
                },
            }
        )

    score_deltas = [item["diff"]["score_delta"] for item in per_case if item["diff"]["score_delta"] is not None]
    abs_score_deltas = [item["diff"]["abs_score_delta"] for item in per_case if item["diff"]["abs_score_delta"] is not None]
    latency_deltas = [item["diff"]["latency_delta_ms"] for item in per_case if item["diff"]["latency_delta_ms"] is not None]
    strong_latencies = [float(item["strong"]["elapsed_ms"]) for item in per_case]
    mini_latencies = [float(item["mini"]["elapsed_ms"]) for item in per_case]
    category_jaccards = [item["diff"]["category_jaccard"] for item in per_case if item["diff"]["category_jaccard"] is not None]
    subcategory_jaccards = [item["diff"]["subcategory_jaccard"] for item in per_case if item["diff"]["subcategory_jaccard"] is not None]
    strong_total_tokens = [int(item["strong"]["total_tokens"]) for item in per_case if item["strong"]["total_tokens"] is not None]
    mini_total_tokens = [int(item["mini"]["total_tokens"]) for item in per_case if item["mini"]["total_tokens"] is not None]
    strong_feedback_chars = [int(item["strong"]["feedback_chars"]) for item in per_case]
    mini_feedback_chars = [int(item["mini"]["feedback_chars"]) for item in per_case]
    strong_category_counts = [int(item["strong"]["category_count"]) for item in per_case]
    mini_category_counts = [int(item["mini"]["category_count"]) for item in per_case]
    strong_subcategory_counts = [int(item["strong"]["subcategory_count"]) for item in per_case]
    mini_subcategory_counts = [int(item["mini"]["subcategory_count"]) for item in per_case]

    summary = {
        "input_file": str(Path(args.input_file).resolve()),
        "case_count": len(per_case),
        "strong_model": args.strong_model,
        "mini_model": args.mini_model,
        "avg_strong_elapsed_ms": (sum(strong_latencies) / len(strong_latencies)) if strong_latencies else None,
        "avg_mini_elapsed_ms": (sum(mini_latencies) / len(mini_latencies)) if mini_latencies else None,
        "avg_latency_delta_ms_mini_minus_strong": (sum(latency_deltas) / len(latency_deltas)) if latency_deltas else None,
        "avg_strong_feedback_chars": (sum(strong_feedback_chars) / len(strong_feedback_chars)) if strong_feedback_chars else None,
        "avg_mini_feedback_chars": (sum(mini_feedback_chars) / len(mini_feedback_chars)) if mini_feedback_chars else None,
        "avg_strong_total_tokens": (sum(strong_total_tokens) / len(strong_total_tokens)) if strong_total_tokens else None,
        "avg_mini_total_tokens": (sum(mini_total_tokens) / len(mini_total_tokens)) if mini_total_tokens else None,
        "avg_strong_category_count": (sum(strong_category_counts) / len(strong_category_counts)) if strong_category_counts else None,
        "avg_mini_category_count": (sum(mini_category_counts) / len(mini_category_counts)) if mini_category_counts else None,
        "avg_strong_subcategory_count": (sum(strong_subcategory_counts) / len(strong_subcategory_counts)) if strong_subcategory_counts else None,
        "avg_mini_subcategory_count": (sum(mini_subcategory_counts) / len(mini_subcategory_counts)) if mini_subcategory_counts else None,
        "avg_score_delta_mini_minus_strong": (sum(score_deltas) / len(score_deltas)) if score_deltas else None,
        "avg_abs_score_delta": (sum(abs_score_deltas) / len(abs_score_deltas)) if abs_score_deltas else None,
        "avg_category_jaccard": (sum(category_jaccards) / len(category_jaccards)) if category_jaccards else None,
        "avg_subcategory_jaccard": (sum(subcategory_jaccards) / len(subcategory_jaccards)) if subcategory_jaccards else None,
        "mini_missing_score_count": sum(1 for item in per_case if item["mini"]["missing_score"]),
        "mini_non_numeric_score_count": sum(1 for item in per_case if item["mini"]["non_numeric_score"]),
        "mini_missing_correct_translation_count": sum(1 for item in per_case if item["mini"]["missing_correct_translation"]),
        "strong_missing_score_count": sum(1 for item in per_case if item["strong"]["missing_score"]),
        "strong_non_numeric_score_count": sum(1 for item in per_case if item["strong"]["non_numeric_score"]),
        "strong_missing_correct_translation_count": sum(1 for item in per_case if item["strong"]["missing_correct_translation"]),
        "exact_correct_translation_match_count": sum(1 for item in per_case if item["diff"]["correct_translation_equal"]),
    }

    return {
        "summary": summary,
        "cases": per_case,
    }


def main() -> None:
    args = _parse_args()
    results = asyncio.run(_run_compare(args))
    output_path = Path(args.output)
    output_path.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(results["summary"], ensure_ascii=False, indent=2))
    print(f"\nWrote {output_path.resolve()}")


if __name__ == "__main__":
    main()
