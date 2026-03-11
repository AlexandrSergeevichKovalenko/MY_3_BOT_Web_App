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

from backend.openai_manager import get_last_llm_usage, run_check_translation_multilang  # noqa: E402
from backend.translation_workflow import _get_language_taxonomy, _parse_translation_feedback_payload  # noqa: E402


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare multiple models on the same fixed translation-check set using the single-item path.",
    )
    parser.add_argument("input_file", help="JSON array of test items.")
    parser.add_argument(
        "--models",
        nargs="+",
        default=["gpt-4.1-2025-04-14", "gpt-5.1", "gpt-5.2"],
        help="Ordered list of models to compare. First model is treated as baseline.",
    )
    parser.add_argument("--source-lang", default="ru")
    parser.add_argument("--target-lang", default="de")
    parser.add_argument("--output", default="translation_model_matrix_results.json")
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
        "feedback": feedback,
        "feedback_chars": len(feedback),
        "score": parsed["score"],
        "score_str": parsed["score_str"],
        "correct_translation": parsed["correct_translation"],
        "categories": list(parsed["categories"]),
        "subcategories": list(parsed["subcategories"]),
        "missing_score": not bool(parsed["score_str"]),
        "non_numeric_score": bool(parsed["score_str"]) and not str(parsed["score_str"]).isdigit(),
        "missing_correct_translation": not bool(parsed["correct_translation"]),
        "elapsed_ms": elapsed_ms,
        "gateway": usage.get("gateway"),
        "usage_model": usage.get("model"),
        "prompt_tokens": usage.get("prompt_tokens"),
        "completion_tokens": usage.get("completion_tokens"),
        "total_tokens": usage.get("total_tokens"),
    }


def _avg(values: list[float | int]) -> float | None:
    return (sum(values) / len(values)) if values else None


async def _run_matrix(args: argparse.Namespace) -> dict[str, Any]:
    cases = _load_cases(args.input_file)
    model_names = [str(model).strip() for model in args.models if str(model).strip()]
    if not model_names:
        raise ValueError("At least one model must be provided.")

    case_results: list[dict[str, Any]] = []
    model_aggregates: dict[str, dict[str, Any]] = {model: {"per_case": []} for model in model_names}

    for item in cases:
        by_model: dict[str, dict[str, Any]] = {}
        for model in model_names:
            result = await _evaluate_case(
                item=item,
                model_name=model,
                default_source_lang=args.source_lang,
                default_target_lang=args.target_lang,
            )
            by_model[model] = result
            model_aggregates[model]["per_case"].append(result)
        case_results.append(
            {
                "case_id": item.get("id"),
                "original_text": item["original_text"],
                "user_translation": item["user_translation"],
                "by_model": by_model,
            }
        )

    baseline = model_names[0]
    model_summaries: dict[str, Any] = {}
    for model in model_names:
        per_case = model_aggregates[model]["per_case"]
        model_summaries[model] = {
            "avg_latency_ms": _avg([float(item["elapsed_ms"]) for item in per_case]),
            "total_latency_ms": sum(float(item["elapsed_ms"]) for item in per_case),
            "avg_feedback_chars": _avg([int(item["feedback_chars"]) for item in per_case]),
            "avg_total_tokens": _avg([int(item["total_tokens"]) for item in per_case if item["total_tokens"] is not None]),
            "missing_score_count": sum(1 for item in per_case if item["missing_score"]),
            "non_numeric_score_count": sum(1 for item in per_case if item["non_numeric_score"]),
            "missing_correct_translation_count": sum(1 for item in per_case if item["missing_correct_translation"]),
        }
        if model == baseline:
            continue
        score_deltas = []
        abs_score_deltas = []
        category_jaccards = []
        subcategory_jaccards = []
        correct_translation_matches = 0
        for case in case_results:
            base_item = case["by_model"][baseline]
            model_item = case["by_model"][model]
            if base_item.get("score") is not None and model_item.get("score") is not None:
                delta = int(model_item["score"]) - int(base_item["score"])
                score_deltas.append(delta)
                abs_score_deltas.append(abs(delta))
            category_jaccards.append(_jaccard(base_item["categories"], model_item["categories"]))
            subcategory_jaccards.append(_jaccard(base_item["subcategories"], model_item["subcategories"]))
            if str(base_item.get("correct_translation") or "").strip() == str(model_item.get("correct_translation") or "").strip():
                correct_translation_matches += 1
        model_summaries[model]["vs_baseline"] = {
            "baseline_model": baseline,
            "avg_score_delta": _avg(score_deltas),
            "avg_abs_score_delta": _avg(abs_score_deltas),
            "avg_category_jaccard": _avg([value for value in category_jaccards if value is not None]),
            "avg_subcategory_jaccard": _avg([value for value in subcategory_jaccards if value is not None]),
            "exact_correct_translation_match_count": correct_translation_matches,
        }

    return {
        "summary": {
            "input_file": str(Path(args.input_file).resolve()),
            "baseline_model": baseline,
            "models": model_names,
            "item_count": len(cases),
        },
        "model_summaries": model_summaries,
        "cases": case_results,
    }


def main() -> None:
    args = _parse_args()
    results = asyncio.run(_run_matrix(args))
    output_path = Path(args.output)
    output_path.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(results["summary"], ensure_ascii=False, indent=2))
    print(json.dumps(results["model_summaries"], ensure_ascii=False, indent=2))
    print(f"\nWrote {output_path.resolve()}")


if __name__ == "__main__":
    main()
