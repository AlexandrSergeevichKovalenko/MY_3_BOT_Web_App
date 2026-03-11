#!/usr/bin/env python3
import argparse
import asyncio
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.openai_manager import (  # noqa: E402
    _extract_response_text,
    _extract_usage_dict,
    client,
    run_check_translation_multilang,
    system_message,
)
from backend.translation_workflow import _get_language_taxonomy, _parse_translation_feedback_payload  # noqa: E402


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="A/B experiment: strong-model text output vs strong-model structured output for translation check.",
    )
    parser.add_argument("input_file", help="JSON array of fixed test items.")
    parser.add_argument("--model", default="gpt-4.1-2025-04-14", help="Strong model used in both modes.")
    parser.add_argument("--source-lang", default="ru")
    parser.add_argument("--target-lang", default="de")
    parser.add_argument("--output", default="translation_strong_text_vs_structured_results.json")
    return parser.parse_args()


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


def _build_taxonomy_hint(categories: list[str], subcategories: dict[str, list[str]]) -> str:
    lines: list[str] = []
    if categories:
        lines.append(f"allowed_categories: {', '.join(categories)}")
    if subcategories:
        lines.append("allowed_subcategories:")
        for category, values in subcategories.items():
            if values:
                lines.append(f"- {category}: {', '.join(values)}")
    return ("\n" + "\n".join(lines)) if lines else ""


def _structured_schema(*, categories: list[str], subcategories: dict[str, list[str]]) -> dict[str, Any]:
    all_subcategories = sorted({value for values in subcategories.values() for value in values})
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["score", "correct_translation", "categories", "subcategories", "feedback"],
        "properties": {
            "score": {"type": "integer", "minimum": 0, "maximum": 100},
            "correct_translation": {"type": "string", "minLength": 1},
            "categories": {
                "type": "array",
                "items": {"type": "string", "enum": categories},
            },
            "subcategories": {
                "type": "array",
                "items": {"type": "string", "enum": all_subcategories},
            },
            "feedback": {"type": "string", "minLength": 1},
        },
    }


async def _run_text_mode_case(
    *,
    item: dict[str, Any],
    model_name: str,
    default_source_lang: str,
    default_target_lang: str,
) -> dict[str, Any]:
    source_lang = str(item.get("source_lang") or default_source_lang).strip() or default_source_lang
    target_lang = str(item.get("target_lang") or default_target_lang).strip() or default_target_lang
    categories, subcategories, _ = _get_language_taxonomy(target_lang)

    previous_override = os.environ.get("LLM_TASK_MODEL_CHECK_TRANSLATION_MULTILANG")
    os.environ["LLM_TASK_MODEL_CHECK_TRANSLATION_MULTILANG"] = model_name
    started_at = time.perf_counter()
    try:
        feedback = await run_check_translation_multilang(
            original_text=str(item["original_text"]).strip(),
            user_translation=str(item["user_translation"]).strip(),
            source_lang=source_lang,
            target_lang=target_lang,
            allowed_categories=categories,
            allowed_subcategories=subcategories,
        )
    finally:
        if previous_override is None:
            os.environ.pop("LLM_TASK_MODEL_CHECK_TRANSLATION_MULTILANG", None)
        else:
            os.environ["LLM_TASK_MODEL_CHECK_TRANSLATION_MULTILANG"] = previous_override
    elapsed_ms = (time.perf_counter() - started_at) * 1000.0
    parsed = _parse_translation_feedback_payload(
        feedback,
        language_categories=categories,
        language_subcategories=subcategories,
    )
    return {
        "case_id": item.get("id"),
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
        "validation_error": None,
    }


async def _run_structured_mode_case(
    *,
    item: dict[str, Any],
    model_name: str,
    default_source_lang: str,
    default_target_lang: str,
) -> dict[str, Any]:
    source_lang = str(item.get("source_lang") or default_source_lang).strip() or default_source_lang
    target_lang = str(item.get("target_lang") or default_target_lang).strip() or default_target_lang
    categories, subcategories, _ = _get_language_taxonomy(target_lang)
    system_instruction = (
        str(system_message.get("check_translation_multilang") or "").strip()
        + "\nReturn the result as strict JSON matching the provided schema. "
        + "The feedback field must concisely explain the main translation mistakes in plain text."
    )
    user_message = (
        f"source_language: {source_lang}\n"
        f"target_language: {target_lang}\n"
        f'original_text: "{str(item["original_text"]).strip()}"\n'
        f'user_translation: "{str(item["user_translation"]).strip()}"'
        f"{_build_taxonomy_hint(categories, subcategories)}"
    )

    started_at = time.perf_counter()
    response = await client.responses.create(
        model=model_name,
        instructions=system_instruction,
        input=user_message,
        text={
            "format": {
                "type": "json_schema",
                "name": "translation_check_result",
                "description": "Structured output for a single translation-check result.",
                "strict": True,
                "schema": _structured_schema(categories=categories, subcategories=subcategories),
            },
            "verbosity": "medium",
        },
    )
    elapsed_ms = (time.perf_counter() - started_at) * 1000.0
    raw_text = _extract_response_text(response).strip()
    usage = _extract_usage_dict(response, task_name="check_translation_structured_experiment") or {}
    validation_error = None
    payload: dict[str, Any] | None = None
    try:
        parsed_json = json.loads(raw_text)
        if not isinstance(parsed_json, dict):
            raise ValueError("Structured output is not a JSON object")
        payload = parsed_json
    except Exception as exc:
        validation_error = f"invalid_json:{exc}"
        payload = {}

    score = payload.get("score")
    correct_translation = str(payload.get("correct_translation") or "").strip()
    feedback = str(payload.get("feedback") or "").strip()
    parsed_categories = payload.get("categories") if isinstance(payload.get("categories"), list) else []
    parsed_subcategories = payload.get("subcategories") if isinstance(payload.get("subcategories"), list) else []

    normalized = _parse_translation_feedback_payload(
        (
            f"Score: {score}/100\n"
            f"Mistake Categories: {', '.join(str(x) for x in parsed_categories)}\n"
            f"Subcategories: {', '.join(str(x) for x in parsed_subcategories)}\n"
            f"Correct Translation: {correct_translation}\n"
        ),
        language_categories=categories,
        language_subcategories=subcategories,
    )

    missing_score = not isinstance(score, int)
    non_numeric_score = False
    missing_correct_translation = not bool(correct_translation)
    if validation_error is None:
        if missing_score:
            validation_error = "missing_score"
        elif missing_correct_translation:
            validation_error = "missing_correct_translation"
        elif not feedback:
            validation_error = "missing_feedback"

    return {
        "case_id": item.get("id"),
        "feedback": feedback,
        "feedback_chars": len(feedback),
        "score": int(score) if isinstance(score, int) else None,
        "score_str": str(score) if isinstance(score, int) else None,
        "correct_translation": correct_translation or None,
        "categories": list(normalized["categories"]),
        "subcategories": list(normalized["subcategories"]),
        "missing_score": missing_score,
        "non_numeric_score": non_numeric_score,
        "missing_correct_translation": missing_correct_translation,
        "elapsed_ms": elapsed_ms,
        "validation_error": validation_error,
        "usage": usage,
        "raw_text": raw_text,
    }


async def _run_experiment(args: argparse.Namespace) -> dict[str, Any]:
    cases = _load_cases(args.input_file)
    text_results = []
    structured_results = []
    for item in cases:
        text_results.append(
            await _run_text_mode_case(
                item=item,
                model_name=args.model,
                default_source_lang=args.source_lang,
                default_target_lang=args.target_lang,
            )
        )
        structured_results.append(
            await _run_structured_mode_case(
                item=item,
                model_name=args.model,
                default_source_lang=args.source_lang,
                default_target_lang=args.target_lang,
            )
        )

    per_case = []
    for item, text_result, structured_result in zip(cases, text_results, structured_results):
        score_delta = None
        abs_score_delta = None
        if text_result.get("score") is not None and structured_result.get("score") is not None:
            score_delta = int(structured_result["score"]) - int(text_result["score"])
            abs_score_delta = abs(score_delta)
        per_case.append(
            {
                "case_id": item.get("id"),
                "original_text": item["original_text"],
                "user_translation": item["user_translation"],
                "text_mode": text_result,
                "structured_mode": structured_result,
                "diff": {
                    "score_delta": score_delta,
                    "abs_score_delta": abs_score_delta,
                    "category_jaccard": _jaccard(text_result["categories"], structured_result["categories"]),
                    "subcategory_jaccard": _jaccard(text_result["subcategories"], structured_result["subcategories"]),
                    "correct_translation_equal": (
                        str(text_result.get("correct_translation") or "").strip()
                        == str(structured_result.get("correct_translation") or "").strip()
                    ),
                    "feedback_char_delta": structured_result["feedback_chars"] - text_result["feedback_chars"],
                },
            }
        )

    text_latencies = [float(item["elapsed_ms"]) for item in text_results]
    structured_latencies = [float(item["elapsed_ms"]) for item in structured_results]
    score_deltas = [case["diff"]["score_delta"] for case in per_case if case["diff"]["score_delta"] is not None]
    abs_score_deltas = [case["diff"]["abs_score_delta"] for case in per_case if case["diff"]["abs_score_delta"] is not None]
    category_jaccards = [case["diff"]["category_jaccard"] for case in per_case if case["diff"]["category_jaccard"] is not None]
    subcategory_jaccards = [case["diff"]["subcategory_jaccard"] for case in per_case if case["diff"]["subcategory_jaccard"] is not None]

    summary = {
        "model": args.model,
        "item_count": len(cases),
        "text_mode_avg_latency_ms": (sum(text_latencies) / len(text_latencies)) if text_latencies else None,
        "structured_mode_avg_latency_ms": (sum(structured_latencies) / len(structured_latencies)) if structured_latencies else None,
        "text_mode_total_latency_ms": sum(text_latencies),
        "structured_mode_total_latency_ms": sum(structured_latencies),
        "avg_score_delta_structured_minus_text": (sum(score_deltas) / len(score_deltas)) if score_deltas else None,
        "avg_abs_score_delta": (sum(abs_score_deltas) / len(abs_score_deltas)) if abs_score_deltas else None,
        "avg_category_jaccard": (sum(category_jaccards) / len(category_jaccards)) if category_jaccards else None,
        "avg_subcategory_jaccard": (sum(subcategory_jaccards) / len(subcategory_jaccards)) if subcategory_jaccards else None,
        "exact_correct_translation_match_count": sum(1 for case in per_case if case["diff"]["correct_translation_equal"]),
        "text_missing_score_count": sum(1 for item in text_results if item["missing_score"]),
        "text_non_numeric_score_count": sum(1 for item in text_results if item["non_numeric_score"]),
        "text_missing_correct_translation_count": sum(1 for item in text_results if item["missing_correct_translation"]),
        "structured_missing_score_count": sum(1 for item in structured_results if item["missing_score"]),
        "structured_non_numeric_score_count": sum(1 for item in structured_results if item["non_numeric_score"]),
        "structured_missing_correct_translation_count": sum(1 for item in structured_results if item["missing_correct_translation"]),
        "structured_validation_error_count": sum(1 for item in structured_results if item["validation_error"]),
        "text_avg_feedback_chars": (sum(item["feedback_chars"] for item in text_results) / len(text_results)) if text_results else None,
        "structured_avg_feedback_chars": (sum(item["feedback_chars"] for item in structured_results) / len(structured_results)) if structured_results else None,
    }
    return {
        "summary": summary,
        "cases": per_case,
    }


def main() -> None:
    args = _parse_args()
    results = asyncio.run(_run_experiment(args))
    output_path = Path(args.output)
    output_path.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(results["summary"], ensure_ascii=False, indent=2))
    print(f"\nWrote {output_path.resolve()}")


if __name__ == "__main__":
    main()
