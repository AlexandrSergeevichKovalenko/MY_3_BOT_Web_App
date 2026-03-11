#!/usr/bin/env python3
import argparse
import asyncio
import json
import os
import re
import sys
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.openai_manager import (  # noqa: E402
    _extract_response_text,
    _extract_usage_dict,
    client,
    get_last_llm_usage,
    run_check_translation_multilang,
)
from backend.translation_workflow import _get_language_taxonomy, _parse_translation_feedback_payload  # noqa: E402


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="A/B experiment: strong-model single-item evaluation vs strong-model batched evaluation.",
    )
    parser.add_argument("input_file", help="JSON array of test items.")
    parser.add_argument("--model", default="gpt-4.1-2025-04-14", help="Strong model to use in both modes.")
    parser.add_argument("--source-lang", default="ru")
    parser.add_argument("--target-lang", default="de")
    parser.add_argument("--batch-size", type=int, default=2)
    parser.add_argument("--output", default="translation_strong_single_vs_batch_results.json")
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


def _canonicalize_labels(
    categories: list[Any],
    subcategories: list[Any],
    *,
    language_categories: list[str],
    language_subcategories: dict[str, list[str]],
) -> tuple[list[str], list[str]]:
    valid_categories: list[str] = []
    valid_subcategories: list[str] = []
    for cat in categories or []:
        cat_lower = str(cat or "").strip().lower()
        canonical = next((value for value in language_categories if value.lower() == cat_lower), None)
        if canonical:
            valid_categories.append(canonical)
    for sub in subcategories or []:
        sub_lower = str(sub or "").strip().lower()
        for _, values in language_subcategories.items():
            canonical_sub = next((value for value in values if value.lower() == sub_lower), None)
            if canonical_sub:
                valid_subcategories.append(canonical_sub)
                break
    return list(dict.fromkeys(valid_categories)), list(dict.fromkeys(valid_subcategories))


async def _evaluate_single_item(
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
        "case_id": item.get("id"),
        "source_lang": source_lang,
        "target_lang": target_lang,
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
        "usage": usage,
        "gateway": usage.get("gateway"),
        "usage_model": usage.get("model"),
        "prompt_tokens": usage.get("prompt_tokens"),
        "completion_tokens": usage.get("completion_tokens"),
        "total_tokens": usage.get("total_tokens"),
        "validation_error": None,
    }


def _build_batch_instructions(
    *,
    source_lang: str,
    target_lang: str,
    language_categories: list[str],
    language_subcategories: dict[str, list[str]],
) -> str:
    taxonomy_rows = []
    if language_categories:
        taxonomy_rows.append("allowed_categories: " + ", ".join(language_categories))
    if language_subcategories:
        taxonomy_rows.append("allowed_subcategories:")
        for category, values in language_subcategories.items():
            taxonomy_rows.append(f"- {category}: {', '.join(values)}")
    taxonomy_block = "\n".join(taxonomy_rows)
    return (
        "You are a strict translation evaluator.\n"
        "You will receive JSON with multiple translation-check items.\n"
        "Return STRICT JSON only in this exact shape:\n"
        "{\n"
        '  "items": [\n'
        "    {\n"
        '      "batch_item_id": "string",\n'
        '      "item_order": 0,\n'
        '      "score": 0,\n'
        '      "correct_translation": "string",\n'
        '      "categories": ["string"],\n'
        '      "subcategories": ["string"],\n'
        '      "feedback": "string"\n'
        "    }\n"
        "  ]\n"
        "}\n"
        "Rules:\n"
        "- Every input item must appear exactly once.\n"
        "- score must be integer 0..100.\n"
        "- correct_translation must be in target_language.\n"
        "- categories and subcategories must be arrays.\n"
        "- feedback must be a compact plain-text explanation for that item.\n"
        "- no markdown, no prose outside JSON.\n"
        f"- source_language is {source_lang} and target_language is {target_lang}.\n"
        f"{taxonomy_block}\n"
    )


def _extract_json_object(text: str) -> dict[str, Any]:
    stripped = str(text or "").strip()
    try:
        parsed = json.loads(stripped)
        if isinstance(parsed, dict):
            return parsed
    except Exception:
        pass
    match = re.search(r"\{.*\}", stripped, flags=re.DOTALL)
    if match:
        parsed = json.loads(match.group(0))
        if isinstance(parsed, dict):
            return parsed
    raise ValueError("Batch response is not valid JSON object")


def _validate_batch_item(
    raw_item: dict[str, Any],
    *,
    expected_item: dict[str, Any],
    language_categories: list[str],
    language_subcategories: dict[str, list[str]],
) -> dict[str, Any]:
    if not isinstance(raw_item, dict):
        raise ValueError("Batch item is not an object")
    batch_item_id = str(raw_item.get("batch_item_id") or "").strip()
    if batch_item_id != str(expected_item["batch_item_id"]):
        raise ValueError("Batch item id mismatch")
    score = raw_item.get("score")
    if not isinstance(score, int):
        raise ValueError("Missing or non-integer score")
    correct_translation = str(raw_item.get("correct_translation") or "").strip()
    if not correct_translation:
        raise ValueError("Missing correct_translation")
    feedback = str(raw_item.get("feedback") or "").strip()
    if not feedback:
        raise ValueError("Missing feedback")
    categories = raw_item.get("categories")
    subcategories = raw_item.get("subcategories")
    if not isinstance(categories, list):
        raise ValueError("categories is not an array")
    if not isinstance(subcategories, list):
        raise ValueError("subcategories is not an array")
    normalized_categories, normalized_subcategories = _canonicalize_labels(
        categories,
        subcategories,
        language_categories=language_categories,
        language_subcategories=language_subcategories,
    )
    return {
        "case_id": expected_item.get("id"),
        "source_lang": expected_item["source_lang"],
        "target_lang": expected_item["target_lang"],
        "feedback": feedback,
        "feedback_chars": len(feedback),
        "score": int(score),
        "score_str": str(score),
        "correct_translation": correct_translation,
        "categories": normalized_categories,
        "subcategories": normalized_subcategories,
        "missing_score": False,
        "non_numeric_score": False,
        "missing_correct_translation": False,
        "elapsed_ms": None,
        "usage": {},
        "gateway": "responses",
        "usage_model": None,
        "prompt_tokens": None,
        "completion_tokens": None,
        "total_tokens": None,
        "validation_error": None,
    }


async def _evaluate_batch(
    *,
    items: list[dict[str, Any]],
    model_name: str,
    default_source_lang: str,
    default_target_lang: str,
) -> dict[str, Any]:
    if not items:
        return {"items": [], "batch_duration_ms": 0.0, "usage": {}, "malformed": False, "malformed_reason": None}
    source_lang = str(items[0].get("source_lang") or default_source_lang).strip() or default_source_lang
    target_lang = str(items[0].get("target_lang") or default_target_lang).strip() or default_target_lang
    language_categories, language_subcategories, _ = _get_language_taxonomy(target_lang)

    prepared_items = []
    for index, item in enumerate(items):
        prepared_items.append(
            {
                "id": item.get("id"),
                "source_lang": str(item.get("source_lang") or default_source_lang).strip() or default_source_lang,
                "target_lang": str(item.get("target_lang") or default_target_lang).strip() or default_target_lang,
                "batch_item_id": f"batch-{index}-{item.get('id') or index}",
                "item_order": index,
                "sentence_number": item.get("sentence_number"),
                "original_text": str(item["original_text"]).strip(),
                "user_translation": str(item["user_translation"]).strip(),
            }
        )

    payload = {
        "task": "translation_check_batch_experiment",
        "source_lang": source_lang,
        "target_lang": target_lang,
        "items": [
            {
                "batch_item_id": item["batch_item_id"],
                "item_order": item["item_order"],
                "sentence_number": item["sentence_number"],
                "original_text": item["original_text"],
                "user_translation": item["user_translation"],
            }
            for item in prepared_items
        ],
    }
    started_at = time.perf_counter()
    response = await client.responses.create(
        model=model_name,
        instructions=_build_batch_instructions(
            source_lang=source_lang,
            target_lang=target_lang,
            language_categories=language_categories,
            language_subcategories=language_subcategories,
        ),
        input=json.dumps(payload, ensure_ascii=False),
    )
    batch_duration_ms = (time.perf_counter() - started_at) * 1000.0
    usage = _extract_usage_dict(response, task_name="translation_check_batch_experiment") or {}
    usage["gateway"] = "responses"
    text = _extract_response_text(response).strip()

    try:
        parsed = _extract_json_object(text)
        raw_items = parsed.get("items")
        if not isinstance(raw_items, list):
            raise ValueError("Top-level items array missing")
        if len(raw_items) != len(prepared_items):
            raise ValueError("Returned item count does not match request")
        raw_by_id = {}
        for raw_item in raw_items:
            batch_item_id = str((raw_item or {}).get("batch_item_id") or "").strip()
            if not batch_item_id:
                raise ValueError("Returned item missing batch_item_id")
            if batch_item_id in raw_by_id:
                raise ValueError("Duplicate batch_item_id in response")
            raw_by_id[batch_item_id] = raw_item
        validated_items = []
        validation_failures = []
        for expected_item in prepared_items:
            try:
                result = _validate_batch_item(
                    raw_by_id.get(expected_item["batch_item_id"]),
                    expected_item=expected_item,
                    language_categories=language_categories,
                    language_subcategories=language_subcategories,
                )
                validated_items.append(result)
            except Exception as exc:
                validation_failures.append(
                    {
                        "case_id": expected_item.get("id"),
                        "batch_item_id": expected_item["batch_item_id"],
                        "error": str(exc),
                    }
                )
        return {
            "items": validated_items,
            "batch_duration_ms": batch_duration_ms,
            "usage": usage,
            "malformed": False,
            "malformed_reason": None,
            "validation_failures": validation_failures,
            "raw_text": text,
        }
    except Exception as exc:
        return {
            "items": [],
            "batch_duration_ms": batch_duration_ms,
            "usage": usage,
            "malformed": True,
            "malformed_reason": str(exc),
            "validation_failures": [],
            "raw_text": text,
        }


def _chunked(items: list[dict[str, Any]], size: int) -> list[list[dict[str, Any]]]:
    return [items[index:index + size] for index in range(0, len(items), size)]


async def _run_experiment(args: argparse.Namespace) -> dict[str, Any]:
    cases = _load_cases(args.input_file)
    single_results = []
    for item in cases:
        single_results.append(
            await _evaluate_single_item(
                item=item,
                model_name=args.model,
                default_source_lang=args.source_lang,
                default_target_lang=args.target_lang,
            )
        )

    batch_groups = _chunked(cases, max(1, int(args.batch_size)))
    batch_runs = []
    for group in batch_groups:
        batch_runs.append(
            await _evaluate_batch(
                items=group,
                model_name=args.model,
                default_source_lang=args.source_lang,
                default_target_lang=args.target_lang,
            )
        )

    batch_items_by_case: dict[str, dict[str, Any]] = {}
    per_item_validation_failures = []
    for batch_run in batch_runs:
        for item in batch_run["items"]:
            batch_items_by_case[str(item.get("case_id"))] = item
        per_item_validation_failures.extend(batch_run.get("validation_failures") or [])

    per_case = []
    for input_case, single in zip(cases, single_results):
        case_id = str(input_case.get("id"))
        batch = batch_items_by_case.get(case_id)
        if batch is None:
            per_case.append(
                {
                    "case_id": case_id,
                    "original_text": input_case["original_text"],
                    "user_translation": input_case["user_translation"],
                    "single": single,
                    "batch": None,
                    "diff": {
                        "score_delta": None,
                        "abs_score_delta": None,
                        "category_jaccard": None,
                        "subcategory_jaccard": None,
                        "correct_translation_equal": False,
                        "batch_missing": True,
                    },
                }
            )
            continue
        single_score = single.get("score")
        batch_score = batch.get("score")
        score_delta = None
        abs_score_delta = None
        if single_score is not None and batch_score is not None:
            score_delta = int(batch_score) - int(single_score)
            abs_score_delta = abs(score_delta)
        per_case.append(
            {
                "case_id": case_id,
                "original_text": input_case["original_text"],
                "user_translation": input_case["user_translation"],
                "single": single,
                "batch": batch,
                "diff": {
                    "score_delta": score_delta,
                    "abs_score_delta": abs_score_delta,
                    "category_jaccard": _jaccard(single["categories"], batch["categories"]),
                    "subcategory_jaccard": _jaccard(single["subcategories"], batch["subcategories"]),
                    "correct_translation_equal": (
                        str(single.get("correct_translation") or "").strip()
                        == str(batch.get("correct_translation") or "").strip()
                    ),
                    "batch_missing": False,
                },
            }
        )

    score_deltas = [case["diff"]["score_delta"] for case in per_case if case["diff"]["score_delta"] is not None]
    abs_score_deltas = [case["diff"]["abs_score_delta"] for case in per_case if case["diff"]["abs_score_delta"] is not None]
    category_jaccards = [case["diff"]["category_jaccard"] for case in per_case if case["diff"]["category_jaccard"] is not None]
    subcategory_jaccards = [case["diff"]["subcategory_jaccard"] for case in per_case if case["diff"]["subcategory_jaccard"] is not None]
    single_latencies = [float(item["elapsed_ms"]) for item in single_results]
    batch_call_latencies = [float(run["batch_duration_ms"]) for run in batch_runs]
    total_single_latency = sum(single_latencies)
    total_batch_latency = sum(batch_call_latencies)
    malformed_batches = [run for run in batch_runs if run.get("malformed")]

    summary = {
        "model": args.model,
        "item_count": len(cases),
        "batch_size": int(args.batch_size),
        "batch_call_count": len(batch_runs),
        "single_item_avg_latency_ms": (total_single_latency / len(single_latencies)) if single_latencies else None,
        "single_item_total_latency_ms": total_single_latency,
        "batch_call_avg_latency_ms": (total_batch_latency / len(batch_call_latencies)) if batch_call_latencies else None,
        "batch_total_latency_ms": total_batch_latency,
        "batch_item_equivalent_avg_latency_ms": (total_batch_latency / len(cases)) if cases else None,
        "avg_score_delta_batch_minus_single": (sum(score_deltas) / len(score_deltas)) if score_deltas else None,
        "avg_abs_score_delta": (sum(abs_score_deltas) / len(abs_score_deltas)) if abs_score_deltas else None,
        "avg_category_jaccard": (sum(category_jaccards) / len(category_jaccards)) if category_jaccards else None,
        "avg_subcategory_jaccard": (sum(subcategory_jaccards) / len(subcategory_jaccards)) if subcategory_jaccards else None,
        "exact_correct_translation_match_count": sum(1 for case in per_case if case["diff"]["correct_translation_equal"]),
        "single_missing_score_count": sum(1 for item in single_results if item["missing_score"]),
        "single_non_numeric_score_count": sum(1 for item in single_results if item["non_numeric_score"]),
        "single_missing_correct_translation_count": sum(1 for item in single_results if item["missing_correct_translation"]),
        "batch_missing_score_count": sum(1 for case in per_case if case.get("batch") and case["batch"]["missing_score"]),
        "batch_non_numeric_score_count": sum(1 for case in per_case if case.get("batch") and case["batch"]["non_numeric_score"]),
        "batch_missing_correct_translation_count": sum(1 for case in per_case if case.get("batch") and case["batch"]["missing_correct_translation"]),
        "batch_malformed_count": len(malformed_batches),
        "per_item_validation_failure_count": len(per_item_validation_failures),
        "items_requiring_fallback_count": len(per_item_validation_failures) + sum(len(group) for group, run in zip(batch_groups, batch_runs) if run.get("malformed")),
    }

    return {
        "summary": summary,
        "batch_runs": batch_runs,
        "per_item_validation_failures": per_item_validation_failures,
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
