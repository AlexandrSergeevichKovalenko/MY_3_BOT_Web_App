#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
import subprocess
import sys
from collections import defaultdict
from contextlib import contextmanager
from pathlib import Path
from typing import Any

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
except Exception:
    psycopg2 = None
    RealDictCursor = None

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.database import get_db_connection  # noqa: E402
from backend.translation_workflow import (  # noqa: E402
    REMEDIATION_PRIMARY_DEMOTION_SKILL_IDS,
    _build_errored_skill_details,
    _build_phase1_shadow_payload,
    _build_phase1_tested_skill_profile_with_cursor,
    _build_remediation_profile_with_cursor,
    _collect_sentence_anchor_skill_weights,
    _load_sentence_skill_shadow_state_with_cursor,
    _load_sentence_skill_targets_with_cursor,
    _normalize_category_pairs,
    check_translation,
    rerank_tested_skill_profile_for_sentence,
)


REUSED_PRODUCTION_FUNCTIONS: list[str] = [
    "backend.translation_workflow.check_translation",
    "backend.translation_workflow._normalize_category_pairs",
    "backend.translation_workflow._build_errored_skill_details",
    "backend.translation_workflow._build_remediation_profile_with_cursor",
    "backend.translation_workflow._build_phase1_tested_skill_profile_with_cursor",
    "backend.translation_workflow._load_sentence_skill_targets_with_cursor",
    "backend.translation_workflow._load_sentence_skill_shadow_state_with_cursor",
    "backend.translation_workflow._build_phase1_shadow_payload",
    "backend.translation_workflow.rerank_tested_skill_profile_for_sentence",
]

NOISE_PRIMARY_SKILL_IDS: set[str] = set(REMEDIATION_PRIMARY_DEMOTION_SKILL_IDS) | {
    "de_orthography_spelling_common_spelling_errors",
    "de_punctuation_comma_in_subordinate_clause",
}

OUTCOME_PRIORITY: tuple[str, ...] = (
    "fail_repeat_no_progress",
    "fail_repeat_progress",
    "fail_new",
    "recovered_final",
    "recovered_partial",
    "clean_success",
    "clean_progress_credit",
    "clean_neutral",
    "untargeted_error_fail",
)

EXPECTATION_ALIAS_MAP: dict[str, set[str]] = {
    "prepositions_usage": {
        "prepositions_usage",
        "de_cases_case_after_preposition",
        "cases_preposition_accusative",
        "cases_preposition_dative",
        "cases_preposition_genitive",
    },
    "de_cases_case_after_preposition": {
        "de_cases_case_after_preposition",
        "prepositions_usage",
    },
    "cases_accusative": {"cases_accusative", "cases_preposition_accusative"},
    "word_order_subordinate_clause": {
        "word_order_subordinate_clause",
        "verbs_placement_subordinate",
        "de_clauses_sentence_types_main_vs_subordinate_clause",
        "word_order_modal_structure",
    },
    "de_clauses_sentence_types_relative_clauses": {
        "de_clauses_sentence_types_relative_clauses",
        "de_pronouns_relative_pronouns",
        "word_order_subordinate_clause",
    },
    "de_clauses_sentence_types_concessive_clauses_obwohl": {
        "de_clauses_sentence_types_concessive_clauses_obwohl",
        "word_order_subordinate_clause",
    },
    "de_clauses_sentence_types_conditionals_wenn_falls": {
        "de_clauses_sentence_types_conditionals_wenn_falls",
    },
    "de_clauses_sentence_types_purpose_clauses_damit_um_zu": {
        "de_clauses_sentence_types_purpose_clauses_damit_um_zu",
        "word_order_subordinate_clause",
    },
    "de_infinitive_participles_zu_infinitive": {
        "de_infinitive_participles_zu_infinitive",
        "de_clauses_sentence_types_infinitive_clauses_vs_dass_clause",
    },
    "nouns_compounds": {"nouns_compounds", "nouns_declension"},
    "moods_subjunctive2": {
        "moods_subjunctive2",
        "de_moods_konjunktiv_ii_wuerde_form",
    },
    "de_voice_active_passive_zustandspassiv_sein_partizip_ii": {
        "de_voice_active_passive_zustandspassiv_sein_partizip_ii",
        "de_voice_active_passive_vorgangspassiv_werden_partizip_ii",
    },
    "de_verbs_verb_valency_missing_object_complement": {
        "de_verbs_verb_valency_missing_object_complement",
        "verbs_auxiliaries",
    },
    "word_order_v2_rule": {"word_order_v2_rule", "word_order_standard"},
    "de_negation_negation_placement": {"de_negation_negation_placement"},
    "de_pronouns_reflexive_pronouns": {"de_pronouns_reflexive_pronouns", "verbs_reflexive"},
}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a semantic benchmark harness against the production skills pipeline helpers.",
    )
    parser.add_argument(
        "input_file",
        nargs="?",
        default="scripts/semantic_skill_benchmark_cases.example.json",
        help="JSON benchmark file. Defaults to scripts/semantic_skill_benchmark_cases.example.json",
    )
    parser.add_argument(
        "--mode",
        choices=("db-backed", "direct"),
        default="db-backed",
        help="db-backed reads stored production attempts; direct runs the live production LLM evaluator plus post-check helpers.",
    )
    parser.add_argument(
        "--database-url-env",
        default="DATABASE_URL_RAILWAY",
        help="Environment variable with the Postgres connection string.",
    )
    parser.add_argument(
        "--user-id",
        type=int,
        default=117649764,
        help="Default user id for DB lookups.",
    )
    parser.add_argument(
        "--all-users",
        action="store_true",
        help="In db-backed mode, evaluate matching attempts across all users instead of filtering by user_id.",
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
        "--recent-limit",
        type=int,
        default=5,
        help="Maximum number of recent DB attempts per benchmark case in db-backed mode.",
    )
    parser.add_argument(
        "--source-session-ids",
        default="",
        help="Optional comma-separated source_session_id filter for db-backed mode.",
    )
    parser.add_argument(
        "--local-replay-targeting",
        action="store_true",
        help="In db-backed mode, recompute tested profiles locally with the current reranker on top of stored profiles.",
    )
    parser.add_argument(
        "--llm-model",
        default="",
        help="Optional model override for direct mode via LLM_TASK_MODEL_CHECK_TRANSLATION_MULTILANG.",
    )
    parser.add_argument(
        "--output-json",
        default="semantic_skill_benchmark_report.json",
        help="Machine-readable JSON output path.",
    )
    parser.add_argument(
        "--output-md",
        default="",
        help="Human-readable Markdown output path. Defaults to the JSON stem with .md.",
    )
    return parser.parse_args()


@contextmanager
def _temporary_env(name: str, value: str):
    if not value:
        yield
        return
    previous = os.environ.get(name)
    os.environ[name] = value
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop(name, None)
        else:
            os.environ[name] = previous


def _normalize_sentence_key(text: str | None) -> str:
    normalized = str(text or "").strip().lower().replace("ё", "е")
    normalized = re.sub(r"\s+", " ", normalized, flags=re.UNICODE).strip()
    return normalized


def _normalize_json_value(value: Any) -> Any:
    if value is None:
        return []
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        return json.loads(text)
    return value


def _parse_csv_text_list(raw_value: str | None) -> list[str]:
    return [
        item.strip()
        for item in str(raw_value or "").split(",")
        if item and item.strip()
    ]


def _coerce_string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item or "").strip() for item in value if str(item or "").strip()]
    text = str(value or "").strip()
    if not text:
        return []
    if text.upper().startswith("TBD"):
        return []
    return [text]


def _load_benchmark_cases(path: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    defaults: dict[str, Any] = {}
    raw_cases: Any = payload
    if isinstance(payload, dict):
        defaults = dict(payload.get("defaults") or {})
        raw_cases = payload.get("cases") or []
    if not isinstance(raw_cases, list):
        raise ValueError("Benchmark input must be a JSON array or an object with a 'cases' array.")

    cases: list[dict[str, Any]] = []
    seen_case_ids: set[str] = set()
    for index, raw in enumerate(raw_cases, start=1):
        if not isinstance(raw, dict):
            raise ValueError(f"Case #{index} is not an object.")
        case_id = str(raw.get("case_id") or f"case_{index}").strip()
        if case_id in seen_case_ids:
            raise ValueError(f"Duplicate case_id {case_id!r}.")
        source_sentence = str(raw.get("source_sentence") or raw.get("original_text") or "").strip()
        if not source_sentence:
            raise ValueError(f"Case {case_id!r} must include source_sentence.")
        case = dict(defaults)
        case.update(raw)
        case["case_id"] = case_id
        case["source_sentence"] = source_sentence
        case["user_translation"] = str(case.get("user_translation") or "").strip()
        case["expected_tested_primary"] = str(case.get("expected_tested_primary") or case.get("expected_primary") or "").strip()
        case["expected_tested_secondary"] = _coerce_string_list(
            case.get("expected_tested_secondary", case.get("expected_secondary"))
        )
        raw_expected_errors = case.get("expected_errored_skills")
        case["expected_errored_skills"] = _coerce_string_list(raw_expected_errors)
        case["expected_errored_skills_comparable"] = bool(
            case["expected_errored_skills"] or (
                raw_expected_errors is not None
                and not str(raw_expected_errors).strip().upper().startswith("TBD")
            )
        )
        case["expected_outcome_type"] = str(
            case.get("expected_outcome_type") or case.get("expected_outcome") or ""
        ).strip()
        case["source_lang"] = str(case.get("source_lang") or defaults.get("source_lang") or "ru").strip() or "ru"
        case["target_lang"] = str(case.get("target_lang") or defaults.get("target_lang") or "de").strip() or "de"
        case["normalized_source_sentence"] = _normalize_sentence_key(source_sentence)
        cases.append(case)
        seen_case_ids.add(case_id)
    return defaults, cases


def _sql_literal(value: str) -> str:
    return "'" + str(value or "").replace("\\", "\\\\").replace("'", "''") + "'"


def _run_psql_json_rows(database_url: str, sql: str) -> list[dict[str, Any]]:
    completed = subprocess.run(
        [
            "psql",
            database_url,
            "-P",
            "pager=off",
            "-At",
            "-F",
            "\t",
            "-c",
            sql,
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    rows: list[dict[str, Any]] = []
    for line in completed.stdout.splitlines():
        text = line.strip()
        if not text:
            continue
        rows.append(json.loads(text))
    return rows


def _build_db_backed_sql(
    *,
    cases: list[dict[str, Any]],
    user_id: int | None,
    source_lang: str,
    target_lang: str,
    source_session_ids: list[str] | None = None,
) -> str:
    session_filter_sql = ""
    normalized_session_ids = [str(item).strip() for item in list(source_session_ids or []) if str(item).strip()]
    if normalized_session_ids:
        session_literals = ", ".join(_sql_literal(item) for item in normalized_session_ids)
        session_filter_sql = f" AND ds.session_id::text = ANY(ARRAY[{session_literals}]::text[]) "
    translation_user_filter_sql = ""
    sentence_user_filter_sql = ""
    if user_id is not None:
        translation_user_filter_sql = f" AND t.user_id = {int(user_id)} "
        sentence_user_filter_sql = f" AND ds.user_id = {int(user_id)} "
    values_sql = ",\n        ".join(
        f"({_sql_literal(str(case['case_id']))}, {_sql_literal(str(case['normalized_source_sentence']))})"
        for case in cases
    )
    return f"""
    WITH benchmark_sentences(case_id, normalized_sentence) AS (
        VALUES
        {values_sql}
    ),
    latest_translation AS (
        SELECT *
        FROM (
            SELECT
                t.*,
                ROW_NUMBER() OVER (
                    PARTITION BY t.sentence_id
                    ORDER BY t.timestamp DESC, t.id DESC
                ) AS rn
            FROM bt_3_translations t
            WHERE LOWER(COALESCE(t.source_lang, 'ru')) = LOWER({_sql_literal(source_lang)})
              AND LOWER(COALESCE(t.target_lang, 'de')) = LOWER({_sql_literal(target_lang)})
              {translation_user_filter_sql}
        ) ranked
        WHERE rn = 1
    ),
    sentences AS (
        SELECT
            b.case_id,
            ds.id AS sentence_id,
            ds.session_id::bigint AS session_id,
            ds.id_for_mistake_table,
            ds.unique_id,
            ds.sentence,
            LOWER(REGEXP_REPLACE(REPLACE(COALESCE(ds.sentence, ''), 'ё', 'е'), '\\s+', ' ', 'g')) AS normalized_sentence,
            COALESCE(lt.timestamp, ds.date::timestamp) AS attempt_at,
            lt.score AS translation_score,
            lt.user_translation,
            lt.feedback
        FROM bt_3_daily_sentences ds
        JOIN benchmark_sentences b
          ON LOWER(REGEXP_REPLACE(REPLACE(COALESCE(ds.sentence, ''), 'ё', 'е'), '\\s+', ' ', 'g')) = b.normalized_sentence
        LEFT JOIN latest_translation lt
          ON lt.sentence_id = ds.id
        WHERE LOWER(COALESCE(ds.source_lang, 'ru')) = LOWER({_sql_literal(source_lang)})
          AND LOWER(COALESCE(ds.target_lang, 'de')) = LOWER({_sql_literal(target_lang)})
          {sentence_user_filter_sql}
          {session_filter_sql}
    ),
    skill_meta AS (
        SELECT
            k.skill_id,
            k.title,
            COALESCE(m.is_diagnostic_only, false) AS is_diagnostic_only
        FROM bt_3_skills k
        LEFT JOIN bt_3_skill_mastery_group_members m
          ON m.language_code = k.language_code
         AND m.diagnostic_skill_id = k.skill_id
        WHERE k.language_code = LOWER({_sql_literal(target_lang)})
    ),
    profiles AS (
        SELECT
            s.sentence_id,
            json_agg(
                json_build_object(
                    'skill_id', t.skill_id,
                    'title', sm.title,
                    'role', t.role,
                    'role_rank', t.role_rank,
                    'profile_source', t.profile_source,
                    'profile_confidence', t.profile_confidence,
                    'profile_version', t.profile_version
                )
                ORDER BY t.role_rank ASC, t.skill_id ASC
            ) FILTER (WHERE t.skill_id IS NOT NULL) AS tested_profile
        FROM sentences s
        LEFT JOIN bt_3_sentence_skill_targets t
          ON t.sentence_id = s.sentence_id
        LEFT JOIN skill_meta sm
          ON sm.skill_id = t.skill_id
        GROUP BY s.sentence_id
    ),
    errors AS (
        SELECT
            s.sentence_id,
            json_agg(
                json_build_object(
                    'skill_id', e.skill_id,
                    'title', sm.title,
                    'skill_role', e.skill_role,
                    'per_skill_outcome', e.per_skill_outcome,
                    'is_tested', e.is_tested,
                    'is_errored_now', e.is_errored_now,
                    'is_diagnostic_only', sm.is_diagnostic_only
                )
                ORDER BY e.skill_id ASC
            ) FILTER (WHERE e.is_errored_now) AS errored_rows,
            json_agg(DISTINCT e.per_skill_outcome) FILTER (WHERE e.per_skill_outcome IS NOT NULL) AS outcome_set,
            MAX(e.overall_score) AS overall_score
        FROM sentences s
        LEFT JOIN bt_3_skill_events_v2 e
          ON e.session_id = s.session_id
         AND e.sentence_id = s.sentence_id
        LEFT JOIN skill_meta sm
          ON sm.skill_id = e.skill_id
        GROUP BY s.sentence_id
    ),
    final_rows AS (
        SELECT
            s.case_id,
            s.sentence_id,
            s.session_id,
            s.id_for_mistake_table,
            s.unique_id,
            s.sentence,
            s.normalized_sentence,
            s.attempt_at,
            s.translation_score,
            s.user_translation,
            s.feedback,
            p.tested_profile,
            e.errored_rows,
            e.outcome_set,
            e.overall_score
        FROM sentences s
        LEFT JOIN profiles p USING (sentence_id)
        LEFT JOIN errors e USING (sentence_id)
    )
    SELECT json_build_object(
        'case_id', case_id,
        'sentence_id', sentence_id,
        'session_id', session_id,
        'id_for_mistake_table', id_for_mistake_table,
        'unique_id', unique_id,
        'sentence', sentence,
        'normalized_sentence', normalized_sentence,
        'attempt_at', attempt_at,
        'translation_score', translation_score,
        'user_translation', user_translation,
        'feedback', feedback,
        'tested_profile', tested_profile,
        'errored_rows', errored_rows,
        'outcome_set', outcome_set,
        'overall_score', overall_score
    )::text
    FROM final_rows
    ORDER BY normalized_sentence ASC, attempt_at ASC NULLS LAST, sentence_id ASC;
    """


def _fetch_db_backed_rows(
    *,
    database_url: str,
    cases: list[dict[str, Any]],
    user_id: int | None,
    source_lang: str,
    target_lang: str,
    source_session_ids: list[str] | None = None,
) -> tuple[list[dict[str, Any]], str]:
    sql = _build_db_backed_sql(
        cases=cases,
        user_id=user_id,
        source_lang=source_lang,
        target_lang=target_lang,
        source_session_ids=source_session_ids,
    )
    if psycopg2 is not None and RealDictCursor is not None:
        with psycopg2.connect(database_url) as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                rows = cursor.fetchall() or []
        parsed_rows: list[dict[str, Any]] = []
        for row in rows:
            if not row:
                continue
            payload = row[0] if isinstance(row, (list, tuple)) else row
            parsed_rows.append(_normalize_json_value(payload))
        return parsed_rows, "psycopg2"
    return _run_psql_json_rows(database_url, sql), "psql"


def _extract_primary(profile: list[dict[str, Any]]) -> str | None:
    for item in profile:
        if str(item.get("role") or "") == "primary":
            skill_id = str(item.get("skill_id") or "").strip()
            if skill_id:
                return skill_id
    return None


def _extract_secondaries(profile: list[dict[str, Any]]) -> list[str]:
    result: list[str] = []
    for item in profile:
        role = str(item.get("role") or "").strip()
        if role not in {"secondary", "supporting"}:
            continue
        skill_id = str(item.get("skill_id") or "").strip()
        if skill_id:
            result.append(skill_id)
    return result


def _skill_matches(expected_skill_id: str, actual_skill_id: str | None) -> bool:
    if not expected_skill_id:
        return True
    allowed = EXPECTATION_ALIAS_MAP.get(expected_skill_id, {expected_skill_id})
    return bool(actual_skill_id and actual_skill_id in allowed)


def _secondary_overlap(expected_skill_ids: list[str], actual_skill_ids: list[str]) -> tuple[float, list[str]]:
    if not expected_skill_ids:
        return 1.0, []
    actual_set = {str(skill_id or "").strip() for skill_id in actual_skill_ids if str(skill_id or "").strip()}
    matched: list[str] = []
    for expected_skill_id in expected_skill_ids:
        allowed = EXPECTATION_ALIAS_MAP.get(expected_skill_id, {expected_skill_id})
        if actual_set & allowed:
            matched.append(expected_skill_id)
    return len(matched) / len(expected_skill_ids), matched


def _normalize_outcome(outcomes: list[str]) -> str:
    normalized = {str(item or "").strip() for item in outcomes if str(item or "").strip()}
    for candidate in OUTCOME_PRIORITY:
        if candidate in normalized:
            return candidate
    return "unknown"


def _resolve_expected_outcome(
    *,
    expected_outcome_type: str,
    attempt_index: int,
    prior_fail_count: int,
    score_value: int | None,
) -> str:
    normalized = str(expected_outcome_type or "").strip()
    if normalized and normalized != "depends_on_attempt_context":
        return normalized
    score = int(score_value or 0)
    if score >= 85:
        return "recovered_final" if prior_fail_count > 0 or attempt_index > 1 else "clean_success"
    if attempt_index <= 1 and prior_fail_count <= 0:
        return "fail_new"
    return "fail_repeat_context"


def _outcome_matches(expected_outcome: str, actual_outcome: str) -> bool:
    if expected_outcome == "fail_repeat_context":
        return actual_outcome in {"fail_repeat_no_progress", "fail_repeat_progress"}
    return actual_outcome == expected_outcome


def _compare_errored_skills(
    *,
    expected_skill_ids: list[str],
    actual_skill_ids: list[str],
    comparable: bool,
) -> dict[str, Any]:
    if not comparable:
        return {
            "comparable": False,
            "match": None,
            "matched_expected": [],
            "missing_expected": [],
            "unexpected_actual": [],
        }
    actual_set = {skill_id for skill_id in actual_skill_ids if skill_id}
    matched: list[str] = []
    missing: list[str] = []
    for expected_skill_id in expected_skill_ids:
        allowed = EXPECTATION_ALIAS_MAP.get(expected_skill_id, {expected_skill_id})
        if actual_set & allowed:
            matched.append(expected_skill_id)
        else:
            missing.append(expected_skill_id)
    unexpected = [
        skill_id
        for skill_id in sorted(actual_set)
        if not any(skill_id in EXPECTATION_ALIAS_MAP.get(expected_skill_id, {expected_skill_id}) for expected_skill_id in expected_skill_ids)
    ]
    return {
        "comparable": True,
        "match": not missing and not unexpected,
        "matched_expected": matched,
        "missing_expected": missing,
        "unexpected_actual": unexpected,
    }


def _is_noise_primary(skill_id: str | None) -> bool:
    normalized = str(skill_id or "").strip()
    if not normalized:
        return False
    if normalized in NOISE_PRIMARY_SKILL_IDS:
        return True
    return normalized.startswith("de_orthography_spelling") or normalized.startswith("de_punctuation")


def _build_anchor_analysis(source_sentence: str, actual_primary: str | None, actual_profile_skill_ids: list[str]) -> dict[str, Any]:
    anchor_weights, has_structural_anchor = _collect_sentence_anchor_skill_weights(source_sentence)
    anchor_skill_ids = sorted(anchor_weights.keys())
    actual_set = {str(skill_id or "").strip() for skill_id in actual_profile_skill_ids if str(skill_id or "").strip()}
    overlap = sorted(actual_set & set(anchor_skill_ids))
    missed_anchor = bool(has_structural_anchor and not overlap)
    primary_overpromotion = bool(has_structural_anchor and _is_noise_primary(actual_primary))
    return {
        "has_structural_anchor": has_structural_anchor,
        "anchor_skill_ids": anchor_skill_ids,
        "anchor_profile_overlap": overlap,
        "missed_sentence_level_anchor": missed_anchor,
        "noise_primary_overpromotion": primary_overpromotion,
    }


def _build_attempt_report(
    *,
    case: dict[str, Any],
    actual_primary: str | None,
    actual_secondary: list[str],
    actual_errored_skills: list[str],
    actual_outcome_type: str,
    score_value: int | None,
    attempt_index: int,
    prior_fail_count: int,
    metadata: dict[str, Any],
) -> dict[str, Any]:
    secondary_overlap_score, matched_secondary = _secondary_overlap(
        list(case.get("expected_tested_secondary") or []),
        actual_secondary,
    )
    expected_outcome_resolved = _resolve_expected_outcome(
        expected_outcome_type=str(case.get("expected_outcome_type") or ""),
        attempt_index=attempt_index,
        prior_fail_count=prior_fail_count,
        score_value=score_value,
    )
    anchor_analysis = _build_anchor_analysis(
        str(case["source_sentence"]),
        actual_primary=actual_primary,
        actual_profile_skill_ids=([actual_primary] if actual_primary else []) + actual_secondary,
    )
    errored_comparison = _compare_errored_skills(
        expected_skill_ids=list(case.get("expected_errored_skills") or []),
        actual_skill_ids=actual_errored_skills,
        comparable=bool(case.get("expected_errored_skills_comparable")),
    )
    return {
        "attempt_index": attempt_index,
        "prior_fail_count": prior_fail_count,
        "expected_tested_primary": str(case.get("expected_tested_primary") or ""),
        "actual_tested_primary": actual_primary,
        "primary_match": _skill_matches(str(case.get("expected_tested_primary") or ""), actual_primary),
        "expected_tested_secondary": list(case.get("expected_tested_secondary") or []),
        "actual_tested_secondary": actual_secondary,
        "matched_expected_secondary": matched_secondary,
        "secondary_skill_overlap_score": round(float(secondary_overlap_score), 4),
        "expected_errored_skills": list(case.get("expected_errored_skills") or []),
        "actual_errored_skills": actual_errored_skills,
        "errored_skill_comparison": errored_comparison,
        "expected_outcome_type_input": str(case.get("expected_outcome_type") or ""),
        "expected_outcome_type_resolved": expected_outcome_resolved,
        "actual_outcome_type": actual_outcome_type,
        "outcome_match": _outcome_matches(expected_outcome_resolved, actual_outcome_type),
        "actual_score": int(score_value or 0),
        "anchor_analysis": anchor_analysis,
        "metadata": metadata,
    }


def _compute_metrics(per_case: list[dict[str, Any]]) -> dict[str, Any]:
    attempt_reports: list[dict[str, Any]] = [
        attempt
        for case in per_case
        for attempt in list(case.get("attempts") or [])
    ]
    attempt_count = len(attempt_reports)
    if not attempt_count:
        return {
            "evaluated_attempt_count": 0,
            "primary_skill_accuracy": None,
            "secondary_skill_overlap": None,
            "outcome_classification_accuracy": None,
            "noise_primary_overpromotion_count": 0,
            "noise_primary_overpromotion_rate": None,
            "missed_sentence_level_anchor_count": 0,
            "missed_sentence_level_anchor_rate": None,
        }

    primary_matches = sum(1 for item in attempt_reports if bool(item.get("primary_match")))
    secondary_total = sum(float(item.get("secondary_skill_overlap_score") or 0.0) for item in attempt_reports)
    outcome_matches = sum(1 for item in attempt_reports if bool(item.get("outcome_match")))
    overpromotion_count = sum(
        1
        for item in attempt_reports
        if bool((item.get("anchor_analysis") or {}).get("noise_primary_overpromotion"))
    )
    missed_anchor_count = sum(
        1
        for item in attempt_reports
        if bool((item.get("anchor_analysis") or {}).get("missed_sentence_level_anchor"))
    )
    return {
        "evaluated_attempt_count": attempt_count,
        "primary_skill_accuracy": round(primary_matches / attempt_count, 4),
        "secondary_skill_overlap": round(secondary_total / attempt_count, 4),
        "outcome_classification_accuracy": round(outcome_matches / attempt_count, 4),
        "noise_primary_overpromotion_count": overpromotion_count,
        "noise_primary_overpromotion_rate": round(overpromotion_count / attempt_count, 4),
        "missed_sentence_level_anchor_count": missed_anchor_count,
        "missed_sentence_level_anchor_rate": round(missed_anchor_count / attempt_count, 4),
    }


def _build_case_shell(case: dict[str, Any]) -> dict[str, Any]:
    return {
        "case_id": str(case["case_id"]),
        "source_sentence": str(case["source_sentence"]),
        "notes": str(case.get("notes") or ""),
        "attempts": [],
    }


def _build_db_backed_report(
    *,
    cases: list[dict[str, Any]],
    rows: list[dict[str, Any]],
    recent_limit: int,
    local_replay_targeting: bool,
) -> list[dict[str, Any]]:
    rows_by_case: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        rows_by_case[str(row.get("case_id") or "")].append(dict(row))

    per_case: list[dict[str, Any]] = []
    for case in cases:
        case_report = _build_case_shell(case)
        matched_rows = sorted(
            rows_by_case.get(str(case["case_id"]), []),
            key=lambda item: (
                str(item.get("attempt_at") or ""),
                int(item.get("sentence_id") or 0),
            ),
        )
        if recent_limit > 0 and len(matched_rows) > recent_limit:
            matched_rows = matched_rows[-recent_limit:]

        prior_fail_count = 0
        for attempt_index, row in enumerate(matched_rows, start=1):
            tested_profile = _normalize_json_value(row.get("tested_profile"))
            if local_replay_targeting:
                tested_profile = rerank_tested_skill_profile_for_sentence(
                    str(row.get("sentence") or case["source_sentence"]),
                    list(tested_profile or []),
                )
            errored_rows = _normalize_json_value(row.get("errored_rows"))
            outcome_set = _normalize_json_value(row.get("outcome_set"))
            score_value = row.get("translation_score")
            if score_value is None:
                score_value = row.get("overall_score")
            actual_outcome_type = _normalize_outcome(
                [str(item or "").strip() for item in outcome_set if str(item or "").strip()]
            )
            actual_primary = _extract_primary(tested_profile)
            actual_secondary = _extract_secondaries(tested_profile)
            actual_errored_skills = [
                str(item.get("skill_id") or "").strip()
                for item in errored_rows
                if str(item.get("skill_id") or "").strip()
            ]
            case_report["attempts"].append(
                _build_attempt_report(
                    case=case,
                    actual_primary=actual_primary,
                    actual_secondary=actual_secondary,
                    actual_errored_skills=actual_errored_skills,
                    actual_outcome_type=actual_outcome_type,
                    score_value=score_value,
                    attempt_index=attempt_index,
                    prior_fail_count=prior_fail_count,
                    metadata={
                        "mode": "db-backed",
                        "local_replay_targeting": bool(local_replay_targeting),
                        "sentence_id": int(row.get("sentence_id") or 0) or None,
                        "session_id": int(row.get("session_id") or 0) or None,
                        "id_for_mistake_table": int(row.get("id_for_mistake_table") or 0) or None,
                        "attempt_at": row.get("attempt_at"),
                        "user_translation": row.get("user_translation"),
                        "feedback_present": bool(str(row.get("feedback") or "").strip()),
                    },
                )
            )
            if int(score_value or 0) < 85:
                prior_fail_count += 1
        per_case.append(case_report)
    return per_case


def _lookup_latest_sentence_row_with_cursor(
    cursor,
    *,
    user_id: int,
    source_sentence: str,
    source_lang: str,
    target_lang: str,
) -> dict[str, Any] | None:
    cursor.execute(
        """
        SELECT
            id AS sentence_id,
            session_id,
            id_for_mistake_table,
            unique_id,
            sentence
        FROM bt_3_daily_sentences
        WHERE user_id = %s
          AND LOWER(COALESCE(source_lang, 'ru')) = LOWER(%s)
          AND LOWER(COALESCE(target_lang, 'de')) = LOWER(%s)
          AND LOWER(REGEXP_REPLACE(REPLACE(COALESCE(sentence, ''), 'ё', 'е'), '\s+', ' ', 'g')) = %s
        ORDER BY id DESC
        LIMIT 1;
        """,
        (int(user_id), source_lang, target_lang, _normalize_sentence_key(source_sentence)),
    )
    row = cursor.fetchone()
    if not row:
        return None
    return {
        "sentence_id": int(row[0]),
        "session_id": int(row[1]) if row[1] is not None else None,
        "id_for_mistake_table": int(row[2]) if row[2] is not None else None,
        "unique_id": int(row[3]) if row[3] is not None else None,
        "sentence": str(row[4] or ""),
    }


def _load_open_mistake_pairs_with_cursor(
    cursor,
    *,
    user_id: int,
    sentence_id_for_mistake: int,
) -> list[tuple[str, str]]:
    if not sentence_id_for_mistake:
        return []
    cursor.execute(
        """
        SELECT
            COALESCE(NULLIF(main_category, ''), 'Other mistake') AS main_category,
            COALESCE(NULLIF(sub_category, ''), 'Unclassified mistake') AS sub_category
        FROM bt_3_detailed_mistakes
        WHERE user_id = %s
          AND sentence_id = %s
        ORDER BY main_category ASC, sub_category ASC;
        """,
        (int(user_id), int(sentence_id_for_mistake)),
    )
    return [
        (str(main_category or "Other mistake"), str(sub_category or "Unclassified mistake"))
        for main_category, sub_category in (cursor.fetchall() or [])
    ]


def _resolve_direct_tested_targets(
    cursor,
    *,
    case: dict[str, Any],
    user_id: int,
    source_lang: str,
    target_lang: str,
) -> tuple[list[dict[str, Any]], dict[str, Any] | None, str]:
    latest_row = _lookup_latest_sentence_row_with_cursor(
        cursor,
        user_id=user_id,
        source_sentence=str(case["source_sentence"]),
        source_lang=source_lang,
        target_lang=target_lang,
    )
    lookup = dict(case.get("tested_profile_lookup") or {})
    strategy = str(
        lookup.get("strategy")
        or ("generation_seed" if isinstance(case.get("tested_skill_profile_seed"), dict) else "auto")
    ).strip() or "auto"

    if strategy in {"auto", "latest_targets_by_source_sentence"} and latest_row:
        existing_targets = _load_sentence_skill_targets_with_cursor(
            cursor,
            sentence_id=int(latest_row["sentence_id"]),
        )
        if existing_targets:
            return existing_targets, latest_row, "latest_targets_by_source_sentence"

    if strategy in {"auto", "remediation_by_source_sentence"}:
        sentence_id_for_mistake = int(
            lookup.get("sentence_id_for_mistake")
            or case.get("sentence_id_for_mistake")
            or (latest_row or {}).get("id_for_mistake_table")
            or 0
        )
        if sentence_id_for_mistake:
            profile = _build_remediation_profile_with_cursor(
                cursor,
                user_id=int(user_id),
                sentence_id_for_mistake=sentence_id_for_mistake,
                source_lang=source_lang,
                target_lang=target_lang,
                sentence_text=str(case["source_sentence"]),
            )
            if profile:
                return profile, latest_row, "remediation_by_source_sentence"

    if strategy in {"auto", "generation_seed"} and isinstance(case.get("tested_skill_profile_seed"), dict):
        profile = _build_phase1_tested_skill_profile_with_cursor(
            cursor,
            target_lang=target_lang,
            profile_seed=dict(case.get("tested_skill_profile_seed") or {}),
        )
        if profile:
            return profile, latest_row, "generation_seed"

    raise ValueError(
        f"Case {case['case_id']!r} could not resolve tested targets. "
        "Provide tested_profile_lookup or tested_skill_profile_seed."
    )


def _resolve_direct_history_context(
    cursor,
    *,
    case: dict[str, Any],
    user_id: int,
    source_lang: str,
    target_lang: str,
    latest_row: dict[str, Any] | None,
) -> tuple[int, int | None, bool, dict[str, Any] | None, dict[str, dict[str, Any]] | None]:
    history_lookup = dict(case.get("history_lookup") or {})
    history_strategy = str(history_lookup.get("strategy") or "latest_by_source_sentence").strip() or "latest_by_source_sentence"
    history_row = latest_row
    if history_strategy == "latest_by_source_sentence" and latest_row is None:
        history_row = _lookup_latest_sentence_row_with_cursor(
            cursor,
            user_id=user_id,
            source_sentence=str(case["source_sentence"]),
            source_lang=source_lang,
            target_lang=target_lang,
        )

    sentence_pk_id = int((history_row or {}).get("sentence_id") or 0)
    session_id = (history_row or {}).get("session_id")
    if not sentence_pk_id:
        fake_seed = hashlib.md5(
            f"{user_id}:{source_lang}:{target_lang}:{case['source_sentence']}".encode("utf-8")
        ).hexdigest()
        sentence_pk_id = -1 * (int(fake_seed[:8], 16) or 1)

    sentence_id_for_mistake = int(
        history_lookup.get("sentence_id_for_mistake")
        or case.get("sentence_id_for_mistake")
        or (history_row or {}).get("id_for_mistake_table")
        or 0
    )
    was_in_mistakes = bool(_load_open_mistake_pairs_with_cursor(
        cursor,
        user_id=user_id,
        sentence_id_for_mistake=sentence_id_for_mistake,
    ))
    previous_shadow_state = None
    if sentence_pk_id > 0:
        previous_shadow_state = _load_sentence_skill_shadow_state_with_cursor(
            cursor,
            sentence_id=sentence_pk_id,
        )
    fallback_previous_errored_details = None
    if not previous_shadow_state and was_in_mistakes:
        previous_pairs = _load_open_mistake_pairs_with_cursor(
            cursor,
            user_id=user_id,
            sentence_id_for_mistake=sentence_id_for_mistake,
        )
        fallback_previous_errored_details = _build_errored_skill_details(
            error_pairs=previous_pairs,
            target_lang=target_lang,
        )
    return sentence_pk_id, session_id, was_in_mistakes, previous_shadow_state, fallback_previous_errored_details


async def _evaluate_direct_case(
    *,
    case: dict[str, Any],
    default_user_id: int,
    llm_model: str,
) -> dict[str, Any]:
    user_translation = str(case.get("user_translation") or "").strip()
    if not user_translation or user_translation == "FROM_DB":
        raise ValueError(f"Case {case['case_id']!r} requires explicit user_translation in direct mode.")

    source_lang = str(case.get("source_lang") or "ru").strip() or "ru"
    target_lang = str(case.get("target_lang") or "de").strip() or "de"
    user_id = int(case.get("user_id") or default_user_id)

    with _temporary_env("LLM_TASK_MODEL_CHECK_TRANSLATION_MULTILANG", llm_model):
        _feedback, categories, subcategories, score_value, correct_translation = await check_translation(
            original_text=str(case["source_sentence"]),
            user_translation=user_translation,
            sentence_number=None,
            source_lang=source_lang,
            target_lang=target_lang,
        )

    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        tested_targets, latest_row, tested_profile_source = _resolve_direct_tested_targets(
            cursor,
            case=case,
            user_id=user_id,
            source_lang=source_lang,
            target_lang=target_lang,
        )
        sentence_pk_id, session_id, was_in_mistakes, previous_shadow_state, fallback_previous_errored_details = _resolve_direct_history_context(
            cursor,
            case=case,
            user_id=user_id,
            source_lang=source_lang,
            target_lang=target_lang,
            latest_row=latest_row,
        )
        current_error_pairs = _normalize_category_pairs(
            list(categories or []),
            list(subcategories or []),
            target_lang=target_lang,
            fallback_to_other=False,
        )
        current_errored_details = _build_errored_skill_details(
            error_pairs=current_error_pairs,
            target_lang=target_lang,
        )
        event_rows, _shadow_state = _build_phase1_shadow_payload(
            user_id=user_id,
            sentence_pk_id=sentence_pk_id,
            session_id=int(session_id) if session_id is not None else None,
            source_lang=source_lang,
            target_lang=target_lang,
            score_value=int(score_value or 0),
            was_in_mistakes=was_in_mistakes,
            tested_targets=tested_targets,
            previous_shadow_state=previous_shadow_state,
            current_errored_details=current_errored_details,
            fallback_previous_errored_details=fallback_previous_errored_details,
        )
    finally:
        try:
            cursor.close()
        except Exception:
            pass
        conn.close()

    actual_outcome_type = _normalize_outcome([str(row[17]) for row in event_rows if len(row) > 17])
    actual_errored_skills = [
        str(row[11])
        for row in event_rows
        if len(row) > 15 and bool(row[15])
    ]
    actual_primary = _extract_primary(tested_targets)
    actual_secondary = _extract_secondaries(tested_targets)
    prior_fail_count = 1 if previous_shadow_state and int(previous_shadow_state.get("last_score") or 0) < 85 else 0
    attempt_index = int(previous_shadow_state.get("last_attempt_no") or 0) + 1 if previous_shadow_state else 1

    case_report = _build_case_shell(case)
    case_report["attempts"].append(
        _build_attempt_report(
            case=case,
            actual_primary=actual_primary,
            actual_secondary=actual_secondary,
            actual_errored_skills=actual_errored_skills,
            actual_outcome_type=actual_outcome_type,
            score_value=score_value,
            attempt_index=attempt_index,
            prior_fail_count=prior_fail_count,
            metadata={
                "mode": "direct",
                "evaluated_via": "check_translation + production shadow helpers",
                "tested_profile_source": tested_profile_source,
                "sentence_pk_id": sentence_pk_id,
                "reference_session_id": session_id,
                "matched_existing_sentence_id": (latest_row or {}).get("sentence_id"),
                "correct_translation": correct_translation,
                "categories": list(categories or []),
                "subcategories": list(subcategories or []),
                "normalized_error_pairs": [list(pair) for pair in current_error_pairs],
                "was_in_mistakes": was_in_mistakes,
            },
        )
    )
    return case_report


async def _run_direct_mode(
    *,
    cases: list[dict[str, Any]],
    user_id: int,
    llm_model: str,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for case in cases:
        results.append(
            await _evaluate_direct_case(
                case=case,
                default_user_id=user_id,
                llm_model=llm_model,
            )
        )
    return results


def _format_metric(value: Any) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:.4f}"
    return str(value)


def _build_markdown_report(
    *,
    input_file: str,
    mode: str,
    metrics: dict[str, Any],
    per_case: list[dict[str, Any]],
    meta: dict[str, Any],
) -> str:
    lines: list[str] = []
    lines.append("# Semantic Skill Benchmark")
    lines.append("")
    lines.append(f"- Mode: `{mode}`")
    lines.append(f"- Input: `{input_file}`")
    lines.append(f"- Cases: `{len(per_case)}`")
    lines.append(f"- Evaluated attempts: `{metrics.get('evaluated_attempt_count')}`")
    lines.append(f"- Reused production functions: `{', '.join(REUSED_PRODUCTION_FUNCTIONS)}`")
    lines.append("")
    lines.append("## Metrics")
    lines.append("")
    lines.append(f"- `primary_skill_accuracy`: `{_format_metric(metrics.get('primary_skill_accuracy'))}`")
    lines.append(f"- `secondary_skill_overlap`: `{_format_metric(metrics.get('secondary_skill_overlap'))}`")
    lines.append(f"- `outcome_classification_accuracy`: `{_format_metric(metrics.get('outcome_classification_accuracy'))}`")
    lines.append(f"- `noise_primary_overpromotion_count`: `{_format_metric(metrics.get('noise_primary_overpromotion_count'))}`")
    lines.append(f"- `noise_primary_overpromotion_rate`: `{_format_metric(metrics.get('noise_primary_overpromotion_rate'))}`")
    lines.append(f"- `missed_sentence_level_anchor_count`: `{_format_metric(metrics.get('missed_sentence_level_anchor_count'))}`")
    lines.append(f"- `missed_sentence_level_anchor_rate`: `{_format_metric(metrics.get('missed_sentence_level_anchor_rate'))}`")
    lines.append("")

    if meta:
        lines.append("## Run Meta")
        lines.append("")
        for key, value in meta.items():
            if value is None or value == "":
                continue
            lines.append(f"- `{key}`: `{value}`")
        lines.append("")

    lines.append("## Per Case")
    lines.append("")
    for case in per_case:
        lines.append(f"### {case['case_id']}")
        lines.append("")
        lines.append(f"- Source sentence: {case['source_sentence']}")
        if case.get("notes"):
            lines.append(f"- Notes: {case['notes']}")
        if not case.get("attempts"):
            lines.append("- No matching attempts found.")
            lines.append("")
            continue
        for attempt in case["attempts"]:
            metadata = dict(attempt.get("metadata") or {})
            lines.append(
                f"- Attempt `{attempt['attempt_index']}`:"
                f" primary `{attempt.get('actual_tested_primary')}`"
                f" vs expected `{attempt.get('expected_tested_primary')}`"
                f" -> `{'match' if attempt.get('primary_match') else 'mismatch'}`;"
                f" secondary overlap `{_format_metric(attempt.get('secondary_skill_overlap_score'))}`;"
                f" outcome `{attempt.get('actual_outcome_type')}`"
                f" vs expected `{attempt.get('expected_outcome_type_resolved')}`"
                f" -> `{'correct' if attempt.get('outcome_match') else 'incorrect'}`."
            )
            lines.append(f"- Actual secondaries: `{', '.join(attempt.get('actual_tested_secondary') or []) or 'none'}`")
            lines.append(f"- Actual errored skills: `{', '.join(attempt.get('actual_errored_skills') or []) or 'none'}`")
            anchor_analysis = dict(attempt.get("anchor_analysis") or {})
            lines.append(
                f"- Anchor analysis: has_structural_anchor=`{anchor_analysis.get('has_structural_anchor')}`,"
                f" overlap=`{', '.join(anchor_analysis.get('anchor_profile_overlap') or []) or 'none'}`,"
                f" missed=`{anchor_analysis.get('missed_sentence_level_anchor')}`,"
                f" noise_primary_overpromotion=`{anchor_analysis.get('noise_primary_overpromotion')}`"
            )
            if metadata:
                summary_bits: list[str] = []
                for key in (
                    "mode",
                    "session_id",
                    "sentence_id",
                    "attempt_at",
                    "tested_profile_source",
                    "was_in_mistakes",
                ):
                    if key in metadata and metadata.get(key) not in (None, ""):
                        summary_bits.append(f"{key}={metadata.get(key)}")
                if summary_bits:
                    lines.append(f"- Metadata: `{'; '.join(summary_bits)}`")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def main() -> int:
    args = _parse_args()
    defaults, cases = _load_benchmark_cases(args.input_file)
    output_json_path = Path(args.output_json)
    output_md_path = Path(args.output_md) if args.output_md else output_json_path.with_suffix(".md")

    if args.mode == "db-backed":
        database_url = os.getenv(args.database_url_env, "").strip()
        if not database_url:
            raise SystemExit(f"Missing database URL in env var {args.database_url_env!r}")
        rows, driver = _fetch_db_backed_rows(
            database_url=database_url,
            cases=cases,
            user_id=None if bool(args.all_users) else int(args.user_id),
            source_lang=args.source_lang,
            target_lang=args.target_lang,
            source_session_ids=_parse_csv_text_list(args.source_session_ids),
        )
        per_case = _build_db_backed_report(
            cases=cases,
            rows=rows,
            recent_limit=max(1, int(args.recent_limit)),
            local_replay_targeting=bool(args.local_replay_targeting),
        )
        meta = {
            "driver": driver,
            "user_id": None if bool(args.all_users) else int(args.user_id),
            "all_users": bool(args.all_users),
            "source_lang": args.source_lang,
            "target_lang": args.target_lang,
            "recent_limit_per_case": int(args.recent_limit),
            "source_session_ids": _parse_csv_text_list(args.source_session_ids),
            "local_replay_targeting": bool(args.local_replay_targeting),
        }
    else:
        if psycopg2 is None:
            raise SystemExit("Direct mode requires a Python environment with psycopg2 available.")
        per_case = asyncio.run(
            _run_direct_mode(
                cases=cases,
                user_id=int(args.user_id),
                llm_model=str(args.llm_model or "").strip(),
            )
        )
        meta = {
            "driver": "production_python_helpers",
            "user_id": int(args.user_id),
            "source_lang": args.source_lang,
            "target_lang": args.target_lang,
            "llm_model_override": str(args.llm_model or "").strip() or None,
        }

    metrics = _compute_metrics(per_case)
    result = {
        "meta": {
            "mode": args.mode,
            "input_file": str(Path(args.input_file).resolve()),
            "defaults": defaults,
            "reused_production_functions": REUSED_PRODUCTION_FUNCTIONS,
            **meta,
        },
        "per_case": per_case,
        "metrics": metrics,
    }

    output_json_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    output_md_path.write_text(
        _build_markdown_report(
            input_file=str(Path(args.input_file).resolve()),
            mode=args.mode,
            metrics=metrics,
            per_case=per_case,
            meta=meta,
        ),
        encoding="utf-8",
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    print(f"JSON report: {output_json_path.resolve()}")
    print(f"Markdown report: {output_md_path.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
