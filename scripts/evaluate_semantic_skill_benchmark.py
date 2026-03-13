#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
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


BENCHMARK_CASES: list[dict[str, Any]] = [
    {
        "case_id": 1,
        "source_sentence": "Несмотря на нестабильную экономическую ситуацию, компания решила инвестировать значительные средства в разработку инновационных решений для отечественного рынка.",
        "expected_primary": "prepositions_governing_cases",
        "expected_secondary": ["cases_dative", "noun_phrases_complex"],
        "expected_outcome": "clean_success",
    },
    {
        "case_id": 2,
        "source_sentence": "Несмотря на отсутствие официальных заявлений, кажется, что обсуждение новых законопроектов проводится уже несколько недель.",
        "expected_primary": "subordinate_clause_word_order",
        "expected_secondary": ["impersonal_constructions", "present_perfect_vs_present"],
        "expected_outcome": "clean_success",
    },
    {
        "case_id": 3,
        "source_sentence": "Честно говоря, многие люди считают, что новости должны быть представлены так, чтобы их мог понять каждый.",
        "expected_primary": "subordinate_clause_word_order",
        "expected_secondary": ["modal_verbs_usage", "relative_clause_structure"],
        "expected_outcome": "clean_success",
    },
    {
        "case_id": 4,
        "source_sentence": "Честно говоря, мне сложно понять, почему, несмотря на всю известность художника, его поздние работы до сих пор остаются недооценёнными критиками.",
        "expected_primary": "subordinate_clause_word_order",
        "expected_secondary": ["passive_voice", "participial_adjectives"],
        "expected_outcome": "clean_success",
    },
    {
        "case_id": 5,
        "source_sentence": "Хотя казалось бы, что после стольких лет поиска индивидуального стиля автор наконец-то достиг своей цели, многие по-прежнему спорят о подлинности его новейших картин.",
        "expected_primary": "concessive_clauses_word_order",
        "expected_secondary": ["perfect_tense_usage", "genitive_possession"],
        "expected_outcome": "clean_success",
    },
    {
        "case_id": 6,
        "source_sentence": "В условиях жёсткой конкуренции на глобальном рынке предприятия вынуждены повышать свою операционную эффективность и внедрять инновационные подходы к управлению проектами.",
        "expected_primary": "infinitive_structures",
        "expected_secondary": ["noun_compounds", "modal_necessity_structures"],
        "expected_outcome": "clean_success",
    },
    {
        "case_id": 7,
        "source_sentence": "Честно говоря, если бы не моя встреча с этим художником, я бы так никогда и не стал интересоваться абстрактным искусством.",
        "expected_primary": "conditional_konjunktiv_ii",
        "expected_secondary": ["verb_prefix_usage", "reflexive_verbs"],
        "expected_outcome": "clean_success",
    },
]

EXPECTATION_ALIAS_MAP: dict[str, set[str]] = {
    "prepositions_governing_cases": {
        "prepositions_usage",
        "de_cases_case_after_preposition",
        "cases_preposition_dative",
        "cases_preposition_genitive",
        "cases_preposition_accusative",
    },
    "cases_dative": {"cases_dative", "cases_preposition_dative"},
    "noun_phrases_complex": {
        "nouns_compounds",
        "nouns_declension",
        "de_cases_case_agreement_in_noun_phrase",
        "adjectives_case_agreement",
    },
    "subordinate_clause_word_order": {
        "word_order_subordinate_clause",
        "word_order_modal_structure",
        "verbs_placement_subordinate",
        "de_clauses_sentence_types_main_vs_subordinate_clause",
        "de_clauses_sentence_types_concessive_clauses_obwohl",
    },
    "impersonal_constructions": {
        "de_clauses_sentence_types_main_vs_subordinate_clause",
        "verbs_auxiliaries",
    },
    "present_perfect_vs_present": {
        "verbs_auxiliaries",
        "de_word_order_placement_of_participle_perfekt_passive",
    },
    "modal_verbs_usage": {"verbs_modals", "word_order_modal_structure"},
    "relative_clause_structure": {
        "de_clauses_sentence_types_relative_clauses",
        "de_pronouns_relative_pronouns",
        "word_order_subordinate_clause",
    },
    "passive_voice": {
        "de_voice_active_passive_vorgangspassiv_werden_partizip_ii",
        "de_voice_active_passive_zustandspassiv_sein_partizip_ii",
        "de_voice_active_passive_passive_word_order",
    },
    "participial_adjectives": {
        "de_infinitive_participles_partizip_ii",
        "adjectives_case_agreement",
    },
    "concessive_clauses_word_order": {
        "de_clauses_sentence_types_concessive_clauses_obwohl",
        "word_order_subordinate_clause",
        "de_clauses_sentence_types_main_vs_subordinate_clause",
    },
    "perfect_tense_usage": {
        "verbs_auxiliaries",
        "de_word_order_placement_of_participle_perfekt_passive",
    },
    "genitive_possession": {
        "cases_genitive",
        "de_pronouns_possessive_pronouns",
    },
    "infinitive_structures": {
        "de_infinitive_participles_zu_infinitive",
        "de_clauses_sentence_types_infinitive_clauses_vs_dass_clause",
    },
    "noun_compounds": {"nouns_compounds"},
    "modal_necessity_structures": {
        "verbs_modals",
        "de_infinitive_participles_infinitive_with_modal_verbs",
    },
    "conditional_konjunktiv_ii": {
        "moods_subjunctive2",
        "de_moods_konjunktiv_ii_wuerde_form",
        "de_clauses_sentence_types_conditionals_wenn_falls",
    },
    "verb_prefix_usage": {
        "de_verbs_inseparable_prefix_verbs",
        "de_word_order_placement_of_separable_prefix",
    },
    "reflexive_verbs": {
        "verbs_reflexive",
        "de_pronouns_reflexive_pronouns",
    },
}

OUTCOME_EQUIVALENCE: dict[str, set[str]] = {
    "clean_success": {"clean_success", "recovered_final"},
    "recovered_final": {"recovered_final"},
    "fail_new": {"fail_new"},
    "fail_repeat_no_progress": {"fail_repeat_no_progress"},
    "fail_repeat_progress": {"fail_repeat_progress"},
}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Evaluate the latest semantic skill benchmark against the newest production rerun.",
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
        help="User id to inspect.",
    )
    parser.add_argument(
        "--output",
        default="semantic_skill_benchmark_results.json",
        help="Output JSON path.",
    )
    return parser.parse_args()


def _normalize_json_value(value: Any) -> Any:
    if value is None:
        return []
    if isinstance(value, (list, dict)):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        return json.loads(text)
    return value


def _normalize_outcome(outcome_set: list[str]) -> str:
    normalized = {str(item or "").strip() for item in outcome_set if str(item or "").strip()}
    for candidate in (
        "clean_success",
        "recovered_final",
        "fail_repeat_no_progress",
        "fail_repeat_progress",
        "fail_new",
        "clean_neutral",
        "untargeted_error_fail",
    ):
        if candidate in normalized:
            return candidate
    return "unknown"


def _fetch_latest_sessions(conn, user_id: int) -> list[dict[str, Any]]:
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(
            """
            WITH recent_sessions AS (
                SELECT
                    session_id,
                    MAX(created_at) AS last_event_at,
                    COUNT(DISTINCT sentence_id) AS sentence_count
                FROM bt_3_skill_events_v2
                WHERE user_id = %s
                  AND session_id IS NOT NULL
                GROUP BY session_id
            )
            SELECT session_id, last_event_at
            FROM recent_sessions
            WHERE sentence_count = 7
            ORDER BY last_event_at DESC
            LIMIT 2;
            """,
            (int(user_id),),
        )
        rows = cursor.fetchall() or []
    return [dict(row) for row in rows]


def _fetch_sentence_payloads(conn, user_id: int, session_ids: list[int]) -> list[dict[str, Any]]:
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(
            """
            WITH target_sessions AS (
                SELECT unnest(%s::bigint[]) AS session_id
            ),
            sentences AS (
                SELECT
                    ds.session_id::bigint AS session_id,
                    ds.id AS sentence_id,
                    ds.id_for_mistake_table,
                    ds.unique_id,
                    ds.sentence
                FROM bt_3_daily_sentences ds
                JOIN target_sessions ts ON ts.session_id = ds.session_id::bigint
                WHERE ds.user_id = %s
            ),
            skill_meta AS (
                SELECT
                    k.skill_id,
                    k.title,
                    m.mastery_group_id,
                    COALESCE(m.is_mastery_leaf, false) AS is_mastery_leaf,
                    COALESCE(m.is_diagnostic_only, false) AS is_diagnostic_only
                FROM bt_3_skills k
                LEFT JOIN bt_3_skill_mastery_group_members m
                  ON m.language_code = k.language_code
                 AND m.diagnostic_skill_id = k.skill_id
                WHERE k.language_code = 'de'
            ),
            profiles AS (
                SELECT
                    s.session_id,
                    s.sentence_id,
                    json_agg(
                        json_build_object(
                            'skill_id', t.skill_id,
                            'title', sm.title,
                            'role', t.role,
                            'role_rank', t.role_rank,
                            'profile_source', t.profile_source,
                            'profile_confidence', t.profile_confidence,
                            'mastery_group_id', sm.mastery_group_id,
                            'is_mastery_leaf', sm.is_mastery_leaf,
                            'is_diagnostic_only', sm.is_diagnostic_only
                        )
                        ORDER BY t.role_rank ASC, t.skill_id ASC
                    ) FILTER (WHERE t.skill_id IS NOT NULL) AS tested_profile
                FROM sentences s
                LEFT JOIN bt_3_sentence_skill_targets t ON t.sentence_id = s.sentence_id
                LEFT JOIN skill_meta sm ON sm.skill_id = t.skill_id
                GROUP BY s.session_id, s.sentence_id
            ),
            errors AS (
                SELECT
                    e.session_id,
                    e.sentence_id,
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
                FROM bt_3_skill_events_v2 e
                JOIN sentences s ON s.session_id = e.session_id AND s.sentence_id = e.sentence_id
                LEFT JOIN skill_meta sm ON sm.skill_id = e.skill_id
                GROUP BY e.session_id, e.sentence_id
            )
            SELECT
                s.session_id,
                s.sentence_id,
                s.id_for_mistake_table,
                s.unique_id,
                s.sentence,
                p.tested_profile,
                e.errored_rows,
                e.outcome_set,
                e.overall_score
            FROM sentences s
            LEFT JOIN profiles p USING (session_id, sentence_id)
            LEFT JOIN errors e USING (session_id, sentence_id)
            ORDER BY s.session_id DESC, s.unique_id ASC, s.sentence_id ASC;
            """,
            (list(session_ids), int(user_id)),
        )
        rows = cursor.fetchall() or []
    return [dict(row) for row in rows]


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


def _fetch_latest_sessions_psql(database_url: str, user_id: int) -> list[dict[str, Any]]:
    sql = f"""
    WITH recent_sessions AS (
        SELECT
            session_id,
            MAX(created_at) AS last_event_at,
            COUNT(DISTINCT sentence_id) AS sentence_count
        FROM bt_3_skill_events_v2
        WHERE user_id = {int(user_id)}
          AND session_id IS NOT NULL
        GROUP BY session_id
    )
    SELECT json_build_object(
        'session_id', session_id,
        'last_event_at', last_event_at
    )::text
    FROM recent_sessions
    WHERE sentence_count = 7
    ORDER BY last_event_at DESC
    LIMIT 2;
    """
    return _run_psql_json_rows(database_url, sql)


def _fetch_sentence_payloads_psql(database_url: str, user_id: int, session_ids: list[int]) -> list[dict[str, Any]]:
    session_id_sql = ", ".join(str(int(session_id)) for session_id in session_ids)
    sql = f"""
    WITH target_sessions AS (
        SELECT unnest(ARRAY[{session_id_sql}]::bigint[]) AS session_id
    ),
    sentences AS (
        SELECT
            ds.session_id::bigint AS session_id,
            ds.id AS sentence_id,
            ds.id_for_mistake_table,
            ds.unique_id,
            ds.sentence
        FROM bt_3_daily_sentences ds
        JOIN target_sessions ts ON ts.session_id = ds.session_id::bigint
        WHERE ds.user_id = {int(user_id)}
    ),
    skill_meta AS (
        SELECT
            k.skill_id,
            k.title,
            m.mastery_group_id,
            COALESCE(m.is_mastery_leaf, false) AS is_mastery_leaf,
            COALESCE(m.is_diagnostic_only, false) AS is_diagnostic_only
        FROM bt_3_skills k
        LEFT JOIN bt_3_skill_mastery_group_members m
          ON m.language_code = k.language_code
         AND m.diagnostic_skill_id = k.skill_id
        WHERE k.language_code = 'de'
    ),
    profiles AS (
        SELECT
            s.session_id,
            s.sentence_id,
            json_agg(
                json_build_object(
                    'skill_id', t.skill_id,
                    'title', sm.title,
                    'role', t.role,
                    'role_rank', t.role_rank,
                    'profile_source', t.profile_source,
                    'profile_confidence', t.profile_confidence,
                    'mastery_group_id', sm.mastery_group_id,
                    'is_mastery_leaf', sm.is_mastery_leaf,
                    'is_diagnostic_only', sm.is_diagnostic_only
                )
                ORDER BY t.role_rank ASC, t.skill_id ASC
            ) FILTER (WHERE t.skill_id IS NOT NULL) AS tested_profile
        FROM sentences s
        LEFT JOIN bt_3_sentence_skill_targets t ON t.sentence_id = s.sentence_id
        LEFT JOIN skill_meta sm ON sm.skill_id = t.skill_id
        GROUP BY s.session_id, s.sentence_id
    ),
    errors AS (
        SELECT
            e.session_id,
            e.sentence_id,
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
        FROM bt_3_skill_events_v2 e
        JOIN sentences s ON s.session_id = e.session_id AND s.sentence_id = e.sentence_id
        LEFT JOIN skill_meta sm ON sm.skill_id = e.skill_id
        GROUP BY e.session_id, e.sentence_id
    )
    SELECT json_build_object(
        'session_id', s.session_id,
        'sentence_id', s.sentence_id,
        'id_for_mistake_table', s.id_for_mistake_table,
        'unique_id', s.unique_id,
        'sentence', s.sentence,
        'tested_profile', p.tested_profile,
        'errored_rows', e.errored_rows,
        'outcome_set', e.outcome_set,
        'overall_score', e.overall_score
    )::text
    FROM sentences s
    LEFT JOIN profiles p USING (session_id, sentence_id)
    LEFT JOIN errors e USING (session_id, sentence_id)
    ORDER BY s.session_id DESC, s.unique_id ASC, s.sentence_id ASC;
    """
    return _run_psql_json_rows(database_url, sql)


def _extract_primary(profile: list[dict[str, Any]]) -> str | None:
    for item in profile:
        if str(item.get("role") or "") == "primary":
            return str(item.get("skill_id") or "").strip() or None
    return None


def _extract_secondaries(profile: list[dict[str, Any]]) -> list[str]:
    rows = [
        item for item in profile
        if str(item.get("role") or "") in {"secondary", "supporting"}
    ]
    return [str(item.get("skill_id") or "").strip() for item in rows if str(item.get("skill_id") or "").strip()]


def _primary_matches(expected_label: str, actual_skill_id: str | None) -> bool:
    allowed = EXPECTATION_ALIAS_MAP.get(expected_label, {expected_label})
    return bool(actual_skill_id and actual_skill_id in allowed)


def _secondary_overlap(expected_labels: list[str], actual_skill_ids: list[str]) -> tuple[float, list[str]]:
    matched_labels: list[str] = []
    actual_set = {str(skill_id or "").strip() for skill_id in actual_skill_ids if str(skill_id or "").strip()}
    for label in expected_labels:
        allowed = EXPECTATION_ALIAS_MAP.get(label, {label})
        if actual_set & allowed:
            matched_labels.append(label)
    if not expected_labels:
        return 1.0, matched_labels
    return len(matched_labels) / len(expected_labels), matched_labels


def _outcome_matches(expected_outcome: str, actual_outcome: str) -> bool:
    return actual_outcome in OUTCOME_EQUIVALENCE.get(expected_outcome, {expected_outcome})


def _build_case_report(case: dict[str, Any], fail_row: dict[str, Any] | None, recovery_row: dict[str, Any] | None) -> dict[str, Any]:
    recovery_profile = _normalize_json_value((recovery_row or {}).get("tested_profile"))
    recovery_errors = _normalize_json_value((recovery_row or {}).get("errored_rows"))
    recovery_outcomes = _normalize_json_value((recovery_row or {}).get("outcome_set"))
    fail_outcomes = _normalize_json_value((fail_row or {}).get("outcome_set"))

    actual_primary = _extract_primary(recovery_profile)
    actual_secondary = _extract_secondaries(recovery_profile)
    primary_match = _primary_matches(str(case["expected_primary"]), actual_primary)
    secondary_overlap_score, matched_secondaries = _secondary_overlap(
        list(case["expected_secondary"]),
        actual_secondary,
    )
    actual_outcome = _normalize_outcome(list(recovery_outcomes))
    outcome_correct = _outcome_matches(str(case["expected_outcome"]), actual_outcome)

    return {
        "case_id": int(case["case_id"]),
        "source_sentence": str(case["source_sentence"]),
        "fail_session_id": int((fail_row or {}).get("session_id") or 0) or None,
        "recovery_session_id": int((recovery_row or {}).get("session_id") or 0) or None,
        "expected_primary": str(case["expected_primary"]),
        "actual_primary": actual_primary,
        "primary_match": primary_match,
        "expected_secondary": list(case["expected_secondary"]),
        "actual_secondary": actual_secondary,
        "matched_expected_secondary": matched_secondaries,
        "secondary_overlap_score": round(float(secondary_overlap_score), 3),
        "expected_outcome": str(case["expected_outcome"]),
        "actual_outcome": actual_outcome,
        "outcome_correct": outcome_correct,
        "fail_outcome_raw": _normalize_outcome(list(fail_outcomes)),
        "recovery_errored_skills": [
            str(item.get("skill_id") or "").strip()
            for item in recovery_errors
            if str(item.get("skill_id") or "").strip()
        ],
        "recovery_overall_score": (recovery_row or {}).get("overall_score"),
    }


def main() -> int:
    args = _parse_args()
    database_url = os.getenv(args.database_url_env, "").strip()
    if not database_url:
        raise SystemExit(f"Missing database URL in env var {args.database_url_env!r}")

    if psycopg2 is not None:
        with psycopg2.connect(database_url) as conn:
            sessions = _fetch_latest_sessions(conn, user_id=int(args.user_id))
            if len(sessions) < 2:
                raise SystemExit("Need at least two 7-sentence sessions to compare.")
            recovery_session_id = int(sessions[0]["session_id"])
            fail_session_id = int(sessions[1]["session_id"])
            rows = _fetch_sentence_payloads(
                conn,
                user_id=int(args.user_id),
                session_ids=[fail_session_id, recovery_session_id],
            )
        driver = "psycopg2"
    else:
        sessions = _fetch_latest_sessions_psql(database_url, user_id=int(args.user_id))
        if len(sessions) < 2:
            raise SystemExit("Need at least two 7-sentence sessions to compare.")
        recovery_session_id = int(sessions[0]["session_id"])
        fail_session_id = int(sessions[1]["session_id"])
        rows = _fetch_sentence_payloads_psql(
            database_url,
            user_id=int(args.user_id),
            session_ids=[fail_session_id, recovery_session_id],
        )
        driver = "psql"

    rows_by_sentence_and_session = {
        (str(row.get("sentence") or "").strip(), int(row.get("session_id") or 0)): row
        for row in rows
    }

    per_case: list[dict[str, Any]] = []
    primary_matches = 0
    secondary_overlap_total = 0.0
    outcome_matches = 0
    for case in BENCHMARK_CASES:
        sentence = str(case["source_sentence"]).strip()
        fail_row = rows_by_sentence_and_session.get((sentence, fail_session_id))
        recovery_row = rows_by_sentence_and_session.get((sentence, recovery_session_id))
        case_report = _build_case_report(case, fail_row=fail_row, recovery_row=recovery_row)
        per_case.append(case_report)
        primary_matches += 1 if case_report["primary_match"] else 0
        secondary_overlap_total += float(case_report["secondary_overlap_score"])
        outcome_matches += 1 if case_report["outcome_correct"] else 0

    case_count = max(1, len(per_case))
    result = {
        "meta": {
            "user_id": int(args.user_id),
            "fail_session_id": fail_session_id,
            "recovery_session_id": recovery_session_id,
            "case_count": len(per_case),
            "driver": driver,
        },
        "per_case": per_case,
        "metrics": {
            "primary_skill_accuracy": round(primary_matches / case_count, 4),
            "secondary_skill_overlap": round(secondary_overlap_total / case_count, 4),
            "outcome_classification_accuracy": round(outcome_matches / case_count, 4),
        },
    }
    output_path = Path(args.output)
    output_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
