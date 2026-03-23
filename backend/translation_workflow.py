import asyncio
import ast
import hashlib
import logging
import os
import re
import json
import time
from datetime import datetime
from typing import Any
from urllib.parse import quote

import openai
import psycopg2
from psycopg2.extras import Json, execute_values

from backend.config_mistakes_data import (
    VALID_CATEGORIES,
    VALID_SUBCATEGORIES,
)
from backend.grammar_focuses import focus_matches_error_pair, resolve_webapp_focus
from backend.openai_manager import (
    generate_sentences_multilang,
    llm_execute,
    run_check_translation_multilang,
    run_check_translation_story,
    run_check_story_guess_semantic,
)
from backend.analytics import _calculate_final_score
from backend.database import (
    get_db_connection,
    get_db_connection_context,
    apply_skill_events_for_error,
    close_stale_open_translation_sessions_for_user,
    get_skill_mapping_for_error,
    update_translation_check_item_result,
    delete_translation_draft_state,
    build_translation_session_minutes_sql,
    enforce_feature_limit,
)

PHASE1_SKILL_ROLE_WEIGHTS = {
    "primary": 1.0,
    "secondary": 0.65,
    "supporting": 0.35,
}
PHASE1_MAX_UNTARGETED_ERROR_SKILLS = 4
PHASE1_MISSING_PROFILE_SOURCE = "missing"
PHASE1_MISSING_PROFILE_CONFIDENCE = 0.25
PHASE1_CATASTROPHIC_FAIL_SCORE_THRESHOLD = 20
PHASE1_CATASTROPHIC_FAIL_MAP_WEIGHT = 0.6
AUTHORED_PROFILE_SOURCE = "authored_generation"
AUTHORED_PROFILE_CONFIDENCE = 1.0
REMEDIATION_PROFILE_SOURCE = "remediation_history"
REMEDIATION_PROFILE_CONFIDENCE = 0.85


def _extract_nested_error_message(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, dict):
        for key in ("error", "message", "detail", "reason"):
            nested = _extract_nested_error_message(value.get(key))
            if nested:
                return nested
        return ""
    if isinstance(value, (list, tuple, set)):
        for item in value:
            nested = _extract_nested_error_message(item)
            if nested:
                return nested
        return ""
    text = str(value).strip()
    if not text:
        return ""
    if text[:1] in "{[" and text[-1:] in "}]":
        for parser in (json.loads, ast.literal_eval):
            try:
                parsed = parser(text)
            except Exception:
                continue
            nested = _extract_nested_error_message(parsed)
            if nested:
                return nested
    return text


def finalize_open_translation_sessions() -> dict[str, int]:
    """Force-close unfinished translation sessions that already have issued sentences."""
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE bt_3_user_progress up
                SET
                    active_seconds = COALESCE(up.active_seconds, 0)
                        + CASE
                            WHEN COALESCE(up.active_running, FALSE) = TRUE
                             AND up.active_started_at IS NOT NULL
                                THEN GREATEST(
                                    0,
                                    EXTRACT(EPOCH FROM (NOW() - up.active_started_at))::BIGINT
                                )
                            ELSE 0
                        END,
                    active_started_at = NULL,
                    active_running = FALSE,
                    end_time = NOW(),
                    completed = TRUE
                WHERE up.completed = FALSE
                  AND EXISTS (
                    SELECT 1
                    FROM bt_3_daily_sentences ds
                    WHERE ds.user_id = up.user_id
                      AND ds.session_id = up.session_id
                  );
                """
            )
            closed_sessions = int(cursor.rowcount or 0)
    return {"closed_sessions": closed_sessions}


def _get_active_session_id(
    cursor,
    user_id: int,
    source_lang: str | None = None,
    target_lang: str | None = None,
) -> str | None:
    if source_lang and target_lang:
        cursor.execute(
            """
            SELECT up.session_id
            FROM bt_3_user_progress up
            WHERE up.user_id = %s
              AND up.completed = FALSE
              AND EXISTS (
                SELECT 1
                FROM bt_3_daily_sentences ds
                WHERE ds.user_id = up.user_id
                  AND ds.session_id = up.session_id
                  AND COALESCE(ds.source_lang, 'ru') = %s
                  AND COALESCE(ds.target_lang, 'de') = %s
              )
            ORDER BY up.start_time DESC
            LIMIT 1;
            """,
            (user_id, source_lang, target_lang),
        )
    else:
        cursor.execute(
            """
            SELECT session_id
            FROM bt_3_user_progress
            WHERE user_id = %s AND completed = FALSE
            ORDER BY start_time DESC
            LIMIT 1;
            """,
            (user_id,),
        )
    row = cursor.fetchone()
    return row[0] if row else None


def _get_latest_pending_session_id(
    cursor,
    user_id: int,
    source_lang: str = "ru",
    target_lang: str = "de",
) -> str | None:
    cursor.execute(
        """
        SELECT up.session_id
        FROM bt_3_user_progress up
        WHERE up.user_id = %s
          AND EXISTS (
            SELECT 1
            FROM bt_3_daily_sentences ds
            LEFT JOIN bt_3_translations tr
              ON tr.user_id = ds.user_id
             AND tr.sentence_id = ds.id
             AND tr.session_id = ds.session_id
             AND COALESCE(tr.source_lang, 'ru') = %s
             AND COALESCE(tr.target_lang, 'de') = %s
            WHERE ds.user_id = up.user_id
              AND ds.session_id = up.session_id
              AND ds.date = CURRENT_DATE
              AND COALESCE(ds.source_lang, 'ru') = %s
              AND COALESCE(ds.target_lang, 'de') = %s
              AND tr.id IS NULL
          )
        ORDER BY up.start_time DESC
        LIMIT 1;
        """,
        (user_id, source_lang, target_lang, source_lang, target_lang),
    )
    row = cursor.fetchone()
    return row[0] if row else None


def _get_translation_session_state_with_cursor(
    cursor,
    *,
    user_id: int,
    session_id: str | int,
    source_lang: str = "ru",
    target_lang: str = "de",
) -> dict[str, int]:
    normalized_session_id = str(session_id or "").strip()
    if not normalized_session_id:
        return {
            "pending_count": 0,
            "translated_count": 0,
            "shown_count": 0,
            "shown_pending_count": 0,
            "stored_count": 0,
        }

    cursor.execute(
        """
        SELECT
            COUNT(DISTINCT ds.id) AS stored_count,
            COUNT(DISTINCT CASE WHEN COALESCE(ds.shown_to_user, FALSE) = TRUE THEN ds.id END) AS shown_count,
            COUNT(DISTINCT CASE WHEN tr.id IS NOT NULL THEN ds.id END) AS translated_count,
            COUNT(DISTINCT CASE WHEN tr.id IS NULL THEN ds.id END) AS pending_count,
            COUNT(DISTINCT CASE
                WHEN tr.id IS NULL AND COALESCE(ds.shown_to_user, FALSE) = TRUE THEN ds.id
            END) AS shown_pending_count
        FROM bt_3_daily_sentences ds
        LEFT JOIN bt_3_translations tr
          ON tr.user_id = ds.user_id
         AND tr.sentence_id = ds.id
         AND tr.session_id = ds.session_id
         AND COALESCE(tr.source_lang, 'ru') = %s
         AND COALESCE(tr.target_lang, 'de') = %s
        WHERE ds.user_id = %s
          AND ds.session_id = %s
          AND COALESCE(ds.source_lang, 'ru') = %s
          AND COALESCE(ds.target_lang, 'de') = %s;
        """,
        (
            source_lang,
            target_lang,
            int(user_id),
            normalized_session_id,
            source_lang,
            target_lang,
        ),
    )
    row = cursor.fetchone() or (0, 0, 0, 0, 0)
    return {
        "stored_count": int(row[0] or 0),
        "shown_count": int(row[1] or 0),
        "translated_count": int(row[2] or 0),
        "pending_count": int(row[3] or 0),
        "shown_pending_count": int(row[4] or 0),
    }


def _close_user_progress_session(
    cursor,
    *,
    user_id: int,
    session_id: str | int | None,
) -> None:
    normalized_session_id = str(session_id or "").strip()
    if not normalized_session_id:
        return
    cursor.execute(
        """
        UPDATE bt_3_user_progress
        SET
            active_seconds = COALESCE(active_seconds, 0)
                + CASE
                    WHEN COALESCE(active_running, FALSE) = TRUE
                     AND active_started_at IS NOT NULL
                        THEN GREATEST(
                            0,
                            EXTRACT(EPOCH FROM (NOW() - active_started_at))::BIGINT
                        )
                    ELSE 0
                END,
            active_started_at = NULL,
            active_running = FALSE,
            end_time = NOW(),
            completed = TRUE
        WHERE user_id = %s AND session_id = %s;
        """,
        (user_id, normalized_session_id),
    )


def correct_numbering(sentences: list[str]) -> list[str]:
    corrected_sentences = []
    for sentence in sentences:
        cleaned_sentence = re.sub(r"^(\d+)\.\s*\d+\.\s*", r"\1. ", sentence).strip()
        corrected_sentences.append(cleaned_sentence)
    return corrected_sentences


def _dedupe_sentence_texts(items: list[str]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for item in items or []:
        text = " ".join(str(item or "").strip().split())
        if not text:
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        normalized.append(text)
    return normalized


def _normalize_level(level: str | None) -> str:
    normalized = (level or "c1").strip().lower().replace(" ", "")
    if normalized in {"c1-c2", "c1c2", "c1/c2"}:
        return "c2"
    if normalized == "a1":
        return "a1"
    allowed = {"a2", "b1", "b2", "c1", "c2"}
    return normalized if normalized in allowed else "c1"


def _sentence_word_count(text: str) -> int:
    return len(re.findall(r"[A-Za-zÀ-ÿА-Яа-яЁё0-9]+(?:[-'][A-Za-zÀ-ÿА-Яа-яЁё0-9]+)*", str(text or ""), flags=re.UNICODE))


def _sentence_has_any_marker(text: str, markers: list[str]) -> bool:
    lowered = str(text or "").lower()
    return any(marker in lowered for marker in markers)


def _sentence_fits_level(sentence: str, level: str | None) -> bool:
    text = " ".join(str(sentence or "").strip().split())
    if not text:
        return False
    level_key = _normalize_level(level)
    word_count = _sentence_word_count(text)
    comma_count = text.count(",")
    hard_punctuation = sum(text.count(mark) for mark in ";:()")
    dash_count = text.count(" — ") + text.count(" - ")

    medium_markers = [
        " если ", " когда ", " потому что ", " чтобы ", " который", " которая", " которые",
        " although ", " because ", " when ", " if ", " that ", " which ",
        " obwohl ", " weil ", " wenn ", " dass ", " damit ",
        " aunque ", " porque ", " cuando ", " para que ",
        " sebbene ", " perché ", " quando ", " affinché ",
    ]
    advanced_markers = [
        " несмотря на", " в то время как", " едва ", " поскольку ", " хотя ", " так как ",
        " whereby ", " whereas ", " provided that ", " unless ", " however ",
        " während ", " nachdem ", " sofern ", " indem ", " weshalb ",
        " sin embargo ", " por lo tanto ", " cuyo ", " cuya ",
        " nonostante ", " pertanto ", " qualora ", " il quale ",
    ]
    has_medium = _sentence_has_any_marker(text, medium_markers)
    has_advanced = _sentence_has_any_marker(text, advanced_markers)

    if level_key == "a1":
        if word_count < 3 or word_count > 8:
            return False
        if comma_count > 0 or hard_punctuation > 0 or dash_count > 0:
            return False
        if has_medium or has_advanced:
            return False
        return True

    if level_key == "a2":
        if word_count < 4 or word_count > 12:
            return False
        if comma_count > 0 or hard_punctuation > 0 or dash_count > 0:
            return False
        if has_medium or has_advanced:
            return False
        return True

    if level_key == "b1":
        if word_count < 6 or word_count > 18:
            return False
        if comma_count > 2 or hard_punctuation > 1:
            return False
        if has_advanced and word_count > 14:
            return False
        return True

    if level_key == "b2":
        if word_count < 9 or word_count > 24:
            return False
        if comma_count > 3 or hard_punctuation > 2:
            return False
        return has_medium or has_advanced or word_count >= 12

    if level_key == "c1":
        if word_count < 12 or word_count > 30:
            return False
        if comma_count < 1 and not has_medium and not has_advanced and word_count < 16:
            return False
        return True

    if level_key == "c2":
        if word_count < 15 or word_count > 36:
            return False
        if comma_count < 1 and not has_advanced and word_count < 20:
            return False
        return True

    return True


def _filter_sentences_for_level(items: list[str], level: str | None) -> list[str]:
    accepted: list[str] = []
    seen: set[str] = set()
    for item in items or []:
        text = " ".join(str(item or "").strip().split())
        if not text:
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        if _sentence_fits_level(text, level):
            accepted.append(text)
    return accepted


def _get_recent_sentence_reuse_lookback_days() -> int:
    raw_value = str(os.getenv("TRANSLATION_SENTENCE_RECENT_REUSE_LOOKBACK_DAYS") or "3").strip()
    try:
        return max(0, min(30, int(raw_value)))
    except Exception:
        return 3


def _is_legacy_ru_de_pair(source_lang: str | None, target_lang: str | None) -> bool:
    return (source_lang or "").strip().lower() == "ru" and (target_lang or "").strip().lower() == "de"


VALID_CATEGORIES_EN = [
    "Nouns", "Pronouns", "Verbs", "Tenses & Aspect", "Adjectives", "Adverbs",
    "Prepositions", "Word Order", "Articles & Determiners", "Other mistake",
]
VALID_SUBCATEGORIES_EN = {
    "Nouns": ["Pluralization", "Countable vs Uncountable", "Possessives", "Articles/Determiners"],
    "Pronouns": ["Subject/Object", "Possessive Pronouns", "Reflexive Pronouns"],
    "Verbs": ["Conjugation/Agreement", "Auxiliaries (do/be/have)", "Modal Verbs", "Phrasal Verbs", "Verb Patterns (to V / V-ing)"],
    "Tenses & Aspect": ["Present Simple", "Present Continuous", "Past Simple", "Past Continuous", "Present Perfect", "Past Perfect", "Future (will/going to)", "Conditionals"],
    "Adjectives": ["Order of Adjectives", "Comparative", "Superlative"],
    "Adverbs": ["Placement", "Frequency Adverbs", "Too/Enough"],
    "Prepositions": ["Time", "Place", "Verb+Preposition Collocations"],
    "Word Order": ["Questions (aux inversion)", "Negation", "Indirect Questions", "Relative Clauses"],
    "Articles & Determiners": ["a/an/the/zero", "Some/Any", "Much/Many", "This/That"],
    "Other mistake": ["Unclassified mistake"],
}

VALID_CATEGORIES_ES = [
    "Nouns", "Pronouns", "Verbs", "Tenses", "Moods", "Prepositions",
    "Word Order", "Adjectives/Adverbs", "Orthography", "Other mistake",
]
VALID_SUBCATEGORIES_ES = {
    "Nouns": ["Gendered Articles", "Pluralization", "Agreement (gender/number)"],
    "Pronouns": ["Subject Pronouns", "Object Pronouns (lo/la/le)", "Clitic Placement", "Reflexive (se)"],
    "Verbs": ["Conjugation", "Ser vs Estar", "Reflexive Verbs", "Modal/Periphrasis (ir a, tener que)", "Imperatives"],
    "Tenses": ["Present", "Preterito Perfecto", "Preterito Indefinido", "Imperfecto", "Pluscuamperfecto", "Future", "Conditional"],
    "Moods": ["Subjunctive (Present)", "Subjunctive (Past)", "Indicative vs Subjunctive"],
    "Prepositions": ["Por vs Para", "A personal", "De/En/Con", "Preposition Usage"],
    "Word Order": ["Questions", "Negation", "Clitic order (se lo)", "Placement with infinitive/gerund"],
    "Adjectives/Adverbs": ["Agreement", "Comparative/Superlative", "Adverb Formation"],
    "Orthography": ["Accent Marks", "Punctuation (¿¡)", "Spelling"],
    "Other mistake": ["Unclassified mistake"],
}

VALID_CATEGORIES_IT = [
    "Nouns", "Pronouns", "Verbs", "Tenses", "Moods", "Prepositions",
    "Word Order", "Adjectives/Adverbs", "Orthography", "Other mistake",
]
VALID_SUBCATEGORIES_IT = {
    "Nouns": ["Gendered Articles", "Pluralization", "Agreement (gender/number)", "Partitive (del, della)"],
    "Pronouns": ["Direct/Indirect (lo/la/gli/le)", "Clitic Placement", "Reflexive (si)", "Ci/Ne"],
    "Verbs": ["Conjugation", "Essere vs Avere (aux)", "Reflexive Verbs", "Modal Verbs", "Imperatives"],
    "Tenses": ["Presente", "Passato Prossimo", "Imperfetto", "Trapassato Prossimo", "Futuro", "Condizionale"],
    "Moods": ["Congiuntivo (Present)", "Congiuntivo (Past)", "Indicative vs Congiuntivo"],
    "Prepositions": ["Articulated (nel, sul)", "Di/A/Da/In/Con/Su/Per/Tra", "Preposition Usage"],
    "Word Order": ["Questions", "Negation", "Clitic with infinitive", "Double pronouns"],
    "Adjectives/Adverbs": ["Agreement", "Comparative/Superlative", "Adverbs"],
    "Orthography": ["Accents", "Spelling"],
    "Other mistake": ["Unclassified mistake"],
}


def _get_language_taxonomy(target_lang: str | None) -> tuple[list[str], dict[str, list[str]], dict[str, list[str]]]:
    lang = (target_lang or "de").strip().lower()
    if lang == "en":
        categories = VALID_CATEGORIES_EN
        subcategories = VALID_SUBCATEGORIES_EN
    elif lang == "es":
        categories = VALID_CATEGORIES_ES
        subcategories = VALID_SUBCATEGORIES_ES
    elif lang == "it":
        categories = VALID_CATEGORIES_IT
        subcategories = VALID_SUBCATEGORIES_IT
    else:
        categories = VALID_CATEGORIES
        subcategories = VALID_SUBCATEGORIES
    subcategories_lower = {k.lower(): [v.lower() for v in values] for k, values in subcategories.items()}
    return categories, subcategories, subcategories_lower


def _extract_categories_from_feedback(text: str) -> tuple[list[str], list[str]]:
    if not text:
        return [], []
    categories: list[str] = []
    subcategories: list[str] = []

    cat_match = re.search(r"Mistake Categories?:\s*(.+)", text, flags=re.IGNORECASE)
    if cat_match:
        categories.extend([x.strip() for x in cat_match.group(1).split(",") if x.strip()])
    single_cat = re.search(r"Mistake category:\s*(.+)", text, flags=re.IGNORECASE)
    if single_cat:
        categories.append(single_cat.group(1).strip())

    sub_match = re.search(r"Subcategories?:\s*(.+)", text, flags=re.IGNORECASE)
    if sub_match:
        subcategories.extend([x.strip() for x in sub_match.group(1).split(",") if x.strip()])
    first_sub = re.search(r"First subcategory:\s*(.+)", text, flags=re.IGNORECASE)
    second_sub = re.search(r"Second subcategory:\s*(.+)", text, flags=re.IGNORECASE)
    if first_sub:
        subcategories.append(first_sub.group(1).strip())
    if second_sub:
        subcategories.append(second_sub.group(1).strip())

    categories = [re.sub(r"[^0-9a-zA-Z\u00C0-\u024F\s,+\-–&/()¿¡]", "", cat).strip() for cat in categories if cat.strip()]
    subcategories = [re.sub(r"[^0-9a-zA-Z\u00C0-\u024F\s,+\-–&/()¿¡]", "", sub).strip() for sub in subcategories if sub.strip()]
    return categories, subcategories


def _extract_json_payload(text: str) -> dict[str, Any] | None:
    if not text:
        return None
    stripped = text.strip()
    if stripped.startswith("```"):
        stripped = re.sub(r"^```[a-zA-Z]*\n?", "", stripped)
        stripped = stripped.rstrip("`").strip()
    try:
        return json.loads(stripped)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", stripped, re.DOTALL)
        if not match:
            return None
        try:
            return json.loads(match.group(0))
        except json.JSONDecodeError:
            return None


def _normalize_guess(text: str) -> str:
    cleaned = re.sub(r"[^\w\s\-]", " ", text.lower())
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def _normalize_story_difficulty(value: str | None) -> str:
    if not value:
        return "intermediate"
    normalized = value.strip().lower()
    if normalized in {"начальный", "beginner", "a2"}:
        return "beginner"
    if normalized in {"средний", "intermediate", "b1", "b2"}:
        return "intermediate"
    if normalized in {"продвинутый", "advanced", "c1", "c2"}:
        return "advanced"
    return "intermediate"


async def generate_mystery_story(
    story_type: str,
    difficulty: str,
    topic: str = "ЗАГАДОЧНАЯ ИСТОРИЯ",
) -> dict[str, Any]:
    task_name = "generate_mystery_story"
    system_instruction_key = "generate_mystery_story"

    user_message = f"""
    Story Type: "{story_type}"
    Difficulty: "{difficulty}"
    Topic: "{topic}"
    """

    for attempt in range(4):
        try:
            content = await llm_execute(
                task_name=task_name,
                system_instruction_key=system_instruction_key,
                user_message=user_message,
                poll_interval_seconds=1.0,
            )

            payload = _extract_json_payload(content)
            if payload:
                return payload
        except openai.RateLimitError:
            await asyncio.sleep((attempt + 1) * 2)
        except Exception:
            break

    return {}


def _parse_story_feedback(text: str) -> dict[str, Any]:
    score = None
    feedback = ""

    match_score = re.search(r"Score:\s*(\d{1,3})\s*/\s*100", text, flags=re.IGNORECASE)
    if match_score:
        score = int(match_score.group(1))

    # Preferred format starts with "Feedback:" and then structured blocks.
    match_feedback = re.search(r"Feedback:\s*(.+)$", text, flags=re.IGNORECASE | re.DOTALL)
    if match_feedback:
        feedback = match_feedback.group(1).strip()
    else:
        # Fallback: keep full text except first score line.
        feedback = re.sub(r"^Score:\s*\d{1,3}\s*/\s*100\s*$", "", text.strip(), flags=re.IGNORECASE | re.MULTILINE).strip()

    return {
        "score": score if score is not None else 0,
        "feedback": feedback,
    }


def _build_story_source_links(answer: str) -> list[dict[str, str]]:
    normalized = (answer or "").strip()
    if not normalized:
        return []
    slug = quote(normalized.replace(" ", "_"))
    return [
        {"lang": "DE", "url": f"https://de.wikipedia.org/wiki/{slug}"},
        {"lang": "EN", "url": f"https://en.wikipedia.org/wiki/{slug}"},
        {"lang": "RU", "url": f"https://ru.wikipedia.org/wiki/{slug}"},
    ]


def _insert_story_session_sentences(
    cursor,
    user_id: int,
    session_id: str,
    sentences: list[str],
) -> None:
    cursor.execute(
        """
        SELECT COALESCE(MAX(unique_id), 0)
        FROM bt_3_daily_sentences
        WHERE user_id = %s AND date = CURRENT_DATE;
        """,
        (user_id,),
    )
    row = cursor.fetchone()
    start_index = (row[0] or 0) + 1

    for i, sentence in enumerate(sentences, start=start_index):
        cursor.execute(
            """
            SELECT id_for_mistake_table
            FROM bt_3_daily_sentences
            WHERE sentence = %s
            LIMIT 1;
            """,
            (sentence,),
        )
        result = cursor.fetchone()

        if result:
            id_for_mistake_table = result[0]
        else:
            cursor.execute("SELECT MAX(id_for_mistake_table) FROM bt_3_daily_sentences;")
            result = cursor.fetchone()
            max_id = result[0] if result and result[0] is not None else 0
            id_for_mistake_table = max_id + 1

        cursor.execute(
            """
            INSERT INTO bt_3_daily_sentences (date, sentence, unique_id, user_id, session_id, id_for_mistake_table)
            VALUES (CURRENT_DATE, %s, %s, %s, %s, %s);
            """,
            (sentence, i, user_id, session_id, id_for_mistake_table),
        )
        cursor.execute(
            """
            UPDATE bt_3_daily_sentences
            SET source_lang = COALESCE(source_lang, 'ru'),
                target_lang = COALESCE(target_lang, 'de')
            WHERE user_id = %s AND session_id = %s AND sentence = %s
              AND source_lang IS NULL AND target_lang IS NULL;
            """,
            (user_id, session_id, sentence),
        )


def _save_story_bank(
    cursor,
    title: str | None,
    answer: str,
    aliases: list[str],
    extra_de: str,
    story_type: str,
    difficulty: str,
    sentences: list[str],
) -> int:
    cursor.execute(
        """
        INSERT INTO bt_3_story_bank (title, answer, answer_aliases, extra_de, story_type, difficulty)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING id;
        """,
        (title or None, answer, json.dumps(aliases, ensure_ascii=False), extra_de, story_type, difficulty),
    )
    story_id = cursor.fetchone()[0]

    for idx, sentence in enumerate(sentences, start=1):
        cursor.execute(
            """
            INSERT INTO bt_3_story_sentences (story_id, sentence_index, sentence)
            VALUES (%s, %s, %s);
            """,
            (story_id, idx, sentence),
        )
    return story_id


def get_story_history_webapp(user_id: int, limit: int = 10) -> list[dict[str, Any]]:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT s.story_id, b.title, b.story_type, b.difficulty, s.created_at
                FROM bt_3_story_sessions s
                JOIN bt_3_story_bank b ON b.id = s.story_id
                WHERE s.user_id = %s
                ORDER BY s.created_at DESC
                LIMIT %s;
                """,
                (user_id, limit),
            )
            rows = cursor.fetchall()
    return [
        {
            "story_id": row[0],
            "title": row[1],
            "story_type": row[2],
            "difficulty": row[3],
            "created_at": row[4].isoformat() if row[4] else None,
        }
        for row in rows
    ]


def get_active_session_type(user_id: int, *, close_stale_sessions: bool = True) -> dict[str, Any]:
    if close_stale_sessions:
        close_stale_open_translation_sessions_for_user(user_id=int(user_id))
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT session_id
                FROM bt_3_user_progress
                WHERE user_id = %s AND completed = FALSE
                ORDER BY start_time DESC
                """,
                (user_id,),
            )
            rows = cursor.fetchall() or []
            if not rows:
                return {"type": "none", "session_id": None}

            for row in rows:
                session_id = row[0]

                cursor.execute(
                    """
                    SELECT story_id
                    FROM bt_3_story_sessions
                    WHERE user_id = %s AND session_id = %s
                    ORDER BY created_at DESC
                    LIMIT 1;
                    """,
                    (user_id, str(session_id)),
                )
                story_row = cursor.fetchone()
                if story_row:
                    return {"type": "story", "story_id": story_row[0], "session_id": str(session_id)}

                cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM bt_3_daily_sentences
                    WHERE user_id = %s AND session_id = %s;
                    """,
                    (user_id, session_id),
                )
                sentence_count = int((cursor.fetchone() or [0])[0] or 0)
                if sentence_count > 0:
                    return {"type": "regular", "session_id": str(session_id)}

                cursor.execute(
                    """
                    UPDATE bt_3_user_progress
                    SET
                        active_seconds = COALESCE(active_seconds, 0)
                            + CASE
                                WHEN COALESCE(active_running, FALSE) = TRUE
                                 AND active_started_at IS NOT NULL
                                    THEN GREATEST(
                                        0,
                                        EXTRACT(EPOCH FROM (NOW() - active_started_at))::BIGINT
                                    )
                                ELSE 0
                            END,
                        active_started_at = NULL,
                        active_running = FALSE,
                        end_time = NOW(),
                        completed = TRUE
                    WHERE user_id = %s AND session_id = %s;
                    """,
                    (user_id, session_id),
                )
                logging.warning(
                    "Auto-closed empty active translation session: user_id=%s session_id=%s",
                    user_id,
                    session_id,
                )

            return {"type": "none", "session_id": None}


async def start_story_session_webapp(
    user_id: int,
    username: str | None,
    mode: str,
    story_type: str,
    difficulty: str,
    story_id: int | None = None,
    source_lang: str = "ru",
    target_lang: str = "de",
) -> dict[str, Any]:
    close_stale_open_translation_sessions_for_user(
        user_id=int(user_id),
        source_lang=source_lang,
        target_lang=target_lang,
    )
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        active_session_id = _get_active_session_id(cursor, user_id, source_lang=source_lang, target_lang=target_lang)
        if active_session_id:
            return {"session_id": active_session_id, "created": False, "blocked": True}

        cursor.execute(
            """
            UPDATE bt_3_user_progress
            SET
                active_seconds = COALESCE(active_seconds, 0)
                    + CASE
                        WHEN COALESCE(active_running, FALSE) = TRUE
                         AND active_started_at IS NOT NULL
                            THEN GREATEST(
                                0,
                                EXTRACT(EPOCH FROM (NOW() - active_started_at))::BIGINT
                            )
                        ELSE 0
                    END,
                active_started_at = NULL,
                active_running = FALSE,
                end_time = NOW(),
                completed = TRUE
            WHERE user_id = %s AND completed = FALSE;
            """,
            (user_id,),
        )

        session_id = int(hashlib.md5(f"{user_id}{datetime.now()}".encode()).hexdigest(), 16) % (10**12)
        cursor.execute(
            """
            INSERT INTO bt_3_user_progress (
                session_id,
                user_id,
                username,
                start_time,
                active_seconds,
                active_running,
                completed
            )
            VALUES (%s, %s, %s, NOW(), 0, FALSE, FALSE);
            """,
            (session_id, user_id, username),
        )

        story_payload: dict[str, Any] | None = None
        story_sentences: list[str] = []
        story_title = None
        story_answer = None
        story_aliases: list[str] = []
        story_extra_de = None

        if mode == "repeat":
            if not story_id:
                cursor.execute(
                    """
                    SELECT story_id
                    FROM bt_3_story_sessions
                    WHERE user_id = %s
                    ORDER BY created_at DESC
                    LIMIT 1;
                    """,
                    (user_id,),
                )
                row = cursor.fetchone()
                story_id = row[0] if row else None
            if not story_id:
                return {"error": "Нет сохранённых историй для повтора.", "created": False}

            cursor.execute(
                """
                SELECT title, answer, answer_aliases, extra_de, story_type, difficulty
                FROM bt_3_story_bank
                WHERE id = %s;
                """,
                (story_id,),
            )
            row = cursor.fetchone()
            if not row:
                return {"error": "История не найдена.", "created": False}
            story_title, story_answer, aliases_json, story_extra_de, story_type, difficulty = row
            if isinstance(aliases_json, str):
                try:
                    story_aliases = json.loads(aliases_json)
                except json.JSONDecodeError:
                    story_aliases = []
            else:
                story_aliases = aliases_json or []
            cursor.execute(
                """
                SELECT sentence
                FROM bt_3_story_sentences
                WHERE story_id = %s
                ORDER BY sentence_index ASC;
                """,
                (story_id,),
            )
            story_sentences = [row[0] for row in cursor.fetchall()]
        else:
            normalized_difficulty = _normalize_story_difficulty(difficulty)
            story_payload = await generate_mystery_story(story_type, normalized_difficulty)
            story_title = story_payload.get("title")
            story_answer = story_payload.get("answer")
            story_aliases = story_payload.get("aliases") or []
            story_extra_de = story_payload.get("extra_de")
            story_sentences = story_payload.get("story_ru") or []

            if not story_answer or not story_extra_de or len(story_sentences) != 7:
                return {"error": "Не удалось сформировать историю.", "created": False}

            story_id = _save_story_bank(
                cursor,
                story_title,
                story_answer,
                story_aliases,
                story_extra_de,
                story_type,
                normalized_difficulty,
                story_sentences,
            )

        cursor.execute(
            """
            INSERT INTO bt_3_story_sessions (user_id, session_id, story_id, mode)
            VALUES (%s, %s, %s, %s);
            """,
            (user_id, str(session_id), story_id, mode),
        )

        _insert_story_session_sentences(cursor, user_id, session_id, story_sentences)
        conn.commit()

        return {
            "session_id": session_id,
            "created": True,
            "count": len(story_sentences),
            "story_id": story_id,
            "title": story_title,
        }
    finally:
        cursor.close()
        conn.close()


async def submit_story_translation_webapp(
    user_id: int,
    username: str | None,
    translations: list[dict[str, Any]],
    guess: str,
    source_lang: str = "ru",
    target_lang: str = "de",
) -> dict[str, Any]:
    if not translations:
        return {"error": "translations обязательны"}

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(
            """
            SELECT session_id
            FROM bt_3_user_progress
            WHERE user_id = %s AND completed = FALSE
            ORDER BY start_time DESC
            LIMIT 1;
            """,
            (user_id,),
        )
        row = cursor.fetchone()
        if not row:
            return {"error": "Активная сессия не найдена."}
        session_id = row[0]

        cursor.execute(
            """
            SELECT s.story_id, b.answer, b.answer_aliases, b.extra_de
            FROM bt_3_story_sessions s
            JOIN bt_3_story_bank b ON b.id = s.story_id
            WHERE s.user_id = %s AND s.session_id = %s
            ORDER BY s.created_at DESC
            LIMIT 1;
            """,
            (user_id, str(session_id)),
        )
        story_row = cursor.fetchone()
        if not story_row:
            return {"error": "История для этой сессии не найдена."}

        story_id, answer, aliases_json, extra_de = story_row
        if isinstance(aliases_json, str):
            try:
                aliases = json.loads(aliases_json)
            except json.JSONDecodeError:
                aliases = []
        else:
            aliases = aliases_json or []

        cursor.execute(
            """
            SELECT id, id_for_mistake_table, sentence, unique_id
            FROM bt_3_daily_sentences
            WHERE user_id = %s AND session_id = %s
            ORDER BY unique_id ASC;
            """,
            (user_id, session_id),
        )
        daily_rows = cursor.fetchall()
        if not daily_rows:
            return {"error": "Предложения истории не найдены."}

        if len(daily_rows) != 7:
            logging.warning("Story session has %s sentences, expected 7", len(daily_rows))

        translations_by_id = {
            int(item.get("id_for_mistake_table")): (item.get("translation") or "").strip()
            for item in translations
            if item.get("id_for_mistake_table")
        }

        original_sentences = [row[2] for row in daily_rows]
        user_sentences = [translations_by_id.get(row[1], "") for row in daily_rows]

        if any(not text for text in user_sentences):
            return {"error": "Нужно заполнить все 7 предложений истории."}

        cursor.execute(
            """
            SELECT COUNT(*) FROM bt_3_translations
            WHERE user_id = %s AND session_id = %s;
            """,
            (user_id, session_id),
        )
        if (cursor.fetchone()[0] or 0) > 0:
            return {"error": "История уже была отправлена."}

        original_text = "\n".join(original_sentences)
        user_text = "\n".join(user_sentences)
        try:
            raw_feedback = await run_check_translation_story(original_text, user_text)
        except Exception as exc:
            detailed_message = _extract_nested_error_message(exc)
            logging.warning(
                "Story translation check failed for user_id=%s session_id=%s: %s",
                user_id,
                session_id,
                detailed_message or exc,
                exc_info=True,
            )
            if detailed_message:
                return {"error": f"Не удалось проверить историю. {detailed_message}"}
            return {"error": "Не удалось проверить историю. Попробуйте ещё раз."}

        if not isinstance(raw_feedback, str):
            detailed_message = _extract_nested_error_message(raw_feedback)
            logging.warning(
                "Story translation check returned non-string payload for user_id=%s session_id=%s: %r",
                user_id,
                session_id,
                raw_feedback,
            )
            if detailed_message:
                return {"error": f"Не удалось проверить историю. {detailed_message}"}
            return {"error": "Не удалось проверить историю. Попробуйте ещё раз."}

        stripped_feedback = raw_feedback.strip()
        if stripped_feedback.startswith("{") and stripped_feedback.endswith("}"):
            detailed_message = _extract_nested_error_message(stripped_feedback)
            if detailed_message and detailed_message != stripped_feedback:
                logging.warning(
                    "Story translation check returned error payload for user_id=%s session_id=%s: %s",
                    user_id,
                    session_id,
                    detailed_message,
                )
                return {"error": f"Не удалось проверить историю. {detailed_message}"}

        parsed = _parse_story_feedback(raw_feedback)
        score_value = parsed["score"]
        feedback = parsed["feedback"] or raw_feedback

        for row, user_sentence in zip(daily_rows, user_sentences):
            sentence_pk_id = row[0]
            sentence_id_for_mistake = row[1]
            cursor.execute(
                """
                INSERT INTO bt_3_translations (user_id, id_for_mistake_table, session_id, username, sentence_id,
                user_translation, score, feedback, source_lang, target_lang)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (user_id, sentence_id, session_id) WHERE session_id IS NOT NULL DO NOTHING
                RETURNING id;
                """,
                (
                    user_id,
                    sentence_id_for_mistake,
                    session_id,
                    username,
                    sentence_pk_id,
                    user_sentence,
                    score_value,
                    feedback,
                    (source_lang or "ru"),
                    (target_lang or "de"),
                ),
            )

        normalized_guess = _normalize_guess(guess)
        normalized_answer = _normalize_guess(answer or "")
        alias_matches = {_normalize_guess(item) for item in aliases if item}
        heuristic_match = bool(
            normalized_guess and (
                normalized_guess == normalized_answer
                or normalized_guess in alias_matches
                or normalized_answer in normalized_guess
                or any(alias and alias in normalized_guess for alias in alias_matches)
            )
        )

        semantic_result = {"is_correct": False, "reason": ""}
        try:
            semantic_result = await run_check_story_guess_semantic(
                canonical_answer=answer or "",
                aliases=[item for item in aliases if item],
                user_guess=guess,
            )
        except Exception as exc:
            logging.warning("Semantic guess check failed: %s", exc)

        is_correct = bool(heuristic_match or semantic_result.get("is_correct"))
        guess_reason = (semantic_result.get("reason") or "").strip()
        source_links = _build_story_source_links(answer or "")

        cursor.execute(
            """
            UPDATE bt_3_story_sessions
            SET completed_at = NOW(), guess = %s, guess_correct = %s, score = %s, feedback = %s
            WHERE user_id = %s AND session_id = %s AND story_id = %s;
            """,
            (guess, is_correct, score_value, feedback, user_id, str(session_id), story_id),
        )

        conn.commit()

        return {
            "ok": True,
            "score": score_value,
            "feedback": feedback,
            "guess_correct": is_correct,
            "guess_reason": guess_reason,
            "answer": answer,
            "extra_de": extra_de,
            "source_links": source_links,
        }
    finally:
        cursor.close()
        conn.close()


async def generate_sentences_webapp(
    user_id: int,
    num_sentences: int,
    topic: str = "Random sentences",
    level: str | None = None,
) -> list[str]:
    level_key = _normalize_level(level)
    instruction_level_key = "a2" if level_key == "a1" else level_key
    # Keep assistants separated per CEFR tier so cached instructions cannot bleed across levels.
    task_name = f"generate_sentences_{instruction_level_key}"
    system_instruction_key = f"generate_sentences_{instruction_level_key}"
    target_count = int(max(1, num_sentences))
    level_notes = {
        "a1": "A1 only: very short, very concrete everyday sentences. No subordinate clauses. Prefer 3-8 words.",
        "a2": "A2 only: short, concrete, everyday sentences. No heavy subordinate clauses. Prefer 4-12 words.",
        "b1": "B1 only: moderately simple sentences with at most one light subordinate clause. Prefer 6-18 words.",
        "b2": "B2 only: clearly more developed sentences with some clause complexity. Prefer 9-24 words.",
        "c1": "C1 only: advanced sentences with visible syntactic complexity. Prefer 12-30 words.",
        "c2": "C2 only: very advanced, nuanced, syntactically dense sentences. Prefer 15-36 words.",
    }

    user_message = f"""
    Number of sentences: {target_count}. Topic: "{topic}".
    Required level: {level_key.upper()}.
    {level_notes.get(level_key, "")}
    Reject sentences that are clearly easier or harder than the requested level.
    """

    for attempt in range(5):
        try:
            sentences = await llm_execute(
                task_name=task_name,
                system_instruction_key=system_instruction_key,
                user_message=user_message,
                poll_interval_seconds=1.0,
            )

            raw_lines = [s.strip() for s in sentences.split("\n") if s.strip()]
            filtered = _filter_sentences_for_level(raw_lines, level_key)
            if len(filtered) >= target_count:
                return filtered[:target_count]
        except openai.RateLimitError:
            wait_time = (attempt + 1) * 2
            await asyncio.sleep(wait_time)
        except Exception:
            break

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT sentence FROM bt_3_spare_sentences ORDER BY RANDOM() LIMIT 30;
                """
            )
            spare_rows = cursor.fetchall()
    if spare_rows:
        spare_filtered = _filter_sentences_for_level(
            [row[0].strip() for row in spare_rows if row[0] and row[0].strip()],
            level_key,
        )
        if spare_filtered:
            return spare_filtered[:target_count]
    return []


def _load_skill_catalog_with_cursor(
    cursor,
    *,
    target_lang: str,
    limit: int = 300,
    authored_mastery_leaves_only: bool = False,
) -> list[dict[str, str]]:
    normalized_target_lang = str(target_lang or "de").strip().lower() or "de"
    if authored_mastery_leaves_only and normalized_target_lang == "de":
        cursor.execute(
            """
            SELECT
                s.skill_id,
                COALESCE(NULLIF(m.display_title_override, ''), s.title) AS title,
                g.display_title AS category,
                m.mastery_group_id
            FROM bt_3_skills s
            JOIN bt_3_skill_mastery_group_members m
              ON m.diagnostic_skill_id = s.skill_id
             AND m.language_code = s.language_code
            JOIN bt_3_skill_mastery_groups g
              ON g.mastery_group_id = m.mastery_group_id
             AND g.language_code = m.language_code
            WHERE s.language_code = %s
              AND COALESCE(s.is_active, TRUE) = TRUE
              AND COALESCE(g.is_active, TRUE) = TRUE
              AND COALESCE(m.is_mastery_leaf, FALSE) = TRUE
              AND COALESCE(m.is_diagnostic_only, FALSE) = FALSE
            ORDER BY g.sort_order ASC, m.sort_order ASC, title ASC, s.skill_id ASC
            LIMIT %s;
            """,
            (normalized_target_lang, max(1, min(int(limit or 300), 500))),
        )
        rows = cursor.fetchall() or []
        return [
            {
                "skill_id": str(row[0] or "").strip(),
                "title": str(row[1] or "").strip(),
                "category": str(row[2] or "").strip(),
                "mastery_group_id": str(row[3] or "").strip(),
            }
            for row in rows
            if str(row[0] or "").strip()
        ]

    cursor.execute(
        """
        SELECT skill_id, title, category
        FROM bt_3_skills
        WHERE language_code = %s
          AND COALESCE(is_active, TRUE) = TRUE
        ORDER BY category ASC, title ASC, skill_id ASC
        LIMIT %s;
        """,
        (normalized_target_lang, max(1, min(int(limit or 300), 500))),
    )
    rows = cursor.fetchall() or []
    return [
        {
            "skill_id": str(row[0] or "").strip(),
            "title": str(row[1] or "").strip(),
            "category": str(row[2] or "").strip(),
        }
        for row in rows
        if str(row[0] or "").strip()
    ]


def _load_skill_mastery_memberships_with_cursor(
    cursor,
    *,
    target_lang: str,
    skill_ids: list[str],
    cache: dict[tuple[str, str], dict[str, Any] | None] | None = None,
) -> dict[str, dict[str, Any]]:
    normalized_skill_ids = [
        str(skill_id or "").strip()
        for skill_id in list(skill_ids or [])
        if str(skill_id or "").strip()
    ]
    if not normalized_skill_ids:
        return {}
    normalized_target_lang = str(target_lang or "de").strip().lower() or "de"
    result: dict[str, dict[str, Any]] = {}
    missing_skill_ids: list[str] = []
    if cache is not None:
        for skill_id in normalized_skill_ids:
            cached = cache.get((normalized_target_lang, skill_id))
            if cached is None:
                missing_skill_ids.append(skill_id)
            elif cached:
                result[skill_id] = dict(cached)
    else:
        missing_skill_ids = list(normalized_skill_ids)
    if not missing_skill_ids:
        return result
    cursor.execute(
        """
        SELECT
            diagnostic_skill_id,
            mastery_group_id,
            is_mastery_leaf,
            is_diagnostic_only,
            rollup_weight,
            sort_order
        FROM bt_3_skill_mastery_group_members
        WHERE language_code = %s
          AND diagnostic_skill_id = ANY(%s);
        """,
        (normalized_target_lang, missing_skill_ids),
    )
    rows = cursor.fetchall() or []
    loaded = {
        str(row[0] or "").strip(): {
            "mastery_group_id": str(row[1] or "").strip(),
            "is_mastery_leaf": bool(row[2]),
            "is_diagnostic_only": bool(row[3]),
            "rollup_weight": float(row[4] or 0.0),
            "sort_order": int(row[5] or 100),
        }
        for row in rows
        if str(row[0] or "").strip()
    }
    if cache is not None:
        for skill_id in missing_skill_ids:
            cache[(normalized_target_lang, skill_id)] = dict(loaded.get(skill_id) or {})
    result.update(loaded)
    return result


def _list_mastery_leaf_skill_ids_for_groups_with_cursor(
    cursor,
    *,
    target_lang: str,
    mastery_group_ids: list[str],
    cache: dict[tuple[str, str], list[str]] | None = None,
) -> dict[str, list[str]]:
    normalized_group_ids = [
        str(group_id or "").strip()
        for group_id in list(mastery_group_ids or [])
        if str(group_id or "").strip()
    ]
    if not normalized_group_ids:
        return {}
    normalized_target_lang = str(target_lang or "de").strip().lower() or "de"
    grouped: dict[str, list[str]] = {}
    missing_group_ids: list[str] = []
    if cache is not None:
        for group_id in normalized_group_ids:
            cached = cache.get((normalized_target_lang, group_id))
            if cached is None:
                missing_group_ids.append(group_id)
            else:
                grouped[group_id] = list(cached)
    else:
        missing_group_ids = list(normalized_group_ids)
    if not missing_group_ids:
        return grouped
    cursor.execute(
        """
        SELECT mastery_group_id, diagnostic_skill_id
        FROM bt_3_skill_mastery_group_members
        WHERE language_code = %s
          AND mastery_group_id = ANY(%s)
          AND COALESCE(is_mastery_leaf, FALSE) = TRUE
          AND COALESCE(is_diagnostic_only, FALSE) = FALSE
        ORDER BY mastery_group_id ASC, sort_order ASC, diagnostic_skill_id ASC;
        """,
        (normalized_target_lang, missing_group_ids),
    )
    rows = cursor.fetchall() or []
    for mastery_group_id, diagnostic_skill_id in rows:
        group_id = str(mastery_group_id or "").strip()
        skill_id = str(diagnostic_skill_id or "").strip()
        if not group_id or not skill_id:
            continue
        grouped.setdefault(group_id, []).append(skill_id)
    if cache is not None:
        for group_id in missing_group_ids:
            cache[(normalized_target_lang, group_id)] = list(grouped.get(group_id) or [])
    return grouped


def _build_skill_profile_from_skill_ids(
    *,
    primary_skill_id: str,
    secondary_skill_ids: list[str],
    supporting_skill_ids: list[str],
    profile_source: str,
    profile_confidence: float,
) -> list[dict[str, Any]]:
    profile: list[dict[str, Any]] = [
        {
            "skill_id": str(primary_skill_id),
            "role": "primary",
            "role_rank": 1,
            "role_weight": PHASE1_SKILL_ROLE_WEIGHTS["primary"],
            "profile_source": profile_source,
            "profile_confidence": profile_confidence,
            "profile_version": 1,
        }
    ]
    for index, skill_id in enumerate(list(secondary_skill_ids or [])[:2], start=2):
        profile.append(
            {
                "skill_id": str(skill_id),
                "role": "secondary",
                "role_rank": index,
                "role_weight": PHASE1_SKILL_ROLE_WEIGHTS["secondary"],
                "profile_source": profile_source,
                "profile_confidence": profile_confidence,
                "profile_version": 1,
            }
        )
    if supporting_skill_ids:
        profile.append(
            {
                "skill_id": str(supporting_skill_ids[0]),
                "role": "supporting",
                "role_rank": 4,
                "role_weight": PHASE1_SKILL_ROLE_WEIGHTS["supporting"],
                "profile_source": profile_source,
                "profile_confidence": profile_confidence,
                "profile_version": 1,
            }
        )
    return profile


def _normalize_sentence_entries(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: dict[str, dict[str, Any]] = {}
    for item in entries:
        sentence = str(item.get("sentence") or "").strip()
        if not sentence:
            continue
        sentence = re.sub(r"^\s*\d+\.\s*", "", sentence).strip()
        if not sentence:
            continue
        payload = {
            "sentence": sentence,
            "tested_skill_profile": list(item.get("tested_skill_profile") or []),
        }
        existing = normalized.get(sentence)
        if existing is None:
            normalized[sentence] = payload
            continue
        if not existing.get("tested_skill_profile") and payload.get("tested_skill_profile"):
            normalized[sentence] = payload
    return list(normalized.values())


def _normalize_sentence_pool_text(sentence: str | None) -> str:
    return " ".join(str(sentence or "").strip().split())


def _sentence_pool_hash(sentence: str | None) -> str:
    normalized = _normalize_sentence_pool_text(sentence)
    return hashlib.sha1(normalized.encode("utf-8")).hexdigest() if normalized else ""


def _fetch_shared_sentence_pool_entries_with_cursor(
    cursor,
    *,
    focus: dict[str, Any] | None,
    level: str | None,
    source_lang: str,
    target_lang: str,
    exclude_sentences: set[str] | None = None,
    limit: int = 0,
) -> list[dict[str, Any]]:
    safe_limit = max(0, int(limit or 0))
    if safe_limit <= 0:
        return []
    if not isinstance(focus, dict) or str(focus.get("kind") or "").strip().lower() != "preset":
        return []
    focus_key = str(focus.get("key") or "").strip()
    if not focus_key:
        return []
    level_key = _normalize_level(level)
    excluded = {
        _normalize_sentence_pool_text(item).lower()
        for item in (exclude_sentences or set())
        if _normalize_sentence_pool_text(item)
    }
    cursor.execute(
        """
        SELECT
            id,
            sentence,
            tested_skill_profile
        FROM bt_3_translation_sentence_pool
        WHERE source_lang = %s
          AND target_lang = %s
          AND focus_key = %s
          AND level = %s
          AND is_active = TRUE
        ORDER BY use_count ASC, COALESCE(last_used_at, created_at) ASC, id ASC
        LIMIT %s;
        """,
        (
            str(source_lang or "").strip().lower() or "ru",
            str(target_lang or "").strip().lower() or "de",
            focus_key,
            level_key,
            max(safe_limit * 4, safe_limit),
        ),
    )
    rows = cursor.fetchall() or []
    selected_ids: list[int] = []
    selected_entries: list[dict[str, Any]] = []
    seen_texts = set(excluded)
    for row_id, sentence, tested_skill_profile in rows:
        normalized_sentence = _normalize_sentence_pool_text(sentence)
        if not normalized_sentence:
            continue
        dedupe_key = normalized_sentence.lower()
        if dedupe_key in seen_texts:
            continue
        selected_ids.append(int(row_id))
        selected_entries.append(
            {
                "sentence": normalized_sentence,
                "tested_skill_profile": list(tested_skill_profile or []),
            }
        )
        seen_texts.add(dedupe_key)
        if len(selected_entries) >= safe_limit:
            break
    if selected_ids:
        cursor.execute(
            """
            UPDATE bt_3_translation_sentence_pool
            SET use_count = use_count + 1,
                last_used_at = NOW(),
                updated_at = NOW()
            WHERE id = ANY(%s);
            """,
            (selected_ids,),
        )
    return _normalize_sentence_entries(selected_entries)


def _upsert_shared_sentence_pool_entries_with_cursor(
    cursor,
    *,
    focus: dict[str, Any] | None,
    level: str | None,
    source_lang: str,
    target_lang: str,
    entries: list[dict[str, Any]] | None,
) -> int:
    if not isinstance(focus, dict) or str(focus.get("kind") or "").strip().lower() != "preset":
        return 0
    focus_key = str(focus.get("key") or "").strip()
    focus_label = str(focus.get("label") or "").strip()
    if not focus_key or not focus_label:
        return 0
    level_key = _normalize_level(level)
    normalized_entries = _normalize_sentence_entries(list(entries or []))
    rows: list[tuple[Any, ...]] = []
    for item in normalized_entries:
        sentence = _normalize_sentence_pool_text(item.get("sentence"))
        if not sentence:
            continue
        sentence_hash = _sentence_pool_hash(sentence)
        if not sentence_hash:
            continue
        rows.append(
            (
                str(source_lang or "").strip().lower() or "ru",
                str(target_lang or "").strip().lower() or "de",
                focus_key,
                focus_label,
                level_key,
                sentence,
                sentence_hash,
                Json(list(item.get("tested_skill_profile") or [])),
            )
        )
    if not rows:
        return 0
    execute_values(
        cursor,
        """
        INSERT INTO bt_3_translation_sentence_pool (
            source_lang,
            target_lang,
            focus_key,
            focus_label,
            level,
            sentence,
            sentence_hash,
            tested_skill_profile
        )
        VALUES %s
        ON CONFLICT (source_lang, target_lang, focus_key, level, sentence_hash) DO UPDATE
        SET
            focus_label = EXCLUDED.focus_label,
            sentence = EXCLUDED.sentence,
            tested_skill_profile = EXCLUDED.tested_skill_profile,
            is_active = TRUE,
            updated_at = NOW();
        """,
        rows,
    )
    return len(rows)


def _count_shared_sentence_pool_entries_with_cursor(
    cursor,
    *,
    focus: dict[str, Any] | None,
    level: str | None,
    source_lang: str,
    target_lang: str,
) -> int:
    if not isinstance(focus, dict) or str(focus.get("kind") or "").strip().lower() != "preset":
        return 0
    focus_key = str(focus.get("key") or "").strip()
    if not focus_key:
        return 0
    level_key = _normalize_level(level)
    cursor.execute(
        """
        SELECT COUNT(*)
        FROM bt_3_translation_sentence_pool
        WHERE source_lang = %s
          AND target_lang = %s
          AND focus_key = %s
          AND level = %s
          AND is_active = TRUE;
        """,
        (
            str(source_lang or "").strip().lower() or "ru",
            str(target_lang or "").strip().lower() or "de",
            focus_key,
            level_key,
        ),
    )
    row = cursor.fetchone()
    return int(row[0] or 0) if row else 0


def _list_shared_sentence_pool_sentence_keys_with_cursor(
    cursor,
    *,
    focus: dict[str, Any] | None,
    level: str | None,
    source_lang: str,
    target_lang: str,
) -> set[str]:
    if not isinstance(focus, dict) or str(focus.get("kind") or "").strip().lower() != "preset":
        return set()
    focus_key = str(focus.get("key") or "").strip()
    if not focus_key:
        return set()
    level_key = _normalize_level(level)
    cursor.execute(
        """
        SELECT sentence
        FROM bt_3_translation_sentence_pool
        WHERE source_lang = %s
          AND target_lang = %s
          AND focus_key = %s
          AND level = %s
          AND is_active = TRUE;
        """,
        (
            str(source_lang or "").strip().lower() or "ru",
            str(target_lang or "").strip().lower() or "de",
            focus_key,
            level_key,
        ),
    )
    rows = cursor.fetchall() or []
    return {
        _normalize_sentence_text_key(row[0])
        for row in rows
        if row and row[0] and _normalize_sentence_text_key(row[0])
    }


async def _top_up_immediate_sentence_entries_with_cursor(
    cursor,
    *,
    topic: str,
    level: str | None,
    source_lang: str,
    target_lang: str,
    resolved_focus: dict[str, Any],
    target_min_ready: int,
    existing_entries: list[dict[str, Any]] | None,
    recent_sentence_keys: set[str] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    normalized_entries = _normalize_sentence_entries(list(existing_entries or []))
    focus_kind = str(resolved_focus.get("kind") or "").strip().lower()
    minimum_ready = max(1, int(target_min_ready or 1))
    if len(normalized_entries) >= minimum_ready or focus_kind not in {"preset", "custom"}:
        return normalized_entries, {
            "quick_topup_ms": 0,
            "quick_topup_requested": 0,
            "quick_topup_generated": 0,
            "quick_topup_added": 0,
        }

    started_at = time.perf_counter()
    level_key = _normalize_level(level)
    skill_catalog = _load_skill_catalog_with_cursor(
        cursor,
        target_lang=target_lang,
        authored_mastery_leaves_only=(str(target_lang or "").strip().lower() == "de"),
    )
    excluded_sentence_keys = set(recent_sentence_keys or set())
    excluded_sentence_keys.update(
        _normalize_sentence_text_key(str(item.get("sentence") or ""))
        for item in normalized_entries
        if str(item.get("sentence") or "").strip()
    )
    requested_count = max(2, minimum_ready - len(normalized_entries))
    generated_entries = await _generate_legacy_sentence_entries_with_profiles(
        topic=topic,
        level=level,
        target_count=requested_count,
        skill_catalog=skill_catalog,
        focus_hint=resolved_focus,
    )
    filtered_entries = _filter_sentence_entries_for_session(
        generated_entries,
        level=level_key,
        excluded_sentence_keys=excluded_sentence_keys,
    )
    if filtered_entries and focus_kind == "preset":
        _upsert_shared_sentence_pool_entries_with_cursor(
            cursor,
            focus=resolved_focus,
            level=level,
            source_lang=source_lang,
            target_lang=target_lang,
            entries=filtered_entries,
        )
    merged_entries = _normalize_sentence_entries(normalized_entries + filtered_entries)
    return merged_entries, {
        "quick_topup_ms": int((time.perf_counter() - started_at) * 1000),
        "quick_topup_requested": int(requested_count),
        "quick_topup_generated": int(len(generated_entries)),
        "quick_topup_added": max(0, int(len(merged_entries) - len(normalized_entries))),
    }


async def prewarm_shared_translation_sentence_pool(
    *,
    focuses: list[dict[str, Any]] | None,
    levels: list[str] | None = None,
    source_lang: str = "ru",
    target_lang: str = "de",
    target_ready_per_bucket: int = 8,
    max_generate_per_bucket: int = 4,
) -> dict[str, Any]:
    started_at = time.perf_counter()
    candidate_focuses = [
        focus
        for focus in (focuses or [])
        if isinstance(focus, dict) and str(focus.get("kind") or "").strip().lower() == "preset"
    ]
    normalized_levels = [
        _normalize_level(level)
        for level in (levels or ["b1", "b2", "c1"])
        if str(level or "").strip()
    ] or ["b1", "b2", "c1"]
    if not candidate_focuses:
        return {
            "ok": True,
            "focuses": 0,
            "levels": len(normalized_levels),
            "generated": 0,
            "upserted": 0,
            "bucket_results": [],
            "elapsed_ms": 0,
        }

    conn = get_db_connection()
    cursor = conn.cursor()
    skill_catalog: list[dict[str, str]] | None = None
    bucket_results: list[dict[str, Any]] = []
    generated_total = 0
    upserted_total = 0
    try:
        for focus in candidate_focuses:
            for level_key in normalized_levels:
                ready_before = _count_shared_sentence_pool_entries_with_cursor(
                    cursor,
                    focus=focus,
                    level=level_key,
                    source_lang=source_lang,
                    target_lang=target_lang,
                )
                if ready_before >= int(target_ready_per_bucket):
                    bucket_results.append(
                        {
                            "focus_key": str(focus.get("key") or "").strip(),
                            "level": level_key,
                            "ready_before": int(ready_before),
                            "ready_after": int(ready_before),
                            "generated": 0,
                            "upserted": 0,
                            "skipped": "already_ready",
                        }
                    )
                    continue
                if skill_catalog is None:
                    skill_catalog = _load_skill_catalog_with_cursor(
                        cursor,
                        target_lang=target_lang,
                        authored_mastery_leaves_only=(str(target_lang or "").strip().lower() == "de"),
                    )
                existing_sentence_keys = _list_shared_sentence_pool_sentence_keys_with_cursor(
                    cursor,
                    focus=focus,
                    level=level_key,
                    source_lang=source_lang,
                    target_lang=target_lang,
                )
                requested_count = max(
                    1,
                    min(
                        int(max_generate_per_bucket),
                        int(target_ready_per_bucket) - int(ready_before),
                    ),
                )
                generated_entries = await _generate_legacy_sentence_entries_with_profiles(
                    topic=str(focus.get("prompt_topic") or focus.get("label") or "Grammar practice").strip(),
                    level=level_key,
                    target_count=requested_count,
                    skill_catalog=skill_catalog,
                    focus_hint=focus,
                )
                filtered_entries = _filter_sentence_entries_for_session(
                    generated_entries,
                    level=level_key,
                    excluded_sentence_keys=existing_sentence_keys,
                )
                upserted = _upsert_shared_sentence_pool_entries_with_cursor(
                    cursor,
                    focus=focus,
                    level=level_key,
                    source_lang=source_lang,
                    target_lang=target_lang,
                    entries=filtered_entries,
                )
                conn.commit()
                ready_after = _count_shared_sentence_pool_entries_with_cursor(
                    cursor,
                    focus=focus,
                    level=level_key,
                    source_lang=source_lang,
                    target_lang=target_lang,
                )
                generated_total += int(len(filtered_entries))
                upserted_total += int(upserted)
                bucket_results.append(
                    {
                        "focus_key": str(focus.get("key") or "").strip(),
                        "level": level_key,
                        "ready_before": int(ready_before),
                        "ready_after": int(ready_after),
                        "generated": int(len(filtered_entries)),
                        "upserted": int(upserted),
                        "requested": int(requested_count),
                    }
                )
        return {
            "ok": True,
            "focuses": int(len(candidate_focuses)),
            "levels": int(len(normalized_levels)),
            "generated": int(generated_total),
            "upserted": int(upserted_total),
            "bucket_results": bucket_results,
            "elapsed_ms": int((time.perf_counter() - started_at) * 1000),
        }
    finally:
        cursor.close()
        conn.close()


_AUTHORED_PRIMARY_SKILL_SENTENCE_HINTS: dict[str, tuple[str, ...]] = {
    "de_moods_reported_speech_indirekte_rede": (
        "по словам",
        "по данным",
        "как сообщ",
        "сообщается",
        "сообщил",
        "сообщила",
        "сообщили",
        "сообщают",
        "заявил",
        "заявила",
        "заявили",
        "сказал",
        "сказала",
        "сказали",
        "объяснил",
        "объяснила",
        "объяснили",
        "отметил",
        "отметила",
        "отметили",
        "передал",
        "передала",
        "передали",
        "утвержд",
        "согласно",
    ),
    "moods_subjunctive1": (
        "по словам",
        "по данным",
        "как сообщ",
        "сообщается",
        "сообщил",
        "сообщила",
        "сообщили",
        "сообщают",
        "заявил",
        "заявила",
        "заявили",
        "сказал",
        "сказала",
        "сказали",
        "объяснил",
        "объяснила",
        "объяснили",
        "отметил",
        "отметила",
        "отметили",
        "передал",
        "передала",
        "передали",
        "утвержд",
        "согласно",
    ),
    "de_clauses_sentence_types_relative_clauses": ("котор", "чей", "где", "куда", "откуда"),
    "de_clauses_sentence_types_conditionals_wenn_falls": ("если", "в случае"),
    "moods_subjunctive2": ("если бы", "мог бы", "могла бы", "могли бы", "хотел бы", "хотела бы", "хотели бы", "следовало бы", "стоило бы"),
    "de_moods_konjunktiv_ii_wuerde_form": ("если бы", "мог бы", "могла бы", "могли бы", "хотел бы", "хотела бы", "хотели бы", "следовало бы", "стоило бы"),
    "de_voice_active_passive_vorgangspassiv_werden_partizip_ii": ("был", "была", "было", "были", "проводил", "проводилась", "проводилось", "проводились", "отлож", "получен", "сделан", "интерпретир"),
    "de_voice_active_passive_zustandspassiv_sein_partizip_ii": ("был", "была", "было", "были", "остаётся", "находится"),
}

REMEDIATION_PRIMARY_DEMOTION_SKILL_IDS: set[str] = {
    "de_articles_determiners_definite_articles_der_die_das",
    "de_articles_determiners_indefinite_articles_ein_eine",
    "adjectives_case_agreement",
    "adjectives_endings_general",
}

STRUCTURAL_PRIMARY_NOISE_SKILL_IDS: set[str] = {
    "de_orthography_spelling_common_spelling_errors",
    "de_punctuation_comma_in_subordinate_clause",
    *REMEDIATION_PRIMARY_DEMOTION_SKILL_IDS,
}
REPORTED_SPEECH_PRIMARY_SKILL_IDS: set[str] = {
    "moods_subjunctive1",
    "de_moods_reported_speech_indirekte_rede",
}
CLAUSE_LEVEL_PRIMARY_SKILL_IDS: set[str] = {
    "word_order_subordinate_clause",
    "de_clauses_sentence_types_concessive_clauses_obwohl",
    "de_clauses_sentence_types_conditionals_wenn_falls",
    "de_clauses_sentence_types_purpose_clauses_damit_um_zu",
    "de_clauses_sentence_types_relative_clauses",
    "moods_subjunctive2",
}

REMEDIATION_SENTENCE_ANCHOR_RULES: tuple[dict[str, Any], ...] = (
    {
        "rule_id": "hypothetical_advice_not_stal",
        "substring_markers": (" бы не стал ", " бы не стала ", " бы не стали ", " бы на твоем месте ", " бы на твоём месте "),
        "primary": ("moods_subjunctive2",),
        "secondary": ("word_order_subordinate_clause", "de_voice_active_passive_vorgangspassiv_werden_partizip_ii"),
        "supporting": ("de_clauses_sentence_types_main_vs_subordinate_clause",),
        "structural": True,
        "priority": 5.25,
        "suppressed_primary": (
            "cases_accusative",
            "cases_dative",
            "de_articles_determiners_definite_articles_der_die_das",
            "de_articles_determiners_demonstratives_dieser_jener",
            "de_articles_determiners_indefinite_articles_ein_eine",
        ),
    },
    {
        "rule_id": "reporting_despite_clause",
        "substring_markers": (" заметил что несмотря ", " заметила что несмотря ", " заметили что несмотря "),
        "primary": ("word_order_subordinate_clause",),
        "secondary": ("de_clauses_sentence_types_concessive_clauses_obwohl", "adjectives_comparative"),
        "supporting": ("prepositions_usage",),
        "structural": True,
        "priority": 5.1,
        "suppressed_primary": ("prepositions_usage",),
    },
    {
        "rule_id": "concessive_clause_despite_that",
        "markers": ("несмотря на то что",),
        "primary": ("de_clauses_sentence_types_concessive_clauses_obwohl",),
        "secondary": ("word_order_subordinate_clause", "de_voice_active_passive_zustandspassiv_sein_partizip_ii"),
        "supporting": ("de_clauses_sentence_types_main_vs_subordinate_clause",),
        "structural": True,
        "priority": 4.5,
        "suppressed_primary": ("prepositions_usage", "word_order_subordinate_clause"),
    },
    {
        "markers": ("несмотря на",),
        "primary": ("prepositions_usage",),
        "secondary": ("de_cases_case_after_preposition", "cases_preposition_genitive"),
        "supporting": ("cases_genitive",),
        "structural": True,
        "priority": 1.75,
        "negative_markers": ("несмотря на то что",),
    },
    {
        "rule_id": "explicit_subordinate_frame",
        "markers": ("кажется что", "считают что", "объявлено что", "было объявлено что", "объяснил что", "объяснил врачу что", "подчеркивают что", "подчёркивают что"),
        "primary": ("word_order_subordinate_clause",),
        "secondary": ("de_clauses_sentence_types_main_vs_subordinate_clause",),
        "supporting": ("verbs_placement_subordinate",),
        "structural": True,
        "priority": 4.0,
        "suppressed_primary": ("moods_subjunctive1", "de_moods_reported_speech_indirekte_rede"),
    },
    {
        "rule_id": "plain_conditional",
        "substring_markers": (" если ",),
        "primary": ("de_clauses_sentence_types_conditionals_wenn_falls",),
        "secondary": ("word_order_subordinate_clause",),
        "supporting": ("de_clauses_sentence_types_main_vs_subordinate_clause",),
        "structural": True,
        "priority": 2.75,
        "negative_markers": ("если бы",),
    },
    {
        "markers": ("чтобы",),
        "primary": ("de_clauses_sentence_types_purpose_clauses_damit_um_zu",),
        "secondary": ("word_order_subordinate_clause", "verbs_modals", "de_verbs_verb_valency_missing_object_complement"),
        "supporting": ("de_punctuation_comma_in_subordinate_clause",),
        "structural": True,
        "priority": 4.0,
    },
    {
        "markers": ("хотя",),
        "primary": ("de_clauses_sentence_types_concessive_clauses_obwohl",),
        "secondary": ("word_order_subordinate_clause", "de_clauses_sentence_types_main_vs_subordinate_clause"),
        "supporting": ("de_punctuation_comma_in_subordinate_clause",),
        "structural": True,
        "priority": 4.0,
    },
    {
        "markers": ("если бы",),
        "primary": ("moods_subjunctive2",),
        "secondary": ("de_clauses_sentence_types_conditionals_wenn_falls", "de_moods_konjunktiv_ii_wuerde_form", "de_voice_active_passive_zustandspassiv_sein_partizip_ii"),
        "supporting": ("word_order_subordinate_clause",),
        "structural": True,
        "priority": 5.0,
        "suppressed_primary": ("de_clauses_sentence_types_conditionals_wenn_falls",),
    },
    {
        "markers": ("вынужден", "вынуждена", "вынуждены", "вынуждено"),
        "primary": ("de_infinitive_participles_zu_infinitive",),
        "secondary": ("nouns_compounds", "prepositions_usage", "verbs_modals"),
        "supporting": ("de_clauses_sentence_types_infinitive_clauses_vs_dass_clause",),
        "structural": True,
        "priority": 4.0,
        "suppressed_primary": ("verbs_modals",),
    },
    {
        "rule_id": "relative_clause",
        "substring_markers": (" котор",),
        "primary": ("de_clauses_sentence_types_relative_clauses",),
        "secondary": ("word_order_subordinate_clause",),
        "supporting": ("word_order_v2_rule",),
        "structural": True,
        "priority": 3.5,
    },
    {
        "rule_id": "participial_modifier",
        "substring_markers": (" позволяющ", " разработанн"),
        "secondary": ("de_clauses_sentence_types_relative_clauses",),
        "supporting": ("word_order_subordinate_clause",),
        "structural": True,
        "priority": 2.25,
    },
    {
        "rule_id": "result_state_passive",
        "substring_markers": (" не изучен", " не изучены", " оказался поставлен", " остались нереш", " остаются недооцен", " остаётся недооцен"),
        "primary": ("de_voice_active_passive_zustandspassiv_sein_partizip_ii",),
        "secondary": ("de_voice_active_passive_vorgangspassiv_werden_partizip_ii", "word_order_subordinate_clause"),
        "supporting": ("de_word_order_placement_of_participle_perfekt_passive",),
        "structural": True,
        "priority": 3.0,
    },
    {
        "rule_id": "negation_secondary",
        "substring_markers": (" не исчез",),
        "secondary": ("de_negation_negation_placement",),
        "structural": False,
        "priority": 2.0,
    },
    {
        "markers": ("остаются недооцен", "остается недооцен", "остаётся недооцен"),
        "primary": ("de_voice_active_passive_zustandspassiv_sein_partizip_ii",),
        "secondary": ("de_voice_active_passive_vorgangspassiv_werden_partizip_ii", "de_word_order_placement_of_participle_perfekt_passive"),
        "supporting": ("de_voice_active_passive_passive_word_order",),
        "structural": True,
        "priority": 3.0,
    },
)


def _normalize_sentence_anchor_text(sentence: str | None) -> str:
    normalized = str(sentence or "").strip().lower()
    if not normalized:
        return ""
    normalized = re.sub(r"[^\w\s]+", " ", normalized, flags=re.UNICODE)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return f" {normalized} "


def _collect_sentence_anchor_skill_weights(sentence: str | None) -> tuple[dict[str, float], bool]:
    anchor_context = _collect_sentence_anchor_context(sentence)
    return dict(anchor_context.get("anchored_skill_weights") or {}), bool(anchor_context.get("has_structural_anchor"))


def _sentence_has_strict_reported_speech_semantics(sentence: str | None) -> bool:
    normalized_sentence = _normalize_sentence_anchor_text(sentence)
    if not normalized_sentence:
        return False
    markers = _AUTHORED_PRIMARY_SKILL_SENTENCE_HINTS.get("de_moods_reported_speech_indirekte_rede") or ()
    return any(str(marker or "").strip().lower() in normalized_sentence for marker in markers)


def _collect_sentence_anchor_context(sentence: str | None) -> dict[str, Any]:
    normalized_sentence = _normalize_sentence_anchor_text(sentence)
    if not normalized_sentence:
        return {
            "anchored_skill_weights": {},
            "has_structural_anchor": False,
            "matched_rule_ids": [],
            "suppressed_primary_skill_ids": set(),
            "strong_clause_anchor_present": False,
            "reported_speech_semantics": False,
        }

    anchored_skill_weights: dict[str, float] = {}
    has_structural_anchor = False
    matched_rule_ids: list[str] = []
    structural_rule_count = 0
    suppressed_primary_skill_ids: set[str] = set()
    for rule in REMEDIATION_SENTENCE_ANCHOR_RULES:
        markers = tuple(str(marker or "").strip().lower() for marker in rule.get("markers") or ())
        substring_markers = tuple(str(marker or "").strip().lower() for marker in rule.get("substring_markers") or ())
        negative_markers = tuple(str(marker or "").strip().lower() for marker in rule.get("negative_markers") or ())
        if negative_markers and any(marker in normalized_sentence for marker in negative_markers):
            continue
        if markers and any(marker in normalized_sentence for marker in markers):
            matched = True
        else:
            matched = bool(substring_markers and any(marker in normalized_sentence for marker in substring_markers))
        if not matched:
            continue
        matched_rule_ids.append(str(rule.get("rule_id") or "|".join(markers or substring_markers)))
        if bool(rule.get("structural")):
            has_structural_anchor = True
            structural_rule_count += 1
        priority = max(1.0, float(rule.get("priority") or 1.0))
        suppressed_primary_skill_ids.update(
            str(skill_id or "").strip()
            for skill_id in (rule.get("suppressed_primary") or ())
            if str(skill_id or "").strip()
        )
        for skill_id in rule.get("primary") or ():
            anchored_skill_weights[str(skill_id)] = anchored_skill_weights.get(str(skill_id), 0.0) + (8.5 * priority)
        for skill_id in rule.get("secondary") or ():
            anchored_skill_weights[str(skill_id)] = anchored_skill_weights.get(str(skill_id), 0.0) + (5.25 * priority)
        for skill_id in rule.get("supporting") or ():
            anchored_skill_weights[str(skill_id)] = anchored_skill_weights.get(str(skill_id), 0.0) + (2.5 * priority)

    if structural_rule_count >= 2:
        anchored_skill_weights["word_order_subordinate_clause"] = anchored_skill_weights.get("word_order_subordinate_clause", 0.0) + 9.0
        anchored_skill_weights["de_clauses_sentence_types_main_vs_subordinate_clause"] = anchored_skill_weights.get("de_clauses_sentence_types_main_vs_subordinate_clause", 0.0) + 5.0
    if "concessive_clause_despite_that" in matched_rule_ids:
        anchored_skill_weights["prepositions_usage"] = anchored_skill_weights.get("prepositions_usage", 0.0) * 0.55

    return {
        "anchored_skill_weights": anchored_skill_weights,
        "has_structural_anchor": has_structural_anchor,
        "matched_rule_ids": matched_rule_ids,
        "suppressed_primary_skill_ids": suppressed_primary_skill_ids,
        "strong_clause_anchor_present": any(
            rule_id in {
                "concessive_clause_despite_that",
                "explicit_subordinate_frame",
                "relative_clause",
            }
            for rule_id in matched_rule_ids
        ),
        "reported_speech_semantics": _sentence_has_strict_reported_speech_semantics(sentence),
    }


def _should_suppress_primary_candidate(
    *,
    skill_id: str,
    anchor_context: dict[str, Any],
) -> bool:
    normalized_skill_id = str(skill_id or "").strip()
    if not normalized_skill_id:
        return True
    if normalized_skill_id in REPORTED_SPEECH_PRIMARY_SKILL_IDS and not bool(anchor_context.get("reported_speech_semantics")):
        return True
    if normalized_skill_id == "de_voice_active_passive_zustandspassiv_sein_partizip_ii" and "moods_subjunctive2" in set((anchor_context.get("anchored_skill_weights") or {}).keys()):
        return True
    if bool(anchor_context.get("has_structural_anchor")) and normalized_skill_id in STRUCTURAL_PRIMARY_NOISE_SKILL_IDS:
        return True
    if normalized_skill_id in set(anchor_context.get("suppressed_primary_skill_ids") or set()):
        return True
    if normalized_skill_id == "prepositions_usage" and bool(anchor_context.get("strong_clause_anchor_present")):
        return True
    return False


def rerank_tested_skill_profile_for_sentence(
    sentence: str | None,
    tested_skill_profile: list[dict[str, Any]] | None,
    *,
    profile_source: str | None = None,
    profile_confidence: float | None = None,
) -> list[dict[str, Any]]:
    profile = [dict(item) for item in list(tested_skill_profile or []) if str(item.get("skill_id") or "").strip()]
    anchor_context = _collect_sentence_anchor_context(sentence)
    anchored_skill_weights = dict(anchor_context.get("anchored_skill_weights") or {})
    if not profile and not anchored_skill_weights:
        return []

    default_source = str(
        profile_source
        or ((profile[0] or {}).get("profile_source") if profile else "")
        or AUTHORED_PROFILE_SOURCE
    ).strip() or AUTHORED_PROFILE_SOURCE
    try:
        default_confidence = float(
            profile_confidence
            if profile_confidence is not None
            else ((profile[0] or {}).get("profile_confidence") if profile else AUTHORED_PROFILE_CONFIDENCE)
        )
    except Exception:
        default_confidence = AUTHORED_PROFILE_CONFIDENCE
    default_confidence = max(0.0, min(1.0, default_confidence))

    base_role_scores = {
        "primary": 10.0,
        "secondary": 6.0,
        "supporting": 3.0,
    }
    candidate_scores: dict[str, float] = {}
    for item in profile:
        skill_id = str(item.get("skill_id") or "").strip()
        if not skill_id:
            continue
        role = str(item.get("role") or "supporting").strip()
        candidate_scores[skill_id] = candidate_scores.get(skill_id, 0.0) + base_role_scores.get(role, 1.0)

    for skill_id, bonus in anchored_skill_weights.items():
        skill_key = str(skill_id or "").strip()
        if not skill_key:
            continue
        candidate_scores[skill_key] = candidate_scores.get(skill_key, 0.0) + float(bonus or 0.0)

    if bool(anchor_context.get("has_structural_anchor")):
        for skill_id in list(candidate_scores.keys()):
            if skill_id in STRUCTURAL_PRIMARY_NOISE_SKILL_IDS and skill_id not in anchored_skill_weights:
                candidate_scores[skill_id] = candidate_scores.get(skill_id, 0.0) * 0.12
            if skill_id in REPORTED_SPEECH_PRIMARY_SKILL_IDS and not bool(anchor_context.get("reported_speech_semantics")):
                candidate_scores[skill_id] = candidate_scores.get(skill_id, 0.0) * 0.1
            if skill_id == "prepositions_usage" and bool(anchor_context.get("strong_clause_anchor_present")):
                candidate_scores[skill_id] = candidate_scores.get(skill_id, 0.0) * 0.45
            if skill_id == "de_clauses_sentence_types_conditionals_wenn_falls" and "moods_subjunctive2" in anchored_skill_weights:
                candidate_scores[skill_id] = candidate_scores.get(skill_id, 0.0) * 0.72
            if skill_id == "verbs_modals" and "de_infinitive_participles_zu_infinitive" in anchored_skill_weights:
                candidate_scores[skill_id] = candidate_scores.get(skill_id, 0.0) * 0.68
            if skill_id == "de_voice_active_passive_zustandspassiv_sein_partizip_ii" and "moods_subjunctive2" in anchored_skill_weights:
                candidate_scores[skill_id] = candidate_scores.get(skill_id, 0.0) * 0.58
    if bool(anchor_context.get("strong_clause_anchor_present")):
        candidate_scores["word_order_subordinate_clause"] = candidate_scores.get("word_order_subordinate_clause", 0.0) + 4.0

    ranked_skill_ids = [
        skill_id
        for skill_id, _score in sorted(
            candidate_scores.items(),
            key=lambda item: (-float(item[1] or 0.0), item[0]),
        )
    ]
    primary_skill_id = None
    for skill_id in ranked_skill_ids:
        if not _should_suppress_primary_candidate(
            skill_id=skill_id,
            anchor_context=anchor_context,
        ):
            primary_skill_id = skill_id
            break
    if primary_skill_id is None and ranked_skill_ids:
        primary_skill_id = ranked_skill_ids[0]
    if not primary_skill_id:
        return []

    remaining_skill_ids = [skill_id for skill_id in ranked_skill_ids if skill_id != primary_skill_id]
    secondary_skill_ids = remaining_skill_ids[:2]
    supporting_skill_ids = remaining_skill_ids[2:3]
    return _build_skill_profile_from_skill_ids(
        primary_skill_id=primary_skill_id,
        secondary_skill_ids=secondary_skill_ids,
        supporting_skill_ids=supporting_skill_ids,
        profile_source=default_source,
        profile_confidence=default_confidence,
    )


def _load_sentence_text_for_remediation_with_cursor(
    cursor,
    *,
    sentence_id_for_mistake: int,
    source_lang: str = "ru",
    target_lang: str = "de",
) -> str:
    cursor.execute(
        """
        SELECT sentence
        FROM bt_3_daily_sentences
        WHERE id_for_mistake_table = %s
          AND COALESCE(source_lang, 'ru') = %s
          AND COALESCE(target_lang, 'de') = %s
        ORDER BY id DESC
        LIMIT 1;
        """,
        (int(sentence_id_for_mistake), source_lang or "ru", target_lang or "de"),
    )
    row = cursor.fetchone()
    return str(row[0] or "").strip() if row and row[0] else ""


def _authored_sentence_matches_primary_skill(
    *,
    sentence: str,
    primary_skill_id: str,
) -> bool:
    normalized_sentence = _normalize_sentence_anchor_text(sentence)
    normalized_primary_skill_id = str(primary_skill_id or "").strip()
    if not normalized_sentence or not normalized_primary_skill_id:
        return False
    if normalized_primary_skill_id in REPORTED_SPEECH_PRIMARY_SKILL_IDS:
        return _sentence_has_strict_reported_speech_semantics(sentence)
    required_markers = _AUTHORED_PRIMARY_SKILL_SENTENCE_HINTS.get(normalized_primary_skill_id)
    if not required_markers:
        return True
    return any(str(marker or "").strip().lower() in normalized_sentence for marker in required_markers)


def _parse_generated_sentence_entries_payload(
    content: str,
    *,
    valid_skill_ids: set[str],
    profile_source: str = AUTHORED_PROFILE_SOURCE,
    profile_confidence: float = AUTHORED_PROFILE_CONFIDENCE,
) -> list[dict[str, Any]]:
    cleaned = str(content or "").strip()
    if not cleaned:
        raise ValueError("empty_generation_payload")
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```[a-zA-Z]*\n?", "", cleaned).rstrip("`").strip()
    payload = json.loads(cleaned)
    items = payload.get("items") if isinstance(payload, dict) else None
    if not isinstance(items, list) or not items:
        raise ValueError("missing_generation_items")

    entries: list[dict[str, Any]] = []
    dropped_item_errors: list[str] = []
    for raw_item in items:
        if not isinstance(raw_item, dict):
            dropped_item_errors.append("generation_item_not_object")
            continue
        sentence = str(raw_item.get("sentence") or "").strip()
        primary_skill_id = str(raw_item.get("primary_skill_id") or "").strip()
        secondary_skill_ids = [
            str(skill_id).strip()
            for skill_id in (raw_item.get("secondary_skill_ids") or [])
            if str(skill_id).strip()
        ] if isinstance(raw_item.get("secondary_skill_ids"), list) else []
        supporting_skill_ids = [
            str(skill_id).strip()
            for skill_id in (raw_item.get("supporting_skill_ids") or [])
            if str(skill_id).strip()
        ] if isinstance(raw_item.get("supporting_skill_ids"), list) else []
        if not sentence or not primary_skill_id:
            dropped_item_errors.append("generation_item_missing_fields")
            continue
        if primary_skill_id not in valid_skill_ids:
            dropped_item_errors.append(f"invalid_primary_skill:{primary_skill_id}")
            continue
        if len(secondary_skill_ids) < 1 or len(secondary_skill_ids) > 2:
            dropped_item_errors.append("invalid_secondary_count")
            continue
        if len(supporting_skill_ids) > 1:
            dropped_item_errors.append("invalid_supporting_count")
            continue
        ordered_ids = [primary_skill_id] + secondary_skill_ids + supporting_skill_ids
        if any(skill_id not in valid_skill_ids for skill_id in ordered_ids):
            dropped_item_errors.append("invalid_profile_skill_id")
            continue
        if len(set(ordered_ids)) != len(ordered_ids):
            dropped_item_errors.append("duplicate_profile_skill_id")
            continue
        if not _authored_sentence_matches_primary_skill(
            sentence=sentence,
            primary_skill_id=primary_skill_id,
        ):
            dropped_item_errors.append(f"primary_skill_sentence_mismatch:{primary_skill_id}")
            continue
        entries.append(
            {
                "sentence": sentence,
                "tested_skill_profile": rerank_tested_skill_profile_for_sentence(
                    sentence,
                    _build_skill_profile_from_skill_ids(
                        primary_skill_id=primary_skill_id,
                        secondary_skill_ids=secondary_skill_ids,
                        supporting_skill_ids=supporting_skill_ids,
                        profile_source=profile_source,
                        profile_confidence=profile_confidence,
                    ),
                    profile_source=profile_source,
                    profile_confidence=profile_confidence,
                ),
            }
        )
    if dropped_item_errors:
        logging.warning(
            "Structured sentence generation dropped invalid items: kept=%s dropped=%s sample=%s",
            len(entries),
            len(dropped_item_errors),
            dropped_item_errors[:3],
        )
    if not entries:
        raise ValueError(
            "no_generation_items_after_validation"
            + (f":{dropped_item_errors[0]}" if dropped_item_errors else "")
        )
    return entries


async def _generate_sentence_entries_with_profiles(
    *,
    task_name: str,
    system_instruction_key: str,
    user_message: str,
    target_count: int,
    level: str | None,
    valid_skill_ids: set[str],
) -> list[dict[str, Any]]:
    level_key = _normalize_level(level)
    for attempt in range(5):
        try:
            content = await llm_execute(
                task_name=task_name,
                system_instruction_key=system_instruction_key,
                user_message=user_message,
                poll_interval_seconds=1.0,
            )
            parsed_entries = _parse_generated_sentence_entries_payload(
                content,
                valid_skill_ids=valid_skill_ids,
            )
            normalized_entries = _normalize_sentence_entries(parsed_entries)
            filtered_entries = [
                item
                for item in normalized_entries
                if item.get("sentence")
                and _filter_sentences_for_level([str(item.get("sentence") or "")], level_key)
            ]
            if len(filtered_entries) >= target_count and all(item.get("tested_skill_profile") for item in filtered_entries[:target_count]):
                return filtered_entries[:target_count]
        except openai.RateLimitError:
            wait_time = (attempt + 1) * 2
            await asyncio.sleep(wait_time)
        except Exception:
            logging.exception("Structured sentence generation attempt failed")
    return []


async def _generate_legacy_sentence_entries_with_profiles(
    *,
    topic: str,
    level: str | None,
    target_count: int,
    skill_catalog: list[dict[str, str]],
    focus_hint: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    level_key = _normalize_level(level)
    instruction_level_key = "a2" if level_key == "a1" else level_key
    task_name = f"generate_sentences_{instruction_level_key}"
    system_instruction_key = f"generate_sentences_{instruction_level_key}"
    level_notes = {
        "a1": "A1 only: very short, very concrete everyday sentences. No subordinate clauses. Prefer 3-8 words.",
        "a2": "A2 only: short, concrete, everyday sentences. No heavy subordinate clauses. Prefer 4-12 words.",
        "b1": "B1 only: moderately simple sentences with at most one light subordinate clause. Prefer 6-18 words.",
        "b2": "B2 only: clearly more developed sentences with some clause complexity. Prefer 9-24 words.",
        "c1": "C1 only: advanced sentences with visible syntactic complexity. Prefer 12-30 words.",
        "c2": "C2 only: very advanced, nuanced, syntactically dense sentences. Prefer 15-36 words.",
    }
    user_message = json.dumps(
        {
            "count": int(max(1, target_count)),
            "topic": str(topic or "General").strip(),
            "level": level_key,
            "level_note": level_notes.get(level_key, ""),
            "source_language": "ru",
            "target_language": "de",
            "skill_catalog": skill_catalog,
            "focus_hint": focus_hint if isinstance(focus_hint, dict) else None,
        },
        ensure_ascii=False,
    )
    return await _generate_sentence_entries_with_profiles(
        task_name=task_name,
        system_instruction_key=system_instruction_key,
        user_message=user_message,
        target_count=target_count,
        level=level,
        valid_skill_ids={str(item.get("skill_id") or "").strip() for item in skill_catalog if str(item.get("skill_id") or "").strip()},
    )


async def get_original_sentences_webapp(
    cursor,
    *,
    user_id: int,
    topic: str = "Random sentences",
    level: str | None = None,
    source_lang: str = "ru",
    target_lang: str = "de",
    generation_profile_seed: dict[str, Any] | None = None,
    grammar_focus: dict[str, Any] | None = None,
    target_count: int = 7,
    diagnostics: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    target_total = max(1, int(target_count or 7))
    level_key = _normalize_level(level)
    resolved_focus = grammar_focus if isinstance(grammar_focus, dict) else resolve_webapp_focus(topic)
    focus_kind = str(resolved_focus.get("kind") or "").strip().lower()
    recent_sentence_keys = _get_recently_served_sentence_keys_with_cursor(
        cursor,
        user_id=int(user_id),
        source_lang=source_lang,
        target_lang=target_lang,
    )
    skill_catalog = _load_skill_catalog_with_cursor(
        cursor,
        target_lang=target_lang,
        authored_mastery_leaves_only=(str(target_lang or "").strip().lower() == "de"),
    )

    seed_diagnostics: dict[str, Any] = {}
    sentence_entries = _collect_seed_sentence_entries_with_cursor(
        cursor,
        user_id=int(user_id),
        level_key=level_key,
        source_lang=source_lang,
        target_lang=target_lang,
        resolved_focus=resolved_focus,
        recent_sentence_keys=recent_sentence_keys,
        target_count=target_total,
        diagnostics=seed_diagnostics,
    )
    num_sentences = target_total - len(sentence_entries)
    llm_elapsed_ms = 0
    llm_calls = 0
    if num_sentences > 0:
        llm_started_at = time.perf_counter()
        generated_entries = await _generate_legacy_sentence_entries_with_profiles(
            topic=topic,
            level=level,
            target_count=num_sentences,
            skill_catalog=skill_catalog,
            focus_hint=generation_profile_seed,
        )
        generated_entries = _filter_sentence_entries_for_session(
            generated_entries,
            level=level_key,
            excluded_sentence_keys=recent_sentence_keys.union(
                {
                    _normalize_sentence_text_key(str(item.get("sentence") or ""))
                    for item in sentence_entries
                    if str(item.get("sentence") or "").strip()
                }
            ),
            )
        if generated_entries and focus_kind == "preset":
            _upsert_shared_sentence_pool_entries_with_cursor(
                cursor,
                focus=resolved_focus,
                level=level,
                source_lang=source_lang,
                target_lang=target_lang,
                entries=generated_entries,
            )
        sentence_entries = _normalize_sentence_entries(sentence_entries + generated_entries)
        llm_elapsed_ms += int((time.perf_counter() - llm_started_at) * 1000)
        llm_calls += 1
    attempts = 0
    while len(sentence_entries) < target_total and attempts < 3:
        needed = target_total - len(sentence_entries)
        llm_started_at = time.perf_counter()
        extra_entries = await _generate_legacy_sentence_entries_with_profiles(
            topic=topic,
            level=level,
            target_count=needed,
            skill_catalog=skill_catalog,
            focus_hint=generation_profile_seed,
        )
        extra_entries = _filter_sentence_entries_for_session(
            extra_entries,
            level=level_key,
            excluded_sentence_keys=recent_sentence_keys.union(
                {
                    _normalize_sentence_text_key(str(item.get("sentence") or ""))
                    for item in sentence_entries
                    if str(item.get("sentence") or "").strip()
                }
            ),
        )
        if not extra_entries:
            break
        if focus_kind == "preset":
            _upsert_shared_sentence_pool_entries_with_cursor(
                cursor,
                focus=resolved_focus,
                level=level,
                source_lang=source_lang,
                target_lang=target_lang,
                entries=extra_entries,
            )
        sentence_entries = _normalize_sentence_entries(sentence_entries + extra_entries)
        llm_elapsed_ms += int((time.perf_counter() - llm_started_at) * 1000)
        llm_calls += 1
        attempts += 1
    if diagnostics is not None:
        diagnostics.update(seed_diagnostics)
        diagnostics.update(
            {
                "llm_ms": int(llm_elapsed_ms),
                "llm_calls": int(llm_calls),
                "result_count": int(len(sentence_entries)),
            }
        )
    return sentence_entries[:target_total]


def _collect_seed_sentence_entries_with_cursor(
    cursor,
    *,
    user_id: int,
    level_key: str,
    source_lang: str,
    target_lang: str,
    resolved_focus: dict[str, Any],
    recent_sentence_keys: set[str] | None = None,
    target_count: int = 7,
    diagnostics: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    started_at = time.perf_counter()
    target_total = max(1, int(target_count or 7))
    focus_kind = str(resolved_focus.get("kind") or "").strip().lower()
    blocked_sentence_keys = set(recent_sentence_keys or set())
    remediation_skill_seed_cache: dict[tuple[str, str], dict[str, str] | None] = {}
    remediation_membership_cache: dict[tuple[str, str], dict[str, Any] | None] = {}
    remediation_leaf_cache: dict[tuple[str, str], list[str]] = {}
    personal_rows_scanned = 0

    sentence_entries: list[dict[str, Any]] = []
    if focus_kind not in {"preset", "custom"}:
        cursor.execute("SELECT sentence FROM bt_3_sentences ORDER BY RANDOM() LIMIT 20;")
        rows = [
            sentence
            for sentence in _filter_sentences_for_level([row[0] for row in cursor.fetchall()], level_key)
            if _normalize_sentence_text_key(sentence) not in blocked_sentence_keys
        ][:min(1, target_total)]
        sentence_entries = [{"sentence": str(sentence or "").strip(), "tested_skill_profile": []} for sentence in rows if str(sentence or "").strip()]

    already_given_sentence_ids = set()
    seen_sentence_keys = set(blocked_sentence_keys)
    seen_sentence_keys.update(
        _normalize_sentence_text_key(str(item.get("sentence") or ""))
        for item in sentence_entries
        if str(item.get("sentence") or "").strip()
    )
    if focus_kind != "custom":
        personal_started_at = time.perf_counter()
        cursor.execute(
            """
            SELECT
                sentence,
                sentence_id,
                COALESCE(NULLIF(main_category, ''), 'Other mistake') AS main_category,
                COALESCE(NULLIF(sub_category, ''), 'Unclassified mistake') AS sub_category
            FROM bt_3_detailed_mistakes
            WHERE user_id = %s
            ORDER BY mistake_count DESC, COALESCE(last_seen, added_data, NOW()) ASC;
            """,
            (user_id,),
        )
        detailed_rows = cursor.fetchall() or []
        personal_rows_scanned = len(detailed_rows)
        for sentence, sentence_id, main_category, sub_category in detailed_rows:
            normalized_sentence = " ".join(str(sentence or "").strip().split())
            sentence_key = _normalize_sentence_text_key(normalized_sentence)
            if focus_kind == "preset" and not focus_matches_error_pair(resolved_focus, main_category, sub_category):
                continue
            if not normalized_sentence or not _sentence_fits_level(normalized_sentence, level_key):
                continue
            if not sentence_id or sentence_id in already_given_sentence_ids or sentence_key in seen_sentence_keys:
                continue
            already_given_sentence_ids.add(sentence_id)
            sentence_entries.append(
                {
                    "sentence": normalized_sentence,
                    "tested_skill_profile": _build_remediation_profile_with_cursor(
                        cursor,
                        user_id=int(user_id),
                        sentence_id_for_mistake=int(sentence_id),
                        target_lang=target_lang,
                        source_lang=source_lang,
                        sentence_text=normalized_sentence,
                        skill_seed_cache=remediation_skill_seed_cache,
                        membership_cache=remediation_membership_cache,
                        leaf_cache=remediation_leaf_cache,
                    ),
                }
            )
            seen_sentence_keys.add(sentence_key)
            if len([item for item in sentence_entries if item.get("tested_skill_profile")]) >= min(5, target_total):
                break
        personal_elapsed_ms = int((time.perf_counter() - personal_started_at) * 1000)
    else:
        personal_elapsed_ms = 0

    num_sentences = target_total - len(sentence_entries)
    personal_ready_count = len(sentence_entries)
    if num_sentences > 0 and focus_kind == "preset":
        pool_started_at = time.perf_counter()
        pooled_entries = _fetch_shared_sentence_pool_entries_with_cursor(
            cursor,
            focus=resolved_focus,
            level=level_key,
            source_lang=source_lang,
            target_lang=target_lang,
            exclude_sentences=(
                {str(item.get("sentence") or "") for item in sentence_entries}
                | blocked_sentence_keys
            ),
            limit=num_sentences,
        )
        if pooled_entries:
            sentence_entries = _normalize_sentence_entries(
                sentence_entries
                + _filter_sentence_entries_for_session(
                    pooled_entries,
                    level=level_key,
                    excluded_sentence_keys=seen_sentence_keys,
                )
            )
        pool_elapsed_ms = int((time.perf_counter() - pool_started_at) * 1000)
    else:
        pool_elapsed_ms = 0
    if diagnostics is not None:
        diagnostics.update(
            {
                "seed_total_ms": int((time.perf_counter() - started_at) * 1000),
                "personal_ms": int(personal_elapsed_ms),
                "pool_ms": int(pool_elapsed_ms),
                "personal_rows_scanned": int(personal_rows_scanned),
                "personal_added": int(personal_ready_count),
                "pool_added": max(0, int(len(sentence_entries) - personal_ready_count)),
                "seed_ready_count": int(len(sentence_entries)),
            }
        )
    return sentence_entries[:target_total]


def _normalize_sentence_text_key(sentence: str) -> str:
    return " ".join(str(sentence or "").strip().split()).lower()


def _get_recently_served_sentence_keys_with_cursor(
    cursor,
    *,
    user_id: int,
    source_lang: str,
    target_lang: str,
    limit: int = 50,
) -> set[str]:
    lookback_days = _get_recent_sentence_reuse_lookback_days()
    if lookback_days <= 0:
        return set()
    safe_limit = max(1, int(limit or 50))
    cursor.execute(
        """
        SELECT sentence
        FROM bt_3_daily_sentences
        WHERE user_id = %s
          AND COALESCE(source_lang, 'ru') = %s
          AND COALESCE(target_lang, 'de') = %s
          AND date >= (CURRENT_DATE - (%s::int * INTERVAL '1 day'))::date
        ORDER BY date DESC, unique_id DESC, id DESC
        LIMIT %s;
        """,
        (
            int(user_id),
            str(source_lang or "").strip().lower() or "ru",
            str(target_lang or "").strip().lower() or "de",
            int(lookback_days),
            safe_limit,
        ),
    )
    rows = cursor.fetchall() or []
    return {
        _normalize_sentence_text_key(row[0])
        for row in rows
        if row and row[0] and _normalize_sentence_text_key(row[0])
    }


def _filter_sentence_entries_for_session(
    sentence_entries: list[dict[str, Any]] | None,
    *,
    level: str | None,
    excluded_sentence_keys: set[str] | None = None,
) -> list[dict[str, Any]]:
    filtered_entries: list[dict[str, Any]] = []
    seen_sentence_keys = set(excluded_sentence_keys or set())
    for entry in _normalize_sentence_entries(list(sentence_entries or [])):
        sentence = str(entry.get("sentence") or "").strip()
        sentence_key = _normalize_sentence_text_key(sentence)
        if not sentence or not sentence_key or sentence_key in seen_sentence_keys:
            continue
        if not _sentence_fits_level(sentence, level):
            continue
        filtered_entries.append(
            {
                "sentence": sentence,
                "tested_skill_profile": list(entry.get("tested_skill_profile") or []),
            }
        )
        seen_sentence_keys.add(sentence_key)
    return filtered_entries


def _get_session_sentence_keys_with_cursor(
    cursor,
    *,
    user_id: int,
    session_id: int,
    source_lang: str,
    target_lang: str,
) -> set[str]:
    cursor.execute(
        """
        SELECT sentence
        FROM bt_3_daily_sentences
        WHERE user_id = %s
          AND session_id = %s
          AND COALESCE(source_lang, 'ru') = %s
          AND COALESCE(target_lang, 'de') = %s;
        """,
        (int(user_id), int(session_id), source_lang, target_lang),
    )
    return {
        _normalize_sentence_text_key(row[0])
        for row in (cursor.fetchall() or [])
        if row and row[0]
    }


def _summarize_sentence_entry_batch(
    sentence_entries: list[dict[str, Any]],
    *,
    existing_sentence_keys: set[str],
    limit: int,
) -> dict[str, int]:
    normalized_entries = _normalize_sentence_entries(sentence_entries)
    seen_keys = set(existing_sentence_keys or set())
    safe_limit = max(1, int(limit or 7))
    duplicate_existing = 0
    insertable = 0
    overflow = 0

    for entry in normalized_entries:
        sentence_key = _normalize_sentence_text_key(entry.get("sentence") or "")
        if not sentence_key:
            continue
        if sentence_key in seen_keys:
            duplicate_existing += 1
            continue
        if len(seen_keys) >= safe_limit:
            overflow += 1
            continue
        seen_keys.add(sentence_key)
        insertable += 1

    return {
        "raw_count": len(sentence_entries or []),
        "normalized_count": len(normalized_entries),
        "dropped_during_normalization": max(0, len(sentence_entries or []) - len(normalized_entries)),
        "duplicate_existing": duplicate_existing,
        "insertable": insertable,
        "overflow": overflow,
        "remaining_slots": max(0, safe_limit - len(existing_sentence_keys or set())),
    }


def _insert_sentence_entries_into_session_with_cursor(
    cursor,
    *,
    user_id: int,
    session_id: int,
    source_lang: str,
    target_lang: str,
    sentence_entries: list[dict[str, Any]],
    limit: int = 7,
) -> list[dict[str, Any]]:
    safe_limit = max(1, int(limit or 7))
    cursor.execute(
        """
        SELECT id, sentence
        FROM bt_3_daily_sentences
        WHERE user_id = %s
          AND session_id = %s
          AND COALESCE(source_lang, 'ru') = %s
          AND COALESCE(target_lang, 'de') = %s
        ORDER BY unique_id ASC, id ASC;
        """,
        (int(user_id), int(session_id), source_lang, target_lang),
    )
    existing_rows = cursor.fetchall() or []
    existing_sentence_keys = {
        _normalize_sentence_text_key(row[1])
        for row in existing_rows
        if row and row[1]
    }
    if len(existing_sentence_keys) >= safe_limit:
        return []

    cursor.execute(
        """
        SELECT COALESCE(MAX(unique_id), 0)
        FROM bt_3_daily_sentences
        WHERE user_id = %s
          AND date = CURRENT_DATE
          AND COALESCE(source_lang, 'ru') = %s
          AND COALESCE(target_lang, 'de') = %s;
        """,
        (int(user_id), source_lang, target_lang),
    )
    row = cursor.fetchone()
    next_unique_id = int(row[0] or 0) + 1
    created_sentence_profiles: list[tuple[int, list[dict[str, Any]]]] = []
    inserted_items: list[dict[str, Any]] = []

    for entry in _normalize_sentence_entries(sentence_entries):
        sentence = str(entry.get("sentence") or "").strip()
        if not sentence:
            continue
        sentence_key = _normalize_sentence_text_key(sentence)
        if not sentence_key or sentence_key in existing_sentence_keys:
            continue
        if len(existing_sentence_keys) >= safe_limit:
            break

        cursor.execute(
            """
            SELECT id_for_mistake_table
            FROM bt_3_daily_sentences
            WHERE sentence = %s
              AND COALESCE(source_lang, 'ru') = %s
              AND COALESCE(target_lang, 'de') = %s
            LIMIT 1;
            """,
            (sentence, source_lang, target_lang),
        )
        result = cursor.fetchone()
        if result:
            id_for_mistake_table = int(result[0])
        else:
            cursor.execute("SELECT MAX(id_for_mistake_table) FROM bt_3_daily_sentences;")
            max_row = cursor.fetchone()
            max_id = int(max_row[0] or 0) if max_row else 0
            id_for_mistake_table = max_id + 1

        cursor.execute(
            """
            INSERT INTO bt_3_daily_sentences (
                date,
                sentence,
                unique_id,
                user_id,
                session_id,
                id_for_mistake_table,
                source_lang,
                target_lang
            )
            VALUES (CURRENT_DATE, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id;
            """,
            (sentence, next_unique_id, int(user_id), int(session_id), id_for_mistake_table, source_lang, target_lang),
        )
        inserted_row = cursor.fetchone()
        if not inserted_row or not inserted_row[0]:
            continue
        created_sentence_profiles.append(
            (
                int(inserted_row[0]),
                list(entry.get("tested_skill_profile") or []),
            )
        )
        inserted_items.append(
            {
                "id_for_mistake_table": int(id_for_mistake_table),
                "sentence": sentence,
                "unique_id": int(next_unique_id),
                "source_session_id": str(session_id),
            }
        )
        existing_sentence_keys.add(sentence_key)
        next_unique_id += 1

    _insert_sentence_skill_targets_for_entries_with_cursor(
        cursor,
        sentence_profiles=created_sentence_profiles,
    )
    return inserted_items


def _translation_session_is_open_with_cursor(
    cursor,
    *,
    user_id: int,
    session_id: int,
) -> bool:
    cursor.execute(
        """
        SELECT 1
        FROM bt_3_user_progress
        WHERE user_id = %s
          AND session_id = %s
          AND completed = FALSE
        LIMIT 1;
        """,
        (int(user_id), int(session_id)),
    )
    return bool(cursor.fetchone())


async def fill_translation_session_webapp(
    *,
    user_id: int,
    username: str | None = None,
    session_id: int,
    topic: str = "Random sentences",
    level: str | None = None,
    source_lang: str = "ru",
    target_lang: str = "de",
    tested_skill_profile_seed: dict[str, Any] | None = None,
    grammar_focus: dict[str, Any] | None = None,
    target_count: int = 7,
    max_rounds: int = 4,
) -> dict[str, Any]:
    conn = get_db_connection()
    cursor = conn.cursor()
    advisory_lock_acquired = False
    round_diagnostics: list[dict[str, Any]] = []
    try:
        cursor.execute("SELECT pg_try_advisory_lock(%s);", (int(session_id),))
        lock_row = cursor.fetchone()
        advisory_lock_acquired = bool(lock_row and lock_row[0])
        if not advisory_lock_acquired:
            return {"session_id": int(session_id), "filled": 0, "ready_count": 0, "expected_total": int(target_count), "skipped": "lock"}

        total_inserted = 0
        current_count = 0
        for _round in range(max(1, int(max_rounds or 1))):
            if not _translation_session_is_open_with_cursor(
                cursor,
                user_id=int(user_id),
                session_id=int(session_id),
            ):
                break
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM bt_3_daily_sentences
                WHERE user_id = %s
                  AND session_id = %s
                  AND COALESCE(source_lang, 'ru') = %s
                  AND COALESCE(target_lang, 'de') = %s;
                """,
                (int(user_id), int(session_id), source_lang, target_lang),
            )
            count_row = cursor.fetchone()
            current_count = int(count_row[0] or 0) if count_row else 0
            if current_count >= int(target_count):
                break
            missing_count = max(0, int(target_count) - int(current_count))
            if missing_count <= 0:
                break

            existing_sentence_keys = _get_session_sentence_keys_with_cursor(
                cursor,
                user_id=int(user_id),
                session_id=int(session_id),
                source_lang=source_lang,
                target_lang=target_lang,
            )
            if _is_legacy_ru_de_pair(source_lang, target_lang):
                generation_diagnostics: dict[str, Any] = {}
                candidate_entries = await get_original_sentences_webapp(
                    cursor,
                    user_id=int(user_id),
                    topic=topic,
                    level=level,
                    source_lang=source_lang,
                    target_lang=target_lang,
                    generation_profile_seed=tested_skill_profile_seed,
                    grammar_focus=grammar_focus,
                    target_count=missing_count,
                    diagnostics=generation_diagnostics,
                )
            else:
                generation_diagnostics = {}
                skill_catalog = _load_skill_catalog_with_cursor(
                    cursor,
                    target_lang=target_lang,
                    authored_mastery_leaves_only=(str(target_lang or "").strip().lower() == "de"),
                )
                generated_entries = await _generate_sentence_entries_with_profiles(
                    task_name="generate_sentences_multilang",
                    system_instruction_key="generate_sentences_multilang",
                    user_message=json.dumps(
                        {
                            "source_language": (source_lang or "").strip().lower(),
                            "target_language": (target_lang or "").strip().lower(),
                            "level": (level or "b1").strip().lower(),
                            "topic": (topic or "General").strip(),
                            "count": int(max(1, int(missing_count))),
                            "skill_catalog": skill_catalog,
                            "focus_hint": tested_skill_profile_seed if isinstance(tested_skill_profile_seed, dict) else None,
                        },
                        ensure_ascii=False,
                    ),
                    target_count=int(max(1, int(missing_count))),
                    level=level,
                    valid_skill_ids={str(item.get("skill_id") or "").strip() for item in skill_catalog if str(item.get("skill_id") or "").strip()},
                )
                candidate_entries = _normalize_sentence_entries(generated_entries)

            batch_summary = _summarize_sentence_entry_batch(
                candidate_entries,
                existing_sentence_keys=existing_sentence_keys,
                limit=int(target_count),
            )
            inserted_items = _insert_sentence_entries_into_session_with_cursor(
                cursor,
                user_id=int(user_id),
                session_id=int(session_id),
                source_lang=source_lang,
                target_lang=target_lang,
                sentence_entries=candidate_entries,
                limit=int(target_count),
            )
            round_info = {
                "round": int(_round + 1),
                "before_count": int(current_count),
                "candidate_raw_count": int(batch_summary["raw_count"]),
                "candidate_normalized_count": int(batch_summary["normalized_count"]),
                "candidate_dropped_during_normalization": int(batch_summary["dropped_during_normalization"]),
                "candidate_duplicate_existing": int(batch_summary["duplicate_existing"]),
                "candidate_insertable": int(batch_summary["insertable"]),
                "candidate_overflow": int(batch_summary["overflow"]),
                "remaining_slots": int(batch_summary["remaining_slots"]),
                "requested_count": int(missing_count),
                "personal_ms": int(generation_diagnostics.get("personal_ms") or 0),
                "pool_ms": int(generation_diagnostics.get("pool_ms") or 0),
                "llm_ms": int(generation_diagnostics.get("llm_ms") or 0),
                "llm_calls": int(generation_diagnostics.get("llm_calls") or 0),
                "inserted_count": int(len(inserted_items)),
            }
            round_diagnostics.append(round_info)
            logging.info(
                "translation fill round: user_id=%s session_id=%s round=%s before_count=%s candidate_raw=%s normalized=%s "
                "dropped_norm=%s duplicate_existing=%s insertable=%s overflow=%s inserted=%s target=%s requested=%s "
                "personal_ms=%s pool_ms=%s llm_ms=%s llm_calls=%s",
                int(user_id),
                int(session_id),
                round_info["round"],
                round_info["before_count"],
                round_info["candidate_raw_count"],
                round_info["candidate_normalized_count"],
                round_info["candidate_dropped_during_normalization"],
                round_info["candidate_duplicate_existing"],
                round_info["candidate_insertable"],
                round_info["candidate_overflow"],
                round_info["inserted_count"],
                int(target_count),
                round_info["requested_count"],
                round_info["personal_ms"],
                round_info["pool_ms"],
                round_info["llm_ms"],
                round_info["llm_calls"],
            )
            if not inserted_items:
                logging.warning(
                    "translation fill stopped without inserts: user_id=%s session_id=%s round=%s before_count=%s target=%s "
                    "candidate_raw=%s normalized=%s duplicate_existing=%s insertable=%s",
                    int(user_id),
                    int(session_id),
                    round_info["round"],
                    round_info["before_count"],
                    int(target_count),
                    round_info["candidate_raw_count"],
                    round_info["candidate_normalized_count"],
                    round_info["candidate_duplicate_existing"],
                    round_info["candidate_insertable"],
                )
                continue
            total_inserted += len(inserted_items)
            conn.commit()

        cursor.execute(
            """
            SELECT COUNT(*)
            FROM bt_3_daily_sentences
            WHERE user_id = %s
              AND session_id = %s
              AND COALESCE(source_lang, 'ru') = %s
              AND COALESCE(target_lang, 'de') = %s;
            """,
            (int(user_id), int(session_id), source_lang, target_lang),
        )
        count_row = cursor.fetchone()
        current_count = int(count_row[0] or 0) if count_row else 0
        if current_count < int(target_count):
            logging.warning(
                "translation session underfilled after background fill: user_id=%s session_id=%s ready_count=%s expected_total=%s diagnostics=%s",
                int(user_id),
                int(session_id),
                int(current_count),
                int(target_count),
                round_diagnostics,
            )
        return {
            "session_id": int(session_id),
            "filled": int(total_inserted),
            "ready_count": int(current_count),
            "expected_total": int(target_count),
            "round_diagnostics": round_diagnostics,
        }
    finally:
        if advisory_lock_acquired:
            try:
                cursor.execute("SELECT pg_advisory_unlock(%s);", (int(session_id),))
            except Exception:
                logging.debug("Failed to release translation session fill advisory lock: %s", session_id, exc_info=True)
        cursor.close()
        conn.close()


async def start_translation_session_webapp(
    user_id: int,
    username: str | None = None,
    topic: str = "Random sentences",
    level: str | None = None,
    source_lang: str = "ru",
    target_lang: str = "de",
    force_new_session: bool = False,
    tested_skill_profile_seed: dict[str, Any] | None = None,
    grammar_focus: dict[str, Any] | None = None,
) -> dict[str, Any]:
    close_stale_open_translation_sessions_for_user(
        user_id=int(user_id),
        source_lang=source_lang,
        target_lang=target_lang,
    )
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        active_session_id = _get_active_session_id(
            cursor,
            user_id,
            source_lang=source_lang,
            target_lang=target_lang,
        )
        if not active_session_id:
            # Recover sessions that were accidentally marked as completed but
            # still have pending sentences.
            recovered_session_id = _get_latest_pending_session_id(
                cursor,
                user_id,
                source_lang=source_lang,
                target_lang=target_lang,
            )
            if recovered_session_id:
                cursor.execute(
                    """
                    UPDATE bt_3_user_progress
                    SET
                        completed = FALSE,
                        end_time = NULL,
                        active_started_at = NULL,
                        active_running = FALSE
                    WHERE user_id = %s AND session_id = %s;
                    """,
                    (user_id, recovered_session_id),
                )
                conn.commit()
                active_session_id = recovered_session_id
        if active_session_id:
            session_state = _get_translation_session_state_with_cursor(
                cursor,
                user_id=int(user_id),
                session_id=active_session_id,
                source_lang=source_lang,
                target_lang=target_lang,
            )
            has_stored_sentences = session_state["stored_count"] > 0
            if has_stored_sentences:
                completion_required = (
                    session_state["pending_count"] == 0
                    and session_state["translated_count"] > 0
                )
                return {
                    "session_id": active_session_id,
                    "created": False,
                    "blocked": True,
                    "completion_required": completion_required,
                    "pending_count": int(session_state["pending_count"]),
                    "translated_count": int(session_state["translated_count"]),
                    "shown_count": int(session_state["shown_count"]),
                }
            if force_new_session:
                logging.info(
                    "Ignoring force_new_session for empty active translation session: user_id=%s session_id=%s",
                    int(user_id),
                    active_session_id,
                )
            # Auto-close stale empty session and allow creating a fresh one.
            if not has_stored_sentences:
                _close_user_progress_session(
                    cursor,
                    user_id=int(user_id),
                    session_id=active_session_id,
                )
                conn.commit()

        translation_limit_error = enforce_feature_limit(
            user_id=int(user_id),
            feature_code="translation_daily_sets",
            requested_units=1.0,
            tz="Europe/Vienna",
        )
        if translation_limit_error:
            return translation_limit_error

        cursor.execute(
            """
            UPDATE bt_3_user_progress
            SET
                active_seconds = COALESCE(active_seconds, 0)
                    + CASE
                        WHEN COALESCE(active_running, FALSE) = TRUE
                         AND active_started_at IS NOT NULL
                            THEN GREATEST(
                                0,
                                EXTRACT(EPOCH FROM (NOW() - active_started_at))::BIGINT
                            )
                        ELSE 0
                    END,
                active_started_at = NULL,
                active_running = FALSE,
                end_time = NOW(),
                completed = TRUE
            WHERE user_id = %s AND start_time::date < CURRENT_DATE AND completed = FALSE;
            """,
            (user_id,),
        )

        session_id = int(hashlib.md5(f"{user_id}{datetime.now()}".encode()).hexdigest(), 16) % (10**12)
        cursor.execute(
            """
            INSERT INTO bt_3_user_progress (
                session_id,
                user_id,
                username,
                start_time,
                active_seconds,
                active_running,
                completed
            )
            VALUES (%s, %s, %s, NOW(), 0, FALSE, FALSE);
            """,
            (session_id, user_id, username),
        )

        immediate_entries: list[dict[str, Any]] = []
        seed_started_at = time.perf_counter()
        seed_diagnostics: dict[str, Any] = {}
        if _is_legacy_ru_de_pair(source_lang, target_lang):
            level_key = _normalize_level(level)
            resolved_focus = grammar_focus if isinstance(grammar_focus, dict) else resolve_webapp_focus(topic)
            recent_sentence_keys = _get_recently_served_sentence_keys_with_cursor(
                cursor,
                user_id=int(user_id),
                source_lang=source_lang,
                target_lang=target_lang,
            )
            immediate_entries = _collect_seed_sentence_entries_with_cursor(
                cursor,
                user_id=int(user_id),
                level_key=level_key,
                source_lang=source_lang,
                target_lang=target_lang,
                resolved_focus=resolved_focus,
                recent_sentence_keys=recent_sentence_keys,
                target_count=7,
                diagnostics=seed_diagnostics,
            )
            immediate_entries, topup_diagnostics = await _top_up_immediate_sentence_entries_with_cursor(
                cursor,
                topic=str(resolved_focus.get("prompt_topic") or topic or "Random sentences").strip(),
                level=level,
                source_lang=source_lang,
                target_lang=target_lang,
                resolved_focus=resolved_focus,
                target_min_ready=2,
                existing_entries=immediate_entries,
                recent_sentence_keys=recent_sentence_keys,
            )
            seed_diagnostics.update(topup_diagnostics)

        inserted_items = _insert_sentence_entries_into_session_with_cursor(
            cursor,
            user_id=int(user_id),
            session_id=int(session_id),
            source_lang=source_lang,
            target_lang=target_lang,
            sentence_entries=immediate_entries,
            limit=7,
        )
        conn.commit()
        ready_count = len(inserted_items)
        immediate_batch_summary = _summarize_sentence_entry_batch(
            immediate_entries,
            existing_sentence_keys=set(),
            limit=7,
        )
        logging.info(
            "translation session seeded: user_id=%s session_id=%s seed_raw=%s seed_normalized=%s seed_dropped_norm=%s "
            "seed_insertable=%s inserted=%s expected_total=%s seed_ms=%s personal_ms=%s pool_ms=%s "
            "personal_rows_scanned=%s personal_added=%s pool_added=%s quick_topup_ms=%s quick_topup_requested=%s "
            "quick_topup_generated=%s quick_topup_added=%s",
            int(user_id),
            int(session_id),
            int(immediate_batch_summary["raw_count"]),
            int(immediate_batch_summary["normalized_count"]),
            int(immediate_batch_summary["dropped_during_normalization"]),
            int(immediate_batch_summary["insertable"]),
            int(ready_count),
            7,
            int((time.perf_counter() - seed_started_at) * 1000),
            int(seed_diagnostics.get("personal_ms") or 0),
            int(seed_diagnostics.get("pool_ms") or 0),
            int(seed_diagnostics.get("personal_rows_scanned") or 0),
            int(seed_diagnostics.get("personal_added") or 0),
            int(seed_diagnostics.get("pool_added") or 0),
            int(seed_diagnostics.get("quick_topup_ms") or 0),
            int(seed_diagnostics.get("quick_topup_requested") or 0),
            int(seed_diagnostics.get("quick_topup_generated") or 0),
            int(seed_diagnostics.get("quick_topup_added") or 0),
        )
        if ready_count < 7:
            logging.warning(
                "translation session requires background fill: user_id=%s session_id=%s seeded_ready_count=%s expected_total=%s",
                int(user_id),
                int(session_id),
                int(ready_count),
                7,
            )
        return {
            "session_id": int(session_id),
            "created": True,
            "count": int(ready_count),
            "ready_count": int(ready_count),
            "expected_total": 7,
            "remaining_count": max(0, 7 - int(ready_count)),
        }
    finally:
        cursor.close()
        conn.close()


def _parse_translation_feedback_payload(
    feedback: str,
    *,
    language_categories: list[str],
    language_subcategories: dict[str, list[str]],
) -> dict[str, Any]:
    score_str = (
        feedback.split("Score:")[-1].split("/")[0].strip()
        if "Score:" in feedback
        else None
    )
    score = int(score_str) if score_str and score_str.isdigit() else None
    correct_translation = None
    categories, subcategories = _extract_categories_from_feedback(feedback)
    valid_categories: list[str] = []
    valid_subcategories: list[str] = []

    translation_match = re.search(r"Correct Translation:\s*(.+?)(?:\n|\Z)", feedback, flags=re.IGNORECASE)
    if translation_match:
        correct_translation = translation_match.group(1).strip()

    for cat in categories:
        cat_lower = str(cat or "").lower().strip()
        canonical_cat = next((x for x in language_categories if x.lower() == cat_lower), None)
        if canonical_cat:
            valid_categories.append(canonical_cat)
    for sub in subcategories:
        sub_lower = str(sub or "").lower().strip()
        for cat, values in language_subcategories.items():
            if sub_lower in [value.lower() for value in values]:
                canonical_sub = next((value for value in values if value.lower() == sub_lower), None)
                if canonical_sub:
                    valid_subcategories.append(canonical_sub)
                    break

    return {
        "score_str": score_str,
        "score": score,
        "correct_translation": correct_translation,
        "categories": list(set(valid_categories)),
        "subcategories": list(set(valid_subcategories)),
    }


def _build_translation_feedback_with_sentence_prefix(
    *,
    original_text: str,
    user_translation: str,
    sentence_number: int | None,
    score: int | None,
    correct_translation: str | None,
    feedback: str,
) -> str:
    sentence_label = sentence_number if sentence_number is not None else "—"
    if score is not None and "Sentence number" not in feedback and "Mistake Categories" not in feedback:
        return (
            f"🟢 *Sentence number:* {sentence_label}\n"
            f"✅ *Score:* {score}/100\n"
            f"🔵 *Original Sentence:* {original_text}\n"
            f"🟡 *User Translation:* {user_translation}\n"
            f"🟣 *Correct Translation:* {correct_translation or '—'}\n"
            f"{feedback}"
        )
    return feedback


async def _run_legacy_ru_de_check_translation(
    *,
    original_text: str,
    user_translation: str,
    sentence_number: int | None,
    language_categories: list[str],
    language_subcategories: dict[str, list[str]],
) -> tuple[str, list[str], list[str], int | None, str | None]:
    task_name = "check_translation"
    system_instruction_key = "check_translation"

    score = None
    categories: list[str] = []
    subcategories: list[str] = []
    correct_translation = None

    taxonomy_lines = []
    if language_categories:
        taxonomy_lines.append(
            "allowed_categories: " + ", ".join([str(item).strip() for item in language_categories if str(item).strip()])
        )
    if language_subcategories:
        compact = []
        for cat, values in language_subcategories.items():
            normalized_values = [str(value).strip() for value in (values or []) if str(value).strip()]
            if normalized_values:
                compact.append(f"{str(cat).strip()}: {', '.join(normalized_values)}")
        if compact:
            taxonomy_lines.append("allowed_subcategories:")
            taxonomy_lines.extend([f"- {row}" for row in compact])
    taxonomy_hint = ("\n" + "\n".join(taxonomy_lines)) if taxonomy_lines else ""

    user_message = f"""

    **Original sentence (Russian):** "{original_text}"
    **User's translation (German):** "{user_translation}"{taxonomy_hint}

    """

    for attempt in range(3):
        try:
            logging.info("GPT started working on sentence %s", original_text)
            collected_text = await llm_execute(
                task_name=task_name,
                system_instruction_key=system_instruction_key,
                user_message=user_message,
                poll_interval_seconds=2.0,
            )

            logging.info("GPT response for sentence %s: %s", original_text, collected_text)

            score_str = (
                collected_text.split("Score: ")[-1].split("/")[0].strip()
                if "Score:" in collected_text
                else None
            )
            categories, subcategories = _extract_categories_from_feedback(collected_text)

            match = re.search(r"Correct Translation:\s*(.+?)(?:\n|\Z)", collected_text)
            if match:
                correct_translation = match.group(1).strip()

            categories = [
                re.sub(r"[^0-9a-zA-Z\u00C0-\u024F\s,+\-–&/()¿¡]", "", cat).strip()
                for cat in categories
                if cat.strip()
            ]
            subcategories = [
                re.sub(r"[^0-9a-zA-Z\u00C0-\u024F\s,+\-–&/()¿¡]", "", subcat).strip()
                for subcat in subcategories
                if subcat.strip()
            ]
            categories = [cat.strip() for cat in categories if cat.strip()]
            subcategories = [subcat.strip() for subcat in subcategories if subcat.strip()]

            if score_str and correct_translation:
                if score_str.isdigit():
                    score = int(score_str)
                    if score == 0:
                        score = await recheck_score_only(original_text, user_translation)
                else:
                    score = await recheck_score_only(original_text, user_translation)
                score_display = score if score is not None else 0
                sentence_label = sentence_number if sentence_number is not None else "—"

                result_text = (
                    f"🟢 *Sentence number:* {sentence_label}\n"
                    f"✅ *Score:* {score_display}/100\n"
                    f"🔵 *Original Sentence:* {original_text}\n"
                    f"🟡 *User Translation:* {user_translation}\n"
                    f"🟣 *Correct Translation:* {correct_translation}\n"
                )

                return result_text, categories, subcategories, score, correct_translation

        except Exception as exc:
            logging.error(
                "Ошибка при проверке перевода (attempt %s, sentence %s): %s",
                attempt + 1,
                sentence_number,
                exc,
                exc_info=True,
            )
            await asyncio.sleep(1)

    if score is None:
        score = await recheck_score_only(original_text, user_translation)
    if correct_translation is None:
        correct_translation = "—"

    sentence_label = sentence_number if sentence_number is not None else "—"
    result_text = (
        f"🟢 *Sentence number:* {sentence_label}\n"
        f"✅ *Score:* {score}/100\n"
        f"🔵 *Original Sentence:* {original_text}\n"
        f"🟡 *User Translation:* {user_translation}\n"
        f"🟣 *Correct Translation:* {correct_translation}\n"
    )

    return result_text, categories, subcategories, score, correct_translation


async def check_translation(
    original_text: str,
    user_translation: str,
    sentence_number: int | None = None,
    source_lang: str = "ru",
    target_lang: str = "de",
) -> tuple[str, list[str], list[str], int | None, str | None]:
    language_categories, language_subcategories, _ = _get_language_taxonomy(target_lang)
    is_legacy_ru_de = _is_legacy_ru_de_pair(source_lang, target_lang)

    if is_legacy_ru_de:
        try:
            feedback = await run_check_translation_multilang(
                original_text=original_text,
                user_translation=user_translation,
                source_lang=source_lang,
                target_lang=target_lang,
                allowed_categories=language_categories,
                allowed_subcategories=language_subcategories,
            )
            parsed = _parse_translation_feedback_payload(
                feedback,
                language_categories=language_categories,
                language_subcategories=language_subcategories,
            )
            if not parsed["score_str"]:
                logging.warning("Primary multilang ru->de translation check missing score; falling back to legacy path")
                return await _run_legacy_ru_de_check_translation(
                    original_text=original_text,
                    user_translation=user_translation,
                    sentence_number=sentence_number,
                    language_categories=language_categories,
                    language_subcategories=language_subcategories,
                )
            if not str(parsed["score_str"]).isdigit():
                logging.warning("Primary multilang ru->de translation check returned non-numeric score; falling back to legacy path")
                return await _run_legacy_ru_de_check_translation(
                    original_text=original_text,
                    user_translation=user_translation,
                    sentence_number=sentence_number,
                    language_categories=language_categories,
                    language_subcategories=language_subcategories,
                )
            if not parsed["correct_translation"]:
                logging.warning("Primary multilang ru->de translation check missing correct translation; falling back to legacy path")
                return await _run_legacy_ru_de_check_translation(
                    original_text=original_text,
                    user_translation=user_translation,
                    sentence_number=sentence_number,
                    language_categories=language_categories,
                    language_subcategories=language_subcategories,
                )

            score = int(parsed["score"])
            if score == 0:
                score = await recheck_score_only(original_text, user_translation)
            sentence_label = sentence_number if sentence_number is not None else "—"
            result_text = (
                f"🟢 *Sentence number:* {sentence_label}\n"
                f"✅ *Score:* {score}/100\n"
                f"🔵 *Original Sentence:* {original_text}\n"
                f"🟡 *User Translation:* {user_translation}\n"
                f"🟣 *Correct Translation:* {parsed['correct_translation']}\n"
            )
            return (
                result_text,
                list(parsed["categories"]),
                list(parsed["subcategories"]),
                score,
                parsed["correct_translation"],
            )
        except Exception as exc:
            logging.warning("Primary multilang ru->de translation check failed; falling back to legacy path: %s", exc)
            return await _run_legacy_ru_de_check_translation(
                original_text=original_text,
                user_translation=user_translation,
                sentence_number=sentence_number,
                language_categories=language_categories,
                language_subcategories=language_subcategories,
            )

    feedback = await run_check_translation_multilang(
        original_text=original_text,
        user_translation=user_translation,
        source_lang=source_lang,
        target_lang=target_lang,
        allowed_categories=language_categories,
        allowed_subcategories=language_subcategories,
    )
    parsed = _parse_translation_feedback_payload(
        feedback,
        language_categories=language_categories,
        language_subcategories=language_subcategories,
    )
    feedback = _build_translation_feedback_with_sentence_prefix(
        original_text=original_text,
        user_translation=user_translation,
        sentence_number=sentence_number,
        score=parsed["score"],
        correct_translation=parsed["correct_translation"],
        feedback=feedback,
    )
    return (
        feedback,
        list(parsed["categories"]),
        list(parsed["subcategories"]),
        parsed["score"],
        parsed["correct_translation"],
    )


async def recheck_score_only(original_text: str, user_translation: str) -> int:
    task_name = "recheck_translation"
    system_instruction_key = "recheck_translation"

    user_message = (
        f'Original sentence (Russian): "{original_text}"\n'
        f'User\'s translation (German): "{user_translation}"'
    )

    for attempt in range(3):
        try:
            collected_text = await llm_execute(
                task_name=task_name,
                system_instruction_key=system_instruction_key,
                user_message=user_message,
                poll_interval_seconds=1.0,
            )

            match = re.search(r"Score:\s*(\d+)", collected_text, flags=re.IGNORECASE)
            if match:
                return int(match.group(1))

        except Exception as exc:
            logging.warning("Recheck failed (attempt %s): %s", attempt + 1, exc)

    return 0


def _extract_correct_translation(feedback: str | None, sentence_number: int | None = None) -> str | None:
    if not feedback:
        return None
    text = str(feedback or "")
    if sentence_number is not None:
        try:
            sentence_idx = int(sentence_number)
        except Exception:
            sentence_idx = None
        if sentence_idx:
            story_patterns = [
                rf"(?ms)^{sentence_idx}\)\s*Верный вариант\s*\(DE\):\s*(.+?)\s*$",
                rf"(?ms)^Satz\s*{sentence_idx}\s*[:\-].*?^3\)\s*Верный вариант\s*\(DE\):\s*(.+?)\s*$",
                rf"(?ms)^Sentence\s*{sentence_idx}\s*[:\-].*?^3\)\s*Correct Translation\s*:\s*(.+?)\s*$",
            ]
            for pattern in story_patterns:
                match = re.search(pattern, text, flags=re.IGNORECASE)
                if match:
                    candidate = str(match.group(1) or "").strip()
                    if candidate:
                        return candidate

    generic_patterns = [
        r"Correct Translation:\*?\s*(.+?)(?:\n|\Z)",
        r"Korrigierte Version:\*?\s*(.+?)(?:\n|\Z)",
        r"Исправленный вариант:\*?\s*(.+?)(?:\n|\Z)",
        r"Верный вариант\s*\(DE\):\s*(.+?)(?:\n|\Z)",
    ]
    for pattern in generic_patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            candidate = str(match.group(1) or "").strip()
            if candidate:
                return candidate
    return None


def get_daily_translation_history(
    user_id: int,
    limit: int = 50,
    source_lang: str = "ru",
    target_lang: str = "de",
) -> list[dict[str, Any]]:
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                WITH ranked AS (
                    SELECT
                        t.id,
                        t.score,
                        t.user_translation,
                        t.feedback,
                        t.timestamp,
                        ds.sentence,
                        ds.unique_id,
                        t.source_lang,
                        t.target_lang,
                        ROW_NUMBER() OVER (
                            PARTITION BY t.sentence_id
                            ORDER BY t.timestamp DESC, t.id DESC
                        ) AS rn
                    FROM bt_3_translations t
                    JOIN bt_3_daily_sentences ds
                        ON ds.id = t.sentence_id
                    WHERE t.user_id = %s
                      AND COALESCE(t.source_lang, 'ru') = %s
                      AND COALESCE(t.target_lang, 'de') = %s
                      AND t.timestamp::date = CURRENT_DATE
                )
                SELECT
                    id,
                    score,
                    user_translation,
                    feedback,
                    timestamp,
                    sentence,
                    unique_id,
                    source_lang,
                    target_lang
                FROM ranked
                WHERE rn = 1
                ORDER BY unique_id ASC, timestamp DESC
                LIMIT %s;
                """,
                (user_id, source_lang, target_lang, limit),
            )
            rows = cursor.fetchall()

    items: list[dict[str, Any]] = []
    for row in rows:
        feedback = row[3]
        items.append(
            {
                "id": row[0],
                "score": row[1],
                "user_translation": row[2],
                "feedback": feedback,
                "created_at": row[4].isoformat() if row[4] else None,
                "original_text": row[5],
                "sentence_number": row[6],
                "correct_translation": _extract_correct_translation(feedback, row[6]),
                "source_lang": row[7] or source_lang,
                "target_lang": row[8] or target_lang,
            }
        )
    return items


def _normalize_translation_session_id(session_id: str | int | None) -> str | int | None:
    normalized = session_id
    if isinstance(normalized, str):
        stripped_session_id = normalized.strip()
        if not stripped_session_id:
            return None
        if stripped_session_id.isdigit():
            return int(stripped_session_id)
        return stripped_session_id
    return normalized


def _resolve_latest_translation_session_id(
    cursor,
    *,
    user_id: int,
    source_lang: str,
    target_lang: str,
) -> str | int | None:
    cursor.execute(
        """
        SELECT session_id
        FROM bt_3_daily_sentences
        WHERE user_id = %s
          AND COALESCE(source_lang, 'ru') = %s
          AND COALESCE(target_lang, 'de') = %s
        ORDER BY id DESC
        LIMIT 1;
        """,
        (user_id, source_lang, target_lang),
    )
    latest_session = cursor.fetchone()
    return latest_session[0] if latest_session else None


def _score_band_label(score_value: int) -> str:
    score = max(0, min(100, int(score_value or 0)))
    if score >= 95:
        return "95_100"
    if score >= 85:
        return "85_94"
    if score >= 70:
        return "70_84"
    if score >= 50:
        return "50_69"
    return "lt50"


def _retention_state_label(*, was_in_mistakes: bool, score_value: int) -> str:
    score = int(score_value or 0)
    if was_in_mistakes:
        return "repeat_success_remove" if score >= 85 else "repeat_fail_keep"
    return "new_pass" if score >= 80 else "new_fail_store"


def _load_skill_seeds_map_with_cursor(
    cursor,
    *,
    skill_ids: list[str],
    target_lang: str,
    cache: dict[tuple[str, str], dict[str, str] | None] | None = None,
) -> dict[str, dict[str, str]]:
    normalized_target_lang = str(target_lang or "de").strip().lower() or "de"
    normalized_skill_ids = [
        str(skill_id or "").strip()
        for skill_id in list(skill_ids or [])
        if str(skill_id or "").strip()
    ]
    if not normalized_skill_ids:
        return {}
    result: dict[str, dict[str, str]] = {}
    missing_skill_ids: list[str] = []
    if cache is not None:
        for skill_id in normalized_skill_ids:
            cached = cache.get((normalized_target_lang, skill_id))
            if cached is None:
                missing_skill_ids.append(skill_id)
            elif cached:
                result[skill_id] = dict(cached)
    else:
        missing_skill_ids = list(normalized_skill_ids)
    if missing_skill_ids:
        cursor.execute(
            """
            SELECT skill_id, title, category
            FROM bt_3_skills
            WHERE skill_id = ANY(%s)
              AND language_code = %s
              AND COALESCE(is_active, TRUE) = TRUE;
            """,
            (missing_skill_ids, normalized_target_lang),
        )
        loaded = {
            str(row[0] or "").strip(): {
                "skill_id": str(row[0] or ""),
                "title": str(row[1] or ""),
                "category": str(row[2] or ""),
            }
            for row in (cursor.fetchall() or [])
            if str(row[0] or "").strip()
        }
        if cache is not None:
            for skill_id in missing_skill_ids:
                cache[(normalized_target_lang, skill_id)] = dict(loaded.get(skill_id) or {})
        result.update(loaded)
    return result


def _load_skill_seed_with_cursor(
    cursor,
    *,
    skill_id: str,
    target_lang: str,
    cache: dict[tuple[str, str], dict[str, str] | None] | None = None,
) -> dict[str, str] | None:
    normalized_skill_id = str(skill_id or "").strip()
    normalized_target_lang = str(target_lang or "de").strip().lower() or "de"
    if not normalized_skill_id:
        return None
    loaded = _load_skill_seeds_map_with_cursor(
        cursor,
        skill_ids=[normalized_skill_id],
        target_lang=normalized_target_lang,
        cache=cache,
    )
    return loaded.get(normalized_skill_id)


def _list_related_skill_ids_with_cursor(
    cursor,
    *,
    target_lang: str,
    category: str,
    exclude_skill_ids: set[str],
    limit: int,
) -> list[str]:
    if not category or limit <= 0:
        return []
    cursor.execute(
        """
        SELECT skill_id
        FROM bt_3_skills
        WHERE category = %s
          AND language_code = %s
          AND COALESCE(is_active, TRUE) = TRUE
        ORDER BY skill_id ASC
        LIMIT %s;
        """,
        (category, str(target_lang or "de").strip().lower() or "de", max(limit + len(exclude_skill_ids), limit)),
    )
    rows = cursor.fetchall() or []
    result: list[str] = []
    for row in rows:
        skill_id = str((row or [None])[0] or "").strip()
        if not skill_id or skill_id in exclude_skill_ids:
            continue
        result.append(skill_id)
        if len(result) >= limit:
            break
    return result


def _list_focus_mapped_skill_ids(
    *,
    main_category: str | None,
    sub_category: str | None,
    target_lang: str,
    exclude_skill_ids: set[str],
    limit: int,
) -> list[str]:
    if not main_category or not sub_category or limit <= 0:
        return []
    mapped = get_skill_mapping_for_error(
        str(main_category or "").strip(),
        str(sub_category or "").strip(),
        language_code=target_lang or "de",
    )
    result: list[str] = []
    for item in mapped:
        skill_id = str(item.get("skill_id") or "").strip()
        if not skill_id or skill_id in exclude_skill_ids:
            continue
        result.append(skill_id)
        if len(result) >= limit:
            break
    return result


def _build_phase1_tested_skill_profile_with_cursor(
    cursor,
    *,
    target_lang: str,
    profile_seed: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    if not isinstance(profile_seed, dict):
        return []
    primary_skill_id = str(profile_seed.get("primary_skill_id") or profile_seed.get("skill_id") or "").strip()
    if not primary_skill_id:
        return []
    primary_skill = _load_skill_seed_with_cursor(
        cursor,
        skill_id=primary_skill_id,
        target_lang=target_lang,
    )
    if not primary_skill:
        return []

    profile_source = str(profile_seed.get("profile_source") or "seeded").strip() or "seeded"
    try:
        profile_confidence = max(0.0, min(1.0, float(profile_seed.get("profile_confidence") or 0.0)))
    except Exception:
        profile_confidence = 0.0
    if profile_confidence <= 0.0:
        return []

    selected_skill_ids = {primary_skill["skill_id"]}
    secondaries = _list_focus_mapped_skill_ids(
        main_category=profile_seed.get("main_category"),
        sub_category=profile_seed.get("sub_category"),
        target_lang=target_lang,
        exclude_skill_ids=selected_skill_ids,
        limit=2,
    )
    selected_skill_ids.update(secondaries)
    if len(secondaries) < 1:
        filler = _list_related_skill_ids_with_cursor(
            cursor,
            target_lang=target_lang,
            category=primary_skill.get("category") or "",
            exclude_skill_ids=selected_skill_ids,
            limit=2 - len(secondaries),
        )
        secondaries.extend(filler)
        selected_skill_ids.update(filler)

    supporting = _list_related_skill_ids_with_cursor(
        cursor,
        target_lang=target_lang,
        category=primary_skill.get("category") or "",
        exclude_skill_ids=selected_skill_ids,
        limit=1,
    )

    profile: list[dict[str, Any]] = [
        {
            "skill_id": primary_skill["skill_id"],
            "role": "primary",
            "role_rank": 1,
            "role_weight": PHASE1_SKILL_ROLE_WEIGHTS["primary"],
            "profile_source": profile_source,
            "profile_confidence": profile_confidence,
            "profile_version": 1,
        }
    ]
    for index, skill_id in enumerate(secondaries[:2], start=2):
        profile.append(
            {
                "skill_id": skill_id,
                "role": "secondary",
                "role_rank": index,
                "role_weight": PHASE1_SKILL_ROLE_WEIGHTS["secondary"],
                "profile_source": profile_source,
                "profile_confidence": profile_confidence,
                "profile_version": 1,
            }
        )
    if supporting:
        profile.append(
            {
                "skill_id": supporting[0],
                "role": "supporting",
                "role_rank": 4,
                "role_weight": PHASE1_SKILL_ROLE_WEIGHTS["supporting"],
                "profile_source": profile_source,
                "profile_confidence": profile_confidence,
                "profile_version": 1,
            }
        )
    return profile


def _build_remediation_profile_with_cursor(
    cursor,
    *,
    user_id: int,
    sentence_id_for_mistake: int,
    target_lang: str,
    source_lang: str = "ru",
    sentence_text: str | None = None,
    skill_seed_cache: dict[tuple[str, str], dict[str, str] | None] | None = None,
    membership_cache: dict[tuple[str, str], dict[str, Any] | None] | None = None,
    leaf_cache: dict[tuple[str, str], list[str]] | None = None,
) -> list[dict[str, Any]]:
    if not user_id or not sentence_id_for_mistake:
        return []
    cursor.execute(
        """
        SELECT
            COALESCE(NULLIF(main_category, ''), 'Other mistake') AS main_category,
            COALESCE(NULLIF(sub_category, ''), 'Unclassified mistake') AS sub_category,
            SUM(COALESCE(mistake_count, 1))::BIGINT AS total_mistakes
        FROM bt_3_detailed_mistakes
        WHERE user_id = %s
          AND sentence_id = %s
        GROUP BY 1, 2
        ORDER BY total_mistakes DESC, main_category ASC, sub_category ASC
        LIMIT 12;
        """,
        (int(user_id), int(sentence_id_for_mistake)),
    )
    rows = cursor.fetchall() or []
    if not rows:
        return []

    aggregated_skill_weights: dict[str, float] = {}
    mapped_pairs: list[tuple[list[dict[str, Any]], float]] = []
    candidate_skill_ids: set[str] = set()
    for main_category, sub_category, total_mistakes in rows:
        mapped = get_skill_mapping_for_error(
            str(main_category or "").strip(),
            str(sub_category or "").strip(),
            language_code=target_lang or "de",
        )
        pair_weight = max(1.0, float(total_mistakes or 1))
        mapped_pairs.append((mapped, pair_weight))
        for item in mapped:
            skill_id = str(item.get("skill_id") or "").strip()
            if not skill_id:
                continue
            candidate_skill_ids.add(skill_id)

    available_skill_seeds = _load_skill_seeds_map_with_cursor(
        cursor,
        skill_ids=list(candidate_skill_ids),
        target_lang=target_lang,
        cache=skill_seed_cache,
    )
    for mapped, pair_weight in mapped_pairs:
        for item in mapped:
            skill_id = str(item.get("skill_id") or "").strip()
            if not skill_id or skill_id not in available_skill_seeds:
                continue
            aggregated_skill_weights[skill_id] = aggregated_skill_weights.get(skill_id, 0.0) + pair_weight * max(float(item.get("weight") or 1.0), 0.1)

    resolved_sentence_text = str(sentence_text or "").strip() or _load_sentence_text_for_remediation_with_cursor(
        cursor,
        sentence_id_for_mistake=int(sentence_id_for_mistake),
        source_lang=source_lang or "ru",
        target_lang=target_lang or "de",
    )
    anchor_skill_weights, has_structural_anchor = _collect_sentence_anchor_skill_weights(resolved_sentence_text)
    anchor_skill_seeds = _load_skill_seeds_map_with_cursor(
        cursor,
        skill_ids=list(anchor_skill_weights.keys()),
        target_lang=target_lang,
        cache=skill_seed_cache,
    )
    for skill_id, bonus in anchor_skill_weights.items():
        if skill_id not in anchor_skill_seeds:
            continue
        aggregated_skill_weights[skill_id] = aggregated_skill_weights.get(skill_id, 0.0) + float(bonus)

    if has_structural_anchor:
        for skill_id in REMEDIATION_PRIMARY_DEMOTION_SKILL_IDS:
            if skill_id in aggregated_skill_weights and skill_id not in anchor_skill_weights:
                aggregated_skill_weights[skill_id] = aggregated_skill_weights.get(skill_id, 0.0) * 0.4
    if not aggregated_skill_weights:
        return []

    memberships = _load_skill_mastery_memberships_with_cursor(
        cursor,
        target_lang=target_lang,
        skill_ids=list(aggregated_skill_weights.keys()),
        cache=membership_cache,
    )
    leaf_candidates_by_group = _list_mastery_leaf_skill_ids_for_groups_with_cursor(
        cursor,
        target_lang=target_lang,
        mastery_group_ids=[
            membership.get("mastery_group_id")
            for membership in memberships.values()
            if str(membership.get("mastery_group_id") or "").strip()
        ],
        cache=leaf_cache,
    )

    promoted_leaf_scores: dict[str, float] = {}
    for skill_id, raw_score in aggregated_skill_weights.items():
        membership = memberships.get(skill_id) or {}
        mastery_group_id = str(membership.get("mastery_group_id") or "").strip()
        is_mastery_leaf = bool(membership.get("is_mastery_leaf"))
        is_diagnostic_only = bool(membership.get("is_diagnostic_only"))
        score_value = float(raw_score or 0.0)
        if is_mastery_leaf and not is_diagnostic_only:
            promoted_leaf_scores[skill_id] = promoted_leaf_scores.get(skill_id, 0.0) + score_value
            continue
        if mastery_group_id:
            representative_leaf_ids = leaf_candidates_by_group.get(mastery_group_id) or []
            if representative_leaf_ids:
                representative_leaf_id = representative_leaf_ids[0]
                promoted_leaf_scores[representative_leaf_id] = promoted_leaf_scores.get(representative_leaf_id, 0.0) + (score_value * 0.9)

    for skill_id, bonus in anchor_skill_weights.items():
        if skill_id in promoted_leaf_scores:
            promoted_leaf_scores[skill_id] = promoted_leaf_scores.get(skill_id, 0.0) + (float(bonus) * 0.75)

    ranked_leaf_skill_ids = [
        skill_id
        for skill_id, _score in sorted(
            promoted_leaf_scores.items(),
            key=lambda item: (-float(item[1] or 0.0), item[0]),
        )
    ]
    ranked_diagnostic_fallback_ids = [
        skill_id
        for skill_id, _score in sorted(
            aggregated_skill_weights.items(),
            key=lambda item: (-float(item[1] or 0.0), item[0]),
        )
        if skill_id not in ranked_leaf_skill_ids
    ]
    ranked_skill_ids = ranked_leaf_skill_ids + ranked_diagnostic_fallback_ids
    base_profile = _build_skill_profile_from_skill_ids(
        primary_skill_id=ranked_skill_ids[0],
        secondary_skill_ids=ranked_skill_ids[1:3],
        supporting_skill_ids=ranked_skill_ids[3:4],
        profile_source=REMEDIATION_PROFILE_SOURCE,
        profile_confidence=REMEDIATION_PROFILE_CONFIDENCE,
    )
    return rerank_tested_skill_profile_for_sentence(
        resolved_sentence_text,
        base_profile,
        profile_source=REMEDIATION_PROFILE_SOURCE,
        profile_confidence=REMEDIATION_PROFILE_CONFIDENCE,
    )


def _insert_sentence_skill_targets_with_cursor(
    cursor,
    *,
    sentence_ids: list[int],
    tested_skill_profile: list[dict[str, Any]],
) -> None:
    if not sentence_ids or not tested_skill_profile:
        return
    values: list[tuple[Any, ...]] = []
    for sentence_id in sentence_ids:
        for item in tested_skill_profile:
            values.append(
                (
                    int(sentence_id),
                    str(item.get("skill_id") or ""),
                    str(item.get("role") or "supporting"),
                    int(item.get("role_rank") or 99),
                    float(item.get("role_weight") or 0.0),
                    str(item.get("profile_source") or "seeded"),
                    float(item.get("profile_confidence") or 0.0),
                    int(item.get("profile_version") or 1),
                )
            )
    if not values:
        return
    execute_values(
        cursor,
        """
        INSERT INTO bt_3_sentence_skill_targets (
            sentence_id,
            skill_id,
            role,
            role_rank,
            role_weight,
            profile_source,
            profile_confidence,
            profile_version
        ) VALUES %s
        ON CONFLICT (sentence_id, skill_id) DO NOTHING;
        """,
        values,
    )


def _insert_sentence_skill_targets_for_entries_with_cursor(
    cursor,
    *,
    sentence_profiles: list[tuple[int, list[dict[str, Any]]]],
) -> None:
    if not sentence_profiles:
        return
    values: list[tuple[Any, ...]] = []
    for sentence_id, tested_skill_profile in sentence_profiles:
        for item in list(tested_skill_profile or []):
            values.append(
                (
                    int(sentence_id),
                    str(item.get("skill_id") or ""),
                    str(item.get("role") or "supporting"),
                    int(item.get("role_rank") or 99),
                    float(item.get("role_weight") or 0.0),
                    str(item.get("profile_source") or "seeded"),
                    float(item.get("profile_confidence") or 0.0),
                    int(item.get("profile_version") or 1),
                )
            )
    if not values:
        return
    execute_values(
        cursor,
        """
        INSERT INTO bt_3_sentence_skill_targets (
            sentence_id,
            skill_id,
            role,
            role_rank,
            role_weight,
            profile_source,
            profile_confidence,
            profile_version
        ) VALUES %s
        ON CONFLICT (sentence_id, skill_id) DO NOTHING;
        """,
        values,
    )


def _normalize_category_pairs(
    categories: list[str],
    subcategories: list[str],
    *,
    target_lang: str,
    fallback_to_other: bool = False,
) -> list[tuple[str, str]]:
    language_categories, language_subcategories, language_subcategories_lower = _get_language_taxonomy(target_lang)
    normalized_pairs: list[tuple[str, str]] = []
    for cat in categories:
        cat_lower = str(cat or "").strip().lower()
        if not cat_lower or cat_lower not in language_subcategories_lower:
            continue
        for subcat in subcategories:
            subcat_lower = str(subcat or "").strip().lower()
            if not subcat_lower or subcat_lower not in language_subcategories_lower[cat_lower]:
                continue
            canonical_cat = next(
                (value for value in language_categories if value.lower() == cat_lower),
                str(cat or "").strip() or "Other mistake",
            )
            canonical_sub = next(
                (
                    value
                    for value in language_subcategories.get(canonical_cat, [])
                    if value.lower() == subcat_lower
                ),
                str(subcat or "").strip() or "Unclassified mistake",
            )
            normalized_pairs.append((canonical_cat, canonical_sub))

    if not normalized_pairs and fallback_to_other:
        normalized_pairs.append(("Other mistake", "Unclassified mistake"))
    return list(dict.fromkeys(normalized_pairs))


def _load_sentence_skill_targets_with_cursor(cursor, *, sentence_id: int) -> list[dict[str, Any]]:
    cursor.execute(
        """
        SELECT skill_id, role, role_rank, role_weight, profile_source, profile_confidence, profile_version
        FROM bt_3_sentence_skill_targets
        WHERE sentence_id = %s
        ORDER BY role_rank ASC, skill_id ASC
        LIMIT 4;
        """,
        (int(sentence_id),),
    )
    rows = cursor.fetchall() or []
    return [
        {
            "skill_id": str(row[0] or ""),
            "role": str(row[1] or "supporting"),
            "role_rank": int(row[2] or 99),
            "role_weight": float(row[3] or 0.0),
            "profile_source": str(row[4] or PHASE1_MISSING_PROFILE_SOURCE),
            "profile_confidence": float(row[5] or 0.0),
            "profile_version": int(row[6] or 1),
        }
        for row in rows
        if str(row[0] or "").strip()
    ]


def _load_sentence_skill_shadow_state_with_cursor(cursor, *, sentence_id: int) -> dict[str, Any] | None:
    cursor.execute(
        """
        SELECT
            user_id,
            source_lang,
            target_lang,
            last_attempt_no,
            last_score,
            last_score_band,
            last_retention_state,
            last_tested_skill_ids,
            last_errored_skill_ids,
            last_profile_source,
            last_profile_confidence,
            last_checked_at
        FROM bt_3_sentence_skill_shadow_state_v2
        WHERE sentence_id = %s
        LIMIT 1;
        """,
        (int(sentence_id),),
    )
    row = cursor.fetchone()
    if not row:
        return None
    tested_ids = row[7] if isinstance(row[7], list) else []
    errored_ids = row[8] if isinstance(row[8], list) else []
    return {
        "user_id": int(row[0]),
        "source_lang": str(row[1] or "ru"),
        "target_lang": str(row[2] or "de"),
        "last_attempt_no": int(row[3] or 0),
        "last_score": int(row[4] or 0),
        "last_score_band": str(row[5] or "lt50"),
        "last_retention_state": str(row[6] or "new_fail_store"),
        "last_tested_skill_ids": [str(item) for item in tested_ids if str(item).strip()],
        "last_errored_skill_ids": [str(item) for item in errored_ids if str(item).strip()],
        "last_profile_source": str(row[9] or PHASE1_MISSING_PROFILE_SOURCE),
        "last_profile_confidence": float(row[10] or 0.0),
        "last_checked_at": row[11].isoformat() if row[11] else None,
    }


def _build_errored_skill_details(
    *,
    error_pairs: list[tuple[str, str]],
    target_lang: str,
) -> dict[str, dict[str, Any]]:
    details: dict[str, dict[str, Any]] = {}
    for main_category, sub_category in list(dict.fromkeys(error_pairs))[:8]:
        mapping = get_skill_mapping_for_error(
            str(main_category or "").strip(),
            str(sub_category or "").strip(),
            language_code=target_lang or "de",
        )
        for item in mapping:
            skill_id = str(item.get("skill_id") or "").strip()
            if not skill_id:
                continue
            map_weight = float(item.get("weight") or 1.0)
            bucket = details.setdefault(
                skill_id,
                {
                    "map_weight": map_weight,
                    "error_pairs": [],
                },
            )
            bucket["map_weight"] = max(float(bucket.get("map_weight") or 0.0), map_weight)
            pair_payload = [str(main_category or ""), str(sub_category or "")]
            if pair_payload not in bucket["error_pairs"]:
                bucket["error_pairs"].append(pair_payload)
    return details


def _classify_sentence_progress(
    *,
    previous_errored_skill_ids: set[str],
    current_errored_skill_ids: set[str],
    primary_skill_ids: set[str],
    previous_score: int | None,
    current_score: int,
    was_in_mistakes: bool,
) -> str:
    if previous_score is None and not previous_errored_skill_ids:
        return "first_attempt"
    if was_in_mistakes and current_score >= 85:
        return "final_recovery"
    recovered_skill_ids = previous_errored_skill_ids - current_errored_skill_ids
    new_skill_ids = current_errored_skill_ids - previous_errored_skill_ids
    score_improved = previous_score is not None and int(current_score or 0) > int(previous_score or 0)
    material_score_improved = previous_score is not None and int(current_score or 0) >= int(previous_score or 0) + 5
    primary_skill_recovered = bool(recovered_skill_ids & set(primary_skill_ids or set()))
    if recovered_skill_ids and new_skill_ids:
        if primary_skill_recovered or material_score_improved:
            return "mixed_progress"
        return "no_progress"
    if primary_skill_recovered or (recovered_skill_ids and not new_skill_ids):
        return "partial_progress"
    if len(current_errored_skill_ids) < len(previous_errored_skill_ids) and not new_skill_ids:
        return "partial_progress"
    if score_improved and not new_skill_ids:
        return "partial_progress"
    return "no_progress"


def _shadow_delta_signal(
    *,
    outcome: str,
    role_weight: float,
    profile_confidence: float,
    map_weight: float,
) -> float:
    confidence_factor = max(0.0, min(1.0, float(profile_confidence or 0.0)))
    role = max(0.0, float(role_weight or 0.0))
    mapping = max(0.1, float(map_weight or 1.0))
    multipliers = {
        "fail_new": -1.0 * role * mapping * confidence_factor,
        "fail_repeat_no_progress": -1.2 * role * mapping * confidence_factor,
        "fail_repeat_progress": -0.7 * role * mapping * confidence_factor,
        "catastrophic_fail_fallback": -0.45 * role * max(0.6, confidence_factor),
        "clean_neutral": 0.0,
        "clean_progress_credit": 0.2 * role * confidence_factor,
        "clean_success": 0.6 * role * confidence_factor,
        "recovered_partial": 0.35 * role * confidence_factor,
        "recovered_final": 1.25 * role * confidence_factor,
        "untargeted_error_fail": -0.75 * mapping * max(0.5, confidence_factor),
    }
    return round(float(multipliers.get(outcome, 0.0)), 3)


def _build_phase1_shadow_payload(
    *,
    user_id: int,
    sentence_pk_id: int,
    session_id: int | None,
    source_lang: str,
    target_lang: str,
    score_value: int,
    was_in_mistakes: bool,
    tested_targets: list[dict[str, Any]],
    previous_shadow_state: dict[str, Any] | None,
    current_errored_details: dict[str, dict[str, Any]],
    fallback_previous_errored_details: dict[str, dict[str, Any]] | None = None,
) -> tuple[list[tuple[Any, ...]], dict[str, Any]]:
    tested_ids = [str(item.get("skill_id") or "").strip() for item in tested_targets if str(item.get("skill_id") or "").strip()]
    tested_ids = list(dict.fromkeys(tested_ids))
    primary_skill_ids = {
        str(item.get("skill_id") or "").strip()
        for item in tested_targets
        if str(item.get("role") or "").strip() == "primary" and str(item.get("skill_id") or "").strip()
    }
    current_errored_skill_ids = set(current_errored_details.keys())
    previous_errored_skill_ids = set(previous_shadow_state.get("last_errored_skill_ids") or []) if previous_shadow_state else set()
    previous_score = int(previous_shadow_state.get("last_score") or 0) if previous_shadow_state else None
    previous_attempt_no = int(previous_shadow_state.get("last_attempt_no") or 0) if previous_shadow_state else 0
    historical_recovery_candidate_skill_ids = {
        str(item.get("skill_id") or "").strip()
        for item in tested_targets
        if str(item.get("profile_source") or "").strip() == REMEDIATION_PROFILE_SOURCE
        and str(item.get("role") or "").strip() in {"primary", "secondary"}
        and str(item.get("skill_id") or "").strip()
    }

    if not previous_errored_skill_ids and fallback_previous_errored_details:
        previous_errored_skill_ids = set(fallback_previous_errored_details.keys())
        previous_score = previous_score if previous_score is not None else max(0, int(score_value or 0) - 1)

    attempt_no = previous_attempt_no + 1 if previous_attempt_no > 0 else 1
    score_band = _score_band_label(score_value)
    retention_state = _retention_state_label(
        was_in_mistakes=was_in_mistakes,
        score_value=score_value,
    )
    catastrophic_fail_skill_ids: set[str] = set()
    if tested_ids and not current_errored_skill_ids and int(score_value or 0) <= PHASE1_CATASTROPHIC_FAIL_SCORE_THRESHOLD:
        catastrophic_fail_skill_ids = set(tested_ids[:PHASE1_MAX_UNTARGETED_ERROR_SKILLS])
    effective_current_errored_skill_ids = set(current_errored_skill_ids) | catastrophic_fail_skill_ids
    progress_kind = _classify_sentence_progress(
        previous_errored_skill_ids=previous_errored_skill_ids,
        current_errored_skill_ids=effective_current_errored_skill_ids,
        primary_skill_ids=primary_skill_ids,
        previous_score=previous_score,
        current_score=score_value,
        was_in_mistakes=was_in_mistakes,
    )

    tested_profile_available = bool(tested_targets)
    default_profile_source = (
        str(tested_targets[0].get("profile_source") or PHASE1_MISSING_PROFILE_SOURCE)
        if tested_targets else PHASE1_MISSING_PROFILE_SOURCE
    )
    default_profile_confidence = (
        float(tested_targets[0].get("profile_confidence") or 0.0)
        if tested_targets else PHASE1_MISSING_PROFILE_CONFIDENCE
    )

    event_rows: list[tuple[Any, ...]] = []
    tested_skill_id_set = set(tested_ids)
    for target in tested_targets:
        skill_id = str(target.get("skill_id") or "").strip()
        if not skill_id:
            continue
        role = str(target.get("role") or "supporting")
        role_weight = float(target.get("role_weight") or 0.0)
        profile_source = str(target.get("profile_source") or default_profile_source)
        profile_confidence = float(target.get("profile_confidence") or default_profile_confidence)
        is_errored_now = skill_id in effective_current_errored_skill_ids
        was_errored_prev = skill_id in previous_errored_skill_ids
        is_catastrophic_fallback = skill_id in catastrophic_fail_skill_ids and skill_id not in current_errored_skill_ids
        if is_catastrophic_fallback:
            map_weight = float(PHASE1_CATASTROPHIC_FAIL_MAP_WEIGHT)
        else:
            map_weight = float((current_errored_details.get(skill_id) or {}).get("map_weight") or 1.0)
        error_pairs_json = (current_errored_details.get(skill_id) or {}).get("error_pairs")

        if is_errored_now:
            if is_catastrophic_fallback:
                outcome = "catastrophic_fail_fallback"
            elif was_errored_prev:
                outcome = "fail_repeat_progress" if progress_kind in {"partial_progress", "mixed_progress"} else "fail_repeat_no_progress"
            else:
                outcome = "fail_new"
            recovery_kind = "none"
        else:
            if was_errored_prev or (
                progress_kind == "final_recovery"
                and skill_id in historical_recovery_candidate_skill_ids
            ):
                outcome = "recovered_final" if progress_kind == "final_recovery" else "recovered_partial"
                recovery_kind = "final" if progress_kind == "final_recovery" else "partial"
            else:
                if was_in_mistakes and score_value < 85:
                    outcome = "clean_progress_credit" if attempt_no > 1 and progress_kind in {"partial_progress", "mixed_progress"} else "clean_neutral"
                elif score_value >= 80:
                    outcome = "clean_success"
                else:
                    outcome = "clean_neutral"
                recovery_kind = "none"

        event_rows.append(
            (
                int(user_id),
                int(sentence_pk_id),
                int(session_id) if session_id is not None else None,
                source_lang or "ru",
                target_lang or "de",
                attempt_no,
                int(score_value),
                score_band,
                retention_state,
                bool(was_in_mistakes),
                tested_profile_available,
                skill_id,
                role,
                role_weight,
                True,
                is_errored_now,
                was_errored_prev,
                outcome,
                progress_kind,
                recovery_kind,
                profile_source,
                profile_confidence,
                int(target.get("profile_version") or 1),
                map_weight,
                Json(error_pairs_json) if error_pairs_json else None,
                _shadow_delta_signal(
                    outcome=outcome,
                    role_weight=role_weight,
                    profile_confidence=profile_confidence,
                    map_weight=map_weight,
                ),
                Json(
                    {
                        "current_error_count": len(effective_current_errored_skill_ids),
                        "previous_error_count": len(previous_errored_skill_ids),
                        "previous_score": previous_score,
                        "catastrophic_fail_fallback": bool(is_catastrophic_fallback),
                    }
                ),
            )
        )

    extra_error_skill_ids = [
        skill_id
        for skill_id in sorted(current_errored_skill_ids)
        if skill_id not in tested_skill_id_set
    ][:PHASE1_MAX_UNTARGETED_ERROR_SKILLS]
    for skill_id in extra_error_skill_ids:
        details = current_errored_details.get(skill_id) or {}
        event_rows.append(
            (
                int(user_id),
                int(sentence_pk_id),
                int(session_id) if session_id is not None else None,
                source_lang or "ru",
                target_lang or "de",
                attempt_no,
                int(score_value),
                score_band,
                retention_state,
                bool(was_in_mistakes),
                tested_profile_available,
                skill_id,
                "untargeted_error",
                0.0,
                False,
                True,
                skill_id in previous_errored_skill_ids,
                "untargeted_error_fail",
                progress_kind,
                "none",
                default_profile_source,
                default_profile_confidence,
                1,
                float(details.get("map_weight") or 1.0),
                Json(details.get("error_pairs")) if details.get("error_pairs") else None,
                _shadow_delta_signal(
                    outcome="untargeted_error_fail",
                    role_weight=0.0,
                    profile_confidence=default_profile_confidence,
                    map_weight=float(details.get("map_weight") or 1.0),
                ),
                Json(
                    {
                        "current_error_count": len(effective_current_errored_skill_ids),
                        "previous_error_count": len(previous_errored_skill_ids),
                        "previous_score": previous_score,
                    }
                ),
            )
        )

    shadow_state = {
        "sentence_id": int(sentence_pk_id),
        "user_id": int(user_id),
        "source_lang": source_lang or "ru",
        "target_lang": target_lang or "de",
        "last_attempt_no": attempt_no,
        "last_score": int(score_value),
        "last_score_band": score_band,
        "last_retention_state": retention_state,
        "last_tested_skill_ids": tested_ids,
        "last_errored_skill_ids": sorted(effective_current_errored_skill_ids),
        "last_profile_source": default_profile_source,
        "last_profile_confidence": default_profile_confidence,
    }
    return event_rows, shadow_state


def _write_phase1_shadow_payload_with_cursor(
    cursor,
    *,
    event_rows: list[tuple[Any, ...]],
    shadow_state: dict[str, Any],
) -> list[tuple[int, int, str, str, str]]:
    inserted_event_refs: list[tuple[int, int, str, str, str]] = []
    if event_rows:
        execute_values(
            cursor,
            """
            INSERT INTO bt_3_skill_events_v2 (
                user_id,
                sentence_id,
                session_id,
                source_lang,
                target_lang,
                attempt_no,
                overall_score,
                score_band,
                retention_state,
                was_in_error_bank,
                tested_profile_available,
                skill_id,
                skill_role,
                role_weight,
                is_tested,
                is_errored_now,
                was_errored_prev,
                per_skill_outcome,
                sentence_progress_kind,
                recovery_kind,
                profile_source,
                profile_confidence,
                profile_version,
                map_weight,
                error_pairs_json,
                shadow_delta_signal,
                metadata_json
            ) VALUES %s
            RETURNING id, user_id, skill_id, source_lang, target_lang;
            """,
            event_rows,
            page_size=max(1, len(event_rows)),
        )
        inserted_event_refs = [
            (
                int(row[0]),
                int(row[1]),
                str(row[2]),
                str(row[3] or "ru"),
                str(row[4] or "de"),
            )
            for row in (cursor.fetchall() or [])
        ]

        dirty_rows_by_key: dict[tuple[int, str, str, str], int] = {}
        for event_id, event_user_id, event_skill_id, event_source_lang, event_target_lang in inserted_event_refs:
            key = (
                int(event_user_id),
                str(event_skill_id),
                str(event_source_lang or "ru"),
                str(event_target_lang or "de"),
            )
            previous_max_event_id = dirty_rows_by_key.get(key)
            if previous_max_event_id is None or int(event_id) > previous_max_event_id:
                dirty_rows_by_key[key] = int(event_id)
        if dirty_rows_by_key:
            execute_values(
                cursor,
                """
                INSERT INTO bt_3_skill_state_v2_dirty (
                    user_id,
                    skill_id,
                    source_lang,
                    target_lang,
                    max_event_id,
                    enqueue_count
                ) VALUES %s
                ON CONFLICT (user_id, skill_id, source_lang, target_lang) DO UPDATE
                SET
                    max_event_id = GREATEST(bt_3_skill_state_v2_dirty.max_event_id, EXCLUDED.max_event_id),
                    enqueue_count = bt_3_skill_state_v2_dirty.enqueue_count + EXCLUDED.enqueue_count,
                    next_attempt_at = LEAST(bt_3_skill_state_v2_dirty.next_attempt_at, NOW()),
                    updated_at = NOW(),
                    last_error = NULL;
                """,
                [
                    (
                        int(event_user_id),
                        str(event_skill_id),
                        str(event_source_lang or "ru"),
                        str(event_target_lang or "de"),
                        int(max_event_id),
                        1,
                    )
                    for (event_user_id, event_skill_id, event_source_lang, event_target_lang), max_event_id in dirty_rows_by_key.items()
                ],
                page_size=max(1, len(dirty_rows_by_key)),
            )

    cursor.execute(
        """
        INSERT INTO bt_3_sentence_skill_shadow_state_v2 (
            sentence_id,
            user_id,
            source_lang,
            target_lang,
            last_attempt_no,
            last_score,
            last_score_band,
            last_retention_state,
            last_tested_skill_ids,
            last_errored_skill_ids,
            last_profile_source,
            last_profile_confidence,
            last_checked_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (sentence_id) DO UPDATE
        SET
            user_id = EXCLUDED.user_id,
            source_lang = EXCLUDED.source_lang,
            target_lang = EXCLUDED.target_lang,
            last_attempt_no = EXCLUDED.last_attempt_no,
            last_score = EXCLUDED.last_score,
            last_score_band = EXCLUDED.last_score_band,
            last_retention_state = EXCLUDED.last_retention_state,
            last_tested_skill_ids = EXCLUDED.last_tested_skill_ids,
            last_errored_skill_ids = EXCLUDED.last_errored_skill_ids,
            last_profile_source = EXCLUDED.last_profile_source,
            last_profile_confidence = EXCLUDED.last_profile_confidence,
            last_checked_at = NOW();
        """,
        (
            int(shadow_state.get("sentence_id") or 0),
            int(shadow_state.get("user_id") or 0),
            str(shadow_state.get("source_lang") or "ru"),
            str(shadow_state.get("target_lang") or "de"),
            int(shadow_state.get("last_attempt_no") or 0),
            int(shadow_state.get("last_score") or 0),
            str(shadow_state.get("last_score_band") or "lt50"),
            str(shadow_state.get("last_retention_state") or "new_fail_store"),
            Json(list(shadow_state.get("last_tested_skill_ids") or [])),
            Json(list(shadow_state.get("last_errored_skill_ids") or [])),
            str(shadow_state.get("last_profile_source") or PHASE1_MISSING_PROFILE_SOURCE),
            float(shadow_state.get("last_profile_confidence") or PHASE1_MISSING_PROFILE_CONFIDENCE),
        ),
    )
    return inserted_event_refs


def _log_translation_mistake_with_cursor(
    cursor,
    *,
    user_id: int,
    original_text: str,
    categories: list[str],
    subcategories: list[str],
    score: int,
    correct_translation: str | None,
    source_lang: str = "ru",
    target_lang: str = "de",
) -> list[tuple[str, str]]:
    language_categories, language_subcategories, _ = _get_language_taxonomy(target_lang)
    if categories:
        logging.info("Categories from log_translation_mistake: %s", ", ".join(categories))
    if subcategories:
        logging.info("Subcategories from log_translation_mistake: %s", ", ".join(subcategories))

    valid_combinations = _normalize_category_pairs(
        categories,
        subcategories,
        target_lang=target_lang,
        fallback_to_other=True,
    )
    cursor.execute(
        """
        SELECT id_for_mistake_table
        FROM bt_3_daily_sentences
        WHERE sentence=%s
          AND COALESCE(source_lang, 'ru') = %s
          AND COALESCE(target_lang, 'de') = %s
        LIMIT 1;
        """,
        (original_text, source_lang or "ru", target_lang or "de"),
    )
    result = cursor.fetchone()
    sentence_id = result[0] if result else None
    safe_correct_translation = correct_translation if correct_translation is not None else ""
    applied_pairs: list[tuple[str, str]] = []

    insert_sql = """
        INSERT INTO bt_3_detailed_mistakes (
            user_id, sentence, added_data, main_category, sub_category, mistake_count, sentence_id,
            correct_translation, score
        ) VALUES (%s, %s, NOW(), %s, %s, 1, %s, %s, %s)
        ON CONFLICT (user_id, sentence, main_category, sub_category)
        DO UPDATE SET
            mistake_count = bt_3_detailed_mistakes.mistake_count + 1,
            attempt = bt_3_detailed_mistakes.attempt + 1,
            last_seen = NOW(),
            score = EXCLUDED.score;
    """

    for main_category, sub_category in valid_combinations:
        used_main_category = next(
            (cat for cat in language_categories if cat.lower() == main_category.lower()),
            main_category,
        )
        used_sub_category = next(
            (
                subcat
                for subcat in language_subcategories.get(used_main_category, [])
                if subcat.lower() == sub_category.lower()
            ),
            sub_category,
        )

        try:
            cursor.execute("SAVEPOINT log_translation_mistake_sp;")
            cursor.execute(
                insert_sql,
                (
                    user_id,
                    original_text,
                    used_main_category,
                    used_sub_category,
                    sentence_id,
                    safe_correct_translation,
                    score,
                ),
            )
        except psycopg2.Error as exc:
            logging.warning(
                "Ошибка записи detailed_mistakes (%s / %s). "
                "Пробуем fallback Other mistake / Unclassified mistake. %s",
                used_main_category,
                used_sub_category,
                exc,
            )
            try:
                cursor.execute("ROLLBACK TO SAVEPOINT log_translation_mistake_sp;")
            except Exception:
                pass
            used_main_category = "Other mistake"
            used_sub_category = "Unclassified mistake"
            try:
                cursor.execute(
                    insert_sql,
                    (
                        user_id,
                        original_text,
                        used_main_category,
                        used_sub_category,
                        sentence_id,
                        safe_correct_translation,
                        score,
                    ),
                )
            except psycopg2.Error:
                logging.exception(
                    "Не удалось записать ошибку даже в fallback-категорию "
                    "for user_id=%s sentence_id=%s",
                    user_id,
                    sentence_id,
                )
                try:
                    cursor.execute("ROLLBACK TO SAVEPOINT log_translation_mistake_sp;")
                    cursor.execute("RELEASE SAVEPOINT log_translation_mistake_sp;")
                except Exception:
                    pass
                continue
        finally:
            try:
                cursor.execute("RELEASE SAVEPOINT log_translation_mistake_sp;")
            except Exception:
                pass

        applied_pairs.append((used_main_category, used_sub_category))

    return list(dict.fromkeys(applied_pairs))


async def log_translation_mistake(
    user_id: int,
    original_text: str,
    user_translation: str,
    categories: list[str],
    subcategories: list[str],
    score: int,
    correct_translation: str | None,
    source_lang: str = "ru",
    target_lang: str = "de",
) -> None:
    applied_pairs: list[tuple[str, str]] = []
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            applied_pairs = _log_translation_mistake_with_cursor(
                cursor,
                user_id=user_id,
                original_text=original_text,
                categories=categories,
                subcategories=subcategories,
                score=score,
                correct_translation=correct_translation,
                source_lang=source_lang,
                target_lang=target_lang,
            )

    for main_category, sub_category in applied_pairs:
        try:
            apply_skill_events_for_error(
                user_id=int(user_id),
                source_lang=source_lang or "ru",
                target_lang=target_lang or "de",
                error_category=main_category,
                error_subcategory=sub_category,
                event_type="fail",
                fail_delta=-3.0,
                success_delta=2.0,
            )
        except Exception as exc:
            logging.warning("Skill fail update skipped: %s", exc)


async def apply_translation_result_side_effects(
    *,
    user_id: int,
    original_text: str,
    user_translation: str,
    sentence_pk_id: int | None,
    session_id: int | None,
    sentence_id_for_mistake: int,
    score_value: int,
    correct_translation: str | None,
    categories: list[str],
    subcategories: list[str],
    source_lang: str = "ru",
    target_lang: str = "de",
) -> None:
    success_pairs: list[tuple[str, str]] = []
    fail_pairs: list[tuple[str, str]] = []
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT EXISTS(
                    SELECT 1
                    FROM bt_3_detailed_mistakes
                    WHERE sentence_id = %s AND user_id = %s
                );
                """,
                (sentence_id_for_mistake, user_id),
            )
            was_in_mistakes = bool(cursor.fetchone()[0])
            tested_targets: list[dict[str, Any]] = []
            tested_targets_seeded = False

            if sentence_pk_id:
                tested_targets = _load_sentence_skill_targets_with_cursor(
                    cursor,
                    sentence_id=int(sentence_pk_id),
                )
                tested_targets_seeded = bool(tested_targets)
                if not tested_targets and sentence_id_for_mistake and was_in_mistakes:
                    # Capture remediation targets before a recovery success deletes
                    # the detailed_mistakes rows that the profile builder depends on.
                    tested_targets = _build_remediation_profile_with_cursor(
                        cursor,
                        user_id=int(user_id),
                        sentence_id_for_mistake=int(sentence_id_for_mistake),
                        source_lang=source_lang or "ru",
                        target_lang=target_lang or "de",
                        sentence_text=original_text,
                    )

            if was_in_mistakes:
                if score_value >= 85:
                    cursor.execute(
                        """
                        SELECT DISTINCT
                            COALESCE(NULLIF(main_category, ''), 'Other mistake') AS main_category,
                            COALESCE(NULLIF(sub_category, ''), 'Unclassified mistake') AS sub_category
                        FROM bt_3_detailed_mistakes
                        WHERE sentence_id = %s AND user_id = %s;
                        """,
                        (sentence_id_for_mistake, user_id),
                    )
                    success_pairs.extend(
                        (
                            str(main_category or "Other mistake"),
                            str(sub_category or "Unclassified mistake"),
                        )
                        for main_category, sub_category in (cursor.fetchall() or [])
                    )

                    cursor.execute(
                        """
                        SELECT attempt
                        FROM bt_3_attempts
                        WHERE id_for_mistake_table = %s AND user_id = %s;
                        """,
                        (sentence_id_for_mistake, user_id),
                    )
                    result = cursor.fetchone()
                    total_attempts = ((result[0] or 0) if result else 0) + 1

                    cursor.execute(
                        """
                        INSERT INTO bt_3_successful_translations (user_id, sentence_id, score, attempt, date)
                        VALUES (%s, %s, %s, %s, NOW());
                        """,
                        (user_id, sentence_id_for_mistake, score_value, total_attempts),
                    )

                    cursor.execute(
                        """
                        DELETE FROM bt_3_detailed_mistakes
                        WHERE sentence_id = %s AND user_id = %s;
                        """,
                        (sentence_id_for_mistake, user_id),
                    )

                    cursor.execute(
                        """
                        DELETE FROM bt_3_attempts
                        WHERE id_for_mistake_table = %s AND user_id= %s;
                        """,
                        (sentence_id_for_mistake, user_id),
                    )
                else:
                    cursor.execute(
                        """
                        INSERT INTO bt_3_attempts (user_id, id_for_mistake_table, timestamp)
                        VALUES (%s, %s, NOW())
                        ON CONFLICT (user_id, id_for_mistake_table)
                        DO UPDATE SET
                            attempt = bt_3_attempts.attempt + 1,
                            timestamp= NOW();
                        """,
                        (user_id, sentence_id_for_mistake),
                    )
                    fail_pairs.extend(
                        _log_translation_mistake_with_cursor(
                            cursor,
                            user_id=user_id,
                            original_text=original_text,
                            categories=categories,
                            subcategories=subcategories,
                            score=score_value,
                            correct_translation=correct_translation,
                            source_lang=source_lang or "ru",
                            target_lang=target_lang or "de",
                        )
                    )
            else:
                if score_value >= 80:
                    cursor.execute(
                        """
                        INSERT INTO bt_3_successful_translations (user_id, sentence_id, score, attempt, date)
                        VALUES(%s, %s, %s, %s, NOW());
                        """,
                        (user_id, sentence_id_for_mistake, score_value, 1),
                    )
                    success_pairs.extend(
                        _normalize_category_pairs(
                            categories,
                            subcategories,
                            target_lang=target_lang or "de",
                            fallback_to_other=False,
                        )
                    )
                else:
                    cursor.execute(
                        """
                        INSERT INTO bt_3_attempts (user_id, id_for_mistake_table)
                        VALUES (%s, %s)
                        ON CONFLICT (user_id, id_for_mistake_table)
                        DO UPDATE SET attempt = bt_3_attempts.attempt + 1;
                        """,
                        (user_id, sentence_id_for_mistake),
                    )
                    fail_pairs.extend(
                        _log_translation_mistake_with_cursor(
                            cursor,
                            user_id=user_id,
                            original_text=original_text,
                            categories=categories,
                            subcategories=subcategories,
                            score=score_value,
                            correct_translation=correct_translation,
                            source_lang=source_lang or "ru",
                            target_lang=target_lang or "de",
                        )
                    )

            if sentence_pk_id:
                if tested_targets and not tested_targets_seeded:
                    _insert_sentence_skill_targets_for_entries_with_cursor(
                        cursor,
                        sentence_profiles=[(int(sentence_pk_id), tested_targets)],
                    )
                    tested_targets_seeded = True
                if not tested_targets and sentence_id_for_mistake:
                    tested_targets = _build_remediation_profile_with_cursor(
                        cursor,
                        user_id=int(user_id),
                        sentence_id_for_mistake=int(sentence_id_for_mistake),
                        source_lang=source_lang or "ru",
                        target_lang=target_lang or "de",
                        sentence_text=original_text,
                    )
                    if tested_targets:
                        _insert_sentence_skill_targets_for_entries_with_cursor(
                            cursor,
                            sentence_profiles=[(int(sentence_pk_id), tested_targets)],
                        )
                        tested_targets_seeded = True
                previous_shadow_state = _load_sentence_skill_shadow_state_with_cursor(
                    cursor,
                    sentence_id=int(sentence_pk_id),
                )
                current_error_pairs = _normalize_category_pairs(
                    categories,
                    subcategories,
                    target_lang=target_lang or "de",
                    fallback_to_other=False,
                )
                current_errored_details = _build_errored_skill_details(
                    error_pairs=current_error_pairs,
                    target_lang=target_lang or "de",
                )
                fallback_previous_errored_details = None
                if not previous_shadow_state and success_pairs and was_in_mistakes:
                    fallback_previous_errored_details = _build_errored_skill_details(
                        error_pairs=success_pairs,
                        target_lang=target_lang or "de",
                    )
                event_rows, shadow_state = _build_phase1_shadow_payload(
                    user_id=int(user_id),
                    sentence_pk_id=int(sentence_pk_id),
                    session_id=int(session_id) if session_id is not None else None,
                    source_lang=source_lang or "ru",
                    target_lang=target_lang or "de",
                    score_value=int(score_value),
                    was_in_mistakes=was_in_mistakes,
                    tested_targets=tested_targets,
                    previous_shadow_state=previous_shadow_state,
                    current_errored_details=current_errored_details,
                    fallback_previous_errored_details=fallback_previous_errored_details,
                )
                _write_phase1_shadow_payload_with_cursor(
                    cursor,
                    event_rows=event_rows,
                    shadow_state=shadow_state,
                )

    for main_category, sub_category in list(dict.fromkeys(success_pairs)):
        try:
            apply_skill_events_for_error(
                user_id=int(user_id),
                source_lang=source_lang or "ru",
                target_lang=target_lang or "de",
                error_category=main_category,
                error_subcategory=sub_category,
                event_type="success",
                success_delta=2.0,
                fail_delta=-3.0,
            )
        except Exception as exc:
            logging.warning("Skill success update skipped: %s", exc)

    for main_category, sub_category in list(dict.fromkeys(fail_pairs)):
        try:
            apply_skill_events_for_error(
                user_id=int(user_id),
                source_lang=source_lang or "ru",
                target_lang=target_lang or "de",
                error_category=main_category,
                error_subcategory=sub_category,
                event_type="fail",
                fail_delta=-3.0,
                success_delta=2.0,
            )
        except Exception as exc:
            logging.warning("Skill fail update skipped: %s", exc)


async def check_user_translation_webapp_item(
    user_id: int,
    username: str | None,
    translation: dict[str, Any],
    source_lang: str = "ru",
    target_lang: str = "de",
    daily_session_id: str | int | None = None,
    *,
    checkpoint_item_id: int | None = None,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    sentence_id_for_mistake = translation.get("id_for_mistake_table")
    if isinstance(sentence_id_for_mistake, str) and sentence_id_for_mistake.isdigit():
        sentence_id_for_mistake = int(sentence_id_for_mistake)
    user_translation = (translation.get("translation") or "").strip()
    if not sentence_id_for_mistake or not user_translation:
        return None, None

    normalized_source_lang = source_lang or "ru"
    normalized_target_lang = target_lang or "de"
    latest_session_id = _normalize_translation_session_id(daily_session_id)

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            if latest_session_id is None:
                latest_session_id = _resolve_latest_translation_session_id(
                    cursor,
                    user_id=int(user_id),
                    source_lang=normalized_source_lang,
                    target_lang=normalized_target_lang,
                )
            if latest_session_id is None:
                return None, None

            cursor.execute(
                """
                SELECT unique_id, id_for_mistake_table, id, sentence, session_id
                FROM bt_3_daily_sentences
                WHERE session_id = %s
                  AND user_id = %s
                  AND COALESCE(source_lang, 'ru') = %s
                  AND COALESCE(target_lang, 'de') = %s
                  AND id_for_mistake_table = %s
                LIMIT 1;
                """,
                (
                    latest_session_id,
                    user_id,
                    normalized_source_lang,
                    normalized_target_lang,
                    int(sentence_id_for_mistake),
                ),
            )
            sentence_row = cursor.fetchone()
            if not sentence_row:
                return {
                    "sentence_number": None,
                    "error": "Предложение не принадлежит пользователю или не найдено.",
                }, None

            sentence_number = sentence_row[0]
            sentence_pk_id = sentence_row[2]
            original_text = sentence_row[3]
            source_session_id = sentence_row[4]

            cursor.execute(
                """
                SELECT id, score, user_translation, feedback
                FROM bt_3_translations
                WHERE user_id = %s
                  AND sentence_id = %s
                  AND COALESCE(source_lang, 'ru') = %s
                  AND COALESCE(target_lang, 'de') = %s
                ORDER BY timestamp DESC, id DESC
                LIMIT 1;
                """,
                (user_id, sentence_pk_id, normalized_source_lang, normalized_target_lang),
            )
            existing_translation = cursor.fetchone()

    if existing_translation:
        result_item = {
            "translation_id": int(existing_translation[0]),
            "audio_grammar_opt_in": False,
            "sentence_number": sentence_number,
            "score": int(existing_translation[1] or 0),
            "original_text": original_text,
            "user_translation": str(existing_translation[2] or "").strip(),
            "correct_translation": None,
            "feedback": str(existing_translation[3] or "").strip(),
        }
        if checkpoint_item_id is not None:
            update_translation_check_item_result(
                item_id=int(checkpoint_item_id),
                status="running",
                result_json=result_item,
                result_text=str(result_item.get("feedback") or "").strip() or None,
                error_text=None,
                webapp_check_id=result_item.get("translation_id"),
                started=True,
                finished=False,
            )
        return result_item, None

    try:
        feedback, categories, subcategories, score, correct_translation = await check_translation(
            original_text,
            user_translation,
            sentence_number,
            source_lang=source_lang,
            target_lang=target_lang,
        )
    except Exception as exc:
        logging.error("Ошибка при проверке перевода №%s: %s", sentence_number, exc, exc_info=True)
        return {
            "sentence_number": sentence_number,
            "error": "Ошибка: не удалось проверить перевод.",
        }, None

    score_value = int(score) if score and str(score).isdigit() else 0
    translation_id = None
    stored_user_translation = user_translation
    stored_score_value = score_value
    stored_feedback = feedback
    inserted_new_row = False
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_translations (user_id, id_for_mistake_table, session_id, username, sentence_id,
                user_translation, score, feedback, source_lang, target_lang)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (user_id, sentence_id, session_id) WHERE session_id IS NOT NULL DO NOTHING
                RETURNING id, user_translation, score, feedback;
                """,
                (
                    user_id,
                    sentence_id_for_mistake,
                    source_session_id,
                    username,
                    sentence_pk_id,
                    user_translation,
                    score_value,
                    feedback,
                    normalized_source_lang,
                    normalized_target_lang,
                ),
            )
            created_translation = cursor.fetchone()
            if created_translation:
                inserted_new_row = True
                translation_id = int(created_translation[0]) if created_translation[0] is not None else None
                stored_user_translation = str(created_translation[1] or "").strip() or user_translation
                stored_score_value = int(created_translation[2] or 0)
                stored_feedback = str(created_translation[3] or "").strip() or feedback
            else:
                cursor.execute(
                    """
                    SELECT id, user_translation, score, feedback
                    FROM bt_3_translations
                    WHERE user_id = %s
                      AND sentence_id = %s
                      AND session_id = %s
                    LIMIT 1;
                    """,
                    (user_id, sentence_pk_id, source_session_id),
                )
                existing_row = cursor.fetchone()
                if existing_row:
                    translation_id = int(existing_row[0]) if existing_row[0] is not None else None
                    stored_user_translation = str(existing_row[1] or "").strip() or user_translation
                    stored_score_value = int(existing_row[2] or 0)
                    stored_feedback = str(existing_row[3] or "").strip() or feedback

    result_item = {
        "translation_id": translation_id,
        "audio_grammar_opt_in": False,
        "sentence_number": sentence_number,
        "score": stored_score_value,
        "original_text": original_text,
        "user_translation": stored_user_translation,
        "correct_translation": correct_translation,
        "feedback": stored_feedback,
    }
    if inserted_new_row:
        deferred_payload = {
            "user_id": int(user_id),
            "original_text": original_text,
            "user_translation": user_translation,
            "sentence_pk_id": int(sentence_pk_id),
            "session_id": int(source_session_id) if source_session_id is not None else None,
            "sentence_id_for_mistake": int(sentence_id_for_mistake),
            "score_value": score_value,
            "correct_translation": correct_translation,
            "categories": list(categories or []),
            "subcategories": list(subcategories or []),
            "source_lang": normalized_source_lang,
            "target_lang": normalized_target_lang,
        }
        await apply_translation_result_side_effects(**deferred_payload)
    if checkpoint_item_id is not None:
        update_translation_check_item_result(
            item_id=int(checkpoint_item_id),
            status="running",
            result_json=result_item,
            result_text=str(result_item.get("feedback") or "").strip() or None,
            error_text=str(result_item.get("error") or "").strip() or None,
            webapp_check_id=result_item.get("translation_id"),
            started=True,
            finished=False,
        )
    return result_item, None


async def check_user_translation_webapp(
    user_id: int,
    username: str | None,
    translations: list[dict[str, Any]],
    source_lang: str = "ru",
    target_lang: str = "de",
    daily_session_id: str | int | None = None,
) -> list[dict[str, Any]]:
    if not translations:
        return []

    normalized_source_lang = source_lang or "ru"
    normalized_target_lang = target_lang or "de"
    latest_session_id = _normalize_translation_session_id(daily_session_id)
    if latest_session_id is None:
        with get_db_connection_context() as conn:
            with conn.cursor() as cursor:
                latest_session_id = _resolve_latest_translation_session_id(
                    cursor,
                    user_id=int(user_id),
                    source_lang=normalized_source_lang,
                    target_lang=normalized_target_lang,
                )
    if latest_session_id is None:
        return []

    results: list[dict[str, Any]] = []
    for entry in translations:
        result_item, _ = await check_user_translation_webapp_item(
            user_id,
            username,
            entry,
            source_lang=normalized_source_lang,
            target_lang=normalized_target_lang,
            daily_session_id=latest_session_id,
            checkpoint_item_id=None,
        )
        if result_item:
            results.append(result_item)

    results.sort(key=lambda item: item.get("sentence_number") or 0)
    return results


def finish_translation_webapp(user_id: int) -> dict[str, Any]:
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(
            """
            SELECT session_id
            FROM bt_3_user_progress
            WHERE user_id = %s AND completed = FALSE
            ORDER BY start_time DESC
            """,
            (user_id,),
        )
        session_rows = cursor.fetchall() or []
        if not session_rows:
            return {
                "message": (
                    "❌ У вас нет активных сессий! Используйте кнопки: "
                    "'📌 Выбрать тему' -> '🚀 Начать перевод' чтобы начать."
                ),
                "status": "no_session",
            }
        active_session_ids = [row[0] for row in session_rows if row and row[0] is not None]
        session_id = active_session_ids[0]
        if len(active_session_ids) > 1:
            logging.warning(
                "finish_translation_webapp: multiple active sessions detected for user_id=%s sessions=%s; closing all.",
                user_id,
                active_session_ids,
            )

        cursor.execute(
            """
            SELECT COUNT(*)
            FROM bt_3_daily_sentences
            WHERE user_id = %s
              AND session_id = %s
              AND COALESCE(shown_to_user, FALSE) = TRUE;
            """,
            (user_id, session_id),
        )
        total_sentences = cursor.fetchone()[0] or 0

        cursor.execute(
            """
            SELECT COUNT(DISTINCT t.sentence_id)
            FROM bt_3_translations t
            JOIN bt_3_daily_sentences ds
              ON ds.id = t.sentence_id
             AND ds.user_id = t.user_id
            WHERE t.user_id = %s
              AND t.session_id = %s
              AND COALESCE(ds.shown_to_user, FALSE) = TRUE;
            """,
            (user_id, session_id),
        )
        translated_count = cursor.fetchone()[0] or 0

        cursor.execute(
            """
            UPDATE bt_3_user_progress
            SET
                active_seconds = COALESCE(active_seconds, 0)
                    + CASE
                        WHEN COALESCE(active_running, FALSE) = TRUE
                         AND active_started_at IS NOT NULL
                            THEN GREATEST(
                                0,
                                EXTRACT(EPOCH FROM (NOW() - active_started_at))::BIGINT
                            )
                        ELSE 0
                    END,
                active_started_at = NULL,
                active_running = FALSE,
                end_time = NOW(),
                completed = TRUE
            WHERE user_id = %s AND completed = FALSE;
            """,
            (user_id,),
        )
        conn.commit()
        try:
            delete_translation_draft_state(user_id=int(user_id), source_session_id=str(session_id))
        except Exception:
            logging.warning(
                "finish_translation_webapp: failed to clear translation drafts for user_id=%s session_id=%s",
                user_id,
                session_id,
                exc_info=True,
            )

        if total_sentences == 0:
            message = (
                "Сессия завершена.\n"
                "В статистику попадают только предложения, которые были реально показаны пользователю."
            )
        elif translated_count == 0:
            message = (
                f"😔 Вы не перевели ни одного предложения из {total_sentences} в этой сессии.\n"
                "Попробуйте начать новую сессию с помощью кнопок "
                "'📌 Выбрать тему' -> '🚀 Начать перевод'."
            )
        elif translated_count < total_sentences:
            message = (
                f"⚠️ Вы перевели {translated_count} из {total_sentences} предложений.\n"
                "Перевод завершён, но не все предложения переведены. "
                "Это повлияет на ваш итоговый балл."
            )
        else:
            message = (
                "🎉 Вы успешно завершили перевод!\n"
                f"Все {total_sentences} предложений этой сессии переведены! 🚀"
            )

        return {
            "message": message,
            "status": "completed",
            "total_sentences": total_sentences,
            "translated_count": translated_count,
        }
    finally:
        cursor.close()
        conn.close()


def build_user_daily_summary(user_id: int, username: str | None) -> str | None:
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                WITH latest_translations AS (
                    SELECT
                        sentence_id,
                        score
                    FROM (
                        SELECT
                            t.sentence_id,
                            t.score,
                            ROW_NUMBER() OVER (
                                PARTITION BY t.sentence_id
                                ORDER BY t.timestamp DESC, t.id DESC
                            ) AS rn
                        FROM bt_3_translations t
                        WHERE t.user_id = %s
                          AND t.timestamp::date = CURRENT_DATE
                    ) ranked
                    WHERE rn = 1
                )
                SELECT
                    COUNT(DISTINCT CASE WHEN COALESCE(ds.shown_to_user, FALSE) = TRUE THEN ds.id END) AS total_sentences,
                    COUNT(DISTINCT CASE WHEN COALESCE(ds.shown_to_user, FALSE) = TRUE THEN lt.sentence_id END) AS translated,
                    (
                        COUNT(DISTINCT CASE WHEN COALESCE(ds.shown_to_user, FALSE) = TRUE THEN ds.id END)
                        - COUNT(DISTINCT CASE WHEN COALESCE(ds.shown_to_user, FALSE) = TRUE THEN lt.sentence_id END)
                    ) AS missed,
                    COALESCE(progress_summary.avg_time, 0) AS avg_time_minutes,
                    COALESCE(progress_summary.total_time, 0) AS total_time_minutes,
                    COALESCE(AVG(CASE WHEN COALESCE(ds.shown_to_user, FALSE) = TRUE THEN lt.score END), 0) AS avg_score
                FROM bt_3_daily_sentences ds
                LEFT JOIN latest_translations lt
                    ON ds.id = lt.sentence_id
                LEFT JOIN (
                    SELECT user_id,
                        AVG({build_translation_session_minutes_sql('p')}) AS avg_time,
                        SUM({build_translation_session_minutes_sql('p')}) AS total_time
                    FROM bt_3_user_progress p
                    WHERE completed = TRUE
                        AND start_time::date = CURRENT_DATE
                    GROUP BY user_id
                ) progress_summary ON ds.user_id = progress_summary.user_id
                WHERE ds.date = CURRENT_DATE AND ds.user_id = %s
                GROUP BY ds.user_id, progress_summary.avg_time, progress_summary.total_time;
                """,
                (user_id, user_id),
            )
            row = cursor.fetchone()

    if not row:
        return None

    total_sentences, translated, missed, avg_minutes, total_minutes, avg_score = row
    missed_days = 1 if int(missed or 0) > 0 else 0
    final_score = _calculate_final_score(
        avg_score=float(avg_score or 0.0),
        avg_time_min=float(avg_minutes or 0.0),
        missed_days=missed_days,
    )
    display_name = username or f"user_{user_id}"

    return (
        f"📅 Сегодняшняя статистика ({display_name})\n"
        f"📜 Всего предложений: {total_sentences}\n"
        f"✅ Переведено: {translated}\n"
        f"🚨 Не переведено: {missed}\n"
        f"⏱ Среднее время сессии: {avg_minutes:.1f} мин\n"
        f"⏱ Время общее: {total_minutes:.1f} мин\n"
        f"🎯 Средняя оценка: {avg_score:.1f}/100\n"
        f"🏆 Итоговый балл: {final_score:.1f}"
    )
