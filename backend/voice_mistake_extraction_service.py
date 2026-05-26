"""Isolated extraction pipeline for structured voice session grammar mistakes.

Extracts grammar mistakes from a completed voice session transcript and stores
them in bt_3_voice_session_mistakes. Fully isolated from the existing assessment
flow: all exceptions are caught and logged, and the public function always returns
an integer (mistakes inserted) without propagating exceptions to callers.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from typing import Any

try:
    from backend.database import (
        fetch_agent_voice_transcript_segments,
        insert_voice_session_mistake,
    )
    from backend.openai_manager import llm_execute
    from backend.voice_mistake_taxonomy import (
        validate_alternatives,
        validate_category,
        validate_confidence,
        validate_severity,
        validate_subtype,
    )
except Exception:
    from database import (  # type: ignore
        fetch_agent_voice_transcript_segments,
        insert_voice_session_mistake,
    )
    from openai_manager import llm_execute  # type: ignore
    from voice_mistake_taxonomy import (  # type: ignore
        validate_alternatives,
        validate_category,
        validate_confidence,
        validate_severity,
        validate_subtype,
    )


# ── Thresholds ────────────────────────────────────────────────────────────────

_MAX_QUOTE_CHARS = 300
_MAX_ALTERNATIVES = 3
_MAX_MISTAKES_PER_SESSION = 25
_EXTRACTION_JSON_RETRIES = 2
_MIN_USER_SEGMENTS = 2
_MIN_USER_CHARS = 60


# ── Transcript helpers ────────────────────────────────────────────────────────


def _build_user_corpus(segments: list[dict]) -> tuple[list[str], str]:
    """Returns (user_texts_list, concatenated_user_text)."""
    user_texts: list[str] = []
    for seg in segments:
        speaker = str(seg.get("speaker") or "").strip().lower()
        text = str(seg.get("text") or "").strip()
        if speaker == "user" and text:
            user_texts.append(text)
    return user_texts, " ".join(user_texts)


def _build_transcript_turns(segments: list[dict]) -> list[dict[str, str]]:
    turns: list[dict[str, str]] = []
    for seg in segments:
        speaker = str(seg.get("speaker") or "").strip().lower()
        text = str(seg.get("text") or "").strip()
        if text and speaker in ("user", "assistant"):
            turns.append({"speaker": speaker, "text": text})
    return turns


# ── Quote anchoring ───────────────────────────────────────────────────────────


def _normalize_for_match(value: str) -> str:
    s = str(value or "").casefold()
    return re.sub(r"\s+", " ", s).strip()


def _quote_in_user_text(quote: str, user_texts: list[str]) -> bool:
    norm_quote = _normalize_for_match(quote)
    if not norm_quote:
        return False
    for text in user_texts:
        if norm_quote in _normalize_for_match(text):
            return True
    return False


# ── LLM call with retry ───────────────────────────────────────────────────────


def _parse_mistakes_from_raw(raw: str) -> list[dict] | None:
    cleaned = str(raw or "").strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```[a-zA-Z]*\n?", "", cleaned).rstrip("`").strip()
    try:
        parsed = json.loads(cleaned)
    except Exception:
        match = re.search(r"\{.*\}", cleaned, re.DOTALL)
        if not match:
            return None
        try:
            parsed = json.loads(match.group(0))
        except Exception:
            return None
    if not isinstance(parsed, dict):
        return None
    mistakes = parsed.get("mistakes")
    if not isinstance(mistakes, list):
        return None
    return mistakes


async def _call_extraction_llm_with_retry(
    *,
    session_id: int,
    payload_json: str,
) -> list[dict] | None:
    for attempt in range(1 + _EXTRACTION_JSON_RETRIES):
        if attempt > 0:
            logging.info(
                "extraction_retry session_id=%s attempt=%d",
                session_id,
                attempt + 1,
            )
        try:
            raw = await llm_execute(
                task_name="voice_mistake_extraction",
                system_instruction_key="voice_mistake_extraction",
                user_message=payload_json,
                poll_interval_seconds=1.0,
                responses_timeout_seconds=30.0,
            )
        except Exception as exc:
            logging.warning(
                "extraction_llm_error session_id=%s attempt=%d error=%s",
                session_id,
                attempt + 1,
                exc,
            )
            continue

        mistakes = _parse_mistakes_from_raw(raw)
        if mistakes is not None:
            return mistakes

        logging.warning(
            "extraction_json_parse_failed session_id=%s attempt=%d raw_preview=%r",
            session_id,
            attempt + 1,
            str(raw or "")[:200],
        )

    return None


# ── Per-mistake validation ────────────────────────────────────────────────────


def _validate_mistake(
    obj: Any,
    *,
    session_id: int,
    user_texts: list[str],
) -> dict | None:
    """Validate and clean one raw mistake dict from LLM output.

    Returns a normalized dict on success, or None on any validation failure.
    Every rejection is logged with a reason.
    """
    if not isinstance(obj, dict):
        logging.warning(
            "extraction_validation_failed session_id=%s reason=not_a_dict type=%s",
            session_id,
            type(obj).__name__,
        )
        return None

    user_quote = str(obj.get("user_quote") or "").strip()
    corrected_form = str(obj.get("corrected_form") or "").strip()
    rule_explanation = str(obj.get("rule_explanation") or "").strip()

    if not user_quote:
        logging.warning(
            "extraction_validation_failed session_id=%s reason=empty_user_quote",
            session_id,
        )
        return None

    if len(user_quote) > _MAX_QUOTE_CHARS:
        logging.warning(
            "extraction_validation_failed session_id=%s reason=quote_too_long "
            "len=%d quote_preview=%r",
            session_id,
            len(user_quote),
            user_quote[:80],
        )
        return None

    if not corrected_form:
        logging.warning(
            "extraction_validation_failed session_id=%s reason=empty_corrected_form "
            "quote=%r",
            session_id,
            user_quote[:80],
        )
        return None

    if not rule_explanation:
        logging.warning(
            "extraction_validation_failed session_id=%s reason=empty_rule_explanation "
            "quote=%r",
            session_id,
            user_quote[:80],
        )
        return None

    if not _quote_in_user_text(user_quote, user_texts):
        logging.warning(
            "extraction_validation_failed session_id=%s reason=quote_not_in_user_text "
            "quote=%r",
            session_id,
            user_quote[:120],
        )
        return None

    try:
        validated_category = validate_category(obj.get("error_category"))
    except ValueError as exc:
        logging.warning(
            "extraction_validation_failed session_id=%s reason=bad_category "
            "error=%s quote=%r",
            session_id,
            exc,
            user_quote[:80],
        )
        return None

    try:
        validated_subtype = validate_subtype(
            obj.get("error_subtype"), category=validated_category
        )
    except ValueError as exc:
        logging.warning(
            "extraction_validation_failed session_id=%s reason=bad_subtype "
            "error=%s quote=%r",
            session_id,
            exc,
            user_quote[:80],
        )
        return None

    try:
        validated_severity = validate_severity(obj.get("severity") or "medium")
    except ValueError as exc:
        logging.warning(
            "extraction_validation_failed session_id=%s reason=bad_severity "
            "error=%s quote=%r",
            session_id,
            exc,
            user_quote[:80],
        )
        return None

    grammar_confidence: float | None = None
    raw_gc = obj.get("grammar_confidence")
    if raw_gc is not None:
        try:
            grammar_confidence = validate_confidence(
                raw_gc, field_name="grammar_confidence"
            )
        except ValueError as exc:
            logging.warning(
                "extraction_validation_failed session_id=%s "
                "reason=bad_grammar_confidence error=%s — using None",
                session_id,
                exc,
            )

    raw_alts = obj.get("alternatives") or []
    try:
        validated_alternatives = validate_alternatives(raw_alts)[:_MAX_ALTERNATIVES]
    except ValueError:
        validated_alternatives = []

    return {
        "user_quote": user_quote,
        "corrected_form": corrected_form,
        "rule_explanation": rule_explanation,
        "error_category": validated_category,
        "error_subtype": validated_subtype,
        "severity": validated_severity,
        "grammar_confidence": grammar_confidence,
        "alternatives": validated_alternatives,
    }


# ── Public entrypoint ─────────────────────────────────────────────────────────


async def extract_and_store_voice_mistakes(*, session_id: int) -> int:
    """Extract grammar mistakes from a completed voice session and store them.

    Fully isolated: never raises, always returns an int (count inserted).
    Safe to call from any context without affecting the assessment pipeline.
    """
    log_prefix = f"session_id={session_id}"
    logging.info("extraction_started %s", log_prefix)

    try:
        segments = await asyncio.to_thread(
            fetch_agent_voice_transcript_segments,
            session_id=int(session_id),
        )
    except Exception as exc:
        logging.warning(
            "extraction_failed %s reason=transcript_fetch_error error=%s",
            log_prefix,
            exc,
        )
        return 0

    user_texts, user_concat = _build_user_corpus(segments)
    user_segment_count = len(user_texts)
    user_chars = len(user_concat)

    if user_segment_count < _MIN_USER_SEGMENTS:
        logging.info(
            "extraction_skipped %s reason=too_few_user_segments "
            "user_segments=%d threshold=%d",
            log_prefix,
            user_segment_count,
            _MIN_USER_SEGMENTS,
        )
        return 0

    if user_chars < _MIN_USER_CHARS:
        logging.info(
            "extraction_skipped %s reason=too_few_user_chars "
            "user_chars=%d threshold=%d",
            log_prefix,
            user_chars,
            _MIN_USER_CHARS,
        )
        return 0

    transcript_turns = _build_transcript_turns(segments)
    payload_json = json.dumps(
        {"transcript": transcript_turns, "user_segments_only": user_texts},
        ensure_ascii=False,
    )

    try:
        mistakes_raw = await _call_extraction_llm_with_retry(
            session_id=int(session_id),
            payload_json=payload_json,
        )
    except Exception as exc:
        logging.warning(
            "extraction_failed %s reason=llm_call_exception error=%s",
            log_prefix,
            exc,
        )
        return 0

    if mistakes_raw is None:
        logging.warning(
            "extraction_failed %s reason=json_parse_failed_all_attempts",
            log_prefix,
        )
        return 0

    mistakes_raw = mistakes_raw[:_MAX_MISTAKES_PER_SESSION]

    validated: list[dict] = []
    seen_dedup: set[tuple[str, str, str]] = set()

    for raw_obj in mistakes_raw:
        clean = _validate_mistake(
            raw_obj,
            session_id=int(session_id),
            user_texts=user_texts,
        )
        if clean is None:
            continue
        dedup_key = (
            clean["user_quote"].casefold(),
            clean["error_category"],
            clean["error_subtype"],
        )
        if dedup_key in seen_dedup:
            logging.info(
                "extraction_dedup_skip %s category=%s subtype=%s quote=%r",
                log_prefix,
                clean["error_category"],
                clean["error_subtype"],
                clean["user_quote"][:60],
            )
            continue
        seen_dedup.add(dedup_key)
        validated.append(clean)

    inserted_count = 0
    for mistake in validated:
        try:
            await asyncio.to_thread(
                insert_voice_session_mistake,
                session_id=int(session_id),
                error_category=mistake["error_category"],
                error_subtype=mistake["error_subtype"],
                severity=mistake["severity"],
                user_quote=mistake["user_quote"],
                corrected_form=mistake["corrected_form"],
                rule_explanation=mistake["rule_explanation"],
                alternatives=mistake["alternatives"],
                grammar_confidence=mistake["grammar_confidence"],
            )
            inserted_count += 1
        except Exception as exc:
            logging.warning(
                "extraction_insert_failed %s category=%s subtype=%s error=%s",
                log_prefix,
                mistake["error_category"],
                mistake["error_subtype"],
                exc,
            )

    logging.info(
        "extraction_inserted_count %s inserted=%d validated=%d raw=%d",
        log_prefix,
        inserted_count,
        len(validated),
        len(mistakes_raw),
    )
    logging.info("extraction_completed %s", log_prefix)
    return inserted_count
