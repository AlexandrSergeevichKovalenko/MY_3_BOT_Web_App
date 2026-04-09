"""Minimal post-call assessment service for voice sessions.

This module turns the already stored transcript plus optional session context
into one persisted qualitative assessment record per voice session.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field

try:
    from backend.database import (
        fetch_agent_voice_transcript_segments,
        get_agent_voice_session_context,
        get_voice_session_assessment,
        upsert_voice_session_assessment,
    )
    from backend.openai_manager import llm_execute
except Exception:
    from database import (  # type: ignore
        fetch_agent_voice_transcript_segments,
        get_agent_voice_session_context,
        get_voice_session_assessment,
        upsert_voice_session_assessment,
    )
    from openai_manager import llm_execute  # type: ignore


_SHORT_TRANSCRIPT_SEGMENT_THRESHOLD = 4
_SHORT_TRANSCRIPT_CHAR_THRESHOLD = 220
_ASSESSMENT_TEXT_LIMITS = {
    "summary": 220,
    "strict_feedback": 320,
    "lexical_range_note": 260,
    "grammar_control_note": 260,
    "fluency_note": 260,
    "coherence_relevance_note": 260,
    "self_correction_note": 220,
    "recommended_next_focus": 220,
}
_LOW_SIGNAL_PATTERNS = [
    r"\bgood job\b",
    r"\bwell done\b",
    r"\bkeep practicing\b",
    r"\bcontinue practicing\b",
    r"\bcontinue to practice\b",
    r"\bgreat job\b",
    r"\bnice work\b",
    r"\bvery good\b",
]
_GENERIC_RECOMMENDED_FOCUS_PATTERNS = [
    r"^keep practicing[.!]*$",
    r"^practice more[.!]*$",
    r"^continue practicing[.!]*$",
    r"^work on everything[.!]*$",
]
_NOTE_FIELD_FALLBACKS = {
    "lexical_range_note": "Not enough structured lexical feedback was returned.",
    "grammar_control_note": "Not enough structured grammar feedback was returned.",
    "fluency_note": "Not enough structured fluency feedback was returned.",
    "coherence_relevance_note": "Not enough structured coherence feedback was returned.",
    "self_correction_note": "No structured self-correction note was returned.",
}


@dataclass(slots=True)
class VoiceAssessment:
    """Minimal stored voice assessment."""

    session_id: int
    summary: str
    strict_feedback: str
    lexical_range_note: str
    grammar_control_note: str
    fluency_note: str
    coherence_relevance_note: str
    self_correction_note: str
    target_vocab_used: list[str] = field(default_factory=list)
    target_vocab_missed: list[str] = field(default_factory=list)
    recommended_next_focus: str | None = None
    created_at: str | None = None
    updated_at: str | None = None


def _trim_sentence_boundary(text: str, *, limit: int) -> str:
    normalized = str(text or "").strip()
    if len(normalized) <= limit:
        return normalized
    truncated = normalized[:limit].rstrip()
    last_break = max(truncated.rfind("."), truncated.rfind("!"), truncated.rfind("?"), truncated.rfind(";"))
    if last_break >= int(limit * 0.55):
        return truncated[: last_break + 1].strip()
    return truncated.rstrip(" ,;:-") + "..."


def _clean_assessment_text(value: object, *, fallback: str, field_name: str) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    for pattern in _LOW_SIGNAL_PATTERNS:
        text = re.sub(pattern, "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s+", " ", text).strip(" ,;:-")
    limit = int(_ASSESSMENT_TEXT_LIMITS.get(field_name, 260))
    text = _trim_sentence_boundary(text, limit=limit)
    if field_name == "recommended_next_focus":
        for pattern in _GENERIC_RECOMMENDED_FOCUS_PATTERNS:
            if re.match(pattern, text, flags=re.IGNORECASE):
                text = ""
                break
    if field_name == "summary":
        text = re.sub(r"^(the user|the learner)\s+", "", text, flags=re.IGNORECASE).strip()
        if text:
            text = text[:1].upper() + text[1:]
    return text or fallback


def _normalize_string(value: object, *, fallback: str) -> str:
    text = str(value or "").strip()
    return text or fallback


def _normalize_string_list(value: object) -> list[str]:
    if isinstance(value, list):
        raw_items = value
    else:
        raw_items = []
    seen: set[str] = set()
    normalized: list[str] = []
    for item in raw_items:
        text = str(item or "").strip()
        if not text:
            continue
        key = text.casefold()
        if key in seen:
            continue
        seen.add(key)
        normalized.append(text)
    return normalized


def _build_transcript_text(segments: list[dict]) -> str:
    lines: list[str] = []
    for segment in segments:
        speaker = str(segment.get("speaker") or "unknown").strip().lower() or "unknown"
        text = str(segment.get("text") or "").strip()
        if not text:
            continue
        lines.append(f"{speaker}: {text}")
    return "\n".join(lines)


def _normalize_text_for_match(value: str) -> str:
    normalized = str(value or "").casefold()
    normalized = re.sub(r"[^\w\s]+", " ", normalized, flags=re.UNICODE)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def _target_item_used(item: str, transcript_text: str) -> bool:
    normalized_item = _normalize_text_for_match(str(item or ""))
    normalized_transcript = _normalize_text_for_match(str(transcript_text or ""))
    if not normalized_item or not normalized_transcript:
        return False
    pattern = r"(?<!\w)" + r"\s+".join(re.escape(part) for part in normalized_item.split()) + r"(?!\w)"
    return re.search(pattern, normalized_transcript, flags=re.IGNORECASE) is not None


def _compute_target_vocab_heuristics(*, transcript_text: str, prep_pack: dict | None) -> tuple[list[str], list[str]]:
    raw_targets = []
    if prep_pack:
        raw_targets.extend(list(prep_pack.get("target_vocab") or []))
        raw_targets.extend(list(prep_pack.get("target_expressions") or []))
    normalized_targets = _normalize_string_list(raw_targets)
    used = [item for item in normalized_targets if _target_item_used(item, transcript_text)]
    used_keys = {item.casefold() for item in used}
    missed = [item for item in normalized_targets if item.casefold() not in used_keys]
    return used, missed


def _dedupe_low_signal_notes(values: dict[str, str]) -> dict[str, str]:
    seen: set[str] = set()
    result = dict(values)
    for field_name in (
        "lexical_range_note",
        "grammar_control_note",
        "fluency_note",
        "coherence_relevance_note",
        "self_correction_note",
    ):
        text = str(result.get(field_name) or "").strip()
        key = text.casefold()
        if not text:
            result[field_name] = _NOTE_FIELD_FALLBACKS[field_name]
            continue
        if key in seen:
            result[field_name] = _NOTE_FIELD_FALLBACKS[field_name]
            continue
        seen.add(key)
    return result


def _build_short_transcript_fallback(
    *,
    session_id: int,
    transcript_text: str,
    prep_pack: dict | None,
) -> VoiceAssessment:
    used, missed = _compute_target_vocab_heuristics(
        transcript_text=transcript_text,
        prep_pack=prep_pack,
    )
    return VoiceAssessment(
        session_id=int(session_id),
        summary="Transcript too short for a detailed post-call assessment.",
        strict_feedback="There is not enough spoken material yet to make strong claims about sustained speaking performance.",
        lexical_range_note="Limited evidence. The learner produced too little language to judge lexical range reliably.",
        grammar_control_note="Limited evidence. A longer exchange is needed to assess grammar control fairly.",
        fluency_note="Limited evidence. The transcript is too short to assess sustained fluency.",
        coherence_relevance_note="The available material is too short to evaluate coherence and topic maintenance in a reliable way.",
        self_correction_note="No reliable self-correction pattern can be inferred from the short transcript.",
        target_vocab_used=used,
        target_vocab_missed=missed,
        recommended_next_focus="Run a longer speaking exchange and gather more transcript evidence before making a stronger assessment.",
    )


def _assessment_from_payload(*, session_id: int, payload: dict, fallback_used: list[str], fallback_missed: list[str]) -> VoiceAssessment:
    note_values = _dedupe_low_signal_notes(
        {
            "lexical_range_note": _clean_assessment_text(
                payload.get("lexical_range_note"),
                fallback="Not enough structured lexical feedback was returned.",
                field_name="lexical_range_note",
            ),
            "grammar_control_note": _clean_assessment_text(
                payload.get("grammar_control_note"),
                fallback="Not enough structured grammar feedback was returned.",
                field_name="grammar_control_note",
            ),
            "fluency_note": _clean_assessment_text(
                payload.get("fluency_note"),
                fallback="Not enough structured fluency feedback was returned.",
                field_name="fluency_note",
            ),
            "coherence_relevance_note": _clean_assessment_text(
                payload.get("coherence_relevance_note"),
                fallback="Not enough structured coherence feedback was returned.",
                field_name="coherence_relevance_note",
            ),
            "self_correction_note": _clean_assessment_text(
                payload.get("self_correction_note"),
                fallback="No structured self-correction note was returned.",
                field_name="self_correction_note",
            ),
        }
    )
    return VoiceAssessment(
        session_id=int(session_id),
        summary=_clean_assessment_text(
            payload.get("summary"),
            fallback="The session ended, but the assessment summary was incomplete.",
            field_name="summary",
        ),
        strict_feedback=_clean_assessment_text(
            payload.get("strict_feedback"),
            fallback="The transcript does not yet contain enough precise critique, so the next session should force more complex speaking.",
            field_name="strict_feedback",
        ),
        lexical_range_note=note_values["lexical_range_note"],
        grammar_control_note=note_values["grammar_control_note"],
        fluency_note=note_values["fluency_note"],
        coherence_relevance_note=note_values["coherence_relevance_note"],
        self_correction_note=note_values["self_correction_note"],
        target_vocab_used=_normalize_string_list(payload.get("target_vocab_used")) or list(fallback_used),
        target_vocab_missed=_normalize_string_list(payload.get("target_vocab_missed")) or list(fallback_missed),
        recommended_next_focus=_clean_assessment_text(
            payload.get("recommended_next_focus"),
            fallback="Force one narrower speaking target in the next session instead of repeating the same easy task.",
            field_name="recommended_next_focus",
        ),
    )


async def _build_llm_assessment(
    *,
    session_id: int,
    transcript_text: str,
    session_context: dict | None,
    fallback_used: list[str],
    fallback_missed: list[str],
    transcript_segments: list[dict],
) -> VoiceAssessment | None:
    payload = {
        "session_context": {
            "session": dict((session_context or {}).get("session") or {}),
            "scenario": dict((session_context or {}).get("scenario") or {}),
            "prep_pack": dict((session_context or {}).get("prep_pack") or {}),
        },
        "transcript_stats": {
            "segment_count": len(transcript_segments),
            "character_count": len(transcript_text),
        },
        "transcript": transcript_text,
    }
    raw = await llm_execute(
        task_name="voice_session_assessment",
        system_instruction_key="voice_session_assessment",
        user_message=json.dumps(payload, ensure_ascii=False),
        poll_interval_seconds=1.0,
        responses_timeout_seconds=25.0,
    )
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
    return _assessment_from_payload(
        session_id=int(session_id),
        payload=parsed,
        fallback_used=fallback_used,
        fallback_missed=fallback_missed,
    )


def get_stored_voice_assessment(*, session_id: int) -> VoiceAssessment | None:
    payload = get_voice_session_assessment(int(session_id))
    if not payload:
        return None
    return VoiceAssessment(
        session_id=int(payload["session_id"]),
        summary=str(payload.get("summary") or ""),
        strict_feedback=str(payload.get("strict_feedback") or ""),
        lexical_range_note=str(payload.get("lexical_range_note") or ""),
        grammar_control_note=str(payload.get("grammar_control_note") or ""),
        fluency_note=str(payload.get("fluency_note") or ""),
        coherence_relevance_note=str(payload.get("coherence_relevance_note") or ""),
        self_correction_note=str(payload.get("self_correction_note") or ""),
        target_vocab_used=list(payload.get("target_vocab_used") or []),
        target_vocab_missed=list(payload.get("target_vocab_missed") or []),
        recommended_next_focus=payload.get("recommended_next_focus"),
        created_at=payload.get("created_at"),
        updated_at=payload.get("updated_at"),
    )


def load_voice_assessment(*, session_id: int) -> VoiceAssessment | None:
    """Load one stored assessment for a completed voice session."""

    return get_stored_voice_assessment(session_id=int(session_id))


async def build_voice_assessment(*, session_id: int) -> VoiceAssessment | None:
    """Build a minimal qualitative voice assessment from transcript plus context."""

    transcript_segments = fetch_agent_voice_transcript_segments(session_id=int(session_id))
    session_context = get_agent_voice_session_context(int(session_id))
    transcript_text = _build_transcript_text(transcript_segments)
    prep_pack = dict((session_context or {}).get("prep_pack") or {})
    fallback_used, fallback_missed = _compute_target_vocab_heuristics(
        transcript_text=transcript_text,
        prep_pack=prep_pack,
    )

    if not transcript_text:
        return _build_short_transcript_fallback(
            session_id=int(session_id),
            transcript_text="",
            prep_pack=prep_pack,
        )

    if (
        len(transcript_segments) < _SHORT_TRANSCRIPT_SEGMENT_THRESHOLD
        or len(transcript_text) < _SHORT_TRANSCRIPT_CHAR_THRESHOLD
    ):
        return _build_short_transcript_fallback(
            session_id=int(session_id),
            transcript_text=transcript_text,
            prep_pack=prep_pack,
        )

    try:
        assessment = await _build_llm_assessment(
            session_id=int(session_id),
            transcript_text=transcript_text,
            session_context=session_context,
            fallback_used=fallback_used,
            fallback_missed=fallback_missed,
            transcript_segments=transcript_segments,
        )
        if assessment:
            return assessment
    except Exception as exc:
        logging.warning(
            "Voice assessment LLM pass failed for session_id=%s: %s",
            int(session_id),
            exc,
        )

    return VoiceAssessment(
        session_id=int(session_id),
        summary="A full structured assessment could not be generated, but the session transcript was stored.",
        strict_feedback="Use the stored transcript as review material and rerun the assessment path later if needed.",
        lexical_range_note="Assessment generation fallback was used; lexical detail is limited.",
        grammar_control_note="Assessment generation fallback was used; grammar detail is limited.",
        fluency_note="Assessment generation fallback was used; fluency detail is limited.",
        coherence_relevance_note="Assessment generation fallback was used; coherence detail is limited.",
        self_correction_note="Assessment generation fallback was used; self-correction detail is limited.",
        target_vocab_used=fallback_used,
        target_vocab_missed=fallback_missed,
        recommended_next_focus="Review the transcript manually or rerun assessment generation later.",
    )


def store_voice_assessment(assessment: VoiceAssessment) -> VoiceAssessment | None:
    """Persist one qualitative assessment row for a voice session."""

    payload = upsert_voice_session_assessment(
        session_id=int(assessment.session_id),
        summary=assessment.summary,
        strict_feedback=assessment.strict_feedback,
        lexical_range_note=assessment.lexical_range_note,
        grammar_control_note=assessment.grammar_control_note,
        fluency_note=assessment.fluency_note,
        coherence_relevance_note=assessment.coherence_relevance_note,
        self_correction_note=assessment.self_correction_note,
        target_vocab_used=assessment.target_vocab_used,
        target_vocab_missed=assessment.target_vocab_missed,
        recommended_next_focus=assessment.recommended_next_focus,
    )
    if not payload:
        return None
    return get_stored_voice_assessment(session_id=int(assessment.session_id))


async def build_and_store_voice_assessment(*, session_id: int) -> VoiceAssessment | None:
    """Build and persist a best-effort assessment for one completed voice session."""

    assessment = await build_voice_assessment(session_id=int(session_id))
    if not assessment:
        return None
    return store_voice_assessment(assessment)
