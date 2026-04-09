"""Minimal conservative bridge from stored voice assessments into skills.

This bridge intentionally uses only a very narrow subset of assessment signals.
It does not attempt to model full speaking mastery. The goal is to convert a
small number of explicit word-order weaknesses into low-impact existing skill
events, while staying safe under repeated session-complete calls.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import datetime

try:
    from backend.database import (
        apply_user_skill_event,
        claim_voice_session_assessment_for_skill_bridge,
        get_agent_voice_session,
        get_skill_by_id,
        get_voice_session_assessment,
        set_voice_session_assessment_skill_bridge_status,
    )
except Exception:
    from database import (  # type: ignore
        apply_user_skill_event,
        claim_voice_session_assessment_for_skill_bridge,
        get_agent_voice_session,
        get_skill_by_id,
        get_voice_session_assessment,
        set_voice_session_assessment_skill_bridge_status,
    )


_WORD_ORDER_MAIN_SKILL_ID = "word_order_v2_rule"
_WORD_ORDER_SUBORDINATE_SKILL_ID = "word_order_subordinate_clause"
_VOICE_BRIDGE_FAIL_DELTA = -0.75
_MAIN_CUES = (
    "word order",
    "verb-second",
    "verb second",
    "v2",
    "main clause",
    "inversion",
    "verb placement",
)
_SUBORDINATE_CUES = (
    "subordinate clause",
    "subordinate clauses",
    "verb-final",
    "verb final",
    "verb at the end",
    "nebensatz",
)
_NEGATIVE_CUES = (
    "weak",
    "wrong",
    "incorrect",
    "error",
    "errors",
    "unstable",
    "shaky",
    "broken",
    "drifted",
    "problem",
    "problems",
    "needs",
    "must",
    "missing",
    "collapsed",
    "inconsistent",
)


@dataclass(slots=True)
class VoiceSkillBridgeResult:
    """Minimal result envelope for one bridge attempt."""

    session_id: int
    applied: bool
    notes: str = ""
    skill_updates: list[dict] = field(default_factory=list)


def _normalize_text(value: object) -> str:
    text = str(value or "").casefold()
    text = re.sub(r"[^\w\s-]+", " ", text, flags=re.UNICODE)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _contains_any(text: str, cues: tuple[str, ...]) -> bool:
    normalized = _normalize_text(text)
    return any(cue in normalized for cue in cues)


def _pick_word_order_skill(*, assessment: dict) -> tuple[str | None, str | None, list[dict]]:
    grammar_note = str(assessment.get("grammar_control_note") or "")
    strict_feedback = str(assessment.get("strict_feedback") or "")
    recommended_focus = str(assessment.get("recommended_next_focus") or "")
    direct_text = f"{grammar_note}\n{strict_feedback}"
    focus_text = recommended_focus
    signals: list[dict] = []

    if _contains_any(direct_text, _SUBORDINATE_CUES) and _contains_any(direct_text, _NEGATIVE_CUES):
        signals.append({"field": "grammar_or_feedback", "signal": "subordinate_clause_word_order"})
        return (
            _WORD_ORDER_SUBORDINATE_SKILL_ID,
            "Assessment explicitly points to weak subordinate-clause / verb-final order.",
            signals,
        )
    if _contains_any(focus_text, _SUBORDINATE_CUES):
        signals.append({"field": "recommended_next_focus", "signal": "subordinate_clause_word_order"})
        return (
            _WORD_ORDER_SUBORDINATE_SKILL_ID,
            "Recommended next focus explicitly targets subordinate-clause / verb-final order.",
            signals,
        )
    if _contains_any(direct_text, _MAIN_CUES) and _contains_any(direct_text, _NEGATIVE_CUES):
        signals.append({"field": "grammar_or_feedback", "signal": "main_clause_word_order"})
        return (
            _WORD_ORDER_MAIN_SKILL_ID,
            "Assessment explicitly points to weak main-clause word order or verb placement.",
            signals,
        )
    if _contains_any(focus_text, _MAIN_CUES):
        signals.append({"field": "recommended_next_focus", "signal": "main_clause_word_order"})
        return (
            _WORD_ORDER_MAIN_SKILL_ID,
            "Recommended next focus explicitly targets main-clause word order or verb placement.",
            signals,
        )
    return None, None, signals


def build_voice_skill_bridge_payload(*, session_id: int) -> dict:
    """Build one tiny, low-confidence skill event payload when evidence is explicit."""

    session = get_agent_voice_session(int(session_id))
    assessment = get_voice_session_assessment(int(session_id))
    if not session or not assessment:
        return {}

    skill_id, reason, signals = _pick_word_order_skill(assessment=assessment)
    if not skill_id or not reason:
        return {
            "session_id": int(session_id),
            "user_id": int(session.get("user_id") or 0),
            "signals": signals,
            "events": [],
            "notes": "No explicit low-risk voice-to-skill mapping was found.",
        }

    skill = get_skill_by_id(skill_id)
    if not skill or not bool(skill.get("is_active")):
        return {
            "session_id": int(session_id),
            "user_id": int(session.get("user_id") or 0),
            "signals": signals,
            "events": [],
            "notes": f"Mapped skill {skill_id} is missing or inactive.",
        }

    event_at = None
    raw_event_at = session.get("ended_at") or assessment.get("updated_at")
    if raw_event_at:
        try:
            event_at = datetime.fromisoformat(str(raw_event_at).replace("Z", "+00:00"))
        except Exception:
            event_at = None

    return {
        "session_id": int(session_id),
        "user_id": int(session.get("user_id") or 0),
        "source_lang": str(session.get("source_lang") or "ru"),
        "target_lang": str(session.get("target_lang") or "de"),
        "signals": signals,
        "notes": reason,
        "events": [
            {
                "skill_id": skill_id,
                "event_type": "fail",
                "base_delta": _VOICE_BRIDGE_FAIL_DELTA,
                "event_at": event_at,
                "reason": reason,
            }
        ],
    }


def apply_voice_skill_bridge(*, session_id: int) -> VoiceSkillBridgeResult:
    """Apply a very small conservative bridge after assessment persistence."""

    assessment = get_voice_session_assessment(int(session_id))
    if not assessment:
        return VoiceSkillBridgeResult(
            session_id=int(session_id),
            applied=False,
            notes="assessment_missing",
        )

    claimed_assessment = claim_voice_session_assessment_for_skill_bridge(int(session_id))
    if not claimed_assessment:
        existing_status = str(assessment.get("skill_bridge_status") or "pending").strip().lower() or "pending"
        return VoiceSkillBridgeResult(
            session_id=int(session_id),
            applied=False,
            notes=f"bridge_not_claimed:{existing_status}",
        )

    payload = build_voice_skill_bridge_payload(session_id=int(session_id))
    events = list(payload.get("events") or [])
    if not events:
        set_voice_session_assessment_skill_bridge_status(
            int(session_id),
            status="skipped",
            notes={
                "reason": str(payload.get("notes") or "no_low_risk_mapping"),
                "signals": list(payload.get("signals") or []),
            },
        )
        return VoiceSkillBridgeResult(
            session_id=int(session_id),
            applied=False,
            notes=str(payload.get("notes") or "no_low_risk_mapping"),
        )

    user_id = int(payload.get("user_id") or 0)
    source_lang = str(payload.get("source_lang") or "ru")
    target_lang = str(payload.get("target_lang") or "de")
    if user_id <= 0:
        set_voice_session_assessment_skill_bridge_status(
            int(session_id),
            status="failed",
            notes={"reason": "session_user_missing"},
        )
        return VoiceSkillBridgeResult(
            session_id=int(session_id),
            applied=False,
            notes="session_user_missing",
        )

    applied_updates: list[dict] = []
    try:
        for event in events:
            update = apply_user_skill_event(
                user_id=user_id,
                skill_id=str(event["skill_id"]),
                source_lang=source_lang,
                target_lang=target_lang,
                event_type=str(event["event_type"]),
                base_delta=float(event["base_delta"]),
                event_at=event.get("event_at"),
            )
            applied_updates.append(update)
        set_voice_session_assessment_skill_bridge_status(
            int(session_id),
            status="applied",
            notes={
                "reason": str(payload.get("notes") or "applied"),
                "signals": list(payload.get("signals") or []),
                "skill_ids": [str(item.get("skill_id") or "") for item in events if item.get("skill_id")],
            },
        )
        return VoiceSkillBridgeResult(
            session_id=int(session_id),
            applied=True,
            notes=str(payload.get("notes") or "applied"),
            skill_updates=applied_updates,
        )
    except Exception as exc:
        logging.warning(
            "Voice skill bridge failed for session_id=%s: %s",
            int(session_id),
            exc,
        )
        set_voice_session_assessment_skill_bridge_status(
            int(session_id),
            status="failed",
            notes={
                "reason": "bridge_apply_failed",
                "error": str(exc),
                "signals": list(payload.get("signals") or []),
            },
        )
        return VoiceSkillBridgeResult(
            session_id=int(session_id),
            applied=False,
            notes=f"bridge_apply_failed:{exc}",
        )
