"""Minimal voice session service.

This module loads the existing voice-session envelope together with optional
scenario and prep-pack runtime context. It also keeps transcript persistence
helpers used by the current LiveKit runtime.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

try:
    from backend.database import (
        append_agent_voice_transcript_segment,
        fetch_agent_voice_transcript_segments,
        get_agent_voice_session_context,
        get_agent_voice_session,
        get_latest_active_agent_voice_session,
    )
    from backend.voice_preparation_service import VoicePrepPack
    from backend.voice_scenario_service import VoiceScenario
except Exception:
    from database import (  # type: ignore
        append_agent_voice_transcript_segment,
        fetch_agent_voice_transcript_segments,
        get_agent_voice_session_context,
        get_agent_voice_session,
        get_latest_active_agent_voice_session,
    )
    from voice_preparation_service import VoicePrepPack  # type: ignore
    from voice_scenario_service import VoiceScenario  # type: ignore


@dataclass(slots=True)
class VoiceSessionContext:
    """Minimal runtime voice-session context."""

    session_id: int | None
    user_id: int
    source_lang: str = "ru"
    target_lang: str = "de"
    scenario_id: int | None = None
    prep_pack_id: int | None = None
    topic_mode: str | None = None
    custom_topic_text: str | None = None
    status: str = "planned"
    started_at: str | None = None
    ended_at: str | None = None
    duration_seconds: int | None = None
    scenario: VoiceScenario | None = None
    prep_pack: VoicePrepPack | None = None


@dataclass(slots=True)
class VoiceTranscriptSegment:
    """Stored transcript segment for one voice session."""

    segment_id: int
    session_id: int
    seq_no: int
    speaker: str
    text: str
    created_at: str | None = None
    metadata: dict[str, Any] | None = None


def create_voice_session_context(*, user_id: int, scenario_id: int | None = None, prep_pack_id: int | None = None) -> VoiceSessionContext:
    """Return an in-memory session context stub for future integration."""

    return VoiceSessionContext(
        session_id=None,
        user_id=int(user_id),
        scenario_id=scenario_id,
        prep_pack_id=prep_pack_id,
        status="planned",
    )


def _build_voice_session_context(payload: dict[str, Any] | None) -> VoiceSessionContext | None:
    if not payload:
        return None
    session = dict(payload.get("session") or {})
    if not session:
        return None
    raw_scenario = payload.get("scenario")
    raw_prep_pack = payload.get("prep_pack")
    scenario = None
    if raw_scenario:
        scenario = VoiceScenario(
            scenario_id=int(raw_scenario["scenario_id"]),
            slug=str(raw_scenario["slug"] or ""),
            title=str(raw_scenario["title"] or ""),
            topic=str(raw_scenario["topic"] or ""),
            level=str(raw_scenario["level"] or "mixed"),
            system_prompt=raw_scenario.get("system_prompt"),
            is_active=bool(raw_scenario.get("is_active", True)),
            created_at=raw_scenario.get("created_at"),
            updated_at=raw_scenario.get("updated_at"),
        )
    prep_pack = None
    if raw_prep_pack:
        prep_pack = VoicePrepPack(
            prep_pack_id=int(raw_prep_pack["prep_pack_id"]),
            user_id=int(raw_prep_pack["user_id"]),
            scenario_id=int(raw_prep_pack["scenario_id"]) if raw_prep_pack.get("scenario_id") is not None else None,
            custom_topic_text=raw_prep_pack.get("custom_topic_text"),
            target_vocab=list(raw_prep_pack.get("target_vocab") or []),
            target_expressions=list(raw_prep_pack.get("target_expressions") or []),
            created_at=raw_prep_pack.get("created_at"),
            updated_at=raw_prep_pack.get("updated_at"),
        )
    return VoiceSessionContext(
        session_id=int(session["session_id"]),
        user_id=int(session["user_id"]),
        source_lang=str(session.get("source_lang") or "ru"),
        target_lang=str(session.get("target_lang") or "de"),
        scenario_id=int(session["scenario_id"]) if session.get("scenario_id") is not None else None,
        prep_pack_id=int(session["prep_pack_id"]) if session.get("prep_pack_id") is not None else None,
        topic_mode=session.get("topic_mode"),
        custom_topic_text=session.get("custom_topic_text"),
        status="completed" if session.get("ended_at") else "active",
        started_at=session.get("started_at"),
        ended_at=session.get("ended_at"),
        duration_seconds=int(session["duration_seconds"]) if session.get("duration_seconds") is not None else None,
        scenario=scenario,
        prep_pack=prep_pack,
    )


def load_voice_session_context(session_id: int) -> VoiceSessionContext | None:
    """Load the persisted voice-session envelope plus optional runtime context."""

    return _build_voice_session_context(get_agent_voice_session_context(int(session_id)))


def load_latest_active_voice_session_context(*, user_id: int) -> VoiceSessionContext | None:
    """Load the latest active voice-session envelope for a user."""

    payload = get_latest_active_agent_voice_session(user_id=int(user_id))
    if not payload:
        return None
    return VoiceSessionContext(
        session_id=int(payload["session_id"]),
        user_id=int(payload["user_id"]),
        source_lang=str(payload.get("source_lang") or "ru"),
        target_lang=str(payload.get("target_lang") or "de"),
        scenario_id=int(payload["scenario_id"]) if payload.get("scenario_id") is not None else None,
        prep_pack_id=int(payload["prep_pack_id"]) if payload.get("prep_pack_id") is not None else None,
        topic_mode=payload.get("topic_mode"),
        custom_topic_text=payload.get("custom_topic_text"),
        status="active",
        started_at=payload.get("started_at"),
    )


def append_transcript_segment(
    *,
    session_id: int,
    speaker: str,
    text: str,
    metadata: dict[str, Any] | None = None,
) -> VoiceTranscriptSegment | None:
    """Persist one transcript segment for a voice session."""

    payload = append_agent_voice_transcript_segment(
        session_id=int(session_id),
        speaker=str(speaker or "").strip().lower(),
        text=str(text or ""),
        metadata=metadata,
    )
    if not payload:
        return None
    return VoiceTranscriptSegment(
        segment_id=int(payload["id"]),
        session_id=int(payload["session_id"]),
        seq_no=int(payload["seq_no"]),
        speaker=str(payload["speaker"] or "unknown"),
        text=str(payload["text"] or ""),
        created_at=payload.get("created_at"),
        metadata=payload.get("metadata"),
    )


def load_transcript_segments(*, session_id: int) -> list[VoiceTranscriptSegment]:
    """Load all stored transcript segments for a voice session."""

    return [
        VoiceTranscriptSegment(
            segment_id=int(item["id"]),
            session_id=int(item["session_id"]),
            seq_no=int(item["seq_no"]),
            speaker=str(item["speaker"] or "unknown"),
            text=str(item["text"] or ""),
            created_at=item.get("created_at"),
            metadata=item.get("metadata"),
        )
        for item in fetch_agent_voice_transcript_segments(session_id=int(session_id))
    ]


def finalize_voice_session(*, session_id: int, ended_reason: str | None = None) -> None:
    """Placeholder for future voice-session finalization."""

    _ = (session_id, ended_reason)
