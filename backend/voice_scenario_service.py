"""Minimal voice scenario service.

This module provides the narrow persistence helpers needed to create and load
voice scenarios for runtime context loading. It does not implement scenario
selection or step execution.
"""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any

try:
    from backend.database import create_voice_scenario as db_create_voice_scenario, get_voice_scenario as db_get_voice_scenario
except Exception:
    from database import create_voice_scenario as db_create_voice_scenario, get_voice_scenario as db_get_voice_scenario  # type: ignore

VOICE_SCENARIO_LEVELS = {"a1", "a2", "b1", "b2", "c1", "c2", "mixed"}
VOICE_SCENARIO_SLUG_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{1,79}$")


@dataclass(slots=True)
class VoiceScenario:
    """Minimal persisted voice scenario."""

    scenario_id: int | None
    slug: str
    title: str
    topic: str
    level: str
    system_prompt: str | None = None
    is_active: bool = True
    created_at: str | None = None
    updated_at: str | None = None


def _build_voice_scenario(payload: dict[str, Any] | None) -> VoiceScenario | None:
    if not payload:
        return None
    return VoiceScenario(
        scenario_id=int(payload["scenario_id"]),
        slug=str(payload["slug"] or ""),
        title=str(payload["title"] or ""),
        topic=str(payload["topic"] or ""),
        level=str(payload["level"] or "mixed"),
        system_prompt=payload.get("system_prompt"),
        is_active=bool(payload.get("is_active", True)),
        created_at=payload.get("created_at"),
        updated_at=payload.get("updated_at"),
    )


def create_voice_scenario(
    *,
    slug: str,
    title: str,
    topic: str,
    level: str | None = None,
    system_prompt: str | None = None,
    is_active: bool = True,
) -> VoiceScenario | None:
    """Create a minimal persisted voice scenario."""

    normalized_slug = str(slug or "").strip().lower()
    normalized_title = str(title or "").strip()
    normalized_topic = str(topic or "").strip()
    normalized_level = str(level or "mixed").strip().lower() or "mixed"
    normalized_system_prompt = str(system_prompt or "").strip()

    if not normalized_slug:
        raise ValueError("slug обязателен")
    if not VOICE_SCENARIO_SLUG_RE.match(normalized_slug):
        raise ValueError("slug должен содержать только a-z, 0-9, _ и -")
    if not normalized_title:
        raise ValueError("title обязателен")
    if len(normalized_title) > 200:
        raise ValueError("title слишком длинный")
    if not normalized_topic:
        raise ValueError("topic обязателен")
    if len(normalized_topic) > 200:
        raise ValueError("topic слишком длинный")
    if not normalized_system_prompt:
        raise ValueError("system_prompt обязателен")
    if len(normalized_system_prompt) > 8000:
        raise ValueError("system_prompt слишком длинный")
    if normalized_level not in VOICE_SCENARIO_LEVELS:
        raise ValueError("level должен быть одним из: a1, a2, b1, b2, c1, c2, mixed")

    return _build_voice_scenario(
        db_create_voice_scenario(
            slug=normalized_slug,
            title=normalized_title,
            topic=normalized_topic,
            level=normalized_level,
            system_prompt=normalized_system_prompt,
            is_active=is_active,
        )
    )


def get_voice_scenario(scenario_id: int) -> VoiceScenario | None:
    """Load one persisted voice scenario by id."""

    normalized_scenario_id = int(scenario_id)
    if normalized_scenario_id <= 0:
        return None
    return _build_voice_scenario(db_get_voice_scenario(normalized_scenario_id))
