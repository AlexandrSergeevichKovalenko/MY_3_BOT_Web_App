"""Minimal voice preparation service.

This module provides the narrow persistence helpers needed to create and load
voice prep packs for runtime context loading. It is intentionally not connected
to scheduler or delivery flows.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

try:
    from backend.database import (
        create_voice_prep_pack as db_create_voice_prep_pack,
        get_voice_prep_pack as db_get_voice_prep_pack,
        get_voice_scenario as db_get_voice_scenario,
    )
except Exception:
    from database import (  # type: ignore
        create_voice_prep_pack as db_create_voice_prep_pack,
        get_voice_prep_pack as db_get_voice_prep_pack,
        get_voice_scenario as db_get_voice_scenario,
    )


@dataclass(slots=True)
class VoicePrepPack:
    """Minimal persisted prep-pack structure."""

    prep_pack_id: int | None
    user_id: int
    scenario_id: int | None = None
    custom_topic_text: str | None = None
    target_vocab: list[str] = field(default_factory=list)
    target_expressions: list[str] = field(default_factory=list)
    created_at: str | None = None
    updated_at: str | None = None


def _normalize_text_list(value: Any) -> list[str]:
    if value is None:
        return []
    raw_items: list[Any]
    if isinstance(value, str):
        raw_items = [part for chunk in value.splitlines() for part in chunk.split(",")]
    elif isinstance(value, (list, tuple, set)):
        raw_items = list(value)
    else:
        raise ValueError("список должен быть массивом строк или строкой")

    normalized: list[str] = []
    seen: set[str] = set()
    for item in raw_items:
        text = str(item or "").strip()
        if not text:
            continue
        if len(text) > 120:
            raise ValueError("элемент списка слишком длинный")
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        normalized.append(text)
    if len(normalized) > 128:
        raise ValueError("слишком много элементов в списке")
    return normalized


def _build_voice_prep_pack(payload: dict[str, Any] | None) -> VoicePrepPack | None:
    if not payload:
        return None
    return VoicePrepPack(
        prep_pack_id=int(payload["prep_pack_id"]),
        user_id=int(payload["user_id"]),
        scenario_id=int(payload["scenario_id"]) if payload.get("scenario_id") is not None else None,
        custom_topic_text=payload.get("custom_topic_text"),
        target_vocab=list(payload.get("target_vocab") or []),
        target_expressions=list(payload.get("target_expressions") or []),
        created_at=payload.get("created_at"),
        updated_at=payload.get("updated_at"),
    )


def create_voice_prep_pack(
    *,
    user_id: int,
    scenario_id: int | None = None,
    custom_topic_text: str | None = None,
    target_vocab: list[str] | None = None,
    target_expressions: list[str] | None = None,
) -> VoicePrepPack | None:
    """Create a minimal persisted prep pack."""

    normalized_user_id = int(user_id)
    if normalized_user_id <= 0:
        raise ValueError("user_id должен быть положительным числом")

    normalized_scenario_id = int(scenario_id) if scenario_id is not None else None
    if normalized_scenario_id is not None:
        if normalized_scenario_id <= 0:
            raise ValueError("scenario_id должен быть положительным числом")
        if not db_get_voice_scenario(normalized_scenario_id):
            raise ValueError("scenario_id не найден")

    normalized_custom_topic_text = str(custom_topic_text or "").strip() or None
    if normalized_custom_topic_text and len(normalized_custom_topic_text) > 500:
        raise ValueError("custom_topic_text слишком длинный")

    normalized_target_vocab = _normalize_text_list(target_vocab)
    normalized_target_expressions = _normalize_text_list(target_expressions)
    if (
        normalized_scenario_id is None
        and not normalized_custom_topic_text
        and not normalized_target_vocab
        and not normalized_target_expressions
    ):
        raise ValueError("prep pack должен содержать хотя бы scenario_id, custom_topic_text или target lists")

    return _build_voice_prep_pack(
        db_create_voice_prep_pack(
            user_id=normalized_user_id,
            scenario_id=normalized_scenario_id,
            custom_topic_text=normalized_custom_topic_text,
            target_vocab=normalized_target_vocab,
            target_expressions=normalized_target_expressions,
        )
    )


def get_voice_prep_pack(prep_pack_id: int) -> VoicePrepPack | None:
    """Load one persisted prep pack by id."""

    normalized_prep_pack_id = int(prep_pack_id)
    if normalized_prep_pack_id <= 0:
        return None
    return _build_voice_prep_pack(db_get_voice_prep_pack(normalized_prep_pack_id))
