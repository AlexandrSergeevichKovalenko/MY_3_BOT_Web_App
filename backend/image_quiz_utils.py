from __future__ import annotations

import re
from typing import Any, Mapping


def _normalize_space(value: str | None) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def contains_cyrillic_text(value: str | None) -> bool:
    return bool(re.search(r"[А-Яа-яЁё]", str(value or "")))


def contains_latin_text(value: str | None) -> bool:
    return bool(re.search(r"[A-Za-zÄÖÜäöüß]", str(value or "")))


def normalize_image_quiz_option_text(option_text: str | None) -> str:
    return _normalize_space(option_text)


def is_valid_german_image_quiz_option(option_text: str | None) -> bool:
    normalized = normalize_image_quiz_option_text(option_text)
    if not normalized:
        return False
    return contains_latin_text(normalized) and not contains_cyrillic_text(normalized)


def build_image_quiz_feedback_payload(template: Mapping[str, Any] | None) -> dict | None:
    if not isinstance(template, Mapping):
        return None
    raw_options = template.get("answer_options")
    if not isinstance(raw_options, list) or len(raw_options) != 4:
        return None

    options: list[str] = []
    seen: set[str] = set()
    for item in raw_options:
        option_text = normalize_image_quiz_option_text(item)
        if not is_valid_german_image_quiz_option(option_text):
            return None
        if option_text in seen:
            return None
        seen.add(option_text)
        options.append(option_text)

    try:
        correct_option_id = int(template.get("correct_option_index"))
    except Exception:
        return None
    if correct_option_id < 0 or correct_option_id >= len(options):
        return None

    return {
        "options": options,
        "correct_option_id": correct_option_id,
        "correct_text": options[correct_option_id],
        "question_de": _normalize_space(template.get("question_de")) or "Was zeigt das Bild?",
        "word_ru": _normalize_space(template.get("source_text")) or _normalize_space(template.get("target_text")),
        "explanation": _normalize_space(template.get("explanation")),
    }


def build_image_quiz_feedback_alert(
    *,
    is_correct: bool,
    correct_text: str | None,
    answer_accepted: bool,
    max_chars: int = 180,
) -> str:
    lines = []
    if answer_accepted:
        lines.append("✅ Верно." if is_correct else "❌ Неверно.")
    else:
        lines.append("Ответ уже был принят.")
        lines.append("✅ Верно." if is_correct else "❌ Неверно.")
    normalized_correct = normalize_image_quiz_option_text(correct_text)
    if normalized_correct and not is_correct:
        lines.append(f"Правильно: {normalized_correct}")
    message = " ".join(part for part in lines if part).strip()
    if len(message) <= max_chars:
        return message
    return message[: max_chars - 3].rstrip() + "..."
