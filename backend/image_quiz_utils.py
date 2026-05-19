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


def _normalize_image_quiz_answer_key(value: str | None) -> str:
    normalized = normalize_image_quiz_option_text(value).casefold()
    normalized = re.sub(r"[„“\"'`´.,;:!?()\\[\\]{}]", "", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    parts = normalized.split(" ", 1)
    if len(parts) == 2 and parts[0] in {
        "der", "die", "das", "den", "dem", "des",
        "ein", "eine", "einen", "einem", "einer", "eines",
    }:
        normalized = parts[1].strip()
    return normalized


def _expected_image_quiz_answer_text(template: Mapping[str, Any] | None) -> str:
    if not isinstance(template, Mapping):
        return ""
    source_lang = _normalize_space(template.get("source_lang")).casefold()
    target_lang = _normalize_space(template.get("target_lang")).casefold()
    source_text = _normalize_space(template.get("source_text"))
    target_text = _normalize_space(template.get("target_text"))
    if target_lang == "de" and target_text:
        return target_text
    if source_lang == "de" and source_text:
        return source_text
    return target_text or source_text


def is_valid_german_image_quiz_option(option_text: str | None) -> bool:
    normalized = normalize_image_quiz_option_text(option_text)
    if not normalized:
        return False
    return contains_latin_text(normalized) and not contains_cyrillic_text(normalized)


def _is_generic_image_quiz_question(value: str | None) -> bool:
    normalized = re.sub(r"[?!.,:;]+", "", _normalize_space(value)).casefold()
    if not normalized:
        return True
    generic_patterns = {
        "was zeigt das bild",
        "was sieht man auf dem bild",
        "was ist auf dem bild zu sehen",
        "was passt zum bild",
        "welches wort passt zum bild",
        "welcher begriff passt zum bild",
    }
    return normalized in generic_patterns


_NON_VISUAL_RELATION_PATTERNS = (
    "anstelle von",
    "an statt",
    "anstatt",
    "statt",
    "stattdessen",
    "im gegensatz zu",
    "anders als",
    "im unterschied zu",
)


def _is_likely_non_visual_relation_answer(value: str | None) -> bool:
    normalized = _normalize_space(value).casefold()
    if not normalized:
        return False
    return any(
        normalized == pattern or normalized.startswith(f"{pattern} ")
        for pattern in _NON_VISUAL_RELATION_PATTERNS
    )


def _looks_like_sentence(value: str | None) -> bool:
    text = _normalize_space(value)
    if not text:
        return False
    tokens = [part for part in re.split(r"\s+", text) if part]
    return len(tokens) >= 4 or bool(re.search(r"[.!?]", text))


def validate_ready_image_quiz_template(template: Mapping[str, Any] | None) -> str | None:
    payload = build_image_quiz_feedback_payload(template)
    if not payload:
        return "invalid_feedback_payload"
    correct_text = normalize_image_quiz_option_text(payload.get("correct_text"))
    if _is_likely_non_visual_relation_answer(correct_text):
        return "non_visual_relation_answer"
    options = [normalize_image_quiz_option_text(item) for item in (payload.get("options") or [])]
    if len(options) != 4:
        return "invalid_options"
    sentence_flags = [_looks_like_sentence(option) for option in options]
    if len(set(sentence_flags)) > 1:
        return "mixed_answer_shapes"
    correct_option_id = int(payload.get("correct_option_id") or 0)
    if correct_option_id < 0 or correct_option_id >= len(sentence_flags):
        return "invalid_correct_option_id"
    if sentence_flags[correct_option_id] != _looks_like_sentence(correct_text):
        return "correct_answer_shape_mismatch"
    question_de = _normalize_space(payload.get("question_de"))
    if _is_generic_image_quiz_question(question_de):
        return "generic_question"
    return None


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
    expected_answer = _expected_image_quiz_answer_text(template)
    expected_key = _normalize_image_quiz_answer_key(expected_answer)
    actual_key = _normalize_image_quiz_answer_key(options[correct_option_id])
    if is_valid_german_image_quiz_option(expected_answer) and expected_key and actual_key and expected_key != actual_key:
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
