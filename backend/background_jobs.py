import asyncio
import hashlib
import json
import logging
import os
import re
import time
from datetime import datetime

import dramatiq

from backend.job_queue import (
    get_translation_check_dispatch_state,
    get_dramatiq_broker,
    set_translation_check_dispatch_state,
    set_translation_check_job_status,
    set_translation_fill_job_status,
    set_youtube_transcript_job_status,
)


dramatiq.set_broker(get_dramatiq_broker())

_TRANSLATION_CHECK_QUEUE_NAME = str(
    os.getenv("TRANSLATION_CHECK_QUEUE_NAME") or "translation_check"
).strip() or "translation_check"
_TRANSLATION_CHECK_COMPLETION_QUEUE_NAME = str(
    os.getenv("TRANSLATION_CHECK_COMPLETION_QUEUE_NAME") or "translation_check_completion"
).strip() or "translation_check_completion"
_PROJECTION_MATERIALIZATION_LIVE_QUEUE_NAME = str(
    os.getenv("PROJECTION_MATERIALIZATION_LIVE_QUEUE_NAME") or "projection_materialization_live"
).strip() or "projection_materialization_live"
_PROJECTION_MATERIALIZATION_BACKFILL_QUEUE_NAME = str(
    os.getenv("PROJECTION_MATERIALIZATION_BACKFILL_QUEUE_NAME") or "projection_materialization_backfill"
).strip() or "projection_materialization_backfill"
_READER_INGEST_QUEUE_NAME = str(
    os.getenv("READER_INGEST_QUEUE_NAME") or "reader_ingest"
).strip() or "reader_ingest"
_IMAGE_QUIZ_PREP_QUEUE_NAME = "image_quiz_prepare"
_IMAGE_QUIZ_RENDER_QUEUE_NAME = "image_quiz_render"
_IMAGE_QUIZ_R2_PREFIX = str(
    os.getenv("IMAGE_QUIZ_R2_PREFIX") or "image_quizzes"
).strip().strip("/") or "image_quizzes"
_IMAGE_QUIZ_RENDERING_STALE_MINUTES = max(
    5,
    int((os.getenv("IMAGE_QUIZ_RENDERING_STALE_MINUTES") or "45").strip() or "45"),
)
_IMAGE_QUIZ_VISUAL_STYLES = (
    {
        "key": "clean_editorial",
        "label": "clean editorial illustration",
        "weight": 35,
        "guidance": "clean editorial illustration, crisp shapes, natural colors, modern educational art direction, highly legible objects and actions",
    },
    {
        "key": "realistic_cinematic",
        "label": "realistic cinematic scene",
        "weight": 25,
        "guidance": "realistic cinematic scene, believable lighting, natural proportions, detailed environment, emotionally neutral clarity",
    },
    {
        "key": "storybook",
        "label": "detailed storybook illustration",
        "weight": 15,
        "guidance": "detailed storybook illustration, warm but clear composition, expressive yet realistic body language, learner-friendly visual storytelling",
    },
    {
        "key": "naturalist_textbook",
        "label": "naturalist textbook illustration",
        "weight": 10,
        "guidance": "naturalist textbook illustration, accurate objects, clear educational composition, minimal ambiguity, reference-like precision",
    },
    {
        "key": "watercolor_detail",
        "label": "detailed watercolor illustration",
        "weight": 5,
        "guidance": "detailed watercolor illustration, controlled edges, readable silhouettes, soft paint texture without losing clarity",
    },
    {
        "key": "colored_pencil",
        "label": "colored pencil illustration",
        "weight": 5,
        "guidance": "colored pencil illustration, visible linework, clean contours, detailed but readable textures, classroom-friendly clarity",
    },
    {
        "key": "ink_and_wash",
        "label": "ink and wash illustration",
        "weight": 5,
        "guidance": "ink and wash illustration, precise contours, selective tonal wash, strong figure-ground separation, unambiguous action",
    },
)


def _normalize_space(value: str | None) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def _coerce_json_object(value: object) -> dict:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            return {}
    return {}


def _looks_like_sentence(value: str | None) -> bool:
    text = _normalize_space(value)
    if not text:
        return False
    tokens = [part for part in re.split(r"\s+", text) if part]
    return len(tokens) >= 4 or bool(re.search(r"[.!?]", text))


def _contains_cyrillic_text(value: str | None) -> bool:
    return bool(re.search(r"[А-Яа-яЁё]", str(value or "")))


def _contains_latin_text(value: str | None) -> bool:
    return bool(re.search(r"[A-Za-zÄÖÜäöüß]", str(value or "")))


def _text_matches_expected_language(value: str | None, language_code: str | None) -> bool:
    text = _normalize_space(value)
    code = str(language_code or "").strip().lower()
    if not text or not code:
        return False
    if code == "de":
        return _contains_latin_text(text) and not _contains_cyrillic_text(text)
    if code == "ru":
        return _contains_cyrillic_text(text) and not _contains_latin_text(text)
    return True


def _answer_language_for_candidate(candidate: dict) -> str:
    source_lang = str(candidate.get("source_lang") or "").strip().lower()
    target_lang = str(candidate.get("target_lang") or "").strip().lower()
    if source_lang == "de":
        return "de"
    if target_lang == "de":
        return "de"
    return target_lang or source_lang or "de"


def _answer_text_for_candidate(candidate: dict) -> str:
    answer_language = _answer_language_for_candidate(candidate)
    source_lang = str(candidate.get("source_lang") or "").strip().lower()
    target_lang = str(candidate.get("target_lang") or "").strip().lower()
    source_text = _normalize_space(candidate.get("source_text"))
    target_text = _normalize_space(candidate.get("target_text"))
    if answer_language == source_lang and source_text:
        return source_text
    if answer_language == target_lang and target_text:
        return target_text
    return target_text or source_text


def _extract_usage_example_sentence(candidate: dict) -> tuple[str, str] | None:
    response_json = _coerce_json_object(candidate.get("response_json"))
    raw_examples = response_json.get("usage_examples")
    if not isinstance(raw_examples, list):
        return None
    answer_language = _answer_language_for_candidate(candidate)
    source_lang = str(candidate.get("source_lang") or "").strip().lower()
    target_lang = str(candidate.get("target_lang") or "").strip().lower()
    for item in raw_examples:
        if isinstance(item, dict):
            if answer_language == source_lang:
                sentence = _normalize_space(item.get("source") or item.get("sentence") or "")
            elif answer_language == target_lang:
                sentence = _normalize_space(item.get("target") or item.get("sentence") or "")
            else:
                sentence = _normalize_space(item.get("target") or item.get("source") or item.get("sentence") or "")
        else:
            sentence = _normalize_space(item)
        if _looks_like_sentence(sentence):
            return sentence, "usage_examples"
    return None


def _extract_sentence_like_saved_entry(candidate: dict) -> tuple[str, str] | None:
    answer_language = _answer_language_for_candidate(candidate)
    source_lang = str(candidate.get("source_lang") or "").strip().lower()
    target_lang = str(candidate.get("target_lang") or "").strip().lower()
    source_text = _normalize_space(candidate.get("source_text"))
    target_text = _normalize_space(candidate.get("target_text"))
    if answer_language == source_lang and _looks_like_sentence(source_text):
        return source_text, "saved_entry"
    if answer_language == target_lang and _looks_like_sentence(target_text):
        return target_text, "saved_entry"
    fallback = target_text if answer_language != source_lang else source_text
    if _looks_like_sentence(fallback):
        return fallback, "saved_entry"
    return None


def _normalize_visual_status(value: str | None) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"valid", "rejected"}:
        return normalized
    return "rejected"


def _image_extension_for_mime_type(mime_type: str | None) -> str:
    normalized = str(mime_type or "").strip().lower()
    if normalized == "image/jpeg":
        return ".jpg"
    if normalized == "image/webp":
        return ".webp"
    return ".png"


def _build_image_quiz_object_key(*, user_id: int, template_id: int, mime_type: str | None) -> str:
    extension = _image_extension_for_mime_type(mime_type)
    date_prefix = time.strftime("%Y/%m", time.gmtime())
    return f"{_IMAGE_QUIZ_R2_PREFIX}/{date_prefix}/{int(user_id)}/{int(template_id)}{extension}"


def _normalize_string_list(value: object) -> list[str]:
    if isinstance(value, list):
        items = value
    elif isinstance(value, tuple):
        items = list(value)
    elif isinstance(value, str):
        items = re.split(r"[;\n]+", value)
    else:
        return []
    normalized: list[str] = []
    for item in items:
        text = _normalize_space(item)
        if text:
            normalized.append(text)
    return normalized


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


def _select_image_quiz_visual_style(
    *,
    template_id: int,
    correct_answer: str,
    source_sentence: str,
) -> dict:
    safe_styles = [dict(item) for item in _IMAGE_QUIZ_VISUAL_STYLES if int(item.get("weight") or 0) > 0]
    if not safe_styles:
        raise RuntimeError("image_quiz_visual_styles_missing")
    seed = f"{int(template_id)}|{_normalize_space(correct_answer)}|{_normalize_space(source_sentence)}"
    digest = hashlib.sha1(seed.encode("utf-8")).hexdigest()
    bucket = int(digest[:8], 16) % sum(int(item["weight"]) for item in safe_styles)
    cumulative = 0
    for item in safe_styles:
        cumulative += int(item["weight"])
        if bucket < cumulative:
            return item
    return safe_styles[0]


def _build_image_quiz_style_catalog() -> list[dict]:
    return [
        {
            "key": str(item.get("key") or "").strip(),
            "label": str(item.get("label") or "").strip(),
            "guidance": str(item.get("guidance") or "").strip(),
        }
        for item in _IMAGE_QUIZ_VISUAL_STYLES
    ]


def _compose_image_quiz_render_prompt(
    *,
    source_sentence: str,
    correct_answer: str,
    answer_language: str,
    visual_style: dict,
    blueprint: dict,
) -> str:
    image_prompt = _normalize_space(blueprint.get("image_prompt"))
    scene_core = _normalize_space(blueprint.get("scene_core"))
    camera_framing = _normalize_space(blueprint.get("camera_framing")) or (
        "medium shot or wide shot, main action centered, important object unobstructed"
    )
    key_disambiguator = _normalize_space(blueprint.get("key_disambiguator"))
    must_show = _normalize_string_list(blueprint.get("must_show"))
    must_not_show = _normalize_string_list(blueprint.get("must_not_show"))

    prompt_lines = [
        f"Create one highly detailed, visually unambiguous image for a Telegram language-learning quiz in {answer_language}.",
        f"Target answer: {correct_answer}.",
        f"Sentence to depict: {source_sentence}.",
        f"Visual style: {str(visual_style.get('label') or '').strip()}.",
        f"Style guidance: {str(visual_style.get('guidance') or '').strip()}.",
        "Show exactly one real-world scene with one clear central action.",
        "The intended answer must be the single best label for the scene at first glance.",
        "No symbolism, no surrealism, no metaphorical interpretation, no split-screen, no collage, no text overlay, no multiple competing actions.",
        "Use clear lighting, readable composition, realistic spatial relationships, and strong separation between the main subject and background.",
        "The main subject, action, and decisive object must be fully visible and easy to identify.",
        f"Camera framing: {camera_framing}.",
    ]
    if scene_core:
        prompt_lines.append(f"Scene core: {scene_core}.")
    if image_prompt:
        prompt_lines.append(f"Scene details to include: {image_prompt}.")
    if must_show:
        prompt_lines.append(f"Must show: {'; '.join(must_show)}.")
    if must_not_show:
        prompt_lines.append(f"Must not show: {'; '.join(must_not_show)}.")
    if key_disambiguator:
        prompt_lines.append(f"Key disambiguator: {key_disambiguator}.")
    prompt_lines.append(
        "Prefer concrete, learner-friendly detail that reinforces the intended meaning without introducing irrelevant props or secondary stories."
    )
    return "\n".join(line for line in prompt_lines if _normalize_space(line))


_IMAGE_QUIZ_PERSON_NOUNS = {
    "mann",
    "frau",
    "kind",
    "person",
    "mensch",
    "junge",
    "mädchen",
    "maedchen",
    "fahrer",
    "fahrerin",
    "schüler",
    "schueler",
    "schülerin",
    "schuelerin",
}

_IMAGE_QUIZ_ABSTRACT_TRAIT_WORDS = {
    "gesetzestreu",
    "gesetzeswidrig",
    "gehorsam",
    "ungehorsam",
    "höflich",
    "hoeflich",
    "unhöflich",
    "unhoeflich",
    "mutig",
    "feige",
    "ehrlich",
    "unehrlich",
    "fleißig",
    "fleissig",
    "faul",
    "geduldig",
    "ungeduldig",
    "vorsichtig",
    "rücksichtsvoll",
    "ruecksichtsvoll",
    "verantwortungsvoll",
    "verantwortungslos",
    "freundlich",
    "unfreundlich",
    "hilfsbereit",
    "zuverlässig",
    "zuverlaessig",
    "egoistisch",
    "tolerant",
    "intolerant",
    "diszipliniert",
    "ordentlich",
    "tapfer",
    "loyal",
    "pünktlich",
    "puenktlich",
}


def _is_likely_abstract_person_label(value: str | None) -> bool:
    normalized = _normalize_space(value).casefold()
    if not normalized:
        return False
    tokens = [token for token in re.split(r"[^a-zA-Zäöüß]+", normalized) if token]
    if not tokens:
        return False
    has_person_noun = any(token in _IMAGE_QUIZ_PERSON_NOUNS for token in tokens)
    has_trait_word = any(
        token == trait or token.startswith(f"{trait}e") or token.startswith(f"{trait}en")
        for token in tokens
        for trait in _IMAGE_QUIZ_ABSTRACT_TRAIT_WORDS
    )
    return has_person_noun and has_trait_word


_IMAGE_QUIZ_NON_VISUAL_RELATION_PATTERNS = (
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
        for pattern in _IMAGE_QUIZ_NON_VISUAL_RELATION_PATTERNS
    )


def _sanitize_image_quiz_blueprint(
    payload: dict,
    *,
    expected_correct_answer: str,
    answer_language: str,
) -> dict:
    source_sentence = _normalize_space(payload.get("source_sentence"))
    image_prompt = _normalize_space(payload.get("image_prompt"))
    question_de = _normalize_space(payload.get("question_de"))
    explanation = _normalize_space(payload.get("explanation"))
    scene_core = _normalize_space(payload.get("scene_core"))
    camera_framing = _normalize_space(payload.get("camera_framing"))
    key_disambiguator = _normalize_space(payload.get("key_disambiguator"))
    must_show = _normalize_string_list(payload.get("must_show"))
    must_not_show = _normalize_string_list(payload.get("must_not_show"))
    raw_options = payload.get("answer_options")
    if not source_sentence:
        raise ValueError("blueprint_missing_source_sentence")
    if not image_prompt:
        raise ValueError("blueprint_missing_image_prompt")
    if not isinstance(raw_options, list):
        raise ValueError("blueprint_options_invalid")
    options: list[str] = []
    for item in raw_options:
        value = _normalize_space(item)
        if value:
            options.append(value)
    if len(options) != 4 or len(set(options)) != 4:
        raise ValueError("blueprint_options_invalid")
    try:
        correct_index = int(payload.get("correct_option_index"))
    except Exception as exc:
        raise ValueError("blueprint_correct_index_invalid") from exc
    if correct_index < 0 or correct_index >= len(options):
        raise ValueError("blueprint_correct_index_invalid")
    normalized_expected = _normalize_space(expected_correct_answer)
    if not normalized_expected:
        raise ValueError("expected_correct_answer_missing")
    if _is_likely_abstract_person_label(normalized_expected):
        raise ValueError("blueprint_abstract_person_label_unsupported")
    if _is_likely_non_visual_relation_answer(normalized_expected):
        raise ValueError("blueprint_non_visual_relation_answer")
    options[correct_index] = normalized_expected
    if len(set(options)) != 4:
        raise ValueError("blueprint_options_conflict_with_correct_answer")
    option_sentence_flags = [_looks_like_sentence(option) for option in options]
    if len(set(option_sentence_flags)) > 1:
        raise ValueError("blueprint_options_mixed_answer_shapes")
    if option_sentence_flags[correct_index] != _looks_like_sentence(normalized_expected):
        raise ValueError("blueprint_correct_answer_shape_mismatch")
    if not _text_matches_expected_language(source_sentence, answer_language):
        raise ValueError("blueprint_source_sentence_language_invalid")
    if any(not _text_matches_expected_language(option, answer_language) for option in options):
        raise ValueError("blueprint_options_language_invalid")
    if not _text_matches_expected_language(question_de, "de") or _is_generic_image_quiz_question(question_de):
        raise ValueError("blueprint_question_invalid")
    if not must_show:
        raise ValueError("blueprint_must_show_missing")
    if not key_disambiguator:
        raise ValueError("blueprint_key_disambiguator_missing")
    return {
        "source_sentence": source_sentence,
        "image_prompt": image_prompt,
        "question_de": question_de,
        "answer_options": options,
        "correct_option_index": correct_index,
        "explanation": explanation,
        "scene_core": scene_core,
        "camera_framing": camera_framing,
        "key_disambiguator": key_disambiguator,
        "must_show": must_show,
        "must_not_show": must_not_show,
    }


async def _prepare_single_image_quiz_template_async(
    *,
    user_id: int,
    source_lang: str,
    target_lang: str,
) -> dict:
    from backend.database import (
        claim_image_quiz_template_candidate,
        mark_image_quiz_template_failed,
        mark_image_quiz_template_visual_status,
        store_image_quiz_template_blueprint,
    )
    from backend.openai_manager import (
        run_image_quiz_blueprint,
        run_image_quiz_sentence_fallback,
        run_image_quiz_visual_screen,
    )

    claimed = claim_image_quiz_template_candidate(
        user_id=int(user_id),
        source_lang=source_lang,
        target_lang=target_lang,
    )
    if not claimed:
        return {
            "status": "no_candidate",
            "template_id": None,
            "entry_id": None,
        }

    template = claimed.get("template") or {}
    candidate = claimed.get("candidate") or {}
    template_id = int(template.get("id") or 0)
    entry_id = int(candidate.get("entry_id") or 0) if candidate.get("entry_id") is not None else None
    answer_language = _answer_language_for_candidate(candidate)
    correct_answer = _answer_text_for_candidate(candidate)
    if not template_id or not correct_answer:
        if template_id:
            mark_image_quiz_template_failed(
                template_id,
                last_error="image_quiz_candidate_invalid",
                provider_name="image_quiz_prepare",
                provider_meta={"candidate_entry_id": entry_id},
            )
        return {
            "status": "failed",
            "template_id": template_id or None,
            "entry_id": entry_id,
            "reason": "candidate_invalid",
        }

    selected_sentence: str | None = None
    sentence_source = "none"
    usage_sentence = _extract_usage_example_sentence(candidate)
    if usage_sentence:
        selected_sentence, sentence_source = usage_sentence
    else:
        saved_sentence = _extract_sentence_like_saved_entry(candidate)
        if saved_sentence:
            selected_sentence, sentence_source = saved_sentence

    if not selected_sentence:
        fallback_payload = await run_image_quiz_sentence_fallback(
            {
                "source_language": str(source_lang or "").strip().lower() or "ru",
                "target_language": str(target_lang or "").strip().lower() or "de",
                "source_text": str(candidate.get("source_text") or ""),
                "target_text": str(candidate.get("target_text") or ""),
                "answer_language": answer_language,
                "usage_hint": correct_answer,
            }
        )
        fallback_visual_status = _normalize_visual_status(fallback_payload.get("visual_status"))
        fallback_sentence = _normalize_space(fallback_payload.get("source_sentence"))
        if fallback_visual_status != "valid" or not _looks_like_sentence(fallback_sentence):
            reason = _normalize_space(fallback_payload.get("reason")) or "sentence_fallback_rejected"
            mark_image_quiz_template_failed(
                template_id,
                last_error=reason,
                provider_name="image_quiz_prepare",
                provider_meta={
                    "candidate_entry_id": entry_id,
                    "sentence_fallback": fallback_payload if isinstance(fallback_payload, dict) else {},
                },
            )
            return {
                "status": "failed",
                "template_id": template_id,
                "entry_id": entry_id,
                "reason": reason,
            }
        selected_sentence = fallback_sentence
        sentence_source = "llm_fallback"

    visual_payload = await run_image_quiz_visual_screen(
        {
            "answer_language": answer_language,
            "answer_text": correct_answer,
            "source_text": str(candidate.get("source_text") or ""),
            "target_text": str(candidate.get("target_text") or ""),
            "source_sentence": selected_sentence,
        }
    )
    visual_status = _normalize_visual_status(visual_payload.get("visual_status"))
    visual_reason = _normalize_space(visual_payload.get("reason")) or "visual_rejected"
    if visual_status != "valid":
        mark_image_quiz_template_failed(
            template_id,
            last_error=visual_reason,
            visual_status="rejected",
            provider_name="image_quiz_prepare",
            provider_meta={
                "candidate_entry_id": entry_id,
                "sentence_source": sentence_source,
                "visual_screen": visual_payload if isinstance(visual_payload, dict) else {},
            },
        )
        return {
            "status": "rejected",
            "template_id": template_id,
            "entry_id": entry_id,
            "reason": visual_reason,
        }

    mark_image_quiz_template_visual_status(
        template_id,
        visual_status="valid",
        provider_name="image_quiz_prepare",
        provider_meta={
            "candidate_entry_id": entry_id,
            "sentence_source": sentence_source,
            "visual_screen": visual_payload if isinstance(visual_payload, dict) else {},
        },
        last_error="",
    )
    visual_style = _select_image_quiz_visual_style(
        template_id=template_id,
        correct_answer=correct_answer,
        source_sentence=selected_sentence,
    )

    blueprint_payload = await run_image_quiz_blueprint(
        {
            "answer_language": answer_language,
            "source_language": str(source_lang or "").strip().lower() or "ru",
            "target_language": str(target_lang or "").strip().lower() or "de",
            "source_text": str(candidate.get("source_text") or ""),
            "target_text": str(candidate.get("target_text") or ""),
            "source_sentence": selected_sentence,
            "visual_style_key": str(visual_style.get("key") or ""),
            "visual_style_label": str(visual_style.get("label") or ""),
            "visual_style_guidance": str(visual_style.get("guidance") or ""),
            "available_visual_styles": _build_image_quiz_style_catalog(),
        }
    )
    try:
        sanitized_blueprint = _sanitize_image_quiz_blueprint(
            blueprint_payload if isinstance(blueprint_payload, dict) else {},
            expected_correct_answer=correct_answer,
            answer_language=answer_language,
        )
    except Exception as exc:
        mark_image_quiz_template_failed(
            template_id,
            last_error=str(exc),
            visual_status="valid",
            provider_name="image_quiz_prepare",
            provider_meta={
                "candidate_entry_id": entry_id,
                "sentence_source": sentence_source,
                "blueprint": blueprint_payload if isinstance(blueprint_payload, dict) else {},
            },
        )
        return {
            "status": "failed",
            "template_id": template_id,
            "entry_id": entry_id,
            "reason": str(exc),
        }

    final_render_prompt = _compose_image_quiz_render_prompt(
        source_sentence=sanitized_blueprint["source_sentence"],
        correct_answer=correct_answer,
        answer_language=answer_language,
        visual_style=visual_style,
        blueprint=sanitized_blueprint,
    )
    store_image_quiz_template_blueprint(
        template_id,
        source_sentence=sanitized_blueprint["source_sentence"],
        image_prompt=final_render_prompt,
        question_de=sanitized_blueprint["question_de"],
        answer_options=sanitized_blueprint["answer_options"],
        correct_option_index=int(sanitized_blueprint["correct_option_index"]),
        explanation=sanitized_blueprint["explanation"] or None,
        provider_name="image_quiz_prepare",
        provider_meta={
            "candidate_entry_id": entry_id,
            "sentence_source": sentence_source,
            "visual_style": visual_style,
            "scene_blueprint": {
                "scene_core": sanitized_blueprint.get("scene_core") or "",
                "camera_framing": sanitized_blueprint.get("camera_framing") or "",
                "key_disambiguator": sanitized_blueprint.get("key_disambiguator") or "",
                "must_show": sanitized_blueprint.get("must_show") or [],
                "must_not_show": sanitized_blueprint.get("must_not_show") or [],
            },
            "blueprint": blueprint_payload if isinstance(blueprint_payload, dict) else {},
        },
    )
    return {
        "status": "blueprint_ready",
        "template_id": template_id,
        "entry_id": entry_id,
        "sentence_source": sentence_source,
    }


async def _run_image_quiz_template_prepare_job_async(
    *,
    user_id: int,
    source_lang: str,
    target_lang: str,
    requested_count: int,
) -> dict:
    safe_requested_count = max(1, min(int(requested_count or 1), 10))
    prepared = 0
    rejected = 0
    failed = 0
    empty = 0
    items: list[dict] = []
    for _ in range(safe_requested_count):
        item = await _prepare_single_image_quiz_template_async(
            user_id=int(user_id),
            source_lang=str(source_lang or "").strip().lower() or "ru",
            target_lang=str(target_lang or "").strip().lower() or "de",
        )
        items.append(item)
        status = str(item.get("status") or "").strip().lower()
        if status == "blueprint_ready":
            prepared += 1
        elif status == "rejected":
            rejected += 1
        elif status == "no_candidate":
            empty += 1
            break
        else:
            failed += 1
    return {
        "requested_count": safe_requested_count,
        "prepared_count": prepared,
        "rejected_count": rejected,
        "failed_count": failed,
        "empty_count": empty,
        "items": items,
    }


def _render_single_image_quiz_template(
    *,
    user_id: int,
    source_lang: str,
    target_lang: str,
) -> dict:
    from backend.database import (
        claim_next_blueprint_ready_template,
        mark_image_quiz_template_failed,
        mark_image_quiz_template_ready,
    )
    from backend.image_generation_provider import (
        generate_image_bytes,
        get_image_generation_provider_name,
    )
    from backend.r2_storage import r2_public_url, r2_put_bytes

    provider_name = get_image_generation_provider_name()
    claimed = claim_next_blueprint_ready_template(
        user_id=int(user_id),
        source_lang=source_lang,
        target_lang=target_lang,
        provider_name=provider_name,
        provider_meta={"stage": "render_claimed"},
    )
    if not claimed:
        return {
            "status": "no_template",
            "template_id": None,
        }

    template_id = int(claimed.get("id") or 0)
    if template_id <= 0:
        return {
            "status": "no_template",
            "template_id": None,
        }

    try:
        render_result = generate_image_bytes(
            prompt=str(claimed.get("image_prompt") or ""),
            template_id=template_id,
            user_id=int(claimed.get("user_id") or user_id),
        )
        image_bytes = bytes(render_result.get("data") or b"")
        mime_type = str(render_result.get("mime_type") or "image/png").strip().lower() or "image/png"
        if not image_bytes:
            raise RuntimeError("image_generation_empty_payload")
        object_key = _build_image_quiz_object_key(
            user_id=int(claimed.get("user_id") or user_id),
            template_id=template_id,
            mime_type=mime_type,
        )
        r2_put_bytes(
            object_key,
            image_bytes,
            content_type=mime_type,
            cache_control="public, max-age=31536000, immutable",
        )
        public_url = r2_public_url(object_key)
        ready_template = mark_image_quiz_template_ready(
            template_id,
            source_sentence=str(claimed.get("source_sentence") or "").strip() or None,
            image_object_key=object_key,
            image_url=public_url,
            provider_name=str(render_result.get("provider_name") or provider_name).strip() or provider_name,
            provider_meta={
                **(render_result.get("provider_meta") if isinstance(render_result.get("provider_meta"), dict) else {}),
                "storage": {
                    "provider": "cloudflare_r2",
                    "object_key": object_key,
                    "image_url": public_url,
                    "content_type": mime_type,
                    "bytes": len(image_bytes),
                },
            },
            image_prompt=str(claimed.get("image_prompt") or "").strip() or None,
            question_de=str(claimed.get("question_de") or "").strip() or None,
            answer_options=list(claimed.get("answer_options") or []),
            correct_option_index=(
                int(claimed.get("correct_option_index"))
                if claimed.get("correct_option_index") is not None
                else None
            ),
            explanation=str(claimed.get("explanation") or "").strip() or None,
        )
        return {
            "status": "ready",
            "template_id": template_id,
            "image_object_key": object_key,
            "image_url": public_url,
            "template": ready_template,
        }
    except Exception as exc:
        mark_image_quiz_template_failed(
            template_id,
            last_error=str(exc),
            visual_status=str(claimed.get("visual_status") or "").strip().lower() or "valid",
            provider_name=provider_name,
            provider_meta={
                "stage": "render_or_upload_failed",
                "error_type": exc.__class__.__name__,
                "image_prompt_sha1": (
                    hashlib.sha1(str(claimed.get("image_prompt") or "").encode("utf-8")).hexdigest()
                    if str(claimed.get("image_prompt") or "").strip()
                    else None
                ),
            },
        )
        return {
            "status": "failed",
            "template_id": template_id,
            "reason": str(exc),
        }


def _run_image_quiz_template_render_job_sync(
    *,
    user_id: int,
    source_lang: str,
    target_lang: str,
    requested_count: int,
) -> dict:
    from backend.database import fail_stale_rendering_image_quiz_templates
    from backend.image_generation_provider import get_image_generation_provider_name

    safe_requested_count = max(1, min(int(requested_count or 1), 10))
    provider_name = get_image_generation_provider_name()
    recovered = fail_stale_rendering_image_quiz_templates(
        user_id=int(user_id),
        source_lang=str(source_lang or "").strip().lower() or "ru",
        target_lang=str(target_lang or "").strip().lower() or "de",
        stale_after_minutes=_IMAGE_QUIZ_RENDERING_STALE_MINUTES,
        provider_name=provider_name,
        provider_meta={
            "stage": "rendering_stale_recovery",
            "stale_timeout_minutes": _IMAGE_QUIZ_RENDERING_STALE_MINUTES,
        },
    )
    ready = 0
    failed = 0
    empty = 0
    items: list[dict] = []
    for _ in range(safe_requested_count):
        item = _render_single_image_quiz_template(
            user_id=int(user_id),
            source_lang=str(source_lang or "").strip().lower() or "ru",
            target_lang=str(target_lang or "").strip().lower() or "de",
        )
        items.append(item)
        status = str(item.get("status") or "").strip().lower()
        if status == "ready":
            ready += 1
        elif status == "no_template":
            empty += 1
            break
        else:
            failed += 1
    return {
        "requested_count": safe_requested_count,
        "recovered_stale_count": len(recovered),
        "recovered_stale_template_ids": [int(item.get("id") or 0) for item in recovered if item.get("id") is not None],
        "ready_count": ready,
        "failed_count": failed,
        "empty_count": empty,
        "items": items,
    }


@dramatiq.actor(max_retries=0, queue_name="youtube_transcript")
def fetch_youtube_transcript_job(video_id: str, lang: str = "", allow_proxy: bool = True) -> None:
    normalized_video_id = str(video_id or "").strip()
    normalized_lang = str(lang or "").strip().lower()
    set_youtube_transcript_job_status(
        normalized_video_id,
        normalized_lang,
        status="running",
    )
    started_at = time.perf_counter()
    try:
        from backend.backend_server import _fetch_youtube_transcript, _yt_transcript_cache
        from backend.database import upsert_youtube_transcript_cache

        data = _fetch_youtube_transcript(
            normalized_video_id,
            lang=normalized_lang or None,
            allow_proxy=bool(allow_proxy),
        )
        try:
            upsert_youtube_transcript_cache(
                normalized_video_id,
                data.get("items", []),
                data.get("language"),
                data.get("is_generated"),
                data.get("translations"),
            )
        except Exception:
            logging.exception(
                "youtube_transcript_job persist failed video_id=%s lang=%s",
                normalized_video_id,
                normalized_lang,
            )
        _yt_transcript_cache[normalized_video_id] = {"ts": time.time(), "data": data}
        set_youtube_transcript_job_status(
            normalized_video_id,
            normalized_lang,
            status="ready",
            source=str(data.get("source") or "").strip() or None,
            item_count=len(data.get("items") or []),
            fetch_duration_ms=int((time.perf_counter() - started_at) * 1000),
        )
    except Exception as exc:
        logging.exception(
            "youtube_transcript_job failed video_id=%s lang=%s",
            normalized_video_id,
            normalized_lang,
        )
        set_youtube_transcript_job_status(
            normalized_video_id,
            normalized_lang,
            status="failed",
            error=str(exc),
            fetch_duration_ms=int((time.perf_counter() - started_at) * 1000),
        )
        raise


@dramatiq.actor(max_retries=0, queue_name=_TRANSLATION_CHECK_QUEUE_NAME)
def run_translation_check_job(
    session_id: int,
    dispatch_job_id: str | None = None,
    correlation_id: str | None = None,
    request_id: str | None = None,
    accepted_at_ms: int | None = None,
) -> None:
    safe_session_id = int(session_id)
    normalized_dispatch_job_id = str(dispatch_job_id or "").strip() or None
    worker_received_at_ms = int(time.time() * 1000)
    current_dispatch_state = get_translation_check_dispatch_state(safe_session_id) or {}
    current_dispatch_job_id = str(current_dispatch_state.get("worker_job_id") or "").strip() or None
    stale_message = bool(
        normalized_dispatch_job_id
        and current_dispatch_job_id
        and current_dispatch_job_id != normalized_dispatch_job_id
    )
    if stale_message:
        logging.warning(
            "translation_check_dispatch transition=stale_message_received queue=%s session_id=%s worker_job_id=%s active_dispatch_job_id=%s message_id=%s dispatch_generation=%s redispatch_count=%s ts_ms=%s",
            _TRANSLATION_CHECK_QUEUE_NAME,
            safe_session_id,
            normalized_dispatch_job_id,
            current_dispatch_job_id,
            current_dispatch_state.get("message_id"),
            current_dispatch_state.get("dispatch_generation"),
            current_dispatch_state.get("redispatch_count"),
            worker_received_at_ms,
        )
    set_translation_check_dispatch_state(
        safe_session_id,
        {
            "status": current_dispatch_state.get("status") if stale_message else "worker_received",
            "worker_job_id": current_dispatch_job_id if stale_message else normalized_dispatch_job_id,
            "message_id": current_dispatch_state.get("message_id"),
            "queue_name": _TRANSLATION_CHECK_QUEUE_NAME,
            "dispatched_at_ms": current_dispatch_state.get("dispatched_at_ms"),
            "broker_enqueued_at_ms": current_dispatch_state.get("broker_enqueued_at_ms"),
            "worker_received_at_ms": current_dispatch_state.get("worker_received_at_ms") if stale_message else worker_received_at_ms,
            "claimed_at_ms": current_dispatch_state.get("claimed_at_ms"),
            "first_heartbeat_at_ms": current_dispatch_state.get("first_heartbeat_at_ms"),
            "last_heartbeat_ms": current_dispatch_state.get("last_heartbeat_ms"),
            "dispatch_generation": current_dispatch_state.get("dispatch_generation"),
            "redispatch_count": current_dispatch_state.get("redispatch_count"),
            "last_force_dispatch_at_ms": current_dispatch_state.get("last_force_dispatch_at_ms"),
            "runtime_status": current_dispatch_state.get("runtime_status") if stale_message else "queued",
        },
    )
    if not stale_message:
        logging.info(
            "translation_check_dispatch transition=worker_received queue=%s session_id=%s dispatch_job_id=%s message_id=%s dispatch_generation=%s redispatch_count=%s ts_ms=%s",
            _TRANSLATION_CHECK_QUEUE_NAME,
            safe_session_id,
            normalized_dispatch_job_id,
            current_dispatch_state.get("message_id"),
            current_dispatch_state.get("dispatch_generation"),
            current_dispatch_state.get("redispatch_count"),
            worker_received_at_ms,
        )
    try:
        from backend.backend_server import _run_translation_check_session

        _run_translation_check_session(
            session_id=safe_session_id,
            dispatch_job_id=normalized_dispatch_job_id,
            correlation_id=correlation_id,
            request_id=request_id,
            accepted_at_ms=accepted_at_ms,
        )
        set_translation_check_job_status(
            safe_session_id,
            status="ready",
            job_id=normalized_dispatch_job_id,
        )
    except Exception as exc:
        logging.exception("translation_check_job failed session_id=%s", safe_session_id)
        set_translation_check_job_status(
            safe_session_id,
            status="failed",
            job_id=normalized_dispatch_job_id,
            error=str(exc),
        )
        raise


@dramatiq.actor(max_retries=2, queue_name=_TRANSLATION_CHECK_COMPLETION_QUEUE_NAME)
def run_translation_check_completion_job(
    session_id: int,
    correlation_id: str | None = None,
    request_id: str | None = None,
) -> None:
    safe_session_id = int(session_id)
    try:
        from backend.backend_server import _run_translation_check_completion_side_effects

        _run_translation_check_completion_side_effects(
            session_id=safe_session_id,
            correlation_id=correlation_id,
            request_id=request_id,
        )
    except Exception:
        logging.exception(
            "translation_check_completion_job failed session_id=%s request_id=%s correlation_id=%s",
            safe_session_id,
            request_id,
            correlation_id,
        )
        raise


@dramatiq.actor(max_retries=0, queue_name="translation_fill")
def run_translation_fill_job(
    user_id: int,
    username: str | None,
    session_id: int,
    topic: str,
    level: str | None,
    source_lang: str,
    target_lang: str,
    grammar_focus: dict | None = None,
    tested_skill_profile_seed: dict | None = None,
) -> None:
    safe_session_id = int(session_id)
    set_translation_fill_job_status(
        safe_session_id,
        status="running",
    )
    try:
        from backend.backend_server import _run_translation_session_fill
        from backend.translation_workflow import _translation_fill_reached_target

        result = _run_translation_session_fill(
            user_id=int(user_id),
            username=username,
            session_id=safe_session_id,
            topic=topic,
            level=level,
            source_lang=source_lang,
            target_lang=target_lang,
            grammar_focus=grammar_focus,
            tested_skill_profile_seed=tested_skill_profile_seed,
        )
        if _translation_fill_reached_target(result):
            set_translation_fill_job_status(
                safe_session_id,
                status="ready",
            )
            return
        ready_count = int((result or {}).get("ready_count") or 0)
        expected_total = int((result or {}).get("expected_total") or 0)
        error_message = str((result or {}).get("error") or "").strip()
        if not error_message:
            if expected_total > 0:
                error_message = (
                    f"translation_fill_underfilled: ready_count={ready_count}, expected_total={expected_total}"
                )
            else:
                error_message = "translation_fill_incomplete"
        logging.warning(
            "translation_fill_job underfilled session_id=%s ready_count=%s expected_total=%s error=%s",
            safe_session_id,
            ready_count,
            expected_total,
            error_message,
        )
        set_translation_fill_job_status(
            safe_session_id,
            status="failed",
            error=error_message,
        )
    except Exception as exc:
        logging.exception("translation_fill_job failed session_id=%s", safe_session_id)
        set_translation_fill_job_status(
            safe_session_id,
            status="failed",
            error=str(exc),
        )
        raise


@dramatiq.actor(max_retries=0, queue_name="finish_summary")
def run_finish_daily_summary_job(
    user_id: int,
    username: str | None = None,
    user_name: str | None = None,
    request_id: str | None = None,
    correlation_id: str | None = None,
) -> None:
    safe_user_id = int(user_id)
    started_at = time.perf_counter()
    try:
        from backend.translation_workflow import build_user_daily_summary
        from backend.backend_server import (
            _resolve_user_delivery_chat_id,
            _send_group_message,
            _send_private_message,
        )

        summary_started_at = time.perf_counter()
        summary = build_user_daily_summary(user_id=safe_user_id, username=username or user_name)
        daily_summary_ms = int((time.perf_counter() - summary_started_at) * 1000)
        if not summary:
            logging.info(
                "finish_daily_summary_job skipped user_id=%s request_id=%s correlation_id=%s daily_summary_ms=%s reason=no_summary",
                safe_user_id,
                request_id,
                correlation_id,
                daily_summary_ms,
            )
            return

        delivery_started_at = time.perf_counter()
        target_chat_id = _resolve_user_delivery_chat_id(safe_user_id, job_name="finish_webapp_translation_async_summary")
        if int(target_chat_id) < 0:
            delivery_target = "group"
            _send_group_message(summary, chat_id=int(target_chat_id))
        else:
            delivery_target = "private"
            _send_private_message(user_id=int(target_chat_id), text=summary)
        summary_delivery_ms = int((time.perf_counter() - delivery_started_at) * 1000)
        total_ms = int((time.perf_counter() - started_at) * 1000)
        logging.info(
            "finish_daily_summary_job completed user_id=%s request_id=%s correlation_id=%s daily_summary_ms=%s summary_delivery_ms=%s delivery_target=%s total_ms=%s",
            safe_user_id,
            request_id,
            correlation_id,
            daily_summary_ms,
            summary_delivery_ms,
            delivery_target,
            total_ms,
        )
    except Exception:
        logging.exception(
            "finish_daily_summary_job failed user_id=%s request_id=%s correlation_id=%s",
            safe_user_id,
            request_id,
            correlation_id,
        )
        raise


@dramatiq.actor(max_retries=0, queue_name="shortcut_lookup")
def run_shortcut_lookup_job(
    user_id: int,
    text: str,
    request_key: str | None = None,
) -> None:
    safe_user_id = int(user_id or 0)
    normalized_text = str(text or "").strip()
    normalized_request_key = str(request_key or "").strip() or None
    if safe_user_id <= 0 or not normalized_text:
        logging.warning(
            "shortcut_lookup_job skipped invalid_payload user_id=%s request_key=%s has_text=%s",
            safe_user_id,
            normalized_request_key,
            bool(normalized_text),
        )
        return
    started_at = time.perf_counter()
    try:
        from backend.backend_server import _run_shortcut_lookup_delivery

        sent = _run_shortcut_lookup_delivery(
            user_id=safe_user_id,
            text=normalized_text,
        )
        logging.info(
            "shortcut_lookup_job completed user_id=%s request_key=%s sent=%s total_ms=%s",
            safe_user_id,
            normalized_request_key,
            int(sent or 0),
            int((time.perf_counter() - started_at) * 1000),
        )
    except Exception:
        logging.exception(
            "shortcut_lookup_job failed user_id=%s request_key=%s total_ms=%s",
            safe_user_id,
            normalized_request_key,
            int((time.perf_counter() - started_at) * 1000),
        )
        raise


def _run_projection_materialization_job_impl(
    *,
    job_id: int,
    expected_job_source: str,
    queue_name: str,
    request_id: str | None = None,
    correlation_id: str | None = None,
) -> None:
    safe_job_id = int(job_id or 0)
    if safe_job_id <= 0:
        logging.warning(
            "projection_materialization_job skipped invalid job_id=%s job_source=%s queue_name=%s request_id=%s correlation_id=%s",
            job_id,
            expected_job_source,
            queue_name,
            request_id,
            correlation_id,
        )
        return
    started_perf = time.perf_counter()
    lease_token = None
    projection_job = None
    try:
        from backend.database import (
            claim_projection_job,
            complete_projection_job,
            fail_projection_job,
            get_projection_job_status_counts,
        )
        from backend.backend_server import materialize_projection_job_payload

        projection_job = claim_projection_job(
            job_id=safe_job_id,
            expected_job_source=expected_job_source,
            expected_queue_name=queue_name,
        )
        if not projection_job:
            counts = get_projection_job_status_counts(job_source=expected_job_source)
            logging.info(
                "projection_materialization_job no_claim job_id=%s job_source=%s queue_name=%s request_id=%s correlation_id=%s pending=%s running=%s done=%s failed=%s retrying=%s",
                safe_job_id,
                expected_job_source,
                queue_name,
                request_id,
                correlation_id,
                counts.get("pending"),
                counts.get("running"),
                counts.get("done"),
                counts.get("failed"),
                counts.get("retrying"),
            )
            return
        lease_token = str(projection_job.get("lease_token") or "").strip() or None
        enqueued_at = projection_job.get("enqueued_at")
        started_at = projection_job.get("started_at")
        enqueue_to_claim_ms = None
        try:
            if enqueued_at and started_at:
                enqueue_to_claim_ms = max(
                    0,
                    int(
                        (
                            datetime.fromisoformat(str(started_at))
                            - datetime.fromisoformat(str(enqueued_at))
                        ).total_seconds()
                        * 1000
                    ),
                )
        except Exception:
            enqueue_to_claim_ms = None
        counts = get_projection_job_status_counts(job_source=expected_job_source)
        logging.info(
            "projection_materialization_job started job_id=%s job_source=%s queue_name=%s projection_kind=%s user_id=%s enqueued_at=%s started_at=%s enqueue_to_claim_ms=%s attempt_count=%s request_id=%s correlation_id=%s pending=%s running=%s done=%s failed=%s retrying=%s",
            safe_job_id,
            expected_job_source,
            queue_name,
            projection_job.get("projection_kind"),
            projection_job.get("user_id"),
            enqueued_at,
            started_at,
            enqueue_to_claim_ms,
            projection_job.get("attempt_count"),
            request_id,
            correlation_id,
            counts.get("pending"),
            counts.get("running"),
            counts.get("done"),
            counts.get("failed"),
            counts.get("retrying"),
        )
        materialize_projection_job_payload(projection_job)
        completed_job = complete_projection_job(job_id=safe_job_id, lease_token=lease_token or "")
        counts = get_projection_job_status_counts(job_source=expected_job_source)
        logging.info(
            "projection_materialization_job completed job_id=%s job_source=%s queue_name=%s projection_kind=%s user_id=%s enqueued_at=%s started_at=%s completed_at=%s enqueue_to_claim_ms=%s total_ms=%s attempt_count=%s request_id=%s correlation_id=%s pending=%s running=%s done=%s failed=%s retrying=%s",
            safe_job_id,
            expected_job_source,
            queue_name,
            projection_job.get("projection_kind"),
            projection_job.get("user_id"),
            enqueued_at,
            started_at,
            (completed_job or {}).get("completed_at") if isinstance(completed_job, dict) else None,
            enqueue_to_claim_ms,
            int((time.perf_counter() - started_perf) * 1000),
            projection_job.get("attempt_count"),
            request_id,
            correlation_id,
            counts.get("pending"),
            counts.get("running"),
            counts.get("done"),
            counts.get("failed"),
            counts.get("retrying"),
        )
    except Exception as exc:
        if lease_token:
            try:
                from backend.database import fail_projection_job, get_projection_job_status_counts

                failed_job = fail_projection_job(
                    job_id=safe_job_id,
                    lease_token=lease_token,
                    error_text=str(exc),
                )
                counts = get_projection_job_status_counts(job_source=expected_job_source)
                logging.warning(
                    "projection_materialization_job failed job_id=%s job_source=%s queue_name=%s projection_kind=%s status=%s last_error=%s request_id=%s correlation_id=%s pending=%s running=%s done=%s failed=%s retrying=%s",
                    safe_job_id,
                    expected_job_source,
                    queue_name,
                    projection_job.get("projection_kind") if isinstance(projection_job, dict) else None,
                    (failed_job or {}).get("status") if isinstance(failed_job, dict) else None,
                    (failed_job or {}).get("last_error") if isinstance(failed_job, dict) else None,
                    request_id,
                    correlation_id,
                    counts.get("pending"),
                    counts.get("running"),
                    counts.get("done"),
                    counts.get("failed"),
                    counts.get("retrying"),
                )
            except Exception:
                logging.exception(
                    "projection_materialization_job retry bookkeeping failed job_id=%s job_source=%s queue_name=%s request_id=%s correlation_id=%s",
                    safe_job_id,
                    expected_job_source,
                    queue_name,
                    request_id,
                    correlation_id,
                )
        logging.exception(
            "projection_materialization_job crashed job_id=%s job_source=%s queue_name=%s projection_kind=%s request_id=%s correlation_id=%s",
            safe_job_id,
            expected_job_source,
            queue_name,
            projection_job.get("projection_kind") if isinstance(projection_job, dict) else None,
            request_id,
            correlation_id,
        )
        raise


@dramatiq.actor(max_retries=0, queue_name=_PROJECTION_MATERIALIZATION_LIVE_QUEUE_NAME)
def run_projection_materialization_live_job(
    job_id: int,
    request_id: str | None = None,
    correlation_id: str | None = None,
) -> None:
    _run_projection_materialization_job_impl(
        job_id=job_id,
        expected_job_source="live",
        queue_name=_PROJECTION_MATERIALIZATION_LIVE_QUEUE_NAME,
        request_id=request_id,
        correlation_id=correlation_id,
    )


@dramatiq.actor(max_retries=0, queue_name=_PROJECTION_MATERIALIZATION_BACKFILL_QUEUE_NAME)
def run_projection_materialization_backfill_job(
    job_id: int,
    request_id: str | None = None,
    correlation_id: str | None = None,
) -> None:
    _run_projection_materialization_job_impl(
        job_id=job_id,
        expected_job_source="backfill",
        queue_name=_PROJECTION_MATERIALIZATION_BACKFILL_QUEUE_NAME,
        request_id=request_id,
        correlation_id=correlation_id,
    )


@dramatiq.actor(max_retries=0, queue_name="translation_pool_refill")
def run_translation_focus_pool_refill_job(
    force: bool = False,
    tz_name: str | None = None,
    request_id: str | None = None,
    correlation_id: str | None = None,
) -> None:
    started_at = time.perf_counter()
    normalized_tz_name = str(tz_name or "").strip() or None
    logging.info(
        "translation_focus_pool_refill_job start request_id=%s correlation_id=%s tz_name=%s force=%s",
        request_id,
        correlation_id,
        normalized_tz_name,
        bool(force),
    )
    try:
        from backend.backend_server import _dispatch_translation_focus_pool_refill, TODAY_PLAN_DEFAULT_TZ

        result = _dispatch_translation_focus_pool_refill(
            force=bool(force),
            tz_name=normalized_tz_name or TODAY_PLAN_DEFAULT_TZ,
        )
        total_ms = int((time.perf_counter() - started_at) * 1000)
        logging.info(
            "translation_focus_pool_refill_job completed request_id=%s correlation_id=%s tz_name=%s total_ms=%s result=%s",
            request_id,
            correlation_id,
            normalized_tz_name,
            total_ms,
            result,
        )
    except Exception:
        logging.exception(
            "translation_focus_pool_refill_job failed request_id=%s correlation_id=%s tz_name=%s",
            request_id,
            correlation_id,
            normalized_tz_name,
        )
        raise


@dramatiq.actor(max_retries=0, queue_name=_READER_INGEST_QUEUE_NAME)
def run_reader_library_ingest_job(**payload) -> None:
    safe_payload = dict(payload or {})
    try:
        from backend.backend_server import _process_reader_library_ingest_job

        _process_reader_library_ingest_job(**safe_payload)
    except Exception:
        logging.exception(
            "reader_library_ingest_job crashed document_id=%s user_id=%s",
            safe_payload.get("document_id"),
            safe_payload.get("user_id"),
        )
        raise


@dramatiq.actor(max_retries=0, queue_name=_IMAGE_QUIZ_PREP_QUEUE_NAME)
def run_image_quiz_template_prepare_job(
    user_id: int,
    source_lang: str,
    target_lang: str,
    requested_count: int = 1,
    request_id: str | None = None,
    correlation_id: str | None = None,
) -> None:
    safe_user_id = int(user_id)
    normalized_source_lang = str(source_lang or "").strip().lower() or "ru"
    normalized_target_lang = str(target_lang or "").strip().lower() or "de"
    safe_requested_count = max(1, min(int(requested_count or 1), 10))
    started_at = time.perf_counter()
    try:
        result = asyncio.run(
            _run_image_quiz_template_prepare_job_async(
                user_id=safe_user_id,
                source_lang=normalized_source_lang,
                target_lang=normalized_target_lang,
                requested_count=safe_requested_count,
            )
        )
        total_ms = int((time.perf_counter() - started_at) * 1000)
        logging.info(
            "image_quiz_template_prepare_job completed user_id=%s source_lang=%s target_lang=%s requested_count=%s prepared=%s rejected=%s failed=%s empty=%s request_id=%s correlation_id=%s total_ms=%s",
            safe_user_id,
            normalized_source_lang,
            normalized_target_lang,
            safe_requested_count,
            result.get("prepared_count"),
            result.get("rejected_count"),
            result.get("failed_count"),
            result.get("empty_count"),
            request_id,
            correlation_id,
            total_ms,
        )
    except Exception:
        logging.exception(
            "image_quiz_template_prepare_job failed user_id=%s source_lang=%s target_lang=%s requested_count=%s request_id=%s correlation_id=%s",
            safe_user_id,
            normalized_source_lang,
            normalized_target_lang,
            safe_requested_count,
            request_id,
            correlation_id,
        )
        raise


@dramatiq.actor(max_retries=0, queue_name=_IMAGE_QUIZ_RENDER_QUEUE_NAME)
def run_image_quiz_template_render_job(
    user_id: int,
    source_lang: str,
    target_lang: str,
    requested_count: int = 1,
    request_id: str | None = None,
    correlation_id: str | None = None,
) -> None:
    safe_user_id = int(user_id)
    normalized_source_lang = str(source_lang or "").strip().lower() or "ru"
    normalized_target_lang = str(target_lang or "").strip().lower() or "de"
    safe_requested_count = max(1, min(int(requested_count or 1), 10))
    started_at = time.perf_counter()
    try:
        result = _run_image_quiz_template_render_job_sync(
            user_id=safe_user_id,
            source_lang=normalized_source_lang,
            target_lang=normalized_target_lang,
            requested_count=safe_requested_count,
        )
        total_ms = int((time.perf_counter() - started_at) * 1000)
        logging.info(
            "image_quiz_template_render_job completed user_id=%s source_lang=%s target_lang=%s requested_count=%s recovered_stale=%s ready=%s failed=%s empty=%s request_id=%s correlation_id=%s total_ms=%s",
            safe_user_id,
            normalized_source_lang,
            normalized_target_lang,
            safe_requested_count,
            result.get("recovered_stale_count"),
            result.get("ready_count"),
            result.get("failed_count"),
            result.get("empty_count"),
            request_id,
            correlation_id,
            total_ms,
        )
    except Exception:
        logging.exception(
            "image_quiz_template_render_job failed user_id=%s source_lang=%s target_lang=%s requested_count=%s request_id=%s correlation_id=%s",
            safe_user_id,
            normalized_source_lang,
            normalized_target_lang,
            safe_requested_count,
            request_id,
            correlation_id,
        )
        raise


@dramatiq.actor(max_retries=0, queue_name=_IMAGE_QUIZ_PREP_QUEUE_NAME)
def run_image_quiz_template_refresh_job(
    user_id: int,
    source_lang: str,
    target_lang: str,
    requested_count: int = 1,
    request_id: str | None = None,
    correlation_id: str | None = None,
) -> None:
    safe_user_id = int(user_id)
    normalized_source_lang = str(source_lang or "").strip().lower() or "ru"
    normalized_target_lang = str(target_lang or "").strip().lower() or "de"
    safe_requested_count = max(1, min(int(requested_count or 1), 10))
    started_at = time.perf_counter()
    try:
        prepare_result = asyncio.run(
            _run_image_quiz_template_prepare_job_async(
                user_id=safe_user_id,
                source_lang=normalized_source_lang,
                target_lang=normalized_target_lang,
                requested_count=safe_requested_count,
            )
        )
        render_result = _run_image_quiz_template_render_job_sync(
            user_id=safe_user_id,
            source_lang=normalized_source_lang,
            target_lang=normalized_target_lang,
            requested_count=safe_requested_count,
        )
        total_ms = int((time.perf_counter() - started_at) * 1000)
        logging.info(
            "image_quiz_template_refresh_job completed user_id=%s source_lang=%s target_lang=%s requested_count=%s prepared=%s rejected=%s prepare_failed=%s prepare_empty=%s render_ready=%s render_failed=%s render_empty=%s recovered_stale=%s request_id=%s correlation_id=%s total_ms=%s",
            safe_user_id,
            normalized_source_lang,
            normalized_target_lang,
            safe_requested_count,
            prepare_result.get("prepared_count"),
            prepare_result.get("rejected_count"),
            prepare_result.get("failed_count"),
            prepare_result.get("empty_count"),
            render_result.get("ready_count"),
            render_result.get("failed_count"),
            render_result.get("empty_count"),
            render_result.get("recovered_stale_count"),
            request_id,
            correlation_id,
            total_ms,
        )
    except Exception:
        logging.exception(
            "image_quiz_template_refresh_job failed user_id=%s source_lang=%s target_lang=%s requested_count=%s request_id=%s correlation_id=%s",
            safe_user_id,
            normalized_source_lang,
            normalized_target_lang,
            safe_requested_count,
            request_id,
            correlation_id,
        )
        raise


@dramatiq.actor(max_retries=0, queue_name="translation_side_effects")
def run_translation_result_side_effects_job(
    *,
    user_id: int,
    original_text: str,
    user_translation: str,
    sentence_pk_id: int | None,
    session_id: int | None,
    sentence_id_for_mistake: int,
    score_value: int,
    correct_translation: str | None = None,
    categories: list[str] | None = None,
    subcategories: list[str] | None = None,
    source_lang: str = "ru",
    target_lang: str = "de",
) -> None:
    try:
        from backend.translation_workflow import apply_translation_result_side_effects

        asyncio.run(
            apply_translation_result_side_effects(
                user_id=int(user_id),
                original_text=str(original_text or ""),
                user_translation=str(user_translation or ""),
                sentence_pk_id=int(sentence_pk_id) if sentence_pk_id is not None else None,
                session_id=int(session_id) if session_id is not None else None,
                sentence_id_for_mistake=int(sentence_id_for_mistake),
                score_value=int(score_value),
                correct_translation=str(correct_translation or "").strip() or None,
                categories=list(categories or []),
                subcategories=list(subcategories or []),
                source_lang=str(source_lang or "ru").strip().lower() or "ru",
                target_lang=str(target_lang or "de").strip().lower() or "de",
            )
        )
    except Exception:
        logging.exception(
            "translation_result_side_effects_job failed user_id=%s sentence_id_for_mistake=%s",
            user_id,
            sentence_id_for_mistake,
        )
        raise


# ---------------------------------------------------------------------------
# Scheduler-dispatched actors
# Each actor is a thin wrapper that calls the corresponding _run_*_scheduler_job
# function from backend_server via a deferred import (same pattern used throughout
# this module).  The scheduler service enqueues these; the worker executes them.
# All use max_retries=0 — the underlying functions already carry their own
# DB-level deduplication (claim_scheduler_run_guard / has_admin_scheduler_run).
# ---------------------------------------------------------------------------

@dramatiq.actor(max_retries=0, queue_name="scheduler_jobs")
def run_daily_audio_scheduler_actor() -> None:
    from backend.backend_server import _run_audio_scheduler_job
    _run_audio_scheduler_job()


@dramatiq.actor(max_retries=0, queue_name="scheduler_jobs")
def run_private_analytics_scheduler_actor() -> None:
    from backend.backend_server import _run_private_analytics_scheduler_job
    _run_private_analytics_scheduler_job()


@dramatiq.actor(max_retries=0, queue_name="scheduler_jobs")
def run_weekly_goals_scheduler_actor() -> None:
    from backend.backend_server import _run_weekly_goals_scheduler_job
    _run_weekly_goals_scheduler_job()


@dramatiq.actor(max_retries=0, queue_name="scheduler_jobs")
def run_daily_group_summary_scheduler_actor() -> None:
    from backend.backend_server import _run_daily_group_summary_scheduler_job
    _run_daily_group_summary_scheduler_job()


@dramatiq.actor(max_retries=0, queue_name="scheduler_jobs")
def run_weekly_group_summary_scheduler_actor() -> None:
    from backend.backend_server import _run_weekly_group_summary_scheduler_job
    _run_weekly_group_summary_scheduler_job()


@dramatiq.actor(max_retries=0, queue_name="scheduler_jobs")
def run_today_plan_scheduler_actor() -> None:
    from backend.backend_server import _run_today_plan_scheduler_job
    _run_today_plan_scheduler_job()


@dramatiq.actor(max_retries=0, queue_name="scheduler_jobs")
def run_today_evening_reminders_scheduler_actor() -> None:
    from backend.backend_server import _run_today_evening_reminders_scheduler_job
    _run_today_evening_reminders_scheduler_job()


@dramatiq.actor(max_retries=0, queue_name="scheduler_jobs")
def run_translation_sessions_auto_close_actor() -> None:
    from backend.scheduler_jobs_core import run_translation_sessions_auto_close_job
    run_translation_sessions_auto_close_job()


@dramatiq.actor(max_retries=0, queue_name="scheduler_jobs")
def run_system_message_cleanup_actor() -> None:
    from backend.scheduler_jobs_core import run_system_message_cleanup_job
    run_system_message_cleanup_job()


@dramatiq.actor(max_retries=0, queue_name="scheduler_jobs")
def run_flashcard_feel_cleanup_actor() -> None:
    from backend.scheduler_jobs_core import run_flashcard_feel_cleanup_job
    run_flashcard_feel_cleanup_job()


@dramatiq.actor(max_retries=0, queue_name="scheduler_jobs")
def run_tts_db_cache_cleanup_actor() -> None:
    from backend.scheduler_jobs_core import run_tts_db_cache_cleanup_job
    run_tts_db_cache_cleanup_job()


@dramatiq.actor(max_retries=0, queue_name="scheduler_jobs")
def run_tts_r2_cache_cleanup_actor() -> None:
    from backend.scheduler_jobs_core import run_tts_r2_cache_cleanup_job
    run_tts_r2_cache_cleanup_job()


@dramatiq.actor(max_retries=0, queue_name="scheduler_jobs")
def run_image_quiz_r2_cleanup_actor() -> None:
    from backend.scheduler_jobs_core import run_image_quiz_r2_cleanup_job
    run_image_quiz_r2_cleanup_job()


@dramatiq.actor(max_retries=0, queue_name="scheduler_jobs")
def run_visual_riddle_r2_cleanup_actor() -> None:
    from backend.scheduler_jobs_core import run_visual_riddle_r2_cleanup_job
    run_visual_riddle_r2_cleanup_job()


@dramatiq.actor(max_retries=0, queue_name="scheduler_jobs")
def run_database_table_sizes_report_actor() -> None:
    from backend.scheduler_jobs_core import run_database_table_sizes_report_job
    run_database_table_sizes_report_job()


@dramatiq.actor(max_retries=0, queue_name="scheduler_jobs")
def run_tts_prewarm_scheduler_actor() -> None:
    from backend.tts_scheduler import run_tts_prewarm_scheduler_job
    run_tts_prewarm_scheduler_job()


@dramatiq.actor(max_retries=0, queue_name="scheduler_jobs")
def run_tts_generation_recovery_actor() -> None:
    from backend.tts_scheduler import run_tts_generation_recovery_scheduler_job
    run_tts_generation_recovery_scheduler_job()


@dramatiq.actor(max_retries=0, queue_name="scheduler_jobs")
def run_tts_prewarm_quota_control_actor() -> None:
    from backend.tts_scheduler import run_tts_prewarm_quota_control_scheduler_job
    run_tts_prewarm_quota_control_scheduler_job()


@dramatiq.actor(max_retries=0, queue_name="scheduler_jobs")
def run_sentence_prewarm_actor() -> None:
    from backend.backend_server import _run_sentence_prewarm_scheduler_job
    _run_sentence_prewarm_scheduler_job()


@dramatiq.actor(max_retries=0, queue_name="scheduler_jobs")
def run_translation_focus_pool_admin_report_actor() -> None:
    from backend.backend_server import _run_translation_focus_pool_admin_report_scheduler_job
    _run_translation_focus_pool_admin_report_scheduler_job()


@dramatiq.actor(max_retries=0, queue_name="scheduler_jobs")
def run_semantic_benchmark_prep_actor() -> None:
    from backend.backend_server import _run_semantic_benchmark_prep_scheduler_job
    _run_semantic_benchmark_prep_scheduler_job()


@dramatiq.actor(max_retries=0, queue_name="scheduler_jobs")
def run_semantic_audit_actor() -> None:
    from backend.backend_server import _run_semantic_audit_scheduler_job
    _run_semantic_audit_scheduler_job()


@dramatiq.actor(max_retries=0, queue_name="scheduler_jobs")
def run_skill_state_v2_aggregation_actor() -> None:
    from backend.backend_server import _run_skill_state_v2_aggregation_scheduler_job
    _run_skill_state_v2_aggregation_scheduler_job()


@dramatiq.actor(max_retries=0, queue_name="scheduler_jobs")
def run_agent_worker_schedule_control_actor(action: str = "reconcile_stop") -> None:
    from backend.agent_worker_schedule import run_agent_worker_schedule_control
    run_agent_worker_schedule_control(action=action, source="scheduler_jobs_actor")


@dramatiq.actor(max_retries=0, queue_name="scheduler_jobs")
def run_translation_check_worker_schedule_control_actor(action: str = "reconcile_stop") -> None:
    from backend.translation_check_worker_schedule import run_translation_check_worker_schedule_control
    run_translation_check_worker_schedule_control(action=action, source="scheduler_jobs_actor")


@dramatiq.actor(max_retries=0, queue_name="scheduler_jobs")
def run_service_resource_schedule_control_actor(
    service_name: str,
    action: str = "reconcile",
) -> None:
    from backend.service_resource_schedule import run_service_resource_schedule_control

    run_service_resource_schedule_control(
        action=action,
        service_name=service_name,
        source="scheduler_jobs_actor",
    )


@dramatiq.actor(max_retries=0, queue_name="tts_generation")
def run_tts_generation_actor(
    *,
    cache_key: str,
    user_id: int,
    language: str,
    tts_lang_short: str,
    voice: str,
    speaking_rate: float,
    normalized_text: str,
    object_key: str,
    had_existing_meta: bool,
    correlation_id: str | None = None,
    request_id: str | None = None,
    enqueue_ts_ms: int | None = None,
) -> None:
    from backend.job_queue import release_tts_generation_in_flight
    from backend.backend_server import _run_tts_generation_job

    try:
        _run_tts_generation_job(
            cache_key=cache_key,
            user_id=user_id,
            language=language,
            tts_lang_short=tts_lang_short,
            voice=voice,
            speaking_rate=speaking_rate,
            normalized_text=normalized_text,
            object_key=object_key,
            had_existing_meta=had_existing_meta,
            correlation_id=correlation_id,
            request_id=request_id,
            enqueue_ts_ms=enqueue_ts_ms,
        )
    finally:
        release_tts_generation_in_flight(cache_key)


@dramatiq.actor(max_retries=0, queue_name="tts_generation")
def run_reader_audio_page_generation_actor(
    *,
    job_key: str,
    user_id: int,
    document_id: int,
    page: int,
    page_source: str,
    page_text: str,
    text_hash: str,
    voice_name: str,
    rate: float,
    google_lang_code: str,
    language_for_tts: str,
    source_lang: str,
    target_lang: str,
) -> None:
    from backend.job_queue import release_reader_audio_page_in_flight
    from backend.backend_server import _run_reader_audio_page_generation_job

    try:
        _run_reader_audio_page_generation_job(
            job_key=job_key,
            user_id=user_id,
            document_id=document_id,
            page=page,
            page_source=page_source,
            page_text=page_text,
            text_hash=text_hash,
            voice_name=voice_name,
            rate=rate,
            google_lang_code=google_lang_code,
            language_for_tts=language_for_tts,
            source_lang=source_lang,
            target_lang=target_lang,
        )
    finally:
        release_reader_audio_page_in_flight(job_key)


# ═══════════════════════════════════════════════════════════════════════
# Visual Riddles — generation pipeline (Phase 2A)
# ═══════════════════════════════════════════════════════════════════════

_VR_PREP_QUEUE_NAME = str(
    os.getenv("VISUAL_RIDDLE_PREP_QUEUE_NAME") or "riddle_prepare"
).strip() or "riddle_prepare"

_VR_RENDER_QUEUE_NAME = str(
    os.getenv("VISUAL_RIDDLE_RENDER_QUEUE_NAME") or "riddle_render"
).strip() or "riddle_render"

_VR_R2_PREFIX = str(
    os.getenv("VISUAL_RIDDLE_R2_PREFIX") or "visual_riddles"
).strip().strip("/") or "visual_riddles"

VISUAL_RIDDLE_POOL_TARGET = max(
    1,
    int((os.getenv("VISUAL_RIDDLE_POOL_TARGET") or "12").strip() or "12"),
)

_VR_RENDERING_STALE_MINUTES = max(
    5,
    int((os.getenv("VISUAL_RIDDLE_RENDERING_STALE_MINUTES") or "45").strip() or "45"),
)

_VR_MAX_CAPTION_CHARS = max(
    100,
    int((os.getenv("MAX_VISUAL_RIDDLE_CAPTION_CHARS") or "900").strip() or "900"),
)
_VR_MAX_QUESTION_CHARS = max(
    50,
    int((os.getenv("MAX_VISUAL_RIDDLE_QUESTION_CHARS") or "300").strip() or "300"),
)
_VR_MAX_ANSWER_CHARS = max(
    20,
    int((os.getenv("MAX_VISUAL_RIDDLE_ANSWER_CHARS") or "120").strip() or "120"),
)

_VR_ALLOWED_QUIZ_TYPES = frozenset({
    "VISUAL_WORD_REBUS",
    "SITUATIONAL_REBUS",
})

_VR_ALLOWED_DIFFICULTIES = frozenset({"A2", "B1", "B2"})
_VR_ALLOWED_ANSWER_IDS = frozenset({"A", "B", "C", "D"})

_VR_NSFW_PATTERN = re.compile(
    r"\b(nude|naked|sex|porn|explicit|violence|gore|blood|kill|murder|"
    r"terror|terrorist|bomb|weapon|gun|pistol|rifle|suicide)\b",
    re.IGNORECASE,
)

# --- Diversity / anti-repetition helpers ----------------------------------

_VR_THEME_STOP_WORDS = frozenset({
    # English function words
    "a", "an", "the", "of", "in", "on", "at", "to", "for", "with", "by",
    "from", "up", "down", "and", "or", "but", "is", "are", "was", "were",
    "be", "been", "being", "have", "has", "had", "do", "does", "did",
    "will", "would", "could", "should", "may", "might", "can", "this",
    "that", "these", "those", "it", "its", "he", "she", "they", "we",
    "you", "one", "two", "three", "as", "not", "no", "so", "into", "out",
    "very", "just", "also", "about", "over", "under", "there", "here",
    "between", "through", "while", "where", "when", "who", "what", "how",
    # German function words
    "der", "die", "das", "ein", "eine", "ist", "sind", "hat", "haben",
    "wird", "werden", "des", "dem", "den", "sich", "nicht", "auch",
    "wie", "was", "wer", "wenn", "dann", "aber", "und", "oder", "doch",
    "im", "am", "an", "auf", "aus", "bei", "nach", "seit", "von", "zu",
    "er", "sie", "es", "wir", "ihr", "man", "zum", "zur", "beim",
    "nur", "noch", "schon", "mal", "sehr", "mehr", "als", "bereits",
})

_VR_EMOTION_PATTERNS: list[tuple[str, list[str]]] = [
    ("sadness/travel longing",       ["fernweh", "longing", "travel", "journey", "airport", "departure",
                                      "missing", "sehnsucht", "koffer", "abschied", "fern", "abflug"]),
    ("panic/being late",             ["late", "hurry", "rush", "panic", "alarm", "running", "train",
                                      "deadline", "spät", "beeilen", "zug", "verpassen", "rennen"]),
    ("embarrassment/social mistake", ["awkward", "embarrass", "blush", "mistake", "sorry", "apologize",
                                      "peinlich", "rot", "entschuldigung", "fehler", "scham"]),
    ("confusion/detective mystery",  ["detective", "mystery", "clue", "suspect", "investigate", "crime",
                                      "puzzle", "evidence", "geheimnis", "detektiv", "verdächtig"]),
    ("stress/work overload",         ["office", "desk", "overload", "stress", "deadline", "paper",
                                      "tired", "büro", "schreibtisch", "müde", "überlastet"]),
    ("joy/celebration",              ["celebrate", "party", "happy", "birthday", "smile", "laugh",
                                      "feier", "glücklich", "lachen", "geburtstag", "jubel"]),
    ("frustration/strict rules",     ["grammar", "rule", "strict", "teacher", "school", "correct",
                                      "lehrer", "schule", "grammatik", "regel", "klasse"]),
    ("cold/bad weather",             ["rain", "umbrella", "cold", "snow", "wet", "wind", "storm",
                                      "regen", "schirm", "kalt", "schnee", "sturm", "nass"]),
    ("domestic/home situation",      ["home", "kitchen", "cooking", "clean", "family", "dinner",
                                      "broken", "fix", "haus", "küche", "kochen", "kaputt"]),
    ("outdoor/nature adventure",     ["forest", "mountain", "lake", "hiking", "nature", "trail",
                                      "wald", "berg", "see", "wandern", "draußen", "natur"]),
    ("shopping/market",              ["shop", "market", "buy", "price", "money", "store", "cash",
                                      "kaufen", "markt", "preis", "laden", "einkaufen"]),
    ("health/medical",               ["doctor", "hospital", "sick", "medicine", "pain", "nurse",
                                      "arzt", "krank", "medizin", "krankenhaus", "schmerz"]),
]

# German cultural tropes to suppress via prompt — not banned, just reduced via context
_VR_GERMAN_STEREOTYPE_TROPES: list[str] = [
    "punctual German stereotype",
    "beer/Bier stereotype",
    "sausage/Wurst stereotype",
    "strict teacher in German classroom",
    "missed train / verpasster Zug",
    "umbrella in the rain as grammar cliché",
    "airport farewell sadness",
    "broken vase detective puzzle",
    "office desk procrastination",
    "Oktoberfest costume",
    "German engineering precision joke",
]

# -------------------------------------------------------------------------

_VR_QUIZ_TYPE_WEIGHTS = (
    ("VISUAL_WORD_REBUS",  65),
    ("SITUATIONAL_REBUS",  35),
)

_VR_DIFFICULTY_WEIGHTS = (
    ("A2", 20),
    ("B1", 50),
    ("B2", 30),
)

_VR_SKILL_WEIGHTS = (
    ("vocabulary",       65),
    ("speaking_phrase",  20),
    ("cultural_context", 15),
)


def _vr_weighted_choice(options: tuple, seed: str | None = None) -> str:
    import random as _random
    total = sum(w for _, w in options)
    if seed:
        rng = _random.Random(seed)
        bucket = rng.randint(0, total - 1)
    else:
        bucket = _random.randint(0, total - 1)
    cumulative = 0
    for name, weight in options:
        cumulative += weight
        if bucket < cumulative:
            return name
    return options[0][0]


def extract_visual_riddle_theme_signature(
    image_prompt: str,
    question_text: str = "",
) -> str:
    """Extract a short normalized theme signature from a riddle's image prompt and question.

    Returns 4-8 non-trivial lowercase tokens joined by space.
    Examples: "rain umbrella wet street", "airport suitcase sadness", "broken vase detective"
    """
    combined = f"{image_prompt} {question_text}".lower()
    tokens = re.findall(r"[a-zäöüß]{3,}", combined)
    seen: set[str] = set()
    result: list[str] = []
    for tok in tokens:
        if tok not in _VR_THEME_STOP_WORDS and tok not in seen:
            seen.add(tok)
            result.append(tok)
            if len(result) >= 8:
                break
    return " ".join(result)


def extract_visual_riddle_emotional_pattern(
    image_prompt: str,
    question_text: str = "",
    title: str = "",
) -> str | None:
    """Heuristic emotional/situational pattern extraction.

    Matches against _VR_EMOTION_PATTERNS keyword clusters.
    Returns the best-matching pattern name if ≥2 keywords matched, else None.
    """
    combined = f"{image_prompt} {question_text} {title}".lower()
    best_name: str | None = None
    best_score = 0
    for pattern_name, keywords in _VR_EMOTION_PATTERNS:
        score = sum(1 for kw in keywords if kw in combined)
        if score > best_score:
            best_score = score
            best_name = pattern_name
    return best_name if best_score >= 2 else None


def _apply_vr_quiz_type_diversity_pressure(
    base_weights: tuple,
    distribution: dict,
    lookback_count: int = 30,
) -> tuple:
    """Return adjusted quiz-type weights that penalise overused types and boost underused ones.

    Rules (non-deterministic — soft pressure only):
    - count ≥ 3× fair share → weight ÷ 4
    - count ≥ 2× fair share → weight ÷ 2
    - count = 0             → weight × 1.5
    - otherwise             → weight unchanged
    """
    total_seen = sum(distribution.values())
    if total_seen == 0:
        return base_weights
    fair_share = total_seen / max(1, len(base_weights))
    result: list[tuple[str, int]] = []
    for name, weight in base_weights:
        count = int(distribution.get(name) or 0)
        if count >= fair_share * 3:
            adjusted = max(1, weight // 4)
            logging.info(
                "vr_quiz_type_penalized type=%s count=%s fair_share=%.1f weight=%s→%s",
                name, count, fair_share, weight, adjusted,
            )
        elif count >= fair_share * 2:
            adjusted = max(1, weight // 2)
            logging.info(
                "vr_quiz_type_penalized type=%s count=%s fair_share=%.1f weight=%s→%s",
                name, count, fair_share, weight, adjusted,
            )
        elif count == 0:
            adjusted = int(weight * 1.5)
            logging.info(
                "vr_quiz_type_preferred_underused type=%s weight=%s→%s",
                name, weight, adjusted,
            )
        else:
            adjusted = weight
        result.append((name, adjusted))
    return tuple(result)


def build_vr_generation_memory(*, limit: int = 60, lookback_days: int = 30) -> dict:
    """Build a compact diversity memory dict from recent ready VR templates.

    Returns a bounded payload intended to be injected into the LLM generation prompt.
    All lists are deduplicated (except recent_quiz_types and recent_emotional_patterns
    which keep duplicates for frequency analysis).
    """
    from backend.database import get_recent_visual_riddle_generation_memory_rows

    rows = get_recent_visual_riddle_generation_memory_rows(
        limit=limit, lookback_days=lookback_days,
    )

    recent_target_words: list[str] = []
    recent_quiz_types: list[str] = []
    recent_titles: list[str] = []
    recent_visual_themes: list[str] = []
    recent_question_patterns: list[str] = []
    recent_emotional_patterns: list[str] = []

    seen_words: set[str] = set()
    seen_titles: set[str] = set()
    seen_themes: set[str] = set()
    seen_qpatterns: set[str] = set()

    for row in rows:
        qt = str(row.get("quiz_type") or "").strip()
        word = str(row.get("target_word_or_phrase") or "").strip()
        title = str(row.get("title") or "").strip()
        img = str(row.get("image_prompt") or "").strip()
        question = str(row.get("question_text") or "").strip()

        if qt:
            recent_quiz_types.append(qt)

        if word and word not in seen_words:
            seen_words.add(word)
            recent_target_words.append(word)

        if title and title not in seen_titles:
            seen_titles.add(title)
            recent_titles.append(title)

        theme = extract_visual_riddle_theme_signature(img, question) if img else ""
        if theme and theme not in seen_themes:
            seen_themes.add(theme)
            recent_visual_themes.append(theme)

        if question:
            qpat = question[:50].strip()
            if qpat and qpat not in seen_qpatterns:
                seen_qpatterns.add(qpat)
                recent_question_patterns.append(qpat)

        emotion = extract_visual_riddle_emotional_pattern(img, question, title) if img else None
        if emotion:
            recent_emotional_patterns.append(emotion)

    return {
        "recent_target_words":     recent_target_words[:30],
        "recent_quiz_types":       recent_quiz_types[:30],
        "recent_titles":           recent_titles[:20],
        "recent_visual_themes":    recent_visual_themes[:20],
        "recent_question_patterns": recent_question_patterns[:15],
        "recent_emotional_patterns": recent_emotional_patterns[:20],
    }


def _generate_vr_generation_params(
    *,
    seed: str | None = None,
    quiz_type_distribution: dict | None = None,
) -> dict:
    effective_weights = _VR_QUIZ_TYPE_WEIGHTS
    if quiz_type_distribution:
        effective_weights = _apply_vr_quiz_type_diversity_pressure(
            _VR_QUIZ_TYPE_WEIGHTS, quiz_type_distribution,
        )
    return {
        "quiz_type":    _vr_weighted_choice(effective_weights, seed=seed),
        "difficulty":   _vr_weighted_choice(_VR_DIFFICULTY_WEIGHTS, seed=seed),
        "target_skill": _vr_weighted_choice(_VR_SKILL_WEIGHTS, seed=seed),
    }


def validate_visual_riddle_blueprint(payload: dict) -> dict:
    """
    Validates and normalises a raw JSON payload from the LLM.
    Returns a cleaned dict on success.
    Raises ValueError with a descriptive reason on any failure.
    Never silently falls back.
    """
    if not isinstance(payload, dict):
        raise ValueError("blueprint_not_a_dict")

    required_fields = {
        "quiz_type", "difficulty", "target_skill", "target_word_or_phrase",
        "question_text", "image_prompt", "answers", "correct_answer_id",
        "short_explanation",
    }
    missing = required_fields - payload.keys()
    if missing:
        raise ValueError(f"blueprint_missing_fields:{','.join(sorted(missing))}")

    quiz_type = str(payload.get("quiz_type") or "").strip().upper()
    if quiz_type not in _VR_ALLOWED_QUIZ_TYPES:
        raise ValueError(f"blueprint_invalid_quiz_type:{quiz_type}")

    difficulty = str(payload.get("difficulty") or "").strip().upper()
    if difficulty not in _VR_ALLOWED_DIFFICULTIES:
        raise ValueError(f"blueprint_invalid_difficulty:{difficulty}")

    image_prompt = _normalize_space(payload.get("image_prompt"))
    if not image_prompt:
        raise ValueError("blueprint_missing_image_prompt")
    if len(image_prompt) < 50:
        raise ValueError(f"blueprint_image_prompt_too_short:{len(image_prompt)}")
    if "```" in image_prompt or "***" in image_prompt:
        raise ValueError("blueprint_image_prompt_has_markdown")
    if _VR_NSFW_PATTERN.search(image_prompt):
        raise ValueError("blueprint_image_prompt_nsfw")

    question_text = _normalize_space(payload.get("question_text"))
    if not question_text:
        raise ValueError("blueprint_missing_question_text")
    if "```" in question_text:
        raise ValueError("blueprint_question_text_has_code_block")

    short_explanation = _normalize_space(payload.get("short_explanation"))
    if not short_explanation:
        raise ValueError("blueprint_missing_short_explanation")

    raw_answers = payload.get("answers")
    if not isinstance(raw_answers, list):
        raise ValueError("blueprint_answers_not_a_list")
    if len(raw_answers) != 4:
        raise ValueError(f"blueprint_answers_wrong_count:{len(raw_answers)}")

    seen_ids: set[str] = set()
    correct_ids: list[str] = []
    normalized_answers: list[dict] = []

    for item in raw_answers:
        if not isinstance(item, dict):
            raise ValueError("blueprint_answer_item_not_a_dict")
        answer_id = str(item.get("id") or "").strip().upper()
        if answer_id not in _VR_ALLOWED_ANSWER_IDS:
            raise ValueError(f"blueprint_invalid_answer_id:{answer_id}")
        if answer_id in seen_ids:
            raise ValueError(f"blueprint_duplicate_answer_id:{answer_id}")
        seen_ids.add(answer_id)
        answer_text = _normalize_space(item.get("text"))
        if not answer_text:
            raise ValueError(f"blueprint_empty_answer_text:{answer_id}")
        is_correct = bool(item.get("is_correct"))
        if is_correct:
            correct_ids.append(answer_id)
        normalized_answers.append({"id": answer_id, "text": answer_text, "is_correct": is_correct})

    if seen_ids != _VR_ALLOWED_ANSWER_IDS:
        raise ValueError(f"blueprint_answers_missing_ids:got={''.join(sorted(seen_ids))}")
    if not correct_ids:
        raise ValueError("blueprint_no_correct_answer")
    if len(correct_ids) > 1:
        raise ValueError(f"blueprint_multiple_correct_answers:{','.join(correct_ids)}")

    correct_answer_id = str(payload.get("correct_answer_id") or "").strip().upper()
    if correct_answer_id not in _VR_ALLOWED_ANSWER_IDS:
        raise ValueError(f"blueprint_invalid_correct_answer_id:{correct_answer_id}")
    if correct_answer_id != correct_ids[0]:
        raise ValueError(
            f"blueprint_correct_answer_id_mismatch:{correct_answer_id}!={correct_ids[0]}"
        )

    # Safety limits: fail loudly, never silently truncate
    telegram_caption_check = _normalize_space(payload.get("telegram_caption")) or ""
    if telegram_caption_check and len(telegram_caption_check) > _VR_MAX_CAPTION_CHARS:
        raise ValueError(
            f"blueprint_telegram_caption_too_long:{len(telegram_caption_check)}>{_VR_MAX_CAPTION_CHARS}"
        )
    if len(question_text) > _VR_MAX_QUESTION_CHARS:
        raise ValueError(
            f"blueprint_question_text_too_long:{len(question_text)}>{_VR_MAX_QUESTION_CHARS}"
        )
    for ans in normalized_answers:
        if len(ans["text"]) > _VR_MAX_ANSWER_CHARS:
            raise ValueError(
                f"blueprint_answer_text_too_long:id={ans['id']}:len={len(ans['text'])}>{_VR_MAX_ANSWER_CHARS}"
            )

    return {
        "quiz_type":             quiz_type,
        "difficulty":            difficulty,
        "target_language":       "German",
        "target_skill":          _normalize_space(payload.get("target_skill")) or "vocabulary",
        "target_word_or_phrase": _normalize_space(payload.get("target_word_or_phrase")) or None,
        "title":                 _normalize_space(payload.get("title")) or None,
        "telegram_caption":      _normalize_space(payload.get("telegram_caption")) or None,
        "question_text":         question_text,
        "image_prompt":          image_prompt,
        "answers":               normalized_answers,
        "correct_answer_id":     correct_answer_id,
        "short_explanation":     short_explanation,
        "language_explanation":  _normalize_space(payload.get("language_explanation")) or None,
    }


def _build_vr_object_key(*, template_id: int, mime_type: str | None) -> str:
    extension = _image_extension_for_mime_type(mime_type)
    date_prefix = time.strftime("%Y/%m", time.gmtime())
    return f"{_VR_R2_PREFIX}/{date_prefix}/{int(template_id)}{extension}"


async def _prepare_single_vr_template_async(*, seed: str | None = None) -> dict:
    from backend.database import (
        create_visual_riddle_template_pending,
        store_visual_riddle_template_blueprint,
        mark_visual_riddle_template_failed,
        get_visual_riddle_quiz_type_distribution,
    )
    from backend.openai_manager import run_visual_riddle_blueprint

    # --- Load diversity context (failures are non-fatal) -------------------
    quiz_type_distribution: dict = {}
    diversity_memory: dict = {}

    try:
        quiz_type_distribution = get_visual_riddle_quiz_type_distribution(lookback_count=30)
    except Exception:
        logging.warning("vr_prepare: failed to load quiz_type_distribution", exc_info=True)

    try:
        diversity_memory = build_vr_generation_memory(limit=60, lookback_days=30)
        logging.info(
            "vr_diversity_memory_loaded quiz_type_count=%s target_word_count=%s "
            "theme_count=%s emotional_pattern_count=%s",
            len(diversity_memory.get("recent_quiz_types") or []),
            len(diversity_memory.get("recent_target_words") or []),
            len(diversity_memory.get("recent_visual_themes") or []),
            len(diversity_memory.get("recent_emotional_patterns") or []),
        )
    except Exception:
        logging.warning("vr_prepare: failed to build diversity_memory", exc_info=True)

    # --- Generation parameters with diversity-weighted quiz type -----------
    gen_params = _generate_vr_generation_params(
        seed=seed,
        quiz_type_distribution=quiz_type_distribution or None,
    )
    logging.info(
        "vr_prepare start quiz_type=%s difficulty=%s target_skill=%s",
        gen_params["quiz_type"],
        gen_params["difficulty"],
        gen_params["target_skill"],
    )

    template_id = create_visual_riddle_template_pending(
        quiz_type=gen_params["quiz_type"],
        difficulty=gen_params["difficulty"],
        target_skill=gen_params["target_skill"],
        question_text="",
        image_prompt="",
        answers=[],
        correct_answer_id="A",
    )
    if not template_id:
        logging.error("vr_prepare failed to create pending template")
        return {"status": "failed", "template_id": None, "reason": "db_insert_failed"}

    # --- Enrich LLM payload with diversity context -------------------------
    llm_payload: dict = dict(gen_params)
    if diversity_memory:
        llm_payload["recent_generation_memory"] = diversity_memory
        logging.info(
            "vr_diversity_context_sent target_words=%s themes=%s emotional_patterns=%s",
            len(diversity_memory.get("recent_target_words") or []),
            len(diversity_memory.get("recent_visual_themes") or []),
            len(diversity_memory.get("recent_emotional_patterns") or []),
        )

    llm_started = time.perf_counter()
    try:
        raw_payload = await run_visual_riddle_blueprint(llm_payload)
    except Exception as exc:
        llm_ms = int((time.perf_counter() - llm_started) * 1000)
        reason = f"llm_exception:{exc.__class__.__name__}"
        logging.error(
            "vr_prepare llm_failed template_id=%s reason=%s llm_ms=%s",
            template_id, reason, llm_ms,
        )
        mark_visual_riddle_template_failed(template_id, failure_reason=reason)
        return {"status": "failed", "template_id": template_id, "reason": reason}

    llm_ms = int((time.perf_counter() - llm_started) * 1000)

    if not isinstance(raw_payload, dict) or not raw_payload:
        reason = "llm_empty_response"
        logging.warning(
            "vr_prepare validation_failed template_id=%s reason=%s llm_ms=%s",
            template_id, reason, llm_ms,
        )
        mark_visual_riddle_template_failed(template_id, failure_reason=reason)
        return {"status": "failed", "template_id": template_id, "reason": reason}

    try:
        blueprint = validate_visual_riddle_blueprint(raw_payload)
    except ValueError as exc:
        reason = str(exc)
        logging.warning(
            "vr_prepare validation_failed template_id=%s reason=%s llm_ms=%s",
            template_id, reason, llm_ms,
        )
        mark_visual_riddle_template_failed(template_id, failure_reason=reason)
        return {"status": "validation_failed", "template_id": template_id, "reason": reason}

    store_visual_riddle_template_blueprint(
        template_id,
        image_prompt=blueprint["image_prompt"],
        question_text=blueprint["question_text"],
        answers=blueprint["answers"],
        correct_answer_id=blueprint["correct_answer_id"],
        short_explanation=blueprint["short_explanation"],
        language_explanation=blueprint.get("language_explanation"),
        why_wrong_answers=None,
    )
    logging.info(
        "vr_prepare blueprint_ready template_id=%s quiz_type=%s difficulty=%s llm_ms=%s",
        template_id,
        blueprint["quiz_type"],
        blueprint["difficulty"],
        llm_ms,
    )
    return {
        "status": "blueprint_ready",
        "template_id": template_id,
        "quiz_type": blueprint["quiz_type"],
        "difficulty": blueprint["difficulty"],
        "llm_ms": llm_ms,
    }


def _render_single_vr_template() -> dict:
    from backend.database import (
        claim_next_blueprint_ready_vr_template,
        mark_visual_riddle_template_failed,
        mark_visual_riddle_template_ready,
    )
    from backend.image_generation_provider import generate_image_bytes
    from backend.r2_storage import r2_public_url, r2_put_bytes

    claimed = claim_next_blueprint_ready_vr_template()
    if not claimed:
        return {"status": "no_template", "template_id": None}

    template_id = int(claimed.get("id") or 0)
    if template_id <= 0:
        return {"status": "no_template", "template_id": None}

    image_prompt = str(claimed.get("image_prompt") or "").strip()
    logging.info(
        "vr_render start template_id=%s quiz_type=%s difficulty=%s",
        template_id,
        claimed.get("quiz_type"),
        claimed.get("difficulty"),
    )

    render_started = time.perf_counter()
    try:
        render_result = generate_image_bytes(
            prompt=image_prompt,
            template_id=template_id,
            user_id=0,
        )
        image_bytes = bytes(render_result.get("data") or b"")
        mime_type = str(render_result.get("mime_type") or "image/png").strip().lower() or "image/png"
        if not image_bytes:
            raise RuntimeError("vr_image_generation_empty_payload")

        render_ms = int((time.perf_counter() - render_started) * 1000)
        object_key = _build_vr_object_key(template_id=template_id, mime_type=mime_type)

        r2_put_bytes(
            object_key,
            image_bytes,
            content_type=mime_type,
            cache_control="public, max-age=31536000, immutable",
        )
        public_url = r2_public_url(object_key)

        mark_visual_riddle_template_ready(
            template_id,
            image_object_key=object_key,
            image_url=public_url,
        )
        logging.info(
            "vr_render ready template_id=%s object_key=%s render_ms=%s bytes=%s",
            template_id, object_key, render_ms, len(image_bytes),
        )
        return {
            "status": "ready",
            "template_id": template_id,
            "image_object_key": object_key,
            "image_url": public_url,
            "render_ms": render_ms,
        }
    except Exception as exc:
        render_ms = int((time.perf_counter() - render_started) * 1000)
        reason = f"{exc.__class__.__name__}:{str(exc)[:120]}"
        logging.error(
            "vr_render failed template_id=%s reason=%s render_ms=%s",
            template_id, reason, render_ms,
        )
        mark_visual_riddle_template_failed(template_id, failure_reason=reason)
        return {"status": "failed", "template_id": template_id, "reason": reason}


def prepare_visual_riddle_pool(*, topup_limit: int = 3) -> dict:
    """
    Checks the global riddle pool size and enqueues prepare jobs if below target.
    Call this from bot_3.py pool maintenance logic.
    Does NOT generate per-user — global pool only.
    """
    from backend.database import count_ready_visual_riddle_templates, count_pipeline_visual_riddle_templates

    target = VISUAL_RIDDLE_POOL_TARGET
    ready = count_ready_visual_riddle_templates()
    pipeline = count_pipeline_visual_riddle_templates()
    total = ready + pipeline

    if total >= target:
        logging.info(
            "vr_pool_topup skipped ready=%s pipeline=%s total=%s target=%s",
            ready, pipeline, total, target,
        )
        return {"status": "pool_sufficient", "ready": ready, "pipeline": pipeline, "target": target}

    needed = max(0, target - total)
    safe_topup = min(needed, max(1, int(topup_limit or 3)))

    for _ in range(safe_topup):
        run_visual_riddle_template_prepare_job.send(requested_count=1)

    logging.info(
        "vr_pool_topup enqueued=%s ready=%s pipeline=%s total=%s target=%s",
        safe_topup, ready, pipeline, total, target,
    )
    return {
        "status": "enqueued",
        "ready": ready,
        "pipeline": pipeline,
        "enqueued": safe_topup,
        "target": target,
    }


@dramatiq.actor(max_retries=0, queue_name=_VR_PREP_QUEUE_NAME)
def run_visual_riddle_template_prepare_job(
    requested_count: int = 1,
    request_id: str | None = None,
    correlation_id: str | None = None,
) -> None:
    safe_count = max(1, min(int(requested_count or 1), 5))
    started_at = time.perf_counter()
    blueprint_ready = 0
    validation_failed = 0
    failed = 0
    items: list[dict] = []
    try:
        for i in range(safe_count):
            seed = f"{int(time.time() * 1000)}:{i}:{request_id or ''}"
            item = asyncio.run(_prepare_single_vr_template_async(seed=seed))
            items.append(item)
            status = str(item.get("status") or "").strip()
            if status == "blueprint_ready":
                blueprint_ready += 1
                # Enqueue render job immediately
                run_visual_riddle_template_render_job.send(requested_count=1)
            elif status == "validation_failed":
                validation_failed += 1
            else:
                failed += 1
        total_ms = int((time.perf_counter() - started_at) * 1000)
        logging.info(
            "vr_prepare_job completed requested=%s blueprint_ready=%s validation_failed=%s failed=%s request_id=%s total_ms=%s",
            safe_count, blueprint_ready, validation_failed, failed, request_id, total_ms,
        )
    except Exception:
        logging.exception(
            "vr_prepare_job crashed requested=%s request_id=%s correlation_id=%s",
            safe_count, request_id, correlation_id,
        )
        raise


@dramatiq.actor(max_retries=0, queue_name=_VR_RENDER_QUEUE_NAME)
def run_visual_riddle_template_render_job(
    requested_count: int = 1,
    request_id: str | None = None,
    correlation_id: str | None = None,
) -> None:
    safe_count = max(1, min(int(requested_count or 1), 5))
    started_at = time.perf_counter()
    ready = 0
    failed = 0
    empty = 0
    try:
        from backend.database import fail_stale_rendering_vr_templates
        recovered = fail_stale_rendering_vr_templates(stale_after_minutes=_VR_RENDERING_STALE_MINUTES)
        if recovered:
            logging.warning(
                "visual_riddle_stale_render_recovery recovered=%s stale_after_minutes=%s ids=%s",
                len(recovered),
                _VR_RENDERING_STALE_MINUTES,
                [int(r.get("id") or 0) for r in recovered],
            )
        for _ in range(safe_count):
            item = _render_single_vr_template()
            status = str(item.get("status") or "").strip()
            if status == "ready":
                ready += 1
            elif status == "no_template":
                empty += 1
                break
            else:
                failed += 1
        total_ms = int((time.perf_counter() - started_at) * 1000)
        logging.info(
            "vr_render_job completed requested=%s ready=%s failed=%s empty=%s recovered_stale=%s request_id=%s total_ms=%s",
            safe_count, ready, failed, empty, len(recovered), request_id, total_ms,
        )
    except Exception:
        logging.exception(
            "vr_render_job crashed requested=%s request_id=%s correlation_id=%s",
            safe_count, request_id, correlation_id,
        )
        raise


def generate_and_prepare_single_visual_riddle(*, seed: int | None = None) -> dict:
    """Synchronous end-to-end pipeline: LLM blueprint → image render → ready template.

    Returns a result dict with keys: status ("ready" | "failed"), template_id, error.
    Runs entirely in the calling thread — use asyncio.to_thread() from async callers.
    """
    import time as _time
    import asyncio as _asyncio
    started_at = _time.perf_counter()
    effective_seed = int(seed) if seed is not None else int(_time.time() * 1000) % (2 ** 31)
    try:
        blueprint_result = _asyncio.run(_prepare_single_vr_template_async(seed=effective_seed))
    except Exception as exc:
        logging.warning("vr_manual_generate: blueprint phase failed seed=%s: %s", effective_seed, exc, exc_info=True)
        return {"status": "failed", "template_id": None, "error": str(exc)}

    template_id = blueprint_result.get("template_id")
    step = blueprint_result.get("status")
    if step != "blueprint_ready" or not template_id:
        return {
            "status": "failed",
            "template_id": template_id,
            "error": f"blueprint phase ended with status={step!r}",
        }

    render_result = _render_single_vr_template()
    render_status = render_result.get("status")
    rendered_id = render_result.get("template_id")
    if render_status != "ready":
        total_ms = int((_time.perf_counter() - started_at) * 1000)
        logging.warning(
            "vr_manual_generate: render phase failed template_id=%s status=%s total_ms=%s",
            template_id, render_status, total_ms,
        )
        return {
            "status": "failed",
            "template_id": template_id,
            "error": f"render phase ended with status={render_status!r}",
        }

    total_ms = int((_time.perf_counter() - started_at) * 1000)
    logging.info(
        "vr_manual_generate: ready template_id=%s total_ms=%s",
        rendered_id or template_id, total_ms,
    )
    return {"status": "ready", "template_id": rendered_id or template_id, "error": None}


def build_visual_riddle_preview_payload(template_id: int) -> dict | None:
    """Load a visual riddle template from DB and return a structured preview dict.

    Returns None if the template does not exist or is not ready.
    """
    from backend.database import get_visual_riddle_template
    template = get_visual_riddle_template(int(template_id))
    if not template:
        return None
    gen_status = str(template.get("generation_status") or "").strip()
    if gen_status != "ready":
        return None
    answers = list(template.get("answers") or [])
    correct_id = str(template.get("correct_answer_id") or "A").strip().upper()
    correct_index = next(
        (i for i, a in enumerate(answers) if str(a.get("id") or "").strip().upper() == correct_id),
        0,
    )
    return {
        "template_id": int(template["id"]),
        "quiz_type": str(template.get("quiz_type") or ""),
        "difficulty": str(template.get("difficulty") or ""),
        "target_skill": str(template.get("target_skill") or ""),
        "target_word_or_phrase": template.get("target_word_or_phrase"),
        "title": template.get("title"),
        "telegram_caption": str(template.get("telegram_caption") or "").strip() or None,
        "question_text": str(template.get("question_text") or ""),
        "image_url": str(template.get("image_url") or "").strip() or None,
        "answers": answers,
        "correct_answer_id": correct_id,
        "correct_index": correct_index,
        "short_explanation": template.get("short_explanation"),
        "language_explanation": template.get("language_explanation"),
    }



def get_visual_riddle_pool_health() -> dict:
    """Return current visual riddle pool health for monitoring.

    Returns dict with keys: ready, pipeline, rendering, failed,
    oldest_ready_age_hours, latest_generation_at, pool_target, topup_trigger.
    """
    from backend.database import get_visual_riddle_pool_health_stats
    stats = get_visual_riddle_pool_health_stats()
    return {
        **stats,
        "pool_target": VISUAL_RIDDLE_POOL_TARGET,
        "topup_trigger": max(
            1,
            int((os.getenv("VISUAL_RIDDLE_POOL_TOPUP_TRIGGER") or "5").strip() or "5"),
        ),
    }
