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
_IMAGE_QUIZ_PREP_QUEUE_NAME = "image_quiz_prepare"
_IMAGE_QUIZ_RENDER_QUEUE_NAME = "image_quiz_render"
_IMAGE_QUIZ_R2_PREFIX = str(
    os.getenv("IMAGE_QUIZ_R2_PREFIX") or "image_quizzes"
).strip().strip("/") or "image_quizzes"
_IMAGE_QUIZ_RENDERING_STALE_MINUTES = max(
    5,
    int((os.getenv("IMAGE_QUIZ_RENDERING_STALE_MINUTES") or "45").strip() or "45"),
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


def _sanitize_image_quiz_blueprint(
    payload: dict,
    *,
    expected_correct_answer: str,
    answer_language: str,
) -> dict:
    source_sentence = _normalize_space(payload.get("source_sentence"))
    image_prompt = _normalize_space(payload.get("image_prompt"))
    question_de = _normalize_space(payload.get("question_de")) or "Was zeigt das Bild?"
    explanation = _normalize_space(payload.get("explanation"))
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
    options[correct_index] = normalized_expected
    if len(set(options)) != 4:
        raise ValueError("blueprint_options_conflict_with_correct_answer")
    if not _text_matches_expected_language(source_sentence, answer_language):
        raise ValueError("blueprint_source_sentence_language_invalid")
    if any(not _text_matches_expected_language(option, answer_language) for option in options):
        raise ValueError("blueprint_options_language_invalid")
    if not _text_matches_expected_language(question_de, "de"):
        question_de = "Was zeigt das Bild?"
    return {
        "source_sentence": source_sentence,
        "image_prompt": image_prompt,
        "question_de": question_de,
        "answer_options": options,
        "correct_option_index": correct_index,
        "explanation": explanation,
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

    blueprint_payload = await run_image_quiz_blueprint(
        {
            "answer_language": answer_language,
            "source_language": str(source_lang or "").strip().lower() or "ru",
            "target_language": str(target_lang or "").strip().lower() or "de",
            "source_text": str(candidate.get("source_text") or ""),
            "target_text": str(candidate.get("target_text") or ""),
            "source_sentence": selected_sentence,
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

    store_image_quiz_template_blueprint(
        template_id,
        source_sentence=sanitized_blueprint["source_sentence"],
        image_prompt=sanitized_blueprint["image_prompt"],
        question_de=sanitized_blueprint["question_de"],
        answer_options=sanitized_blueprint["answer_options"],
        correct_option_index=int(sanitized_blueprint["correct_option_index"]),
        explanation=sanitized_blueprint["explanation"] or None,
        provider_name="image_quiz_prepare",
        provider_meta={
            "candidate_entry_id": entry_id,
            "sentence_source": sentence_source,
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
    from backend.backend_server import _run_system_message_cleanup_job
    _run_system_message_cleanup_job()


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
def run_database_table_sizes_report_actor() -> None:
    from backend.backend_server import _run_database_table_sizes_report_job
    _run_database_table_sizes_report_job()


@dramatiq.actor(max_retries=0, queue_name="scheduler_jobs")
def run_tts_prewarm_scheduler_actor() -> None:
    from backend.backend_server import _run_tts_prewarm_scheduler_job
    _run_tts_prewarm_scheduler_job()


@dramatiq.actor(max_retries=0, queue_name="scheduler_jobs")
def run_tts_generation_recovery_actor() -> None:
    from backend.backend_server import _run_tts_generation_recovery_scheduler_job
    _run_tts_generation_recovery_scheduler_job()


@dramatiq.actor(max_retries=0, queue_name="scheduler_jobs")
def run_tts_prewarm_quota_control_actor() -> None:
    from backend.backend_server import _run_tts_prewarm_quota_control_scheduler_job
    _run_tts_prewarm_quota_control_scheduler_job()


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
