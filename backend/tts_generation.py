"""
TTS generation helpers that carry no backend_server dependency.

Slice 1 — pure constants, normalisation utilities, and the budget-blocked
exception.

Slice 2 — job-kwargs builder for the recovery scheduler path.

Slice 4 — budget enforcement, admin alerting, and Google TTS synthesis.
  Added: _notify_google_tts_budget_thresholds, _enforce_google_tts_monthly_budget,
         _synthesize_mp3.
  These were previously blocked by _send_private_message living only in
  backend_server; that primitive now lives in backend.telegram_notify.

Slice 5 — TTS execution core.
  Added: _run_tts_generation_core.
  Shell (_run_tts_generation_job) remains in backend_server and injects
  _billing_log_event_safe + pre-resolved language pair.
"""

import io
import logging
import os
import re
import time
from uuid import uuid4

from pydub import AudioSegment

from backend.database import (
    get_admin_telegram_ids,
    get_google_tts_monthly_budget_status,
    mark_provider_budget_threshold_notified,
    mark_tts_object_failed,
    mark_tts_object_ready,
    set_provider_budget_block_state,
)
from backend.observability import _elapsed_ms_since
from backend.r2_storage import r2_exists, r2_put_bytes, r2_public_url
from backend.telegram_notify import _send_private_message
from backend.tts_admin_monitor import _shorten_tts_admin_text
from backend.tts_runtime_state import _clear_tts_url_poll_attempt
from backend.utils import prepare_google_creds_for_tts


# ---------------------------------------------------------------------------
# Voice / language tables
# ---------------------------------------------------------------------------

_TTS_VOICES = {
    "de": "de-DE-Neural2-C",
    "ru": "ru-RU-Wavenet-B",
    "en": "en-US-Wavenet-D",
    "es": "es-ES-Standard-A",
    "it": "it-IT-Standard-A",
}

_TTS_LANG_CODES = {
    "de": "de-DE",
    "ru": "ru-RU",
    "en": "en-US",
    "es": "es-ES",
    "it": "it-IT",
}

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

TTS_OBJECT_PREFIX = str(os.getenv("TTS_OBJECT_PREFIX") or "tts").strip().strip("/") or "tts"

TTS_WEBAPP_DEFAULT_SPEED: float = 0.95

# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


def _normalize_short_lang_code(value: str | None, fallback: str = "ru") -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        return fallback
    raw = raw.replace("_", "-")
    if "-" in raw:
        raw = raw.split("-", 1)[0]
    return raw or fallback


def _sanitize_object_segment(value: str, fallback: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9._-]+", "-", str(value or "").strip())
    cleaned = re.sub(r"-{2,}", "-", cleaned).strip("-.")
    if not cleaned:
        return fallback
    if ".." in cleaned:
        cleaned = cleaned.replace("..", ".")
    return cleaned or fallback


def _normalize_utterance_text(text: str) -> str:
    cleaned = (text or "").strip()
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned


def _to_epoch_ms() -> int:
    return int(time.time() * 1000)


# ---------------------------------------------------------------------------
# TTS-specific normalisation
# ---------------------------------------------------------------------------


def _normalize_tts_language_code(language: str | None) -> tuple[str, str]:
    short_lang = _normalize_short_lang_code(language, fallback="de")
    language_code = _TTS_LANG_CODES.get(short_lang, _TTS_LANG_CODES["de"])
    return short_lang, language_code


def _normalize_tts_voice_name(voice: str | None, short_lang: str) -> str:
    candidate = str(voice or "").strip()
    if candidate:
        return candidate
    return str(_TTS_VOICES.get(short_lang, _TTS_VOICES["de"])).strip()


def _tts_object_key(short_lang: str, voice: str, cache_key: str) -> str:
    safe_lang = _sanitize_object_segment(short_lang, "de")
    safe_voice = _sanitize_object_segment(voice, "voice")
    safe_key = _sanitize_object_segment(cache_key, "key")
    return f"{TTS_OBJECT_PREFIX}/{safe_lang}/{safe_voice}/{safe_key}.mp3"


def _tts_recovery_correlation_id(cache_key_prefix: str) -> str:
    """Background-job-only correlation ID for TTS recovery paths.

    Covers the no-request-context branch of _build_observability_correlation_id
    for recovery scheduler call sites (prefix="tts", fallback_seed only).
    NOT a general-purpose replacement for _build_observability_correlation_id.
    """
    safe_seed = re.sub(r"[^a-zA-Z0-9._:-]+", "-", str(cache_key_prefix or "")[:64]).strip("-")
    if safe_seed:
        return f"tts_{safe_seed}"
    return f"tts_{uuid4().hex[:16]}"


# ---------------------------------------------------------------------------
# Job-kwargs builder (recovery scheduler path)
# ---------------------------------------------------------------------------


def _build_tts_generation_job_kwargs_from_meta(meta: dict, *, user_id: int | None = None) -> dict | None:
    if not isinstance(meta, dict):
        return None
    cache_key = str(meta.get("cache_key") or "").strip()
    normalized_text = _normalize_utterance_text(meta.get("source_text") or "")
    if not cache_key or not normalized_text:
        return None
    short_lang, language_code = _normalize_tts_language_code(meta.get("language"))
    voice = _normalize_tts_voice_name(meta.get("voice"), short_lang)
    speaking_rate = float(meta.get("speed")) if meta.get("speed") is not None else TTS_WEBAPP_DEFAULT_SPEED
    object_key = str(meta.get("object_key") or "").strip() or _tts_object_key(short_lang, voice, cache_key)
    safe_user_id = max(0, int(user_id or 0))
    return {
        "user_id": safe_user_id,
        "language": language_code,
        "tts_lang_short": short_lang,
        "voice": voice,
        "speaking_rate": speaking_rate,
        "normalized_text": normalized_text,
        "cache_key": cache_key,
        "object_key": object_key,
        "had_existing_meta": True,
        "request_id": f"req_tts_recover_{uuid4().hex[:16]}",
        "correlation_id": _tts_recovery_correlation_id(f"recover:{cache_key[:16]}"),
        "enqueue_ts_ms": _to_epoch_ms(),
    }


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class GoogleTTSBudgetBlockedError(RuntimeError):
    def __init__(self, message: str, *, payload: dict | None = None):
        super().__init__(message)
        self.payload = dict(payload or {})


# ---------------------------------------------------------------------------
# Budget alerting and enforcement
# ---------------------------------------------------------------------------


def _notify_google_tts_budget_thresholds(
    *,
    status: dict,
    requested_chars: int,
) -> None:
    effective_limit = int(status.get("effective_limit_units") or 0)
    if effective_limit <= 0:
        return

    used_units = float(status.get("used_units") or 0.0)
    projected_used = used_units + max(0, int(requested_chars or 0))
    thresholds = [50, 75, 90]
    notified = status.get("notified_thresholds") if isinstance(status.get("notified_thresholds"), dict) else {}
    period_month = status.get("period_month")

    for threshold in thresholds:
        threshold_key = str(threshold)
        threshold_units = effective_limit * (threshold / 100.0)
        if projected_used < threshold_units:
            continue
        if notified.get(threshold_key):
            continue

        used_out = int(round(used_units))
        projected_out = int(round(projected_used))
        remaining_out = max(0, effective_limit - projected_out)
        message_text = (
            "⚠️ Google TTS budget alert\n\n"
            f"Threshold: {threshold}%\n"
            f"Month: {period_month or '—'}\n"
            f"Used now: {used_out} chars\n"
            f"Projected after current request: {projected_out} chars\n"
            f"Limit: {effective_limit} chars\n"
            f"Remaining after request: {remaining_out} chars\n\n"
            "Budget tracking is active. If needed, increase the monthly limit before the hard stop is reached."
        )

        admin_ids = sorted(int(item) for item in get_admin_telegram_ids() if int(item) > 0)
        sent = False
        for admin_id in admin_ids:
            try:
                _send_private_message(int(admin_id), message_text, disable_web_page_preview=True)
                sent = True
            except Exception:
                logging.warning("Failed to send Google TTS budget alert to admin_id=%s", admin_id, exc_info=True)

        if sent:
            try:
                updated = mark_provider_budget_threshold_notified(
                    provider="google_tts",
                    threshold_percent=threshold,
                    metadata={
                        "last_threshold_alert": threshold,
                        "last_threshold_projected_used": projected_out,
                        "last_threshold_limit": effective_limit,
                    },
                )
                if isinstance(updated, dict):
                    notified = updated.get("notified_thresholds") if isinstance(updated.get("notified_thresholds"), dict) else notified
            except Exception:
                logging.warning("Failed to mark Google TTS threshold=%s as notified", threshold, exc_info=True)


def _enforce_google_tts_monthly_budget(requested_chars: int) -> dict:
    requested_value = max(0, int(requested_chars or 0))
    status = get_google_tts_monthly_budget_status()
    if not status:
        return {
            "provider": "google_tts",
            "unit": "chars",
            "used_units": 0.0,
            "effective_limit_units": 0,
            "remaining_units": 0.0,
            "usage_ratio": 0.0,
            "is_blocked": False,
        }

    _notify_google_tts_budget_thresholds(status=status, requested_chars=requested_value)

    effective_limit = int(status.get("effective_limit_units") or 0)
    used_units = float(status.get("used_units") or 0.0)
    payload = {
        "provider": "google_tts",
        "unit": "chars",
        "used": int(round(used_units)),
        "requested": requested_value,
        "limit": effective_limit,
        "remaining": max(0, int(round(effective_limit - used_units))),
        "period_month": status.get("period_month"),
        "is_blocked": bool(status.get("is_blocked")),
    }

    if bool(status.get("is_blocked")):
        reason = str(status.get("block_reason") or "").strip() or "Google TTS monthly budget is blocked"
        raise GoogleTTSBudgetBlockedError(reason, payload=payload)

    if effective_limit > 0 and used_units + requested_value > effective_limit:
        over_reason = (
            f"Google TTS monthly limit reached: "
            f"{int(round(used_units))} + {requested_value} > {effective_limit} chars"
        )
        try:
            set_provider_budget_block_state(
                provider="google_tts",
                is_blocked=True,
                block_reason=over_reason,
            )
        except Exception:
            logging.warning("Failed to persist Google TTS budget block state", exc_info=True)
        payload["is_blocked"] = True
        payload["remaining"] = max(0, effective_limit - int(round(used_units)))
        raise GoogleTTSBudgetBlockedError(over_reason, payload=payload)

    return status


# ---------------------------------------------------------------------------
# Google TTS synthesis
# ---------------------------------------------------------------------------


def _synthesize_mp3(
    text: str,
    language: str = "de-DE",
    voice: str = "de-DE-Neural2-C",
    speed: float = 0.9,
) -> bytes:
    normalized_text = str(text or "").strip()
    if not normalized_text:
        raise RuntimeError("Google TTS получил пустой текст")

    try:
        from google.cloud import texttospeech
    except Exception as exc:
        raise RuntimeError(f"Google TTS не установлен: {exc}") from exc

    # Google TTS has request length limits; chunk long reader documents to avoid
    # forced fallback to offline engine for otherwise valid requests.
    max_chars_per_request = 4500

    def split_for_google_tts(raw_text: str) -> list[str]:
        compact = re.sub(r"[ \t]+", " ", raw_text).strip()
        if not compact:
            return []
        if len(compact) <= max_chars_per_request:
            return [compact]

        chunks: list[str] = []
        paragraphs = [part.strip() for part in re.split(r"\n{2,}", compact) if part.strip()]
        if not paragraphs:
            paragraphs = [compact]

        def append_piece(piece: str) -> None:
            piece = piece.strip()
            if not piece:
                return
            if len(piece) <= max_chars_per_request:
                chunks.append(piece)
                return
            sentences = [s.strip() for s in re.split(r"(?<=[.!?])\s+", piece) if s.strip()]
            if not sentences:
                sentences = [piece]
            current = ""
            for sentence in sentences:
                candidate = f"{current} {sentence}".strip() if current else sentence
                if len(candidate) <= max_chars_per_request:
                    current = candidate
                    continue
                if current:
                    chunks.append(current)
                if len(sentence) <= max_chars_per_request:
                    current = sentence
                    continue
                words = sentence.split()
                bucket = ""
                for word in words:
                    next_bucket = f"{bucket} {word}".strip() if bucket else word
                    if len(next_bucket) <= max_chars_per_request:
                        bucket = next_bucket
                    else:
                        if bucket:
                            chunks.append(bucket)
                        bucket = word
                if bucket:
                    current = bucket
                else:
                    current = ""
            if current:
                chunks.append(current)

        accumulator = ""
        for paragraph in paragraphs:
            candidate = f"{accumulator}\n\n{paragraph}".strip() if accumulator else paragraph
            if len(candidate) <= max_chars_per_request:
                accumulator = candidate
            else:
                if accumulator:
                    append_piece(accumulator)
                accumulator = paragraph
        if accumulator:
            append_piece(accumulator)
        return chunks

    text_chunks = split_for_google_tts(normalized_text)
    if not text_chunks:
        raise RuntimeError("Google TTS не получил чанки текста")
    _enforce_google_tts_monthly_budget(sum(len(chunk) for chunk in text_chunks))

    key_path = prepare_google_creds_for_tts()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path
    tts_client = texttospeech.TextToSpeechClient()
    voice_params = texttospeech.VoiceSelectionParams(language_code=language, name=voice)
    audio_config = texttospeech.AudioConfig(
        audio_encoding=texttospeech.AudioEncoding.MP3,
        speaking_rate=speed,
    )
    if len(text_chunks) == 1:
        response = tts_client.synthesize_speech(
            input=texttospeech.SynthesisInput(text=text_chunks[0]),
            voice=voice_params,
            audio_config=audio_config,
        )
        return response.audio_content

    combined = AudioSegment.silent(duration=0)
    for chunk in text_chunks:
        response = tts_client.synthesize_speech(
            input=texttospeech.SynthesisInput(text=chunk),
            voice=voice_params,
            audio_config=audio_config,
        )
        if not response.audio_content:
            continue
        segment = AudioSegment.from_file(io.BytesIO(response.audio_content), format="mp3")
        combined += segment

    if len(combined) == 0:
        raise RuntimeError("Google TTS вернул пустой аудиопоток")

    out = io.BytesIO()
    combined.export(out, format="mp3", bitrate="192k")
    return out.getvalue()


# ---------------------------------------------------------------------------
# TTS execution core (Slice 5)
# ---------------------------------------------------------------------------


def _run_tts_generation_core(
    *,
    user_id_int: int,
    language: str,
    tts_lang_short: str,
    voice: str,
    speaking_rate: float,
    normalized_text: str,
    cache_key: str,
    object_key: str,
    had_existing_meta: bool,
    user_source_lang: str | None,
    user_target_lang: str | None,
    billing_fn,
) -> dict:
    """Execute the TTS pipeline: cache-hit check, synthesis, upload, mark-ready.

    Receives pre-resolved language pair and billing callable from the shell so
    this function has no direct dependency on _get_user_language_pair or
    _billing_log_event_safe. Always returns a result dict; never raises.
    """
    provider_duration_ms = None
    storage_upload_duration_ms = None
    r2_head_duration_ms = None
    final_status = "error"
    cache_hit = False
    error_code: str | None = None
    exception_type: str | None = None
    error_message: str | None = None
    failure_stage = "prepare"
    try:
        if had_existing_meta:
            failure_stage = "r2_head"
            r2_head_started_perf = time.perf_counter()
            object_exists = bool(r2_exists(object_key))
            r2_head_duration_ms = _elapsed_ms_since(r2_head_started_perf)
            if user_id_int > 0 and billing_fn is not None:
                billing_fn(
                    user_id=user_id_int,
                    action_type="r2_head_object",
                    provider="cloudflare_r2_class_b",
                    units_type="operations",
                    units_value=1.0,
                    source_lang=user_source_lang,
                    target_lang=user_target_lang,
                    idempotency_seed=f"r2-head:{user_id_int}:{object_key}:{time.time_ns()}",
                    status="estimated",
                    metadata={"storage": "r2", "operation": "head_object", "cached": object_exists},
                )
            if object_exists:
                url = r2_public_url(object_key)
                mark_tts_object_ready(
                    cache_key=cache_key,
                    object_key=object_key,
                    url=url,
                    size_bytes=None,
                    language=language,
                    voice=voice,
                    speed=speaking_rate,
                    source_text=normalized_text,
                )
                final_status = "hit"
                cache_hit = True
                _clear_tts_url_poll_attempt(cache_key)
                return {
                    "final_status": final_status,
                    "cache_hit": cache_hit,
                    "error_code": error_code,
                    "exception_type": exception_type,
                    "error_message": error_message,
                    "failure_stage": failure_stage,
                    "provider_duration_ms": provider_duration_ms,
                    "storage_upload_duration_ms": storage_upload_duration_ms,
                    "r2_head_duration_ms": r2_head_duration_ms,
                }

        failure_stage = "google_synthesize"
        provider_started_perf = time.perf_counter()
        response_audio = _synthesize_mp3(
            normalized_text,
            language=language,
            voice=voice,
            speed=speaking_rate,
        )
        provider_duration_ms = _elapsed_ms_since(provider_started_perf)
        failure_stage = "r2_upload"
        upload_started_perf = time.perf_counter()
        r2_put_bytes(
            object_key,
            response_audio,
            content_type="audio/mpeg",
            cache_control="public, max-age=31536000, immutable",
        )
        storage_upload_duration_ms = _elapsed_ms_since(upload_started_perf)
        if user_id_int > 0 and billing_fn is not None:
            billing_fn(
                user_id=user_id_int,
                action_type="r2_put_object",
                provider="cloudflare_r2_class_a",
                units_type="operations",
                units_value=1.0,
                source_lang=user_source_lang,
                target_lang=user_target_lang,
                idempotency_seed=f"r2-put:{user_id_int}:{object_key}:{time.time_ns()}",
                status="estimated",
                metadata={"storage": "r2", "operation": "put_object", "bytes": len(response_audio)},
            )
            billing_fn(
                user_id=user_id_int,
                action_type="r2_storage_allocation",
                provider="cloudflare_r2_storage",
                units_type="mb_month",
                units_value=float(len(response_audio)) / (1024.0 * 1024.0),
                source_lang=user_source_lang,
                target_lang=user_target_lang,
                idempotency_seed=f"r2-storage:{user_id_int}:{object_key}:{len(response_audio)}:{time.time_ns()}",
                status="estimated",
                metadata={"storage": "r2", "bytes": len(response_audio)},
            )
        failure_stage = "mark_ready"
        public_url = r2_public_url(object_key)
        mark_tts_object_ready(
            cache_key=cache_key,
            object_key=object_key,
            url=public_url,
            size_bytes=len(response_audio),
            language=language,
            voice=voice,
            speed=speaking_rate,
            source_text=normalized_text,
        )
        if user_id_int > 0 and billing_fn is not None:
            billing_fn(
                user_id=user_id_int,
                action_type="webapp_tts_chars",
                provider="google_tts",
                units_type="chars",
                units_value=float(len(normalized_text)),
                source_lang=user_source_lang,
                target_lang=user_target_lang,
                idempotency_seed=f"webapp-tts-generate:{user_id_int}:{cache_key}:{int(time.time())}",
                status="estimated",
                metadata={
                    "cached": False,
                    "language": language,
                    "tts_lang": tts_lang_short,
                    "voice": voice,
                    "storage": "r2",
                },
            )
        final_status = "generated"
        cache_hit = False
        _clear_tts_url_poll_attempt(cache_key)
    except GoogleTTSBudgetBlockedError as exc:
        error_code = "google_tts_budget_blocked"
        exception_type = exc.__class__.__name__
        error_message = _shorten_tts_admin_text(str(exc), 220)
        mark_tts_object_failed(
            cache_key=cache_key,
            error_code="google_tts_budget_blocked",
            error_msg=str(exc),
            language=language,
            voice=voice,
            speed=speaking_rate,
            source_text=normalized_text,
            object_key=object_key,
        )
        final_status = "error"
    except Exception as exc:
        error_code = "tts_generation_failed"
        exception_type = exc.__class__.__name__
        error_message = _shorten_tts_admin_text(str(exc), 220)
        logging.exception("R2 TTS generation failed for cache_key=%s", cache_key)
        mark_tts_object_failed(
            cache_key=cache_key,
            error_code="tts_generation_failed",
            error_msg=str(exc),
            language=language,
            voice=voice,
            speed=speaking_rate,
            source_text=normalized_text,
            object_key=object_key,
        )
        final_status = "error"
    return {
        "final_status": final_status,
        "cache_hit": cache_hit,
        "error_code": error_code,
        "exception_type": exception_type,
        "error_message": error_message,
        "failure_stage": failure_stage,
        "provider_duration_ms": provider_duration_ms,
        "storage_upload_duration_ms": storage_upload_duration_ms,
        "r2_head_duration_ms": r2_head_duration_ms,
    }
