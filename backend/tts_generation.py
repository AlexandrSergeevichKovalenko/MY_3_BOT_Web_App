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
import threading
import time
from uuid import uuid4

from pydub import AudioSegment

from backend.database import (
    get_admin_telegram_ids,
    get_google_tts_monthly_budget_status,
    get_provider_monthly_budget_status,
    mark_provider_budget_threshold_notified,
    mark_tts_object_failed,
    mark_tts_object_ready,
    set_provider_budget_block_state,
)
from backend import tts_budget_counter
from backend.observability import _elapsed_ms_since
from backend.r2_storage import r2_exists, r2_put_bytes, r2_public_url
from backend.telegram_notify import _send_private_message
from backend.tts_admin_monitor import _shorten_tts_admin_text
from backend.tts_runtime_state import _clear_tts_url_poll_attempt
from backend.utils import prepare_google_creds_for_tts


# One shared Google TextToSpeechClient, built lazily and reused. Constructing it per
# call (the old behaviour at every synthesis site) meant a fresh gRPC channel + TLS
# handshake + OAuth token exchange EACH time — several seconds of cold latency, the
# cause of "🔊 waited 15s" on quick-dict pronunciation. One warm client keeps the
# channel/token alive so synthesis is ~1s. The google client is thread-safe to share.
_TTS_CLIENT = None
_TTS_CLIENT_LOCK = threading.Lock()


def _get_tts_client():
    """Return the shared stable TextToSpeechClient, constructing (and warming creds) once."""
    global _TTS_CLIENT
    client = _TTS_CLIENT
    if client is not None:
        return client
    with _TTS_CLIENT_LOCK:
        if _TTS_CLIENT is None:
            from google.cloud import texttospeech
            key_path = prepare_google_creds_for_tts()
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path
            _TTS_CLIENT = texttospeech.TextToSpeechClient()
        return _TTS_CLIENT


# Reader-page audio needs the v1beta1 client (SSML-mark time-pointing for word timing) —
# a different class from the stable one, so it gets its own reused instance.
_TTS_BETA_CLIENT = None
_TTS_BETA_CLIENT_LOCK = threading.Lock()


def _get_tts_beta_client():
    """Return the shared v1beta1 TextToSpeechClient (time-pointing), constructed once."""
    global _TTS_BETA_CLIENT
    client = _TTS_BETA_CLIENT
    if client is not None:
        return client
    with _TTS_BETA_CLIENT_LOCK:
        if _TTS_BETA_CLIENT is None:
            from google.cloud import texttospeech_v1beta1 as texttospeech
            key_path = prepare_google_creds_for_tts()
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path
            _TTS_BETA_CLIENT = texttospeech.TextToSpeechClient()
        return _TTS_BETA_CLIENT


# ---------------------------------------------------------------------------
# Voice / language tables
# ---------------------------------------------------------------------------

_TTS_VOICES = {
    "de": str(os.getenv("GOOGLE_TTS_VOICE_DE") or "de-DE-Polyglot-1").strip() or "de-DE-Polyglot-1",
    "ru": str(os.getenv("GOOGLE_TTS_VOICE_RU") or "ru-RU-Wavenet-B").strip() or "ru-RU-Wavenet-B",
    "en": str(os.getenv("GOOGLE_TTS_VOICE_EN") or "en-US-Wavenet-D").strip() or "en-US-Wavenet-D",
    "es": str(os.getenv("GOOGLE_TTS_VOICE_ES") or "es-ES-Standard-A").strip() or "es-ES-Standard-A",
    "it": str(os.getenv("GOOGLE_TTS_VOICE_IT") or "it-IT-Standard-A").strip() or "it-IT-Standard-A",
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


def _estimate_reader_page_tts_budget_chars(page_text: str) -> int:
    """
    Count billable chars for reader page synthesis using the exact normalized
    page text that is sent into the TTS pipeline.
    """
    return max(0, len(str(page_text or "")))


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


# Friendly per-bucket names for admin alerts (which bucket is filling up).
_TTS_BUDGET_LABELS = {
    "google_tts": "Google TTS — премиум (WaveNet): словарь/SRS/игры",
    "google_tts_standard": "Google TTS — Standard: библиотека классики",
    "google_tts_paid": "Google TTS — платная озвучка книг",
}


def _notify_tts_budget_thresholds(
    *,
    provider: str,
    status: dict,
    requested_chars: int,
) -> None:
    """Pre-threshold (50/75/90%) admin DM for ANY TTS bucket. Deduped per row via
    notified_thresholds. The paid bucket gets an 'extend from /budgets' hint since
    its spend is revenue-covered and the admin may legitimately raise it."""
    provider_value = str(provider or "").strip().lower() or "google_tts"
    effective_limit = int(status.get("effective_limit_units") or 0)
    if effective_limit <= 0:
        return

    used_units = float(status.get("used_units") or 0.0)
    projected_used = used_units + max(0, int(requested_chars or 0))
    thresholds = [50, 75, 90]
    notified = status.get("notified_thresholds") if isinstance(status.get("notified_thresholds"), dict) else {}
    period_month = status.get("period_month")
    label = _TTS_BUDGET_LABELS.get(provider_value, provider_value)
    if provider_value == "google_tts_paid":
        tail = (
            "\nЭто платная полка — озвучку книг оплачивают пользователи. "
            "Если спрос честный, продли лимит кнопками в /budgets."
        )
    else:
        tail = "\nЕсли нужно, подними месячный лимит до жёсткого стопа."

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
            f"⚠️ Бюджет озвучки — {label}\n\n"
            f"Порог: {threshold}%\n"
            f"Месяц: {period_month or '—'}\n"
            f"Сейчас: {used_out} симв.\n"
            f"После текущего запроса: {projected_out} симв.\n"
            f"Лимит: {effective_limit} симв.\n"
            f"Останется: {remaining_out} симв.\n"
            f"{tail}"
        )

        admin_ids = sorted(int(item) for item in get_admin_telegram_ids() if int(item) > 0)
        sent = False
        for admin_id in admin_ids:
            try:
                _send_private_message(int(admin_id), message_text, disable_web_page_preview=True)
                sent = True
            except Exception:
                logging.warning("Failed to send TTS budget alert provider=%s admin_id=%s", provider_value, admin_id, exc_info=True)

        if sent:
            try:
                updated = mark_provider_budget_threshold_notified(
                    provider=provider_value,
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
                logging.warning("Failed to mark %s threshold=%s as notified", provider_value, threshold, exc_info=True)


def _notify_google_tts_budget_thresholds(
    *,
    status: dict,
    requested_chars: int,
) -> None:
    """Premium-bucket wrapper kept for existing call sites."""
    _notify_tts_budget_thresholds(provider="google_tts", status=status, requested_chars=requested_chars)


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

    # Read usage from the fast O(1) per-month counter instead of SUM-ing the whole
    # billing table. It is INCR'd by EVERY synth path below (not just the few that
    # write a billing row), so the threshold alerts finally reflect real Google
    # usage. Seeds from the ledger SUM on a cold Redis key; fail-open to the SUM.
    period_month = status.get("period_month")
    status["used_units"] = tts_budget_counter.get_used(
        "google_tts", period_month, fallback=status.get("used_units")
    )

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

    # Passed the gate → record these chars so the very next check sees them.
    tts_budget_counter.add("google_tts", period_month, requested_value)
    return status


def _enforce_tts_monthly_budget(requested_chars: int, *, provider: str = "google_tts") -> dict:
    """Provider-parameterized monthly char budget guard. `provider="google_tts"` is
    the app's premium bucket (delegates to the specialized guard above, keeping its
    threshold alerts). Other providers (e.g. `google_tts_standard`, the public-domain
    library's separate Standard-voice free tier) are enforced generically so they
    never draw down the premium bucket."""
    normalized = str(provider or "google_tts").strip().lower() or "google_tts"
    if normalized == "google_tts":
        return _enforce_google_tts_monthly_budget(requested_chars)

    requested_value = max(0, int(requested_chars or 0))
    status = get_provider_monthly_budget_status(provider=normalized, units_type="chars", unit_label="chars")
    if not status:
        return {"provider": normalized, "unit": "chars", "used_units": 0.0, "effective_limit_units": 0, "is_blocked": False}

    # Same fast counter for the Standard/paid buckets (each synth INCRs exactly once).
    period_month = status.get("period_month")
    status["used_units"] = tts_budget_counter.get_used(
        normalized, period_month, fallback=status.get("used_units")
    )

    # Pre-threshold DM alerts for these buckets too (esp. the paid one, so the admin
    # is pinged and can extend from /budgets before the hard stop).
    _notify_tts_budget_thresholds(provider=normalized, status=status, requested_chars=requested_value)

    effective_limit = int(status.get("effective_limit_units") or 0)
    used_units = float(status.get("used_units") or 0.0)
    payload = {
        "provider": normalized,
        "unit": "chars",
        "used": int(round(used_units)),
        "requested": requested_value,
        "limit": effective_limit,
        "remaining": max(0, int(round(effective_limit - used_units))),
        "period_month": status.get("period_month"),
        "is_blocked": bool(status.get("is_blocked")),
    }
    if bool(status.get("is_blocked")):
        reason = str(status.get("block_reason") or "").strip() or f"{normalized} monthly budget is blocked"
        raise GoogleTTSBudgetBlockedError(reason, payload=payload)
    if effective_limit > 0 and used_units + requested_value > effective_limit:
        over_reason = f"{normalized} monthly limit reached: {int(round(used_units))} + {requested_value} > {effective_limit} chars"
        try:
            set_provider_budget_block_state(provider=normalized, is_blocked=True, block_reason=over_reason)
        except Exception:
            logging.warning("Failed to persist %s budget block state", normalized, exc_info=True)
        payload["is_blocked"] = True
        raise GoogleTTSBudgetBlockedError(over_reason, payload=payload)

    # Passed the gate → record these chars into the same fast counter.
    tts_budget_counter.add(normalized, period_month, requested_value)
    return status


# ---------------------------------------------------------------------------
# Google TTS synthesis
# ---------------------------------------------------------------------------


def _synthesize_mp3(
    text: str,
    language: str = "de-DE",
    voice: str | None = None,
    speed: float = 0.9,
) -> bytes:
    normalized_text = str(text or "").strip()
    if not normalized_text:
        raise RuntimeError("Google TTS получил пустой текст")
    # SYNTHETIC_LOAD_MODE: return a tiny fake audio payload, no Google TTS network.
    from backend.synthetic_load import synthetic_tts_mp3_or_none
    _synthetic = synthetic_tts_mp3_or_none(normalized_text)
    if _synthetic is not None:
        return _synthetic
    voice_name = str(voice or _TTS_VOICES["de"]).strip() or _TTS_VOICES["de"]

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

    tts_client = _get_tts_client()
    voice_params = texttospeech.VoiceSelectionParams(language_code=language, name=voice_name)
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
# Per-page TTS with word-level timepoints (Reader Patch 2.4)
# ---------------------------------------------------------------------------


def _synthesize_page_plaintext_with_timings(
    *,
    page_text: str,
    words: list,
    lang_code: str,
    voice_name: str,
    speaking_rate: float,
) -> dict:
    """Plain-text synthesis for voices that don't support SSML timepointing (Chirp /
    Chirp3-HD). Same return contract as synthesize_page_with_timings, but word timings
    are interpolated across the measured audio duration, weighted by word length — good
    enough to keep the reader's highlight roughly in sync. The monthly TTS budget is
    already enforced by the caller, so we don't double-count here."""
    from google.cloud import texttospeech

    client = _get_tts_client()
    voice_params = texttospeech.VoiceSelectionParams(language_code=lang_code, name=voice_name)
    audio_config = texttospeech.AudioConfig(
        audio_encoding=texttospeech.AudioEncoding.MP3,
        speaking_rate=float(speaking_rate),
    )

    # Chunk plain text under Google's per-request limit, splitting on sentence bounds.
    max_chars = 4500
    if len(page_text) <= max_chars:
        chunks = [page_text]
    else:
        chunks = []
        current = ""
        for sentence in re.split(r"(?<=[.!?])\s+", page_text):
            candidate = f"{current} {sentence}".strip() if current else sentence
            if len(candidate) <= max_chars:
                current = candidate
            else:
                if current:
                    chunks.append(current)
                current = sentence
        if current:
            chunks.append(current)

    combined = AudioSegment.silent(duration=0)
    for chunk in chunks:
        if not chunk.strip():
            continue
        response = client.synthesize_speech(
            input=texttospeech.SynthesisInput(text=chunk),
            voice=voice_params,
            audio_config=audio_config,
        )
        if response.audio_content:
            combined += AudioSegment.from_file(io.BytesIO(response.audio_content), format="mp3")

    if len(combined) == 0:
        raise RuntimeError("Google TTS (Chirp) вернул пустой аудиопоток")
    duration_ms = len(combined)

    weights = [max(1, len(str(w.get("value") or ""))) for w in words]
    total_weight = sum(weights) or 1
    timings = []
    acc = 0
    for i, w in enumerate(words):
        start_ms = int(round(duration_ms * acc / total_weight))
        acc += weights[i]
        end_ms = int(round(duration_ms * acc / total_weight))
        timings.append({
            "wid": str(i),
            "word": w.get("value"),
            "start_ms": start_ms,
            "end_ms": end_ms,
            "char_start": w.get("char_start"),
            "char_end": w.get("char_end"),
        })

    out = io.BytesIO()
    combined.export(out, format="mp3", bitrate="192k")
    return {
        "audio_bytes": out.getvalue(),
        "mime": "audio/mpeg",
        "duration_ms": duration_ms,
        "word_timings": timings,
    }


def synthesize_page_with_timings(
    *,
    page_text: str,
    lang_code: str = "de-DE",
    voice_name: str | None = None,
    speaking_rate: float = 1.0,
    budget_provider: str = "google_tts",
) -> dict:
    """
    Synthesize one reader page with natural prosody plus word-level timings.

    The SSML preserves the full original text and uses sparse marks at natural
    timing spans instead of injecting a mark before every word. Word timings are
    then interpolated inside each span so the frontend keeps highlighting and
    seek-by-word without forcing robotic delivery.

    Returns:
        {
            "audio_bytes": bytes,
            "mime": "audio/mpeg",
            "duration_ms": int,
            "word_timings": [{"wid": "0", "word": "Als", "start_ms": 50,
                               "end_ms": 280, "char_start": 0, "char_end": 3}, ...]
        }
    wid values are 0-based positional indices — the frontend maps them to
    token.wid by position. char_start/char_end are byte offsets in page_text.
    """
    from backend.tts_ssml import (
        segment_page_words,
        chunk_text_with_words,
        segment_timing_spans,
        _build_ssml_from_spans,
        parse_timepoints_for_spans,
    )

    try:
        from google.cloud import texttospeech_v1beta1 as texttospeech
    except Exception as exc:
        raise RuntimeError(f"Google TTS не установлен: {exc}") from exc

    words = segment_page_words(page_text)
    if not words:
        raise RuntimeError("Страница не содержит слов")
    resolved_voice_name = str(voice_name or _TTS_VOICES["de"]).strip() or _TTS_VOICES["de"]

    _enforce_tts_monthly_budget(_estimate_reader_page_tts_budget_chars(page_text), provider=budget_provider)

    # Chirp / Chirp3-HD are plain-text-only voices — they reject SSML, so timepointing
    # (word <mark>s) isn't available. Synthesize from plain text and interpolate word
    # timings across the measured duration so the reader still highlights approximately.
    if "chirp" in resolved_voice_name.lower():
        return _synthesize_page_plaintext_with_timings(
            page_text=page_text,
            words=words,
            lang_code=lang_code,
            voice_name=resolved_voice_name,
            speaking_rate=float(speaking_rate),
        )

    tts_client = _get_tts_beta_client()
    voice_params = texttospeech.VoiceSelectionParams(language_code=lang_code, name=resolved_voice_name)
    audio_config = texttospeech.AudioConfig(
        audio_encoding=texttospeech.AudioEncoding.MP3,
        speaking_rate=float(speaking_rate),
    )

    # Split at natural boundaries while keeping SSML under the provider limit.
    text_chunks = chunk_text_with_words(page_text, words)

    def _looks_like_sentence_too_long(exc: Exception) -> bool:
        msg = str(exc).lower()
        return (
            "too long" in msg
            or "sentences that are too long" in msg
            or ("invalid" in msg and "argument" in msg)
        )

    def _synthesize_chunk(chunk_text, chunk_words, char_offset, mark_offset, time_offset_ms, depth=0):
        """Synthesize one chunk and return (AudioSegment, timings, marks_used).

        Google occasionally rejects a chunk whose single sentence would produce
        too much audio ("sentences that are too long"). Dense, sparsely-punctuated
        pages hit this. We bisect the chunk at a word boundary and retry each half
        through the SAME span/mark machinery, so word timings stay correct no
        matter how deep we recurse."""
        if not chunk_words:
            return AudioSegment.silent(duration=0), [], 0

        timing_spans = segment_timing_spans(chunk_text, chunk_words, text_char_offset=char_offset)
        if not timing_spans:
            return AudioSegment.silent(duration=0), [], 0

        ssml_text, mark_index = _build_ssml_from_spans(
            chunk_text, timing_spans, text_char_offset=char_offset, mark_offset=mark_offset,
        )
        tts_request = texttospeech.SynthesizeSpeechRequest(
            input=texttospeech.SynthesisInput(ssml=ssml_text),
            voice=voice_params,
            audio_config=audio_config,
            enable_time_pointing=[texttospeech.SynthesizeSpeechRequest.TimepointType.SSML_MARK],
        )
        try:
            response = tts_client.synthesize_speech(request=tts_request)
        except Exception as exc:
            if depth < 8 and len(chunk_words) > 1 and _looks_like_sentence_too_long(exc):
                mid = len(chunk_words) // 2
                left_words = chunk_words[:mid]
                right_words = chunk_words[mid:]
                split_char = int(right_words[0]["char_start"])
                rel_split = max(0, split_char - char_offset)
                left_text = chunk_text[:rel_split]
                right_text = chunk_text[rel_split:]
                logging.warning(
                    "[READER_AUDIO] chunk too long (depth=%s, %s words) — bisecting and retrying",
                    depth, len(chunk_words),
                )
                seg_l, tim_l, marks_l = _synthesize_chunk(
                    left_text, left_words, char_offset, mark_offset, time_offset_ms, depth + 1,
                )
                seg_r, tim_r, marks_r = _synthesize_chunk(
                    right_text, right_words, split_char, mark_offset + marks_l,
                    time_offset_ms + len(seg_l), depth + 1,
                )
                return seg_l + seg_r, tim_l + tim_r, marks_l + marks_r
            raise

        if not response.audio_content:
            return AudioSegment.silent(duration=0), [], 0

        segment = AudioSegment.from_file(io.BytesIO(response.audio_content), format="mp3")
        chunk_timings = parse_timepoints_for_spans(
            list(response.timepoints),
            mark_index,
            chunk_duration_ms=len(segment),
            time_offset_ms=time_offset_ms,
        )
        return segment, chunk_timings, len(mark_index)

    combined = AudioSegment.silent(duration=0)
    all_timings: list[dict] = []
    mark_offset = 0
    char_offset = 0  # char position of this chunk's start in page_text

    for chunk_text, chunk_words in text_chunks:
        if not chunk_words:
            char_offset += len(chunk_text)
            continue
        segment, chunk_timings, marks_used = _synthesize_chunk(
            chunk_text, chunk_words, char_offset, mark_offset, time_offset_ms=len(combined),
        )
        all_timings.extend(chunk_timings)
        combined += segment
        mark_offset += marks_used
        char_offset += len(chunk_text)

    if len(combined) == 0:
        raise RuntimeError("Google TTS вернул пустой аудиопоток")

    out = io.BytesIO()
    combined.export(out, format="mp3", bitrate="192k")
    return {
        "audio_bytes": out.getvalue(),
        "mime": "audio/mpeg",
        "duration_ms": len(combined),
        "word_timings": all_timings,
    }


def _numdict_digit_run_ssml(digits: str) -> list[str]:
    """Read a pure-digit run the German way: grouped in PAIRS, each pair a compound
    cardinal ("85" → «fünfundachtzig»). A pair with a leading zero or a lone trailing
    digit falls back to digit-by-digit so the zero/odd digit is still heard. Returns the
    list of SSML <say-as> fragments (caller joins them with breaks)."""
    import re as _re
    parts: list[str] = []
    for i in range(0, len(digits), 2):
        g = digits[i:i + 2]
        gd = _re.sub(r"\D", "", g)
        if not gd:
            continue
        if gd[0] == "0" or len(gd) == 1:
            parts.append(f'<say-as interpret-as="digits">{gd}</say-as>')
        else:
            parts.append(f'<say-as interpret-as="cardinal">{gd}</say-as>')
    return parts


def _numdict_number_ssml(number_str: str, number_type: str) -> str:
    """Read a number the way Germans dictate it: in GROUPS, each group as a compound
    cardinal (units-before-tens), not digit-by-digit. e.g. "85" → «fünfundachtzig»,
    "227" → «zweihundertsiebenundzwanzig». Groups with a leading zero (phone area
    codes) are read digit-by-digit so the zero is heard ("0341" → «null-drei-vier-eins»).
    Alphanumeric codes: LETTERS are spelled, but digit runs are read in PAIRS as
    two-digit numbers ("94NC72" → «vierundneunzig, N, C, zweiundsiebzig») — reading the
    digits one-by-one has no training value (learners confuse 2-/3-digit numbers, not
    single digits). Honors spaces in the embedded number as the author's grouping; if
    there are none, auto-groups into pairs."""
    import re as _re
    from xml.sax.saxutils import escape as _esc
    nt = str(number_type or "digits").strip().lower()
    s = str(number_str or "").strip()
    if nt == "characters":
        code = _re.sub(r"[^A-Za-z0-9]", "", s)   # drop the dashes, keep letters+digits
        parts: list[str] = []
        for run in _re.findall(r"\d+|[A-Za-z]+", code):
            if run[0].isdigit():
                parts.extend(_numdict_digit_run_ssml(run))   # pairs → two-digit numbers
            else:
                parts.append(f'<say-as interpret-as="characters">{_esc(run)}</say-as>')
        return '<break time="280ms"/>'.join(parts) or _esc(code)
    if nt == "cardinal":
        d = _re.sub(r"\D", "", s)
        return f'<say-as interpret-as="cardinal">{d}</say-as>' if d else _esc(s)
    # telephone / digits → grouped compound reading
    groups = s.split()
    if len(groups) <= 1:
        d = _re.sub(r"\D", "", s)
        groups = [d[i:i + 2] for i in range(0, len(d), 2)]
    parts: list[str] = []
    for g in groups:
        gd = _re.sub(r"\D", "", g)
        if not gd:
            continue
        if gd[0] == "0" or len(gd) == 1:
            # leading zero (prefix) or a lone trailing digit → read digit-by-digit
            parts.append(f'<say-as interpret-as="digits">{gd}</say-as>')
        else:
            parts.append(f'<say-as interpret-as="cardinal">{gd}</say-as>')
    return '<break time="280ms"/>'.join(parts) or _esc(s)


def synthesize_numdict_mp3(
    *,
    scenario_text: str,
    number_type: str = "digits",
    spoken_number: str | None = None,
    lang_code: str = "de-DE",
    voice_name: str | None = None,
    speaking_rate: float = 1.0,
) -> bytes:
    """Synthesize a Zahlen-Diktat scene to MP3, reading the embedded number the way a
    German would dictate it — in groups, as compound cardinals (see
    _numdict_number_ssml), not digit-by-digit. The number is wrapped in «NUM»…«/NUM».

    spoken_number (optional) overrides the grouping used for the AUDIO — pass the
    bank's display_answer so the spoken grouping always matches what's shown on the
    reveal screen (e.g. "754 683" → triples), regardless of how the number was woven
    into the scene text. number_type ∈ {telephone, digits, characters, cardinal}.
    Returns MP3 bytes (Telegram iOS webview can't decode Opus)."""
    from xml.sax.saxutils import escape as _xml_escape

    try:
        from google.cloud import texttospeech_v1beta1 as texttospeech
    except Exception as exc:
        raise RuntimeError(f"Google TTS не установлен: {exc}") from exc

    text = str(scenario_text or "").strip()
    if not text:
        raise RuntimeError("scenario_text leer")

    say_as = str(number_type or "digits").strip().lower()
    if say_as not in ("telephone", "digits", "characters", "cardinal"):
        say_as = "digits"

    open_m, close_m = "«NUM»", "«/NUM»"
    oi, ci = text.find(open_m), text.find(close_m)
    if 0 <= oi < ci:
        before = text[:oi]
        number = text[oi + len(open_m):ci]
        after = text[ci + len(close_m):]
        # Audio grouping follows display_answer when given, so "754 683" is heard as
        # triples even if the scene embeds the bare digits — never the auto-paired form.
        num_for_reading = str(spoken_number).strip() if str(spoken_number or "").strip() else number
        inner = (
            f"{_xml_escape(before)}"
            f"{_numdict_number_ssml(num_for_reading, say_as)}"
            f"{_xml_escape(after)}"
        )
    else:
        # No marker (shouldn't happen) — speak the plain text, stripping stray markers.
        inner = _xml_escape(text.replace(open_m, " ").replace(close_m, " "))
    ssml_text = f"<speak>{inner}</speak>"

    _enforce_google_tts_monthly_budget(len(text))

    resolved_voice_name = str(voice_name or _TTS_VOICES["de"]).strip() or _TTS_VOICES["de"]
    tts_client = _get_tts_beta_client()
    voice_params = texttospeech.VoiceSelectionParams(language_code=lang_code, name=resolved_voice_name)
    audio_config = texttospeech.AudioConfig(
        audio_encoding=texttospeech.AudioEncoding.MP3,
        speaking_rate=float(speaking_rate),
    )
    response = tts_client.synthesize_speech(
        input=texttospeech.SynthesisInput(ssml=ssml_text),
        voice=voice_params,
        audio_config=audio_config,
    )
    if not response.audio_content:
        raise RuntimeError("Google TTS вернул пустой аудиопоток")
    return response.audio_content


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
