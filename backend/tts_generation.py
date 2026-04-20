"""
TTS generation helpers that carry no backend_server dependency.

Slice 1 — pure constants, normalisation utilities, and the budget-blocked
exception.

Slice 2 — job-kwargs builder for the recovery scheduler path.
  Added: TTS_WEBAPP_DEFAULT_SPEED, _normalize_utterance_text, _to_epoch_ms,
         _tts_recovery_correlation_id, _build_tts_generation_job_kwargs_from_meta.
  _to_epoch_ms is duplicated here (not extracted from backend_server) so that
  backend_server's 22 other callers are untouched.
  _tts_recovery_correlation_id is an intentionally narrow, no-Flask subset of
  _build_observability_correlation_id, valid only for background-job contexts
  where has_request_context() is always False.  It is NOT a replacement for
  the generic helper.

NOT moved (blocked by backend_server-only helpers):
  - _enforce_google_tts_monthly_budget  →  calls _notify_google_tts_budget_thresholds
                                            → _send_private_message (backend_server only)
  - _synthesize_mp3                     →  depends on _enforce_google_tts_monthly_budget
"""

import os
import re
import time
from uuid import uuid4


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
