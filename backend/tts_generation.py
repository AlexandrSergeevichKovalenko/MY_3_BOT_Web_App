"""
TTS generation helpers that carry no backend_server dependency.

Slice 1 — pure constants, normalisation utilities, and the budget-blocked
exception.  Nothing here imports backend_server; it is safe to import from
any worker process.

NOT moved in Slice 1 (blocked by backend_server-only helpers):
  - _enforce_google_tts_monthly_budget  →  calls _notify_google_tts_budget_thresholds
                                            → _send_private_message (backend_server only)
  - _synthesize_mp3                     →  depends on _enforce_google_tts_monthly_budget
  - _build_tts_generation_job_kwargs_from_meta
                                        →  calls _build_observability_correlation_id
                                           and _to_epoch_ms (generic backend_server helpers
                                           with 50+ callers across other subsystems)
"""

import os
import re


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
# R2 object-key prefix (read once at import time, same as backend_server.py)
# ---------------------------------------------------------------------------

TTS_OBJECT_PREFIX = str(os.getenv("TTS_OBJECT_PREFIX") or "tts").strip().strip("/") or "tts"

# ---------------------------------------------------------------------------
# Pure helpers (previously private to backend_server)
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


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class GoogleTTSBudgetBlockedError(RuntimeError):
    def __init__(self, message: str, *, payload: dict | None = None):
        super().__init__(message)
        self.payload = dict(payload or {})
