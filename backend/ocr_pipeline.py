"""
OCR Intelligence Pipeline — v1

Centralized text cleaning for Shortcut/Reels/TikTok screenshot ingestion.
Runs BEFORE payload is stored to the durable ingest DB row and BEFORE
the LLM vocabulary extraction step.

Pipeline stages (ordered, each emits a structured log line):
  1. whitespace_normalization  — normalize Unicode whitespace, collapse runs
  2. structural_cleanup        — remove social-media UI noise (usernames,
                                  engagement counters, CTAs, hashtags,
                                  isolated emoji lines, timestamps)
  3. ocr_artifact_cleanup      — fix OCR misreads (mangled German quotes,
                                  excessive punctuation runs)
  4. language_detection        — detect Unicode scripts present (read-only,
                                  no mutations)

Contract:
  - run_ocr_pipeline() is a pure function: no DB, no network, no side effects.
  - Every cleanup rule has a unique name; fires a DEBUG log when it matches.
  - Returns OcrPipelineResult with cleaned text and full observability data.
  - Never raises; returns raw text unchanged on unexpected input.
"""
from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass
from typing import NamedTuple


# ---------------------------------------------------------------------------
# Public result types
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class OcrStageMetric:
    stage: str
    duration_ms: float
    chars_before: int
    chars_after: int
    rules_fired: int


@dataclass(frozen=True, slots=True)
class OcrPipelineResult:
    raw_text: str
    cleaned_text: str
    raw_size: int
    cleaned_size: int
    removed_noise_count: int
    detected_languages: tuple[str, ...]
    stage_metrics: tuple[OcrStageMetric, ...]


# ---------------------------------------------------------------------------
# Internal: cleanup rule definition and application
# ---------------------------------------------------------------------------

class _CleanupRule(NamedTuple):
    name: str
    pattern: re.Pattern[str]
    replacement: str


def _apply_rules(text: str, rules: list[_CleanupRule]) -> tuple[str, int]:
    """Apply rules sequentially. Returns (new_text, count_of_rules_that_fired)."""
    fired = 0
    for rule in rules:
        new_text = rule.pattern.sub(rule.replacement, text)
        if new_text != text:
            fired += 1
            logging.debug("ocr_pipeline_rule_fired rule=%s", rule.name)
            text = new_text
    return text, fired


# ---------------------------------------------------------------------------
# Stage 1: Whitespace normalization
# ---------------------------------------------------------------------------

_WHITESPACE_RULES: list[_CleanupRule] = [
    _CleanupRule(
        "normalize_crlf",
        re.compile(r"\r\n|\r"),
        "\n",
    ),
    _CleanupRule(
        "tab_to_space",
        re.compile(r"\t"),
        " ",
    ),
    _CleanupRule(
        "non_breaking_space",
        re.compile(r" "),
        " ",
    ),
    _CleanupRule(
        "zero_width_chars",
        re.compile(r"[​‌‍﻿]"),
        "",
    ),
    _CleanupRule(
        "collapse_multiple_spaces",
        re.compile(r"[ ]{2,}"),
        " ",
    ),
    _CleanupRule(
        "collapse_multiple_blank_lines",
        re.compile(r"\n{3,}"),
        "\n\n",
    ),
]


# ---------------------------------------------------------------------------
# Stage 2: Structural cleanup (social media UI noise)
# ---------------------------------------------------------------------------

# Broad emoji ranges used by isolated-emoji-line rule
_EMOJI_RANGE = (
    r"\U00002600-\U000027BF"   # Misc Symbols + Dingbats
    r"\U0001F000-\U0001FFFF"   # Emoticons, Misc Symbols and Pictographs, Supplemental
    r"\U0001FA00-\U0001FAFF"   # Symbols and Pictographs Extended-A
)

_STRUCTURAL_RULES: list[_CleanupRule] = [
    _CleanupRule(
        "instagram_tiktok_username",
        re.compile(r"@[A-Za-z0-9_.\-]{1,50}"),
        "",
    ),
    _CleanupRule(
        "hashtag",
        # Covers Latin, Latin Extended (À-ɏ), and Cyrillic (А-яЁё) hashtags
        re.compile(r"#[A-Za-z0-9_À-ɏА-яЁё]{1,80}", re.UNICODE),
        "",
    ),
    _CleanupRule(
        "engagement_counter_en",
        re.compile(
            r"\b\d[\d\s,.]*[KkMm]?\s*(?:likes?|comments?|shares?|views?|followers?|saves?)\b",
            re.IGNORECASE,
        ),
        "",
    ),
    _CleanupRule(
        "engagement_counter_ru",
        # "number [K/M] label" — Latin and Cyrillic K/M suffixes
        re.compile(
            r"\b\d[\d\s,.]*[KkMmКкМм]?\s*"
            r"(?:лайков?|лайк|комментари[йяев]+|репостов?|просмотров?"
            r"|подписчиков?|сохранени[йяев]+)\b",
            re.IGNORECASE | re.UNICODE,
        ),
        "",
    ),
    _CleanupRule(
        "engagement_label_colon_counter",
        # "label: number" format common in Russian social media (подписчиков: 5,2K)
        re.compile(
            r"\b(?:лайков?|лайк|комментари[йяев]+|просмотров?|подписчиков?|"
            r"сохранени[йяев]+)\s*:\s*\d[\d\s,.]*[KkMmКкМм]?\b",
            re.IGNORECASE | re.UNICODE,
        ),
        "",
    ),
    _CleanupRule(
        "standalone_km_suffixed_number",
        # Standalone K/M-suffixed engagement numbers (e.g. "2.4K", "1M") — these
        # appear in Instagram/TikTok engagement bars without explicit labels.
        # Only matches when not adjacent to other alphabetic word characters.
        re.compile(r"(?<!\w)\d[\d.]*[KkMm](?!\w)"),
        "",
    ),
    _CleanupRule(
        "share_cta_button",
        # "Share" as a social media action button (standalone or in engagement bar context).
        # Anchored to word boundary; only fires when "Share" is not part of a longer word.
        re.compile(r"(?i)\bShare\b"),
        "",
    ),
    _CleanupRule(
        "original_audio_label",
        re.compile(r"(?i)\boriginal\s+audio\b"),
        "",
    ),
    _CleanupRule(
        "tiktok_duet_stitch",
        re.compile(r"(?i)\b(?:duet|stitch)\s+with\b"),
        "",
    ),
    _CleanupRule(
        "sponsored_label",
        re.compile(r"(?i)\b(?:sponsored|реклама|promoted)\b"),
        "",
    ),
    _CleanupRule(
        "see_more_cta",
        re.compile(
            r"(?i)(?:^|\b)(?:see\s+more|mehr\s+(?:anzeigen|sehen))\s*$",
            re.UNICODE | re.MULTILINE,
        ),
        "",
    ),
    # Lines containing only emoji (3 or more), no letters from any script
    _CleanupRule(
        "isolated_emoji_only_line",
        re.compile(
            rf"^[ \t]*(?:[{_EMOJI_RANGE}][ \t]*){{3,}}$",
            re.MULTILINE,
        ),
        "",
    ),
    # Lines that are solely a video timestamp (e.g. "3:42" or "1:23:45")
    _CleanupRule(
        "standalone_timestamp_line",
        re.compile(r"^\d{1,2}:\d{2}(?::\d{2})?\s*$", re.MULTILINE),
        "",
    ),
]


# ---------------------------------------------------------------------------
# Stage 3: OCR artifact cleanup
# ---------------------------------------------------------------------------

_OCR_ARTIFACT_RULES: list[_CleanupRule] = [
    # OCR misreads German opening quote „ as ,,
    _CleanupRule(
        "double_comma_german_opening_quote",
        re.compile(r",,"),
        "„",
    ),
    # Collapse 3+ ! or ? runs to single — preserves emphasis while removing OCR noise
    _CleanupRule(
        "repeated_exclamation_collapse",
        re.compile(r"!{3,}"),
        "!",
    ),
    _CleanupRule(
        "repeated_question_collapse",
        re.compile(r"\?{3,}"),
        "?",
    ),
    # 4+ dots → standard ellipsis (3 dots)
    _CleanupRule(
        "excessive_dots_normalize",
        re.compile(r"\.{4,}"),
        "...",
    ),
    # Per-line trailing whitespace left after removals
    _CleanupRule(
        "trailing_whitespace_per_line",
        re.compile(r"[ \t]+$", re.MULTILINE),
        "",
    ),
]


# ---------------------------------------------------------------------------
# Stage 4: Language / script detection (read-only, zero mutations)
# ---------------------------------------------------------------------------

_SCRIPT_PATTERNS: dict[str, re.Pattern[str]] = {
    "latin":      re.compile(r"[A-Za-zÀ-ɏ]"),
    "cyrillic":   re.compile(r"[Ѐ-ӿ]"),
    "cjk":        re.compile(r"[一-鿿぀-ヿ豈-﫿]"),
    "arabic":     re.compile(r"[؀-ۿ]"),
    "devanagari": re.compile(r"[ऀ-ॿ]"),
}


def _detect_scripts(text: str) -> tuple[str, ...]:
    return tuple(script for script, pat in _SCRIPT_PATTERNS.items() if pat.search(text))


# ---------------------------------------------------------------------------
# Internal: collapse orphaned blank lines created by noise removal
# ---------------------------------------------------------------------------

def _collapse_blank_lines(text: str) -> str:
    lines = [line.rstrip() for line in text.split("\n")]
    result: list[str] = []
    blank_run = 0
    for line in lines:
        if not line:
            blank_run += 1
            if blank_run == 1:
                result.append("")
        else:
            blank_run = 0
            result.append(line)
    return "\n".join(result).strip()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def run_ocr_pipeline(raw_text: str, *, source: str = "unknown") -> OcrPipelineResult:
    """
    Run the OCR intelligence pipeline on raw ingested text.

    Returns OcrPipelineResult with cleaned_text and per-stage metrics.
    Never raises.
    """
    text = str(raw_text or "")
    raw_size = len(text)
    t_start = time.monotonic()

    logging.info("ocr_pipeline_start source=%s raw_size=%d", source, raw_size)

    stages: list[OcrStageMetric] = []
    total_fired = 0

    # Stage 1: whitespace normalization
    t0 = time.monotonic()
    cb = len(text)
    text, fired = _apply_rules(text, _WHITESPACE_RULES)
    ca = len(text)
    dt = (time.monotonic() - t0) * 1000
    total_fired += fired
    stages.append(OcrStageMetric("whitespace_normalization", round(dt, 2), cb, ca, fired))
    logging.info(
        "ocr_pipeline_stage stage=whitespace_normalization "
        "duration_ms=%.1f chars_before=%d chars_after=%d rules_fired=%d",
        dt, cb, ca, fired,
    )

    # Stage 2: structural cleanup
    t0 = time.monotonic()
    cb = len(text)
    text, fired = _apply_rules(text, _STRUCTURAL_RULES)
    text = _collapse_blank_lines(text)
    ca = len(text)
    dt = (time.monotonic() - t0) * 1000
    total_fired += fired
    stages.append(OcrStageMetric("structural_cleanup", round(dt, 2), cb, ca, fired))
    logging.info(
        "ocr_pipeline_stage stage=structural_cleanup "
        "duration_ms=%.1f chars_before=%d chars_after=%d rules_fired=%d",
        dt, cb, ca, fired,
    )

    # Stage 3: OCR artifact cleanup
    t0 = time.monotonic()
    cb = len(text)
    text, fired = _apply_rules(text, _OCR_ARTIFACT_RULES)
    ca = len(text)
    dt = (time.monotonic() - t0) * 1000
    total_fired += fired
    stages.append(OcrStageMetric("ocr_artifact_cleanup", round(dt, 2), cb, ca, fired))
    logging.info(
        "ocr_pipeline_stage stage=ocr_artifact_cleanup "
        "duration_ms=%.1f chars_before=%d chars_after=%d rules_fired=%d",
        dt, cb, ca, fired,
    )

    # Stage 4: language detection (read-only)
    t0 = time.monotonic()
    detected = _detect_scripts(text)
    dt = (time.monotonic() - t0) * 1000
    stages.append(OcrStageMetric("language_detection", round(dt, 2), len(text), len(text), 0))
    logging.info(
        "ocr_pipeline_stage stage=language_detection "
        "duration_ms=%.1f detected_languages=%s",
        dt, ",".join(detected) or "none",
    )

    cleaned = text
    cleaned_size = len(cleaned)
    total_ms = (time.monotonic() - t_start) * 1000

    logging.info(
        "ocr_pipeline_complete source=%s raw_size=%d cleaned_size=%d "
        "removed_noise_count=%d detected_languages=%s total_duration_ms=%.1f",
        source, raw_size, cleaned_size,
        total_fired, ",".join(detected) or "none", total_ms,
    )

    return OcrPipelineResult(
        raw_text=raw_text,
        cleaned_text=cleaned,
        raw_size=raw_size,
        cleaned_size=cleaned_size,
        removed_noise_count=total_fired,
        detected_languages=detected,
        stage_metrics=tuple(stages),
    )


# ---------------------------------------------------------------------------
# Rule registry — exported for auditing in tests
# ---------------------------------------------------------------------------

ALL_RULE_LISTS: tuple[list[_CleanupRule], ...] = (
    _WHITESPACE_RULES,
    _STRUCTURAL_RULES,
    _OCR_ARTIFACT_RULES,
)
