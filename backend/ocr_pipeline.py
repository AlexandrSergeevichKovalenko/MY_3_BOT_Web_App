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


# ============================================================================
# OCR Intelligence Pipeline — v2: Semantic Learnability Filtering
# ============================================================================
#
# Runs AFTER OCR pipeline v1 (whitespace + structural cleanup).
# Segments cleaned text into candidate units and scores each for learnability
# BEFORE the expensive LLM vocabulary extraction step.
#
# Flow:
#   cleaned_text
#       → segment_candidates()   → list[OcrCandidate]
#       → route_candidates()     → (routing, pass_list, noise_list)
#       → caller: skip LLM if routing=="skip_all_noise"; else filter noise and extract
# ============================================================================


# ---------------------------------------------------------------------------
# v2 public types
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class OcrCandidate:
    """One logical unit extracted from cleaned OCR text."""
    text: str
    line_count: int
    token_estimate: int          # rough: len(text) // 4, min 1
    detected_languages: tuple[str, ...]
    char_offset: int             # byte offset in original cleaned text (debugging)


@dataclass(frozen=True, slots=True)
class LearnabilityScore:
    """
    Learnability classification for one OcrCandidate.

    label     — "likely_learnable" | "likely_noise" | "uncertain"
    score     — weighted sum of signals, clamped to [-1.0, +1.0]
    breakdown — {dimension: contribution} for every dimension that fired
    """
    label: str
    score: float
    breakdown: dict[str, float]


# ---------------------------------------------------------------------------
# Scoring thresholds — explicit constants, referenced by tests
# ---------------------------------------------------------------------------

# score >= _LEARNABLE_THRESHOLD → "likely_learnable"
# score <= _NOISE_THRESHOLD     → "likely_noise"
# otherwise                     → "uncertain"
LEARNABLE_THRESHOLD: float = 0.2
NOISE_THRESHOLD: float = -0.2


# ---------------------------------------------------------------------------
# Known vocabulary for residual-noise detection
# ---------------------------------------------------------------------------

_ENGAGEMENT_RESIDUAL_WORDS: frozenset[str] = frozenset({
    "likes", "views", "followers", "subscribers", "shares", "comments",
    "лайков", "просмотров", "подписчиков", "комментариев",
})

_ISOLATED_CTA_WORDS: frozenset[str] = frozenset({
    "follow", "subscribe", "подписаться", "like", "comment", "share",
    "поделиться", "нравится", "repost", "репост",
})


# ---------------------------------------------------------------------------
# Internal: emoji character counter (reuses v1 ranges)
# ---------------------------------------------------------------------------

def _count_emoji_chars(text: str) -> int:
    return sum(
        1 for c in text
        if "\U00002600" <= c <= "\U000027BF"
        or "\U0001F000" <= c <= "\U0001FFFF"
        or "\U0001FA00" <= c <= "\U0001FAFF"
    )


# ---------------------------------------------------------------------------
# Scoring: _compute_signals
# ---------------------------------------------------------------------------

def _compute_signals(candidate: OcrCandidate) -> dict[str, float]:
    """
    Compute per-dimension scoring signals for one candidate.

    Each dimension contributes at most one entry to the returned dict.
    Only dimensions with a non-zero contribution are included.
    """
    text = candidate.text
    total = len(text)

    if total == 0:
        return {"text_length": -0.8}

    alpha = sum(1 for c in text if c.isalpha())

    # Hard override: no alphabetic characters — pure numeric/emoji/symbol content.
    if alpha == 0:
        return {"no_alphabetic_chars": -1.0}

    digits  = sum(1 for c in text if c.isdigit())
    emoji   = _count_emoji_chars(text)
    symbols = sum(1 for c in text if not c.isalpha() and not c.isdigit() and not c.isspace())

    alpha_ratio   = alpha   / total
    numeric_ratio = digits  / total
    emoji_ratio   = emoji   / total
    symbol_ratio  = symbols / total

    tokens = text.split()
    meaningful = [t for t in tokens if len(t) >= 3 and any(c.isalpha() for c in t)]

    signals: dict[str, float] = {}

    # ---- text_length ----
    if total < 3:
        signals["text_length"] = -0.8
    elif total < 6:
        signals["text_length"] = -0.3
    elif 15 <= total <= 300:
        signals["text_length"] = 0.15
    elif 5 <= total < 15:
        signals["text_length"] = 0.05

    # ---- alpha_ratio ----
    if alpha_ratio >= 0.7:
        signals["alpha_ratio"] = 0.2
    elif alpha_ratio >= 0.55:
        signals["alpha_ratio"] = 0.1
    elif alpha_ratio < 0.3:
        signals["alpha_ratio"] = -0.3
    elif alpha_ratio < 0.5:
        signals["alpha_ratio"] = -0.1

    # ---- emoji_ratio ----
    if emoji_ratio > 0.3:
        signals["emoji_ratio"] = -0.6
    elif emoji_ratio > 0.15:
        signals["emoji_ratio"] = -0.2

    # ---- numeric_ratio ----
    if numeric_ratio > 0.4:
        signals["numeric_ratio"] = -0.4
    elif numeric_ratio > 0.25:
        signals["numeric_ratio"] = -0.15

    # ---- symbol_density ----
    if symbol_ratio > 0.25:
        signals["symbol_density"] = -0.3
    elif symbol_ratio > 0.15:
        signals["symbol_density"] = -0.1

    # ---- all_caps_label ----
    # ALL-UPPERCASE short strings are typically UI labels, not learnable content.
    alpha_only = "".join(c for c in text if c.isalpha())
    if alpha_only and alpha_only == alpha_only.upper() and len(tokens) <= 4 and total <= 30:
        signals["all_caps_label"] = -0.5

    # ---- engagement_residual ----
    text_lower = text.lower()
    if any(w in text_lower for w in _ENGAGEMENT_RESIDUAL_WORDS):
        signals["engagement_residual"] = -0.5

    # ---- isolated_cta ----
    stripped_lower = text.strip().lower()
    if stripped_lower in _ISOLATED_CTA_WORDS:
        # Entire text is a single CTA word.
        signals["isolated_cta"] = -0.6
    elif tokens and len(tokens) <= 3 and all(t.lower() in _ISOLATED_CTA_WORDS for t in tokens):
        # Up to 3 tokens, all recognised CTA words — strong noise.
        signals["isolated_cta"] = -0.9

    # ---- url_pattern ----
    if re.search(r"https?://|www\.", text, re.IGNORECASE):
        signals["url_pattern"] = -0.5

    # ---- timestamp_pattern ----
    # Inline video timestamps (e.g. "3:42") are noise residuals, not learnable.
    if re.search(r"(?<!\w)\d{1,2}:\d{2}(?!\w)", text):
        signals["timestamp_pattern"] = -0.2

    # ---- sentence_punct ----
    # Require punctuation that is genuinely sentence-terminal: ! or ?, or a period
    # NOT surrounded by digits (to exclude decimal points like "1.2K").
    if re.search(r"[!?]|(?<!\d)\.(?!\d)", text):
        signals["sentence_punct"] = 0.2

    # ---- pedagogical_marker ----
    # Definition separators (→ — –) or explicit grammar case labels.
    if re.search(r"[→—–]|[:=](?!\d)", text) or re.search(
        r"\b(?:Akkusativ|Dativ|Nominativ|Genitiv)\b", text, re.IGNORECASE
    ):
        signals["pedagogical_marker"] = 0.25

    # ---- verb_structure ----
    # German infinitives end in -en, -ieren, -eln, -ern.
    if re.search(r"\b[A-Za-zÄÖÜäöüß]{3,}(?:ieren|eln|ern|en)\b", text, re.UNICODE):
        signals["verb_structure"] = 0.15

    # ---- noun_capitalization ----
    # German nouns are capitalised; ≥2 such words is a strong signal for real German text.
    if len(re.findall(r"\b[A-ZÄÖÜ][a-zäöüß]{2,}\b", text)) >= 2:
        signals["noun_capitalization"] = 0.1

    # ---- token_count ----
    if len(meaningful) >= 3:
        signals["token_count"] = 0.15
    elif len(meaningful) >= 2:
        signals["token_count"] = 0.07

    return signals


# ---------------------------------------------------------------------------
# Public: score_learnability
# ---------------------------------------------------------------------------

def score_learnability(candidate: OcrCandidate) -> LearnabilityScore:
    """
    Score one OcrCandidate for learnability.

    Returns LearnabilityScore with:
      label     — "likely_learnable" | "likely_noise" | "uncertain"
      score     — sum of fired signals, clamped to [-1.0, +1.0]
      breakdown — per-dimension contributions (only fired dimensions)
    """
    breakdown = _compute_signals(candidate)
    raw = sum(breakdown.values())
    score = max(-1.0, min(1.0, raw))

    if score >= LEARNABLE_THRESHOLD:
        label = "likely_learnable"
    elif score <= NOISE_THRESHOLD:
        label = "likely_noise"
    else:
        label = "uncertain"

    logging.debug(
        "ocr_candidate_scored label=%s score=%.3f breakdown=%s text_preview=%r",
        label, score, breakdown, candidate.text[:40],
    )
    return LearnabilityScore(label=label, score=round(score, 4), breakdown=breakdown)


# ---------------------------------------------------------------------------
# Public: segment_candidates
# ---------------------------------------------------------------------------

def segment_candidates(text: str) -> list[OcrCandidate]:
    """
    Split cleaned OCR text into ordered candidate units.

    Blank lines (2+ consecutive newlines) create hard segment boundaries.
    Consecutive non-blank lines within a paragraph are kept together to
    preserve multiline subtitle coherence.

    Returns at least one candidate even when no blank lines are found.
    """
    raw = str(text or "").strip()
    if not raw:
        return []

    paragraphs = re.split(r"\n{2,}", raw)
    candidates: list[OcrCandidate] = []
    search_from = 0

    for para in paragraphs:
        para = para.strip()
        if not para:
            continue
        char_offset = raw.find(para, search_from)
        if char_offset < 0:
            char_offset = search_from
        lines = [ln for ln in para.splitlines() if ln.strip()]
        detected = _detect_scripts(para)
        cand = OcrCandidate(
            text=para,
            line_count=len(lines),
            token_estimate=max(1, len(para) // 4),
            detected_languages=detected,
            char_offset=char_offset,
        )
        logging.debug(
            "ocr_candidate_created line_count=%d token_estimate=%d "
            "detected_languages=%s text_preview=%r",
            cand.line_count, cand.token_estimate,
            ",".join(detected) or "none", para[:40],
        )
        candidates.append(cand)
        search_from = char_offset + len(para)

    if not candidates:
        return [OcrCandidate(
            text=raw,
            line_count=len([ln for ln in raw.splitlines() if ln.strip()]),
            token_estimate=max(1, len(raw) // 4),
            detected_languages=_detect_scripts(raw),
            char_offset=0,
        )]
    return candidates


# ---------------------------------------------------------------------------
# Public: route_candidates
# ---------------------------------------------------------------------------

def route_candidates(
    candidates: list[OcrCandidate],
) -> tuple[
    str,
    list[tuple[OcrCandidate, LearnabilityScore]],
    list[tuple[OcrCandidate, LearnabilityScore]],
]:
    """
    Score every candidate and split into pass and noise lists.

    Returns:
      routing  — "skip_all_noise" if every candidate is noise, else "proceed"
      pass_list  — [(candidate, score), ...] for non-noise candidates
      noise_list — [(candidate, score), ...] for likely_noise candidates
    """
    pass_list:  list[tuple[OcrCandidate, LearnabilityScore]] = []
    noise_list: list[tuple[OcrCandidate, LearnabilityScore]] = []

    for cand in candidates:
        lscore = score_learnability(cand)
        if lscore.label == "likely_noise":
            logging.info(
                "ocr_candidate_skipped_noise candidate_length=%d token_estimate=%d "
                "score=%.3f breakdown=%s text_preview=%r",
                len(cand.text), cand.token_estimate,
                lscore.score, lscore.breakdown, cand.text[:60],
            )
            noise_list.append((cand, lscore))
        else:
            pass_list.append((cand, lscore))

    routing = "skip_all_noise" if not pass_list else "proceed"
    return routing, pass_list, noise_list
