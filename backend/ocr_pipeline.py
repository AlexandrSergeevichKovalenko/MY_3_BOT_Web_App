"""
OCR Intelligence Pipeline — v1 / v2 / v3

Centralized text cleaning for Shortcut/Reels/TikTok screenshot ingestion.
Runs BEFORE payload is stored to the durable ingest DB row and BEFORE
the LLM vocabulary extraction step.

Pipeline stages (ordered, each emits a structured log line):
  1. whitespace_normalization      — normalize Unicode whitespace, collapse runs
  2. structural_cleanup            — remove social-media UI noise (usernames,
                                      engagement counters, CTAs, hashtags,
                                      isolated emoji lines, timestamps,
                                      bare numeric orphan lines)
  3. ocr_artifact_cleanup          — fix OCR misreads (mangled German quotes,
                                      excessive punctuation runs)
  4. self_ingestion_suppression    — strip lines that are our own bot-generated
                                      UI wrappers (query labels, section headers,
                                      dividers) before candidate segmentation
  5. language_detection            — detect Unicode scripts present (read-only,
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
    # Lines containing only a bare number (1–5 digits, optionally parenthesized).
    # These are OCR artefacts: engagement positions, list indices, partially visible
    # counters.  "3 neue Wörter" is safe — the alpha chars prevent a match.
    _CleanupRule(
        "isolated_number_line",
        re.compile(r"(?m)^[ \t]*\(?[ \t]*\d{1,5}[ \t]*\)?[ \t]*$"),
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

    # Stage 4: self-ingestion suppression
    t0 = time.monotonic()
    cb = len(text)
    text, sup_names = _apply_self_ingestion_suppression(text)
    text = _collapse_blank_lines(text)
    ca = len(text)
    dt = (time.monotonic() - t0) * 1000
    sup_count = len(sup_names)
    total_fired += sup_count
    stages.append(OcrStageMetric("self_ingestion_suppression", round(dt, 2), cb, ca, sup_count))
    logging.info(
        "ocr_pipeline_stage stage=self_ingestion_suppression "
        "duration_ms=%.1f chars_before=%d chars_after=%d suppressed_lines=%d patterns=%s",
        dt, cb, ca, sup_count, ",".join(sup_names) or "none",
    )

    # Stage 5: language detection (read-only)
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
# OCR Pipeline v3 — §1: Self-Ingestion Suppression
# ============================================================================
#
# Users sometimes screenshot Telegram conversations that include messages our
# own bot sent.  The OCR then re-ingests our transport wrappers (query labels,
# card section headers, dividers) as if they were learnable content.
#
# This layer strips those lines BEFORE candidate segmentation so they never
# reach the learnability scorer or the LLM extraction step.
#
# Vocabulary is centralized here — no scattered regex fragments elsewhere.
# Every suppressed line is observable via the self_ingestion_suppression stage
# metric and structured log entries.
# ============================================================================

class _SelfIngestionPattern(NamedTuple):
    name: str
    pattern: re.Pattern


_SELF_INGESTION_PATTERNS: tuple[_SelfIngestionPattern, ...] = (
    # "Запрос: <word>" — shortcut query delivery wrapper (bot → user)
    _SelfIngestionPattern(
        "query_wrapper_ru",
        re.compile(r"^\s*Запрос:\s*\S", re.IGNORECASE),
    ),
    # "Выберите языковую пару для перевода:" — translation pair selector prompt
    _SelfIngestionPattern(
        "language_pair_selector_ru",
        re.compile(r"Выберите языковую пару для перевода", re.IGNORECASE),
    ),
    # "🧠 Feel the Word" — vocabulary card section header
    _SelfIngestionPattern(
        "feel_the_word_header",
        re.compile(r"Feel the Word"),
    ),
    # "✨ Слово и перевод" — vocabulary card word/translation section
    _SelfIngestionPattern(
        "bot_word_translation_header",
        re.compile(r"✨\s*Слово и перевод"),
    ),
    # "📚 Разбор" — vocabulary card breakdown section (emoji prefix required to
    # avoid false positives on the standalone Russian word "разбор")
    _SelfIngestionPattern(
        "bot_breakdown_section",
        re.compile(r"📚\s*Разбор"),
    ),
    # "━━━━" — horizontal divider used in bot card formatting
    _SelfIngestionPattern(
        "bot_horizontal_divider",
        re.compile(r"^\s*━{3,}\s*$"),
    ),
    # "🌐 DE → RU" — language pair indicator line
    _SelfIngestionPattern(
        "bot_language_pair_label",
        re.compile(r"🌐\s*[A-Z]{2,3}\s*→\s*[A-Z]{2,3}"),
    ),
    # "Оцени ответ кнопкой ниже" — rating prompt line
    _SelfIngestionPattern(
        "bot_rate_answer_prompt_ru",
        re.compile(r"Оцени ответ кнопкой ниже", re.IGNORECASE),
    ),
)


def _apply_self_ingestion_suppression(text: str) -> tuple[str, list[str]]:
    """
    Strip lines that match known bot-generated wrapper vocabulary.

    Returns (cleaned_text, fired_pattern_names).
    fired_pattern_names contains one entry per suppressed line (duplicates allowed).
    """
    fired: list[str] = []
    lines = text.split("\n")
    kept: list[str] = []
    for line in lines:
        matched_name: str | None = None
        for pat in _SELF_INGESTION_PATTERNS:
            if pat.pattern.search(line):
                matched_name = pat.name
                break
        if matched_name is not None:
            fired.append(matched_name)
        else:
            kept.append(line)
    return "\n".join(kept), fired


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

    alpha  = sum(1 for c in text if c.isalpha())
    digits = sum(1 for c in text if c.isdigit())

    # Hard override: no alphabetic characters at all.
    if alpha == 0:
        # Distinguish numeric orphan fragments from emoji/symbol noise so
        # observability logs carry the right category name.
        non_numeric = re.sub(r'[\s\d(),.+-]', '', text)
        if not non_numeric:
            return {"numeric_orphan": -1.0}
        return {"no_alphabetic_chars": -1.0}
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

    # ---- underscore_artifact ----
    # Tokens with internal underscores (e.g. "frau_deuen") are typically OCR
    # corruption artefacts — subtitle boundary merges or partial username fragments.
    # Only fires when at least 2 alpha chars flank each side of the underscore.
    # Weight -0.55: strong enough to push an otherwise learnable sentence into
    # "uncertain" territory, signalling reduced extraction confidence.
    if re.search(r"\b[A-Za-zÄÖÜäöüßА-ЯЁа-яёЇїІіЄєҐґ]{2,}_[A-Za-zÄÖÜäöüßА-ЯЁа-яёЇїІіЄєҐґ]{2,}\b", text, re.UNICODE):
        signals["underscore_artifact"] = -0.55

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


# ---------------------------------------------------------------------------
# Public: build_extraction_text
# ---------------------------------------------------------------------------

def build_extraction_text(
    pass_list: list[tuple[OcrCandidate, LearnabilityScore]],
) -> str:
    """
    Assemble extraction text from explicitly-routed pass candidates.

    Raises ValueError if pass_list is empty — callers must not invoke this
    function after a skip_all_noise routing decision.  Passing an empty list
    here means the caller bypassed the routing contract.
    """
    if not pass_list:
        raise ValueError(
            "build_extraction_text: pass_list is empty — "
            "routing contract violation: 'proceed' routing requires non-empty pass_list"
        )
    return "\n\n".join(c.text for c, _ in pass_list)


# ============================================================================
# OCR Pipeline v3 — §3: Context Reconstruction Scaffold
# ============================================================================
#
# Advisory relationship signals between adjacent candidates.
# Does NOT filter or modify candidates — purely informational.
# Designed to support future context-aware reconstruction: question/answer
# pairing, language transition detection, subtitle continuation grouping.
#
# Usage:
#   candidates = segment_candidates(text)
#   signals = annotate_context(candidates)
#   # signals is a list of OcrContextSignal; empty = no relationships found
# ============================================================================

@dataclass(frozen=True, slots=True)
class OcrContextSignal:
    """
    Advisory relationship signal between two adjacent OCR candidates.
    candidate_index refers to the left/source candidate in the pair.
    """
    candidate_index: int
    signal_type: str   # "question_before_answer" | "language_transition_pair"
    detail: str        # human-readable description for logging/debugging


def annotate_context(candidates: list[OcrCandidate]) -> list[OcrContextSignal]:
    """
    Detect advisory relationship signals between adjacent candidates.

    Returns a list of OcrContextSignal; empty means no relationships found.
    Does not modify candidates, does not filter, does not route.
    """
    result: list[OcrContextSignal] = []
    for i in range(len(candidates) - 1):
        left  = candidates[i]
        right = candidates[i + 1]

        left_langs  = frozenset(left.detected_languages)
        right_langs = frozenset(right.detected_languages)

        # Question/answer adjacency: left ends with "?" and right is a different language
        if left.text.rstrip().endswith("?") and left_langs != right_langs:
            result.append(OcrContextSignal(
                candidate_index=i,
                signal_type="question_before_answer",
                detail=(
                    f"candidate[{i}] ends with '?' in {sorted(left_langs)}; "
                    f"candidate[{i+1}] is in {sorted(right_langs)}"
                ),
            ))

        # Language transition pair: adjacent candidates in different scripts
        if left_langs and right_langs and left_langs != right_langs:
            result.append(OcrContextSignal(
                candidate_index=i,
                signal_type="language_transition_pair",
                detail=(
                    f"candidate[{i}] scripts={sorted(left_langs)}; "
                    f"candidate[{i+1}] scripts={sorted(right_langs)}"
                ),
            ))

    return result


# ============================================================================
# OCR Pipeline v3 — §4: Educational Layout Archetype Detection
#                        & Spatial Grouping Preservation
# ============================================================================
#
# Detects the educational layout archetype of cleaned OCR text and groups
# lines into spatial/semantic units BEFORE extraction.
#
# Primary target: German learning content.
# German morphology signals get priority weight in every heuristic.
# English/Russian/Ukrainian appear only as translation metadata / support.
#
# Flow (called from delivery after v1 cleanup, before LLM extraction):
#   cleaned_text
#       → classify_archetype()             → OcrArchetypeResult
#       → group_spatially()                → list[OcrSpatialGroup]
#       → build_grouped_extraction_payload → OcrGroupedPayload
#       → observability logs
#       → (archetype-specific extraction routing — future slice)
#
# Contract:
#   - Both classify_archetype and group_spatially are pure functions.
#   - No LLM, no network, no side effects.
#   - No hallucinated pairings — only structure that is directly observable.
#   - build_grouped_extraction_payload raises on empty groups (no flat fallback).
# ============================================================================

# ---------------------------------------------------------------------------
# v3 §4: Constants
# ---------------------------------------------------------------------------

ARCHETYPE_VOCABULARY_PAIR       = "vocabulary_pair"
ARCHETYPE_MULTILINGUAL_STACK    = "multilingual_stack"
ARCHETYPE_BILINGUAL_PHRASE      = "bilingual_phrase_overlay"
ARCHETYPE_GRAMMAR_BOARD         = "grammar_board"
ARCHETYPE_SUBTITLE_DIALOGUE     = "subtitle_dialogue"
ARCHETYPE_EDUCATIONAL_LIST      = "educational_list"
ARCHETYPE_UNKNOWN_MIXED         = "unknown_mixed"

GROUP_TYPE_HORIZONTAL_PAIR              = "horizontal_pair"
GROUP_TYPE_VERTICAL_TRANSLATION_STACK   = "vertical_translation_stack"
GROUP_TYPE_GRAMMAR_CLUSTER              = "grammar_cluster"
GROUP_TYPE_SUBTITLE_CLUSTER             = "subtitle_cluster"
GROUP_TYPE_EDUCATIONAL_LIST_CLUSTER     = "educational_list_cluster"
GROUP_TYPE_ISOLATED_PHRASE              = "isolated_phrase"
GROUP_TYPE_UNKNOWN_CLUSTER              = "unknown_cluster"

ALL_ARCHETYPES: tuple[str, ...] = (
    ARCHETYPE_VOCABULARY_PAIR,
    ARCHETYPE_MULTILINGUAL_STACK,
    ARCHETYPE_BILINGUAL_PHRASE,
    ARCHETYPE_GRAMMAR_BOARD,
    ARCHETYPE_SUBTITLE_DIALOGUE,
    ARCHETYPE_EDUCATIONAL_LIST,
    ARCHETYPE_UNKNOWN_MIXED,
)

ALL_GROUP_TYPES: tuple[str, ...] = (
    GROUP_TYPE_HORIZONTAL_PAIR,
    GROUP_TYPE_VERTICAL_TRANSLATION_STACK,
    GROUP_TYPE_GRAMMAR_CLUSTER,
    GROUP_TYPE_SUBTITLE_CLUSTER,
    GROUP_TYPE_EDUCATIONAL_LIST_CLUSTER,
    GROUP_TYPE_ISOLATED_PHRASE,
    GROUP_TYPE_UNKNOWN_CLUSTER,
)


# ---------------------------------------------------------------------------
# v3 §4: Result types
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class OcrSpatialGroup:
    """
    A semantically coherent group of lines from OCR text.
    Preserves observable layout structure — does not filter or reorder.
    """
    group_id: int
    group_type: str             # one of GROUP_TYPE_* constants
    lines: tuple[str, ...]
    ordering: int               # 0-based position among all groups
    confidence: float           # 0.0..1.0
    signals: dict[str, float]   # named signals that drove classification
    german_target_detected: bool


@dataclass(frozen=True, slots=True)
class OcrArchetypeResult:
    """
    Result of educational layout archetype classification.
    German content is always the primary extraction target;
    other languages are support metadata.
    """
    archetype: str              # one of ARCHETYPE_* constants
    confidence: float           # 0.0..1.0
    signals: dict[str, float]
    german_target_count: int    # distinct German morphology signals detected
    support_language_count: int # non-Latin scripts present (Cyrillic etc.)


@dataclass(frozen=True, slots=True)
class OcrGroupedPayload:
    """
    Archetype + spatial groups combined for extraction routing.
    No fallback to flat text — extraction must use grouped structure.
    """
    archetype_result: OcrArchetypeResult
    groups: tuple[OcrSpatialGroup, ...]
    candidate_count: int
    german_group_count: int


# ---------------------------------------------------------------------------
# v3 §4: German morphology detector
# ---------------------------------------------------------------------------

_FLAG_EMOJI_RE = re.compile(r"[\U0001F1E6-\U0001F1FF]{2}")

_AUSTRIAN_DIALECT_TERMS: frozenset[str] = frozenset({
    "fesch", "oida", "gfrast", "baba", "griaß", "servus", "pfoah",
    "leiwand", "pfiat", "heast", "jo", "na",
})


def _detect_german_morphology(text: str) -> dict[str, float]:
    """
    Detect German-specific morphological signals.

    Returns a dict of signal_name → strength (0.0..1.0).
    Empty dict means no German morphology detected.
    German content is the primary extraction target; signals here give it
    priority weight in archetype and grouping classification.
    """
    sigs: dict[str, float] = {}
    if not text.strip():
        return sigs

    text_lower = text.lower()

    # Umlaut presence — near-unique to German
    umlaut_count = sum(1 for c in text if c in "äöüÄÖÜß")
    if umlaut_count > 0:
        sigs["umlaut_present"] = min(1.0, umlaut_count * 0.25)

    # -ieren infinitive is almost exclusively German
    if re.search(r"\b\w{3,}ieren\b", text_lower):
        sigs["infinitive_ieren"] = 0.9
    elif re.search(r"\b[a-zäöüß]{3,}en\b", text_lower):
        sigs["infinitive_en"] = 0.5

    # Partizip Perfekt: auxiliary + ge- form
    if re.search(r"\b(?:ist|hat|sind|haben|bin|habe)\s+ge\w{3,}", text_lower):
        sigs["partizip_perfekt"] = 0.95
    elif re.search(r"\bge[a-zäöüß]{3,}(?:en|t)\b", text_lower):
        sigs["ge_participle"] = 0.7

    # German noun capitalization (≥2 mid-sentence capitalized words)
    cap_words = re.findall(r"(?<![.!?\n])\s+([A-ZÄÖÜ][a-zäöüß]{2,})\b", text)
    if len(cap_words) >= 2:
        sigs["noun_capitalization"] = 0.6
    elif len(cap_words) == 1:
        sigs["noun_capitalization_single"] = 0.3

    # Austrian dialect vocabulary
    words_lower = re.findall(r"\b[a-zäöüß]+\b", text_lower)
    if any(w in _AUSTRIAN_DIALECT_TERMS for w in words_lower):
        sigs["austrian_dialect"] = 1.0

    # Separable verb prefixes (common German-specific morphology)
    if re.search(
        r"\b(?:ab|an|auf|aus|bei|durch|ein|fort|hin|los|mit|nach|vor|weg|zu|zurück)\w{3,}\b",
        text_lower,
    ):
        sigs["separable_verb_prefix"] = 0.45

    # Long compound nouns (Donaudampfschifffahrtsgesellschaft style)
    if re.search(r"\b[A-ZÄÖÜ][a-zäöüß]{10,}\b", text):
        sigs["compound_noun"] = 0.5

    return sigs


# ---------------------------------------------------------------------------
# v3 §4: Archetype scoring functions (operate on ALL non-empty lines)
# ---------------------------------------------------------------------------

def _score_archetype_vocabulary_pair(
    lines: list[str],
) -> tuple[float, dict[str, float]]:
    sigs: dict[str, float] = {}
    if not lines:
        return 0.0, sigs

    arrow_lines = [l for l in lines if "↔" in l]
    if arrow_lines:
        sigs["bidirectional_arrow_count"] = float(len(arrow_lines))
        return min(1.0, 0.75 + len(arrow_lines) * 0.1), sigs

    pipe_lines = [l for l in lines if " | " in l]
    if pipe_lines:
        sigs["pipe_separator_count"] = float(len(pipe_lines))
        return 0.8, sigs

    if len(lines) == 2:
        t0, t1 = lines[0].split(), lines[1].split()
        if len(t0) <= 3 and len(t1) <= 3:
            g0 = bool(_detect_german_morphology(lines[0]))
            g1 = bool(_detect_german_morphology(lines[1]))
            if g0 != g1:
                sigs["bilingual_short_pair"] = 0.65
                return 0.65, sigs
            sigs["short_aligned_lines"] = 0.35
            return 0.35, sigs

    return 0.05, sigs


def _score_archetype_multilingual_stack(
    lines: list[str],
) -> tuple[float, dict[str, float]]:
    sigs: dict[str, float] = {}
    if len(lines) < 2:
        return 0.0, sigs

    flag_count = sum(1 for l in lines if _FLAG_EMOJI_RE.search(l))
    if flag_count >= 3:
        sigs["flag_emoji_stack"] = float(flag_count)
        return 0.95, sigs
    if flag_count == 2:
        sigs["flag_emoji_pair"] = 2.0
        return 0.85, sigs

    return 0.05, sigs


def _score_archetype_bilingual_phrase(
    lines: list[str],
) -> tuple[float, dict[str, float]]:
    sigs: dict[str, float] = {}
    if len(lines) < 2 or len(lines) > 5:
        return 0.0, sigs

    german_idx = [i for i, l in enumerate(lines) if _detect_german_morphology(l)]
    cyrillic_idx = [i for i, l in enumerate(lines) if "cyrillic" in _detect_scripts(l)]

    if german_idx and cyrillic_idx:
        sigs["german_cyrillic_pair"] = 1.0
        sigs["german_line_count"] = float(len(german_idx))
        return 0.90, sigs

    all_scripts: set[str] = set()
    for l in lines:
        all_scripts.update(_detect_scripts(l))
    if "cyrillic" in all_scripts and "latin" in all_scripts:
        sigs["cyrillic_latin_mix"] = 0.6
        return 0.55, sigs

    return 0.05, sigs


def _score_archetype_grammar_board(
    lines: list[str],
) -> tuple[float, dict[str, float]]:
    sigs: dict[str, float] = {}
    if len(lines) < 2:
        return 0.0, sigs

    text = "\n".join(lines)
    if re.search(r"\b(?:ist|hat|sind|haben|bin|habe)\s+ge\w{3,}", text, re.IGNORECASE):
        sigs["partizip_perfekt"] = 1.0
        return 0.92, sigs

    german_count = sum(1 for l in lines if _detect_german_morphology(l))
    short_count  = sum(1 for l in lines if len(l.split()) <= 4)

    if german_count >= 2 and short_count >= 2 and len(lines) <= 6:
        sigs["multi_german_short_lines"] = german_count / len(lines)
        return min(0.80, 0.40 + german_count * 0.15), sigs

    return 0.05, sigs


def _score_archetype_subtitle_dialogue(
    lines: list[str],
) -> tuple[float, dict[str, float]]:
    sigs: dict[str, float] = {}
    long_lines = [l for l in lines if len(l.split()) >= 5]
    if len(long_lines) < 2:
        return 0.0, sigs

    sigs["long_sentence_count"] = float(len(long_lines))
    script_sets = [frozenset(_detect_scripts(l)) for l in lines if l.strip()]
    if script_sets and len(set(script_sets)) <= 2:
        sigs["consistent_script"] = 0.8
        return min(0.85, 0.50 + len(long_lines) * 0.07), sigs

    return 0.45, sigs


def _score_archetype_educational_list(
    lines: list[str],
) -> tuple[float, dict[str, float]]:
    sigs: dict[str, float] = {}
    if len(lines) < 3:
        return 0.0, sigs

    short_lines = [l for l in lines if 1 <= len(l.split()) <= 5]
    if len(short_lines) < 3:
        return 0.0, sigs

    sigs["short_list_density"] = len(short_lines) / len(lines)

    first_words = [l.split()[0].lower() if l.split() else "" for l in lines]
    mode_fw  = max(set(first_words), key=first_words.count) if first_words else ""
    mode_cnt = first_words.count(mode_fw)
    if mode_cnt >= 3 and mode_fw:
        sigs["common_prefix"] = mode_cnt / len(lines)
        return min(0.88, 0.60 + mode_cnt * 0.05), sigs

    german_count = sum(1 for l in lines if _detect_german_morphology(l))
    if german_count >= max(3, len(lines) // 2):
        sigs["german_list_items"] = german_count / len(lines)
        return 0.60, sigs

    return 0.30, sigs


# ---------------------------------------------------------------------------
# v3 §4: Public — classify_archetype
# ---------------------------------------------------------------------------

def classify_archetype(text: str) -> OcrArchetypeResult:
    """
    Classify the educational layout archetype of cleaned OCR text.

    Deterministic, pure, no LLM. Returns ARCHETYPE_UNKNOWN_MIXED when
    no archetype reaches 0.30 confidence.

    German morphology is the primary priority signal — when German content
    is detected, archetype confidence is boosted to reflect German-target
    semantics.
    """
    if not text.strip():
        return OcrArchetypeResult(
            archetype=ARCHETYPE_UNKNOWN_MIXED,
            confidence=0.0,
            signals={},
            german_target_count=0,
            support_language_count=0,
        )

    lines = [l.strip() for l in text.split("\n") if l.strip()]

    german_sigs   = _detect_german_morphology(text)
    german_target_count = len(german_sigs)

    all_scripts = set(_detect_scripts(text))
    # Support languages = non-Latin scripts (Latin covers German + English + French etc.)
    support_language_count = len(all_scripts - {"latin"})

    scorers = [
        (ARCHETYPE_VOCABULARY_PAIR,    _score_archetype_vocabulary_pair),
        (ARCHETYPE_MULTILINGUAL_STACK, _score_archetype_multilingual_stack),
        (ARCHETYPE_BILINGUAL_PHRASE,   _score_archetype_bilingual_phrase),
        (ARCHETYPE_GRAMMAR_BOARD,      _score_archetype_grammar_board),
        (ARCHETYPE_SUBTITLE_DIALOGUE,  _score_archetype_subtitle_dialogue),
        (ARCHETYPE_EDUCATIONAL_LIST,   _score_archetype_educational_list),
    ]

    best_archetype = ARCHETYPE_UNKNOWN_MIXED
    best_score     = 0.0
    best_sigs: dict[str, float] = {}

    for name, scorer in scorers:
        score, sigs = scorer(lines)
        if score > best_score:
            best_score     = score
            best_archetype = name
            best_sigs      = sigs

    if best_score < 0.30:
        best_archetype = ARCHETYPE_UNKNOWN_MIXED

    # German morphology corroboration: slight boost when German is present
    if german_target_count >= 2 and best_archetype != ARCHETYPE_UNKNOWN_MIXED:
        best_sigs = dict(best_sigs)
        best_sigs["german_morphology_corroboration"] = min(1.0, german_target_count * 0.15)
        best_score = min(1.0, best_score + 0.05)

    return OcrArchetypeResult(
        archetype=best_archetype,
        confidence=round(best_score, 4),
        signals=dict(best_sigs),
        german_target_count=german_target_count,
        support_language_count=support_language_count,
    )


# ---------------------------------------------------------------------------
# v3 §4: Group scoring helpers (operate on one blank-line-separated block)
# ---------------------------------------------------------------------------

def _score_group_horizontal_pair(
    lines: list[str],
) -> tuple[float, dict[str, float]]:
    sigs: dict[str, float] = {}
    if not 2 <= len(lines) <= 4:
        return 0.0, sigs

    for line in lines:
        if "↔" in line:
            sigs["bidirectional_arrow"] = 1.0
            return 0.95, sigs
        if " | " in line:
            sigs["pipe_separator"] = 0.9
            return 0.88, sigs

    if len(lines) == 2:
        t0, t1 = lines[0].split(), lines[1].split()
        if len(t0) <= 3 and len(t1) <= 3:
            g0 = bool(_detect_german_morphology(lines[0]))
            g1 = bool(_detect_german_morphology(lines[1]))
            if g0 != g1:
                sigs["bilingual_short_pair"] = 0.70
                return 0.70, sigs
            sigs["short_aligned_lines"] = 0.38
            return 0.38, sigs

    return 0.0, sigs


def _score_group_translation_stack(
    lines: list[str],
) -> tuple[float, dict[str, float]]:
    sigs: dict[str, float] = {}
    if not 2 <= len(lines) <= 6:
        return 0.0, sigs

    flag_count = sum(1 for l in lines if _FLAG_EMOJI_RE.search(l))
    if flag_count >= 2:
        sigs["flag_emoji_stack"] = float(flag_count)
        return min(0.95, 0.80 + flag_count * 0.05), sigs

    script_changes = 0
    for i in range(len(lines) - 1):
        sa = set(_detect_scripts(lines[i]))
        sb = set(_detect_scripts(lines[i + 1]))
        if sa != sb:
            script_changes += 1

    if script_changes >= 1:
        sigs["script_change_count"] = float(script_changes)
        german_present = any(_detect_german_morphology(l) for l in lines)
        if german_present:
            sigs["german_target_in_stack"] = 0.9
            return 0.80, sigs
        return 0.55, sigs

    return 0.0, sigs


def _score_group_grammar_cluster(
    lines: list[str],
) -> tuple[float, dict[str, float]]:
    sigs: dict[str, float] = {}
    if not 2 <= len(lines) <= 6:
        return 0.0, sigs

    text = "\n".join(lines)
    if re.search(r"\b(?:ist|hat|sind|haben|bin|habe)\s+ge\w{3,}", text, re.IGNORECASE):
        sigs["partizip_perfekt"] = 1.0
        return 0.93, sigs

    german_count = sum(1 for l in lines if _detect_german_morphology(l))
    short_count  = sum(1 for l in lines if len(l.split()) <= 4)

    if german_count >= 2 and short_count >= 2:
        sigs["german_morphology_short_lines"] = german_count / len(lines)
        return min(0.78, 0.45 + german_count * 0.12), sigs

    return 0.0, sigs


def _score_group_subtitle_cluster(
    lines: list[str],
) -> tuple[float, dict[str, float]]:
    sigs: dict[str, float] = {}
    long_count = sum(1 for l in lines if len(l.split()) >= 5)
    if long_count < 2:
        return 0.0, sigs

    sigs["long_line_count"] = float(long_count)
    all_scripts_set: set[str] = set()
    for l in lines:
        all_scripts_set.update(_detect_scripts(l))
    if len(all_scripts_set) <= 2:
        sigs["consistent_script"] = 0.8
        return min(0.82, 0.50 + long_count * 0.08), sigs

    return 0.45, sigs


def _score_group_educational_list(
    lines: list[str],
) -> tuple[float, dict[str, float]]:
    sigs: dict[str, float] = {}
    if len(lines) < 3:
        return 0.0, sigs

    short_count = sum(1 for l in lines if 1 <= len(l.split()) <= 5)
    if short_count < 3:
        return 0.0, sigs

    sigs["short_list_density"] = short_count / len(lines)

    first_words = [l.split()[0].lower() if l.split() else "" for l in lines]
    mode_fw  = max(set(first_words), key=first_words.count) if first_words else ""
    mode_cnt = first_words.count(mode_fw)
    if mode_cnt >= 3 and mode_fw:
        prefix_density = mode_cnt / len(lines)
        sigs["common_prefix"] = prefix_density
        # Strong prefix gives educational_list a clear edge over grammar_cluster
        # (which caps at 0.78 without partizip_perfekt).
        return min(0.83, 0.63 + mode_cnt * 0.06), sigs

    return 0.40, sigs


# ---------------------------------------------------------------------------
# v3 §4: Internal — classify one block into a spatial group
# ---------------------------------------------------------------------------

def _classify_block_as_group(
    lines: list[str],
    group_id: int,
) -> OcrSpatialGroup:
    german_sigs    = _detect_german_morphology("\n".join(lines))
    german_detected = bool(german_sigs)

    if not lines:
        return OcrSpatialGroup(
            group_id=group_id, group_type=GROUP_TYPE_UNKNOWN_CLUSTER,
            lines=(), ordering=group_id, confidence=0.0,
            signals={}, german_target_detected=False,
        )

    if len(lines) == 1:
        all_sigs = {f"german_{k}": v for k, v in german_sigs.items()}
        return OcrSpatialGroup(
            group_id=group_id, group_type=GROUP_TYPE_ISOLATED_PHRASE,
            lines=(lines[0],), ordering=group_id, confidence=0.70,
            signals=all_sigs, german_target_detected=german_detected,
        )

    scorer_results: list[tuple[str, float, dict[str, float]]] = [
        (GROUP_TYPE_HORIZONTAL_PAIR,            *_score_group_horizontal_pair(lines)),
        (GROUP_TYPE_VERTICAL_TRANSLATION_STACK, *_score_group_translation_stack(lines)),
        (GROUP_TYPE_GRAMMAR_CLUSTER,            *_score_group_grammar_cluster(lines)),
        (GROUP_TYPE_SUBTITLE_CLUSTER,           *_score_group_subtitle_cluster(lines)),
        (GROUP_TYPE_EDUCATIONAL_LIST_CLUSTER,   *_score_group_educational_list(lines)),
    ]

    best_type, best_score, best_sigs = max(scorer_results, key=lambda x: x[1])

    if best_score < 0.35:
        best_type  = GROUP_TYPE_UNKNOWN_CLUSTER
        best_score = 0.0
        best_sigs  = {}

    all_sigs = dict(best_sigs)
    all_sigs.update({f"german_{k}": v for k, v in german_sigs.items()})

    return OcrSpatialGroup(
        group_id=group_id,
        group_type=best_type,
        lines=tuple(lines),
        ordering=group_id,
        confidence=round(best_score, 4),
        signals=all_sigs,
        german_target_detected=german_detected,
    )


# ---------------------------------------------------------------------------
# v3 §4: Public — group_spatially
# ---------------------------------------------------------------------------

def group_spatially(text: str) -> list[OcrSpatialGroup]:
    """
    Segment cleaned OCR text into spatial/semantic groups.

    Uses blank-line boundaries (same as segment_candidates) then classifies
    each block's group type deterministically.

    Does not filter, correct, or reorder content.
    Preserves original line ordering within every group.
    Returns empty list for empty or whitespace-only input.
    """
    if not text.strip():
        return []

    raw_blocks = re.split(r"\n[ \t]*\n", text.strip())
    groups: list[OcrSpatialGroup] = []
    group_id = 0

    for block in raw_blocks:
        block = block.strip()
        if not block:
            continue
        lines = [l.strip() for l in block.split("\n") if l.strip()]
        if not lines:
            continue
        groups.append(_classify_block_as_group(lines, group_id))
        group_id += 1

    return groups


# ---------------------------------------------------------------------------
# v3 §4: Public — build_grouped_extraction_payload
# ---------------------------------------------------------------------------

def build_grouped_extraction_payload(
    groups: list[OcrSpatialGroup],
    archetype_result: OcrArchetypeResult,
) -> OcrGroupedPayload:
    """
    Combine spatial groups and archetype result into an extraction-ready payload.

    Raises ValueError if groups is empty — no fallback to flat text.
    Callers must ensure group_spatially produced results before calling this.
    """
    if not groups:
        raise ValueError(
            "build_grouped_extraction_payload: groups is empty — "
            "grouped extraction requires non-empty spatial groups"
        )

    german_group_count = sum(1 for g in groups if g.german_target_detected)

    return OcrGroupedPayload(
        archetype_result=archetype_result,
        groups=tuple(groups),
        candidate_count=len(groups),
        german_group_count=german_group_count,
    )


# ============================================================================
# OCR Pipeline v4 — Structured Extraction Preparation
# ============================================================================
#
# Builds typed, archetype-aware extraction payload BEFORE the LLM extraction step.
# Each spatial group becomes a GroupedExtractionUnit with:
#   - preserved_semantics: typed structural annotation (no hallucination)
#   - extraction_priority: high/medium/low (German-target prioritized)
#   - source_metadata: spatial group signals passed through verbatim
#
# Contract:
#   - build_structured_extraction_payload raises on empty grouped_payload.groups
#   - No fallback to flat text join
#   - No semantic inference — only structural annotation from observable signals
#   - Deterministic: same input → same output
# ============================================================================

# ---------------------------------------------------------------------------
# v4: Preserved semantics constants
# ---------------------------------------------------------------------------

SEMANTICS_TRANSLATION_PAIR   = "translation_pair_candidate"
SEMANTICS_GRAMMAR_CLUSTER    = "grammar_cluster_candidate"
SEMANTICS_SUBTITLE_DIALOGUE  = "subtitle_dialogue_candidate"
SEMANTICS_EDUCATIONAL_LIST   = "educational_list_candidate"
SEMANTICS_MULTILINGUAL_STACK = "multilingual_stack_candidate"
SEMANTICS_DIALECT_MAPPING    = "dialect_mapping_candidate"
SEMANTICS_PLAIN_PHRASE       = "plain_phrase"

ALL_PRESERVED_SEMANTICS: tuple[str, ...] = (
    SEMANTICS_TRANSLATION_PAIR,
    SEMANTICS_GRAMMAR_CLUSTER,
    SEMANTICS_SUBTITLE_DIALOGUE,
    SEMANTICS_EDUCATIONAL_LIST,
    SEMANTICS_MULTILINGUAL_STACK,
    SEMANTICS_DIALECT_MAPPING,
    SEMANTICS_PLAIN_PHRASE,
)

# ---------------------------------------------------------------------------
# v4: Extraction priority constants
# ---------------------------------------------------------------------------

PRIORITY_HIGH   = "high"
PRIORITY_MEDIUM = "medium"
PRIORITY_LOW    = "low"

ALL_EXTRACTION_PRIORITIES: tuple[str, ...] = (
    PRIORITY_HIGH,
    PRIORITY_MEDIUM,
    PRIORITY_LOW,
)

# ---------------------------------------------------------------------------
# v4: Result types
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class GroupedExtractionUnit:
    """
    One spatial group prepared for archetype-aware extraction.
    Lines are preserved verbatim — no filtering, no joining, no hallucination.
    preserved_semantics is a typed structural annotation of what this group IS,
    not a semantic inference of its meaning.
    """
    unit_id: int
    group_type: str            # one of GROUP_TYPE_* constants
    lines: tuple[str, ...]
    ordering: int              # 0-based position among all units
    preserved_semantics: str   # one of SEMANTICS_* constants
    confidence: float
    extraction_priority: str   # one of PRIORITY_* constants
    source_metadata: dict[str, float]


@dataclass(frozen=True, slots=True)
class StructuredExtractionPayload:
    """
    Complete archetype-aware extraction payload.
    Replaces the flat text blob that was previously sent directly to split_blocks.
    German-learning content is always the primary semantic target.
    Support languages (Cyrillic etc.) are preserved as metadata, not discarded.
    """
    payload_id: int
    archetype: str
    german_target_detected: bool
    support_languages: tuple[str, ...]  # non-Latin scripts present in content
    grouped_units: tuple[GroupedExtractionUnit, ...]
    extraction_priority: str            # highest priority across all units
    signals: dict[str, float]          # archetype-level signals
    confidence: float

# ---------------------------------------------------------------------------
# v4: Internal — group semantics resolution
# ---------------------------------------------------------------------------

_GROUP_SEMANTICS_MAP: dict[str, str] = {
    GROUP_TYPE_HORIZONTAL_PAIR:            SEMANTICS_TRANSLATION_PAIR,
    GROUP_TYPE_VERTICAL_TRANSLATION_STACK: SEMANTICS_MULTILINGUAL_STACK,
    GROUP_TYPE_GRAMMAR_CLUSTER:            SEMANTICS_GRAMMAR_CLUSTER,
    GROUP_TYPE_SUBTITLE_CLUSTER:           SEMANTICS_SUBTITLE_DIALOGUE,
    GROUP_TYPE_EDUCATIONAL_LIST_CLUSTER:   SEMANTICS_EDUCATIONAL_LIST,
    GROUP_TYPE_ISOLATED_PHRASE:            SEMANTICS_PLAIN_PHRASE,
    GROUP_TYPE_UNKNOWN_CLUSTER:            SEMANTICS_PLAIN_PHRASE,
}


def _resolve_group_semantics(
    grp: OcrSpatialGroup,
    archetype_result: OcrArchetypeResult,
) -> str:
    """
    Map a spatial group to its preserved_semantics annotation.
    Archetype-level context overrides the default group-type mapping when
    the overall layout provides a stronger structural signal than the group alone.
    """
    # bilingual_phrase archetype: both stacks and horizontal pairs are translation pairs
    if archetype_result.archetype == ARCHETYPE_BILINGUAL_PHRASE:
        if grp.group_type in (GROUP_TYPE_HORIZONTAL_PAIR, GROUP_TYPE_VERTICAL_TRANSLATION_STACK):
            return SEMANTICS_TRANSLATION_PAIR

    # multilingual_stack archetype with German + vertical stack: Austrian dialect mapping
    if (
        archetype_result.archetype == ARCHETYPE_MULTILINGUAL_STACK
        and grp.group_type == GROUP_TYPE_VERTICAL_TRANSLATION_STACK
        and grp.german_target_detected
    ):
        return SEMANTICS_DIALECT_MAPPING

    return _GROUP_SEMANTICS_MAP.get(grp.group_type, SEMANTICS_PLAIN_PHRASE)


# ---------------------------------------------------------------------------
# v4: Internal — unit extraction priority
# ---------------------------------------------------------------------------

_HIGH_PRIORITY_ARCHETYPES: frozenset[str] = frozenset({
    ARCHETYPE_GRAMMAR_BOARD,
    ARCHETYPE_VOCABULARY_PAIR,
    ARCHETYPE_BILINGUAL_PHRASE,
    ARCHETYPE_EDUCATIONAL_LIST,
    ARCHETYPE_MULTILINGUAL_STACK,  # Austrian dialect stacks are primary German content
})

_PRIORITY_RANK: dict[str, int] = {
    PRIORITY_HIGH: 2,
    PRIORITY_MEDIUM: 1,
    PRIORITY_LOW: 0,
}


def _compute_unit_priority(
    grp: OcrSpatialGroup,
    archetype_result: OcrArchetypeResult,
) -> str:
    """
    Assign extraction priority to a spatial group unit.

    HIGH:   German grammar structures / vocabulary pairs / Austrian dialect stacks /
            subtitle dialogue with German / bilingual phrase overlays
    MEDIUM: aligned translations without German morphology / support explanations /
            educational list items
    LOW:    non-German groups / unknown clusters / isolated phrases without German

    German content is always the primary semantic target.
    Support translations (Cyrillic etc.) are MEDIUM — preserved, never silently dropped.
    """
    if grp.german_target_detected:
        if grp.group_type in (GROUP_TYPE_GRAMMAR_CLUSTER, GROUP_TYPE_HORIZONTAL_PAIR):
            return PRIORITY_HIGH
        if archetype_result.archetype in _HIGH_PRIORITY_ARCHETYPES:
            return PRIORITY_HIGH
        if grp.group_type == GROUP_TYPE_SUBTITLE_CLUSTER:
            return PRIORITY_HIGH
        return PRIORITY_MEDIUM

    # Support content: aligned translations and lists without confirmed German morphology
    if grp.group_type in (
        GROUP_TYPE_VERTICAL_TRANSLATION_STACK,
        GROUP_TYPE_EDUCATIONAL_LIST_CLUSTER,
    ):
        return PRIORITY_MEDIUM

    return PRIORITY_LOW


# ---------------------------------------------------------------------------
# v4: Public — build_structured_extraction_payload
# ---------------------------------------------------------------------------

def build_structured_extraction_payload(
    grouped_payload: OcrGroupedPayload,
    payload_id: int = 0,
) -> StructuredExtractionPayload:
    """
    Build a typed, archetype-aware structured extraction payload from spatial groups.

    Raises ValueError if grouped_payload has no groups — no flat fallback.
    Every unit preserves group ordering, lines, type, and semantics annotation.
    Support languages (non-Latin scripts) are collected and preserved as metadata.
    """
    if not grouped_payload.groups:
        raise ValueError(
            "build_structured_extraction_payload: grouped_payload has no groups — "
            "structured extraction requires non-empty spatial groups"
        )

    archetype_result = grouped_payload.archetype_result

    support_langs: set[str] = set()
    for grp in grouped_payload.groups:
        for script in _detect_scripts("\n".join(grp.lines)):
            if script != "latin":
                support_langs.add(script)

    units: list[GroupedExtractionUnit] = []
    for grp in grouped_payload.groups:
        units.append(GroupedExtractionUnit(
            unit_id=grp.group_id,
            group_type=grp.group_type,
            lines=grp.lines,
            ordering=grp.ordering,
            preserved_semantics=_resolve_group_semantics(grp, archetype_result),
            confidence=grp.confidence,
            extraction_priority=_compute_unit_priority(grp, archetype_result),
            source_metadata=dict(grp.signals),
        ))

    overall_priority = max(
        (u.extraction_priority for u in units),
        key=lambda p: _PRIORITY_RANK.get(p, 0),
        default=PRIORITY_LOW,
    )

    return StructuredExtractionPayload(
        payload_id=payload_id,
        archetype=archetype_result.archetype,
        german_target_detected=archetype_result.german_target_count > 0,
        support_languages=tuple(sorted(support_langs)),
        grouped_units=tuple(units),
        extraction_priority=overall_priority,
        signals=dict(archetype_result.signals),
        confidence=archetype_result.confidence,
    )
