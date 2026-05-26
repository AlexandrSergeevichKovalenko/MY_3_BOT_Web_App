"""Post-call assessment service for voice sessions.

Assessment is now composed in two layers:

  1. Deterministic aggregation layer (grammar_control_note, strict_feedback,
     recommended_next_focus) — built from bt_3_voice_session_mistakes rows.
     No LLM hallucination possible here: every claim is backed by an extracted
     mistake with a verbatim user_quote.

  2. Prose layer (summary, fluency_note, coherence_relevance_note,
     lexical_range_note, self_correction_note) — one small LLM pass whose
     input is the structured mistakes aggregate + session stats, NOT the raw
     transcript.  The LLM is explicitly forbidden from evaluating grammar.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from dataclasses import dataclass, field

try:
    from backend.database import (
        fetch_agent_voice_transcript_segments,
        fetch_voice_session_mistakes,
        get_agent_voice_session_context,
        get_voice_session_assessment,
        upsert_voice_session_assessment,
    )
    from backend.openai_manager import llm_execute
except Exception:
    from database import (  # type: ignore
        fetch_agent_voice_transcript_segments,
        fetch_voice_session_mistakes,
        get_agent_voice_session_context,
        get_voice_session_assessment,
        upsert_voice_session_assessment,
    )
    from openai_manager import llm_execute  # type: ignore


_SHORT_TRANSCRIPT_SEGMENT_THRESHOLD = 4
_SHORT_TRANSCRIPT_CHAR_THRESHOLD = 220
_MIN_USER_SEGMENT_RETRY_THRESHOLD = 2
_MIN_USER_CHAR_RETRY_THRESHOLD = 40
_ASSESSMENT_TRANSCRIPT_RETRY_DELAYS_SECONDS = (2.0, 5.0, 8.0)
_ASSESSMENT_TEXT_LIMITS = {
    "summary": 220,
    "lexical_range_note": 260,
    "fluency_note": 260,
    "coherence_relevance_note": 260,
    "self_correction_note": 220,
    "recommended_next_focus": 220,
}
_LOW_SIGNAL_PATTERNS = [
    r"\bgood job\b",
    r"\bwell done\b",
    r"\bkeep practicing\b",
    r"\bcontinue practicing\b",
    r"\bcontinue to practice\b",
    r"\bgreat job\b",
    r"\bnice work\b",
    r"\bvery good\b",
]
_GENERIC_RECOMMENDED_FOCUS_PATTERNS = [
    r"^keep practicing[.!]*$",
    r"^practice more[.!]*$",
    r"^continue practicing[.!]*$",
    r"^work on everything[.!]*$",
]
_NOTE_FIELD_FALLBACKS = {
    "lexical_range_note": "Not enough structured lexical feedback was returned.",
    "grammar_control_note": "Not enough structured grammar feedback was returned.",
    "fluency_note": "Not enough structured fluency feedback was returned.",
    "coherence_relevance_note": "Not enough structured coherence feedback was returned.",
    "self_correction_note": "No structured self-correction note was returned.",
}

# Human-readable labels for the 20 taxonomy categories.
_CATEGORY_LABELS: dict[str, str] = {
    "ADJECTIVE_ENDINGS":  "Adjective Endings",
    "ARTICLES":           "Articles",
    "CASES":              "Cases",
    "CONJUNCTIONS":       "Conjunctions",
    "INFINITIVE_CLAUSES": "Infinitive Clauses",
    "KONJUNKTIV":         "Konjunktiv",
    "LEXIS":              "Lexical Choice",
    "MODAL_VERBS":        "Modal Verbs",
    "NEGATION":           "Negation",
    "NOUN_GENDER":        "Noun Gender",
    "PASSIVE":            "Passive",
    "PLURAL_FORM":        "Plural Form",
    "PREPOSITIONS":       "Prepositions",
    "PRONUNCIATION_STT":  "Speech Clarity / STT",
    "REFLEXIVE_VERBS":    "Reflexive Verbs",
    "RELATIVE_CLAUSES":   "Relative Clauses",
    "SEPARABLE_VERBS":    "Separable Verbs",
    "TENSES":             "Tenses",
    "VERB_FORM":          "Verb Form",
    "WORD_ORDER":         "Word Order",
}


@dataclass(slots=True)
class VoiceAssessment:
    """Minimal stored voice assessment."""

    session_id: int
    summary: str
    strict_feedback: str
    lexical_range_note: str
    grammar_control_note: str
    fluency_note: str
    coherence_relevance_note: str
    self_correction_note: str
    target_vocab_used: list[str] = field(default_factory=list)
    target_vocab_missed: list[str] = field(default_factory=list)
    recommended_next_focus: str | None = None
    is_short_transcript: bool = False
    created_at: str | None = None
    updated_at: str | None = None


# ── Unchanged transcript helpers ──────────────────────────────────────────────


def _summarize_transcript_material(segments: list[dict]) -> dict[str, int]:
    total_chars = 0
    assistant_segments = 0
    user_segments = 0
    user_chars = 0
    for segment in segments:
        speaker = str(segment.get("speaker") or "").strip().lower()
        text = str(segment.get("text") or "").strip()
        if not text:
            continue
        total_chars += len(text)
        if speaker == "user":
            user_segments += 1
            user_chars += len(text)
        elif speaker == "assistant":
            assistant_segments += 1
    return {
        "segment_count": len([s for s in segments if str(s.get("text") or "").strip()]),
        "assistant_segments": assistant_segments,
        "user_segments": user_segments,
        "total_chars": total_chars,
        "user_chars": user_chars,
    }


def _transcript_looks_premature(snapshot: dict[str, int]) -> tuple[bool, str]:
    segment_count = int(snapshot.get("segment_count") or 0)
    user_segments = int(snapshot.get("user_segments") or 0)
    total_chars = int(snapshot.get("total_chars") or 0)
    user_chars = int(snapshot.get("user_chars") or 0)
    if segment_count <= 0:
        return True, "no_transcript_rows"
    if user_segments <= 0:
        return True, "assistant_only_transcript"
    if (
        user_segments < _MIN_USER_SEGMENT_RETRY_THRESHOLD
        and user_chars < _MIN_USER_CHAR_RETRY_THRESHOLD
    ):
        return True, "very_low_user_material"
    if (
        total_chars < _MIN_USER_CHAR_RETRY_THRESHOLD
        and int(snapshot.get("assistant_segments") or 0) > 0
        and user_segments <= 1
    ):
        return True, "extremely_low_total_material"
    return False, ""


async def _load_transcript_segments_for_assessment(*, session_id: int) -> list[dict]:
    segments = fetch_agent_voice_transcript_segments(session_id=int(session_id))
    snapshot = _summarize_transcript_material(segments)
    logging.info(
        "Voice assessment transcript snapshot for session_id=%s: %s",
        int(session_id),
        snapshot,
    )
    should_retry, reason = _transcript_looks_premature(snapshot)
    if not should_retry:
        return segments

    for delay_seconds in _ASSESSMENT_TRANSCRIPT_RETRY_DELAYS_SECONDS:
        logging.info(
            "Voice assessment waiting for transcript settlement for session_id=%s "
            "reason=%s delay=%.1fs snapshot=%s",
            int(session_id),
            reason,
            float(delay_seconds),
            snapshot,
        )
        await asyncio.sleep(float(delay_seconds))
        segments = fetch_agent_voice_transcript_segments(session_id=int(session_id))
        snapshot = _summarize_transcript_material(segments)
        should_retry, reason = _transcript_looks_premature(snapshot)
        if not should_retry:
            break
    return segments


def _trim_sentence_boundary(text: str, *, limit: int) -> str:
    normalized = str(text or "").strip()
    if len(normalized) <= limit:
        return normalized
    truncated = normalized[:limit].rstrip()
    last_break = max(
        truncated.rfind("."), truncated.rfind("!"),
        truncated.rfind("?"), truncated.rfind(";"),
    )
    if last_break >= int(limit * 0.55):
        return truncated[: last_break + 1].strip()
    return truncated.rstrip(" ,;:-") + "..."


def _clean_assessment_text(value: object, *, fallback: str, field_name: str) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    for pattern in _LOW_SIGNAL_PATTERNS:
        text = re.sub(pattern, "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s+", " ", text).strip(" ,;:-")
    limit = int(_ASSESSMENT_TEXT_LIMITS.get(field_name, 260))
    text = _trim_sentence_boundary(text, limit=limit)
    if field_name == "recommended_next_focus":
        for pattern in _GENERIC_RECOMMENDED_FOCUS_PATTERNS:
            if re.match(pattern, text, flags=re.IGNORECASE):
                text = ""
                break
    if field_name == "summary":
        text = re.sub(r"^(the user|the learner)\s+", "", text, flags=re.IGNORECASE).strip()
        if text:
            text = text[:1].upper() + text[1:]
    return text or fallback


def _normalize_string(value: object, *, fallback: str) -> str:
    text = str(value or "").strip()
    return text or fallback


def _normalize_string_list(value: object) -> list[str]:
    if isinstance(value, list):
        raw_items = value
    else:
        raw_items = []
    seen: set[str] = set()
    normalized: list[str] = []
    for item in raw_items:
        text = str(item or "").strip()
        if not text:
            continue
        key = text.casefold()
        if key in seen:
            continue
        seen.add(key)
        normalized.append(text)
    return normalized


def _build_transcript_text(segments: list[dict]) -> str:
    lines: list[str] = []
    for segment in segments:
        speaker = str(segment.get("speaker") or "unknown").strip().lower() or "unknown"
        text = str(segment.get("text") or "").strip()
        if not text:
            continue
        lines.append(f"{speaker}: {text}")
    return "\n".join(lines)


def _normalize_text_for_match(value: str) -> str:
    normalized = str(value or "").casefold()
    normalized = re.sub(r"[^\w\s]+", " ", normalized, flags=re.UNICODE)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def _target_item_used(item: str, transcript_text: str) -> bool:
    normalized_item = _normalize_text_for_match(str(item or ""))
    normalized_transcript = _normalize_text_for_match(str(transcript_text or ""))
    if not normalized_item or not normalized_transcript:
        return False
    pattern = (
        r"(?<!\w)"
        + r"\s+".join(re.escape(part) for part in normalized_item.split())
        + r"(?!\w)"
    )
    return re.search(pattern, normalized_transcript, flags=re.IGNORECASE) is not None


def _compute_target_vocab_heuristics(
    *, transcript_text: str, prep_pack: dict | None
) -> tuple[list[str], list[str]]:
    raw_targets = []
    if prep_pack:
        raw_targets.extend(list(prep_pack.get("target_vocab") or []))
        raw_targets.extend(list(prep_pack.get("target_expressions") or []))
    normalized_targets = _normalize_string_list(raw_targets)
    used = [item for item in normalized_targets if _target_item_used(item, transcript_text)]
    used_keys = {item.casefold() for item in used}
    missed = [item for item in normalized_targets if item.casefold() not in used_keys]
    return used, missed


def _dedupe_low_signal_notes(values: dict[str, str]) -> dict[str, str]:
    seen: set[str] = set()
    result = dict(values)
    for field_name in (
        "lexical_range_note",
        "grammar_control_note",
        "fluency_note",
        "coherence_relevance_note",
        "self_correction_note",
    ):
        text = str(result.get(field_name) or "").strip()
        key = text.casefold()
        if not text:
            result[field_name] = _NOTE_FIELD_FALLBACKS[field_name]
            continue
        if key in seen:
            result[field_name] = _NOTE_FIELD_FALLBACKS[field_name]
            continue
        seen.add(key)
    return result


def _build_short_transcript_fallback(
    *,
    session_id: int,
    transcript_text: str,
    prep_pack: dict | None,
    source_lang: str = "ru",
) -> VoiceAssessment:
    used, missed = _compute_target_vocab_heuristics(
        transcript_text=transcript_text,
        prep_pack=prep_pack,
    )
    lang = str(source_lang or "ru").strip().lower()
    if lang == "de":
        summary = "Das Gespräch war zu kurz für eine ausführliche Analyse."
        strict_feedback = "Es gibt noch nicht genug Sprachmaterial, um verlässliche Aussagen über deine Sprechleistung zu machen."
        lexical_range_note = "Zu wenig Material. Der Wortschatz lässt sich aus einem so kurzen Gespräch nicht zuverlässig beurteilen."
        grammar_control_note = "Zu wenig Material. Für eine faire Grammatikbewertung brauchen wir einen längeren Austausch."
        fluency_note = "Zu wenig Material. Das Gespräch war zu kurz, um die Sprechflüssigkeit einzuschätzen."
        coherence_relevance_note = "Zu wenig Material. Kohärenz und Themenkontinuität lassen sich nicht zuverlässig bewerten."
        self_correction_note = "Kein verlässliches Selbstkorrekturmuster aus dem kurzen Gespräch erkennbar."
        recommended_next_focus = "Führe ein längeres Gespräch und sammle mehr Transkriptmaterial für eine fundierte Auswertung."
    else:
        summary = "Сессия была слишком короткой для подробного разбора."
        strict_feedback = "Пока недостаточно разговорного материала, чтобы делать выводы о качестве речи."
        lexical_range_note = "Мало материала. Из такого короткого разговора сложно судить о словарном запасе."
        grammar_control_note = "Мало материала. Для честной оценки грамматики нужен более длинный диалог."
        fluency_note = "Мало материала. Разговор слишком короткий, чтобы оценить беглость речи."
        coherence_relevance_note = "Мало материала. Связность и поддержание темы нельзя надёжно оценить."
        self_correction_note = "Из короткого разговора не удаётся выявить паттерны самокоррекции."
        recommended_next_focus = "Проведи более длинную сессию — тогда разбор будет полноценным."
    return VoiceAssessment(
        session_id=int(session_id),
        summary=summary,
        strict_feedback=strict_feedback,
        lexical_range_note=lexical_range_note,
        grammar_control_note=grammar_control_note,
        fluency_note=fluency_note,
        coherence_relevance_note=coherence_relevance_note,
        self_correction_note=self_correction_note,
        target_vocab_used=used,
        target_vocab_missed=missed,
        recommended_next_focus=recommended_next_focus,
        is_short_transcript=True,
    )


# ── Deterministic aggregation layer ──────────────────────────────────────────


def _cat_label(cat: str) -> str:
    return _CATEGORY_LABELS.get(str(cat or ""), str(cat or "").replace("_", " ").title())


def aggregate_voice_mistakes(mistakes: list[dict]) -> dict:
    """Deterministic aggregation over structured mistake rows.

    Returns:
        total_count            — total rows including PRONUNCIATION_STT
        grammar_mistake_count  — total minus PRONUNCIATION_STT
        stt_count              — PRONUNCIATION_STT rows
        by_category            — {category: count}
        by_severity            — {severity: count} grammar only
        high_confidence_count  — grammar_confidence >= 0.7
        low_confidence_count   — grammar_confidence < 0.5 or None
        top_categories         — up to 5 grammar cats, count desc
        highest_severity_cat   — cat with highest weighted severity score
        dominant_category      — most frequent grammar category
        quotes_by_category     — {cat: [(quote, corrected), …]} max 3 per cat
        stt_uncertain_ratio    — stt_count / max(total_count, 1)
    """
    total = len(mistakes)
    by_category: dict[str, int] = {}
    by_severity: dict[str, int] = {"low": 0, "medium": 0, "high": 0}
    high_confidence_count = 0
    low_confidence_count = 0
    stt_count = 0
    quotes_by_category: dict[str, list[tuple[str, str]]] = {}
    severity_score_by_cat: dict[str, int] = {}

    for m in mistakes:
        cat = str(m.get("error_category") or "")
        sev = str(m.get("severity") or "medium").lower()
        gc = m.get("grammar_confidence")
        quote = str(m.get("user_quote") or "").strip()
        corrected = str(m.get("corrected_form") or "").strip()

        by_category[cat] = by_category.get(cat, 0) + 1

        if cat == "PRONUNCIATION_STT":
            stt_count += 1
        else:
            sev_norm = sev if sev in ("low", "medium", "high") else "medium"
            by_severity[sev_norm] = by_severity.get(sev_norm, 0) + 1
            sev_weight = {"high": 3, "medium": 2, "low": 1}.get(sev_norm, 1)
            severity_score_by_cat[cat] = severity_score_by_cat.get(cat, 0) + sev_weight

        if gc is not None:
            try:
                gc_f = float(gc)
                if gc_f >= 0.7:
                    high_confidence_count += 1
                elif gc_f < 0.5:
                    low_confidence_count += 1
            except (ValueError, TypeError):
                low_confidence_count += 1
        else:
            low_confidence_count += 1

        if cat != "PRONUNCIATION_STT" and quote and corrected:
            if cat not in quotes_by_category:
                quotes_by_category[cat] = []
            if len(quotes_by_category[cat]) < 3:
                quotes_by_category[cat].append((quote, corrected))

    grammar_count = total - stt_count

    top_categories = sorted(
        [c for c in by_category if c != "PRONUNCIATION_STT"],
        key=lambda c: (-by_category[c], c),
    )[:5]

    highest_severity_cat: str | None = None
    if severity_score_by_cat:
        highest_severity_cat = max(
            severity_score_by_cat, key=lambda c: severity_score_by_cat[c]
        )

    dominant_category = top_categories[0] if top_categories else None

    return {
        "total_count": total,
        "grammar_mistake_count": grammar_count,
        "stt_count": stt_count,
        "by_category": by_category,
        "by_severity": by_severity,
        "high_confidence_count": high_confidence_count,
        "low_confidence_count": low_confidence_count,
        "top_categories": top_categories,
        "highest_severity_cat": highest_severity_cat,
        "dominant_category": dominant_category,
        "quotes_by_category": quotes_by_category,
        "stt_uncertain_ratio": stt_count / max(total, 1),
    }


def build_grammar_control_note(aggregate: dict) -> str:
    """Build grammar_control_note deterministically from aggregate.

    Every claim maps directly to a row in bt_3_voice_session_mistakes.
    No LLM involved.
    """
    grammar_count = int(aggregate.get("grammar_mistake_count") or 0)
    stt_count = int(aggregate.get("stt_count") or 0)
    by_category = dict(aggregate.get("by_category") or {})
    by_severity = dict(aggregate.get("by_severity") or {})
    top_categories = list(aggregate.get("top_categories") or [])
    highest_sev_cat = aggregate.get("highest_severity_cat")
    low_conf = int(aggregate.get("low_confidence_count") or 0)

    if grammar_count == 0 and stt_count == 0:
        logging.info("assessment_no_valid_mistakes — grammar_control_note=none_detected")
        return "No grammar mistakes were detected in this session."

    parts: list[str] = []

    if grammar_count > 0:
        parts.append(f"Grammar mistakes detected: {grammar_count}")
        category_lines = []
        for cat in top_categories[:4]:
            count = by_category.get(cat, 0)
            category_lines.append(
                f"  • {_cat_label(cat)} — {count} {'mistake' if count == 1 else 'mistakes'}"
            )
        if category_lines:
            parts.append("\n".join(category_lines))

        high_sev = int(by_severity.get("high") or 0)
        med_sev = int(by_severity.get("medium") or 0)
        if high_sev > 0 and highest_sev_cat:
            parts.append(
                f"Most severe area: {_cat_label(highest_sev_cat)} "
                f"({high_sev} high-severity {'mistake' if high_sev == 1 else 'mistakes'})."
            )
        elif med_sev > 0:
            parts.append(
                f"{med_sev} medium-severity {'mistake' if med_sev == 1 else 'mistakes'} "
                "— comprehension affected in places."
            )

    if stt_count > 0:
        parts.append(
            f"{stt_count} {'segment' if stt_count == 1 else 'segments'} flagged as "
            "unclear speech / STT uncertainty — not counted in grammar total."
        )

    if low_conf > 0:
        parts.append(
            f"{low_conf} {'mistake' if low_conf == 1 else 'mistakes'} had low extraction "
            "confidence — may reflect transcription ambiguity rather than actual grammar errors."
        )

    return "\n\n".join(parts).strip()


def build_strict_feedback(aggregate: dict, mistakes: list[dict]) -> str:
    """Build strict_feedback with exact verbatim quotes and corrected forms.

    Every quote in the output is a real user utterance from the transcript.
    No LLM involved.
    """
    grammar_mistakes = [
        m for m in mistakes
        if str(m.get("error_category") or "") != "PRONUNCIATION_STT"
    ]

    if not grammar_mistakes:
        return (
            "No grammar mistakes requiring direct correction were identified in this session."
        )

    top_categories = list(aggregate.get("top_categories") or [])
    quotes_by_category = dict(aggregate.get("quotes_by_category") or {})
    dominant = aggregate.get("dominant_category")
    by_category = dict(aggregate.get("by_category") or {})

    parts: list[str] = []

    if dominant:
        dom_count = int(by_category.get(dominant, 0))
        quotes = list(quotes_by_category.get(dominant, []))
        lines = [
            f"{_cat_label(dominant)} "
            f"({dom_count} {'mistake' if dom_count == 1 else 'mistakes'}):"
        ]
        for quote, corrected in quotes[:2]:
            lines.append(f'  ✗ "{quote}"')
            lines.append(f'  ✓ "{corrected}"')
        parts.append("\n".join(lines))

    for cat in top_categories[1:3]:
        count = int(by_category.get(cat, 0))
        quotes = list(quotes_by_category.get(cat, []))
        if not quotes:
            continue
        lines = [f"{_cat_label(cat)} ({count} {'mistake' if count == 1 else 'mistakes'}):"]
        for quote, corrected in quotes[:1]:
            lines.append(f'  ✗ "{quote}"')
            lines.append(f'  ✓ "{corrected}"')
        parts.append("\n".join(lines))

    # Extra high-severity mistakes not already shown
    shown_quotes: set[str] = {
        q
        for cat in top_categories[:3]
        for q, _ in quotes_by_category.get(cat, [])[:2]
    }
    extra_high = [
        m for m in grammar_mistakes
        if str(m.get("severity") or "") == "high"
        and str(m.get("user_quote") or "") not in shown_quotes
    ][:2]
    if extra_high:
        lines = ["Additional high-severity issues:"]
        for m in extra_high:
            q = str(m.get("user_quote") or "")
            c = str(m.get("corrected_form") or "")
            if q and c:
                lines.append(f'  ✗ "{q}" → "{c}"')
        if len(lines) > 1:
            parts.append("\n".join(lines))

    return "\n\n".join(parts).strip()


def _format_next_focus(category: str, count: int, dominant_severity: str) -> str:
    label = _cat_label(category)
    n = f"{count} {'mistake' if count == 1 else 'mistakes'}"
    if dominant_severity == "high":
        return (
            f"Drill {label} — {n} with high severity. "
            "This is your primary weakness this session."
        )
    return (
        f"Practice {label} — {n} this session. "
        "Make this your focused target for the next session."
    )


def determine_next_focus(aggregate: dict, mistakes: list[dict]) -> str | None:
    """Select recommended_next_focus deterministically.

    Ranking (first match wins):
      1. Grammar category with >= 1 high-severity mistake AND >= 2 total mistakes
      2. Grammar category with highest severity score (high×3 + medium×2) AND >= 2 mistakes
      3. Most frequent grammar category (any severity)
    """
    grammar_mistakes = [
        m for m in mistakes
        if str(m.get("error_category") or "") != "PRONUNCIATION_STT"
    ]
    if not grammar_mistakes:
        return None

    by_category = dict(aggregate.get("by_category") or {})
    by_cat_severity: dict[str, dict[str, int]] = {}
    for m in grammar_mistakes:
        cat = str(m.get("error_category") or "")
        sev = str(m.get("severity") or "medium").lower()
        if cat not in by_cat_severity:
            by_cat_severity[cat] = {"high": 0, "medium": 0, "low": 0}
        if sev in by_cat_severity[cat]:
            by_cat_severity[cat][sev] += 1

    # Priority 1: high-severity AND repeated
    high_repeated = sorted(
        [
            c for c, sevs in by_cat_severity.items()
            if sevs.get("high", 0) >= 1 and by_category.get(c, 0) >= 2
        ],
        key=lambda c: (-by_category.get(c, 0), c),
    )
    if high_repeated:
        cat = high_repeated[0]
        logging.info(
            "assessment_next_focus_selected category=%s severity=high count=%d",
            cat, by_category[cat],
        )
        return _format_next_focus(cat, by_category[cat], "high")

    # Priority 2: highest severity score AND repeated
    scored = {
        c: by_cat_severity[c].get("high", 0) * 3 + by_cat_severity[c].get("medium", 0) * 2
        for c in by_cat_severity
        if by_category.get(c, 0) >= 2
    }
    if scored:
        top = max(scored, key=lambda c: (scored[c], -by_category.get(c, 0), c))
        if scored[top] > 0:
            dom_sev = "high" if by_cat_severity[top].get("high", 0) > 0 else "medium"
            logging.info(
                "assessment_next_focus_selected category=%s severity=%s score=%d",
                top, dom_sev, scored[top],
            )
            return _format_next_focus(top, by_category[top], dom_sev)

    # Priority 3: most frequent (frequency fallback)
    dominant = aggregate.get("dominant_category")
    if dominant:
        dom_sev = "high" if by_cat_severity.get(dominant, {}).get("high", 0) > 0 else "medium"
        logging.info(
            "assessment_next_focus_selected category=%s severity=%s (frequency_fallback)",
            dominant, dom_sev,
        )
        return _format_next_focus(dominant, by_category.get(dominant, 1), dom_sev)

    return None


# ── Prose layer (one small LLM pass, no raw transcript) ──────────────────────


async def _build_prose_from_mistakes(
    *,
    session_id: int,
    aggregate: dict,
    mistakes: list[dict],
    transcript_stats: dict,
    session_context: dict | None,
    fallback_used: list[str],
    fallback_missed: list[str],
) -> dict:
    """One small LLM call for summary + fluency + coherence + lexical + self_correction.

    The LLM receives structured mistakes + session stats only — NOT the raw transcript.
    If the call fails, static fallbacks are returned so the deterministic fields are
    still persisted.
    """
    scenario = dict((session_context or {}).get("scenario") or {})

    compact_mistakes = []
    for m in mistakes[:20]:
        if str(m.get("error_category") or "") == "PRONUNCIATION_STT":
            continue
        compact_mistakes.append({
            "category": m.get("error_category"),
            "subtype": m.get("error_subtype"),
            "severity": m.get("severity"),
            "quote": str(m.get("user_quote") or "")[:80],
            "correction": str(m.get("corrected_form") or "")[:80],
        })

    payload = json.dumps(
        {
            "session_stats": {
                "user_turns": int(transcript_stats.get("user_segments") or 0),
                "user_chars": int(transcript_stats.get("user_chars") or 0),
                "total_turns": int(transcript_stats.get("segment_count") or 0),
                "avg_user_chars_per_turn": (
                    int(transcript_stats.get("user_chars") or 0)
                    // max(int(transcript_stats.get("user_segments") or 1), 1)
                ),
            },
            "grammar_summary": {
                "total_grammar_mistakes": int(aggregate.get("grammar_mistake_count") or 0),
                "top_categories": list(aggregate.get("top_categories") or [])[:4],
                "high_severity_count": int((aggregate.get("by_severity") or {}).get("high") or 0),
                "stt_flagged_count": int(aggregate.get("stt_count") or 0),
            },
            "structured_mistakes": compact_mistakes,
            "scenario_title": str(scenario.get("title") or ""),
            "scenario_topic": str(scenario.get("topic") or ""),
            "target_vocab_used": list(fallback_used)[:10],
            "target_vocab_missed": list(fallback_missed)[:10],
        },
        ensure_ascii=False,
    )

    try:
        raw = await llm_execute(
            task_name="voice_prose_from_mistakes",
            system_instruction_key="voice_prose_from_mistakes",
            user_message=payload,
            poll_interval_seconds=1.0,
            responses_timeout_seconds=25.0,
        )
        cleaned = str(raw or "").strip()
        if cleaned.startswith("```"):
            cleaned = re.sub(r"^```[a-zA-Z]*\n?", "", cleaned).rstrip("`").strip()
        parsed = json.loads(cleaned)
        if not isinstance(parsed, dict):
            raise ValueError("prose LLM returned non-dict")
        return {
            "summary": _clean_assessment_text(
                parsed.get("summary"),
                fallback="Session completed.",
                field_name="summary",
            ),
            "lexical_range_note": _clean_assessment_text(
                parsed.get("lexical_range_note"),
                fallback=_NOTE_FIELD_FALLBACKS["lexical_range_note"],
                field_name="lexical_range_note",
            ),
            "fluency_note": _clean_assessment_text(
                parsed.get("fluency_note"),
                fallback=_NOTE_FIELD_FALLBACKS["fluency_note"],
                field_name="fluency_note",
            ),
            "coherence_relevance_note": _clean_assessment_text(
                parsed.get("coherence_relevance_note"),
                fallback=_NOTE_FIELD_FALLBACKS["coherence_relevance_note"],
                field_name="coherence_relevance_note",
            ),
            "self_correction_note": _clean_assessment_text(
                parsed.get("self_correction_note"),
                fallback=_NOTE_FIELD_FALLBACKS["self_correction_note"],
                field_name="self_correction_note",
            ),
            "target_vocab_used": (
                _normalize_string_list(parsed.get("target_vocab_used")) or list(fallback_used)
            ),
            "target_vocab_missed": (
                _normalize_string_list(parsed.get("target_vocab_missed")) or list(fallback_missed)
            ),
        }
    except Exception as exc:
        logging.warning(
            "assessment_prose_llm_failed session_id=%s error=%s — using static fallback",
            session_id,
            exc,
        )
        return {
            "summary": "Session completed. Grammar analysis processed from structured extraction.",
            "lexical_range_note": _NOTE_FIELD_FALLBACKS["lexical_range_note"],
            "fluency_note": _NOTE_FIELD_FALLBACKS["fluency_note"],
            "coherence_relevance_note": _NOTE_FIELD_FALLBACKS["coherence_relevance_note"],
            "self_correction_note": _NOTE_FIELD_FALLBACKS["self_correction_note"],
            "target_vocab_used": list(fallback_used),
            "target_vocab_missed": list(fallback_missed),
        }


# ── Assessment composition ────────────────────────────────────────────────────


async def _build_voice_assessment_from_segments(
    *,
    session_id: int,
    transcript_segments: list[dict],
    session_context: dict | None = None,
) -> VoiceAssessment | None:
    resolved_context = (
        session_context
        if session_context is not None
        else get_agent_voice_session_context(int(session_id))
    )
    transcript_text = _build_transcript_text(transcript_segments)
    prep_pack = dict((resolved_context or {}).get("prep_pack") or {})
    source_lang = (
        str((resolved_context or {}).get("session", {}).get("source_lang") or "ru")
        .strip()
        .lower()
        or "ru"
    )
    fallback_used, fallback_missed = _compute_target_vocab_heuristics(
        transcript_text=transcript_text,
        prep_pack=prep_pack,
    )
    transcript_stats = _summarize_transcript_material(transcript_segments)

    # Short transcript guard (unchanged behavior)
    if not transcript_text:
        logging.warning(
            "Voice assessment fallback: session_id=%s has 0 transcript segments — "
            "transcript is empty. This usually means session_id was not bound in the "
            "agent (participant attributes missing or DB lookup failed).",
            int(session_id),
        )
        return _build_short_transcript_fallback(
            session_id=int(session_id),
            transcript_text="",
            prep_pack=prep_pack,
            source_lang=source_lang,
        )

    if (
        len(transcript_segments) < _SHORT_TRANSCRIPT_SEGMENT_THRESHOLD
        or len(transcript_text) < _SHORT_TRANSCRIPT_CHAR_THRESHOLD
    ):
        logging.warning(
            "Voice assessment fallback: session_id=%s has only %d segments / %d chars "
            "(thresholds: %d segments, %d chars). Transcript too short for analysis.",
            int(session_id),
            len(transcript_segments),
            len(transcript_text),
            _SHORT_TRANSCRIPT_SEGMENT_THRESHOLD,
            _SHORT_TRANSCRIPT_CHAR_THRESHOLD,
        )
        return _build_short_transcript_fallback(
            session_id=int(session_id),
            transcript_text=transcript_text,
            prep_pack=prep_pack,
            source_lang=source_lang,
        )

    # ── Deterministic aggregation layer ──────────────────────────────────────
    logging.info("assessment_aggregation_started session_id=%s", int(session_id))

    try:
        all_mistakes = await asyncio.to_thread(
            fetch_voice_session_mistakes, session_id=int(session_id)
        )
    except Exception as exc:
        logging.warning(
            "assessment_mistakes_fetch_failed session_id=%s error=%s — proceeding with empty",
            int(session_id),
            exc,
        )
        all_mistakes = []

    # Exclude mistakes with very low extraction confidence (< 0.5) to avoid
    # surfacing STT-ambiguous noise in the feedback.
    reliable_mistakes = [
        m for m in all_mistakes
        if m.get("grammar_confidence") is None
        or float(m.get("grammar_confidence") or 0.0) >= 0.5
    ]

    aggregate = aggregate_voice_mistakes(reliable_mistakes)

    if aggregate["stt_uncertain_ratio"] > 0.3:
        logging.info(
            "assessment_low_confidence_segments session_id=%s stt_count=%d total=%d ratio=%.2f",
            int(session_id),
            aggregate["stt_count"],
            aggregate["total_count"],
            aggregate["stt_uncertain_ratio"],
        )

    if aggregate["grammar_mistake_count"] == 0:
        logging.info(
            "assessment_no_valid_mistakes session_id=%s total_rows=%d",
            int(session_id),
            aggregate["total_count"],
        )

    # Deterministic grammar fields — no LLM, no hallucination
    grammar_control_note = build_grammar_control_note(aggregate)
    strict_feedback_text = build_strict_feedback(aggregate, reliable_mistakes)
    next_focus = determine_next_focus(aggregate, reliable_mistakes)

    logging.info(
        "assessment_aggregation_completed session_id=%s "
        "total=%d grammar=%d stt=%d high_conf=%d low_conf=%d",
        int(session_id),
        aggregate["total_count"],
        aggregate["grammar_mistake_count"],
        aggregate["stt_count"],
        aggregate["high_confidence_count"],
        aggregate["low_confidence_count"],
    )

    # ── Prose layer (small LLM call, no raw transcript) ───────────────────────
    prose = await _build_prose_from_mistakes(
        session_id=int(session_id),
        aggregate=aggregate,
        mistakes=reliable_mistakes,
        transcript_stats=transcript_stats,
        session_context=resolved_context,
        fallback_used=fallback_used,
        fallback_missed=fallback_missed,
    )

    return VoiceAssessment(
        session_id=int(session_id),
        summary=prose.get("summary", "Session completed."),
        strict_feedback=strict_feedback_text,
        lexical_range_note=prose.get(
            "lexical_range_note", _NOTE_FIELD_FALLBACKS["lexical_range_note"]
        ),
        grammar_control_note=grammar_control_note,
        fluency_note=prose.get("fluency_note", _NOTE_FIELD_FALLBACKS["fluency_note"]),
        coherence_relevance_note=prose.get(
            "coherence_relevance_note", _NOTE_FIELD_FALLBACKS["coherence_relevance_note"]
        ),
        self_correction_note=prose.get(
            "self_correction_note", _NOTE_FIELD_FALLBACKS["self_correction_note"]
        ),
        target_vocab_used=(
            _normalize_string_list(prose.get("target_vocab_used")) or list(fallback_used)
        ),
        target_vocab_missed=(
            _normalize_string_list(prose.get("target_vocab_missed")) or list(fallback_missed)
        ),
        recommended_next_focus=next_focus,
    )


# ── Public API (unchanged signatures) ────────────────────────────────────────


def get_stored_voice_assessment(*, session_id: int) -> VoiceAssessment | None:
    payload = get_voice_session_assessment(int(session_id))
    if not payload:
        return None
    return VoiceAssessment(
        session_id=int(payload["session_id"]),
        summary=str(payload.get("summary") or ""),
        strict_feedback=str(payload.get("strict_feedback") or ""),
        lexical_range_note=str(payload.get("lexical_range_note") or ""),
        grammar_control_note=str(payload.get("grammar_control_note") or ""),
        fluency_note=str(payload.get("fluency_note") or ""),
        coherence_relevance_note=str(payload.get("coherence_relevance_note") or ""),
        self_correction_note=str(payload.get("self_correction_note") or ""),
        target_vocab_used=list(payload.get("target_vocab_used") or []),
        target_vocab_missed=list(payload.get("target_vocab_missed") or []),
        recommended_next_focus=payload.get("recommended_next_focus"),
        is_short_transcript=bool(payload.get("is_short_transcript") or False),
        created_at=payload.get("created_at"),
        updated_at=payload.get("updated_at"),
    )


def load_voice_assessment(*, session_id: int) -> VoiceAssessment | None:
    """Load one stored assessment for a completed voice session."""
    return get_stored_voice_assessment(session_id=int(session_id))


async def build_voice_assessment(*, session_id: int) -> VoiceAssessment | None:
    """Build a minimal qualitative voice assessment from transcript plus context."""
    transcript_segments = fetch_agent_voice_transcript_segments(session_id=int(session_id))
    return await _build_voice_assessment_from_segments(
        session_id=int(session_id),
        transcript_segments=transcript_segments,
    )


def store_voice_assessment(assessment: VoiceAssessment) -> VoiceAssessment | None:
    """Persist one qualitative assessment row for a voice session."""
    payload = upsert_voice_session_assessment(
        session_id=int(assessment.session_id),
        summary=assessment.summary,
        strict_feedback=assessment.strict_feedback,
        lexical_range_note=assessment.lexical_range_note,
        grammar_control_note=assessment.grammar_control_note,
        fluency_note=assessment.fluency_note,
        coherence_relevance_note=assessment.coherence_relevance_note,
        self_correction_note=assessment.self_correction_note,
        target_vocab_used=assessment.target_vocab_used,
        target_vocab_missed=assessment.target_vocab_missed,
        recommended_next_focus=assessment.recommended_next_focus,
        is_short_transcript=assessment.is_short_transcript,
    )
    if not payload:
        return None
    return get_stored_voice_assessment(session_id=int(assessment.session_id))


async def build_and_store_voice_assessment(*, session_id: int) -> VoiceAssessment | None:
    """Build and persist a best-effort assessment for one completed voice session."""
    transcript_segments = await _load_transcript_segments_for_assessment(
        session_id=int(session_id)
    )
    assessment = await _build_voice_assessment_from_segments(
        session_id=int(session_id),
        transcript_segments=transcript_segments,
    )
    if not assessment:
        return None
    return store_voice_assessment(assessment)
