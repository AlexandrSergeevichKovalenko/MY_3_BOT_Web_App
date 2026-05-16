"""
SSML builder and timepoint parser for per-page TTS with reader sync.

Reader audio should sound natural first. To avoid robotic delivery, the SSML
now keeps marks sparse: one mark per natural timing span (usually a sentence,
sometimes a chunk fragment when a sentence is too long for the provider limit).
Word-level timings for highlighting/seeking are then interpolated inside each
span from the returned mark timepoints.

The SSML still preserves the FULL original text (punctuation, commas,
whitespace preserved) so Google TTS can apply natural prosody.
"""

import io
import re
import xml.sax.saxutils

# Words included in SSML marks (same class as frontend fallback regex).
_WORD_RE = re.compile(
    r"[A-Za-z0-9À-ÿЀ-ӿªµºÀ-Ö"
    r"Ø-öø-ˁˆ-ˑˠ-ˤ'-]+"
)

# Google TTS SSML hard limit; leave generous buffer for XML overhead.
_SSML_CHUNK_MAX_CHARS = 4500

# Per-word SSML mark overhead estimate: <mark name="wNNNNN"/>  (~22 chars)
_MARK_OVERHEAD = 22
_PERIOD_SENTENCE_BREAK_RE = re.compile(r'\.(?:["\'»”’)\]]*)\s*$')
_READER_CHUNK_SAFETY_OVERHEAD = 1000


# ---------------------------------------------------------------------------
# Word segmentation
# ---------------------------------------------------------------------------

def segment_page_words(text: str) -> list[dict]:
    """
    Find all words in *text* and return their positions.
    Returns [{"idx": 0, "value": "Als", "char_start": 0, "char_end": 3}, ...]
    """
    words = []
    idx = 0
    for match in _WORD_RE.finditer(str(text or "")):
        words.append({
            "idx": idx,
            "value": match.group(0),
            "char_start": match.start(),
            "char_end": match.end(),
        })
        idx += 1
    return words


# ---------------------------------------------------------------------------
# Sentence-boundary chunking
# ---------------------------------------------------------------------------

def _is_sentence_boundary(text: str, pos: int) -> bool:
    """True if *pos* (0-based index in text) looks like a sentence end."""
    if pos >= len(text):
        return True
    ch = text[pos]
    if ch in ".!?":
        # Check it's followed by space/newline (not 'e.g.' etc.)
        after = text[pos + 1] if pos + 1 < len(text) else " "
        return after in (" ", "\n", "\r", "\t", '"', "'", "»", "\"")
    return ch in ("\n",)


def _find_best_split(text: str, start: int, end: int) -> int:
    """
    Search backwards from *end* in text[start:end] for a good split point
    (sentence boundary first, then paragraph, then any whitespace).
    Returns the split position (absolute index in *text*).
    """
    # Prefer sentence boundary within last 30 % of the window
    search_from = max(start, end - max(80, (end - start) // 3))
    for i in range(end - 1, search_from - 1, -1):
        if _is_sentence_boundary(text, i):
            return i + 1
    # Fallback: paragraph break
    for i in range(end - 1, start, -1):
        if text[i] == "\n":
            return i + 1
    # Last resort: word boundary (space)
    for i in range(end - 1, start, -1):
        if text[i] == " ":
            return i + 1
    return end


def chunk_text_with_words(page_text: str, words: list[dict]) -> list[tuple[str, list[dict]]]:
    """
    Split (page_text, words) into SSML-sized chunks at natural boundaries.
    Returns [(chunk_text_slice, chunk_words), ...].
    Each chunk_text_slice is the *original* substring of page_text so that
    punctuation and whitespace are preserved in the SSML. Trailing punctuation
    and spacing stay attached to the chunk that produced them instead of being
    pushed to the next chunk start.
    """
    if not words:
        return [(page_text, [])]

    chunks: list[tuple[str, list[dict]]] = []
    word_start = 0        # index into words[]
    char_start = 0        # char position in page_text

    while word_start < len(words):
        # Find how much raw text can fit in one SSML chunk. Reader audio now
        # uses sparse marks at timing spans, so keep a fixed safety budget for
        # SSML overhead instead of charging every single word as a hard split
        # cost. This allows longer, more natural synthesis chunks.
        word_end = word_start

        for wi in range(word_start, len(words)):
            w = words[wi]
            # Text length from char_start to end of this word (full text slice)
            slice_len = w["char_end"] - char_start
            needed = slice_len + _READER_CHUNK_SAFETY_OVERHEAD
            if needed > _SSML_CHUNK_MAX_CHARS and wi > word_start:
                break
            word_end = wi + 1

        # word_end is now the tentative end (exclusive) of this chunk.
        # Try to push it to a sentence boundary in the *following* text.
        if word_end < len(words):
            raw_char_end = words[word_end - 1]["char_end"]
            # Scan up to 300 chars ahead in the text for a sentence end that
            # still precedes the next word's start.
            next_word_start = words[word_end]["char_start"]
            scan_end = min(next_word_start, raw_char_end + 300)
            for ci in range(raw_char_end, scan_end):
                if _is_sentence_boundary(page_text, ci):
                    # Everything up to ci+1 is part of this chunk.
                    # Include any words whose char_start < ci+1
                    new_end = word_end
                    while (new_end < len(words) and
                           words[new_end]["char_start"] < ci + 1):
                        extra_w = words[new_end]
                        extra_slice = extra_w["char_end"] - char_start
                        extra_needed = extra_slice + _READER_CHUNK_SAFETY_OVERHEAD
                        if extra_needed > _SSML_CHUNK_MAX_CHARS:
                            break
                        new_end += 1
                    word_end = new_end
                    break

            chunk_char_end = words[word_end]["char_start"] if word_end < len(words) else len(page_text)
        else:
            chunk_char_end = len(page_text)

        chunk_text = page_text[char_start:chunk_char_end]
        chunk_words = words[word_start:word_end]
        chunks.append((chunk_text, chunk_words))

        char_start = chunk_char_end
        word_start = word_end

    return chunks


def _gap_indicates_sentence_break(gap_text: str, next_word_value: str | None) -> bool:
    gap = str(gap_text or "")
    if not gap:
        return not next_word_value
    if "\n\n" in gap:
        return True

    stripped = gap.rstrip()
    if not stripped:
        return False
    if any(ch in stripped for ch in ("!", "?", "…")):
        return True
    if _PERIOD_SENTENCE_BREAK_RE.search(gap):
        next_word = str(next_word_value or "").strip()
        if not next_word:
            return True
        first_char = next_word[:1]
        return not first_char.isalpha() or first_char.isupper()
    return False


def segment_timing_spans(
    chunk_text: str,
    chunk_words: list[dict],
    text_char_offset: int,
) -> list[dict]:
    """
    Split a chunk into natural timing spans for SSML marks.

    In the common case a span is a full sentence. If the main chunker had to
    cut a very long sentence for provider limits, the span simply becomes that
    chunk fragment.
    """
    if not chunk_words:
        return []

    spans: list[dict] = []
    span_word_start = 0
    span_char_start = text_char_offset
    chunk_end_char = text_char_offset + len(chunk_text)

    for idx, word in enumerate(chunk_words):
        next_word = chunk_words[idx + 1] if idx + 1 < len(chunk_words) else None
        next_char_start = next_word["char_start"] if next_word else chunk_end_char
        gap_start = max(0, word["char_end"] - text_char_offset)
        gap_end = max(gap_start, next_char_start - text_char_offset)
        gap_text = chunk_text[gap_start:gap_end]

        if next_word is not None and not _gap_indicates_sentence_break(gap_text, next_word.get("value")):
            continue

        span_char_end = next_char_start
        rel_start = max(0, span_char_start - text_char_offset)
        rel_end = max(rel_start, span_char_end - text_char_offset)
        spans.append({
            "char_start": span_char_start,
            "char_end": span_char_end,
            "text": chunk_text[rel_start:rel_end],
            "words": chunk_words[span_word_start:idx + 1],
        })
        span_word_start = idx + 1
        span_char_start = span_char_end

    return [span for span in spans if span.get("words")]


# ---------------------------------------------------------------------------
# SSML building
# ---------------------------------------------------------------------------

def _build_ssml_from_chunk(
    chunk_text: str,
    chunk_words: list[dict],
    text_char_offset: int,
    mark_offset: int,
) -> tuple[str, list[dict]]:
    """
    Build a single <speak>…</speak> block that contains the FULL chunk_text
    with <mark> tags injected before each word.

    text_char_offset: chunk_text starts at this position in the original page.
    mark_offset: start mark numbering from here (for multi-chunk pages).
    Returns (ssml_string, mark_index).
    """
    parts = ["<speak>"]
    mark_index: list[dict] = []
    prev_rel = 0  # position relative to chunk_text

    for i, w in enumerate(chunk_words):
        rel_start = w["char_start"] - text_char_offset
        rel_end = w["char_end"] - text_char_offset

        # Text between the previous word and this one (spaces, punctuation, …)
        gap = chunk_text[prev_rel:rel_start]
        if gap:
            parts.append(xml.sax.saxutils.escape(gap))

        mark_name = f"w{mark_offset + i + 1}"
        parts.append(f'<mark name="{mark_name}"/>')
        parts.append(xml.sax.saxutils.escape(chunk_text[rel_start:rel_end]))
        prev_rel = rel_end

        mark_index.append({
            "mark_name": mark_name,
            "idx": w["idx"],
            "value": w["value"],
            "char_start": w["char_start"],
            "char_end": w["char_end"],
        })

    # Trailing text after the last word (e.g. period, newline)
    tail = chunk_text[prev_rel:]
    if tail:
        parts.append(xml.sax.saxutils.escape(tail))

    parts.append("</speak>")
    return "".join(parts), mark_index


def _build_ssml_from_spans(
    chunk_text: str,
    timing_spans: list[dict],
    text_char_offset: int,
    mark_offset: int,
) -> tuple[str, list[dict]]:
    """
    Build a single <speak> block with sparse marks at natural timing spans.

    We keep the original text untouched and insert one mark before the first
    word of each span. This gives Google TTS room for natural prosody while we
    later reconstruct word timings inside each span.
    """
    parts = ["<speak>"]
    mark_index: list[dict] = []
    prev_rel = 0

    for span_idx, span in enumerate(timing_spans):
        span_words = list(span.get("words") or [])
        if not span_words:
            continue
        rel_mark_start = max(0, span_words[0]["char_start"] - text_char_offset)
        rel_span_end = max(rel_mark_start, span["char_end"] - text_char_offset)

        gap = chunk_text[prev_rel:rel_mark_start]
        if gap:
            parts.append(xml.sax.saxutils.escape(gap))

        mark_name = f"s{mark_offset + span_idx + 1}"
        parts.append(f'<mark name="{mark_name}"/>')
        parts.append(xml.sax.saxutils.escape(chunk_text[rel_mark_start:rel_span_end]))
        prev_rel = rel_span_end

        mark_index.append({
            "mark_name": mark_name,
            "char_start": span["char_start"],
            "char_end": span["char_end"],
            "words": span_words,
        })

    tail = chunk_text[prev_rel:]
    if tail:
        parts.append(xml.sax.saxutils.escape(tail))

    parts.append("</speak>")
    return "".join(parts), mark_index


# ---------------------------------------------------------------------------
# Timepoint parsing
# ---------------------------------------------------------------------------

def parse_timepoints_for_chunk(
    timepoints,
    mark_index: list[dict],
    chunk_duration_ms: int,
    time_offset_ms: int = 0,
) -> list[dict]:
    """
    Convert Google TTS timepoints for one chunk into word_timings dicts.
    time_offset_ms: added to all start/end values (multi-chunk concatenation).
    """
    starts: dict[str, int] = {
        tp.mark_name: int(tp.time_seconds * 1000)
        for tp in timepoints
    }
    result = []
    for i, m in enumerate(mark_index):
        raw_start = starts.get(m["mark_name"], 0)
        start_ms = raw_start + time_offset_ms

        if i + 1 < len(mark_index):
            raw_next = starts.get(mark_index[i + 1]["mark_name"], raw_start + 200)
            end_ms = raw_next + time_offset_ms
        else:
            end_ms = chunk_duration_ms + time_offset_ms

        result.append({
            "wid": str(m["idx"]),
            "word": m["value"],
            "start_ms": start_ms,
            "end_ms": max(end_ms, start_ms + 50),
            "char_start": m.get("char_start", 0),
            "char_end": m.get("char_end", 0),
        })
    return result


def parse_timepoints_for_spans(
    timepoints,
    mark_index: list[dict],
    chunk_duration_ms: int,
    time_offset_ms: int = 0,
) -> list[dict]:
    """
    Convert sparse mark timepoints into per-word timings.

    Each mark anchors a natural timing span. Word timings inside that span are
    distributed proportionally using original text distances, which keeps
    punctuation and whitespace influencing the pacing without forcing the TTS
    provider to process a mark before every word.
    """
    if not mark_index:
        return []

    starts: dict[str, int] = {
        tp.mark_name: int(tp.time_seconds * 1000)
        for tp in timepoints
    }
    chunk_end_ms = time_offset_ms + int(chunk_duration_ms or 0)
    result: list[dict] = []
    previous_span_end_ms = time_offset_ms

    for idx, span in enumerate(mark_index):
        raw_start = starts.get(span["mark_name"], previous_span_end_ms - time_offset_ms)
        span_start_ms = max(time_offset_ms, time_offset_ms + raw_start)
        if idx + 1 < len(mark_index):
            raw_next = starts.get(mark_index[idx + 1]["mark_name"], raw_start)
            span_end_ms = max(span_start_ms, time_offset_ms + raw_next)
        else:
            span_end_ms = max(span_start_ms, chunk_end_ms)
        previous_span_end_ms = span_end_ms

        words = list(span.get("words") or [])
        if not words:
            continue

        weights: list[int] = []
        for word_index, word in enumerate(words):
            if word_index + 1 < len(words):
                next_char = int(words[word_index + 1]["char_start"])
            else:
                next_char = int(span.get("char_end") or word.get("char_end") or word["char_start"])
            weight = max(1, next_char - int(word["char_start"]))
            weights.append(weight)

        total_weight = sum(weights) or len(words)
        span_duration_ms = max(0, span_end_ms - span_start_ms)
        accumulated_weight = 0
        current_start_ms = span_start_ms

        for word_index, word in enumerate(words):
            accumulated_weight += weights[word_index]
            if word_index == len(words) - 1:
                current_end_ms = span_end_ms
            else:
                current_end_ms = span_start_ms + int(round((accumulated_weight / total_weight) * span_duration_ms))
                if current_end_ms <= current_start_ms:
                    current_end_ms = current_start_ms + 1
                if current_end_ms > span_end_ms:
                    current_end_ms = span_end_ms

            result.append({
                "wid": str(word["idx"]),
                "word": word["value"],
                "start_ms": current_start_ms,
                "end_ms": max(current_end_ms, current_start_ms + 1),
                "char_start": word.get("char_start", 0),
                "char_end": word.get("char_end", 0),
            })
            current_start_ms = min(current_end_ms, span_end_ms)

    return result


# ---------------------------------------------------------------------------
# Legacy helpers (kept for import compatibility)
# ---------------------------------------------------------------------------

def _build_ssml_chunk(words: list[dict], mark_offset: int = 0) -> tuple[str, list[dict]]:
    """Deprecated word-only SSML builder — use _build_ssml_from_chunk instead."""
    parts = ["<speak>"]
    mark_index = []
    for i, w in enumerate(words):
        mark_num = mark_offset + i + 1
        mark_name = f"w{mark_num}"
        mark_index.append({
            "mark_name": mark_name,
            "idx": w["idx"],
            "value": w["value"],
            "char_start": w.get("char_start", 0),
            "char_end": w.get("char_end", 0),
        })
        escaped = xml.sax.saxutils.escape(str(w["value"]))
        parts.append(f'<mark name="{mark_name}"/>{escaped} ')
    parts.append("</speak>")
    return "".join(parts), mark_index


def chunk_words_for_ssml(words: list[dict]) -> list[list[dict]]:
    """Deprecated word-count chunker — use chunk_text_with_words instead."""
    chunks: list[list[dict]] = []
    current: list[dict] = []
    current_chars = len("<speak></speak>")
    for w in words:
        overhead = len(f'<mark name="w99999"/>{xml.sax.saxutils.escape(w["value"])} ')
        if current_chars + overhead > _SSML_CHUNK_MAX_CHARS and current:
            chunks.append(current)
            current = []
            current_chars = len("<speak></speak>")
        current.append(w)
        current_chars += overhead
    if current:
        chunks.append(current)
    return chunks
