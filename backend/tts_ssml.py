"""
SSML builder and timepoint parser for per-page TTS with word-level sync.

Key design: the SSML contains the FULL original text (punctuation, commas,
whitespace preserved) so Google TTS can apply natural prosody. <mark> tags are
injected directly before each word in the text; they are transparent to speech.
Chunks are split at sentence/paragraph boundaries, never mid-sentence.

Usage:
    result = synthesize_page_with_timings(
        page_text=page_text, lang_code="de-DE", voice_name="de-DE-Neural2-C"
    )
    # result: {audio_bytes, mime, duration_ms, word_timings}
    # word_timings: [{wid: "0", word: "Als", start_ms: 50, end_ms: 280,
    #                 char_start: 0, char_end: 3}, ...]
    # wid is positional index (0-based string), mapped to frontend token by position
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
    punctuation and whitespace are preserved in the SSML.
    """
    if not words:
        return [(page_text, [])]

    chunks: list[tuple[str, list[dict]]] = []
    word_start = 0        # index into words[]
    char_start = 0        # char position in page_text

    while word_start < len(words):
        # Find how many words can fit in one SSML chunk (linear scan is fine
        # for typical page sizes of a few hundred words).
        word_end = word_start
        ssml_len = len("<speak></speak>")

        for wi in range(word_start, len(words)):
            w = words[wi]
            # Text length from char_start to end of this word (full text slice)
            slice_len = w["char_end"] - char_start
            # Add mark overhead for every word in the chunk
            needed = slice_len + (wi - word_start + 1) * _MARK_OVERHEAD
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
                        extra_needed = extra_slice + (new_end - word_start + 1) * _MARK_OVERHEAD
                        if extra_needed > _SSML_CHUNK_MAX_CHARS:
                            break
                        new_end += 1
                    word_end = new_end
                    break

            chunk_char_end = words[word_end - 1]["char_end"]
        else:
            chunk_char_end = len(page_text)

        chunk_text = page_text[char_start:chunk_char_end]
        chunk_words = words[word_start:word_end]
        chunks.append((chunk_text, chunk_words))

        char_start = chunk_char_end
        word_start = word_end

    return chunks


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
