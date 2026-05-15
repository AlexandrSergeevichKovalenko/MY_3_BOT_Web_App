"""
SSML builder and timepoint parser for per-page TTS with word-level sync.

Usage:
    words = segment_page_words(page_text)
    result = synthesize_page_with_timings(
        page_text=page_text, lang_code="de-DE", voice_name="de-DE-Neural2-C"
    )
    # result: {audio_bytes, mime, duration_ms, word_timings}
    # word_timings: [{wid: "0", word: "Als", start_ms: 50, end_ms: 280}, ...]
    # wid is positional index (0-based string), mapped to frontend token.wid by position
"""

import io
import re
import xml.sax.saxutils

# Words that match this regex are included in SSML marks.
# Mirrors the frontend fallback regex for word detection.
_WORD_RE = re.compile(
    r"[A-Za-z0-9À-ÿЀ-ӿªµºÀ-Ö"
    r"Ø-öø-ˁˆ-ˑˠ-ˤ'-]+"
)

# Google TTS SSML limit is 5000 chars; leave buffer for XML overhead
_SSML_CHUNK_MAX_CHARS = 4500


def segment_page_words(text: str) -> list[dict]:
    """
    Split page text into a flat word list with positional indices.
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


def _build_ssml_chunk(words: list[dict], mark_offset: int = 0) -> tuple[str, list[dict]]:
    """
    Build a single SSML string for a list of words.
    mark_offset: starting mark number (for multi-chunk runs).
    Returns (ssml_text, mark_index) where mark_index maps mark_name -> word info.
    """
    parts = ["<speak>"]
    mark_index = []
    for i, w in enumerate(words):
        mark_num = mark_offset + i + 1
        mark_name = f"w{mark_num}"
        mark_index.append({
            "mark_name": mark_name,
            "idx": w["idx"],
            "value": w["value"],
        })
        escaped = xml.sax.saxutils.escape(str(w["value"]))
        parts.append(f'<mark name="{mark_name}"/>{escaped} ')
    parts.append("</speak>")
    return "".join(parts), mark_index


def chunk_words_for_ssml(words: list[dict]) -> list[list[dict]]:
    """Split words into chunks that each fit within the SSML char limit."""
    chunks: list[list[dict]] = []
    current: list[dict] = []
    current_chars = len("<speak></speak>")

    for w in words:
        # Estimate SSML overhead per word: <mark name="w99999"/> + word + space
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


def parse_timepoints_for_chunk(
    timepoints,
    mark_index: list[dict],
    chunk_duration_ms: int,
    time_offset_ms: int = 0,
) -> list[dict]:
    """
    Convert Google TTS timepoints for one chunk into word_timings dicts.
    time_offset_ms: add to all start/end values (for multi-chunk concatenation).
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
        })
    return result
