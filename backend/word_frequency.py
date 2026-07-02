"""German word-frequency ranking for spaced-repetition new-card ordering.

Produces a single comparable integer `frequency_rank` per saved vocabulary
entry, so the new-card introduction query can simply
`ORDER BY frequency_rank ASC NULLS LAST, created_at ASC` and introduce the most
useful (most frequent) words first.

Hybrid signal, by descending reliability:
  1. Corpus rank  — the entry's lemma is found in an offline everyday-German
     frequency list (OpenSubtitles-based, ~49k word forms). Real ordinal
     rank 1..~49_311. Free, instant, deterministic.
  2. LLM band     — not in the corpus, but the dictionary enrichment stored a
     `frequency` / `level` estimate in response_json. Mapped to a synthetic
     band that always sorts AFTER every corpus word but still keeps
     common > uncommon > rare.
  3. None         — no signal; sorts last (NULLS LAST) and falls back to
     insertion order (created_at).
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

_FREQ_PATH = Path(__file__).resolve().parent / "data" / "de_frequency_50k.txt"

# Corpus ranks are small (1..~49_311). LLM bands start well above any possible
# corpus rank so a corpus hit always wins.
_LLM_BASE = 1_000_000
_BUCKET_OFFSET = {
    "very_common": 0,
    "common": 100_000,
    "uncommon": 200_000,
    "rare": 300_000,
}
# Tiny nudge so, within a bucket, easier CEFR sorts a touch earlier.
_CEFR_NUDGE = {"A1": 0, "A2": 200, "B1": 400, "B2": 600, "C1": 800, "C2": 1_000}

# Words with neither a corpus hit nor an LLM frequency band get this "unknown"
# band — after every ranked word (rare = 300_000), but a REAL value, not NULL.
# This keeps them out of the nightly re-scan and safe from being clobbered, while
# still sorting them last (ordered by created_at via the SQL tiebreak).
_FALLBACK_BAND = _LLM_BASE + 500_000

# German articles / determiners to strip before matching a single-word lemma.
_ARTICLES = {
    "der", "die", "das", "den", "dem", "des",
    "ein", "eine", "einen", "einem", "einer", "eines",
}

_RANKS: dict[str, int] | None = None


def _load_ranks() -> dict[str, int]:
    """Load word -> rank (1-based, 1 = most frequent). Cached for process life."""
    global _RANKS
    if _RANKS is not None:
        return _RANKS
    ranks: dict[str, int] = {}
    try:
        with _FREQ_PATH.open("r", encoding="utf-8") as fh:
            for i, line in enumerate(fh, start=1):
                word = line.strip()
                if word and word not in ranks:
                    ranks[word] = i
    except FileNotFoundError:
        logger.warning("word_frequency: list not found at %s; corpus ranking disabled", _FREQ_PATH)
    except Exception:  # pragma: no cover - defensive
        logger.exception("word_frequency: failed to load frequency list")
    _RANKS = ranks
    return _RANKS


def _clean_token(tok: str) -> str:
    return tok.strip().strip(".,;:!?«»\"'()[]–—…").lower()


def normalize_lemma(word_de: str | None) -> str | None:
    """Reduce a German headword to a single lowercase lemma for corpus lookup.

    Strips a leading article and lowercases. Returns None for phrases
    (more than one content token) so multi-word entries do NOT get matched to
    their first word's frequency — they fall through to the LLM band instead.
    """
    if not word_de:
        return None
    tokens = [t for t in _clean_token(word_de).split() if t]
    if tokens and tokens[0] in _ARTICLES:
        tokens = tokens[1:]
    if len(tokens) != 1:
        return None
    tok = tokens[0]
    return tok or None


def _coerce_response_json(response_json) -> dict:
    if isinstance(response_json, dict):
        return response_json
    if isinstance(response_json, str) and response_json.strip():
        try:
            obj = json.loads(response_json)
            return obj if isinstance(obj, dict) else {}
        except (ValueError, TypeError):
            return {}
    return {}


def compute_frequency_rank(word_de: str | None, response_json=None) -> int | None:
    """Return a comparable rank (lower = introduce earlier), or None if unknown."""
    lemma = normalize_lemma(word_de)
    if lemma:
        rank = _load_ranks().get(lemma)
        if rank is not None:
            return rank

    data = _coerce_response_json(response_json)
    level = (data.get("level") or "").strip().upper()
    freq = (data.get("frequency") or "").strip().lower()
    offset = _BUCKET_OFFSET.get(freq)
    if offset is not None:
        return _LLM_BASE + offset + _CEFR_NUDGE.get(level, 500)

    # No corpus hit and no frequency band → a real "unknown" rank (never NULL),
    # as long as there is an actual word to rank.
    if str(word_de or "").strip():
        return _FALLBACK_BAND + _CEFR_NUDGE.get(level, 500)
    return None


# Warm the corpus at import time (process startup) so the first word-save never
# pays the ~49k-line load inside its DB transaction.
try:
    _load_ranks()
except Exception:  # pragma: no cover - defensive
    logger.debug("word_frequency: eager warm failed", exc_info=True)
