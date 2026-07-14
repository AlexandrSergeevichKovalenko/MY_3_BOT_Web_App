"""Shared answer-evaluation logic for in-group tasks (rebus + crossword).

Single source of truth for *correctness*, used by both:
  • the bot's free-text answer handler (bot_3.py), and
  • the Mini App answer endpoints (backend_server.py /api/answer/*).

The pure ``check_*`` functions take already-loaded data and return verdicts —
no DB, no IO — so they are trivially unit-testable. The ``evaluate_*`` functions
load the task from the DB, run the pure check, record the answer (with
anti-replay), and return a render-ready dict.

Anti-spoiler / anti-replay contract:
  • ``load_*_task`` never returns the correct answer (it only describes the
    input affordance) unless the user has *already answered*.
  • ``evaluate_*`` records once; a second submission returns the stored verdict
    instead of overwriting it.
"""

from __future__ import annotations

import re

KNOWN_ARTICLES = {"der", "die", "das"}

# Freeform-quiz answer matching (ported from bot_3 so the bot text handler and
# the Mini-App endpoint stay one logic).
_GERMAN_ARTICLE_TOKENS = {
    "der", "die", "das", "den", "dem", "des",
    "ein", "eine", "einen", "einem", "einer", "eines",
}


def _normalize_quiz_text(value: str) -> str:
    # Fold ß → ss AND the umlaut transliterations ä→ae, ö→oe, ü→ue so a learner typing
    # on a keyboard without German keys (schoen ↔ schön, Maedchen ↔ Mädchen) isn't
    # graded as wrong. This is the standard German ASCII equivalence (same justification
    # as the ß↔ss fold) — it never makes a genuinely different word match (schon ≠ schön).
    lowered = str(value or "").lower().replace("ß", "ss")
    lowered = lowered.replace("ä", "ae").replace("ö", "oe").replace("ü", "ue")
    cleaned = re.sub(r"[^a-zäöüà-ÿ0-9\s'\-]", " ", lowered)
    cleaned = cleaned.replace("-", " ")
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def check_quiz_freeform_exact(*, user_text: str, correct_text: str) -> bool:
    """Lightly normalized exact comparison for full phrases/groups."""
    user_norm = _normalize_quiz_text(user_text)
    correct_norm = _normalize_quiz_text(correct_text)
    return bool(user_norm and correct_norm and user_norm == correct_norm)


def check_quiz_freeform_deterministic(*, user_text: str, correct_text: str) -> bool:
    user_norm = _normalize_quiz_text(user_text)
    correct_norm = _normalize_quiz_text(correct_text)
    if not user_norm or not correct_norm:
        return False
    if user_norm == correct_norm:
        return True
    user_tokens = user_norm.split()
    correct_tokens = correct_norm.split()
    if not user_tokens or not correct_tokens:
        return False
    user_article = user_tokens[0] if user_tokens[0] in _GERMAN_ARTICLE_TOKENS else ""
    correct_article = correct_tokens[0] if correct_tokens[0] in _GERMAN_ARTICLE_TOKENS else ""
    allow_article_strip = not (user_article and correct_article and user_article != correct_article)

    def _variants(tokens, strip_article):
        variants = {" ".join(tokens)}
        if strip_article and tokens and tokens[0] in _GERMAN_ARTICLE_TOKENS:
            tail = tokens[1:]
            if tail:
                variants.add(" ".join(tail))
        return {v for v in variants if v}

    return bool(_variants(user_tokens, allow_article_strip) & _variants(correct_tokens, allow_article_strip))


def accepted_pairs(accepted) -> list[dict]:
    """Normalize a synonym/antonym 'accepted' list to [{de, ru}] objects.
    Backward-compatible: old items stored plain German strings (ru = '')."""
    out: list[dict] = []
    for a in (accepted or []):
        if isinstance(a, dict):
            de = str(a.get("de") or "").strip()
            ru = str(a.get("ru") or "").strip()
        else:
            de = str(a or "").strip()
            ru = ""
        if de:
            out.append({"de": de, "ru": ru})
    return out


def accepted_de(accepted) -> list[str]:
    """Just the German strings of an 'accepted' list (for matching/hashing)."""
    return [p["de"] for p in accepted_pairs(accepted)]


# ── Pure correctness checks (no IO) ──────────────────────────────────────────

def check_rebus(*, correct_word: str, article: str, raw_input: str) -> dict:
    """Compare a free-text rebus answer against the expected ``article word``.

    Returns a verdict dict. ``needs_article`` is True when an article is
    required but the user omitted it — the caller should nudge *without*
    consuming the attempt.
    """
    correct_word = str(correct_word or "").strip()
    article = str(article or "").strip()
    user_input = str(raw_input or "").strip()

    input_parts = user_input.split(None, 1)  # split on first whitespace
    user_article = input_parts[0].lower() if len(input_parts) >= 2 else ""
    user_word = input_parts[1].strip() if len(input_parts) >= 2 else user_input

    if article and user_article not in KNOWN_ARTICLES:
        return {
            "needs_article": True,
            "is_correct": False,
            "article_correct": False,
            "word_correct": False,
            "user_word": user_word,
            "user_article": user_article,
        }

    article_correct = (not article) or (user_article == article.lower())
    word_correct = user_word.lower() == correct_word.lower()
    return {
        "needs_article": False,
        "is_correct": bool(article_correct and word_correct),
        "article_correct": bool(article_correct),
        "word_correct": bool(word_correct),
        "user_word": user_word,
        "user_article": user_article,
    }


def check_crossword(*, hidden_words: list[dict], raw_input: str) -> list[dict]:
    """Map a whitespace-separated answer string onto the hidden words in order.

    ``hidden_words`` is a list of {number, word, direction, clue_de, clue_ru}
    sorted by number. Returns one result dict per hidden word.
    """
    raw_parts = str(raw_input or "").strip().split()
    results: list[dict] = []
    for i, hw in enumerate(hidden_words):
        user_answer = raw_parts[i].upper().strip() if i < len(raw_parts) else ""
        correct = str(hw.get("word") or "").upper()
        results.append({
            "number": hw.get("number"),
            "direction": hw.get("direction", "across"),
            "correct": correct,
            "user_answer": user_answer,
            "is_correct": user_answer == correct,
            "clue_de": str(hw.get("clue_de") or ""),
            "clue_ru": str(hw.get("clue_ru") or ""),
        })
    return results


# ── Rebus: load + evaluate ───────────────────────────────────────────────────

def _load_rebus(dispatch_id: int) -> tuple[dict, dict] | None:
    from backend.database import get_rebus_dispatch_by_id, get_rebus_bank_entry
    dispatch = get_rebus_dispatch_by_id(int(dispatch_id))
    if not dispatch:
        return None
    entry = get_rebus_bank_entry(str(dispatch.get("compound_id") or ""))
    if not entry:
        return None
    return dispatch, entry


def load_rebus_task(*, dispatch_id: int, user_id: int) -> dict | None:
    """Metadata to render the rebus input. Never reveals the answer unless the
    user has already answered (then it surfaces the stored verdict + answer)."""
    loaded = _load_rebus(dispatch_id)
    if not loaded:
        return None
    _dispatch, entry = loaded
    correct_word = str(entry.get("compound_word") or "")
    article = str(entry.get("article") or "")

    from backend.database import get_rebus_answer
    existing = get_rebus_answer(dispatch_id=int(dispatch_id), user_id=int(user_id))

    meta = {
        "kind": "rebus",
        "letter_count": len(correct_word),
        "requires_article": bool(article),
        "already_answered": bool(existing),
        "image_url": "",
    }
    # The rebus image (the two component pictures + "= ?") is the actual puzzle — it's
    # sent to the Telegram chat, but when answering in the Mini-App the user no longer
    # sees it. Surface the composed card URL so the overlay can show the same picture.
    composed_key = str(entry.get("composed_image_object_key") or "")
    if composed_key:
        try:
            from backend.r2_storage import r2_public_url
            meta["image_url"] = r2_public_url(composed_key)
        except Exception:
            meta["image_url"] = ""
    if existing:
        meta["result"] = _rebus_result_payload(
            entry=entry,
            is_correct=bool(existing.get("is_correct")),
            already_answered=True,
        )
    return meta


def _rebus_result_payload(*, entry: dict, is_correct: bool, already_answered: bool,
                          article_correct: bool = True, word_correct: bool = True) -> dict:
    correct_word = str(entry.get("compound_word") or "")
    article = str(entry.get("article") or "")
    meaning_ru = str(entry.get("meaning_ru") or "")
    full_word = f"{article} {correct_word}".strip() if article else correct_word
    return {
        "kind": "rebus",
        "is_correct": bool(is_correct),
        "article_correct": bool(article_correct),
        "word_correct": bool(word_correct),
        "correct_word": correct_word,
        "article": article,
        "full_word": full_word,
        "meaning_ru": meaning_ru,
        "explanation_ru": str(entry.get("explanation_ru") or ""),
        "already_answered": bool(already_answered),
        "saveable_words": (
            [{"source": full_word, "target": meaning_ru}] if meaning_ru else []
        ),
    }


def evaluate_rebus(*, dispatch_id: int, user_id: int, raw_input: str) -> dict | None:
    """Load → check → record (once) → render-ready verdict.

    Returns None if the dispatch/bank entry is missing. Returns
    ``{"needs_article": True}`` (without recording) when the article is missing.
    """
    loaded = _load_rebus(dispatch_id)
    if not loaded:
        return None
    _dispatch, entry = loaded
    correct_word = str(entry.get("compound_word") or "")
    article = str(entry.get("article") or "")

    from backend.database import get_rebus_answer, record_rebus_answer

    existing = get_rebus_answer(dispatch_id=int(dispatch_id), user_id=int(user_id))
    if existing:
        return _rebus_result_payload(
            entry=entry, is_correct=bool(existing.get("is_correct")), already_answered=True,
        )

    verdict = check_rebus(correct_word=correct_word, article=article, raw_input=raw_input)
    if verdict.get("needs_article"):
        return {"needs_article": True, "kind": "rebus", "article": article}

    if correct_word:
        record_rebus_answer(
            dispatch_id=int(dispatch_id),
            user_id=int(user_id),
            selected_option=str(raw_input or "").strip()[:50],
            is_correct=bool(verdict["is_correct"]),
        )

    return _rebus_result_payload(
        entry=entry,
        is_correct=bool(verdict["is_correct"]),
        already_answered=False,
        article_correct=bool(verdict["article_correct"]),
        word_correct=bool(verdict["word_correct"]),
    )


# ── Crossword: load + evaluate ───────────────────────────────────────────────

def _load_crossword_hidden(dispatch_id: int) -> list[dict] | None:
    from backend.database import get_crossword_dispatch_by_id
    dispatch = get_crossword_dispatch_by_id(int(dispatch_id))
    if not dispatch:
        return None
    words_json = list(dispatch.get("words_json") or [])
    hidden = sorted(
        [w for w in words_json if w.get("hidden")],
        key=lambda x: x.get("number", 0),
    )
    return [
        {
            "number": w.get("number"),
            "word": str(w.get("word") or ""),
            "direction": w.get("direction", "across"),
            "clue_de": str(w.get("clue_de") or ""),
            "clue_ru": str(w.get("clue_ru") or ""),
        }
        for w in hidden
    ]


def load_crossword_task(*, dispatch_id: int, user_id: int) -> dict | None:
    """Spoiler-safe interactive grid: given letters (already visible in the image)
    are shown; hidden cells are empty/fillable; the answer letters never leave the
    server. Plus per-hidden-word clue + cells (for highlighting + submission), and
    the stored result if the user already answered."""
    from backend.database import get_crossword_dispatch_by_id, get_crossword_answers
    dispatch = get_crossword_dispatch_by_id(int(dispatch_id))
    if not dispatch:
        return None
    words_json = list(dispatch.get("words_json") or [])
    grid_json = list(dispatch.get("grid_json") or [])
    hidden = sorted([w for w in words_json if w.get("hidden")], key=lambda x: x.get("number", 0))
    if not hidden:
        return None

    from backend.crossword_renderer import _compute_revealed_cells, _word_start_numbers
    revealed = _compute_revealed_cells(words_json)
    starts = _word_start_numbers(words_json)
    rows = len(grid_json)
    cols = max((len(r) for r in grid_json), default=0)
    grid = []
    for r in range(rows):
        grow = grid_json[r] if r < len(grid_json) else []
        row_cells = []
        for c in range(cols):
            letter = grow[c] if c < len(grow) else None
            if not letter:
                row_cells.append(None)  # blocked cell
                continue
            cell = {}
            if (r, c) in revealed:
                cell["l"] = str(letter)   # given letter (already visible)
            else:
                cell["e"] = True          # empty / fillable (answer withheld)
            nums = starts.get((r, c))
            if nums:
                cell["n"] = min(nums)
            row_cells.append(cell)
        grid.append(row_cells)

    def _word_cells(w):
        r, c = int(w["row"]), int(w["col"])
        dr, dc = (0, 1) if w.get("direction") == "across" else (1, 0)
        return [[r + dr * i, c + dc * i] for i in range(len(str(w.get("word") or "")))]

    words = [
        {
            "number": w.get("number"),
            "direction": w.get("direction", "across"),
            "clue_de": str(w.get("clue_de") or ""),
            "clue_ru": str(w.get("clue_ru") or ""),
            "length": len(str(w.get("word") or "")),
            "cells": _word_cells(w),
        }
        for w in hidden
    ]

    existing = get_crossword_answers(dispatch_id=int(dispatch_id), user_id=int(user_id))
    meta = {
        "kind": "crossword",
        "topic": str(dispatch.get("topic") or ""),
        "rows": rows, "cols": cols, "grid": grid,
        "words": words,
        "already_answered": bool(existing),
    }
    if existing:
        hidden_for_result = [
            {"number": w.get("number"), "word": str(w.get("word") or ""),
             "direction": w.get("direction", "across"),
             "clue_de": str(w.get("clue_de") or ""), "clue_ru": str(w.get("clue_ru") or "")}
            for w in hidden
        ]
        meta["result"] = _crossword_result_from_stored(hidden_for_result, existing)
    return meta


def _summarize_crossword(results: list[dict], *, already_answered: bool) -> dict:
    correct_count = sum(1 for r in results if r["is_correct"])
    wrong = [r for r in results if not r["is_correct"] and r.get("clue_ru") and r.get("correct")]
    return {
        "kind": "crossword",
        "results": results,
        "correct_count": correct_count,
        "total": len(results),
        "already_answered": bool(already_answered),
        "saveable_words": [
            {"source": r["correct"], "target": r["clue_ru"]} for r in wrong
        ],
    }


def _crossword_result_from_stored(hidden: list[dict], stored: list[dict]) -> dict:
    by_number = {int(s["word_number"]): s for s in stored}
    results: list[dict] = []
    for hw in hidden:
        s = by_number.get(int(hw["number"])) if hw.get("number") is not None else None
        results.append({
            "number": hw["number"],
            "direction": hw["direction"],
            "correct": str(hw["word"]).upper(),
            "user_answer": str((s or {}).get("user_answer") or ""),
            "is_correct": bool((s or {}).get("is_correct")),
            "clue_de": hw["clue_de"],
            "clue_ru": hw["clue_ru"],
        })
    return _summarize_crossword(results, already_answered=True)


def evaluate_crossword(*, dispatch_id: int, user_id: int, raw_input: str) -> dict | None:
    """Load → check → record (once) → render-ready verdict for all words."""
    hidden = _load_crossword_hidden(dispatch_id)
    if not hidden:
        return None

    from backend.database import get_crossword_answers, record_crossword_answer

    existing = get_crossword_answers(dispatch_id=int(dispatch_id), user_id=int(user_id))
    if existing:
        return _crossword_result_from_stored(hidden, existing)

    results = check_crossword(hidden_words=hidden, raw_input=raw_input)
    for r in results:
        if r["number"] is not None and r["correct"] and r["user_answer"]:
            record_crossword_answer(
                dispatch_id=int(dispatch_id),
                user_id=int(user_id),
                word_number=int(r["number"]),
                user_answer=r["user_answer"],
                is_correct=bool(r["is_correct"]),
            )
    return _summarize_crossword(results, already_answered=False)


# ── Anagram (assemble-the-word): load + evaluate ─────────────────────────────

def check_anagram(*, correct_word: str, assembled: str) -> bool:
    """Case-insensitive compare of the assembled letters against the word."""
    a = "".join(str(assembled or "").split()).strip().lower()
    w = str(correct_word or "").strip().lower()
    return bool(a) and a == w


def _anagram_result_payload(*, card: dict, is_correct: bool, already_answered: bool) -> dict:
    word = str(card.get("word") or "")
    hint_ru = str(card.get("hint_ru") or "")
    return {
        "kind": "anagram",
        "is_correct": bool(is_correct),
        "correct_word": word,
        "hint_ru": hint_ru,
        "explanation": str(card.get("explanation") or ""),
        "already_answered": bool(already_answered),
        "saveable_words": ([{"source": word, "target": hint_ru}] if hint_ru else []),
    }


def load_anagram_task(*, dispatch_id: int, user_id: int) -> dict | None:
    """Render metadata: scrambled letters + hint, never the solved word (unless
    the user already answered). All letters are given by design — only their
    solved order is withheld."""
    from backend.database import get_anagram_dispatch_by_id, get_anagram_answer
    card = get_anagram_dispatch_by_id(int(dispatch_id))
    if not card:
        return None

    scrambled = str(card.get("scrambled") or "")
    if len(scrambled) < 3:
        return None

    existing = get_anagram_answer(dispatch_id=int(dispatch_id), user_id=int(user_id))
    meta = {
        "kind": "anagram",
        "hint_ru": str(card.get("hint_ru") or ""),
        "first_letter": scrambled[0],
        "last_letter": scrambled[-1],
        "length": len(scrambled),
        "middle_letters": list(scrambled[1:-1]),  # already shuffled at creation
        "already_answered": bool(existing),
    }
    if existing:
        meta["result"] = _anagram_result_payload(
            card=card, is_correct=bool(existing.get("is_correct")), already_answered=True,
        )
    return meta


def evaluate_anagram(*, dispatch_id: int, user_id: int, assembled: str) -> dict | None:
    """Load → compare → record (once) → render-ready verdict."""
    from backend.database import (
        get_anagram_dispatch_by_id, get_anagram_answer, record_anagram_answer,
    )
    card = get_anagram_dispatch_by_id(int(dispatch_id))
    if not card:
        return None

    existing = get_anagram_answer(dispatch_id=int(dispatch_id), user_id=int(user_id))
    if existing:
        return _anagram_result_payload(
            card=card, is_correct=bool(existing.get("is_correct")), already_answered=True,
        )

    is_correct = check_anagram(correct_word=card.get("word"), assembled=assembled)
    record_anagram_answer(
        dispatch_id=int(dispatch_id),
        user_id=int(user_id),
        assembled=str(assembled or ""),
        is_correct=bool(is_correct),
    )
    return _anagram_result_payload(card=card, is_correct=is_correct, already_answered=False)


# ── Multiple-choice quiz (kind="mc") — the Mini-App replacement for polls ────

def _mc_result_payload(dispatch: dict, *, is_correct: bool, selected_option_id: int,
                       freeform_text: str, already_answered: bool) -> dict:
    """Verdict view: reveal the correct option + explanation only AFTER answering."""
    options = list(dispatch.get("options") or [])
    correct_id = int(dispatch.get("correct_option_id") or 0)
    correct_text = str(dispatch.get("correct_text") or "")
    hide_correct = bool(dispatch.get("hide_correct"))
    # The answer to SHOW: the correct option (the German sentence/word the user
    # should have picked). Only in hide-correct mode is the right answer not among
    # the options → fall back to the stored correct_text (the hidden German answer).
    if not hide_correct and 0 <= correct_id < len(options):
        correct_display = str(options[correct_id])
    else:
        correct_display = correct_text
    your_text = freeform_text or (
        options[selected_option_id] if 0 <= selected_option_id < len(options) else ""
    )
    word_ru = str(dispatch.get("word_ru") or "")
    return {
        "kind": "mc",
        "is_correct": bool(is_correct),
        "correct_option_id": correct_id,
        "correct_text": correct_display,
        "correct_de": correct_display,
        "word_ru": word_ru,
        "your_answer": your_text,
        "selected_option_id": int(selected_option_id),
        "explanation": str(dispatch.get("explanation") or ""),
        "hint_ru": word_ru,
        "already_answered": bool(already_answered),
        "deepdive_card_id": dispatch.get("deepdive_card_id"),
        "saveable_words": ([{"source": correct_text, "target": word_ru}]
                           if (correct_text and word_ru) else []),
    }


def _ensure_mc_deepdive_card(dispatch: dict, user_id: int) -> int | None:
    """Create-once (and store on the dispatch) a deep-dive card for the correct
    answer, so the result's 📌/🧩 reuse it instead of minting one per open. Only
    for the dispatch owner (DM feed); group non-owners get None → the frontend
    mints its own via /deepdive/from_text."""
    if int(dispatch.get("user_id") or 0) != int(user_id):
        return None
    existing = dispatch.get("deepdive_card_id")
    if existing:
        return int(existing)
    options = list(dispatch.get("options") or [])
    correct_id = int(dispatch.get("correct_option_id") or 0)
    if not dispatch.get("hide_correct") and 0 <= correct_id < len(options):
        correct_de = str(options[correct_id])
    else:
        correct_de = str(dispatch.get("correct_text") or "")
    if not correct_de.strip():
        return None
    from backend.database import create_deepdive_card, set_mc_deepdive_card_id
    card_id = create_deepdive_card(
        user_id=int(user_id), source_text=correct_de,
        target_text=str(dispatch.get("word_ru") or ""),
        source_lang="de", target_lang="ru",
    )
    if card_id:
        set_mc_deepdive_card_id(int(dispatch["id"]), int(card_id))
        dispatch["deepdive_card_id"] = int(card_id)
    return card_id


def load_mc_task(*, dispatch_id: int, user_id: int) -> dict | None:
    """Render metadata: question + options, with the correct one hidden until the
    user answers (then the stored verdict is returned for an anti-replay reopen)."""
    from backend.database import get_mc_dispatch_by_id, get_mc_answer
    dispatch = get_mc_dispatch_by_id(int(dispatch_id))
    if not dispatch:
        return None
    options = list(dispatch.get("options") or [])
    if not options:
        return None
    existing = get_mc_answer(dispatch_id=int(dispatch_id), user_id=int(user_id))
    meta = {
        "kind": "mc",
        "question": str(dispatch.get("question") or ""),
        "options": options,
        "freeform_option": str(dispatch.get("freeform_option") or ""),
        "quiz_type": str(dispatch.get("quiz_type") or ""),
        "already_answered": bool(existing),
    }
    if existing:
        _ensure_mc_deepdive_card(dispatch, user_id)
        meta["result"] = _mc_result_payload(
            dispatch, is_correct=bool(existing.get("is_correct")),
            selected_option_id=int(existing.get("selected_option_id") or -1),
            freeform_text=str(existing.get("freeform_text") or ""),
            already_answered=True,
        )
    return meta


def evaluate_mc(*, dispatch_id: int, user_id: int, selected_option_id: int,
                freeform_text: str = "") -> dict | None:
    """Grade a multiple-choice answer in place (anti-replay). A normal option is
    correct iff it is the correct_option_id; picking the freeform option grades
    the typed text against the correct answer (deterministic, like the qf path)."""
    from backend.database import get_mc_dispatch_by_id, get_mc_answer, record_mc_answer
    dispatch = get_mc_dispatch_by_id(int(dispatch_id))
    if not dispatch:
        return None

    existing = get_mc_answer(dispatch_id=int(dispatch_id), user_id=int(user_id))
    if existing:
        return _mc_result_payload(
            dispatch, is_correct=bool(existing.get("is_correct")),
            selected_option_id=int(existing.get("selected_option_id") or -1),
            freeform_text=str(existing.get("freeform_text") or ""),
            already_answered=True,
        )

    options = list(dispatch.get("options") or [])
    freeform_option = str(dispatch.get("freeform_option") or "").strip()
    correct_text = str(dispatch.get("correct_text") or "")
    sid = int(selected_option_id)
    selected_text = options[sid] if 0 <= sid < len(options) else ""
    is_freeform_pick = bool(freeform_option) and selected_text == freeform_option

    if is_freeform_pick:
        typed = str(freeform_text or "")
        is_correct = check_quiz_freeform_deterministic(user_text=typed, correct_text=correct_text)
        if not is_correct and typed.strip():
            is_correct = _quiz_freeform_semantic_match(typed, correct_text)
    else:
        is_correct = sid == int(dispatch.get("correct_option_id") or 0)

    record_mc_answer(
        dispatch_id=int(dispatch_id), user_id=int(user_id),
        selected_option_id=sid, freeform_text=str(freeform_text or ""),
        is_correct=bool(is_correct),
    )
    _ensure_mc_deepdive_card(dispatch, user_id)
    return _mc_result_payload(
        dispatch, is_correct=is_correct, selected_option_id=sid,
        freeform_text=str(freeform_text or ""), already_answered=False,
    )


# ── Hörverständnis (listening): load + async-graded submit + poll ────────────

def _listening_result_payload(dispatch: dict, answers: list, evaluation: list,
                              *, already_answered: bool) -> dict:
    """Merge questions + the user's answers + the LLM evals into one verdict.
    Each question is scored on two axes — Inhalt (50%) + Grammatik (50%)."""
    from backend.listening_evaluator import score_evaluation
    questions = list(dispatch.get("questions_json") or [])
    answers = answers if isinstance(answers, list) else []
    evaluation = evaluation if isinstance(evaluation, list) else []
    scored = score_evaluation(len(questions), evaluation)
    per = scored["per"]
    items = []
    for i, q in enumerate(questions):
        ev = evaluation[i] if i < len(evaluation) and isinstance(evaluation[i], dict) else {}
        sc = per[i] if i < len(per) else {}
        items.append({
            "number": int(q.get("number") or (i + 1)),
            "question_de": str(q.get("question_de") or ""),
            "user_answer": str(answers[i]) if i < len(answers) else "",
            "content_correct": bool(sc.get("content_correct")),
            "content_pts": int(sc.get("content_pts") or 0),
            "grammar_pts": int(sc.get("grammar_pts") or 0),
            "q_points": int(sc.get("q_points") or 0),
            "q_max": int(sc.get("q_max") or 0),
            "content_feedback_ru": str(ev.get("content_feedback_ru") or ""),
            "grammar_feedback_ru": str(ev.get("grammar_feedback_ru") or ""),
            "grammar_errors": [str(e) for e in (ev.get("grammar_errors") or [])][:4],
            "model_answer_de": str(ev.get("model_answer_de") or ""),
            "correct_answer_de": str(q.get("correct_answer_de") or ""),
        })
    return {
        "kind": "listening",
        "items": items,
        "correct_count": scored["content_correct_count"],
        "total": len(questions),
        "total_points": scored["total_points"],
        "max_points": scored["max_points"],
        "percent": scored["percent"],
        "passed": scored["passed"],
        "already_answered": bool(already_answered),
    }


def load_listening_task(*, dispatch_id: int, user_id: int) -> dict | None:
    """Audio URL + questions for the player. Never the model answers until graded."""
    from backend.database import get_listening_dispatch_by_id, get_listening_answer_status
    dispatch = get_listening_dispatch_by_id(int(dispatch_id))
    if not dispatch:
        return None

    audio_url = ""
    object_key = str(dispatch.get("audio_object_key") or "")
    if object_key and str(dispatch.get("audio_status") or "") == "ready":
        try:
            from backend.r2_storage import r2_public_url
            audio_url = r2_public_url(object_key)
        except Exception:
            audio_url = ""

    questions = list(dispatch.get("questions_json") or [])
    meta = {
        "kind": "listening",
        "id": int(dispatch_id),
        "dispatch_id": int(dispatch_id),
        "topic": str(dispatch.get("topic") or ""),
        "difficulty": str(dispatch.get("difficulty") or "B2"),
        "audio_url": audio_url,
        "questions": [
            {"number": int(q.get("number") or (i + 1)), "question_de": str(q.get("question_de") or "")}
            for i, q in enumerate(questions)
        ],
        "already_answered": False,
    }
    status = get_listening_answer_status(dispatch_id=int(dispatch_id), user_id=int(user_id))
    if status and status.get("status") == "done" and status.get("evaluation") is not None:
        meta["already_answered"] = True
        meta["result"] = _listening_result_payload(
            dispatch, status.get("answers") or [], status.get("evaluation") or [],
            already_answered=True,
        )
    return meta


_CONTENT_ID_FIELD = {
    "rb": "compound_id", "cw": "crossword_id", "ag": "card_id",
    "au": "aufgabe_id", "ls": "listening_id", "qf": "poll_id",
}


def content_ranking_key(kind: str, dispatch_id: int) -> str:
    """Stable ranking key based on the shared TASK content, not the per-recipient
    dispatch. The same task is dispatched separately to each chat/user (own
    dispatch_id), so a dispatch-based key would make every user their own ranking
    ("1 of 1"). Keying by the content id (compound/crossword/card/aufgabe/listening/
    poll) aggregates everyone who answered the same task."""
    # MC quizzes are generated per-user (each its own dispatch), so key by the
    # shared content (quiz_type + correct answer) → everyone who got a quiz with
    # the same answer aggregates into one crowd ranking ("% ответили верно").
    if kind == "mc":
        try:
            from backend.database import get_mc_dispatch_by_id
            d = get_mc_dispatch_by_id(int(dispatch_id))
            if d:
                qt = str(d.get("quiz_type") or "").strip().lower()
                ans = _normalize_quiz_text(str(d.get("correct_text") or ""))
                if ans:
                    return f"mc:{qt}:{ans}"
        except Exception:
            pass
        return f"mc:{dispatch_id}"

    from backend.database import (
        get_rebus_dispatch_by_id, get_crossword_dispatch_by_id, get_anagram_dispatch_by_id,
        get_aufgabe_dispatch_by_id, get_listening_dispatch_by_id, get_quiz_freeform_dispatch_by_id,
    )
    loaders = {
        "rb": get_rebus_dispatch_by_id, "cw": get_crossword_dispatch_by_id,
        "ag": get_anagram_dispatch_by_id, "au": get_aufgabe_dispatch_by_id,
        "ls": get_listening_dispatch_by_id, "qf": get_quiz_freeform_dispatch_by_id,
    }
    field = _CONTENT_ID_FIELD.get(kind)
    loader = loaders.get(kind)
    if field and loader:
        try:
            d = loader(int(dispatch_id))
            cid = d.get(field) if d else None
            if cid:
                return f"{kind}:{cid}"
        except Exception:
            pass
    return f"{kind}:{dispatch_id}"


def _spawn_listening_grader(answer_id: int, german_text: str, questions: list, answers: list,
                            *, ranking_key: str, user_id: int, user_name: str, time_ms: int) -> None:
    import threading

    def _run():
        import logging
        try:
            from backend.listening_evaluator import evaluate_listening_answers, score_evaluation
            from backend.database import save_listening_evaluation, record_challenge_result
            evals = evaluate_listening_answers(german_text, questions, answers)
            # Record the ranking result BEFORE flipping status to 'done', so the
            # first poll that sees 'done' already has a ranking to show. The task
            # counts as "solved" for the rating when the combined Inhalt+Grammatik
            # score reaches >= 50% (PASS_PERCENT), not only on a perfect run.
            try:
                total = len(questions)
                scored = score_evaluation(total, evals)
                record_challenge_result(
                    challenge_key=str(ranking_key), user_id=int(user_id),
                    user_name=str(user_name or ""), is_correct=bool(total > 0 and scored["passed"]),
                    time_ms=int(time_ms or 0),
                )
            except Exception:
                logging.warning("listening grader: ranking record failed answer_id=%s", answer_id, exc_info=True)
            save_listening_evaluation(answer_id=int(answer_id), evaluation_json=evals)
        except Exception:
            logging.warning("listening grader thread failed answer_id=%s", answer_id, exc_info=True)
            try:
                from backend.database import mark_listening_evaluation_failed
                mark_listening_evaluation_failed(int(answer_id))
            except Exception:
                logging.warning("listening grader: mark_failed failed answer_id=%s", answer_id, exc_info=True)

    threading.Thread(target=_run, daemon=True).start()


def _listening_result_with_ranking(dispatch: dict, dispatch_id: int, user_id: int,
                                   answers: list, evaluation: list) -> dict:
    result = _listening_result_payload(dispatch, answers, evaluation, already_answered=True)
    ranking_key = f"ls:{dispatch.get('listening_id') or dispatch_id}"
    try:
        from backend.database import compute_challenge_ranking
        result["ranking"] = compute_challenge_ranking(challenge_key=ranking_key, user_id=int(user_id))
    except Exception:
        pass
    return result


def start_listening_evaluation(*, dispatch_id: int, user_id: int, answers: list,
                               user_name: str = "", time_ms: int = 0) -> dict | None:
    """Save answers and kick off async LLM grading. Returns pending (or the
    stored result if this attempt was already graded — anti-replay)."""
    from backend.database import (
        get_listening_dispatch_by_id, get_listening_answer_status, save_listening_answers,
    )
    dispatch = get_listening_dispatch_by_id(int(dispatch_id))
    if not dispatch:
        return None

    existing = get_listening_answer_status(dispatch_id=int(dispatch_id), user_id=int(user_id))
    if existing and existing.get("status") == "done" and existing.get("evaluation") is not None:
        return _listening_result_with_ranking(
            dispatch, int(dispatch_id), int(user_id),
            existing.get("answers") or [], existing.get("evaluation") or [],
        )

    clean_answers = [str(a or "").strip() for a in (answers or [])]
    answer_id = save_listening_answers(
        dispatch_id=int(dispatch_id), user_id=int(user_id), answers_json=clean_answers,
    )
    if not answer_id:
        return {"kind": "listening", "status": "failed"}

    ranking_key = f"ls:{dispatch.get('listening_id') or dispatch_id}"
    _spawn_listening_grader(
        answer_id, str(dispatch.get("german_text") or ""),
        list(dispatch.get("questions_json") or []), clean_answers,
        ranking_key=ranking_key, user_id=int(user_id),
        user_name=str(user_name or ""), time_ms=int(time_ms or 0),
    )
    return {"kind": "listening", "status": "pending"}


def get_listening_status(*, dispatch_id: int, user_id: int) -> dict:
    """Poll endpoint: pending / done (+result) / failed / none."""
    from backend.database import get_listening_dispatch_by_id, get_listening_answer_status
    status = get_listening_answer_status(dispatch_id=int(dispatch_id), user_id=int(user_id))
    if not status:
        return {"kind": "listening", "status": "none"}
    st = str(status.get("status") or "pending")
    if st == "done" and status.get("evaluation") is not None:
        dispatch = get_listening_dispatch_by_id(int(dispatch_id))
        if not dispatch:
            return {"kind": "listening", "status": "failed"}
        return {
            "kind": "listening", "status": "done",
            "result": _listening_result_with_ranking(
                dispatch, int(dispatch_id), int(user_id),
                status.get("answers") or [], status.get("evaluation") or [],
            ),
        }
    return {"kind": "listening", "status": st}


# ── Zahlen-Diktat (number dictation): session load + deterministic grade ──────

import re as _re_numdict


def _normalize_numdict(value: str, input_mode: str) -> str:
    """Canonical comparison: numeric → digits only; alnum → uppercase A-Z0-9.
    Strips spaces, dashes, dots, slashes, brackets so '030 12-34.56' == '0301234 56'."""
    s = str(value or "").upper()
    if str(input_mode) == "alnum":
        return _re_numdict.sub(r"[^A-Z0-9]", "", s)
    return _re_numdict.sub(r"\D", "", s)


def _numdict_audio_url(item: dict) -> str:
    key = str(item.get("audio_object_key") or "")
    if key and str(item.get("audio_status") or "") == "ready":
        try:
            from backend.r2_storage import r2_public_url
            return r2_public_url(key)
        except Exception:
            return ""
    return ""


def load_numdict_session(*, dispatch_id: int, user_id: int) -> dict | None:
    """The 3-item session bundle for the player. Never leaks unanswered answers."""
    from backend.database import get_numdict_dispatch_by_id, get_numdict_answers
    dispatch = get_numdict_dispatch_by_id(int(dispatch_id))
    if not dispatch:
        return None
    answered = get_numdict_answers(dispatch_id=int(dispatch_id), user_id=int(user_id))
    items_by_id = {it["numdict_id"]: it for it in (dispatch.get("items") or [])}
    items = []
    for idx, numdict_id in enumerate(dispatch.get("item_ids") or []):
        it = items_by_id.get(numdict_id)
        if not it:
            continue
        prior = answered.get(idx)
        items.append({
            "item_index": idx,
            "numdict_id": numdict_id,
            "prompt_de": str(it.get("prompt_de") or ""),
            "prompt_ru": str(it.get("prompt_ru") or ""),
            "input_mode": str(it.get("input_mode") or "numeric"),
            "audio_url": _numdict_audio_url(it),
            "already": (
                {"is_correct": bool(prior["is_correct"]), "typed": str(prior.get("typed") or ""),
                 "display_answer": str(it.get("display_answer") or "")}
                if prior else None
            ),
        })
    return {
        "kind": "numdict",
        "id": int(dispatch_id),
        "dispatch_id": int(dispatch_id),
        "total": len(items),
        "items": items,
    }


def grade_numdict_item(*, dispatch_id: int, user_id: int, item_index: int, typed: str,
                       user_name: str = "", time_ms: int = 0) -> dict | None:
    """Deterministic per-item grade (digit/char compare). Records ranking + anti-replay."""
    from backend.database import (
        get_numdict_dispatch_by_id, get_numdict_answers, record_numdict_item_answer,
        record_challenge_result,
    )
    dispatch = get_numdict_dispatch_by_id(int(dispatch_id))
    if not dispatch:
        return None
    item_ids = dispatch.get("item_ids") or []
    idx = int(item_index)
    if idx < 0 or idx >= len(item_ids):
        return {"kind": "numdict", "error": "bad_index"}
    numdict_id = item_ids[idx]
    item = next((it for it in (dispatch.get("items") or []) if it["numdict_id"] == numdict_id), None)
    if not item:
        return {"kind": "numdict", "error": "not_found"}

    total = len(item_ids)
    mode = str(item.get("input_mode") or "numeric")
    display_answer = str(item.get("display_answer") or "")

    answered = get_numdict_answers(dispatch_id=int(dispatch_id), user_id=int(user_id))
    if idx in answered:  # anti-replay: return the stored verdict, don't re-record
        prior = answered[idx]
        return {
            "kind": "numdict", "item_index": idx, "is_correct": bool(prior["is_correct"]),
            "display_answer": display_answer, "typed": str(prior.get("typed") or ""),
            "remaining": max(0, total - len(answered)), "already_answered": True,
            "ranking": _safe_numdict_ranking(numdict_id, user_id),
        }

    is_correct = _normalize_numdict(typed, mode) == _normalize_numdict(item.get("answer_value"), mode)
    record_numdict_item_answer(
        dispatch_id=int(dispatch_id), user_id=int(user_id), item_index=idx,
        numdict_id=str(numdict_id), typed_answer=str(typed or ""), is_correct=is_correct,
    )
    try:
        record_challenge_result(
            challenge_key=f"nd:{numdict_id}", user_id=int(user_id),
            user_name=str(user_name or ""), is_correct=bool(is_correct), time_ms=int(time_ms or 0),
        )
    except Exception:
        pass
    answered_count = len(answered) + 1
    return {
        "kind": "numdict", "item_index": idx, "is_correct": bool(is_correct),
        "display_answer": display_answer, "typed": str(typed or ""),
        "remaining": max(0, total - answered_count),
        "ranking": _safe_numdict_ranking(numdict_id, user_id),
    }


def _safe_numdict_ranking(numdict_id: str, user_id: int) -> dict | None:
    try:
        from backend.database import compute_challenge_ranking
        return compute_challenge_ranking(challenge_key=f"nd:{numdict_id}", user_id=int(user_id))
    except Exception:
        return None


# ── Self-paced practice trainer (kind np): endless feed + private grade ───────
#
# Sibling of the scheduled Zahlen-Diktat: no dispatch row, no leaderboard/mastery
# writes — a pure practice sandbox. The daily free cap is reserved on SERVE so each
# delivered exercise costs the user one unit (Pro is unlimited). Items are pulled
# least-recently-seen-by-this-user from the same pre-generated, audio-ready bank,
# so serving stays a light, indexed read (heavy GPT+TTS work stays in the nightly
# pool job). NUMDICT_PRACTICE_FEATURE_KEY mirrors the database registry entry.
NUMDICT_PRACTICE_FEATURE_KEY = "numdict_practice_daily"


def load_numdict_practice_next(*, user_id: int) -> dict:
    """Serve ONE endless practice item (least-recently-seen-by-user). Reserves the
    daily free-feature unit first; if capped, returns a friendly 'done' payload for
    the upsell screen instead of an error. Marks the item seen so the next call
    advances. NEVER leaks answer_value / display_answer."""
    import time
    from backend.database import (
        reserve_free_feature_usage, pick_numdict_practice_item, mark_numdict_practice_seen,
    )

    reservation = reserve_free_feature_usage(
        user_id=int(user_id),
        feature_key=NUMDICT_PRACTICE_FEATURE_KEY,
        idempotency_key=f"{NUMDICT_PRACTICE_FEATURE_KEY}:{int(user_id)}:{time.time_ns()}",
        metadata={"origin": "numdict_practice"},
    )
    if reservation.get("blocked"):
        err = reservation.get("error") if isinstance(reservation.get("error"), dict) else {}
        return {
            "kind": "numdict_practice",
            "done": True,
            "capped": True,
            "upsell": True,
            "limit": reservation.get("limit"),
            "used": reservation.get("used"),
            "reset_at": err.get("reset_at"),
            "title": err.get("feature_title") or err.get("title"),
            "message": err.get("message")
            or "На сегодня хватит — лимит обновится завтра. Premium снимает ограничение.",
        }

    item = pick_numdict_practice_item(user_id=int(user_id))
    if not item:
        return {"kind": "numdict_practice", "done": True, "empty": True}

    mark_numdict_practice_seen(user_id=int(user_id), numdict_id=str(item["numdict_id"]))
    return {
        "kind": "numdict_practice",
        "done": False,
        "numdict_id": str(item["numdict_id"]),
        "prompt_de": str(item.get("prompt_de") or ""),
        "prompt_ru": str(item.get("prompt_ru") or ""),
        "input_mode": str(item.get("input_mode") or "numeric"),
        "audio_url": _numdict_audio_url(item),
        "used": reservation.get("used"),
        "limit": reservation.get("limit"),
    }


def grade_numdict_practice_item(*, user_id: int, numdict_id: str, typed: str,
                                time_ms: int = 0) -> dict:
    """Deterministic grade for self-paced practice. Private drill: NO
    record_challenge_result, NO bt_3_numdict_answers write, NO mastery/leaderboard
    side-effects, NO anti-replay (each item is pulled fresh in the endless feed)."""
    from backend.database import get_numdict_bank_item

    item = get_numdict_bank_item(str(numdict_id))
    if not item:
        return {"kind": "numdict_practice", "error": "not_found"}
    mode = str(item.get("input_mode") or "numeric")
    is_correct = _normalize_numdict(typed, mode) == _normalize_numdict(item.get("answer_value"), mode)
    return {
        "kind": "numdict_practice",
        "is_correct": bool(is_correct),
        "display_answer": str(item.get("display_answer") or ""),
        "typed": str(typed or ""),
    }


# ── Freeform quiz ("keine korrekte Antworten" → type answer): load + evaluate ──

def _quiz_freeform_semantic_match(user_text: str, correct_text: str) -> bool:
    """STRICT meaning-equivalence fallback (only when the deterministic match
    misses). Uses the synonym judge — which accepts genuine equivalents but
    REJECTS different-meaning phrases — NOT the lenient story-guess checker that
    used to pass clearly-wrong answers (e.g. "er ist gelungen" for "er hat
    geflunkert").
    """
    canonical = str(correct_text or "").strip()
    guess = str(user_text or "").strip()
    if not canonical or not guess:
        return False
    if len(canonical.split()) > 6 or len(guess.split()) > 6 or len(canonical) > 80 or len(guess) > 80:
        return False
    try:
        import asyncio
        from backend.openai_manager import run_check_synonym
        result = asyncio.run(asyncio.wait_for(
            run_check_synonym(target_word=canonical, candidate=guess, relation="synonym"),
            timeout=7.0,
        ))
        return bool((result or {}).get("match"))
    except Exception:
        return False


def _freeform_result_payload(dispatch: dict, *, is_correct: bool, already_answered: bool) -> dict:
    correct_text = str(dispatch.get("correct_text") or "")
    word_ru = str(dispatch.get("word_ru") or "")
    return {
        "kind": "freeform",
        "is_correct": bool(is_correct),
        "correct_word": correct_text,
        "hint_ru": word_ru,
        "explanation": str(dispatch.get("explanation") or ""),
        "already_answered": bool(already_answered),
        "saveable_words": ([{"source": correct_text, "target": word_ru}] if (correct_text and word_ru) else []),
    }


# The poll's "no correct answer" option. Picking it is NOT a committed answer —
# it's the cue to type your own — so it leaves the freeform open. Any OTHER poll
# choice is a committed answer and blocks the freeform (no second answer).
_QUIZ_FREEFORM_SENTINEL = "keine korrekte Antworten"


def _freeform_committed_via_poll(dispatch: dict, user_id: int) -> dict | None:
    """The user's committed native-poll answer (a real option, not the
    'keine korrekte' cue) — if present, the freeform must not accept a 2nd answer."""
    from backend.database import get_telegram_quiz_attempt
    poll_id = str(dispatch.get("poll_id") or "").strip()
    if not poll_id:
        return None
    att = get_telegram_quiz_attempt(poll_id, int(user_id))
    if not att:
        return None
    sel = str(att.get("selected_text") or "").strip()
    if sel and sel != _QUIZ_FREEFORM_SENTINEL:
        return att
    return None


def load_freeform_task(*, dispatch_id: int, user_id: int) -> dict | None:
    from backend.database import get_quiz_freeform_dispatch_by_id, get_quiz_freeform_answer
    dispatch = get_quiz_freeform_dispatch_by_id(int(dispatch_id))
    if not dispatch:
        return None
    existing = get_quiz_freeform_answer(dispatch_id=int(dispatch_id), user_id=int(user_id))
    poll_committed = None if existing else _freeform_committed_via_poll(dispatch, user_id)
    meta = {
        "kind": "freeform",
        "hint_ru": str(dispatch.get("word_ru") or ""),
        "already_answered": bool(existing or poll_committed),
    }
    if existing:
        meta["result"] = _freeform_result_payload(
            dispatch, is_correct=bool(existing.get("is_correct")), already_answered=True,
        )
    elif poll_committed:
        meta["result"] = _freeform_result_payload(
            dispatch, is_correct=bool(poll_committed.get("is_correct")), already_answered=True,
        )
    return meta


def evaluate_freeform(*, dispatch_id: int, user_id: int, raw_input: str) -> dict | None:
    from backend.database import (
        get_quiz_freeform_dispatch_by_id, get_quiz_freeform_answer, record_quiz_freeform_answer,
    )
    dispatch = get_quiz_freeform_dispatch_by_id(int(dispatch_id))
    if not dispatch:
        return None

    existing = get_quiz_freeform_answer(dispatch_id=int(dispatch_id), user_id=int(user_id))
    if existing:
        return _freeform_result_payload(
            dispatch, is_correct=bool(existing.get("is_correct")), already_answered=True,
        )
    # Block a second answer if the user already committed via the native poll.
    poll_committed = _freeform_committed_via_poll(dispatch, user_id)
    if poll_committed:
        return _freeform_result_payload(
            dispatch, is_correct=bool(poll_committed.get("is_correct")), already_answered=True,
        )

    correct_text = str(dispatch.get("correct_text") or "")
    answer = str(raw_input or "").strip()
    is_correct = check_quiz_freeform_deterministic(user_text=answer, correct_text=correct_text)
    if not is_correct:
        is_correct = _quiz_freeform_semantic_match(answer, correct_text)

    record_quiz_freeform_answer(
        dispatch_id=int(dispatch_id), user_id=int(user_id),
        answer=answer, is_correct=bool(is_correct),
    )
    return _freeform_result_payload(dispatch, is_correct=is_correct, already_answered=False)


# ── B2+ text tasks ("Aufgabe": cloze / …): load + evaluate ───────────────────

# Deterministic gender-signal mnemonics for the `artikel` review card ("чуйка").
# Only shown AFTER the answer is revealed AND only when the signal agrees with the
# true article (`a == art` guard) — so a tendency-suffix (-er→der, -at→das) is never
# stated for a word that breaks it. No LLM. Falls back to an honest "just remember it".
_ARTIKEL_SIGNALS = [
    ("chen", "das", "-chen → всегда das (уменьшительное): das Mädchen, das Kätzchen"),
    ("lein", "das", "-lein → всегда das (уменьшительное): das Fräulein"),
    ("ung", "die", "-ung → почти всегда die: die Ordnung, die Zeitung"),
    ("heit", "die", "-heit → die: die Freiheit, die Krankheit"),
    ("keit", "die", "-keit → die: die Möglichkeit"),
    ("schaft", "die", "-schaft → die: die Mannschaft, die Wissenschaft"),
    ("ität", "die", "-ität → die: die Universität, die Aktivität"),
    ("tät", "die", "-tät → die: die Fakultät"),
    ("ion", "die", "-ion → die: die Nation, die Direktion"),
    ("enz", "die", "-enz → die: die Konferenz"),
    ("anz", "die", "-anz → die: die Distanz"),
    ("ie", "die", "-ie → die: die Geografie, die Linguistik→die Ling…"),
    ("ling", "der", "-ling → der: der Lehrling, der Schmetterling"),
    ("ismus", "der", "-ismus → der: der Kapitalismus"),
    ("ment", "das", "-ment → часто das: das Instrument, das Dokument"),
    ("um", "das", "-um → часто das (лат.): das Zentrum, das Studium"),
    ("at", "das", "-at → часто das: das Direktorat, das Referat"),
    ("or", "der", "-or → часто der: der Motor, der Direktor"),
    ("er", "der", "-er (деятель/предмет) → часто der: der Lehrer, der Rechner"),
]


def artikel_review_hint(wort: str, article: str) -> str:
    """A deterministic gender-signal tip for the artikel review card, or an honest
    'just memorize it' when the word carries no reliable signal."""
    w = str(wort or "").strip().lower()
    art = str(article or "").strip().lower()
    for suf, a, hint in _ARTIKEL_SIGNALS:
        if a == art and w.endswith(suf) and len(w) - len(suf) >= 2:
            return hint
    return "У этого слова нет надёжного суффикс-сигнала — запоминай род вместе со словом (der/die/das)."


def _aufgabe_correct_answer(payload: dict) -> str:
    """The canonical answer to show after answering (per format)."""
    art = str(payload.get("correct") or "").strip().lower()
    if art in ("der", "die", "das") and payload.get("wort") and not payload.get("satz"):  # artikel
        from backend.article_sprint_generator import resolve_article
        art = resolve_article(payload.get("wort"), art)  # correct a mislabelled row before revealing
        return f"{art} {payload.get('wort')}".strip()
    if payload.get("before") is not None and payload.get("after") is not None:  # adjektiv
        full = str(payload.get("full") or "").strip()
        if full:
            return full
        return f"{payload.get('before','')}{payload.get('correct','')}{payload.get('after','')}".strip()
    if payload.get("wort") and payload.get("accepted"):  # synonym/antonym: show a few
        return " · ".join(accepted_de(payload.get("accepted"))[:3])
    gaps = payload.get("gaps")
    if isinstance(gaps, list) and gaps:  # multi-gap hörlücke
        return " · ".join(str(g.get("correct") or "") for g in gaps)
    correct = str(payload.get("correct") or "").strip()
    if correct:
        return correct
    cw = str(payload.get("correct_word") or "").strip()
    if cw:
        return cw
    tl = str(payload.get("target_label") or "").strip()
    if tl:
        return tl
    satz = str(payload.get("satz") or "").strip()
    if satz:
        return satz
    accepted = payload.get("accepted") or []
    return str(accepted[0]) if accepted else ""


def _parse_error_submission(raw_input) -> list:
    """Parse a 'Finde den Fehler' answer string into [(index, correction), …] in tap order.
    v2 multi: 'i1|c1;i2|c2' (one pair per tapped word). v1 single: 'i|c' → one pair. So the
    old single-tap client keeps working against the multi-aware grader. Malformed pairs are
    skipped; order is preserved for the result card."""
    out = []
    for chunk in str(raw_input or "").split(";"):
        chunk = chunk.strip()
        if not chunk or "|" not in chunk:
            continue
        idx_str, _, corr = chunk.partition("|")
        try:
            idx = int(idx_str.strip())
        except ValueError:
            continue
        out.append((idx, corr.strip()))
    return out


def _display_user_answer(fmt: str, raw: str, payload: dict) -> str:
    """Human-readable form of a composite answer for the result card, so the learner
    never sees the internal encoding (e.g. '2|das'). For the `error` format it shows
    «tapped → correction» for EACH tapped word (joined) so a wrong TAP (not a wrong word)
    is visible; for `pin` the typed article; for `hoerluecke` the gaps joined. Other
    formats pass through."""
    s = str(raw or "").strip()
    if not s:
        return ""
    if fmt == "error":
        subs = _parse_error_submission(s)
        if not subs:
            return s
        woerter = payload.get("woerter") if isinstance(payload.get("woerter"), list) else []
        parts = []
        for idx, corr in subs:
            try:
                tapped = str(woerter[idx])
            except (IndexError, TypeError):
                tapped = ""
            if tapped and corr:
                parts.append(f"{tapped} → {corr}")
            elif tapped or corr:
                parts.append(tapped or corr)
        return "; ".join(parts) if parts else s
    if fmt == "pin":
        _coords, sep, article = s.partition("|")
        return article.strip() if sep else s   # show the article they typed (not the tap coords)
    if fmt == "hoerluecke":
        return " · ".join(p.strip() for p in s.split("|") if p.strip())
    return s


def _error_result_breakdown(payload: dict, raw_input: str) -> tuple:
    """Per-error breakdown for the 'Finde den Fehler' result card. For EACH real error in
    the answer key returns {word, correct, erklaerung, hint_ru, status, user}:
      - 'fixed'     the learner tapped it AND typed a correct form
      - 'wrong_fix' tapped it but the correction is wrong (user = what they typed)
      - 'missed'    a real error they never tapped
    Plus extra_taps = words they tapped that were ALREADY correct. Returns
    (errors, extra_taps, correct_join)."""
    from backend.database import normalize_error_payload
    key = normalize_error_payload(payload)
    woerter = payload.get("woerter") if isinstance(payload.get("woerter"), list) else []
    subs = {}
    for idx, corr in _parse_error_submission(raw_input):
        subs[idx] = corr
    key_idx = {e["index"] for e in key}

    def _word(i):
        return str(woerter[i]) if 0 <= i < len(woerter) else ""

    errors = []
    for e in key:
        i = e["index"]
        entry = {"word": _word(i), "correct": e["correct_word"],
                 "erklaerung": e["erklaerung"], "hint_ru": e["hint_ru"], "user": ""}
        if i not in subs:
            entry["status"] = "missed"
        else:
            corr = subs[i]
            entry["user"] = corr
            cands = [e["correct_word"]] + list(e["aliases"])
            ok = any(check_quiz_freeform_deterministic(user_text=corr, correct_text=c)
                     for c in cands if str(c).strip())
            entry["status"] = "fixed" if ok else "wrong_fix"
        errors.append(entry)

    extra = [{"word": _word(i), "user": corr} for i, corr in subs.items() if i not in key_idx]
    correct_join = " · ".join(e["correct_word"] for e in key)
    return errors, extra, correct_join


def _aufgabe_result_payload(dispatch: dict, *, is_correct: bool, already_answered: bool,
                           user_answer: str = "", wrong_reason: str = "") -> dict:
    payload = dispatch.get("payload") or {}
    fmt = str(dispatch.get("format") or "")
    # Sentence formats (satzbau/transform) get a word-level diff in the FE:
    # the correct sentence in green over the user's sentence with wrong words
    # struck through. Other formats keep the simple "correct answer" line.
    is_sentence = fmt in ("satzbau", "transform")
    # Synonym/antonym: every accepted word is tappable to save to the dictionary
    # (German in our save format + its own Russian translation).
    saveable = accepted_pairs(payload.get("accepted")) if fmt in ("synonym", "antonym") else []
    # Artikel review: no stored tip — derive a deterministic gender-signal mnemonic
    # from the TRUE (guard-resolved) article so the hint never contradicts the answer.
    if fmt == "artikel":
        from backend.article_sprint_generator import resolve_article
        tip = artikel_review_hint(str(payload.get("wort") or ""),
                                  resolve_article(payload.get("wort"), payload.get("correct")))
    else:
        tip = str(payload.get("tip") or "")
    result = {
        "kind": "aufgabe",
        "format": fmt,
        "is_correct": bool(is_correct),
        "correct_word": _aufgabe_correct_answer(payload),
        "is_sentence": bool(is_sentence),
        "user_answer": _display_user_answer(fmt, user_answer, payload),
        "saveable": saveable,
        "hint_ru": str(payload.get("hint_ru") or payload.get("ru") or ""),
        "explanation": str(payload.get("erklaerung") or payload.get("explanation") or ""),
        "tip": tip,
        "wrong_reason": str(wrong_reason or ""),
        "already_answered": bool(already_answered),
        "saveable_words": [],
    }
    if fmt == "error":
        # Per-error breakdown drives the result card (✅ fixed / ❌ wrong / 🔎 missed +
        # per-error rule). The generic correct/explanation/hint lines would duplicate it,
        # so fold them into the breakdown and blank the top-level ones.
        errors, extra, correct_join = _error_result_breakdown(payload, user_answer)
        result["errors"] = errors
        result["extra_taps"] = extra
        result["correct_word"] = correct_join
        result["explanation"] = ""
        result["hint_ru"] = ""
        result["tip"] = ""
    return result


def aufgabe_client_meta(fmt: str, payload: dict) -> dict:
    """Per-format render fields shown to the user — NEVER the answer/explanation.
    Shared by the live task loader and the mistakes-review session."""
    payload = payload or {}
    meta = {"format": fmt, "hint_ru": str(payload.get("hint_ru") or "")}
    if fmt == "cloze":
        meta["satz"] = str(payload.get("satz") or "")
    elif fmt == "wortbildung":
        meta["satz"] = str(payload.get("satz") or "")
        meta["stamm"] = str(payload.get("stamm") or "")
        meta["stamm_ru"] = str(payload.get("stamm_ru") or "")
    elif fmt == "wortgruppe":
        # Send the sentence + the base-form lemmas + (optional) required tense.
        # NEVER send preposition/correct/aliases — finding the preposition and
        # building the correct form is the task.
        meta["satz"] = str(payload.get("satz") or "")
        meta["lemmas"] = [str(w) for w in (payload.get("lemmas") or []) if str(w).strip()]
        meta["tense"] = str(payload.get("tense") or "")
    elif fmt == "adjektiv":
        # Send only the phrase parts (NOT the correct ending) so it can't be read off.
        before = str(payload.get("before") or "")
        after = str(payload.get("after") or "")
        try:
            from backend.database import derive_adjektiv_split
            der = derive_adjektiv_split(str(payload.get("full") or ""), str(payload.get("correct") or ""))
            if der:
                before, after = der
        except Exception:
            pass
        meta["before"] = before
        meta["after"] = after
    elif fmt == "wofrage":
        # Wo-Frage Sprint review card (mixed "Alle" flow). Send the Q&A sentence (with
        # the "___" blank), the clue line, and the 4 options; NEVER the correct answer.
        meta["s"] = str(payload.get("s") or "")
        meta["clue"] = str(payload.get("clue") or "")
        meta["opts"] = [str(o) for o in (payload.get("opts") or []) if str(o).strip()]
        meta["hint_ru"] = str(payload.get("hint_ru") or payload.get("verb_ru") or "")
    elif fmt == "artikel":
        # der/die/das review card (mirrors Artikel Sprint). Send the noun + the three
        # options; NEVER the correct article. The RU meaning is not the answer, so it
        # may be shown as a subtitle to identify the word. A photo of the word (from the
        # noun bank, precomputed) makes the card match the Artikel Trainer template.
        wort = str(payload.get("wort") or "")
        meta["wort"] = wort
        meta["options"] = ["der", "die", "das"]
        meta["hint_ru"] = str(payload.get("ru") or "")
        try:
            from backend.database import get_article_noun_images
            from backend.r2_storage import r2_public_url
            ikey = get_article_noun_images([wort]).get(wort.lower(), "") if wort else ""
            if ikey:
                meta["image"] = r2_public_url(ikey)
        except Exception:
            pass
    elif fmt == "video":
        # Remedial theory video ("тебе было сложно"): a skippable PICKER card, not a
        # gradeable task. Send ALL curated clips for the topic so the learner chooses
        # whose explanation they like; the client embeds a player + «Посмотрел»/«Пропустить».
        vids = payload.get("videos")
        if not isinstance(vids, list) or not vids:  # back-compat: legacy single-video card
            vids = ([{"video_id": payload.get("video_id"),
                      "video_url": payload.get("video_url"),
                      "video_title": payload.get("video_title")}]
                    if payload.get("video_id") else [])
        meta["videos"] = [
            {"video_id": str((v or {}).get("video_id") or ""),
             "video_url": str((v or {}).get("video_url") or ""),
             "video_title": str((v or {}).get("video_title") or "")}
            for v in vids if (v or {}).get("video_id")
        ]
        meta["topic_ru"] = str(payload.get("topic_ru") or "")
        meta["topic_de"] = str(payload.get("topic_de") or "")
        meta["hint_ru"] = str(payload.get("topic_ru") or payload.get("hint_ru") or "")
    elif fmt == "transform":
        meta["original"] = str(payload.get("original") or "")
        meta["schluesselwort"] = str(payload.get("schluesselwort") or "")
        meta["target_prefix"] = str(payload.get("target_prefix") or "")
        meta["target_suffix"] = str(payload.get("target_suffix") or "")
    elif fmt in ("synonym", "antonym"):
        meta["wort"] = str(payload.get("wort") or "")
        meta["relation"] = fmt
    elif fmt == "error":
        meta["woerter"] = [str(w) for w in (payload.get("woerter") or [])]
    elif fmt == "satzbau":
        import random as _rnd
        tiles = [str(w) for w in (payload.get("woerter") or []) if str(w).strip()]
        shuffled = tiles[:]
        for _ in range(8):
            _rnd.shuffle(shuffled)
            if shuffled != tiles or len(tiles) <= 1:
                break
        meta["tiles"] = shuffled
    elif fmt == "hoerluecke":
        gaps = payload.get("gaps")
        if isinstance(gaps, list) and gaps:  # new multi-gap format
            meta["transcript"] = str(payload.get("transcript") or "")
            meta["gap_count"] = len(gaps)
        else:  # backward-compat single gap
            meta["satz_luecke"] = str(payload.get("satz_luecke") or "")
            meta["gap_count"] = 1
        meta["audio_url"] = ""
        key = str(payload.get("audio_object_key") or "")
        if key:
            try:
                from backend.r2_storage import r2_public_url
                meta["audio_url"] = r2_public_url(key)
            except Exception:
                meta["audio_url"] = ""
    elif fmt == "pin":
        # The challenge is: read the GERMAN word, find the (hidden) object, tap it,
        # then supply its article. So during the task we must NOT reveal (a) the
        # Russian meaning — a Russian speaker would find "ножницы" trivially — nor
        # (b) the article. We rebuild the question from target_label MINUS the article
        # (fixes old rows whose stored question_de leaked "die …"), and blank hint_ru
        # (the Russian meaning is still shown afterwards, in the result card).
        target_label = str(payload.get("target_label") or "").strip()
        wort = re.sub(r"^(der|die|das)\s+", "", target_label, flags=re.IGNORECASE).strip()
        if wort:
            meta["question_de"] = f"Finde {wort} im Bild — tippe darauf und gib den Artikel ein."
        else:  # no target_label (shouldn't happen) → fall back to stored text
            meta["question_de"] = str(payload.get("question_de") or "")
        meta["hint_ru"] = ""  # never show the Russian meaning during the pin task
        meta["needs_article"] = bool(str(payload.get("article") or "").strip())
        meta["image_url"] = ""
        key = str(payload.get("image_object_key") or "")
        if key:
            try:
                from backend.r2_storage import r2_public_url
                meta["image_url"] = r2_public_url(key)
            except Exception:
                meta["image_url"] = ""
    return meta


def load_aufgabe_task(*, dispatch_id: int, user_id: int) -> dict | None:
    from backend.database import get_aufgabe_dispatch_by_id, get_aufgabe_answer
    dispatch = get_aufgabe_dispatch_by_id(int(dispatch_id))
    if not dispatch:
        return None
    payload = dispatch.get("payload") or {}
    fmt = str(dispatch.get("format") or "")
    meta = {
        "kind": "aufgabe",
        "level": str(dispatch.get("level") or "B2"),
        "already_answered": False,
        **aufgabe_client_meta(fmt, payload),
    }
    existing = get_aufgabe_answer(dispatch_id=int(dispatch_id), user_id=int(user_id))
    if existing:
        meta["already_answered"] = True
        meta["result"] = _aufgabe_result_payload(
            dispatch, is_correct=bool(existing.get("is_correct")), already_answered=True,
        )
    return meta


# ── Mistakes review ("работа над ошибками"): record + spaced-repetition session ─
_REVIEW_FORMATS = {"cloze", "wortbildung", "wortgruppe", "transform", "error", "satzbau", "adjektiv", "video"}


def review_overview(*, user_id: int) -> dict:
    """Sections of работа над ошибками with their due-counts, so the client can show a
    dedicated 'Artikel' block separate from the grammar drills. Empty sections are omitted."""
    from backend.database import count_due_mistakes_by_family
    c = count_due_mistakes_by_family(int(user_id))
    sections = []
    if int(c.get("artikel", 0)) > 0:
        sections.append({"family": "artikel", "emoji": "⚡",
                         "title_de": "Artikel", "title_ru": "Артикли",
                         "subtitle_de": "der / die / das", "count": int(c["artikel"])})
    if int(c.get("wofrage", 0)) > 0:
        sections.append({"family": "wofrage", "emoji": "❓",
                         "title_de": "Wo-Fragen", "title_ru": "Wo-Fragen",
                         "subtitle_de": "Worauf / An wem …", "count": int(c["wofrage"])})
    if int(c.get("grammar", 0)) > 0:
        sections.append({"family": "grammar", "emoji": "🧩",
                         "title_de": "Grammatik", "title_ru": "Грамматика",
                         "subtitle_de": "Sätze, Lücken, Wortgruppen", "count": int(c["grammar"])})
    return {"kind": "review", "sections": sections, "total": int(c.get("total", 0))}


def load_review_next(*, user_id: int, family: str | None = None) -> dict:
    """Next due mistake for this user as a renderable task (reuses Aufgabe UI). `family`
    scopes the session to one section ('artikel' | 'grammar'); None = all mistakes."""
    from backend.database import get_next_due_mistake, count_due_mistakes
    fam = (str(family).strip().lower() or None) if family else None
    if fam not in ("artikel", "wofrage", "grammar", None):
        fam = None
    remaining = count_due_mistakes(int(user_id), family=fam)
    if remaining <= 0:
        return {"kind": "review", "done": True, "remaining": 0, "family": fam}
    m = get_next_due_mistake(int(user_id), family=fam)
    if not m:
        return {"kind": "review", "done": True, "remaining": 0, "family": fam}
    return {
        "kind": "review",
        "done": False,
        "family": fam,
        "remaining": int(remaining),
        "mistake_id": int(m["id"]),
        "task": aufgabe_client_meta(str(m["format"]), m["payload"] or {}),
    }


def load_artikel_review_batch(*, user_id: int, limit: int = 20) -> dict:
    """FAST path for the Artikel section of работа над ошибками: return a whole page of
    due der/die/das mistakes as ready-to-render cards (noun + true article + photo +
    audio + tip) in ONE query, so the client prefetches the batch, grades locally, and
    advances spaced-repetition in the background — no per-card server round-trip. The
    true article is guard-resolved (die Börsenwert → der) so mislabelled mistakes are
    fixed on the fly. `a` is sent because review is personal study, not a scored test."""
    from backend.database import get_due_artikel_mistakes_batch, count_due_mistakes
    from backend.article_sprint_generator import resolve_article
    from backend.r2_storage import r2_public_url
    rows = get_due_artikel_mistakes_batch(int(user_id), int(limit))
    cards = []
    for r in rows:
        wort = str(r.get("word") or "")
        if not wort:
            continue
        a = resolve_article(wort, r.get("correct"))
        ikey = str(r.get("image_object_key") or "")
        akey = str(r.get("audio_object_key") or "")
        tip = str(r.get("mnemonic_ru") or "") or artikel_review_hint(wort, a)
        cards.append({
            "id": int(r["id"]), "w": wort, "ru": str(r.get("ru") or ""), "a": a,
            "image": r2_public_url(ikey) if ikey else "",
            "audio": r2_public_url(akey) if akey else "",
            "tip": tip,
        })
    return {"kind": "artikel_review", "cards": cards,
            "remaining": count_due_mistakes(int(user_id), family="artikel")}


def answer_artikel_review(*, user_id: int, mistake_id: int, is_correct: bool) -> dict:
    """Advance ONE artikel review card's spaced-repetition schedule (fire-and-forget from
    the batch client). Grading already happened locally on the deterministic article, so
    this only reschedules — off the user's critical path."""
    from backend.database import reschedule_mistake, count_due_mistakes
    reschedule_mistake(mistake_id=int(mistake_id), user_id=int(user_id), is_correct=bool(is_correct))
    return {"kind": "artikel_review", "ok": True,
            "remaining": count_due_mistakes(int(user_id), family="artikel")}


def load_wofrage_review_batch(*, user_id: int, limit: int = 20) -> dict:
    """FAST path for the Wo-Fragen section of работа над ошибками: a whole page of due
    Wo-Frage Sprint mistakes as ready-to-render cards (question + clue + 4 options + the
    true answer + rule/tip) in ONE query. The client prefetches the batch, grades LOCALLY
    (the answer ships with the card, same as the Wo-Frage Trainer) and advances
    spaced-repetition fire-and-forget. `a` is sent because review is personal study, not a
    scored test."""
    from backend.database import get_due_wofrage_mistakes_batch, count_due_mistakes
    rows = get_due_wofrage_mistakes_batch(int(user_id), int(limit))
    cards = []
    for r in rows:
        p = r.get("payload") or {}
        s = str(p.get("s") or "")
        a = str(p.get("correct") or "")
        if not s or not a:
            continue
        cards.append({
            "id": int(r["id"]), "s": s, "a": a,
            "clue": str(p.get("clue") or ""),
            "opts": [str(o) for o in (p.get("opts") or []) if str(o).strip()],
            "erklaerung": str(p.get("erklaerung") or ""),
            "tip": str(p.get("tip") or ""),
            "lemma": str(p.get("lemma") or ""),
            "verb_ru": str(p.get("verb_ru") or ""),
        })
    return {"kind": "wofrage_review", "cards": cards,
            "remaining": count_due_mistakes(int(user_id), family="wofrage")}


def answer_wofrage_review(*, user_id: int, mistake_id: int, is_correct: bool) -> dict:
    """Advance ONE Wo-Fragen review card's spaced-repetition schedule (fire-and-forget
    from the batch client). Grading already happened locally on the deterministic answer,
    so this only reschedules — off the user's critical path."""
    from backend.database import reschedule_mistake, count_due_mistakes
    reschedule_mistake(mistake_id=int(mistake_id), user_id=int(user_id), is_correct=bool(is_correct))
    return {"kind": "wofrage_review", "ok": True,
            "remaining": count_due_mistakes(int(user_id), family="wofrage")}


def evaluate_review(*, user_id: int, mistake_id: int, answer: str,
                    family: str | None = None) -> dict:
    """Grade a review answer, advance/reset its spaced-repetition schedule, and
    return the same rich result (rule + tip) the live task shows. `family` keeps the
    'remaining' count scoped to the section the learner is drilling."""
    from backend.database import get_aufgabe_mistake, reschedule_mistake, count_due_mistakes
    fam = str(family).strip().lower() if family else None
    if fam not in ("artikel", "wofrage", "grammar"):
        fam = None
    m = get_aufgabe_mistake(int(user_id), int(mistake_id))
    if not m:
        return {"kind": "review", "error": "not_found"}
    fmt = str(m["format"])
    payload = m["payload"] or {}
    if fmt == "video":
        # No grading — the learner either watched or skipped. Consume it (mastered) so
        # it never resurfaces, and hand the client a plain "done, load next" signal.
        from backend.database import consume_video_review
        consume_video_review(user_id=int(user_id), mistake_id=int(mistake_id))
        return {"kind": "review", "video_done": True,
                "remaining": count_due_mistakes(int(user_id), family=fam)}
    # Grade with the EXACT same logic (incl. LLM-judge fallbacks) as the live task, so a
    # valid answer isn't rejected here — otherwise the item would never advance to
    # "mastered" and would reappear forever ("I answered correctly but it keeps coming back").
    is_correct, wrong_reason = _grade_aufgabe(fmt, payload, str(answer or ""))
    reschedule_mistake(mistake_id=int(mistake_id), user_id=int(user_id), is_correct=bool(is_correct))
    result = _aufgabe_result_payload(
        {"payload": payload, "format": fmt},
        is_correct=is_correct, already_answered=False, user_answer=str(answer or ""),
        wrong_reason=wrong_reason,
    )
    return {"kind": "review", "result": result,
            "remaining": count_due_mistakes(int(user_id), family=fam)}


def _norm_sentence(s: str) -> str:
    """Order-preserving sentence normalizer for Satzbau: lowercase, collapse spaces,
    drop punctuation. Same words in the SAME order → equal; wrong order → different."""
    import re as _re
    # Fold ß → ss and ä/ö/ü → ae/oe/ue so a word-order answer isn't failed over
    # Abschluß vs Abschluss or schön vs schoen (matches _normalize_quiz_text).
    lowered = str(s or "").lower().replace("ß", "ss")
    lowered = lowered.replace("ä", "ae").replace("ö", "oe").replace("ü", "ue")
    t = _re.sub(r"[^\wäöü\s]", " ", lowered, flags=_re.UNICODE)
    return _re.sub(r"\s+", " ", t).strip()


def _synonym_judge_full(target: str, candidate: str, relation: str) -> dict:
    """Bounded LLM judge: {'match': bool, 'reason_ru': str}. reason_ru explains
    (in Russian) why a wrong candidate is NOT a valid synonym/antonym — shown to
    the learner so a plausible near-miss (e.g. genehmigen for bestätigen) gets a
    real explanation, not just the generic correct-answer description."""
    t, c = str(target or "").strip(), str(candidate or "").strip()
    if not t or not c or len(c.split()) > 3 or len(c) > 60:
        return {"match": False, "reason_ru": ""}
    try:
        import asyncio
        from backend.openai_manager import run_check_synonym
        res = asyncio.run(asyncio.wait_for(
            run_check_synonym(target_word=t, candidate=c, relation=str(relation or "synonym")),
            timeout=7.0,
        ))
        return {
            "match": bool((res or {}).get("match")),
            "reason_ru": str((res or {}).get("reason_ru") or "").strip(),
        }
    except Exception:
        return {"match": False, "reason_ru": ""}


def _synonym_semantic_match(target: str, candidate: str, relation: str) -> bool:
    """Bounded LLM fallback: only fires when the accepted-list misses, so the hot
    path stays fast for the common answers."""
    return bool(_synonym_judge_full(target, candidate, relation).get("match"))


def _satzbau_same_words(payload: dict, answer: str) -> bool:
    """Satzbau requires REORDERING the given word cards. True iff `answer` uses exactly
    the same word multiset as the task tokens (case/punctuation/ß-fold normalized) — a
    cheap guard so the LLM judge only fires for genuine re-orderings, not different words."""
    woerter = payload.get("woerter")
    if isinstance(woerter, list) and woerter:
        ref = " ".join(str(w) for w in woerter)
    else:
        accepted = payload.get("accepted") or []
        ref = str(payload.get("satz") or (accepted[0] if accepted else ""))
    ans_tokens = sorted(_norm_sentence(answer).split())
    return bool(ans_tokens) and ans_tokens == sorted(_norm_sentence(ref).split())


def _satzbau_judge_full(reference: str, user: str) -> dict:
    """Bounded LLM judge for a Satzbau miss: {'match': bool, 'reason_ru': str}. German
    allows several valid word orders, so a grammatically-correct re-ordering of the same
    cards (e.g. fronting the subordinate clause) is accepted even if it isn't the one
    canonical `accepted` string. reason_ru explains a genuinely wrong order to the learner."""
    ref, u = str(reference or "").strip(), str(user or "").strip()
    if not ref or not u or len(u) > 220 or len(u.split()) > 30:
        return {"match": False, "reason_ru": ""}
    try:
        import asyncio
        from backend.openai_manager import run_check_satzbau
        res = asyncio.run(asyncio.wait_for(
            run_check_satzbau(reference=ref, user=u), timeout=7.0,
        ))
        return {
            "match": bool((res or {}).get("match")),
            "reason_ru": str((res or {}).get("reason_ru") or "").strip(),
        }
    except Exception:
        return {"match": False, "reason_ru": ""}


def _cloze_judge_full(satz: str, correct: str, user: str) -> dict:
    """Bounded LLM judge for an open-cloze (Lückentext) miss: {'match': bool, 'reason_ru': str}.
    A gap often admits more than the one canonical key (e.g. causal weil ≡ da), and the pool's
    `aliases` rarely lists them all — so on an exact-match miss we ask ONE judge whether the
    typed word is ALSO correct in THIS sentence. reason_ru explains a genuine miss to the learner."""
    s, c, u = str(satz or "").strip(), str(correct or "").strip(), str(user or "").strip()
    if not s or not c or not u or len(u) > 60 or len(u.split()) > 4:
        return {"match": False, "reason_ru": ""}
    try:
        import asyncio
        from backend.openai_manager import run_check_cloze
        res = asyncio.run(asyncio.wait_for(
            run_check_cloze(satz=s, correct=c, user=u), timeout=7.0,
        ))
        return {
            "match": bool((res or {}).get("match")),
            "reason_ru": str((res or {}).get("reason_ru") or "").strip(),
        }
    except Exception:
        return {"match": False, "reason_ru": ""}


def _error_judge_full(woerter: list, tapped_index: int, correction: str) -> dict:
    """Bounded LLM judge for a 'Finde den Fehler' miss: {'match': bool, 'reason_ru': str}.
    A generated sentence can carry a second latent error, or the intended fix can have a
    valid variant the pool's `aliases` misses — so on a deterministic miss we ask ONE judge
    whether the word the learner tapped was GENUINELY wrong and their retype fixes THAT word
    (judging only that token, not the whole sentence). This credits a learner who spots a
    real error other than the intended one. reason_ru explains a genuine miss."""
    corr = str(correction or "").strip()
    if not isinstance(woerter, list) or not woerter or not corr:
        return {"match": False, "reason_ru": ""}
    try:
        idx = int(tapped_index)
    except (TypeError, ValueError):
        return {"match": False, "reason_ru": ""}
    if not (0 <= idx < len(woerter)) or len(corr) > 40 or len(corr.split()) > 3:
        return {"match": False, "reason_ru": ""}
    try:
        import asyncio
        from backend.openai_manager import run_check_error
        res = asyncio.run(asyncio.wait_for(
            run_check_error(woerter=[str(w) for w in woerter], index=idx, user=corr),
            timeout=7.0,
        ))
        return {
            "match": bool((res or {}).get("match")),
            "reason_ru": str((res or {}).get("reason_ru") or "").strip(),
        }
    except Exception:
        return {"match": False, "reason_ru": ""}


def _grade_error(payload: dict, raw_input: str) -> tuple:
    """Grade a 'Finde den Fehler' answer (single OR multi) all-or-nothing, with tolerance.
    Deterministic first (index set == answer key AND every correction matches — the hot
    path). If the index sets match but a correction misses the accepted list, ONE bounded
    LLM judge per un-matched token credits a valid variant German allows in that exact slot.
    A WRONG index set (a real error missed, or a correct word tapped) can't be rescued — the
    verifier guarantees no other errors exist, so the detailed per-word breakdown is left to
    the result card. Returns (is_correct, wrong_reason)."""
    if _check_aufgabe("error", payload, raw_input):
        return True, ""
    from backend.database import normalize_error_payload
    key = {e["index"]: e for e in normalize_error_payload(payload)}
    subs = _parse_error_submission(raw_input)
    if not key or not subs:
        return False, ""
    sub = {}
    for idx, corr in subs:
        if not corr:
            return False, ""
        sub[idx] = corr
    if set(sub) != set(key):
        return False, ""
    woerter = payload.get("woerter") or []
    for idx, corr in sub.items():
        candidates = [str(key[idx]["correct_word"])] + [str(a) for a in key[idx]["aliases"]]
        if any(check_quiz_freeform_deterministic(user_text=corr, correct_text=c)
               for c in candidates if str(c).strip()):
            continue
        judged = _error_judge_full(woerter, idx, corr)
        if not bool(judged.get("match")):
            return False, str(judged.get("reason_ru") or "")
    return True, ""


def _wortgruppe_judge_full(satz: str, correct: str, user: str) -> dict:
    """Bounded LLM judge for a Wortgruppe miss: {'match': bool, 'reason_ru': str}. The
    solution is a connected word group (usually preposition/conjunction + noun phrase)
    that fills the single "_____" gap, so it has the SAME shape as a cloze — we reuse the
    cloze judge (grammatical + same meaning + correct case/government), with the length
    limits relaxed for a multi-word group. Catches valid equivalent spellings/forms the
    pool's `accepted` list misses, so a learner who typed a correct variant isn't failed."""
    s, c, u = str(satz or "").strip(), str(correct or "").strip(), str(user or "").strip()
    if not s or not c or not u or "_" not in s or len(u) > 120 or len(u.split()) > 8:
        return {"match": False, "reason_ru": ""}
    try:
        import asyncio
        from backend.openai_manager import run_check_cloze
        res = asyncio.run(asyncio.wait_for(
            run_check_cloze(satz=s, correct=c, user=u), timeout=7.0,
        ))
        return {
            "match": bool((res or {}).get("match")),
            "reason_ru": str((res or {}).get("reason_ru") or "").strip(),
        }
    except Exception:
        return {"match": False, "reason_ru": ""}


def _grade_aufgabe(fmt: str, payload: dict, raw_input: str) -> tuple:
    """Single source of truth for grading a submitted aufgabe answer — used by BOTH the
    live task path (evaluate_aufgabe) and the mistakes-review path (evaluate_review), so a
    valid answer is judged IDENTICALLY in both. Deterministic/accepted-list hot path first;
    on a miss, one bounded LLM judge credits genuine equivalents German allows in this exact
    slot (so a correct answer isn't failed → it can actually clear the review queue).
    Returns (is_correct, wrong_reason)."""
    payload = payload or {}
    answer = str(raw_input or "").strip()
    wrong_reason = ""
    if fmt in ("synonym", "antonym"):
        accepted = accepted_de(payload.get("accepted"))
        is_correct = bool(answer) and any(
            check_quiz_freeform_deterministic(user_text=answer, correct_text=c)
            for c in accepted if str(c).strip()
        )
        if not is_correct and answer:
            judged = _synonym_judge_full(str(payload.get("wort") or ""), answer, fmt)
            is_correct = bool(judged.get("match"))
            if not is_correct:
                wrong_reason = str(judged.get("reason_ru") or "")
        return is_correct, wrong_reason
    if fmt == "satzbau":
        is_correct = _check_aufgabe(fmt, payload, raw_input)
        if not is_correct and answer and _satzbau_same_words(payload, answer):
            accepted = payload.get("accepted") or []
            reference = str(payload.get("satz") or (accepted[0] if accepted else ""))
            judged = _satzbau_judge_full(reference, answer)
            is_correct = bool(judged.get("match"))
            if not is_correct:
                wrong_reason = str(judged.get("reason_ru") or "")
        return is_correct, wrong_reason
    if fmt == "cloze":
        is_correct = _check_aufgabe(fmt, payload, raw_input)
        if not is_correct and answer:
            judged = _cloze_judge_full(
                str(payload.get("satz") or ""), str(payload.get("correct") or ""), answer,
            )
            is_correct = bool(judged.get("match"))
            if not is_correct:
                wrong_reason = str(judged.get("reason_ru") or "")
        return is_correct, wrong_reason
    if fmt == "wortgruppe":
        is_correct = _check_aufgabe(fmt, payload, raw_input)
        if not is_correct and answer:
            judged = _wortgruppe_judge_full(
                str(payload.get("satz") or ""), str(payload.get("correct") or ""), answer,
            )
            is_correct = bool(judged.get("match"))
            if not is_correct:
                wrong_reason = str(judged.get("reason_ru") or "")
        return is_correct, wrong_reason
    if fmt == "error":
        return _grade_error(payload, raw_input)
    return _check_aufgabe(fmt, payload, raw_input), ""


def _check_aufgabe(fmt: str, payload: dict, raw_input: str) -> bool:
    answer = str(raw_input or "").strip()
    if not answer:
        return False
    if fmt == "artikel":
        # raw_input = the chosen article ("der"/"die"/"das"). The correct one is
        # carried in payload["correct"] (never sent to the client) — but re-resolved
        # through the deterministic guard so a mislabelled mistake (die Börsenwert,
        # recorded before the fix) grades against the TRUE article (der).
        from backend.article_sprint_generator import resolve_article
        correct = resolve_article(payload.get("wort"), payload.get("correct"))
        return bool(correct) and answer.strip().lower() == correct
    if fmt in ("synonym", "antonym"):
        accepted = accepted_de(payload.get("accepted"))
        if any(check_quiz_freeform_deterministic(user_text=answer, correct_text=c) for c in accepted if str(c).strip()):
            return True
        return _synonym_semantic_match(str(payload.get("wort") or ""), answer, fmt)
    if fmt == "satzbau":
        accepted = [str(a) for a in (payload.get("accepted") or [])]
        if not accepted:
            accepted = [str(payload.get("satz") or "")]
        na = _norm_sentence(answer)
        return any(na and na == _norm_sentence(c) for c in accepted)
    if fmt == "error":
        # raw_input = "i1|c1;i2|c2" (v2 multi) or "i|c" (v1 single). All-or-nothing: the
        # tapped-index set MUST equal the answer key's (found every error, no extra taps on
        # already-correct words), and EVERY correction must match its slot deterministically.
        # The verifier guarantees exactly these errors and nothing else, so a strict index
        # match is right. Correction-text variants get the LLM fallback in _grade_error.
        from backend.database import normalize_error_payload
        key = {e["index"]: e for e in normalize_error_payload(payload)}
        subs = _parse_error_submission(answer)
        if not key or not subs:
            return False
        sub = {}
        for idx, corr in subs:
            if not corr:
                return False
            sub[idx] = corr
        if set(sub) != set(key):
            return False
        for idx, corr in sub.items():
            candidates = [str(key[idx]["correct_word"])] + [str(a) for a in key[idx]["aliases"]]
            if not any(check_quiz_freeform_deterministic(user_text=corr, correct_text=c)
                       for c in candidates if str(c).strip()):
                return False
        return True
    if fmt == "pin":
        # raw_input = "x,y" or "x,y|article": tap inside the bbox AND (if required) the article.
        coords, _, article = answer.partition("|")
        bbox = payload.get("bbox")
        if not (isinstance(bbox, list) and len(bbox) == 4):
            return False
        try:
            x_str, _, y_str = coords.partition(",")
            x, y = float(x_str), float(y_str)
            bx, by, bw, bh = (float(v) for v in bbox)
        except (TypeError, ValueError):
            return False
        m = 0.06  # forgiving margin so a near-miss on a clear object still counts
        in_box = (bx - m) <= x <= (bx + bw + m) and (by - m) <= y <= (by + bh + m)
        req_article = str(payload.get("article") or "").strip().lower()
        if req_article:
            ok_article = check_quiz_freeform_deterministic(user_text=article.strip(), correct_text=req_article)
            return in_box and ok_article
        return in_box
    if fmt == "hoerluecke":
        gaps = payload.get("gaps")
        if isinstance(gaps, list) and gaps:  # multi-gap: answers joined by "|", in order
            answers = answer.split("|")
            if len(answers) != len(gaps):
                return False
            for i, g in enumerate(gaps):
                cands = [str(g.get("correct") or "")] + [str(a) for a in (g.get("aliases") or [])]
                if not any(check_quiz_freeform_deterministic(user_text=answers[i], correct_text=c) for c in cands if str(c).strip()):
                    return False
            return True
        # backward-compat single gap
        candidates = [str(payload.get("correct") or "")] + [str(a) for a in (payload.get("aliases") or [])]
        return any(check_quiz_freeform_deterministic(user_text=answer, correct_text=c) for c in candidates if str(c).strip())
    if fmt == "wortgruppe":
        # Uniqueness is guaranteed at generation, so exact-match is fair; `accepted`
        # (built by the verifier) lists every equivalent spelling (e.g. with/without a
        # repeated article). Fall back to legacy `aliases` for older pool rows.
        variants = payload.get("accepted") or payload.get("aliases") or []
        candidates = [str(payload.get("correct") or "")] + [str(a) for a in variants]
        return any(check_quiz_freeform_exact(user_text=answer, correct_text=c) for c in candidates if str(c).strip())
    if fmt == "transform":
        candidates = [str(a) for a in (payload.get("accepted") or [])]
    else:  # cloze, wortbildung
        candidates = [str(payload.get("correct") or "")]
        candidates += [str(a) for a in (payload.get("aliases") or [])]
    return any(
        check_quiz_freeform_deterministic(user_text=answer, correct_text=c)
        for c in candidates if str(c).strip()
    )


def evaluate_aufgabe(*, dispatch_id: int, user_id: int, raw_input: str) -> dict | None:
    from backend.database import (
        get_aufgabe_dispatch_by_id, get_aufgabe_answer, record_aufgabe_answer,
    )
    dispatch = get_aufgabe_dispatch_by_id(int(dispatch_id))
    if not dispatch:
        return None
    existing = get_aufgabe_answer(dispatch_id=int(dispatch_id), user_id=int(user_id))
    if existing:
        return _aufgabe_result_payload(
            dispatch, is_correct=bool(existing.get("is_correct")), already_answered=True,
            user_answer=str(existing.get("answer") or ""),
        )
    fmt = str(dispatch.get("format") or "")
    payload = dispatch.get("payload") or {}
    answer = str(raw_input or "").strip()
    # One grader for every surface: deterministic/accepted-list hot path, then a bounded
    # LLM judge for the formats that admit genuine equivalents (synonym/antonym, satzbau,
    # cloze, wortgruppe, error). The SAME call runs in the review path so a correct answer
    # is judged identically and can actually clear the "работа над ошибками" queue.
    is_correct, wrong_reason = _grade_aufgabe(fmt, payload, raw_input)
    record_aufgabe_answer(
        dispatch_id=int(dispatch_id), user_id=int(user_id),
        answer=answer, is_correct=bool(is_correct),
    )
    # "Работа над ошибками": remember a wrong grammar answer for spaced-repetition
    # review (light insert on the main path; never blocks the response).
    if not is_correct and fmt in _REVIEW_FORMATS:
        try:
            from backend.database import record_aufgabe_mistake
            record_aufgabe_mistake(
                user_id=int(user_id), fmt=fmt, payload=payload,
                correct_answer=_aufgabe_correct_answer(payload), wrong_answer=answer,
            )
        except Exception:
            logging.warning("aufgabe: record mistake failed dispatch_id=%s", dispatch_id, exc_info=True)
    return _aufgabe_result_payload(
        dispatch, is_correct=is_correct, already_answered=False,
        user_answer=str(raw_input or ""), wrong_reason=wrong_reason,
    )


# ── Synonym/Antonym SPRINT (60s, list as many as you can; rank by count) ──────
SPRINT_DURATION_S = 60


def _sprint_key(relation: str, sprint_id: str) -> str:
    return f"sp_{relation}:{sprint_id}"


def _sprint_core(word: str) -> str:
    """Normalized core of a word for hashing/matching: lowercase, article-stripped,
    punctuation removed. MUST stay in sync with the client's normalize() in
    SprintGame.jsx so live (client-side, no round-trip) checks agree with grading."""
    toks = _normalize_quiz_text(word).split()
    if toks and toks[0] in _GERMAN_ARTICLE_TOKENS:
        toks = toks[1:]
    return " ".join(toks)


def _sprint_hash(word: str) -> str:
    import hashlib
    core = _sprint_core(word)
    return hashlib.sha256(core.encode("utf-8")).hexdigest()[:16] if core else ""


def load_sprint_task(*, dispatch_id: int, user_id: int) -> dict | None:
    from backend.database import get_sprint_dispatch_by_id, get_sprint_item, get_sprint_result
    dispatch = get_sprint_dispatch_by_id(int(dispatch_id))
    if not dispatch:
        return None
    item = get_sprint_item(str(dispatch.get("sprint_id") or ""))
    if not item:
        return None
    relation = str(item.get("relation") or "synonym")
    key = _sprint_key(relation, str(item.get("sprint_id")))
    existing = get_sprint_result(sprint_key=key, user_id=int(user_id))
    # Hashes (not plaintext) of the accepted answers → the client validates each
    # typed word LOCALLY (instant, zero round-trips during the 60s), without the
    # answer key leaking. /finish stays the authoritative grader.
    hashes = sorted({h for de in accepted_de(item.get("accepted")) if (h := _sprint_hash(de))})
    meta = {
        "kind": "sprint",
        "relation": relation,
        "wort": str(item.get("wort") or ""),
        "hint_ru": str(item.get("hint_ru") or ""),
        "duration_s": SPRINT_DURATION_S,
        "accepted_hashes": hashes,
        "already_played": bool(existing),
    }
    if existing:
        meta["result"] = _sprint_result_view(item, existing.get("correct_count") or 0,
                                              existing.get("time_ms") or 0,
                                              user_id=int(user_id), found=None)
    return meta


def check_sprint_word(*, dispatch_id: int, word: str) -> dict:
    """Fast live check: is `word` in the prepared accepted list? (no LLM)."""
    from backend.database import get_sprint_dispatch_by_id, get_sprint_item
    dispatch = get_sprint_dispatch_by_id(int(dispatch_id))
    if not dispatch:
        return {"status": "miss"}
    item = get_sprint_item(str(dispatch.get("sprint_id") or ""))
    if not item:
        return {"status": "miss"}
    accepted = accepted_de(item.get("accepted"))
    hit = any(check_quiz_freeform_deterministic(user_text=word, correct_text=a)
              for a in accepted if str(a).strip())
    return {"status": "hit" if hit else "miss"}


def _sprint_result_view(item: dict, count: int, time_ms: int, *, user_id: int, found: list | None) -> dict:
    from backend.database import compute_sprint_ranking
    relation = str(item.get("relation") or "synonym")
    key = _sprint_key(relation, str(item.get("sprint_id")))
    pairs = accepted_pairs(item.get("accepted"))  # [{de, ru}] for tappable save-chips
    return {
        "kind": "sprint",
        "relation": relation,
        "wort": str(item.get("wort") or ""),
        "count": int(count),
        "found": found or [],
        "accepted": pairs,
        "accepted_total": len(pairs),
        "erklaerung": str(item.get("erklaerung") or ""),
        "tip": str(item.get("tip") or ""),
        "ranking": compute_sprint_ranking(sprint_key=key, user_id=int(user_id)),
    }


def evaluate_sprint(*, dispatch_id: int, user_id: int, words: list, time_ms: int,
                    user_name: str = "") -> dict | None:
    """Grade a finished sprint: count distinct correct (prepared list + one LLM
    batch pass over the misses), record the result (first round counts), rank."""
    from backend.database import (
        get_sprint_dispatch_by_id, get_sprint_item, get_sprint_result, record_sprint_result,
    )
    dispatch = get_sprint_dispatch_by_id(int(dispatch_id))
    if not dispatch:
        return None
    item = get_sprint_item(str(dispatch.get("sprint_id") or ""))
    if not item:
        return None
    relation = str(item.get("relation") or "synonym")
    key = _sprint_key(relation, str(item.get("sprint_id")))

    existing = get_sprint_result(sprint_key=key, user_id=int(user_id))
    if existing:  # anti-replay: keep the first finished round
        return _sprint_result_view(item, existing.get("correct_count") or 0,
                                   existing.get("time_ms") or 0, user_id=int(user_id), found=None)

    accepted = accepted_de(item.get("accepted"))
    # distinct, normalized candidate words
    seen, candidates = set(), []
    for w in (words or []):
        w = str(w or "").strip()
        norm = _normalize_quiz_text(w)
        if not norm or norm in seen:
            continue
        seen.add(norm)
        candidates.append(w)

    found, misses = [], []
    for w in candidates:
        if any(check_quiz_freeform_deterministic(user_text=w, correct_text=a) for a in accepted if str(a).strip()):
            found.append(w)
        else:
            misses.append(w)

    # One bounded LLM batch over the misses → fair to valid words not in the list.
    if misses:
        try:
            import asyncio
            from backend.openai_manager import run_check_synonym_batch
            valid = asyncio.run(asyncio.wait_for(
                run_check_synonym_batch(target_word=str(item.get("wort") or ""),
                                        candidates=misses, relation=relation),
                timeout=11.0,
            ))
            for w in misses:
                if w in valid:
                    found.append(w)
        except Exception:
            pass

    count = len(found)
    record_sprint_result(sprint_key=key, user_id=int(user_id), user_name=str(user_name or ""),
                         correct_count=count, time_ms=int(time_ms or 0))
    return _sprint_result_view(item, count, int(time_ms or 0), user_id=int(user_id), found=found)
