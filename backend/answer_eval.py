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
    lowered = str(value or "").lower()
    cleaned = re.sub(r"[^a-zäöüßà-ÿ0-9\s'\-]", " ", lowered)
    cleaned = cleaned.replace("-", " ")
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


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
    }
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


# ── Hörverständnis (listening): load + async-graded submit + poll ────────────

def _listening_result_payload(dispatch: dict, answers: list, evaluation: list,
                              *, already_answered: bool) -> dict:
    """Merge questions + the user's answers + the LLM evals into one verdict."""
    questions = list(dispatch.get("questions_json") or [])
    answers = answers if isinstance(answers, list) else []
    evaluation = evaluation if isinstance(evaluation, list) else []
    items = []
    correct = 0
    for i, q in enumerate(questions):
        ev = evaluation[i] if i < len(evaluation) and isinstance(evaluation[i], dict) else {}
        is_ok = bool(ev.get("content_correct"))
        if is_ok:
            correct += 1
        items.append({
            "number": int(q.get("number") or (i + 1)),
            "question_de": str(q.get("question_de") or ""),
            "user_answer": str(answers[i]) if i < len(answers) else "",
            "content_correct": is_ok,
            "content_feedback_ru": str(ev.get("content_feedback_ru") or ""),
            "correct_answer_de": str(q.get("correct_answer_de") or ""),
        })
    return {
        "kind": "listening",
        "items": items,
        "correct_count": correct,
        "total": len(questions),
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
            from backend.listening_evaluator import evaluate_listening_answers
            from backend.database import save_listening_evaluation, record_challenge_result
            evals = evaluate_listening_answers(german_text, questions, answers)
            # Record the ranking result BEFORE flipping status to 'done', so the
            # first poll that sees 'done' already has a ranking to show.
            try:
                total = len(questions)
                correct = sum(1 for i in range(total)
                              if i < len(evals) and isinstance(evals[i], dict) and evals[i].get("content_correct"))
                record_challenge_result(
                    challenge_key=str(ranking_key), user_id=int(user_id),
                    user_name=str(user_name or ""), is_correct=(total > 0 and correct == total),
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


# ── Freeform quiz ("keine korrekte Antworten" → type answer): load + evaluate ──

def _quiz_freeform_semantic_match(user_text: str, correct_text: str) -> bool:
    """Bounded LLM fallback for paraphrases/synonyms (single-word answers).

    Sync wrapper around the async semantic checker — only called when the
    deterministic match misses, so it's off the hot path for most answers.
    """
    canonical = str(correct_text or "").strip()
    guess = str(user_text or "").strip()
    if not canonical or not guess:
        return False
    if len(canonical.split()) > 5 or len(guess.split()) > 5 or len(canonical) > 80 or len(guess) > 80:
        return False
    try:
        import asyncio
        from backend.openai_manager import run_check_story_guess_semantic
        result = asyncio.run(asyncio.wait_for(
            run_check_story_guess_semantic(canonical_answer=canonical, aliases=[], user_guess=guess),
            timeout=8.0,
        ))
        return bool((result or {}).get("is_correct"))
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


def load_freeform_task(*, dispatch_id: int, user_id: int) -> dict | None:
    from backend.database import get_quiz_freeform_dispatch_by_id, get_quiz_freeform_answer
    dispatch = get_quiz_freeform_dispatch_by_id(int(dispatch_id))
    if not dispatch:
        return None
    existing = get_quiz_freeform_answer(dispatch_id=int(dispatch_id), user_id=int(user_id))
    meta = {
        "kind": "freeform",
        "hint_ru": str(dispatch.get("word_ru") or ""),
        "already_answered": bool(existing),
    }
    if existing:
        meta["result"] = _freeform_result_payload(
            dispatch, is_correct=bool(existing.get("is_correct")), already_answered=True,
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

def _aufgabe_correct_answer(payload: dict) -> str:
    """The canonical answer to show after answering (per format)."""
    if payload.get("wort") and payload.get("accepted"):  # synonym/antonym: show a few
        return " · ".join(str(a) for a in (payload.get("accepted") or [])[:3])
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


def _aufgabe_result_payload(dispatch: dict, *, is_correct: bool, already_answered: bool,
                           user_answer: str = "") -> dict:
    payload = dispatch.get("payload") or {}
    fmt = str(dispatch.get("format") or "")
    # Sentence formats (satzbau/transform) get a word-level diff in the FE:
    # the correct sentence in green over the user's sentence with wrong words
    # struck through. Other formats keep the simple "correct answer" line.
    is_sentence = fmt in ("satzbau", "transform")
    return {
        "kind": "aufgabe",
        "format": fmt,
        "is_correct": bool(is_correct),
        "correct_word": _aufgabe_correct_answer(payload),
        "is_sentence": bool(is_sentence),
        "user_answer": str(user_answer or "").strip(),
        "hint_ru": str(payload.get("hint_ru") or ""),
        "explanation": str(payload.get("erklaerung") or payload.get("explanation") or ""),
        "tip": str(payload.get("tip") or ""),
        "already_answered": bool(already_answered),
        "saveable_words": [],
    }


def load_aufgabe_task(*, dispatch_id: int, user_id: int) -> dict | None:
    from backend.database import get_aufgabe_dispatch_by_id, get_aufgabe_answer
    dispatch = get_aufgabe_dispatch_by_id(int(dispatch_id))
    if not dispatch:
        return None
    payload = dispatch.get("payload") or {}
    fmt = str(dispatch.get("format") or "")
    meta = {
        "kind": "aufgabe",
        "format": fmt,
        "level": str(dispatch.get("level") or "B2"),
        "hint_ru": str(payload.get("hint_ru") or ""),
        "already_answered": False,
    }
    # Prompt fields shown to the user (never the answer/explanation until answered).
    if fmt == "cloze":
        meta["satz"] = str(payload.get("satz") or "")
    elif fmt == "wortbildung":
        meta["satz"] = str(payload.get("satz") or "")
        meta["stamm"] = str(payload.get("stamm") or "")
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
        meta["question_de"] = str(payload.get("question_de") or "")
        meta["needs_article"] = bool(str(payload.get("article") or "").strip())
        meta["image_url"] = ""
        key = str(payload.get("image_object_key") or "")
        if key:
            try:
                from backend.r2_storage import r2_public_url
                meta["image_url"] = r2_public_url(key)
            except Exception:
                meta["image_url"] = ""
    existing = get_aufgabe_answer(dispatch_id=int(dispatch_id), user_id=int(user_id))
    if existing:
        meta["already_answered"] = True
        meta["result"] = _aufgabe_result_payload(
            dispatch, is_correct=bool(existing.get("is_correct")), already_answered=True,
        )
    return meta


def _norm_sentence(s: str) -> str:
    """Order-preserving sentence normalizer for Satzbau: lowercase, collapse spaces,
    drop punctuation. Same words in the SAME order → equal; wrong order → different."""
    import re as _re
    t = _re.sub(r"[^\wäöüß\s]", " ", str(s or "").lower(), flags=_re.UNICODE)
    return _re.sub(r"\s+", " ", t).strip()


def _synonym_semantic_match(target: str, candidate: str, relation: str) -> bool:
    """Bounded LLM fallback: only fires when the accepted-list misses, so the hot
    path stays fast for the common answers."""
    t, c = str(target or "").strip(), str(candidate or "").strip()
    if not t or not c or len(c.split()) > 3 or len(c) > 60:
        return False
    try:
        import asyncio
        from backend.openai_manager import run_check_synonym
        res = asyncio.run(asyncio.wait_for(
            run_check_synonym(target_word=t, candidate=c, relation=str(relation or "synonym")),
            timeout=7.0,
        ))
        return bool((res or {}).get("match"))
    except Exception:
        return False


def _check_aufgabe(fmt: str, payload: dict, raw_input: str) -> bool:
    answer = str(raw_input or "").strip()
    if not answer:
        return False
    if fmt in ("synonym", "antonym"):
        accepted = [str(a) for a in (payload.get("accepted") or [])]
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
        # raw_input = "{tapped_index}|{correction}"
        idx_str, _, correction = answer.partition("|")
        try:
            tapped = int(idx_str)
        except ValueError:
            return False
        if tapped != int(payload.get("error_index", -1)):
            return False
        candidates = [str(payload.get("correct_word") or "")] + [str(a) for a in (payload.get("aliases") or [])]
        return any(check_quiz_freeform_deterministic(user_text=correction, correct_text=c) for c in candidates if str(c).strip())
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
    is_correct = _check_aufgabe(str(dispatch.get("format") or ""), dispatch.get("payload") or {}, raw_input)
    record_aufgabe_answer(
        dispatch_id=int(dispatch_id), user_id=int(user_id),
        answer=str(raw_input or "").strip(), is_correct=bool(is_correct),
    )
    return _aufgabe_result_payload(
        dispatch, is_correct=is_correct, already_answered=False,
        user_answer=str(raw_input or ""),
    )


# ── Synonym/Antonym SPRINT (60s, list as many as you can; rank by count) ──────
SPRINT_DURATION_S = 60


def _sprint_key(relation: str, sprint_id: str) -> str:
    return f"sp_{relation}:{sprint_id}"


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
    meta = {
        "kind": "sprint",
        "relation": relation,
        "wort": str(item.get("wort") or ""),
        "hint_ru": str(item.get("hint_ru") or ""),
        "duration_s": SPRINT_DURATION_S,
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
    accepted = [str(a) for a in (item.get("accepted") or [])]
    hit = any(check_quiz_freeform_deterministic(user_text=word, correct_text=a)
              for a in accepted if str(a).strip())
    return {"status": "hit" if hit else "miss"}


def _sprint_result_view(item: dict, count: int, time_ms: int, *, user_id: int, found: list | None) -> dict:
    from backend.database import compute_sprint_ranking
    relation = str(item.get("relation") or "synonym")
    key = _sprint_key(relation, str(item.get("sprint_id")))
    accepted = [str(a) for a in (item.get("accepted") or [])]
    return {
        "kind": "sprint",
        "relation": relation,
        "wort": str(item.get("wort") or ""),
        "count": int(count),
        "found": found or [],
        "accepted": accepted,
        "accepted_total": len(accepted),
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

    accepted = [str(a) for a in (item.get("accepted") or [])]
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
