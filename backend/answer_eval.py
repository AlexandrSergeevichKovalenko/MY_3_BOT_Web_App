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

KNOWN_ARTICLES = {"der", "die", "das"}


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
    """Metadata to render N crossword inputs (clue + length, never the word),
    plus the stored result if the user already answered."""
    hidden = _load_crossword_hidden(dispatch_id)
    if not hidden:
        return None

    from backend.database import get_crossword_answers
    existing = get_crossword_answers(dispatch_id=int(dispatch_id), user_id=int(user_id))

    meta = {
        "kind": "crossword",
        "words": [
            {
                "number": hw["number"],
                "direction": hw["direction"],
                "clue_de": hw["clue_de"],
                "clue_ru": hw["clue_ru"],
                "length": len(hw["word"]),
            }
            for hw in hidden
        ],
        "already_answered": bool(existing),
    }
    if existing:
        meta["result"] = _crossword_result_from_stored(hidden, existing)
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


def _spawn_listening_grader(answer_id: int, german_text: str, questions: list, answers: list) -> None:
    import threading

    def _run():
        import logging
        try:
            from backend.listening_evaluator import evaluate_listening_answers
            from backend.database import save_listening_evaluation
            evals = evaluate_listening_answers(german_text, questions, answers)
            save_listening_evaluation(answer_id=int(answer_id), evaluation_json=evals)
        except Exception:
            logging.warning("listening grader thread failed answer_id=%s", answer_id, exc_info=True)
            try:
                from backend.database import mark_listening_evaluation_failed
                mark_listening_evaluation_failed(int(answer_id))
            except Exception:
                logging.warning("listening grader: mark_failed failed answer_id=%s", answer_id, exc_info=True)

    threading.Thread(target=_run, daemon=True).start()


def start_listening_evaluation(*, dispatch_id: int, user_id: int, answers: list) -> dict | None:
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
        return _listening_result_payload(
            dispatch, existing.get("answers") or [], existing.get("evaluation") or [],
            already_answered=True,
        )

    clean_answers = [str(a or "").strip() for a in (answers or [])]
    answer_id = save_listening_answers(
        dispatch_id=int(dispatch_id), user_id=int(user_id), answers_json=clean_answers,
    )
    if not answer_id:
        return {"kind": "listening", "status": "failed"}

    _spawn_listening_grader(
        answer_id, str(dispatch.get("german_text") or ""),
        list(dispatch.get("questions_json") or []), clean_answers,
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
            "result": _listening_result_payload(
                dispatch, status.get("answers") or [], status.get("evaluation") or [],
                already_answered=True,
            ),
        }
    return {"kind": "listening", "status": st}
