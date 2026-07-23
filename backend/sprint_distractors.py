"""Nightly adversarial pipeline that turns one synonym/antonym sprint word into
multiple-choice TRAINER rounds.

A trainer round shows the anchor word plus one CORRECT answer (drawn from the
sprint's `accepted` list) and up to four DISTRACTORS — plausible-looking words
that are NOT a valid {synonym/antonym}. The whole risk of the trainer is that a
"distractor" is secretly a real answer: then we'd tell the learner they're wrong
when they're right. So every distractor must survive three independent stages and
a unanimous "clearly not an answer" verdict before it ships:

    SETTER      generate candidates + self-justify why each is NOT a {relation}
      ↓         (backend.openai_manager.run_generate_sprint_distractors)
    PROSECUTOR  adversarially try to PROVE each candidate IS a valid answer
      ↓         (+ substitution test); any credible case → drop
    JUDGE       fresh, independent strict classification; only DEFINITELY_NOT lives

Consensus rule: keep a distractor only if PROSECUTOR == PASS AND JUDGE ==
DEFINITELY_NOT. Every gate fails toward DROPPING. This runs at pool-build time on
the full model, never on the user's hot path.
"""

from __future__ import annotations

import asyncio
import logging
import re

logger = logging.getLogger(__name__)

_ARTICLES = {"der", "die", "das", "den", "dem", "des", "ein", "eine", "einen",
             "einem", "einer", "eines"}

# How many clean distractors a word needs before it may enter the trainer rotation.
# Below this the round would offer no real choice, so the word is held back (the
# rail keeps a buffer of warmed words ahead so this never leaves a gap).
MIN_CLEAN_DISTRACTORS = 2


def _norm(s: str) -> str:
    """Loose comparison key: lowercase, drop a leading article, collapse spacing.
    Mirrors answer_eval._sprint_core closely enough for dedup against the accepted list."""
    x = re.sub(r"[^a-zäöüßà-ÿ0-9\s'-]", " ", str(s or "").lower())
    x = re.sub(r"-", " ", x)
    x = re.sub(r"\s+", " ", x).strip()
    toks = x.split(" ") if x else []
    if toks and toks[0] in _ARTICLES:
        toks = toks[1:]
    return " ".join(toks)


def _pair_de(p) -> str:
    return str((p.get("de") if isinstance(p, dict) else p) or "").strip()


async def build_trainer_distractors(
    *, target_word: str, relation: str, correct_pairs: list, hint_ru: str = "",
    pos: str = "", want: int = 16,
) -> dict:
    """Run the full pipeline for ONE word. Returns:
      {
        "kept":      [{word, article, ru_gloss, trap_type, why_not, example_de, example_ru}],
        "rejected":  [{word, stage, reason}],   # for tuning the SETTER prompt
        "target_example": {"de": "...", "ru": "..."},
        "trainer_ready": bool,                   # >= MIN_CLEAN_DISTRACTORS survived
        "counts": {"generated": n, "after_prosecutor": n, "kept": n},
      }
    On a SETTER failure returns an empty, not-ready result.
    """
    from backend.openai_manager import (
        run_generate_sprint_distractors, run_prosecute_distractor, run_judge_distractor_set,
    )

    relation = str(relation or "synonym")
    correct_de = [_pair_de(p) for p in (correct_pairs or []) if _pair_de(p)]
    sense_ru = str(hint_ru or "").strip()

    # ── Stage 1: SETTER ──────────────────────────────────────────────────────
    setter = await run_generate_sprint_distractors(
        target_word=target_word, relation=relation, sense_ru=sense_ru, sense_de="",
        correct=correct_de, pos=pos, count=int(want),
    )
    target_example = {
        "de": str((setter or {}).get("target_example_de") or "").strip(),
        "ru": str((setter or {}).get("target_example_ru") or "").strip(),
    }
    raw = (setter or {}).get("candidates") or []
    rejected: list[dict] = []

    # Deterministic pre-filter: drop empties, the target itself, and anything that
    # collides with an accepted answer or another candidate before spending LLM calls.
    seen: set[str] = {_norm(target_word)} | {_norm(c) for c in correct_de}
    candidates: list[dict] = []
    for c in raw:
        word = str(c.get("word") or "").strip()
        key = _norm(word)
        if not key:
            continue
        if key in seen:
            rejected.append({"word": word, "stage": "prefilter",
                             "reason": "duplicate of target/accepted/other candidate"})
            continue
        seen.add(key)
        candidates.append(c)

    generated = len(candidates)
    if not candidates:
        return {"kept": [], "rejected": rejected, "target_example": target_example,
                "trainer_ready": False,
                "counts": {"generated": 0, "after_prosecutor": 0, "kept": 0}}

    # ── Stage 2: PROSECUTOR (adversarial, one bounded call per candidate) ─────
    prosecutions = await asyncio.gather(*[
        run_prosecute_distractor(
            target_word=target_word, relation=relation, sense_de="",
            candidate=str(c.get("word") or ""), target_example_de=target_example["de"],
        )
        for c in candidates
    ], return_exceptions=True)

    survivors: list[dict] = []
    for c, verdict in zip(candidates, prosecutions):
        word = str(c.get("word") or "").strip()
        if isinstance(verdict, Exception) or not isinstance(verdict, dict):
            rejected.append({"word": word, "stage": "prosecutor", "reason": "prosecutor_error"})
            continue
        if verdict.get("verdict") == "PASS":
            survivors.append(c)
        else:
            reason = verdict.get("best_case_for_acceptance") or "argued as a valid answer"
            if verdict.get("substitution_ok"):
                reason = f"substitution works · {reason}"
            rejected.append({"word": word, "stage": "prosecutor", "reason": reason})

    after_prosecutor = len(survivors)
    if not survivors:
        return {"kept": [], "rejected": rejected, "target_example": target_example,
                "trainer_ready": False,
                "counts": {"generated": generated, "after_prosecutor": 0, "kept": 0}}

    # ── Stage 3: JUDGE (independent, strict classification over survivors) ────
    verdicts = await run_judge_distractor_set(
        target_word=target_word, relation=relation, sense_de="",
        distractors=[str(c.get("word") or "") for c in survivors],
    )
    # Fail closed: if the judge returns nothing (or a misaligned list), drop all —
    # an unverified distractor never reaches a learner.
    by_word = {_norm(v.get("word")): v for v in verdicts if v.get("word")}

    kept: list[dict] = []
    for c in survivors:
        word = str(c.get("word") or "").strip()
        v = by_word.get(_norm(word))
        cls = (v or {}).get("class")
        issues = (v or {}).get("issues") or []
        if cls == "DEFINITELY_NOT" and not issues:
            kept.append({
                "word": word,
                "article": (c.get("article") if c.get("article") in ("der", "die", "das") else None),
                "ru_gloss": str(c.get("ru_gloss") or "").strip(),
                "trap_type": str(c.get("trap_type") or "").strip(),
                "why_not": str(c.get("why_not") or "").strip(),
                "example_de": str(c.get("example_de") or "").strip(),
                "example_ru": str(c.get("example_ru") or "").strip(),
            })
        else:
            reason = f"judge={cls or 'no_verdict'}"
            if issues:
                reason += f" issues={','.join(issues)}"
            rejected.append({"word": word, "stage": "judge", "reason": reason})

    result = {
        "kept": kept,
        "rejected": rejected,
        "target_example": target_example,
        "trainer_ready": len(kept) >= MIN_CLEAN_DISTRACTORS,
        "counts": {"generated": generated, "after_prosecutor": after_prosecutor, "kept": len(kept)},
    }
    logger.info(
        "trainer distractors word=%s relation=%s generated=%d prosecutor=%d kept=%d ready=%s",
        target_word, relation, generated, after_prosecutor, len(kept), result["trainer_ready"],
    )
    return result
