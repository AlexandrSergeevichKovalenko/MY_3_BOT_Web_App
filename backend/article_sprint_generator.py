"""
Artikel Sprint — theme noun-bank filler.

For a theme it walks EVERY subtopic, asks GPT for nouns (word + article +
meaning_ru + plural + difficulty), then a second LLM verifies each article is
correct AND unambiguous (rejects der/die-See type words), dedups, and inserts
verified rows. Mirrors our other two-model quality gates. Sync (call via thread);
each LLM call runs through asyncio.run, the proven pattern in this codebase.
"""
from __future__ import annotations

import asyncio
import logging


def _run(coro):
    return asyncio.run(coro)


def fill_theme(theme_key: str, *, max_to_add: int | None = None, per_subtopic: int = 30) -> dict:
    """Generate+verify+insert nouns for `theme_key`.
    max_to_add: cap how many NEW words to add this run (None → up to target).
    Returns stats dict."""
    from backend.article_sprint_themes import article_sprint_themes
    from backend.openai_manager import run_article_noun_gen, run_article_verify
    from backend.database import (
        ensure_article_sprint_schema, count_article_sprint_nouns,
        insert_article_sprint_nouns, list_article_sprint_words,
    )

    theme = next((t for t in article_sprint_themes() if t["key"] == theme_key), None)
    if not theme:
        return {"error": "unknown_theme", "theme": theme_key}
    ensure_article_sprint_schema()

    target = int(theme["target_count"])
    have = count_article_sprint_nouns(theme_key, verified_only=True)
    cap = int(max_to_add) if max_to_add else max(0, target - have)
    if cap <= 0:
        return {"theme": theme_key, "added": 0, "rejected": 0,
                "final_verified": have, "target": target, "note": "already at target"}

    existing = {w.lower() for w in list_article_sprint_words(theme_key)}
    added = 0
    rejected = 0
    by_subtopic: dict[str, int] = {}

    for subtopic in theme["subtopics"]:
        if added >= cap:
            break
        try:
            gen = _run(run_article_noun_gen(
                theme=theme["label_de"], subtopic=subtopic,
                count=per_subtopic, avoid=list(existing)[:200],
            ))
        except Exception:
            logging.warning("fill_theme: gen failed theme=%s subtopic=%s", theme_key, subtopic, exc_info=True)
            continue

        candidates: list[dict] = []
        for n in gen:
            w = str(n.get("word") or "").strip()
            art = str(n.get("article") or "").strip().lower()
            if not w or art not in ("der", "die", "das") or w.lower() in existing:
                continue
            candidates.append(n)
        if not candidates:
            continue

        try:
            verdicts = _run(run_article_verify(
                items=[{"word": n["word"], "article": str(n["article"]).lower()} for n in candidates]
            ))
        except Exception:
            logging.warning("fill_theme: verify failed theme=%s subtopic=%s", theme_key, subtopic, exc_info=True)
            continue

        rows: list[dict] = []
        for n, v in zip(candidates, verdicts):
            if not isinstance(v, dict) or not v.get("ok"):
                rejected += 1
                continue
            art = str(v.get("article") or n.get("article") or "").strip().lower()
            w = str(n.get("word") or "").strip()
            if art not in ("der", "die", "das") or not w or w.lower() in existing:
                rejected += 1
                continue
            rows.append({
                "word": w, "article": art,
                "meaning_ru": str(n.get("meaning_ru") or ""),
                "plural": str(n.get("plural") or ""),
                "difficulty": str(n.get("difficulty") or "B"),
                "subtopic": subtopic, "source": "gpt", "verified": True,
            })
            existing.add(w.lower())

        if added + len(rows) > cap:
            rows = rows[: max(0, cap - added)]
        if rows:
            res = insert_article_sprint_nouns(theme_key, rows)
            added += int(res.get("inserted") or 0)
            by_subtopic[subtopic] = int(res.get("inserted") or 0)

    final = count_article_sprint_nouns(theme_key, verified_only=True)
    return {
        "theme": theme_key, "added": added, "rejected": rejected,
        "final_verified": final, "target": target, "by_subtopic": by_subtopic,
    }
