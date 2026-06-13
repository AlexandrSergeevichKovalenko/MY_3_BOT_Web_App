"""
Artikel Sprint — daily shared set builder.

Freezes ONE ordered word set per day from the day's theme (verified nouns only),
so every player competes on the same set (fair ranking). If the chosen theme is
sparse, tops up from other themes so the 2-minute game never runs short.
"""
from __future__ import annotations

import logging
import random

DEFAULT_SET_SIZE = 140      # plenty for a 2-min game even for fast players
MIN_PLAYABLE = 60           # below this we won't ship a daily set


def _pick_fallback_theme(play_date, min_have: int) -> str | None:
    """Deterministic theme rotation among themes that have enough verified words."""
    from backend.database import list_article_sprint_themes
    themes = [t for t in list_article_sprint_themes() if int(t.get("verified_count") or 0) >= min_have]
    if not themes:
        return None
    themes.sort(key=lambda t: t["theme_key"])
    idx = play_date.toordinal() % len(themes)
    return themes[idx]["theme_key"]


def build_daily_set(play_date, *, size: int = DEFAULT_SET_SIZE) -> dict:
    """Build (or rebuild) the daily shared set for play_date. Returns stats dict."""
    from backend.database import (
        ensure_article_sprint_schema, get_article_sprint_theme_for_date,
        get_article_sprint_verified_sample, upsert_article_sprint_set,
    )
    ensure_article_sprint_schema()

    theme_key = get_article_sprint_theme_for_date(play_date)
    if not theme_key:
        theme_key = _pick_fallback_theme(play_date, min_have=MIN_PLAYABLE)
    if not theme_key:
        # No theme has enough verified words yet → try a pure mix across all themes.
        words = get_article_sprint_verified_sample(None, size)
        theme_key = "gemischt"
    else:
        words = get_article_sprint_verified_sample(theme_key, size)
        if len(words) < size:
            # top up from any theme (mix), excluding already-picked words
            have = {str(w["w"]).lower() for w in words}
            extra = get_article_sprint_verified_sample(None, size - len(words), exclude_words=list(have))
            words.extend(extra)

    # dedup (case-insensitive) + shuffle
    seen: set[str] = set()
    uniq: list[dict] = []
    for w in words:
        k = str(w.get("w") or "").lower()
        if k and k not in seen:
            seen.add(k)
            uniq.append({"w": w["w"], "a": str(w["a"]).lower(), "ru": w.get("ru") or ""})
    random.shuffle(uniq)

    if len(uniq) < MIN_PLAYABLE:
        return {"status": "insufficient", "theme_key": theme_key,
                "available": len(uniq), "min_playable": MIN_PLAYABLE,
                "hint": "наполни темы через /artikel_fill"}

    set_id = f"asd_{play_date.isoformat()}"
    upsert_article_sprint_set(
        set_id=set_id, kind="daily", play_date=play_date,
        theme_key=theme_key, words=uniq,
    )
    logging.info("article_sprint: built daily set %s theme=%s words=%s", set_id, theme_key, len(uniq))
    return {"status": "ready", "set_id": set_id, "theme_key": theme_key, "word_count": len(uniq)}


PRACTICE_SET_SIZE = 120
PRACTICE_MIN = 20


def build_practice_set(theme_key: str, user_id: int, play_date, *, size: int = PRACTICE_SET_SIZE) -> dict:
    """Build a fresh personal practice set for a Pro user from one of the 21 themes
    (solo, not ranked). A new set_id each call → always replayable."""
    import time
    from backend.database import get_article_sprint_verified_sample, upsert_article_sprint_set
    words = get_article_sprint_verified_sample(theme_key, size)
    seen: set[str] = set()
    uniq: list[dict] = []
    for w in words:
        k = str(w.get("w") or "").lower()
        if k and k not in seen:
            seen.add(k)
            uniq.append({"w": w["w"], "a": str(w["a"]).lower(), "ru": w.get("ru") or ""})
    random.shuffle(uniq)
    if len(uniq) < PRACTICE_MIN:
        return {"status": "insufficient", "theme_key": theme_key, "available": len(uniq)}
    set_id = f"asp_{int(user_id)}_{theme_key}_{int(time.time())}"
    upsert_article_sprint_set(
        set_id=set_id, kind="practice", play_date=play_date,
        theme_key=theme_key, words=uniq, owner_user_id=int(user_id),
    )
    return {"status": "ready", "set_id": set_id, "theme_key": theme_key, "word_count": len(uniq)}


def build_battle_set(theme_key: str, battle_id: int, play_date, *, size: int = DEFAULT_SET_SIZE) -> dict:
    """One frozen shared set for a battle (set_id = 'asb_<battle_id>'). All members
    compete on the same words."""
    from backend.database import get_article_sprint_verified_sample, upsert_article_sprint_set
    words = get_article_sprint_verified_sample(theme_key, size)
    if len(words) < size:
        have = {str(w["w"]).lower() for w in words}
        words.extend(get_article_sprint_verified_sample(None, size - len(words), exclude_words=list(have)))
    seen: set[str] = set()
    uniq: list[dict] = []
    for w in words:
        k = str(w.get("w") or "").lower()
        if k and k not in seen:
            seen.add(k)
            uniq.append({"w": w["w"], "a": str(w["a"]).lower(), "ru": w.get("ru") or ""})
    random.shuffle(uniq)
    if len(uniq) < PRACTICE_MIN:
        return {"status": "insufficient", "theme_key": theme_key, "available": len(uniq)}
    set_id = f"asb_{int(battle_id)}"
    upsert_article_sprint_set(
        set_id=set_id, kind="battle", play_date=play_date, theme_key=theme_key, words=uniq,
    )
    return {"status": "ready", "set_id": set_id, "theme_key": theme_key, "word_count": len(uniq)}
