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
        count_article_theme_verified,
    )
    ensure_article_sprint_schema()

    # The set's CONTENT must match its LABEL. The scheduled theme is used ONLY if it
    # actually has enough verified words; otherwise we switch to a theme that does
    # (and relabel to it) instead of topping up with foreign-theme words — that
    # top-up was what put medical nouns under a "Technik & Computer" header.
    scheduled = get_article_sprint_theme_for_date(play_date)
    theme_key = scheduled
    if not theme_key or count_article_theme_verified(theme_key) < MIN_PLAYABLE:
        fallback = _pick_fallback_theme(play_date, min_have=MIN_PLAYABLE)
        if scheduled and fallback and fallback != scheduled:
            logging.warning(
                "article_sprint: scheduled theme '%s' too sparse (<%s verified) → using '%s'",
                scheduled, MIN_PLAYABLE, fallback,
            )
        theme_key = fallback
    if not theme_key:
        # No single theme has enough verified words yet → honest mixed set.
        words = get_article_sprint_verified_sample(None, size)
        theme_key = "gemischt"
    else:
        # Single coherent theme — no cross-theme top-up (a theme with >= MIN_PLAYABLE
        # words is plenty for a 2-min game and a learning deck).
        words = get_article_sprint_verified_sample(theme_key, size)

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


BATTLE_SET_SIZE = 350   # 2-min battle, ~150 taps/min fast → preload with a buffer


def build_battle_set_mixed(theme_keys, battle_id: int, play_date, *, size: int = BATTLE_SET_SIZE) -> dict:
    """Battle words mixed RANDOMLY (different every time). theme_keys empty/None →
    sample across ALL themes; otherwise across the selected themes only. ~`size`
    words preloaded so a 2-min battle never runs out."""
    from backend.database import get_article_sprint_verified_sample, upsert_article_sprint_set
    keys = [str(k) for k in (theme_keys or []) if str(k).strip()]
    words: list[dict] = []
    if not keys:
        words = get_article_sprint_verified_sample(None, size)  # ORDER BY random() — fresh mix
    else:
        per = max(1, size // len(keys)) + 8
        have: list[str] = []
        for tk in keys:
            for w in get_article_sprint_verified_sample(tk, per, exclude_words=have):
                words.append(w)
                have.append(str(w["w"]))
        if len(words) < size:  # top up from the SAME selected themes (keeps the focus)
            for tk in keys:
                if len(words) >= size:
                    break
                for w in get_article_sprint_verified_sample(tk, size - len(words), exclude_words=have):
                    words.append(w)
                    have.append(str(w["w"]))
    seen: set[str] = set()
    uniq: list[dict] = []
    for w in words:
        k = str(w.get("w") or "").lower()
        if k and k not in seen:
            seen.add(k)
            uniq.append({"w": w["w"], "a": str(w["a"]).lower(), "ru": w.get("ru") or ""})
    random.shuffle(uniq)
    uniq = uniq[:size]
    if len(uniq) < PRACTICE_MIN:
        return {"status": "insufficient", "available": len(uniq)}
    set_id = f"asb_{int(battle_id)}"
    upsert_article_sprint_set(
        set_id=set_id, kind="battle", play_date=play_date,
        theme_key=(keys[0] if len(keys) == 1 else "gemischt"), words=uniq,
    )
    return {"status": "ready", "set_id": set_id, "word_count": len(uniq),
            "themes": keys or ["all"]}


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
