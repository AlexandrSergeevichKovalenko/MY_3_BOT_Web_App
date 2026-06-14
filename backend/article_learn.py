"""Artikel Trainer — the LEARNING companion to the Artikel Sprint game.

Where Sprint is the timed test, this is the self-paced study deck: look at a noun,
pick der/die/das, get instant feedback + a short "why" tip + (later) audio/image,
then swipe to the next card. The daily deck reuses the day's Sprint set words, so
"learn today → the game tests the same words tonight".

This module owns:
  • gender_tip(word, article)  — deterministic, instant Russian hint by ending
    (no LLM). The pedagogical core for Step 1; Phase 4 layers LLM mnemonics on top.
  • build_learn_deck(...)       — assembles the card list from the daily Sprint set
    (+ the user's review pile, wired in Step 3).
"""
from __future__ import annotations

import logging


# der = синий, die = красный, das = зелёный (common German-learner colour code).
GENDER_COLOR = {"der": "blue", "die": "red", "das": "green"}

# STRONG endings: near-100% reliable. If the article matches → confident rule; if
# not → a genuine rare exception worth flagging. (suffix, expected_article, ru_rule)
_STRONG_SUFFIX_RULES: list[tuple[str, str, str]] = [
    ("chen", "das", "-chen → всегда das (уменьшительное)"),
    ("lein", "das", "-lein → всегда das (уменьшительное)"),
    ("ung", "die", "-ung → почти всегда die"),
    ("heit", "die", "-heit → die"),
    ("keit", "die", "-keit → die"),
    ("schaft", "die", "-schaft → die"),
    ("tion", "die", "-tion → die"),
    ("sion", "die", "-sion → die"),
    ("tät", "die", "-tät → die"),
    ("ung", "die", "-ung → die"),
    ("ling", "der", "-ling → der"),
    ("ismus", "der", "-ismus → der"),
    ("ment", "das", "-ment → das"),
]

# WEAK endings: a tendency only. Used when the article matches; on a mismatch we
# fall through to the generic tip (the rule isn't reliable enough to call it an
# "exception"). (suffix, expected_article, ru_rule)
_WEAK_SUFFIX_RULES: list[tuple[str, str, str]] = [
    ("ität", "die", "-ität → die"),
    ("ik", "die", "-ik → обычно die"),
    ("enz", "die", "-enz → die"),
    ("anz", "die", "-anz → die"),
    ("ei", "die", "-ei → обычно die"),
    ("ie", "die", "-ie → обычно die"),
    ("ur", "die", "-ur → обычно die"),
    ("tum", "das", "-tum → обычно das"),
    ("um", "das", "-um → обычно das (латинские слова)"),
    ("nis", "das", "-nis → обычно das (бывает и die)"),
    ("ma", "das", "-ma → обычно das"),
    ("or", "der", "-or → обычно der"),
    ("ent", "der", "-ent → обычно der (лицо/деятель)"),
    ("ant", "der", "-ant → обычно der"),
    ("ich", "der", "-ich → обычно der"),
    ("ig", "der", "-ig → обычно der"),
    ("ner", "der", "-ner → обычно der"),
    ("er", "der", "-er → часто der (деятель/профессия), но есть исключения"),
]

_ART_RU = {"der": "мужской (der)", "die": "женский (die)", "das": "средний (das)"}

# Conservative compound-head map: a German compound takes the gender of its LAST
# noun. Only LONG, low-false-positive heads (≥4 chars) with their RU gloss; we emit
# the hint ONLY when the head's article equals the word's article (self-validating)
# and the word is strictly longer than the head (it's really a compound). This is a
# STOPGAP shown before the LLM mnemonic is generated; the LLM handles splitting far
# better, so we keep this list short and safe rather than exhaustive.
_HEAD_NOUNS: dict[str, tuple[str, str]] = {
    "körper": ("der", "тело"), "knochen": ("der", "кость"), "muskel": ("der", "мышца"),
    "zahn": ("der", "зуб"), "hals": ("der", "горло/шея"), "kopf": ("der", "голова"),
    "bogen": ("der", "дуга"), "bruch": ("der", "перелом"), "riss": ("der", "разрыв"),
    "guss": ("der", "поток"), "nerv": ("der", "нерв"), "kanal": ("der", "канал"),
    "höhle": ("die", "полость/пещера"), "drüse": ("die", "железа"),
    "klappe": ("die", "клапан"), "säule": ("die", "столб"), "ader": ("die", "сосуд"),
    "bein": ("das", "нога/кость"), "gerät": ("das", "прибор"), "organ": ("das", "орган"),
    "gelenk": ("das", "сустав"), "gewebe": ("das", "ткань"), "fell": ("das", "плёнка/шкура"),
    "blut": ("das", "кровь"), "herz": ("das", "сердце"), "auge": ("das", "глаз"),
}


def _compound_head_tip(low: str, word: str, article: str) -> str | None:
    """If `word` ends in a known head noun of the SAME gender (and is longer than
    it), return a 'der Körper → der Wirbelkörper' style hint, else None."""
    for head, (art, gloss) in _HEAD_NOUNS.items():
        if art == article and len(low) > len(head) and low.endswith(head):
            Head = head.capitalize()
            return f"🧩 {article} {Head} ({gloss}) → значит {article} {word}. Род — по последнему слову."
    return None


def gender_tip(word: str, article: str) -> str:
    """A short Russian 'why' hint for this noun's gender. Deterministic + instant.
    Strong ending → confident rule (or 'rare exception' on mismatch); weak ending →
    tendency (only if it matches); otherwise a 'feel it' fallback."""
    w = str(word or "").strip()
    a = str(article or "").strip().lower()
    if not w or a not in ("der", "die", "das"):
        return ""
    low = w.lower()

    # Compound head noun — the strongest, most explanatory hint when it applies.
    head_tip = _compound_head_tip(low, w, a)
    if head_tip:
        return head_tip

    # Strong rules: confident, and flag true exceptions.
    for suf, expected, rule in _STRONG_SUFFIX_RULES:
        if low.endswith(suf):
            if a == expected:
                return f"✔️ {rule}."
            return f"⚠️ Обычно {rule}, но «{w}» — исключение: {a}."

    # Ge-… collective nouns lean neuter.
    if low.startswith("ge") and len(low) > 4 and a == "das":
        return "✔️ Ge-… → часто das (собирательное)."

    # Weak tendencies: only when they agree (don't teach an unreliable 'exception').
    for suf, expected, rule in _WEAK_SUFFIX_RULES:
        if low.endswith(suf) and a == expected:
            return f"💡 {rule}."

    # No reliable rule → lean on colour + sound + image (filled by later phases).
    return f"🎨 Запоминай образом и цветом: {_ART_RU[a]}. Простого правила по окончанию нет."


def _generate_and_cache_mnemonics(items: list[dict]) -> dict:
    """Synchronously generate + cache real LLM mnemonics for [{w,a,ru}] words that
    lack one. Returns {lower(word): mnemonic}. Best-effort (returns {} on failure)
    so the deck still renders, but the LLM hint is the intended path — the
    deterministic gender_tip is only a last resort when the LLM is unreachable."""
    if not items:
        return {}
    import asyncio
    from backend.openai_manager import run_article_mnemonics
    from backend.database import store_article_noun_mnemonic
    payload = [{"word": str(i.get("w") or ""), "article": str(i.get("a") or "").lower(),
                "ru": str(i.get("ru") or "")} for i in items if str(i.get("w") or "")]
    if not payload:
        return {}
    try:
        results = asyncio.run(asyncio.wait_for(
            run_article_mnemonics(items=payload), timeout=55))
    except Exception:
        logging.warning("learn mnemonic generation failed n=%s", len(payload), exc_info=True)
        return {}
    by_word = {str(i.get("w") or "").lower(): i for i in items}
    out: dict[str, str] = {}
    for r in results or []:
        word = str(r.get("word") or "").strip()
        src = by_word.get(word.lower())
        if not src:
            continue
        mn = str(r.get("mnemonic") or "").strip()
        method = str(r.get("method") or "").strip().lower()
        head = str(r.get("head") or "").strip()
        if not mn or len(mn) < 8:
            continue
        if method == "compound" and head and head.lower() not in word.lower():
            continue  # rejected false compound split
        try:
            store_article_noun_mnemonic(word=word, article=str(src.get("a") or "").lower(),
                                        mnemonic=mn, method=method, head=head)
        except Exception:
            pass
        out[word.lower()] = mn
    return out


def ensure_daily_learn_mnemonics(play_date) -> int:
    """Pre-warm: generate+cache mnemonics for today's daily-set words that lack one,
    so the trainer opens instantly. Returns how many were generated."""
    from backend.database import (
        get_daily_article_sprint_set_id, get_article_sprint_set,
        get_article_noun_mnemonics,
    )
    set_id = get_daily_article_sprint_set_id(play_date)
    if not set_id:
        return 0
    s = get_article_sprint_set(set_id)
    words = (s or {}).get("words") or []
    if not words:
        return 0
    have = get_article_noun_mnemonics([str(w.get("w") or "") for w in words])
    missing = [w for w in words if str(w.get("w") or "").lower() not in have]
    return len(_generate_and_cache_mnemonics(missing))


LEARN_NEW_SIZE = 15


def focus_new_words(play_date, theme_key: str, *, new_size: int = LEARN_NEW_SIZE,
                    offset: int = 0) -> list[dict]:
    """Deterministic daily slice of a focus theme (same for everyone on a date, so
    it's prewarmable). `offset` pages forward through the theme (wraps). [{w,a,ru}]."""
    from backend.database import count_article_theme_verified, get_article_theme_words_slice
    total = count_article_theme_verified(theme_key)
    if total <= 0:
        return []
    base = (play_date.toordinal() * new_size) % total
    start = (base + int(offset)) % total
    words = get_article_theme_words_slice(theme_key, start, new_size)
    if len(words) < min(new_size, total):  # wrap around the end of the theme
        seen = {w["w"].lower() for w in words}
        for w in get_article_theme_words_slice(theme_key, 0, new_size):
            if w["w"].lower() not in seen:
                words.append(w)
            if len(words) >= new_size:
                break
    return words


def build_learn_deck(play_date, user_id: int, *, new_size: int = LEARN_NEW_SIZE,
                     review_size: int = 8, offset: int = 0, pick_theme: str | None = None) -> dict:
    """Assemble the day's learning deck: up to `new_size` words from the daily
    Sprint set (so it's aligned with the game) + up to `review_size` of the user's
    past mistakes (resurfaced). Each card carries the gender tip + colour.

    Returns {ok, set_id, theme_key, theme_label, cards:[{w,a,ru,tip,color,review}]}
    or {ok: False, error_code, error} when the daily set isn't ready."""
    from backend.database import (
        get_daily_article_sprint_set_id, get_article_sprint_set,
        get_article_sprint_theme, get_article_learn_review_words,
        get_article_noun_mnemonics, get_article_learn_progress,
        count_article_theme_verified, get_article_noun_audio,
        get_article_noun_images, get_article_learn_focus,
        get_article_sprint_verified_sample, get_article_learn_streak,
    )
    from backend.r2_storage import r2_public_url

    # Source of NEW words, in priority order:
    #   1) pick_theme  — the theme the user just chose to learn NOW (ad-hoc);
    #   2) focus theme — a Pro user's personal focus for this date (prepped overnight);
    #   3) the shared daily Sprint set.
    new_size = max(1, int(new_size))
    offset = max(0, int(offset))
    pick_theme = (str(pick_theme).strip() or None) if pick_theme else None
    focus_theme = None
    if not pick_theme:
        try:
            focus_theme = get_article_learn_focus(int(user_id), play_date)
        except Exception:
            focus_theme = None

    theme_key = None
    new_words: list[dict] = []
    set_id = None
    pool_size = 0
    active_theme = pick_theme or focus_theme
    if active_theme:
        new_words = focus_new_words(play_date, active_theme, new_size=new_size, offset=offset)
        if new_words:
            theme_key = active_theme
            pool_size = count_article_theme_verified(active_theme)
            tag = "aln" if pick_theme else "alf"
            set_id = f"{tag}_{int(user_id)}_{active_theme}_{play_date.isoformat()}"
        else:
            active_theme = None  # empty theme → fall back to the shared set

    if not active_theme:
        set_id = get_daily_article_sprint_set_id(play_date)
        if not set_id:
            try:
                from backend.article_sprint_sets import build_daily_set
                built = build_daily_set(play_date)
                if built.get("status") == "ready":
                    set_id = built["set_id"]
            except Exception:
                set_id = None
        if not set_id:
            return {"ok": False, "error_code": "learn_set_not_ready",
                    "error": "Набор на сегодня ещё готовится. Загляни чуть позже."}
        s = get_article_sprint_set(set_id)
        if not s or not s.get("words"):
            return {"ok": False, "error_code": "learn_set_empty", "error": "Набор пуст."}
        theme_key = s["theme_key"]
        words_all = s["words"] or []
        pool_size = len(words_all)
        # Page through the set, wrapping — so the user can keep doing batches.
        start = offset % pool_size if pool_size else 0
        new_words = [words_all[(start + i) % pool_size] for i in range(min(new_size, pool_size))]

    theme = get_article_sprint_theme(theme_key) or {}
    theme_label = theme.get("label_de") or theme_key

    review_raw: list[dict] = []
    if offset == 0:  # only the first batch resurfaces past mistakes
        try:
            review_raw = get_article_learn_review_words(int(user_id), limit=int(review_size))
        except Exception:
            review_raw = []

    # Cached LLM mnemonics (preferred); deterministic gender_tip is the fallback.
    all_words = [str(w.get("w") or "") for w in (new_words + review_raw)]
    try:
        mnem = get_article_noun_mnemonics(all_words)
    except Exception:
        mnem = {}
    try:
        audio_keys = get_article_noun_audio(all_words)
    except Exception:
        audio_keys = {}
    try:
        image_keys = get_article_noun_images(all_words)
    except Exception:
        image_keys = {}

    # Real LLM mnemonics are the product — generate any missing ones now and cache
    # them, rather than showing a generic 'just memorize' fallback (precision matters
    # for a language app). Usually a no-op because the morning pre-warm filled them.
    missing = [w for w in (new_words + review_raw)
               if str(w.get("w") or "").lower() not in mnem]
    if missing:
        try:
            mnem.update(_generate_and_cache_mnemonics(missing))
        except Exception:
            logging.warning("build_learn_deck: lazy mnemonic fill failed", exc_info=True)

    def _card(w: dict, review: bool) -> dict:
        word = str(w.get("w") or "")
        art = str(w.get("a") or "").strip().lower()
        tip = mnem.get(word.lower()) or gender_tip(word, art)
        akey = audio_keys.get(word.lower())
        ikey = image_keys.get(word.lower())
        return {
            "w": word, "a": art, "ru": str(w.get("ru") or ""),
            "tip": tip,
            "color": GENDER_COLOR.get(art, "blue"),
            "review": review,
            "audio": r2_public_url(akey) if akey else "",
            "image": r2_public_url(ikey) if ikey else "",
        }

    new_cards = [_card(w, False) for w in new_words]
    seen = {c["w"].lower() for c in new_cards}
    review_cards = [_card(rw, True) for rw in review_raw
                    if str(rw.get("w") or "").lower() not in seen]

    # Theme progress for the done screen ("выучено X из Y").
    progress = {"mastered": 0, "theme_total": 0}
    try:
        prog = get_article_learn_progress(int(user_id), theme_key)
        progress = {"mastered": int(prog.get("mastered") or 0),
                    "theme_total": count_article_theme_verified(theme_key)}
    except Exception:
        pass

    try:
        streak = get_article_learn_streak(int(user_id))
    except Exception:
        streak = 0

    return {
        "ok": True, "set_id": set_id, "theme_key": theme_key,
        "theme_label": theme_label, "focus": bool(focus_theme),
        "cards": new_cards + review_cards,
        "new_count": len(new_cards), "review_count": len(review_cards),
        "progress": progress, "streak": streak,
        "next_offset": offset + len(new_cards),
        "has_more": bool(pool_size > new_size),
    }
