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


def build_learn_deck(play_date, user_id: int, *, new_size: int = 15,
                     review_size: int = 8) -> dict:
    """Assemble the day's learning deck: up to `new_size` words from the daily
    Sprint set (so it's aligned with the game) + up to `review_size` of the user's
    past mistakes (resurfaced). Each card carries the gender tip + colour.

    Returns {ok, set_id, theme_key, theme_label, cards:[{w,a,ru,tip,color,review}]}
    or {ok: False, error_code, error} when the daily set isn't ready."""
    from backend.database import (
        get_daily_article_sprint_set_id, get_article_sprint_set,
        get_article_sprint_theme, get_article_learn_review_words,
        get_article_noun_mnemonics,
    )

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

    theme = get_article_sprint_theme(s["theme_key"]) or {}
    theme_label = theme.get("label_de") or s["theme_key"]

    new_words = (s["words"] or [])[: max(1, int(new_size))]
    review_raw: list[dict] = []
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
        return {
            "w": word, "a": art, "ru": str(w.get("ru") or ""),
            "tip": tip,
            "color": GENDER_COLOR.get(art, "blue"),
            "review": review,
        }

    new_cards = [_card(w, False) for w in new_words]
    seen = {c["w"].lower() for c in new_cards}
    review_cards = [_card(rw, True) for rw in review_raw
                    if str(rw.get("w") or "").lower() not in seen]

    return {
        "ok": True, "set_id": set_id, "theme_key": s["theme_key"],
        "theme_label": theme_label,
        "cards": new_cards + review_cards,
        "new_count": len(new_cards), "review_count": len(review_cards),
    }
