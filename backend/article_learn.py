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


def gender_tip(word: str, article: str) -> str:
    """A short Russian 'why' hint for this noun's gender. Deterministic + instant.
    Strong ending → confident rule (or 'rare exception' on mismatch); weak ending →
    tendency (only if it matches); otherwise a 'feel it' fallback."""
    w = str(word or "").strip()
    a = str(article or "").strip().lower()
    if not w or a not in ("der", "die", "das"):
        return ""
    low = w.lower()

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

    def _card(w: dict, review: bool) -> dict:
        word = str(w.get("w") or "")
        art = str(w.get("a") or "").strip().lower()
        return {
            "w": word, "a": art, "ru": str(w.get("ru") or ""),
            "tip": gender_tip(word, art),
            "color": GENDER_COLOR.get(art, "blue"),
            "review": review,
        }

    new_cards = [_card(w, False) for w in (s["words"] or [])[: max(1, int(new_size))]]
    seen = {c["w"].lower() for c in new_cards}

    review_cards: list[dict] = []
    try:
        for rw in get_article_learn_review_words(int(user_id), limit=int(review_size)):
            if str(rw.get("w") or "").lower() in seen:
                continue
            review_cards.append(_card(rw, True))
    except Exception:
        review_cards = []

    return {
        "ok": True, "set_id": set_id, "theme_key": s["theme_key"],
        "theme_label": theme_label,
        "cards": new_cards + review_cards,
        "new_count": len(new_cards), "review_count": len(review_cards),
    }
