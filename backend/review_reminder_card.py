"""
Review-reminder hero card (PIL).

Replaces the flat "Пора повторить слова" text nudge with a branded card whose
HERO element is the number of SRS cards due for THIS user, plus their streak and
a short motivating line. Reuses the shared brand kit (article_quiz_card
primitives) and the proud-Smurf study background (lazy_day_card active
background) — no new assets.

The caller only sends this to users who actually have cards due, so the number is
always ≥ 1 and the nudge is always actionable (never "0 words to review").
"""
from __future__ import annotations

import io
import random
import time

from backend.article_quiz_card import _font, _ctext, _fit_font, _vgrad, _glow, W, H
from backend.lazy_day_card import _strip_emoji
from backend.mascot_style import mascot_prompt

WHITE = (255, 255, 255)

# Dedicated review-reminder background (its own art, distinct from the day-report
# "active" Smurf). Generated once via gpt-image-1 → R2 (see /admin_review_image);
# a green study gradient is used until it's there.
_REVIEW_BG_KEY = "review/fox_review.png"

# gpt-image-1 prompt — brand mascot (Fox, see backend/mascot_style.py) happily
# reviewing vocabulary flashcards. NO text/letters (the card draws its own). Green
# study palette to sit under the card's green scrim; empty upper area for headline.
REVIEW_IMAGE_PROMPT = mascot_prompt(
    "A Fox mascot in a cozy study nook, cheerfully reviewing a fan of vocabulary "
    "flashcards in his paws, a neat stack of word cards and a steaming cup of tea on "
    "the desk beside him, soft glowing sparkles of memory and a warm lightbulb above "
    "his head, a motivated confident smile and bright focused eyes, warm green-toned "
    "study lighting, plenty of empty space in the upper half. Encouraging 'time to "
    "review your words' mood"
)

_bg_cache: dict = {"t": 0.0, "img": None}
_BG_CACHE_TTL = 600.0


def review_bg_key() -> str:
    return _REVIEW_BG_KEY


def pick_review_background() -> bytes | None:
    """Pre-generated review-Smurf background from R2 (cached ~10 min). None → the
    green gradient fallback is drawn instead."""
    from backend.r2_storage import r2_get_bytes
    now = time.time()
    if now - float(_bg_cache.get("t") or 0.0) > _BG_CACHE_TTL or not _bg_cache.get("img"):
        img = None
        try:
            b = r2_get_bytes(_REVIEW_BG_KEY)
            if b:
                img = bytes(b)
        except Exception:
            img = None
        _bg_cache["img"] = img
        _bg_cache["t"] = now
    return _bg_cache.get("img")

# Rotated so the daily card never reads identically two days running.
_MOTIVATION = [
    "5 минут сейчас — и слова закрепятся",
    "Повтори, пока не забыл — это быстро",
    "Маленький шаг сегодня — большой результат",
    "Слова ещё свежи — забери их, пока помнишь",
    "Один подход — и серия продолжится",
]


def plural_words_ru(n: int) -> str:
    """Russian plural for 'слово' agreeing with n (слово / слова / слов)."""
    n = abs(int(n))
    if 11 <= n % 100 <= 14:
        return "слов"
    d = n % 10
    if d == 1:
        return "слово"
    if 2 <= d <= 4:
        return "слова"
    return "слов"


def _plural_days_ru(n: int) -> str:
    n = abs(int(n))
    if 11 <= n % 100 <= 14:
        return "дней"
    d = n % 10
    if d == 1:
        return "день"
    if 2 <= d <= 4:
        return "дня"
    return "дней"


def render_review_reminder_card(*, due_count: int, streak_days: int = 0,
                                motivation: str | None = None) -> bytes:
    """Branded personal SRS-review card. `due_count` is the hero number; `streak_days`
    (0 → 'start a streak') and one motivating line sit below it. Returns PNG bytes."""
    from PIL import Image, ImageDraw, ImageOps

    due_count = max(1, int(due_count or 0))
    streak_days = max(0, int(streak_days or 0))
    motivation = _strip_emoji(motivation or random.choice(_MOTIVATION))

    # Background: dedicated review-Smurf scene; green study gradient fallback when
    # the R2 asset isn't there yet (before /admin_review_image is run).
    bg = None
    try:
        bg = pick_review_background()
    except Exception:
        bg = None
    if bg:
        try:
            base = ImageOps.fit(
                Image.open(io.BytesIO(bg)).convert("RGBA"), (W, H), Image.LANCZOS
            )
        except Exception:
            base = _vgrad((56, 104, 72), (20, 46, 36)).convert("RGBA")
    else:
        base = _vgrad((56, 104, 72), (20, 46, 36)).convert("RGBA")

    # Legibility scrim: darken (stronger toward the bottom, where the text lives)
    # so white type always reads over the mascot.
    ov = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    od = ImageDraw.Draw(ov)
    for y in range(H):
        a = 90
        if y > 430:
            a = int(90 + 120 * min(1.0, (y - 430) / (H - 430)))
        od.line([(0, y), (W, y)], fill=(10, 22, 14, a))
    base = Image.alpha_composite(base, ov)

    d = ImageDraw.Draw(base)

    # Top pill — headline
    pill = "ПОРА ПОВТОРИТЬ"
    pf = _font(46, True)
    pw = d.textlength(pill, font=pf)
    ov = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    ImageDraw.Draw(ov).rounded_rectangle(
        [W // 2 - pw // 2 - 44, 92, W // 2 + pw // 2 + 44, 182], radius=45, fill=(0, 0, 0, 120),
    )
    base.paste(ov, (0, 0), ov)
    _ctext(d, W // 2, 108, pill, pf, (255, 255, 255, 240))

    # Hero number — the due count, huge, with a soft glow + drop shadow.
    _glow(base, W // 2, 560, 300, 60)
    num = str(due_count)
    nf = _font(300, True)
    nbb = d.textbbox((0, 0), num, font=nf)
    ny = 560 - (nbb[3] - nbb[1]) // 2 - nbb[1]
    _ctext(d, W // 2 + 5, ny + 6, num, nf, (0, 0, 0, 110))  # shadow
    _ctext(d, W // 2, ny, num, nf, WHITE)

    # Label under the number
    label = f"{plural_words_ru(due_count)} ждут тебя"
    _ctext(d, W // 2, 748, label, _font(52, True), (255, 255, 255, 235))

    # Streak line (warm amber) — motivate by "don't break the streak".
    if streak_days > 0:
        streak_txt = f"{streak_days} {_plural_days_ru(streak_days)} подряд — не прерывай серию"
    else:
        streak_txt = "Начни серию уже сегодня"
    sf = _fit_font(d, streak_txt, W - 140, 40, bold=True, floor=28)
    _ctext(d, W // 2, 840, streak_txt, sf, (255, 224, 130, 235))

    # Motivating line
    mf = _fit_font(d, motivation, W - 160, 40, bold=False, floor=26)
    _ctext(d, W // 2, 922, motivation, mf, (235, 245, 238, 220))

    _ctext(d, W // 2, 1012, "Deutsche Sprache · Карточки", _font(30, False), (220, 235, 226, 170))

    out = io.BytesIO()
    base.convert("RGB").save(out, format="PNG", optimize=True)
    return out.getvalue()
