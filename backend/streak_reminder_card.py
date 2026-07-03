"""
Streak re-engagement hero card (PIL).

The HERO element is the user's current streak (days in a row), with a warm
"don't let it burn out" framing and a short motivating line. Sent once/day (17:30)
under the "загляни в приложение" nudge, alongside the «Открыть приложение» button.

Reuses the shared brand kit (article_quiz_card primitives) and the proud-Smurf
"active" background (lazy_day_card) — no new assets. A warm amber gradient is the
fallback when the R2 background isn't present.

The caller only sends this card to users whose streak is ≥ 1, so the hero number
is always meaningful (never a sad "0"). Streak-0 users get a plain-text invite.
"""
from __future__ import annotations

import io
import random

from backend.article_quiz_card import _font, _ctext, _fit_font, _vgrad, _glow, W, H
from backend.lazy_day_card import _strip_emoji, pick_active_background

WHITE = (255, 255, 255)

# Rotated so the daily card never reads identically two days running.
_MOTIVATION = [
    "Не дай серии сгореть — вернись сегодня",
    "5 минут сейчас — и серия продолжится",
    "Один подход — и ты снова в деле",
    "Держи ритм — ты отлично идёшь",
    "Продли серию, пока она горит",
]


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


def render_streak_reminder_card(*, streak_days: int, motivation: str | None = None) -> bytes:
    """Branded personal streak card. `streak_days` is the hero number; one motivating
    line sits below it. Returns PNG bytes. Intended for streak_days ≥ 1."""
    from PIL import Image, ImageDraw, ImageOps

    streak_days = max(1, int(streak_days or 0))
    motivation = _strip_emoji(motivation or random.choice(_MOTIVATION))

    # Background: proud-Smurf "active" scene from R2; warm amber gradient fallback
    # when the asset isn't there.
    bg = None
    try:
        bg = pick_active_background()
    except Exception:
        bg = None
    if bg:
        try:
            base = ImageOps.fit(
                Image.open(io.BytesIO(bg)).convert("RGBA"), (W, H), Image.LANCZOS
            )
        except Exception:
            base = _vgrad((150, 92, 34), (46, 22, 10)).convert("RGBA")
    else:
        base = _vgrad((150, 92, 34), (46, 22, 10)).convert("RGBA")

    # Legibility scrim: darken (stronger toward the bottom, where the text lives).
    ov = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    od = ImageDraw.Draw(ov)
    for y in range(H):
        a = 90
        if y > 430:
            a = int(90 + 120 * min(1.0, (y - 430) / (H - 430)))
        od.line([(0, y), (W, y)], fill=(24, 12, 4, a))
    base = Image.alpha_composite(base, ov)

    d = ImageDraw.Draw(base)

    # Top pill — headline (no emoji in drawn text; the caption carries the 🔥).
    pill = "ТВОЯ СЕРИЯ"
    pf = _font(46, True)
    pw = d.textlength(pill, font=pf)
    ov = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    ImageDraw.Draw(ov).rounded_rectangle(
        [W // 2 - pw // 2 - 44, 92, W // 2 + pw // 2 + 44, 182], radius=45, fill=(0, 0, 0, 120),
    )
    base.paste(ov, (0, 0), ov)
    _ctext(d, W // 2, 108, pill, pf, (255, 255, 255, 240))

    # Hero number — the streak, huge, with a soft glow + drop shadow.
    _glow(base, W // 2, 560, 300, 60)
    num = str(streak_days)
    nf = _font(300, True)
    nbb = d.textbbox((0, 0), num, font=nf)
    ny = 560 - (nbb[3] - nbb[1]) // 2 - nbb[1]
    _ctext(d, W // 2 + 5, ny + 6, num, nf, (0, 0, 0, 110))  # shadow
    _ctext(d, W // 2, ny, num, nf, WHITE)

    # Label under the number
    label = f"{_plural_days_ru(streak_days)} подряд"
    _ctext(d, W // 2, 748, label, _font(52, True), (255, 255, 255, 235))

    # Warm amber "don't break it" line
    burn_txt = "Не прерывай — ты в огне"
    bf = _fit_font(d, burn_txt, W - 140, 40, bold=True, floor=28)
    _ctext(d, W // 2, 840, burn_txt, bf, (255, 200, 110, 240))

    # Motivating line
    mf = _fit_font(d, motivation, W - 160, 40, bold=False, floor=26)
    _ctext(d, W // 2, 922, motivation, mf, (245, 238, 232, 220))

    _ctext(d, W // 2, 1012, "Deutsche Sprache · Streak", _font(30, False), (235, 226, 220, 170))

    out = io.BytesIO()
    base.convert("RGB").save(out, format="PNG", optimize=True)
    return out.getvalue()
