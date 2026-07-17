"""
"Lazy day" plaque (PIL).

Sent instead of the boring text report when NOBODY translated anything today:
a pre-generated Fox-as-a-sloth-on-the-couch scene (TV + chips) with a playful
headline drawn on top. The list of slackers goes in the photo CAPTION
(dynamic, readable, no overflow) — not drawn on the image.

The background is generated once via gpt-image-1 → R2 (see /admin_lazy_image).
If it isn't there yet, a soft gradient fallback is used.
"""
from __future__ import annotations

import io
import time

from backend.article_quiz_card import _font, _ctext, _vgrad, _glow, W, H
from backend.mascot_style import mascot_prompt

WHITE = (255, 255, 255)

_LAZY_BG_KEY = "lazy/fox_sloth.png"

# gpt-image-1 prompt — brand mascot (Fox, see backend/mascot_style.py) as a
# couch-potato sloth. NO text/letters (the card draws its own headline).
LAZY_IMAGE_PROMPT = mascot_prompt(
    "A Fox mascot lazing like a sloth, sprawled lazily on a cozy couch at night in "
    "front of a glowing TV, one hand in a big bag of potato chips, droopy sleepy "
    "half-closed eyes and a goofy content smile, slippers on, a blanket half over "
    "him, warm cozy night lighting. Humorous couch-potato vibe"
)

_ACTIVE_BG_KEY = "lazy/fox_active.png"

# Celebratory counterpart: shown when someone DID complete the task today.
ACTIVE_IMAGE_PROMPT = mascot_prompt(
    "A Fox mascot sitting proudly and energetically at a tidy desk with an open book "
    "and a laptop, a shiny golden star trophy beside him, colorful confetti in the "
    "air, a big proud confident smile and one thumb up, bright motivated eyes — a "
    "'great job, work done' celebration vibe, bright cheerful lighting"
)

_bg_cache: dict = {"lazy": {"t": 0.0, "img": None}, "active": {"t": 0.0, "img": None}}
_BG_CACHE_TTL = 600.0


def lazy_bg_key() -> str:
    return _LAZY_BG_KEY


def active_bg_key() -> str:
    return _ACTIVE_BG_KEY


def _pick_bg(slot: str, key: str) -> bytes | None:
    from backend.r2_storage import r2_get_bytes
    now = time.time()
    cache = _bg_cache.setdefault(slot, {"t": 0.0, "img": None})
    if now - float(cache.get("t") or 0.0) > _BG_CACHE_TTL or not cache.get("img"):
        img = None
        try:
            b = r2_get_bytes(key)
            if b:
                img = bytes(b)
        except Exception:
            img = None
        cache["img"] = img
        cache["t"] = now
    return cache.get("img")


def pick_lazy_background() -> bytes | None:
    """Pre-generated lazy-Smurf background from R2 (cached). None → gradient fallback."""
    return _pick_bg("lazy", _LAZY_BG_KEY)


def pick_active_background() -> bytes | None:
    """Pre-generated celebratory-Smurf background from R2 (cached). None → fallback."""
    return _pick_bg("active", _ACTIVE_BG_KEY)


def _strip_emoji(s: str) -> str:
    """Server font has no color-emoji glyphs → strip emoji so drawn text never shows
    tofu boxes (emoji stay in the Telegram caption)."""
    import re as _re
    s = _re.sub(r"[\U0001F000-\U0001FAFF☀-➿←-⇿️‍]", "", str(s or ""))
    return _re.sub(r"\s+", " ", s).strip()


def _render_day_card(*, title: str, subtitle: str, background_bytes: bytes | None,
                     grad_top, grad_bot) -> bytes:
    """Shared 'day report' plaque renderer: bg (or gradient fallback) + title pill +
    subtitle. Dynamic name lists go in the photo caption, never drawn here."""
    from PIL import Image, ImageDraw, ImageOps
    title = _strip_emoji(title) or "Итоги"
    subtitle = _strip_emoji(subtitle)

    if background_bytes:
        try:
            base = ImageOps.fit(
                Image.open(io.BytesIO(background_bytes)).convert("RGBA"), (W, H), Image.LANCZOS
            )
        except Exception:
            base = _vgrad(grad_top, grad_bot).convert("RGBA")
        ov = Image.new("RGBA", (W, H), (0, 0, 0, 0))
        od = ImageDraw.Draw(ov)
        for y in range(H):
            a = 0
            if y < 330:
                a = int(165 * (1 - y / 330))
            elif y > 620:
                a = int(180 * ((y - 620) / (H - 620)))
            if a:
                od.line([(0, y), (W, y)], fill=(15, 12, 8, a))
        base = Image.alpha_composite(base, ov)
    else:
        base = _vgrad(grad_top, grad_bot).convert("RGBA")
        _glow(base, W // 2, 470, 460, 36)

    d = ImageDraw.Draw(base)
    tf = _font(48, True)
    tw = d.textlength(title, font=tf)
    ov = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    ImageDraw.Draw(ov).rounded_rectangle(
        [W // 2 - tw // 2 - 46, 96, W // 2 + tw // 2 + 46, 190], radius=46, fill=(0, 0, 0, 110),
    )
    base.paste(ov, (0, 0), ov)
    _ctext(d, W // 2, 114, title, tf, (255, 255, 255, 240))

    sf = _font(44, True)
    if d.textlength(subtitle, font=sf) > W - 150:
        sf = _font(36, True)
    _ctext(d, W // 2, 880, subtitle, sf, WHITE)
    _ctext(d, W // 2, 1010, "Deutsche Sprache · Сегодня", _font(30, False), (235, 224, 205, 170))

    out = io.BytesIO()
    base.convert("RGB").save(out, format="PNG", optimize=True)
    return out.getvalue()


def render_lazy_day_card(*, title: str = "ДЕНЬ ЛЕНИ",
                         subtitle: str = "Сегодня задание никто не сделал",
                         background_bytes: bytes | None = None) -> bytes:
    """'Lazy day' plaque (nobody did the task). Names go in the caption."""
    return _render_day_card(title=title, subtitle=subtitle, background_bytes=background_bytes,
                            grad_top=(120, 96, 60), grad_bot=(52, 40, 28))


def render_active_day_card(*, title: str = "МОЛОДЦЫ!",
                           subtitle: str = "Сегодня поработали!",
                           background_bytes: bytes | None = None) -> bytes:
    """Celebratory plaque (someone completed the task). Per-user stats go in the caption."""
    return _render_day_card(title=title, subtitle=subtitle, background_bytes=background_bytes,
                            grad_top=(56, 104, 72), grad_bot=(20, 46, 36))
