"""
"Lazy day" plaque (PIL).

Sent instead of the boring text report when NOBODY translated anything today:
a pre-generated Smurf-as-a-sloth-on-the-couch scene (TV + chips + beer) with a
playful headline drawn on top. The list of slackers goes in the photo CAPTION
(dynamic, readable, no overflow) — not drawn on the image.

The background is generated once via gpt-image-1 → R2 (see /admin_lazy_image).
If it isn't there yet, a soft gradient fallback is used.
"""
from __future__ import annotations

import io
import time

from backend.article_quiz_card import _font, _ctext, _vgrad, _glow, W, H

WHITE = (255, 255, 255)

_LAZY_BG_KEY = "lazy/smurf_sloth.png"

# gpt-image-1 prompt — brand mascot (cute blue smurf-like) as a couch-potato sloth.
# NO text/letters (the card draws its own headline).
LAZY_IMAGE_PROMPT = (
    "A cute friendly blue smurf-like cartoon character lazing like a sloth, sprawled "
    "lazily on a cozy couch in front of a glowing TV at night, one hand in a big bag of "
    "potato chips and a can of beer on the side table, droopy sleepy half-closed eyes and "
    "a goofy content smile, slippers on, blanket half over him. Humorous couch-potato vibe. "
    "Soft 3D Pixar-like render, warm cozy lighting, vibrant playful colors, clean simple "
    "background, no text, no letters, no numbers, centered composition."
)

_bg_cache: dict = {"t": 0.0, "img": None}
_BG_CACHE_TTL = 600.0


def lazy_bg_key() -> str:
    return _LAZY_BG_KEY


def pick_lazy_background() -> bytes | None:
    """The pre-generated lazy-Smurf background bytes from R2 (cached in-proc, TTL).
    Returns None if not generated yet → renderer uses the gradient fallback."""
    from backend.r2_storage import r2_get_bytes
    now = time.time()
    if now - float(_bg_cache.get("t") or 0.0) > _BG_CACHE_TTL or not _bg_cache.get("img"):
        img = None
        try:
            b = r2_get_bytes(_LAZY_BG_KEY)
            if b:
                img = bytes(b)
        except Exception:
            img = None
        _bg_cache["img"] = img
        _bg_cache["t"] = now
    return _bg_cache.get("img")


def render_lazy_day_card(
    *,
    title: str = "ДЕНЬ ЛЕНИ",
    subtitle: str = "Сегодня задание никто не сделал",
    background_bytes: bytes | None = None,
) -> bytes:
    """Render the 'lazy day' plaque. Returns PNG bytes. Names go in the caption."""
    from PIL import Image, ImageDraw, ImageOps

    if background_bytes:
        try:
            base = ImageOps.fit(
                Image.open(io.BytesIO(background_bytes)).convert("RGBA"), (W, H), Image.LANCZOS
            )
        except Exception:
            base = _vgrad((120, 96, 60), (52, 40, 28)).convert("RGBA")
        # Darken the top + bottom bands so the white text stays legible.
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
        base = _vgrad((120, 96, 60), (52, 40, 28)).convert("RGBA")
        _glow(base, W // 2, 470, 460, 36)

    d = ImageDraw.Draw(base)

    # ── title pill ───────────────────────────────────────────────────────────
    tf = _font(48, True)
    tw = d.textlength(title, font=tf)
    ov = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    ImageDraw.Draw(ov).rounded_rectangle(
        [W // 2 - tw // 2 - 46, 96, W // 2 + tw // 2 + 46, 190], radius=46, fill=(0, 0, 0, 110),
    )
    base.paste(ov, (0, 0), ov)
    _ctext(d, W // 2, 114, title, tf, (255, 255, 255, 240))

    # ── subtitle near the bottom ─────────────────────────────────────────────
    sf = _font(44, True)
    # wrap subtitle to <= W-160 if needed
    if d.textlength(subtitle, font=sf) > W - 150:
        sf = _font(36, True)
    _ctext(d, W // 2, 880, subtitle, sf, WHITE)
    _ctext(d, W // 2, 1010, "Deutsche Sprache · Сегодня", _font(30, False), (235, 224, 205, 170))

    out = io.BytesIO()
    base.convert("RGB").save(out, format="PNG", optimize=True)
    return out.getvalue()
