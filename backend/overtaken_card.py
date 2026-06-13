"""
"You were overtaken" plaque (PIL).

A card sent once when a faster correct answer pushes the user off the lead. The
*current place* lives on an inline button under the photo and is edited in place
as the user sinks further (2nd → 3rd → …), so we never spam a new message.

The visual: a random pre-generated Smurf-style "overtaken" background (podium /
race), with the overtaker's name + circular Telegram avatar composited on top,
the specific task label, and the title. If no background has been generated yet
(see /admin_overtaken_images), it falls back to a soft gradient + a sad blob.
"""
from __future__ import annotations

import io
import time

from backend.article_quiz_card import _font, _ctext, _vgrad, _glow, W, H

WHITE = (255, 255, 255)

# Pre-generated background object keys in R2 (filled by the admin generator).
_OVERTAKEN_BG_KEYS = [f"overtaken/smurf_{i}.png" for i in range(1, 4)]

# Curated gpt-image-1 prompts — brand mascot (cute blue smurf-like) + podium /
# race humour. NO text/letters (the card draws its own).
OVERTAKEN_IMAGE_PROMPTS = [
    ("A winners podium scene with two cute friendly blue smurf-like cartoon characters: "
     "the winner stands on the top step beaming and holding a shiny golden trophy; the "
     "second character on the lower step looks up with comic, exaggerated envy and a pout. "
     "Soft 3D Pixar-like render, vibrant playful colors, clean simple background, no text, "
     "no letters, no numbers, centered composition."),
    ("Two cute friendly blue smurf-like cartoon runners at a race finish line: one bursts "
     "triumphantly through a red finish ribbon with arms up, the other is just half a step "
     "behind, panting and comically surprised. Dynamic motion, humorous, soft 3D Pixar-like "
     "render, vibrant colors, clean background, no text, no letters, no numbers."),
    ("A funny cartoon snail race: one cute friendly blue smurf-like character rides a fast "
     "rocket-snail zooming ahead with a confident grin, while another smurf-like character "
     "on a slow snail watches in shocked dismay. Soft 3D Pixar-like render, vibrant playful "
     "colors, clean background, no text, no letters, no numbers."),
]

# In-process cache of fetched backgrounds (TTL so newly generated ones appear).
_bg_cache: dict = {"t": 0.0, "imgs": []}
_BG_CACHE_TTL = 600.0


def overtaken_bg_keys() -> list[str]:
    return list(_OVERTAKEN_BG_KEYS)


def pick_overtaken_background() -> bytes | None:
    """Random pre-generated Smurf background bytes from R2 (cached in-proc, TTL).
    Returns None if none generated yet → renderer uses the gradient fallback."""
    import random
    from backend.r2_storage import r2_get_bytes
    now = time.time()
    if now - float(_bg_cache.get("t") or 0.0) > _BG_CACHE_TTL or not _bg_cache.get("imgs"):
        found: list[bytes] = []
        for k in _OVERTAKEN_BG_KEYS:
            try:
                b = r2_get_bytes(k)
                if b:
                    found.append(bytes(b))
            except Exception:
                pass
        _bg_cache["imgs"] = found
        _bg_cache["t"] = now
    imgs = _bg_cache.get("imgs") or []
    return random.choice(imgs) if imgs else None


def _fit_font(draw, text, max_w, start, bold=True, floor=34):
    size = start
    while size > floor:
        f = _font(size, bold)
        if draw.textlength(text, font=f) <= max_w:
            return f
        size -= 4
    return _font(floor, bold)


def _circle_avatar(avatar_bytes: bytes, size: int):
    """Crop avatar bytes into a circular RGBA image of (size, size)."""
    from PIL import Image, ImageDraw, ImageOps
    try:
        im = Image.open(io.BytesIO(avatar_bytes)).convert("RGBA")
    except Exception:
        return None
    im = ImageOps.fit(im, (size, size), Image.LANCZOS)
    mask = Image.new("L", (size, size), 0)
    ImageDraw.Draw(mask).ellipse([0, 0, size - 1, size - 1], fill=255)
    im.putalpha(mask)
    return im


def _sad_character(d, cx, cy, r):
    """A cute slumped blob with downcast eyes, sad brows and a tear (fallback)."""
    body = (150, 170, 235)
    dark = (40, 48, 80)
    d.ellipse([cx - r, cy - r, cx + r, cy + r], fill=body)
    d.ellipse([cx - r, cy - r, cx + r, cy + r], outline=(255, 255, 255, 60), width=4)
    eye_dx, eye_dy = int(r * 0.42), int(r * 0.12)
    ew, eh = int(r * 0.20), int(r * 0.26)
    for sx in (-1, 1):
        ex = cx + sx * eye_dx
        d.ellipse([ex - ew, cy - eye_dy - eh, ex + ew, cy - eye_dy + eh], fill=WHITE)
        pr = int(ew * 0.62)
        py = cy - eye_dy + eh - pr - 2
        d.ellipse([ex - pr, py - pr, ex + pr, py + pr], fill=dark)
        bx_in, by_in = cx + sx * int(r * 0.18), cy - eye_dy - eh - int(r * 0.18)
        bx_out, by_out = cx + sx * int(r * 0.66), cy - eye_dy - eh - int(r * 0.02)
        d.line([(bx_in, by_in), (bx_out, by_out)], fill=dark, width=max(5, r // 22))
    mw, my = int(r * 0.40), cy + int(r * 0.50)
    d.arc([cx - mw, my, cx + mw, my + int(r * 0.55)], start=180, end=360, fill=dark, width=max(6, r // 18))
    tx, ty = cx - eye_dx + int(r * 0.04), cy - eye_dy + eh + int(r * 0.10)
    tr = max(7, r // 16)
    d.polygon([(tx, ty - tr * 2), (tx - tr, ty), (tx + tr, ty)], fill=(120, 200, 250))
    d.ellipse([tx - tr, ty - tr, tx + tr, ty + tr], fill=(120, 200, 250))


def render_overtaken_card(
    label: str,
    *,
    overtaker_name: str = "",
    avatar_bytes: bytes | None = None,
    background_bytes: bytes | None = None,
) -> bytes:
    """Render the 'you were overtaken' plaque. Returns PNG bytes."""
    from PIL import Image, ImageDraw, ImageOps

    label = (label or "задании").strip()
    name = (overtaker_name or "").strip()

    if background_bytes:
        try:
            base = ImageOps.fit(
                Image.open(io.BytesIO(background_bytes)).convert("RGBA"), (W, H), Image.LANCZOS
            )
        except Exception:
            base = _vgrad((73, 84, 140), (40, 46, 78)).convert("RGBA")
        # Darken the top and bottom bands so the white text stays legible.
        ov = Image.new("RGBA", (W, H), (0, 0, 0, 0))
        od = ImageDraw.Draw(ov)
        for y in range(H):
            a = 0
            if y < 330:
                a = int(160 * (1 - y / 330))
            elif y > 600:
                a = int(175 * ((y - 600) / (H - 600)))
            if a:
                od.line([(0, y), (W, y)], fill=(15, 17, 35, a))
        base = Image.alpha_composite(base, ov)
        has_bg = True
    else:
        base = _vgrad((73, 84, 140), (40, 46, 78)).convert("RGBA")
        _glow(base, W // 2, 470, 460, 38)
        has_bg = False

    d = ImageDraw.Draw(base)

    # ── title pill ───────────────────────────────────────────────────────────
    title = "ТЕБЯ ОБОШЛИ"
    tf = _font(46, True)
    tw = d.textlength(title, font=tf)
    ov = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    ImageDraw.Draw(ov).rounded_rectangle(
        [W // 2 - tw // 2 - 44, 96, W // 2 + tw // 2 + 44, 184], radius=44, fill=(0, 0, 0, 95),
    )
    base.paste(ov, (0, 0), ov)
    _ctext(d, W // 2, 112, title, tf, (255, 255, 255, 240))

    # ── overtaker avatar (or sad blob fallback) ──────────────────────────────
    av = _circle_avatar(avatar_bytes, 240) if avatar_bytes else None
    if av:
        ax, ay = W // 2 - 120, 280
        ImageDraw.Draw(base).ellipse(
            [ax - 8, ay - 8, ax + 248, ay + 248], outline=(255, 255, 255, 230), width=8
        )
        base.paste(av, (ax, ay), av)
    elif not has_bg:
        _sad_character(d, W // 2, 440, 150)

    # ── overtaker name ───────────────────────────────────────────────────────
    if name:
        nf = _fit_font(d, f"Тебя обошёл {name}", W - 160, 52, bold=True, floor=32)
        _ctext(d, W // 2, 600, f"Тебя обошёл {name}", nf, (255, 255, 255, 245))

    # ── challenge label + subtitle ───────────────────────────────────────────
    lf = _fit_font(d, f"«{label}»", W - 170, 54, bold=True, floor=32)
    _ctext(d, W // 2, 700, f"«{label}»", lf, WHITE)
    subtitle = "ответил(а) быстрее тебя" if name else "Кто-то ответил быстрее тебя"
    _ctext(d, W // 2, 778, subtitle, _font(36, False), (225, 230, 255, 230))
    _ctext(d, W // 2, 1010, "Deutsche Sprache · Рейтинг", _font(30, False), (205, 212, 242, 160))

    out = io.BytesIO()
    base.convert("RGB").save(out, format="PNG", optimize=True)
    return out.getvalue()
