"""
"You were overtaken" plaque (PIL).

A soft, somber card with a cute dejected character, sent once when a faster
correct answer pushes the user off the lead. The *current place* lives on an
inline button under the photo and is edited in place as the user sinks further
(2nd → 3rd → …), so we never spam a new message. The image itself only shows
the challenge label, so it stays valid across edits.
"""
from __future__ import annotations

import io

from backend.article_quiz_card import _font, _ctext, _vgrad, _glow, W, H

WHITE = (255, 255, 255)


def _fit_font(draw, text, max_w, start, bold=True, floor=34):
    size = start
    while size > floor:
        f = _font(size, bold)
        if draw.textlength(text, font=f) <= max_w:
            return f
        size -= 4
    return _font(floor, bold)


def _sad_character(d, cx, cy, r):
    """A cute slumped blob with downcast eyes, sad brows and a tear."""
    body = (150, 170, 235)        # calm periwinkle
    dark = (40, 48, 80)
    # head / body blob
    d.ellipse([cx - r, cy - r, cx + r, cy + r], fill=body)
    d.ellipse([cx - r, cy - r, cx + r, cy + r], outline=(255, 255, 255, 60), width=4)

    eye_dx, eye_dy = int(r * 0.42), int(r * 0.12)
    ew, eh = int(r * 0.20), int(r * 0.26)
    for sx in (-1, 1):
        ex = cx + sx * eye_dx
        # eye white
        d.ellipse([ex - ew, cy - eye_dy - eh, ex + ew, cy - eye_dy + eh], fill=WHITE)
        # pupil sits low → downcast / sad gaze
        pr = int(ew * 0.62)
        py = cy - eye_dy + eh - pr - 2
        d.ellipse([ex - pr, py - pr, ex + pr, py + pr], fill=dark)
        # sad eyebrow: inner end raised, outer end drooped
        bx_in, by_in = cx + sx * int(r * 0.18), cy - eye_dy - eh - int(r * 0.18)
        bx_out, by_out = cx + sx * int(r * 0.66), cy - eye_dy - eh - int(r * 0.02)
        d.line([(bx_in, by_in), (bx_out, by_out)], fill=dark, width=max(5, r // 22))

    # frown — corners turned down (top half of a circle)
    mw, my = int(r * 0.40), cy + int(r * 0.50)
    d.arc([cx - mw, my, cx + mw, my + int(r * 0.55)], start=180, end=360, fill=dark, width=max(6, r // 18))

    # a single tear under the left eye
    tx, ty = cx - eye_dx + int(r * 0.04), cy - eye_dy + eh + int(r * 0.10)
    tr = max(7, r // 16)
    d.polygon([(tx, ty - tr * 2), (tx - tr, ty), (tx + tr, ty)], fill=(120, 200, 250))
    d.ellipse([tx - tr, ty - tr, tx + tr, ty + tr], fill=(120, 200, 250))


def render_overtaken_card(label: str) -> bytes:
    """Render the somber plaque. Returns PNG bytes."""
    from PIL import ImageDraw

    label = (label or "задании").strip()

    # muted indigo → slate gradient (somber but still stylish)
    base = _vgrad((73, 84, 140), (40, 46, 78)).convert("RGBA")
    _glow(base, W // 2, 470, 460, 38)
    d = ImageDraw.Draw(base)

    # title pill
    title = "ТЕБЯ ОБОШЛИ"
    tf = _font(46, True)
    tw = d.textlength(title, font=tf)
    from PIL import Image
    ov = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    ImageDraw.Draw(ov).rounded_rectangle(
        [W // 2 - tw // 2 - 44, 96, W // 2 + tw // 2 + 44, 184],
        radius=44, fill=(0, 0, 0, 70),
    )
    base.paste(ov, (0, 0), ov)
    _ctext(d, W // 2, 112, title, tf, (255, 255, 255, 240))

    # the sad character
    _sad_character(d, W // 2, 470, 165)

    # challenge label
    lf = _fit_font(d, f"«{label}»", W - 180, 60, bold=True, floor=36)
    _ctext(d, W // 2, 700, f"«{label}»", lf, WHITE)
    _ctext(d, W // 2, 778, "Кто-то ответил быстрее тебя", _font(38, False), (225, 230, 255, 230))
    _ctext(d, W // 2, 838, "Следи за своим местом ниже", _font(34, False), (190, 200, 235, 220))

    _ctext(d, W // 2, 1010, "Deutsche Sprache · Рейтинг", _font(30, False), (200, 208, 240, 150))

    out = io.BytesIO()
    base.convert("RGB").save(out, format="PNG", optimize=True)
    return out.getvalue()
