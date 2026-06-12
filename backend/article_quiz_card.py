"""
Article-quiz GRAMMAR card renderer (no DALL·E).

For abstract / homonym nouns (der Gedanke, die Angst, der Band …) a photo is
useless — the gender is taught by a *rule*, and for homonyms a picture cannot
even show which meaning is meant. So instead of a generated image we render a
bright, branded typographic card locally with PIL: the German word big and
bold, its meaning underneath, three frosted der/die/das chips and a large "?"
watermark. The gradient hue varies per word (hashed) so cards never look alike.

The card is cached in R2 exactly like a DALL·E image (image_object_key +
image_status='ready'), so the whole pick/dispatch/answer path is unchanged —
these entries are simply marked by dalle_prompt IS NULL.
"""
from __future__ import annotations

import hashlib
import io
import logging

# Curated vibrant gradient pairs (top, bottom) — picked by word hash so the
# deck looks varied and stylish, never muddy like random HSV.
_GRADIENTS: list[tuple[tuple[int, int, int], tuple[int, int, int]]] = [
    ((124, 58, 237), (67, 56, 202)),    # violet → indigo
    ((236, 72, 153), (157, 23, 77)),    # pink → rose
    ((249, 115, 22), (190, 24, 24)),    # orange → red
    ((20, 184, 166), (8, 120, 150)),    # teal → cyan
    ((59, 130, 246), (109, 40, 217)),   # blue → violet
    ((16, 185, 129), (13, 130, 120)),   # emerald → teal
    ((245, 158, 11), (200, 70, 12)),    # amber → orange
    ((217, 70, 239), (112, 26, 180)),   # fuchsia → purple
    ((14, 165, 233), (29, 78, 216)),    # sky → blue
    ((99, 102, 241), (67, 56, 180)),    # indigo
]

W = H = 1080
WHITE = (255, 255, 255)


def _font(size: int, bold: bool = True):
    from PIL import ImageFont
    candidates = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
        "/Library/Fonts/Arial Bold.ttf" if bold else "/Library/Fonts/Arial.ttf",
        "/System/Library/Fonts/Supplemental/Arial Bold.ttf" if bold else "/System/Library/Fonts/Supplemental/Arial.ttf",
        "/System/Library/Fonts/Helvetica.ttc",
    ]
    for p in candidates:
        try:
            return ImageFont.truetype(p, size)
        except Exception:
            continue
    return ImageFont.load_default()


def _ctext(draw, cx, y, text, font, fill):
    bb = draw.textbbox((0, 0), text, font=font)
    draw.text((cx - (bb[2] - bb[0]) / 2, y), text, font=font, fill=fill)


def _fit_font(draw, text, max_w, start, bold=True, floor=64):
    """Largest bold font (≤ start) whose text width fits max_w, not below floor."""
    size = start
    while size > floor:
        f = _font(size, bold)
        if draw.textlength(text, font=f) <= max_w:
            return f
        size -= 6
    return _font(floor, bold)


def _vgrad(top, bottom):
    from PIL import Image, ImageDraw
    img = Image.new("RGB", (W, H), top)
    d = ImageDraw.Draw(img)
    for y in range(H):
        t = y / (H - 1)
        d.line(
            [(0, y), (W, y)],
            fill=(
                int(top[0] + (bottom[0] - top[0]) * t),
                int(top[1] + (bottom[1] - top[1]) * t),
                int(top[2] + (bottom[2] - top[2]) * t),
            ),
        )
    return img


def _glow(base, cx, cy, r, alpha):
    from PIL import Image, ImageDraw, ImageFilter
    ov = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    ImageDraw.Draw(ov).ellipse([cx - r, cy - r, cx + r, cy + r], fill=(255, 255, 255, alpha))
    ov = ov.filter(ImageFilter.GaussianBlur(120))
    base.paste(ov, (0, 0), ov)


def _chip(base, cx, cy, w, h, label):
    """Frosted-glass der/die/das chip (neutral — does not reveal the answer)."""
    from PIL import Image, ImageDraw
    ov = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    d = ImageDraw.Draw(ov)
    d.rounded_rectangle(
        [cx - w // 2, cy - h // 2, cx + w // 2, cy + h // 2],
        radius=h // 2, fill=(255, 255, 255, 38), outline=(255, 255, 255, 110), width=3,
    )
    base.paste(ov, (0, 0), ov)
    f = _font(46, True)
    bb = ImageDraw.Draw(base).textbbox((0, 0), label, font=f)
    ImageDraw.Draw(base).text(
        (cx - (bb[2] - bb[0]) / 2, cy - (bb[3] - bb[1]) / 2 - bb[1]),
        label, font=f, fill=(255, 255, 255, 235),
    )


def render_article_quiz_card(word: str, meaning_ru: str = "") -> bytes:
    """Render a bright typographic article-quiz card. Returns PNG bytes."""
    from PIL import Image, ImageDraw

    word = (word or "").strip()
    meaning_ru = (meaning_ru or "").strip()

    idx = int(hashlib.md5(word.encode("utf-8")).hexdigest(), 16) % len(_GRADIENTS)
    top, bottom = _GRADIENTS[idx]

    base = _vgrad(top, bottom).convert("RGBA")
    _glow(base, W // 2, 360, 540, 55)
    d = ImageDraw.Draw(base)

    # Top pill — "WELCHER ARTIKEL?"
    pill = "WELCHER ARTIKEL?"
    pf = _font(40, True)
    pw = d.textlength(pill, font=pf)
    ov = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    ImageDraw.Draw(ov).rounded_rectangle(
        [W // 2 - pw // 2 - 40, 84, W // 2 + pw // 2 + 40, 166],
        radius=41, fill=(0, 0, 0, 70),
    )
    base.paste(ov, (0, 0), ov)
    _ctext(d, W // 2, 100, pill, pf, (255, 255, 255, 235))

    # Hero "?" motif — its own element ABOVE the word (no text collision)
    qf = _font(300, True)
    qbb = d.textbbox((0, 0), "?", font=qf)
    _ctext(d, W // 2, 215 - qbb[1], "?", qf, (255, 255, 255, 40))

    # The word — auto-fit, with soft drop shadow
    wf = _fit_font(d, word, W - 140, 160, bold=True, floor=64)
    wbb = d.textbbox((0, 0), word, font=wf)
    wy = 600 - (wbb[3] - wbb[1]) // 2 - wbb[1]
    _ctext(d, W // 2 + 4, wy + 5, word, wf, (0, 0, 0, 80))   # shadow
    _ctext(d, W // 2, wy, word, wf, WHITE)

    # Meaning (Russian) — essential for homonyms, nice everywhere
    if meaning_ru:
        mf = _fit_font(d, meaning_ru, W - 220, 56, bold=False, floor=34)
        _ctext(d, W // 2, 720, meaning_ru, mf, (255, 255, 255, 225))

    # Three frosted chips at the bottom
    chip_w, chip_h, gap = 270, 118, 36
    total = chip_w * 3 + gap * 2
    x0 = (W - total) // 2 + chip_w // 2
    for i, lbl in enumerate(("der", "die", "das")):
        _chip(base, x0 + i * (chip_w + gap), 880, chip_w, chip_h, lbl)

    _ctext(d, W // 2, 1010, "Deutsche Sprache · Artikel-Quiz", _font(30, False), (255, 255, 255, 150))

    out = io.BytesIO()
    base.convert("RGB").save(out, format="PNG", optimize=True)
    return out.getvalue()


def _object_key(word_id: str) -> str:
    safe = "".join(c if c.isalnum() or c in "-_" else "_" for c in str(word_id))
    return f"article_quiz/cards/{safe}.png"


def generate_article_quiz_card(word_id: str) -> str:
    """Render + cache a grammar card in R2; mark the bank row ready. Returns key."""
    from backend.database import (
        get_article_quiz_entry,
        mark_article_quiz_image_ready,
        mark_article_quiz_image_failed,
    )
    from backend.r2_storage import r2_put_bytes

    entry = get_article_quiz_entry(word_id)
    if not entry:
        raise RuntimeError(f"article_quiz card: no entry for {word_id}")
    if entry.get("image_status") == "ready" and entry.get("image_object_key"):
        return str(entry["image_object_key"])

    try:
        png = render_article_quiz_card(entry.get("word", ""), entry.get("meaning_ru", ""))
        object_key = _object_key(word_id)
        r2_put_bytes(
            object_key, png, content_type="image/png",
            cache_control="public, max-age=31536000, immutable",
        )
        mark_article_quiz_image_ready(word_id, image_object_key=object_key)
        logging.info("article_quiz_card: ready word_id=%s key=%s bytes=%s", word_id, object_key, len(png))
        return object_key
    except Exception as exc:
        mark_article_quiz_image_failed(word_id)
        raise RuntimeError(f"article_quiz card render failed for {word_id}: {exc}") from exc
