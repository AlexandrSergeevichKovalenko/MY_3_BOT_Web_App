"""Branded PNG card for a shared «Полный разбор» word breakdown.

Rendered locally with PIL (no LLM, no DALL·E) and hosted on R2 so it can be the
photo of a Telegram prepared inline message. Shows the German word (article in
its gender colour), IPA, level/POS/frequency chips and 1-2 meanings, with a
footer inviting the recipient to open the full interactive breakdown in the bot.

No emoji is drawn into the image (PIL fonts render them as tofu) — emoji live in
the Telegram message caption instead.
"""
from __future__ import annotations

import hashlib
import io

W, H = 1080, 1350
WHITE = (255, 255, 255)

# Same curated palette as article_quiz_card — picked per word so cards vary.
_GRADIENTS: list[tuple[tuple[int, int, int], tuple[int, int, int]]] = [
    ((124, 58, 237), (67, 56, 202)),
    ((236, 72, 153), (157, 23, 77)),
    ((249, 115, 22), (190, 24, 24)),
    ((20, 184, 166), (8, 120, 150)),
    ((59, 130, 246), (109, 40, 217)),
    ((16, 185, 129), (13, 130, 120)),
    ((245, 158, 11), (200, 70, 12)),
    ((217, 70, 239), (112, 26, 180)),
    ((14, 165, 233), (29, 78, 216)),
    ((99, 102, 241), (67, 56, 180)),
]

# Bright gender tints for the article (legible on the dark gradient).
_GENDER = {
    "der": (125, 185, 255),
    "die": (255, 150, 200),
    "das": (130, 235, 190),
}


def _font(size: int, bold: bool = True):
    from PIL import ImageFont
    candidates = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
        "/System/Library/Fonts/Supplemental/Arial Bold.ttf" if bold else "/System/Library/Fonts/Supplemental/Arial.ttf",
        "/System/Library/Fonts/Helvetica.ttc",
    ]
    for p in candidates:
        try:
            return ImageFont.truetype(p, size)
        except Exception:
            continue
    return ImageFont.load_default()


def _vgrad(top, bottom):
    from PIL import Image, ImageDraw
    img = Image.new("RGB", (W, H), top)
    d = ImageDraw.Draw(img)
    for y in range(H):
        t = y / (H - 1)
        d.line([(0, y), (W, y)], fill=(
            int(top[0] + (bottom[0] - top[0]) * t),
            int(top[1] + (bottom[1] - top[1]) * t),
            int(top[2] + (bottom[2] - top[2]) * t),
        ))
    return img


def _glow(base, cx, cy, r, alpha):
    from PIL import Image, ImageDraw, ImageFilter
    ov = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    ImageDraw.Draw(ov).ellipse([cx - r, cy - r, cx + r, cy + r], fill=(255, 255, 255, alpha))
    ov = ov.filter(ImageFilter.GaussianBlur(120))
    base.paste(ov, (0, 0), ov)


def _ctext(draw, cx, y, text, font, fill):
    bb = draw.textbbox((0, 0), text, font=font)
    draw.text((cx - (bb[2] - bb[0]) / 2, y), text, font=font, fill=fill)


def _fit_font(draw, text, max_w, start, bold=True, floor=48):
    size = start
    while size > floor:
        f = _font(size, bold)
        if draw.textlength(text, font=f) <= max_w:
            return f
        size -= 6
    return _font(floor, bold)


def _wrap(draw, text, font, max_w, max_lines=2):
    words = str(text or "").split()
    lines, cur = [], ""
    for w in words:
        trial = f"{cur} {w}".strip()
        if draw.textlength(trial, font=font) <= max_w or not cur:
            cur = trial
        else:
            lines.append(cur)
            cur = w
            if len(lines) == max_lines - 1:
                break
    if cur:
        lines.append(cur)
    rest = words[sum(len(l.split()) for l in lines):]
    if rest and lines:
        while lines[-1] and draw.textlength(lines[-1] + "…", font=font) > max_w:
            lines[-1] = lines[-1][:-1]
        lines[-1] += "…"
    return lines[:max_lines]


def _pill(base, d, cx, cy, label, font, fg, bg):
    from PIL import Image, ImageDraw
    tw = d.textlength(label, font=font)
    bb = d.textbbox((0, 0), label, font=font)
    pad_x, h = 26, (bb[3] - bb[1]) + 30
    ov = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    ImageDraw.Draw(ov).rounded_rectangle(
        [cx - tw / 2 - pad_x, cy - h / 2, cx + tw / 2 + pad_x, cy + h / 2],
        radius=int(h / 2), fill=bg,
    )
    base.paste(ov, (0, 0), ov)
    d.text((cx - tw / 2, cy - (bb[3] - bb[1]) / 2 - bb[1]), label, font=font, fill=fg)
    return tw + pad_x * 2


def render_share_card(
    *,
    word: str,
    article: str = "",
    ipa: str = "",
    translation: str = "",
    level: str = "",
    pos: str = "",
    frequency: str = "",
    meanings: list[str] | None = None,
    bot_label: str = "",
) -> bytes:
    """Render the shareable word card. Returns PNG bytes."""
    from PIL import Image, ImageDraw

    word = (word or "").strip()
    article = (article or "").strip().lower()
    meanings = [m.strip() for m in (meanings or []) if m and m.strip()][:2]

    idx = int(hashlib.md5(word.encode("utf-8")).hexdigest(), 16) % len(_GRADIENTS)
    base = _vgrad(*_GRADIENTS[idx]).convert("RGBA")
    _glow(base, W // 2, 380, 560, 52)
    d = ImageDraw.Draw(base)

    # Eyebrow pill
    _pill(base, d, W // 2, 118, "DEUTSCHE SPRACHE · РАЗБОР СЛОВА", _font(30, True),
          (255, 255, 255, 235), (0, 0, 0, 70))
    d = ImageDraw.Draw(base)

    # Headword (article coloured + word white), centred as a group, auto-fit
    art_disp = f"{article} " if article else ""
    combo = f"{art_disp}{word}"
    wf = _fit_font(d, combo, W - 130, 150, bold=True, floor=52)
    art_w = d.textlength(art_disp, font=wf) if art_disp else 0
    word_w = d.textlength(word, font=wf)
    wbb = d.textbbox((0, 0), word or "X", font=wf)
    wy = 330 - (wbb[3] - wbb[1]) // 2 - wbb[1]
    x0 = (W - (art_w + word_w)) / 2
    if art_disp:
        d.text((x0, wy + 4), art_disp, font=wf, fill=(0, 0, 0, 45))
        d.text((x0, wy), art_disp, font=wf, fill=_GENDER.get(article, (220, 220, 220)))
    d.text((x0 + art_w + 3, wy + 5), word, font=wf, fill=(0, 0, 0, 80))
    d.text((x0 + art_w, wy), word, font=wf, fill=WHITE)

    y = 470
    if ipa:
        _ctext(d, W // 2, y, f"[{ipa.strip('[]')}]", _font(46, False), (255, 255, 255, 220))
        y += 78

    # Chips row: level / POS / frequency
    chips = [c for c in (level.strip().upper(), pos.strip(), frequency.strip()) if c]
    if chips:
        fonts = _font(30, True)
        widths = [d.textlength(c, font=fonts) + 52 for c in chips]
        total = sum(widths) + 20 * (len(chips) - 1)
        cx = (W - total) / 2
        for c, cw in zip(chips, widths):
            _pill(base, ImageDraw.Draw(base), cx + cw / 2, y + 6, c, fonts,
                  (255, 255, 255, 240), (255, 255, 255, 40))
            cx += cw + 20
        d = ImageDraw.Draw(base)
        y += 90

    if translation:
        tf = _fit_font(d, translation, W - 200, 54, bold=True, floor=34)
        _ctext(d, W // 2, y, translation, tf, WHITE)
        y += 90

    # Meanings
    d.line([(120, y + 6), (W - 120, y + 6)], fill=(255, 255, 255, 60), width=2)
    y += 40
    mf = _font(38, False)
    for i, m in enumerate(meanings, start=1):
        for j, line in enumerate(_wrap(d, m, mf, W - 220, max_lines=2)):
            prefix = f"{i}.  " if j == 0 else "     "
            d.text((150, y), prefix + line, font=mf, fill=(255, 255, 255, 225))
            y += 54
        y += 12

    # Footer band + CTA
    fy = H - 168
    d.line([(120, fy), (W - 120, fy)], fill=(255, 255, 255, 60), width=2)
    foot = "Полный разбор, примеры и озвучка — в боте"
    _ctext(d, W // 2, fy + 30, foot, _fit_font(d, foot, W - 150, 32, bold=False, floor=24),
           (255, 255, 255, 215))
    if bot_label:
        _pill(base, ImageDraw.Draw(base), W // 2, H - 56, bot_label, _font(32, True),
              (30, 30, 40, 255), (255, 255, 255, 240))

    out = io.BytesIO()
    base.convert("RGB").save(out, format="PNG", optimize=True)
    return out.getvalue()
