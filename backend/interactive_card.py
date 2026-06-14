"""Premium hero cards (PNG) for the interactive tasks posted into group chats.

A plain text message is dull; each interactive gets a branded card that reflects
what the task IS (headphones for Hörverständnis, word-tiles for Satzbau, a
stopwatch for Artikel Sprint, …). Vector-drawn on the same dark-navy + gold
brand kit as the champion/battle posters (no color-emoji font dependency), so it
renders identically on the server. Rendered in-memory and sent via send_photo;
callers fall back to the text card if this returns None.

This module starts with 3 templates (listening / satzbau / sprint); more task
types plug in by adding a motif + a render_* wrapper.
"""

from __future__ import annotations

import math
from io import BytesIO

try:
    from PIL import Image, ImageDraw, ImageFilter
except Exception:  # pragma: no cover
    Image = None

# Reuse the brand-kit font + text helpers from the champion poster.
from backend.champion_poster import _font, _ctext, _ltext_trunc, _star  # noqa: F401

W = H = 1080
BG_TOP = (20, 30, 56)
BG_BOT = (2, 6, 23)
GOLD = (255, 209, 74)
GOLD_LT = (255, 240, 178)
WHITE = (248, 250, 252)
MUTED = (158, 174, 199)
INK = (15, 21, 38)


def _lerp(a, b, t):
    return tuple(int(a[i] + (b[i] - a[i]) * t) for i in range(3))


def _base(accent):
    """Dark vertical gradient + a soft accent glow behind the motif."""
    img = Image.new("RGB", (W, H))
    d = ImageDraw.Draw(img)
    for y in range(H):
        d.line([(0, y), (W, y)], fill=_lerp(BG_TOP, BG_BOT, y / H))
    img = img.convert("RGBA")
    ov = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    ImageDraw.Draw(ov).ellipse([W // 2 - 360, 360, W // 2 + 360, 1080], fill=accent + (70,))
    ov = ov.filter(ImageFilter.GaussianBlur(130))
    return Image.alpha_composite(img, ov)


def _text_h(d, text, font):
    bb = d.textbbox((0, 0), text, font=font)
    return bb[3] - bb[1], bb[1]


def _pill(d, cx, cy, text, font, *, bg, fg, pad_x=34, pad_y=18, radius=None):
    """A centered rounded pill with text. Returns its (left, top, right, bottom)."""
    tw = d.textlength(text, font=font)
    th, off = _text_h(d, text, font)
    w, h = tw + pad_x * 2, th + pad_y * 2
    x0, y0 = cx - w / 2, cy - h / 2
    x1, y1 = cx + w / 2, cy + h / 2
    r = radius if radius is not None else h / 2
    d.rounded_rectangle([x0, y0, x1, y1], radius=r, fill=bg)
    d.text((cx - tw / 2, cy - h / 2 + pad_y - off), text, font=font, fill=fg)
    return (x0, y0, x1, y1)


def _header(base, d, *, badge, title, subtitle, accent):
    """Shared top block: an accent outline chip (category), big gold title,
    muted subtitle. No emoji in drawn text — the server font has no color glyphs."""
    f = _font(34)
    tw = f.getlength(badge)
    th, off = _text_h(d, badge, f)
    pad_x, pad_y = 34, 16
    w, h = tw + pad_x * 2, th + pad_y * 2
    x0, y0 = W / 2 - w / 2, 150 - h / 2
    d.rounded_rectangle([x0, y0, x0 + w, y0 + h], radius=h / 2, outline=accent, width=4)
    d.text((W / 2 - tw / 2, 150 - h / 2 + pad_y - off), badge, font=f, fill=accent)
    _ctext(d, W // 2, 232, _ltext_trunc(title, _font(86), d, W - 120), _font(86), GOLD)
    if subtitle:
        _ctext(d, W // 2, 352, _ltext_trunc(subtitle, _font(40, False), d, W - 160), _font(40, False), MUTED)


def _footer_cta(d, text, accent):
    _pill(d, W // 2, H - 120, text, _font(44), bg=accent, fg=INK, pad_x=56, pad_y=28)


# ── Motifs ───────────────────────────────────────────────────────────────────

def _motif_headphones(base, d, cx, cy, accent):
    """Over-ear headphones + sound waves (Hörverständnis / Hörlücke)."""
    r = 210
    # headband arc
    d.arc([cx - r, cy - r, cx + r, cy + r], start=180, end=360, fill=accent, width=34)
    d.arc([cx - r, cy - r, cx + r, cy + r], start=180, end=360, fill=GOLD_LT, width=10)
    # ear cups
    cup_w, cup_h = 96, 150
    for side in (-1, 1):
        ex = cx + side * r
        d.rounded_rectangle([ex - cup_w / 2, cy - 10, ex + cup_w / 2, cy - 10 + cup_h],
                            radius=34, fill=accent)
        d.rounded_rectangle([ex - cup_w / 2 + 14, cy + 4, ex + cup_w / 2 - 14, cy - 10 + cup_h - 14],
                            radius=24, fill=tuple(min(255, c + 26) for c in accent))
    # sound waves on the right
    for i, rr in enumerate((70, 130, 190)):
        ax = cx + r + 90
        d.arc([ax - rr, cy - rr, ax + rr, cy + rr], start=-45, end=45,
              fill=tuple(c for c in GOLD), width=12 - i * 2)


def _motif_word_tiles(base, d, cx, cy, accent):
    """A row of word-card tiles snapping into a sentence (Satzbau)."""
    tiles = [(150, "Der"), (190, "Satz"), (150, "wird"), (210, "gebaut")]
    gap = 26
    total = sum(w for w, _ in tiles) + gap * (len(tiles) - 1)
    x = cx - total / 2
    y = cy - 70
    for i, (w, label) in enumerate(tiles):
        dy = (-1) ** i * 16  # gentle stagger
        col = accent if i % 2 == 0 else tuple(min(255, c + 30) for c in accent)
        d.rounded_rectangle([x, y + dy, x + w, y + dy + 140], radius=26, fill=col)
        d.rounded_rectangle([x, y + dy, x + w, y + dy + 18], radius=26,
                            fill=tuple(min(255, c + 40) for c in col))
        f = _font(46)
        tw = d.textlength(label, font=f)
        th, off = _text_h(d, label, f)
        d.text((x + w / 2 - tw / 2, y + dy + 70 - th / 2 - off), label, font=f, fill=INK)
        x += w + gap
    # baseline they assemble onto
    d.rounded_rectangle([cx - total / 2 - 10, cy + 120, cx + total / 2 + 10, cy + 134],
                        radius=8, fill=GOLD)


def _motif_stopwatch(base, d, cx, cy, accent):
    """Stopwatch + der/die/das pills + a spark (Artikel Sprint)."""
    r = 150
    # top button + stem
    d.rounded_rectangle([cx - 34, cy - r - 64, cx + 34, cy - r - 24], radius=14, fill=accent)
    d.line([(cx, cy - r - 24), (cx, cy - r + 6)], fill=accent, width=18)
    # dial
    d.ellipse([cx - r, cy - r, cx + r, cy + r], fill=(18, 26, 46), outline=accent, width=20)
    d.ellipse([cx - r + 26, cy - r + 26, cx + r - 26, cy + r - 26], outline=tuple(min(255, c + 30) for c in accent), width=6)
    for a in range(0, 360, 30):
        rad = math.radians(a)
        x0, y0 = cx + (r - 40) * math.cos(rad), cy + (r - 40) * math.sin(rad)
        x1, y1 = cx + (r - 22) * math.cos(rad), cy + (r - 22) * math.sin(rad)
        d.line([(x0, y0), (x1, y1)], fill=MUTED, width=5)
    # hand pointing to ~2 o'clock
    ha = math.radians(-52)
    d.line([(cx, cy), (cx + (r - 56) * math.cos(ha), cy + (r - 56) * math.sin(ha))], fill=GOLD, width=12)
    d.ellipse([cx - 14, cy - 14, cx + 14, cy + 14], fill=GOLD)
    # spark
    _star(d, cx + r + 70, cy - r + 40, 46, GOLD)
    # der/die/das pills under the watch
    labels = [("der", (59, 130, 246)), ("die", (239, 68, 68)), ("das", (148, 163, 184))]
    f = _font(44)
    widths = [d.textlength(t, font=f) + 60 for t, _ in labels]
    gap = 22
    total = sum(widths) + gap * (len(labels) - 1)
    x = cx - total / 2
    py = cy + r + 78
    for (t, col), w in zip(labels, widths):
        d.rounded_rectangle([x, py - 38, x + w, py + 38], radius=38, fill=col)
        tw = d.textlength(t, font=f)
        th, off = _text_h(d, t, f)
        d.text((x + w / 2 - tw / 2, py - th / 2 - off), t, font=f, fill=WHITE)
        x += w + gap


def _finish(base) -> bytes:
    out = BytesIO()
    base.convert("RGB").save(out, format="PNG")
    return out.getvalue()


# ── Public renderers ─────────────────────────────────────────────────────────

def render_listening_card(*, topic: str = "", level: str = "B2", n_questions: int = 4) -> bytes | None:
    if Image is None:
        return None
    accent = (56, 189, 248)
    base = _base(accent)
    d = ImageDraw.Draw(base)
    _header(base, d, badge="HÖREN", title="Hörverständnis",
            subtitle=(topic.strip() or "Hörtraining") + f"  ·  {level}", accent=accent)
    _motif_headphones(base, d, W // 2, 640, accent)
    _footer_cta(d, f"Höre & beantworte {int(n_questions)} Fragen", GOLD)
    return _finish(base)


def render_satzbau_card(*, level: str = "B2+") -> bytes | None:
    if Image is None:
        return None
    accent = (52, 211, 153)
    base = _base(accent)
    d = ImageDraw.Draw(base)
    _header(base, d, badge="GRAMMATIK", title="Satzbau",
            subtitle=f"Wortstellung  ·  {level}", accent=accent)
    _motif_word_tiles(base, d, W // 2, 660, accent)
    _footer_cta(d, "Baue den richtigen Satz", GOLD)
    return _finish(base)


def render_sprint_card(*, level: str = "") -> bytes | None:
    if Image is None:
        return None
    accent = (251, 191, 36)
    base = _base(accent)
    d = ImageDraw.Draw(base)
    _header(base, d, badge="TEMPO-SPIEL", title="Artikel Sprint",
            subtitle="2 Minuten  ·  der / die / das", accent=accent)
    _motif_stopwatch(base, d, W // 2, 628, accent)
    _footer_cta(d, "Wer schafft die meisten?", GOLD)
    return _finish(base)
