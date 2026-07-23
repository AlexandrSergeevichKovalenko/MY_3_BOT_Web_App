"""Daily "Итоги дня" summary as a clean branded PNG table — replaces the hard-to-read
text per-category lines. Light/airy card: a header band, three big stat tiles
(answered / correct / accuracy) and a per-category table with an accuracy bar.

No color-emoji in drawn text (the server font has none) — category icons are small
vector dots, the flame/streak is plain text. Rendered in-memory; sent via send_photo.
The bot keeps a plain-text fallback for when PIL is unavailable.
"""
from __future__ import annotations

import re
from io import BytesIO

try:
    from PIL import Image, ImageDraw
except Exception:  # pragma: no cover
    Image = None

from backend.champion_poster import _font, _avatar_circle  # shared brand-kit

W = 1080
BG_TOP = (248, 250, 253)
BG_BOT = (238, 242, 249)
HEAD_TOP = (37, 99, 235)
HEAD_BOT = (59, 130, 246)
INK = (31, 41, 55)
MUTED = (120, 130, 145)
WHITE = (248, 250, 252)
CARD = (255, 255, 255)
LINE = (228, 233, 240)
TRACK = (232, 236, 243)
GREEN = (22, 163, 74)
AMBER = (217, 154, 22)
RED = (220, 38, 38)
BLUE = (37, 99, 235)

# A distinct dot colour per category row (cycled).
_DOTS = [(37, 99, 235), (139, 92, 246), (16, 185, 129), (236, 72, 153),
         (245, 158, 11), (14, 165, 233), (244, 63, 94), (99, 102, 241)]

_EMOJI = re.compile(
    "[\U0001F000-\U0001FAFF\U00002600-\U000027BF\U0001F1E6-\U0001F1FF←-⇿⌀-⏿️‍]+"
)


def _plain(s: str) -> str:
    """Strip HTML tags + emoji so the (emoji-less) server font renders cleanly."""
    s = re.sub(r"<[^>]+>", "", str(s or ""))
    s = _EMOJI.sub("", s)
    return re.sub(r"\s+", " ", s).strip()


def _lerp(a, b, t):
    return tuple(int(a[i] + (b[i] - a[i]) * t) for i in range(3))


def _ctext(d, cx, y, text, font, fill):
    tw = d.textlength(text, font=font)
    d.text((cx - tw / 2, y), text, font=font, fill=fill)


def _rtext(d, rx, y, text, font, fill):
    tw = d.textlength(text, font=font)
    d.text((rx - tw, y), text, font=font, fill=fill)


def _acc_color(pct: int):
    if pct >= 70:
        return GREEN
    if pct >= 40:
        return AMBER
    return RED


def _fmtn(x) -> str:
    """Whole numbers as ints ("3"); halves kept ("0.5"). Sprint/Battle categories score
    a fractional {0, 0.5, 1} per set, so "Верно" and per-row correct can be non-integer."""
    x = float(x or 0)
    return str(int(x)) if x.is_integer() else f"{x:g}"


def render_daily_summary(*, date_str: str, answered: int, correct: int, accuracy_pct: int,
                         streak_text: str = "", rows: list[dict] | None = None,
                         hero_png: bytes | None = None) -> bytes | None:
    """rows: [{"label","correct","answered","acc"}] (only categories with activity).
    Returns PNG bytes, or None if PIL is unavailable."""
    if Image is None:
        return None
    rows = [r for r in (rows or []) if int(r.get("answered") or 0) > 0]

    pad = 56
    head_h = 248
    tiles_y = head_h + 40
    tiles_h = 196
    streak_h = 88 if streak_text else 24
    table_head_y = tiles_y + tiles_h + streak_h + 28
    row_h = 92
    table_h = 70 + max(1, len(rows)) * row_h
    H = table_head_y + table_h + 96

    img = Image.new("RGB", (W, H))
    d = ImageDraw.Draw(img)
    for y in range(H):
        d.line([(0, y), (W, y)], fill=_lerp(BG_TOP, BG_BOT, y / H))

    # ── Header band ──
    for y in range(head_h):
        d.line([(0, y), (W, y)], fill=_lerp(HEAD_TOP, HEAD_BOT, y / head_h))
    _ctext(d, W // 2, 64, "Итоги дня", _font(76), WHITE)
    _ctext(d, W // 2, 162, _plain(date_str), _font(38, False), (219, 234, 254))
    if hero_png:
        seal = _avatar_circle(hero_png, 132, WHITE)
        if seal:
            img.paste(seal, (W - 188, 58), seal)

    # ── Three stat tiles ──
    tiles = [("Ответил", str(int(answered)), INK),
             ("Верно", _fmtn(correct), GREEN),
             ("Точность", f"{int(accuracy_pct)}%", _acc_color(int(accuracy_pct)))]
    gap = 28
    tw = (W - 2 * pad - 2 * gap) // 3
    for i, (lbl, val, col) in enumerate(tiles):
        x0 = pad + i * (tw + gap)
        d.rounded_rectangle([x0, tiles_y, x0 + tw, tiles_y + tiles_h], radius=28,
                            fill=CARD, outline=LINE, width=2)
        _ctext(d, x0 + tw / 2, tiles_y + 36, val, _font(78), col)
        _ctext(d, x0 + tw / 2, tiles_y + 138, lbl.upper(), _font(28, False), MUTED)

    # ── Streak line ──
    if streak_text:
        sy = tiles_y + tiles_h + 22
        d.rounded_rectangle([pad, sy, W - pad, sy + 56], radius=18, fill=(255, 247, 237))
        _ctext(d, W // 2, sy + 12, _plain(streak_text), _font(32, False), (180, 83, 9))

    # ── Per-category table ──
    label_x = pad + 16
    bar_x = 540
    bar_w = 196
    score_r = W - pad - 150
    pct_r = W - pad - 16
    fh = _font(26, False)
    _rtext(d, score_r, table_head_y, "верно / ответил", fh, MUTED)
    _rtext(d, pct_r, table_head_y, "точность", fh, MUTED)
    d.text((label_x, table_head_y), "КАТЕГОРИЯ", font=fh, fill=MUTED)

    y = table_head_y + 56
    d.rounded_rectangle([pad, y, W - pad, y + len(rows) * row_h], radius=24,
                        fill=CARD, outline=LINE, width=2)
    f_lbl = _font(36)
    f_score = _font(34, False)
    f_pct = _font(38)
    for i, r in enumerate(rows):
        cy = y + i * row_h
        if i:
            d.line([(label_x, cy), (W - pad - 16, cy)], fill=LINE, width=2)
        a = int(r.get("answered") or 0)
        c = float(r.get("correct") or 0)
        acc = int(r.get("acc") if r.get("acc") is not None else (round(c / a * 100) if a else 0))
        mid = cy + row_h / 2
        # dot + label
        dot = _DOTS[i % len(_DOTS)]
        d.ellipse([label_x, mid - 9, label_x + 18, mid + 9], fill=dot)
        d.text((label_x + 34, mid - 24), _plain(r.get("label") or ""), font=f_lbl, fill=INK)
        # accuracy bar
        d.rounded_rectangle([bar_x, mid - 9, bar_x + bar_w, mid + 9], radius=9, fill=TRACK)
        fillw = int(bar_w * max(0, min(100, acc)) / 100)
        if fillw > 0:
            d.rounded_rectangle([bar_x, mid - 9, bar_x + max(18, fillw), mid + 9],
                                radius=9, fill=_acc_color(acc))
        # numbers
        _rtext(d, score_r, mid - 22, f"{_fmtn(c)} / {a}", f_score, MUTED)
        _rtext(d, pct_r, mid - 26, f"{acc}%", f_pct, _acc_color(acc))

    _ctext(d, W // 2, H - 66, "Поделись с другом · +3 дня полного доступа обоим", _font(30, False), MUTED)

    out = BytesIO()
    img.save(out, format="PNG")
    return out.getvalue()
