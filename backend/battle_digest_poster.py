"""Render a single "Итоги батлов" digest PNG that combines ALL battles a user
played in one card, instead of spamming one podium photo per battle.

Style matches champion_poster.py (dark brand gradient, gold accents, Smurf-knight
medallion). Height grows with the number of battles. One row per battle: a colored
rank medal, the battle title, and the user's place/score. Winners are highlighted
in gold. Vector primitives only (no color-emoji font dependency) so it renders
identically on the server.
"""

from io import BytesIO

try:
    from PIL import Image, ImageDraw, ImageFont, ImageFilter
except Exception:  # pragma: no cover
    Image = None

W = 1080
BG_TOP = (17, 26, 49)
BG_BOT = (2, 6, 23)
GOLD = (255, 209, 74)
GOLD_LT = (255, 240, 178)
GOLD_DK = (181, 132, 26)
SILVER = (203, 213, 225)
SILVER_DK = (120, 132, 150)
BRONZE = (214, 142, 86)
BRONZE_DK = (150, 92, 50)
WHITE = (248, 250, 252)
MUTED = (150, 165, 190)
CARD = (30, 41, 59)
CARD_LT = (38, 51, 74)


def _font(size: int, bold: bool = True):
    candidates = [
        "/System/Library/Fonts/Supplemental/Arial Bold.ttf" if bold else "/System/Library/Fonts/Supplemental/Arial.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
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


def _ltext_trunc(text, font, draw, max_w):
    t = str(text or "")
    while t and draw.textlength(t, font=font) > max_w:
        t = t[:-1]
    if t != str(text or "") and len(t) > 1:
        t = t[:-1] + "…"
    return t


def _star(d, cx, cy, r, fill):
    import math
    pts = []
    for i in range(10):
        ang = -math.pi / 2 + i * math.pi / 5
        rr = r if i % 2 == 0 else r * 0.45
        pts.append((cx + rr * math.cos(ang), cy + rr * math.sin(ang)))
    d.polygon(pts, fill=fill)


def _vgrad(w, h):
    img = Image.new("RGB", (w, h))
    d = ImageDraw.Draw(img)
    for y in range(h):
        t = y / h
        d.line([(0, y), (w, y)], fill=tuple(int(BG_TOP[i] + (BG_BOT[i] - BG_TOP[i]) * t) for i in range(3)))
    return img.convert("RGBA")


def _glow(base, cx, cy, r, color, alpha, w, h):
    ov = Image.new("RGBA", (w, h), (0, 0, 0, 0))
    ImageDraw.Draw(ov).ellipse([cx - r, cy - r, cx + r, cy + r], fill=color + (alpha,))
    ov = ov.filter(ImageFilter.GaussianBlur(110))
    return Image.alpha_composite(base, ov)


def _avatar_circle(av_bytes, size, ring):
    try:
        im = Image.open(BytesIO(av_bytes)).convert("RGB").resize((size, size))
    except Exception:
        return None
    mask = Image.new("L", (size, size), 0)
    ImageDraw.Draw(mask).ellipse([0, 0, size, size], fill=255)
    out = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    out.paste(im, (0, 0), mask)
    ImageDraw.Draw(out).ellipse([2, 2, size - 3, size - 3], outline=ring, width=6)
    return out


def _place_colors(place):
    if place == 1:
        return GOLD, GOLD_DK
    if place == 2:
        return SILVER, SILVER_DK
    if place == 3:
        return BRONZE, BRONZE_DK
    if place:
        return (120, 132, 150), (70, 80, 95)
    return (70, 80, 95), (45, 52, 62)


def render_battle_digest(entries: list, *, hero_png: bytes | None = None,
                         subtitle: str | None = None) -> bytes | None:
    """entries: list of dicts sorted best-first, each with keys:
       title (str), place (int|None), total (int), count (int),
       is_win (bool), win_name (str)."""
    if Image is None or not entries:
        return None

    n = len(entries)
    header_h = 210
    hero_h = 220 if hero_png else 30
    row_h = 108
    summary_h = 90
    footer_h = 80
    H = header_h + hero_h + n * row_h + summary_h + footer_h

    base = _vgrad(W, H)
    base = _glow(base, W // 2, header_h + hero_h // 2, 340, GOLD, 55, W, H)
    d = ImageDraw.Draw(base)

    # Header
    _ctext(d, W // 2, 54, "ИТОГИ БАТЛОВ", _font(58), GOLD)
    _ctext(d, W // 2, 132, subtitle if subtitle is not None else f"{n} батлов", _font(34), MUTED)
    d.line([(W // 2 - 150, 180), (W // 2 + 150, 180)], fill=GOLD_DK, width=3)
    for sx in (W // 2 - 168, W // 2 + 168):
        _star(d, sx, 181, 10, GOLD)

    # Hero medallion (Smurf-knight) — brand seal
    y = header_h
    if hero_png:
        hero = _avatar_circle(hero_png, 180, GOLD)
        if hero:
            base.paste(hero, (W // 2 - 90, y + 6), hero)
    y += hero_h

    # Rows — one per battle
    x0, x1 = 56, W - 56
    for e in entries:
        cy = y + row_h // 2
        place = e.get("place")
        # row card
        d.rounded_rectangle([x0, y + 8, x1, y + row_h - 8], radius=20,
                            fill=(CARD_LT if e.get("is_win") else CARD))
        if e.get("is_win"):
            d.rounded_rectangle([x0, y + 8, x0 + 8, y + row_h - 8], radius=4, fill=GOLD)
        # rank medal
        col, dk = _place_colors(place)
        r = 40
        cxm = x0 + 34 + r
        d.ellipse([cxm - r, cy - r, cxm + r, cy + r], fill=col, outline=dk, width=4)
        _ctext(d, cxm, cy - 24, (str(place) if place else "—"), _font(42), (20, 24, 40))
        # title + info
        tx = cxm + r + 30
        title_col = GOLD if e.get("is_win") else WHITE
        d.text((tx, cy - 40), _ltext_trunc(e.get("title", ""), _font(40), d, x1 - tx - 40),
               font=_font(40), fill=title_col)
        if place:
            info = f"{place} место из {e.get('total', 0)} · {e.get('count', 0)} верных"
        else:
            wn = str(e.get("win_name") or "").strip()
            info = f"не сыграл · чемпион: {wn}" if wn and wn != "—" else "не сыграл"
        d.text((tx, cy + 8), _ltext_trunc(info, _font(28, False), d, x1 - tx - 40),
               font=_font(28, False), fill=MUTED)
        y += row_h

    # Summary line
    played = sum(1 for e in entries if e.get("place"))
    wins = sum(1 for e in entries if e.get("is_win"))
    y += 24
    summ = f"Сыграно {played} из {n}"
    if wins:
        summ += f"  ·  побед: {wins}"
    _ctext(d, W // 2, y, summ, _font(34), GOLD_LT)

    # Footer
    _ctext(d, W // 2, H - 58, "Deutsche Sprache · батлы", _font(26, False), MUTED)

    out = BytesIO()
    base.convert("RGB").save(out, format="PNG")
    return out.getvalue()
