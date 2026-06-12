"""Render a premium "Champion of the week" poster (PNG) for the global quiz
leaderboard. Cup/medals/podium are drawn with vector primitives (no color-emoji
font dependency), so it renders identically on the server. Used by the bot."""

from io import BytesIO

try:
    from PIL import Image, ImageDraw, ImageFont, ImageFilter
except Exception:  # pragma: no cover
    Image = None

W, H = 1080, 1500
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


def _vgrad():
    img = Image.new("RGB", (W, H))
    d = ImageDraw.Draw(img)
    for y in range(H):
        t = y / H
        d.line([(0, y), (W, y)], fill=tuple(int(BG_TOP[i] + (BG_BOT[i] - BG_TOP[i]) * t) for i in range(3)))
    return img.convert("RGBA")


def _glow(base, cx, cy, r, color, alpha):
    ov = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    ImageDraw.Draw(ov).ellipse([cx - r, cy - r, cx + r, cy + r], fill=color + (alpha,))
    ov = ov.filter(ImageFilter.GaussianBlur(110))
    return Image.alpha_composite(base, ov)


def _draw_trophy(d, cx, top, scale=1.0):
    gold, lt, dk = GOLD, GOLD_LT, GOLD_DK
    bowl_w = int(220 * scale)
    bowl_h = int(170 * scale)
    rim_y = top
    # handles (drawn first, behind bowl)
    hw = int(70 * scale)
    for side in (-1, 1):
        x0 = cx + side * (bowl_w // 2 - 6)
        d.arc([x0 - hw if side < 0 else x0, rim_y + 6, x0 if side < 0 else x0 + hw, rim_y + int(120 * scale)],
              start=(270 if side < 0 else 270), end=(90 if side < 0 else 90), fill=dk, width=int(16 * scale))
        d.arc([x0 - hw if side < 0 else x0, rim_y + 6, x0 if side < 0 else x0 + hw, rim_y + int(120 * scale)],
              start=(270 if side < 0 else 270), end=(90 if side < 0 else 90), fill=gold, width=int(9 * scale))
    # bowl body: trapezoid + rounded bottom
    bw2 = bowl_w // 2
    bb2 = int(bowl_w * 0.30)
    body_bottom = rim_y + int(bowl_h * 0.62)
    d.polygon([(cx - bw2, rim_y + 10), (cx + bw2, rim_y + 10),
               (cx + bb2, body_bottom), (cx - bb2, body_bottom)], fill=gold)
    d.pieslice([cx - bb2, body_bottom - bb2, cx + bb2, body_bottom + bb2], start=0, end=180, fill=gold)
    # rim (top opening)
    d.ellipse([cx - bw2, rim_y - int(20 * scale), cx + bw2, rim_y + int(28 * scale)], fill=gold, outline=dk, width=3)
    d.ellipse([cx - bw2 + 10, rim_y - int(14 * scale), cx + bw2 - 10, rim_y + int(20 * scale)], fill=dk)
    d.ellipse([cx - bw2 + 16, rim_y - int(10 * scale), cx + bw2 - 16, rim_y + int(14 * scale)], fill=gold)
    # sheen highlight
    d.ellipse([cx - bw2 + 24, rim_y + 24, cx - 8, body_bottom - 6], fill=lt)
    d.polygon([(cx - bw2, rim_y + 10), (cx - bw2 + 26, rim_y + 10), (cx - bb2 + 10, body_bottom), (cx - bb2, body_bottom)], fill=lt)
    # star on the cup
    _star(d, cx, rim_y + int(bowl_h * 0.30), int(34 * scale), dk)
    # stem + base
    stem_top = body_bottom + bb2 - 6
    d.rectangle([cx - int(16 * scale), stem_top, cx + int(16 * scale), stem_top + int(40 * scale)], fill=dk)
    base_y = stem_top + int(40 * scale)
    d.polygon([(cx - int(50 * scale), base_y), (cx + int(50 * scale), base_y),
               (cx + int(70 * scale), base_y + int(34 * scale)), (cx - int(70 * scale), base_y + int(34 * scale))], fill=gold)
    d.rounded_rectangle([cx - int(95 * scale), base_y + int(30 * scale), cx + int(95 * scale), base_y + int(60 * scale)],
                        radius=10, fill=dk)
    return base_y + int(60 * scale)


def _star(d, cx, cy, r, fill):
    import math
    pts = []
    for i in range(10):
        ang = -math.pi / 2 + i * math.pi / 5
        rr = r if i % 2 == 0 else r * 0.45
        pts.append((cx + rr * math.cos(ang), cy + rr * math.sin(ang)))
    d.polygon(pts, fill=fill)


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


def _initials_circle(name, size, ring, bg):
    out = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    dd = ImageDraw.Draw(out)
    dd.ellipse([0, 0, size, size], fill=bg)
    dd.ellipse([2, 2, size - 3, size - 3], outline=ring, width=6)
    initial = (str(name or "?").strip()[:1] or "?").upper()
    f = _font(int(size * 0.46))
    bb = dd.textbbox((0, 0), initial, font=f)
    dd.text((size / 2 - (bb[2] - bb[0]) / 2, size / 2 - (bb[3] - bb[1]) / 2 - bb[1]), initial, font=f, fill=WHITE)
    return out


def _podium_bar(base, d, x, base_y, w, h, top_col, dk_col, rank, name, points, av_bytes):
    top_y = base_y - h
    d.rounded_rectangle([x, top_y, x + w, base_y], radius=14, fill=top_col)
    d.rounded_rectangle([x, top_y, x + w, top_y + 12], radius=14, fill=tuple(min(255, c + 30) for c in top_col))
    _ctext(d, x + w // 2, base_y - 52, f"{points}", _font(40), (20, 24, 40))
    _ctext(d, x + w // 2, base_y - 24, "очк.", _font(20, False), (40, 48, 70))
    # avatar above the bar (real photo, else initials), with a rank badge
    av_size = 104
    avx, avy = x + w // 2 - av_size // 2, top_y - 40 - av_size
    av = (_avatar_circle(av_bytes, av_size, top_col) if av_bytes else None) or _initials_circle(name, av_size, top_col, dk_col)
    base.paste(av, (avx, avy), av)
    br = 30
    bcx, bcy = avx + av_size - 12, avy + av_size - 12
    d.ellipse([bcx - br, bcy - br, bcx + br, bcy + br], fill=top_col, outline=dk_col, width=4)
    _ctext(d, bcx, bcy - 20, str(rank), _font(28), (20, 24, 40))
    _ctext(d, x + w // 2, top_y - 34, _ltext_trunc(name, _font(30), d, w + 30), _font(30), WHITE)


def render_champion_poster(lb: dict, *, week_no: int, days: int, avatars: dict | None = None,
                           header: str | None = None, subtitle: str | None = None) -> bytes | None:
    if Image is None:
        return None
    leaders = (lb or {}).get("leaders") or []
    if not leaders:
        return None
    avatars = avatars or {}

    base = _vgrad()
    base = _glow(base, W // 2, 420, 360, GOLD, 70)
    d = ImageDraw.Draw(base)

    # Header (overridable so a daily card can read "CHAMPION DES TAGES" etc.)
    period = "WOCHE" if days == 7 else f"{days} TAGE"
    _ctext(d, W // 2, 58, header or ("CHAMPION DER " + period), _font(58), GOLD)
    _ctext(d, W // 2, 132, subtitle if subtitle is not None else f"№ {week_no}", _font(34), MUTED)
    d.line([(W // 2 - 150, 180), (W // 2 + 150, 180)], fill=GOLD_DK, width=3)
    for sx in (W // 2 - 168, W // 2 + 168):
        _star(d, sx, 181, 10, GOLD)

    # Trophy
    _draw_trophy(d, W // 2, 230, scale=1.15)

    # Champion
    champ = leaders[0]
    _ctext(d, W // 2, 600, _ltext_trunc(champ["name"], _font(70), d, W - 120), _font(70), WHITE)
    _ctext(d, W // 2, 686,
           f"{champ['points']} очков · {champ['correct']}/{champ['answered']} ✓ · {champ['golds']}× Gold",
           _font(34, False), GOLD_LT)

    # Podium (2nd left, 1st center, 3rd right)
    base_y = 1140
    bw, gap = 220, 24
    start_x = (W - (3 * bw + 2 * gap)) // 2
    cols = [
        (start_x, 170, SILVER, SILVER_DK, 2),
        (start_x + bw + gap, 250, GOLD, GOLD_DK, 1),
        (start_x + 2 * (bw + gap), 125, BRONZE, BRONZE_DK, 3),
    ]
    order = {1: leaders[0] if len(leaders) >= 1 else None,
             2: leaders[1] if len(leaders) >= 2 else None,
             3: leaders[2] if len(leaders) >= 3 else None}
    for x, h, col, dk, rank in cols:
        ldr = order.get(rank)
        if not ldr:
            continue
        _podium_bar(base, d, x, base_y, bw, h, col, dk, rank, ldr["name"], ldr["points"],
                    avatars.get(int(ldr["user_id"])))

    # Nominations
    y = 1205
    _ctext(d, W // 2, y, "НОМИНАЦИИ", _font(34), GOLD)
    y += 56
    noms = []
    if lb.get("fastest"):
        f = lb["fastest"]
        noms.append(((86, 200, 240), "Самый быстрый", f"{f['name']} · {(f['ctime_sum']/f['ctime_n']/1000):.1f} с"))
    if lb.get("accurate"):
        a = lb["accurate"]
        noms.append(((110, 220, 130), "Самый точный", f"{a['name']} · {round(a['correct']/a['answered']*100)}%"))
    if lb.get("active"):
        ac = lb["active"]
        noms.append(((240, 130, 110), "Самый активный", f"{ac['name']} · {ac['answered']} зад."))
    lbl_f, val_f = _font(28, False), _font(30)
    for col, label, val in noms:
        d.ellipse([110, y + 8, 134, y + 32], fill=col)
        d.text((156, y), label + ":", font=lbl_f, fill=MUTED)
        d.text((156 + d.textlength(label + ": ", font=lbl_f) + 8, y - 1), val, font=val_f, fill=WHITE)
        y += 52

    # Footer
    _ctext(d, W // 2, H - 64, f"Всего игроков: {lb.get('total_players', 0)} · заданий: {lb.get('total_tasks', 0)}",
           _font(26, False), MUTED)

    out = BytesIO()
    base.convert("RGB").save(out, format="PNG")
    return out.getvalue()
