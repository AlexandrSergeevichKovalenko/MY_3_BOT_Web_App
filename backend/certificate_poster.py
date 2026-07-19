"""Light "грамота" (achievement certificate) PNG — a bright, diploma-style card
showing a user's stats for a period in three columns: this period · previous ·
Δ (green ▲ up / red ▼ down). Deliberately light/airy so the numbers and labels
pop. No color-emoji in drawn text (server font has none) — icons are vector; our
Smurf appears as a small circular seal. Rendered in-memory; sent via send_photo.
"""
from __future__ import annotations

from io import BytesIO

try:
    from PIL import Image, ImageDraw, ImageFilter
except Exception:  # pragma: no cover
    Image = None

from backend.champion_poster import _font, _avatar_circle  # noqa: F401

W, H = 1080, 1380
CREAM_TOP = (255, 251, 241)
CREAM_BOT = (248, 240, 222)
INK = (31, 41, 55)
MUTED = (120, 113, 100)
GOLD = (176, 137, 38)
GOLD_LT = (212, 175, 75)
BLUE = (37, 99, 235)
GREEN = (22, 163, 74)
RED = (220, 38, 38)
LINE = (228, 219, 198)


def _lerp(a, b, t):
    return tuple(int(a[i] + (b[i] - a[i]) * t) for i in range(3))


def _rtext(d, right_x, y, text, font, fill):
    tw = d.textlength(text, font=font)
    d.text((right_x - tw, y), text, font=font, fill=fill)


def _ctext(d, cx, y, text, font, fill):
    tw = d.textlength(text, font=font)
    d.text((cx - tw / 2, y), text, font=font, fill=fill)


def _fit_font(d, text, max_w, size, bold=True, min_size=40):
    """Largest font (<= size, >= min_size) whose rendered width fits max_w."""
    while size > min_size:
        f = _font(size, bold)
        if d.textlength(text, font=f) <= max_w:
            return f
        size -= 2
    return _font(min_size, bold)


def _tri(d, cx, cy, size, up, fill):
    if up:
        d.polygon([(cx, cy - size), (cx - size, cy + size), (cx + size, cy + size)], fill=fill)
    else:
        d.polygon([(cx, cy + size), (cx - size, cy - size), (cx + size, cy - size)], fill=fill)


def _medal(d, cx, cy, r):
    """A small gold medal/star seal (vector)."""
    # ribbon
    d.polygon([(cx - 26, cy + r - 6), (cx - 10, cy + r + 60), (cx, cy + r + 30),
               (cx + 10, cy + r + 60), (cx + 26, cy + r - 6)], fill=(193, 60, 60))
    d.ellipse([cx - r, cy - r, cx + r, cy + r], fill=GOLD_LT, outline=GOLD, width=6)
    d.ellipse([cx - r + 12, cy - r + 12, cx + r - 12, cy + r - 12], outline=GOLD, width=3)
    # star
    import math
    pts = []
    for i in range(10):
        ang = -math.pi / 2 + i * math.pi / 5
        rad = (r - 26) if i % 2 == 0 else (r - 26) * 0.45
        pts.append((cx + rad * math.cos(ang), cy + rad * math.sin(ang)))
    d.polygon(pts, fill=GOLD)


def render_certificate(*, name: str, title: str, subtitle: str, rows: list[dict],
                       col_now: str = "Сейчас", col_prev: str = "Раньше",
                       footer: str = "", hero_png: bytes | None = None) -> bytes | None:
    """rows: [{label, now, prev, dir}] where now/prev are display strings and dir is
    +1 / 0 / -1 (improvement vs the previous period). Returns PNG bytes."""
    if Image is None:
        return None
    img = Image.new("RGB", (W, H))
    d = ImageDraw.Draw(img)
    for y in range(H):
        d.line([(0, y), (W, y)], fill=_lerp(CREAM_TOP, CREAM_BOT, y / H))

    # Ornate double border.
    d.rounded_rectangle([26, 26, W - 26, H - 26], radius=34, outline=GOLD, width=6)
    d.rounded_rectangle([44, 44, W - 44, H - 44], radius=26, outline=GOLD_LT, width=2)

    # Header.
    _medal(d, W // 2, 150, 52)
    title_font = _fit_font(d, title, W - 140, 82)
    _ctext(d, W // 2, 250, title, title_font, GOLD)
    _ctext(d, W // 2, 352, subtitle, _font(34, False), MUTED)
    _ctext(d, W // 2, 410, name, _font(58), INK)
    d.line([(W // 2 - 180, 492), (W // 2 + 180, 492)], fill=GOLD, width=3)

    # Column geometry.
    label_x = 96
    now_r = 720
    prev_r = 884
    delta_cx = 980
    head_y = 530
    f_head = _font(26, False)
    _rtext(d, now_r, head_y, col_now, f_head, MUTED)
    _rtext(d, prev_r, head_y, col_prev, f_head, MUTED)
    _ctext(d, delta_cx, head_y, "Δ", f_head, MUTED)

    f_label = _font(36)
    f_now = _font(44)
    f_prev = _font(34, False)
    y = 588
    row_h = 104
    for r in (rows or [])[:7]:
        d.line([(label_x, y - 14), (W - 80, y - 14)], fill=LINE, width=2)
        d.text((label_x, y + 18), str(r.get("label") or ""), font=f_label, fill=INK)
        _rtext(d, now_r, y + 10, str(r.get("now") or "—"), f_now, BLUE)
        _rtext(d, prev_r, y + 18, str(r.get("prev") or "—"), f_prev, MUTED)
        dirv = int(r.get("dir") or 0)
        if dirv > 0:
            _tri(d, delta_cx, y + 36, 18, True, GREEN)
        elif dirv < 0:
            _tri(d, delta_cx, y + 36, 18, False, RED)
        else:
            d.line([(delta_cx - 16, y + 36), (delta_cx + 16, y + 36)], fill=MUTED, width=5)
        y += row_h

    d.line([(label_x, y - 14), (W - 80, y - 14)], fill=LINE, width=2)

    # Smurf seal in the free top-right corner (won't collide with the rows/numbers).
    if hero_png:
        seal = _avatar_circle(hero_png, 150, GOLD)
        if seal:
            img.paste(seal, (W - 214, 70), seal)

    if footer:
        _ctext(d, W // 2, H - 92, footer, _font(34, False), MUTED)

    out = BytesIO()
    img.save(out, format="PNG")
    return out.getvalue()


def render_monthly_group_poster(*, group_name: str, month_label: str, total: int,
                                prev_total: int | None, champion_name: str | None,
                                together_days: int, month_days: int,
                                hero_png: bytes | None = None) -> bytes | None:
    """Co-op monthly milestone card for a GROUP: big hero number (задания вместе за
    месяц), growth vs the previous month, month champion, and 'all together' days.
    Cream/gold Fuchs style with a Felix seal. NO color emoji in drawn text (server
    font has none) — celebratory emoji live in the Telegram caption instead."""
    if Image is None:
        return None
    img = Image.new("RGB", (W, H))
    d = ImageDraw.Draw(img)
    for y in range(H):
        d.line([(0, y), (W, y)], fill=_lerp(CREAM_TOP, CREAM_BOT, y / H))
    d.rounded_rectangle([26, 26, W - 26, H - 26], radius=34, outline=GOLD, width=6)
    d.rounded_rectangle([44, 44, W - 44, H - 44], radius=26, outline=GOLD_LT, width=2)

    # Felix seal (or a vector medal) top-center.
    seal = _avatar_circle(hero_png, 168, GOLD) if hero_png else None
    if seal:
        img.paste(seal, (W // 2 - 84, 92), seal)
    else:
        _medal(d, W // 2, 176, 60)

    _ctext(d, W // 2, 300, "ИТОГ МЕСЯЦА", _fit_font(d, "ИТОГ МЕСЯЦА", W - 160, 78), GOLD)
    _ctext(d, W // 2, 398, month_label, _font(40, False), MUTED)
    if group_name:
        _ctext(d, W // 2, 456, group_name, _fit_font(d, group_name, W - 200, 54, min_size=30), INK)
    d.line([(W // 2 - 180, 540), (W // 2 + 180, 540)], fill=GOLD, width=3)

    # Hero number.
    hero = str(int(total))
    _ctext(d, W // 2, 596, hero, _fit_font(d, hero, W - 240, 250, min_size=90), GOLD)
    _ctext(d, W // 2, 892, "заданий вместе за месяц", _font(44, False), INK)

    # Growth vs previous month.
    if prev_total and int(prev_total) > 0:
        pct = round((int(total) - int(prev_total)) / int(prev_total) * 100)
        if pct >= 0:
            _ctext(d, W // 2, 968, f"на {pct}% больше, чем в прошлом месяце", _font(38, False), GREEN)
        else:
            _ctext(d, W // 2, 968, f"на {abs(pct)}% меньше, чем в прошлом месяце", _font(38, False), MUTED)
    else:
        _ctext(d, W // 2, 968, "первый месяц вместе — так держать!", _font(38, False), MUTED)

    # Highlights.
    y = 1074
    if champion_name:
        line = f"Чемпион месяца — {champion_name}"
        _ctext(d, W // 2, y, line, _fit_font(d, line, W - 200, 46, min_size=28), INK)
        y += 76
    if together_days and month_days:
        _ctext(d, W // 2, y, f"Все вместе занимались {together_days} из {month_days} дней",
               _font(36, False), MUTED)

    _ctext(d, W // 2, H - 96, "Schlaufuchs", _font(32, False), MUTED)

    out = BytesIO()
    img.save(out, format="PNG")
    return out.getvalue()
