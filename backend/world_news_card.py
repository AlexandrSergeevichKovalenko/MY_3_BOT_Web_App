"""
World-news hero card (PIL) — «Начни день с коротких новостей».

Replaces the flat text card with a branded morning plашка whose mascot is a
Smurf reading a newspaper. Reuses the shared brand kit (article_quiz_card
primitives) and a dedicated Smurf-reading-newspaper background from R2
(generated once via /admin_worldnews_image); a blue morning gradient is used
until that asset exists.

Draws its own text (title + summary + meta), so emoji are stripped from anything
painted on the canvas — the server font has no color-emoji glyphs. Emoji live
only in the Telegram caption, never on the PNG.
"""
from __future__ import annotations

import io
import time

from backend.article_quiz_card import _font, _ctext, _fit_font, _vgrad, W, H
from backend.lazy_day_card import _strip_emoji

WHITE = (255, 255, 255)

# Dedicated Smurf-reading-the-news background. Generated once via gpt-image-1 → R2
# (see /admin_worldnews_image); a blue morning gradient is drawn until it's there.
_WORLD_NEWS_BG_KEY = "worldnews/smurf_news.png"

# gpt-image-1 prompt — brand mascot reading a morning newspaper. NO text/letters
# (the card draws its own). Blue morning palette to sit under the card's scrim;
# empty lower area for the headline + title + summary.
WORLD_NEWS_IMAGE_PROMPT = (
    "A cute friendly blue smurf-like cartoon character sitting comfortably in a cozy armchair "
    "in the early morning, calmly reading an open paper newspaper, a steaming cup of coffee on a "
    "small side table, warm sunrise light coming through a window behind him, a relaxed curious "
    "smile and bright awake eyes. Encouraging 'start your day with short news' morning mood. "
    "Soft 3D Pixar-like render, warm morning blue-and-amber lighting, vibrant playful colors, "
    "clean simple uncluttered background with plenty of empty space in the lower half, "
    "no text, no letters, no numbers, centered composition."
)

_bg_cache: dict = {"t": 0.0, "img": None}
_BG_CACHE_TTL = 600.0


def world_news_bg_key() -> str:
    return _WORLD_NEWS_BG_KEY


def pick_world_news_background() -> bytes | None:
    """Pre-generated Smurf-news background from R2 (cached ~10 min). None → the blue
    morning gradient fallback is drawn instead."""
    from backend.r2_storage import r2_get_bytes
    now = time.time()
    if now - float(_bg_cache.get("t") or 0.0) > _BG_CACHE_TTL or not _bg_cache.get("img"):
        img = None
        try:
            b = r2_get_bytes(_WORLD_NEWS_BG_KEY)
            if b:
                img = bytes(b)
        except Exception:
            img = None
        _bg_cache["img"] = img
        _bg_cache["t"] = now
    return _bg_cache.get("img")


def bust_world_news_bg_cache() -> None:
    _bg_cache["t"] = 0.0
    _bg_cache["img"] = None


def _plural_words_ru(n: int) -> str:
    n = abs(int(n))
    if 11 <= n % 100 <= 14:
        return "слов"
    d = n % 10
    if d == 1:
        return "слово"
    if 2 <= d <= 4:
        return "слова"
    return "слов"


def _wrap_lines(draw, text: str, font, max_w: int, max_lines: int) -> list[str]:
    """Greedy word-wrap to <= max_lines; the last line gets an ellipsis if truncated."""
    words = str(text or "").split()
    lines: list[str] = []
    cur = ""
    for word in words:
        trial = (cur + " " + word).strip()
        if not cur or draw.textlength(trial, font=font) <= max_w:
            cur = trial
        else:
            lines.append(cur)
            cur = word
            if len(lines) >= max_lines:
                break
    if cur and len(lines) < max_lines:
        lines.append(cur)
    # Truncation: if words remain beyond max_lines, tack on an ellipsis.
    joined = " ".join(lines)
    if joined and len(joined.split()) < len(words):
        last = lines[-1]
        while last and draw.textlength(last + "…", font=font) > max_w:
            last = last.rsplit(" ", 1)[0] if " " in last else last[:-1]
        lines[-1] = (last + "…") if last else "…"
    return lines


def render_world_news_card(entry: dict) -> bytes:
    """Branded «Начни день с коротких новостей» card: Smurf reading the news + the day's
    video title, RU summary and a meta line (channel · duration · N слов). Returns PNG bytes."""
    from PIL import Image, ImageDraw, ImageOps

    entry = entry or {}
    title = _strip_emoji(str(entry.get("video_title") or "").strip())
    summary = _strip_emoji(str(entry.get("summary_ru") or "").strip())
    channel = _strip_emoji(str(entry.get("channel_title") or "").strip())
    dur = int(entry.get("duration_seconds") or 0)
    dur_txt = f"{dur // 60}:{dur % 60:02d}" if dur else ""
    phrases = entry.get("phrases") or []
    n_words = sum(1 for p in phrases if isinstance(p, dict) and str(p.get("de") or "").strip())

    # Background: Smurf reading the news; blue morning gradient fallback until the R2
    # asset is seeded (before /admin_worldnews_image is run).
    bg = None
    try:
        bg = pick_world_news_background()
    except Exception:
        bg = None
    if bg:
        try:
            base = ImageOps.fit(
                Image.open(io.BytesIO(bg)).convert("RGBA"), (W, H), Image.LANCZOS
            )
        except Exception:
            base = _vgrad((48, 84, 150), (14, 24, 58)).convert("RGBA")
    else:
        base = _vgrad((48, 84, 150), (14, 24, 58)).convert("RGBA")

    # Legibility scrim: darken (stronger toward the bottom, where the text lives).
    ov = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    od = ImageDraw.Draw(ov)
    for y in range(H):
        a = 80
        if y > 430:
            a = int(80 + 135 * min(1.0, (y - 430) / (H - 430)))
        od.line([(0, y), (W, y)], fill=(8, 16, 36, a))
    base = Image.alpha_composite(base, ov)

    d = ImageDraw.Draw(base)

    # Top pill — headline.
    pill = "НОВОСТЬ ДНЯ"
    pf = _font(46, True)
    pw = d.textlength(pill, font=pf)
    ovp = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    ImageDraw.Draw(ovp).rounded_rectangle(
        [W // 2 - pw // 2 - 44, 92, W // 2 + pw // 2 + 44, 182], radius=45, fill=(0, 0, 0, 120),
    )
    base.paste(ovp, (0, 0), ovp)
    _ctext(d, W // 2, 108, pill, pf, (255, 255, 255, 240))

    # Text block flows from the lower half (over the darkest part of the scrim).
    y = 560

    # Title (up to 2 lines, auto-shrunk to fit width).
    if title:
        tf = _font(60, True)
        tlines = _wrap_lines(d, title, tf, W - 150, 2)
        # Shrink if a single very long word still overflows (guard .size for bitmap fallback font).
        while tlines and any(d.textlength(ln, font=tf) > W - 150 for ln in tlines) and getattr(tf, "size", 0) > 40:
            tf = _font(getattr(tf, "size", 44) - 4, True)
            tlines = _wrap_lines(d, title, tf, W - 150, 2)
        for ln in tlines:
            _ctext(d, W // 2 + 3, y + 4, ln, tf, (0, 0, 0, 110))  # shadow
            _ctext(d, W // 2, y, ln, tf, WHITE)
            y += int(tf.size * 1.18)
        y += 20

    # Summary (up to 3 lines).
    if summary:
        sf = _font(38, False)
        for ln in _wrap_lines(d, summary, sf, W - 170, 3):
            _ctext(d, W // 2, y, ln, sf, (232, 240, 252, 226))
            y += int(sf.size * 1.28)
        y += 16

    # Meta line (amber): channel · duration · N слов. No emoji — the server font would
    # render e.g. ⏱ as a tofu box, so duration is spelled out in minutes.
    dur_min = (dur + 59) // 60 if dur else 0
    meta_parts = [p for p in (channel, (f"{dur_min} мин" if dur_min else ""),
                              (f"{n_words} {_plural_words_ru(n_words)}" if n_words else "")) if p]
    meta = _strip_emoji(" · ".join(meta_parts))
    if meta:
        mf = _fit_font(d, meta, W - 160, 36, bold=True, floor=26)
        _ctext(d, W // 2, min(y, 966), meta, mf, (255, 224, 130, 235))

    _ctext(d, W // 2, 1016, "Deutsche Sprache · Новости", _font(30, False), (220, 232, 250, 175))

    out = io.BytesIO()
    base.convert("RGB").save(out, format="PNG", optimize=True)
    return out.getvalue()
