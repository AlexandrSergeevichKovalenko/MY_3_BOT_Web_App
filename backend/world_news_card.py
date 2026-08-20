"""
World-news hero card (PIL) — «Начни день с коротких новостей».

Replaces the flat text card with a branded morning plашка whose mascot is the
Fox reading a newspaper. Reuses the shared brand kit (article_quiz_card
primitives) and a dedicated Fox-reading-newspaper background from R2
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
from backend.mascot_style import mascot_prompt

WHITE = (255, 255, 255)

# Dedicated Smurf-reading-the-news background. Generated once via gpt-image-1 → R2
# (see /admin_worldnews_image); a blue morning gradient is drawn until it's there.
_WORLD_NEWS_BG_KEY = "worldnews/fox_news.png"

# Стендап дня — своя картинка: лис с микрофоном на сцене. Ключ отдельный, чтобы рубрики
# не перетирали фон друг другу; засевается той же командой /admin_worldnews_image.
_STANDUP_BG_KEY = "standup/fox_standup.png"

# gpt-image-1 prompt — brand mascot (Fox, see backend/mascot_style.py) reading a
# morning newspaper. NO text/letters (the card draws its own). Empty lower area
# for the headline + title + summary.
WORLD_NEWS_IMAGE_PROMPT = mascot_prompt(
    "A Fox mascot sitting comfortably in a cozy armchair in the early morning, calmly "
    "reading an open paper newspaper, a steaming cup of coffee on a small side table, "
    "warm sunrise light coming through a window behind him, a relaxed curious smile "
    "and bright awake eyes, warm morning blue-and-amber lighting, plenty of empty "
    "space in the lower half. Encouraging 'start your day with short news' morning mood"
)

STANDUP_IMAGE_PROMPT = mascot_prompt(
    "A Fox mascot standing on a small comedy club stage in front of a vintage microphone "
    "on a stand, one paw raised mid-joke, laughing warmly with a wide confident grin, a "
    "red brick wall behind him lit by a single warm spotlight, a dark cosy club atmosphere "
    "with soft bokeh lights, plenty of empty space in the lower half. Lively, funny "
    "stand-up comedy evening mood"
)

_bg_cache: dict = {}  # ключ фона → {"t", "img"}; у каждой рубрики свой фон
_BG_CACHE_TTL = 600.0


def world_news_bg_key(rubric: str = "news") -> str:
    return _STANDUP_BG_KEY if str(rubric or "").strip().lower() == "standup" else _WORLD_NEWS_BG_KEY


def pick_world_news_background(rubric: str = "news") -> bytes | None:
    """Pre-generated Smurf-news background from R2 (cached ~10 min). None → the blue
    morning gradient fallback is drawn instead."""
    from backend.r2_storage import r2_get_bytes
    key = world_news_bg_key(rubric)
    slot = _bg_cache.setdefault(key, {"t": 0.0, "img": None})
    now = time.time()
    if now - float(slot.get("t") or 0.0) > _BG_CACHE_TTL or not slot.get("img"):
        img = None
        try:
            b = r2_get_bytes(key)
            if b:
                img = bytes(b)
        except Exception:
            img = None
        slot["img"] = img
        slot["t"] = now
    return slot.get("img")


def bust_world_news_bg_cache() -> None:
    _bg_cache.clear()


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
    # Рубрика решает картинку и подписи. Выступление комика под шапкой «НОВОСТЬ ДНЯ»
    # читалось бы как сбой, поэтому шапка, подпись и фон берутся по рубрике записи.
    rubric = str(entry.get("rubric") or "news").strip().lower()
    is_standup = rubric == "standup"
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
        bg = pick_world_news_background(rubric)
    except Exception:
        bg = None
    grad_top, grad_bottom = ((120, 52, 48), (34, 16, 26)) if is_standup else ((48, 84, 150), (14, 24, 58))
    if bg:
        try:
            base = ImageOps.fit(
                Image.open(io.BytesIO(bg)).convert("RGBA"), (W, H), Image.LANCZOS
            )
        except Exception:
            base = _vgrad(grad_top, grad_bottom).convert("RGBA")
    else:
        base = _vgrad(grad_top, grad_bottom).convert("RGBA")

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
    pill = "СТЕНДАП ДНЯ" if is_standup else "НОВОСТЬ ДНЯ"
    pf = _font(46, True)
    pw = d.textlength(pill, font=pf)
    ovp = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    ImageDraw.Draw(ovp).rounded_rectangle(
        [W // 2 - pw // 2 - 44, 92, W // 2 + pw // 2 + 44, 182], radius=45, fill=(0, 0, 0, 120),
    )
    base.paste(ovp, (0, 0), ovp)
    _ctext(d, W // 2, 108, pill, pf, (255, 255, 255, 240))

    # The actual news text (title + theses) lives in the message caption — do NOT duplicate it
    # on the image. Keep the photo clean & branded: just the mascot + a generic tagline.
    tagline = ("Смейся · Разбирай сленг · Проверь себя" if is_standup
               else "Смотри · Учи слова · Проверь себя")
    tf = _fit_font(d, tagline, W - 160, 42, bold=True, floor=30)
    _ctext(d, W // 2, 912, tagline, tf, (255, 224, 130, 235))

    _ctext(d, W // 2, 1016, "Deutsche Sprache · " + ("Стендап" if is_standup else "Новости"),
           _font(30, False), (220, 232, 250, 175))

    out = io.BytesIO()
    base.convert("RGB").save(out, format="PNG", optimize=True)
    return out.getvalue()
