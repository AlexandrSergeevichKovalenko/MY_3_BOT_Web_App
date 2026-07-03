"""
Morning "работа над ошибками" audio hero card (PIL).

Every morning the bot sends each user an mp3 built from THEIR own translation
mistakes of the previous day (correct German re-read aloud, optionally with a
grammar explanation). It's one of the most valuable sends — but a bare audio
file with a one-line caption ("Ошибки за 2026-07-02 …") is easy to scroll past.

This renders a branded hero card sent right BEFORE the audio so the send is
impossible to miss: a big drawn headphones motif (instantly reads as "listen"),
the number of phrases waiting, the date, and a short motivating line. Reuses the
shared brand kit (article_quiz_card primitives) — no new assets, no R2/network
dependency, so it always renders.
"""
from __future__ import annotations

import io

from backend.article_quiz_card import _font, _ctext, _fit_font, _vgrad, _glow, W, H

WHITE = (255, 255, 255)

# Warm "morning" gradient (sunrise amber → deep violet) — distinct from every
# other card so the send stands out in the feed.
_TOP = (245, 158, 11)
_BOTTOM = (91, 33, 182)

_RU_MONTHS = [
    "", "января", "февраля", "марта", "апреля", "мая", "июня",
    "июля", "августа", "сентября", "октября", "ноября", "декабря",
]


def _ru_date(d) -> str:
    """'2026-07-02' date → '2 июля'. Accepts a date/datetime or ISO string."""
    try:
        day = int(getattr(d, "day"))
        month = int(getattr(d, "month"))
    except Exception:
        s = str(d or "").strip()
        try:
            parts = s.split("-")
            month = int(parts[1])
            day = int(parts[2][:2])
        except Exception:
            return s
    if 1 <= month <= 12:
        return f"{day} {_RU_MONTHS[month]}"
    return str(d)


def _plural_phrases_ru(n: int) -> str:
    """Russian plural for 'фраза' agreeing with n (фраза / фразы / фраз)."""
    n = abs(int(n))
    if 11 <= n % 100 <= 14:
        return "фраз"
    d = n % 10
    if d == 1:
        return "фраза"
    if 2 <= d <= 4:
        return "фразы"
    return "фраз"


def _draw_headphones(base, cx: int, cy: int, R: int, color=(255, 255, 255, 245)) -> None:
    """Draw a clean headphones glyph centred on (cx, cy): a top band arc + two
    vertical ear-cup pills at its ends. Purely geometric — no font/emoji."""
    from PIL import Image, ImageDraw

    ov = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    d = ImageDraw.Draw(ov)

    band_w = max(18, R // 6)          # thickness of the headband
    cup_w = max(46, R // 2)           # ear-cup width
    cup_h = int(R * 1.15)             # ear-cup height

    # Headband — top semicircle (180° → 360°).
    d.arc([cx - R, cy - R, cx + R, cy + R], start=180, end=360, fill=color, width=band_w)

    # Ear cups — a vertical rounded pill hanging from each band end.
    for ex in (cx - R, cx + R):
        d.rounded_rectangle(
            [ex - cup_w // 2, cy - cup_w // 2, ex + cup_w // 2, cy - cup_w // 2 + cup_h],
            radius=cup_w // 2, fill=color,
        )

    base.paste(ov, (0, 0), ov)


def render_mistakes_audio_card(*, name: str = "", date, count: int = 0,
                               kind: str = "daily") -> bytes:
    """Branded hero card for the morning mistakes-audio send.

    `count` = number of phrases in the audio (hero number), `kind` = 'daily'
    (translation mistakes) or 'story' (Загадочная история re-read). Returns PNG
    bytes.
    """
    from PIL import Image, ImageDraw

    name = (name or "").strip()
    count = max(0, int(count or 0))
    date_label = _ru_date(date)

    base = _vgrad(_TOP, _BOTTOM).convert("RGBA")

    # Soft warm glow behind the headphones motif.
    _glow(base, W // 2, 380, 460, 55)
    d = ImageDraw.Draw(base)

    # Top pill — the headline.
    pill = "РАЗБОР ИСТОРИИ" if kind == "story" else "РАБОТА НАД ОШИБКАМИ"
    pf = _font(44, True)
    pw = d.textlength(pill, font=pf)
    ov = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    ImageDraw.Draw(ov).rounded_rectangle(
        [W // 2 - pw // 2 - 44, 84, W // 2 + pw // 2 + 44, 172], radius=44, fill=(0, 0, 0, 90),
    )
    base.paste(ov, (0, 0), ov)
    _ctext(d, W // 2, 100, pill, pf, (255, 255, 255, 240))

    # Hero motif — headphones (instantly reads as "audio to listen to").
    _draw_headphones(base, W // 2, 330, 150, (255, 255, 255, 245))
    d = ImageDraw.Draw(base)

    # Hero number — how many phrases are waiting.
    num = str(count)
    nf = _font(210, True)
    nbb = d.textbbox((0, 0), num, font=nf)
    ny = 620 - (nbb[3] - nbb[1]) // 2 - nbb[1]
    _ctext(d, W // 2 + 4, ny + 5, num, nf, (0, 0, 0, 90))   # shadow
    _ctext(d, W // 2, ny, num, nf, WHITE)

    # Label under the number.
    label = f"{_plural_phrases_ru(count)} — послушай и запомни"
    lf = _fit_font(d, label, W - 140, 52, bold=True, floor=32)
    _ctext(d, W // 2, 748, label, lf, (255, 255, 255, 235))

    # Date + name line (warm amber).
    who = f"Твои ошибки за {date_label}" if kind != "story" else f"Твоя история за {date_label}"
    if name:
        who = f"{who} · {name}"
    wf = _fit_font(d, who, W - 140, 44, bold=True, floor=28)
    _ctext(d, W // 2, 838, who, wf, (255, 224, 130, 240))

    # Motivating line.
    motivation = "Слушай в наушниках — правильные варианты осядут в памяти"
    mf = _fit_font(d, motivation, W - 150, 40, bold=False, floor=26)
    _ctext(d, W // 2, 922, motivation, mf, (235, 238, 250, 220))

    _ctext(d, W // 2, 1012, "Deutsche Sprache · Аудио-разбор", _font(30, False), (225, 225, 245, 175))

    out = io.BytesIO()
    base.convert("RGB").save(out, format="PNG", optimize=True)
    return out.getvalue()
