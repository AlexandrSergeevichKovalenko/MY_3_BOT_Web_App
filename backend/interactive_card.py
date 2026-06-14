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

def _tile(d, cx, cy, w, h, label, col, *, fs=46, radius=24, ink=INK):
    """A rounded word/letter tile with a top sheen and centered label."""
    x0, y0 = cx - w / 2, cy - h / 2
    d.rounded_rectangle([x0, y0, x0 + w, y0 + h], radius=radius, fill=col)
    d.rounded_rectangle([x0, y0, x0 + w, y0 + min(16, h / 3)], radius=radius,
                        fill=tuple(min(255, c + 38) for c in col))
    if label:
        f = _font(fs)
        tw = f.getlength(label)
        th, off = _text_h(d, label, f)
        d.text((cx - tw / 2, cy - th / 2 - off), label, font=f, fill=ink)


def _gap_line(d, cx, cy, w, accent):
    """A sentence baseline with a glowing empty gap box in the middle."""
    seg = (w - 200) / 2
    for sx in (cx - w / 2, cx + w / 2 - seg):
        d.rounded_rectangle([sx, cy - 7, sx + seg, cy + 7], radius=7, fill=(90, 104, 130))
    d.rounded_rectangle([cx - 100, cy - 46, cx + 100, cy + 46], radius=18, outline=accent, width=6)
    _ctext(d, cx, cy - 34, "?", _font(54), accent)

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


def _motif_anagram(base, d, cx, cy, accent):
    """Scrambled letter tiles above an empty target row (Anagramm)."""
    scrambled = ["R", "T", "A", "G", "N", "E"]
    n = len(scrambled)
    sz, gap = 116, 22
    total = n * sz + (n - 1) * gap
    x = cx - total / 2 + sz / 2
    for i, ch in enumerate(scrambled):
        dy = (-1) ** i * 22
        col = accent if i % 2 == 0 else tuple(min(255, c + 28) for c in accent)
        _tile(d, x, cy - 90 + dy, sz, sz, ch, col, fs=60, radius=22)
        x += sz + gap
    # empty target slots they snap into
    x = cx - total / 2 + sz / 2
    for _ in range(n):
        d.rounded_rectangle([x - sz / 2, cy + 95, x + sz / 2, cy + 95 + sz], radius=22,
                            outline=(110, 124, 150), width=5)
        x += sz + gap


def _motif_blocks(base, d, cx, cy, accent):
    """Prefix + stem + suffix building blocks (Wortbildung)."""
    parts = [("un", 150), ("glaub", 230), ("lich", 170)]
    gap = 70
    total = sum(w for _, w in parts) + gap * (len(parts) - 1)
    x = cx - total / 2
    f = _font(72)
    for i, (label, w) in enumerate(parts):
        col = accent if i == 1 else tuple(min(255, c + 26) for c in accent)
        _tile(d, x + w / 2, cy, w, 150, label, col, fs=50, radius=26)
        if i < len(parts) - 1:
            _ctext(d, x + w + gap / 2, cy - 48, "+", f, GOLD)
        x += w + gap


def _motif_transform(base, d, cx, cy, accent):
    """Two sentence bars with a circular arrow — one form becomes another."""
    bw, bh = 560, 96
    _tile(d, cx, cy - 110, bw, bh, "Aktiv", tuple(min(255, c + 20) for c in accent), fs=46, radius=24)
    _tile(d, cx, cy + 110, bw, bh, "Passiv", accent, fs=46, radius=24)
    # circular transform arrow in the middle
    r = 54
    d.arc([cx - r, cy - r, cx + r, cy + r], start=40, end=320, fill=GOLD, width=12)
    d.polygon([(cx + r - 6, cy - 30), (cx + r + 18, cy - 36), (cx + r + 6, cy - 8)], fill=GOLD)


def _motif_error(base, d, cx, cy, accent):
    """A sentence bar with one wrong word boxed + wavy underline + magnifier."""
    bw, bh = 620, 150
    d.rounded_rectangle([cx - bw / 2, cy - bh / 2, cx + bw / 2, cy + bh / 2], radius=28, fill=(30, 41, 59))
    # three "word" bars; the middle one is the error (accent box)
    words = [(-200, 150, (90, 104, 130)), (0, 170, accent), (190, 130, (90, 104, 130))]
    for dx, w, col in words:
        d.rounded_rectangle([cx + dx - w / 2, cy - 22, cx + dx + w / 2, cy + 22], radius=12, fill=col)
    # wavy underline under the error word
    import math as _m
    pts = [(cx - 85 + i * 6, cy + 44 + 7 * _m.sin(i / 2.2)) for i in range(28)]
    d.line(pts, fill=accent, width=6, joint="curve")
    # magnifier
    mx, my, mr = cx + bw / 2 - 6, cy + bh / 2 - 6, 56
    d.ellipse([mx - mr, my - mr, mx + mr, my + mr], outline=GOLD, width=14)
    d.line([(mx + mr * 0.7, my + mr * 0.7), (mx + mr * 1.5, my + mr * 1.5)], fill=GOLD, width=18)


def _motif_listen_gap(base, d, cx, cy, accent):
    """Headphones above a gapped sentence line (Hörlücke)."""
    _motif_headphones(base, d, cx, cy - 150, accent)
    _gap_line(d, cx, cy + 210, 720, GOLD)


def _motif_search_pic(base, d, cx, cy, accent):
    """A picture frame with small objects + a magnifier/crosshair (Finde im Bild)."""
    fw, fh = 560, 380
    x0, y0 = cx - fw / 2, cy - fh / 2
    d.rounded_rectangle([x0, y0, x0 + fw, y0 + fh], radius=28, fill=(30, 41, 59), outline=accent, width=8)
    # a few simple "objects" in the scene
    d.rounded_rectangle([x0 + 60, y0 + fh - 150, x0 + 200, y0 + fh - 40], radius=16, fill=(90, 104, 130))
    d.ellipse([x0 + 250, y0 + 70, x0 + 340, y0 + 160], fill=(120, 134, 160))
    d.rounded_rectangle([x0 + fw - 190, y0 + fh - 130, x0 + fw - 70, y0 + fh - 40], radius=12, fill=(110, 124, 150))
    # small highlighted target object
    tx, ty = x0 + fw - 150, y0 + 110
    d.rounded_rectangle([tx - 34, ty - 34, tx + 34, ty + 34], radius=10, fill=GOLD)
    # magnifier over the target
    mr = 92
    d.ellipse([tx - mr, ty - mr, tx + mr, ty + mr], outline=GOLD, width=14)
    d.line([(tx + mr * 0.7, ty + mr * 0.7), (tx + mr * 1.5, ty + mr * 1.5)], fill=GOLD, width=20)


def _motif_relation(base, d, cx, cy, accent, symbol):
    """Two word bubbles joined by a relation symbol (Synonym '=', Antonym '↔')."""
    bw, bh = 360, 150
    _tile(d, cx - bw / 2 - 70, cy, bw, bh, "Wort A", accent, fs=46, radius=34)
    _tile(d, cx + bw / 2 + 70, cy, bw, bh, "Wort B",
          tuple(min(255, c + 26) for c in accent), fs=46, radius=34)
    d.ellipse([cx - 60, cy - 60, cx + 60, cy + 60], fill=INK, outline=GOLD, width=8)
    _ctext(d, cx, cy - 44, symbol, _font(76), GOLD)


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


def _card(*, badge, title, subtitle, accent, motif, cta) -> bytes | None:
    if Image is None:
        return None
    base = _base(accent)
    d = ImageDraw.Draw(base)
    _header(base, d, badge=badge, title=title, subtitle=subtitle, accent=accent)
    motif(base, d, W // 2, 640, accent)
    _footer_cta(d, cta, GOLD)
    return _finish(base)


def render_anagram_card(*, level: str = "B2+") -> bytes | None:
    return _card(badge="WORTRÄTSEL", title="Anagramm", subtitle=f"Buchstabensalat  ·  {level}",
                 accent=(167, 139, 250), motif=_motif_anagram, cta="Setz die Buchstaben zusammen")


def render_cloze_card(*, level: str = "B2+") -> bytes | None:
    return _card(badge="LÜCKE", title="Lückentext", subtitle=f"Grammatik  ·  {level}",
                 accent=(56, 189, 248),
                 motif=lambda b, d, cx, cy, a: _gap_line(d, cx, cy, 760, a),
                 cta="Fülle die Lücke")


def render_wortbildung_card(*, level: str = "B2+") -> bytes | None:
    return _card(badge="WORTBILDUNG", title="Wortbildung", subtitle=f"Wortformen  ·  {level}",
                 accent=(45, 212, 191), motif=_motif_blocks, cta="Bilde die richtige Form")


def render_transform_card(*, level: str = "C1") -> bytes | None:
    return _card(badge="UMFORMEN", title="Satztransformation", subtitle=f"Umformung  ·  {level}",
                 accent=(129, 140, 248), motif=_motif_transform, cta="Forme den Satz um")


def render_error_card(*, level: str = "B2+") -> bytes | None:
    return _card(badge="FEHLER", title="Fehler finden", subtitle=f"Korrektur  ·  {level}",
                 accent=(248, 113, 113), motif=_motif_error, cta="Finde & korrigiere den Fehler")


def render_hoerluecke_card(*, level: str = "B2+") -> bytes | None:
    return _card(badge="HÖREN", title="Hörlücke", subtitle=f"Hören + Grammatik  ·  {level}",
                 accent=(34, 211, 238), motif=_motif_listen_gap, cta="Höre & ergänze die Wörter")


def render_pin_card(*, level: str = "B2") -> bytes | None:
    return _card(badge="SUCHEN", title="Finde im Bild", subtitle=f"Wortschatz  ·  {level}",
                 accent=(251, 146, 60), motif=_motif_search_pic, cta="Tippe auf das Objekt")


def render_synonym_card(*, level: str = "B2+") -> bytes | None:
    return _card(badge="WORTSCHATZ", title="Synonym", subtitle=f"Bedeutung  ·  {level}",
                 accent=(52, 211, 153),
                 motif=lambda b, d, cx, cy, a: _motif_relation(b, d, cx, cy, a, "="),
                 cta="Finde ein Synonym")


def render_antonym_card(*, level: str = "B2+") -> bytes | None:
    return _card(badge="WORTSCHATZ", title="Antonym", subtitle=f"Gegenteil  ·  {level}",
                 accent=(244, 114, 182),
                 motif=lambda b, d, cx, cy, a: _motif_relation(b, d, cx, cy, a, "↔"),
                 cta="Finde das Gegenteil")


def render_sprint_relation_card(relation: str) -> bytes | None:
    """Card for the 60-second synonym/antonym SPRINT race (distinct from the
    single-word synonym/antonym aufgabe). relation = 'synonym' | 'antonym'."""
    is_syn = relation == "synonym"
    return _card(
        badge="TEMPO-SPIEL",
        title="Synonym-Sprint" if is_syn else "Antonym-Sprint",
        subtitle="60 Sekunden  ·  " + ("möglichst viele" if is_syn else "Gegenteile"),
        accent=(52, 211, 153) if is_syn else (244, 114, 182),
        motif=(lambda b, d, cx, cy, a: _motif_relation(b, d, cx, cy, a, "=" if is_syn else "↔")),
        cta="Wer findet die meisten?",
    )
