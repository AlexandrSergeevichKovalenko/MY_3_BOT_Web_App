"""
Карточка напоминания «бесплатный месяц закончился» (PIL).

Раз в неделю (8 недель, дальше раз в месяц) запертому человеку уходит эта картинка с
двумя кнопками под ней: «Лайт» и «Полный доступ». Решение владельца 04.09.2026,
стратегия — docs/tasks/light_tier_strategy.md §7. Те же примитивы бренда, что у
остальных карточек (article_quiz_card); фон — синий градиент, без чужих ассетов.
"""
from __future__ import annotations

import io

from backend.article_quiz_card import _font, _ctext, _fit_font, _vgrad, _glow, W, H

WHITE = (255, 255, 255)


def render_access_reminder_card(*, light_stars: int, pro_stars: int) -> bytes:
    """Одна картинка на всех: цены — те же, что спишет счёт (light_price_stars /
    pro_price_stars). Возвращает PNG."""
    from PIL import Image, ImageDraw

    light_stars = int(light_stars or 0)
    pro_stars = int(pro_stars or 0)

    base = _vgrad((58, 123, 213), (31, 79, 156)).convert("RGBA")
    ov = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    od = ImageDraw.Draw(ov)
    for y in range(H):
        a = 40 if y < 520 else int(40 + 90 * min(1.0, (y - 520) / (H - 520)))
        od.line([(0, y), (W, y)], fill=(8, 20, 48, a))
    base = Image.alpha_composite(base, ov)
    d = ImageDraw.Draw(base)

    pill = "МЕСЯЦ С НАМИ"
    pf = _font(44, True)
    pw = d.textlength(pill, font=pf)
    pv = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    ImageDraw.Draw(pv).rounded_rectangle(
        [W // 2 - pw // 2 - 44, 92, W // 2 + pw // 2 + 44, 180], radius=44, fill=(0, 0, 0, 110),
    )
    base.paste(pv, (0, 0), pv)
    _ctext(d, W // 2, 108, pill, pf, (255, 255, 255, 240))

    _glow(base, W // 2, 420, 320, 50)
    head = "Ты позанимался месяц!"
    hf = _fit_font(d, head, W - 120, 92, bold=True, floor=60)
    _ctext(d, W // 2 + 4, 344, head, hf, (0, 0, 0, 100))
    _ctext(d, W // 2, 340, head, hf, WHITE)
    sub = "Чтобы задания приходили дальше, выбери тариф"
    sf = _fit_font(d, sub, W - 140, 42, bold=False, floor=28)
    _ctext(d, W // 2, 460, sub, sf, (230, 240, 255, 230))

    # Две плашки тарифов
    def box(y, title, price, line1, line2, fill):
        bv = Image.new("RGBA", (W, H), (0, 0, 0, 0))
        ImageDraw.Draw(bv).rounded_rectangle([90, y, W - 90, y + 190], radius=34, fill=fill)
        base.paste(bv, (0, 0), bv)
        dd = ImageDraw.Draw(base)
        tf = _font(54, True)
        dd.text((130, y + 26), title, font=tf, fill=WHITE)
        pf2 = _font(54, True)
        pw2 = dd.textlength(price, font=pf2)
        dd.text((W - 130 - pw2, y + 26), price, font=pf2, fill=(255, 224, 130))
        lf = _fit_font(dd, line1, W - 260, 34, bold=False, floor=24)
        dd.text((130, y + 104), line1, font=lf, fill=(235, 242, 255, 230))
        lf2 = _fit_font(dd, line2, W - 260, 34, bold=False, floor=24)
        dd.text((130, y + 144), line2, font=lf2, fill=(235, 242, 255, 210))

    box(560, "Лайт", f"{light_stars} ⭐ / мес" if light_stars else "",
        "Тот же объём, что был в бесплатный месяц:", "6 заданий в день, словарь, карточки, тренажёры",
        (16, 140, 110, 200))
    box(780, "Полный доступ", f"{pro_stars} ⭐ / мес" if pro_stars else "",
        "Все функции открыты: переводы, разборы, субтитры,", "аналитика, свои книги, до 20 заданий в день",
        (91, 52, 213, 200))

    _ctext(d, W // 2, 1012, "Deutsche Sprache · оплата звёздами Telegram, отмена в любой момент",
           _font(28, False), (220, 232, 250, 170))

    out = io.BytesIO()
    base.convert("RGB").save(out, format="PNG", optimize=True)
    return out.getvalue()
