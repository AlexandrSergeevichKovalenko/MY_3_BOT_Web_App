"""Пригласительная карточка с Феликсом — картинка, которой делятся с друзьями.

Раньше приглашение уходило голой ссылкой: «https://…/tour?ref=123» плюс строка
текста. Это не продаёт продукт и не выглядит как что-то, чем хочется поделиться.
Здесь рисуется брендовая карточка (кремовый градиент + амбер-акцент — как в
Mini-App, а не тёмно-синий кит постеров) с Феликсом и тремя фактами о боте.

Ключевое решение: карточка ОДНА И ТА ЖЕ для всех. Персональный ref НЕ рисуется
внутри картинки — он живёт в кнопке и в тексте рядом. Поэтому PNG рендерится
один раз, кладётся в R2 и дальше просто раздаётся ссылкой; иначе пришлось бы
генерить и хранить по картинке на каждого пользователя.

Два варианта:
  • "tg"  — для inline-режима в Telegram: под фото будет живая inline-кнопка,
            поэтому на самой картинке нарисован CTA-пилюля без адреса.
  • "web" — для превью ссылки (Open Graph) в других мессенджерах: кнопки там
            не будет никогда, поэтому на картинке видно, куда идти.

Эмодзи внутрь PNG не рисуем — на сервере нет цветного эмодзи-шрифта (см.
bot_3.py, отправка грамоты). Галочки векторные, что совпадает с нашим стилем.
"""

from __future__ import annotations

import logging
from io import BytesIO

try:
    from PIL import Image, ImageDraw, ImageFilter
except Exception:  # pragma: no cover
    Image = None

from backend.champion_poster import _font

logger = logging.getLogger(__name__)

W = H = 1080

CREAM_TOP = (255, 250, 242)
CREAM_BOT = (240, 224, 199)
INK = (35, 28, 20)
INK_SOFT = (104, 88, 70)
AMBER = (214, 150, 46)
AMBER_SOFT = (245, 158, 11)
AMBER_DEEP = (150, 106, 30)
GREEN = (31, 81, 55)
PAPER = (255, 253, 248)

HERO_R2_KEY = "brand/felix_original.png"

# Версия в ключе: поменял текст или композицию — подними, иначе в R2 останется старая.
CARD_VERSION = "v1"
_CACHE_KEY = "share/invite_card_{version}_{variant}.png"

EYEBROW = "НЕМЕЦКИЙ С ФЕЛИКСОМ"
HEADLINE = "Со мной немецкий идёт легче"
BULLETS = (
    "Разбор любого слова — за секунду",
    "Тренажёры вместо зубрёжки",
    "YouTube с двойными субтитрами",
)
CTA = "Пройди короткий тур"


def _lerp(a, b, t):
    return tuple(int(a[i] + (b[i] - a[i]) * t) for i in range(3))


def _base() -> "Image.Image":
    img = Image.new("RGB", (W, H))
    d = ImageDraw.Draw(img)
    for y in range(H):
        d.line([(0, y), (W, y)], fill=_lerp(CREAM_TOP, CREAM_BOT, y / H))
    img = img.convert("RGBA")
    for cx, cy, r, alpha in ((int(W * 0.86), int(H * 0.18), 300, 46),
                             (int(W * 0.10), int(H * 0.92), 260, 30)):
        ov = Image.new("RGBA", (W, H), (0, 0, 0, 0))
        ImageDraw.Draw(ov).ellipse([cx - r, cy - r, cx + r, cy + r], fill=AMBER_SOFT + (alpha,))
        img.alpha_composite(ov.filter(ImageFilter.GaussianBlur(r // 2)))
    return img


def _check(d, x, y, size, color, width):
    """Векторная галочка вместо эмодзи ✅."""
    d.line([(x, y + size * 0.52), (x + size * 0.36, y + size * 0.86)], fill=color, width=width)
    d.line([(x + size * 0.36, y + size * 0.86), (x + size * 0.96, y + size * 0.12)],
           fill=color, width=width)


def _wrap(d, text, font, max_w):
    words, lines, cur = str(text or "").split(), [], ""
    for word in words:
        trial = (cur + " " + word).strip()
        if d.textlength(trial, font=font) <= max_w:
            cur = trial
        else:
            if cur:
                lines.append(cur)
            cur = word
    if cur:
        lines.append(cur)
    return lines


def _load_hero():
    """Феликс лежит в R2 (его генерит hero_images.py). Нет — рисуем карточку без него."""
    try:
        from backend.r2_storage import r2_get_bytes
        data = r2_get_bytes(HERO_R2_KEY)
        if data:
            return Image.open(BytesIO(data)).convert("RGBA")
    except Exception as exc:
        logger.warning("share_card: hero unavailable (%s), rendering without it", exc)
    return None


def render_share_card(variant: str = "tg", bot_username: str = "") -> bytes | None:
    """Отрисовать карточку. Возвращает PNG-байты либо None, если Pillow недоступен."""
    if Image is None:
        logger.warning("share_card: Pillow unavailable")
        return None

    img = _base()
    d = ImageDraw.Draw(img)
    pad = 78
    y = 70

    f_eyebrow = _font(29)
    eb_w = int(d.textlength(EYEBROW, font=f_eyebrow)) + 52
    d.rounded_rectangle([pad, y, pad + eb_w, y + 58], radius=29,
                        fill=(255, 255, 255, 190), outline=AMBER + (150,), width=2)
    d.text((pad + 26, y + 15), EYEBROW, font=f_eyebrow, fill=AMBER_DEEP)
    y += 104

    f_h1 = _font(82)
    for line in _wrap(d, HEADLINE, f_h1, W - pad * 2 - 20):
        d.text((pad, y), line, font=f_h1, fill=INK)
        y += 94
    y += 30

    # Феликс справа; колонка текста заканчивается до него, иначе строки наезжают.
    hero = _load_hero()
    hero_size = 470
    hero_x = W - hero_size - 16
    hero_y = H - hero_size - (128 if variant == "tg" else 150)
    text_col = hero_x - pad - 24 if hero is not None else W - pad * 2

    f_item = _font(37, bold=False)
    for bullet in BULLETS:
        _check(d, pad + 4, y + 2, 38, GREEN, 9)
        for i, line in enumerate(_wrap(d, bullet, f_item, text_col - 66)):
            d.text((pad + 64, y + i * 46), line, font=f_item, fill=INK_SOFT)
            if i:
                y += 46
        y += 72

    if hero is not None:
        hero = hero.resize((hero_size, hero_size), Image.LANCZOS)
        shadow = Image.new("RGBA", (W, H), (0, 0, 0, 0))
        ImageDraw.Draw(shadow).ellipse(
            [hero_x + 90, hero_y + hero_size - 74, hero_x + hero_size - 60, hero_y + hero_size + 6],
            fill=(120, 92, 52, 78))
        img.alpha_composite(shadow.filter(ImageFilter.GaussianBlur(26)))
        img.alpha_composite(hero, (hero_x, hero_y))
        d = ImageDraw.Draw(img)

    if variant == "tg":
        # В Telegram под фото будет настоящая inline-кнопка — рисуем её визуальный двойник.
        f_cta = _font(38)
        cta_w = int(d.textlength(CTA, font=f_cta)) + 88
        d.rounded_rectangle([pad, H - 172, pad + cta_w, H - 82], radius=45, fill=AMBER)
        d.text((pad + 44, H - 147), CTA, font=f_cta, fill=PAPER)
    else:
        # Вне Telegram кнопки не будет — человек должен видеть, куда идти.
        f_note = _font(29, bold=False)
        f_url = _font(35)
        handle = f"t.me/{str(bot_username or '').lstrip('@')}".rstrip("/")
        d.text((pad, H - 186), "Открой короткий тур:", font=f_note, fill=INK_SOFT)
        d.text((pad, H - 138), handle, font=f_url, fill=AMBER_DEEP)
        d.line([(pad, H - 82), (pad + int(d.textlength(handle, font=f_url)), H - 82)],
               fill=AMBER + (120,), width=4)

    buf = BytesIO()
    img.convert("RGB").save(buf, format="PNG", optimize=True)
    return buf.getvalue()


_URL_MEMO: dict[str, str] = {}


def share_card_url(variant: str = "tg", bot_username: str = "", *, force: bool = False) -> str | None:
    """Публичный URL карточки в R2; рендерит и заливает при первом обращении.

    Картинка общая для всех пользователей, поэтому это ровно один рендер на
    вариант за всё время жизни версии. Блокирующая функция — из асинхронного
    кода вызывать через asyncio.to_thread.

    URL запоминается в памяти процесса: страницу /tour дёргают при КАЖДОЙ вставке
    ссылки в мессенджер, и ходить за этим в R2 каждый раз — лишний сетевой круг
    прямо в отрисовке страницы.
    """
    key = _CACHE_KEY.format(version=CARD_VERSION, variant=variant)
    if not force:
        memo = _URL_MEMO.get(key)
        if memo:
            return memo
    try:
        from backend.r2_storage import r2_exists, r2_public_url, r2_put_bytes
    except Exception as exc:
        logger.warning("share_card: R2 unavailable (%s)", exc)
        return None
    try:
        if not force and r2_exists(key):
            _URL_MEMO[key] = r2_public_url(key)
            return _URL_MEMO[key]
        png = render_share_card(variant, bot_username=bot_username)
        if not png:
            return None
        r2_put_bytes(key, png, content_type="image/png",
                     cache_control="public, max-age=86400")
        logger.info("share_card: uploaded %s (%d bytes)", key, len(png))
        _URL_MEMO[key] = r2_public_url(key)
        return _URL_MEMO[key]
    except Exception as exc:
        logger.warning("share_card: could not publish %s (%s)", key, exc)
        return None
