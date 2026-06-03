"""
Rebus image generation and compositing.

Flow per compound word:
  1. For each part word: generate DALL-E image → cache in R2 + DB
  2. Download both component images from R2
  3. Compose dark rebus card with PIL
  4. Upload composed card to R2 → mark compound as ready in DB
"""
from __future__ import annotations

import io
import logging
import random
import time

# ─── PIL ──────────────────────────────────────────────────────────────────────
try:
    from PIL import Image, ImageDraw, ImageFont, ImageFilter
    _PIL_AVAILABLE = True
except ImportError:
    _PIL_AVAILABLE = False

# ─── Card dimensions ──────────────────────────────────────────────────────────
CARD_W = 1024
CARD_H = 480
PANEL_SIZE = 320        # each component image panel (square)
PANEL_PAD = 20          # padding inside white panel
CORNER_R = 24           # rounded corner radius for panels
SHADOW_BLUR = 12

# Dark background colour (deep navy)
BG_COLOR = (15, 17, 35)
# Panel card background
PANEL_BG = (255, 255, 255)
# Accent / plus sign colour
ACCENT_COLOR = (255, 220, 80)       # warm yellow
TEXT_COLOR = (240, 240, 255)        # near-white
QUESTION_COLOR = (180, 190, 220)    # soft lavender

FONT_SIZE_PLUS = 96
FONT_SIZE_QUESTION = 28
FONT_SIZE_LABEL = 22


def _object_key_component(word: str) -> str:
    safe = "".join(c if c.isalnum() or c in "-_" else "_" for c in word)
    return f"rebus/components/{safe}.png"


def _object_key_composed(compound_id: str) -> str:
    safe = "".join(c if c.isalnum() or c in "-_" else "_" for c in compound_id)
    return f"rebus/composed/{safe}.png"


# ─── Step 1: generate one component image ────────────────────────────────────

def generate_component_image(word: str, dalle_prompt: str) -> str:
    """
    Generate a DALL-E image for a single component word, cache in R2 + DB.
    Returns the R2 object_key. Raises on failure.
    """
    from backend.database import get_rebus_component_image, upsert_rebus_component_image
    from backend.image_generation_provider import generate_image_bytes
    from backend.r2_storage import r2_put_bytes

    # Already done?
    existing = get_rebus_component_image(word)
    if existing and existing.get("generation_status") == "ready" and existing.get("image_object_key"):
        return str(existing["image_object_key"])

    logging.info("rebus_generator: generating component image word=%s", word)
    upsert_rebus_component_image(word, generation_status="pending")

    try:
        result = generate_image_bytes(
            prompt=dalle_prompt,
            template_id=0,          # system-level, no template_id
            user_id=0,
        )
        img_bytes = bytes(result.get("data") or b"")
        mime = str(result.get("mime_type") or "image/png").strip() or "image/png"
        if not img_bytes:
            raise RuntimeError("empty image payload")

        ext = "png" if "png" in mime else "webp" if "webp" in mime else "png"
        object_key = _object_key_component(word).replace(".png", f".{ext}")
        r2_put_bytes(
            object_key,
            img_bytes,
            content_type=mime,
            cache_control="public, max-age=31536000, immutable",
        )
        upsert_rebus_component_image(word, image_object_key=object_key, generation_status="ready")
        logging.info("rebus_generator: component ready word=%s key=%s bytes=%s", word, object_key, len(img_bytes))
        return object_key

    except Exception as exc:
        upsert_rebus_component_image(word, generation_status="failed", failure_reason=str(exc)[:500])
        raise


# ─── Step 2: PIL card composition ─────────────────────────────────────────────

def _load_image_from_r2(object_key: str) -> "Image.Image":
    from backend.r2_storage import r2_get_bytes
    raw = r2_get_bytes(object_key)
    if not raw:
        raise RuntimeError(f"r2_get_bytes returned None for {object_key}")
    return Image.open(io.BytesIO(raw)).convert("RGBA")


def _rounded_rect_mask(size: tuple[int, int], radius: int) -> "Image.Image":
    """Create a mask with rounded corners (white=visible, black=transparent)."""
    mask = Image.new("L", size, 0)
    draw = ImageDraw.Draw(mask)
    draw.rounded_rectangle([(0, 0), (size[0] - 1, size[1] - 1)], radius=radius, fill=255)
    return mask


def _paste_with_shadow(
    canvas: "Image.Image",
    panel_img: "Image.Image",
    xy: tuple[int, int],
    shadow_offset: int = 6,
    shadow_blur: int = SHADOW_BLUR,
) -> None:
    """Paste a panel image onto canvas with a soft drop shadow."""
    shadow = Image.new("RGBA", (panel_img.width + shadow_blur * 2, panel_img.height + shadow_blur * 2), (0, 0, 0, 0))
    shadow_draw = ImageDraw.Draw(shadow)
    shadow_draw.rectangle(
        [shadow_blur, shadow_blur, shadow.width - shadow_blur, shadow.height - shadow_blur],
        fill=(0, 0, 0, 120),
    )
    shadow = shadow.filter(ImageFilter.GaussianBlur(shadow_blur // 2))
    sx = xy[0] - shadow_blur + shadow_offset
    sy = xy[1] - shadow_blur + shadow_offset
    canvas.paste(shadow, (sx, sy), shadow)
    canvas.paste(panel_img, xy, panel_img)


def _make_component_panel(component_img: "Image.Image", size: int = PANEL_SIZE) -> "Image.Image":
    """
    Fit component image into a white rounded square panel.
    Returns RGBA image of size (size, size).
    """
    panel = Image.new("RGBA", (size, size), (*PANEL_BG, 255))

    # Fit component image inside with padding
    inner = size - PANEL_PAD * 2
    comp = component_img.convert("RGBA")
    comp.thumbnail((inner, inner), Image.LANCZOS)

    # Centre in panel
    ox = (size - comp.width) // 2
    oy = (size - comp.height) // 2
    panel.paste(comp, (ox, oy), comp)

    # Apply rounded-corner mask
    mask = _rounded_rect_mask((size, size), CORNER_R)
    panel.putalpha(mask)
    return panel


def _get_font(size: int, bold: bool = False):
    """Try to load a clean system font; fall back to default."""
    candidates = [
        "/System/Library/Fonts/Supplemental/Arial Bold.ttf" if bold else "/System/Library/Fonts/Supplemental/Arial.ttf",
        "/System/Library/Fonts/Helvetica.ttc",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
    ]
    for path in candidates:
        try:
            return ImageFont.truetype(path, size)
        except Exception:
            continue
    return ImageFont.load_default()


def compose_rebus_card(
    component_key_1: str,
    component_key_2: str,
    *,
    word1: str = "",
    word2: str = "",
) -> bytes:
    """
    Download two component images from R2, compose a dark rebus card.
    Returns PNG bytes of the finished card.
    Layout:

      ┌──────────────────────────────────────────────┐
      │  🧩 Deutsches Rätsel             [difficulty]│
      │                                              │
      │   ┌──────────┐    +    ┌──────────┐   = ?   │
      │   │  img 1   │         │  img 2   │         │
      │   └──────────┘         └──────────┘         │
      │                                              │
      │        Was ergibt das zusammen?              │
      └──────────────────────────────────────────────┘
    """
    if not _PIL_AVAILABLE:
        raise RuntimeError("Pillow is not installed — cannot compose rebus card")

    img1 = _load_image_from_r2(component_key_1)
    img2 = _load_image_from_r2(component_key_2)

    canvas = Image.new("RGBA", (CARD_W, CARD_H), (*BG_COLOR, 255))

    # Subtle gradient overlay (lighter at top)
    gradient = Image.new("RGBA", (CARD_W, CARD_H), (0, 0, 0, 0))
    for y in range(CARD_H):
        alpha = int(30 * (1 - y / CARD_H))
        for x in range(CARD_W):
            gradient.putpixel((x, y), (255, 255, 255, alpha))
    canvas = Image.alpha_composite(canvas, gradient)

    draw = ImageDraw.Draw(canvas)

    # ── Header label ────────────────────────────────────────────────────────
    font_label = _get_font(FONT_SIZE_LABEL, bold=False)
    header_text = "🧩  Deutsches Rätsel"
    draw.text((36, 22), header_text, font=font_label, fill=(*TEXT_COLOR, 200))

    # ── Component panels ────────────────────────────────────────────────────
    panel1 = _make_component_panel(img1, PANEL_SIZE)
    panel2 = _make_component_panel(img2, PANEL_SIZE)

    # Vertical centre of panels
    panels_top = (CARD_H - PANEL_SIZE) // 2 + 10   # slight downward offset for header

    # Horizontal layout: [margin] [panel1] [gap] [+] [gap] [panel2] [gap] [=?] [margin]
    margin_x = 52
    plus_zone = 100     # width reserved for "+"
    eq_zone = 90        # width reserved for "= ?"
    gap = 30

    p1_x = margin_x
    plus_x = p1_x + PANEL_SIZE + gap
    p2_x = plus_x + plus_zone + gap
    eq_x = p2_x + PANEL_SIZE + gap

    # If total width overflows, compress gaps
    total = eq_x + eq_zone + margin_x
    if total > CARD_W:
        shrink = (total - CARD_W) // 3
        gap = max(10, gap - shrink)
        plus_x = p1_x + PANEL_SIZE + gap
        p2_x = plus_x + plus_zone + gap
        eq_x = p2_x + PANEL_SIZE + gap

    _paste_with_shadow(canvas, panel1, (p1_x, panels_top))
    _paste_with_shadow(canvas, panel2, (p2_x, panels_top))

    # ── "+" sign ───────────────────────────────────────────────────────────
    font_plus = _get_font(FONT_SIZE_PLUS, bold=True)
    plus_center_x = plus_x + plus_zone // 2
    plus_center_y = panels_top + PANEL_SIZE // 2
    draw.text(
        (plus_center_x, plus_center_y),
        "+",
        font=font_plus,
        fill=(*ACCENT_COLOR, 255),
        anchor="mm",
    )

    # ── "= ?" sign ─────────────────────────────────────────────────────────
    font_eq = _get_font(FONT_SIZE_PLUS - 10, bold=True)
    eq_center_x = eq_x + eq_zone // 2
    eq_center_y = panels_top + PANEL_SIZE // 2
    draw.text(
        (eq_center_x, eq_center_y),
        "= ?",
        font=font_eq,
        fill=(*TEXT_COLOR, 255),
        anchor="mm",
    )

    # ── Bottom question ─────────────────────────────────────────────────────
    font_q = _get_font(FONT_SIZE_QUESTION, bold=False)
    q_text = "Was ergibt das zusammen?"
    draw.text(
        (CARD_W // 2, CARD_H - 36),
        q_text,
        font=font_q,
        fill=(*QUESTION_COLOR, 210),
        anchor="mm",
    )

    # ── Render to PNG bytes ─────────────────────────────────────────────────
    out = io.BytesIO()
    canvas.convert("RGB").save(out, format="PNG", optimize=True)
    return out.getvalue()


# ─── Step 3: full pipeline for one compound ───────────────────────────────────

def prepare_rebus_entry(compound_id: str) -> dict:
    """
    Full pipeline: generate component images → compose card → upload → mark ready.
    Returns {"status": "ready"|"failed", "compound_id": ..., "object_key": ...}
    """
    from backend.database import (
        get_rebus_bank_entry,
        mark_rebus_composed,
        mark_rebus_compose_failed,
        get_rebus_component_image,
    )
    from backend.rebus_bank import COMPONENT_IMAGE_PROMPTS
    from backend.r2_storage import r2_put_bytes

    entry = get_rebus_bank_entry(compound_id)
    if not entry:
        return {"status": "failed", "compound_id": compound_id, "reason": "not_in_db"}

    if entry.get("composed_status") == "ready" and entry.get("composed_image_object_key"):
        return {"status": "ready", "compound_id": compound_id, "object_key": entry["composed_image_object_key"]}

    parts = entry.get("parts") or []
    if len(parts) < 2:
        mark_rebus_compose_failed(compound_id)
        return {"status": "failed", "compound_id": compound_id, "reason": "parts_missing"}

    try:
        # Generate component images (cached if already done)
        component_keys: list[str] = []
        for part in parts[:2]:
            word = str(part.get("word") or "").strip()
            if not word:
                raise ValueError(f"empty part word in {compound_id}")
            prompt = str(COMPONENT_IMAGE_PROMPTS.get(word) or "").strip()
            if not prompt:
                raise ValueError(f"no DALL-E prompt for component word '{word}'")
            key = generate_component_image(word, prompt)
            component_keys.append(key)

        # Compose the card
        card_bytes = compose_rebus_card(
            component_keys[0],
            component_keys[1],
            word1=str((parts[0].get("word") or "")),
            word2=str((parts[1].get("word") or "")),
        )

        composed_key = _object_key_composed(compound_id)
        r2_put_bytes(
            composed_key,
            card_bytes,
            content_type="image/png",
            cache_control="public, max-age=86400",
        )
        mark_rebus_composed(compound_id, image_object_key=composed_key)
        logging.info(
            "rebus_generator: composed ready compound_id=%s key=%s bytes=%s",
            compound_id, composed_key, len(card_bytes),
        )
        return {"status": "ready", "compound_id": compound_id, "object_key": composed_key}

    except Exception as exc:
        logging.warning("rebus_generator: prepare_rebus_entry failed compound_id=%s: %s", compound_id, exc, exc_info=True)
        mark_rebus_compose_failed(compound_id)
        return {"status": "failed", "compound_id": compound_id, "reason": str(exc)[:300]}


def prepare_rebus_pool(*, target_ready: int = 20, max_attempts: int = 30) -> dict:
    """
    Ensure at least `target_ready` composed rebuses exist.
    Iterates over pending bank entries, generates up to `max_attempts`.
    Returns stats dict.
    """
    from backend.database import count_available_rebuses, sync_rebus_bank_from_code
    import psycopg2

    # Sync Python bank → DB (idempotent upsert)
    sync_stats = sync_rebus_bank_from_code()
    logging.info("rebus_generator: bank synced %s", sync_stats)

    already_ready = count_available_rebuses()
    if already_ready >= target_ready:
        return {"status": "sufficient", "ready": already_ready, "generated": 0}

    need = max(0, target_ready - already_ready)
    generated = 0
    failed = 0
    attempts = 0

    # Get pending entries ordered by compound_id
    with __import__("backend.database", fromlist=["get_db_connection_context"]).get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT compound_id FROM bt_3_rebus_bank
                WHERE composed_status IN ('pending', 'failed')
                  AND retired = FALSE
                ORDER BY compound_id
                LIMIT %s
                """,
                (int(max_attempts),),
            )
            rows = cur.fetchall() or []

    pending_ids = [str(r[0]) for r in rows if r[0]]
    random.shuffle(pending_ids)   # mix difficulties

    for cid in pending_ids:
        if generated >= need or attempts >= max_attempts:
            break
        attempts += 1
        try:
            result = prepare_rebus_entry(cid)
            if result.get("status") == "ready":
                generated += 1
            else:
                failed += 1
            time.sleep(1.5)  # rate-limit DALL-E calls
        except Exception:
            failed += 1
            logging.warning("rebus_generator: pool prep failed for %s", cid, exc_info=True)

    return {
        "status": "done",
        "ready_before": already_ready,
        "generated": generated,
        "failed": failed,
        "attempts": attempts,
    }
