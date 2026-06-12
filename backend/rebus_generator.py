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

def generate_component_image(word: str, dalle_prompt: str, *,
                             meaning_ru: str = "", forbid: str = "") -> str:
    """
    Generate a DALL-E image for a single component word, cache in R2 + DB.
    Returns the R2 object_key. Raises on failure.

    A vision gate verifies the image actually depicts `word` (its plain literal
    meaning) and does not reveal `forbid` (the compound answer) before caching —
    so wrong-object images (e.g. a pine cone for "Konus") never reach players.
    """
    from backend.database import get_rebus_component_image, upsert_rebus_component_image
    from backend.image_generation_provider import generate_image_bytes
    from backend.openai_manager import run_image_depicts
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

        # Vision gate (pool time, off the hot path): the image must clearly show
        # the part word and must not reveal the compound answer.
        label = f"{word} ({meaning_ru})" if meaning_ru else word
        verdict = run_image_depicts(img_bytes, label, forbid=forbid, mime=mime)
        if not verdict.get("ok"):
            reason = str(verdict.get("reason") or "vision_rejected")
            upsert_rebus_component_image(word, generation_status="failed", failure_reason=f"vision: {reason}"[:500])
            raise RuntimeError(f"vision rejected component '{word}': {reason}")

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
        compound_word = str(entry.get("compound") or "").strip()
        for part in parts[:2]:
            word = str(part.get("word") or "").strip()
            if not word:
                raise ValueError(f"empty part word in {compound_id}")
            # Static bank first, then DB-stored prompt (for GPT-generated entries)
            prompt = str(COMPONENT_IMAGE_PROMPTS.get(word) or "").strip()
            if not prompt:
                cached = get_rebus_component_image(word)
                prompt = str((cached or {}).get("dalle_prompt") or "").strip()
            if not prompt:
                raise ValueError(f"no DALL-E prompt for component word '{word}'")
            key = generate_component_image(
                word, prompt,
                meaning_ru=str(part.get("meaning_ru") or ""),
                forbid=compound_word,
            )
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


# ─── GPT replenishment ────────────────────────────────────────────────────────

_REPLENISHMENT_STYLE = (
    "children's book illustration style, soft watercolor, vibrant colors, "
    "clean white background, single object centered, no text, no labels, "
    "no other objects, high clarity, detailed"
)

_REPLENISHMENT_SYSTEM = """You are a German linguistics expert specializing in Komposita (compound nouns).
Generate German compound word entries for a visual rebus puzzle game.

STRICT requirements for EVERY entry:
1. EXACTLY 2 component parts, each a standalone German noun with a clear visual form
2. Both parts must be concrete, drawable PHYSICAL OBJECTS (e.g. Hand ✓, Freude ✗, Mut ✗).
   ✗ FORBIDDEN as a part: numbers/quantities (ein/eins/zwei…), articles (der/die/das), pronouns, prepositions, verb prefixes (un-/ver-/vor-…) or any abstract concept.
   ✗ WRONG: Einhorn = "ein"(one) + "Horn" — "one" is a number, it cannot be drawn (DALL-E will draw a random object).
   ✓ RIGHT: Seepferd, Tischbein, Handschuh — every part is a tangible thing you can photograph.
3. The compound must be a real, standard German word (not archaic or regional)
4. wrong_options: exactly 3 real German compound words, each sharing EXACTLY ONE part with the answer
5. dalle_prompts: describe ONE concrete object on a plain white background — no text, no labels, no other objects
6. difficulty: A2=very common everyday, B1=intermediate everyday, B2=less common but standard
7. explanation_ru: etymology in Russian (e.g. "рука + обувь = перчатка — буквально «обувь для руки»")
8. CRITICAL — dalle_prompts must show the GENERIC LITERAL meaning of each component word IN ISOLATION, completely divorced from the compound context. The image must NOT hint at or reveal the compound answer.
   ✗ WRONG: compound=Angelrute, part=Rute → "A long fishing rod" (this IS Angelrute — reveals the answer!)
   ✓ RIGHT: compound=Angelrute, part=Rute → "A plain thin wooden rod or stick, bare, no fishing equipment"
   ✗ WRONG: compound=Hausschlüssel, part=Schlüssel → "A house key at a door" (context reveals the compound)
   ✓ RIGHT: compound=Hausschlüssel, part=Schlüssel → "A single metal key, plain, no door or lock visible"
   ✗ WRONG: compound=Schneeball, part=Schnee → "Children throwing snowballs" (reveals the activity)
   ✓ RIGHT: compound=Schneeball, part=Schnee → "A small pile of white snow"
   Rule of thumb: ask yourself — if someone sees ONLY this image with no context, could they guess the compound? If yes, simplify until they cannot.
8b. CRITICAL — the image must depict the part word's EXACT, PLAIN, DICTIONARY meaning — never a visually-similar but DIFFERENT object, and never the compound's referent itself.
   ✗ WRONG: part=Konus (geometric cone) → "a pine cone" (a pine cone is a Zapfen/Tannenzapfen, NOT a Konus — and it leaks a fir-cone compound).
   ✓ RIGHT: part=Konus → "a single plain geometric cone shape (like an orange traffic cone), solid color, white background".
   ✗ WRONG: part=Birne (pear, the fruit) when compound is Glühbirne → "a light bulb" (that IS Glühbirne). ✓ RIGHT: part=Birne → "a single green pear fruit".
   A vision model WILL reject the item if the image is the wrong object or reveals the answer — so make the prompt depict the literal word and nothing more. If a part word's literal image would itself be the compound's object, the entry is BAD — do not generate it.
9. CRITICAL — The dalle_prompt for part 1 must NOT visually contain the object depicted in part 2, and vice versa.
   If the parts are visually related (e.g., Brat=frying + Pfanne=pan), you MUST separate them:
   ✗ WRONG: Brat → "Meat sizzling in a frying pan" (shows the pan which IS part 2!)
   ✓ RIGHT: Brat → "A raw beef steak on a plain white cutting board, no pan visible"
   ✗ WRONG: Blumen=flowers + Strauß=bouquet → part 1 shows a bouquet (that IS the compound already)
   ✓ RIGHT: Blumen → "Three separate flowers lying flat on white background, not arranged into a bouquet"
   Always ask: does part 1's image contain the object shown in part 2? If yes, redesign the prompt."""

_REPLENISHMENT_USER_TMPL = """\
Generate {count} new German Komposita entries. Return ONLY valid JSON, no markdown.

ALREADY IN BANK — do NOT repeat: {existing_words}

Format (return exactly this structure):
{{
  "words": [
    {{
      "id": "handschuh",
      "compound": "Handschuh",
      "article": "der",
      "meaning_ru": "перчатка",
      "difficulty": "A2",
      "category": "Kleidung",
      "parts": [
        {{"word": "Hand", "meaning_ru": "рука"}},
        {{"word": "Schuh", "meaning_ru": "ботинок"}}
      ],
      "dalle_prompts": {{
        "Hand": "An open human hand, palm facing viewer, fingers spread, {style}",
        "Schuh": "A single classic leather shoe, side view, {style}"
      }},
      "wrong_options": ["Hausschuh", "Handtuch", "Handtasche"],
      "explanation_ru": "Hand (рука) + Schuh (ботинок) = Handschuh (перчатка) — буквально «обувь для руки»"
    }}
  ]
}}"""


def _call_gpt_for_replenishment(count: int, existing_words: list[str]) -> list[dict]:
    """Call GPT-4.1-mini to generate new compound entries. Returns validated list."""
    import json
    import os
    import requests as _requests

    api_key = (os.getenv("OPENAI_API_KEY") or "").strip()
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY not set")
    model = (os.getenv("OPENAI_QUIZ_MODEL") or os.getenv("OPENAI_MODEL") or "gpt-4.1-mini").strip()

    existing_str = ", ".join(sorted(existing_words)) if existing_words else "none"
    user_msg = _REPLENISHMENT_USER_TMPL.format(
        count=count, existing_words=existing_str, style=_REPLENISHMENT_STYLE
    )

    payload = {
        "model": model,
        "temperature": 0.7,
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": _REPLENISHMENT_SYSTEM},
            {"role": "user", "content": user_msg},
        ],
    }
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    resp = _requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers=headers,
        json=payload,
        timeout=60,
    )
    if not resp.ok:
        raise RuntimeError(f"OpenAI replenishment HTTP {resp.status_code}: {resp.text[:300]}")

    data = resp.json()
    raw = str((data.get("choices") or [{}])[0].get("message", {}).get("content") or "")
    try:
        parsed = json.loads(raw)
    except Exception as exc:
        raise RuntimeError(f"GPT replenishment JSON parse failed: {exc}") from exc

    words = parsed.get("words") if isinstance(parsed, dict) else None
    if not isinstance(words, list):
        raise RuntimeError("GPT replenishment: missing 'words' array in response")

    return words


# Parts that cannot be drawn as a single concrete object — numbers, articles,
# pronouns, prepositions and inseparable prefixes. A rebus needs BOTH parts to
# be depictable (e.g. Einhorn = "ein"(one) + "Horn" is invalid: "one" can't be
# drawn, so DALL-E renders a random object like a ring).
_NON_DEPICTABLE_PARTS = {
    "ein", "eine", "einen", "eins", "zwei", "drei",
    "der", "die", "das", "den", "dem", "des",
    "un", "vor", "nach", "mit", "ab", "an", "auf", "aus", "bei", "zu", "zur", "zum",
    "ueber", "über", "unter", "be", "ge", "ver", "er", "ent", "zer", "um", "durch",
    "ich", "du", "er", "sie", "es", "wir", "ihr", "man", "kein", "nicht", "sehr",
}


def _validate_replenishment_entry(entry: dict, existing_set: set[str]) -> str | None:
    """Return None if valid, or error string if invalid."""
    compound = str(entry.get("compound") or "").strip()
    if not compound:
        return "missing compound"
    if compound.lower() in existing_set:
        return f"duplicate: {compound}"
    article = str(entry.get("article") or "").strip().lower()
    if article not in ("der", "die", "das"):
        return f"bad article '{article}'"
    parts = entry.get("parts")
    if not isinstance(parts, list) or len(parts) != 2:
        return "parts must be list of exactly 2"
    for p in parts:
        word = str(p.get("word") or "").strip()
        if not word:
            return "empty part word"
        # Reject non-depictable parts: a rebus image must show a concrete object.
        if word.lower() in _NON_DEPICTABLE_PARTS:
            return f"non-depictable part: {word}"
    dalle = entry.get("dalle_prompts")
    if not isinstance(dalle, dict) or len(dalle) < 2:
        return "dalle_prompts must map both part words"
    wrong = entry.get("wrong_options")
    if not isinstance(wrong, list) or len(wrong) != 3:
        return "wrong_options must be list of 3"
    return None


def generate_rebus_replenishment(count: int = 20) -> dict:
    """
    Call GPT to generate `count` new Komposita entries, validate them,
    store DALL-E prompts for new component words, upsert into bt_3_rebus_bank.
    Returns stats dict.
    """
    from backend.database import (
        get_existing_rebus_compound_words,
        upsert_rebus_bank_entry,
        upsert_rebus_component_image,
    )

    existing_words = get_existing_rebus_compound_words()
    existing_set = {w.lower() for w in existing_words}

    logging.info("rebus_replenishment: requesting %s new entries from GPT (existing=%s)", count, len(existing_words))

    try:
        raw_entries = _call_gpt_for_replenishment(count, existing_words)
    except Exception as exc:
        logging.warning("rebus_replenishment: GPT call failed: %s", exc, exc_info=True)
        return {"status": "error", "error": str(exc), "added": 0}

    added = 0
    skipped = 0
    errors = []

    for entry in raw_entries:
        compound = str(entry.get("compound") or "").strip()
        err = _validate_replenishment_entry(entry, existing_set)
        if err:
            logging.info("rebus_replenishment: skip %s — %s", compound, err)
            skipped += 1
            errors.append(f"{compound}: {err}")
            continue

        # Store DALL-E prompts for component words not yet in DB
        dalle_prompts: dict = entry.get("dalle_prompts") or {}
        parts = entry.get("parts") or []
        for part in parts:
            word = str(part.get("word") or "").strip()
            if word and dalle_prompts.get(word):
                upsert_rebus_component_image(
                    word,
                    generation_status="pending",
                    dalle_prompt=str(dalle_prompts[word]),
                )

        # Build DB entry (same structure as REBUS_COMPOUND_BANK)
        compound_id = str(entry.get("id") or compound.lower().replace(" ", "_"))
        db_entry = {
            "id": compound_id,
            "compound": compound,
            "article": str(entry.get("article") or "").strip(),
            "meaning_ru": str(entry.get("meaning_ru") or "").strip(),
            "difficulty": str(entry.get("difficulty") or "B1").strip(),
            "category": str(entry.get("category") or "").strip(),
            "parts": [
                {"word": str(p.get("word") or ""), "meaning_ru": str(p.get("meaning_ru") or "")}
                for p in parts
            ],
            "wrong_options": [str(w) for w in (entry.get("wrong_options") or [])[:3]],
            "explanation_ru": str(entry.get("explanation_ru") or "").strip(),
        }
        try:
            upsert_rebus_bank_entry(db_entry)
            existing_set.add(compound.lower())
            added += 1
            logging.info("rebus_replenishment: added %s", compound)
        except Exception as exc:
            logging.warning("rebus_replenishment: upsert failed for %s: %s", compound, exc)
            errors.append(f"{compound}: upsert error {exc}")
            skipped += 1

    logging.info("rebus_replenishment: done added=%s skipped=%s", added, skipped)
    return {"status": "done", "added": added, "skipped": skipped, "errors": errors[:10]}
