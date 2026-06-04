"""
Crossword grid image renderer.

Renders a PIL image of a crossword puzzle and uploads to R2.

Grid visual rules:
  - Black cell  → dark square (no letter there)
  - Visible cell → white square with letter
  - Hidden cell  → white square, blank (user must guess)
                   EXCEPT if the cell is also part of a visible word
                   (intersection letter is always shown)
  - Word-start cell → small number in top-left corner
"""
from __future__ import annotations

import io
import logging
from typing import Optional

try:
    from PIL import Image, ImageDraw, ImageFont
    _PIL_AVAILABLE = True
except ImportError:
    _PIL_AVAILABLE = False

# ─── Visual constants ──────────────────────────────────────────────────────────

CELL_SIZE   = 46          # px per grid cell (interior)
CELL_BORDER = 2           # px border between cells
PADDING     = 36          # outer padding around grid
TITLE_H     = 72          # height reserved for title above grid
FOOTER_H    = 28          # small footer below grid

# Colours
BG_COLOR          = (15,  17,  35)   # deep navy background
WHITE_CELL        = (255, 255, 255)  # revealed / visible cell
HIDDEN_CELL       = (235, 238, 255)  # hidden blank cell (very light lavender)
BLACK_CELL        = (32,  36,  60)   # blocked / null cell
GRID_LINE_COLOR   = (70,  82, 140)   # grid border lines
LETTER_COLOR      = (15,  20,  50)   # letter on white cell
NUMBER_COLOR      = (90, 130, 220)   # small word-number in corner
TITLE_COLOR       = (240, 240, 255)  # title text
TOPIC_COLOR       = (255, 220,  80)  # topic accent (warm yellow)
HIDDEN_MARK_COLOR = (160, 175, 220)  # "?" in hidden cells

FONT_LETTER_SIZE  = 22
FONT_NUMBER_SIZE  = 11
FONT_TITLE_SIZE   = 26
FONT_TOPIC_SIZE   = 18


# ─── Font loading ──────────────────────────────────────────────────────────────

def _get_font(size: int, bold: bool = False) -> "ImageFont.FreeTypeFont":
    candidates = [
        "/System/Library/Fonts/Supplemental/Arial Bold.ttf" if bold
        else "/System/Library/Fonts/Supplemental/Arial.ttf",
        "/System/Library/Fonts/Helvetica.ttc",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf" if bold
        else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf" if bold
        else "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
    ]
    for path in candidates:
        try:
            return ImageFont.truetype(path, size)
        except Exception:
            continue
    return ImageFont.load_default()


# ─── Revealed-cell computation ─────────────────────────────────────────────────

def _compute_revealed_cells(words_json: list[dict]) -> set[tuple[int, int]]:
    """
    Return set of (row, col) positions that belong to at least one VISIBLE word.
    These cells always show their letter even if they are also part of a hidden word.
    """
    revealed: set[tuple[int, int]] = set()
    for word in words_json:
        if word.get("hidden"):
            continue
        r, c = int(word["row"]), int(word["col"])
        text = str(word["word"])
        dr, dc = (0, 1) if word["direction"] == "across" else (1, 0)
        for i in range(len(text)):
            revealed.add((r + dr * i, c + dc * i))
    return revealed


def _word_start_numbers(words_json: list[dict]) -> dict[tuple[int, int], list[int]]:
    """Return map of (row, col) → list of word numbers that start there."""
    result: dict[tuple[int, int], list[int]] = {}
    for word in words_json:
        key = (int(word["row"]), int(word["col"]))
        result.setdefault(key, []).append(int(word["number"]))
    return result


# ─── Core renderer ─────────────────────────────────────────────────────────────

def render_crossword_card(
    grid_json: list[list],
    words_json: list[dict],
    *,
    topic: str = "",
    difficulty: str = "",
) -> bytes:
    """
    Render the crossword grid to PNG bytes.

    grid_json : 2-D list, each element is a letter str or None (black cell)
    words_json: list of word dicts with number/direction/row/col/word/hidden
    """
    if not _PIL_AVAILABLE:
        raise RuntimeError("Pillow not installed — cannot render crossword")

    rows = len(grid_json)
    cols = max((len(r) for r in grid_json), default=0)
    if rows == 0 or cols == 0:
        raise ValueError("Empty grid")

    revealed   = _compute_revealed_cells(words_json)
    start_nums = _word_start_numbers(words_json)

    font_letter = _get_font(FONT_LETTER_SIZE, bold=True)
    font_number = _get_font(FONT_NUMBER_SIZE, bold=False)
    font_title  = _get_font(FONT_TITLE_SIZE,  bold=True)
    font_topic  = _get_font(FONT_TOPIC_SIZE,  bold=False)

    # Image dimensions
    grid_w = cols * CELL_SIZE + (cols + 1) * CELL_BORDER
    grid_h = rows * CELL_SIZE + (rows + 1) * CELL_BORDER
    img_w  = max(480, grid_w + 2 * PADDING)
    img_h  = PADDING + TITLE_H + grid_h + FOOTER_H + PADDING

    canvas = Image.new("RGB", (img_w, img_h), BG_COLOR)
    draw   = ImageDraw.Draw(canvas)

    # ── Title ─────────────────────────────────────────────────────────────────
    title_text = "Kreuzworträtsel"
    tw, _ = draw.textbbox((0, 0), title_text, font=font_title)[2:]
    tx = (img_w - tw) // 2
    draw.text((tx, PADDING), title_text, font=font_title, fill=TITLE_COLOR)

    if topic:
        label = f"{topic}  ·  {difficulty}" if difficulty else topic
        lw, _ = draw.textbbox((0, 0), label, font=font_topic)[2:]
        lx = (img_w - lw) // 2
        draw.text((lx, PADDING + FONT_TITLE_SIZE + 6), label,
                  font=font_topic, fill=TOPIC_COLOR)

    # ── Grid origin ───────────────────────────────────────────────────────────
    grid_x0 = (img_w - grid_w) // 2
    grid_y0 = PADDING + TITLE_H

    # Draw outer grid border
    draw.rectangle(
        [grid_x0, grid_y0, grid_x0 + grid_w - 1, grid_y0 + grid_h - 1],
        outline=GRID_LINE_COLOR,
        width=CELL_BORDER,
    )

    # ── Cells ─────────────────────────────────────────────────────────────────
    for row_i, row in enumerate(grid_json):
        for col_i in range(cols):
            letter = row[col_i] if col_i < len(row) else None

            # Pixel coords of this cell's top-left corner
            cx = grid_x0 + CELL_BORDER + col_i * (CELL_SIZE + CELL_BORDER)
            cy = grid_y0 + CELL_BORDER + row_i * (CELL_SIZE + CELL_BORDER)
            cell_rect = [cx, cy, cx + CELL_SIZE - 1, cy + CELL_SIZE - 1]

            if letter is None:
                # Black cell
                draw.rectangle(cell_rect, fill=BLACK_CELL)
                continue

            pos = (row_i, col_i)
            is_revealed = pos in revealed

            # Cell background
            cell_fill = WHITE_CELL if is_revealed else HIDDEN_CELL
            draw.rectangle(cell_rect, fill=cell_fill, outline=GRID_LINE_COLOR, width=1)

            # Small word-number in top-left corner
            nums = start_nums.get(pos)
            if nums:
                num_str = "/".join(str(n) for n in nums)
                draw.text((cx + 2, cy + 1), num_str,
                          font=font_number, fill=NUMBER_COLOR)

            if is_revealed:
                # Draw letter centred in cell
                bbox = draw.textbbox((0, 0), letter, font=font_letter)
                lw = bbox[2] - bbox[0]
                lh = bbox[3] - bbox[1]
                lx = cx + (CELL_SIZE - lw) // 2
                ly = cy + (CELL_SIZE - lh) // 2
                draw.text((lx, ly), letter, font=font_letter, fill=LETTER_COLOR)
            # Hidden cells: left blank (user fills in mentally)

    # ── Footer hint ───────────────────────────────────────────────────────────
    hidden_count = sum(1 for w in words_json if w.get("hidden"))
    if hidden_count:
        hint = f"Finde {hidden_count} fehlende {'Wort' if hidden_count == 1 else 'Wörter'}!"
        hw, _ = draw.textbbox((0, 0), hint, font=font_topic)[2:]
        draw.text(
            ((img_w - hw) // 2, grid_y0 + grid_h + 8),
            hint,
            font=font_topic,
            fill=TOPIC_COLOR,
        )

    buf = io.BytesIO()
    canvas.save(buf, format="PNG", optimize=True)
    return buf.getvalue()


# ─── Full pipeline: render + upload to R2 ─────────────────────────────────────

def prepare_crossword_image(crossword_id: str) -> str:
    """
    Fetch crossword from DB, render PNG, upload to R2, mark image ready.
    Returns R2 object_key. Raises on failure.
    """
    from backend.database import (
        get_crossword_bank_entry,
        mark_crossword_image_ready,
        mark_crossword_image_failed,
    )
    from backend.r2_storage import r2_put_bytes

    entry = get_crossword_bank_entry(crossword_id)
    if not entry:
        raise RuntimeError(f"crossword_id not found: {crossword_id}")

    grid_json  = entry["grid_json"]
    words_json = entry["words_json"]
    topic      = str(entry.get("topic") or "")
    difficulty = str(entry.get("difficulty") or "")

    try:
        png_bytes = render_crossword_card(
            grid_json, words_json, topic=topic, difficulty=difficulty
        )
    except Exception as exc:
        mark_crossword_image_failed(crossword_id)
        raise RuntimeError(f"render failed: {exc}") from exc

    safe_id = "".join(c if c.isalnum() or c in "-_" else "_" for c in crossword_id)
    object_key = f"crossword/cards/{safe_id}.png"

    try:
        r2_put_bytes(
            object_key,
            png_bytes,
            content_type="image/png",
            cache_control="public, max-age=31536000, immutable",
        )
    except Exception as exc:
        mark_crossword_image_failed(crossword_id)
        raise RuntimeError(f"R2 upload failed: {exc}") from exc

    mark_crossword_image_ready(crossword_id, image_object_key=object_key)
    logging.info(
        "crossword_renderer: ready crossword_id=%s key=%s bytes=%d",
        crossword_id, object_key, len(png_bytes),
    )
    return object_key


def prepare_crossword_images_batch(*, limit: int = 10) -> dict:
    """
    Find all pending crosswords and render their images.
    Returns stats dict.
    """
    from backend.database import get_db_connection_context

    stats = {"attempted": 0, "succeeded": 0, "failed": 0}

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT crossword_id FROM bt_3_crossword_bank
                WHERE image_status = 'pending' AND retired = FALSE
                ORDER BY created_at
                LIMIT %s
                """,
                (int(limit),),
            )
            rows = cursor.fetchall()

    for (cid,) in rows:
        stats["attempted"] += 1
        try:
            prepare_crossword_image(str(cid))
            stats["succeeded"] += 1
        except Exception as exc:
            stats["failed"] += 1
            logging.warning("crossword_renderer: failed cid=%s: %s", cid, exc)

    return stats
