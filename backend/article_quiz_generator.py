"""
Article quiz image generation.

For each word in ARTICLE_QUIZ_BANK, generate one DALL-E image (cached
permanently in R2 + DB). The image shows a single concrete object on a
white background so the article quiz is visually unambiguous.
"""
from __future__ import annotations

import asyncio
import logging
import time


def _object_key(word_id: str) -> str:
    safe = "".join(c if c.isalnum() or c in "-_" else "_" for c in word_id)
    return f"article_quiz/images/{safe}.png"


def generate_article_quiz_image(word_id: str, dalle_prompt: str) -> str:
    """
    Generate and cache DALL-E image for one article quiz word.
    Returns R2 object_key. Raises on failure.
    """
    from backend.database import (
        get_article_quiz_entry,
        mark_article_quiz_image_ready,
        mark_article_quiz_image_failed,
    )
    from backend.image_generation_provider import generate_image_bytes
    from backend.r2_storage import r2_put_bytes

    existing = get_article_quiz_entry(word_id)
    if existing and existing.get("image_status") == "ready" and existing.get("image_object_key"):
        return str(existing["image_object_key"])

    logging.info("article_quiz_gen: generating image word_id=%s", word_id)

    try:
        result = generate_image_bytes(
            prompt=dalle_prompt,
            template_id=0,
            user_id=0,
        )
        img_bytes = bytes(result.get("data") or b"")
        mime = str(result.get("mime_type") or "image/png").strip() or "image/png"
        if not img_bytes:
            raise RuntimeError("empty image payload")

        ext = "png" if "png" in mime else "webp" if "webp" in mime else "png"
        object_key = _object_key(word_id).replace(".png", f".{ext}")
        r2_put_bytes(
            object_key,
            img_bytes,
            content_type=mime,
            cache_control="public, max-age=31536000, immutable",
        )
        mark_article_quiz_image_ready(word_id, image_object_key=object_key)
        logging.info(
            "article_quiz_gen: image ready word_id=%s key=%s bytes=%s",
            word_id, object_key, len(img_bytes),
        )
        return object_key

    except Exception as exc:
        mark_article_quiz_image_failed(word_id)
        raise RuntimeError(f"article_quiz image gen failed for {word_id}: {exc}") from exc


def _render_pending_grammar_cards(*, max_cards: int = 400) -> dict:
    """Render bright text cards for grammar entries (dalle_prompt IS NULL).

    These are free + instant (no DALL·E), so we always top them all up. They
    live in the same bank and are picked/sent identically once image_status is
    'ready' — the photo pool below is counted separately so it never starves."""
    from backend.article_quiz_card import generate_article_quiz_card
    from backend.database import get_db_connection_context

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT word_id FROM bt_3_article_quiz_bank
                WHERE image_status IN ('pending', 'failed')
                  AND dalle_prompt IS NULL
                  AND retired = FALSE
                ORDER BY created_at
                LIMIT %s
                """,
                (int(max_cards),),
            )
            rows = [r[0] for r in (cur.fetchall() or [])]

    rendered = failed = 0
    for word_id in rows:
        try:
            generate_article_quiz_card(str(word_id))
            rendered += 1
        except Exception:
            failed += 1
            logging.warning("article_quiz_card: render failed word_id=%s", word_id, exc_info=True)
    if rows:
        logging.info("article_quiz_card: rendered=%s failed=%s pending=%s", rendered, failed, len(rows))
    return {"rendered": rendered, "failed": failed, "pending": len(rows)}


def prepare_article_quiz_pool(*, target_ready: int = 30, max_attempts: int = 40) -> dict:
    """
    Sync both banks from code; render all pending grammar (text) cards, then
    generate missing DALL·E photos until the *photo* pool reaches target_ready.
    Grammar cards and photos are counted independently so neither starves the
    other. Returns stats dict.
    """
    from backend.database import (
        sync_article_quiz_bank_from_code,
        sync_article_quiz_text_bank_from_code,
        get_db_connection_context,
    )

    sync_stats = sync_article_quiz_bank_from_code()
    text_stats = sync_article_quiz_text_bank_from_code()
    logging.info("article_quiz_gen: banks synced photo=%s grammar=%s", sync_stats, text_stats)

    # 1) Grammar text cards — cheap, render them all.
    card_stats = _render_pending_grammar_cards()

    # 2) DALL·E photos — count ONLY photo entries so grammar cards can't starve them.
    def _photo_ready() -> int:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(*) FROM bt_3_article_quiz_bank
                    WHERE image_status = 'ready' AND retired = FALSE
                      AND dalle_prompt IS NOT NULL
                    """
                )
                return int((cur.fetchone() or [0])[0])

    already_ready = _photo_ready()
    if already_ready >= target_ready:
        return {"status": "sufficient", "photo_ready": already_ready, "generated": 0, "cards": card_stats}

    need = max(0, target_ready - already_ready)
    generated = 0
    failed = 0
    attempts = 0

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT word_id, dalle_prompt FROM bt_3_article_quiz_bank
                WHERE image_status IN ('pending', 'failed')
                  AND dalle_prompt IS NOT NULL
                  AND retired = FALSE
                ORDER BY word_id
                LIMIT %s
                """,
                (int(max_attempts),),
            )
            rows = cur.fetchall() or []

    import random
    random.shuffle(rows)

    for word_id, dalle_prompt in rows:
        if generated >= need or attempts >= max_attempts:
            break
        attempts += 1
        if not dalle_prompt:
            continue
        try:
            generate_article_quiz_image(str(word_id), str(dalle_prompt))
            generated += 1
            time.sleep(1.5)  # rate-limit DALL-E calls
        except Exception:
            failed += 1
            logging.warning("article_quiz_gen: failed for word_id=%s", word_id, exc_info=True)

    return {
        "status": "done",
        "photo_ready_before": already_ready,
        "generated": generated,
        "failed": failed,
        "attempts": attempts,
        "cards": card_stats,
    }


async def backfill_article_gender_hints(*, limit: int = 50) -> dict:
    """Fill gender_hint for active words that don't have one yet.

    Runs off the critical path (pool-prep job), so the answer popup just reads
    the stored string — no LLM call when the user taps a der/die/das button.
    """
    from backend.database import (
        get_article_quiz_words_missing_hint,
        set_article_quiz_gender_hint,
    )
    from backend.openai_manager import run_article_gender_hint

    words = await asyncio.to_thread(get_article_quiz_words_missing_hint, limit)
    filled = 0
    for w in words:
        try:
            hint = await run_article_gender_hint(
                w.get("word", ""), w.get("article", ""), w.get("meaning_ru", "")
            )
            if hint:
                await asyncio.to_thread(
                    set_article_quiz_gender_hint, w["word_id"], gender_hint=hint
                )
                filled += 1
        except Exception:
            logging.warning(
                "article_gender_hint: backfill failed word_id=%s",
                w.get("word_id"), exc_info=True,
            )
    if words:
        logging.info("article_gender_hint: backfill missing=%s filled=%s", len(words), filled)
    return {"missing": len(words), "filled": filled}
