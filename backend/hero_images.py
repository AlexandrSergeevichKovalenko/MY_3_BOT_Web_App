"""
Felix (Fox mascot) hero stickers for the app UI.

These are the transparent die-cut character images the frontend uses everywhere:
  • hero_original.webp — happy, presenting a green check (correct answer)
  • hero_cry.webp      — sad + red warning triangle (wrong answer)
  • hero_think.webp     — thinking (unanswered)
  • hero_sticker.webp   — full-body thumbs-up (favicon + corner logo + app icon src)

Generated ONCE via /admin_hero_images (gpt-image-1, background="transparent") and
uploaded to R2 under brand/felix_*.png. A human then pulls them down, converts to
.webp and rebuilds the icon set into frontend/public (+ dist). Kept out of the
frontend build so regeneration never needs a code change — only the files swap.
"""
from __future__ import annotations

import logging

from backend.mascot_style import mascot_sticker_prompt

# (short name, R2 key, pose). name maps to the frontend file hero_<name>.webp.
HERO_POSES: list[tuple[str, str, str]] = [
    ("original", "brand/felix_original.png", mascot_sticker_prompt(
        "The Fox mascot standing cheerfully and proudly, presenting a big bright green "
        "checkmark held up in one paw, a beaming happy smile and sparkling eyes, upbeat "
        "and encouraging"
    )),
    ("cry", "brand/felix_cry.png", mascot_sticker_prompt(
        "The Fox mascot looking sad and disappointed, teary watery eyes and slightly "
        "drooping ears, holding up a red warning triangle sign with a white exclamation "
        "mark, a single cartoon tear on the cheek, a gentle apologetic expression"
    )),
    ("think", "brand/felix_think.png", mascot_sticker_prompt(
        "The Fox mascot in a thoughtful curious pose, one paw touching his chin, head "
        "tilted slightly, eyes looking up wondering, a small puzzled but friendly and "
        "hopeful expression"
    )),
    ("sticker", "brand/felix_sticker.png", mascot_sticker_prompt(
        "The Fox mascot standing full-body, cheerful and confident, giving a big thumbs "
        "up with one paw, a warm happy welcoming smile, energetic friendly vibe"
    )),
]


def hero_pose_keys() -> list[str]:
    return [key for _name, key, _prompt in HERO_POSES]


def generate_and_upload_hero_images(user_id: int = 0) -> list[dict]:
    """Generate every Felix hero sticker (transparent PNG) and upload to R2.

    Returns a list of {name, key, url, size, error} — synchronous/blocking, so call
    it via asyncio.to_thread from the admin handler.
    """
    from backend.image_generation_provider import generate_image_bytes
    from backend.r2_storage import r2_put_bytes, r2_public_url

    results: list[dict] = []
    for name, key, prompt in HERO_POSES:
        row: dict = {"name": name, "key": key, "url": None, "size": 0, "error": None}
        try:
            res = generate_image_bytes(
                prompt=prompt, template_id=0, user_id=int(user_id or 0),
                action_type="hero_image", background="transparent",
            )
            data = bytes(res.get("data") or b"")
            if not data:
                raise RuntimeError("empty image payload")
            r2_put_bytes(key, data, content_type="image/png",
                         cache_control="public, max-age=86400")
            row["url"] = r2_public_url(key)
            row["size"] = len(data)
            row["bytes"] = data
        except Exception as exc:
            row["error"] = str(exc)[:160]
            logging.warning("hero image %s failed", name, exc_info=True)
        results.append(row)
    return results
