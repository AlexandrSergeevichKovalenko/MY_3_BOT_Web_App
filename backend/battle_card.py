"""Artikel Battle invite/reminder artwork.

Two gpt-image-1 pictures, generated ONCE via /admin_battle_images and cached on R2:
  • battle/fox_invite.png   — two Fox knights, crossed swords, armor «der» / «das».
  • battle/fox_reminder.png  — a lone Fox knight (Lancelot), sword planted, ready.
The bot sends them by R2 public URL; if missing, it falls back to text.

NOTE: these prompts KEEP the 'der'/'das' letters on the armor, so they do NOT use
mascot_prompt() (its shared render block forbids letters) — they append only the
Fox character identity (MASCOT_CHARACTER) to the scene.
"""
from __future__ import annotations

import logging

from backend.mascot_style import MASCOT_CHARACTER

INVITE_KEY = "battle/fox_invite.png"
REMINDER_KEY = "battle/fox_reminder.png"

# (key, prompt) pairs for /admin_battle_images.
BATTLE_IMAGE_PROMPTS = [
    (INVITE_KEY,
     "Two Fox mascot knights facing each other in an epic duel, "
     "crossed swords raised and touching in the center, wearing shiny colorful medieval armor "
     "and flowing capes, NO helmets (round friendly fox faces visible, confident grins). "
     "The left knight's chest armor plate shows the large bold white letters 'der'; "
     "the right knight's chest armor plate shows the large bold white letters 'das'. "
     "Dramatic colorful battle-arena background, torches, sparks where the swords meet, "
     "playful high-quality cartoon illustration, vibrant lighting. " + MASCOT_CHARACTER + "."),
    (REMINDER_KEY,
     "A single Fox mascot knight in full shiny medieval armor, "
     "NO helmet (round friendly fox face visible, determined look), standing heroically like "
     "Lancelot with a large sword planted point-down into the ground, both hands resting on "
     "the hilt, ready for battle. Epic colorful background with dramatic lighting, banners, "
     "playful high-quality cartoon illustration. " + MASCOT_CHARACTER + "."),
]


def battle_invite_image_url() -> str | None:
    """Public R2 URL of the invite image, or None if it isn't generated yet."""
    try:
        from backend.r2_storage import r2_public_url
        return r2_public_url(INVITE_KEY)
    except Exception:
        logging.warning("battle_invite_image_url failed", exc_info=True)
        return None


def battle_reminder_image_url() -> str | None:
    try:
        from backend.r2_storage import r2_public_url
        return r2_public_url(REMINDER_KEY)
    except Exception:
        logging.warning("battle_reminder_image_url failed", exc_info=True)
        return None
