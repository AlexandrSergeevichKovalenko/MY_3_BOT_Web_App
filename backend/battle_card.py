"""Battle invite/reminder artwork.

gpt-image-1 pictures, generated ONCE via /admin_battle_images and cached on R2:
  • battle/fox_invite.png           — two Fox knights, crossed swords, armor «der» / «das» (Artikel).
  • battle/fox_invite_adjektiv.png  — two Fox knights, armor «-en» / «-es» (Adjektivendungen).
  • battle/fox_invite_wofrage.png   — two Fox knights, armor «Wo?» / «Wie?» (Wo-Fragen).
  • battle/fox_reminder.png         — a lone Fox knight (Lancelot), sword planted, ready.
Every kind gets the SAME two-Fox-knights composition (so an invite always looks like a
battle), only the letters on the armor change per theme. The bot sends them by R2 public
URL; if the image is missing/unreachable the send degrades to text (see bot_3._dm_one_battle_invite).

NOTE: these prompts KEEP letters on the armor, so they do NOT use mascot_prompt() (its
shared render block forbids letters) — they append only the Fox character identity
(MASCOT_CHARACTER) to the scene.
"""
from __future__ import annotations

import logging

from backend.mascot_style import MASCOT_CHARACTER

INVITE_KEY = "battle/fox_invite.png"
ADJEKTIV_INVITE_KEY = "battle/fox_invite_adjektiv.png"
WOFRAGE_INVITE_KEY = "battle/fox_invite_wofrage.png"
REMINDER_KEY = "battle/fox_reminder.png"

# Per-kind invite image key. Unknown kinds fall back to the Artikel invite.
INVITE_KEY_BY_KIND = {
    "artikel": INVITE_KEY,
    "adjektiv": ADJEKTIV_INVITE_KEY,
    "wofrage": WOFRAGE_INVITE_KEY,
}


def _two_knights_prompt(left_label: str, right_label: str) -> str:
    """Shared «two Fox knights, crossed swords» composition; only the armor text differs
    per battle kind, so all battle invites share one look."""
    return (
        "Two Fox mascot knights facing each other in an epic duel, "
        "crossed swords raised and touching in the center, wearing shiny colorful medieval armor "
        "and flowing capes, NO helmets (round friendly fox faces visible, confident grins). "
        f"The left knight's chest armor plate shows the large bold white letters '{left_label}'; "
        f"the right knight's chest armor plate shows the large bold white letters '{right_label}'. "
        "Dramatic colorful battle-arena background, torches, sparks where the swords meet, "
        "playful high-quality cartoon illustration, vibrant lighting. " + MASCOT_CHARACTER + "."
    )


# (key, prompt) pairs for /admin_battle_images. Existing keys are skipped unless «force».
BATTLE_IMAGE_PROMPTS = [
    (INVITE_KEY, _two_knights_prompt("der", "das")),
    (ADJEKTIV_INVITE_KEY, _two_knights_prompt("-en", "-es")),
    (WOFRAGE_INVITE_KEY, _two_knights_prompt("Wo?", "Wie?")),
    (REMINDER_KEY,
     "A single Fox mascot knight in full shiny medieval armor, "
     "NO helmet (round friendly fox face visible, determined look), standing heroically like "
     "Lancelot with a large sword planted point-down into the ground, both hands resting on "
     "the hilt, ready for battle. Epic colorful background with dramatic lighting, banners, "
     "playful high-quality cartoon illustration. " + MASCOT_CHARACTER + "."),
]


def battle_invite_image_url(kind: str = "artikel") -> str | None:
    """Public R2 URL of the invite image for a battle ``kind`` (artikel/adjektiv/wofrage).
    Unknown kinds fall back to the Artikel invite art. Returns None only if URL building
    fails; the caller degrades to text if the object itself is missing/unreachable."""
    try:
        from backend.r2_storage import r2_public_url
        key = INVITE_KEY_BY_KIND.get(str(kind or "").strip().lower(), INVITE_KEY)
        return r2_public_url(key)
    except Exception:
        logging.warning("battle_invite_image_url failed kind=%s", kind, exc_info=True)
        return None


def battle_reminder_image_url() -> str | None:
    try:
        from backend.r2_storage import r2_public_url
        return r2_public_url(REMINDER_KEY)
    except Exception:
        logging.warning("battle_reminder_image_url failed", exc_info=True)
        return None
