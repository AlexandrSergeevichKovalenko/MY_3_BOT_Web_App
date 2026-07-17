"""
Single source of truth for the brand mascot's appearance.

The mascot is **Fox** — an original, friendly anthropomorphic red fox used across
every generated image and hero card. Keeping the description in ONE place is what
makes the art read as the *same* character everywhere (единый типаж). To restyle
the mascot, edit MASCOT_CHARACTER / MASCOT_RENDER below and re-run the admin
image-generation commands (they read these strings) so the cached R2 art is
overwritten with the new look.

Deliberately NOT a blue gnome: no blue skin, no white Phrygian cap. The previous
"smurf-like" mascot was a trademark risk (Schlumpf™ / IMPS), so the whole typage —
species, silhouette and palette — is changed, not just the colour.
"""
from __future__ import annotations

# WHO the mascot is — appended verbatim to every prompt so it stays the same fox.
MASCOT_CHARACTER = (
    "the mascot is Fox — a cute, friendly anthropomorphic red fox character with "
    "warm orange fur, a soft cream muzzle, chest and belly, two tall pointed ears "
    "with cream insides, a big fluffy tail with a white tip, a small dark nose, "
    "large warm rounded eyes and a gentle happy smile; rounded chibi proportions "
    "with a big head and a small body; he wears a cozy petrol-teal scarf. Clever, "
    "charming and warm-hearted"
)

# HOW it is rendered — art direction shared by every surface.
MASCOT_RENDER = (
    "Soft 3D Pixar-like render, vibrant playful colors, soft studio lighting, "
    "rounded shapes, clean simple uncluttered background, centered composition, "
    "no text, no letters, no numbers, no watermark"
)


def mascot_prompt(scene: str) -> str:
    """Compose a full gpt-image-1 prompt: <scene> + who Fox is + shared render style.

    `scene` describes the pose/situation and refers to the mascot as "Fox" (use
    "two Fox mascots" for two-character scenes). The character + render blocks are
    appended so the same fox and the same art style come out on every surface.
    """
    scene = (scene or "").strip().rstrip(".")
    return f"{scene}. {MASCOT_CHARACTER}. {MASCOT_RENDER}."


# Render block for the die-cut hero STICKERS used in the app UI (frontend
# hero_*.webp + app icon source). These sit over the interface, so they need a
# fully TRANSPARENT background — pair this with background="transparent" on the
# image request (see generate_image_bytes(background=...)).
MASCOT_STICKER_RENDER = (
    "Full-body die-cut sticker with a thin soft white outline, the character "
    "isolated on a fully transparent background (alpha channel), no scene, no floor, "
    "no ground shadow. Soft 3D Pixar-like render, vibrant playful colors, soft studio "
    "lighting, rounded shapes, no text, no letters, no numbers, no watermark."
)


def mascot_sticker_prompt(pose: str) -> str:
    """Compose a transparent die-cut sticker prompt: <pose> + who Fox is + sticker
    render. Use for the frontend hero poses and the app icon source."""
    pose = (pose or "").strip().rstrip(".")
    return f"{pose}. {MASCOT_CHARACTER}. {MASCOT_STICKER_RENDER}"
