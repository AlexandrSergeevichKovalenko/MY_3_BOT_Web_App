#!/usr/bin/env python3
"""One-time uploader for onboarding photos/videos -> Telegram file_ids.

Sends each local asset to the admin chat via the bot token, reads the file_id
Telegram returns, and writes them to backend/onboarding_assets.py. After that the
bot delivers onboarding media by file_id (instant, no re-upload, nothing in git).

file_ids are tied to THIS bot token; re-run only if the bot token changes or you
add/replace assets.

Usage:
    TELEGRAM_Deutsch_BOT_TOKEN=... python scripts/upload_onboarding_assets.py
    # admin chat id defaults to 117649764, override with ONBOARDING_UPLOAD_CHAT_ID
"""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

import requests

REPO_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_PATH = REPO_ROOT / "backend" / "onboarding_assets.py"

DEFAULT_CHAT_ID = 117649764

# (asset_key, absolute_source_path, kind)  -- kind is "photo" or "video".
ASSETS: list[tuple[str, str, str]] = [
    ("step1_photo_1", "/Users/alexandr/Downloads/Telegram Lite/photo_1_2026-06-07_21-09-27.jpg", "photo"),
    ("step1_photo_2", "/Users/alexandr/Downloads/Telegram Lite/photo_2_2026-06-07_21-09-27.jpg", "photo"),
    ("step1_photo_3", "/Users/alexandr/Downloads/Telegram Lite/photo_3_2026-06-07_21-09-27.jpg", "photo"),
    ("step2_action_button", "/Users/alexandr/Desktop/Screenshots_from_my_Mac_book/video_2026-06-07_21-11-23.mp4", "video"),
    ("step2_back_tap", "/Users/alexandr/Desktop/Screenshots_from_my_Mac_book/video_2026-06-07_21-13-34.mp4", "video"),
    ("howto_shortcut", "/Users/alexandr/Desktop/Screenshots_from_my_Mac_book/video_2026-06-07_21-29-59.mp4", "video"),
    ("howto_learn_words", "/Users/alexandr/Desktop/Screenshots_from_my_Mac_book/video_2026-06-07_21-30-37.mp4", "video"),
    ("howto_tests_quizzes", "/Users/alexandr/Desktop/Screenshots_from_my_Mac_book/video_2026-06-07_21-31-01.mp4", "video"),
    ("howto_translate_sentences", "/Users/alexandr/Desktop/Screenshots_from_my_Mac_book/video_2026-06-07_21-31-38.mp4", "video"),
    ("howto_youtube_subs", "/Users/alexandr/Desktop/Screenshots_from_my_Mac_book/video_2026-06-07_21-32-04.mp4", "video"),
    # Step 3 photo is added later: drop the file at this exact path and re-run.
    ("step3_photo", "/Users/alexandr/Desktop/Screenshots_from_my_Mac_book/step3_photo.jpg", "photo"),
]


def _bot_token() -> str:
    token = (os.getenv("TELEGRAM_Deutsch_BOT_TOKEN") or "").strip()
    if not token:
        print("ERROR: TELEGRAM_Deutsch_BOT_TOKEN is not set", file=sys.stderr)
        sys.exit(1)
    return token


def _extract_file_id(kind: str, result: dict) -> str:
    if kind == "photo":
        photos = result.get("photo") or []
        if not photos:
            raise RuntimeError("no photo sizes in response")
        # Largest size is last; its file_id resolves to the full image.
        return str(photos[-1]["file_id"])
    if kind == "video":
        video = result.get("video") or {}
        if not video.get("file_id"):
            raise RuntimeError("no video.file_id in response")
        return str(video["file_id"])
    raise ValueError(f"unknown kind {kind!r}")


def _load_existing_file_ids() -> dict[str, str]:
    """Return file_ids already stored, so re-runs only upload what is missing."""
    try:
        from backend.onboarding_assets import ONBOARDING_ASSETS  # type: ignore
        return {k: str(v or "").strip() for k, v in (ONBOARDING_ASSETS or {}).items()}
    except Exception:
        return {}


def main() -> int:
    token = _bot_token()
    chat_id = int(os.getenv("ONBOARDING_UPLOAD_CHAT_ID") or DEFAULT_CHAT_ID)
    force = (os.getenv("ONBOARDING_FORCE_REUPLOAD") or "").strip().lower() in {"1", "true", "yes", "on"}
    existing = _load_existing_file_ids()
    file_ids: dict[str, str] = {}

    for key, path_str, kind in ASSETS:
        # Keep an already-uploaded file_id unless a re-upload is forced. This makes
        # "add the Step 3 photo later" a one-shot: only the empty key is uploaded.
        if not force and existing.get(key):
            print(f"✓ keeping {key} (already uploaded)")
            file_ids[key] = existing[key]
            continue
        path = Path(path_str)
        if not path.exists():
            # Missing file is non-fatal: keep the key empty so the bot skips that
            # media, and the asset can be dropped in + re-uploaded later.
            print(f"… skipping {key}: file not found ({path}) — will stay empty")
            file_ids[key] = ""
            continue
        method = "sendPhoto" if kind == "photo" else "sendVideo"
        field = "photo" if kind == "photo" else "video"
        url = f"https://api.telegram.org/bot{token}/{method}"
        print(f"→ uploading {key} ({kind}, {path.stat().st_size // 1024} KB)...")
        with path.open("rb") as fh:
            resp = requests.post(
                url,
                data={"chat_id": chat_id, "caption": f"[setup] {key}", "disable_notification": True},
                files={field: (path.name, fh)},
                timeout=180,
            )
        payload = resp.json() if resp.content else {}
        if not payload.get("ok"):
            print(f"ERROR: {key} failed http={resp.status_code} body={json.dumps(payload, ensure_ascii=False)}", file=sys.stderr)
            return 3
        file_id = _extract_file_id(kind, payload.get("result") or {})
        file_ids[key] = file_id
        print(f"   ✓ {key} -> {file_id}")
        time.sleep(0.5)

    lines = [
        '"""Telegram file_ids for onboarding media. Auto-generated by',
        'scripts/upload_onboarding_assets.py. file_ids are bot-specific (tied to the',
        'bot token). Re-run that script if the token changes or assets are replaced.',
        '"""',
        "",
        "ONBOARDING_ASSETS: dict[str, str] = {",
    ]
    for key, _path, kind in ASSETS:
        lines.append(f"    {key!r}: {file_ids[key]!r},  # {kind}")
    lines.append("}")
    lines.append("")
    OUTPUT_PATH.write_text("\n".join(lines), encoding="utf-8")
    print(f"\nWrote {len(file_ids)} file_ids -> {OUTPUT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
