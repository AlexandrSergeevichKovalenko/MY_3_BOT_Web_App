"""
Minimal Telegram private-message transport primitives.

Only _send_private_message and _send_private_message_chunks live here.
Group, photo, and media helpers remain in backend_server.py.
This module exists solely to break the backend_server import dependency
for callers (including tts_generation.py) that need plain-text delivery.
"""

import logging
import os

import requests

from backend.database import record_telegram_system_message


def _send_private_message(
    user_id: int,
    text: str,
    reply_markup: dict | None = None,
    disable_web_page_preview: bool = True,
    parse_mode: str | None = None,
    message_type: str | None = None,
) -> None:
    token = os.getenv("TELEGRAM_Deutsch_BOT_TOKEN")
    payload = {
        "chat_id": int(user_id),
        "text": text,
        "disable_web_page_preview": bool(disable_web_page_preview),
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup
    if parse_mode:
        payload["parse_mode"] = str(parse_mode).strip()
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    response = requests.post(
        url,
        json=payload,
        timeout=15,
    )
    if response.status_code >= 400:
        raise RuntimeError(f"Telegram API error: {response.text}")
    try:
        payload = response.json() if response.content else {}
        message_id = (payload.get("result") or {}).get("message_id")
        if message_id is not None:
            record_telegram_system_message(
                chat_id=int(user_id),
                message_id=int(message_id),
                message_type=(message_type or "text"),
            )
    except Exception:
        logging.debug("Failed to track private system message", exc_info=True)


def _send_private_message_chunks(user_id: int, text: str, limit: int = 3800) -> None:
    parts: list[str] = []
    buf = ""
    for line in text.splitlines():
        chunk = (buf + "\n" + line) if buf else line
        if len(chunk) > limit:
            if buf:
                parts.append(buf)
            buf = line
        else:
            buf = chunk
    if buf:
        parts.append(buf)
    for part in parts:
        _send_private_message(user_id, part)
