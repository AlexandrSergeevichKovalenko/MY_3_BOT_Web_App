"""Free stock-photo lookup (Pixabay) for the Artikel Trainer's concrete nouns.

Pixabay has a generous free API. Set PIXABAY_API_KEY (free signup). If the key is
absent we return None — the trainer then just shows the colour card (graceful).
Images are re-hosted on R2 by the caller, so we only need the bytes here.
"""
from __future__ import annotations

import logging
import os

import requests

_PIXABAY_URL = "https://pixabay.com/api/"


def stock_image_enabled() -> bool:
    return bool((os.getenv("PIXABAY_API_KEY") or "").strip())


def fetch_stock_image_bytes(query: str, *, timeout: float = 12.0) -> bytes | None:
    """Return JPEG bytes of the top stock photo for `query`, or None (no key, no
    hit, or error). Best-effort and quiet."""
    key = (os.getenv("PIXABAY_API_KEY") or "").strip()
    q = str(query or "").strip()
    if not key or not q:
        return None
    try:
        resp = requests.get(
            _PIXABAY_URL,
            params={
                "key": key, "q": q, "image_type": "photo",
                "safesearch": "true", "per_page": 3, "order": "popular",
                "lang": "en",
            },
            timeout=timeout,
        )
        if resp.status_code != 200:
            logging.warning("pixabay search HTTP %s for %r", resp.status_code, q)
            return None
        hits = (resp.json() or {}).get("hits") or []
        if not hits:
            return None
        img_url = hits[0].get("webformatURL") or hits[0].get("largeImageURL")
        if not img_url:
            return None
        img = requests.get(img_url, timeout=timeout)
        if img.status_code != 200 or not img.content:
            return None
        return img.content
    except Exception:
        logging.warning("fetch_stock_image_bytes failed for %r", q, exc_info=True)
        return None
