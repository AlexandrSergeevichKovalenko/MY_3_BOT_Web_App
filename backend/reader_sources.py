"""Curated reading sources for the in-app reader ("Источники").

Instead of the copy-a-link-from-the-browser dance, the reader shows a live list
of fresh German articles pulled from a few curated publishers (Deutsche Welle,
Tagesschau, …). Tapping an article opens its URL straight through the existing
reader ingest pipeline — no external browser, no clipboard.

This module is intentionally dependency-light: it fetches each publisher's RSS /
RDF / Atom feed with a browser User-Agent, parses it with the stdlib XML parser
(namespaces stripped to local names for robustness), and caches the result
in-process for a few minutes so we never hammer the publishers.
"""

from __future__ import annotations

import html as _html
import logging
import re
import threading
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import xml.etree.ElementTree as ET

import requests

logger = logging.getLogger(__name__)

_BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"
)

# ── Curated publishers ─────────────────────────────────────────────────────
# level     — rough CEFR band, shown to the learner as a hint.
# via_proxy — fetch through the residential proxy (some publishers block
#             datacenter IPs). Only honoured when a proxy is configured.
READER_SOURCES: list[dict] = [
    {
        "id": "dw",
        "name": "Deutsche Welle",
        "section": "Nachrichten",
        "level": "B1–C1",
        "homepage": "https://www.dw.com/de/",
        "feed_url": "https://rss.dw.com/rdf/rss-de-all",
        "via_proxy": False,
    },
    {
        "id": "dw_top",
        "name": "Deutsche Welle",
        "section": "Themen des Tages",
        "level": "B1–C1",
        "homepage": "https://www.dw.com/de/themen/",
        "feed_url": "https://rss.dw.com/rdf/rss-de-top",
        "via_proxy": False,
    },
    {
        "id": "tagesschau",
        "name": "Tagesschau",
        "section": "Nachrichten",
        "level": "B2+",
        "homepage": "https://www.tagesschau.de/",
        "feed_url": "https://www.tagesschau.de/index~rss2.xml",
        "via_proxy": False,
    },
    {
        # Einfache Sprache — the most valuable band for learners (A2–B1), but the
        # publisher bot-blocks datacenter IPs, so we route it through the proxy.
        "id": "nachrichtenleicht",
        "name": "Nachrichtenleicht",
        "section": "Einfache Sprache",
        "level": "A2–B1",
        "homepage": "https://www.nachrichtenleicht.de/",
        "feed_url": "https://www.nachrichtenleicht.de/nachrichtenleicht-nachrichten-100.rss",
        "via_proxy": True,
    },
]

_SOURCE_BY_ID = {s["id"]: s for s in READER_SOURCES}

_CACHE_TTL_SECONDS = 600  # 10 min — feeds refresh ~hourly, cheap to hold
_CACHE_LOCK = threading.Lock()
_CACHE: dict[str, tuple[float, list[dict]]] = {}


def public_source_meta(source: dict) -> dict:
    """Client-facing subset (no internal feed_url / proxy flags)."""
    return {
        "id": source["id"],
        "name": source["name"],
        "section": source.get("section") or "",
        "level": source.get("level") or "",
        "homepage": source.get("homepage") or "",
    }


def list_reader_sources() -> list[dict]:
    return [public_source_meta(s) for s in READER_SOURCES]


def _local(tag: str) -> str:
    """Strip the XML namespace so we can match by local name."""
    return tag.rsplit("}", 1)[-1].lower() if "}" in tag else tag.lower()


def _clean_text(value: str | None) -> str:
    if not value:
        return ""
    text = re.sub(r"(?s)<[^>]+>", " ", str(value))
    text = _html.unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def _parse_date_to_epoch(value: str | None) -> int:
    raw = (value or "").strip()
    if not raw:
        return 0
    # RFC-822 (pubDate)  e.g. "Mon, 13 Jul 2026 19:05:00 +0200"
    try:
        dt = parsedate_to_datetime(raw)
        if dt is not None:
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp())
    except Exception:
        pass
    # ISO-8601 (dc:date / Atom updated)  e.g. "2026-07-14T11:27:00Z"
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except Exception:
        return 0


def _extract_image(item: ET.Element, description_html: str) -> str:
    """Best-effort thumbnail: media/enclosure/dwsyn tags, else first <img> in body."""
    for child in item.iter():
        name = _local(child.tag)
        if name in {"thumbnail", "imageurl"}:
            url = (child.get("url") or child.text or "").strip()
            if url:
                return url
        if name == "content" and str(child.get("type") or "").startswith("image"):
            if child.get("url"):
                return child.get("url").strip()
        if name == "enclosure" and str(child.get("type") or "").startswith("image"):
            if child.get("url"):
                return child.get("url").strip()
    m = re.search(r'<img[^>]+src=["\']([^"\']+)["\']', description_html or "", re.I)
    return m.group(1).strip() if m else ""


def _extract_link(item: ET.Element) -> str:
    fallback = ""
    for child in item:
        if _local(child.tag) != "link":
            continue
        # Atom: <link href=... rel="alternate"/> ; RSS: <link>text</link>
        href = (child.get("href") or "").strip()
        text = (child.text or "").strip()
        rel = (child.get("rel") or "alternate").strip().lower()
        if href and rel == "alternate":
            return href
        if href and not fallback:
            fallback = href
        if text and not fallback:
            fallback = text
    # RDF/RSS-1.0 stores the canonical URL on the item's rdf:about attribute.
    if not fallback:
        for key, val in item.attrib.items():
            if _local(key) == "about" and val:
                return val.strip()
    return fallback


def _parse_feed(xml_bytes: bytes) -> list[dict]:
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError as exc:
        raise ValueError(f"feed is not valid XML: {exc}") from exc

    # Collect every <item> (RSS/RDF) and <entry> (Atom) anywhere in the tree.
    nodes = [el for el in root.iter() if _local(el.tag) in {"item", "entry"}]
    articles: list[dict] = []
    for node in nodes:
        title = ""
        description_html = ""
        published_raw = ""
        for child in node:
            name = _local(child.tag)
            if name == "title" and not title:
                title = _clean_text(child.text)
            elif name in {"description", "summary", "encoded"} and not description_html:
                description_html = child.text or ""
            elif name in {"date", "pubdate", "published", "updated"} and not published_raw:
                published_raw = (child.text or "").strip()
        url = _extract_link(node)
        if not title or not url:
            continue
        articles.append(
            {
                "title": title,
                "url": url,
                "image": _extract_image(node, description_html),
                "summary": _clean_text(description_html)[:280],
                "published_ts": _parse_date_to_epoch(published_raw),
            }
        )
    # Freshest first when dates are present; otherwise keep feed order.
    if any(a["published_ts"] for a in articles):
        articles.sort(key=lambda a: a["published_ts"], reverse=True)
    return articles


def fetch_reader_source_articles(
    source_id: str,
    *,
    limit: int = 25,
    proxy_url: str = "",
    force_refresh: bool = False,
) -> list[dict]:
    """Return cached-or-fresh article list for a source. Raises on hard failure."""
    source = _SOURCE_BY_ID.get(str(source_id or "").strip())
    if not source:
        raise KeyError(f"unknown reader source: {source_id!r}")

    now = time.time()
    if not force_refresh:
        with _CACHE_LOCK:
            cached = _CACHE.get(source["id"])
        if cached and (now - cached[0]) < _CACHE_TTL_SECONDS:
            return cached[1][:limit]

    proxies = None
    if source.get("via_proxy") and proxy_url:
        proxies = {"http": proxy_url, "https": proxy_url}

    response = requests.get(
        source["feed_url"],
        timeout=15,
        allow_redirects=True,
        headers={
            "User-Agent": _BROWSER_UA,
            "Accept": "application/rss+xml, application/rdf+xml, application/xml, text/xml, */*",
        },
        proxies=proxies,
    )
    response.raise_for_status()
    body = response.content or b""
    # Bot-blocked publishers answer with an HTML page instead of XML.
    head = body[:200].lstrip().lower()
    if head.startswith(b"<!doctype html") or head.startswith(b"<html"):
        raise ValueError("publisher returned HTML instead of a feed (likely bot-blocked)")

    articles = _parse_feed(body)
    with _CACHE_LOCK:
        _CACHE[source["id"]] = (now, articles)
    logger.info(
        "reader_sources fetched source=%s count=%s via_proxy=%s",
        source["id"],
        len(articles),
        bool(proxies),
    )
    return articles[:limit]
