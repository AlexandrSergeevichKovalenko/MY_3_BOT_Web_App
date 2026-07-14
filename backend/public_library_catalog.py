"""Curated public-domain German library — catalog + Project Gutenberg resolver.

These 20 works are all firmly in the public domain (authors long dead) and are
hosted on Project Gutenberg in German. We do NOT hardcode Gutenberg ebook ids
(they are easy to get subtly wrong); instead we resolve each book at ingest time
via the Gutendex API (a JSON index over Project Gutenberg) by search query,
restricted to German-language results, and pick the best EPUB / plain-text
download URL. The admin ingest routine downloads and extracts these into the
shared reader library owned by PUBLIC_LIBRARY_OWNER_ID.

Levels are a rough CEFR hint for future UI grouping, not a hard classification.
"""

from __future__ import annotations

import json
import logging
import urllib.parse
import urllib.request
from dataclasses import dataclass

logger = logging.getLogger(__name__)

GUTENDEX_API = "https://gutendex.com/books"
_HTTP_USER_AGENT = "DeutscheSprache-Reader/1.0 (public-domain library ingest)"

# Preferred download formats, in priority order. EPUB first (its own markup gives
# us proper heading/paragraph blocks in the reader); plain text as fallback.
_EPUB_MIME_HINTS = ("application/epub+zip", "application/epub")
_TEXT_MIME_HINTS = ("text/plain; charset=utf-8", "text/plain; charset=us-ascii", "text/plain")


@dataclass(frozen=True)
class CatalogBook:
    slug: str          # stable public id (used as public_slug, never changes)
    title: str         # display title (German)
    author: str        # display author
    level: str         # rough CEFR hint: A2 / B1 / B2 / C1
    query: str         # Gutendex search string (title + author keywords)
    sort: int          # display order in the "Классика" shelf (ascending)


# Ordered easy → hard so the shelf reads as a gentle ramp for learners.
PUBLIC_LIBRARY_CATALOG: list[CatalogBook] = [
    CatalogBook("heidi", "Heidi", "Johanna Spyri", "A2", "Heidi Spyri", 10),
    CatalogBook("maerchen-grimm", "Kinder- und Hausmärchen", "Brüder Grimm", "A2", "Kinder und Hausmärchen Grimm", 20),
    CatalogBook("maerchen-hauff", "Märchen", "Wilhelm Hauff", "A2", "Märchen Hauff", 30),
    CatalogBook("taugenichts", "Aus dem Leben eines Taugenichts", "Joseph von Eichendorff", "B1", "Aus dem Leben eines Taugenichts Eichendorff", 40),
    CatalogBook("schimmelreiter", "Der Schimmelreiter", "Theodor Storm", "B1", "Der Schimmelreiter Storm", 50),
    CatalogBook("kleider-machen-leute", "Kleider machen Leute", "Gottfried Keller", "B1", "Kleider machen Leute Keller", 60),
    CatalogBook("peter-schlemihl", "Peter Schlemihls wundersame Geschichte", "Adelbert von Chamisso", "B1", "Peter Schlemihl Chamisso", 70),
    CatalogBook("winnetou-1", "Winnetou I", "Karl May", "B1", "Winnetou May", 80),
    CatalogBook("sandmann", "Der Sandmann", "E. T. A. Hoffmann", "B2", "Der Sandmann Hoffmann", 90),
    CatalogBook("verwandlung", "Die Verwandlung", "Franz Kafka", "B2", "Die Verwandlung Kafka", 100),
    CatalogBook("werther", "Die Leiden des jungen Werther", "Johann Wolfgang von Goethe", "B2", "Die Leiden des jungen Werther Goethe", 110),
    CatalogBook("effi-briest", "Effi Briest", "Theodor Fontane", "B2", "Effi Briest Fontane", 120),
    CatalogBook("wilhelm-tell", "Wilhelm Tell", "Friedrich Schiller", "B2", "Wilhelm Tell Schiller", 130),
    CatalogBook("die-raeuber", "Die Räuber", "Friedrich Schiller", "B2", "Die Räuber Schiller", 140),
    CatalogBook("wintermaerchen", "Deutschland. Ein Wintermärchen", "Heinrich Heine", "B2", "Deutschland Ein Wintermärchen Heine", 150),
    CatalogBook("prozess", "Der Process", "Franz Kafka", "C1", "Der Process Kafka", 160),
    CatalogBook("nathan-der-weise", "Nathan der Weise", "Gotthold Ephraim Lessing", "C1", "Nathan der Weise Lessing", 170),
    CatalogBook("faust-1", "Faust — Der Tragödie erster Teil", "Johann Wolfgang von Goethe", "C1", "Faust Erster Teil Goethe", 180),
    CatalogBook("zarathustra", "Also sprach Zarathustra", "Friedrich Nietzsche", "C1", "Also sprach Zarathustra Nietzsche", 190),
    CatalogBook("wahlverwandtschaften", "Die Wahlverwandtschaften", "Johann Wolfgang von Goethe", "C1", "Die Wahlverwandtschaften Goethe", 200),
]

CATALOG_BY_SLUG: dict[str, CatalogBook] = {b.slug: b for b in PUBLIC_LIBRARY_CATALOG}


def _pick_format(formats: dict, hints: tuple[str, ...]) -> str | None:
    """Return the first download URL whose mime matches a hint (ignoring .zip
    bundles, which we cannot feed straight into the extractor)."""
    for hint in hints:
        for mime, url in formats.items():
            if not isinstance(url, str):
                continue
            if mime.startswith(hint) and not url.endswith(".zip"):
                return url
    return None


def resolve_gutendex(book: CatalogBook, *, timeout: int = 30) -> dict | None:
    """Look up a catalog book on Gutendex (German only) and return the best
    download target. Returns a dict:
        {slug, gutenberg_id, matched_title, matched_author, source_type, download_url}
    or None if nothing suitable was found. Network call — used only at ingest.
    """
    params = urllib.parse.urlencode({"search": book.query, "languages": "de"})
    req = urllib.request.Request(f"{GUTENDEX_API}?{params}", headers={"User-Agent": _HTTP_USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except Exception:
        logger.exception("gutendex lookup failed slug=%s query=%r", book.slug, book.query)
        return None

    results = payload.get("results") or []
    if not results:
        logger.warning("gutendex: no German result for slug=%s query=%r", book.slug, book.query)
        return None

    for result in results:
        formats = result.get("formats") or {}
        download_url = _pick_format(formats, _EPUB_MIME_HINTS)
        source_type = "epub"
        if not download_url:
            download_url = _pick_format(formats, _TEXT_MIME_HINTS)
            source_type = "file"
        if not download_url:
            continue
        authors = result.get("authors") or []
        matched_author = authors[0].get("name") if authors and isinstance(authors[0], dict) else book.author
        return {
            "slug": book.slug,
            "gutenberg_id": result.get("id"),
            "matched_title": result.get("title") or book.title,
            "matched_author": matched_author,
            "source_type": source_type,
            "download_url": download_url,
        }

    logger.warning("gutendex: no downloadable format for slug=%s query=%r", book.slug, book.query)
    return None


def download_book_bytes(download_url: str, *, timeout: int = 60) -> bytes:
    """Download the resolved book file from Project Gutenberg."""
    req = urllib.request.Request(download_url, headers={"User-Agent": _HTTP_USER_AGENT})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()
