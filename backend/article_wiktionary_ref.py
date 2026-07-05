"""
German Wiktionary genus reference — authoritative der/die/das lookup for the
Artikel Sprint correctness audit.

For a noun we ask de.wiktionary.org for the page wikitext, isolate the German
language section, and read the grammatical genus (Genus=m/f/n) from the noun
template. Multiple distinct genera → the word itself is two-gender (ambiguous).
When a compound has no direct entry we decompose it: the head (Grundwort) is the
longest trailing substring that is itself a German noun with a single genus, and
the compound inherits that genus (der Wert → der Börsenwert).

Results are cached per title in bt_3_wiktionary_genus_cache so repeat runs and the
apply-fix pass never re-hit the network. Genus codes stored: 'm'/'f'/'n' (single),
'x' (two-gender), '-' (page missing or no noun genus).
"""
from __future__ import annotations

import json
import logging
import re
import time
import urllib.parse
import urllib.request

_API = "https://de.wiktionary.org/w/api.php"
_UA = "DeutschBot/1.0 (Artikel Sprint article audit; contact via Telegram bot)"
_BATCH = 45  # MediaWiki allows 50 titles/query; keep margin.

_GENUS_TO_ARTICLE = {"m": "der", "f": "die", "n": "das"}

# German section header, e.g. "== Börsenwert ({{Sprache|Deutsch}}) =="
_DE_HEADER = re.compile(r"==\s*[^=]*\(\{\{Sprache\|Deutsch\}\}\)\s*==")
_NEXT_LANG = re.compile(r"\n==\s*[^=].*\(\{\{Sprache\|")
_IS_NOUN = re.compile(r"\{\{Wortart\|Substantiv\|Deutsch\}\}")
_GENUS = re.compile(r"\bGenus[^=\n]*=\s*([mfn])\b")


def article_from_genus(genus: str | None) -> str | None:
    return _GENUS_TO_ARTICLE.get(str(genus or ""))


def _german_block(wikitext: str) -> str | None:
    m = _DE_HEADER.search(wikitext)
    if not m:
        return None
    start = m.end()
    nxt = _NEXT_LANG.search(wikitext[start:])
    return wikitext[start: start + nxt.start()] if nxt else wikitext[start:]


def _genus_from_wikitext(wikitext: str) -> str:
    """'m'/'f'/'n' for a single genus, 'x' for two-gender, '-' otherwise."""
    blk = _german_block(wikitext)
    if blk is None:
        return "-"
    if not _IS_NOUN.search(blk) and "Substantiv Übersicht" not in blk:
        return "-"
    genera = set(_GENUS.findall(blk))
    if not genera:
        return "-"
    return "x" if len(genera) > 1 else genera.pop()


def _api_fetch(titles: list[str]) -> dict[str, str]:
    """Raw network fetch → {title: genus}. Missing pages map to '-'. Never raises;
    on error returns {} so the caller treats those titles as unknown this run."""
    params = {
        "action": "query", "format": "json", "formatversion": "2",
        "prop": "revisions", "rvprop": "content", "rvslots": "main",
        "redirects": "1", "titles": "|".join(titles),
    }
    url = _API + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": _UA})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read())
    except Exception:
        logging.warning("wiktionary genus fetch failed (%d titles)", len(titles), exc_info=True)
        return {}

    out: dict[str, str] = {}
    # Map any redirect normalizations back so we key on the requested title.
    query = data.get("query") or {}
    for p in query.get("pages") or []:
        title = str(p.get("title") or "")
        if p.get("missing"):
            out[title] = "-"
            continue
        try:
            content = p["revisions"][0]["slots"]["main"]["content"]
        except (KeyError, IndexError, TypeError):
            out[title] = "-"
            continue
        out[title] = _genus_from_wikitext(content)
    return out


def genus_for_titles(titles: list[str]) -> dict[str, str]:
    """Cache-aware genus lookup for a set of exact page titles.
    Returns {title: 'm'|'f'|'n'|'x'|'-'} for every title we could resolve (cached
    or freshly fetched). Titles whose network fetch failed are simply omitted."""
    from backend.database import (
        ensure_wiktionary_genus_cache_schema, get_cached_genus, upsert_genus_cache,
    )
    uniq = sorted({str(t).strip() for t in titles if t and str(t).strip()})
    if not uniq:
        return {}
    ensure_wiktionary_genus_cache_schema()
    result = dict(get_cached_genus(uniq))
    missing = [t for t in uniq if t not in result]

    fresh: dict[str, str] = {}
    for i in range(0, len(missing), _BATCH):
        batch = missing[i:i + _BATCH]
        got = _api_fetch(batch)
        # Any requested title the API didn't return (should be rare) → treat as '-'.
        for t in batch:
            if t not in got:
                got[t] = "-"
        fresh.update(got)
        time.sleep(0.1)  # be polite to the API
    if fresh:
        upsert_genus_cache(fresh)
        result.update(fresh)
    return result


def reference_articles(words: list[str]) -> dict[str, dict]:
    """Authoritative verdict per word from the word's OWN Wiktionary page.
    Returns {word: {"article": der/die/das|None, "genus": code, "basis": str}}.
      basis ∈ {"direct", "none"}; genus 'x' means two-gender (ambiguous).

    Deliberately NO compound-head decomposition: naive suffix matching grabs
    garbage substrings (Betriebs‑kosten→"Osten", Kassen‑bereich→"Reich") and can't
    handle plurale-tantum, producing FALSE corrections. Words with no direct entry
    are returned article=None and left to the curated deterministic guard (in the
    audit layer) or reported as 'unknown' — never auto-fixed on a guess."""
    words = [str(w).strip() for w in words if str(w).strip()]
    titles = {w: (w[:1].upper() + w[1:]) for w in words}
    genus = genus_for_titles(list(set(titles.values())))

    out: dict[str, dict] = {}
    for w in words:
        g = genus.get(titles[w], "-")
        if g in _GENUS_TO_ARTICLE:
            out[w] = {"article": _GENUS_TO_ARTICLE[g], "genus": g, "basis": "direct"}
        elif g == "x":
            out[w] = {"article": None, "genus": "x", "basis": "direct"}
        else:
            out[w] = {"article": None, "genus": "-", "basis": "none"}
    return out
