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
# Noun overview templates carry the canonical grammatical gender(s). Reading genus
# only from these (not stray page text) avoids picking up unrelated mentions.
_OVERVIEW = re.compile(r"\{\{Deutsch[- ]Substantiv[- ]Übersicht[^}]*\}\}", re.DOTALL)
_GENUS = re.compile(r"\bGenus\s*\d*\s*=\s*([mfn])\b")


def _genera_from_wikitext(wikitext: str) -> str:
    """Sorted genus letters documented for the German noun, e.g. 'm', 'fm', 'fn';
    '-' if none. A word with several documented genera (regional/rare variants or
    genuinely two-gender) yields more than one letter — the audit treats a stored
    article as correct if it's among them."""
    m = _DE_HEADER.search(wikitext)
    blk = wikitext[m.end():] if m else None
    if blk is not None:
        nxt = _NEXT_LANG.search(blk)
        if nxt:
            blk = blk[:nxt.start()]
    if blk is None:
        return "-"
    genera: set[str] = set()
    for tmpl in _OVERVIEW.findall(blk):
        genera.update(_GENUS.findall(tmpl))
    if not genera:
        return "-"
    return "".join(sorted(genera))


def genera_to_articles(code: str | None) -> set:
    """'fm' → {'die','der'}; '-'/'' → set()."""
    return {_GENUS_TO_ARTICLE[c] for c in str(code or "") if c in _GENUS_TO_ARTICLE}


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
        out[title] = _genera_from_wikitext(content)
    return out


def genus_for_titles(titles: list[str]) -> dict[str, str]:
    """Cache-aware genus lookup for a set of exact page titles.
    Returns {title: genus-code} — sorted genus letters ('m','fm',…) or '-' — for
    every title we could resolve (cached or fetched). Failed fetches are omitted."""
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
    Returns {word: {"articles": set[str], "code": str, "basis": str}} where
    `articles` is the SET of documented genders (empty if the page has none) and
    `basis` ∈ {"direct", "none"}. The audit accepts a stored article that is among
    `articles`, and only proposes a fix when a single gender is documented.

    Deliberately NO compound-head decomposition: naive suffix matching grabs
    garbage substrings (Betriebs‑kosten→"Osten", Kassen‑bereich→"Reich") and can't
    handle plurale-tantum, producing FALSE corrections. Words with no direct entry
    fall to the curated deterministic guard (audit layer) or 'unknown'."""
    words = [str(w).strip() for w in words if str(w).strip()]
    titles = {w: (w[:1].upper() + w[1:]) for w in words}
    genus = genus_for_titles(list(set(titles.values())))

    out: dict[str, dict] = {}
    for w in words:
        code = genus.get(titles[w], "-")
        arts = genera_to_articles(code)
        out[w] = {"articles": arts, "code": code, "basis": "direct" if arts else "none"}
    return out
