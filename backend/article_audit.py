"""
Artikel Sprint article-correctness audit.

Cross-checks every stored der/die/das against an authoritative reference and
reports mismatches. Reference precedence per word:
  1. Two-gender / person-adjective words → 'ambiguous' (no single article; flagged).
  2. Wiktionary genus (direct page, else compound-head decomposition) — the эталон.
  3. Deterministic gender guard (strong_gender) — corroborates and covers the
     compounds Wiktionary misses.
When Wiktionary and the guard disagree → 'conflict' (surfaced, never auto-fixed).

audit_all() only READS + reports. apply_fixes() writes the confirmed corrections.
Both are theme-scopable. Genus lookups are cached in the DB, so apply runs off the
same data the report was built from without new network calls.
"""
from __future__ import annotations

import logging

_CHUNK = 150  # words per reference batch (for progress + bounded memory)


def _iter_rows(theme_keys: list[str] | None) -> list[dict]:
    from backend.database import list_all_article_sprint_rows, list_article_sprint_rows
    if not theme_keys:
        return list_all_article_sprint_rows()
    rows: list[dict] = []
    for key in theme_keys:
        for r in list_article_sprint_rows(key):
            rows.append({"id": r["id"], "theme_key": key, "word": r["word"], "article": r["article"]})
    return rows


def _classify(rows: list[dict], progress_cb=None) -> dict:
    from backend.article_sprint_generator import (
        strong_gender, is_ambiguous_noun, is_nominalized_person_adjective,
    )
    from backend.article_wiktionary_ref import reference_articles

    report = {
        "checked": len(rows), "ok": 0,
        "mismatch": [], "conflict": [], "ambiguous": [], "unknown": [],
    }
    done = 0
    for i in range(0, len(rows), _CHUNK):
        chunk = rows[i:i + _CHUNK]
        refs = reference_articles([r["word"] for r in chunk])
        for r in chunk:
            w, stored = r["word"], str(r["article"]).lower()
            ref = refs.get(w, {})
            wik_art = ref.get("article")
            wik_genus = ref.get("genus")
            basis = ref.get("basis", "none")

            # 1) inherently ambiguous — two-gender roots or person-adjective nouns.
            if is_ambiguous_noun(w) or wik_genus == "x":
                why = ("person-adj" if is_nominalized_person_adjective(w)
                       else ("two-gender/wiktionary" if wik_genus == "x" else "two-gender/list"))
                report["ambiguous"].append(
                    {"id": r["id"], "theme": r["theme_key"], "word": w, "stored": stored, "why": why})
                continue

            guard = strong_gender(w)
            # 2) resolve the reference article.
            if wik_art and guard and wik_art != guard:
                report["conflict"].append(
                    {"id": r["id"], "theme": r["theme_key"], "word": w,
                     "stored": stored, "wiktionary": wik_art, "guard": guard, "basis": basis})
                continue
            ref_art = wik_art or guard
            ref_basis = basis if wik_art else ("guard" if guard else "none")
            if wik_art and guard:
                ref_basis = "wiktionary+guard"

            if not ref_art:
                report["unknown"].append(
                    {"id": r["id"], "theme": r["theme_key"], "word": w, "stored": stored})
                continue

            if ref_art == stored:
                report["ok"] += 1
            else:
                report["mismatch"].append(
                    {"id": r["id"], "theme": r["theme_key"], "word": w,
                     "stored": stored, "ref": ref_art, "basis": ref_basis})
        done += len(chunk)
        if progress_cb:
            try:
                progress_cb(done, len(rows))
            except Exception:
                pass
    return report


def audit_all(theme_keys: list[str] | None = None, progress_cb=None) -> dict:
    """Read-only audit. Returns a report dict with mismatch/conflict/ambiguous/unknown
    buckets and counts. Changes nothing."""
    rows = _iter_rows(theme_keys)
    report = _classify(rows, progress_cb=progress_cb)
    report["counts"] = {
        "checked": report["checked"], "ok": report["ok"],
        "mismatch": len(report["mismatch"]), "conflict": len(report["conflict"]),
        "ambiguous": len(report["ambiguous"]), "unknown": len(report["unknown"]),
    }
    return report


def apply_fixes(theme_keys: list[str] | None = None, progress_cb=None) -> dict:
    """Apply the confirmed corrections (mismatch bucket only) to the article bank.
    Ambiguous / conflict / unknown rows are left untouched for manual review.
    Re-derives from the (now cached) reference, so it fixes exactly what a fresh
    audit reports."""
    report = audit_all(theme_keys, progress_cb=progress_cb)
    from backend.database import update_article_sprint_article
    fixed = 0
    examples: list[str] = []
    for m in report["mismatch"]:
        try:
            update_article_sprint_article(m["id"], m["ref"])
            fixed += 1
            if len(examples) < 25:
                examples.append(f"{m['stored']} → {m['ref']} {m['word']} ({m['basis']})")
        except Exception:
            logging.warning("apply_fixes: update failed id=%s word=%s", m["id"], m["word"], exc_info=True)
    return {"fixed": fixed, "attempted": len(report["mismatch"]),
            "conflict": len(report["conflict"]), "ambiguous": len(report["ambiguous"]),
            "unknown": len(report["unknown"]), "examples": examples}
