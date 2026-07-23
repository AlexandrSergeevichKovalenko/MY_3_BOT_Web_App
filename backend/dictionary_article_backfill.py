"""
Authoritative der/die/das backfill for the shared dictionary pool
(bt_3_dictionary_entries) — "import Wiktionary genus into the pool".

For every single-word NOUN entry we ask de.wiktionary for the DOCUMENTED
grammatical genus (via backend.article_wiktionary_ref, the same authoritative
source the Artikel-Sprint audit uses) and, when a SINGLE gender is documented,
treat it as the source of truth:
  - fills a MISSING article,
  - replaces a DIRTY article field (prose like "der (Hinterwäldler)"),
  - CORRECTS a clean-but-wrong article (the "der Kabel" -> "das Kabel" class that
    the gpt-4.1-mini switch exposed).

The verdict is written into response_json.article, response_json.word_de
("<art> <Lemma>") and the top-level word_de column (kept in sync), so the served
card and the client declension engine agree. Two-gender or undocumented words are
left untouched -- we never overwrite with a guess.

Dry-run by default (writes nothing, prints what it WOULD do). Pass --apply to
write. DSN from $DICT_BACKFILL_DSN, else $DATABASE_PUBLIC_URL.

    python3 -m backend.dictionary_article_backfill            # dry run
    python3 -m backend.dictionary_article_backfill --apply    # write
    python3 -m backend.dictionary_article_backfill --limit 50 # sample first N
"""
from __future__ import annotations

import json
import os
import re
import sys
import time

import psycopg2

from backend.article_wiktionary_ref import _api_fetch, genera_to_articles

_DEF_ARTS = {"der", "die", "das"}
_LEADING_ART = re.compile(r"^(der|die|das)\s+", re.IGNORECASE)
# A clean single German noun lemma: one token, starts uppercase, letters only
# (umlauts/ss and an internal hyphen for compounds like "E-Mail" are allowed).
_LEMMA_OK = re.compile(r"^[A-ZÄÖÜ][A-Za-zÄÖÜäöüß-]*$")
_BATCH = 45


def _clean_article(value) -> str:
    t = str(value or "").strip().lower()
    return t if t in _DEF_ARTS else ""


def _bare_lemma(word_de: str) -> str:
    """Bare single-word noun from word_de ('das Kabel' -> 'Kabel'), or '' if it
    isn't a clean single lemma (phrases, lowercase, punctuation -> skip)."""
    s = _LEADING_ART.sub("", str(word_de or "").strip()).strip()
    return s if _LEMMA_OK.match(s) else ""


def _title(lemma: str) -> str:
    return lemma[:1].upper() + lemma[1:]


def _load_nouns(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, source_text, word_de, response_json
            FROM bt_3_dictionary_entries
            WHERE lower(response_json->>'part_of_speech') = 'noun'
            ORDER BY id
            """
        )
        return cur.fetchall()


def _genus_map(lemmas: list[str]) -> dict[str, set]:
    """{lemma: {article,...}} from de.wiktionary, single-batched. Only lemmas with
    a documented genus appear; two-gender lemmas map to a 2+ element set."""
    titles = sorted({_title(l) for l in lemmas})
    by_title: dict[str, set] = {}
    for i in range(0, len(titles), _BATCH):
        batch = titles[i:i + _BATCH]
        got = _api_fetch(batch)
        for t in batch:
            by_title[t] = genera_to_articles(got.get(t, "-"))
        time.sleep(0.1)
        print(f"  …wiktionary {min(i + _BATCH, len(titles))}/{len(titles)} titles",
              file=sys.stderr)
    return {l: by_title.get(_title(l), set()) for l in lemmas}


def main() -> None:
    apply = "--apply" in sys.argv
    limit = None
    for a in sys.argv:
        if a.startswith("--limit"):
            try:
                limit = int(a.split("=", 1)[1]) if "=" in a else int(sys.argv[sys.argv.index(a) + 1])
            except Exception:
                limit = None

    dsn = os.getenv("DICT_BACKFILL_DSN") or os.getenv("DATABASE_PUBLIC_URL")
    if not dsn:
        print("No DSN: set DICT_BACKFILL_DSN or DATABASE_PUBLIC_URL", file=sys.stderr)
        sys.exit(1)

    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    rows = _load_nouns(conn)
    print(f"Loaded {len(rows):,} noun entries from the pool.")

    # Build the work list: entries whose word_de is a clean single lemma.
    work = []  # (id, source_text, word_de_col, rj, bare, stored_article)
    for _id, source_text, word_de_col, rj in rows:
        rj = rj if isinstance(rj, dict) else {}
        bare = _bare_lemma(rj.get("word_de") or word_de_col or "")
        if not bare:
            continue
        work.append((_id, source_text, word_de_col, rj, bare, _clean_article(rj.get("article"))))
    if limit:
        work = work[:limit]
    print(f"{len(work):,} single-word noun entries eligible for a genus check.")

    genus = _genus_map([w[4] for w in work])

    # Classify every proposed change.
    changes = []  # (id, bare, stored, authoritative, klass)
    stats = {"ok": 0, "no_wiktionary": 0, "two_gender": 0,
             "fill_missing": 0, "fix_dirty": 0, "correct_wrong": 0}
    for _id, source_text, word_de_col, rj, bare, stored in work:
        arts = genus.get(bare, set())
        if not arts:
            stats["no_wiktionary"] += 1
            continue
        if len(arts) != 1:
            stats["two_gender"] += 1
            continue
        auth = next(iter(arts))
        if stored == auth:
            stats["ok"] += 1
            continue
        raw_field = str(rj.get("article") or "").strip()
        if not stored and not raw_field:
            klass = "fill_missing"
        elif not stored and raw_field:
            klass = "fix_dirty"
        else:
            klass = "correct_wrong"
        stats[klass] += 1
        changes.append((_id, source_text, word_de_col, rj, bare, stored or raw_field or "∅", auth, klass))

    print("\n=== SUMMARY ===")
    for k in ("ok", "fill_missing", "fix_dirty", "correct_wrong", "two_gender", "no_wiktionary"):
        print(f"  {k:16s}: {stats[k]:,}")
    print(f"  TOTAL CHANGES : {len(changes):,}")

    # Show the most important class first (clean-but-wrong), then samples.
    def _show(klass, n=25):
        sel = [c for c in changes if c[7] == klass]
        if not sel:
            return
        print(f"\n--- {klass} ({len(sel)}) ---")
        for _id, source_text, _wc, _rj, bare, before, auth, _k in sel[:n]:
            print(f"  #{_id}  {before:>28} -> {auth} {bare}   (src: {str(source_text)[:24]})")
        if len(sel) > n:
            print(f"  … +{len(sel) - n} more")

    _show("correct_wrong")
    _show("fix_dirty")
    _show("fill_missing")

    if not apply:
        print("\nDRY RUN — nothing written. Re-run with --apply to persist.")
        conn.close()
        return

    # Apply: update response_json.article + response_json.word_de and, when the
    # top-level word_de column mirrors the same lemma, keep it in sync too.
    written = 0
    with conn.cursor() as cur:
        for _id, _src, word_de_col, rj, bare, _before, auth, _k in changes:
            new_word_de = f"{auth} {bare}"
            new_rj = dict(rj)
            new_rj["article"] = auth
            new_rj["word_de"] = new_word_de
            col_bare = _bare_lemma(word_de_col or "")
            if col_bare and col_bare == bare:
                cur.execute(
                    "UPDATE bt_3_dictionary_entries SET response_json=%s, word_de=%s, updated_at=NOW() WHERE id=%s",
                    (json.dumps(new_rj, ensure_ascii=False), new_word_de, _id),
                )
            else:
                cur.execute(
                    "UPDATE bt_3_dictionary_entries SET response_json=%s, updated_at=NOW() WHERE id=%s",
                    (json.dumps(new_rj, ensure_ascii=False), _id),
                )
            written += 1
            if written % 100 == 0:
                conn.commit()
                print(f"  committed {written}/{len(changes)}")
    conn.commit()
    conn.close()
    print(f"\nAPPLIED {written:,} article corrections to the pool.")


if __name__ == "__main__":
    main()
