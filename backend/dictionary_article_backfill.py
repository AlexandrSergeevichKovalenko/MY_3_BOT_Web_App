"""
Authoritative der/die/das backfill for the shared dictionary pool
(bt_3_dictionary_entries) — "import Wiktionary genus into the pool".

Two jobs in one pass:
  1. WARM the genus cache (bt_3_wiktionary_genus_cache) for every single-word noun
     in the pool, so the runtime backstop (_authoritative_german_article) reads it
     instantly with no network.
  2. Safely CORRECT stored articles from de.wiktionary's documented genus:
       - fill a MISSING article,
       - replace a DIRTY article field ("der (Hinterwäldler)", "die (Plural)"),
       - correct a clean-but-wrong SINGULAR article ("der Kabel" -> "das Kabel").

Safety (this is the whole point — never corrupt a plural or homograph):
  - only act when de.wiktionary documents EXACTLY ONE gender,
  - SKIP plural surfaces (word_de == its own forms.plural),
  - a clean-but-wrong correction is applied ONLY when the entry is a confirmed
    SINGULAR (forms.plural exists and differs from the lemma) — the "die Fehler /
    die Eichen" plural trap is left untouched and reported for manual review.

Dry-run by default. --apply writes. DSN from $DICT_BACKFILL_DSN, else
$DATABASE_PUBLIC_URL.

    python3 -m backend.dictionary_article_backfill            # dry run + warm cache preview
    python3 -m backend.dictionary_article_backfill --apply    # warm cache + write safe fixes
    python3 -m backend.dictionary_article_backfill --limit 50 # sample first N
"""
from __future__ import annotations

import json
import os
import re
import sys
import time
import urllib.error

import psycopg2

from backend.article_wiktionary_ref import _api_fetch, genera_to_articles

_DEF_ARTS = {"der", "die", "das"}
_LEADING_ART = re.compile(r"^(der|die|das)\s+", re.IGNORECASE)
_LEMMA_OK = re.compile(r"^[A-ZÄÖÜ][A-Za-zÄÖÜäöüß-]*$")
_BATCH = 45


def _clean_article(value) -> str:
    t = str(value or "").strip().lower()
    return t if t in _DEF_ARTS else ""


def _bare_lemma(word_de: str) -> str:
    s = _LEADING_ART.sub("", str(word_de or "").strip()).strip()
    return s if _LEMMA_OK.match(s) else ""


def _title(lemma: str) -> str:
    return lemma[:1].upper() + lemma[1:]


def _fetch_with_backoff(batch: list[str]) -> dict[str, str]:
    """_api_fetch, but retry on HTTP 429 / transient errors with backoff."""
    delay = 1.0
    for attempt in range(5):
        try:
            return _api_fetch(batch)
        except urllib.error.HTTPError as exc:
            if exc.code == 429 and attempt < 4:
                time.sleep(delay)
                delay *= 2
                continue
            raise
        except Exception:
            if attempt < 4:
                time.sleep(delay)
                delay *= 2
                continue
            raise
    return {}


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


def _warm_and_map(conn, lemmas: list[str]) -> dict[str, set]:
    """Fetch genus for the lemmas' titles, PERSIST the raw genus codes into
    bt_3_wiktionary_genus_cache, and return {lemma: {article,...}}."""
    titles = sorted({_title(l) for l in lemmas})
    code_by_title: dict[str, str] = {}
    for i in range(0, len(titles), _BATCH):
        batch = titles[i:i + _BATCH]
        got = _fetch_with_backoff(batch)
        for t in batch:
            code_by_title[t] = got.get(t, "-")
        time.sleep(0.2)
        print(f"  …wiktionary {min(i + _BATCH, len(titles))}/{len(titles)} titles", file=sys.stderr)

    # Warm the runtime cache (title -> genus code) so the save-path backstop is
    # instant for every one of these lemmas, even the ones already correct.
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO bt_3_wiktionary_genus_cache (title, genus, checked_at)
            VALUES (%s, %s, NOW())
            ON CONFLICT (title) DO UPDATE SET genus = EXCLUDED.genus, checked_at = NOW()
            """,
            sorted(code_by_title.items()),
        )
    conn.commit()
    print(f"Warmed genus cache: {len(code_by_title):,} titles.")

    return {l: genera_to_articles(code_by_title.get(_title(l), "-")) for l in lemmas}


def main() -> None:
    apply = "--apply" in sys.argv
    limit = None
    for i, a in enumerate(sys.argv):
        if a == "--limit" and i + 1 < len(sys.argv):
            limit = int(sys.argv[i + 1])
        elif a.startswith("--limit="):
            limit = int(a.split("=", 1)[1])

    dsn = os.getenv("DICT_BACKFILL_DSN") or os.getenv("DATABASE_PUBLIC_URL")
    if not dsn:
        print("No DSN: set DICT_BACKFILL_DSN or DATABASE_PUBLIC_URL", file=sys.stderr)
        sys.exit(1)

    conn = psycopg2.connect(dsn)
    conn.autocommit = False

    # Ensure the cache table exists (matches database.ensure_wiktionary_genus_cache_schema).
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS bt_3_wiktionary_genus_cache (
                title      TEXT PRIMARY KEY,
                genus      TEXT NOT NULL,
                checked_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
    conn.commit()

    rows = _load_nouns(conn)
    print(f"Loaded {len(rows):,} noun entries from the pool.")

    work = []  # (id, source_text, word_de_col, rj, bare, stored, raw_field, plural_bare)
    for _id, source_text, word_de_col, rj in rows:
        rj = rj if isinstance(rj, dict) else {}
        bare = _bare_lemma(rj.get("word_de") or word_de_col or "")
        if not bare:
            continue
        forms = rj.get("forms") if isinstance(rj.get("forms"), dict) else {}
        plural_bare = _LEADING_ART.sub("", str(forms.get("plural") or "").strip()).strip()
        work.append((_id, source_text, word_de_col, rj, bare,
                     _clean_article(rj.get("article")), str(rj.get("article") or "").strip(), plural_bare))
    if limit:
        work = work[:limit]
    print(f"{len(work):,} single-word noun entries eligible for a genus check.")

    genus = _warm_and_map(conn, [w[4] for w in work])

    safe, review = [], []
    stats = {"ok": 0, "no_wiktionary": 0, "two_gender": 0,
             "fill_missing": 0, "fix_dirty": 0, "correct_wrong": 0,
             "skip_plural": 0, "review_correct": 0}
    for _id, source_text, word_de_col, rj, bare, stored, raw_field, plural_bare in work:
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

        is_plural_surface = bool(plural_bare) and plural_bare.lower() == bare.lower()
        singular_confirmed = bool(plural_bare) and plural_bare.lower() != bare.lower()
        rec = (_id, source_text, word_de_col, rj, bare, stored or raw_field or "∅", auth)

        if is_plural_surface:
            stats["skip_plural"] += 1
            review.append((*rec, "plural_surface"))
            continue
        if not stored and not raw_field:
            stats["fill_missing"] += 1
            safe.append((*rec, "fill_missing"))
        elif not stored and raw_field:
            stats["fix_dirty"] += 1
            safe.append((*rec, "fix_dirty"))
        else:
            # clean-but-wrong: apply ONLY when confirmed singular; else review.
            stats["correct_wrong"] += 1
            if singular_confirmed:
                safe.append((*rec, "correct_wrong"))
            else:
                stats["review_correct"] += 1
                review.append((*rec, "correct_unconfirmed"))

    print("\n=== SUMMARY ===")
    for k in ("ok", "fill_missing", "fix_dirty", "correct_wrong", "skip_plural",
              "review_correct", "two_gender", "no_wiktionary"):
        print(f"  {k:16s}: {stats[k]:,}")
    print(f"  SAFE TO APPLY : {len(safe):,}")
    print(f"  NEED REVIEW   : {len(review):,}")

    def _show(title, rows_):
        if not rows_:
            return
        print(f"\n--- {title} ({len(rows_)}) ---")
        for _id, source_text, _wc, _rj, bare, before, auth, klass in rows_[:40]:
            print(f"  #{_id} [{klass}]  {before:>26} -> {auth} {bare}   (src: {str(source_text)[:22]})")
        if len(rows_) > 40:
            print(f"  … +{len(rows_) - 40} more")

    _show("SAFE", safe)
    _show("REVIEW (not auto-applied)", review)

    if not apply:
        print("\nDRY RUN — genus cache WAS warmed; no article writes. Re-run with --apply.")
        conn.close()
        return

    written = 0
    skipped = 0
    with conn.cursor() as cur:
        for _id, _src, word_de_col, rj, bare, _before, auth, _k in safe:
            # ДВЕРЬ ПЕРЕД ЗАПИСЬЮ. Прогон разовый и запускается руками, но пишет он в
            # ОБЩИЙ пул — то есть в карточку, которую увидят все, кто искал это слово.
            # Заголовок здесь склеивается из артикля и основы, и обе части приходят
            # снаружи: артикль от справочника, основа из старой записи. Значит вход
            # чистится тем же кодом, что и на остальных дверях словаря, а разбор
            # проверяется тем же стражем целостности — иначе прогон, запущенный дважды,
            # положит в пул то, от чего мы стережём все прочие входы.
            from backend.dictionary_intake import clean_text
            from backend.mangled_text import mangled_strings_inside
            bare = clean_text(bare)
            if not bare:
                skipped += 1
                continue
            new_word_de = clean_text(f"{auth} {bare}")
            new_rj = dict(rj)
            new_rj["article"] = auth
            new_rj["word_de"] = new_word_de
            порча = mangled_strings_inside(new_rj)
            if порча:
                print(f"  ПРОПУЩЕНО {_id}: текст размножен сам на себя — {порча[0][:60]!r}")
                skipped += 1
                continue
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
                print(f"  committed {written}/{len(safe)}")
    conn.commit()
    conn.close()
    print(f"\nAPPLIED {written:,} safe article corrections. {len(review):,} left for review.")
    if skipped:
        print(f"ПРОПУЩЕНО дверью: {skipped:,} — вход не прошёл чистку или разбор испорчен.")


if __name__ == "__main__":
    main()
