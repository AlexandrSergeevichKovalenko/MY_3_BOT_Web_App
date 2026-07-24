"""German word-frequency backbone — Stage 0 of the living shared dictionary.

The «урезанный / ядро» tier is «top-N most frequent words». Frequency is decided by an
OBJECTIVE corpus list (OpenSubtitles 2018, top 50k, via hermitdave/FrequencyWords, MIT),
not by GPT guesses or by hand. `rank` = position in that list (1 = most frequent).

This module: (1) loads the list into bt_3_word_frequency, (2) adds a frequency_rank column
to the shared pool bt_3_dictionary_entries and backfills it by matching each single-word
German lemma to its corpus rank, (3) exposes a fast lookup for the insert path.

Multi-word phrases / sentences get no rank (rank stays NULL) — they belong to «полный» but
never to «ядро». Everything is additive and idempotent (safe to re-run)."""

import os
import logging

from psycopg2.extras import execute_values

_FREQ_FILE = os.path.join(os.path.dirname(__file__), "data", "de_freq_50k.txt")
_FREQ_SOURCE = "opensubtitles_2018_de_50k"

# Leading articles/determiners to strip before matching (the corpus list carries bare lemmas).
_LEADING_ARTICLES = {
    "der", "die", "das", "ein", "eine", "einen", "einem", "einer", "eines",
    "des", "dem", "den", "sich", "zu",
}


def normalize_frequency_lemma(word_de: str | None) -> str:
    """Reduce a pool German headword to a single lowercase lemma matchable against the corpus
    list, or "" when it is not a single word (phrase/sentence/garbage → no frequency rank)."""
    if not word_de:
        return ""
    parts = str(word_de).strip().split()
    if parts and parts[0].lower() in _LEADING_ARTICLES and len(parts) == 2:
        parts = parts[1:]
    if len(parts) != 1:
        return ""
    tok = parts[0].strip().strip(".,;:!?\"'«»()[]").lower()
    if not tok or any(ch.isdigit() for ch in tok):
        return ""
    # German letters only (incl. umlauts / ß, and hyphen for compounds).
    if not all(ch.isalpha() or ch in "äöüß-" for ch in tok):
        return ""
    return tok


def ensure_frequency_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS bt_3_word_frequency (
                lemma      TEXT PRIMARY KEY,
                rank       INTEGER NOT NULL,
                freq_count BIGINT,
                lang       TEXT NOT NULL DEFAULT 'de',
                source     TEXT NOT NULL DEFAULT 'opensubtitles_2018_de_50k'
            );
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_word_frequency_rank ON bt_3_word_frequency(rank);")
        cur.execute("ALTER TABLE bt_3_dictionary_entries ADD COLUMN IF NOT EXISTS frequency_rank INTEGER;")
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_dict_entries_freq_rank "
            "ON bt_3_dictionary_entries(frequency_rank) WHERE frequency_rank IS NOT NULL;"
        )
    conn.commit()


def import_frequency_list(conn, path: str | None = None) -> int:
    """Load the corpus list into bt_3_word_frequency (idempotent upsert). Returns row count."""
    src = path or _FREQ_FILE
    rows: list[tuple[str, int, int]] = []
    seen: set[str] = set()
    with open(src, "r", encoding="utf-8") as fh:
        for i, line in enumerate(fh, start=1):
            line = line.strip()
            if not line:
                continue
            bits = line.split()
            lemma = normalize_frequency_lemma(bits[0])
            if not lemma or lemma in seen:
                continue
            seen.add(lemma)
            count = int(bits[1]) if len(bits) > 1 and bits[1].isdigit() else 0
            rows.append((lemma, len(rows) + 1, count))
    if not rows:
        return 0
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO bt_3_word_frequency (lemma, rank, freq_count, lang, source)
            VALUES %s
            ON CONFLICT (lemma) DO UPDATE SET
                rank = EXCLUDED.rank,
                freq_count = EXCLUDED.freq_count,
                source = EXCLUDED.source;
            """,
            [(lemma, rank, count, "de", _FREQ_SOURCE) for (lemma, rank, count) in rows],
            page_size=1000,
        )
    conn.commit()
    logging.info("dictionary_frequency: imported %s corpus lemmas from %s", len(rows), src)
    return len(rows)


def backfill_pool_frequency_ranks(conn, batch_size: int = 2000, only_unranked: bool = False) -> dict:
    """Match every German-side pool entry's single-word lemma to its corpus rank and store it.
    only_unranked=True processes just rows without a rank yet (cheap nightly top-up of new words).
    Returns {scanned, ranked}."""
    freq: dict[str, int] = {}
    with conn.cursor() as cur:
        cur.execute("SELECT lemma, rank FROM bt_3_word_frequency;")
        for lemma, rank in cur.fetchall():
            freq[lemma] = int(rank)

    scanned = 0
    ranked = 0
    last_id = 0
    while True:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, word_de, source_lang, target_lang, source_text, target_text
                FROM bt_3_dictionary_entries
                WHERE id > %s {only}
                ORDER BY id ASC
                LIMIT %s;
                """.format(only="AND frequency_rank IS NULL" if only_unranked else ""),
                (last_id, batch_size),
            )
            batch = cur.fetchall() or []
        if not batch:
            break
        updates: list[tuple[int, int]] = []
        for row in batch:
            entry_id, word_de, s_lang, t_lang, s_text, t_text = row
            last_id = int(entry_id)
            scanned += 1
            # The German side may sit in word_de, or in source/target text by direction.
            german = word_de
            if not german:
                german = s_text if (s_lang or "").lower() == "de" else t_text if (t_lang or "").lower() == "de" else None
            lemma = normalize_frequency_lemma(german)
            rank = freq.get(lemma) if lemma else None
            if rank is not None:
                updates.append((int(rank), int(entry_id)))
        if updates:
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    """
                    UPDATE bt_3_dictionary_entries AS e
                    SET frequency_rank = v.rank
                    FROM (VALUES %s) AS v(rank, id)
                    WHERE e.id = v.id;
                    """,
                    updates,
                    page_size=1000,
                )
            conn.commit()
            ranked += len(updates)
    logging.info("dictionary_frequency: backfilled ranks scanned=%s ranked=%s", scanned, ranked)
    return {"scanned": scanned, "ranked": ranked}


def frequency_rank_for_word(cursor, word_de: str | None) -> int | None:
    """Fast single lookup for the insert path — the corpus rank of a German headword, or None."""
    lemma = normalize_frequency_lemma(word_de)
    if not lemma:
        return None
    cursor.execute("SELECT rank FROM bt_3_word_frequency WHERE lemma = %s LIMIT 1;", (lemma,))
    row = cursor.fetchone()
    return int(row[0]) if row and row[0] is not None else None


def run_stage0(conn, path: str | None = None) -> dict:
    """One-shot: ensure schema → import corpus → backfill pool ranks. Idempotent."""
    ensure_frequency_schema(conn)
    imported = import_frequency_list(conn, path=path)
    stats = backfill_pool_frequency_ranks(conn)
    return {"imported_lemmas": imported, **stats}
