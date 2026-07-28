# 04b — Word cards ↔ the "units layer" (the "matrix of ones")

Prerequisites: [04](04_shared_word_cache.md) (the shared word pool, `canonical_entry_id`, `ON
CONFLICT`, `JSONB`). This block is a companion to 04 — it documents a **newer, deeper** rework of
how a saved vocabulary card knows what word it is. Built 2026-07-27 (commits `7ab5a9b4`, `f14778c6`,
`4f54638c`, `d46d3afb`).

**The one-paragraph summary.** Before, a saved card knew only its own text — the string `"der
Rüpel"`. To learn the article or the forms, the app had to re-search *by that text*, and the text
could be written any number of ways (`"Rüpel"`, `"der Rüpel"`, `"den Rüpeln"`). Worse: one word
lived in many rows, the reverse direction (`враг → der Feind`) wasn't found, and one word's analysis
could stick to another word's headword (a card `«der Flegel»` ended up with the forms of `«der
Rüpel»`). The fix is a **units layer**: every real word becomes one **unit** with a stable id, and a
card carries a **pointer** to its unit — like a barcode on a product. Through that pointer the card
resolves to the article (`der`), the forms (`die Rüpel / des Rüpels`), the senses, and examples.

You called it the **"matrix of ones"**, and that name is exactly right — section 2 explains why.

# ⚙️ 1. Under the hood

## 1.1 Vocabulary: some plain terms first

- **Card** — one row in a user's personal dictionary, `bt_3_webapp_dictionary_queries` (has
  `user_id`; block 04 §1.9). This is "a word *this user* saved".
- **Lexeme / unit** — the abstract word itself, independent of who saved it or how it's spelled.
  `«der Rüpel»` is one lexeme; `«Rüpel»`, `«den Rüpeln»`, `«des Rüpels»` are all spellings *of* it.
- **Lemma** — the canonical, dictionary-headword spelling of a lexeme, without the article: `Rüpel`.
- **POS** — "part of speech" (noun, verb, adjective…).
- **Homograph** — two different words spelled the same. German disambiguates them by gender: `der
  Kiefer` (jaw) vs `die Kiefer` (pine tree) are **different** lexemes with the **same** spelling.
- **Surface form** — any concrete way a lexeme is written in text (`Rüpeln`, `des Rüpels`).

The old world stored a card's *surface text* and treated that string as the identity. The units
layer stores the *lexeme* and treats every surface as a pointer to it.

## 1.2 Why "matrix of ones" — and what it physically is

In Russian **«единица»** means both **"a unit"** and **"the number one"**. The layer is literally
called «Словарь-**единиц**» (dictionary of **units**), and physically it is a set of **sparse
incidence tables** — junction tables where *each stored row is a "1"* connecting two things:

- a spelling → a unit (a "1" in the spelling×word matrix),
- a unit → its translation unit (a "1" in the word×word matrix),
- a card → its unit (a "1" in the card×word matrix),
- a translation → a sense (a "1" in the translation×meaning matrix).

There is **no column literally named "matrix"**. The "matrix of ones" is this graph of `1`-rows.
And a key trick falls out of it: the **column-sum of the card→unit matrix** — i.e. `COUNT(*)` of
cards pointing at a unit — is exactly "how many people saved this word" = **demand** (section 1.6).

The whole thing is five tables, defined in `backend/lex_units_schema.sql`. Read the table headers —
they're documented in the SQL itself:

```sql
-- 1. Units: the thing you learn                         (lex_units_schema.sql:13)
CREATE TABLE bt_3_lex_units (
    id BIGSERIAL PRIMARY KEY,
    lang TEXT, kind TEXT,          -- 'de'/'ru'/… ; 'word'/'collocation'/'sentence'
    lemma TEXT, lemma_key TEXT,    -- canonical spelling + normalized lookup key
    pos TEXT, gender TEXT,         -- part of speech; 'der'/'die'/'das'
    display TEXT,                  -- how to show it: «der Rüpel», «sich freuen»
    card JSONB, ...                -- the analysis: forms, transcription, examples
);
-- identity = lemma_key + pos + gender  (gender splits the homographs)   (:34)
CREATE UNIQUE INDEX uq_lex_units_identity
    ON bt_3_lex_units (lang, kind, lemma_key, COALESCE(pos,''), COALESCE(gender,''));
```

- The **identity** of a unit is `lang + kind + lemma_key + pos + gender` (`lex_units_schema.sql:34`).
  Gender is in the key on purpose: it's what keeps `der Kiefer` and `die Kiefer` as two separate
  units while merging `Rüpel` and `der Rüpel` into one. `COALESCE(pos,'')` is used because in a SQL
  unique index `NULL` never equals `NULL`, so without it rows with an unknown POS would duplicate.
- `card JSONB` holds the full analysis (article, forms, examples) **once, on the unit** — this is
  what training reads instead of re-parsing a card's text (section 1.5).

The other four tables are the "1"s:

```sql
-- 2. Surfaces: every way to spell it → one unit                        (:39)
bt_3_lex_surfaces (surface_key, unit_id, match_kind)  -- 'exact'|'no_article'|'inflected'|'typo'
   UNIQUE (lang, surface_key, unit_id)
-- 3. Links: translation is unit↔unit, NOT text↔text                    (:55)
bt_3_lex_links (from_unit, to_unit, rank, sense_id, saves_count)  UNIQUE (from_unit, to_unit)
-- 4. Sources: which old-bank rows this unit was assembled from         (:72)
bt_3_lex_unit_sources (unit_id, entry_id, side)
-- 5. Senses: a translation hangs off a MEANING number, not the word    (:89)
bt_3_lex_senses (unit_id, sense_no, label, note)  UNIQUE (unit_id, sense_no)
```

Two of these solve the exact bugs from the summary, structurally:
- **`bt_3_lex_links` is unit→unit**, so the reverse direction works for free: `враг` is a unit, it
  has a link to `der Feind` — no separate reverse row needed (`lex_units_schema.sql:51`).
- Because a link points to *its own* target unit, **a synonym can no longer overwrite another word's
  headword** — the `«der Flegel»` / `«der Rüpel»` bug is impossible by construction
  (`lex_units_schema.sql:52`).

## 1.3 The pointer on the card

The per-user card table gets one new column — the barcode:

```sql
-- scripts/dict_units_link_personal.py:79
ALTER TABLE bt_3_webapp_dictionary_queries ADD COLUMN IF NOT EXISTS lex_unit_id BIGINT;
CREATE INDEX idx_webapp_dict_queries_lex_unit ON bt_3_webapp_dictionary_queries (lex_unit_id);
```

> Note there are now **two** link columns on a card, from two eras: the older `canonical_entry_id →
> bt_3_dictionary_entries` (the *text-pair* pool, block 04) and the newer `lex_unit_id →
> bt_3_lex_units` (the *word* layer, this block). The units layer is the "fundamental change" you're
> asking about; the pool link still exists alongside it.

## 1.4 Attaching the pointer when a word is saved

On every save, the app resolves the word to a unit and stamps the pointer. The resolver
`ensure_unit(text, lang)` (`backend/lex_units.py:299`) finds the unit by spelling or creates it:

```python
def ensure_unit(text, lang):
    key = normalize_query(text)                 # "den Rüpeln" → "rüpeln"/"rüpel" (1.7)
    # 1. is there already a unit for this surface? (prefer one that has a card)
    #    SELECT u.id FROM bt_3_lex_surfaces s JOIN bt_3_lex_units u ... WHERE surface_key = key
    if row: return int(row[0])
    # 2. else create the unit ...
    INSERT INTO bt_3_lex_units (lang, kind, lemma, lemma_key, display, card_source)
        VALUES (..., 'сохранение')
        ON CONFLICT (lang, kind, lemma_key, COALESCE(pos,''), COALESCE(gender,'')) DO UPDATE ...
        RETURNING id;                           # lex_units.py:326
    # 3. ... and register this spelling as a surface pointing at it
    INSERT INTO bt_3_lex_surfaces (lang, surface_key, unit_id, match_kind)
        VALUES (..., 'exact') ON CONFLICT DO NOTHING;   # lex_units.py:337
```

Then `attach_entry_to_unit(entry_id, …)` (`lex_units.py:351`) writes the pointer onto the card, but
only if it's still empty (so it never clobbers an existing link):

```python
UPDATE bt_3_webapp_dictionary_queries SET lex_unit_id = %s
WHERE id = %s AND lex_unit_id IS NULL;          # lex_units.py:379
```

Crucially this is wired at **two** layers so no save path slips through:
- the web save helper `_attach_saved_entry_to_lex_unit` (`backend_server.py:8581`), and
- a DB-level `_attach_entry_to_lex_unit_quietly` (`database.py:16243`) — because the **bot** writes
  cards directly, bypassing the web helpers, and "17 cards/day were left unlinked" until this was
  added.

## 1.5 Training shows the correct article — through the pointer, not the text

This is the payoff you described. In the units layer the article/forms live on the unit's `card`
JSONB and on its `gender` column, and are stamped onto the served item **from the unit**, never
re-parsed from the card's text. In `_build_item` (`lex_units.py:159`) the article comes straight
from the unit (`item["article"] = <unit gender>`, ~`lex_units.py:192`), and `_pick_unit`
(`lex_units.py:110`) chooses the right homograph by the requested article (so a request for `die
Kiefer` gets the pine tree, not the jaw). No more "search by text and hope".

## 1.6 Demand = the column-sum of the card→unit matrix (nightly top-up)

"How many cards point at this word" is computed live and drives the nightly enrichment. In
`units_needing_card` (`lex_units.py:236`), which the nightly job now uses instead of the old bank:

```sql
LEFT JOIN (
    SELECT lex_unit_id, COUNT(*) AS saved            -- ← the column-sum: cards per word = demand
    FROM bt_3_webapp_dictionary_queries
    WHERE lex_unit_id IS NOT NULL GROUP BY lex_unit_id
) p ON p.lex_unit_id = u.id
WHERE u.lang = %s AND u.kind = 'word' AND u.card IS NULL   -- units that still have no analysis
ORDER BY saved DESC, sources DESC, u.id                    -- most-wanted first
```

So the nightly enrich (`backend_server.py:9819`, `report["mode"]="units"`) spends OpenAI on the
words the **most people** actually saved, then writes the analysis onto the unit's `card` (shared by
everyone who points at it). Remaining work is counted honestly by `count_units_needing_card`
(`lex_units.py:531`). This is the "nightly top-up understands which words people need (by number of
cards per word)" you mentioned — it's literally `COUNT(*)` of the pointers.

## 1.7 Normalization: `den Rüpeln` → the unit

`normalize_query` (`lex_units.py:55`) is the lookup key maker: collapse spaces, **strip any
article** (`der/die/das/den/dem/des/einem/…` via `_ANY_ARTICLE_RE`), and `casefold()` (aggressive
lowercase). So `«der Kiefer»` and `«Kiefer»` produce the same key. Inflected forms like `den
Rüpeln`/`des Rüpels` are registered as **extra surface rows** (`match_kind='inflected'`) by the
paradigm scripts, so they all resolve to the one unit.

## 1.8 Lookup: is this word already saved, or new?

The lookup route asks the units layer **first** (behind a feature flag, see below) and only serves a
**full** unit; otherwise it falls back to the old text-pair pool:

- `_load_item_from_units_layer` (`backend_server.py:7103`) → `lex_units.lookup` (`lex_units.py:570`):
  normalize → find the surface → the unit → its links. Reverse direction (`враг → der Feind`) works
  natively; homograph siblings ("also spelled the same") are gathered too.
- `_load_dictionary_item_from_pool` (`backend_server.py:7119`) tries the units layer, then
  `get_pool_dictionary_entry` (the block-04 pool).
- **Already-saved-by-this-user** check: `get_existing_user_dictionary_entry_id_for_save`
  (`database.py:16309`) joins the user's cards to the pool to see if they already own this word.

The units-layer *serving* is gated by an env flag **`DICTIONARY_UNITS_LOOKUP_ENABLED`, default off**
(`backend_server.py:1270`). Important nuance: the **pointer-attach on save and the backfill run
regardless** of the flag — only *reading from* the layer is flagged, so the data is being built up
now and can be switched on when ready.

## 1.9 "Split into meanings" («Разбить на значения»)

A word like `anlegen` can mean *put on / build / invest*. The button splits one card into one card
per sense:
- Endpoint `POST /api/webapp/vocabulary/split-senses` → `webapp_vocabulary_split_senses`
  (`backend_server.py:48669`). It calls `lex_senses.split_translation(text)` (`lex_senses.py:35`),
  which parses a dumped string like `«1 прикладывать; 2 надевать 3 строить»` into
  `[{value,label}, …]`.
- `split_vocabulary_entry_senses` (`database.py:26582`): the **original card keeps sense #1 with its
  SRS history** (`UPDATE … SET word_ru=%s`, `database.py:26619`), and each extra sense becomes a
  **new card that copies the same `lex_unit_id`** (and `canonical_entry_id`) with
  `origin_process='sense_split'` (`database.py:26645`). So every split card still resolves through
  the same word pointer — they're the same lexeme, different meanings.
- In the layer proper, translations attach to `bt_3_lex_senses` rows via `bt_3_lex_links.sense_id`
  (`lex_units_schema.sql:89`, `:103`) — which is *why* meanings are splittable at all: a sense has
  its own number, and translations hang off it, exactly like a real dictionary (Wiktionary/Duden).

## 1.10 Backfill — attaching pointers to the old text-only cards

The pointer isn't only for new saves; an offline script attached it to the whole back catalogue:
`scripts/dict_units_link_personal.py` matched each existing card to a unit "reliable → fallback"
(first by the bank row the card grew from, else by spelling, homographs resolved by the card's own
article), bulk-`UPDATE`d `lex_unit_id`, and **self-verified the counts, rolling back on mismatch**.
Result: **24,164 of 24,559 cards got a pointer** (commit `f14778c6`). The layer itself is assembled
from the old bank by `scripts/dict_units_build.py` (which applies `lex_units_schema.sql`). The older
`canonical_entry_id` had its own backfill too (`_run_dictionary_canonical_schema_migration`,
`database.py:3790`).

# 🥷 2. Threats

The units layer is **shared, user-agnostic content** (like the block-04 pool), so it inherits that
risk class, plus a couple specific to a graph of words.

- **T1 — Poisoning a shared unit.** The `card` (analysis) and the `links` (translations) on a unit
  are shown to *everyone* who saves that word. A user who can steer what gets written onto a unit
  could push a wrong/garbage/offensive analysis to all of them.
- **T2 — Synonym/headword hijack.** The exact bug the layer fixes: making one word's analysis attach
  to another word's headword. Could a crafted save re-point a surface at the wrong unit?
- **T3 — Homograph collision.** Force `der Kiefer` and `die Kiefer` to merge into one unit so
  learners of one meaning get the other.
- **T4 — Cross-user inference via demand.** The `COUNT(*)` demand aggregates who saved what — could
  it leak *which specific user* saved a word?
- **T5 — Prompt injection into the unit `card`.** The nightly enrich sends the word to OpenAI to
  build `card`; a crafted "word" could try to inject content that then lands on the shared unit.

# 🛡️ 3. Defenses

- **T1 (poisoning) — user-agnostic data + provenance + shared-write discipline.** Units carry **no
  `user_id`** — a user only writes the *pointer on their own card*, not the unit's content; unit
  `card`/`links` are written by the enrich/build pipelines, not raw user text. `card_source` /
  `link.source` / `bt_3_lex_unit_sources` record provenance so a bad batch is traceable. (Content
  moderation on unit writes is still a recommendation — same as block 04.)
- **T2 (hijack) — structural.** A `bt_3_lex_links` row points `from_unit → to_unit` (`schema:55`);
  there is no "headword text" to overwrite. `attach_entry_to_unit` only fills a card's pointer when
  it's `NULL` (`lex_units.py:379`), so it can't repoint an existing card either.
- **T3 (homograph) — the identity key includes gender.** `uq_lex_units_identity` is
  `(lang, kind, lemma_key, pos, gender)` (`schema:34`), so `der`/`die Kiefer` *cannot* share a unit,
  and `_pick_unit` (`lex_units.py:110`) selects by requested article.
- **T4 (demand leak) — aggregation only.** `units_needing_card` returns `COUNT(*)` per unit
  (`lex_units.py:261`), a bare number; it never returns *who*. The per-user link is on the user's own
  card row, gated by the block-02 auth on every save/lookup route.
- **T5 (injection) — the block-04 containment applies.** The model output is parsed into the
  structured `card` shape, not shown as free prose; and the "word" input is short and normalized.
  The same delimiter-hardening recommendation from block 04 §4 carries over.

# 📈 4. Recommendations

1. **Content-validate unit `card`/`links` before a shared write** (as in block 04 rec 1) — poisoning
   a unit reaches everyone who saved that word.
2. **Wire `saves_count` or drop it.** `bt_3_lex_links.saves_count` is declared (`schema:61`) but has
   **no writer** in current code — demand is the live `COUNT(*)`. Either populate it (a cached demand
   signal) or remove it so it doesn't mislead a future reader.
3. **Turn on `DICTIONARY_UNITS_LOOKUP_ENABLED` behind monitoring.** Serving from the layer is still
   off by default (`backend_server.py:1270`); when enabling, watch reverse-lookup hit-rate and
   "served full unit vs fell back to pool" telemetry (`_dictionary_answer_scope`,
   `backend_server.py:7093`).
4. **Add a test that a save always attaches a pointer** (web *and* bot paths), so the "17 unlinked
   cards/day" regression can't come back silently.

# Self-check

1. Why does the unit identity key include **gender**, and what concrete bug would happen if it were
   just `lemma_key`? (Think `Kiefer`.)
2. "Demand" for the nightly top-up is a single SQL aggregate. Write it from memory and say which
   matrix's column-sum it is.
3. The old world let `«der Flegel»` inherit `«der Rüpel»`'s forms. Which table + design decision makes
   that structurally impossible now?
4. Saving a word attaches `lex_unit_id` in **two** code paths. Why two, and what breaks if the
   second (DB-level) one is removed?
5. `attach_entry_to_unit` updates `... WHERE id=%s AND lex_unit_id IS NULL`. Why the `IS NULL`
   guard — what would go wrong without it?

Last checked against the code: 2026-07-28 (line numbers verified against current source; repo edited
concurrently — grep the function name if a line is off).
