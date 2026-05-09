"""
Load FreeDict deu-rus dictionary into bt_base_dictionary.

Run once after deploy:
    python3 -m backend.load_freedict

Source: https://github.com/freedict/fd-dictionaries (deu-rus, CC-BY-SA)
~22 000 German→Russian entries.
"""
import json
import sys
import urllib.request
import xml.etree.ElementTree as ET

FREEDICT_URL = (
    "https://raw.githubusercontent.com/freedict/fd-dictionaries"
    "/master/deu-rus/deu-rus.tei"
)
TEI_NS = "http://www.tei-c.org/ns/1.0"


def _t(name: str) -> str:
    return f"{{{TEI_NS}}}{name}"


# TEI gender → German article
_GEN_TO_ARTICLE = {"m": "der", "f": "die", "n": "das", "nt": "das"}

# TEI short POS → full English name
_POS_MAP = {
    "n": "noun", "v": "verb", "adj": "adjective", "adv": "adverb",
    "prep": "preposition", "conj": "conjunction", "pron": "pronoun",
    "interj": "interjection", "num": "numeral", "art": "article",
    "abbr": "abbreviation", "phrase": "phrase",
}

_ARTICLE_PREFIXES = (
    "der ", "die ", "das ", "ein ", "eine ",
    "einem ", "einen ", "einer ", "eines ",
)


def _strip_articles(word: str) -> str:
    w = word.lower()
    for pfx in _ARTICLE_PREFIXES:
        if w.startswith(pfx):
            return word[len(pfx):]
    return word


def _normalize_key(word: str) -> str:
    import unicodedata
    w = _strip_articles(word.strip()).lower()
    return unicodedata.normalize("NFC", w)


def _download(url: str) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": "DeutschFlow/1.0"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        return resp.read()


def _parse(data: bytes) -> list[dict]:
    root = ET.fromstring(data)
    entries: list[dict] = []

    for entry in root.iter(_t("entry")):
        # ── headword ──────────────────────────────────────────────
        orth_el = entry.find(f".//{_t('orth')}")
        if orth_el is None or not orth_el.text:
            continue
        lemma = orth_el.text.strip()
        if not lemma:
            continue

        # ── POS + gender ──────────────────────────────────────────
        pos_el  = entry.find(f".//{_t('pos')}")
        gen_el  = entry.find(f".//{_t('gen')}")
        pos_raw = (pos_el.text or "").strip().lower() if pos_el is not None else ""
        gen_raw = (gen_el.text or "").strip().lower() if gen_el is not None else ""
        pos     = _POS_MAP.get(pos_raw, pos_raw)
        article = _GEN_TO_ARTICLE.get(gen_raw, "")

        # ── Russian translations ───────────────────────────────────
        translations_ru: list[str] = []
        for sense in entry.iter(_t("sense")):
            for cit in sense.iter(_t("cit")):
                cit_type = cit.get("type", "")
                if cit_type not in ("trans", "translation", ""):
                    continue
                quote = cit.find(_t("quote"))
                if quote is not None and quote.text:
                    t = quote.text.strip()
                    if t and t not in translations_ru:
                        translations_ru.append(t)

        if not translations_ru:
            continue

        senses_json = [{"gloss_en": "", "gloss_ru": ru} for ru in translations_ru[:6]]

        entries.append({
            "lemma":           lemma,
            "lemma_key":       _normalize_key(lemma),
            "source_lang":     "de",
            "pos":             pos,
            "article":         article,
            "translations_ru": translations_ru[:6],
            "glosses_en":      [],
            "senses_json":     senses_json,
            "forms_json":      {},
            "wikt_fetched":    False,
        })

    return entries


def _bulk_insert(entries: list[dict]) -> int:
    from backend.database import get_db_connection_context, ensure_webapp_tables

    ensure_webapp_tables()

    inserted = 0
    batch_size = 500

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            for i in range(0, len(entries), batch_size):
                batch = entries[i : i + batch_size]
                for e in batch:
                    cursor.execute(
                        """
                        INSERT INTO bt_base_dictionary (
                            lemma, lemma_key, source_lang, pos, article,
                            translations_ru, glosses_en, senses_json, forms_json,
                            wikt_fetched, updated_at
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s, NOW())
                        ON CONFLICT (lemma_key, source_lang) DO UPDATE SET
                            translations_ru = CASE
                                WHEN array_length(EXCLUDED.translations_ru, 1) > 0
                                     AND (array_length(bt_base_dictionary.translations_ru, 1) IS NULL
                                          OR array_length(bt_base_dictionary.translations_ru, 1) = 0)
                                THEN EXCLUDED.translations_ru
                                ELSE bt_base_dictionary.translations_ru
                            END,
                            pos     = COALESCE(NULLIF(bt_base_dictionary.pos, ''), EXCLUDED.pos),
                            article = COALESCE(NULLIF(bt_base_dictionary.article, ''), EXCLUDED.article),
                            updated_at = NOW()
                        """,
                        (
                            e["lemma"],
                            e["lemma_key"],
                            e["source_lang"],
                            e["pos"] or None,
                            e["article"] or None,
                            e["translations_ru"],
                            e["glosses_en"],
                            json.dumps(e["senses_json"], ensure_ascii=False),
                            json.dumps(e["forms_json"],  ensure_ascii=False),
                            e["wikt_fetched"],
                        ),
                    )
                    inserted += 1
                conn.commit()
                print(f"  {min(i + batch_size, len(entries))} / {len(entries)}", end="\r")

    return inserted


def main() -> None:
    print(f"Downloading FreeDict deu-rus from GitHub…")
    try:
        data = _download(FREEDICT_URL)
    except Exception as exc:
        print(f"Download failed: {exc}", file=sys.stderr)
        sys.exit(1)

    print(f"Downloaded {len(data):,} bytes. Parsing…")
    entries = _parse(data)
    print(f"Parsed {len(entries):,} entries. Inserting into PostgreSQL…")

    inserted = _bulk_insert(entries)
    print(f"\nDone. {inserted:,} entries loaded into bt_base_dictionary.")


if __name__ == "__main__":
    main()
