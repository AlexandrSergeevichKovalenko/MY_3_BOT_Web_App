"""
Load WikDict deu-rus TEI dictionary into bt_wiktionary_dictionary.

Source: https://download.wikdict.com/dictionaries/tei/recommended/deu-rus.tei
~26,800 German→Russian entries (Wiktionary-derived, CC-BY-SA).
Skips words already present in bt_base_dictionary (FreeDict).

Usage (one-time, run manually or via admin endpoint):
    python3 -m backend.load_wiktionary
"""

import sys
import urllib.request
import xml.etree.ElementTree as ET

WIKDICT_URL = "https://download.wikdict.com/dictionaries/tei/recommended/deu-rus.tei"
TEI_NS = "http://www.tei-c.org/ns/1.0"

_GENDER_TO_ARTICLE = {"masc": "der", "fem": "die", "neut": "das"}
_POS_MAP = {"n": "noun", "pn": "noun", "v": "verb", "adj": "adj", "adv": "adv"}


def _download(url: str) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": "DeutschBot/1.0"})
    with urllib.request.urlopen(req, timeout=120) as resp:
        return resp.read()


def _parse(data: bytes) -> list[dict]:
    root = ET.fromstring(data)
    ns = TEI_NS
    entries = []

    for entry in root.iter(f"{{{ns}}}entry"):
        # lemma
        form_el = entry.find(f"{{{ns}}}form/{{{ns}}}orth")
        if form_el is None:
            continue
        lemma = (form_el.text or "").strip()
        if not lemma:
            continue

        # POS
        gram = entry.find(f".//{{{ns}}}gram[@type='pos']")
        pos_raw = (gram.text or "").strip() if gram is not None else ""
        pos = _POS_MAP.get(pos_raw, pos_raw or None)

        # article from grammatical gender
        gen_el = entry.find(f".//{{{ns}}}gen")
        gender = (gen_el.text or "").strip() if gen_el is not None else ""
        article = _GENDER_TO_ARTICLE.get(gender)

        # translations
        translations_ru = []
        senses = []
        for sense in entry.findall(f".//{{{ns}}}sense"):
            cit = sense.find(f"{{{ns}}}cit[@type='trans']/{{{ns}}}quote")
            if cit is not None and cit.text:
                translation = cit.text.strip()
                if translation:
                    translations_ru.append(translation)
                    senses.append({"gloss": translation})

        if not translations_ru:
            continue

        entries.append({
            "lemma": lemma,
            "pos": pos,
            "article": article,
            "translations_ru": translations_ru[:6],
            "glosses_en": [],
            "senses": senses[:6],
        })

    return entries


def main() -> None:
    from backend.database import bulk_insert_wiktionary_entries, upsert_wiktionary_seed_state

    print(f"Downloading WikDict deu-rus from {WIKDICT_URL} …")
    try:
        data = _download(WIKDICT_URL)
    except Exception as exc:
        print(f"Download failed: {exc}", file=sys.stderr)
        sys.exit(1)

    print(f"Downloaded {len(data):,} bytes. Parsing TEI …")
    entries = _parse(data)
    print(f"Parsed {len(entries):,} entries with Russian translations. Inserting (skipping FreeDict duplicates) …")

    upsert_wiktionary_seed_state(source_lang="de", seed_complete=False,
                                  entry_count=0, source_url=WIKDICT_URL)
    inserted = bulk_insert_wiktionary_entries(entries, source_lang="de")
    upsert_wiktionary_seed_state(source_lang="de", seed_complete=True,
                                  entry_count=inserted, source_url=WIKDICT_URL)
    print(f"Done. Inserted {inserted:,} new entries into bt_wiktionary_dictionary.")


if __name__ == "__main__":
    main()
