"""Deterministic generator for German adjective-ending items.

Adjective endings are a CLOSED, fully rule-based system: the ending depends only
on declension type (weak after der/die/das, mixed after ein/kein/mein, strong
with no determiner) × case (Nom/Akk/Dat) × gender (m/f/n). So we can build
UNLIMITED, 100%-correct items with no LLM and zero "bред":
  noun (+gender) from the Artikel noun bank  ×  a regular adjective  ×  a random
  declension type/case  →  phrase + correct ending + a Russian rule + tip.

We stay in the SINGULAR Nom/Akk/Dat (the noun keeps its dictionary form), which
already exercises every ending (-e/-en/-er/-es/-em). Akk/Dat get a governing
preposition so the case is natural and unambiguous.
"""
from __future__ import annotations

import random

# Regular adjectives only — consonant/vowel stems that just take the ending.
# Deliberately EXCLUDES -el/-er/-e stems (dunkel→dunkles, teuer→teures,
# leise→leiser) and irregular "hoch" → no wrong forms can be produced.
ADJECTIVES = [
    "gut", "neu", "alt", "jung", "klein", "groß", "lang", "kurz", "breit", "schmal",
    "tief", "dick", "dünn", "schwer", "leicht", "schnell", "langsam", "laut", "warm",
    "kalt", "kühl", "heiß", "frisch", "schmutzig", "nass", "trocken", "hart", "weich",
    "glatt", "scharf", "stark", "schwach", "gesund", "krank", "wach", "reich", "arm",
    "billig", "günstig", "wichtig", "nützlich", "interessant", "langweilig", "schön",
    "hässlich", "freundlich", "höflich", "ehrlich", "klug", "dumm", "fleißig", "faul",
    "mutig", "ruhig", "glücklich", "traurig", "lustig", "ernst", "modern", "bekannt",
    "berühmt", "fremd", "ganz", "voll", "leer", "offen", "frei", "möglich", "nötig",
    "richtig", "falsch", "einfach", "schwierig", "sicher", "gefährlich", "bequem",
    "praktisch", "natürlich", "echt", "typisch", "normal", "grün", "blau", "rot",
    "gelb", "schwarz", "weiß", "grau", "braun",
]

# Fallback nouns (word, gender) if the Artikel noun bank is empty.
_FALLBACK_NOUNS = [
    ("Wein", "m"), ("Mann", "m"), ("Tisch", "m"), ("Wagen", "m"), ("Berg", "m"),
    ("Hund", "m"), ("Stuhl", "m"), ("Markt", "m"), ("Film", "m"), ("Brief", "m"),
    ("Frau", "f"), ("Stadt", "f"), ("Idee", "f"), ("Reise", "f"), ("Lampe", "f"),
    ("Tür", "f"), ("Straße", "f"), ("Sprache", "f"), ("Lösung", "f"), ("Frage", "f"),
    ("Buch", "n"), ("Auto", "n"), ("Haus", "n"), ("Bild", "n"), ("Fenster", "n"),
    ("Zimmer", "n"), ("Problem", "n"), ("Spiel", "n"), ("Wasser", "n"), ("Geschenk", "n"),
]

_ART_GENDER = {"der": "m", "die": "f", "das": "n"}

# Ending tables: [case][gender], gender ∈ m/f/n. Singular only.
_WEAK = {"Nom": {"m": "e", "f": "e", "n": "e"},
         "Akk": {"m": "en", "f": "e", "n": "e"},
         "Dat": {"m": "en", "f": "en", "n": "en"}}
_MIXED = {"Nom": {"m": "er", "f": "e", "n": "es"},
          "Akk": {"m": "en", "f": "e", "n": "es"},
          "Dat": {"m": "en", "f": "en", "n": "en"}}
_STRONG = {"Nom": {"m": "er", "f": "e", "n": "es"},
           "Akk": {"m": "en", "f": "e", "n": "es"},
           "Dat": {"m": "em", "f": "er", "n": "em"}}
_TABLES = {"weak": _WEAK, "mixed": _MIXED, "strong": _STRONG}

# Determiner forms by [case][gender].
_DEF = {"Nom": {"m": "der", "f": "die", "n": "das"},
        "Akk": {"m": "den", "f": "die", "n": "das"},
        "Dat": {"m": "dem", "f": "der", "n": "dem"}}
_EIN = {"Nom": {"m": "ein", "f": "eine", "n": "ein"},
        "Akk": {"m": "einen", "f": "eine", "n": "ein"},
        "Dat": {"m": "einem", "f": "einer", "n": "einem"}}

_AKK_PREPS = ["für", "ohne", "gegen", "durch", "um"]
_DAT_PREPS = ["mit", "bei", "nach", "aus", "von", "zu"]

_CASE_RU = {"Nom": "Именительный", "Akk": "Винительный", "Dat": "Дательный"}
_GEN_RU = {"m": "муж. род", "f": "жен. род", "n": "ср. род"}
_TYPE_RU = {"weak": "слабое склонение (после der/die/das)",
            "mixed": "смешанное склонение (после ein/kein)",
            "strong": "сильное склонение (без артикля)"}
_TYPE_SHORT = {"weak": "слабое", "mixed": "смешанное", "strong": "сильное"}


def _load_nouns(limit: int = 400) -> list[tuple]:
    try:
        from backend.database import get_db_connection_context
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT word, article FROM bt_3_article_sprint_nouns "
                    "WHERE retired=FALSE AND verified=TRUE AND article IN ('der','die','das') "
                    "ORDER BY random() LIMIT %s;",
                    (int(limit),),
                )
                rows = [(str(r[0]).strip(), _ART_GENDER.get(str(r[1]), "n"))
                        for r in (cur.fetchall() or []) if r and r[0]]
        if rows:
            return rows
    except Exception:
        pass
    return list(_FALLBACK_NOUNS)


# The strong adjective ending "signals" the gender/case like the definite article
# would: -er↔der, -es↔das, -em↔dem, -e↔die, -en↔den. This is the core "gut feeling".
_REF_ART = {"er": "der", "es": "das", "em": "dem", "e": "die", "en": "den"}
# Canonical example words per gender (for the deterministic "пример" line).
_EX_NOUNS = {"m": ["Wein", "Tag"], "f": ["Suppe", "Idee"], "n": ["Auto", "Kind"]}
_EX_ADJS = ["neu", "klein"]


def _rule(typ: str, case: str, gender: str, ending: str) -> str:
    base = {"weak": "После der/die/das", "mixed": "После ein/kein", "strong": "Без артикля"}[typ]
    return f"{base}, {_GEN_RU[gender]}, {_CASE_RU[case]} → окончание -{ending}."


def _feeling(typ: str, case: str, gender: str, ending: str) -> str:
    ref = _REF_ART.get(ending, "der")
    if typ == "weak":
        return f"der/die/das уже показал род и падеж → прилагательному легко: -{ending}."
    if typ == "mixed":
        strong_slot = (case == "Nom" and gender in ("m", "n")) or (case == "Akk" and gender == "n")
        if strong_slot:
            return f"ein «молчит» о роде → прилагательное досказывает сигнал -{ending} (как {ref})."
        return f"ein/kein здесь уже показал род/падеж → лёгкое -{ending}."
    return f"Без артикля прилагательное «надевает шляпу артикля» → копирует -{ending} (как {ref})."


def _example(typ: str, case: str, gender: str, ending: str) -> str:
    det = _DEF[case][gender] if typ == "weak" else (_EIN[case][gender] if typ == "mixed" else "")
    out = []
    for adj, noun in zip(_EX_ADJS, _EX_NOUNS[gender]):
        out.append(" ".join([p for p in (det, adj + ending, noun) if p]))
    return ", ".join(out)


def _build_one(noun: tuple, adjective: str) -> dict:
    word, gender = noun
    typ = random.choice(["weak", "mixed", "strong"])
    case = random.choice(["Nom", "Akk", "Dat"])
    ending = _TABLES[typ][case][gender]
    if typ == "weak":
        det = _DEF[case][gender]
    elif typ == "mixed":
        det = _EIN[case][gender]
    else:
        det = ""
    prep = ""
    if case == "Akk":
        prep = random.choice(_AKK_PREPS)
    elif case == "Dat":
        prep = random.choice(_DAT_PREPS)
    pre = [p for p in (prep, det) if p]
    full = " ".join(pre + [adjective + ending, word])
    before = " ".join(pre + [adjective]).strip()
    after = " " + word
    hint_ru = f"{_CASE_RU[case]}, {_GEN_RU[gender]}, {_TYPE_SHORT[typ]}"
    return {
        "before": before, "after": after, "correct": ending, "full": full,
        "erklaerung": _rule(typ, case, gender, ending),
        "tip": _feeling(typ, case, gender, ending),
        "example": _example(typ, case, gender, ending),
        "hint_ru": hint_ru,
    }


def build_adjektiv_items(n: int = 15) -> list[dict]:
    """`n` rule-perfect, de-duplicated adjective-ending items."""
    nouns = _load_nouns()
    if not nouns:
        return []
    out: list[dict] = []
    seen: set[str] = set()
    attempts = 0
    while len(out) < int(n) and attempts < int(n) * 8:
        attempts += 1
        item = _build_one(random.choice(nouns), random.choice(ADJECTIVES))
        key = item["full"].lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(item)
    return out
