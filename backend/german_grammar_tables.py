"""Deterministic German grammar-table engine for the Быстрый словарь deep card.

Design decision (see memory project_quick_dict_deep_redesign): German declension
and conjugation are largely RULE-BASED, so we build the full tables here instead
of trusting an LLM to render them (fewer wrong forms, zero token cost). The LLM
only supplies the few genuinely irregular cells it is good at — the noun plural /
genitive, a verb's principal parts (Präteritum, Partizip II / Perfekt, the
irregular du-/er-Präsens forms) — which we feed in as `seed` values. Everything
else is computed.

Every public builder returns a plain dict that is safe to ship to the frontend as
JSON; absent data yields None/empty rather than raising, so a partial LLM payload
still produces a usable (if smaller) table.
"""

from __future__ import annotations

from typing import Any


# ── Definite-article declension by gender × case (singular) and plural ──────────
_ART_SG = {
    "m": {"nom": "der", "akk": "den", "dat": "dem", "gen": "des"},
    "f": {"nom": "die", "akk": "die", "dat": "der", "gen": "der"},
    "n": {"nom": "das", "akk": "das", "dat": "dem", "gen": "des"},
}
_ART_PL = {"nom": "die", "akk": "die", "dat": "den", "gen": "der"}

_CASES = ("nom", "akk", "dat", "gen")
_CASE_LABELS_RU = {
    "nom": "Nominativ",
    "akk": "Akkusativ",
    "dat": "Dativ",
    "gen": "Genitiv",
}


def gender_from_article(article: str | None) -> str | None:
    """Map a definite article to a gender key (m/f/n). Plural «die» is ambiguous
    on its own, so a bare «die» maps to feminine here; the noun builder takes an
    explicit plural form separately."""
    a = str(article or "").strip().lower()
    if a in ("der",):
        return "m"
    if a in ("die",):
        return "f"
    if a in ("das",):
        return "n"
    return None


# Any leading definite/indefinite article in any case — so a seed like
# "des Tisches" (genitive) or "dem Kind" (dative) yields the bare noun.
_ARTICLE_TOKENS = {
    "der", "die", "das", "den", "dem", "des",
    "ein", "eine", "einen", "einem", "einer", "eines",
}


def _strip_article(noun: str) -> str:
    parts = str(noun or "").strip().split()
    if parts and parts[0].lower() in _ARTICLE_TOKENS:
        return " ".join(parts[1:])
    return str(noun or "").strip()


def _genitive_singular(noun: str, gender: str, seed_gen: str | None) -> str:
    """Masculine/neuter add -(e)s in the genitive singular; feminine is unchanged.
    Prefer the LLM-provided genitive form when present (handles -ns, irregulars)."""
    seed = str(seed_gen or "").strip()
    if seed:
        return _strip_article(seed)
    if gender == "f":
        return noun
    low = noun.lower()
    # Monosyllabic / sibilant endings take -es, the rest take -s. This is a
    # pragmatic default; the LLM seed overrides it whenever it matters.
    if low.endswith(("s", "ß", "x", "z", "sch")):
        return noun + "es"
    return noun + "s"


def _dative_plural(plural: str) -> str:
    """Dative plural adds -n unless the plural already ends in -n or -s."""
    p = str(plural or "").strip()
    if not p:
        return p
    if p.lower().endswith(("n", "s")):
        return p
    return p + "n"


def build_noun_declension(
    *,
    word_de: str,
    article: str | None,
    plural: str | None = None,
    genitive: str | None = None,
) -> dict[str, Any] | None:
    """Full 4-case × singular/plural declension table for a noun, with articles.

    Returns None when the gender can't be determined (no usable article)."""
    gender = gender_from_article(article)
    if not gender:
        return None
    noun = _strip_article(word_de)
    if not noun:
        return None

    plural_noun = _strip_article(plural) if plural else ""
    gen_sg = _genitive_singular(noun, gender, genitive)

    sg_forms = {"nom": noun, "akk": noun, "dat": noun, "gen": gen_sg}
    rows = []
    for case in _CASES:
        sg_art = _ART_SG[gender][case]
        row = {
            "case": case,
            "label": _CASE_LABELS_RU[case],
            "singular": f"{sg_art} {sg_forms[case]}".strip(),
        }
        if plural_noun:
            pl_noun = _dative_plural(plural_noun) if case == "dat" else plural_noun
            row["plural"] = f"{_ART_PL[case]} {pl_noun}".strip()
        rows.append(row)

    return {
        "gender": gender,
        "article": _ART_SG[gender]["nom"],
        "singular": noun,
        "plural": plural_noun or None,
        "has_plural": bool(plural_noun),
        "rows": rows,
    }


# ── Verb conjugation ────────────────────────────────────────────────────────────
_PRON = ("ich", "du", "er/sie/es", "wir", "ihr", "sie/Sie")


def _verb_stem(infinitive: str) -> tuple[str, str]:
    """Return (stem, plural_ending). Most verbs end in -en (stem = drop -en,
    plural ending -en); -eln/-ern verbs drop only -n (plural ending -n)."""
    inf = str(infinitive or "").strip()
    if inf.endswith("eln") or inf.endswith("ern"):
        return inf[:-1], "n"
    if inf.endswith("en"):
        return inf[:-2], "en"
    if inf.endswith("n"):
        return inf[:-1], "n"
    return inf, "en"


def _present_du_er(stem: str) -> tuple[str, str]:
    """Regular 2sg/3sg present endings with the -est/-et expansion after t/d and
    the -t (not -st) shortcut after a sibilant stem."""
    low = stem.lower()
    if low.endswith(("t", "d")) or low.endswith(("ffn", "chn", "tm")):
        return stem + "est", stem + "et"
    if low.endswith(("s", "ß", "z", "x")):
        return stem + "t", stem + "t"  # du gives just -t after a sibilant
    return stem + "st", stem + "t"


def build_verb_conjugation(
    *,
    word_de: str,
    seed: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    """Conjugation tables (Präsens, Präteritum, Perfekt, Konjunktiv II, Imperativ)
    built from the infinitive plus the LLM `seed` for the irregular parts:
        present_2sg, present_3sg, praeteritum (ich/er form), perfekt
        ("hat/ist <PII>"), konjunktiv2 (ich form), imperative_sg.
    Regular cells are computed; seed cells override when present."""
    inf = _strip_article(word_de)
    if not inf or " " in inf:
        return None
    seed = seed or {}
    stem, pl_end = _verb_stem(inf)
    reg_du, reg_er = _present_du_er(stem)

    du = str(seed.get("present_2sg") or "").strip() or reg_du
    er = str(seed.get("present_3sg") or "").strip() or reg_er
    # If the LLM gave only the 3sg of a vowel-changing strong verb, mirror its
    # stem change into the du-form when we otherwise only have the regular one.
    if seed.get("present_3sg") and not seed.get("present_2sg") and er.lower().endswith("t"):
        cand = er[:-1]
        if not cand.lower().endswith(("s", "ß", "z", "x")):
            du = cand + "st"

    ihr_present = stem + ("et" if stem.lower().endswith(("t", "d")) else "t")
    praesens = dict(zip(_PRON, [
        stem + "e", du, er, stem + pl_end, ihr_present, stem + pl_end,
    ]))

    tables: dict[str, Any] = {"praesens": praesens}

    # Präteritum: expand from the ich/er base form.
    praet_base = _strip_article(str(seed.get("praeteritum") or "").strip())
    if praet_base:
        tables["praeteritum"] = _expand_from_base(praet_base, ends_in_e_default=True)

    # Konjunktiv II: the ich form already ends in -e for strong verbs (ginge),
    # weak verbs fall back to "würde + Infinitiv".
    konj2_base = _strip_article(str(seed.get("konjunktiv2") or "").strip())
    if konj2_base and konj2_base.lower().startswith("würde"):
        tables["konjunktiv2"] = dict(zip(_PRON, [
            "würde " + inf, "würdest " + inf, "würde " + inf,
            "würden " + inf, "würdet " + inf, "würden " + inf,
        ]))
    elif konj2_base:
        tables["konjunktiv2"] = _expand_from_base(konj2_base, ends_in_e_default=True)

    # Perfekt: conjugate the parsed auxiliary; keep the participle fixed.
    aux, participle = _parse_perfekt(str(seed.get("perfekt") or ""), inf)
    if participle:
        forms = _AUX_FORMS.get(aux)
        if forms:
            tables["perfekt"] = dict(zip(
                _PRON, [f"{a} {participle}" for a in forms]
            ))
            tables["auxiliary"] = aux
            tables["partizip2"] = participle

    # Imperativ (du / ihr / Sie).
    imp_du = str(seed.get("imperative_sg") or "").strip() or stem
    tables["imperativ"] = {
        "du": imp_du,
        "ihr": ihr_present,
        "Sie": f"{stem + pl_end} Sie",
    }

    tables["infinitive"] = inf
    tables["is_separable"] = seed.get("is_separable")
    return tables


def _expand_from_base(base: str, *, ends_in_e_default: bool) -> dict[str, str]:
    """Expand a single ich/er stem (Präteritum or Konjunktiv II) into all six
    persons. Strong bases ('ging') take -en/-st/-t; bases already ending in -e
    ('machte', 'ginge') take -n/-st/-t."""
    low = base.lower()
    ends_e = low.endswith("e")
    du = base + ("est" if low.endswith(("t", "d")) and not ends_e else "st")
    if low.endswith(("s", "ß", "z", "x")) and not ends_e:
        du = base + "t"
    pl = base + ("n" if ends_e else "en")
    ihr = base + ("t" if ends_e or not low.endswith(("t", "d")) else "et")
    return dict(zip(_PRON, [base, du, base, pl, ihr, pl]))


_AUX_FORMS = {
    "haben": ("habe", "hast", "hat", "haben", "habt", "haben"),
    "sein": ("bin", "bist", "ist", "sind", "seid", "sind"),
}


def _parse_perfekt(perfekt: str, infinitive: str) -> tuple[str, str]:
    """From an LLM 'hat gemacht' / 'ist gegangen' / 'haben gemacht' string return
    (auxiliary_infinitive, participle). Defaults to 'haben' when only a participle
    is present."""
    text = str(perfekt or "").strip()
    if not text:
        return "haben", ""
    tokens = text.split()
    aux = "haben"
    rest = tokens
    head = tokens[0].lower()
    if head in ("hat", "habe", "hast", "haben", "habt"):
        aux, rest = "haben", tokens[1:]
    elif head in ("ist", "bin", "bist", "sind", "seid"):
        aux, rest = "sein", tokens[1:]
    participle = " ".join(rest).strip()
    return aux, participle


# ── Adjective comparison ────────────────────────────────────────────────────────
def build_adjective_comparison(
    *,
    word_de: str,
    comparative: str | None = None,
    superlative: str | None = None,
) -> dict[str, Any] | None:
    """Three degrees of comparison. Uses LLM forms when present, else a regular
    -er / am -sten default (irregular adjectives must come from the seed)."""
    positive = _strip_article(word_de)
    if not positive or " " in positive:
        return None
    comp = str(comparative or "").strip()
    sup = str(superlative or "").strip()
    if not comp:
        comp = positive + "er"
    if not sup:
        stem = positive
        sup = f"am {stem}{'sten' if stem.lower().endswith(('t', 'd', 's', 'ß', 'z', 'x')) else 'sten'}"
    elif not sup.lower().startswith("am "):
        sup = "am " + sup
    return {"positive": positive, "comparative": comp, "superlative": sup}


# ── Word formation (compound / affix breakdown) ─────────────────────────────────
def build_grammar_tables(item: dict[str, Any] | None) -> dict[str, Any]:
    """Top-level dispatcher: from a looked-up dictionary `item` (the existing
    /api/webapp/dictionary payload) produce the deep, POS-aware grammar tables.
    Only the section matching the part of speech is populated."""
    if not item or not isinstance(item, dict):
        return {}
    pos = str(item.get("part_of_speech") or "").strip().lower()
    forms = item.get("forms") if isinstance(item.get("forms"), dict) else {}
    word_de = str(item.get("word_de") or "").strip()
    out: dict[str, Any] = {"part_of_speech": pos}

    if pos == "noun":
        table = build_noun_declension(
            word_de=word_de,
            article=item.get("article"),
            plural=forms.get("plural"),
            genitive=forms.get("genitive"),
        )
        if table:
            out["declension"] = table
    elif pos == "verb":
        table = build_verb_conjugation(
            word_de=word_de,
            seed={
                "present_2sg": forms.get("present_2sg"),
                "present_3sg": forms.get("present_3sg"),
                "praeteritum": forms.get("praeteritum"),
                "perfekt": forms.get("perfekt"),
                "konjunktiv2": forms.get("konjunktiv2"),
                "imperative_sg": forms.get("imperative_sg"),
                "is_separable": item.get("is_separable"),
            },
        )
        if table:
            out["conjugation"] = table
    elif pos in ("adjective", "adverb"):
        table = build_adjective_comparison(
            word_de=word_de,
            comparative=forms.get("comparative"),
            superlative=forms.get("superlative"),
        )
        if table:
            out["comparison"] = table

    return out
