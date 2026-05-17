"""Canonical taxonomy for structured voice session grammar mistakes.

This is the single source of truth for error_category and error_subtype values
stored in bt_3_voice_session_mistakes.

INVARIANTS guaranteed by this module:
- VALID_ERROR_CATEGORIES is the closed set of all permitted category codes.
- VALID_ERROR_SUBTYPES is the closed set of all permitted subtype codes.
- VALID_SUBTYPES_BY_CATEGORY maps every category to its permitted subtypes.
  Every subtype belongs to exactly one category.
- Every category has an explicit OTHER_* catch-all subtype.
- validate_category() and validate_subtype() raise ValueError on unknown values;
  they never silently coerce or fall back.

Design notes (spoken-only scope):
- This taxonomy covers SPOKEN German only. It does NOT include Punctuation,
  Orthography & Spelling, or written-text-specific categories.
- Category codes are UPPER_CASE strings to make accidental raw-string usage
  visible at code review time.
- Subtypes follow the pattern <DESCRIPTION>_<CATEGORY_ABBREV> or are named
  after the specific rule they violate.
- PRONUNCIATION_STT is reserved for utterances where the STT transcript is
  uncertain; grammar analysis should be marked low-confidence there.
"""

from __future__ import annotations

from typing import FrozenSet


# ── Error Categories ──────────────────────────────────────────────────────────
# Keep alphabetical within each group for diff-readability.


class ErrorCategory:
    """Closed set of error category codes."""

    # Core morphosyntax
    ADJECTIVE_ENDINGS       = "ADJECTIVE_ENDINGS"
    ARTICLES                = "ARTICLES"
    CASES                   = "CASES"
    CONJUNCTIONS            = "CONJUNCTIONS"
    INFINITIVE_CLAUSES      = "INFINITIVE_CLAUSES"
    KONJUNKTIV              = "KONJUNKTIV"
    MODAL_VERBS             = "MODAL_VERBS"
    NEGATION                = "NEGATION"
    NOUN_GENDER             = "NOUN_GENDER"
    PASSIVE                 = "PASSIVE"
    PLURAL_FORM             = "PLURAL_FORM"
    PREPOSITIONS            = "PREPOSITIONS"
    REFLEXIVE_VERBS         = "REFLEXIVE_VERBS"
    RELATIVE_CLAUSES        = "RELATIVE_CLAUSES"
    SEPARABLE_VERBS         = "SEPARABLE_VERBS"
    TENSES                  = "TENSES"
    VERB_FORM               = "VERB_FORM"
    WORD_ORDER              = "WORD_ORDER"

    # Lexical / pragmatic
    LEXIS                   = "LEXIS"

    # Transcription quality flag
    PRONUNCIATION_STT       = "PRONUNCIATION_STT"


VALID_ERROR_CATEGORIES: FrozenSet[str] = frozenset(
    v for k, v in vars(ErrorCategory).items()
    if not k.startswith("_") and isinstance(v, str)
)


# ── Error Subtypes ────────────────────────────────────────────────────────────


class ErrorSubtype:
    """Closed set of error subtype codes, grouped by parent category."""

    # WORD_ORDER
    # Wortstellung im Hauptsatz und Nebensatz.
    WORD_ORDER_V2_MAIN_CLAUSE          = "WORD_ORDER_V2_MAIN_CLAUSE"
    # Verb nicht auf Position 2 im Hauptsatz: "Gestern ich gegangen bin."
    WORD_ORDER_VERB_FINAL_SUBORDINATE  = "WORD_ORDER_VERB_FINAL_SUBORDINATE"
    # Verb nicht am Satzende im Nebensatz: "weil ich bin müde"
    WORD_ORDER_INVERSION_MISSING       = "WORD_ORDER_INVERSION_MISSING"
    # Keine Inversion nach vorangestelltem Adverb/Element: "Gestern ich..."
    WORD_ORDER_AUXILIARY_POSITION      = "WORD_ORDER_AUXILIARY_POSITION"
    # Hilfsverb (haben/sein) falsch positioniert im Perfekt-Nebensatz
    WORD_ORDER_MODAL_INFINITIVE        = "WORD_ORDER_MODAL_INFINITIVE"
    # Infinitiv mit Modalverb falsch positioniert
    WORD_ORDER_OTHER                   = "WORD_ORDER_OTHER"

    # VERB_FORM
    VERB_FORM_IRREGULAR_STEM           = "VERB_FORM_IRREGULAR_STEM"
    # Falsche Vokalveränderung starker Verben: laufen → "er lauft" statt "er läuft"
    VERB_FORM_PAST_PARTICIPLE          = "VERB_FORM_PAST_PARTICIPLE"
    # Falsches Partizip II: "gegebt" statt "gegeben"
    VERB_FORM_CONJUGATION_AGREEMENT    = "VERB_FORM_CONJUGATION_AGREEMENT"
    # Falsche Person/Numerus-Kongruenz: "du gehst" → "du gehe"
    VERB_FORM_INFINITIVE_AS_FINITE     = "VERB_FORM_INFINITIVE_AS_FINITE"
    # Infinitiv statt konjugierter Form: "Ich gehen heute."
    VERB_FORM_AUXILIARY_SELECTION      = "VERB_FORM_AUXILIARY_SELECTION"
    # haben vs. sein im Perfekt falsch gewählt
    VERB_FORM_OTHER                    = "VERB_FORM_OTHER"

    # TENSES
    TENSES_PERFEKT_PRAETERITUM         = "TENSES_PERFEKT_PRAETERITUM"
    # Falsche Wahl zwischen Perfekt und Präteritum
    TENSES_PRAESENS_FOR_PAST           = "TENSES_PRAESENS_FOR_PAST"
    # Präsens verwendet wo Vergangenheitsform nötig wäre
    TENSES_FUTURE_CONSTRUCTION         = "TENSES_FUTURE_CONSTRUCTION"
    # Falsche Futurform (werden+Inf vs. Präsens)
    TENSES_SEQUENCE_INCONSISTENCY      = "TENSES_SEQUENCE_INCONSISTENCY"
    # Inkonsistente Tempusfolge im selben Kontext
    TENSES_PLUSQUAMPERFEKT             = "TENSES_PLUSQUAMPERFEKT"
    # Plusquamperfekt falsch gebildet oder angewendet
    TENSES_OTHER                       = "TENSES_OTHER"

    # CASES
    CASES_NOM_FOR_ACC                  = "CASES_NOM_FOR_ACC"
    CASES_ACC_FOR_NOM                  = "CASES_ACC_FOR_NOM"
    CASES_ACC_FOR_DAT                  = "CASES_ACC_FOR_DAT"
    CASES_DAT_FOR_ACC                  = "CASES_DAT_FOR_ACC"
    CASES_GENITIVE_ERROR               = "CASES_GENITIVE_ERROR"
    CASES_AFTER_PREPOSITION            = "CASES_AFTER_PREPOSITION"
    # Falscher Kasus nach einer bestimmten Präposition
    CASES_OTHER                        = "CASES_OTHER"

    # ARTICLES
    ARTICLES_WRONG_GENDER              = "ARTICLES_WRONG_GENDER"
    # Falsches Genus: "der Tisch" → "die Tisch"
    ARTICLES_WRONG_CASE_FORM           = "ARTICLES_WRONG_CASE_FORM"
    # Falsche Kasusform des Artikels: dem/den/des
    ARTICLES_MISSING                   = "ARTICLES_MISSING"
    # Artikel weggelassen wo er obligatorisch ist
    ARTICLES_INDEFINITE_FOR_DEFINITE   = "ARTICLES_INDEFINITE_FOR_DEFINITE"
    ARTICLES_DEFINITE_FOR_INDEFINITE   = "ARTICLES_DEFINITE_FOR_INDEFINITE"
    ARTICLES_KEIN_NICHT_CONFUSION      = "ARTICLES_KEIN_NICHT_CONFUSION"
    # kein vs. nicht als Negationsartikel verwechselt
    ARTICLES_OTHER                     = "ARTICLES_OTHER"

    # ADJECTIVE_ENDINGS
    ADJECTIVE_ENDINGS_WEAK_WRONG       = "ADJECTIVE_ENDINGS_WEAK_WRONG"
    # Schwache Deklination (nach best. Artikel) falsch: "das großE" statt "das großE" — z.B. Akkusativ
    ADJECTIVE_ENDINGS_STRONG_WRONG     = "ADJECTIVE_ENDINGS_STRONG_WRONG"
    # Starke Deklination (ohne Artikel) falsch
    ADJECTIVE_ENDINGS_MIXED_WRONG      = "ADJECTIVE_ENDINGS_MIXED_WRONG"
    # Gemischte Deklination (nach unbestimmtem Artikel) falsch
    ADJECTIVE_ENDINGS_CASE_AGREEMENT   = "ADJECTIVE_ENDINGS_CASE_AGREEMENT"
    # Adjektiv stimmt nicht mit Kasus des Nomens überein
    ADJECTIVE_ENDINGS_GENDER_AGREEMENT = "ADJECTIVE_ENDINGS_GENDER_AGREEMENT"
    # Adjektiv stimmt nicht mit Genus überein
    ADJECTIVE_ENDINGS_OTHER            = "ADJECTIVE_ENDINGS_OTHER"

    # PREPOSITIONS
    PREPOSITIONS_WRONG_FIXED           = "PREPOSITIONS_WRONG_FIXED"
    # Falsche Präposition bei fester Rektion: "warten auf" → "warten für"
    PREPOSITIONS_TWO_WAY_CASE          = "PREPOSITIONS_TWO_WAY_CASE"
    # Wechselpräposition mit falschem Kasus (Dativ/Akkusativ)
    PREPOSITIONS_MISSING               = "PREPOSITIONS_MISSING"
    PREPOSITIONS_EXTRA                 = "PREPOSITIONS_EXTRA"
    # Präposition hinzugefügt wo keine benötigt
    PREPOSITIONS_OTHER                 = "PREPOSITIONS_OTHER"

    # MODAL_VERBS
    MODAL_VERBS_CONJUGATION            = "MODAL_VERBS_CONJUGATION"
    # Falsche Konjugation des Modalverbs
    MODAL_VERBS_INFINITIVE_WITH_ZU     = "MODAL_VERBS_INFINITIVE_WITH_ZU"
    # Modalverb mit zu+Infinitiv statt reinem Infinitiv
    MODAL_VERBS_WRONG_CHOICE           = "MODAL_VERBS_WRONG_CHOICE"
    # Semantisch falsches Modalverb: müssen vs. sollen
    MODAL_VERBS_INFINITIVE_POSITION    = "MODAL_VERBS_INFINITIVE_POSITION"
    # Infinitiv nicht am Satzende
    MODAL_VERBS_OTHER                  = "MODAL_VERBS_OTHER"

    # SEPARABLE_VERBS
    SEPARABLE_VERBS_PREFIX_NOT_SPLIT   = "SEPARABLE_VERBS_PREFIX_NOT_SPLIT"
    # Präfix nicht abgetrennt: "Ich anrufe dich." statt "Ich rufe dich an."
    SEPARABLE_VERBS_PREFIX_WRONG_POS   = "SEPARABLE_VERBS_PREFIX_WRONG_POS"
    # Präfix abgetrennt aber falsch positioniert
    SEPARABLE_VERBS_FORM_WRONG         = "SEPARABLE_VERBS_FORM_WRONG"
    # Falsche konjugierte Form des trennbaren Verbs
    SEPARABLE_VERBS_OTHER              = "SEPARABLE_VERBS_OTHER"

    # REFLEXIVE_VERBS
    REFLEXIVE_VERBS_PRONOUN_MISSING    = "REFLEXIVE_VERBS_PRONOUN_MISSING"
    # Reflexivpronomen fehlt
    REFLEXIVE_VERBS_PRONOUN_CASE       = "REFLEXIVE_VERBS_PRONOUN_CASE"
    # Falscher Kasus: mich vs. mir
    REFLEXIVE_VERBS_NOT_REFLEXIVE      = "REFLEXIVE_VERBS_NOT_REFLEXIVE"
    # Verb reflexiv verwendet obwohl es das nicht ist
    REFLEXIVE_VERBS_OTHER              = "REFLEXIVE_VERBS_OTHER"

    # KONJUNKTIV
    KONJUNKTIV_FORM_WRONG              = "KONJUNKTIV_FORM_WRONG"
    # Falsche Konjunktiv-II-Form
    KONJUNKTIV_WUERDE_WRONG            = "KONJUNKTIV_WUERDE_WRONG"
    # Falsche würde+Infinitiv-Konstruktion
    KONJUNKTIV_INDICATIVE_USED         = "KONJUNKTIV_INDICATIVE_USED"
    # Indikativ statt Konjunktiv II bei Hypothese/Höflichkeit
    KONJUNKTIV_KONJUNKTIV1_ERROR       = "KONJUNKTIV_KONJUNKTIV1_ERROR"
    # Konjunktiv I (indirekte Rede) falsch gebildet oder angewendet
    KONJUNKTIV_OTHER                   = "KONJUNKTIV_OTHER"

    # PASSIVE
    PASSIVE_WERDEN_FORM                = "PASSIVE_WERDEN_FORM"
    # Falsche werden-Konjugation im Passiv
    PASSIVE_PARTIZIP_WRONG             = "PASSIVE_PARTIZIP_WRONG"
    # Falsches Partizip II im Passiv
    PASSIVE_ZUSTAND_VORGANG_CONFUSION  = "PASSIVE_ZUSTAND_VORGANG_CONFUSION"
    # Zustandspassiv (sein) vs. Vorgangspassiv (werden) verwechselt
    PASSIVE_WORD_ORDER_WRONG           = "PASSIVE_WORD_ORDER_WRONG"
    PASSIVE_OTHER                      = "PASSIVE_OTHER"

    # CONJUNCTIONS
    CONJUNCTIONS_SUBORDINATING_WRONG   = "CONJUNCTIONS_SUBORDINATING_WRONG"
    # Falsche subordinierende Konjunktion: weil/denn/da verwechselt
    CONJUNCTIONS_WEIL_DENN             = "CONJUNCTIONS_WEIL_DENN"
    # weil (SOV) vs. denn (SVO) Wortstellungsunterschied ignoriert
    CONJUNCTIONS_COORDINATING_WRONG    = "CONJUNCTIONS_COORDINATING_WRONG"
    CONJUNCTIONS_MISSING               = "CONJUNCTIONS_MISSING"
    CONJUNCTIONS_OTHER                 = "CONJUNCTIONS_OTHER"

    # NOUN_GENDER
    NOUN_GENDER_WRONG                  = "NOUN_GENDER_WRONG"
    # Grammatikalisches Geschlecht des Nomens falsch
    NOUN_GENDER_OTHER                  = "NOUN_GENDER_OTHER"

    # PLURAL_FORM
    PLURAL_FORM_WRONG_SUFFIX           = "PLURAL_FORM_WRONG_SUFFIX"
    # Falsche Pluralendung oder Umlaut
    PLURAL_FORM_SINGULAR_USED          = "PLURAL_FORM_SINGULAR_USED"
    # Singular statt Plural
    PLURAL_FORM_PLURAL_USED            = "PLURAL_FORM_PLURAL_USED"
    # Plural statt Singular
    PLURAL_FORM_OTHER                  = "PLURAL_FORM_OTHER"

    # RELATIVE_CLAUSES
    RELATIVE_CLAUSES_PRONOUN_GENDER    = "RELATIVE_CLAUSES_PRONOUN_GENDER"
    # Falsches Genus des Relativpronomens
    RELATIVE_CLAUSES_PRONOUN_CASE      = "RELATIVE_CLAUSES_PRONOUN_CASE"
    # Falscher Kasus des Relativpronomens
    RELATIVE_CLAUSES_VERB_POSITION     = "RELATIVE_CLAUSES_VERB_POSITION"
    # Verb nicht am Ende des Relativsatzes
    RELATIVE_CLAUSES_OTHER             = "RELATIVE_CLAUSES_OTHER"

    # INFINITIVE_CLAUSES
    INFINITIVE_CLAUSES_ZU_MISSING      = "INFINITIVE_CLAUSES_ZU_MISSING"
    # zu fehlt wo zu+Infinitiv erforderlich
    INFINITIVE_CLAUSES_ZU_EXTRA        = "INFINITIVE_CLAUSES_ZU_EXTRA"
    # zu verwendet wo reiner Infinitiv nötig
    INFINITIVE_CLAUSES_UM_ZU_WRONG     = "INFINITIVE_CLAUSES_UM_ZU_WRONG"
    # Falsche um...zu-Konstruktion
    INFINITIVE_CLAUSES_FORM_WRONG      = "INFINITIVE_CLAUSES_FORM_WRONG"
    # Falsche Infinitivform
    INFINITIVE_CLAUSES_OTHER           = "INFINITIVE_CLAUSES_OTHER"

    # NEGATION
    NEGATION_NICHT_KEIN_CONFUSION      = "NEGATION_NICHT_KEIN_CONFUSION"
    # nicht vs. kein Verwechslung
    NEGATION_POSITION_WRONG            = "NEGATION_POSITION_WRONG"
    # nicht an falscher Stelle im Satz
    NEGATION_OTHER                     = "NEGATION_OTHER"

    # LEXIS
    LEXIS_WRONG_WORD_CHOICE            = "LEXIS_WRONG_WORD_CHOICE"
    # Semantisch falsches Wort
    LEXIS_FALSE_FRIEND                 = "LEXIS_FALSE_FRIEND"
    # L1-Interferenz (Russisch/Englisch): "bekommen" statt "erhalten"
    LEXIS_CALQUE_FROM_L1               = "LEXIS_CALQUE_FROM_L1"
    # Direkte Übersetzung aus L1: unnatürliche Struktur
    LEXIS_UNNATURAL_PHRASING           = "LEXIS_UNNATURAL_PHRASING"
    # Grammatikalisch korrekt, aber für native speaker unnatürlich
    LEXIS_REGISTER_MISMATCH            = "LEXIS_REGISTER_MISMATCH"
    # Falsches Register (zu formell/umgangssprachlich für den Kontext)
    LEXIS_OTHER                        = "LEXIS_OTHER"

    # PRONUNCIATION_STT
    PRONUNCIATION_STT_LIKELY_MISREAD   = "PRONUNCIATION_STT_LIKELY_MISREAD"
    # STT-Fehler wahrscheinlich: Transkript klingt phonetisch unwahrscheinlich
    PRONUNCIATION_STT_UNCLEAR          = "PRONUNCIATION_STT_UNCLEAR"
    # Äußerung zu unklar zum Parsen; Grammatikanalyse unmöglich
    PRONUNCIATION_STT_OTHER            = "PRONUNCIATION_STT_OTHER"


VALID_ERROR_SUBTYPES: FrozenSet[str] = frozenset(
    v for k, v in vars(ErrorSubtype).items()
    if not k.startswith("_") and isinstance(v, str)
)


# ── Category → Subtypes mapping (source of truth for pairing validation) ─────

VALID_SUBTYPES_BY_CATEGORY: dict[str, FrozenSet[str]] = {
    ErrorCategory.WORD_ORDER: frozenset({
        ErrorSubtype.WORD_ORDER_V2_MAIN_CLAUSE,
        ErrorSubtype.WORD_ORDER_VERB_FINAL_SUBORDINATE,
        ErrorSubtype.WORD_ORDER_INVERSION_MISSING,
        ErrorSubtype.WORD_ORDER_AUXILIARY_POSITION,
        ErrorSubtype.WORD_ORDER_MODAL_INFINITIVE,
        ErrorSubtype.WORD_ORDER_OTHER,
    }),
    ErrorCategory.VERB_FORM: frozenset({
        ErrorSubtype.VERB_FORM_IRREGULAR_STEM,
        ErrorSubtype.VERB_FORM_PAST_PARTICIPLE,
        ErrorSubtype.VERB_FORM_CONJUGATION_AGREEMENT,
        ErrorSubtype.VERB_FORM_INFINITIVE_AS_FINITE,
        ErrorSubtype.VERB_FORM_AUXILIARY_SELECTION,
        ErrorSubtype.VERB_FORM_OTHER,
    }),
    ErrorCategory.TENSES: frozenset({
        ErrorSubtype.TENSES_PERFEKT_PRAETERITUM,
        ErrorSubtype.TENSES_PRAESENS_FOR_PAST,
        ErrorSubtype.TENSES_FUTURE_CONSTRUCTION,
        ErrorSubtype.TENSES_SEQUENCE_INCONSISTENCY,
        ErrorSubtype.TENSES_PLUSQUAMPERFEKT,
        ErrorSubtype.TENSES_OTHER,
    }),
    ErrorCategory.CASES: frozenset({
        ErrorSubtype.CASES_NOM_FOR_ACC,
        ErrorSubtype.CASES_ACC_FOR_NOM,
        ErrorSubtype.CASES_ACC_FOR_DAT,
        ErrorSubtype.CASES_DAT_FOR_ACC,
        ErrorSubtype.CASES_GENITIVE_ERROR,
        ErrorSubtype.CASES_AFTER_PREPOSITION,
        ErrorSubtype.CASES_OTHER,
    }),
    ErrorCategory.ARTICLES: frozenset({
        ErrorSubtype.ARTICLES_WRONG_GENDER,
        ErrorSubtype.ARTICLES_WRONG_CASE_FORM,
        ErrorSubtype.ARTICLES_MISSING,
        ErrorSubtype.ARTICLES_INDEFINITE_FOR_DEFINITE,
        ErrorSubtype.ARTICLES_DEFINITE_FOR_INDEFINITE,
        ErrorSubtype.ARTICLES_KEIN_NICHT_CONFUSION,
        ErrorSubtype.ARTICLES_OTHER,
    }),
    ErrorCategory.ADJECTIVE_ENDINGS: frozenset({
        ErrorSubtype.ADJECTIVE_ENDINGS_WEAK_WRONG,
        ErrorSubtype.ADJECTIVE_ENDINGS_STRONG_WRONG,
        ErrorSubtype.ADJECTIVE_ENDINGS_MIXED_WRONG,
        ErrorSubtype.ADJECTIVE_ENDINGS_CASE_AGREEMENT,
        ErrorSubtype.ADJECTIVE_ENDINGS_GENDER_AGREEMENT,
        ErrorSubtype.ADJECTIVE_ENDINGS_OTHER,
    }),
    ErrorCategory.PREPOSITIONS: frozenset({
        ErrorSubtype.PREPOSITIONS_WRONG_FIXED,
        ErrorSubtype.PREPOSITIONS_TWO_WAY_CASE,
        ErrorSubtype.PREPOSITIONS_MISSING,
        ErrorSubtype.PREPOSITIONS_EXTRA,
        ErrorSubtype.PREPOSITIONS_OTHER,
    }),
    ErrorCategory.MODAL_VERBS: frozenset({
        ErrorSubtype.MODAL_VERBS_CONJUGATION,
        ErrorSubtype.MODAL_VERBS_INFINITIVE_WITH_ZU,
        ErrorSubtype.MODAL_VERBS_WRONG_CHOICE,
        ErrorSubtype.MODAL_VERBS_INFINITIVE_POSITION,
        ErrorSubtype.MODAL_VERBS_OTHER,
    }),
    ErrorCategory.SEPARABLE_VERBS: frozenset({
        ErrorSubtype.SEPARABLE_VERBS_PREFIX_NOT_SPLIT,
        ErrorSubtype.SEPARABLE_VERBS_PREFIX_WRONG_POS,
        ErrorSubtype.SEPARABLE_VERBS_FORM_WRONG,
        ErrorSubtype.SEPARABLE_VERBS_OTHER,
    }),
    ErrorCategory.REFLEXIVE_VERBS: frozenset({
        ErrorSubtype.REFLEXIVE_VERBS_PRONOUN_MISSING,
        ErrorSubtype.REFLEXIVE_VERBS_PRONOUN_CASE,
        ErrorSubtype.REFLEXIVE_VERBS_NOT_REFLEXIVE,
        ErrorSubtype.REFLEXIVE_VERBS_OTHER,
    }),
    ErrorCategory.KONJUNKTIV: frozenset({
        ErrorSubtype.KONJUNKTIV_FORM_WRONG,
        ErrorSubtype.KONJUNKTIV_WUERDE_WRONG,
        ErrorSubtype.KONJUNKTIV_INDICATIVE_USED,
        ErrorSubtype.KONJUNKTIV_KONJUNKTIV1_ERROR,
        ErrorSubtype.KONJUNKTIV_OTHER,
    }),
    ErrorCategory.PASSIVE: frozenset({
        ErrorSubtype.PASSIVE_WERDEN_FORM,
        ErrorSubtype.PASSIVE_PARTIZIP_WRONG,
        ErrorSubtype.PASSIVE_ZUSTAND_VORGANG_CONFUSION,
        ErrorSubtype.PASSIVE_WORD_ORDER_WRONG,
        ErrorSubtype.PASSIVE_OTHER,
    }),
    ErrorCategory.CONJUNCTIONS: frozenset({
        ErrorSubtype.CONJUNCTIONS_SUBORDINATING_WRONG,
        ErrorSubtype.CONJUNCTIONS_WEIL_DENN,
        ErrorSubtype.CONJUNCTIONS_COORDINATING_WRONG,
        ErrorSubtype.CONJUNCTIONS_MISSING,
        ErrorSubtype.CONJUNCTIONS_OTHER,
    }),
    ErrorCategory.NOUN_GENDER: frozenset({
        ErrorSubtype.NOUN_GENDER_WRONG,
        ErrorSubtype.NOUN_GENDER_OTHER,
    }),
    ErrorCategory.PLURAL_FORM: frozenset({
        ErrorSubtype.PLURAL_FORM_WRONG_SUFFIX,
        ErrorSubtype.PLURAL_FORM_SINGULAR_USED,
        ErrorSubtype.PLURAL_FORM_PLURAL_USED,
        ErrorSubtype.PLURAL_FORM_OTHER,
    }),
    ErrorCategory.RELATIVE_CLAUSES: frozenset({
        ErrorSubtype.RELATIVE_CLAUSES_PRONOUN_GENDER,
        ErrorSubtype.RELATIVE_CLAUSES_PRONOUN_CASE,
        ErrorSubtype.RELATIVE_CLAUSES_VERB_POSITION,
        ErrorSubtype.RELATIVE_CLAUSES_OTHER,
    }),
    ErrorCategory.INFINITIVE_CLAUSES: frozenset({
        ErrorSubtype.INFINITIVE_CLAUSES_ZU_MISSING,
        ErrorSubtype.INFINITIVE_CLAUSES_ZU_EXTRA,
        ErrorSubtype.INFINITIVE_CLAUSES_UM_ZU_WRONG,
        ErrorSubtype.INFINITIVE_CLAUSES_FORM_WRONG,
        ErrorSubtype.INFINITIVE_CLAUSES_OTHER,
    }),
    ErrorCategory.NEGATION: frozenset({
        ErrorSubtype.NEGATION_NICHT_KEIN_CONFUSION,
        ErrorSubtype.NEGATION_POSITION_WRONG,
        ErrorSubtype.NEGATION_OTHER,
    }),
    ErrorCategory.LEXIS: frozenset({
        ErrorSubtype.LEXIS_WRONG_WORD_CHOICE,
        ErrorSubtype.LEXIS_FALSE_FRIEND,
        ErrorSubtype.LEXIS_CALQUE_FROM_L1,
        ErrorSubtype.LEXIS_UNNATURAL_PHRASING,
        ErrorSubtype.LEXIS_REGISTER_MISMATCH,
        ErrorSubtype.LEXIS_OTHER,
    }),
    ErrorCategory.PRONUNCIATION_STT: frozenset({
        ErrorSubtype.PRONUNCIATION_STT_LIKELY_MISREAD,
        ErrorSubtype.PRONUNCIATION_STT_UNCLEAR,
        ErrorSubtype.PRONUNCIATION_STT_OTHER,
    }),
}


# Reverse map: subtype → category (for fast lookup)
_SUBTYPE_TO_CATEGORY: dict[str, str] = {
    subtype: category
    for category, subtypes in VALID_SUBTYPES_BY_CATEGORY.items()
    for subtype in subtypes
}

# ── Severity ──────────────────────────────────────────────────────────────────

VALID_SEVERITIES: FrozenSet[str] = frozenset({"low", "medium", "high"})

# ── Validators ────────────────────────────────────────────────────────────────


def validate_category(value: object) -> str:
    """Return the category code if valid, otherwise raise ValueError.

    Never coerces, never falls back to a default.
    """
    normalized = str(value or "").strip().upper()
    # Accept both UPPER_CASE and already-correct forms
    if normalized not in VALID_ERROR_CATEGORIES:
        raise ValueError(
            f"Unknown error_category {value!r}. "
            f"Valid values: {sorted(VALID_ERROR_CATEGORIES)}"
        )
    return normalized


def validate_subtype(value: object, *, category: str) -> str:
    """Return the subtype code if valid for the given category.

    Raises ValueError if the subtype is unknown or does not belong to category.
    Never coerces, never falls back to OTHER_*.
    """
    normalized = str(value or "").strip().upper()
    if normalized not in VALID_ERROR_SUBTYPES:
        raise ValueError(
            f"Unknown error_subtype {value!r}. "
            f"Valid subtypes for {category!r}: {sorted(VALID_SUBTYPES_BY_CATEGORY.get(category, set()))}"
        )
    allowed = VALID_SUBTYPES_BY_CATEGORY.get(category, frozenset())
    if normalized not in allowed:
        raise ValueError(
            f"error_subtype {value!r} does not belong to category {category!r}. "
            f"Allowed subtypes: {sorted(allowed)}"
        )
    return normalized


def validate_severity(value: object) -> str:
    """Return the severity string if valid, otherwise raise ValueError."""
    normalized = str(value or "").strip().lower()
    if normalized not in VALID_SEVERITIES:
        raise ValueError(
            f"Unknown severity {value!r}. Valid values: {sorted(VALID_SEVERITIES)}"
        )
    return normalized


def validate_confidence(value: object, *, field_name: str) -> float:
    """Return confidence as float in [0.0, 1.0], otherwise raise ValueError."""
    try:
        f = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        raise ValueError(f"{field_name} must be a float, got {value!r}")
    if not (0.0 <= f <= 1.0):
        raise ValueError(f"{field_name} must be between 0.0 and 1.0, got {f!r}")
    return f


def validate_alternatives(value: object) -> list[str]:
    """Return a clean list of alternative phrase strings.

    Raises ValueError if value is not a list or contains non-string items.
    Empty list is valid (no alternatives found).
    """
    if not isinstance(value, list):
        raise ValueError(
            f"alternatives must be a list of strings, got {type(value).__name__!r}"
        )
    result: list[str] = []
    for i, item in enumerate(value):
        if not isinstance(item, str):
            raise ValueError(
                f"alternatives[{i}] must be a string, got {type(item).__name__!r}"
            )
        stripped = item.strip()
        if stripped:
            result.append(stripped)
    return result


# ── Self-consistency assertion (runs at import time) ─────────────────────────
# Catches taxonomy editing mistakes: every subtype must appear in exactly
# one category mapping, and both sets must match.

def _assert_taxonomy_consistent() -> None:
    all_mapped: set[str] = set()
    for category, subtypes in VALID_SUBTYPES_BY_CATEGORY.items():
        if category not in VALID_ERROR_CATEGORIES:
            raise AssertionError(
                f"VALID_SUBTYPES_BY_CATEGORY key {category!r} is not in VALID_ERROR_CATEGORIES"
            )
        for st in subtypes:
            if st in all_mapped:
                raise AssertionError(
                    f"Subtype {st!r} appears in more than one category mapping"
                )
            all_mapped.add(st)
    unmapped = VALID_ERROR_SUBTYPES - all_mapped
    if unmapped:
        raise AssertionError(
            f"Subtypes defined in ErrorSubtype but missing from VALID_SUBTYPES_BY_CATEGORY: {unmapped}"
        )
    extra = all_mapped - VALID_ERROR_SUBTYPES
    if extra:
        raise AssertionError(
            f"Subtypes in VALID_SUBTYPES_BY_CATEGORY but missing from ErrorSubtype class: {extra}"
        )


_assert_taxonomy_consistent()
