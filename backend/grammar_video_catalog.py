"""Fixed catalog of German grammar topics for the weekly YouTube recommendation.

Why this exists
---------------
The weekly recommendation used to feed the raw LLM topic / the (mixed EN/RU/DE)
mistake-category labels straight into YouTube search. That produced two bugs:

* English videos ("if clauses", "Einfach Englisch") for a German topic, because
  an umlaut-free German term got mis-detected as English and translated.
* The topic shown in the header ("Nouns") did not match the video's topic
  ("Conditionals"), because header and video came from two independent queries.

This module turns the *closed* mistake taxonomy (backend/config_mistakes_data.py)
into a small fixed set of canonical German grammar topics. Each topic carries a
hand-verified German YouTube query, so:

* the search is always German (no language drift), and
* the same topic maps to the same ``topic_key`` for every user, which is what
  makes the shared video pool (bt_3_video_recommendations, keyed by focus_key)
  reusable across users instead of burning YouTube quota per person.

The mapping is deterministic: ``match_topic_key(main_category, sub_category)``
looks the mistake labels up in the taxonomy tables below. No LLM is needed to
pick the topic; the taxonomy is finite and known.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Canonical German grammar topics.
# topic_key -> {ru: Russian display, de: German display, query: German search}
# The query is what actually hits YouTube; keep it a real German grammar phrase.
# ---------------------------------------------------------------------------
GRAMMAR_VIDEO_TOPICS: dict[str, dict[str, str]] = {
    # --- Nouns ------------------------------------------------------------
    "nomen_plural": {
        "ru": "Множественное число существительных (Plural)",
        "de": "Pluralbildung der Nomen",
        "query": "Nomen Plural Pluralbildung deutsch Grammatik erklärt B1",
    },
    "komposita": {
        "ru": "Составные существительные (Komposita)",
        "de": "Zusammengesetzte Nomen (Komposita)",
        "query": "zusammengesetzte Nomen Komposita deutsch Grammatik erklärt",
    },
    "nomen_deklination": {
        "ru": "Склонение существительных (Deklination)",
        "de": "Deklination der Nomen",
        "query": "Nomen Deklination deutsch Grammatik erklärt B1 B2",
    },
    "grossschreibung": {
        "ru": "Заглавные буквы: когда слово пишется с большой (Groß-/Kleinschreibung)",
        "de": "Groß- und Kleinschreibung",
        "query": "Groß und Kleinschreibung deutsch Rechtschreibung erklärt",
    },
    # --- Articles & Determiners ------------------------------------------
    "artikel": {
        "ru": "Артикли der/die/das и ein/eine",
        "de": "Artikel der/die/das",
        "query": "Artikel der die das ein eine deutsch erklärt lernen",
    },
    "possessivartikel": {
        "ru": "Притяжательные слова mein/dein (Possessivartikel)",
        "de": "Possessivartikel mein/dein",
        "query": "Possessivartikel mein dein sein deutsch Grammatik erklärt",
    },
    "demonstrativ": {
        "ru": "Указательные слова dieser/jener (Demonstrativa)",
        "de": "Demonstrativpronomen dieser/jener",
        "query": "Demonstrativpronomen dieser jener deutsch Grammatik erklärt",
    },
    # --- Cases ------------------------------------------------------------
    "faelle_overview": {
        "ru": "Четыре падежа немецкого (Kasus: Nominativ–Genitiv)",
        "de": "Die vier Fälle (Kasus)",
        "query": "die vier Fälle Kasus Nominativ Akkusativ Dativ Genitiv deutsch erklärt",
    },
    "akkusativ": {
        "ru": "Винительный падеж (Akkusativ)",
        "de": "Akkusativ",
        "query": "Akkusativ deutsch Grammatik erklärt B1",
    },
    "dativ": {
        "ru": "Дательный падеж (Dativ)",
        "de": "Dativ",
        "query": "Dativ deutsch Grammatik erklärt B1",
    },
    "genitiv": {
        "ru": "Родительный падеж (Genitiv)",
        "de": "Genitiv",
        "query": "Genitiv deutsch Grammatik erklärt B1 B2",
    },
    "praeposition_kasus": {
        "ru": "Падеж после предлога (Präpositionen + Kasus)",
        "de": "Präpositionen mit Kasus",
        "query": "Präpositionen mit Akkusativ und Dativ deutsch erklärt",
    },
    "wechselpraepositionen": {
        "ru": "Предлоги с двумя падежами (Wechselpräpositionen)",
        "de": "Wechselpräpositionen",
        "query": "Wechselpräpositionen Akkusativ Dativ deutsch Grammatik erklärt",
    },
    # --- Pronouns ---------------------------------------------------------
    "personalpronomen": {
        "ru": "Личные местоимения (ich/du/er…)",
        "de": "Personalpronomen",
        "query": "Personalpronomen deutsch Grammatik erklärt",
    },
    "reflexivpronomen": {
        "ru": "Возвратные местоимения (sich)",
        "de": "Reflexivpronomen",
        "query": "Reflexivpronomen sich mich mir deutsch erklärt",
    },
    "relativpronomen": {
        "ru": "Относительные местоимения (der/die/das в придаточном)",
        "de": "Relativpronomen",
        "query": "Relativpronomen Relativsätze deutsch Grammatik erklärt",
    },
    "possessivpronomen": {
        "ru": "Притяжательные местоимения (meiner/deiner)",
        "de": "Possessivpronomen",
        "query": "Possessivpronomen deutsch Grammatik erklärt",
    },
    "indefinitpronomen": {
        "ru": "Неопределённые местоимения (man/jemand/etwas)",
        "de": "Indefinitpronomen",
        "query": "Indefinitpronomen man jemand etwas deutsch erklärt",
    },
    # --- Verbs ------------------------------------------------------------
    "verb_konjugation": {
        "ru": "Спряжение глаголов (Konjugation)",
        "de": "Verbkonjugation",
        "query": "Verben konjugieren Konjugation Präsens deutsch erklärt",
    },
    "starke_verben": {
        "ru": "Сильные и слабые глаголы (starke/schwache Verben)",
        "de": "Starke und schwache Verben",
        "query": "starke und schwache Verben unregelmäßige Verben deutsch erklärt",
    },
    "trennbare_verben": {
        "ru": "Отделяемые приставки глаголов (trennbare Verben)",
        "de": "Trennbare Verben",
        "query": "trennbare und untrennbare Verben deutsch Grammatik erklärt B1",
    },
    "reflexive_verben": {
        "ru": "Возвратные глаголы (sich waschen…)",
        "de": "Reflexive Verben",
        "query": "reflexive Verben sich deutsch Grammatik erklärt",
    },
    "hilfsverben": {
        "ru": "Вспомогательные глаголы sein/haben/werden",
        "de": "Hilfsverben sein/haben/werden",
        "query": "Hilfsverben sein haben werden deutsch Grammatik erklärt",
    },
    "modalverben": {
        "ru": "Модальные глаголы (können/müssen/wollen…)",
        "de": "Modalverben",
        "query": "Modalverben deutsch Grammatik erklärt B1",
    },
    "verbstellung": {
        "ru": "Место глагола в предложении (Verbstellung)",
        "de": "Verbstellung im Satz",
        "query": "Verbstellung im Hauptsatz und Nebensatz deutsch erklärt",
    },
    # --- Voice ------------------------------------------------------------
    "passiv": {
        "ru": "Пассивный залог (Passiv)",
        "de": "Passiv",
        "query": "Passiv Vorgangspassiv Zustandspassiv deutsch Grammatik erklärt",
    },
    # --- Tenses -----------------------------------------------------------
    "praesens": {
        "ru": "Настоящее время (Präsens)",
        "de": "Präsens",
        "query": "Präsens deutsch Grammatik erklärt",
    },
    "praeteritum": {
        "ru": "Прошедшее простое (Präteritum)",
        "de": "Präteritum",
        "query": "Präteritum deutsch Grammatik erklärt",
    },
    "perfekt": {
        "ru": "Прошедшее разговорное (Perfekt: haben/sein + Partizip II)",
        "de": "Perfekt",
        "query": "Perfekt haben sein Partizip 2 deutsch Grammatik erklärt B1",
    },
    "plusquamperfekt": {
        "ru": "Предпрошедшее время (Plusquamperfekt)",
        "de": "Plusquamperfekt",
        "query": "Plusquamperfekt deutsch Grammatik erklärt",
    },
    "futur": {
        "ru": "Будущее время (Futur I и II)",
        "de": "Futur I und II",
        "query": "Futur 1 und Futur 2 deutsch Grammatik erklärt",
    },
    "zeiten_uebersicht": {
        "ru": "Времена немецкого — обзор и выбор времени (Zeitformen)",
        "de": "Zeitformen Übersicht",
        "query": "Zeitformen Übersicht deutsche Zeiten erklärt",
    },
    # --- Moods ------------------------------------------------------------
    "konjunktiv_2": {
        "ru": "Сослагательное наклонение (Konjunktiv II, würde-форма)",
        "de": "Konjunktiv II",
        "query": "Konjunktiv 2 würde Form deutsch Grammatik erklärt B1 B2",
    },
    "konjunktiv_1": {
        "ru": "Косвенная речь (Konjunktiv I / indirekte Rede)",
        "de": "Konjunktiv I / indirekte Rede",
        "query": "Konjunktiv 1 indirekte Rede deutsch Grammatik erklärt",
    },
    "imperativ": {
        "ru": "Повелительное наклонение (Imperativ)",
        "de": "Imperativ",
        "query": "Imperativ Befehlsform deutsch Grammatik erklärt",
    },
    # --- Adjectives -------------------------------------------------------
    "adjektivdeklination": {
        "ru": "Окончания прилагательных (Adjektivdeklination)",
        "de": "Adjektivdeklination",
        "query": "Adjektivdeklination Adjektivendungen deutsch erklärt B1 B2",
    },
    "komparativ_superlativ": {
        "ru": "Степени сравнения прилагательных (Komparativ/Superlativ)",
        "de": "Komparativ und Superlativ",
        "query": "Komparativ Superlativ Steigerung Adjektive deutsch erklärt",
    },
    # --- Adverbs ----------------------------------------------------------
    "adverbien": {
        "ru": "Наречия (Adverbien)",
        "de": "Adverbien",
        "query": "Adverbien deutsch Grammatik erklärt",
    },
    "tekamolo": {
        "ru": "Порядок обстоятельств в предложении (TEKAMOLO)",
        "de": "TEKAMOLO — Satzstellung",
        "query": "TEKAMOLO temporal kausal modal lokal Satzstellung deutsch erklärt",
    },
    # --- Prepositions -----------------------------------------------------
    "praepositionen": {
        "ru": "Предлоги (Präpositionen)",
        "de": "Präpositionen",
        "query": "Präpositionen deutsch Grammatik erklärt B1",
    },
    "verben_mit_praeposition": {
        "ru": "Глаголы с предлогами (feste Präpositionen)",
        "de": "Verben mit Präpositionen",
        "query": "Verben mit Präpositionen feste Präpositionen deutsch erklärt",
    },
    # --- Conjunctions -----------------------------------------------------
    "konjunktionen": {
        "ru": "Союзы (und/aber/oder/denn)",
        "de": "Konjunktionen",
        "query": "Konjunktionen und aber oder denn deutsch Grammatik erklärt",
    },
    "subjunktionen": {
        "ru": "Подчинительные союзы (weil/dass/wenn/ob)",
        "de": "Subjunktionen / Nebensätze",
        "query": "Nebensätze Konjunktionen weil dass wenn ob deutsch erklärt",
    },
    # --- Word Order -------------------------------------------------------
    "wortstellung": {
        "ru": "Порядок слов в предложении (Satzbau, V2)",
        "de": "Wortstellung / Satzbau",
        "query": "Wortstellung Satzbau deutsch Verb an zweiter Stelle erklärt",
    },
    # --- Negation ---------------------------------------------------------
    "negation": {
        "ru": "Отрицание nicht и kein",
        "de": "Negation (nicht / kein)",
        "query": "Negation nicht oder kein deutsch Grammatik erklärt",
    },
    # --- Particles --------------------------------------------------------
    "modalpartikeln": {
        "ru": "Модальные частицы (doch/ja/mal/halt)",
        "de": "Modalpartikeln",
        "query": "Modalpartikeln doch ja mal halt eben deutsch erklärt",
    },
    # --- Clauses & Sentence Types ----------------------------------------
    "nebensatz": {
        "ru": "Главные и придаточные предложения (Haupt-/Nebensatz)",
        "de": "Haupt- und Nebensatz",
        "query": "Hauptsatz und Nebensatz deutsch Grammatik erklärt",
    },
    "relativsatz": {
        "ru": "Определительные придаточные (Relativsätze)",
        "de": "Relativsätze",
        "query": "Relativsätze Relativpronomen deutsch Grammatik erklärt B1",
    },
    "konditionalsatz": {
        "ru": "Условные предложения (Konditionalsätze wenn/falls)",
        "de": "Konditionalsätze (wenn/falls)",
        "query": "Konditionalsätze wenn falls deutsch Grammatik erklärt",
    },
    "finalsatz": {
        "ru": "Придаточные цели (damit / um…zu)",
        "de": "Finalsätze (damit / um…zu)",
        "query": "Finalsätze damit um zu deutsch Grammatik erklärt",
    },
    "konzessivsatz": {
        "ru": "Уступительные придаточные (obwohl)",
        "de": "Konzessivsätze (obwohl)",
        "query": "Konzessivsätze obwohl trotzdem deutsch Grammatik erklärt",
    },
    "indirekte_frage": {
        "ru": "Косвенные вопросы (indirekte Fragen)",
        "de": "Indirekte Fragen",
        "query": "indirekte Fragen Fragesätze deutsch Grammatik erklärt",
    },
    "fragen": {
        "ru": "Как строить вопросы (W-Fragen / Ja-Nein-Fragen)",
        "de": "Fragen bilden",
        "query": "Fragen bilden W-Fragen Ja Nein Fragen deutsch erklärt",
    },
    # --- Infinitive & Participles ----------------------------------------
    "infinitiv_zu": {
        "ru": "Инфинитив с zu и um…zu",
        "de": "Infinitiv mit zu / um…zu",
        "query": "Infinitiv mit zu um zu deutsch Grammatik erklärt",
    },
    "partizip": {
        "ru": "Причастия (Partizip I и Partizip II)",
        "de": "Partizip I und Partizip II",
        "query": "Partizip 1 und Partizip 2 deutsch Grammatik erklärt",
    },
    # --- Punctuation ------------------------------------------------------
    "komma": {
        "ru": "Запятые и знаки препинания (Kommasetzung)",
        "de": "Kommasetzung",
        "query": "Kommasetzung deutsch Zeichensetzung Regeln erklärt",
    },
    # --- Orthography & Spelling ------------------------------------------
    "rechtschreibung": {
        "ru": "Орфография (ß/ss, умлауты, написание)",
        "de": "Rechtschreibung",
        "query": "Rechtschreibung deutsch ß ss Umlaute erklärt",
    },
    # --- Generic fallback (last resort, still a real German query) --------
    "deutsch_grammatik_allgemein": {
        "ru": "Немецкая грамматика — общий разбор",
        "de": "Deutsche Grammatik",
        "query": "deutsche Grammatik B1 B2 einfach erklärt",
    },
}

# Last-resort topic when nothing in the taxonomy maps.
FALLBACK_TOPIC_KEY = "deutsch_grammatik_allgemein"

# ---------------------------------------------------------------------------
# Exact mapping: mistake subcategory label (lowercased) -> topic_key.
# Labels come verbatim from backend/config_mistakes_data.py (VALID_SUBCATEGORIES).
# ---------------------------------------------------------------------------
_SUBCATEGORY_TO_TOPIC: dict[str, str] = {
    # Nouns
    "pluralization": "nomen_plural",
    "compound nouns": "komposita",
    "declension errors": "nomen_deklination",
    "noun capitalization": "grossschreibung",
    "noun-verb confusion": "grossschreibung",
    # Articles & Determiners
    "definite articles (der/die/das)": "artikel",
    "indefinite articles (ein/eine)": "artikel",
    "negation article (kein)": "negation",
    "possessive determiners (mein/dein...)": "possessivartikel",
    "demonstratives (dieser/jener)": "demonstrativ",
    "quantifiers (jeder/mancher/viele...)": "artikel",
    "article omission/redundancy": "artikel",
    # Cases
    "nominative": "faelle_overview",
    "accusative": "akkusativ",
    "dative": "dativ",
    "genitive": "genitiv",
    "case after preposition": "praeposition_kasus",
    "akkusativ + preposition": "praeposition_kasus",
    "dative + preposition": "praeposition_kasus",
    "genitive + preposition": "praeposition_kasus",
    "two-way prepositions (wechselpräpositionen)": "wechselpraepositionen",
    "case agreement in noun phrase": "nomen_deklination",
    # Pronouns
    "personal pronouns": "personalpronomen",
    "reflexive pronouns": "reflexivpronomen",
    "relative pronouns": "relativpronomen",
    "possessive pronouns": "possessivpronomen",
    "demonstrative pronouns": "demonstrativ",
    "indefinite pronouns (man/jemand/etwas)": "indefinitpronomen",
    "pronoun reference (wrong antecedent)": "personalpronomen",
    # Verbs
    "conjugation": "verb_konjugation",
    "weak verbs": "starke_verben",
    "strong verbs": "starke_verben",
    "mixed verbs": "starke_verben",
    "separable verbs": "trennbare_verben",
    "inseparable prefix verbs": "trennbare_verben",
    "reflexive verbs": "reflexive_verben",
    "auxiliary verbs (sein/haben/werden)": "hilfsverben",
    "modal verbs": "modalverben",
    "verb valency (missing object/complement)": "verb_konjugation",
    "verb placement in main clause": "verbstellung",
    "verb placement in subordinate clause": "verbstellung",
    "infinitive form errors": "infinitiv_zu",
    # Voice
    "vorgangspassiv (werden + partizip ii)": "passiv",
    "zustandspassiv (sein + partizip ii)": "passiv",
    "passive with modal verbs": "passiv",
    "passive word order": "passiv",
    "agent phrase (von/durch) misuse": "passiv",
    "active-passive confusion": "passiv",
    # Tenses
    "present (präsens)": "praesens",
    "simple past (präteritum)": "praeteritum",
    "present perfect (perfekt)": "perfekt",
    "past perfect (plusquamperfekt)": "plusquamperfekt",
    "future 1 (futur i)": "futur",
    "future 2 (futur ii)": "futur",
    "sequence of tenses": "zeiten_uebersicht",
    "tense choice (context mismatch)": "zeiten_uebersicht",
    # Moods
    "indicative": "verb_konjugation",
    "imperative": "imperativ",
    "subjunctive 1 (konjunktiv i)": "konjunktiv_1",
    "subjunctive 2 (konjunktiv ii)": "konjunktiv_2",
    "konjunktiv ii: würde-form": "konjunktiv_2",
    "irrealis / hypothetical": "konjunktiv_2",
    "politeness forms": "konjunktiv_2",
    "reported speech (indirekte rede)": "konjunktiv_1",
    # Adjectives
    "endings": "adjektivdeklination",
    "weak declension": "adjektivdeklination",
    "strong declension": "adjektivdeklination",
    "mixed declension": "adjektivdeklination",
    "adjective placement": "adjektivdeklination",
    "comparative": "komparativ_superlativ",
    "superlative": "komparativ_superlativ",
    "incorrect adjective case agreement": "adjektivdeklination",
    "adjective vs participle confusion": "partizip",
    # Adverbs
    "adverb placement": "tekamolo",
    "multiple adverbs (tekamolo)": "tekamolo",
    "incorrect adverb usage": "adverbien",
    "sentence adverbs (leider, vielleicht...)": "adverbien",
    # Prepositions
    "accusative prepositions": "praepositionen",
    "dative prepositions": "praepositionen",
    "genitive prepositions": "praepositionen",
    "two-way prepositions": "wechselpraepositionen",
    "incorrect preposition usage": "praepositionen",
    "preposition omission/redundancy": "praepositionen",
    "fixed prepositional phrases (idiomatic)": "verben_mit_praeposition",
    # Conjunctions
    "coordinating (und/aber/oder/denn)": "konjunktionen",
    "subordinating (weil/dass/ob/wenn...)": "subjunktionen",
    "correlative conjunctions (entweder...oder / sowohl...als auch)": "konjunktionen",
    "incorrect use of conjunctions": "konjunktionen",
    # Word Order
    "standard": "wortstellung",
    "inverted": "wortstellung",
    "verb-second rule (v2)": "wortstellung",
    "verb-first (questions/imperative)": "fragen",
    "position of negation": "negation",
    "position of time/manner/place": "tekamolo",
    "incorrect order in subordinate clause": "nebensatz",
    "incorrect order with modal verb": "verbstellung",
    "placement of separable prefix": "trennbare_verben",
    "placement of participle (perfekt/passive)": "perfekt",
    # Negation
    "nicht vs kein": "negation",
    "negation placement": "negation",
    "double negation": "negation",
    "negation with pronouns/quantifiers": "negation",
    # Particles
    "modal particles (doch/ja/mal/halt/eben)": "modalpartikeln",
    "focus particles (nur/auch/sogar)": "modalpartikeln",
    "antwortpartikeln (ja/nein/doch)": "modalpartikeln",
    "particle misuse/omission": "modalpartikeln",
    # Clauses & Sentence Types
    "main vs subordinate clause": "nebensatz",
    "relative clauses": "relativsatz",
    "indirect questions": "indirekte_frage",
    "conditionals (wenn/falls)": "konditionalsatz",
    "purpose clauses (damit/um...zu)": "finalsatz",
    "concessive clauses (obwohl)": "konzessivsatz",
    "infinitive clauses vs dass-clause": "infinitiv_zu",
    "question formation": "fragen",
    # Infinitive & Participles
    "zu + infinitive": "infinitiv_zu",
    "um...zu": "infinitiv_zu",
    "infinitive with modal verbs": "modalverben",
    "partizip i": "partizip",
    "partizip ii": "partizip",
    "participial constructions": "partizip",
    "gerund-like constructions (beim + infinitive)": "infinitiv_zu",
    # Punctuation
    "comma in subordinate clause": "komma",
    "comma in relative clause": "komma",
    "comma with infinitive group": "komma",
    "question mark / exclamation": "komma",
    "quotation marks": "komma",
    # Orthography & Spelling
    "capitalization": "grossschreibung",
    "ß/ss": "rechtschreibung",
    "umlauts (ä/ö/ü)": "rechtschreibung",
    "hyphenation": "rechtschreibung",
    "common spelling errors": "rechtschreibung",
}

# ---------------------------------------------------------------------------
# Fallback mapping by main category (lowercased) -> topic_key, used when the
# subcategory is empty/unrecognised. Labels from VALID_CATEGORIES.
# ---------------------------------------------------------------------------
_CATEGORY_TO_TOPIC: dict[str, str] = {
    "nouns": "nomen_deklination",
    "articles & determiners": "artikel",
    "cases": "faelle_overview",
    "pronouns": "personalpronomen",
    "verbs": "verb_konjugation",
    "voice (active/passive)": "passiv",
    "tenses": "zeiten_uebersicht",
    "moods": "konjunktiv_2",
    "adjectives": "adjektivdeklination",
    "adverbs": "adverbien",
    "prepositions": "praepositionen",
    "conjunctions": "konjunktionen",
    "word order": "wortstellung",
    "negation": "negation",
    "particles": "modalpartikeln",
    "clauses & sentence types": "nebensatz",
    "infinitive & participles": "infinitiv_zu",
    "punctuation": "komma",
    "orthography & spelling": "rechtschreibung",
    # "Other mistake" intentionally absent -> match_topic_key returns None.
}


def _normalize_label(value) -> str:
    return " ".join(str(value or "").strip().lower().split())


def match_topic_key(main_category, sub_category) -> str | None:
    """Map a mistake (main_category, sub_category) to a canonical topic_key.

    Prefers the more specific subcategory; falls back to the main category.
    Returns ``None`` when the labels are unknown / "Other mistake" so the caller
    can decide whether to use the generic fallback topic or skip.
    """
    sub = _normalize_label(sub_category)
    if sub and sub in _SUBCATEGORY_TO_TOPIC:
        return _SUBCATEGORY_TO_TOPIC[sub]
    main = _normalize_label(main_category)
    if main and main in _CATEGORY_TO_TOPIC:
        return _CATEGORY_TO_TOPIC[main]
    return None


def get_topic(topic_key: str | None) -> dict | None:
    """Return the catalog entry (ru/de/query) for a topic_key, or None."""
    if not topic_key:
        return None
    return GRAMMAR_VIDEO_TOPICS.get(str(topic_key).strip())


def topic_display(topic_key: str | None) -> str:
    """Human-facing topic label for the message (Russian + German in parens)."""
    entry = get_topic(topic_key)
    if not entry:
        return ""
    return str(entry.get("ru") or entry.get("de") or "").strip()
