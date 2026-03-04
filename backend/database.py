import psycopg2
from psycopg2 import Binary
from psycopg2 import OperationalError
from psycopg2.extras import Json
import os
import hashlib
from contextlib import contextmanager
import json
import random
from decimal import Decimal, InvalidOperation
from datetime import datetime, timezone, date, timedelta, time as dt_time
from pathlib import Path
import time
from uuid import uuid4
from calendar import monthrange
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

env_path = Path(__file__).parent / ".env"
load_dotenv(dotenv_path=env_path)

DATABASE_URL = os.getenv("DATABASE_URL_RAILWAY") #
DB_CONNECT_TIMEOUT_SECONDS = int(os.getenv("DB_CONNECT_TIMEOUT_SECONDS", "12"))
DB_CONNECT_RETRIES = max(1, int(os.getenv("DB_CONNECT_RETRIES", "3")))
DB_CONNECT_RETRY_DELAY_SECONDS = float(os.getenv("DB_CONNECT_RETRY_DELAY_SECONDS", "0.6"))
SUPPORTED_LEARNING_LANGUAGES = {"de", "en", "es", "it"}
SUPPORTED_NATIVE_LANGUAGES = {"ru", "en", "de"}
DEFAULT_LEARNING_LANGUAGE = "de"
DEFAULT_NATIVE_LANGUAGE = "ru"
DICTIONARY_ORIGIN_ALLOWED = {
    "unknown",
    "unknown_legacy",
    "webapp_dictionary_save",
    "mobile_dictionary_save",
    "bot_private_save",
    "sentence_gpt_seed",
    "translations_block",
    "youtube",
    "reader",
    "assistant",
    "import",
}


def _normalize_dictionary_origin_process(value: str | None) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in DICTIONARY_ORIGIN_ALLOWED:
        return normalized
    return "unknown"


def _coerce_json_object(value) -> dict:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, dict):
                return dict(parsed)
        except Exception:
            return {}
    return {}


def _env_int(name: str, default: int) -> int:
    try:
        return int(str(os.getenv(name, str(default))).strip() or str(default))
    except Exception:
        return int(default)


def _env_decimal(name: str, default: str | None) -> Decimal | None:
    raw = os.getenv(name)
    if raw is None:
        raw = default
    if raw is None:
        return None
    value = str(raw).strip()
    if not value:
        return None
    if value.lower() == "null":
        return None
    try:
        parsed = Decimal(value)
    except (InvalidOperation, ValueError):
        raise ValueError(f"{name} must be a valid decimal or NULL, got: {value!r}")
    if parsed <= 0:
        raise ValueError(f"{name} must be > 0 or NULL, got: {value!r}")
    return parsed


FLASHCARD_RECENT_SEEN_HOURS = max(1, _env_int("FLASHCARD_RECENT_SEEN_HOURS", 24))


# Used only if billing ledger stores events in USD and caps are enforced in EUR.
FX_USD_TO_EUR = _env_decimal("FX_USD_TO_EUR", "0.92") or Decimal("0.92")
TRIAL_POLICY_DAYS = max(0, _env_int("TRIAL_DAYS", 3))
TRIAL_POLICY_TZ = "Europe/Vienna"
GOOGLE_TTS_MONTHLY_BASE_LIMIT_CHARS = max(1, _env_int("GOOGLE_TTS_MONTHLY_BASE_LIMIT_CHARS", 1_000_000))
GOOGLE_TRANSLATE_MONTHLY_BASE_LIMIT_CHARS = max(1, _env_int("GOOGLE_TRANSLATE_MONTHLY_BASE_LIMIT_CHARS", 500_000))
READER_AUDIO_PRO_MONTHLY_LIMIT_CHARS = max(1, _env_int("READER_AUDIO_PRO_MONTHLY_LIMIT_CHARS", 10_000))


def convert_cost_to_eur(amount, currency: str | None) -> float:
    try:
        amount_decimal = Decimal(str(amount))
    except (InvalidOperation, ValueError, TypeError):
        amount_decimal = Decimal("0")
    currency_code = str(currency or "EUR").strip().upper() or "EUR"
    if currency_code == "EUR":
        eur_amount = amount_decimal
    elif currency_code == "USD":
        eur_amount = amount_decimal * FX_USD_TO_EUR
    else:
        raise ValueError(f"Unsupported currency for EUR conversion: {currency_code}")
    return float(eur_amount)


def _resolve_timezone(tz_name: str | None) -> ZoneInfo:
    candidate = str(tz_name or TRIAL_POLICY_TZ).strip() or TRIAL_POLICY_TZ
    try:
        return ZoneInfo(candidate)
    except Exception:
        return ZoneInfo(TRIAL_POLICY_TZ)


def _to_aware_datetime(value: datetime | None) -> datetime:
    if value is None:
        return datetime.now(timezone.utc)
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _compute_trial_ends_at(
    now_ts: datetime | None,
    *,
    trial_days: int = TRIAL_POLICY_DAYS,
    tz_name: str = TRIAL_POLICY_TZ,
) -> datetime:
    # Contract (calendar trial in Europe/Vienna):
    # trial_ends_at is set to local 00:00 of (local signup date + TRIAL_DAYS).
    # Example: signup at 2026-02-23 15:00 Europe/Vienna with TRIAL_DAYS=3
    # => trial_ends_at = 2026-02-26 00:00 Europe/Vienna.
    tzinfo = _resolve_timezone(tz_name)
    now_utc = _to_aware_datetime(now_ts)
    local_now = now_utc.astimezone(tzinfo)
    trial_end_date = local_now.date() + timedelta(days=max(0, int(trial_days)))
    trial_end_local = datetime.combine(trial_end_date, dt_time.min, tzinfo=tzinfo)
    return trial_end_local.astimezone(timezone.utc)

SKILL_SEED_DE: list[tuple[str, str, str]] = [
    ("nouns_articles_gender", "Nouns: Articles & Gender", "Nouns"),
    ("nouns_plural", "Nouns: Plural", "Nouns"),
    ("nouns_compounds", "Nouns: Compound Nouns", "Nouns"),
    ("nouns_declension", "Nouns: Declension", "Nouns"),
    ("cases_nominative", "Cases: Nominative", "Cases"),
    ("cases_accusative", "Cases: Accusative", "Cases"),
    ("cases_dative", "Cases: Dative", "Cases"),
    ("cases_genitive", "Cases: Genitive", "Cases"),
    ("cases_preposition_accusative", "Cases: Akk + Preposition", "Cases"),
    ("cases_preposition_dative", "Cases: Dat + Preposition", "Cases"),
    ("cases_preposition_genitive", "Cases: Gen + Preposition", "Cases"),
    ("verbs_conjugation", "Verbs: Conjugation", "Verbs"),
    ("verbs_weak", "Verbs: Weak", "Verbs"),
    ("verbs_strong", "Verbs: Strong", "Verbs"),
    ("verbs_mixed", "Verbs: Mixed", "Verbs"),
    ("verbs_separable", "Verbs: Separable", "Verbs"),
    ("verbs_reflexive", "Verbs: Reflexive", "Verbs"),
    ("verbs_auxiliaries", "Verbs: Auxiliaries", "Verbs"),
    ("verbs_modals", "Verbs: Modals", "Verbs"),
    ("verbs_placement_general", "Verbs: Placement", "Verbs"),
    ("verbs_placement_subordinate", "Verbs: Placement in Subordinate Clause", "Verbs"),
    ("tenses_present", "Tenses: Present", "Tenses"),
    ("tenses_past_general", "Tenses: Past (General)", "Tenses"),
    ("tenses_prateritum", "Tenses: Prateritum", "Tenses"),
    ("tenses_perfekt", "Tenses: Perfekt", "Tenses"),
    ("tenses_plusquamperfekt", "Tenses: Plusquamperfekt", "Tenses"),
    ("tenses_future_general", "Tenses: Future (General)", "Tenses"),
    ("tenses_futur1", "Tenses: Futur I", "Tenses"),
    ("tenses_futur2", "Tenses: Futur II", "Tenses"),
    ("voice_passive_plusquamperfekt", "Passive: Plusquamperfekt", "Tenses"),
    ("voice_passive_futur1", "Passive: Futur I", "Tenses"),
    ("voice_passive_futur2", "Passive: Futur II", "Tenses"),
    ("adjectives_endings_general", "Adjectives: Endings", "Adjectives"),
    ("adjectives_declension_weak", "Adjectives: Weak Declension", "Adjectives"),
    ("adjectives_declension_strong", "Adjectives: Strong Declension", "Adjectives"),
    ("adjectives_declension_mixed", "Adjectives: Mixed Declension", "Adjectives"),
    ("adjectives_placement", "Adjectives: Placement", "Adjectives"),
    ("adjectives_comparative", "Adjectives: Comparative", "Adjectives"),
    ("adjectives_superlative", "Adjectives: Superlative", "Adjectives"),
    ("adjectives_case_agreement", "Adjectives: Case Agreement", "Adjectives"),
    ("adverbs_placement", "Adverbs: Placement", "Adverbs"),
    ("adverbs_multiple_order", "Adverbs: Multiple Adverbs", "Adverbs"),
    ("adverbs_usage", "Adverbs: Usage", "Adverbs"),
    ("conj_coordinating", "Conjunctions: Coordinating", "Conjunctions"),
    ("conj_subordinating", "Conjunctions: Subordinating", "Conjunctions"),
    ("conj_usage", "Conjunctions: Usage", "Conjunctions"),
    ("prepositions_accusative_group", "Prepositions: Accusative Group", "Prepositions"),
    ("prepositions_dative_group", "Prepositions: Dative Group", "Prepositions"),
    ("prepositions_genitive_group", "Prepositions: Genitive Group", "Prepositions"),
    ("prepositions_two_way", "Prepositions: Two-way", "Prepositions"),
    ("prepositions_usage", "Prepositions: Usage", "Prepositions"),
    ("moods_indicative", "Moods: Indicative", "Moods"),
    ("moods_declarative", "Moods: Declarative", "Moods"),
    ("moods_interrogative", "Moods: Interrogative", "Moods"),
    ("moods_imperative", "Moods: Imperative", "Moods"),
    ("moods_subjunctive1", "Moods: Subjunctive I", "Moods"),
    ("moods_subjunctive2", "Moods: Subjunctive II", "Moods"),
    ("word_order_standard", "Word Order: Standard", "Word Order"),
    ("word_order_inverted", "Word Order: Inverted", "Word Order"),
    ("word_order_v2_rule", "Word Order: V2 Rule", "Word Order"),
    ("word_order_negation_position", "Word Order: Negation Position", "Word Order"),
    ("word_order_subordinate_clause", "Word Order: Subordinate Clause", "Word Order"),
    ("word_order_modal_structure", "Word Order: Modal Structure", "Word Order"),
    ("other_unclassified", "Other: Unclassified", "Other"),
]

ERROR_SKILL_MAP_SEED_DE: list[tuple[str, str, str, float]] = [
    ("Nouns", "Gendered Articles", "nouns_articles_gender", 1.2),
    ("Nouns", "Pluralization", "nouns_plural", 1.0),
    ("Nouns", "Compound Nouns", "nouns_compounds", 0.8),
    ("Nouns", "Declension Errors", "nouns_declension", 1.0),
    ("Cases", "Nominative", "cases_nominative", 0.8),
    ("Cases", "Accusative", "cases_accusative", 1.0),
    ("Cases", "Dative", "cases_dative", 1.0),
    ("Cases", "Genitive", "cases_genitive", 0.9),
    ("Cases", "Akkusativ + Preposition", "cases_preposition_accusative", 1.1),
    ("Cases", "Dative + Preposition", "cases_preposition_dative", 1.1),
    ("Cases", "Genitive + Preposition", "cases_preposition_genitive", 1.0),
    ("Verbs", "Placement", "verbs_placement_general", 1.2),
    ("Verbs", "Conjugation", "verbs_conjugation", 1.1),
    ("Verbs", "Weak Verbs", "verbs_weak", 0.9),
    ("Verbs", "Strong Verbs", "verbs_strong", 1.0),
    ("Verbs", "Mixed Verbs", "verbs_mixed", 1.0),
    ("Verbs", "Separable Verbs", "verbs_separable", 1.1),
    ("Verbs", "Reflexive Verbs", "verbs_reflexive", 1.1),
    ("Verbs", "Auxiliary Verbs", "verbs_auxiliaries", 1.2),
    ("Verbs", "Modal Verbs", "verbs_modals", 1.2),
    ("Verbs", "Verb Placement in Subordinate Clause", "verbs_placement_subordinate", 1.3),
    ("Tenses", "Present", "tenses_present", 0.7),
    ("Tenses", "Past", "tenses_past_general", 0.8),
    ("Tenses", "Simple Past", "tenses_prateritum", 0.9),
    ("Tenses", "Present Perfect", "tenses_perfekt", 1.0),
    ("Tenses", "Past Perfect", "tenses_plusquamperfekt", 1.0),
    ("Tenses", "Future", "tenses_future_general", 0.9),
    ("Tenses", "Future 1", "tenses_futur1", 0.9),
    ("Tenses", "Future 2", "tenses_futur2", 1.0),
    ("Tenses", "Plusquamperfekt Passive", "voice_passive_plusquamperfekt", 1.2),
    ("Tenses", "Futur 1 Passive", "voice_passive_futur1", 1.2),
    ("Tenses", "Futur 2 Passive", "voice_passive_futur2", 1.2),
    ("Adjectives", "Endings", "adjectives_endings_general", 1.3),
    ("Adjectives", "Weak Declension", "adjectives_declension_weak", 1.2),
    ("Adjectives", "Strong Declension", "adjectives_declension_strong", 1.2),
    ("Adjectives", "Mixed Declension", "adjectives_declension_mixed", 1.2),
    ("Adjectives", "Placement", "adjectives_placement", 0.9),
    ("Adjectives", "Comparative", "adjectives_comparative", 0.8),
    ("Adjectives", "Superlative", "adjectives_superlative", 0.8),
    ("Adjectives", "Incorrect Adjective Case Agreement", "adjectives_case_agreement", 1.3),
    ("Adverbs", "Placement", "adverbs_placement", 0.9),
    ("Adverbs", "Multiple Adverbs", "adverbs_multiple_order", 1.0),
    ("Adverbs", "Incorrect Adverb Usage", "adverbs_usage", 1.0),
    ("Conjunctions", "Coordinating", "conj_coordinating", 0.9),
    ("Conjunctions", "Subordinating", "conj_subordinating", 1.1),
    ("Conjunctions", "Incorrect Use of Conjunctions", "conj_usage", 1.1),
    ("Prepositions", "Accusative", "prepositions_accusative_group", 1.0),
    ("Prepositions", "Dative", "prepositions_dative_group", 1.0),
    ("Prepositions", "Genitive", "prepositions_genitive_group", 0.9),
    ("Prepositions", "Two-way", "prepositions_two_way", 1.2),
    ("Prepositions", "Incorrect Preposition Usage", "prepositions_usage", 1.2),
    ("Moods", "Indicative", "moods_indicative", 0.6),
    ("Moods", "Declarative", "moods_declarative", 0.6),
    ("Moods", "Interrogative", "moods_interrogative", 0.7),
    ("Moods", "Imperative", "moods_imperative", 0.9),
    ("Moods", "Subjunctive 1", "moods_subjunctive1", 1.2),
    ("Moods", "Subjunctive 2", "moods_subjunctive2", 1.2),
    ("Word Order", "Standard", "word_order_standard", 0.8),
    ("Word Order", "Inverted", "word_order_inverted", 1.0),
    ("Word Order", "Verb-Second Rule", "word_order_v2_rule", 1.2),
    ("Word Order", "Position of Negation", "word_order_negation_position", 1.1),
    ("Word Order", "Incorrect Order in Subordinate Clause", "word_order_subordinate_clause", 1.3),
    ("Word Order", "Incorrect Order with Modal Verb", "word_order_modal_structure", 1.2),
    ("Other mistake", "Unclassified mistake", "other_unclassified", 1.0),
]

SKILL_SEED_EN: list[tuple[str, str, str]] = [
    ("en_nouns_plural", "Nouns: Pluralization", "Nouns"),
    ("en_nouns_countability", "Nouns: Countable vs Uncountable", "Nouns"),
    ("en_determiners_articles", "Articles: a/an/the/zero", "Articles & Determiners"),
    ("en_determiners_quantifiers", "Determiners: some/any/much/many", "Articles & Determiners"),
    ("en_pronouns_case", "Pronouns: Subject/Object", "Pronouns"),
    ("en_pronouns_reflexive", "Pronouns: Reflexive", "Pronouns"),
    ("en_verbs_agreement", "Verbs: Conjugation/Agreement", "Verbs"),
    ("en_aux_do_support", "Auxiliaries: do-support", "Verbs"),
    ("en_aux_be_having", "Auxiliaries: be/have", "Verbs"),
    ("en_modals", "Verbs: Modal Verbs", "Verbs"),
    ("en_phrasal_verbs", "Verbs: Phrasal Verbs", "Verbs"),
    ("en_verb_patterns", "Verbs: to V / V-ing", "Verbs"),
    ("en_tense_present_simple", "Tense: Present Simple", "Tenses & Aspect"),
    ("en_tense_present_continuous", "Tense: Present Continuous", "Tenses & Aspect"),
    ("en_tense_past_simple", "Tense: Past Simple", "Tenses & Aspect"),
    ("en_tense_past_continuous", "Tense: Past Continuous", "Tenses & Aspect"),
    ("en_tense_present_perfect", "Tense: Present Perfect", "Tenses & Aspect"),
    ("en_tense_past_perfect", "Tense: Past Perfect", "Tenses & Aspect"),
    ("en_future", "Tense: Future", "Tenses & Aspect"),
    ("en_conditionals", "Conditionals", "Tenses & Aspect"),
    ("en_adjectives_order", "Adjectives: Order", "Adjectives"),
    ("en_comparative_superlative", "Comparative/Superlative", "Adjectives"),
    ("en_adverbs_placement", "Adverbs: Placement", "Adverbs"),
    ("en_prepositions_time_place", "Prepositions: Time/Place", "Prepositions"),
    ("en_word_order_questions_negation", "Word Order: Questions/Negation", "Word Order"),
    ("en_other_unclassified", "Other: Unclassified", "Other"),
]

ERROR_SKILL_MAP_SEED_EN: list[tuple[str, str, str, float]] = [
    ("Nouns", "Pluralization", "en_nouns_plural", 1.0),
    ("Nouns", "Countable vs Uncountable", "en_nouns_countability", 1.2),
    ("Nouns", "Articles/Determiners", "en_determiners_articles", 1.1),
    ("Pronouns", "Subject/Object", "en_pronouns_case", 1.0),
    ("Pronouns", "Reflexive Pronouns", "en_pronouns_reflexive", 1.0),
    ("Verbs", "Conjugation/Agreement", "en_verbs_agreement", 1.2),
    ("Verbs", "Auxiliaries (do/be/have)", "en_aux_do_support", 1.1),
    ("Verbs", "Modal Verbs", "en_modals", 1.1),
    ("Verbs", "Phrasal Verbs", "en_phrasal_verbs", 1.0),
    ("Verbs", "Verb Patterns (to V / V-ing)", "en_verb_patterns", 1.2),
    ("Tenses & Aspect", "Present Simple", "en_tense_present_simple", 0.9),
    ("Tenses & Aspect", "Present Continuous", "en_tense_present_continuous", 0.9),
    ("Tenses & Aspect", "Past Simple", "en_tense_past_simple", 0.9),
    ("Tenses & Aspect", "Past Continuous", "en_tense_past_continuous", 1.0),
    ("Tenses & Aspect", "Present Perfect", "en_tense_present_perfect", 1.1),
    ("Tenses & Aspect", "Past Perfect", "en_tense_past_perfect", 1.0),
    ("Tenses & Aspect", "Future (will/going to)", "en_future", 0.9),
    ("Tenses & Aspect", "Conditionals", "en_conditionals", 1.2),
    ("Adjectives", "Order of Adjectives", "en_adjectives_order", 1.0),
    ("Adjectives", "Comparative", "en_comparative_superlative", 0.8),
    ("Adjectives", "Superlative", "en_comparative_superlative", 0.8),
    ("Adverbs", "Placement", "en_adverbs_placement", 1.0),
    ("Prepositions", "Time", "en_prepositions_time_place", 1.0),
    ("Prepositions", "Place", "en_prepositions_time_place", 1.0),
    ("Word Order", "Questions (aux inversion)", "en_word_order_questions_negation", 1.2),
    ("Word Order", "Negation", "en_word_order_questions_negation", 1.1),
    ("Articles & Determiners", "a/an/the/zero", "en_determiners_articles", 1.2),
    ("Articles & Determiners", "Some/Any", "en_determiners_quantifiers", 1.0),
    ("Articles & Determiners", "Much/Many", "en_determiners_quantifiers", 1.0),
    ("Other mistake", "Unclassified mistake", "en_other_unclassified", 1.0),
]

SKILL_SEED_ES: list[tuple[str, str, str]] = [
    ("es_articles_gender", "Nouns: Gendered Articles", "Nouns"),
    ("es_nouns_plural", "Nouns: Pluralization", "Nouns"),
    ("es_agreement_gender_number", "Agreement: Gender/Number", "Nouns"),
    ("es_pronouns_object_lo_la_le", "Pronouns: lo/la/le", "Pronouns"),
    ("es_clitics_placement", "Pronouns: Clitic Placement", "Pronouns"),
    ("es_reflexive_se", "Pronouns: Reflexive se", "Pronouns"),
    ("es_conjugation_general", "Verbs: Conjugation", "Verbs"),
    ("es_ser_estar", "Verbs: Ser vs Estar", "Verbs"),
    ("es_periphrasis_modals", "Verbs: Periphrasis", "Verbs"),
    ("es_imperatives", "Verbs: Imperatives", "Verbs"),
    ("es_tense_present", "Tense: Present", "Tenses"),
    ("es_tense_perfecto", "Tense: Preterito Perfecto", "Tenses"),
    ("es_tense_indefinido", "Tense: Preterito Indefinido", "Tenses"),
    ("es_tense_imperfecto", "Tense: Imperfecto", "Tenses"),
    ("es_tense_pluscuamperfecto", "Tense: Pluscuamperfecto", "Tenses"),
    ("es_tense_future", "Tense: Future", "Tenses"),
    ("es_tense_conditional", "Tense: Conditional", "Tenses"),
    ("es_subjunctive_present", "Mood: Subjunctive Present", "Moods"),
    ("es_subjunctive_past", "Mood: Subjunctive Past", "Moods"),
    ("es_subjunctive_selection", "Mood: Indicative vs Subjunctive", "Moods"),
    ("es_por_para", "Prepositions: Por vs Para", "Prepositions"),
    ("es_personal_a", "Prepositions: Personal A", "Prepositions"),
    ("es_prepositions_usage_general", "Prepositions: Usage", "Prepositions"),
    ("es_word_order_questions", "Word Order: Questions", "Word Order"),
    ("es_negation", "Word Order: Negation", "Word Order"),
    ("es_clitic_order_se_lo", "Word Order: se lo order", "Word Order"),
    ("es_adj_adv_comparison", "Adjectives/Adverbs: Comparison", "Adjectives/Adverbs"),
    ("es_adverbs_formation", "Adverbs: Formation", "Adjectives/Adverbs"),
    ("es_orthography_accents", "Orthography: Accent Marks", "Orthography"),
    ("es_orthography_punctuation_spelling", "Orthography: Punctuation/Spelling", "Orthography"),
    ("es_other_unclassified", "Other: Unclassified", "Other"),
]

ERROR_SKILL_MAP_SEED_ES: list[tuple[str, str, str, float]] = [
    ("Nouns", "Gendered Articles", "es_articles_gender", 1.2),
    ("Nouns", "Pluralization", "es_nouns_plural", 1.0),
    ("Nouns", "Agreement (gender/number)", "es_agreement_gender_number", 1.2),
    ("Pronouns", "Object Pronouns (lo/la/le)", "es_pronouns_object_lo_la_le", 1.2),
    ("Pronouns", "Clitic Placement", "es_clitics_placement", 1.2),
    ("Pronouns", "Reflexive (se)", "es_reflexive_se", 1.0),
    ("Verbs", "Conjugation", "es_conjugation_general", 1.1),
    ("Verbs", "Ser vs Estar", "es_ser_estar", 1.3),
    ("Verbs", "Modal/Periphrasis (ir a, tener que)", "es_periphrasis_modals", 1.0),
    ("Verbs", "Imperatives", "es_imperatives", 1.0),
    ("Tenses", "Present", "es_tense_present", 0.8),
    ("Tenses", "Preterito Perfecto", "es_tense_perfecto", 1.0),
    ("Tenses", "Preterito Indefinido", "es_tense_indefinido", 1.1),
    ("Tenses", "Imperfecto", "es_tense_imperfecto", 1.1),
    ("Tenses", "Pluscuamperfecto", "es_tense_pluscuamperfecto", 1.1),
    ("Tenses", "Future", "es_tense_future", 0.9),
    ("Tenses", "Conditional", "es_tense_conditional", 1.0),
    ("Moods", "Subjunctive (Present)", "es_subjunctive_present", 1.2),
    ("Moods", "Subjunctive (Past)", "es_subjunctive_past", 1.2),
    ("Moods", "Indicative vs Subjunctive", "es_subjunctive_selection", 1.3),
    ("Prepositions", "Por vs Para", "es_por_para", 1.3),
    ("Prepositions", "A personal", "es_personal_a", 1.1),
    ("Prepositions", "Preposition Usage", "es_prepositions_usage_general", 1.1),
    ("Word Order", "Questions", "es_word_order_questions", 1.0),
    ("Word Order", "Negation", "es_negation", 0.9),
    ("Word Order", "Clitic order (se lo)", "es_clitic_order_se_lo", 1.3),
    ("Adjectives/Adverbs", "Comparative/Superlative", "es_adj_adv_comparison", 0.9),
    ("Adjectives/Adverbs", "Adverb Formation", "es_adverbs_formation", 0.9),
    ("Orthography", "Accent Marks", "es_orthography_accents", 1.1),
    ("Orthography", "Punctuation (¿¡)", "es_orthography_punctuation_spelling", 0.8),
    ("Orthography", "Spelling", "es_orthography_punctuation_spelling", 0.9),
    ("Other mistake", "Unclassified mistake", "es_other_unclassified", 1.0),
]

SKILL_SEED_IT: list[tuple[str, str, str]] = [
    ("it_articles_gender", "Nouns: Gendered Articles", "Nouns"),
    ("it_nouns_plural", "Nouns: Pluralization", "Nouns"),
    ("it_agreement_gender_number", "Agreement: Gender/Number", "Nouns"),
    ("it_partitive_articles", "Nouns: Partitive Articles", "Nouns"),
    ("it_pronouns_direct_indirect", "Pronouns: Direct/Indirect", "Pronouns"),
    ("it_clitics_placement", "Pronouns: Clitic Placement", "Pronouns"),
    ("it_reflexive_si", "Pronouns: Reflexive si", "Pronouns"),
    ("it_ci_ne", "Pronouns: ci/ne", "Pronouns"),
    ("it_conjugation_general", "Verbs: Conjugation", "Verbs"),
    ("it_aux_essere_avere", "Verbs: essere vs avere", "Verbs"),
    ("it_modals", "Verbs: Modal Verbs", "Verbs"),
    ("it_imperatives", "Verbs: Imperatives", "Verbs"),
    ("it_tense_presente", "Tense: Presente", "Tenses"),
    ("it_tense_passato_prossimo", "Tense: Passato Prossimo", "Tenses"),
    ("it_tense_imperfetto", "Tense: Imperfetto", "Tenses"),
    ("it_tense_trapassato", "Tense: Trapassato Prossimo", "Tenses"),
    ("it_tense_futuro", "Tense: Futuro", "Tenses"),
    ("it_tense_condizionale", "Tense: Condizionale", "Tenses"),
    ("it_congiuntivo_present", "Mood: Congiuntivo Present", "Moods"),
    ("it_congiuntivo_past", "Mood: Congiuntivo Past", "Moods"),
    ("it_congiuntivo_selection", "Mood: Indicative vs Congiuntivo", "Moods"),
    ("it_prepositions_articulated", "Prepositions: Articulated", "Prepositions"),
    ("it_prepositions_usage_general", "Prepositions: Usage", "Prepositions"),
    ("it_word_order_questions", "Word Order: Questions", "Word Order"),
    ("it_negation", "Word Order: Negation", "Word Order"),
    ("it_double_pronouns", "Word Order: Double Pronouns", "Word Order"),
    ("it_adj_adv_comparison", "Adjectives/Adverbs: Comparison", "Adjectives/Adverbs"),
    ("it_orthography_accents_spelling", "Orthography: Accents/Spelling", "Orthography"),
    ("it_other_unclassified", "Other: Unclassified", "Other"),
]

ERROR_SKILL_MAP_SEED_IT: list[tuple[str, str, str, float]] = [
    ("Nouns", "Gendered Articles", "it_articles_gender", 1.2),
    ("Nouns", "Pluralization", "it_nouns_plural", 1.0),
    ("Nouns", "Agreement (gender/number)", "it_agreement_gender_number", 1.2),
    ("Nouns", "Partitive (del, della)", "it_partitive_articles", 1.1),
    ("Pronouns", "Direct/Indirect (lo/la/gli/le)", "it_pronouns_direct_indirect", 1.2),
    ("Pronouns", "Clitic Placement", "it_clitics_placement", 1.2),
    ("Pronouns", "Reflexive (si)", "it_reflexive_si", 1.0),
    ("Pronouns", "Ci/Ne", "it_ci_ne", 1.1),
    ("Verbs", "Conjugation", "it_conjugation_general", 1.1),
    ("Verbs", "Essere vs Avere (aux)", "it_aux_essere_avere", 1.3),
    ("Verbs", "Modal Verbs", "it_modals", 1.0),
    ("Verbs", "Imperatives", "it_imperatives", 1.0),
    ("Tenses", "Presente", "it_tense_presente", 0.8),
    ("Tenses", "Passato Prossimo", "it_tense_passato_prossimo", 1.1),
    ("Tenses", "Imperfetto", "it_tense_imperfetto", 1.1),
    ("Tenses", "Trapassato Prossimo", "it_tense_trapassato", 1.1),
    ("Tenses", "Futuro", "it_tense_futuro", 0.9),
    ("Tenses", "Condizionale", "it_tense_condizionale", 1.0),
    ("Moods", "Congiuntivo (Present)", "it_congiuntivo_present", 1.2),
    ("Moods", "Congiuntivo (Past)", "it_congiuntivo_past", 1.2),
    ("Moods", "Indicative vs Congiuntivo", "it_congiuntivo_selection", 1.3),
    ("Prepositions", "Articulated (nel, sul)", "it_prepositions_articulated", 1.1),
    ("Prepositions", "Preposition Usage", "it_prepositions_usage_general", 1.1),
    ("Word Order", "Questions", "it_word_order_questions", 1.0),
    ("Word Order", "Negation", "it_negation", 0.9),
    ("Word Order", "Double pronouns", "it_double_pronouns", 1.2),
    ("Adjectives/Adverbs", "Comparative/Superlative", "it_adj_adv_comparison", 0.9),
    ("Orthography", "Accents", "it_orthography_accents_spelling", 1.0),
    ("Orthography", "Spelling", "it_orthography_accents_spelling", 1.0),
    ("Other mistake", "Unclassified mistake", "it_other_unclassified", 1.0),
]

SKILL_SEED: list[tuple[str, str, str, str]] = (
    [(skill_id, title, category, "de") for skill_id, title, category in SKILL_SEED_DE]
    + [(skill_id, title, category, "en") for skill_id, title, category in SKILL_SEED_EN]
    + [(skill_id, title, category, "es") for skill_id, title, category in SKILL_SEED_ES]
    + [(skill_id, title, category, "it") for skill_id, title, category in SKILL_SEED_IT]
)

ERROR_SKILL_MAP_SEED: list[tuple[str, str, str, str, float]] = (
    [("de", cat, subcat, skill_id, weight) for cat, subcat, skill_id, weight in ERROR_SKILL_MAP_SEED_DE]
    + [("en", cat, subcat, skill_id, weight) for cat, subcat, skill_id, weight in ERROR_SKILL_MAP_SEED_EN]
    + [("es", cat, subcat, skill_id, weight) for cat, subcat, skill_id, weight in ERROR_SKILL_MAP_SEED_ES]
    + [("it", cat, subcat, skill_id, weight) for cat, subcat, skill_id, weight in ERROR_SKILL_MAP_SEED_IT]
)

# Добавим проверку, чтобы сразу видеть ошибку в логах, если адреса нет
if not DATABASE_URL:
    print("❌ ОШИБКА: DATABASE_URL_RAILWAY не найден в .env или переменных окружения!")
else:
    # Для безопасности печатаем только хост, скрывая пароль
    print(f"✅ database.py успешно загрузил URL (хост: {DATABASE_URL.split('@')[-1].split(':')[0]})")

@contextmanager
def get_db_connection_context(): #
    conn = None
    last_error = None
    for attempt in range(1, DB_CONNECT_RETRIES + 1):
        try:
            conn = psycopg2.connect(
                DATABASE_URL,
                sslmode='require',
                connect_timeout=DB_CONNECT_TIMEOUT_SECONDS,
                keepalives=1,
                keepalives_idle=30,
                keepalives_interval=10,
                keepalives_count=5,
            )
            break
        except OperationalError as exc:
            last_error = exc
            if attempt >= DB_CONNECT_RETRIES:
                raise
            time.sleep(DB_CONNECT_RETRY_DELAY_SECONDS * attempt)
    if conn is None and last_error is not None:
        raise last_error
    try:
        yield conn #
        conn.commit() #
    finally:
        conn.close() #


def _build_language_pair_filter(
    source_lang: str | None,
    target_lang: str | None,
    *,
    table_alias: str | None = None,
) -> tuple[str, list]:
    if not source_lang or not target_lang:
        return "", []
    alias_prefix = f"{table_alias}." if table_alias else ""
    source_expr = (
        "LOWER(COALESCE("
        f"NULLIF({alias_prefix}source_lang, ''), "
        f"NULLIF({alias_prefix}response_json->>'source_lang', ''), "
        f"NULLIF({alias_prefix}response_json#>>'{{language_pair,source_lang}}', ''), "
        "'ru'"
        "))"
    )
    target_expr = (
        "LOWER(COALESCE("
        f"NULLIF({alias_prefix}target_lang, ''), "
        f"NULLIF({alias_prefix}response_json->>'target_lang', ''), "
        f"NULLIF({alias_prefix}response_json#>>'{{language_pair,target_lang}}', ''), "
        "'de'"
        "))"
    )
    clause = f" AND {source_expr} = %s AND {target_expr} = %s"
    return clause, [str(source_lang).lower(), str(target_lang).lower()]

def init_db(): #
    with get_db_connection_context() as conn: #
        with conn.cursor() as cursor: 
            # 1. Таблица для клиентов
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS clients (
                    id SERIAL PRIMARY KEY,
                    first_name TEXT NOT NULL,
                    last_name TEXT,
                    system_id TEXT UNIQUE, -- Уникальный ID клиента в системе (если есть)
                    phone_number TEXT UNIQUE, -- Телефон клиента
                    email TEXT UNIQUE,
                    location TEXT, -- Город или регион клиента
                    manager_contact TEXT, -- Контакты ответственного менеджера
                    is_existing_client BOOLEAN DEFAULT FALSE, -- Признак, работает ли клиент с нами
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            print("✅ Таблица 'clients' проверена/создана.")

            # 2. Таблица для продуктов/услуг
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS products (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL UNIQUE,
                    description TEXT,
                    price DECIMAL(10, 2) NOT NULL, -- Цена продукта, 10 цифр всего, 2 после запятой
                    is_new BOOLEAN DEFAULT FALSE, -- Признак новинки
                    available_quantity INT DEFAULT 0, -- Доступное количество на складе
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            print("✅ Таблица 'products' проверена/создана.")

            # 3. Таблица для заказов
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS orders (
                    id SERIAL PRIMARY KEY,
                    client_id INT REFERENCES clients(id), -- Внешний ключ на клиента
                    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    status TEXT DEFAULT 'pending', -- Статус заказа (pending, completed, cancelled)
                    total_amount DECIMAL(10, 2), -- Общая сумма заказа
                    order_details JSONB, -- Подробности заказа в JSON-формате (например, {"product_id": 1, "quantity": 2})
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            print("✅ Таблица 'orders' проверена/создана.")

            # Пример: Добавление базовых продуктов (для тестирования)
            # Внимание: для реального использования, эти данные должны управляться через CRM/API
            products_to_insert = [
                ("LapTop ZenBook Pro", "The powerful Laptop for professionals, 16GB RAM, 1TB SSD", 1500.00, True, 100),
                ("Smartphone UltraVision 2000", "Top smartphone with AI-camera and super detailed night mode", 999.99, False, 250),
                ("Monitor ErgoView", "Energy saving 27 inch monitor with full HD", 450.50, False, 50),
                ("Whireless earphones AirPods", "Earpods with noice cancellation and 30 hours autonomous working time", 120.00, True, 300)
            ]
            for name, description, price, is_new, quantity in products_to_insert:
                cursor.execute("""
                    INSERT INTO products (name, description, price, is_new, available_quantity)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (name) DO UPDATE SET
                        description = EXCLUDED.description, -- специальное ключевое слово в PostgreSQL. Оно ссылается на значение, которое было бы вставлено, если бы конфликта не произошло. То есть, это значение description, которое вы пытались вставить в этой конкретной INSERT операции.
                        price = EXCLUDED.price,
                        is_new = EXCLUDED.is_new,
                        available_quantity = EXCLUDED.available_quantity;
                """, (name, description, price, is_new, quantity))
            print("✅ Базовые продукты вставлены/обновлены.")

    print("✅ Инициализация базы данных завершена.")


def ensure_webapp_tables() -> None:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS assistants (
                    task_name TEXT PRIMARY KEY,
                    assistant_id TEXT NOT NULL
                );
            """)
            # Backward compatibility: if legacy table "assistant" exists,
            # migrate rows into "assistants".
            cursor.execute("""
                DO $$
                BEGIN
                    IF to_regclass('public.assistant') IS NOT NULL THEN
                        INSERT INTO assistants (task_name, assistant_id)
                        SELECT task_name, assistant_id
                        FROM assistant
                        ON CONFLICT (task_name) DO UPDATE
                        SET assistant_id = EXCLUDED.assistant_id;
                    END IF;
                END $$;
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_allowed_users (
                    user_id BIGINT PRIMARY KEY,
                    username TEXT,
                    added_by BIGINT,
                    note TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_allowed_users_updated
                ON bt_3_allowed_users (updated_at);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_user_language_profile (
                    user_id BIGINT PRIMARY KEY,
                    learning_language TEXT NOT NULL DEFAULT 'de',
                    native_language TEXT NOT NULL DEFAULT 'ru',
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            cursor.execute(
                """
                DO $$
                DECLARE c RECORD;
                BEGIN
                    -- Keep language profile columns as plain TEXT for flexible app-level validation.
                    BEGIN
                        ALTER TABLE bt_3_user_language_profile
                        ALTER COLUMN learning_language TYPE TEXT
                        USING learning_language::text;
                    EXCEPTION WHEN undefined_column THEN
                        NULL;
                    END;
                    BEGIN
                        ALTER TABLE bt_3_user_language_profile
                        ALTER COLUMN native_language TYPE TEXT
                        USING native_language::text;
                    EXCEPTION WHEN undefined_column THEN
                        NULL;
                    END;

                    -- Drop legacy CHECK constraints that may enforce old regex patterns.
                    FOR c IN
                        SELECT conname
                        FROM pg_constraint
                        WHERE conrelid = 'public.bt_3_user_language_profile'::regclass
                          AND contype = 'c'
                    LOOP
                        EXECUTE format(
                            'ALTER TABLE public.bt_3_user_language_profile DROP CONSTRAINT IF EXISTS %I',
                            c.conname
                        );
                    END LOOP;
                END $$;
                """
            )
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_user_language_profile_updated
                ON bt_3_user_language_profile (updated_at DESC);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_webapp_scope_state (
                    user_id BIGINT PRIMARY KEY,
                    scope_kind TEXT NOT NULL DEFAULT 'personal',
                    scope_chat_id BIGINT,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    CHECK (scope_kind IN ('personal', 'group')),
                    CHECK (
                        (scope_kind = 'personal' AND scope_chat_id IS NULL)
                        OR
                        (scope_kind = 'group' AND scope_chat_id IS NOT NULL)
                    )
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_webapp_scope_state_updated
                ON bt_3_webapp_scope_state (updated_at DESC);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_webapp_group_contexts (
                    user_id BIGINT NOT NULL,
                    chat_id BIGINT NOT NULL,
                    chat_type TEXT NOT NULL DEFAULT 'group',
                    chat_title TEXT,
                    participation_confirmed BOOLEAN NOT NULL DEFAULT FALSE,
                    participation_confirmed_at TIMESTAMPTZ,
                    participation_confirmed_source TEXT,
                    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (user_id, chat_id),
                    CHECK (chat_type IN ('group', 'supergroup'))
                );
            """)
            cursor.execute("""
                ALTER TABLE bt_3_webapp_group_contexts
                ADD COLUMN IF NOT EXISTS participation_confirmed BOOLEAN NOT NULL DEFAULT FALSE;
            """)
            cursor.execute("""
                ALTER TABLE bt_3_webapp_group_contexts
                ADD COLUMN IF NOT EXISTS participation_confirmed_at TIMESTAMPTZ;
            """)
            cursor.execute("""
                ALTER TABLE bt_3_webapp_group_contexts
                ADD COLUMN IF NOT EXISTS participation_confirmed_source TEXT;
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_webapp_group_contexts_user_seen
                ON bt_3_webapp_group_contexts (user_id, last_seen_at DESC);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_webapp_group_contexts_chat_confirmed
                ON bt_3_webapp_group_contexts (chat_id, participation_confirmed, last_seen_at DESC);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_webapp_checks (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    username TEXT,
                    session_id TEXT,
                    original_text TEXT NOT NULL,
                    user_translation TEXT NOT NULL,
                    result TEXT NOT NULL,
                    source_lang TEXT,
                    target_lang TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            cursor.execute("""
                ALTER TABLE bt_3_webapp_checks
                ADD COLUMN IF NOT EXISTS source_lang TEXT;
            """)
            cursor.execute("""
                ALTER TABLE bt_3_webapp_checks
                ADD COLUMN IF NOT EXISTS target_lang TEXT;
            """)
            cursor.execute("CREATE SEQUENCE IF NOT EXISTS bt_3_webapp_checks_id_seq;")
            cursor.execute("""
                SELECT setval(
                    'bt_3_webapp_checks_id_seq',
                    COALESCE((SELECT MAX(id) FROM bt_3_webapp_checks), 1),
                    true
                );
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_translation_check_sessions (
                    id BIGSERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    username TEXT,
                    source_session_id TEXT,
                    source_lang TEXT,
                    target_lang TEXT,
                    status TEXT NOT NULL DEFAULT 'queued',
                    total_items INT NOT NULL DEFAULT 0,
                    completed_items INT NOT NULL DEFAULT 0,
                    failed_items INT NOT NULL DEFAULT 0,
                    send_private_grammar_text BOOLEAN NOT NULL DEFAULT FALSE,
                    original_text_bundle TEXT,
                    user_translation_bundle TEXT,
                    last_error TEXT,
                    started_at TIMESTAMPTZ,
                    finished_at TIMESTAMPTZ,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    CHECK (status IN ('queued', 'running', 'done', 'failed', 'canceled')),
                    CHECK (total_items >= 0),
                    CHECK (completed_items >= 0),
                    CHECK (failed_items >= 0)
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_translation_check_sessions_user_created
                ON bt_3_translation_check_sessions (user_id, created_at DESC);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_translation_check_sessions_status_created
                ON bt_3_translation_check_sessions (status, created_at DESC);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_translation_check_items (
                    id BIGSERIAL PRIMARY KEY,
                    check_session_id BIGINT NOT NULL REFERENCES bt_3_translation_check_sessions(id) ON DELETE CASCADE,
                    item_order INT NOT NULL DEFAULT 0,
                    sentence_number INT,
                    sentence_id_for_mistake_table BIGINT,
                    original_text TEXT NOT NULL,
                    user_translation TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    result_json JSONB,
                    result_text TEXT,
                    error_text TEXT,
                    webapp_check_id BIGINT,
                    started_at TIMESTAMPTZ,
                    finished_at TIMESTAMPTZ,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    CHECK (status IN ('pending', 'running', 'done', 'failed'))
                );
            """)
            cursor.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS uq_bt_3_translation_check_items_session_order
                ON bt_3_translation_check_items (check_session_id, item_order);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_translation_check_items_status
                ON bt_3_translation_check_items (check_session_id, status, item_order);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_webapp_dictionary_queries (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    word_ru TEXT,
                    translation_de TEXT,
                    word_de TEXT,
                    translation_ru TEXT,
                    source_lang TEXT,
                    target_lang TEXT,
                    origin_process TEXT NOT NULL DEFAULT 'unknown',
                    origin_meta JSONB,
                    response_json JSONB,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            cursor.execute("""
                ALTER TABLE bt_3_webapp_dictionary_queries
                ADD COLUMN IF NOT EXISTS folder_id BIGINT;
            """)
            cursor.execute("""
                ALTER TABLE bt_3_webapp_dictionary_queries
                ALTER COLUMN word_ru DROP NOT NULL;
            """)
            cursor.execute("""
                ALTER TABLE bt_3_webapp_dictionary_queries
                ADD COLUMN IF NOT EXISTS word_de TEXT;
            """)
            cursor.execute("""
                ALTER TABLE bt_3_webapp_dictionary_queries
                ADD COLUMN IF NOT EXISTS translation_ru TEXT;
            """)
            cursor.execute("""
                ALTER TABLE bt_3_webapp_dictionary_queries
                ADD COLUMN IF NOT EXISTS source_lang TEXT;
            """)
            cursor.execute("""
                ALTER TABLE bt_3_webapp_dictionary_queries
                ADD COLUMN IF NOT EXISTS target_lang TEXT;
            """)
            cursor.execute("""
                ALTER TABLE bt_3_webapp_dictionary_queries
                ADD COLUMN IF NOT EXISTS origin_process TEXT;
            """)
            cursor.execute("""
                ALTER TABLE bt_3_webapp_dictionary_queries
                ADD COLUMN IF NOT EXISTS origin_meta JSONB;
            """)
            cursor.execute("""
                ALTER TABLE bt_3_webapp_dictionary_queries
                ALTER COLUMN origin_process SET DEFAULT 'unknown';
            """)
            cursor.execute("""
                UPDATE bt_3_webapp_dictionary_queries
                SET origin_process = 'unknown_legacy'
                WHERE origin_process IS NULL OR origin_process = '';
            """)
            cursor.execute("""
                ALTER TABLE bt_3_webapp_dictionary_queries
                ALTER COLUMN origin_process SET NOT NULL;
            """)
            cursor.execute("""
                ALTER TABLE bt_3_webapp_dictionary_queries
                ADD COLUMN IF NOT EXISTS is_learned BOOLEAN NOT NULL DEFAULT FALSE;
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_dictionary_folders (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    name TEXT NOT NULL,
                    color TEXT NOT NULL,
                    icon TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_dictionary_folders_user
                ON bt_3_dictionary_folders (user_id);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_webapp_dictionary_queries_user_folder
                ON bt_3_webapp_dictionary_queries (user_id, folder_id);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_webapp_dictionary_queries_user_pair_created
                ON bt_3_webapp_dictionary_queries (user_id, source_lang, target_lang, created_at DESC);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_webapp_dictionary_queries_user_origin_created
                ON bt_3_webapp_dictionary_queries (user_id, origin_process, created_at DESC);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_schema_migrations (
                    migration_key TEXT PRIMARY KEY,
                    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_skills (
                    skill_id TEXT PRIMARY KEY,
                    language_code TEXT NOT NULL DEFAULT 'de',
                    title TEXT NOT NULL,
                    category TEXT NOT NULL,
                    is_active BOOLEAN NOT NULL DEFAULT TRUE,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
            """)
            cursor.execute("""
                ALTER TABLE bt_3_skills
                ADD COLUMN IF NOT EXISTS language_code TEXT;
            """)
            cursor.execute("""
                UPDATE bt_3_skills
                SET language_code = COALESCE(NULLIF(language_code, ''), 'de')
                WHERE language_code IS NULL OR language_code = '';
            """)
            cursor.execute("""
                ALTER TABLE bt_3_skills
                ALTER COLUMN language_code SET DEFAULT 'de';
            """)
            cursor.execute("""
                ALTER TABLE bt_3_skills
                ALTER COLUMN language_code SET NOT NULL;
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_skills_category
                ON bt_3_skills (language_code, category, skill_id);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_error_skill_map (
                    id BIGSERIAL PRIMARY KEY,
                    language_code TEXT NOT NULL DEFAULT 'de',
                    error_category TEXT NOT NULL,
                    error_subcategory TEXT NOT NULL,
                    skill_id TEXT NOT NULL REFERENCES bt_3_skills(skill_id) ON DELETE CASCADE,
                    weight DOUBLE PRECISION NOT NULL DEFAULT 1.0,
                    notes TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (error_category, error_subcategory, skill_id)
                );
            """)
            cursor.execute("""
                ALTER TABLE bt_3_error_skill_map
                ADD COLUMN IF NOT EXISTS language_code TEXT;
            """)
            cursor.execute("""
                UPDATE bt_3_error_skill_map
                SET language_code = COALESCE(NULLIF(language_code, ''), 'de')
                WHERE language_code IS NULL OR language_code = '';
            """)
            cursor.execute("""
                ALTER TABLE bt_3_error_skill_map
                ALTER COLUMN language_code SET DEFAULT 'de';
            """)
            cursor.execute("""
                ALTER TABLE bt_3_error_skill_map
                ALTER COLUMN language_code SET NOT NULL;
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_error_skill_map_err
                ON bt_3_error_skill_map (language_code, error_category, error_subcategory);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_error_skill_map_skill
                ON bt_3_error_skill_map (language_code, skill_id);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_user_skill_state (
                    user_id BIGINT NOT NULL,
                    skill_id TEXT NOT NULL REFERENCES bt_3_skills(skill_id) ON DELETE CASCADE,
                    source_lang TEXT NOT NULL DEFAULT 'ru',
                    target_lang TEXT NOT NULL DEFAULT 'de',
                    mastery DOUBLE PRECISION NOT NULL DEFAULT 50.0,
                    success_streak INTEGER NOT NULL DEFAULT 0,
                    fail_streak INTEGER NOT NULL DEFAULT 0,
                    total_events INTEGER NOT NULL DEFAULT 0,
                    last_event_delta DOUBLE PRECISION NOT NULL DEFAULT 0.0,
                    last_event_at TIMESTAMPTZ,
                    last_practiced_at TIMESTAMPTZ,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (user_id, skill_id, source_lang, target_lang)
                );
            """)
            cursor.execute("""
                ALTER TABLE bt_3_user_skill_state
                ADD COLUMN IF NOT EXISTS source_lang TEXT;
            """)
            cursor.execute("""
                ALTER TABLE bt_3_user_skill_state
                ADD COLUMN IF NOT EXISTS target_lang TEXT;
            """)
            cursor.execute("""
                UPDATE bt_3_user_skill_state
                SET
                    source_lang = COALESCE(NULLIF(source_lang, ''), 'ru'),
                    target_lang = COALESCE(NULLIF(target_lang, ''), 'de')
                WHERE source_lang IS NULL
                   OR target_lang IS NULL
                   OR source_lang = ''
                   OR target_lang = '';
            """)
            cursor.execute("""
                ALTER TABLE bt_3_user_skill_state
                ALTER COLUMN source_lang SET DEFAULT 'ru';
            """)
            cursor.execute("""
                ALTER TABLE bt_3_user_skill_state
                ALTER COLUMN target_lang SET DEFAULT 'de';
            """)
            cursor.execute("""
                ALTER TABLE bt_3_user_skill_state
                ALTER COLUMN source_lang SET NOT NULL;
            """)
            cursor.execute("""
                ALTER TABLE bt_3_user_skill_state
                ALTER COLUMN target_lang SET NOT NULL;
            """)
            cursor.execute(
                """
                DO $$
                BEGIN
                    BEGIN
                        ALTER TABLE bt_3_user_skill_state
                        DROP CONSTRAINT IF EXISTS bt_3_user_skill_state_pkey;
                    EXCEPTION WHEN undefined_object THEN
                        NULL;
                    END;
                    BEGIN
                        ALTER TABLE bt_3_user_skill_state
                        ADD CONSTRAINT bt_3_user_skill_state_pkey
                        PRIMARY KEY (user_id, skill_id, source_lang, target_lang);
                    EXCEPTION
                        WHEN duplicate_object THEN NULL;
                    END;
                END $$;
                """
            )
            # Repair incomplete language metadata for dictionary rows inserted by legacy flows.
            cursor.execute(
                """
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1
                        FROM bt_3_schema_migrations
                        WHERE migration_key = '2026_02_25_dictionary_lang_metadata_repair'
                    ) THEN
                        UPDATE bt_3_webapp_dictionary_queries
                        SET
                            source_lang = COALESCE(
                                NULLIF(source_lang, ''),
                                NULLIF(response_json->>'source_lang', ''),
                                NULLIF(response_json#>>'{language_pair,source_lang}', ''),
                                'ru'
                            ),
                            target_lang = COALESCE(
                                NULLIF(target_lang, ''),
                                NULLIF(response_json->>'target_lang', ''),
                                NULLIF(response_json#>>'{language_pair,target_lang}', ''),
                                'de'
                            )
                        WHERE source_lang IS NULL
                           OR target_lang IS NULL
                           OR source_lang = ''
                           OR target_lang = '';

                        UPDATE bt_3_webapp_dictionary_queries
                        SET response_json = jsonb_set(
                            jsonb_set(
                                COALESCE(response_json, '{}'::jsonb),
                                '{source_lang}',
                                to_jsonb(COALESCE(NULLIF(source_lang, ''), 'ru')::text),
                                true
                            ),
                            '{target_lang}',
                            to_jsonb(COALESCE(NULLIF(target_lang, ''), 'de')::text),
                            true
                        )
                        WHERE response_json IS NULL
                           OR COALESCE(response_json->>'source_lang', '') = ''
                           OR COALESCE(response_json->>'target_lang', '') = '';

                        UPDATE bt_3_webapp_dictionary_queries
                        SET response_json = jsonb_set(
                            jsonb_set(
                                COALESCE(response_json, '{}'::jsonb),
                                '{language_pair,source_lang}',
                                to_jsonb(COALESCE(NULLIF(source_lang, ''), 'ru')::text),
                                true
                            ),
                            '{language_pair,target_lang}',
                            to_jsonb(COALESCE(NULLIF(target_lang, ''), 'de')::text),
                            true
                        )
                        WHERE response_json IS NULL
                           OR COALESCE(response_json#>>'{language_pair,source_lang}', '') = ''
                           OR COALESCE(response_json#>>'{language_pair,target_lang}', '') = '';

                        INSERT INTO bt_3_schema_migrations (migration_key)
                        VALUES ('2026_02_25_dictionary_lang_metadata_repair')
                        ON CONFLICT (migration_key) DO NOTHING;
                    END IF;
                END $$;
                """
            )
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_user_skill_state_user_mastery
                ON bt_3_user_skill_state (user_id, source_lang, target_lang, mastery ASC);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_user_skill_state_skill
                ON bt_3_user_skill_state (skill_id, source_lang, target_lang, mastery ASC);
            """)
            cursor.executemany(
                """
                INSERT INTO bt_3_skills (skill_id, title, category, language_code, is_active, updated_at)
                VALUES (%s, %s, %s, %s, TRUE, NOW())
                ON CONFLICT (skill_id) DO UPDATE
                SET
                    title = EXCLUDED.title,
                    category = EXCLUDED.category,
                    language_code = EXCLUDED.language_code,
                    updated_at = NOW();
                """,
                SKILL_SEED,
            )
            cursor.executemany(
                """
                INSERT INTO bt_3_error_skill_map (
                    language_code,
                    error_category,
                    error_subcategory,
                    skill_id,
                    weight,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, NOW())
                ON CONFLICT (error_category, error_subcategory, skill_id) DO UPDATE
                SET
                    language_code = EXCLUDED.language_code,
                    weight = EXCLUDED.weight,
                    updated_at = NOW();
                """,
                ERROR_SKILL_MAP_SEED,
            )
            # One-time backfill for legacy imported dictionary rows (pre-multilang).
            # Must run only once, not on every startup.
            cursor.execute(
                """
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1
                        FROM bt_3_schema_migrations
                        WHERE migration_key = '2026_02_19_legacy_dictionary_lang_backfill_once'
                    ) THEN
                        UPDATE bt_3_webapp_dictionary_queries
                        SET
                            source_lang = COALESCE(NULLIF(source_lang, ''), 'ru'),
                            target_lang = COALESCE(NULLIF(target_lang, ''), 'de')
                        WHERE (source_lang IS NULL OR source_lang = '')
                          AND (target_lang IS NULL OR target_lang = '');

                        UPDATE bt_3_webapp_dictionary_queries
                        SET response_json = jsonb_set(
                            jsonb_set(
                                COALESCE(response_json, '{}'::jsonb),
                                '{source_lang}',
                                to_jsonb(COALESCE(NULLIF(source_lang, ''), 'ru')::text),
                                true
                            ),
                            '{target_lang}',
                            to_jsonb(COALESCE(NULLIF(target_lang, ''), 'de')::text),
                            true
                        )
                        WHERE response_json IS NULL
                           OR COALESCE(response_json->>'source_lang', '') = ''
                           OR COALESCE(response_json->>'target_lang', '') = '';

                        UPDATE bt_3_webapp_dictionary_queries
                        SET response_json = jsonb_set(
                            jsonb_set(
                                COALESCE(response_json, '{}'::jsonb),
                                '{language_pair,source_lang}',
                                to_jsonb(COALESCE(NULLIF(source_lang, ''), 'ru')::text),
                                true
                            ),
                            '{language_pair,target_lang}',
                            to_jsonb(COALESCE(NULLIF(target_lang, ''), 'de')::text),
                            true
                        )
                        WHERE response_json IS NULL
                           OR COALESCE(response_json#>>'{language_pair,source_lang}', '') = ''
                           OR COALESCE(response_json#>>'{language_pair,target_lang}', '') = '';

                        INSERT INTO bt_3_schema_migrations (migration_key)
                        VALUES ('2026_02_19_legacy_dictionary_lang_backfill_once')
                        ON CONFLICT (migration_key) DO NOTHING;
                    END IF;
                END $$;
                """
            )
            # One-time repair for DE->RU dictionary rows created via private bot saves
            # where legacy RU/DE columns could be swapped.
            cursor.execute(
                """
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1
                        FROM bt_3_schema_migrations
                        WHERE migration_key = '2026_02_23_dictionary_de_ru_legacy_fix_once'
                    ) THEN
                        WITH fixed AS (
                            SELECT
                                id,
                                COALESCE(
                                    NULLIF(response_json->>'word_source', ''),
                                    NULLIF(response_json->>'source_text', ''),
                                    NULLIF(word_de, ''),
                                    NULLIF(translation_de, ''),
                                    NULLIF(word_ru, '')
                                ) AS fixed_word_de,
                                COALESCE(
                                    NULLIF(response_json->>'word_target', ''),
                                    NULLIF(response_json->>'target_text', ''),
                                    NULLIF(translation_ru, ''),
                                    NULLIF(word_ru, ''),
                                    NULLIF(translation_de, '')
                                ) AS fixed_translation_ru
                            FROM bt_3_webapp_dictionary_queries
                            WHERE COALESCE(source_lang, '') = 'de'
                              AND COALESCE(target_lang, '') = 'ru'
                        )
                        UPDATE bt_3_webapp_dictionary_queries q
                        SET
                            word_de = COALESCE(f.fixed_word_de, q.word_de),
                            translation_ru = COALESCE(f.fixed_translation_ru, q.translation_ru),
                            word_ru = COALESCE(f.fixed_translation_ru, q.word_ru),
                            translation_de = COALESCE(f.fixed_word_de, q.translation_de),
                            response_json = jsonb_set(
                                jsonb_set(
                                    jsonb_set(
                                        jsonb_set(
                                            COALESCE(q.response_json, '{}'::jsonb),
                                            '{word_de}',
                                            to_jsonb(COALESCE(f.fixed_word_de, q.word_de, '')::text),
                                            true
                                        ),
                                        '{translation_ru}',
                                        to_jsonb(COALESCE(f.fixed_translation_ru, q.translation_ru, '')::text),
                                        true
                                    ),
                                    '{word_ru}',
                                    to_jsonb(COALESCE(f.fixed_translation_ru, q.word_ru, '')::text),
                                    true
                                ),
                                '{translation_de}',
                                to_jsonb(COALESCE(f.fixed_word_de, q.translation_de, '')::text),
                                true
                            )
                        FROM fixed f
                        WHERE q.id = f.id;

                        INSERT INTO bt_3_schema_migrations (migration_key)
                        VALUES ('2026_02_23_dictionary_de_ru_legacy_fix_once')
                        ON CONFLICT (migration_key) DO NOTHING;
                    END IF;
                END $$;
                """
            )
            cursor.execute(
                """
                DO $$
                BEGIN
                    IF to_regclass('public.bt_3_translations') IS NOT NULL THEN
                        ALTER TABLE bt_3_translations ADD COLUMN IF NOT EXISTS source_lang TEXT;
                        ALTER TABLE bt_3_translations ADD COLUMN IF NOT EXISTS target_lang TEXT;
                        ALTER TABLE bt_3_translations ADD COLUMN IF NOT EXISTS audio_grammar_opt_in BOOLEAN DEFAULT FALSE;
                        UPDATE bt_3_translations
                        SET audio_grammar_opt_in = FALSE
                        WHERE audio_grammar_opt_in IS NULL;
                        ALTER TABLE bt_3_translations ALTER COLUMN audio_grammar_opt_in SET DEFAULT FALSE;
                        ALTER TABLE bt_3_translations ALTER COLUMN audio_grammar_opt_in SET NOT NULL;
                        CREATE INDEX IF NOT EXISTS idx_bt_3_translations_user_lang_ts
                        ON bt_3_translations (user_id, source_lang, target_lang, timestamp DESC);
                        CREATE INDEX IF NOT EXISTS idx_bt_3_translations_user_audio_grammar
                        ON bt_3_translations (user_id, audio_grammar_opt_in, timestamp DESC);
                    END IF;
                END $$;
                """
            )
            cursor.execute(
                """
                DO $$
                BEGIN
                    IF to_regclass('public.bt_3_daily_sentences') IS NOT NULL THEN
                        ALTER TABLE bt_3_daily_sentences ADD COLUMN IF NOT EXISTS source_lang TEXT;
                        ALTER TABLE bt_3_daily_sentences ADD COLUMN IF NOT EXISTS target_lang TEXT;
                        CREATE INDEX IF NOT EXISTS idx_bt_3_daily_sentences_user_date_lang
                        ON bt_3_daily_sentences (user_id, date, source_lang, target_lang);
                    END IF;
                END $$;
                """
            )
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_quiz_history (
                    id SERIAL PRIMARY KEY,
                    word_ru TEXT NOT NULL,
                    asked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_quiz_history_word_time
                ON bt_3_quiz_history (word_ru, asked_at);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_dictionary_cache (
                    word_ru TEXT PRIMARY KEY,
                    response_json JSONB NOT NULL,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_youtube_transcripts (
                    video_id TEXT PRIMARY KEY,
                    items JSONB NOT NULL,
                    language TEXT,
                    is_generated BOOLEAN,
                    translations JSONB,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            cursor.execute("""
                ALTER TABLE bt_3_youtube_transcripts
                ADD COLUMN IF NOT EXISTS translations JSONB;
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_youtube_transcripts_updated
                ON bt_3_youtube_transcripts (updated_at);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_flashcard_stats (
                    user_id BIGINT NOT NULL,
                    entry_id BIGINT NOT NULL,
                    correct_count INT DEFAULT 0,
                    wrong_count INT DEFAULT 0,
                    last_result BOOLEAN,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (user_id, entry_id)
                );
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_flashcard_seen (
                    user_id BIGINT NOT NULL,
                    entry_id BIGINT NOT NULL,
                    seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (user_id, entry_id, seen_at)
                );
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_flashcard_feel_feedback_queue (
                    token TEXT PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    entry_id BIGINT NOT NULL,
                    feel_explanation TEXT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    consumed_at TIMESTAMPTZ,
                    feedback_action TEXT
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_flashcard_feel_feedback_queue_user_pending
                ON bt_3_flashcard_feel_feedback_queue (user_id, consumed_at, created_at DESC);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_telegram_system_messages (
                    id BIGSERIAL PRIMARY KEY,
                    chat_id BIGINT NOT NULL,
                    message_id BIGINT NOT NULL,
                    message_type TEXT NOT NULL DEFAULT 'text',
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    deleted_at TIMESTAMPTZ,
                    delete_error TEXT
                );
            """)
            cursor.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_bt_3_telegram_sysmsg_chat_msg
                ON bt_3_telegram_system_messages (chat_id, message_id);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_telegram_sysmsg_pending
                ON bt_3_telegram_system_messages (deleted_at, created_at);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_admin_scheduler_runs (
                    id BIGSERIAL PRIMARY KEY,
                    job_key TEXT NOT NULL,
                    run_period TEXT NOT NULL,
                    target_chat_id BIGINT NOT NULL,
                    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
            """)
            cursor.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_bt_3_admin_scheduler_runs_unique
                ON bt_3_admin_scheduler_runs (job_key, run_period, target_chat_id);
            """)
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS bt_3_support_messages (
                    id BIGSERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    from_role TEXT NOT NULL,
                    message_text TEXT NOT NULL,
                    admin_telegram_id BIGINT,
                    telegram_chat_id BIGINT,
                    telegram_message_id BIGINT,
                    reply_to_id BIGINT,
                    is_read_by_user BOOLEAN NOT NULL DEFAULT FALSE,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_bt_3_support_messages_user_created
                ON bt_3_support_messages (user_id, created_at ASC);
                """
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_bt_3_support_messages_unread
                ON bt_3_support_messages (user_id, is_read_by_user, from_role, created_at DESC);
                """
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_bt_3_support_messages_telegram_ref
                ON bt_3_support_messages (telegram_chat_id, telegram_message_id);
                """
            )
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_card_srs_state (
                    id BIGSERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    card_id BIGINT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'new',
                    due_at TIMESTAMPTZ NOT NULL,
                    last_review_at TIMESTAMPTZ,
                    interval_days INTEGER NOT NULL DEFAULT 0,
                    reps INTEGER NOT NULL DEFAULT 0,
                    lapses INTEGER NOT NULL DEFAULT 0,
                    stability DOUBLE PRECISION NOT NULL DEFAULT 0,
                    difficulty DOUBLE PRECISION NOT NULL DEFAULT 0,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
            """)
            cursor.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS uq_bt_3_card_srs_state_user_card
                ON bt_3_card_srs_state (user_id, card_id);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_card_srs_state_user_due
                ON bt_3_card_srs_state (user_id, due_at);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_card_srs_state_user_created
                ON bt_3_card_srs_state (user_id, created_at);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_card_srs_state_user_status
                ON bt_3_card_srs_state (user_id, status);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_card_review_log (
                    id BIGSERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    card_id BIGINT NOT NULL,
                    reviewed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    rating SMALLINT NOT NULL,
                    response_ms INTEGER,
                    scheduled_due_before TIMESTAMPTZ,
                    scheduled_due_after TIMESTAMPTZ,
                    stability_before DOUBLE PRECISION,
                    difficulty_before DOUBLE PRECISION,
                    stability_after DOUBLE PRECISION,
                    difficulty_after DOUBLE PRECISION,
                    interval_days_after INTEGER
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_card_review_log_user_reviewed_desc
                ON bt_3_card_review_log (user_id, reviewed_at DESC);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_card_review_log_user_card_reviewed_desc
                ON bt_3_card_review_log (user_id, card_id, reviewed_at DESC);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_daily_plans (
                    id BIGSERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    plan_date DATE NOT NULL,
                    total_minutes INTEGER NOT NULL DEFAULT 0,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (user_id, plan_date)
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_daily_plans_user_date
                ON bt_3_daily_plans (user_id, plan_date DESC);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_weekly_goals (
                    id BIGSERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    source_lang TEXT NOT NULL DEFAULT 'ru',
                    target_lang TEXT NOT NULL DEFAULT 'de',
                    week_start DATE NOT NULL,
                    translations_goal INTEGER NOT NULL DEFAULT 0,
                    learned_words_goal INTEGER NOT NULL DEFAULT 0,
                    agent_minutes_goal INTEGER NOT NULL DEFAULT 0,
                    reading_minutes_goal INTEGER NOT NULL DEFAULT 0,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (user_id, source_lang, target_lang, week_start)
                );
            """)
            cursor.execute("""
                ALTER TABLE bt_3_weekly_goals
                ADD COLUMN IF NOT EXISTS agent_minutes_goal INTEGER NOT NULL DEFAULT 0;
            """)
            cursor.execute("""
                ALTER TABLE bt_3_weekly_goals
                ADD COLUMN IF NOT EXISTS reading_minutes_goal INTEGER NOT NULL DEFAULT 0;
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_weekly_goals_user_week
                ON bt_3_weekly_goals (user_id, source_lang, target_lang, week_start DESC);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_agent_voice_sessions (
                    id BIGSERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    source_lang TEXT NOT NULL DEFAULT 'ru',
                    target_lang TEXT NOT NULL DEFAULT 'de',
                    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    ended_at TIMESTAMPTZ,
                    duration_seconds INTEGER,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_reader_sessions (
                    id BIGSERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    source_lang TEXT NOT NULL DEFAULT 'ru',
                    target_lang TEXT NOT NULL DEFAULT 'de',
                    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    ended_at TIMESTAMPTZ,
                    duration_seconds INTEGER,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_reader_sessions_user_active
                ON bt_3_reader_sessions (user_id, source_lang, target_lang, started_at DESC)
                WHERE ended_at IS NULL;
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_reader_sessions_user_range
                ON bt_3_reader_sessions (user_id, source_lang, target_lang, started_at, ended_at);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_reader_library (
                    id BIGSERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    source_lang TEXT NOT NULL DEFAULT 'ru',
                    target_lang TEXT NOT NULL DEFAULT 'de',
                    title TEXT NOT NULL DEFAULT 'Untitled',
                    source_type TEXT NOT NULL DEFAULT 'text',
                    source_url TEXT,
                    text_hash TEXT NOT NULL,
                    content_text TEXT NOT NULL,
                    content_pages JSONB NOT NULL DEFAULT '[]'::jsonb,
                    total_chars INTEGER NOT NULL DEFAULT 0,
                    progress_percent DOUBLE PRECISION NOT NULL DEFAULT 0,
                    bookmark_percent DOUBLE PRECISION NOT NULL DEFAULT 0,
                    reading_mode TEXT NOT NULL DEFAULT 'vertical',
                    is_archived BOOLEAN NOT NULL DEFAULT FALSE,
                    archived_at TIMESTAMPTZ,
                    last_opened_at TIMESTAMPTZ,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (user_id, source_lang, target_lang, text_hash)
                );
            """)
            cursor.execute("""
                ALTER TABLE bt_3_reader_library
                ADD COLUMN IF NOT EXISTS is_archived BOOLEAN NOT NULL DEFAULT FALSE;
            """)
            cursor.execute("""
                ALTER TABLE bt_3_reader_library
                ADD COLUMN IF NOT EXISTS archived_at TIMESTAMPTZ;
            """)
            cursor.execute("""
                ALTER TABLE bt_3_reader_library
                ADD COLUMN IF NOT EXISTS content_pages JSONB NOT NULL DEFAULT '[]'::jsonb;
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_reader_library_user_pair
                ON bt_3_reader_library (user_id, source_lang, target_lang, updated_at DESC);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_agent_voice_sessions_user_active
                ON bt_3_agent_voice_sessions (user_id, source_lang, target_lang, started_at DESC)
                WHERE ended_at IS NULL;
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_agent_voice_sessions_user_range
                ON bt_3_agent_voice_sessions (user_id, source_lang, target_lang, started_at, ended_at);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_daily_plan_items (
                    id BIGSERIAL PRIMARY KEY,
                    plan_id BIGINT NOT NULL REFERENCES bt_3_daily_plans(id) ON DELETE CASCADE,
                    order_index INTEGER NOT NULL DEFAULT 0,
                    task_type TEXT NOT NULL,
                    title TEXT NOT NULL,
                    estimated_minutes INTEGER NOT NULL DEFAULT 0,
                    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
                    status TEXT NOT NULL DEFAULT 'todo',
                    completed_at TIMESTAMPTZ
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_daily_plan_items_plan
                ON bt_3_daily_plan_items (plan_id, order_index);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_daily_plan_items_status
                ON bt_3_daily_plan_items (status);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_video_recommendations (
                    id BIGSERIAL PRIMARY KEY,
                    source_lang TEXT NOT NULL DEFAULT 'ru',
                    target_lang TEXT NOT NULL DEFAULT 'de',
                    focus_key TEXT NOT NULL,
                    skill_id TEXT,
                    main_category TEXT,
                    sub_category TEXT,
                    search_query TEXT,
                    video_id TEXT NOT NULL,
                    video_url TEXT,
                    video_title TEXT,
                    like_count INTEGER NOT NULL DEFAULT 0,
                    dislike_count INTEGER NOT NULL DEFAULT 0,
                    score INTEGER NOT NULL DEFAULT 0,
                    is_active BOOLEAN NOT NULL DEFAULT TRUE,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    last_selected_at TIMESTAMPTZ
                );
            """)
            cursor.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS uq_bt_3_video_recommendations_focus_video
                ON bt_3_video_recommendations (focus_key, video_id);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_video_recommendations_focus_active
                ON bt_3_video_recommendations (focus_key, is_active, score DESC, updated_at DESC);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_video_recommendation_votes (
                    id BIGSERIAL PRIMARY KEY,
                    recommendation_id BIGINT NOT NULL REFERENCES bt_3_video_recommendations(id) ON DELETE CASCADE,
                    user_id BIGINT NOT NULL,
                    vote SMALLINT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (recommendation_id, user_id)
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_video_recommendation_votes_rec
                ON bt_3_video_recommendation_votes (recommendation_id, updated_at DESC);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_today_reminder_settings (
                    user_id BIGINT PRIMARY KEY,
                    enabled BOOLEAN NOT NULL DEFAULT FALSE,
                    timezone TEXT NOT NULL DEFAULT 'Europe/Vienna',
                    reminder_hour SMALLINT NOT NULL DEFAULT 7,
                    reminder_minute SMALLINT NOT NULL DEFAULT 0,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_today_reminder_settings_enabled
                ON bt_3_today_reminder_settings (enabled, updated_at DESC);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_audio_grammar_settings (
                    user_id BIGINT PRIMARY KEY,
                    enabled BOOLEAN NOT NULL DEFAULT FALSE,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_audio_grammar_settings_enabled
                ON bt_3_audio_grammar_settings (enabled, updated_at DESC);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_youtube_proxy_subtitles_access (
                    user_id BIGINT PRIMARY KEY,
                    enabled BOOLEAN NOT NULL DEFAULT TRUE,
                    granted_by BIGINT,
                    note TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_youtube_proxy_subtitles_access_enabled
                ON bt_3_youtube_proxy_subtitles_access (enabled, updated_at DESC);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_today_regenerate_limits (
                    user_id BIGINT NOT NULL,
                    limit_date DATE NOT NULL,
                    consumed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (user_id, limit_date)
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_today_regenerate_limits_date
                ON bt_3_today_regenerate_limits (limit_date, consumed_at DESC);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_default_topics (
                    id BIGSERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    target_language TEXT NOT NULL,
                    topic_name TEXT NOT NULL,
                    error_category TEXT,
                    error_subcategory TEXT,
                    skill_id TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_default_topics_user_lang
                ON bt_3_default_topics (user_id, target_language, created_at DESC);
            """)
            cursor.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS uq_bt_3_default_topics_user_lang_topic
                ON bt_3_default_topics (user_id, target_language, topic_name);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_billing_price_snapshots (
                    id BIGSERIAL PRIMARY KEY,
                    provider TEXT NOT NULL,
                    sku TEXT NOT NULL,
                    unit TEXT NOT NULL,
                    price_per_unit NUMERIC(20, 10) NOT NULL DEFAULT 0,
                    currency TEXT NOT NULL DEFAULT 'USD',
                    valid_from TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    source TEXT NOT NULL DEFAULT 'manual',
                    raw_payload JSONB,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    CHECK (price_per_unit >= 0)
                );
            """)
            cursor.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS uq_bt_3_billing_price_snapshots_provider_sku_unit_from
                ON bt_3_billing_price_snapshots (provider, sku, unit, valid_from);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_billing_price_snapshots_lookup
                ON bt_3_billing_price_snapshots (provider, sku, unit, currency, valid_from DESC);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_billing_events (
                    id BIGSERIAL PRIMARY KEY,
                    idempotency_key TEXT NOT NULL,
                    user_id BIGINT,
                    source_lang TEXT,
                    target_lang TEXT,
                    action_type TEXT NOT NULL,
                    provider TEXT NOT NULL,
                    units_type TEXT NOT NULL,
                    units_value DOUBLE PRECISION NOT NULL DEFAULT 0,
                    price_snapshot_id BIGINT REFERENCES bt_3_billing_price_snapshots(id) ON DELETE SET NULL,
                    cost_amount NUMERIC(20, 10) NOT NULL DEFAULT 0,
                    currency TEXT NOT NULL DEFAULT 'USD',
                    status TEXT NOT NULL DEFAULT 'estimated',
                    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
                    event_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    CHECK (units_value >= 0),
                    CHECK (cost_amount >= 0),
                    CHECK (status IN ('estimated', 'final'))
                );
            """)
            cursor.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS uq_bt_3_billing_events_idempotency
                ON bt_3_billing_events (idempotency_key);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_billing_events_user_time
                ON bt_3_billing_events (user_id, event_time DESC);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_billing_events_action_provider
                ON bt_3_billing_events (action_type, provider, event_time DESC);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_billing_events_currency_time
                ON bt_3_billing_events (currency, event_time DESC);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_billing_fixed_costs (
                    id BIGSERIAL PRIMARY KEY,
                    category TEXT NOT NULL,
                    provider TEXT NOT NULL DEFAULT 'infra',
                    amount NUMERIC(20, 10) NOT NULL DEFAULT 0,
                    currency TEXT NOT NULL DEFAULT 'USD',
                    period_start DATE NOT NULL,
                    period_end DATE NOT NULL,
                    allocation_method_default TEXT NOT NULL DEFAULT 'equal',
                    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    CHECK (amount >= 0),
                    CHECK (period_end >= period_start),
                    CHECK (allocation_method_default IN ('equal', 'weighted'))
                );
            """)
            cursor.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS uq_bt_3_billing_fixed_costs_period
                ON bt_3_billing_fixed_costs (category, provider, period_start, period_end, currency);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_billing_fixed_costs_range
                ON bt_3_billing_fixed_costs (currency, period_start, period_end);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_provider_budget_controls (
                    provider TEXT NOT NULL,
                    period_month DATE NOT NULL,
                    base_limit_units BIGINT NOT NULL DEFAULT 0,
                    extra_limit_units BIGINT NOT NULL DEFAULT 0,
                    is_blocked BOOLEAN NOT NULL DEFAULT FALSE,
                    block_reason TEXT,
                    notified_thresholds JSONB NOT NULL DEFAULT '{}'::jsonb,
                    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    CHECK (base_limit_units >= 0),
                    CHECK (extra_limit_units >= 0),
                    PRIMARY KEY (provider, period_month)
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_provider_budget_controls_period
                ON bt_3_provider_budget_controls (period_month DESC, provider);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS plans (
                    plan_code TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    is_paid BOOLEAN NOT NULL DEFAULT FALSE,
                    stripe_price_id TEXT,
                    daily_cost_cap_eur NUMERIC(20, 10),
                    trial_days INT NOT NULL DEFAULT 0,
                    is_active BOOLEAN NOT NULL DEFAULT TRUE,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    CHECK (trial_days >= 0),
                    CHECK (daily_cost_cap_eur IS NULL OR daily_cost_cap_eur >= 0)
                );
            """)
            cursor.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS uq_plans_stripe_price_id
                ON plans (stripe_price_id)
                WHERE stripe_price_id IS NOT NULL AND stripe_price_id <> '';
            """)
            # Trial policy stays global (TRIAL_DAYS / TRIAL_DAILY_COST_CAP_EUR), not per plan row.
            seed_trial_daily_cap = _env_decimal("TRIAL_DAILY_COST_CAP_EUR", "1.00")
            seed_free_daily_cap = _env_decimal("FREE_DAILY_COST_CAP_EUR", "0.50")
            seed_pro_daily_cap = _env_decimal("PRO_DAILY_COST_CAP_EUR", None)
            seed_pro_price_id = str(os.getenv("STRIPE_PRICE_ID_PRO", "")).strip() or None
            seed_support_coffee_price_id = str(os.getenv("STRIPE_PRICE_ID_SUPPORT_COFFEE", "")).strip() or None
            seed_support_cheesecake_price_id = str(os.getenv("STRIPE_PRICE_ID_SUPPORT_CHEESECAKE", "")).strip() or None
            if not seed_pro_price_id:
                print("⚠️ billing config warning: STRIPE_PRICE_ID_PRO is empty, legacy pro plan will be seeded without stripe_price_id")
            if not seed_support_coffee_price_id:
                print("⚠️ billing config warning: STRIPE_PRICE_ID_SUPPORT_COFFEE is empty, support_coffee plan will be seeded without stripe_price_id")
            if not seed_support_cheesecake_price_id:
                print("⚠️ billing config warning: STRIPE_PRICE_ID_SUPPORT_CHEESECAKE is empty, support_cheesecake plan will be seeded without stripe_price_id")
            if (
                seed_free_daily_cap is not None
                and seed_trial_daily_cap is not None
                and seed_free_daily_cap > seed_trial_daily_cap
            ):
                print(
                    "⚠️ billing config warning: FREE_DAILY_COST_CAP_EUR is greater than TRIAL_DAILY_COST_CAP_EUR"
                )
            cursor.executemany(
                """
                INSERT INTO plans (
                    plan_code,
                    name,
                    is_paid,
                    stripe_price_id,
                    daily_cost_cap_eur,
                    trial_days,
                    is_active,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, TRUE, NOW())
                ON CONFLICT (plan_code) DO UPDATE
                SET
                    name = EXCLUDED.name,
                    is_paid = EXCLUDED.is_paid,
                    stripe_price_id = EXCLUDED.stripe_price_id,
                    daily_cost_cap_eur = EXCLUDED.daily_cost_cap_eur,
                    trial_days = EXCLUDED.trial_days,
                    is_active = TRUE,
                    updated_at = NOW();
                """,
                [
                    (
                        "free",
                        "Free",
                        False,
                        None,
                        seed_free_daily_cap,
                        0,
                    ),
                    (
                        "pro",
                        "Pro",
                        True,
                        seed_pro_price_id,
                        seed_pro_daily_cap,
                        0,
                    ),
                    (
                        "support_coffee",
                        "Поддержать разработчика: кофе ☕️",
                        True,
                        seed_support_coffee_price_id,
                        seed_pro_daily_cap,
                        0,
                    ),
                    (
                        "support_cheesecake",
                        "Поддержать разработчика: кофе ☕️ и чизкейк 🍰",
                        True,
                        seed_support_cheesecake_price_id,
                        seed_pro_daily_cap,
                        0,
                    ),
                ],
            )
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS user_subscriptions (
                    user_id BIGINT UNIQUE NOT NULL,
                    plan_code TEXT NOT NULL REFERENCES plans(plan_code),
                    status TEXT NOT NULL,
                    trial_ends_at TIMESTAMPTZ,
                    current_period_end TIMESTAMPTZ,
                    stripe_customer_id TEXT,
                    stripe_subscription_id TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    CHECK (status IN ('active', 'inactive', 'past_due', 'canceled', 'trialing'))
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_user_subscriptions_plan_status
                ON user_subscriptions (plan_code, status);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_user_subscriptions_customer
                ON user_subscriptions (stripe_customer_id)
                WHERE stripe_customer_id IS NOT NULL AND stripe_customer_id <> '';
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_user_subscriptions_subscription
                ON user_subscriptions (stripe_subscription_id)
                WHERE stripe_subscription_id IS NOT NULL AND stripe_subscription_id <> '';
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS stripe_events_processed (
                    event_id TEXT PRIMARY KEY,
                    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS plan_limits (
                    plan_code TEXT NOT NULL REFERENCES plans(plan_code),
                    feature_code TEXT NOT NULL,
                    limit_value NUMERIC(20, 10) NOT NULL,
                    limit_unit TEXT NOT NULL,
                    period TEXT NOT NULL,
                    is_active BOOLEAN NOT NULL DEFAULT TRUE,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    CHECK (limit_value >= 0),
                    CHECK (limit_unit IN ('count', 'seconds', 'chars', 'tokens', 'eur')),
                    CHECK (period IN ('day')),
                    UNIQUE (plan_code, feature_code, period)
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_plan_limits_lookup
                ON plan_limits (plan_code, is_active, period);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS daily_cost_rollup (
                    user_id BIGINT NOT NULL,
                    day DATE NOT NULL,
                    currency TEXT NOT NULL DEFAULT 'EUR',
                    total_cost_eur NUMERIC(20, 10) NOT NULL DEFAULT 0,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    CHECK (total_cost_eur >= 0),
                    UNIQUE (user_id, day)
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_daily_cost_rollup_user_day
                ON daily_cost_rollup (user_id, day DESC);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_tts_chunk_cache (
                    cache_key TEXT PRIMARY KEY,
                    language TEXT NOT NULL,
                    source_text TEXT NOT NULL,
                    chunks JSONB NOT NULL,
                    hit_count BIGINT NOT NULL DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_tts_chunk_cache_updated
                ON bt_3_tts_chunk_cache (updated_at);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_tts_audio_cache (
                    cache_key TEXT PRIMARY KEY,
                    language TEXT NOT NULL,
                    voice TEXT NOT NULL,
                    speed DOUBLE PRECISION NOT NULL,
                    source_text TEXT NOT NULL,
                    audio_mp3 BYTEA NOT NULL,
                    hit_count BIGINT NOT NULL DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_tts_audio_cache_updated
                ON bt_3_tts_audio_cache (updated_at);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_tts_object_cache (
                    cache_key TEXT PRIMARY KEY,
                    status TEXT NOT NULL DEFAULT 'pending',
                    language TEXT,
                    voice TEXT,
                    speed DOUBLE PRECISION,
                    source_text TEXT,
                    object_key TEXT,
                    url TEXT,
                    size_bytes BIGINT,
                    error_code TEXT,
                    error_msg TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    last_hit_at TIMESTAMPTZ,
                    CHECK (status IN ('pending', 'ready', 'failed'))
                );
            """)
            # Backward-compatible migration path: if table existed with older shape,
            # add required columns lazily without destructive schema operations.
            cursor.execute("""
                ALTER TABLE bt_3_tts_object_cache
                ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'pending';
            """)
            cursor.execute("""
                ALTER TABLE bt_3_tts_object_cache
                ADD COLUMN IF NOT EXISTS object_key TEXT;
            """)
            cursor.execute("""
                ALTER TABLE bt_3_tts_object_cache
                ADD COLUMN IF NOT EXISTS url TEXT;
            """)
            cursor.execute("""
                ALTER TABLE bt_3_tts_object_cache
                ADD COLUMN IF NOT EXISTS size_bytes BIGINT;
            """)
            cursor.execute("""
                ALTER TABLE bt_3_tts_object_cache
                ADD COLUMN IF NOT EXISTS error_code TEXT;
            """)
            cursor.execute("""
                ALTER TABLE bt_3_tts_object_cache
                ADD COLUMN IF NOT EXISTS error_msg TEXT;
            """)
            cursor.execute("""
                ALTER TABLE bt_3_tts_object_cache
                ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
            """)
            cursor.execute("""
                ALTER TABLE bt_3_tts_object_cache
                ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
            """)
            cursor.execute("""
                ALTER TABLE bt_3_tts_object_cache
                ADD COLUMN IF NOT EXISTS last_hit_at TIMESTAMPTZ;
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_tts_object_cache_status_updated
                ON bt_3_tts_object_cache (status, updated_at DESC);
            """)
            cursor.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS uq_bt_3_tts_object_cache_object_key
                ON bt_3_tts_object_cache (object_key)
                WHERE object_key IS NOT NULL;
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_tts_object_cache_last_hit
                ON bt_3_tts_object_cache (last_hit_at DESC);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_story_bank (
                    id SERIAL PRIMARY KEY,
                    title TEXT,
                    answer TEXT NOT NULL,
                    answer_aliases JSONB,
                    extra_de TEXT NOT NULL,
                    story_type TEXT,
                    difficulty TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_story_sentences (
                    id SERIAL PRIMARY KEY,
                    story_id INT NOT NULL REFERENCES bt_3_story_bank(id) ON DELETE CASCADE,
                    sentence_index INT NOT NULL,
                    sentence TEXT NOT NULL
                );
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_story_sessions (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    session_id TEXT NOT NULL,
                    story_id INT NOT NULL REFERENCES bt_3_story_bank(id),
                    mode TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    completed_at TIMESTAMP,
                    guess TEXT,
                    guess_correct BOOLEAN,
                    score INT,
                    feedback TEXT
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_story_sessions_user
                ON bt_3_story_sessions (user_id);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_story_sessions_session
                ON bt_3_story_sessions (session_id);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_story_bank_type
                ON bt_3_story_bank (story_type, difficulty);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_active_quizzes (
                    poll_id TEXT PRIMARY KEY,
                    chat_id BIGINT NOT NULL,
                    message_id BIGINT,
                    correct_option_id INTEGER NOT NULL,
                    correct_text TEXT,
                    options JSONB NOT NULL DEFAULT '[]'::jsonb,
                    freeform_option TEXT,
                    quiz_type TEXT,
                    word_ru TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_active_quizzes_created_at
                ON bt_3_active_quizzes (created_at DESC);
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_access_requests (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    username TEXT,
                    status TEXT NOT NULL DEFAULT 'pending',
                    requested_via TEXT NOT NULL DEFAULT 'bot',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    reviewed_at TIMESTAMP,
                    reviewed_by BIGINT,
                    review_note TEXT
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_access_requests_user_time
                ON bt_3_access_requests (user_id, created_at DESC);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_access_requests_status
                ON bt_3_access_requests (status, created_at DESC);
            """)


def get_admin_telegram_ids() -> set[int]:
    raw_values = [
        os.getenv("BOT_ADMIN_TELEGRAM_IDS"),
        os.getenv("TELEGRAM_ADMIN_IDS"),
        os.getenv("BOT_ADMIN_TELEGRAM_ID"),
        os.getenv("TELEGRAM_ADMIN_ID"),
    ]
    merged = ",".join(v for v in raw_values if v)
    if not merged:
        return set()

    result: set[int] = set()
    for token in merged.replace(";", ",").split(","):
        value = token.strip()
        if not value:
            continue
        try:
            result.add(int(value))
        except ValueError:
            continue
    return result


def is_telegram_user_allowed(user_id: int) -> bool:
    if not user_id:
        return False
    if int(user_id) in get_admin_telegram_ids():
        return True

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT 1 FROM bt_3_allowed_users WHERE user_id = %s LIMIT 1;",
                (int(user_id),),
            )
            return cursor.fetchone() is not None


def allow_telegram_user(
    user_id: int,
    username: str | None = None,
    added_by: int | None = None,
    note: str | None = None,
) -> None:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_allowed_users (user_id, username, added_by, note)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (user_id)
                DO UPDATE SET
                    username = COALESCE(EXCLUDED.username, bt_3_allowed_users.username),
                    added_by = EXCLUDED.added_by,
                    note = COALESCE(EXCLUDED.note, bt_3_allowed_users.note),
                    updated_at = CURRENT_TIMESTAMP;
                """,
                (int(user_id), username, added_by, note),
            )


def revoke_telegram_user(user_id: int) -> bool:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "DELETE FROM bt_3_allowed_users WHERE user_id = %s;",
                (int(user_id),),
            )
            return cursor.rowcount > 0


def list_allowed_telegram_users(limit: int = 100) -> list[dict]:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT user_id, username, added_by, note, created_at, updated_at
                FROM bt_3_allowed_users
                ORDER BY updated_at DESC
                LIMIT %s;
                """,
                (max(1, min(int(limit), 500)),),
            )
            rows = cursor.fetchall()

    return [
        {
            "user_id": row[0],
            "username": row[1],
            "added_by": row[2],
            "note": row[3],
            "created_at": row[4].isoformat() if row[4] else None,
            "updated_at": row[5].isoformat() if row[5] else None,
        }
        for row in rows
    ]


def _normalize_lang_code(value: str | None) -> str:
    return str(value or "").strip().lower()


def get_user_language_profile(user_id: int) -> dict:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT learning_language, native_language, updated_at
                FROM bt_3_user_language_profile
                WHERE user_id = %s
                LIMIT 1;
                """,
                (int(user_id),),
            )
            row = cursor.fetchone()
    if not row:
        return {
            "user_id": int(user_id),
            "learning_language": DEFAULT_LEARNING_LANGUAGE,
            "native_language": DEFAULT_NATIVE_LANGUAGE,
            "updated_at": None,
            "has_profile": False,
        }
    learning_language = _normalize_lang_code(row[0]) or DEFAULT_LEARNING_LANGUAGE
    native_language = _normalize_lang_code(row[1]) or DEFAULT_NATIVE_LANGUAGE
    if learning_language not in SUPPORTED_LEARNING_LANGUAGES:
        learning_language = DEFAULT_LEARNING_LANGUAGE
    if native_language not in SUPPORTED_NATIVE_LANGUAGES:
        native_language = DEFAULT_NATIVE_LANGUAGE
    return {
        "user_id": int(user_id),
        "learning_language": learning_language,
        "native_language": native_language,
        "updated_at": row[2].isoformat() if row[2] else None,
        "has_profile": True,
    }


def upsert_user_language_profile(user_id: int, learning_language: str, native_language: str) -> dict:
    learning = _normalize_lang_code(learning_language)
    native = _normalize_lang_code(native_language)
    if learning not in SUPPORTED_LEARNING_LANGUAGES:
        raise ValueError(f"Unsupported learning language: {learning_language}")
    if native not in SUPPORTED_NATIVE_LANGUAGES:
        raise ValueError(f"Unsupported native language: {native_language}")
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_user_language_profile (user_id, learning_language, native_language, updated_at)
                VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (user_id) DO UPDATE
                SET learning_language = EXCLUDED.learning_language,
                    native_language = EXCLUDED.native_language,
                    updated_at = CURRENT_TIMESTAMP
                RETURNING learning_language, native_language, updated_at;
                """,
                (int(user_id), learning, native),
            )
            row = cursor.fetchone()
    return {
        "user_id": int(user_id),
        "learning_language": row[0],
        "native_language": row[1],
        "updated_at": row[2].isoformat() if row[2] else None,
        "has_profile": True,
    }


def _normalize_webapp_scope_kind(value: str | None) -> str:
    kind = str(value or "").strip().lower()
    if kind == "group":
        return "group"
    return "personal"


def _build_webapp_scope_key(scope_kind: str, scope_chat_id: int | None) -> str:
    if scope_kind == "group" and scope_chat_id is not None:
        return f"group:{int(scope_chat_id)}"
    return "personal"


def get_webapp_scope_state(user_id: int) -> dict:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    s.scope_kind,
                    s.scope_chat_id,
                    g.chat_title,
                    s.updated_at
                FROM bt_3_webapp_scope_state s
                LEFT JOIN bt_3_webapp_group_contexts g
                  ON g.user_id = s.user_id
                 AND g.chat_id = s.scope_chat_id
                WHERE s.user_id = %s
                LIMIT 1;
                """,
                (int(user_id),),
            )
            row = cursor.fetchone()

    if not row:
        return {
            "user_id": int(user_id),
            "scope_kind": "personal",
            "scope_chat_id": None,
            "scope_chat_title": None,
            "scope_key": "personal",
            "updated_at": None,
            "has_state": False,
        }

    scope_kind = _normalize_webapp_scope_kind(row[0])
    scope_chat_id = int(row[1]) if row[1] is not None else None
    scope_chat_title = str(row[2] or "").strip() or None
    return {
        "user_id": int(user_id),
        "scope_kind": scope_kind,
        "scope_chat_id": scope_chat_id,
        "scope_chat_title": scope_chat_title,
        "scope_key": _build_webapp_scope_key(scope_kind, scope_chat_id),
        "updated_at": row[3].isoformat() if row[3] else None,
        "has_state": True,
    }


def upsert_webapp_scope_state(
    *,
    user_id: int,
    scope_kind: str,
    scope_chat_id: int | None = None,
) -> dict:
    normalized_kind = _normalize_webapp_scope_kind(scope_kind)
    normalized_chat_id = int(scope_chat_id) if scope_chat_id is not None else None
    if normalized_kind == "group" and normalized_chat_id is None:
        raise ValueError("scope_chat_id обязателен для group scope")
    if normalized_kind == "personal":
        normalized_chat_id = None

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_webapp_scope_state (
                    user_id,
                    scope_kind,
                    scope_chat_id,
                    updated_at
                )
                VALUES (%s, %s, %s, NOW())
                ON CONFLICT (user_id) DO UPDATE
                SET
                    scope_kind = EXCLUDED.scope_kind,
                    scope_chat_id = EXCLUDED.scope_chat_id,
                    updated_at = NOW()
                RETURNING scope_kind, scope_chat_id, updated_at;
                """,
                (int(user_id), normalized_kind, normalized_chat_id),
            )
            row = cursor.fetchone()

    resolved_kind = _normalize_webapp_scope_kind(row[0] if row else normalized_kind)
    resolved_chat_id = int(row[1]) if row and row[1] is not None else None
    return {
        "user_id": int(user_id),
        "scope_kind": resolved_kind,
        "scope_chat_id": resolved_chat_id,
        "scope_key": _build_webapp_scope_key(resolved_kind, resolved_chat_id),
        "updated_at": row[2].isoformat() if row and row[2] else None,
        "has_state": True,
    }


def upsert_webapp_group_context(
    *,
    user_id: int,
    chat_id: int,
    chat_type: str | None = None,
    chat_title: str | None = None,
) -> dict:
    normalized_chat_type = str(chat_type or "").strip().lower()
    if normalized_chat_type not in {"group", "supergroup"}:
        normalized_chat_type = "group"
    normalized_chat_title = str(chat_title or "").strip() or None

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_webapp_group_contexts (
                    user_id,
                    chat_id,
                    chat_type,
                    chat_title,
                    participation_confirmed,
                    participation_confirmed_at,
                    participation_confirmed_source,
                    first_seen_at,
                    last_seen_at
                )
                VALUES (%s, %s, %s, %s, FALSE, NULL, NULL, NOW(), NOW())
                ON CONFLICT (user_id, chat_id) DO UPDATE
                SET
                    chat_type = EXCLUDED.chat_type,
                    chat_title = COALESCE(NULLIF(EXCLUDED.chat_title, ''), bt_3_webapp_group_contexts.chat_title),
                    last_seen_at = NOW()
                RETURNING
                    user_id,
                    chat_id,
                    chat_type,
                    chat_title,
                    participation_confirmed,
                    participation_confirmed_at,
                    participation_confirmed_source,
                    first_seen_at,
                    last_seen_at;
                """,
                (int(user_id), int(chat_id), normalized_chat_type, normalized_chat_title),
            )
            row = cursor.fetchone()

    return {
        "user_id": int(row[0]),
        "chat_id": int(row[1]),
        "chat_type": str(row[2] or "group"),
        "chat_title": str(row[3] or "").strip() or None,
        "participation_confirmed": bool(row[4]),
        "participation_confirmed_at": row[5].isoformat() if row[5] else None,
        "participation_confirmed_source": str(row[6] or "").strip() or None,
        "first_seen_at": row[7].isoformat() if row[7] else None,
        "last_seen_at": row[8].isoformat() if row[8] else None,
    }


def confirm_webapp_group_participation(
    *,
    user_id: int,
    chat_id: int,
    chat_type: str | None = None,
    chat_title: str | None = None,
    source: str | None = None,
) -> dict:
    normalized_chat_type = str(chat_type or "").strip().lower()
    if normalized_chat_type not in {"group", "supergroup"}:
        normalized_chat_type = "group"
    normalized_chat_title = str(chat_title or "").strip() or None
    normalized_source = str(source or "").strip() or None

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT participation_confirmed
                FROM bt_3_webapp_group_contexts
                WHERE user_id = %s AND chat_id = %s
                LIMIT 1;
                """,
                (int(user_id), int(chat_id)),
            )
            previous = cursor.fetchone()

            cursor.execute(
                """
                INSERT INTO bt_3_webapp_group_contexts (
                    user_id,
                    chat_id,
                    chat_type,
                    chat_title,
                    participation_confirmed,
                    participation_confirmed_at,
                    participation_confirmed_source,
                    first_seen_at,
                    last_seen_at
                )
                VALUES (%s, %s, %s, %s, TRUE, NOW(), %s, NOW(), NOW())
                ON CONFLICT (user_id, chat_id) DO UPDATE
                SET
                    chat_type = EXCLUDED.chat_type,
                    chat_title = COALESCE(NULLIF(EXCLUDED.chat_title, ''), bt_3_webapp_group_contexts.chat_title),
                    participation_confirmed = TRUE,
                    participation_confirmed_at = COALESCE(
                        bt_3_webapp_group_contexts.participation_confirmed_at,
                        NOW()
                    ),
                    participation_confirmed_source = COALESCE(
                        EXCLUDED.participation_confirmed_source,
                        bt_3_webapp_group_contexts.participation_confirmed_source
                    ),
                    last_seen_at = NOW()
                RETURNING
                    user_id,
                    chat_id,
                    chat_type,
                    chat_title,
                    participation_confirmed,
                    participation_confirmed_at,
                    participation_confirmed_source,
                    first_seen_at,
                    last_seen_at;
                """,
                (
                    int(user_id),
                    int(chat_id),
                    normalized_chat_type,
                    normalized_chat_title,
                    normalized_source,
                ),
            )
            row = cursor.fetchone()

    was_confirmed_before = bool(previous and previous[0])
    return {
        "user_id": int(row[0]),
        "chat_id": int(row[1]),
        "chat_type": str(row[2] or "group"),
        "chat_title": str(row[3] or "").strip() or None,
        "participation_confirmed": bool(row[4]),
        "participation_confirmed_at": row[5].isoformat() if row[5] else None,
        "participation_confirmed_source": str(row[6] or "").strip() or None,
        "first_seen_at": row[7].isoformat() if row[7] else None,
        "last_seen_at": row[8].isoformat() if row[8] else None,
        "was_confirmed_before": was_confirmed_before,
    }


def list_webapp_group_contexts(
    user_id: int,
    limit: int = 20,
    *,
    only_confirmed: bool = False,
) -> list[dict]:
    safe_limit = max(1, min(int(limit or 20), 100))
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    user_id,
                    chat_id,
                    chat_type,
                    chat_title,
                    participation_confirmed,
                    participation_confirmed_at,
                    participation_confirmed_source,
                    first_seen_at,
                    last_seen_at
                FROM bt_3_webapp_group_contexts
                WHERE user_id = %s
                  AND (%s = FALSE OR participation_confirmed = TRUE)
                ORDER BY last_seen_at DESC, chat_id DESC
                LIMIT %s;
                """,
                (int(user_id), bool(only_confirmed), safe_limit),
            )
            rows = cursor.fetchall()
    return [
        {
            "user_id": int(row[0]),
            "chat_id": int(row[1]),
            "chat_type": str(row[2] or "group"),
            "chat_title": str(row[3] or "").strip() or None,
            "participation_confirmed": bool(row[4]),
            "participation_confirmed_at": row[5].isoformat() if row[5] else None,
            "participation_confirmed_source": str(row[6] or "").strip() or None,
            "first_seen_at": row[7].isoformat() if row[7] else None,
            "last_seen_at": row[8].isoformat() if row[8] else None,
        }
        for row in rows
    ]


def list_webapp_group_member_user_ids(
    chat_id: int,
    limit: int = 2000,
    *,
    only_confirmed: bool = True,
) -> list[int]:
    safe_limit = max(1, min(int(limit or 2000), 10000))
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT user_id
                FROM bt_3_webapp_group_contexts
                WHERE chat_id = %s
                  AND (%s = FALSE OR participation_confirmed = TRUE)
                ORDER BY last_seen_at DESC, user_id DESC
                LIMIT %s;
                """,
                (int(chat_id), bool(only_confirmed), safe_limit),
            )
            rows = cursor.fetchall() or []
    seen: set[int] = set()
    result: list[int] = []
    for row in rows:
        try:
            candidate = int(row[0])
        except Exception:
            continue
        if candidate in seen:
            continue
        seen.add(candidate)
        result.append(candidate)
    return result


def create_access_request(
    user_id: int,
    username: str | None = None,
    requested_via: str = "bot",
) -> int:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_access_requests (user_id, username, status, requested_via)
                VALUES (%s, %s, 'pending', %s)
                RETURNING id;
                """,
                (int(user_id), username, requested_via),
            )
            row = cursor.fetchone()
    return int(row[0])


def get_access_request_by_id(request_id: int) -> dict | None:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT id, user_id, username, status, requested_via, created_at, reviewed_at, reviewed_by, review_note
                FROM bt_3_access_requests
                WHERE id = %s
                LIMIT 1;
                """,
                (int(request_id),),
            )
            row = cursor.fetchone()

    if not row:
        return None
    return {
        "id": row[0],
        "user_id": row[1],
        "username": row[2],
        "status": row[3],
        "requested_via": row[4],
        "created_at": row[5].isoformat() if row[5] else None,
        "reviewed_at": row[6].isoformat() if row[6] else None,
        "reviewed_by": row[7],
        "review_note": row[8],
    }


def resolve_access_request(
    request_id: int,
    status: str,
    reviewed_by: int,
    review_note: str | None = None,
) -> dict | None:
    final_status = (status or "").strip().lower()
    if final_status not in {"approved", "rejected"}:
        raise ValueError("status must be approved or rejected")

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE bt_3_access_requests
                SET
                    status = %s,
                    reviewed_at = CURRENT_TIMESTAMP,
                    reviewed_by = %s,
                    review_note = %s
                WHERE id = %s AND status = 'pending'
                RETURNING id, user_id, username, status, requested_via, created_at, reviewed_at, reviewed_by, review_note;
                """,
                (final_status, int(reviewed_by), review_note, int(request_id)),
            )
            row = cursor.fetchone()

    if not row:
        return None
    return {
        "id": row[0],
        "user_id": row[1],
        "username": row[2],
        "status": row[3],
        "requested_via": row[4],
        "created_at": row[5].isoformat() if row[5] else None,
        "reviewed_at": row[6].isoformat() if row[6] else None,
        "reviewed_by": row[7],
        "review_note": row[8],
    }


def resolve_latest_pending_access_request_for_user(
    user_id: int,
    status: str,
    reviewed_by: int,
    review_note: str | None = None,
) -> dict | None:
    final_status = (status or "").strip().lower()
    if final_status not in {"approved", "rejected"}:
        raise ValueError("status must be approved or rejected")

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                WITH target AS (
                    SELECT id
                    FROM bt_3_access_requests
                    WHERE user_id = %s AND status = 'pending'
                    ORDER BY created_at DESC
                    LIMIT 1
                )
                UPDATE bt_3_access_requests req
                SET
                    status = %s,
                    reviewed_at = CURRENT_TIMESTAMP,
                    reviewed_by = %s,
                    review_note = %s
                FROM target
                WHERE req.id = target.id
                RETURNING req.id, req.user_id, req.username, req.status, req.requested_via, req.created_at, req.reviewed_at, req.reviewed_by, req.review_note;
                """,
                (int(user_id), final_status, int(reviewed_by), review_note),
            )
            row = cursor.fetchone()

    if not row:
        return None
    return {
        "id": row[0],
        "user_id": row[1],
        "username": row[2],
        "status": row[3],
        "requested_via": row[4],
        "created_at": row[5].isoformat() if row[5] else None,
        "reviewed_at": row[6].isoformat() if row[6] else None,
        "reviewed_by": row[7],
        "review_note": row[8],
    }


def list_pending_access_requests(limit: int = 20) -> list[dict]:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT id, user_id, username, status, requested_via, created_at
                FROM bt_3_access_requests
                WHERE status = 'pending'
                ORDER BY created_at DESC
                LIMIT %s;
                """,
                (max(1, min(int(limit), 100)),),
            )
            rows = cursor.fetchall()

    return [
        {
            "id": row[0],
            "user_id": row[1],
            "username": row[2],
            "status": row[3],
            "requested_via": row[4],
            "created_at": row[5].isoformat() if row[5] else None,
        }
        for row in rows
    ]


def save_webapp_translation(
    user_id: int,
    username: str | None,
    session_id: str | None,
    original_text: str,
    user_translation: str,
    result: str,
    source_lang: str | None = None,
    target_lang: str | None = None,
) -> None:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO bt_3_webapp_checks (
                    user_id,
                    username,
                    session_id,
                    original_text,
                    user_translation,
                    result,
                    source_lang,
                    target_lang
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
            """, (
                user_id,
                username,
                session_id,
                original_text,
                user_translation,
                result,
                source_lang,
                target_lang,
            ))


def get_webapp_translation_history(user_id: int, limit: int = 20) -> list[dict]:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT
                    id,
                    session_id,
                    original_text,
                    user_translation,
                    result,
                    source_lang,
                    target_lang,
                    created_at
                FROM bt_3_webapp_checks
                WHERE user_id = %s
                ORDER BY created_at DESC
                LIMIT %s;
            """, (user_id, limit))
            rows = cursor.fetchall()
            return [
                {
                    "id": row[0],
                    "session_id": row[1],
                    "original_text": row[2],
                    "user_translation": row[3],
                    "result": row[4],
                    "source_lang": row[5],
                    "target_lang": row[6],
                    "created_at": row[7].isoformat() if row[7] else None,
                }
                for row in rows
            ]


def _map_translation_check_session_row(row) -> dict | None:
    if not row:
        return None
    return {
        "id": int(row[0]),
        "user_id": int(row[1]),
        "username": str(row[2] or "") or None,
        "source_session_id": str(row[3] or "") or None,
        "source_lang": str(row[4] or "") or None,
        "target_lang": str(row[5] or "") or None,
        "status": str(row[6] or "queued"),
        "total_items": int(row[7] or 0),
        "completed_items": int(row[8] or 0),
        "failed_items": int(row[9] or 0),
        "send_private_grammar_text": bool(row[10]),
        "original_text_bundle": str(row[11] or "") or None,
        "user_translation_bundle": str(row[12] or "") or None,
        "last_error": str(row[13] or "") or None,
        "started_at": row[14].isoformat() if row[14] else None,
        "finished_at": row[15].isoformat() if row[15] else None,
        "created_at": row[16].isoformat() if row[16] else None,
        "updated_at": row[17].isoformat() if row[17] else None,
    }


def _map_translation_check_item_row(row) -> dict | None:
    if not row:
        return None
    return {
        "id": int(row[0]),
        "check_session_id": int(row[1]),
        "item_order": int(row[2] or 0),
        "sentence_number": int(row[3]) if row[3] is not None else None,
        "sentence_id_for_mistake_table": int(row[4]) if row[4] is not None else None,
        "original_text": str(row[5] or ""),
        "user_translation": str(row[6] or ""),
        "status": str(row[7] or "pending"),
        "result_json": row[8] if isinstance(row[8], dict) else None,
        "result_text": str(row[9] or "") or None,
        "error_text": str(row[10] or "") or None,
        "webapp_check_id": int(row[11]) if row[11] is not None else None,
        "started_at": row[12].isoformat() if row[12] else None,
        "finished_at": row[13].isoformat() if row[13] else None,
        "created_at": row[14].isoformat() if row[14] else None,
        "updated_at": row[15].isoformat() if row[15] else None,
    }


def create_translation_check_session(
    *,
    user_id: int,
    username: str | None,
    source_session_id: str | None,
    source_lang: str | None,
    target_lang: str | None,
    items: list[dict],
    send_private_grammar_text: bool = False,
    original_text_bundle: str | None = None,
    user_translation_bundle: str | None = None,
) -> dict | None:
    normalized_items = items if isinstance(items, list) else []
    total_items = len(normalized_items)
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_translation_check_sessions (
                    user_id,
                    username,
                    source_session_id,
                    source_lang,
                    target_lang,
                    status,
                    total_items,
                    completed_items,
                    failed_items,
                    send_private_grammar_text,
                    original_text_bundle,
                    user_translation_bundle,
                    created_at,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, 'queued', %s, 0, 0, %s, %s, %s, NOW(), NOW())
                RETURNING
                    id, user_id, username, source_session_id, source_lang, target_lang,
                    status, total_items, completed_items, failed_items, send_private_grammar_text,
                    original_text_bundle, user_translation_bundle, last_error, started_at,
                    finished_at, created_at, updated_at;
                """,
                (
                    int(user_id),
                    str(username or "").strip() or None,
                    str(source_session_id or "").strip() or None,
                    str(source_lang or "").strip().lower() or None,
                    str(target_lang or "").strip().lower() or None,
                    max(0, int(total_items)),
                    bool(send_private_grammar_text),
                    str(original_text_bundle or "").strip() or None,
                    str(user_translation_bundle or "").strip() or None,
                ),
            )
            session_row = cursor.fetchone()
            if not session_row:
                return None
            session_id = int(session_row[0])

            for index, item in enumerate(normalized_items):
                cursor.execute(
                    """
                    INSERT INTO bt_3_translation_check_items (
                        check_session_id,
                        item_order,
                        sentence_number,
                        sentence_id_for_mistake_table,
                        original_text,
                        user_translation,
                        status,
                        created_at,
                        updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, 'pending', NOW(), NOW());
                    """,
                    (
                        session_id,
                        int(item.get("item_order", index)),
                        int(item["sentence_number"]) if item.get("sentence_number") is not None else None,
                        int(item["id_for_mistake_table"]) if item.get("id_for_mistake_table") is not None else None,
                        str(item.get("original_text") or "").strip(),
                        str(item.get("translation") or item.get("user_translation") or "").strip(),
                    ),
                )

    return get_translation_check_session(session_id=session_id, user_id=int(user_id))


def get_translation_check_session(*, session_id: int, user_id: int | None = None) -> dict | None:
    where_sql = "WHERE id = %s"
    params: list = [int(session_id)]
    if user_id is not None:
        where_sql += " AND user_id = %s"
        params.append(int(user_id))
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT
                    id, user_id, username, source_session_id, source_lang, target_lang,
                    status, total_items, completed_items, failed_items, send_private_grammar_text,
                    original_text_bundle, user_translation_bundle, last_error, started_at,
                    finished_at, created_at, updated_at
                FROM bt_3_translation_check_sessions
                {where_sql}
                LIMIT 1;
                """,
                params,
            )
            row = cursor.fetchone()
    return _map_translation_check_session_row(row)


def list_translation_check_items(*, session_id: int) -> list[dict]:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    id, check_session_id, item_order, sentence_number, sentence_id_for_mistake_table,
                    original_text, user_translation, status, result_json, result_text, error_text,
                    webapp_check_id, started_at, finished_at, created_at, updated_at
                FROM bt_3_translation_check_items
                WHERE check_session_id = %s
                ORDER BY item_order ASC, id ASC;
                """,
                (int(session_id),),
            )
            rows = cursor.fetchall() or []
    return [_map_translation_check_item_row(row) for row in rows if row]


def get_latest_translation_check_session(
    *,
    user_id: int,
    only_active: bool = False,
) -> dict | None:
    status_sql = "AND status IN ('queued', 'running')" if only_active else ""
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT
                    id, user_id, username, source_session_id, source_lang, target_lang,
                    status, total_items, completed_items, failed_items, send_private_grammar_text,
                    original_text_bundle, user_translation_bundle, last_error, started_at,
                    finished_at, created_at, updated_at
                FROM bt_3_translation_check_sessions
                WHERE user_id = %s
                  {status_sql}
                ORDER BY created_at DESC
                LIMIT 1;
                """,
                (int(user_id),),
            )
            row = cursor.fetchone()
    return _map_translation_check_session_row(row)


def update_translation_check_session_status(
    *,
    session_id: int,
    status: str,
    last_error: str | None = None,
    started: bool = False,
    finished: bool = False,
) -> dict | None:
    normalized = str(status or "").strip().lower()
    if normalized not in {"queued", "running", "done", "failed", "canceled"}:
        raise ValueError("Invalid translation check session status")
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE bt_3_translation_check_sessions
                SET
                    status = %s,
                    last_error = %s,
                    started_at = CASE WHEN %s THEN COALESCE(started_at, NOW()) ELSE started_at END,
                    finished_at = CASE WHEN %s THEN NOW() ELSE finished_at END,
                    updated_at = NOW()
                WHERE id = %s
                RETURNING
                    id, user_id, username, source_session_id, source_lang, target_lang,
                    status, total_items, completed_items, failed_items, send_private_grammar_text,
                    original_text_bundle, user_translation_bundle, last_error, started_at,
                    finished_at, created_at, updated_at;
                """,
                (
                    normalized,
                    str(last_error or "").strip() or None,
                    bool(started),
                    bool(finished),
                    int(session_id),
                ),
            )
            row = cursor.fetchone()
    return _map_translation_check_session_row(row)


def update_translation_check_item_result(
    *,
    item_id: int,
    status: str,
    result_json: dict | None = None,
    result_text: str | None = None,
    error_text: str | None = None,
    webapp_check_id: int | None = None,
    started: bool = False,
    finished: bool = False,
) -> dict | None:
    normalized = str(status or "").strip().lower()
    if normalized not in {"pending", "running", "done", "failed"}:
        raise ValueError("Invalid translation check item status")
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE bt_3_translation_check_items
                SET
                    status = %s,
                    result_json = %s,
                    result_text = %s,
                    error_text = %s,
                    webapp_check_id = %s,
                    started_at = CASE WHEN %s THEN COALESCE(started_at, NOW()) ELSE started_at END,
                    finished_at = CASE WHEN %s THEN NOW() ELSE finished_at END,
                    updated_at = NOW()
                WHERE id = %s
                RETURNING
                    id, check_session_id, item_order, sentence_number, sentence_id_for_mistake_table,
                    original_text, user_translation, status, result_json, result_text, error_text,
                    webapp_check_id, started_at, finished_at, created_at, updated_at;
                """,
                (
                    normalized,
                    Json(result_json) if isinstance(result_json, dict) else None,
                    str(result_text or "").strip() or None,
                    str(error_text or "").strip() or None,
                    int(webapp_check_id) if webapp_check_id is not None else None,
                    bool(started),
                    bool(finished),
                    int(item_id),
                ),
            )
            row = cursor.fetchone()
    return _map_translation_check_item_row(row)


def refresh_translation_check_session_counters(*, session_id: int) -> dict | None:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE bt_3_translation_check_sessions s
                SET
                    total_items = counts.total_items,
                    completed_items = counts.completed_items,
                    failed_items = counts.failed_items,
                    updated_at = NOW()
                FROM (
                    SELECT
                        check_session_id,
                        COUNT(*) AS total_items,
                        COUNT(*) FILTER (WHERE status = 'done') AS completed_items,
                        COUNT(*) FILTER (WHERE status = 'failed') AS failed_items
                    FROM bt_3_translation_check_items
                    WHERE check_session_id = %s
                    GROUP BY check_session_id
                ) AS counts
                WHERE s.id = counts.check_session_id
                RETURNING
                    s.id, s.user_id, s.username, s.source_session_id, s.source_lang, s.target_lang,
                    s.status, s.total_items, s.completed_items, s.failed_items, s.send_private_grammar_text,
                    s.original_text_bundle, s.user_translation_bundle, s.last_error, s.started_at,
                    s.finished_at, s.created_at, s.updated_at;
                """,
                (int(session_id),),
            )
            row = cursor.fetchone()
    return _map_translation_check_session_row(row)


def save_webapp_dictionary_query(
    user_id: int,
    word_ru: str | None,
    translation_de: str | None,
    word_de: str | None,
    translation_ru: str | None,
    response_json: dict,
    folder_id: int | None = None,
    source_lang: str | None = None,
    target_lang: str | None = None,
    origin_process: str | None = None,
    origin_meta: dict | None = None,
) -> None:
    normalized_origin = _normalize_dictionary_origin_process(origin_process)
    normalized_meta = _coerce_json_object(origin_meta)
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO bt_3_webapp_dictionary_queries (
                    user_id,
                    word_ru,
                    folder_id,
                    translation_de,
                    word_de,
                    translation_ru,
                    source_lang,
                    target_lang,
                    origin_process,
                    origin_meta,
                    response_json
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """, (
                user_id,
                word_ru,
                folder_id,
                translation_de,
                word_de,
                translation_ru,
                source_lang,
                target_lang,
                normalized_origin,
                json.dumps(normalized_meta, ensure_ascii=False) if normalized_meta else None,
                json.dumps(response_json, ensure_ascii=False),
            ))


def save_webapp_dictionary_query_returning_id(
    user_id: int,
    word_ru: str | None,
    translation_de: str | None,
    word_de: str | None,
    translation_ru: str | None,
    response_json: dict,
    folder_id: int | None = None,
    source_lang: str | None = None,
    target_lang: str | None = None,
    origin_process: str | None = None,
    origin_meta: dict | None = None,
) -> int:
    normalized_origin = _normalize_dictionary_origin_process(origin_process)
    normalized_meta = _coerce_json_object(origin_meta)
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_webapp_dictionary_queries (
                    user_id,
                    word_ru,
                    folder_id,
                    translation_de,
                    word_de,
                    translation_ru,
                    source_lang,
                    target_lang,
                    origin_process,
                    origin_meta,
                    response_json
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id;
                """,
                (
                    user_id,
                    word_ru,
                    folder_id,
                    translation_de,
                    word_de,
                    translation_ru,
                    source_lang,
                    target_lang,
                    normalized_origin,
                    json.dumps(normalized_meta, ensure_ascii=False) if normalized_meta else None,
                    json.dumps(response_json, ensure_ascii=False),
                ),
            )
            row = cursor.fetchone()
            return int(row[0]) if row and row[0] is not None else 0


def get_webapp_dictionary_entries(
    user_id: int,
    limit: int = 100,
    folder_mode: str = "all",
    folder_id: int | None = None,
    source_lang: str | None = None,
    target_lang: str | None = None,
) -> list[dict]:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            where_clause = "WHERE user_id = %s"
            params = [user_id]
            language_filter_sql, language_params = _build_language_pair_filter(source_lang, target_lang)
            if language_filter_sql:
                where_clause += language_filter_sql
                params.extend(language_params)
            if folder_mode == "folder" and folder_id is not None:
                where_clause += " AND folder_id = %s"
                params.append(folder_id)
            elif folder_mode == "none":
                where_clause += " AND folder_id IS NULL"
            params.append(limit)
            cursor.execute(f"""
                SELECT id, word_ru, translation_de, word_de, translation_ru, source_lang, target_lang, origin_process, origin_meta, response_json, folder_id, created_at
                FROM bt_3_webapp_dictionary_queries
                {where_clause}
                ORDER BY created_at DESC
                LIMIT %s;
            """, params)
            rows = cursor.fetchall()

    items = []
    for row in rows:
        items.append({
            "id": row[0],
            "word_ru": row[1],
            "translation_de": row[2],
            "word_de": row[3],
            "translation_ru": row[4],
            "source_lang": row[5],
            "target_lang": row[6],
            "origin_process": row[7],
            "origin_meta": row[8],
            "response_json": row[9],
            "folder_id": row[10],
            "created_at": row[11].isoformat() if row[11] else None,
        })
    return items


def has_dictionary_entries_for_language_pair(
    user_id: int,
    source_lang: str | None,
    target_lang: str | None,
    cursor=None,
) -> bool:
    def _check(cur) -> bool:
        where_clause = "WHERE user_id = %s"
        params: list = [int(user_id)]
        language_filter_sql, language_params = _build_language_pair_filter(source_lang, target_lang)
        if language_filter_sql:
            where_clause += language_filter_sql
            params.extend(language_params)
        cur.execute(
            f"""
            SELECT 1
            FROM bt_3_webapp_dictionary_queries
            {where_clause}
            LIMIT 1;
            """,
            params,
        )
        return cur.fetchone() is not None

    if cursor is not None:
        return _check(cursor)
    with get_db_connection_context() as conn:
        with conn.cursor() as own_cursor:
            return _check(own_cursor)


def get_latest_dictionary_language_pair_for_user(user_id: int, cursor=None) -> tuple[str, str] | None:
    def _get(cur):
        cur.execute(
            """
            SELECT
                LOWER(COALESCE(
                    NULLIF(source_lang, ''),
                    NULLIF(response_json->>'source_lang', ''),
                    NULLIF(response_json#>>'{language_pair,source_lang}', ''),
                    'ru'
                )) AS source_lang_value,
                LOWER(COALESCE(
                    NULLIF(target_lang, ''),
                    NULLIF(response_json->>'target_lang', ''),
                    NULLIF(response_json#>>'{language_pair,target_lang}', ''),
                    'de'
                )) AS target_lang_value
            FROM bt_3_webapp_dictionary_queries
            WHERE user_id = %s
            ORDER BY created_at DESC
            LIMIT 1;
            """,
            (int(user_id),),
        )
        row = cur.fetchone()
        if not row:
            return None
        return str(row[0] or "ru"), str(row[1] or "de")

    if cursor is not None:
        return _get(cursor)
    with get_db_connection_context() as conn:
        with conn.cursor() as own_cursor:
            return _get(own_cursor)


def get_dictionary_entries_for_tts_prewarm(
    *,
    limit: int = 200,
    lookback_hours: int = 168,
) -> list[dict]:
    safe_limit = max(1, min(int(limit), 2000))
    safe_hours = max(1, min(int(lookback_hours), 24 * 90))
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    id,
                    user_id,
                    word_ru,
                    translation_de,
                    word_de,
                    translation_ru,
                    source_lang,
                    target_lang,
                    response_json,
                    created_at
                FROM bt_3_webapp_dictionary_queries
                WHERE created_at >= NOW() - (%s || ' hours')::interval
                ORDER BY created_at DESC
                LIMIT %s;
                """,
                (safe_hours, safe_limit),
            )
            rows = cursor.fetchall()

    items: list[dict] = []
    for row in rows:
        items.append(
            {
                "id": int(row[0]),
                "user_id": int(row[1]),
                "word_ru": row[2],
                "translation_de": row[3],
                "word_de": row[4],
                "translation_ru": row[5],
                "source_lang": row[6],
                "target_lang": row[7],
                "response_json": row[8],
                "created_at": row[9].isoformat() if row[9] else None,
            }
        )
    return items


def get_recent_dictionary_user_ids(
    *,
    limit: int = 100,
    lookback_hours: int = 168,
) -> list[int]:
    safe_limit = max(1, min(int(limit), 2000))
    safe_hours = max(1, min(int(lookback_hours), 24 * 180))
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT user_id, MAX(created_at) AS last_created_at
                FROM bt_3_webapp_dictionary_queries
                WHERE created_at >= NOW() - (%s || ' hours')::interval
                GROUP BY user_id
                ORDER BY last_created_at DESC
                LIMIT %s;
                """,
                (safe_hours, safe_limit),
            )
            rows = cursor.fetchall()
    return [int(row[0]) for row in rows if row and row[0] is not None]


def get_dictionary_entry_by_id(entry_id: int) -> dict | None:
    if not entry_id:
        return None
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT
                    id,
                    word_ru,
                    translation_de,
                    word_de,
                    translation_ru,
                    source_lang,
                    target_lang,
                    origin_process,
                    origin_meta,
                    response_json,
                    folder_id,
                    created_at
                FROM bt_3_webapp_dictionary_queries
                WHERE id = %s
                LIMIT 1;
            """, (entry_id,))
            row = cursor.fetchone()
            if not row:
                return None
            return {
                "id": row[0],
                "word_ru": row[1],
                "translation_de": row[2],
                "word_de": row[3],
                "translation_ru": row[4],
                "source_lang": row[5],
                "target_lang": row[6],
                "origin_process": row[7],
                "origin_meta": row[8],
                "response_json": row[9],
                "folder_id": row[10],
                "created_at": row[11].isoformat() if row[11] else None,
            }


def get_random_dictionary_entry(
    cooldown_days: int = 5,
    source_lang: str = "ru",
    target_lang: str = "de",
) -> dict | None:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT
                    id,
                    user_id,
                    word_ru,
                    translation_de,
                    response_json,
                    source_lang,
                    target_lang
                FROM bt_3_webapp_dictionary_queries
                WHERE response_json IS NOT NULL
                  AND COALESCE(NULLIF(source_lang, ''), response_json->>'source_lang') = %s
                  AND COALESCE(NULLIF(target_lang, ''), response_json->>'target_lang') = %s
                  AND NOT EXISTS (
                      SELECT 1
                      FROM bt_3_quiz_history h
                      WHERE h.word_ru = bt_3_webapp_dictionary_queries.word_ru
                        AND h.asked_at >= NOW() - INTERVAL %s
                  )
                ORDER BY RANDOM()
                LIMIT 1;
            """, (source_lang, target_lang, f"{cooldown_days} days",))
            row = cursor.fetchone()
            if not row:
                cursor.execute("""
                    SELECT
                        id,
                        user_id,
                        word_ru,
                        translation_de,
                        response_json,
                        source_lang,
                        target_lang
                    FROM bt_3_webapp_dictionary_queries
                    WHERE response_json IS NOT NULL
                      AND COALESCE(NULLIF(source_lang, ''), response_json->>'source_lang') = %s
                      AND COALESCE(NULLIF(target_lang, ''), response_json->>'target_lang') = %s
                    ORDER BY RANDOM()
                    LIMIT 1;
                """, (source_lang, target_lang))
                row = cursor.fetchone()
                if not row:
                    return None
            return {
                "id": row[0],
                "user_id": row[1],
                "word_ru": row[2],
                "translation_de": row[3],
                "response_json": row[4],
                "source_lang": row[5],
                "target_lang": row[6],
            }


def get_random_dictionary_entry_for_quiz_type(
    quiz_type: str,
    cooldown_days: int = 5,
    source_lang: str = "ru",
    target_lang: str = "de",
) -> dict | None:
    quiz_type = (quiz_type or "").strip().lower()

    usage_examples_count_expr = (
        "CASE WHEN jsonb_typeof(response_json->'usage_examples') = 'array' "
        "THEN jsonb_array_length(response_json->'usage_examples') ELSE 0 END"
    )
    prefixes_count_expr = (
        "CASE WHEN jsonb_typeof(response_json->'prefixes') = 'array' "
        "THEN jsonb_array_length(response_json->'prefixes') ELSE 0 END"
    )
    base_word_expr = (
        "COALESCE(NULLIF(word_de, ''), NULLIF(response_json->>'word_de', ''), "
        "NULLIF(translation_de, ''), NULLIF(response_json->>'translation_de', ''))"
    )
    letters_only_len_expr = (
        f"LENGTH(REGEXP_REPLACE({base_word_expr}, '[^A-Za-zÄÖÜäöüß]', '', 'g'))"
    )

    where_by_type = {
        "word_order": f"{usage_examples_count_expr} > 0",
        "prefix": f"({prefixes_count_expr} > 0 OR {base_word_expr} IS NOT NULL)",
        "anagram": f"{letters_only_len_expr} >= 4",
        "word": "COALESCE(NULLIF(translation_de, ''), NULLIF(response_json->>'translation_de', '')) IS NOT NULL",
    }
    extra_where = where_by_type.get(quiz_type, "TRUE")

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT
                    id,
                    user_id,
                    word_ru,
                    translation_de,
                    response_json,
                    source_lang,
                    target_lang
                FROM bt_3_webapp_dictionary_queries
                WHERE response_json IS NOT NULL
                  AND COALESCE(NULLIF(source_lang, ''), response_json->>'source_lang') = %s
                  AND COALESCE(NULLIF(target_lang, ''), response_json->>'target_lang') = %s
                  AND {extra_where}
                  AND NOT EXISTS (
                      SELECT 1
                      FROM bt_3_quiz_history h
                      WHERE h.word_ru = bt_3_webapp_dictionary_queries.word_ru
                        AND h.asked_at >= NOW() - INTERVAL %s
                  )
                ORDER BY RANDOM()
                LIMIT 1;
                """,
                (source_lang, target_lang, f"{cooldown_days} days"),
            )
            row = cursor.fetchone()
            if not row:
                cursor.execute(
                    f"""
                    SELECT
                        id,
                        user_id,
                        word_ru,
                        translation_de,
                        response_json,
                        source_lang,
                        target_lang
                    FROM bt_3_webapp_dictionary_queries
                    WHERE response_json IS NOT NULL
                      AND COALESCE(NULLIF(source_lang, ''), response_json->>'source_lang') = %s
                      AND COALESCE(NULLIF(target_lang, ''), response_json->>'target_lang') = %s
                      AND {extra_where}
                    ORDER BY RANDOM()
                    LIMIT 1;
                    """,
                    (source_lang, target_lang),
                )
                row = cursor.fetchone()
                if not row:
                    return None

    return {
        "id": row[0],
        "user_id": row[1],
        "word_ru": row[2],
        "translation_de": row[3],
        "response_json": row[4],
        "source_lang": row[5],
        "target_lang": row[6],
    }


def record_quiz_word(word_ru: str) -> None:
    if not word_ru:
        return
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_quiz_history (word_ru, asked_at)
                VALUES (%s, NOW());
                """,
                (word_ru,),
            )


def update_webapp_dictionary_entry(entry_id: int, response_json: dict, translation_de: str | None = None) -> None:
    if not entry_id:
        return
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            if translation_de is not None:
                cursor.execute("""
                    UPDATE bt_3_webapp_dictionary_queries
                    SET response_json = %s,
                        translation_de = %s
                    WHERE id = %s;
                """, (
                    json.dumps(response_json, ensure_ascii=False),
                    translation_de,
                    entry_id,
                ))
            else:
                cursor.execute("""
                    UPDATE bt_3_webapp_dictionary_queries
                    SET response_json = %s
                    WHERE id = %s;
                """, (
                    json.dumps(response_json, ensure_ascii=False),
                    entry_id,
                ))


def create_flashcard_feel_feedback_token(
    *,
    user_id: int,
    entry_id: int,
    feel_explanation: str,
) -> str:
    safe_user_id = int(user_id)
    safe_entry_id = int(entry_id)
    safe_text = str(feel_explanation or "").strip()
    if not safe_text:
        raise ValueError("feel_explanation is required")
    token = uuid4().hex
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_flashcard_feel_feedback_queue (
                    token,
                    user_id,
                    entry_id,
                    feel_explanation
                )
                VALUES (%s, %s, %s, %s);
                """,
                (token, safe_user_id, safe_entry_id, safe_text),
            )
    return token


def apply_flashcard_feel_feedback(
    *,
    token: str,
    user_id: int,
    liked: bool,
) -> dict | None:
    safe_token = str(token or "").strip()
    if not safe_token:
        return None
    safe_user_id = int(user_id)
    action = "like" if bool(liked) else "dislike"
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    entry_id,
                    feel_explanation,
                    consumed_at,
                    feedback_action
                FROM bt_3_flashcard_feel_feedback_queue
                WHERE token = %s
                  AND user_id = %s
                FOR UPDATE;
                """,
                (safe_token, safe_user_id),
            )
            row = cursor.fetchone()
            if not row:
                return None
            entry_id = int(row[0] or 0)
            feel_explanation = str(row[1] or "").strip()
            consumed_at = row[2]
            previous_action = str(row[3] or "").strip().lower()
            if consumed_at is not None:
                return {
                    "ok": True,
                    "already_processed": True,
                    "action": previous_action or action,
                    "entry_id": entry_id,
                }
            if not entry_id:
                cursor.execute(
                    """
                    UPDATE bt_3_flashcard_feel_feedback_queue
                    SET consumed_at = NOW(),
                        feedback_action = %s
                    WHERE token = %s;
                    """,
                    (action, safe_token),
                )
                return {
                    "ok": True,
                    "already_processed": False,
                    "action": action,
                    "entry_id": 0,
                }

            cursor.execute(
                """
                SELECT response_json
                FROM bt_3_webapp_dictionary_queries
                WHERE id = %s
                  AND user_id = %s
                FOR UPDATE;
                """,
                (entry_id, safe_user_id),
            )
            entry_row = cursor.fetchone()
            if not entry_row:
                cursor.execute(
                    """
                    UPDATE bt_3_flashcard_feel_feedback_queue
                    SET consumed_at = NOW(),
                        feedback_action = %s
                    WHERE token = %s;
                    """,
                    (action, safe_token),
                )
                return {
                    "ok": True,
                    "already_processed": False,
                    "action": action,
                    "entry_id": entry_id,
                }

            response_json = entry_row[0]
            if isinstance(response_json, str):
                try:
                    response_json = json.loads(response_json)
                except Exception:
                    response_json = {}
            if not isinstance(response_json, dict):
                response_json = {}

            if action == "like":
                response_json["feel_explanation"] = feel_explanation
                response_json["feel_feedback"] = "like"
            else:
                response_json.pop("feel_explanation", None)
                response_json["feel_feedback"] = "dislike"

            cursor.execute(
                """
                UPDATE bt_3_webapp_dictionary_queries
                SET response_json = %s
                WHERE id = %s
                  AND user_id = %s;
                """,
                (json.dumps(response_json, ensure_ascii=False), entry_id, safe_user_id),
            )
            cursor.execute(
                """
                UPDATE bt_3_flashcard_feel_feedback_queue
                SET consumed_at = NOW(),
                    feedback_action = %s
                WHERE token = %s;
                """,
                (action, safe_token),
            )
    return {
        "ok": True,
        "already_processed": False,
        "action": action,
        "entry_id": entry_id,
    }


def get_dictionary_cache(word_ru: str) -> dict | None:
    if not word_ru:
        return None
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT response_json
                FROM bt_3_dictionary_cache
                WHERE word_ru = %s;
            """, (word_ru,))
            row = cursor.fetchone()
            if not row:
                return None
            response_json = row[0]
            if isinstance(response_json, str):
                try:
                    return json.loads(response_json)
                except json.JSONDecodeError:
                    return None
            return response_json


def upsert_dictionary_cache(word_ru: str, response_json: dict) -> None:
    if not word_ru:
        return
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO bt_3_dictionary_cache (word_ru, response_json, updated_at)
                VALUES (%s, %s, NOW())
                ON CONFLICT (word_ru) DO UPDATE
                SET response_json = EXCLUDED.response_json,
                    updated_at = NOW();
            """, (
                word_ru,
                json.dumps(response_json, ensure_ascii=False),
            ))


def get_youtube_transcript_cache(video_id: str) -> dict | None:
    if not video_id:
        return None
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT items, language, is_generated, translations, updated_at
                FROM bt_3_youtube_transcripts
                WHERE video_id = %s;
            """, (video_id,))
            row = cursor.fetchone()
            if not row:
                return None
            items, language, is_generated, translations, updated_at = row
            if isinstance(items, str):
                try:
                    items = json.loads(items)
                except Exception:
                    items = []
            if isinstance(translations, str):
                try:
                    translations = json.loads(translations)
                except Exception:
                    translations = {}
            return {
                "items": items or [],
                "language": language,
                "is_generated": is_generated,
                "translations": translations or {},
                "updated_at": updated_at,
            }


def upsert_youtube_transcript_cache(
    video_id: str,
    items: list,
    language: str | None,
    is_generated: bool | None,
    translations: dict | None = None,
) -> None:
    if not video_id:
        return
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO bt_3_youtube_transcripts (video_id, items, language, is_generated, translations, updated_at)
                VALUES (%s, %s, %s, %s, %s, NOW())
                ON CONFLICT (video_id) DO UPDATE
                SET items = EXCLUDED.items,
                    language = EXCLUDED.language,
                    is_generated = EXCLUDED.is_generated,
                    translations = COALESCE(EXCLUDED.translations, bt_3_youtube_transcripts.translations),
                    updated_at = NOW();
            """, (
                video_id,
                json.dumps(items, ensure_ascii=False),
                language,
                is_generated,
                json.dumps(translations, ensure_ascii=False) if translations is not None else None,
            ))


def purge_old_youtube_transcripts(days: int = 7) -> None:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                DELETE FROM bt_3_youtube_transcripts
                WHERE updated_at < NOW() - (%s || ' days')::interval;
            """, (days,))


def upsert_youtube_translations(video_id: str, translations: dict) -> None:
    if not video_id or not translations:
        return
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO bt_3_youtube_transcripts (video_id, items, translations, updated_at)
                VALUES (%s, '[]'::jsonb, %s, NOW())
                ON CONFLICT (video_id) DO UPDATE
                SET translations = COALESCE(bt_3_youtube_transcripts.translations, '{}'::jsonb) || EXCLUDED.translations,
                    updated_at = NOW();
            """, (video_id, json.dumps(translations, ensure_ascii=False)))


def get_tts_chunk_cache(cache_key: str) -> list[str] | None:
    if not cache_key:
        return None
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT chunks
                FROM bt_3_tts_chunk_cache
                WHERE cache_key = %s;
                """,
                (cache_key,),
            )
            row = cursor.fetchone()
            if not row:
                return None
            cursor.execute(
                """
                UPDATE bt_3_tts_chunk_cache
                SET hit_count = hit_count + 1,
                    updated_at = NOW()
                WHERE cache_key = %s;
                """,
                (cache_key,),
            )
            chunks = row[0]
            if isinstance(chunks, str):
                try:
                    chunks = json.loads(chunks)
                except Exception:
                    chunks = []
            if not isinstance(chunks, list):
                return None
            return [str(item).strip() for item in chunks if str(item).strip()]


def upsert_tts_chunk_cache(
    cache_key: str,
    language: str,
    source_text: str,
    chunks: list[str],
) -> None:
    if not cache_key or not source_text or not chunks:
        return
    normalized_chunks = [str(item).strip() for item in chunks if str(item).strip()]
    if not normalized_chunks:
        return
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_tts_chunk_cache (
                    cache_key,
                    language,
                    source_text,
                    chunks,
                    hit_count,
                    created_at,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, 1, NOW(), NOW())
                ON CONFLICT (cache_key) DO UPDATE
                SET language = EXCLUDED.language,
                    source_text = EXCLUDED.source_text,
                    chunks = EXCLUDED.chunks,
                    hit_count = bt_3_tts_chunk_cache.hit_count + 1,
                    updated_at = NOW();
                """,
                (
                    cache_key,
                    language,
                    source_text,
                    json.dumps(normalized_chunks, ensure_ascii=False),
                ),
            )


def get_tts_audio_cache(cache_key: str) -> bytes | None:
    if not cache_key:
        return None
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT audio_mp3
                FROM bt_3_tts_audio_cache
                WHERE cache_key = %s;
                """,
                (cache_key,),
            )
            row = cursor.fetchone()
            if not row:
                return None
            cursor.execute(
                """
                UPDATE bt_3_tts_audio_cache
                SET hit_count = hit_count + 1,
                    updated_at = NOW()
                WHERE cache_key = %s;
                """,
                (cache_key,),
            )
            payload = row[0]
            if payload is None:
                return None
            if isinstance(payload, memoryview):
                return payload.tobytes()
            return bytes(payload)


def upsert_tts_audio_cache(
    cache_key: str,
    language: str,
    voice: str,
    speed: float,
    source_text: str,
    audio_mp3: bytes,
) -> None:
    if not cache_key or not source_text or not audio_mp3:
        return
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_tts_audio_cache (
                    cache_key,
                    language,
                    voice,
                    speed,
                    source_text,
                    audio_mp3,
                    hit_count,
                    created_at,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, 1, NOW(), NOW())
                ON CONFLICT (cache_key) DO UPDATE
                SET language = EXCLUDED.language,
                    voice = EXCLUDED.voice,
                    speed = EXCLUDED.speed,
                    source_text = EXCLUDED.source_text,
                    audio_mp3 = EXCLUDED.audio_mp3,
                    hit_count = bt_3_tts_audio_cache.hit_count + 1,
                    updated_at = NOW();
                """,
                (
                    cache_key,
                    language,
                    voice,
                    speed,
                    source_text,
                    Binary(audio_mp3),
                ),
            )


def _map_tts_object_cache_row(row: tuple) -> dict:
    return {
        "cache_key": str(row[0] or ""),
        "status": str(row[1] or "pending"),
        "language": str(row[2] or "").strip() or None,
        "voice": str(row[3] or "").strip() or None,
        "speed": float(row[4]) if row[4] is not None else None,
        "source_text": str(row[5] or "").strip() or None,
        "object_key": str(row[6] or "").strip() or None,
        "url": str(row[7] or "").strip() or None,
        "size_bytes": int(row[8]) if row[8] is not None else None,
        "error_code": str(row[9] or "").strip() or None,
        "error_msg": str(row[10] or "").strip() or None,
        "created_at": row[11].isoformat() if row[11] else None,
        "updated_at": row[12].isoformat() if row[12] else None,
        "last_hit_at": row[13].isoformat() if row[13] else None,
    }


def get_tts_object_cache(cache_key: str, *, touch_hit: bool = True) -> dict | None:
    if not cache_key:
        return None
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    cache_key,
                    status,
                    language,
                    voice,
                    speed,
                    source_text,
                    object_key,
                    url,
                    size_bytes,
                    error_code,
                    error_msg,
                    created_at,
                    updated_at,
                    last_hit_at
                FROM bt_3_tts_object_cache
                WHERE cache_key = %s
                LIMIT 1;
                """,
                (str(cache_key),),
            )
            row = cursor.fetchone()
            if not row:
                return None
            if touch_hit:
                cursor.execute(
                    """
                    UPDATE bt_3_tts_object_cache
                    SET last_hit_at = NOW(), updated_at = NOW()
                    WHERE cache_key = %s;
                    """,
                    (str(cache_key),),
                )
    return _map_tts_object_cache_row(row)


def try_create_tts_object_cache_pending(
    *,
    cache_key: str,
    language: str | None = None,
    voice: str | None = None,
    speed: float | None = None,
    source_text: str | None = None,
    object_key: str | None = None,
) -> bool:
    if not cache_key:
        return False
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_tts_object_cache (
                    cache_key,
                    status,
                    language,
                    voice,
                    speed,
                    source_text,
                    object_key,
                    created_at,
                    updated_at
                )
                VALUES (%s, 'pending', %s, %s, %s, %s, %s, NOW(), NOW())
                ON CONFLICT (cache_key) DO NOTHING;
                """,
                (
                    str(cache_key),
                    str(language or "").strip() or None,
                    str(voice or "").strip() or None,
                    float(speed) if speed is not None else None,
                    str(source_text or "").strip() or None,
                    str(object_key or "").strip() or None,
                ),
            )
            created = cursor.rowcount > 0
    return bool(created)


def mark_tts_object_cache_ready(
    *,
    cache_key: str,
    object_key: str,
    url: str,
    size_bytes: int | None = None,
    language: str | None = None,
    voice: str | None = None,
    speed: float | None = None,
    source_text: str | None = None,
) -> dict | None:
    if not cache_key or not object_key or not url:
        return None
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_tts_object_cache (
                    cache_key,
                    status,
                    language,
                    voice,
                    speed,
                    source_text,
                    object_key,
                    url,
                    size_bytes,
                    error_code,
                    error_msg,
                    created_at,
                    updated_at,
                    last_hit_at
                )
                VALUES (%s, 'ready', %s, %s, %s, %s, %s, %s, %s, NULL, NULL, NOW(), NOW(), NOW())
                ON CONFLICT (cache_key) DO UPDATE
                SET
                    status = 'ready',
                    language = COALESCE(EXCLUDED.language, bt_3_tts_object_cache.language),
                    voice = COALESCE(EXCLUDED.voice, bt_3_tts_object_cache.voice),
                    speed = COALESCE(EXCLUDED.speed, bt_3_tts_object_cache.speed),
                    source_text = COALESCE(EXCLUDED.source_text, bt_3_tts_object_cache.source_text),
                    object_key = EXCLUDED.object_key,
                    url = EXCLUDED.url,
                    size_bytes = COALESCE(EXCLUDED.size_bytes, bt_3_tts_object_cache.size_bytes),
                    error_code = NULL,
                    error_msg = NULL,
                    updated_at = NOW(),
                    last_hit_at = NOW()
                RETURNING
                    cache_key,
                    status,
                    language,
                    voice,
                    speed,
                    source_text,
                    object_key,
                    url,
                    size_bytes,
                    error_code,
                    error_msg,
                    created_at,
                    updated_at,
                    last_hit_at;
                """,
                (
                    str(cache_key),
                    str(language or "").strip() or None,
                    str(voice or "").strip() or None,
                    float(speed) if speed is not None else None,
                    str(source_text or "").strip() or None,
                    str(object_key).strip(),
                    str(url).strip(),
                    int(size_bytes) if size_bytes is not None else None,
                ),
            )
            row = cursor.fetchone()
    return _map_tts_object_cache_row(row) if row else None


def mark_tts_object_cache_failed(
    *,
    cache_key: str,
    error_code: str | None = None,
    error_msg: str | None = None,
    language: str | None = None,
    voice: str | None = None,
    speed: float | None = None,
    source_text: str | None = None,
    object_key: str | None = None,
) -> dict | None:
    if not cache_key:
        return None
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_tts_object_cache (
                    cache_key,
                    status,
                    language,
                    voice,
                    speed,
                    source_text,
                    object_key,
                    error_code,
                    error_msg,
                    created_at,
                    updated_at
                )
                VALUES (%s, 'failed', %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                ON CONFLICT (cache_key) DO UPDATE
                SET
                    status = 'failed',
                    language = COALESCE(EXCLUDED.language, bt_3_tts_object_cache.language),
                    voice = COALESCE(EXCLUDED.voice, bt_3_tts_object_cache.voice),
                    speed = COALESCE(EXCLUDED.speed, bt_3_tts_object_cache.speed),
                    source_text = COALESCE(EXCLUDED.source_text, bt_3_tts_object_cache.source_text),
                    object_key = COALESCE(EXCLUDED.object_key, bt_3_tts_object_cache.object_key),
                    error_code = COALESCE(EXCLUDED.error_code, bt_3_tts_object_cache.error_code),
                    error_msg = COALESCE(EXCLUDED.error_msg, bt_3_tts_object_cache.error_msg),
                    updated_at = NOW()
                RETURNING
                    cache_key,
                    status,
                    language,
                    voice,
                    speed,
                    source_text,
                    object_key,
                    url,
                    size_bytes,
                    error_code,
                    error_msg,
                    created_at,
                    updated_at,
                    last_hit_at;
                """,
                (
                    str(cache_key),
                    str(language or "").strip() or None,
                    str(voice or "").strip() or None,
                    float(speed) if speed is not None else None,
                    str(source_text or "").strip() or None,
                    str(object_key or "").strip() or None,
                    str(error_code or "").strip() or None,
                    str(error_msg or "").strip() or None,
                ),
            )
            row = cursor.fetchone()
    return _map_tts_object_cache_row(row) if row else None


def get_tts_object_meta(cache_key: str, *, touch_hit: bool = True) -> dict | None:
    return get_tts_object_cache(cache_key, touch_hit=touch_hit)


def create_tts_object_pending(
    *,
    cache_key: str,
    language: str | None = None,
    voice: str | None = None,
    speed: float | None = None,
    source_text: str | None = None,
    object_key: str | None = None,
) -> bool:
    return try_create_tts_object_cache_pending(
        cache_key=cache_key,
        language=language,
        voice=voice,
        speed=speed,
        source_text=source_text,
        object_key=object_key,
    )


def requeue_tts_object_pending(
    *,
    cache_key: str,
    language: str | None = None,
    voice: str | None = None,
    speed: float | None = None,
    source_text: str | None = None,
    object_key: str | None = None,
) -> bool:
    if not cache_key:
        return False
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE bt_3_tts_object_cache
                SET
                    status = 'pending',
                    language = COALESCE(%s, language),
                    voice = COALESCE(%s, voice),
                    speed = COALESCE(%s, speed),
                    source_text = COALESCE(%s, source_text),
                    object_key = COALESCE(%s, object_key),
                    error_code = NULL,
                    error_msg = NULL,
                    updated_at = NOW()
                WHERE cache_key = %s
                  AND status = 'failed';
                """,
                (
                    str(language or "").strip() or None,
                    str(voice or "").strip() or None,
                    float(speed) if speed is not None else None,
                    str(source_text or "").strip() or None,
                    str(object_key or "").strip() or None,
                    str(cache_key),
                ),
            )
            claimed = cursor.rowcount > 0
    return bool(claimed)


def mark_tts_object_ready(
    *,
    cache_key: str,
    object_key: str,
    url: str,
    size_bytes: int | None = None,
    language: str | None = None,
    voice: str | None = None,
    speed: float | None = None,
    source_text: str | None = None,
) -> dict | None:
    return mark_tts_object_cache_ready(
        cache_key=cache_key,
        object_key=object_key,
        url=url,
        size_bytes=size_bytes,
        language=language,
        voice=voice,
        speed=speed,
        source_text=source_text,
    )


def mark_tts_object_failed(
    *,
    cache_key: str,
    error_code: str | None = None,
    error_msg: str | None = None,
    language: str | None = None,
    voice: str | None = None,
    speed: float | None = None,
    source_text: str | None = None,
    object_key: str | None = None,
) -> dict | None:
    return mark_tts_object_cache_failed(
        cache_key=cache_key,
        error_code=error_code,
        error_msg=error_msg,
        language=language,
        voice=voice,
        speed=speed,
        source_text=source_text,
        object_key=object_key,
    )


def record_flashcard_answer(user_id: int, entry_id: int, is_correct: bool) -> None:
    if not user_id or not entry_id:
        return
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO bt_3_flashcard_stats (user_id, entry_id, correct_count, wrong_count, last_result, updated_at)
                VALUES (%s, %s, %s, %s, %s, NOW())
                ON CONFLICT (user_id, entry_id) DO UPDATE
                SET correct_count = bt_3_flashcard_stats.correct_count + %s,
                    wrong_count = bt_3_flashcard_stats.wrong_count + %s,
                    last_result = EXCLUDED.last_result,
                    updated_at = NOW();
            """, (
                user_id,
                entry_id,
                1 if is_correct else 0,
                0 if is_correct else 1,
                is_correct,
                1 if is_correct else 0,
                0 if is_correct else 1,
            ))
            cursor.execute("""
                INSERT INTO bt_3_flashcard_seen (user_id, entry_id, seen_at)
                VALUES (%s, %s, NOW());
            """, (user_id, entry_id))


def get_flashcard_set(
    user_id: int,
    set_size: int = 15,
    wrong_size: int = 5,
    folder_mode: str = "all",
    folder_id: int | None = None,
    source_lang: str | None = None,
    target_lang: str | None = None,
    randomize_pool: bool = False,
    exclude_recent_seen: bool = True,
    wrong_source: str = "legacy",
    diagnostics: dict | None = None,
) -> list[dict]:
    if not user_id:
        return []
    debug_info = diagnostics if isinstance(diagnostics, dict) else None
    wrong_ids: list[int] = []
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            language_filter_sql_q, language_params = _build_language_pair_filter(
                source_lang,
                target_lang,
                table_alias="q",
            )
            normalized_wrong_source = str(wrong_source or "legacy").strip().lower()
            if normalized_wrong_source == "fsrs_review_log":
                wrong_where = (
                    "l.user_id = %s "
                    "AND l.rating = 1 "
                    "AND COALESCE(q.response_json->>'sentence_origin', '') <> 'gpt_seed'"
                )
                wrong_params = [user_id]
                if language_filter_sql_q:
                    wrong_where += language_filter_sql_q
                    wrong_params.extend(language_params)
                if folder_mode == "folder" and folder_id is not None:
                    wrong_where += " AND q.folder_id = %s"
                    wrong_params.append(folder_id)
                elif folder_mode == "none":
                    wrong_where += " AND q.folder_id IS NULL"
                wrong_params.append(wrong_size)
                cursor.execute(f"""
                    WITH latest_review AS (
                        SELECT
                            l.card_id,
                            l.rating,
                            l.reviewed_at,
                            ROW_NUMBER() OVER (
                                PARTITION BY l.card_id
                                ORDER BY l.reviewed_at DESC
                            ) AS rn
                        FROM bt_3_card_review_log l
                        JOIN bt_3_webapp_dictionary_queries q ON q.id = l.card_id
                        WHERE {wrong_where}
                    )
                    SELECT card_id
                    FROM latest_review
                    WHERE rn = 1
                    ORDER BY reviewed_at DESC
                    LIMIT %s;
                """, wrong_params)
                wrong_ids = [row[0] for row in cursor.fetchall()]
            else:
                wrong_where = (
                    "s.user_id = %s "
                    "AND s.last_result = FALSE "
                    "AND COALESCE(q.response_json->>'sentence_origin', '') <> 'gpt_seed'"
                )
                wrong_params = [user_id]
                if language_filter_sql_q:
                    wrong_where += language_filter_sql_q
                    wrong_params.extend(language_params)
                if folder_mode == "folder" and folder_id is not None:
                    wrong_where += " AND q.folder_id = %s"
                    wrong_params.append(folder_id)
                elif folder_mode == "none":
                    wrong_where += " AND q.folder_id IS NULL"
                wrong_params.append(wrong_size)
                cursor.execute(f"""
                    SELECT s.entry_id
                    FROM bt_3_flashcard_stats s
                    JOIN bt_3_webapp_dictionary_queries q ON q.id = s.entry_id
                    WHERE {wrong_where}
                    ORDER BY s.updated_at DESC
                    LIMIT %s;
                """, wrong_params)
                wrong_ids = [row[0] for row in cursor.fetchall()]
            if debug_info is not None:
                debug_info["wrong_source"] = normalized_wrong_source
                debug_info["wrong_ids_initial"] = len(wrong_ids)

            if normalized_wrong_source != "fsrs_review_log" and len(wrong_ids) < wrong_size:
                extra_where = (
                    "s.user_id = %s "
                    "AND s.entry_id <> ALL(%s::bigint[]) "
                    "AND COALESCE(q.response_json->>'sentence_origin', '') <> 'gpt_seed'"
                )
                extra_params = [user_id, wrong_ids or [0]]
                if language_filter_sql_q:
                    extra_where += language_filter_sql_q
                    extra_params.extend(language_params)
                if folder_mode == "folder" and folder_id is not None:
                    extra_where += " AND q.folder_id = %s"
                    extra_params.append(folder_id)
                elif folder_mode == "none":
                    extra_where += " AND q.folder_id IS NULL"
                extra_params.append(wrong_size - len(wrong_ids))
                cursor.execute(f"""
                    SELECT s.entry_id
                    FROM bt_3_flashcard_stats s
                    JOIN bt_3_webapp_dictionary_queries q ON q.id = s.entry_id
                    WHERE {extra_where}
                    ORDER BY (s.wrong_count - s.correct_count) DESC, s.updated_at DESC
                    LIMIT %s;
                """, extra_params)
                wrong_ids.extend([row[0] for row in cursor.fetchall()])
            if debug_info is not None:
                debug_info["wrong_ids_final"] = len(wrong_ids)

            base_where = (
                "user_id = %s "
                "AND id <> ALL(%s::bigint[]) "
                "AND COALESCE(response_json->>'sentence_origin', '') <> 'gpt_seed'"
            )
            base_params = [user_id, wrong_ids or [0]]
            language_filter_sql, language_params_no_alias = _build_language_pair_filter(source_lang, target_lang)
            if language_filter_sql:
                base_where += language_filter_sql
                base_params.extend(language_params_no_alias)
            if folder_mode == "folder" and folder_id is not None:
                base_where += " AND folder_id = %s"
                base_params.append(folder_id)
            elif folder_mode == "none":
                base_where += " AND folder_id IS NULL"
            needed = max(set_size - len(wrong_ids), 0)
            random_rows = []
            if needed > 0:
                if randomize_pool and not exclude_recent_seen:
                    sample_cap = max(needed * 40, 2000)
                else:
                    sample_cap = max(needed * 12, 300)
                if debug_info is not None:
                    debug_info["needed_after_wrong_ids"] = needed
                    debug_info["sample_cap"] = sample_cap
                    debug_info["randomize_pool"] = bool(randomize_pool)
                    debug_info["exclude_recent_seen"] = bool(exclude_recent_seen)
                sample_order_sql = "ORDER BY RANDOM()" if randomize_pool else "ORDER BY created_at DESC"
                sample_where = base_where
                sample_params = list(base_params)
                if exclude_recent_seen:
                    sample_where += (
                        " AND id NOT IN ("
                        " SELECT entry_id"
                        " FROM bt_3_flashcard_seen"
                        " WHERE user_id = %s"
                        "   AND seen_at >= NOW() - (%s * INTERVAL '1 hour')"
                        " )"
                    )
                    sample_params.extend([user_id, FLASHCARD_RECENT_SEEN_HOURS])
                sample_params.append(sample_cap)
                cursor.execute(f"""
                    SELECT id, word_ru, translation_de, word_de, translation_ru, response_json
                    FROM bt_3_webapp_dictionary_queries
                    WHERE {sample_where}
                    {sample_order_sql}
                    LIMIT %s;
                """, sample_params)
                candidate_rows = list(cursor.fetchall())
                if debug_info is not None:
                    debug_info["sample_candidates"] = len(candidate_rows)
                random.shuffle(candidate_rows)
                random_rows = candidate_rows[:needed]
                if debug_info is not None:
                    debug_info["sample_selected"] = len(random_rows)

                if len(random_rows) < needed:
                    fallback_where = (
                        "user_id = %s "
                        "AND id <> ALL(%s::bigint[]) "
                        "AND COALESCE(response_json->>'sentence_origin', '') <> 'gpt_seed'"
                    )
                    fallback_params = [user_id, wrong_ids or [0]]
                    if language_filter_sql:
                        fallback_where += language_filter_sql
                        fallback_params.extend(language_params_no_alias)
                    if folder_mode == "folder" and folder_id is not None:
                        fallback_where += " AND folder_id = %s"
                        fallback_params.append(folder_id)
                    elif folder_mode == "none":
                        fallback_where += " AND folder_id IS NULL"
                    if randomize_pool and not exclude_recent_seen:
                        fallback_cap = max(needed * 80, 4000)
                    else:
                        fallback_cap = max(needed * 20, 600)
                    fallback_order_sql = "ORDER BY RANDOM()" if randomize_pool else "ORDER BY created_at DESC"
                    fallback_params.append(fallback_cap)
                    cursor.execute(f"""
                        SELECT id, word_ru, translation_de, word_de, translation_ru, response_json
                        FROM bt_3_webapp_dictionary_queries
                        WHERE {fallback_where}
                        {fallback_order_sql}
                        LIMIT %s;
                    """, fallback_params)
                    fallback_rows = list(cursor.fetchall())
                    if debug_info is not None:
                        debug_info["fallback_cap"] = fallback_cap
                        debug_info["fallback_candidates"] = len(fallback_rows)
                    existing_ids = {row[0] for row in random_rows}
                    for row in fallback_rows:
                        if row[0] in existing_ids:
                            continue
                        random_rows.append(row)
                        existing_ids.add(row[0])
                        if len(random_rows) >= needed:
                            break
                    if debug_info is not None:
                        debug_info["fallback_selected_total"] = len(random_rows)

            if wrong_ids:
                wrong_where = (
                    "user_id = %s "
                    "AND id = ANY(%s::bigint[]) "
                    "AND COALESCE(response_json->>'sentence_origin', '') <> 'gpt_seed'"
                )
                wrong_params = [user_id, wrong_ids]
                if language_filter_sql:
                    wrong_where += language_filter_sql
                    wrong_params.extend(language_params_no_alias)
                if folder_mode == "folder" and folder_id is not None:
                    wrong_where += " AND folder_id = %s"
                    wrong_params.append(folder_id)
                elif folder_mode == "none":
                    wrong_where += " AND folder_id IS NULL"
                cursor.execute(f"""
                    SELECT id, word_ru, translation_de, word_de, translation_ru, response_json
                    FROM bt_3_webapp_dictionary_queries
                    WHERE {wrong_where};
                """, wrong_params)
                wrong_rows = cursor.fetchall()
            else:
                wrong_rows = []

    rows = wrong_rows + random_rows
    items = []
    for row in rows:
        items.append({
            "id": row[0],
            "word_ru": row[1],
            "translation_de": row[2],
            "word_de": row[3],
            "translation_ru": row[4],
            "response_json": row[5],
        })
    if debug_info is not None:
        debug_info["wrong_rows_returned"] = len(wrong_rows)
        debug_info["random_rows_returned"] = len(random_rows)
        debug_info["returned_items"] = len(items)
    return items


def get_card_srs_state(user_id: int, card_id: int, cursor=None) -> dict | None:
    def _fetch(cur):
        cur.execute(
            """
            SELECT
                id,
                user_id,
                card_id,
                status,
                due_at,
                last_review_at,
                interval_days,
                reps,
                lapses,
                stability,
                difficulty,
                created_at,
                updated_at
            FROM bt_3_card_srs_state
            WHERE user_id = %s AND card_id = %s
            LIMIT 1;
            """,
            (int(user_id), int(card_id)),
        )
        row = cur.fetchone()
        if not row:
            return None
        return {
            "id": row[0],
            "user_id": row[1],
            "card_id": row[2],
            "status": row[3],
            "due_at": row[4],
            "last_review_at": row[5],
            "interval_days": row[6],
            "reps": row[7],
            "lapses": row[8],
            "stability": float(row[9] or 0.0),
            "difficulty": float(row[10] or 0.0),
            "created_at": row[11],
            "updated_at": row[12],
        }
    if cursor is not None:
        return _fetch(cursor)
    with get_db_connection_context() as conn:
        with conn.cursor() as own_cursor:
            return _fetch(own_cursor)


def upsert_card_srs_state(
    user_id: int,
    card_id: int,
    status: str,
    due_at: datetime,
    last_review_at: datetime | None,
    interval_days: int,
    reps: int,
    lapses: int,
    stability: float,
    difficulty: float,
    cursor=None,
) -> dict:
    def _upsert(cur):
        cur.execute(
            """
            INSERT INTO bt_3_card_srs_state (
                user_id,
                card_id,
                status,
                due_at,
                last_review_at,
                interval_days,
                reps,
                lapses,
                stability,
                difficulty,
                created_at,
                updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
            ON CONFLICT (user_id, card_id) DO UPDATE
            SET status = EXCLUDED.status,
                due_at = EXCLUDED.due_at,
                last_review_at = EXCLUDED.last_review_at,
                interval_days = EXCLUDED.interval_days,
                reps = EXCLUDED.reps,
                lapses = EXCLUDED.lapses,
                stability = EXCLUDED.stability,
                difficulty = EXCLUDED.difficulty,
                updated_at = NOW()
            RETURNING
                id,
                user_id,
                card_id,
                status,
                due_at,
                last_review_at,
                interval_days,
                reps,
                lapses,
                stability,
                difficulty,
                created_at,
                updated_at;
            """,
            (
                int(user_id),
                int(card_id),
                status,
                due_at,
                last_review_at,
                int(interval_days),
                int(reps),
                int(lapses),
                float(stability),
                float(difficulty),
            ),
        )
        row = cur.fetchone()
        return {
            "id": row[0],
            "user_id": row[1],
            "card_id": row[2],
            "status": row[3],
            "due_at": row[4],
            "last_review_at": row[5],
            "interval_days": row[6],
            "reps": row[7],
            "lapses": row[8],
            "stability": float(row[9] or 0.0),
            "difficulty": float(row[10] or 0.0),
            "created_at": row[11],
            "updated_at": row[12],
        }
    if cursor is not None:
        return _upsert(cursor)
    with get_db_connection_context() as conn:
        with conn.cursor() as own_cursor:
            return _upsert(own_cursor)


def count_due_srs_cards(
    user_id: int,
    now_utc: datetime | None = None,
    source_lang: str | None = None,
    target_lang: str | None = None,
    cursor=None,
) -> int:
    now_utc = now_utc or datetime.now(timezone.utc)
    def _count(cur):
        language_filter_sql, language_params = _build_language_pair_filter(
            source_lang,
            target_lang,
            table_alias="q",
        )
        cur.execute(
            f"""
            SELECT COUNT(*)
            FROM bt_3_card_srs_state s
            JOIN bt_3_webapp_dictionary_queries q
              ON q.id = s.card_id AND q.user_id = s.user_id
            WHERE s.user_id = %s
              AND s.status <> 'suspended'
              AND s.due_at <= %s
              {language_filter_sql};
            """,
            [int(user_id), now_utc, *language_params],
        )
        row = cur.fetchone()
        return int(row[0] if row else 0)
    if cursor is not None:
        return _count(cursor)
    with get_db_connection_context() as conn:
        with conn.cursor() as own_cursor:
            return _count(own_cursor)


def count_new_cards_introduced_today(
    user_id: int,
    now_utc: datetime | None = None,
    source_lang: str | None = None,
    target_lang: str | None = None,
    cursor=None,
) -> int:
    now_utc = now_utc or datetime.now(timezone.utc)
    day_start = datetime(now_utc.year, now_utc.month, now_utc.day, tzinfo=timezone.utc)
    day_end = day_start + timedelta(days=1)
    def _count(cur):
        language_filter_sql, language_params = _build_language_pair_filter(
            source_lang,
            target_lang,
            table_alias="q",
        )
        cur.execute(
            f"""
            SELECT COUNT(*)
            FROM bt_3_card_srs_state s
            JOIN bt_3_webapp_dictionary_queries q
              ON q.id = s.card_id AND q.user_id = s.user_id
            WHERE s.user_id = %s
              AND s.created_at >= %s
              AND s.created_at < %s
              {language_filter_sql};
            """,
            [int(user_id), day_start, day_end, *language_params],
        )
        row = cur.fetchone()
        return int(row[0] if row else 0)
    if cursor is not None:
        return _count(cursor)
    with get_db_connection_context() as conn:
        with conn.cursor() as own_cursor:
            return _count(own_cursor)


def has_available_new_srs_cards(
    user_id: int,
    source_lang: str | None = None,
    target_lang: str | None = None,
    cursor=None,
) -> bool:
    def _exists(cur):
        language_filter_sql, language_params = _build_language_pair_filter(
            source_lang,
            target_lang,
            table_alias="q",
        )
        cur.execute(
            f"""
            SELECT 1
            FROM bt_3_webapp_dictionary_queries q
            LEFT JOIN bt_3_card_srs_state s
              ON s.user_id = q.user_id AND s.card_id = q.id
            WHERE q.user_id = %s
              AND s.id IS NULL
              {language_filter_sql}
            LIMIT 1;
            """,
            [int(user_id), *language_params],
        )
        return cur.fetchone() is not None
    if cursor is not None:
        return _exists(cursor)
    with get_db_connection_context() as conn:
        with conn.cursor() as own_cursor:
            return _exists(own_cursor)


def count_available_new_srs_cards(
    user_id: int,
    source_lang: str | None = None,
    target_lang: str | None = None,
) -> int:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            language_filter_sql, language_params = _build_language_pair_filter(
                source_lang,
                target_lang,
                table_alias="q",
            )
            cursor.execute(
                f"""
                SELECT COUNT(*)
                FROM bt_3_webapp_dictionary_queries q
                LEFT JOIN bt_3_card_srs_state s
                  ON s.user_id = q.user_id AND s.card_id = q.id
                WHERE q.user_id = %s
                  AND s.id IS NULL
                  {language_filter_sql};
                """,
                [int(user_id), *language_params],
            )
            row = cursor.fetchone()
            return int(row[0] if row else 0)


def get_next_due_srs_card(
    user_id: int,
    now_utc: datetime | None = None,
    source_lang: str | None = None,
    target_lang: str | None = None,
    cursor=None,
) -> dict | None:
    now_utc = now_utc or datetime.now(timezone.utc)
    def _fetch(cur):
        language_filter_sql, language_params = _build_language_pair_filter(
            source_lang,
            target_lang,
            table_alias="q",
        )
        cur.execute(
            f"""
            SELECT
                s.card_id,
                s.status,
                s.due_at,
                s.last_review_at,
                s.interval_days,
                s.reps,
                s.lapses,
                s.stability,
                s.difficulty,
                q.word_ru,
                q.translation_de,
                q.word_de,
                q.translation_ru,
                q.response_json
            FROM bt_3_card_srs_state s
            JOIN bt_3_webapp_dictionary_queries q
              ON q.id = s.card_id
             AND q.user_id = s.user_id
            WHERE s.user_id = %s
              AND s.status <> 'suspended'
              AND s.due_at <= %s
              {language_filter_sql}
            ORDER BY s.due_at ASC
            LIMIT 1;
            """,
            [int(user_id), now_utc, *language_params],
        )
        row = cur.fetchone()
        if not row:
            return None
        return {
            "card": {
                "id": row[0],
                "word_ru": row[9],
                "translation_de": row[10],
                "word_de": row[11],
                "translation_ru": row[12],
                "response_json": row[13],
            },
            "srs": {
                "status": row[1],
                "due_at": row[2],
                "last_review_at": row[3],
                "interval_days": int(row[4] or 0),
                "reps": int(row[5] or 0),
                "lapses": int(row[6] or 0),
                "stability": float(row[7] or 0.0),
                "difficulty": float(row[8] or 0.0),
            },
        }
    if cursor is not None:
        return _fetch(cursor)
    with get_db_connection_context() as conn:
        with conn.cursor() as own_cursor:
            return _fetch(own_cursor)


def get_next_new_srs_candidate(
    user_id: int,
    source_lang: str | None = None,
    target_lang: str | None = None,
    cursor=None,
) -> dict | None:
    def _fetch(cur):
        language_filter_sql, language_params = _build_language_pair_filter(
            source_lang,
            target_lang,
            table_alias="q",
        )
        cur.execute(
            f"""
            SELECT q.id, q.word_ru, q.translation_de, q.word_de, q.translation_ru, q.response_json
            FROM bt_3_webapp_dictionary_queries q
            LEFT JOIN bt_3_card_srs_state s
              ON s.user_id = q.user_id AND s.card_id = q.id
            WHERE q.user_id = %s
              AND s.id IS NULL
              {language_filter_sql}
            ORDER BY q.created_at ASC
            LIMIT 1;
            """,
            [int(user_id), *language_params],
        )
        row = cur.fetchone()
        if not row:
            return None
        return {
            "id": row[0],
            "word_ru": row[1],
            "translation_de": row[2],
            "word_de": row[3],
            "translation_ru": row[4],
            "response_json": row[5],
        }
    if cursor is not None:
        return _fetch(cursor)
    with get_db_connection_context() as conn:
        with conn.cursor() as own_cursor:
            return _fetch(own_cursor)


def ensure_new_srs_state(user_id: int, card_id: int, now_utc: datetime | None = None, cursor=None) -> dict:
    now_utc = now_utc or datetime.now(timezone.utc)
    state = get_card_srs_state(user_id=user_id, card_id=card_id, cursor=cursor)
    if state:
        return state
    return upsert_card_srs_state(
        user_id=user_id,
        card_id=card_id,
        status="new",
        due_at=now_utc,
        last_review_at=None,
        interval_days=0,
        reps=0,
        lapses=0,
        stability=0.0,
        difficulty=0.0,
        cursor=cursor,
    )


def get_dictionary_entry_for_user(user_id: int, card_id: int, cursor=None) -> dict | None:
    def _fetch(cur):
        cur.execute(
            """
            SELECT
                id,
                word_ru,
                translation_de,
                word_de,
                translation_ru,
                source_lang,
                target_lang,
                response_json
            FROM bt_3_webapp_dictionary_queries
            WHERE user_id = %s AND id = %s
            LIMIT 1;
            """,
            (int(user_id), int(card_id)),
        )
        row = cur.fetchone()
        if not row:
            return None
        return {
            "id": row[0],
            "word_ru": row[1],
            "translation_de": row[2],
            "word_de": row[3],
            "translation_ru": row[4],
            "source_lang": row[5],
            "target_lang": row[6],
            "response_json": row[7],
        }
    if cursor is not None:
        return _fetch(cursor)
    with get_db_connection_context() as conn:
        with conn.cursor() as own_cursor:
            return _fetch(own_cursor)


def insert_card_review_log(
    *,
    user_id: int,
    card_id: int,
    reviewed_at: datetime,
    rating: int,
    response_ms: int | None,
    scheduled_due_before: datetime | None,
    scheduled_due_after: datetime | None,
    stability_before: float | None,
    difficulty_before: float | None,
    stability_after: float | None,
    difficulty_after: float | None,
    interval_days_after: int | None,
    cursor=None,
) -> None:
    def _insert(cur):
        cur.execute(
            """
            INSERT INTO bt_3_card_review_log (
                user_id,
                card_id,
                reviewed_at,
                rating,
                response_ms,
                scheduled_due_before,
                scheduled_due_after,
                stability_before,
                difficulty_before,
                stability_after,
                difficulty_after,
                interval_days_after
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """,
            (
                int(user_id),
                int(card_id),
                reviewed_at,
                int(rating),
                int(response_ms) if response_ms is not None else None,
                scheduled_due_before,
                scheduled_due_after,
                float(stability_before) if stability_before is not None else None,
                float(difficulty_before) if difficulty_before is not None else None,
                float(stability_after) if stability_after is not None else None,
                float(difficulty_after) if difficulty_after is not None else None,
                int(interval_days_after) if interval_days_after is not None else None,
            ),
        )
    if cursor is not None:
        _insert(cursor)
        return
    with get_db_connection_context() as conn:
        with conn.cursor() as own_cursor:
            _insert(own_cursor)


def create_dictionary_folder(
    user_id: int,
    name: str,
    color: str,
    icon: str,
) -> dict:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO bt_3_dictionary_folders (user_id, name, color, icon)
                VALUES (%s, %s, %s, %s)
                RETURNING id, name, color, icon, created_at;
            """, (user_id, name, color, icon))
            row = cursor.fetchone()
            return {
                "id": row[0],
                "name": row[1],
                "color": row[2],
                "icon": row[3],
                "created_at": row[4].isoformat() if row[4] else None,
            }


def get_dictionary_folders(user_id: int) -> list[dict]:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT id, name, color, icon, created_at
                FROM bt_3_dictionary_folders
                WHERE user_id = %s
                ORDER BY created_at DESC;
            """, (user_id,))
            rows = cursor.fetchall()
            return [
                {
                    "id": row[0],
                    "name": row[1],
                    "color": row[2],
                    "icon": row[3],
                    "created_at": row[4].isoformat() if row[4] else None,
                }
                for row in rows
            ]


def get_or_create_dictionary_folder(
    user_id: int,
    name: str,
    color: str,
    icon: str,
) -> dict:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT id, name, color, icon, created_at
                FROM bt_3_dictionary_folders
                WHERE user_id = %s AND name = %s
                LIMIT 1;
                """,
                (user_id, name),
            )
            row = cursor.fetchone()
            if row:
                return {
                    "id": row[0],
                    "name": row[1],
                    "color": row[2],
                    "icon": row[3],
                    "created_at": row[4].isoformat() if row[4] else None,
                }
            cursor.execute(
                """
                INSERT INTO bt_3_dictionary_folders (user_id, name, color, icon)
                VALUES (%s, %s, %s, %s)
                RETURNING id, name, color, icon, created_at;
                """,
                (user_id, name, color, icon),
            )
            row = cursor.fetchone()
            return {
                "id": row[0],
                "name": row[1],
                "color": row[2],
                "icon": row[3],
                "created_at": row[4].isoformat() if row[4] else None,
            }


def record_telegram_system_message(
    chat_id: int,
    message_id: int,
    message_type: str = "text",
) -> None:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_telegram_system_messages (chat_id, message_id, message_type)
                VALUES (%s, %s, %s)
                ON CONFLICT (chat_id, message_id) DO NOTHING;
                """,
                (int(chat_id), int(message_id), (message_type or "text").strip()[:32]),
            )


def get_pending_telegram_system_messages(
    target_date: date,
    tz_name: str = "UTC",
    max_days_back: int = 2,
    limit: int = 5000,
) -> list[dict]:
    max_days_back = max(0, int(max_days_back))
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT id, chat_id, message_id, message_type, created_at
                FROM bt_3_telegram_system_messages
                WHERE deleted_at IS NULL
                  AND ((created_at AT TIME ZONE %s)::date BETWEEN %s AND %s)
                ORDER BY created_at ASC
                LIMIT %s;
                """,
                (
                    tz_name,
                    target_date - timedelta(days=max_days_back),
                    target_date,
                    int(limit),
                ),
            )
            rows = cursor.fetchall()
    return [
        {
            "id": int(row[0]),
            "chat_id": int(row[1]),
            "message_id": int(row[2]),
            "message_type": row[3],
            "created_at": row[4].isoformat() if row[4] else None,
        }
        for row in rows
    ]


def mark_telegram_system_message_deleted(
    row_id: int,
    delete_error: str | None = None,
) -> None:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            if delete_error:
                cursor.execute(
                    """
                    UPDATE bt_3_telegram_system_messages
                    SET delete_error = %s
                    WHERE id = %s;
                    """,
                    (str(delete_error)[:500], int(row_id)),
                )
            else:
                cursor.execute(
                    """
                    UPDATE bt_3_telegram_system_messages
                    SET deleted_at = NOW(),
                        delete_error = NULL
                    WHERE id = %s;
                    """,
                    (int(row_id),),
                )


def has_admin_scheduler_run(
    *,
    job_key: str,
    run_period: str,
    target_chat_id: int,
) -> bool:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT 1
                FROM bt_3_admin_scheduler_runs
                WHERE job_key = %s
                  AND run_period = %s
                  AND target_chat_id = %s
                LIMIT 1;
                """,
                (
                    str(job_key or "").strip()[:80],
                    str(run_period or "").strip()[:32],
                    int(target_chat_id),
                ),
            )
            row = cursor.fetchone()
    return bool(row)


def mark_admin_scheduler_run(
    *,
    job_key: str,
    run_period: str,
    target_chat_id: int,
    metadata: dict | None = None,
) -> None:
    payload = metadata if isinstance(metadata, dict) else {}
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_admin_scheduler_runs (job_key, run_period, target_chat_id, metadata)
                VALUES (%s, %s, %s, %s::jsonb)
                ON CONFLICT (job_key, run_period, target_chat_id) DO NOTHING;
                """,
                (
                    str(job_key or "").strip()[:80],
                    str(run_period or "").strip()[:32],
                    int(target_chat_id),
                    Json(payload),
                ),
            )


def create_support_message(
    *,
    user_id: int,
    from_role: str,
    message_text: str,
    admin_telegram_id: int | None = None,
    telegram_chat_id: int | None = None,
    telegram_message_id: int | None = None,
    reply_to_id: int | None = None,
    is_read_by_user: bool | None = None,
) -> dict:
    normalized_role = str(from_role or "").strip().lower()
    if normalized_role not in {"user", "admin", "system"}:
        raise ValueError("from_role must be one of: user, admin, system")
    text = str(message_text or "").strip()
    if not text:
        raise ValueError("message_text is required")
    if is_read_by_user is None:
        is_read_by_user = normalized_role != "admin"

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_support_messages (
                    user_id,
                    from_role,
                    message_text,
                    admin_telegram_id,
                    telegram_chat_id,
                    telegram_message_id,
                    reply_to_id,
                    is_read_by_user
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id, user_id, from_role, message_text, admin_telegram_id, telegram_chat_id, telegram_message_id, reply_to_id, is_read_by_user, created_at;
                """,
                (
                    int(user_id),
                    normalized_role,
                    text,
                    int(admin_telegram_id) if admin_telegram_id is not None else None,
                    int(telegram_chat_id) if telegram_chat_id is not None else None,
                    int(telegram_message_id) if telegram_message_id is not None else None,
                    int(reply_to_id) if reply_to_id is not None else None,
                    bool(is_read_by_user),
                ),
            )
            row = cursor.fetchone()
    return {
        "id": int(row[0]),
        "user_id": int(row[1]),
        "from_role": str(row[2] or ""),
        "message_text": str(row[3] or ""),
        "admin_telegram_id": int(row[4]) if row[4] is not None else None,
        "telegram_chat_id": int(row[5]) if row[5] is not None else None,
        "telegram_message_id": int(row[6]) if row[6] is not None else None,
        "reply_to_id": int(row[7]) if row[7] is not None else None,
        "is_read_by_user": bool(row[8]),
        "created_at": row[9].isoformat() if row[9] else None,
    }


def list_support_messages_for_user(*, user_id: int, limit: int = 100) -> list[dict]:
    safe_limit = max(1, min(int(limit or 100), 500))
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT id, user_id, from_role, message_text, admin_telegram_id, telegram_chat_id, telegram_message_id, reply_to_id, is_read_by_user, created_at
                FROM bt_3_support_messages
                WHERE user_id = %s
                ORDER BY created_at ASC
                LIMIT %s;
                """,
                (int(user_id), safe_limit),
            )
            rows = cursor.fetchall()
    return [
        {
            "id": int(row[0]),
            "user_id": int(row[1]),
            "from_role": str(row[2] or ""),
            "message_text": str(row[3] or ""),
            "admin_telegram_id": int(row[4]) if row[4] is not None else None,
            "telegram_chat_id": int(row[5]) if row[5] is not None else None,
            "telegram_message_id": int(row[6]) if row[6] is not None else None,
            "reply_to_id": int(row[7]) if row[7] is not None else None,
            "is_read_by_user": bool(row[8]),
            "created_at": row[9].isoformat() if row[9] else None,
        }
        for row in rows
    ]


def count_unread_support_messages_for_user(*, user_id: int) -> int:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM bt_3_support_messages
                WHERE user_id = %s
                  AND from_role = 'admin'
                  AND is_read_by_user = FALSE;
                """,
                (int(user_id),),
            )
            row = cursor.fetchone()
    return int(row[0] or 0) if row else 0


def mark_support_messages_read_for_user(*, user_id: int) -> int:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE bt_3_support_messages
                SET is_read_by_user = TRUE
                WHERE user_id = %s
                  AND from_role = 'admin'
                  AND is_read_by_user = FALSE;
                """,
                (int(user_id),),
            )
            affected = cursor.rowcount
    return int(affected or 0)


def get_support_message_by_telegram_ref(*, telegram_chat_id: int, telegram_message_id: int) -> dict | None:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT id, user_id, from_role, message_text, admin_telegram_id, telegram_chat_id, telegram_message_id, reply_to_id, is_read_by_user, created_at
                FROM bt_3_support_messages
                WHERE telegram_chat_id = %s
                  AND telegram_message_id = %s
                ORDER BY created_at DESC
                LIMIT 1;
                """,
                (int(telegram_chat_id), int(telegram_message_id)),
            )
            row = cursor.fetchone()
    if not row:
        return None
    return {
        "id": int(row[0]),
        "user_id": int(row[1]),
        "from_role": str(row[2] or ""),
        "message_text": str(row[3] or ""),
        "admin_telegram_id": int(row[4]) if row[4] is not None else None,
        "telegram_chat_id": int(row[5]) if row[5] is not None else None,
        "telegram_message_id": int(row[6]) if row[6] is not None else None,
        "reply_to_id": int(row[7]) if row[7] is not None else None,
        "is_read_by_user": bool(row[8]),
        "created_at": row[9].isoformat() if row[9] else None,
    }


def _map_daily_plan_item(row: tuple) -> dict:
    return {
        "id": int(row[0]),
        "plan_id": int(row[1]),
        "order_index": int(row[2] or 0),
        "task_type": row[3],
        "title": row[4],
        "estimated_minutes": int(row[5] or 0),
        "payload": row[6] if isinstance(row[6], dict) else {},
        "status": row[7] or "todo",
        "completed_at": row[8].isoformat() if row[8] else None,
    }


def _week_bounds(anchor_date: date | None = None) -> tuple[date, date]:
    current = anchor_date or date.today()
    week_start = current - timedelta(days=current.weekday())
    return week_start, week_start + timedelta(days=6)


def _period_bounds(period: str, anchor_date: date | None = None) -> tuple[date, date]:
    current = anchor_date or date.today()
    normalized = str(period or "week").strip().lower()
    if normalized == "week":
        return _week_bounds(current)
    if normalized == "month":
        start = current.replace(day=1)
        end = date(current.year, current.month, monthrange(current.year, current.month)[1])
        return start, end
    if normalized == "quarter":
        quarter_index = (current.month - 1) // 3
        start_month = quarter_index * 3 + 1
        end_month = start_month + 2
        start = date(current.year, start_month, 1)
        end = date(current.year, end_month, monthrange(current.year, end_month)[1])
        return start, end
    if normalized == "half-year":
        if current.month <= 6:
            return date(current.year, 1, 1), date(current.year, 6, 30)
        return date(current.year, 7, 1), date(current.year, 12, 31)
    if normalized == "year":
        return date(current.year, 1, 1), date(current.year, 12, 31)
    return _week_bounds(current)


def get_weekly_goals(
    *,
    user_id: int,
    source_lang: str,
    target_lang: str,
    week_start: date | None = None,
) -> dict | None:
    normalized_source = str(source_lang or "ru").strip().lower() or "ru"
    normalized_target = str(target_lang or "de").strip().lower() or "de"
    resolved_week_start, week_end = _week_bounds(week_start)
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT translations_goal, learned_words_goal, updated_at
                     , agent_minutes_goal, reading_minutes_goal
                FROM bt_3_weekly_goals
                WHERE user_id = %s
                  AND source_lang = %s
                  AND target_lang = %s
                  AND week_start = %s
                LIMIT 1;
                """,
                (int(user_id), normalized_source, normalized_target, resolved_week_start),
            )
            row = cursor.fetchone()
    if not row:
        return None
    return {
        "week_start": resolved_week_start.isoformat(),
        "week_end": week_end.isoformat(),
        "source_lang": normalized_source,
        "target_lang": normalized_target,
        "translations_goal": max(0, int(row[0] or 0)),
        "learned_words_goal": max(0, int(row[1] or 0)),
        "agent_minutes_goal": max(0, int(row[3] or 0)),
        "reading_minutes_goal": max(0, int(row[4] or 0)),
        "updated_at": row[2].isoformat() if row[2] else None,
    }


def upsert_weekly_goals(
    *,
    user_id: int,
    source_lang: str,
    target_lang: str,
    translations_goal: int,
    learned_words_goal: int,
    agent_minutes_goal: int,
    reading_minutes_goal: int,
    week_start: date | None = None,
) -> dict:
    normalized_source = str(source_lang or "ru").strip().lower() or "ru"
    normalized_target = str(target_lang or "de").strip().lower() or "de"
    resolved_week_start, week_end = _week_bounds(week_start)
    translations_goal = max(0, int(translations_goal or 0))
    learned_words_goal = max(0, int(learned_words_goal or 0))
    agent_minutes_goal = max(0, int(agent_minutes_goal or 0))
    reading_minutes_goal = max(0, int(reading_minutes_goal or 0))

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_weekly_goals (
                    user_id,
                    source_lang,
                    target_lang,
                    week_start,
                    translations_goal,
                    learned_words_goal,
                    agent_minutes_goal,
                    reading_minutes_goal,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (user_id, source_lang, target_lang, week_start) DO UPDATE
                SET
                    translations_goal = EXCLUDED.translations_goal,
                    learned_words_goal = EXCLUDED.learned_words_goal,
                    agent_minutes_goal = EXCLUDED.agent_minutes_goal,
                    reading_minutes_goal = EXCLUDED.reading_minutes_goal,
                    updated_at = NOW()
                RETURNING translations_goal, learned_words_goal, agent_minutes_goal, reading_minutes_goal, updated_at;
                """,
                (
                    int(user_id),
                    normalized_source,
                    normalized_target,
                    resolved_week_start,
                    translations_goal,
                    learned_words_goal,
                    agent_minutes_goal,
                    reading_minutes_goal,
                ),
            )
            saved = cursor.fetchone()

    return {
        "week_start": resolved_week_start.isoformat(),
        "week_end": week_end.isoformat(),
        "source_lang": normalized_source,
        "target_lang": normalized_target,
        "translations_goal": max(0, int(saved[0] if saved else translations_goal)),
        "learned_words_goal": max(0, int(saved[1] if saved else learned_words_goal)),
        "agent_minutes_goal": max(0, int(saved[2] if saved else agent_minutes_goal)),
        "reading_minutes_goal": max(0, int(saved[3] if saved else reading_minutes_goal)),
        "updated_at": saved[4].isoformat() if saved and saved[4] else None,
    }


def get_plan_progress(
    *,
    user_id: int,
    source_lang: str,
    target_lang: str,
    mature_interval_days: int = 21,
    period: str = "week",
    week_start: date | None = None,
    as_of_date: date | None = None,
) -> dict:
    normalized_source = str(source_lang or "ru").strip().lower() or "ru"
    normalized_target = str(target_lang or "de").strip().lower() or "de"
    normalized_period = str(period or "week").strip().lower()
    if normalized_period == "week":
        resolved_start, resolved_end = _week_bounds(week_start or as_of_date)
    else:
        resolved_start, resolved_end = _period_bounds(normalized_period, as_of_date)
    today = as_of_date or date.today()
    effective_end = min(resolved_end, max(resolved_start, today))
    days_total = max(1, (resolved_end - resolved_start).days + 1)
    days_elapsed = max(1, (effective_end - resolved_start).days + 1)
    mature_threshold = max(1, int(mature_interval_days or 21))

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    COALESCE(SUM(translations_goal), 0) AS translations_goal,
                    COALESCE(SUM(learned_words_goal), 0) AS learned_words_goal,
                    COALESCE(SUM(agent_minutes_goal), 0) AS agent_minutes_goal,
                    COALESCE(SUM(reading_minutes_goal), 0) AS reading_minutes_goal
                FROM bt_3_weekly_goals
                WHERE user_id = %s
                  AND source_lang = %s
                  AND target_lang = %s
                  AND week_start BETWEEN %s AND %s;
                """,
                (int(user_id), normalized_source, normalized_target, resolved_start, resolved_end),
            )
            goals_row = cursor.fetchone()
            translations_goal = max(0, int(goals_row[0] or 0)) if goals_row else 0
            learned_words_goal = max(0, int(goals_row[1] or 0)) if goals_row else 0
            agent_minutes_goal = max(0, int(goals_row[2] or 0)) if goals_row else 0
            reading_minutes_goal = max(0, int(goals_row[3] or 0)) if goals_row else 0

            cursor.execute(
                """
                SELECT COUNT(*)
                FROM bt_3_translations t
                JOIN bt_3_daily_sentences ds ON ds.id = t.sentence_id
                WHERE t.user_id = %s
                  AND COALESCE(t.source_lang, 'ru') = %s
                  AND COALESCE(t.target_lang, 'de') = %s
                  AND ds.date BETWEEN %s AND %s;
                """,
                (
                    int(user_id),
                    normalized_source,
                    normalized_target,
                    resolved_start,
                    effective_end,
                ),
            )
            translation_row = cursor.fetchone()
            translations_actual = max(0, int(translation_row[0] or 0)) if translation_row else 0

            language_filter_sql, language_params = _build_language_pair_filter(
                normalized_source,
                normalized_target,
                table_alias="q",
            )
            cursor.execute(
                f"""
                WITH first_learned AS (
                    SELECT
                        l.card_id,
                        MIN(l.reviewed_at) AS learned_at
                    FROM bt_3_card_review_log l
                    JOIN bt_3_webapp_dictionary_queries q
                      ON q.id = l.card_id
                     AND q.user_id = l.user_id
                    WHERE l.user_id = %s
                      AND COALESCE(l.interval_days_after, 0) >= %s
                      {language_filter_sql}
                    GROUP BY l.card_id
                )
                SELECT COUNT(*)
                FROM first_learned
                WHERE (learned_at AT TIME ZONE 'UTC')::date BETWEEN %s AND %s;
                """,
                [
                    int(user_id),
                    mature_threshold,
                    *language_params,
                    resolved_start,
                    effective_end,
                ],
            )
            learned_row = cursor.fetchone()
            learned_words_actual = max(0, int(learned_row[0] or 0)) if learned_row else 0

            period_start_dt = datetime.combine(resolved_start, dt_time.min, tzinfo=timezone.utc)
            end_exclusive_dt = datetime.combine(effective_end + timedelta(days=1), dt_time.min, tzinfo=timezone.utc)
            now_utc = datetime.now(timezone.utc)
            cap_dt = min(end_exclusive_dt, now_utc)
            if cap_dt > period_start_dt:
                cursor.execute(
                    """
                    SELECT COALESCE(
                        SUM(
                            GREATEST(
                                0,
                                EXTRACT(
                                    EPOCH FROM (
                                        LEAST(
                                            COALESCE(
                                                ended_at,
                                                CASE
                                                    WHEN duration_seconds IS NOT NULL
                                                        THEN started_at + (GREATEST(duration_seconds, 0) * INTERVAL '1 second')
                                                    ELSE started_at
                                                END
                                            ),
                                            %s
                                        )
                                        - GREATEST(started_at, %s)
                                    )
                                )
                            )
                        ),
                        0
                    )
                    FROM bt_3_agent_voice_sessions
                    WHERE user_id = %s
                      AND source_lang = %s
                      AND target_lang = %s
                      AND started_at < %s
                      AND COALESCE(
                          ended_at,
                          CASE
                              WHEN duration_seconds IS NOT NULL
                                  THEN started_at + (GREATEST(duration_seconds, 0) * INTERVAL '1 second')
                              ELSE started_at
                          END
                      ) > %s;
                    """,
                    (
                        cap_dt,
                        period_start_dt,
                        int(user_id),
                        normalized_source,
                        normalized_target,
                        cap_dt,
                        period_start_dt,
                    ),
                )
                agent_row = cursor.fetchone()
                agent_minutes_actual = float(agent_row[0] or 0.0) / 60.0 if agent_row else 0.0
            else:
                agent_minutes_actual = 0.0

            if cap_dt > period_start_dt:
                cursor.execute(
                    """
                    SELECT COALESCE(
                        SUM(
                            GREATEST(
                                0,
                                EXTRACT(
                                    EPOCH FROM (
                                        LEAST(
                                            COALESCE(
                                                ended_at,
                                                CASE
                                                    WHEN duration_seconds IS NOT NULL
                                                        THEN started_at + (GREATEST(duration_seconds, 0) * INTERVAL '1 second')
                                                    ELSE started_at
                                                END
                                            ),
                                            %s
                                        )
                                        - GREATEST(started_at, %s)
                                    )
                                )
                            )
                        ),
                        0
                    )
                    FROM bt_3_reader_sessions
                    WHERE user_id = %s
                      AND source_lang = %s
                      AND target_lang = %s
                      AND started_at < %s
                      AND COALESCE(
                          ended_at,
                          CASE
                              WHEN duration_seconds IS NOT NULL
                                  THEN started_at + (GREATEST(duration_seconds, 0) * INTERVAL '1 second')
                              ELSE started_at
                          END
                      ) > %s;
                    """,
                    (
                        cap_dt,
                        period_start_dt,
                        int(user_id),
                        normalized_source,
                        normalized_target,
                        cap_dt,
                        period_start_dt,
                    ),
                )
                reading_row = cursor.fetchone()
                reading_minutes_actual = float(reading_row[0] or 0.0) / 60.0 if reading_row else 0.0
            else:
                reading_minutes_actual = 0.0

    def _metric(goal: int, actual: float) -> dict:
        safe_goal = max(0, int(goal or 0))
        safe_actual = max(0.0, float(actual or 0.0))
        forecast = (safe_actual / float(days_elapsed)) * float(days_total)
        expected_to_date = (safe_goal / float(days_total)) * float(days_elapsed)
        completion = (safe_actual / float(safe_goal) * 100.0) if safe_goal > 0 else 0.0
        return {
            "goal": safe_goal,
            "actual": round(safe_actual, 2),
            "forecast": round(forecast, 2),
            "completion_percent": round(completion, 1),
            "delta_vs_goal": round(safe_actual - safe_goal, 2),
            "forecast_delta_vs_goal": round(forecast - safe_goal, 2),
            "expected_to_date": round(expected_to_date, 2),
            "delta_vs_expected": round(safe_actual - expected_to_date, 2),
        }

    return {
        "period": normalized_period,
        "start_date": resolved_start.isoformat(),
        "end_date": resolved_end.isoformat(),
        "as_of_date": effective_end.isoformat(),
        "days_elapsed": days_elapsed,
        "days_total": days_total,
        "source_lang": normalized_source,
        "target_lang": normalized_target,
        "metrics": {
            "translations": _metric(translations_goal, translations_actual),
            "learned_words": _metric(learned_words_goal, learned_words_actual),
            "agent_minutes": _metric(agent_minutes_goal, agent_minutes_actual),
            "reading_minutes": _metric(reading_minutes_goal, reading_minutes_actual),
        },
    }


def get_weekly_plan_progress(
    *,
    user_id: int,
    source_lang: str,
    target_lang: str,
    mature_interval_days: int = 21,
    week_start: date | None = None,
    as_of_date: date | None = None,
) -> dict:
    result = get_plan_progress(
        user_id=user_id,
        source_lang=source_lang,
        target_lang=target_lang,
        mature_interval_days=mature_interval_days,
        period="week",
        week_start=week_start,
        as_of_date=as_of_date,
    )
    return {
        **result,
        "week_start": result.get("start_date"),
        "week_end": result.get("end_date"),
    }


def start_agent_voice_session(
    *,
    user_id: int,
    source_lang: str = "ru",
    target_lang: str = "de",
    started_at: datetime | None = None,
) -> dict:
    normalized_source = str(source_lang or "ru").strip().lower() or "ru"
    normalized_target = str(target_lang or "de").strip().lower() or "de"
    started = started_at or datetime.now(timezone.utc)
    if started.tzinfo is None:
        started = started.replace(tzinfo=timezone.utc)
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE bt_3_agent_voice_sessions
                SET
                    ended_at = %s,
                    duration_seconds = GREATEST(0, EXTRACT(EPOCH FROM (%s - started_at))::INT),
                    updated_at = NOW()
                WHERE user_id = %s
                  AND source_lang = %s
                  AND target_lang = %s
                  AND ended_at IS NULL;
                """,
                (started, started, int(user_id), normalized_source, normalized_target),
            )
            cursor.execute(
                """
                INSERT INTO bt_3_agent_voice_sessions (
                    user_id,
                    source_lang,
                    target_lang,
                    started_at,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, NOW())
                RETURNING id, started_at;
                """,
                (int(user_id), normalized_source, normalized_target, started),
            )
            row = cursor.fetchone()
    return {
        "session_id": int(row[0]),
        "started_at": row[1].isoformat() if row and row[1] else started.isoformat(),
    }


def finish_agent_voice_session(
    *,
    user_id: int,
    session_id: int | None = None,
    source_lang: str = "ru",
    target_lang: str = "de",
    ended_at: datetime | None = None,
) -> dict | None:
    normalized_source = str(source_lang or "ru").strip().lower() or "ru"
    normalized_target = str(target_lang or "de").strip().lower() or "de"
    ended = ended_at or datetime.now(timezone.utc)
    if ended.tzinfo is None:
        ended = ended.replace(tzinfo=timezone.utc)

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            if session_id is not None:
                cursor.execute(
                    """
                    UPDATE bt_3_agent_voice_sessions
                    SET
                        ended_at = COALESCE(ended_at, %s),
                        duration_seconds = CASE
                            WHEN ended_at IS NOT NULL THEN duration_seconds
                            ELSE GREATEST(0, EXTRACT(EPOCH FROM (%s - started_at))::INT)
                        END,
                        updated_at = NOW()
                    WHERE id = %s
                      AND user_id = %s
                    RETURNING id, started_at, ended_at, duration_seconds, source_lang, target_lang;
                    """,
                    (ended, ended, int(session_id), int(user_id)),
                )
            else:
                cursor.execute(
                    """
                    WITH latest AS (
                        SELECT id
                        FROM bt_3_agent_voice_sessions
                        WHERE user_id = %s
                          AND source_lang = %s
                          AND target_lang = %s
                          AND ended_at IS NULL
                        ORDER BY started_at DESC
                        LIMIT 1
                    )
                    UPDATE bt_3_agent_voice_sessions s
                    SET
                        ended_at = %s,
                        duration_seconds = GREATEST(0, EXTRACT(EPOCH FROM (%s - s.started_at))::INT),
                        updated_at = NOW()
                    FROM latest
                    WHERE s.id = latest.id
                    RETURNING s.id, s.started_at, s.ended_at, s.duration_seconds, s.source_lang, s.target_lang;
                    """,
                    (int(user_id), normalized_source, normalized_target, ended, ended),
                )
            row = cursor.fetchone()
    if not row:
        return None
    duration_seconds = int(row[3] or 0)
    return {
        "session_id": int(row[0]),
        "started_at": row[1].isoformat() if row[1] else None,
        "ended_at": row[2].isoformat() if row[2] else ended.isoformat(),
        "duration_seconds": duration_seconds,
        "duration_minutes": round(duration_seconds / 60.0, 2),
        "source_lang": str(row[4] or normalized_source),
        "target_lang": str(row[5] or normalized_target),
    }


def start_reader_session(
    *,
    user_id: int,
    source_lang: str = "ru",
    target_lang: str = "de",
    started_at: datetime | None = None,
) -> dict:
    normalized_source = str(source_lang or "ru").strip().lower() or "ru"
    normalized_target = str(target_lang or "de").strip().lower() or "de"
    started = started_at or datetime.now(timezone.utc)
    if started.tzinfo is None:
        started = started.replace(tzinfo=timezone.utc)
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE bt_3_reader_sessions
                SET
                    ended_at = %s,
                    duration_seconds = GREATEST(0, EXTRACT(EPOCH FROM (%s - started_at))::INT),
                    updated_at = NOW()
                WHERE user_id = %s
                  AND source_lang = %s
                  AND target_lang = %s
                  AND ended_at IS NULL;
                """,
                (started, started, int(user_id), normalized_source, normalized_target),
            )
            cursor.execute(
                """
                INSERT INTO bt_3_reader_sessions (
                    user_id,
                    source_lang,
                    target_lang,
                    started_at,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, NOW())
                RETURNING id, started_at;
                """,
                (int(user_id), normalized_source, normalized_target, started),
            )
            row = cursor.fetchone()
    return {
        "session_id": int(row[0]),
        "started_at": row[1].isoformat() if row and row[1] else started.isoformat(),
    }


def finish_reader_session(
    *,
    user_id: int,
    session_id: int | None = None,
    source_lang: str = "ru",
    target_lang: str = "de",
    ended_at: datetime | None = None,
) -> dict | None:
    normalized_source = str(source_lang or "ru").strip().lower() or "ru"
    normalized_target = str(target_lang or "de").strip().lower() or "de"
    ended = ended_at or datetime.now(timezone.utc)
    if ended.tzinfo is None:
        ended = ended.replace(tzinfo=timezone.utc)

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            if session_id is not None:
                cursor.execute(
                    """
                    UPDATE bt_3_reader_sessions
                    SET
                        ended_at = COALESCE(ended_at, %s),
                        duration_seconds = CASE
                            WHEN ended_at IS NOT NULL THEN duration_seconds
                            ELSE GREATEST(0, EXTRACT(EPOCH FROM (%s - started_at))::INT)
                        END,
                        updated_at = NOW()
                    WHERE id = %s
                      AND user_id = %s
                    RETURNING id, started_at, ended_at, duration_seconds, source_lang, target_lang;
                    """,
                    (ended, ended, int(session_id), int(user_id)),
                )
            else:
                cursor.execute(
                    """
                    WITH latest AS (
                        SELECT id
                        FROM bt_3_reader_sessions
                        WHERE user_id = %s
                          AND source_lang = %s
                          AND target_lang = %s
                          AND ended_at IS NULL
                        ORDER BY started_at DESC
                        LIMIT 1
                    )
                    UPDATE bt_3_reader_sessions s
                    SET
                        ended_at = %s,
                        duration_seconds = GREATEST(0, EXTRACT(EPOCH FROM (%s - s.started_at))::INT),
                        updated_at = NOW()
                    FROM latest
                    WHERE s.id = latest.id
                    RETURNING s.id, s.started_at, s.ended_at, s.duration_seconds, s.source_lang, s.target_lang;
                    """,
                    (int(user_id), normalized_source, normalized_target, ended, ended),
                )
            row = cursor.fetchone()
    if not row:
        return None
    duration_seconds = int(row[3] or 0)
    return {
        "session_id": int(row[0]),
        "started_at": row[1].isoformat() if row[1] else None,
        "ended_at": row[2].isoformat() if row[2] else ended.isoformat(),
        "duration_seconds": duration_seconds,
        "duration_minutes": round(duration_seconds / 60.0, 2),
        "source_lang": str(row[4] or normalized_source),
        "target_lang": str(row[5] or normalized_target),
    }


def _reader_library_row_to_dict(row: tuple, *, include_content: bool = False) -> dict:
    payload = {
        "id": int(row[0]),
        "user_id": int(row[1]),
        "source_lang": str(row[2] or "ru"),
        "target_lang": str(row[3] or "de"),
        "title": str(row[4] or "Untitled"),
        "source_type": str(row[5] or "text"),
        "source_url": row[6] if row[6] else None,
        "text_hash": str(row[7] or ""),
        "total_chars": max(0, int(row[8] or 0)),
        "progress_percent": round(max(0.0, min(100.0, float(row[9] or 0.0))), 2),
        "bookmark_percent": round(max(0.0, min(100.0, float(row[10] or 0.0))), 2),
        "reading_mode": str(row[11] or "vertical"),
        "is_archived": bool(row[12]),
        "archived_at": row[13].isoformat() if row[13] else None,
        "last_opened_at": row[14].isoformat() if row[14] else None,
        "created_at": row[15].isoformat() if row[15] else None,
        "updated_at": row[16].isoformat() if row[16] else None,
    }
    if include_content:
        payload["content_text"] = str(row[17] or "")
        payload["content_pages"] = row[18] if isinstance(row[18], list) else []
    return payload


def upsert_reader_library_document(
    *,
    user_id: int,
    source_lang: str,
    target_lang: str,
    title: str,
    source_type: str,
    source_url: str | None,
    content_text: str,
    content_pages: list | None = None,
) -> dict:
    normalized_source = str(source_lang or "ru").strip().lower() or "ru"
    normalized_target = str(target_lang or "de").strip().lower() or "de"
    resolved_title = str(title or "Untitled").strip() or "Untitled"
    resolved_source_type = str(source_type or "text").strip().lower() or "text"
    resolved_source_url = str(source_url or "").strip() or None
    resolved_content = str(content_text or "").strip()
    resolved_pages = content_pages if isinstance(content_pages, list) else []
    text_hash = hashlib.sha256(resolved_content.encode("utf-8")).hexdigest()
    total_chars = len(resolved_content)

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_reader_library (
                    user_id,
                    source_lang,
                    target_lang,
                    title,
                    source_type,
                    source_url,
                    text_hash,
                    content_text,
                    content_pages,
                    total_chars,
                    last_opened_at,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, NOW(), NOW())
                ON CONFLICT (user_id, source_lang, target_lang, text_hash) DO UPDATE
                SET
                    title = EXCLUDED.title,
                    source_type = EXCLUDED.source_type,
                    source_url = COALESCE(EXCLUDED.source_url, bt_3_reader_library.source_url),
                    content_text = EXCLUDED.content_text,
                    content_pages = EXCLUDED.content_pages,
                    total_chars = EXCLUDED.total_chars,
                    is_archived = FALSE,
                    archived_at = NULL,
                    last_opened_at = NOW(),
                    updated_at = NOW()
                RETURNING
                    id, user_id, source_lang, target_lang, title, source_type, source_url,
                    text_hash, total_chars, progress_percent, bookmark_percent, reading_mode,
                    is_archived, archived_at, last_opened_at, created_at, updated_at, content_text, content_pages;
                """,
                (
                    int(user_id),
                    normalized_source,
                    normalized_target,
                    resolved_title,
                    resolved_source_type,
                    resolved_source_url,
                    text_hash,
                    resolved_content,
                    json.dumps(resolved_pages, ensure_ascii=False),
                    total_chars,
                ),
            )
            row = cursor.fetchone()
    return _reader_library_row_to_dict(row, include_content=True)


def list_reader_library_documents(
    *,
    user_id: int,
    source_lang: str,
    target_lang: str,
    limit: int = 100,
    include_archived: bool = False,
) -> list[dict]:
    normalized_source = str(source_lang or "ru").strip().lower() or "ru"
    normalized_target = str(target_lang or "de").strip().lower() or "de"
    safe_limit = max(1, min(300, int(limit or 100)))
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    id, user_id, source_lang, target_lang, title, source_type, source_url,
                    text_hash, total_chars, progress_percent, bookmark_percent, reading_mode,
                    is_archived, archived_at, last_opened_at, created_at, updated_at
                FROM bt_3_reader_library
                WHERE user_id = %s
                  AND source_lang = %s
                  AND target_lang = %s
                  AND (%s OR COALESCE(is_archived, FALSE) = FALSE)
                ORDER BY COALESCE(last_opened_at, updated_at, created_at) DESC
                LIMIT %s;
                """,
                (int(user_id), normalized_source, normalized_target, bool(include_archived), safe_limit),
            )
            rows = cursor.fetchall()
    return [_reader_library_row_to_dict(row, include_content=False) for row in rows]


def get_reader_library_document(
    *,
    user_id: int,
    document_id: int,
    source_lang: str,
    target_lang: str,
) -> dict | None:
    normalized_source = str(source_lang or "ru").strip().lower() or "ru"
    normalized_target = str(target_lang or "de").strip().lower() or "de"
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    id, user_id, source_lang, target_lang, title, source_type, source_url,
                    text_hash, total_chars, progress_percent, bookmark_percent, reading_mode,
                    is_archived, archived_at, last_opened_at, created_at, updated_at, content_text
                    , content_pages
                FROM bt_3_reader_library
                WHERE id = %s
                  AND user_id = %s
                  AND source_lang = %s
                  AND target_lang = %s
                LIMIT 1;
                """,
                (int(document_id), int(user_id), normalized_source, normalized_target),
            )
            row = cursor.fetchone()
            if not row:
                return None
            cursor.execute(
                """
                UPDATE bt_3_reader_library
                SET last_opened_at = NOW(), updated_at = NOW()
                WHERE id = %s;
                """,
                (int(document_id),),
            )
    return _reader_library_row_to_dict(row, include_content=True)


def update_reader_library_state(
    *,
    user_id: int,
    document_id: int,
    source_lang: str,
    target_lang: str,
    progress_percent: float | None = None,
    bookmark_percent: float | None = None,
    reading_mode: str | None = None,
) -> dict | None:
    normalized_source = str(source_lang or "ru").strip().lower() or "ru"
    normalized_target = str(target_lang or "de").strip().lower() or "de"
    resolved_progress = None if progress_percent is None else max(0.0, min(100.0, float(progress_percent)))
    resolved_bookmark = None if bookmark_percent is None else max(0.0, min(100.0, float(bookmark_percent)))
    resolved_mode = str(reading_mode or "").strip().lower()
    if resolved_mode not in {"", "vertical", "horizontal"}:
        resolved_mode = ""

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE bt_3_reader_library
                SET
                    progress_percent = COALESCE(%s, progress_percent),
                    bookmark_percent = COALESCE(%s, bookmark_percent),
                    reading_mode = COALESCE(NULLIF(%s, ''), reading_mode),
                    last_opened_at = NOW(),
                    updated_at = NOW()
                WHERE id = %s
                  AND user_id = %s
                  AND source_lang = %s
                  AND target_lang = %s
                RETURNING
                    id, user_id, source_lang, target_lang, title, source_type, source_url,
                    text_hash, total_chars, progress_percent, bookmark_percent, reading_mode,
                    is_archived, archived_at, last_opened_at, created_at, updated_at;
                """,
                (
                    resolved_progress,
                    resolved_bookmark,
                    resolved_mode,
                    int(document_id),
                    int(user_id),
                    normalized_source,
                    normalized_target,
                ),
            )
            row = cursor.fetchone()
    if not row:
        return None
    return _reader_library_row_to_dict(row, include_content=False)


def rename_reader_library_document(
    *,
    user_id: int,
    document_id: int,
    source_lang: str,
    target_lang: str,
    title: str,
) -> dict | None:
    normalized_source = str(source_lang or "ru").strip().lower() or "ru"
    normalized_target = str(target_lang or "de").strip().lower() or "de"
    resolved_title = str(title or "").strip()
    if not resolved_title:
        return None
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE bt_3_reader_library
                SET title = %s, updated_at = NOW()
                WHERE id = %s
                  AND user_id = %s
                  AND source_lang = %s
                  AND target_lang = %s
                RETURNING
                    id, user_id, source_lang, target_lang, title, source_type, source_url,
                    text_hash, total_chars, progress_percent, bookmark_percent, reading_mode,
                    is_archived, archived_at, last_opened_at, created_at, updated_at;
                """,
                (resolved_title, int(document_id), int(user_id), normalized_source, normalized_target),
            )
            row = cursor.fetchone()
    if not row:
        return None
    return _reader_library_row_to_dict(row, include_content=False)


def archive_reader_library_document(
    *,
    user_id: int,
    document_id: int,
    source_lang: str,
    target_lang: str,
    archived: bool = True,
) -> dict | None:
    normalized_source = str(source_lang or "ru").strip().lower() or "ru"
    normalized_target = str(target_lang or "de").strip().lower() or "de"
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE bt_3_reader_library
                SET
                    is_archived = %s,
                    archived_at = CASE WHEN %s THEN NOW() ELSE NULL END,
                    updated_at = NOW()
                WHERE id = %s
                  AND user_id = %s
                  AND source_lang = %s
                  AND target_lang = %s
                RETURNING
                    id, user_id, source_lang, target_lang, title, source_type, source_url,
                    text_hash, total_chars, progress_percent, bookmark_percent, reading_mode,
                    is_archived, archived_at, last_opened_at, created_at, updated_at;
                """,
                (bool(archived), bool(archived), int(document_id), int(user_id), normalized_source, normalized_target),
            )
            row = cursor.fetchone()
    if not row:
        return None
    return _reader_library_row_to_dict(row, include_content=False)


def delete_reader_library_document(
    *,
    user_id: int,
    document_id: int,
    source_lang: str,
    target_lang: str,
) -> bool:
    normalized_source = str(source_lang or "ru").strip().lower() or "ru"
    normalized_target = str(target_lang or "de").strip().lower() or "de"
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                DELETE FROM bt_3_reader_library
                WHERE id = %s
                  AND user_id = %s
                  AND source_lang = %s
                  AND target_lang = %s;
                """,
                (int(document_id), int(user_id), normalized_source, normalized_target),
            )
            deleted = cursor.rowcount > 0
    return bool(deleted)


def get_daily_plan(user_id: int, plan_date: date) -> dict | None:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT id, user_id, plan_date, total_minutes, created_at
                FROM bt_3_daily_plans
                WHERE user_id = %s AND plan_date = %s
                LIMIT 1;
                """,
                (int(user_id), plan_date),
            )
            row = cursor.fetchone()
            if not row:
                return None

            plan_id = int(row[0])
            cursor.execute(
                """
                SELECT
                    id,
                    plan_id,
                    order_index,
                    task_type,
                    title,
                    estimated_minutes,
                    payload,
                    status,
                    completed_at
                FROM bt_3_daily_plan_items
                WHERE plan_id = %s
                ORDER BY order_index ASC, id ASC;
                """,
                (plan_id,),
            )
            items = [_map_daily_plan_item(item_row) for item_row in cursor.fetchall()]

    return {
        "id": plan_id,
        "user_id": int(row[1]),
        "plan_date": row[2].isoformat() if row[2] else None,
        "total_minutes": int(row[3] or 0),
        "created_at": row[4].isoformat() if row[4] else None,
        "items": items,
    }


def create_daily_plan(
    user_id: int,
    plan_date: date,
    total_minutes: int,
    items: list[dict],
) -> dict:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_daily_plans (user_id, plan_date, total_minutes)
                VALUES (%s, %s, %s)
                ON CONFLICT (user_id, plan_date) DO UPDATE
                SET total_minutes = EXCLUDED.total_minutes
                RETURNING id;
                """,
                (int(user_id), plan_date, max(0, int(total_minutes))),
            )
            plan_id = int(cursor.fetchone()[0])

            cursor.execute(
                """
                DELETE FROM bt_3_daily_plan_items
                WHERE plan_id = %s;
                """,
                (plan_id,),
            )

            for index, item in enumerate(items):
                payload = item.get("payload") if isinstance(item, dict) else {}
                if not isinstance(payload, dict):
                    payload = {}
                cursor.execute(
                    """
                    INSERT INTO bt_3_daily_plan_items (
                        plan_id,
                        order_index,
                        task_type,
                        title,
                        estimated_minutes,
                        payload,
                        status
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s);
                    """,
                    (
                        plan_id,
                        int(item.get("order_index", index)),
                        str(item.get("task_type") or "task"),
                        str(item.get("title") or "Задача"),
                        max(0, int(item.get("estimated_minutes", 0))),
                        json.dumps(payload, ensure_ascii=False),
                        str(item.get("status") or "todo"),
                    ),
                )

    return get_daily_plan(user_id=user_id, plan_date=plan_date) or {
        "id": plan_id,
        "user_id": int(user_id),
        "plan_date": plan_date.isoformat(),
        "total_minutes": max(0, int(total_minutes)),
        "created_at": None,
        "items": [],
    }


def update_daily_plan_item_status(
    *,
    user_id: int,
    item_id: int,
    status: str,
) -> dict | None:
    normalized = str(status or "").strip().lower()
    if normalized not in {"todo", "doing", "done", "skipped"}:
        raise ValueError("status must be one of: todo, doing, done, skipped")

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE bt_3_daily_plan_items i
                SET
                    payload = CASE
                        WHEN %s = 'done' THEN
                            COALESCE(i.payload, '{}'::jsonb)
                            || jsonb_build_object(
                                'timer_running', false,
                                'timer_paused', false,
                                'timer_started_at', NULL,
                                'timer_updated_at', NOW() AT TIME ZONE 'UTC'
                            )
                        ELSE COALESCE(i.payload, '{}'::jsonb)
                    END,
                    status = %s,
                    completed_at = CASE
                        WHEN %s = 'done' THEN NOW()
                        ELSE NULL
                    END
                FROM bt_3_daily_plans p
                WHERE i.id = %s
                  AND i.plan_id = p.id
                  AND p.user_id = %s
                RETURNING
                    i.id,
                    i.plan_id,
                    i.order_index,
                    i.task_type,
                    i.title,
                    i.estimated_minutes,
                    i.payload,
                    i.status,
                    i.completed_at;
                """,
                (normalized, normalized, normalized, int(item_id), int(user_id)),
            )
            row = cursor.fetchone()
            if not row:
                return None
            return _map_daily_plan_item(row)


def update_daily_plan_item_payload(
    *,
    user_id: int,
    item_id: int,
    payload_updates: dict,
) -> dict | None:
    updates = payload_updates if isinstance(payload_updates, dict) else {}
    if not updates:
        return None

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE bt_3_daily_plan_items i
                SET payload = COALESCE(i.payload, '{}'::jsonb) || %s::jsonb
                FROM bt_3_daily_plans p
                WHERE i.id = %s
                  AND i.plan_id = p.id
                  AND p.user_id = %s
                RETURNING
                    i.id,
                    i.plan_id,
                    i.order_index,
                    i.task_type,
                    i.title,
                    i.estimated_minutes,
                    i.payload,
                    i.status,
                    i.completed_at;
                """,
                (json.dumps(updates, ensure_ascii=False), int(item_id), int(user_id)),
            )
            row = cursor.fetchone()
            if not row:
                return None
            return _map_daily_plan_item(row)


def update_daily_plan_item_timer(
    *,
    user_id: int,
    item_id: int,
    action: str,
    elapsed_seconds: int | None = None,
    running: bool | None = None,
    event_at: datetime | None = None,
) -> dict | None:
    normalized_action = str(action or "").strip().lower()
    if normalized_action not in {"start", "pause", "resume", "sync"}:
        raise ValueError("action must be one of: start, pause, resume, sync")

    def _parse_iso_dt(raw: object) -> datetime | None:
        text = str(raw or "").strip()
        if not text:
            return None
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except Exception:
            return None

    now_utc = event_at or datetime.now(timezone.utc)
    if now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=timezone.utc)
    else:
        now_utc = now_utc.astimezone(timezone.utc)
    safe_elapsed_override = None if elapsed_seconds is None else max(0, int(elapsed_seconds))

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    i.id,
                    i.plan_id,
                    i.order_index,
                    i.task_type,
                    i.title,
                    i.estimated_minutes,
                    i.payload,
                    i.status,
                    i.completed_at
                FROM bt_3_daily_plan_items i
                JOIN bt_3_daily_plans p ON p.id = i.plan_id
                WHERE i.id = %s
                  AND p.user_id = %s
                LIMIT 1;
                """,
                (int(item_id), int(user_id)),
            )
            row = cursor.fetchone()
            if not row:
                return None

            current_item = _map_daily_plan_item(row)
            current_payload = current_item["payload"] if isinstance(current_item["payload"], dict) else {}
            estimated_minutes = max(0, int(current_item.get("estimated_minutes") or 0))
            goal_seconds = estimated_minutes * 60

            stored_elapsed = max(0, int(current_payload.get("timer_seconds") or 0))
            stored_started_at = _parse_iso_dt(current_payload.get("timer_started_at"))
            stored_running = bool(current_payload.get("timer_running")) and stored_started_at is not None
            live_elapsed = stored_elapsed
            if stored_running and stored_started_at:
                live_elapsed += max(0, int((now_utc - stored_started_at).total_seconds()))

            if safe_elapsed_override is not None:
                live_elapsed = max(live_elapsed, safe_elapsed_override)

            next_elapsed = max(0, int(live_elapsed))
            next_running = stored_running
            next_paused = bool(current_payload.get("timer_paused")) and not stored_running
            next_started_at = stored_started_at if stored_running else None
            next_status = str(current_item.get("status") or "todo").lower()

            if normalized_action == "start":
                if next_status != "done":
                    next_status = "doing"
                    next_running = True
                    next_paused = False
                    next_started_at = now_utc
            elif normalized_action == "resume":
                if next_status != "done":
                    next_status = "doing"
                    if not next_running:
                        next_running = True
                        next_started_at = now_utc
                    next_paused = False
            elif normalized_action == "pause":
                next_running = False
                next_paused = next_status != "done"
                next_started_at = None
            elif normalized_action == "sync":
                if running is not None:
                    if bool(running) and next_status != "done":
                        next_running = True
                        next_paused = False
                        if not next_started_at:
                            next_started_at = now_utc
                    else:
                        next_running = False
                        next_paused = next_status != "done"
                        next_started_at = None

            progress_percent = 0.0
            if goal_seconds > 0:
                progress_percent = min(100.0, (float(next_elapsed) / float(goal_seconds)) * 100.0)
            elif next_elapsed > 0:
                progress_percent = 100.0

            if goal_seconds > 0 and next_elapsed >= goal_seconds:
                next_status = "done"
                next_running = False
                next_paused = False
                next_started_at = None
            elif next_status != "done" and next_elapsed > 0 and next_status in {"todo", "skipped"}:
                next_status = "doing"

            payload_next = {
                **current_payload,
                "timer_seconds": int(next_elapsed),
                "timer_goal_seconds": int(goal_seconds),
                "timer_progress_percent": round(progress_percent, 2),
                "timer_running": bool(next_running),
                "timer_paused": bool(next_paused),
                "timer_started_at": next_started_at.isoformat() if next_started_at else None,
                "timer_updated_at": now_utc.isoformat(),
            }

            cursor.execute(
                """
                UPDATE bt_3_daily_plan_items i
                SET
                    payload = %s::jsonb,
                    status = %s,
                    completed_at = CASE
                        WHEN %s = 'done' THEN NOW()
                        ELSE NULL
                    END
                FROM bt_3_daily_plans p
                WHERE i.id = %s
                  AND i.plan_id = p.id
                  AND p.user_id = %s
                RETURNING
                    i.id,
                    i.plan_id,
                    i.order_index,
                    i.task_type,
                    i.title,
                    i.estimated_minutes,
                    i.payload,
                    i.status,
                    i.completed_at;
                """,
                (
                    json.dumps(payload_next, ensure_ascii=False),
                    next_status,
                    next_status,
                    int(item_id),
                    int(user_id),
                ),
            )
            saved = cursor.fetchone()
            if not saved:
                return None
            return _map_daily_plan_item(saved)


def _normalize_focus_part(value: str | None) -> str:
    return " ".join(str(value or "").strip().lower().split())


def _build_video_focus_key(
    *,
    source_lang: str,
    target_lang: str,
    skill_id: str | None,
    main_category: str | None,
    sub_category: str | None,
) -> str:
    src = _normalize_focus_part(source_lang) or "ru"
    tgt = _normalize_focus_part(target_lang) or "de"
    skill = _normalize_focus_part(skill_id) or "-"
    main = _normalize_focus_part(main_category) or "-"
    sub = _normalize_focus_part(sub_category) or "-"
    return f"{src}|{tgt}|{skill}|{main}|{sub}"


def _map_video_recommendation_row(row: tuple) -> dict:
    return {
        "id": int(row[0]),
        "source_lang": str(row[1] or "ru"),
        "target_lang": str(row[2] or "de"),
        "focus_key": str(row[3] or ""),
        "skill_id": str(row[4] or "") or None,
        "main_category": str(row[5] or "") or None,
        "sub_category": str(row[6] or "") or None,
        "search_query": str(row[7] or "") or None,
        "video_id": str(row[8] or ""),
        "video_url": str(row[9] or "") or None,
        "video_title": str(row[10] or "") or None,
        "like_count": int(row[11] or 0),
        "dislike_count": int(row[12] or 0),
        "score": int(row[13] or 0),
        "is_active": bool(row[14]),
        "updated_at": row[15].isoformat() if row[15] else None,
        "last_selected_at": row[16].isoformat() if row[16] else None,
    }


def get_best_video_recommendation_for_focus(
    *,
    source_lang: str,
    target_lang: str,
    skill_id: str | None,
    main_category: str | None,
    sub_category: str | None,
) -> dict | None:
    focus_key = _build_video_focus_key(
        source_lang=source_lang,
        target_lang=target_lang,
        skill_id=skill_id,
        main_category=main_category,
        sub_category=sub_category,
    )
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    id, source_lang, target_lang, focus_key, skill_id, main_category, sub_category,
                    search_query, video_id, video_url, video_title, like_count, dislike_count,
                    score, is_active, updated_at, last_selected_at
                FROM bt_3_video_recommendations
                WHERE focus_key = %s
                  AND is_active = TRUE
                ORDER BY score DESC, like_count DESC, updated_at DESC
                LIMIT 1;
                """,
                (focus_key,),
            )
            row = cursor.fetchone()
            if not row:
                return None
            cursor.execute(
                """
                UPDATE bt_3_video_recommendations
                SET last_selected_at = NOW(), updated_at = NOW()
                WHERE id = %s;
                """,
                (int(row[0]),),
            )
    return _map_video_recommendation_row(row)


def upsert_video_recommendation(
    *,
    source_lang: str,
    target_lang: str,
    skill_id: str | None,
    main_category: str | None,
    sub_category: str | None,
    search_query: str | None,
    video_id: str,
    video_url: str | None,
    video_title: str | None,
) -> dict | None:
    resolved_video_id = str(video_id or "").strip()
    if not resolved_video_id:
        return None
    normalized_source = str(source_lang or "ru").strip().lower() or "ru"
    normalized_target = str(target_lang or "de").strip().lower() or "de"
    focus_key = _build_video_focus_key(
        source_lang=normalized_source,
        target_lang=normalized_target,
        skill_id=skill_id,
        main_category=main_category,
        sub_category=sub_category,
    )
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_video_recommendations (
                    source_lang,
                    target_lang,
                    focus_key,
                    skill_id,
                    main_category,
                    sub_category,
                    search_query,
                    video_id,
                    video_url,
                    video_title,
                    is_active,
                    last_selected_at,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, TRUE, NOW(), NOW())
                ON CONFLICT (focus_key, video_id) DO UPDATE
                SET
                    source_lang = EXCLUDED.source_lang,
                    target_lang = EXCLUDED.target_lang,
                    skill_id = COALESCE(EXCLUDED.skill_id, bt_3_video_recommendations.skill_id),
                    main_category = COALESCE(EXCLUDED.main_category, bt_3_video_recommendations.main_category),
                    sub_category = COALESCE(EXCLUDED.sub_category, bt_3_video_recommendations.sub_category),
                    search_query = COALESCE(EXCLUDED.search_query, bt_3_video_recommendations.search_query),
                    video_url = COALESCE(EXCLUDED.video_url, bt_3_video_recommendations.video_url),
                    video_title = COALESCE(EXCLUDED.video_title, bt_3_video_recommendations.video_title),
                    is_active = TRUE,
                    last_selected_at = NOW(),
                    updated_at = NOW()
                RETURNING
                    id, source_lang, target_lang, focus_key, skill_id, main_category, sub_category,
                    search_query, video_id, video_url, video_title, like_count, dislike_count,
                    score, is_active, updated_at, last_selected_at;
                """,
                (
                    normalized_source,
                    normalized_target,
                    focus_key,
                    str(skill_id or "").strip() or None,
                    str(main_category or "").strip() or None,
                    str(sub_category or "").strip() or None,
                    str(search_query or "").strip() or None,
                    resolved_video_id,
                    str(video_url or "").strip() or None,
                    str(video_title or "").strip() or None,
                ),
            )
            row = cursor.fetchone()
    return _map_video_recommendation_row(row) if row else None


def get_video_recommendation_by_id(recommendation_id: int) -> dict | None:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    id, source_lang, target_lang, focus_key, skill_id, main_category, sub_category,
                    search_query, video_id, video_url, video_title, like_count, dislike_count,
                    score, is_active, updated_at, last_selected_at
                FROM bt_3_video_recommendations
                WHERE id = %s
                LIMIT 1;
                """,
                (int(recommendation_id),),
            )
            row = cursor.fetchone()
    return _map_video_recommendation_row(row) if row else None


def vote_video_recommendation(
    *,
    user_id: int,
    recommendation_id: int,
    vote: int,
) -> dict | None:
    normalized_vote = 1 if int(vote) >= 0 else -1
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_video_recommendation_votes (
                    recommendation_id, user_id, vote, updated_at
                )
                VALUES (%s, %s, %s, NOW())
                ON CONFLICT (recommendation_id, user_id) DO UPDATE
                SET vote = EXCLUDED.vote, updated_at = NOW();
                """,
                (int(recommendation_id), int(user_id), int(normalized_vote)),
            )
            cursor.execute(
                """
                WITH agg AS (
                    SELECT
                        recommendation_id,
                        COUNT(*) FILTER (WHERE vote = 1) AS like_count,
                        COUNT(*) FILTER (WHERE vote = -1) AS dislike_count
                    FROM bt_3_video_recommendation_votes
                    WHERE recommendation_id = %s
                    GROUP BY recommendation_id
                )
                UPDATE bt_3_video_recommendations r
                SET
                    like_count = COALESCE(agg.like_count, 0),
                    dislike_count = COALESCE(agg.dislike_count, 0),
                    score = COALESCE(agg.like_count, 0) - COALESCE(agg.dislike_count, 0),
                    is_active = (COALESCE(agg.like_count, 0) - COALESCE(agg.dislike_count, 0)) >= 0,
                    updated_at = NOW()
                FROM agg
                WHERE r.id = agg.recommendation_id
                  AND r.id = %s
                RETURNING
                    r.id, r.source_lang, r.target_lang, r.focus_key, r.skill_id, r.main_category, r.sub_category,
                    r.search_query, r.video_id, r.video_url, r.video_title, r.like_count, r.dislike_count,
                    r.score, r.is_active, r.updated_at, r.last_selected_at;
                """,
                (int(recommendation_id), int(recommendation_id)),
            )
            row = cursor.fetchone()
            if not row:
                return None
    result = _map_video_recommendation_row(row)
    result["user_vote"] = int(normalized_vote)
    return result


def consume_today_regenerate_limit(
    *,
    user_id: int,
    limit_date: date,
) -> dict:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_today_regenerate_limits (user_id, limit_date)
                VALUES (%s, %s)
                ON CONFLICT (user_id, limit_date) DO NOTHING
                RETURNING consumed_at;
                """,
                (int(user_id), limit_date),
            )
            row = cursor.fetchone()
            if row:
                return {
                    "allowed": True,
                    "consumed_at": row[0].isoformat() if row[0] else None,
                }

            cursor.execute(
                """
                SELECT consumed_at
                FROM bt_3_today_regenerate_limits
                WHERE user_id = %s AND limit_date = %s
                LIMIT 1;
                """,
                (int(user_id), limit_date),
            )
            existing = cursor.fetchone()
    return {
        "allowed": False,
        "consumed_at": existing[0].isoformat() if existing and existing[0] else None,
    }


def get_top_weak_topic(
    *,
    user_id: int,
    lookback_days: int = 7,
    source_lang: str | None = None,
    target_lang: str | None = None,
) -> dict | None:
    lookback_days = max(1, int(lookback_days))
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        COALESCE(NULLIF(dm.main_category, ''), 'Other mistake') AS main_category,
                        COALESCE(NULLIF(dm.sub_category, ''), 'Unclassified mistake') AS sub_category,
                        SUM(COALESCE(dm.mistake_count, 1)) AS total_mistakes
                    FROM bt_3_detailed_mistakes dm
                    JOIN bt_3_daily_sentences ds
                      ON ds.id_for_mistake_table = dm.sentence_id
                     AND ds.user_id = dm.user_id
                    WHERE dm.user_id = %s
                      AND COALESCE(ds.source_lang, 'ru') = COALESCE(%s, 'ru')
                      AND COALESCE(ds.target_lang, 'de') = COALESCE(%s, 'de')
                      AND COALESCE(dm.last_seen, dm.added_data, NOW()) >= NOW() - (%s::text || ' days')::interval
                    GROUP BY 1, 2
                    ORDER BY total_mistakes DESC, main_category ASC, sub_category ASC
                    LIMIT 1;
                    """,
                    (int(user_id), source_lang or "ru", target_lang or "de", lookback_days),
                )
                row = cursor.fetchone()
    except Exception:
        return None

    if not row:
        return None
    return {
        "main_category": row[0],
        "sub_category": row[1],
        "mistakes": int(row[2] or 0),
    }


def get_weak_topic_sentences(
    *,
    user_id: int,
    main_category: str,
    sub_category: str,
    lookback_days: int = 7,
    limit: int = 5,
    source_lang: str | None = None,
    target_lang: str | None = None,
) -> list[str]:
    if not main_category and not sub_category:
        return []
    lookback_days = max(1, int(lookback_days))
    limit = max(1, min(int(limit), 20))
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT dm.sentence
                    FROM bt_3_detailed_mistakes dm
                    JOIN bt_3_daily_sentences ds
                      ON ds.id_for_mistake_table = dm.sentence_id
                     AND ds.user_id = dm.user_id
                    WHERE dm.user_id = %s
                      AND COALESCE(ds.source_lang, 'ru') = COALESCE(%s, 'ru')
                      AND COALESCE(ds.target_lang, 'de') = COALESCE(%s, 'de')
                      AND COALESCE(NULLIF(dm.main_category, ''), 'Other mistake') = %s
                      AND COALESCE(NULLIF(dm.sub_category, ''), 'Unclassified mistake') = %s
                      AND COALESCE(dm.last_seen, dm.added_data, NOW()) >= NOW() - (%s::text || ' days')::interval
                      AND dm.sentence IS NOT NULL
                      AND dm.sentence <> ''
                    ORDER BY COALESCE(dm.last_seen, dm.added_data, NOW()) DESC, COALESCE(dm.mistake_count, 1) DESC
                    LIMIT %s;
                    """,
                    (
                        int(user_id),
                        source_lang or "ru",
                        target_lang or "de",
                        main_category,
                        sub_category,
                        lookback_days,
                        limit,
                    ),
                )
                rows = cursor.fetchall()
    except Exception:
        return []
    return [str(row[0]).strip() for row in rows if row and str(row[0]).strip()]


def get_recent_mistake_examples_for_topic(
    *,
    user_id: int,
    main_category: str,
    sub_category: str,
    lookback_days: int = 14,
    limit: int = 5,
    source_lang: str | None = None,
    target_lang: str | None = None,
) -> list[dict]:
    if not main_category and not sub_category:
        return []
    lookback_days = max(1, int(lookback_days))
    limit = max(1, min(int(limit), 20))
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        COALESCE(ds.sentence, '') AS source_sentence,
                        COALESCE(latest_tr.user_translation, '') AS user_translation,
                        COALESCE(latest_tr.feedback, '') AS feedback
                    FROM bt_3_detailed_mistakes dm
                    JOIN bt_3_daily_sentences ds
                      ON ds.id_for_mistake_table = dm.sentence_id
                     AND ds.user_id = dm.user_id
                    LEFT JOIN LATERAL (
                        SELECT tr.user_translation, tr.feedback
                        FROM bt_3_translations tr
                        WHERE tr.user_id = dm.user_id
                          AND tr.id_for_mistake_table = dm.sentence_id
                          AND COALESCE(tr.source_lang, 'ru') = COALESCE(%s, 'ru')
                          AND COALESCE(tr.target_lang, 'de') = COALESCE(%s, 'de')
                        ORDER BY tr.timestamp DESC
                        LIMIT 1
                    ) latest_tr ON TRUE
                    WHERE dm.user_id = %s
                      AND COALESCE(ds.source_lang, 'ru') = COALESCE(%s, 'ru')
                      AND COALESCE(ds.target_lang, 'de') = COALESCE(%s, 'de')
                      AND COALESCE(NULLIF(dm.main_category, ''), 'Other mistake') = %s
                      AND COALESCE(NULLIF(dm.sub_category, ''), 'Unclassified mistake') = %s
                      AND COALESCE(dm.last_seen, dm.added_data, NOW()) >= NOW() - (%s::text || ' days')::interval
                    ORDER BY COALESCE(dm.last_seen, dm.added_data, NOW()) DESC, COALESCE(dm.mistake_count, 1) DESC
                    LIMIT %s;
                    """,
                    (
                        source_lang or "ru",
                        target_lang or "de",
                        int(user_id),
                        source_lang or "ru",
                        target_lang or "de",
                        main_category,
                        sub_category,
                        lookback_days,
                        limit,
                    ),
                )
                rows = cursor.fetchall()
    except Exception:
        return []
    result: list[dict] = []
    for row in rows:
        source_sentence = str(row[0] or "").strip()
        user_translation = str(row[1] or "").strip()
        feedback = str(row[2] or "").strip()
        if not source_sentence and not user_translation:
            continue
        result.append(
            {
                "source_sentence": source_sentence,
                "user_translation": user_translation,
                "feedback": feedback,
            }
        )
    return result


def get_lowest_mastery_skill(
    user_id: int,
    *,
    source_lang: str | None = None,
    target_lang: str | None = None,
) -> dict | None:
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        s.skill_id,
                        k.title,
                        k.category,
                        s.mastery,
                        s.total_events,
                        s.updated_at
                    FROM bt_3_user_skill_state s
                    JOIN bt_3_skills k ON k.skill_id = s.skill_id
                    WHERE s.user_id = %s
                      AND s.source_lang = COALESCE(%s, 'ru')
                      AND s.target_lang = COALESCE(%s, 'de')
                      AND k.language_code = COALESCE(%s, 'de')
                    ORDER BY s.mastery ASC, s.total_events DESC, s.updated_at DESC
                    LIMIT 1;
                    """,
                    (
                        int(user_id),
                        source_lang or "ru",
                        target_lang or "de",
                        target_lang or "de",
                    ),
                )
                row = cursor.fetchone()
    except Exception:
        return None

    if not row:
        return None
    return {
        "skill_id": str(row[0]),
        "skill_title": str(row[1] or ""),
        "skill_category": str(row[2] or ""),
        "mastery": float(row[3] or 0.0),
        "total_events": int(row[4] or 0),
        "updated_at": row[5].isoformat() if row[5] else None,
    }


def get_top_error_topic_for_skill(
    *,
    user_id: int,
    skill_id: str,
    lookback_days: int = 7,
    source_lang: str | None = None,
    target_lang: str | None = None,
) -> dict | None:
    if not skill_id:
        return None
    lookback_days = max(1, int(lookback_days))
    normalized_skill_id = str(skill_id).strip()
    normalized_target_lang = str(target_lang or "de").strip().lower() or "de"
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        COALESCE(NULLIF(dm.main_category, ''), 'Other mistake') AS main_category,
                        COALESCE(NULLIF(dm.sub_category, ''), 'Unclassified mistake') AS sub_category,
                        SUM(COALESCE(dm.mistake_count, 1)) AS total_mistakes,
                        MAX(m.weight) AS map_weight
                    FROM bt_3_detailed_mistakes dm
                    JOIN bt_3_daily_sentences ds
                      ON ds.id_for_mistake_table = dm.sentence_id
                     AND ds.user_id = dm.user_id
                    JOIN bt_3_error_skill_map m
                      ON m.error_category = COALESCE(NULLIF(dm.main_category, ''), 'Other mistake')
                     AND m.error_subcategory = COALESCE(NULLIF(dm.sub_category, ''), 'Unclassified mistake')
                    WHERE dm.user_id = %s
                      AND m.language_code = %s
                      AND COALESCE(ds.source_lang, 'ru') = COALESCE(%s, 'ru')
                      AND COALESCE(ds.target_lang, 'de') = COALESCE(%s, 'de')
                      AND m.skill_id = %s
                      AND COALESCE(dm.last_seen, dm.added_data, NOW()) >= NOW() - (%s::text || ' days')::interval
                    GROUP BY 1, 2
                    ORDER BY total_mistakes DESC, map_weight DESC, main_category ASC, sub_category ASC
                    LIMIT 1;
                    """,
                    (
                        int(user_id),
                        normalized_target_lang,
                        source_lang or "ru",
                        normalized_target_lang,
                        normalized_skill_id,
                        lookback_days,
                    ),
                )
                row = cursor.fetchone()
                if row:
                    return {
                        "main_category": str(row[0] or "Other mistake"),
                        "sub_category": str(row[1] or "Unclassified mistake"),
                        "mistakes": int(row[2] or 0),
                        "map_weight": float(row[3] or 1.0),
                    }

                cursor.execute(
                    """
                    SELECT error_category, error_subcategory, weight
                    FROM bt_3_error_skill_map
                    WHERE skill_id = %s
                      AND language_code = %s
                    ORDER BY weight DESC, error_category ASC, error_subcategory ASC
                    LIMIT 1;
                    """,
                    (normalized_skill_id, normalized_target_lang),
                )
                fallback = cursor.fetchone()
    except Exception:
        return None

    if not fallback:
        return None
    return {
        "main_category": str(fallback[0] or "Other mistake"),
        "sub_category": str(fallback[1] or "Unclassified mistake"),
        "mistakes": 0,
        "map_weight": float(fallback[2] or 1.0),
    }


def list_default_topics_for_user(
    *,
    user_id: int,
    target_language: str,
    limit: int = 100,
) -> list[dict]:
    normalized_target = str(target_language or "de").strip().lower() or "de"
    safe_limit = max(1, min(int(limit or 100), 300))
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT topic_name, error_category, error_subcategory, skill_id, created_at
                FROM bt_3_default_topics
                WHERE user_id = %s
                  AND target_language = %s
                ORDER BY created_at DESC
                LIMIT %s;
                """,
                (int(user_id), normalized_target, safe_limit),
            )
            rows = cursor.fetchall()
    return [
        {
            "topic_name": str(row[0] or ""),
            "error_category": str(row[1] or "") or None,
            "error_subcategory": str(row[2] or "") or None,
            "skill_id": str(row[3] or "") or None,
            "created_at": row[4].isoformat() if row[4] else None,
        }
        for row in rows
    ]


def add_default_topic_for_user(
    *,
    user_id: int,
    target_language: str,
    topic_name: str,
    error_category: str | None = None,
    error_subcategory: str | None = None,
    skill_id: str | None = None,
) -> dict | None:
    normalized_target = str(target_language or "de").strip().lower() or "de"
    resolved_topic = str(topic_name or "").strip()
    if not resolved_topic:
        return None
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_default_topics (
                    user_id,
                    target_language,
                    topic_name,
                    error_category,
                    error_subcategory,
                    skill_id
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (user_id, target_language, topic_name) DO UPDATE
                SET
                    error_category = COALESCE(EXCLUDED.error_category, bt_3_default_topics.error_category),
                    error_subcategory = COALESCE(EXCLUDED.error_subcategory, bt_3_default_topics.error_subcategory),
                    skill_id = COALESCE(EXCLUDED.skill_id, bt_3_default_topics.skill_id)
                RETURNING topic_name, error_category, error_subcategory, skill_id, created_at;
                """,
                (
                    int(user_id),
                    normalized_target,
                    resolved_topic,
                    str(error_category or "").strip() or None,
                    str(error_subcategory or "").strip() or None,
                    str(skill_id or "").strip() or None,
                ),
            )
            row = cursor.fetchone()
    if not row:
        return None
    return {
        "topic_name": str(row[0] or ""),
        "error_category": str(row[1] or "") or None,
        "error_subcategory": str(row[2] or "") or None,
        "skill_id": str(row[3] or "") or None,
        "created_at": row[4].isoformat() if row[4] else None,
    }


def _normalize_billing_currency(value: str | None) -> str:
    normalized = str(value or "USD").strip().upper()
    if not normalized:
        return "USD"
    return normalized[:12]


def _normalize_billing_status(value: str | None) -> str:
    normalized = str(value or "estimated").strip().lower()
    return normalized if normalized in {"estimated", "final"} else "estimated"


def _normalize_allocation_method(value: str | None) -> str:
    normalized = str(value or "equal").strip().lower()
    return normalized if normalized in {"equal", "weighted"} else "equal"


def _billing_period_bounds(period: str, as_of_date: date | None = None) -> tuple[date, date]:
    normalized = str(period or "month").strip().lower()
    if normalized == "all":
        return date(1970, 1, 1), (as_of_date or date.today())
    if normalized == "half_year":
        normalized = "half-year"
    if normalized not in {"week", "month", "quarter", "half-year", "year"}:
        normalized = "month"
    return _period_bounds(normalized, as_of_date)


def _month_period_start(as_of: date | datetime | None = None, tz: str = TRIAL_POLICY_TZ) -> date:
    if isinstance(as_of, datetime):
        local_dt = _to_aware_datetime(as_of).astimezone(_resolve_timezone(tz))
        base_date = local_dt.date()
    elif isinstance(as_of, date):
        base_date = as_of
    else:
        base_date = datetime.now(timezone.utc).astimezone(_resolve_timezone(tz)).date()
    return base_date.replace(day=1)


def _provider_budget_default_base_limit(provider: str) -> int:
    normalized = str(provider or "").strip().lower()
    if normalized == "google_tts":
        return int(GOOGLE_TTS_MONTHLY_BASE_LIMIT_CHARS)
    if normalized == "google_translate":
        return int(GOOGLE_TRANSLATE_MONTHLY_BASE_LIMIT_CHARS)
    return 0


def _provider_budget_row_to_dict(row) -> dict | None:
    if not row:
        return None
    base_limit = int(row[2] or 0)
    extra_limit = int(row[3] or 0)
    return {
        "provider": str(row[0] or ""),
        "period_month": row[1].isoformat() if row[1] else None,
        "base_limit_units": base_limit,
        "extra_limit_units": extra_limit,
        "effective_limit_units": max(0, base_limit + extra_limit),
        "is_blocked": bool(row[4]),
        "block_reason": str(row[5] or "").strip() or None,
        "notified_thresholds": row[6] if isinstance(row[6], dict) else {},
        "metadata": row[7] if isinstance(row[7], dict) else {},
        "created_at": row[8].isoformat() if row[8] else None,
        "updated_at": row[9].isoformat() if row[9] else None,
    }


def _billing_event_row_to_dict(row: tuple) -> dict:
    return {
        "id": int(row[0]),
        "idempotency_key": str(row[1] or ""),
        "user_id": int(row[2]) if row[2] is not None else None,
        "source_lang": str(row[3] or "") or None,
        "target_lang": str(row[4] or "") or None,
        "action_type": str(row[5] or ""),
        "provider": str(row[6] or ""),
        "units_type": str(row[7] or ""),
        "units_value": float(row[8] or 0.0),
        "price_snapshot_id": int(row[9]) if row[9] is not None else None,
        "cost_amount": float(row[10] or 0.0),
        "currency": str(row[11] or "USD"),
        "status": str(row[12] or "estimated"),
        "metadata": row[13] if isinstance(row[13], dict) else {},
        "event_time": row[14].isoformat() if row[14] else None,
        "created_at": row[15].isoformat() if row[15] else None,
    }


def upsert_billing_price_snapshot(
    *,
    provider: str,
    sku: str,
    unit: str,
    price_per_unit: float,
    currency: str = "USD",
    valid_from: datetime | None = None,
    source: str = "manual",
    raw_payload: dict | None = None,
) -> dict | None:
    provider_value = str(provider or "").strip().lower()
    sku_value = str(sku or "").strip()
    unit_value = str(unit or "").strip().lower()
    if not provider_value or not sku_value or not unit_value:
        return None
    try:
        price_value = max(0.0, float(price_per_unit or 0.0))
    except Exception:
        price_value = 0.0
    currency_value = _normalize_billing_currency(currency)
    source_value = str(source or "manual").strip() or "manual"
    valid_from_value = valid_from or datetime.now(timezone.utc)
    payload_value = Json(raw_payload) if isinstance(raw_payload, dict) else None
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_billing_price_snapshots (
                    provider,
                    sku,
                    unit,
                    price_per_unit,
                    currency,
                    valid_from,
                    source,
                    raw_payload
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (provider, sku, unit, valid_from) DO UPDATE
                SET
                    price_per_unit = EXCLUDED.price_per_unit,
                    currency = EXCLUDED.currency,
                    source = EXCLUDED.source,
                    raw_payload = COALESCE(EXCLUDED.raw_payload, bt_3_billing_price_snapshots.raw_payload)
                RETURNING
                    id, provider, sku, unit, price_per_unit, currency, valid_from, source, raw_payload, created_at;
                """,
                (
                    provider_value,
                    sku_value,
                    unit_value,
                    price_value,
                    currency_value,
                    valid_from_value,
                    source_value,
                    payload_value,
                ),
            )
            row = cursor.fetchone()
    if not row:
        return None
    return {
        "id": int(row[0]),
        "provider": str(row[1] or ""),
        "sku": str(row[2] or ""),
        "unit": str(row[3] or ""),
        "price_per_unit": float(row[4] or 0.0),
        "currency": str(row[5] or "USD"),
        "valid_from": row[6].isoformat() if row[6] else None,
        "source": str(row[7] or "manual"),
        "raw_payload": row[8] if isinstance(row[8], dict) else None,
        "created_at": row[9].isoformat() if row[9] else None,
    }


def get_effective_billing_price_snapshot(
    *,
    provider: str,
    sku: str,
    unit: str,
    currency: str = "USD",
    as_of: datetime | None = None,
) -> dict | None:
    provider_value = str(provider or "").strip().lower()
    sku_value = str(sku or "").strip()
    unit_value = str(unit or "").strip().lower()
    if not provider_value or not sku_value or not unit_value:
        return None
    currency_value = _normalize_billing_currency(currency)
    as_of_value = as_of or datetime.now(timezone.utc)
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT id, provider, sku, unit, price_per_unit, currency, valid_from, source, raw_payload, created_at
                FROM bt_3_billing_price_snapshots
                WHERE provider = %s
                  AND sku = %s
                  AND unit = %s
                  AND currency = %s
                  AND valid_from <= %s
                ORDER BY valid_from DESC
                LIMIT 1;
                """,
                (
                    provider_value,
                    sku_value,
                    unit_value,
                    currency_value,
                    as_of_value,
                ),
            )
            row = cursor.fetchone()
            if not row:
                cursor.execute(
                    """
                    SELECT id, provider, sku, unit, price_per_unit, currency, valid_from, source, raw_payload, created_at
                    FROM bt_3_billing_price_snapshots
                    WHERE provider = %s
                      AND sku = %s
                      AND unit = %s
                      AND valid_from <= %s
                    ORDER BY valid_from DESC
                    LIMIT 1;
                    """,
                    (
                        provider_value,
                        sku_value,
                        unit_value,
                        as_of_value,
                    ),
                )
                row = cursor.fetchone()
    if not row:
        return None
    return {
        "id": int(row[0]),
        "provider": str(row[1] or ""),
        "sku": str(row[2] or ""),
        "unit": str(row[3] or ""),
        "price_per_unit": float(row[4] or 0.0),
        "currency": str(row[5] or "USD"),
        "valid_from": row[6].isoformat() if row[6] else None,
        "source": str(row[7] or "manual"),
        "raw_payload": row[8] if isinstance(row[8], dict) else None,
        "created_at": row[9].isoformat() if row[9] else None,
    }


def log_billing_event(
    *,
    idempotency_key: str,
    user_id: int | None,
    action_type: str,
    provider: str,
    units_type: str,
    units_value: float,
    source_lang: str | None = None,
    target_lang: str | None = None,
    price_snapshot_id: int | None = None,
    price_provider: str | None = None,
    price_sku: str | None = None,
    price_unit: str | None = None,
    currency: str = "USD",
    status: str = "estimated",
    metadata: dict | None = None,
    event_time: datetime | None = None,
    cost_amount: float | None = None,
) -> dict | None:
    key = str(idempotency_key or "").strip()
    if not key:
        raise ValueError("idempotency_key is required")
    action_value = str(action_type or "").strip()
    provider_value = str(provider or "").strip().lower()
    unit_type_value = str(units_type or "").strip().lower()
    if not action_value or not provider_value or not unit_type_value:
        return None
    try:
        units_value_number = max(0.0, float(units_value or 0.0))
    except Exception:
        units_value_number = 0.0
    event_time_value = event_time or datetime.now(timezone.utc)
    currency_value = _normalize_billing_currency(currency)
    status_value = _normalize_billing_status(status)
    metadata_value = metadata if isinstance(metadata, dict) else {}

    resolved_snapshot_id = int(price_snapshot_id) if price_snapshot_id else None
    resolved_currency = currency_value
    resolved_cost = float(cost_amount) if cost_amount is not None else None

    if resolved_snapshot_id:
        with get_db_connection_context() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT price_per_unit, currency
                    FROM bt_3_billing_price_snapshots
                    WHERE id = %s
                    LIMIT 1;
                    """,
                    (resolved_snapshot_id,),
                )
                row = cursor.fetchone()
        if row:
            resolved_currency = str(row[1] or resolved_currency)
            if resolved_cost is None:
                resolved_cost = units_value_number * float(row[0] or 0.0)
    elif price_provider and price_sku and price_unit:
        snapshot = get_effective_billing_price_snapshot(
            provider=str(price_provider),
            sku=str(price_sku),
            unit=str(price_unit),
            currency=currency_value,
            as_of=event_time_value,
        )
        if snapshot:
            resolved_snapshot_id = int(snapshot["id"])
            resolved_currency = str(snapshot.get("currency") or resolved_currency)
            if resolved_cost is None:
                resolved_cost = units_value_number * float(snapshot.get("price_per_unit") or 0.0)

    if resolved_cost is None:
        resolved_cost = 0.0
    resolved_cost = max(0.0, float(resolved_cost))
    resolved_currency = _normalize_billing_currency(resolved_currency)

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_billing_events (
                    idempotency_key,
                    user_id,
                    source_lang,
                    target_lang,
                    action_type,
                    provider,
                    units_type,
                    units_value,
                    price_snapshot_id,
                    cost_amount,
                    currency,
                    status,
                    metadata,
                    event_time
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (idempotency_key) DO NOTHING
                RETURNING
                    id, idempotency_key, user_id, source_lang, target_lang, action_type, provider,
                    units_type, units_value, price_snapshot_id, cost_amount, currency, status,
                    metadata, event_time, created_at;
                """,
                (
                    key,
                    int(user_id) if user_id is not None else None,
                    str(source_lang or "").strip().lower() or None,
                    str(target_lang or "").strip().lower() or None,
                    action_value,
                    provider_value,
                    unit_type_value,
                    units_value_number,
                    resolved_snapshot_id,
                    resolved_cost,
                    resolved_currency,
                    status_value,
                    metadata_value,
                    event_time_value,
                ),
            )
            row = cursor.fetchone()
            if not row:
                cursor.execute(
                    """
                    SELECT
                        id, idempotency_key, user_id, source_lang, target_lang, action_type, provider,
                        units_type, units_value, price_snapshot_id, cost_amount, currency, status,
                        metadata, event_time, created_at
                    FROM bt_3_billing_events
                    WHERE idempotency_key = %s
                    LIMIT 1;
                    """,
                    (key,),
                )
                row = cursor.fetchone()
    return _billing_event_row_to_dict(row) if row else None


def upsert_billing_fixed_cost(
    *,
    category: str,
    provider: str,
    amount: float,
    currency: str = "USD",
    period_start: date,
    period_end: date,
    allocation_method_default: str = "equal",
    metadata: dict | None = None,
) -> dict | None:
    category_value = str(category or "").strip().lower()
    provider_value = str(provider or "infra").strip().lower() or "infra"
    if not category_value:
        return None
    try:
        amount_value = max(0.0, float(amount or 0.0))
    except Exception:
        amount_value = 0.0
    currency_value = _normalize_billing_currency(currency)
    allocation_value = _normalize_allocation_method(allocation_method_default)
    metadata_value = metadata if isinstance(metadata, dict) else {}
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_billing_fixed_costs (
                    category,
                    provider,
                    amount,
                    currency,
                    period_start,
                    period_end,
                    allocation_method_default,
                    metadata,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (category, provider, period_start, period_end, currency) DO UPDATE
                SET
                    amount = EXCLUDED.amount,
                    allocation_method_default = EXCLUDED.allocation_method_default,
                    metadata = EXCLUDED.metadata,
                    updated_at = NOW()
                RETURNING id, category, provider, amount, currency, period_start, period_end,
                          allocation_method_default, metadata, created_at, updated_at;
                """,
                (
                    category_value,
                    provider_value,
                    amount_value,
                    currency_value,
                    period_start,
                    period_end,
                    allocation_value,
                    metadata_value,
                ),
            )
            row = cursor.fetchone()
    if not row:
        return None
    return {
        "id": int(row[0]),
        "category": str(row[1] or ""),
        "provider": str(row[2] or ""),
        "amount": float(row[3] or 0.0),
        "currency": str(row[4] or "USD"),
        "period_start": row[5].isoformat() if row[5] else None,
        "period_end": row[6].isoformat() if row[6] else None,
        "allocation_method_default": str(row[7] or "equal"),
        "metadata": row[8] if isinstance(row[8], dict) else {},
        "created_at": row[9].isoformat() if row[9] else None,
        "updated_at": row[10].isoformat() if row[10] else None,
    }


def get_or_create_provider_budget_control(
    *,
    provider: str,
    period_month: date | datetime | None = None,
    tz: str = TRIAL_POLICY_TZ,
) -> dict | None:
    provider_value = str(provider or "").strip().lower()
    if not provider_value:
        return None
    period_value = _month_period_start(period_month, tz=tz)
    base_limit_units = _provider_budget_default_base_limit(provider_value)
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_provider_budget_controls (
                    provider,
                    period_month,
                    base_limit_units,
                    extra_limit_units,
                    is_blocked,
                    block_reason,
                    notified_thresholds,
                    metadata,
                    updated_at
                )
                VALUES (%s, %s, %s, 0, FALSE, NULL, '{}'::jsonb, '{}'::jsonb, NOW())
                ON CONFLICT (provider, period_month) DO UPDATE
                SET
                    base_limit_units = GREATEST(bt_3_provider_budget_controls.base_limit_units, EXCLUDED.base_limit_units),
                    updated_at = NOW()
                RETURNING
                    provider,
                    period_month,
                    base_limit_units,
                    extra_limit_units,
                    is_blocked,
                    block_reason,
                    notified_thresholds,
                    metadata,
                    created_at,
                    updated_at;
                """,
                (provider_value, period_value, int(base_limit_units)),
            )
            row = cursor.fetchone()
    return _provider_budget_row_to_dict(row)


def get_provider_budget_month_usage(
    *,
    provider: str,
    units_type: str = "chars",
    period_month: date | datetime | None = None,
    tz: str = TRIAL_POLICY_TZ,
) -> float:
    provider_value = str(provider or "").strip().lower()
    units_type_value = str(units_type or "").strip().lower() or "chars"
    if not provider_value:
        return 0.0
    period_start = _month_period_start(period_month, tz=tz)
    if period_start.month == 12:
        period_end = date(period_start.year + 1, 1, 1)
    else:
        period_end = date(period_start.year, period_start.month + 1, 1)
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT COALESCE(SUM(units_value), 0)
                FROM bt_3_billing_events
                WHERE provider = %s
                  AND units_type = %s
                  AND event_time >= %s
                  AND event_time < %s
                  AND COALESCE(metadata->>'cached', 'false') <> 'true';
                """,
                (
                    provider_value,
                    units_type_value,
                    datetime.combine(period_start, dt_time.min, tzinfo=timezone.utc),
                    datetime.combine(period_end, dt_time.min, tzinfo=timezone.utc),
                ),
            )
            row = cursor.fetchone()
    return float((row or [0])[0] or 0.0)


def get_google_tts_monthly_budget_status(
    *,
    period_month: date | datetime | None = None,
    tz: str = TRIAL_POLICY_TZ,
) -> dict | None:
    control = get_or_create_provider_budget_control(
        provider="google_tts",
        period_month=period_month,
        tz=tz,
    )
    if not control:
        return None
    used_units = get_provider_budget_month_usage(
        provider="google_tts",
        units_type="chars",
        period_month=period_month,
        tz=tz,
    )
    effective_limit = float(control.get("effective_limit_units") or 0.0)
    usage_ratio = (used_units / effective_limit) if effective_limit > 0 else 0.0
    result = dict(control)
    result["used_units"] = float(round(used_units, 3))
    result["remaining_units"] = max(0.0, float(round(effective_limit - used_units, 3)))
    result["usage_ratio"] = max(0.0, usage_ratio)
    result["unit"] = "chars"
    return result


def get_google_translate_monthly_budget_status(
    *,
    period_month: date | datetime | None = None,
    tz: str = TRIAL_POLICY_TZ,
) -> dict | None:
    control = get_or_create_provider_budget_control(
        provider="google_translate",
        period_month=period_month,
        tz=tz,
    )
    if not control:
        return None
    used_units = get_provider_budget_month_usage(
        provider="google_translate",
        units_type="chars",
        period_month=period_month,
        tz=tz,
    )
    effective_limit = float(control.get("effective_limit_units") or 0.0)
    usage_ratio = (used_units / effective_limit) if effective_limit > 0 else 0.0
    result = dict(control)
    result["used_units"] = float(round(used_units, 3))
    result["remaining_units"] = max(0.0, float(round(effective_limit - used_units, 3)))
    result["usage_ratio"] = max(0.0, usage_ratio)
    result["unit"] = "chars"
    return result


def set_provider_budget_extra_limit(
    *,
    provider: str,
    extra_limit_units: int,
    period_month: date | datetime | None = None,
    tz: str = TRIAL_POLICY_TZ,
    metadata: dict | None = None,
) -> dict | None:
    provider_value = str(provider or "").strip().lower()
    if not provider_value:
        return None
    base = get_or_create_provider_budget_control(provider=provider_value, period_month=period_month, tz=tz)
    if not base:
        return None
    extra_value = max(0, int(extra_limit_units or 0))
    metadata_value = metadata if isinstance(metadata, dict) else {}
    period_value = _month_period_start(period_month, tz=tz)
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE bt_3_provider_budget_controls
                SET
                    extra_limit_units = %s,
                    metadata = COALESCE(metadata, '{}'::jsonb) || %s::jsonb,
                    updated_at = NOW()
                WHERE provider = %s
                  AND period_month = %s
                RETURNING
                    provider,
                    period_month,
                    base_limit_units,
                    extra_limit_units,
                    is_blocked,
                    block_reason,
                    notified_thresholds,
                    metadata,
                    created_at,
                    updated_at;
                """,
                (extra_value, Json(metadata_value), provider_value, period_value),
            )
            row = cursor.fetchone()
    return _provider_budget_row_to_dict(row)


def set_provider_budget_block_state(
    *,
    provider: str,
    is_blocked: bool,
    block_reason: str | None = None,
    period_month: date | datetime | None = None,
    tz: str = TRIAL_POLICY_TZ,
) -> dict | None:
    provider_value = str(provider or "").strip().lower()
    if not provider_value:
        return None
    base = get_or_create_provider_budget_control(provider=provider_value, period_month=period_month, tz=tz)
    if not base:
        return None
    period_value = _month_period_start(period_month, tz=tz)
    reason_value = str(block_reason or "").strip() or None
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE bt_3_provider_budget_controls
                SET
                    is_blocked = %s,
                    block_reason = %s,
                    updated_at = NOW()
                WHERE provider = %s
                  AND period_month = %s
                RETURNING
                    provider,
                    period_month,
                    base_limit_units,
                    extra_limit_units,
                    is_blocked,
                    block_reason,
                    notified_thresholds,
                    metadata,
                    created_at,
                    updated_at;
                """,
                (bool(is_blocked), reason_value, provider_value, period_value),
            )
            row = cursor.fetchone()
    return _provider_budget_row_to_dict(row)


def mark_provider_budget_threshold_notified(
    *,
    provider: str,
    threshold_percent: int | float,
    period_month: date | datetime | None = None,
    tz: str = TRIAL_POLICY_TZ,
    metadata: dict | None = None,
) -> dict | None:
    provider_value = str(provider or "").strip().lower()
    if not provider_value:
        return None
    base = get_or_create_provider_budget_control(provider=provider_value, period_month=period_month, tz=tz)
    if not base:
        return None
    try:
        threshold_value = int(round(float(threshold_percent)))
    except Exception:
        threshold_value = 0
    if threshold_value <= 0:
        return base
    period_value = _month_period_start(period_month, tz=tz)
    metadata_value = metadata if isinstance(metadata, dict) else {}
    threshold_key = str(threshold_value)
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE bt_3_provider_budget_controls
                SET
                    notified_thresholds = COALESCE(notified_thresholds, '{}'::jsonb) || jsonb_build_object(%s, NOW()::text),
                    metadata = COALESCE(metadata, '{}'::jsonb) || %s::jsonb,
                    updated_at = NOW()
                WHERE provider = %s
                  AND period_month = %s
                RETURNING
                    provider,
                    period_month,
                    base_limit_units,
                    extra_limit_units,
                    is_blocked,
                    block_reason,
                    notified_thresholds,
                    metadata,
                    created_at,
                    updated_at;
                """,
                (threshold_key, Json(metadata_value), provider_value, period_value),
            )
            row = cursor.fetchone()
    return _provider_budget_row_to_dict(row)


def get_user_billing_summary(
    *,
    user_id: int,
    period: str = "month",
    allocation_method: str = "equal",
    source_lang: str | None = None,
    target_lang: str | None = None,
    currency: str = "USD",
    as_of_date: date | None = None,
) -> dict:
    currency_value = _normalize_billing_currency(currency)
    period_start, period_end = _billing_period_bounds(period, as_of_date)
    allocation = _normalize_allocation_method(allocation_method)
    source_value = str(source_lang or "").strip().lower() or None
    target_value = str(target_lang or "").strip().lower() or None

    language_sql = ""
    language_params: list = []
    if source_value:
        language_sql += " AND COALESCE(source_lang, '') = %s"
        language_params.append(source_value)
    if target_value:
        language_sql += " AND COALESCE(target_lang, '') = %s"
        language_params.append(target_value)

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT
                    COALESCE(SUM(cost_amount), 0) AS total_cost,
                    COALESCE(SUM(units_value), 0) AS total_units,
                    COUNT(*) AS events_count,
                    COALESCE(SUM(CASE WHEN status = 'final' THEN cost_amount ELSE 0 END), 0) AS final_cost,
                    COALESCE(SUM(CASE WHEN status = 'estimated' THEN cost_amount ELSE 0 END), 0) AS estimated_cost,
                    COALESCE(SUM(CASE WHEN COALESCE(metadata->>'pricing_state', '') = 'missing_snapshot' THEN units_value ELSE 0 END), 0) AS unpriced_units,
                    COALESCE(SUM(CASE WHEN COALESCE(metadata->>'pricing_state', '') = 'missing_snapshot' THEN 1 ELSE 0 END), 0) AS unpriced_events
                FROM bt_3_billing_events
                WHERE user_id = %s
                  AND currency = %s
                  AND (event_time AT TIME ZONE 'UTC')::date BETWEEN %s AND %s
                  {language_sql};
                """,
                [int(user_id), currency_value, period_start, period_end, *language_params],
            )
            user_totals_row = cursor.fetchone() or (0, 0, 0, 0, 0, 0, 0)
            user_variable_cost = float(user_totals_row[0] or 0.0)
            user_units = float(user_totals_row[1] or 0.0)
            user_events = int(user_totals_row[2] or 0)
            user_final_cost = float(user_totals_row[3] or 0.0)
            user_estimated_cost = float(user_totals_row[4] or 0.0)
            user_unpriced_units = float(user_totals_row[5] or 0.0)
            user_unpriced_events = int(user_totals_row[6] or 0)

            cursor.execute(
                """
                SELECT
                    COALESCE(SUM(amount), 0) AS fixed_cost_total
                FROM bt_3_billing_fixed_costs
                WHERE currency = %s
                  AND period_end >= %s
                  AND period_start <= %s;
                """,
                (currency_value, period_start, period_end),
            )
            fixed_total = float((cursor.fetchone() or [0])[0] or 0.0)

            cursor.execute(
                """
                SELECT COUNT(DISTINCT user_id)
                FROM bt_3_billing_events
                WHERE user_id IS NOT NULL
                  AND currency = %s
                  AND (event_time AT TIME ZONE 'UTC')::date BETWEEN %s AND %s;
                """,
                (currency_value, period_start, period_end),
            )
            active_users = int((cursor.fetchone() or [0])[0] or 0)

            cursor.execute(
                """
                SELECT COALESCE(SUM(units_value), 0)
                FROM bt_3_billing_events
                WHERE user_id IS NOT NULL
                  AND currency = %s
                  AND (event_time AT TIME ZONE 'UTC')::date BETWEEN %s AND %s;
                """,
                (currency_value, period_start, period_end),
            )
            all_units = float((cursor.fetchone() or [0])[0] or 0.0)

            user_is_active = user_events > 0
            fixed_allocated = 0.0
            if allocation == "weighted":
                if all_units > 0 and user_units > 0:
                    fixed_allocated = fixed_total * (user_units / all_units)
                elif active_users > 0 and user_is_active:
                    fixed_allocated = fixed_total / active_users
            else:
                if active_users > 0 and user_is_active:
                    fixed_allocated = fixed_total / active_users

            cursor.execute(
                f"""
                SELECT provider, SUM(cost_amount) AS cost_total, SUM(units_value) AS units_total, COUNT(*) AS events_count
                FROM bt_3_billing_events
                WHERE user_id = %s
                  AND currency = %s
                  AND (event_time AT TIME ZONE 'UTC')::date BETWEEN %s AND %s
                  {language_sql}
                GROUP BY provider
                ORDER BY cost_total DESC, units_total DESC
                LIMIT 12;
                """,
                [int(user_id), currency_value, period_start, period_end, *language_params],
            )
            providers = [
                {
                    "provider": str(row[0] or ""),
                    "cost": float(row[1] or 0.0),
                    "units": float(row[2] or 0.0),
                    "events": int(row[3] or 0),
                }
                for row in (cursor.fetchall() or [])
            ]

            cursor.execute(
                f"""
                SELECT action_type, SUM(cost_amount) AS cost_total, SUM(units_value) AS units_total, COUNT(*) AS events_count
                FROM bt_3_billing_events
                WHERE user_id = %s
                  AND currency = %s
                  AND (event_time AT TIME ZONE 'UTC')::date BETWEEN %s AND %s
                  {language_sql}
                GROUP BY action_type
                ORDER BY cost_total DESC, units_total DESC
                LIMIT 20;
                """,
                [int(user_id), currency_value, period_start, period_end, *language_params],
            )
            actions = [
                {
                    "action_type": str(row[0] or ""),
                    "cost": float(row[1] or 0.0),
                    "units": float(row[2] or 0.0),
                    "events": int(row[3] or 0),
                }
                for row in (cursor.fetchall() or [])
            ]

            cursor.execute(
                """
                SELECT category, provider, amount, period_start, period_end, allocation_method_default
                FROM bt_3_billing_fixed_costs
                WHERE currency = %s
                  AND period_end >= %s
                  AND period_start <= %s
                ORDER BY period_start DESC, category ASC;
                """,
                (currency_value, period_start, period_end),
            )
            fixed_items = [
                {
                    "category": str(row[0] or ""),
                    "provider": str(row[1] or ""),
                    "amount": float(row[2] or 0.0),
                    "period_start": row[3].isoformat() if row[3] else None,
                    "period_end": row[4].isoformat() if row[4] else None,
                    "allocation_method_default": str(row[5] or "equal"),
                }
                for row in (cursor.fetchall() or [])
            ]

            cursor.execute(
                """
                SELECT COALESCE(SUM(cost_amount), 0), COUNT(*)
                FROM bt_3_billing_events
                WHERE user_id IS NOT NULL
                  AND currency = %s
                  AND (event_time AT TIME ZONE 'UTC')::date BETWEEN %s AND %s;
                """,
                (currency_value, period_start, period_end),
            )
            all_variable_cost_row = cursor.fetchone() or (0, 0)
            all_variable_cost = float(all_variable_cost_row[0] or 0.0)
            all_events = int(all_variable_cost_row[1] or 0)

    total_cost = user_variable_cost + fixed_allocated
    avg_per_event = total_cost / user_events if user_events > 0 else 0.0
    avg_per_active_user = (all_variable_cost + fixed_total) / active_users if active_users > 0 else 0.0
    return {
        "period": str(period or "month"),
        "range": {
            "start_date": period_start.isoformat(),
            "end_date": period_end.isoformat(),
        },
        "allocation_method": allocation,
        "currency": currency_value,
        "totals": {
            "user_variable_cost": round(user_variable_cost, 6),
            "user_fixed_allocated_cost": round(fixed_allocated, 6),
            "user_total_cost": round(total_cost, 6),
            "user_final_cost": round(user_final_cost, 6),
            "user_estimated_cost": round(user_estimated_cost, 6),
            "user_unpriced_events": user_unpriced_events,
            "user_unpriced_units": round(user_unpriced_units, 6),
            "user_events_count": user_events,
            "user_units_total": round(user_units, 6),
            "avg_cost_per_user_event": round(avg_per_event, 6),
            "period_fixed_cost_total": round(fixed_total, 6),
            "period_variable_cost_total": round(all_variable_cost, 6),
            "period_events_total": all_events,
            "period_active_users": active_users,
            "period_avg_cost_per_active_user": round(avg_per_active_user, 6),
        },
        "breakdown": {
            "by_provider": providers,
            "by_action_type": actions,
            "fixed_costs": fixed_items,
        },
    }


def list_billing_plans(*, include_inactive: bool = False) -> list[dict]:
    where_sql = "" if include_inactive else "WHERE is_active = TRUE"
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT
                    plan_code,
                    name,
                    is_paid,
                    stripe_price_id,
                    daily_cost_cap_eur,
                    trial_days,
                    is_active,
                    created_at,
                    updated_at
                FROM plans
                {where_sql}
                ORDER BY is_paid ASC, plan_code ASC;
                """
            )
            rows = cursor.fetchall() or []
    return [
        {
            "plan_code": str(row[0] or ""),
            "name": str(row[1] or ""),
            "is_paid": bool(row[2]),
            "stripe_price_id": str(row[3] or "") or None,
            "daily_cost_cap_eur": float(row[4]) if row[4] is not None else None,
            "trial_days": int(row[5] or 0),
            "is_active": bool(row[6]),
            "created_at": row[7].isoformat() if row[7] else None,
            "updated_at": row[8].isoformat() if row[8] else None,
        }
        for row in rows
    ]


def get_billing_plan(plan_code: str) -> dict | None:
    code = str(plan_code or "").strip().lower()
    if not code:
        return None
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    plan_code,
                    name,
                    is_paid,
                    stripe_price_id,
                    daily_cost_cap_eur,
                    trial_days,
                    is_active,
                    created_at,
                    updated_at
                FROM plans
                WHERE plan_code = %s
                LIMIT 1;
                """,
                (code,),
            )
            row = cursor.fetchone()
    if not row:
        return None
    return {
        "plan_code": str(row[0] or ""),
        "name": str(row[1] or ""),
        "is_paid": bool(row[2]),
        "stripe_price_id": str(row[3] or "") or None,
        "daily_cost_cap_eur": float(row[4]) if row[4] is not None else None,
        "trial_days": int(row[5] or 0),
        "is_active": bool(row[6]),
        "created_at": row[7].isoformat() if row[7] else None,
        "updated_at": row[8].isoformat() if row[8] else None,
    }


def _subscription_row_to_dict(row) -> dict:
    return {
        "user_id": int(row[0]),
        "plan_code": str(row[1] or ""),
        "status": str(row[2] or ""),
        "trial_ends_at": row[3].isoformat() if row[3] else None,
        "current_period_end": row[4].isoformat() if row[4] else None,
        "stripe_customer_id": str(row[5] or "") or None,
        "stripe_subscription_id": str(row[6] or "") or None,
        "created_at": row[7].isoformat() if row[7] else None,
        "updated_at": row[8].isoformat() if row[8] else None,
    }


def get_user_subscription(user_id: int) -> dict | None:
    user_id_value = int(user_id)
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    user_id,
                    plan_code,
                    status,
                    trial_ends_at,
                    current_period_end,
                    stripe_customer_id,
                    stripe_subscription_id,
                    created_at,
                    updated_at
                FROM user_subscriptions
                WHERE user_id = %s
                LIMIT 1;
                """,
                (user_id_value,),
            )
            row = cursor.fetchone()
    return _subscription_row_to_dict(row) if row else None


def get_user_subscription_by_customer_id(stripe_customer_id: str) -> dict | None:
    customer_id_value = str(stripe_customer_id or "").strip()
    if not customer_id_value:
        return None
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    user_id,
                    plan_code,
                    status,
                    trial_ends_at,
                    current_period_end,
                    stripe_customer_id,
                    stripe_subscription_id,
                    created_at,
                    updated_at
                FROM user_subscriptions
                WHERE stripe_customer_id = %s
                ORDER BY updated_at DESC
                LIMIT 1;
                """,
                (customer_id_value,),
            )
            row = cursor.fetchone()
    return _subscription_row_to_dict(row) if row else None


def get_user_subscription_by_stripe_subscription_id(stripe_subscription_id: str) -> dict | None:
    subscription_id_value = str(stripe_subscription_id or "").strip()
    if not subscription_id_value:
        return None
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    user_id,
                    plan_code,
                    status,
                    trial_ends_at,
                    current_period_end,
                    stripe_customer_id,
                    stripe_subscription_id,
                    created_at,
                    updated_at
                FROM user_subscriptions
                WHERE stripe_subscription_id = %s
                ORDER BY updated_at DESC
                LIMIT 1;
                """,
                (subscription_id_value,),
            )
            row = cursor.fetchone()
    return _subscription_row_to_dict(row) if row else None


def _normalize_subscription_status(status: str | None) -> str:
    value = str(status or "").strip().lower() or "inactive"
    allowed = {"active", "inactive", "past_due", "canceled", "trialing"}
    if value not in allowed:
        raise ValueError(f"Unsupported subscription status: {value!r}")
    return value


def get_or_create_user_subscription(
    user_id: int,
    now_ts: datetime | None = None,
    tz: str = TRIAL_POLICY_TZ,
) -> dict:
    """Race-safe create-or-get via INSERT .. ON CONFLICT DO NOTHING + SELECT."""
    user_id_value = int(user_id)
    trial_ends_at = _compute_trial_ends_at(now_ts, trial_days=TRIAL_POLICY_DAYS, tz_name=tz)
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO user_subscriptions (
                    user_id,
                    plan_code,
                    status,
                    trial_ends_at,
                    updated_at
                )
                VALUES (%s, 'free', 'trialing', %s, NOW())
                ON CONFLICT (user_id) DO NOTHING
                RETURNING
                    user_id,
                    plan_code,
                    status,
                    trial_ends_at,
                    current_period_end,
                    stripe_customer_id,
                    stripe_subscription_id,
                    created_at,
                    updated_at;
                """,
                (user_id_value, trial_ends_at),
            )
            row = cursor.fetchone()
            if not row:
                cursor.execute(
                    """
                    SELECT
                        user_id,
                        plan_code,
                        status,
                        trial_ends_at,
                        current_period_end,
                        stripe_customer_id,
                        stripe_subscription_id,
                        created_at,
                        updated_at
                    FROM user_subscriptions
                    WHERE user_id = %s
                    LIMIT 1;
                    """,
                    (user_id_value,),
                )
                row = cursor.fetchone()
    if not row:
        raise RuntimeError("Failed to create or fetch user subscription")
    return _subscription_row_to_dict(row)


def bind_stripe_customer_to_user(user_id: int, stripe_customer_id: str, db_conn=None) -> dict:
    user_id_value = int(user_id)
    customer_id_value = str(stripe_customer_id or "").strip()
    if not customer_id_value:
        raise ValueError("stripe_customer_id is required")
    owns_conn = db_conn is None
    if owns_conn:
        with get_db_connection_context() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO user_subscriptions (
                        user_id,
                        plan_code,
                        status,
                        trial_ends_at,
                        stripe_customer_id,
                        updated_at
                    )
                    VALUES (%s, 'free', 'trialing', %s, %s, NOW())
                    ON CONFLICT (user_id) DO UPDATE
                    SET
                        stripe_customer_id = EXCLUDED.stripe_customer_id,
                        updated_at = NOW()
                    RETURNING
                        user_id,
                        plan_code,
                        status,
                        trial_ends_at,
                        current_period_end,
                        stripe_customer_id,
                        stripe_subscription_id,
                        created_at,
                        updated_at;
                    """,
                    (
                        user_id_value,
                        _compute_trial_ends_at(datetime.now(timezone.utc), trial_days=TRIAL_POLICY_DAYS, tz_name=TRIAL_POLICY_TZ),
                        customer_id_value,
                    ),
                )
                row = cursor.fetchone()
    else:
        with db_conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO user_subscriptions (
                    user_id,
                    plan_code,
                    status,
                    trial_ends_at,
                    stripe_customer_id,
                    updated_at
                )
                VALUES (%s, 'free', 'trialing', %s, %s, NOW())
                ON CONFLICT (user_id) DO UPDATE
                SET
                    stripe_customer_id = EXCLUDED.stripe_customer_id,
                    updated_at = NOW()
                RETURNING
                    user_id,
                    plan_code,
                    status,
                    trial_ends_at,
                    current_period_end,
                    stripe_customer_id,
                    stripe_subscription_id,
                    created_at,
                    updated_at;
                """,
                (
                    user_id_value,
                    _compute_trial_ends_at(datetime.now(timezone.utc), trial_days=TRIAL_POLICY_DAYS, tz_name=TRIAL_POLICY_TZ),
                    customer_id_value,
                ),
            )
            row = cursor.fetchone()
    if not row:
        raise RuntimeError("Failed to bind stripe customer to user subscription")
    return _subscription_row_to_dict(row)


def set_subscription_from_stripe(
    user_id: int,
    plan_code: str,
    stripe_customer_id: str | None,
    stripe_subscription_id: str | None,
    status: str,
    current_period_end: datetime | None,
    db_conn=None,
) -> dict:
    user_id_value = int(user_id)
    plan_code_value = str(plan_code or "pro").strip().lower() or "pro"
    status_value = _normalize_subscription_status(status)
    period_end_value = _to_aware_datetime(current_period_end) if current_period_end else None
    customer_id_value = str(stripe_customer_id or "").strip() or None
    subscription_id_value = str(stripe_subscription_id or "").strip() or None
    owns_conn = db_conn is None
    if owns_conn:
        with get_db_connection_context() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO user_subscriptions (
                        user_id,
                        plan_code,
                        status,
                        trial_ends_at,
                        current_period_end,
                        stripe_customer_id,
                        stripe_subscription_id,
                        updated_at
                    )
                    VALUES (%s, %s, %s, NULL, %s, %s, %s, NOW())
                    ON CONFLICT (user_id) DO UPDATE
                    SET
                        plan_code = EXCLUDED.plan_code,
                        status = EXCLUDED.status,
                        trial_ends_at = NULL,
                        current_period_end = EXCLUDED.current_period_end,
                        stripe_customer_id = COALESCE(EXCLUDED.stripe_customer_id, user_subscriptions.stripe_customer_id),
                        stripe_subscription_id = COALESCE(EXCLUDED.stripe_subscription_id, user_subscriptions.stripe_subscription_id),
                        updated_at = NOW()
                    RETURNING
                        user_id,
                        plan_code,
                        status,
                        trial_ends_at,
                        current_period_end,
                        stripe_customer_id,
                        stripe_subscription_id,
                        created_at,
                        updated_at;
                    """,
                    (
                        user_id_value,
                        plan_code_value,
                        status_value,
                        period_end_value,
                        customer_id_value,
                        subscription_id_value,
                    ),
                )
                row = cursor.fetchone()
    else:
        with db_conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO user_subscriptions (
                    user_id,
                    plan_code,
                    status,
                    trial_ends_at,
                    current_period_end,
                    stripe_customer_id,
                    stripe_subscription_id,
                    updated_at
                )
                    VALUES (%s, %s, %s, NULL, %s, %s, %s, NOW())
                    ON CONFLICT (user_id) DO UPDATE
                    SET
                        plan_code = EXCLUDED.plan_code,
                    status = EXCLUDED.status,
                    trial_ends_at = NULL,
                    current_period_end = EXCLUDED.current_period_end,
                    stripe_customer_id = COALESCE(EXCLUDED.stripe_customer_id, user_subscriptions.stripe_customer_id),
                    stripe_subscription_id = COALESCE(EXCLUDED.stripe_subscription_id, user_subscriptions.stripe_subscription_id),
                    updated_at = NOW()
                RETURNING
                    user_id,
                    plan_code,
                    status,
                    trial_ends_at,
                    current_period_end,
                    stripe_customer_id,
                    stripe_subscription_id,
                    created_at,
                    updated_at;
                """,
                    (
                        user_id_value,
                        plan_code_value,
                        status_value,
                        period_end_value,
                        customer_id_value,
                    subscription_id_value,
                ),
            )
            row = cursor.fetchone()
    if not row:
        raise RuntimeError("Failed to upsert Stripe subscription")
    return _subscription_row_to_dict(row)


def try_mark_stripe_event_processed(event_id: str, db_conn=None) -> bool:
    """Insert-first idempotency marker. True only for the first successful insert."""
    event_id_value = str(event_id or "").strip()
    if not event_id_value:
        return False
    if db_conn is None:
        with get_db_connection_context() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO stripe_events_processed (event_id)
                    VALUES (%s)
                    ON CONFLICT (event_id) DO NOTHING
                    RETURNING event_id;
                    """,
                    (event_id_value,),
                )
                inserted = cursor.fetchone()
    else:
        with db_conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO stripe_events_processed (event_id)
                VALUES (%s)
                ON CONFLICT (event_id) DO NOTHING
                RETURNING event_id;
                """,
                (event_id_value,),
            )
            inserted = cursor.fetchone()
    return bool(inserted)


def mark_stripe_event_processed(event_id: str, db_conn=None) -> bool:
    # Backward-compatible alias; use try_mark_stripe_event_processed in webhook flow.
    return try_mark_stripe_event_processed(event_id, db_conn=db_conn)


def is_stripe_event_processed(event_id: str) -> bool:
    event_id_value = str(event_id or "").strip()
    if not event_id_value:
        return False
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT 1
                FROM stripe_events_processed
                WHERE event_id = %s
                LIMIT 1;
                """,
                (event_id_value,),
            )
            row = cursor.fetchone()
    return bool(row)


def get_today_cost_eur(user_id: int, tz: str = TRIAL_POLICY_TZ) -> float:
    user_id_value = int(user_id)
    tz_name = str(tz or TRIAL_POLICY_TZ).strip() or TRIAL_POLICY_TZ
    tzinfo = _resolve_timezone(tz_name)
    day_local = datetime.now(timezone.utc).astimezone(tzinfo).date()

    # Always recompute from source-of-truth billing events before writing rollup.
    # This avoids stale totals when new events arrive after an earlier rollup read.
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT currency, COALESCE(SUM(cost_amount), 0) AS total_cost
                FROM bt_3_billing_events
                WHERE user_id = %s
                  AND (event_time AT TIME ZONE %s)::date = %s
                GROUP BY currency;
                """,
                (user_id_value, tz_name, day_local),
            )
            rows = cursor.fetchall() or []
            total_eur = 0.0
            for row in rows:
                currency = str(row[0] or "EUR").upper()
                amount = float(row[1] or 0.0)
                try:
                    total_eur += convert_cost_to_eur(amount, currency)
                except ValueError:
                    continue

            cursor.execute(
                """
                INSERT INTO daily_cost_rollup (
                    user_id,
                    day,
                    currency,
                    total_cost_eur,
                    updated_at
                )
                VALUES (%s, %s, 'EUR', %s, NOW())
                ON CONFLICT (user_id, day) DO UPDATE
                SET
                    currency = 'EUR',
                    total_cost_eur = EXCLUDED.total_cost_eur,
                    updated_at = NOW();
                """,
                (user_id_value, day_local, total_eur),
            )
    return float(total_eur)


def _next_local_midnight_iso(now_ts_utc: datetime | None = None, tz: str = TRIAL_POLICY_TZ) -> str:
    tzinfo = _resolve_timezone(tz)
    now_utc = _to_aware_datetime(now_ts_utc)
    local_now = now_utc.astimezone(tzinfo)
    next_day = local_now.date() + timedelta(days=1)
    next_midnight_local = datetime.combine(next_day, dt_time.min, tzinfo=tzinfo)
    return next_midnight_local.isoformat()


def _next_local_month_start_iso(now_ts_utc: datetime | None = None, tz: str = TRIAL_POLICY_TZ) -> str:
    tzinfo = _resolve_timezone(tz)
    now_utc = _to_aware_datetime(now_ts_utc)
    local_now = now_utc.astimezone(tzinfo)
    if local_now.month == 12:
        next_month_date = date(local_now.year + 1, 1, 1)
    else:
        next_month_date = date(local_now.year, local_now.month + 1, 1)
    next_month_local = datetime.combine(next_month_date, dt_time.min, tzinfo=tzinfo)
    return next_month_local.isoformat()


def resolve_entitlement(
    user_id: int,
    now_ts_utc: datetime | None = None,
    tz: str = TRIAL_POLICY_TZ,
) -> dict:
    now_utc = _to_aware_datetime(now_ts_utc)
    subscription = get_user_subscription(int(user_id))
    if not subscription:
        subscription = {
            "user_id": int(user_id),
            "plan_code": "free",
            "status": "inactive",
            "trial_ends_at": None,
            "current_period_end": None,
            "stripe_customer_id": None,
            "stripe_subscription_id": None,
            "created_at": None,
            "updated_at": None,
        }

    plan_code = str(subscription.get("plan_code") or "free").strip().lower() or "free"
    status = _normalize_subscription_status(subscription.get("status"))
    current_plan = get_billing_plan(plan_code) or {}
    if not current_plan and plan_code != "free":
        plan_code = "free"
        current_plan = get_billing_plan("free") or {}

    trial_ends_at_value = subscription.get("trial_ends_at")
    trial_ends_at_dt = None
    if trial_ends_at_value:
        try:
            trial_ends_at_dt = datetime.fromisoformat(str(trial_ends_at_value))
            trial_ends_at_dt = _to_aware_datetime(trial_ends_at_dt)
        except Exception:
            trial_ends_at_dt = None

    if bool(current_plan.get("is_paid")) and status in {"active", "trialing"}:
        effective_mode = "pro"
    elif status == "trialing" and trial_ends_at_dt is not None and now_utc < trial_ends_at_dt:
        effective_mode = "trial"
    else:
        effective_mode = "free"

    free_plan = get_billing_plan("free") or {}
    pro_plan = current_plan if bool(current_plan.get("is_paid")) else (get_billing_plan("pro") or {})
    if effective_mode == "pro":
        cap_eur = pro_plan.get("daily_cost_cap_eur")
    elif effective_mode == "trial":
        cap_eur = float(_env_decimal("TRIAL_DAILY_COST_CAP_EUR", "1.00"))
    else:
        cap_eur = free_plan.get("daily_cost_cap_eur")
        if cap_eur is None:
            fallback = _env_decimal("FREE_DAILY_COST_CAP_EUR", "0.50")
            cap_eur = float(fallback) if fallback is not None else None

    return {
        "user_id": int(user_id),
        "plan_code": plan_code,
        "plan_name": str(current_plan.get("name") or (free_plan.get("name") if plan_code == "free" else plan_code)),
        "status": status,
        "trial_ends_at": trial_ends_at_dt.isoformat() if trial_ends_at_dt else None,
        "effective_mode": effective_mode,
        "cap_eur": float(cap_eur) if cap_eur is not None else None,
        "reset_at": _next_local_midnight_iso(now_utc, tz=tz),
    }


def enforce_daily_cost_cap(
    user_id: int,
    now_ts_utc: datetime | None = None,
    tz: str = TRIAL_POLICY_TZ,
) -> dict | None:
    entitlement = resolve_entitlement(user_id=int(user_id), now_ts_utc=now_ts_utc, tz=tz)
    cap_eur = entitlement.get("cap_eur")
    if cap_eur is None:
        return None
    spent_eur = float(get_today_cost_eur(int(user_id), tz=tz))
    if spent_eur >= float(cap_eur):
        return {
            "error": "cost_cap_exceeded",
            "cap_eur": float(cap_eur),
            "spent_eur": float(round(spent_eur, 6)),
            "currency": "EUR",
            "reset_at": entitlement.get("reset_at"),
            "upgrade": {
                "available": True,
                "plan_code": "pro",
                "action": "checkout",
                "endpoint": "/api/billing/create-checkout-session",
            },
        }
    return None


def get_plan_limit(plan_code: str, feature_code: str, period: str = "day") -> dict | None:
    plan_value = str(plan_code or "").strip().lower()
    feature_value = str(feature_code or "").strip().lower()
    period_value = str(period or "day").strip().lower() or "day"
    if not plan_value or not feature_value:
        return None
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT plan_code, feature_code, limit_value, limit_unit, period, is_active
                FROM plan_limits
                WHERE plan_code = %s
                  AND feature_code = %s
                  AND period = %s
                  AND is_active = TRUE
                LIMIT 1;
                """,
                (plan_value, feature_value, period_value),
            )
            row = cursor.fetchone()
    if not row:
        return None
    return {
        "plan_code": str(row[0] or ""),
        "feature_code": str(row[1] or ""),
        "limit_value": float(row[2] or 0.0),
        "limit_unit": str(row[3] or ""),
        "period": str(row[4] or "day"),
        "is_active": bool(row[5]),
    }


def _get_feature_usage_today(user_id: int, feature_code: str, tz: str = TRIAL_POLICY_TZ) -> float:
    feature = str(feature_code or "").strip().lower()
    tz_name = str(tz or TRIAL_POLICY_TZ).strip() or TRIAL_POLICY_TZ
    day_local = datetime.now(timezone.utc).astimezone(_resolve_timezone(tz_name)).date()

    if feature == "youtube_fetch_daily":
        with get_db_connection_context() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM bt_3_billing_events
                    WHERE user_id = %s
                      AND action_type = 'youtube_transcript_fetch'
                      AND (event_time AT TIME ZONE %s)::date = %s;
                    """,
                    (int(user_id), tz_name, day_local),
                )
                row = cursor.fetchone()
        return float((row or [0])[0] or 0)

    if feature == "tts_chars_daily":
        with get_db_connection_context() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT COALESCE(SUM(units_value), 0)
                    FROM bt_3_billing_events
                    WHERE user_id = %s
                      AND action_type = 'webapp_tts_chars'
                      AND units_type = 'chars'
                      AND (event_time AT TIME ZONE %s)::date = %s;
                    """,
                    (int(user_id), tz_name, day_local),
                )
                row = cursor.fetchone()
        return float((row or [0])[0] or 0.0)

    return 0.0


def enforce_feature_limit(
    user_id: int,
    feature_code: str,
    *,
    requested_units: float = 1.0,
    now_ts_utc: datetime | None = None,
    tz: str = TRIAL_POLICY_TZ,
) -> dict | None:
    entitlement = resolve_entitlement(user_id=int(user_id), now_ts_utc=now_ts_utc, tz=tz)
    effective_mode = str(entitlement.get("effective_mode") or "free")
    plan_code = str(entitlement.get("plan_code") or "free")

    # Product policy: trial has full features, except YouTube safety limit.
    if effective_mode == "trial" and feature_code != "youtube_fetch_daily":
        return None

    lookup_plan = plan_code
    if effective_mode == "trial" and feature_code == "youtube_fetch_daily":
        lookup_plan = "free"
    limit = get_plan_limit(lookup_plan, feature_code, period="day")
    if not limit:
        return None

    usage = _get_feature_usage_today(int(user_id), feature_code, tz=tz)
    requested = max(0.0, float(requested_units or 0.0))
    limit_value = float(limit.get("limit_value") or 0.0)
    if usage + requested > limit_value:
        used_value = float(round(usage, 6))
        limit_out = int(limit_value) if float(limit_value).is_integer() else float(limit_value)
        used_out = int(used_value) if float(used_value).is_integer() else float(used_value)
        return {
            "error": "feature_limit_exceeded",
            "feature": feature_code,
            "limit": limit_out,
            "used": used_out,
            "unit": str(limit.get("limit_unit") or "count"),
            "reset_at": entitlement.get("reset_at"),
            "upgrade": {
                "available": True,
                "plan_code": "pro",
                "action": "checkout",
                "endpoint": "/api/billing/create-checkout-session",
            },
        }
    return None


def get_user_action_month_usage(
    user_id: int,
    action_type: str,
    *,
    units_type: str = "chars",
    provider: str | None = None,
    period_month: date | datetime | None = None,
    tz: str = TRIAL_POLICY_TZ,
) -> float:
    action_value = str(action_type or "").strip().lower()
    units_type_value = str(units_type or "").strip().lower() or "chars"
    provider_value = str(provider or "").strip().lower() or None
    if not action_value:
        return 0.0
    period_start = _month_period_start(period_month, tz=tz)
    if period_start.month == 12:
        period_end = date(period_start.year + 1, 1, 1)
    else:
        period_end = date(period_start.year, period_start.month + 1, 1)

    provider_sql = ""
    provider_params: list = []
    if provider_value:
        provider_sql = " AND provider = %s"
        provider_params.append(provider_value)

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT COALESCE(SUM(units_value), 0)
                FROM bt_3_billing_events
                WHERE user_id = %s
                  AND action_type = %s
                  AND units_type = %s
                  {provider_sql}
                  AND event_time >= %s
                  AND event_time < %s;
                """,
                (
                    int(user_id),
                    action_value,
                    units_type_value,
                    *provider_params,
                    datetime.combine(period_start, dt_time.min, tzinfo=timezone.utc),
                    datetime.combine(period_end, dt_time.min, tzinfo=timezone.utc),
                ),
            )
            row = cursor.fetchone()
    return float((row or [0])[0] or 0.0)


def enforce_reader_audio_pro_monthly_limit(
    user_id: int,
    *,
    requested_units: float = 1.0,
    now_ts_utc: datetime | None = None,
    tz: str = TRIAL_POLICY_TZ,
) -> dict | None:
    used_units = float(
        get_user_action_month_usage(
            int(user_id),
            "reader_audio_tts",
            units_type="chars",
            provider="google_tts",
            period_month=now_ts_utc,
            tz=tz,
        )
    )
    requested = max(0.0, float(requested_units or 0.0))
    limit_value = float(READER_AUDIO_PRO_MONTHLY_LIMIT_CHARS)
    if used_units + requested > limit_value:
        used_out = int(round(used_units))
        limit_out = int(round(limit_value))
        remaining_out = max(0, limit_out - used_out)
        return {
            "error": "reader_audio_monthly_limit_exceeded",
            "feature": "reader_audio_tts_monthly",
            "limit": limit_out,
            "used": used_out,
            "requested": int(round(requested)),
            "remaining": remaining_out,
            "unit": "chars",
            "reset_at": _next_local_month_start_iso(now_ts_utc, tz=tz),
            "message": (
                f"Лимит Reader Audio на месяц исчерпан: {used_out} / {limit_out} chars. "
                f"Следующий сброс: {_next_local_month_start_iso(now_ts_utc, tz=tz)}"
            ),
        }
    return None


def get_today_reminder_settings(user_id: int) -> dict:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT enabled, timezone, reminder_hour, reminder_minute, updated_at
                FROM bt_3_today_reminder_settings
                WHERE user_id = %s
                LIMIT 1;
                """,
                (int(user_id),),
            )
            row = cursor.fetchone()
    if not row:
        return {
            "user_id": int(user_id),
            "enabled": False,
            "timezone": "Europe/Vienna",
            "reminder_hour": 7,
            "reminder_minute": 0,
            "updated_at": None,
        }
    return {
        "user_id": int(user_id),
        "enabled": bool(row[0]),
        "timezone": row[1] or "Europe/Vienna",
        "reminder_hour": int(row[2] or 7),
        "reminder_minute": int(row[3] or 0),
        "updated_at": row[4].isoformat() if row[4] else None,
    }


def upsert_today_reminder_settings(
    user_id: int,
    *,
    enabled: bool,
    timezone_name: str = "Europe/Vienna",
    reminder_hour: int = 7,
    reminder_minute: int = 0,
) -> dict:
    tz_name = (timezone_name or "Europe/Vienna").strip() or "Europe/Vienna"
    hour = max(0, min(int(reminder_hour), 23))
    minute = max(0, min(int(reminder_minute), 59))
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_today_reminder_settings (
                    user_id,
                    enabled,
                    timezone,
                    reminder_hour,
                    reminder_minute,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, NOW())
                ON CONFLICT (user_id) DO UPDATE
                SET
                    enabled = EXCLUDED.enabled,
                    timezone = EXCLUDED.timezone,
                    reminder_hour = EXCLUDED.reminder_hour,
                    reminder_minute = EXCLUDED.reminder_minute,
                    updated_at = NOW()
                RETURNING enabled, timezone, reminder_hour, reminder_minute, updated_at;
                """,
                (int(user_id), bool(enabled), tz_name, hour, minute),
            )
            row = cursor.fetchone()
    return {
        "user_id": int(user_id),
        "enabled": bool(row[0]),
        "timezone": row[1] or "Europe/Vienna",
        "reminder_hour": int(row[2] or 7),
        "reminder_minute": int(row[3] or 0),
        "updated_at": row[4].isoformat() if row[4] else None,
    }


def list_today_reminder_users(limit: int = 1000, offset: int = 0) -> list[dict]:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    s.user_id,
                    COALESCE(NULLIF(u.username, ''), '') AS username,
                    s.timezone,
                    s.reminder_hour,
                    s.reminder_minute
                FROM bt_3_today_reminder_settings s
                LEFT JOIN bt_3_allowed_users u ON u.user_id = s.user_id
                WHERE s.enabled = TRUE
                ORDER BY s.updated_at DESC
                LIMIT %s OFFSET %s;
                """,
                (max(1, min(int(limit), 5000)), max(0, int(offset))),
            )
            rows = cursor.fetchall()
    return [
        {
            "user_id": int(row[0]),
            "username": row[1] or None,
            "timezone": row[2] or "Europe/Vienna",
            "reminder_hour": int(row[3] or 7),
            "reminder_minute": int(row[4] or 0),
        }
        for row in rows
    ]


def get_audio_grammar_settings(user_id: int) -> dict:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT enabled, updated_at
                FROM bt_3_audio_grammar_settings
                WHERE user_id = %s
                LIMIT 1;
                """,
                (int(user_id),),
            )
            row = cursor.fetchone()
    if not row:
        return {
            "user_id": int(user_id),
            "enabled": False,
            "updated_at": None,
        }
    return {
        "user_id": int(user_id),
        "enabled": bool(row[0]),
        "updated_at": row[1].isoformat() if row[1] else None,
    }


def upsert_audio_grammar_settings(user_id: int, *, enabled: bool) -> dict:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_audio_grammar_settings (
                    user_id,
                    enabled,
                    updated_at
                )
                VALUES (%s, %s, NOW())
                ON CONFLICT (user_id) DO UPDATE
                SET
                    enabled = EXCLUDED.enabled,
                    updated_at = NOW()
                RETURNING enabled, updated_at;
                """,
                (int(user_id), bool(enabled)),
            )
            row = cursor.fetchone()
    return {
        "user_id": int(user_id),
        "enabled": bool(row[0]),
        "updated_at": row[1].isoformat() if row[1] else None,
    }


def update_translation_audio_grammar_opt_in(user_id: int, translation_id: int, *, enabled: bool) -> dict:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE bt_3_translations
                SET audio_grammar_opt_in = %s
                WHERE id = %s AND user_id = %s
                RETURNING id, audio_grammar_opt_in, timestamp;
                """,
                (bool(enabled), int(translation_id), int(user_id)),
            )
            row = cursor.fetchone()
    if not row:
        raise ValueError("translation not found")
    return {
        "translation_id": int(row[0]),
        "enabled": bool(row[1]),
        "timestamp": row[2].isoformat() if row[2] else None,
    }


def has_youtube_proxy_subtitles_access(user_id: int) -> bool:
    if not user_id:
        return False
    if int(user_id) in get_admin_telegram_ids():
        return True

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT enabled
                FROM bt_3_youtube_proxy_subtitles_access
                WHERE user_id = %s
                LIMIT 1;
                """,
                (int(user_id),),
            )
            row = cursor.fetchone()
    return bool(row and bool(row[0]))


def upsert_youtube_proxy_subtitles_access(
    *,
    user_id: int,
    enabled: bool = True,
    granted_by: int | None = None,
    note: str | None = None,
) -> dict:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_youtube_proxy_subtitles_access (
                    user_id,
                    enabled,
                    granted_by,
                    note,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, NOW())
                ON CONFLICT (user_id) DO UPDATE
                SET
                    enabled = EXCLUDED.enabled,
                    granted_by = COALESCE(EXCLUDED.granted_by, bt_3_youtube_proxy_subtitles_access.granted_by),
                    note = COALESCE(EXCLUDED.note, bt_3_youtube_proxy_subtitles_access.note),
                    updated_at = NOW()
                RETURNING user_id, enabled, granted_by, note, created_at, updated_at;
                """,
                (int(user_id), bool(enabled), granted_by, note),
            )
            row = cursor.fetchone()
    return {
        "user_id": int(row[0]),
        "enabled": bool(row[1]),
        "granted_by": int(row[2]) if row[2] is not None else None,
        "note": row[3] if row[3] is not None else None,
        "created_at": row[4].isoformat() if row[4] else None,
        "updated_at": row[5].isoformat() if row[5] else None,
    }


def list_youtube_proxy_subtitles_access(
    *,
    limit: int = 200,
    offset: int = 0,
    enabled_only: bool = False,
) -> list[dict]:
    safe_limit = max(1, min(int(limit or 200), 1000))
    safe_offset = max(0, int(offset or 0))
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT user_id, enabled, granted_by, note, created_at, updated_at
                FROM bt_3_youtube_proxy_subtitles_access
                WHERE (%s = FALSE OR enabled = TRUE)
                ORDER BY updated_at DESC, user_id DESC
                LIMIT %s OFFSET %s;
                """,
                (bool(enabled_only), safe_limit, safe_offset),
            )
            rows = cursor.fetchall()
    return [
        {
            "user_id": int(row[0]),
            "enabled": bool(row[1]),
            "granted_by": int(row[2]) if row[2] is not None else None,
            "note": row[3] if row[3] is not None else None,
            "created_at": row[4].isoformat() if row[4] else None,
            "updated_at": row[5].isoformat() if row[5] else None,
        }
        for row in rows
    ]


def upsert_active_quiz(
    poll_id: str,
    *,
    chat_id: int,
    message_id: int | None,
    correct_option_id: int,
    options: list[str],
    correct_text: str | None = None,
    freeform_option: str | None = None,
    quiz_type: str | None = None,
    word_ru: str | None = None,
) -> None:
    payload_options = [str(option) for option in (options or [])]
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bt_3_active_quizzes (
                    poll_id,
                    chat_id,
                    message_id,
                    correct_option_id,
                    correct_text,
                    options,
                    freeform_option,
                    quiz_type,
                    word_ru,
                    created_at
                )
                VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, NOW())
                ON CONFLICT (poll_id) DO UPDATE
                SET
                    chat_id = EXCLUDED.chat_id,
                    message_id = EXCLUDED.message_id,
                    correct_option_id = EXCLUDED.correct_option_id,
                    correct_text = EXCLUDED.correct_text,
                    options = EXCLUDED.options,
                    freeform_option = EXCLUDED.freeform_option,
                    quiz_type = EXCLUDED.quiz_type,
                    word_ru = EXCLUDED.word_ru,
                    created_at = NOW();
                """,
                (
                    str(poll_id),
                    int(chat_id),
                    int(message_id) if message_id is not None else None,
                    int(correct_option_id),
                    (correct_text or None),
                    json.dumps(payload_options, ensure_ascii=False),
                    (freeform_option or None),
                    (quiz_type or None),
                    (word_ru or None),
                ),
            )


def get_active_quiz(poll_id: str) -> dict | None:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    poll_id,
                    chat_id,
                    message_id,
                    correct_option_id,
                    correct_text,
                    options,
                    freeform_option,
                    quiz_type,
                    word_ru,
                    created_at
                FROM bt_3_active_quizzes
                WHERE poll_id = %s
                LIMIT 1;
                """,
                (str(poll_id),),
            )
            row = cursor.fetchone()
    if not row:
        return None
    raw_options = row[5]
    if isinstance(raw_options, str):
        try:
            raw_options = json.loads(raw_options)
        except json.JSONDecodeError:
            raw_options = []
    options = [str(item) for item in (raw_options or [])]
    return {
        "poll_id": str(row[0]),
        "chat_id": int(row[1]),
        "message_id": int(row[2]) if row[2] is not None else None,
        "correct_option_id": int(row[3]),
        "correct_text": row[4] or "",
        "options": options,
        "freeform_option": row[6] or None,
        "quiz_type": row[7] or None,
        "word_ru": row[8] or None,
        "created_at": row[9].isoformat() if row[9] else None,
    }


def delete_active_quiz(poll_id: str) -> bool:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "DELETE FROM bt_3_active_quizzes WHERE poll_id = %s;",
                (str(poll_id),),
            )
            return cursor.rowcount > 0


def list_skills(category: str | None = None, language_code: str | None = None) -> list[dict]:
    lang = (language_code or "").strip().lower()
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            if category:
                if lang:
                    cursor.execute(
                        """
                        SELECT skill_id, title, category, is_active, language_code
                        FROM bt_3_skills
                        WHERE category = %s AND language_code = %s
                        ORDER BY skill_id;
                        """,
                        (category, lang),
                    )
                else:
                    cursor.execute(
                        """
                        SELECT skill_id, title, category, is_active, language_code
                        FROM bt_3_skills
                        WHERE category = %s
                        ORDER BY skill_id;
                        """,
                        (category,),
                    )
            else:
                if lang:
                    cursor.execute(
                        """
                        SELECT skill_id, title, category, is_active, language_code
                        FROM bt_3_skills
                        WHERE language_code = %s
                        ORDER BY category, skill_id;
                        """,
                        (lang,),
                    )
                else:
                    cursor.execute(
                        """
                        SELECT skill_id, title, category, is_active, language_code
                        FROM bt_3_skills
                        ORDER BY language_code, category, skill_id;
                        """
                    )
            rows = cursor.fetchall()
    return [
        {
            "skill_id": row[0],
            "title": row[1],
            "category": row[2],
            "is_active": bool(row[3]),
            "language_code": row[4] or "de",
        }
        for row in rows
    ]


def get_skill_by_id(skill_id: str, language_code: str | None = None) -> dict | None:
    normalized = str(skill_id or "").strip()
    if not normalized:
        return None
    lang = (language_code or "").strip().lower()
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            if lang:
                cursor.execute(
                    """
                    SELECT skill_id, title, category, is_active, language_code
                    FROM bt_3_skills
                    WHERE skill_id = %s AND language_code = %s
                    LIMIT 1;
                    """,
                    (normalized, lang),
                )
            else:
                cursor.execute(
                    """
                    SELECT skill_id, title, category, is_active, language_code
                    FROM bt_3_skills
                    WHERE skill_id = %s
                    LIMIT 1;
                    """,
                    (normalized,),
                )
            row = cursor.fetchone()
    if not row:
        return None
    return {
        "skill_id": row[0],
        "title": row[1],
        "category": row[2],
        "is_active": bool(row[3]),
        "language_code": row[4] or "de",
    }


def get_skill_mapping_for_error(
    error_category: str,
    error_subcategory: str | None,
    language_code: str | None = None,
) -> list[dict]:
    category = str(error_category or "").strip()
    subcategory = str(error_subcategory or "").strip()
    lang = (language_code or "de").strip().lower() or "de"
    fallback_skill = {
        "de": "other_unclassified",
        "en": "en_other_unclassified",
        "es": "es_other_unclassified",
        "it": "it_other_unclassified",
    }.get(lang, "other_unclassified")
    if not category:
        return [{"skill_id": fallback_skill, "weight": 1.0}]

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT skill_id, weight
                FROM bt_3_error_skill_map
                WHERE language_code = %s
                  AND error_category = %s
                  AND error_subcategory = %s
                ORDER BY weight DESC, skill_id ASC;
                """,
                (lang, category, subcategory),
            )
            rows = cursor.fetchall()
            if rows:
                return [{"skill_id": row[0], "weight": float(row[1] or 1.0)} for row in rows]

            cursor.execute(
                """
                SELECT skill_id, weight
                FROM bt_3_error_skill_map
                WHERE language_code = %s
                  AND error_category = %s
                  AND error_subcategory = 'Unclassified mistake'
                ORDER BY weight DESC, skill_id ASC;
                """,
                (lang, category),
            )
            fallback_rows = cursor.fetchall()
            if fallback_rows:
                return [{"skill_id": row[0], "weight": float(row[1] or 1.0)} for row in fallback_rows]

    return [{"skill_id": fallback_skill, "weight": 1.0}]


def _clamp_mastery(value: float) -> float:
    return max(0.0, min(100.0, float(value)))


def apply_user_skill_event(
    *,
    user_id: int,
    skill_id: str,
    source_lang: str = "ru",
    target_lang: str = "de",
    event_type: str,
    base_delta: float,
    event_at: datetime | None = None,
) -> dict:
    normalized_event = str(event_type or "").strip().lower()
    if normalized_event not in {"success", "fail"}:
        raise ValueError("event_type must be success or fail")
    event_at = event_at or datetime.now(timezone.utc)
    if event_at.tzinfo is None:
        event_at = event_at.replace(tzinfo=timezone.utc)

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT mastery, success_streak, fail_streak, total_events, last_practiced_at
                FROM bt_3_user_skill_state
                WHERE user_id = %s
                  AND skill_id = %s
                  AND source_lang = %s
                  AND target_lang = %s
                LIMIT 1;
                """,
                (int(user_id), str(skill_id), source_lang or "ru", target_lang or "de"),
            )
            row = cursor.fetchone()

            mastery = float(row[0]) if row else 50.0
            success_streak = int(row[1]) if row else 0
            fail_streak = int(row[2]) if row else 0
            total_events = int(row[3]) if row else 0
            last_practiced_at = row[4] if row else None

            # Light decay if user did not practice this skill for a while.
            if isinstance(last_practiced_at, datetime):
                last_ts = last_practiced_at if last_practiced_at.tzinfo else last_practiced_at.replace(tzinfo=timezone.utc)
                days_idle = max(0, (event_at.date() - last_ts.date()).days)
                decay = min(8.0, days_idle * 0.15)
                mastery -= decay

            if normalized_event == "success":
                accel = min(success_streak, 5) * 0.2
                effective_delta = max(0.0, float(base_delta)) + accel
                success_streak += 1
                fail_streak = 0
            else:
                accel = min(fail_streak, 5) * 0.3
                effective_delta = min(0.0, float(base_delta)) - accel
                fail_streak += 1
                success_streak = 0

            mastery = _clamp_mastery(mastery + effective_delta)
            total_events += 1

            cursor.execute(
                """
                INSERT INTO bt_3_user_skill_state (
                    user_id,
                    skill_id,
                    source_lang,
                    target_lang,
                    mastery,
                    success_streak,
                    fail_streak,
                    total_events,
                    last_event_delta,
                    last_event_at,
                    last_practiced_at,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (user_id, skill_id, source_lang, target_lang) DO UPDATE
                SET
                    mastery = EXCLUDED.mastery,
                    success_streak = EXCLUDED.success_streak,
                    fail_streak = EXCLUDED.fail_streak,
                    total_events = EXCLUDED.total_events,
                    last_event_delta = EXCLUDED.last_event_delta,
                    last_event_at = EXCLUDED.last_event_at,
                    last_practiced_at = EXCLUDED.last_practiced_at,
                    updated_at = NOW()
                RETURNING mastery, success_streak, fail_streak, total_events, last_event_delta, last_practiced_at;
                """,
                (
                    int(user_id),
                    str(skill_id),
                    source_lang or "ru",
                    target_lang or "de",
                    mastery,
                    success_streak,
                    fail_streak,
                    total_events,
                    float(effective_delta),
                    event_at,
                    event_at,
                ),
            )
            saved = cursor.fetchone()

    return {
        "user_id": int(user_id),
        "skill_id": str(skill_id),
        "source_lang": source_lang or "ru",
        "target_lang": target_lang or "de",
        "mastery": float(saved[0] if saved else mastery),
        "success_streak": int(saved[1] if saved else success_streak),
        "fail_streak": int(saved[2] if saved else fail_streak),
        "total_events": int(saved[3] if saved else total_events),
        "last_event_delta": float(saved[4] if saved else 0.0),
        "last_practiced_at": saved[5].isoformat() if saved and saved[5] else None,
    }


def apply_skill_events_for_error(
    *,
    user_id: int,
    source_lang: str = "ru",
    target_lang: str = "de",
    error_category: str,
    error_subcategory: str | None,
    event_type: str,
    success_delta: float = 2.0,
    fail_delta: float = -3.0,
    event_at: datetime | None = None,
) -> list[dict]:
    mapping = get_skill_mapping_for_error(
        error_category,
        error_subcategory,
        language_code=target_lang or "de",
    )
    base = float(success_delta if str(event_type).lower() == "success" else fail_delta)
    results: list[dict] = []
    for item in mapping:
        skill_id = str(item.get("skill_id") or "").strip()
        weight = float(item.get("weight") or 1.0)
        if not skill_id:
            continue
        try:
            result = apply_user_skill_event(
                user_id=int(user_id),
                skill_id=skill_id,
                source_lang=source_lang or "ru",
                target_lang=target_lang or "de",
                event_type=event_type,
                base_delta=base * weight,
                event_at=event_at,
            )
            results.append(result)
        except Exception:
            continue
    return results


def get_skill_progress_report(
    *,
    user_id: int,
    lookback_days: int = 7,
    source_lang: str | None = None,
    target_lang: str | None = None,
) -> dict:
    window_days = max(1, min(int(lookback_days), 30))
    now_utc = datetime.now(timezone.utc)
    normalized_source_lang = str(source_lang or "ru").strip().lower() or "ru"
    normalized_target_lang = str(target_lang or "de").strip().lower() or "de"

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                WITH err_7d AS (
                    SELECT
                        m.skill_id,
                        SUM(COALESCE(dm.mistake_count, 1))::BIGINT AS errors_7d
                    FROM bt_3_detailed_mistakes dm
                    JOIN bt_3_daily_sentences ds
                      ON ds.id_for_mistake_table = dm.sentence_id
                     AND ds.user_id = dm.user_id
                    JOIN bt_3_error_skill_map m
                      ON m.error_category = COALESCE(NULLIF(dm.main_category, ''), 'Other mistake')
                     AND m.error_subcategory = COALESCE(NULLIF(dm.sub_category, ''), 'Unclassified mistake')
                    WHERE dm.user_id = %s
                      AND m.language_code = %s
                      AND COALESCE(ds.source_lang, 'ru') = COALESCE(%s, 'ru')
                      AND COALESCE(ds.target_lang, 'de') = COALESCE(%s, 'de')
                      AND COALESCE(dm.last_seen, dm.added_data, NOW()) >= NOW() - (%s::text || ' days')::interval
                    GROUP BY m.skill_id
                ),
                err_prev_7d AS (
                    SELECT
                        m.skill_id,
                        SUM(COALESCE(dm.mistake_count, 1))::BIGINT AS errors_prev_7d
                    FROM bt_3_detailed_mistakes dm
                    JOIN bt_3_daily_sentences ds
                      ON ds.id_for_mistake_table = dm.sentence_id
                     AND ds.user_id = dm.user_id
                    JOIN bt_3_error_skill_map m
                      ON m.error_category = COALESCE(NULLIF(dm.main_category, ''), 'Other mistake')
                     AND m.error_subcategory = COALESCE(NULLIF(dm.sub_category, ''), 'Unclassified mistake')
                    WHERE dm.user_id = %s
                      AND m.language_code = %s
                      AND COALESCE(ds.source_lang, 'ru') = COALESCE(%s, 'ru')
                      AND COALESCE(ds.target_lang, 'de') = COALESCE(%s, 'de')
                      AND COALESCE(dm.last_seen, dm.added_data, NOW()) < NOW() - (%s::text || ' days')::interval
                      AND COALESCE(dm.last_seen, dm.added_data, NOW()) >= NOW() - ((%s * 2)::text || ' days')::interval
                    GROUP BY m.skill_id
                )
                SELECT
                    k.skill_id,
                    k.title,
                    k.category,
                    s.mastery AS mastery,
                    COALESCE(s.total_events, 0) AS total_events,
                    COALESCE(e.errors_7d, 0) AS errors_7d,
                    COALESCE(p.errors_prev_7d, 0) AS errors_prev_7d,
                    s.last_practiced_at
                FROM bt_3_skills k
                LEFT JOIN bt_3_user_skill_state s
                  ON s.skill_id = k.skill_id
                 AND s.user_id = %s
                 AND s.source_lang = COALESCE(%s, 'ru')
                 AND s.target_lang = COALESCE(%s, 'de')
                LEFT JOIN err_7d e ON e.skill_id = k.skill_id
                LEFT JOIN err_prev_7d p ON p.skill_id = k.skill_id
                WHERE k.is_active = TRUE
                  AND k.language_code = %s
                ORDER BY k.category ASC, mastery ASC, k.skill_id ASC;
                """,
                (
                    int(user_id),
                    normalized_target_lang,
                    normalized_source_lang,
                    normalized_target_lang,
                    window_days,
                    int(user_id),
                    normalized_target_lang,
                    normalized_source_lang,
                    normalized_target_lang,
                    window_days,
                    window_days,
                    int(user_id),
                    normalized_source_lang,
                    normalized_target_lang,
                    normalized_target_lang,
                ),
            )
            rows = cursor.fetchall()

    skills: list[dict] = []
    groups_map: dict[str, list[dict]] = {}
    for row in rows:
        mastery_raw = row[3]
        total_events = int(row[4] or 0)
        errors_7d = int(row[5] or 0)
        errors_prev_7d = int(row[6] or 0)
        has_data = total_events > 0 and mastery_raw is not None

        mastery: float | None = None
        if has_data:
            mastery = float(mastery_raw)

        if not has_data:
            trend = "none"
            zone = "unknown"
        else:
            if errors_7d < errors_prev_7d:
                trend = "up"
            elif errors_7d > errors_prev_7d:
                trend = "down"
            else:
                trend = "flat"
            if mastery < 40:
                zone = "weak"
            elif mastery < 70:
                zone = "growing"
            elif mastery < 90:
                zone = "confident"
            else:
                zone = "stable"

        skill = {
            "skill_id": str(row[0]),
            "name": str(row[1] or row[0] or ""),
            "group": str(row[2] or "Other"),
            "mastery": round(mastery, 2) if mastery is not None else None,
            "errors_7d": errors_7d,
            "errors_prev_7d": errors_prev_7d,
            "trend": trend,
            "zone": zone,
            "confidence": round(min(1.0, total_events / 20.0), 3) if has_data else 0.0,
            "has_data": has_data,
            "total_events": total_events,
            "last_practiced_at": row[7].isoformat() if row[7] else None,
        }
        skills.append(skill)
        group_name = skill["group"]
        groups_map.setdefault(group_name, []).append(skill)

    skills_with_data = [item for item in skills if bool(item.get("has_data"))]
    top_weak = sorted(
        skills_with_data,
        key=lambda item: (
            float(item.get("mastery") or 0.0),
            -int(item.get("errors_7d") or 0),
            str(item.get("skill_id") or ""),
        ),
    )[:5]
    groups = [
        {"group": group_name, "skills": groups_map[group_name]}
        for group_name in sorted(groups_map.keys())
    ]
    return {
        "updated_at": now_utc.isoformat(),
        "period_days": window_days,
        "top_weak": top_weak,
        "groups": groups,
        "total_skills": len(skills),
        "skills_with_data": len(skills_with_data),
    }


def _get_latest_session_id(cursor, user_id: int) -> str | None:
    cursor.execute(
        """
        SELECT session_id
        FROM bt_3_user_progress
        WHERE user_id = %s AND completed = FALSE
        ORDER BY start_time DESC
        LIMIT 1;
        """,
        (user_id,),
    )
    row = cursor.fetchone()
    return row[0] if row else None


def get_latest_daily_sentences(user_id: int, limit: int = 7) -> list[dict]:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            latest_session_id = _get_latest_session_id(cursor, user_id)
            if not latest_session_id:
                return []

            cursor.execute("""
                SELECT id_for_mistake_table, sentence, unique_id
                FROM bt_3_daily_sentences
                WHERE user_id = %s AND session_id = %s
                ORDER BY unique_id ASC
                LIMIT %s;
            """, (user_id, latest_session_id, limit))
            rows = cursor.fetchall()
            return [
                {
                    "id_for_mistake_table": row[0],
                    "sentence": row[1],
                    "unique_id": row[2],
                }
                for row in rows
            ]


def get_pending_daily_sentences(
    user_id: int,
    limit: int = 7,
    source_lang: str = "ru",
    target_lang: str = "de",
) -> list[dict]:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT up.session_id
                FROM bt_3_user_progress up
                WHERE up.user_id = %s
                  AND up.completed = FALSE
                  AND EXISTS (
                    SELECT 1
                    FROM bt_3_daily_sentences ds
                    WHERE ds.user_id = up.user_id
                      AND ds.session_id = up.session_id
                      AND COALESCE(ds.source_lang, 'ru') = %s
                      AND COALESCE(ds.target_lang, 'de') = %s
                  )
                ORDER BY up.start_time DESC
                LIMIT 1;
                """,
                (user_id, source_lang, target_lang),
            )
            latest_session = cursor.fetchone()
            latest_session_id = latest_session[0] if latest_session else None
            if not latest_session_id:
                return []

            cursor.execute("""
                SELECT ds.id_for_mistake_table, ds.sentence, ds.unique_id
                FROM bt_3_daily_sentences ds
                LEFT JOIN bt_3_translations tr
                    ON tr.user_id = ds.user_id
                    AND tr.sentence_id = ds.id
                    AND tr.session_id = %s
                    AND COALESCE(tr.source_lang, 'ru') = %s
                    AND COALESCE(tr.target_lang, 'de') = %s
                WHERE ds.user_id = %s
                  AND ds.session_id = %s
                  AND COALESCE(ds.source_lang, 'ru') = %s
                  AND COALESCE(ds.target_lang, 'de') = %s
                  AND tr.id IS NULL
                ORDER BY ds.unique_id ASC
                LIMIT %s;
            """, (
                latest_session_id,
                source_lang,
                target_lang,
                user_id,
                latest_session_id,
                source_lang,
                target_lang,
                limit,
            ))
            rows = cursor.fetchall()
            return [
                {
                    "id_for_mistake_table": row[0],
                    "sentence": row[1],
                    "unique_id": row[2],
                }
                for row in rows
            ]

# --- Новые функции для ассистента по продажам ---

async def get_client_by_identifier(identifier: str) -> dict | None:
    """
    Ищет клиента по system_id или номеру телефона.
    Возвращает словарь с данными клиента или None, если клиент не найден.
    """
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT id, first_name, last_name, system_id, phone_number, email, location, manager_contact, is_existing_client
                FROM clients
                WHERE system_id = %s OR phone_number = %s;
            """, (identifier, identifier)) # Поиск по обоим полям
            result = cursor.fetchone()
            if result:
                return {
                    "id": result[0],
                    "first_name": result[1],
                    "last_name": result[2],
                    "system_id": result[3],
                    "phone_number": result[4],
                    "email": result[5],
                    "location": result[6],
                    "manager_contact": result[7],
                    "is_existing_client": result[8]
                }
            return None

async def create_client(
    first_name: str,
    phone_number: str,
    last_name: str = None,
    system_id: str = None,
    email: str = None,
    location: str = None,
    manager_contact: str = None,
    is_existing_client: bool = False
) -> dict:
    """
    Создает новую запись клиента в базе данных.
    Возвращает словарь с данными нового клиента.
    """
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            # Используем ON CONFLICT для обновления, если клиент с таким system_id или phone_number уже существует
            # Это позволяет избежать дубликатов и обновить информацию, если она уже есть
            cursor.execute("""
                INSERT INTO clients (first_name, last_name, system_id, phone_number, email, location, manager_contact, is_existing_client)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (phone_number) DO UPDATE SET -- Конфликт по номеру телефона
                    first_name = EXCLUDED.first_name,
                    last_name = COALESCE(EXCLUDED.last_name, clients.last_name), -- Обновляем, только если новое значение не NULL
                    system_id = COALESCE(EXCLUDED.system_id, clients.system_id),
                    email = COALESCE(EXCLUDED.email, clients.email),
                    location = COALESCE(EXCLUDED.location, clients.location),
                    manager_contact = COALESCE(EXCLUDED.manager_contact, clients.manager_contact),
                    is_existing_client = EXCLUDED.is_existing_client
                RETURNING id, first_name, last_name, system_id, phone_number, email, location, manager_contact, is_existing_client;
            """, (first_name, last_name, system_id, phone_number, email, location, manager_contact, is_existing_client))
            
            result = cursor.fetchone()
            if result:
                return {
                    "id": result[0],
                    "first_name": result[1],
                    "last_name": result[2],
                    "system_id": result[3],
                    "phone_number": result[4],
                    "email": result[5],
                    "location": result[6],
                    "manager_contact": result[7],
                    "is_existing_client": result[8]
                }
            raise RuntimeError("Не удалось создать или обновить клиента")


async def get_new_products() -> list[dict]:
    """
    Возвращает список всех продуктов, помеченных как новинки.
    """
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT id, name, description, price
                FROM products
                WHERE is_new = TRUE;
            """)
            return [{
                "id": row[0],
                "name": row[1],
                "description": row[2],
                "price": float(row[3]) # Преобразуем Decimal в float для удобства
            } for row in cursor.fetchall()]

async def get_product_by_name(product_name: str) -> dict | None:
    """
    Ищет продукт по его названию (регистронезависимо).
    Возвращает словарь с данными продукта или None.
    """
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT id, name, description, price, available_quantity
                FROM products
                WHERE LOWER(name) = LOWER(%s);
            """, (product_name,))
            result = cursor.fetchone()
            if result:
                return {
                    "id": result[0],
                    "name": result[1],
                    "description": result[2],
                    "price": float(result[3]),
                    "available_quantity": result[4]
                }
            return None

async def record_order(
    client_id: int,
    products_with_quantity: list[dict], # Пример: [{"product_id": 1, "quantity": 2}, {"product_id": 4, "quantity": 1}]
    status: str = 'pending'
) -> dict:
    """
    Записывает новый заказ в базу данных.
    products_with_quantity: Список словарей, где каждый словарь содержит 'product_id' и 'quantity'.
    """
    total_amount = 0.0
    order_details_list = [] # Список для хранения деталей заказа для JSONB

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            # Сначала получаем цены продуктов и рассчитываем общую сумму
            for item in products_with_quantity:
                product_id = item["product_id"]
                quantity = item["quantity"]
                
                cursor.execute("SELECT name, price FROM products WHERE id = %s;", (product_id,))
                product_info = cursor.fetchone()
                
                if not product_info:
                    raise ValueError(f"Продукт с ID {product_id} не найден.")
                
                product_name, price_per_item = product_info
                item_total = float(price_per_item) * quantity
                total_amount += item_total
                
                order_details_list.append({
                    "product_id": product_id,
                    "product_name": product_name,
                    "quantity": quantity,
                    "price_per_item": float(price_per_item),
                    "item_total": item_total
                })
            
            # Вставляем новый заказ
            cursor.execute("""
                INSERT INTO orders (client_id, total_amount, order_details, status)
                VALUES (%s, %s, %s, %s)
                RETURNING id, client_id, order_date, status, total_amount, order_details;
            """, (client_id, total_amount, json.dumps(order_details_list), status)) # json.dumps для JSONB.  json.dumps означает "dump string" (выгрузить в строку)
            
            result = cursor.fetchone()
            if result:
                #Когда вы делаете запрос SELECT (в вашем случае через RETURNING), библиотека psycopg2 видит, что данные приходят из колонки типа JSONB.
                # Она автоматически выполняет обратное действие — десериализует данные. Она берет бинарные JSONB-данные из базы, 
                # преобразует их в текстовый JSON, а затем парсит этот текст, создавая из него родной для Python объект
                return {
                    "id": result[0],
                    "client_id": result[1],
                    "order_date": result[2],
                    "status": result[3],
                    "total_amount": float(result[4]),
                    "order_details": result[5] # JSONB возвращается как Python-словарь/список 
                }
            raise RuntimeError("Не удалось записать заказ")


async def get_manager_contact_by_location(location: str) -> str | None:
    """
    Получает контактные данные менеджера, отвечающего за указанную локацию.
    В реальной системе это может быть более сложная логика (таблица managers, зоны покрытия).
    Для простоты пока ищем среди клиентов, у которых указана эта локация и контакт менеджера.
    """
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            # Ищем первого клиента, у которого указана данная локация и есть контакт менеджера
            cursor.execute("""
                SELECT manager_contact
                FROM clients
                WHERE LOWER(location) = LOWER(%s) AND manager_contact IS NOT NULL
                LIMIT 1;
            """, (location,))
            result = cursor.fetchone()
            return result[0] if result else None
