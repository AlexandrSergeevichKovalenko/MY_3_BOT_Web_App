import psycopg2
from psycopg2 import Binary
from psycopg2 import OperationalError
import os
from contextlib import contextmanager
import json
from datetime import datetime, timezone, date, timedelta
from pathlib import Path
import time
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
        f"NULLIF({alias_prefix}response_json#>>'{{language_pair,source_lang}}', '')"
        "))"
    )
    target_expr = (
        "LOWER(COALESCE("
        f"NULLIF({alias_prefix}target_lang, ''), "
        f"NULLIF({alias_prefix}response_json->>'target_lang', ''), "
        f"NULLIF({alias_prefix}response_json#>>'{{language_pair,target_lang}}', '')"
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
                CREATE TABLE IF NOT EXISTS bt_3_webapp_dictionary_queries (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    word_ru TEXT,
                    translation_de TEXT,
                    word_de TEXT,
                    translation_ru TEXT,
                    source_lang TEXT,
                    target_lang TEXT,
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
            cursor.execute(
                """
                DO $$
                BEGIN
                    IF to_regclass('public.bt_3_translations') IS NOT NULL THEN
                        ALTER TABLE bt_3_translations ADD COLUMN IF NOT EXISTS source_lang TEXT;
                        ALTER TABLE bt_3_translations ADD COLUMN IF NOT EXISTS target_lang TEXT;
                        CREATE INDEX IF NOT EXISTS idx_bt_3_translations_user_lang_ts
                        ON bt_3_translations (user_id, source_lang, target_lang, timestamp DESC);
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
) -> None:
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
                    response_json
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
            """, (
                user_id,
                word_ru,
                folder_id,
                translation_de,
                word_de,
                translation_ru,
                source_lang,
                target_lang,
                json.dumps(response_json, ensure_ascii=False),
            ))


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
                SELECT id, word_ru, translation_de, word_de, translation_ru, source_lang, target_lang, response_json, folder_id, created_at
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
            "response_json": row[7],
            "folder_id": row[8],
            "created_at": row[9].isoformat() if row[9] else None,
        })
    return items


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
                "response_json": row[7],
                "folder_id": row[8],
                "created_at": row[9].isoformat() if row[9] else None,
            }


def get_random_dictionary_entry(cooldown_days: int = 5) -> dict | None:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT
                    id,
                    user_id,
                    word_ru,
                    translation_de,
                    response_json
                FROM bt_3_webapp_dictionary_queries
                WHERE response_json IS NOT NULL
                  AND NOT EXISTS (
                      SELECT 1
                      FROM bt_3_quiz_history h
                      WHERE h.word_ru = bt_3_webapp_dictionary_queries.word_ru
                        AND h.asked_at >= NOW() - INTERVAL %s
                  )
                ORDER BY RANDOM()
                LIMIT 1;
            """, (f"{cooldown_days} days",))
            row = cursor.fetchone()
            if not row:
                cursor.execute("""
                    SELECT
                        id,
                        user_id,
                        word_ru,
                        translation_de,
                        response_json
                    FROM bt_3_webapp_dictionary_queries
                    WHERE response_json IS NOT NULL
                    ORDER BY RANDOM()
                    LIMIT 1;
                """)
                row = cursor.fetchone()
                if not row:
                    return None
            return {
                "id": row[0],
                "user_id": row[1],
                "word_ru": row[2],
                "translation_de": row[3],
                "response_json": row[4],
            }


def get_random_dictionary_entry_for_quiz_type(quiz_type: str, cooldown_days: int = 5) -> dict | None:
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
                    response_json
                FROM bt_3_webapp_dictionary_queries
                WHERE response_json IS NOT NULL
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
                (f"{cooldown_days} days",),
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
                        response_json
                    FROM bt_3_webapp_dictionary_queries
                    WHERE response_json IS NOT NULL
                      AND {extra_where}
                    ORDER BY RANDOM()
                    LIMIT 1;
                    """
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
) -> list[dict]:
    if not user_id:
        return []
    wrong_ids: list[int] = []
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            wrong_where = "s.user_id = %s AND s.last_result = FALSE"
            wrong_params = [user_id]
            language_filter_sql_q, language_params = _build_language_pair_filter(
                source_lang,
                target_lang,
                table_alias="q",
            )
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

            if len(wrong_ids) < wrong_size:
                extra_where = "s.user_id = %s AND s.entry_id <> ALL(%s::bigint[])"
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

            base_where = "user_id = %s AND id <> ALL(%s::bigint[])"
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
            base_params.extend([user_id, max(set_size - len(wrong_ids), 0)])
            cursor.execute(f"""
                SELECT id, word_ru, translation_de, word_de, translation_ru, response_json
                FROM bt_3_webapp_dictionary_queries
                WHERE {base_where}
                  AND id NOT IN (
                      SELECT entry_id
                      FROM bt_3_flashcard_seen
                      WHERE user_id = %s
                        AND seen_at >= NOW() - INTERVAL '2 days'
                  )
                ORDER BY RANDOM()
                LIMIT %s;
            """, base_params)
            random_rows = cursor.fetchall()

            if len(random_rows) < max(set_size - len(wrong_ids), 0):
                fallback_where = "user_id = %s AND id <> ALL(%s::bigint[])"
                fallback_params = [user_id, wrong_ids or [0]]
                if language_filter_sql:
                    fallback_where += language_filter_sql
                    fallback_params.extend(language_params_no_alias)
                if folder_mode == "folder" and folder_id is not None:
                    fallback_where += " AND folder_id = %s"
                    fallback_params.append(folder_id)
                elif folder_mode == "none":
                    fallback_where += " AND folder_id IS NULL"
                fallback_params.append(max(set_size - len(wrong_ids), 0))
                cursor.execute(f"""
                SELECT id, word_ru, translation_de, word_de, translation_ru, response_json
                FROM bt_3_webapp_dictionary_queries
                WHERE {fallback_where}
                ORDER BY RANDOM()
                LIMIT %s;
            """, fallback_params)
                random_rows = cursor.fetchall()

            if wrong_ids:
                wrong_where = "user_id = %s AND id = ANY(%s::bigint[])"
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
) -> int:
    now_utc = now_utc or datetime.now(timezone.utc)
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
            row = cursor.fetchone()
            return int(row[0] if row else 0)


def count_new_cards_introduced_today(
    user_id: int,
    now_utc: datetime | None = None,
    source_lang: str | None = None,
    target_lang: str | None = None,
) -> int:
    now_utc = now_utc or datetime.now(timezone.utc)
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
                FROM bt_3_card_srs_state s
                JOIN bt_3_webapp_dictionary_queries q
                  ON q.id = s.card_id AND q.user_id = s.user_id
                WHERE s.user_id = %s
                  AND DATE(s.created_at AT TIME ZONE 'UTC') = DATE(%s AT TIME ZONE 'UTC')
                  {language_filter_sql};
                """,
                [int(user_id), now_utc, *language_params],
            )
            row = cursor.fetchone()
            return int(row[0] if row else 0)


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
) -> dict | None:
    now_utc = now_utc or datetime.now(timezone.utc)
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            language_filter_sql, language_params = _build_language_pair_filter(
                source_lang,
                target_lang,
                table_alias="q",
            )
            cursor.execute(
                f"""
                SELECT
                    s.card_id,
                    s.status,
                    s.due_at,
                    s.interval_days,
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
            row = cursor.fetchone()
            if not row:
                return None
            return {
                "card": {
                    "id": row[0],
                    "word_ru": row[6],
                    "translation_de": row[7],
                    "word_de": row[8],
                    "translation_ru": row[9],
                    "response_json": row[10],
                },
                "srs": {
                    "status": row[1],
                    "due_at": row[2],
                    "interval_days": int(row[3] or 0),
                    "stability": float(row[4] or 0.0),
                    "difficulty": float(row[5] or 0.0),
                },
            }


def get_next_new_srs_candidate(
    user_id: int,
    source_lang: str | None = None,
    target_lang: str | None = None,
) -> dict | None:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            language_filter_sql, language_params = _build_language_pair_filter(
                source_lang,
                target_lang,
                table_alias="q",
            )
            cursor.execute(
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
            row = cursor.fetchone()
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


def ensure_new_srs_state(user_id: int, card_id: int, now_utc: datetime | None = None) -> dict:
    now_utc = now_utc or datetime.now(timezone.utc)
    state = get_card_srs_state(user_id=user_id, card_id=card_id)
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
                (normalized, normalized, int(item_id), int(user_id)),
            )
            row = cursor.fetchone()
            if not row:
                return None
            return _map_daily_plan_item(row)


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
                    COALESCE(s.mastery, 50.0) AS mastery,
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
        mastery = float(row[3] or 0.0)
        total_events = int(row[4] or 0)
        errors_7d = int(row[5] or 0)
        errors_prev_7d = int(row[6] or 0)
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
            "mastery": round(mastery, 2),
            "errors_7d": errors_7d,
            "errors_prev_7d": errors_prev_7d,
            "trend": trend,
            "zone": zone,
            "confidence": round(min(1.0, total_events / 20.0), 3),
            "total_events": total_events,
            "last_practiced_at": row[7].isoformat() if row[7] else None,
        }
        skills.append(skill)
        group_name = skill["group"]
        groups_map.setdefault(group_name, []).append(skill)

    top_weak = sorted(
        skills,
        key=lambda item: (
            float(item.get("mastery") or 50.0),
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
