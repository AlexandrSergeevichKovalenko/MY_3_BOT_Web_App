from __future__ import annotations

from typing import Any

STORY_TOPIC_LABEL = "🧩 ЗАГАДОЧНАЯ ИСТОРИЯ"
CUSTOM_FOCUS_LABEL = "✍️ Свой грамматический фокус"

GRAMMAR_FOCUS_PRESETS: list[dict[str, Any]] = [
    {
        "key": "main_clause_v2",
        "label": "🧱 V2 в главном предложении",
        "prompt_topic": "Verbzweit im Hauptsatz",
        "main_categories": ["Verbs", "Word Order"],
        "subcategories": ["Verb Placement in Main Clause", "Verb-Second Rule (V2)"],
    },
    {
        "key": "subordinate_clause_word_order",
        "label": "🔗 Порядок слов в придаточном",
        "prompt_topic": "Wortstellung im Nebensatz",
        "main_categories": ["Verbs", "Word Order"],
        "subcategories": ["Verb Placement in Subordinate Clause", "Incorrect Order in Subordinate Clause"],
    },
    {
        "key": "subordinating_conjunctions",
        "label": "🪢 weil / dass / wenn / obwohl",
        "prompt_topic": "Nebensaetze mit weil, dass, wenn, obwohl",
        "main_categories": ["Conjunctions", "Clauses & Sentence Types"],
        "subcategories": [
            "Subordinating (weil/dass/ob/wenn...)",
            "Main vs Subordinate Clause",
            "Conditionals (wenn/falls)",
            "Concessive Clauses (obwohl)",
        ],
    },
    {
        "key": "accusative_dative",
        "label": "🧭 Akkusativ и Dativ",
        "prompt_topic": "Akkusativ und Dativ",
        "main_categories": ["Cases"],
        "subcategories": ["Accusative", "Dative"],
    },
    {
        "key": "wechselpraepositionen",
        "label": "📍 Wechselpraepositionen",
        "prompt_topic": "Wechselpraepositionen",
        "main_categories": ["Cases", "Prepositions"],
        "subcategories": ["Two-way Prepositions (Wechselpräpositionen)", "Two-way Prepositions"],
    },
    {
        "key": "articles",
        "label": "👑 Артикли: der / die / das / ein / eine / kein",
        "prompt_topic": "Artikel im Deutschen",
        "main_categories": ["Articles & Determiners"],
        "subcategories": [
            "Definite Articles (der/die/das)",
            "Indefinite Articles (ein/eine)",
            "Negation Article (kein)",
        ],
    },
    {
        "key": "adjective_declension",
        "label": "🎨 Склонение прилагательных",
        "prompt_topic": "Adjektivdeklination",
        "main_categories": ["Adjectives"],
        "subcategories": [
            "Endings",
            "Weak Declension",
            "Strong Declension",
            "Mixed Declension",
            "Incorrect Adjective Case Agreement",
        ],
    },
    {
        "key": "separable_verbs",
        "label": "🔁 Отделяемые глаголы",
        "prompt_topic": "Trennbare Verben",
        "main_categories": ["Verbs", "Word Order"],
        "subcategories": ["Separable Verbs", "Placement of Separable Prefix"],
    },
    {
        "key": "modal_verbs",
        "label": "🛠️ Модальные глаголы",
        "prompt_topic": "Modalverben",
        "main_categories": ["Verbs", "Word Order"],
        "subcategories": ["Modal Verbs", "Incorrect Order with Modal Verb"],
    },
    {
        "key": "reflexive_verbs",
        "label": "🪞 Возвратные глаголы",
        "prompt_topic": "Reflexive Verben",
        "main_categories": ["Verbs", "Pronouns"],
        "subcategories": ["Reflexive Verbs", "Reflexive Pronouns"],
    },
    {
        "key": "perfekt_praeteritum",
        "label": "⏳ Perfekt и Praeteritum",
        "prompt_topic": "Perfekt und Praeteritum",
        "main_categories": ["Tenses"],
        "subcategories": [
            "Present Perfect (Perfekt)",
            "Simple Past (Präteritum)",
            "Tense Choice (context mismatch)",
        ],
    },
    {
        "key": "konjunktiv_ii",
        "label": "💭 Konjunktiv II",
        "prompt_topic": "Konjunktiv II",
        "main_categories": ["Moods"],
        "subcategories": [
            "Subjunctive 2 (Konjunktiv II)",
            "Konjunktiv II: würde-Form",
            "Irrealis / Hypothetical",
        ],
    },
    {
        "key": "passive_voice",
        "label": "🧮 Passiv",
        "prompt_topic": "Passiv im Deutschen",
        "main_categories": ["Voice (Active/Passive)"],
        "subcategories": [
            "Vorgangspassiv (werden + Partizip II)",
            "Zustandspassiv (sein + Partizip II)",
            "Passive Word Order",
        ],
    },
    {
        "key": "relative_clauses",
        "label": "🔍 Relativsaetze",
        "prompt_topic": "Relativsaetze",
        "main_categories": ["Clauses & Sentence Types", "Pronouns", "Punctuation"],
        "subcategories": ["Relative Clauses", "Relative Pronouns", "Comma in Relative Clause"],
    },
    {
        "key": "zu_infinitive",
        "label": "⚙️ zu + Infinitiv / um ... zu",
        "prompt_topic": "zu + Infinitiv und um ... zu",
        "main_categories": ["Infinitive & Participles", "Clauses & Sentence Types", "Verbs"],
        "subcategories": [
            "zu + Infinitive",
            "um...zu",
            "Infinitive Clauses vs dass-clause",
            "Infinitive Form Errors",
        ],
    },
]

WEBAPP_TOPICS = [
    STORY_TOPIC_LABEL,
    *[str(item["label"]) for item in GRAMMAR_FOCUS_PRESETS],
    CUSTOM_FOCUS_LABEL,
]


def get_grammar_focus_by_label(label: str | None) -> dict[str, Any] | None:
    normalized = str(label or "").strip()
    if not normalized:
        return None
    for item in GRAMMAR_FOCUS_PRESETS:
        if str(item.get("label") or "").strip() == normalized:
            return {
                "kind": "preset",
                "key": str(item.get("key") or "").strip(),
                "label": str(item.get("label") or "").strip(),
                "prompt_topic": str(item.get("prompt_topic") or "").strip(),
                "main_categories": [str(value).strip() for value in (item.get("main_categories") or []) if str(value).strip()],
                "subcategories": [str(value).strip() for value in (item.get("subcategories") or []) if str(value).strip()],
            }
    return None


def get_grammar_focus_by_key(key: str | None) -> dict[str, Any] | None:
    normalized = str(key or "").strip()
    if not normalized:
        return None
    for item in GRAMMAR_FOCUS_PRESETS:
        if str(item.get("key") or "").strip() == normalized:
            return {
                "kind": "preset",
                "key": str(item.get("key") or "").strip(),
                "label": str(item.get("label") or "").strip(),
                "prompt_topic": str(item.get("prompt_topic") or "").strip(),
                "main_categories": [str(value).strip() for value in (item.get("main_categories") or []) if str(value).strip()],
                "subcategories": [str(value).strip() for value in (item.get("subcategories") or []) if str(value).strip()],
            }
    return None


def recommend_webapp_focus_for_error_pair(
    main_category: str | None,
    sub_category: str | None,
) -> dict[str, str]:
    normalized_main = str(main_category or "").strip()
    normalized_sub = str(sub_category or "").strip()

    if normalized_sub:
        for item in GRAMMAR_FOCUS_PRESETS:
            preset_subcategories = {
                str(value or "").strip()
                for value in (item.get("subcategories") or [])
                if str(value or "").strip()
            }
            if normalized_sub in preset_subcategories:
                return {
                    "label": str(item.get("label") or "").strip(),
                    "custom_focus": "",
                }

    if normalized_main:
        matching_by_main = []
        for item in GRAMMAR_FOCUS_PRESETS:
            preset_main_categories = {
                str(value or "").strip()
                for value in (item.get("main_categories") or [])
                if str(value or "").strip()
            }
            if normalized_main in preset_main_categories:
                matching_by_main.append(item)
        if len(matching_by_main) == 1:
            return {
                "label": str(matching_by_main[0].get("label") or "").strip(),
                "custom_focus": "",
            }

    custom_focus = normalized_sub or normalized_main
    return {
        "label": CUSTOM_FOCUS_LABEL,
        "custom_focus": custom_focus,
    }


def resolve_webapp_focus(topic_label: str | None, custom_focus: str | None = None) -> dict[str, Any]:
    raw_label = str(topic_label or "").strip()
    custom_text = " ".join(str(custom_focus or "").strip().split())

    if "ЗАГАДОЧНАЯ ИСТОРИЯ" in raw_label:
        return {
            "kind": "story",
            "key": "story",
            "label": STORY_TOPIC_LABEL,
            "prompt_topic": STORY_TOPIC_LABEL,
            "main_categories": [],
            "subcategories": [],
            "custom_text": "",
        }

    if raw_label == CUSTOM_FOCUS_LABEL:
        return {
            "kind": "custom",
            "key": "custom",
            "label": CUSTOM_FOCUS_LABEL,
            "prompt_topic": custom_text or "Deutsche Grammatik",
            "main_categories": [],
            "subcategories": [],
            "custom_text": custom_text,
        }

    preset = get_grammar_focus_by_label(raw_label)
    if preset:
        return {**preset, "custom_text": ""}

    fallback_topic = raw_label or "Random sentences"
    return {
        "kind": "legacy",
        "key": "",
        "label": fallback_topic,
        "prompt_topic": fallback_topic,
        "main_categories": [],
        "subcategories": [],
        "custom_text": "",
    }


def focus_matches_error_pair(focus: dict[str, Any] | None, main_category: str | None, sub_category: str | None) -> bool:
    if not isinstance(focus, dict) or str(focus.get("kind") or "") != "preset":
        return False
    normalized_sub = str(sub_category or "").strip()
    normalized_main = str(main_category or "").strip()
    focus_subcategories = {str(item or "").strip() for item in (focus.get("subcategories") or []) if str(item or "").strip()}
    focus_main_categories = {str(item or "").strip() for item in (focus.get("main_categories") or []) if str(item or "").strip()}
    if focus_subcategories:
        return normalized_sub in focus_subcategories
    if focus_main_categories:
        return normalized_main in focus_main_categories
    return False
