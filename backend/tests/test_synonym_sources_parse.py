"""Разбор {{Synonyme}} / {{Gegenwörter}} de.wiktionary и ключ слова (backend/synonym_sources.py)."""
from __future__ import annotations

from backend.synonym_sources import parse_wiktionary_relations, term_key

WIKITEXT = """== Möglichkeit ({{Sprache|Deutsch}}) ==
=== {{Wortart|Substantiv|Deutsch}}, {{f}} ===
{{Bedeutungen}}
:[1] etwas, das machbar ist
{{Synonyme}}
:[1] [[Alternative]], [[Chance]], [[Option]]
:[2] [[Fähigkeit]], [[Vermögen]]
{{Gegenwörter}}
:[1] [[Unmöglichkeit]]
{{Beispiele}}
:[1] Es gibt keine [[Möglichkeit]].

== Möglichkeit ({{Sprache|Englisch}}) ==
{{Synonyme}}
:[1] [[possibility]]
"""


def test_синонимы_и_антонимы_только_из_немецкого_раздела():
    rel = parse_wiktionary_relations(WIKITEXT)
    assert rel["synonym"] == ["Alternative", "Chance", "Option", "Fähigkeit", "Vermögen"]
    assert rel["antonym"] == ["Unmöglichkeit"]


def test_страницы_нет_это_None_а_не_пустой_список():
    assert parse_wiktionary_relations(None) is None
    assert parse_wiktionary_relations("== x ({{Sprache|Deutsch}}) ==\n{{Bedeutungen}}\n:[1] y") == \
        {"synonym": [], "antonym": []}


def test_ключ_слова_срезает_артикль_sich_и_пометки():
    assert term_key("die Option") == term_key("Option (fachspr.)") == "option"
    assert term_key("sich erinnern") == term_key("(sich) erinnern") == "erinnern"
    assert term_key("schön") != term_key("schon")
