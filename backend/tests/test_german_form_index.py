"""Индекс форм: откуда система узнаёт, что «Probleme» — это форма от «das Problem».

Без индекса опознание вынуждено догадываться по окончанию, а догадка ошибается в
обе стороны: реальные слова «Stürmer», «Abkommen», «Laster» она принимает за формы
(их просто нет в кэше родов). Догадкой чинить данные нельзя — поэтому проверяем,
что разбор страницы Wiktionary отвечает однозначно и что слабый источник не имеет
права перетереть сильный.

Куски вики-текста ниже — сокращённые, но настоящие: сняты со страниц de.wiktionary
29.07.2026.
"""

import unittest

from backend.article_wiktionary_ref import form_facts_from_wikitext
from backend.german_form_warm import _rows_from_facts
from backend.german_surface import PL, SG, UNKNOWN, _SOURCE_PRIORITY

PROBLEME = """
== Probleme ({{Sprache|Deutsch}}) ==
=== {{Wortart|Deklinierte Form|Deutsch}} ===

{{Grammatische Merkmale}}
*Nominativ Plural des Substantivs '''[[Problem]]'''
*Genitiv Plural des Substantivs '''[[Problem]]'''
*Akkusativ Plural des Substantivs '''[[Problem]]'''

{{Grundformverweis Dekl|Problem}}
"""

KINDES = """
== Kindes ({{Sprache|Deutsch}}) ==
=== {{Wortart|Deklinierte Form|Deutsch}} ===

{{Grammatische Merkmale}}
*Genitiv Singular des Substantivs '''[[Kind]]'''

{{Grundformverweis Dekl|Kind}}
"""

STUERMER = """
== Stürmer ({{Sprache|Deutsch}}) ==
=== {{Wortart|Substantiv|Deutsch}}, {{m}} ===

{{Deutsch Substantiv Übersicht
|Genus=m
|Nominativ Singular=Stürmer
|Nominativ Plural=Stürmer
|Genitiv Singular=Stürmers
}}
"""

PROBLEM = """
== Problem ({{Sprache|Deutsch}}) ==
=== {{Wortart|Substantiv|Deutsch}}, {{n}} ===

{{Deutsch Substantiv Übersicht
|Genus=n
|Nominativ Singular=Problem
|Nominativ Plural=Probleme
|Genitiv Singular=Problems
}}
"""

ELTERN = """
== Eltern ({{Sprache|Deutsch}}) ==
=== {{Wortart|Substantiv|Deutsch}} ===

{{Deutsch Substantiv Übersicht
|Genus=0
|Nominativ Singular=—
|Nominativ Plural=Eltern
|Genitiv Singular=—
|Genitiv Plural=Eltern
}}
"""

# Английская секция на той же странице не должна попасть в разбор немецкой.
NUR_ENGLISCH = """
== Rat ({{Sprache|Englisch}}) ==
=== {{Wortart|Substantiv|Englisch}} ===
{{Englisch Substantiv Übersicht|Singular=rat|Plural=rats}}
"""


class WikitextFormParsingTests(unittest.TestCase):
    def test_declined_form_gives_number_and_lemma(self):
        facts = form_facts_from_wikitext(PROBLEME)
        self.assertFalse(facts["is_lemma"])
        self.assertEqual(facts["number"], PL)
        self.assertEqual(facts["lemma"], "Problem")

    def test_genitive_singular_form_is_not_mistaken_for_plural(self):
        facts = form_facts_from_wikitext(KINDES)
        self.assertEqual(facts["number"], SG)
        self.assertEqual(facts["lemma"], "Kind")

    def test_lemma_page_is_recognised_as_a_word(self):
        """«Stürmer» — настоящее слово. Именно такие правило окончания принимало за
        форму от «Sturm» и глушило верный артикль."""
        facts = form_facts_from_wikitext(STUERMER)
        self.assertTrue(facts["is_lemma"])
        self.assertEqual(facts["number"], "")

    def test_lemma_page_also_yields_its_plural_surface(self):
        facts = form_facts_from_wikitext(PROBLEM)
        self.assertTrue(facts["is_lemma"])
        self.assertEqual(facts["plural_surface"], "Probleme")

    def test_plurale_tantum_page_is_marked_plural(self):
        facts = form_facts_from_wikitext(ELTERN)
        self.assertTrue(facts["is_lemma"])
        self.assertEqual(facts["number"], PL)

    def test_foreign_section_alone_yields_nothing(self):
        facts = form_facts_from_wikitext(NUR_ENGLISCH)
        self.assertFalse(facts["is_lemma"])
        self.assertEqual(facts["number"], "")


class WarmRowsTests(unittest.TestCase):
    def test_form_page_writes_one_row(self):
        rows = _rows_from_facts("Probleme", form_facts_from_wikitext(PROBLEME))
        self.assertEqual(rows, [{"surface": "Probleme", "lemma": "Problem",
                                 "number_tag": PL, "source": "wiktionary"}])

    def test_lemma_page_writes_the_word_and_its_plural(self):
        rows = _rows_from_facts("Problem", form_facts_from_wikitext(PROBLEM))
        self.assertIn({"surface": "Problem", "lemma": "Problem",
                       "number_tag": SG, "source": "wiktionary"}, rows)
        self.assertIn({"surface": "Probleme", "lemma": "Problem",
                       "number_tag": PL, "source": "wiktionary_übersicht"}, rows)

    def test_lemma_whose_plural_equals_itself_writes_no_duplicate(self):
        rows = _rows_from_facts("Stürmer", form_facts_from_wikitext(STUERMER))
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["number_tag"], SG)

    def test_ambiguous_page_writes_nothing(self):
        """Страница без ясного ответа не должна порождать выдумку — вызывающий
        пометит её как «спрашивали, ответа нет»."""
        self.assertEqual(_rows_from_facts("Rat", form_facts_from_wikitext(NUR_ENGLISCH)), [])


class SourcePriorityTests(unittest.TestCase):
    def test_direct_page_outranks_indirect_mentions(self):
        self.assertGreater(_SOURCE_PRIORITY["wiktionary"], _SOURCE_PRIORITY["wiktionary_übersicht"])
        self.assertGreater(_SOURCE_PRIORITY["wiktionary_übersicht"], _SOURCE_PRIORITY["forms_json"])

    def test_no_answer_marker_can_never_outrank_a_real_verdict(self):
        self.assertEqual(_SOURCE_PRIORITY.get("wiktionary_нет_страницы", 0), 0)
        self.assertEqual(UNKNOWN, "unknown")


if __name__ == "__main__":
    unittest.main()
