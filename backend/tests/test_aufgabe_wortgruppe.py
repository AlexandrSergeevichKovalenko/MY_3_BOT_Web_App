import unittest

from backend.answer_eval import _check_aufgabe, aufgabe_client_meta
from backend.database import is_degenerate_aufgabe, wortgruppe_lemma_leak


class AufgabeWortgruppeTests(unittest.TestCase):
    def test_wortgruppe_requires_full_phrase(self) -> None:
        payload = {
            "satz": "____ ist in einer Demokratie besonders wichtig.",
            "correct": "Die Freiheit der Presse",
            "aliases": ["Die Pressefreiheit"],
        }
        self.assertTrue(_check_aufgabe("wortgruppe", payload, "Die Freiheit der Presse"))
        self.assertTrue(_check_aufgabe("wortgruppe", payload, "Die Pressefreiheit"))
        self.assertFalse(_check_aufgabe("wortgruppe", payload, "Freiheit der Presse"))
        self.assertFalse(_check_aufgabe("wortgruppe", payload, "die freiheit"))

    def test_wortgruppe_client_meta_exposes_sentence_only(self) -> None:
        meta = aufgabe_client_meta(
            "wortgruppe",
            {"satz": "____ ist in einer Demokratie besonders wichtig.", "hint_ru": "свобода прессы"},
        )
        self.assertEqual(meta["satz"], "____ ist in einer Demokratie besonders wichtig.")
        self.assertEqual(meta["hint_ru"], "свобода прессы")
        self.assertNotIn("stamm", meta)


class WortgruppeBaseFormTests(unittest.TestCase):
    """The shown Stützwörter are the task's INPUT, never its answer: they must be
    dictionary forms, so the learner supplies every ending, article and case."""

    def test_base_form_lemmas_pass(self) -> None:
        for payload in (
            {"correct": "Infolge der steigenden Nachfrage",
             "lemmas": ["steigend", "Nachfrage"]},
            {"correct": "dass die Folgen des Klimawandels",
             "lemmas": ["Folgen", "Klimawandel"]},
            {"correct": "zwischen den Vorteilen und Nachteilen",
             "lemmas": ["Vorteil", "Nachteil"]},
            {"correct": "die Konsequenzen aus eigenen Fehlern",
             "lemmas": ["Konsequenz", "eigen", "Fehler"]},
            {"correct": "sich mit der Aufgabe zu beschäftigen",
             "lemmas": ["sich beschäftigen", "Aufgabe"]},
            {"correct": "Dank ihrer langjährigen Erfahrung",
             "lemmas": ["ihr", "langjährig", "Erfahrung"]},
        ):
            with self.subTest(payload["correct"]):
                self.assertEqual(wortgruppe_lemma_leak(payload), "")
                self.assertFalse(is_degenerate_aufgabe("wortgruppe", payload))

    def test_inflected_adjective_shown_verbatim_is_rejected(self) -> None:
        payload = {"correct": "trotz der schlechten Wetterbedingungen",
                   "lemmas": ["schlechten", "Wetterbedingungen"]}
        self.assertIn("schlechten", wortgruppe_lemma_leak(payload))
        self.assertTrue(is_degenerate_aufgabe("wortgruppe", payload))

    def test_adjective_in_another_declined_form_is_rejected(self) -> None:
        # Shown "alternde", wanted "alternden" — still not the base form "alternd".
        payload = {"correct": "mit den Problemen der alternden Bevölkerung",
                   "lemmas": ["Probleme", "alternde", "Bevölkerung"]}
        self.assertIn("alternde", wortgruppe_lemma_leak(payload))
        self.assertTrue(is_degenerate_aufgabe("wortgruppe", payload))

    def test_declined_possessive_is_rejected(self) -> None:
        payload = {"correct": "Dank ihrer langjährigen Erfahrung",
                   "lemmas": ["ihrer", "langjährigen", "Erfahrung"]}
        self.assertIn("ihrer", wortgruppe_lemma_leak(payload))
        self.assertTrue(is_degenerate_aufgabe("wortgruppe", payload))

    def test_article_word_or_contraction_is_rejected(self) -> None:
        payload = {"correct": "Im Gegensatz zu den Erwartungen",
                   "lemmas": ["im", "Gegensatz", "Erwartungen"]}
        self.assertIn("im", wortgruppe_lemma_leak(payload))
        self.assertTrue(is_degenerate_aufgabe("wortgruppe", payload))

    def test_item_without_lemmas_stays_degenerate(self) -> None:
        payload = {"correct": "aufgrund neuer Erkenntnisse", "lemmas": []}
        self.assertTrue(wortgruppe_lemma_leak(payload))
        self.assertTrue(is_degenerate_aufgabe("wortgruppe", payload))


class WortgruppeLemmaCoverageTests(unittest.TestCase):
    """The Stützwörter are the word list of THE GAP. A word that isn't in the answer
    is a false lead; a content word of the answer without a Stützwort is guessing."""

    def test_lemma_taken_from_the_visible_sentence_is_rejected(self) -> None:
        # The item served in prod: "Arbeiter" is already printed next to the gap,
        # so offering it as a Stützwort invites the learner to type it twice.
        payload = {
            "satz": "Es ist kaum vorstellbar, _____ die Arbeiter damals arbeiten mussten.",
            "correct": "unter welchen Bedingungen",
            "lemmas": ["Bedingungen", "Arbeiter"],
        }
        self.assertIn("Arbeiter", wortgruppe_lemma_leak(payload))
        self.assertIn("предложении", wortgruppe_lemma_leak(payload))
        self.assertTrue(is_degenerate_aufgabe("wortgruppe", payload))
        # Same item with the stray word dropped is fine.
        payload["lemmas"] = ["Bedingung"]
        self.assertEqual(wortgruppe_lemma_leak(payload), "")

    def test_lemma_absent_from_the_answer_is_rejected(self) -> None:
        payload = {
            "satz": "_____ wurde der Antrag genehmigt.",
            "correct": "Nach sorgfältiger Prüfung aller Unterlagen",
            "lemmas": ["sorgfältig", "Prüfung", "Unterlage", "Behörde"],
        }
        self.assertIn("Behörde", wortgruppe_lemma_leak(payload))
        self.assertTrue(is_degenerate_aufgabe("wortgruppe", payload))

    def test_content_word_without_a_lemma_is_rejected(self) -> None:
        payload = {
            "satz": "Der Erfolg hängt oft davon ab, _____ und energisch zu handeln.",
            "correct": "in einer Situation schnell eine Entscheidung zu treffen",
            "lemmas": ["Situation", "schnell", "Entscheidung"],
        }
        self.assertIn("treffen", wortgruppe_lemma_leak(payload))
        self.assertTrue(is_degenerate_aufgabe("wortgruppe", payload))

    def test_hidden_glue_needs_no_lemma(self) -> None:
        # Preposition / conjunction / article / "zu" / auxiliary are what the learner
        # has to find — they must NOT be counted as uncovered content words.
        for payload in (
            {"satz": "_____ muss die Politik schnell handeln.",
             "correct": "angesichts der neuesten Entwicklungen",
             "lemmas": ["neu", "Entwicklung"]},
            {"satz": "_____ erzielt haben, bleiben Fragen ungeklärt.",
             "correct": "obwohl die Forscher viele Fortschritte",
             "lemmas": ["Forscher", "viel", "Fortschritt"]},
            {"satz": "Die Firma passte sich an, _____ verändert hatten.",
             "correct": "nachdem sich die Marktbedingungen drastisch",
             "lemmas": ["Marktbedingung", "drastisch"]},
            {"satz": "Er war überrascht, _____ auf Zustimmung stieß.",
             "correct": "dass sein Vorschlag",
             "lemmas": ["Vorschlag"]},
            {"satz": "Sie hat sich intensiv _____ beschäftigt.",
             "correct": "mit den möglichen Folgen der Maßnahme",
             "lemmas": ["möglich", "Folge", "Maßnahme"]},
        ):
            with self.subTest(payload["correct"]):
                self.assertEqual(wortgruppe_lemma_leak(payload), "")
                self.assertFalse(is_degenerate_aufgabe("wortgruppe", payload))

    def test_plural_and_umlaut_forms_still_count_as_covered(self) -> None:
        # "Kraft" → "Kräften", "steigen" → "steigenden": inflection changes the end
        # of the word (and may add an umlaut) — the Stützwort is still the same word.
        for payload in (
            {"satz": "_____ konnte das Projekt gerettet werden.",
             "correct": "dank den vereinten Kräften aller Beteiligten",
             "lemmas": ["vereint", "Kraft", "Beteiligte"]},
            {"satz": "_____ müssen Familien sparen.",
             "correct": "wegen der steigenden Lebenshaltungskosten",
             "lemmas": ["steigen", "Lebenshaltungskosten"]},
        ):
            with self.subTest(payload["correct"]):
                self.assertEqual(wortgruppe_lemma_leak(payload), "")


if __name__ == "__main__":
    unittest.main()
