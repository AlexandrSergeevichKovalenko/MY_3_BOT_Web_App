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


if __name__ == "__main__":
    unittest.main()
