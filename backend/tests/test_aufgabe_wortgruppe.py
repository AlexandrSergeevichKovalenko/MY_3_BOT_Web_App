import unittest

from backend.answer_eval import _check_aufgabe, aufgabe_client_meta


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


if __name__ == "__main__":
    unittest.main()
