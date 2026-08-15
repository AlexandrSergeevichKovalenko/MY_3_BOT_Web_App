"""Adjektiv Sprint перестаёт быть слепым: тренировка налегает на слабые клетки.

Клеток в таблице окончаний всего 27 (3 типа склонения × 3 падежа × 3 рода), и человек
обычно стабильно валит две-три. До 15.08.2026 генератор эти три величины считал и тут же
ВЫБРАСЫВАЛ — оставалась только русская подсказка строкой, поэтому мерить было нечего.

Формула подбора взята готовой из `backend/weak_spot.py`: там она уже откалибрована после
поломки, найденной в Wo-Fragen 14.08.2026 (неизмеренное с весом 0.5 забивало любое
измеренное с долей ошибок ниже половины).
"""

import unittest
from unittest.mock import patch

import backend.database as db
from backend.adjektiv_endings import build_adjektiv_items
from backend.weak_spot import UNMEASURED_WEIGHT, measured_weight, order_by_weakness


class GeneratorExposesTheCellTests(unittest.TestCase):
    def test_every_item_says_what_it_is_made_of(self):
        for it in build_adjektiv_items(30):
            self.assertIn(it["typ"], ("weak", "mixed", "strong"))
            self.assertIn(it["case"], ("Nom", "Akk", "Dat"))
            self.assertIn(it["gender"], ("m", "f", "n", "pl"), it)

    def test_cell_key_is_stable_and_readable(self):
        key = db.adjektiv_cell_key({"typ": "weak", "case": "Dat", "gender": "f"})
        self.assertEqual(key, "weak:Dat:f")


class WeightingTests(unittest.TestCase):
    def test_unmeasured_sits_above_mastered_and_below_failing(self):
        stats = {"a": {"attempts": 20, "correct": 19},   # освоено, доля ошибок 0.05
                 "b": {"attempts": 20, "correct": 6}}    # валится, доля ошибок 0.70
        self.assertLess(measured_weight(stats, "a"), UNMEASURED_WEIGHT)
        self.assertGreater(measured_weight(stats, "b"), UNMEASURED_WEIGHT)

    def test_thin_data_counts_as_unmeasured(self):
        """Одна промашка из двух — это шум, а не «человек этого не знает»."""
        stats = {"a": {"attempts": 2, "correct": 1}}
        self.assertEqual(measured_weight(stats, "a"), UNMEASURED_WEIGHT)

    def test_weak_cells_dominate_the_deck(self):
        items = ([{"cell": "weak:Dat:f"} for _ in range(20)]
                 + [{"cell": "mixed:Nom:m"} for _ in range(20)])
        stats = {"weak:Dat:f": {"attempts": 20, "correct": 4},
                 "mixed:Nom:m": {"attempts": 20, "correct": 19}}
        hits = shown = 0
        for _ in range(60):
            deck = order_by_weakness(items, stats, lambda i: i["cell"])[:10]
            hits += sum(1 for d in deck if d["cell"] == "weak:Dat:f")
            shown += len(deck)
        self.assertGreater(hits / shown, 0.8,
                           "слабая клетка должна вытеснять освоенную")


class PersonalDeckTests(unittest.TestCase):
    def test_thin_history_keeps_the_plain_deck(self):
        pool = [{"full": f"x{i}"} for i in range(40)]
        with patch.object(db, "pick_adjektiv_payloads", return_value=pool), \
             patch.object(db, "get_user_adjektiv_weakness",
                          return_value={"attempts": 4, "cells": {}}):
            out = db.pick_adjektiv_payloads_for_user(7, 10)
        self.assertEqual([i["full"] for i in out], [i["full"] for i in pool[:10]])

    def test_weak_cell_comes_first_once_there_is_data(self):
        pool = ([{"full": "weak", "typ": "weak", "case": "Dat", "gender": "f"}] * 20
                + [{"full": "easy", "typ": "mixed", "case": "Nom", "gender": "m"}] * 20)
        weak = {"attempts": 40,
                "cells": {"weak:Dat:f": {"attempts": 20, "correct": 3},
                          "mixed:Nom:m": {"attempts": 20, "correct": 19}}}
        with patch.object(db, "pick_adjektiv_payloads", return_value=pool), \
             patch.object(db, "get_user_adjektiv_weakness", return_value=weak):
            out = db.pick_adjektiv_payloads_for_user(7, 10)
        self.assertGreater(sum(1 for i in out if i["full"] == "weak"), 7)

    def test_broken_history_falls_back_to_the_plain_deck(self):
        pool = [{"full": "x"}]
        with patch.object(db, "pick_adjektiv_payloads", return_value=pool), \
             patch.object(db, "get_user_adjektiv_weakness", side_effect=RuntimeError):
            self.assertEqual(len(db.pick_adjektiv_payloads_for_user(7, 5)), 1)


if __name__ == "__main__":
    unittest.main()
