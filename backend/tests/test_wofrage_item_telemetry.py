"""Банк Wo-Fragen перестаёт быть слепым.

Раньше хранился только итог по набору («7 из 10»), поэтому сломанное задание было
невидимо: понять, что люди спотыкаются именно на нём, было не из чего — владелец
нашёл ошибку глазами. Теперь пишется ответ по КАЖДОМУ заданию, и банк сам жалуется
на подозрительные: доля верных ответов + поведение неверных вариантов. Так следят
за банками экзаменационных вопросов, и это правило поймало бы пару Woran/Worunter
без чьей-либо догадливости.
"""

import unittest
from unittest.mock import MagicMock, patch

import backend.database as db
import backend.wofrage_generator as wg


class AcceptAnyCorrectAnswerTests(unittest.TestCase):
    """Где обе формы — немецкий, засчитываем любую и объясняем разницу."""

    def _item(self, lemma):
        entry = next(e for e in wg._BANK if e["lemma"] == lemma)
        for _ in range(200):
            item = wg._build_one(entry)
            if len(wg.accepted_answers(item)) > 1:
                return item
        self.fail(f"{lemma}: не удалось получить задание с двумя верными формами")

    def test_both_forms_are_accepted(self):
        item = self._item("leiden an")
        self.assertEqual(wg.accepted_answers(item), {"Woran", "Worunter"})
        for form in ("Woran", "Worunter"):
            self.assertIn(form, item["opts"], "обе верные формы должны быть на кнопках")

    def test_difference_is_explained(self):
        item = self._item("sich freuen auf")
        self.assertIn("auf", item["unterschied"])
        self.assertIn("über", item["unterschied"])

    def test_wrong_form_is_still_wrong(self):
        item = self._item("leiden an")
        self.assertNotIn("Wofür", wg.accepted_answers(item))

    def test_undeclared_verbs_accept_exactly_one(self):
        entry = next(e for e in wg._BANK if e["lemma"] == "warten auf")
        item = wg._build_one(entry)
        self.assertEqual(len(wg.accepted_answers(item)), 1)


class ItemHealthRuleTests(unittest.TestCase):
    """Правило разбраковки: неверный вариант популярнее верного → задание сломано."""

    def _rows(self, rows):
        cursor = MagicMock()
        cursor.fetchall.return_value = rows
        cursor.__enter__ = lambda s: s
        cursor.__exit__ = lambda s, *a: False
        conn = MagicMock()
        conn.cursor.return_value = cursor
        ctx = MagicMock()
        ctx.__enter__ = lambda s: conn
        ctx.__exit__ = lambda s, *a: False
        return ctx

    def test_wrong_answer_beating_the_key_is_flagged(self):
        rows = [
            # ключ, попыток, верных, фраза, лемма, ответ, объект, самый частый неверный, сколько раз
            ("k1", 40, 9, "___ leidest du?", "leiden an", "Woran", "die Grippe", "Worunter", 28),
            ("k2", 40, 33, "___ wartest du?", "warten auf", "Worauf", "der Bus", "Auf wen", 4),
        ]
        with patch.object(db, "ensure_wofrage_sprint_schema", lambda: None), \
             patch.object(db, "get_db_connection_context", return_value=self._rows(rows)):
            health = db.get_wofrage_item_health(min_attempts=12)
        by_key = {h["item_key"]: h for h in health}
        self.assertTrue(by_key["k1"]["wrong_beats_key"], "второй верный ответ должен всплыть в цифрах")
        self.assertFalse(by_key["k2"]["wrong_beats_key"])
        self.assertAlmostEqual(by_key["k2"]["correct_rate"], 0.825, places=3)

    def test_quarantined_items_are_not_served(self):
        built = [dict(wg._build_one(wg._BANK[i]), key=f"key{i}") for i in range(6)]
        with patch("backend.wofrage_generator.build_wofrage_items", return_value=built), \
             patch.object(db, "get_quarantined_wofrage_keys", return_value={"key0", "key3"}):
            out = db.pick_wofrage_payloads(4)
        self.assertTrue(out, "выдача не должна опустеть из-за карантина")
        self.assertFalse({i["key"] for i in out} & {"key0", "key3"},
                         "задание из карантина попало человеку")


class NightlyWatchdogTests(unittest.TestCase):
    """Ночной сторож: подозрительное задание уходит из выдачи САМО, не дожидаясь рук."""

    def test_verdicts(self):
        import bot_3
        two_answers = {"wrong_beats_key": True, "top_wrong": "Worunter", "answer": "Woran",
                       "correct_rate": 0.22, "attempts": 40}
        verdict = bot_3._wofrage_health_verdict(two_answers)
        self.assertIn("чаще верного", verdict)

        too_hard = {"wrong_beats_key": False, "top_wrong": "Auf wen", "answer": "Worauf",
                    "correct_rate": 0.20, "attempts": 30}
        self.assertIn("20%", bot_3._wofrage_health_verdict(too_hard))

        healthy = {"wrong_beats_key": False, "top_wrong": "Auf wen", "answer": "Worauf",
                   "correct_rate": 0.83, "attempts": 30}
        self.assertIsNone(bot_3._wofrage_health_verdict(healthy),
                          "здоровое задание не должно попадать в карантин")

    def test_threshold_is_not_quietly_loosened(self):
        import bot_3
        self.assertLessEqual(bot_3.WOFRAGE_HEALTH_MIN_ATTEMPTS, 20,
                             "слишком высокий порог = сторож молчит месяцами")
        self.assertGreaterEqual(bot_3.WOFRAGE_HEALTH_MIN_RATE, 0.3)


class PersonalisedPracticeTests(unittest.TestCase):
    """Дневной набор общий (по нему сравнивают всех), а личная тренировка — под человека."""

    def test_weak_prepositions_come_first(self):
        pool = []
        for prep in ("auf", "mit", "über", "von", "zu", "an"):
            entry = next(e for e in wg._BANK if e["prep"] == prep and not e.get("person_only"))
            pool.extend(dict(wg._build_one(entry)) for _ in range(4))
        weakness = {
            "attempts": 60, "correct": 30,
            "preps": {"auf": {"attempts": 20, "correct": 3},    # валится
                      "mit": {"attempts": 20, "correct": 19},   # освоено
                      "über": {"attempts": 20, "correct": 8}},
            "targets": {},
        }
        with patch.object(db, "pick_wofrage_payloads", return_value=pool), \
             patch.object(db, "get_user_wofrage_weakness", return_value=weakness):
            out = db.pick_wofrage_payloads_for_user(777, 6)
        preps = [i["prep"] for i in out]
        self.assertGreater(preps.count("auf"), preps.count("mit"),
                           f"слабый предлог должен идти чаще освоенного: {preps}")

    def test_falls_back_to_plain_selection_while_data_is_thin(self):
        pool = [dict(wg._build_one(e)) for e in wg._BANK[:8]]
        thin = {"attempts": 4, "correct": 2, "preps": {}, "targets": {}}
        with patch.object(db, "pick_wofrage_payloads", return_value=pool), \
             patch.object(db, "get_user_wofrage_weakness", return_value=thin):
            out = db.pick_wofrage_payloads_for_user(777, 5)
        self.assertEqual([i["s"] for i in out], [i["s"] for i in pool[:5]])


if __name__ == "__main__":
    unittest.main()
