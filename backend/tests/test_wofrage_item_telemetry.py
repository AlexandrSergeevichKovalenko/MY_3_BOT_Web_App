"""Банк Wo-Fragen перестаёт быть слепым.

Раньше хранился только итог по набору («7 из 10»), поэтому сломанное задание было
невидимо: понять, что люди спотыкаются именно на нём, было не из чего — владелец
нашёл ошибку глазами. Теперь пишется ответ по КАЖДОМУ заданию, и банк сам жалуется
на подозрительные: доля верных ответов + поведение неверных вариантов. Так следят
за банками экзаменационных вопросов, и это правило поймало бы пару Woran/Worunter
без чьей-либо догадливости.
"""

import time
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


class SubmitEndpointTests(unittest.TestCase):
    """Связка «человек отправил ответы → засчитали → записали» целиком.

    Раньше эта часть проверялась только живой игрой: пропажу телеметрии заметили бы
    лишь по подозрительно пустой таблице.
    """

    def setUp(self):
        import backend.backend_server as server
        self.server = server
        self.client = server.app.test_client()

    def _patches(self, item, record):
        """Соседние фоновые помощники глушим: они лезут в базу и в Telegram, а нас
        интересует только цепочка «ответ → зачёт → запись». Сами потоки НЕ подменяем:
        подменишь — и эти помощники побегут в этом же потоке, в реальную базу."""
        server = self.server
        return [
            # Привратник /api/webapp/* проверяет подпись initData ДО эндпоинта —
            # подписываем «понарошку», иначе до проверяемой логики не дойти.
            patch.object(server, "_telegram_hash_is_valid", lambda *_a, **_k: True),
            patch.object(server, "_parse_telegram_init_data",
                         lambda *_a, **_k: {"user": {"id": 4242, "first_name": "Тест"}}),
            patch.object(server, "_resolve_webapp_user_allowed", lambda *_a, **_k: (True, "test")),
            patch.object(server, "_maybe_persist_display_name", lambda *_a, **_k: None),
            patch.object(server, "_answer_auth_user_id", return_value=(4242, "Тест", None)),
            patch.object(server, "_unpin_battle_invite_async", lambda *a, **k: None),
            patch.object(server, "_flip_battle_ctas_done_async", lambda *a, **k: None),
            patch("backend.database.get_wofrage_sprint_set", return_value={"items": [item]}),
            patch("backend.database.record_wofrage_sprint_result", return_value=True),
            patch("backend.database.get_wofrage_sprint_result", return_value=None),
            patch("backend.database.compute_wofrage_sprint_ranking", return_value={}),
            patch("backend.database.record_wofrage_item_answers", record),
            patch("backend.database.record_aufgabe_mistake", return_value=None),
        ]

    def _submit(self, item, chosen):
        from contextlib import ExitStack
        self.recorded = {}
        record = lambda rows: self.recorded.setdefault("rows", rows) and len(rows)
        with ExitStack() as stack:
            for p in self._patches(item, record):
                stack.enter_context(p)
            resp = self.client.post("/api/webapp/wofrage/submit", json={
                # initData обязателен: его наличие проверяет привратник ДО эндпоинта,
                # а сам разбор подписи подменён в _patches.
                "initData": "signed",
                "set_id": "s1", "answers": [{"chosen": chosen}], "time_ms": 5000,
            })
            # Запись идёт фоном — ждём её, но недолго и без sleep-наугад.
            deadline = time.monotonic() + 5
            while "rows" not in self.recorded and time.monotonic() < deadline:
                time.sleep(0.02)
            return resp

    def _item_with_two_answers(self, lemma):
        entry = next(e for e in wg._BANK if e["lemma"] == lemma)
        for _ in range(300):
            item = wg._build_one(entry)
            if len(wg.accepted_answers(item)) > 1:
                return item
        self.fail("нет задания с двумя верными формами")

    def test_alternative_answer_is_counted_and_logged(self):
        item = self._item_with_two_answers("leiden an")
        other = sorted(wg.accepted_answers(item) - {item["a"]})[0]
        resp = self._submit(item, other)

        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertEqual(data.get("correct"), 1, "вторая верная форма должна засчитаться")
        review = (data.get("items") or [{}])[0]
        self.assertTrue(review.get("ok"))
        self.assertTrue(review.get("unterschied"), "в разборе нет объяснения разницы")

        rows = self.recorded.get("rows") or []
        self.assertEqual(len(rows), 1, "ответ по заданию не записан")
        self.assertEqual(rows[0]["item_key"], item["key"])
        self.assertTrue(rows[0]["correct"])
        self.assertEqual(rows[0]["chosen"], other)

    def test_wrong_answer_is_logged_as_wrong(self):
        item = wg._build_one(next(e for e in wg._BANK if e["lemma"] == "warten auf"))
        wrong = next(o for o in item["opts"] if o not in wg.accepted_answers(item))
        resp = self._submit(item, wrong)

        self.assertEqual(resp.get_json().get("correct"), 0)
        rows = self.recorded.get("rows") or []
        self.assertEqual(len(rows), 1)
        self.assertFalse(rows[0]["correct"])
        self.assertEqual(rows[0]["chosen"], wrong)

    def test_legacy_sets_without_a_key_still_get_logged(self):
        """Наборы, собранные ДО этой правки, ключа не содержат — считаем его на лету,
        иначе 640 уже лежащих заданий остались бы вне статистики."""
        item = dict(wg._build_one(wg._BANK[0]))
        item.pop("key", None)
        self._submit(item, item["a"])
        rows = self.recorded.get("rows") or []
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["item_key"], wg.item_key(item))


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

    def test_report_carries_the_release_command(self):
        """Владелец не должен разыскивать команду: она стоит рядом с заданием."""
        import bot_3
        row = {"item_key": "abc123", "sentence": "___ leidest du?", "answer": "Woran",
               "lemma": "leiden an", "attempts": 40, "correct_rate": 0.22,
               "verdict": "неверный вариант выбирают чаще верного"}
        text = bot_3._format_wofrage_health_text([row], [row])
        self.assertIn("/wofrage_health release abc123", text)
        self.assertIn("___ leidest du?", text)

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
