"""Self-paced Zahlen-Diktat practice trainer (kind np): deterministic grade,
no answer leak, no leaderboard/mastery side-effects, daily-cap behaviour, and the
endless-feed serve/grade endpoints. Pure unit tests (DB calls are patched) — the
per-user seen-cycling SQL + last_sent_at isolation are verified on staging (see plan)."""
from contextlib import ExitStack
import unittest
from unittest.mock import patch

import backend.answer_eval as ev
import backend.backend_server as server


READY_ITEM = {
    "numdict_id": "nd-1",
    "scenario_text": "Ruf mich an: «NUM»030 12 34 56«/NUM».",
    "number_type": "telephone",
    "answer_value": "0301234 56",
    "display_answer": "030 12 34 56",
    "prompt_de": "Welche Telefonnummer?",
    "prompt_ru": "Какой номер телефона?",
    "input_mode": "numeric",
    "audio_object_key": "numdict/audio/nd-1.v3.mp3",
    "audio_status": "ready",
}


class GradeTests(unittest.TestCase):
    def _grade(self, typed, item=None):
        with patch.object(ev, "_normalize_numdict", wraps=ev._normalize_numdict), \
             patch("backend.database.get_numdict_bank_item", return_value=item or READY_ITEM):
            return ev.grade_numdict_practice_item(user_id=77, numdict_id="nd-1", typed=typed)

    def test_numeric_normalization_accepts_spacing_and_punctuation(self):
        # "0301234 56" (answer_value) vs a differently grouped/punctuated entry.
        res = self._grade("030 12-34.56")
        self.assertTrue(res["is_correct"])
        self.assertEqual(res["display_answer"], "030 12 34 56")  # reveal only on submit

    def test_wrong_number_is_incorrect(self):
        res = self._grade("030 99 99 99")
        self.assertFalse(res["is_correct"])
        self.assertEqual(res["typed"], "030 99 99 99")

    def test_alnum_uppercase_folding(self):
        item = dict(READY_ITEM, input_mode="alnum", answer_value="A2C-9K", display_answer="A2C-9K")
        res = self._grade("a2c9k", item=item)
        self.assertTrue(res["is_correct"])

    def test_missing_item_returns_not_found(self):
        with patch("backend.database.get_numdict_bank_item", return_value=None):
            res = ev.grade_numdict_practice_item(user_id=77, numdict_id="gone", typed="1")
        self.assertEqual(res.get("error"), "not_found")

    def test_grade_has_no_leaderboard_or_mastery_side_effects(self):
        with patch("backend.database.get_numdict_bank_item", return_value=READY_ITEM), \
             patch("backend.database.record_challenge_result") as ranking, \
             patch("backend.database.record_numdict_item_answer") as record:
            ev.grade_numdict_practice_item(user_id=77, numdict_id="nd-1", typed="030 12 34 56")
        ranking.assert_not_called()
        record.assert_not_called()


class ServeTests(unittest.TestCase):
    def _serve(self, *, reservation, item):
        stack = ExitStack()
        stack.enter_context(patch("backend.database.reserve_free_feature_usage", return_value=reservation))
        pick = stack.enter_context(patch("backend.database.pick_numdict_practice_item", return_value=item))
        mark = stack.enter_context(patch("backend.database.mark_numdict_practice_seen"))
        stack.enter_context(patch.object(ev, "_numdict_audio_url", return_value="https://r2/nd-1.mp3"))
        with stack:
            data = ev.load_numdict_practice_next(user_id=77)
        return data, pick, mark

    def test_served_item_never_leaks_the_answer(self):
        data, _pick, mark = self._serve(
            reservation={"ok": True, "blocked": False, "used": 1.0, "limit": 20.0},
            item=READY_ITEM,
        )
        self.assertFalse(data["done"])
        self.assertEqual(data["numdict_id"], "nd-1")
        self.assertNotIn("answer_value", data)
        self.assertNotIn("display_answer", data)
        self.assertEqual(data["audio_url"], "https://r2/nd-1.mp3")
        mark.assert_called_once()  # serve marks seen so the next call advances

    def test_capped_free_user_gets_friendly_done_payload(self):
        data, pick, mark = self._serve(
            reservation={
                "ok": False, "blocked": True, "used": 20.0, "limit": 20.0,
                "error": {"feature_title": "Числа на слух (тренажёр)", "limit": 20,
                          "used": 20, "reset_at": "2026-06-29T00:00:00+02:00",
                          "message": "На сегодня хватит."},
            },
            item=READY_ITEM,
        )
        self.assertTrue(data["done"])
        self.assertTrue(data["capped"])
        self.assertTrue(data["upsell"])
        self.assertEqual(data["limit"], 20)
        pick.assert_not_called()   # capped before touching the pool
        mark.assert_not_called()

    def test_empty_pool_returns_done_empty(self):
        data, _pick, mark = self._serve(
            reservation={"ok": True, "blocked": False, "used": 1.0, "limit": 20.0},
            item=None,
        )
        self.assertTrue(data["done"])
        self.assertTrue(data["empty"])
        mark.assert_not_called()

    def test_pro_user_is_unlimited(self):
        data, _pick, _mark = self._serve(
            reservation={"ok": True, "blocked": False, "used": None, "limit": None},
            item=READY_ITEM,
        )
        self.assertFalse(data["done"])
        self.assertEqual(data["numdict_id"], "nd-1")


class EndpointTests(unittest.TestCase):
    def setUp(self):
        self.client = server.app.test_client()

    def test_next_endpoint_passes_through_eval(self):
        with patch.object(server, "_answer_auth_user_id", return_value=(77, "test", None)), \
             patch("backend.answer_eval.load_numdict_practice_next",
                   return_value={"kind": "numdict_practice", "done": False, "numdict_id": "nd-1",
                                 "prompt_de": "x", "prompt_ru": "y", "input_mode": "numeric",
                                 "audio_url": "u", "used": 1.0, "limit": 20.0}):
            resp = self.client.post("/api/answer/numdict/practice/next", json={"initData": "signed"})
        self.assertEqual(resp.status_code, 200)
        body = resp.get_json()
        self.assertTrue(body["ok"])
        self.assertEqual(body["numdict_id"], "nd-1")
        self.assertNotIn("answer_value", body)

    def test_submit_requires_numdict_id(self):
        with patch.object(server, "_answer_auth_user_id", return_value=(77, "test", None)):
            resp = self.client.post("/api/answer/numdict/practice/submit", json={"initData": "signed", "answer": "1"})
        self.assertEqual(resp.status_code, 400)

    def test_submit_returns_verdict(self):
        with patch.object(server, "_answer_auth_user_id", return_value=(77, "test", None)), \
             patch("backend.answer_eval.grade_numdict_practice_item",
                   return_value={"kind": "numdict_practice", "is_correct": True,
                                 "display_answer": "030 12 34 56", "typed": "0301234 56"}):
            resp = self.client.post(
                "/api/answer/numdict/practice/submit",
                json={"initData": "signed", "numdict_id": "nd-1", "answer": "0301234 56"},
            )
        self.assertEqual(resp.status_code, 200)
        body = resp.get_json()
        self.assertTrue(body["ok"])
        self.assertTrue(body["is_correct"])
        self.assertEqual(body["display_answer"], "030 12 34 56")


if __name__ == "__main__":
    unittest.main()
