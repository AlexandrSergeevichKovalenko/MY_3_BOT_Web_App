"""Grading a learner's own translation must be billed to that learner.

Measured on the live ledger 2026-08-01: over 30 days `check_translation_multilang` (4581
events) and `recheck_translation` (4181) carried user_id=NULL 100% of the time. Cause: the
grading runs in the TRANSLATION_CHECK_WORKER, whose dramatiq job takes only a session_id,
so nothing ever set the billing contextvar and the cost reached nobody's daily budget.
"""
import unittest
from unittest.mock import patch

from backend import backend_server as bs
from backend import openai_manager as om


SESSION = {
    "user_id": 117649764,
    "username": "Aleksandr",
    "source_lang": "ru",
    "target_lang": "de",
    "source_session_id": 42,
    "send_private_grammar_text": False,
}
ITEM = {
    "id": 1,
    "item_order": 1,
    "sentence_number": 1,
    "original_text": "Я написал письмо.",
    "user_translation": "Ich habe einen Brief geschrieben.",
    "sentence_id_for_mistake_table": 7,
    "source_daily_sentence_id": 7,
}


class TranslationCheckBillingUserTests(unittest.TestCase):
    def setUp(self):
        om.set_llm_billing_user(None)
        self.addCleanup(om.set_llm_billing_user, None)

    def _run_item(self, grader):
        with patch.object(bs, "check_user_translation_webapp_item", side_effect=grader):
            return bs._process_translation_check_session_item(
                session_id=1, session=dict(SESSION), item=dict(ITEM)
            )

    def test_grading_runs_with_the_session_owner_as_billing_user(self):
        seen = {}

        async def grader(*args, **kwargs):
            seen["billing_user"] = om._LLM_BILLING_USER_ID.get()
            return {"sentence_number": 1, "feedback": "ok", "translation_id": 5}, None

        result = self._run_item(grader)

        self.assertEqual(seen["billing_user"], SESSION["user_id"])
        self.assertEqual(result["item_status"], "done")
        # And the worker thread is left clean for whatever runs next on it.
        self.assertIsNone(om._LLM_BILLING_USER_ID.get())

    def test_billing_user_is_cleared_even_when_grading_blows_up(self):
        async def grader(*args, **kwargs):
            raise RuntimeError("gateway down")

        result = self._run_item(grader)

        self.assertEqual(result["item_status"], "failed")
        self.assertIsNone(om._LLM_BILLING_USER_ID.get())

    def test_checkpointed_item_does_not_leak_a_billing_user(self):
        # Already-graded item: no LLM call at all, and nothing left in the contextvar.
        item = dict(ITEM, result_json={"sentence_number": 1, "feedback": "ok"})

        async def grader(*args, **kwargs):  # pragma: no cover - must not be called
            raise AssertionError("checkpointed item must not be re-graded")

        with patch.object(bs, "check_user_translation_webapp_item", side_effect=grader):
            bs._process_translation_check_session_item(
                session_id=1, session=dict(SESSION), item=item
            )
        self.assertIsNone(om._LLM_BILLING_USER_ID.get())


if __name__ == "__main__":
    unittest.main()
