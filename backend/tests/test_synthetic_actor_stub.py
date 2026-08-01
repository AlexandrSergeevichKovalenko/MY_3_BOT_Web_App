"""Load-runner traffic must not reach paid providers.

SYNTHETIC_LOAD_MODE is a whole-service switch, so it can never be enabled in production —
real learners would get fake answers. The load runner drives the LIVE services, which is
why six runs since April 2026 (1777 fake translations) were graded by the real model at
roughly €0.4–5.7 per run. The gate therefore has to be the acting user, not the service.
"""
import asyncio
import unittest
from unittest.mock import patch

from backend import openai_manager as om
from backend import synthetic_load as sl


class IsSyntheticUserTests(unittest.TestCase):
    def test_load_runner_fleet_is_recognised(self):
        # The LOAD_RUNNER command starts the fleet at --start-id 9937001801.
        for uid in (9937001801, 9937001825, 9937001850, "9937001830"):
            with self.subTest(uid=uid):
                self.assertTrue(sl.is_synthetic_user(uid))

    def test_real_people_are_not(self):
        for uid in (117649764, 7263482531, 8546091375, 572603263, 0, -1, None, "", "abc"):
            with self.subTest(uid=uid):
                self.assertFalse(sl.is_synthetic_user(uid))


class LlmExecuteSyntheticActorTests(unittest.TestCase):
    def setUp(self):
        om.set_llm_billing_user(None)
        self.addCleanup(om.set_llm_billing_user, None)

    def _execute(self):
        return asyncio.run(
            om.llm_execute(
                task_name="check_translation_multilang",
                system_instruction_key="check_translation_multilang",
                user_message="Ich habe einen Brief geschrieben.",
            )
        )

    def test_synthetic_actor_never_calls_openai_and_books_no_usage(self):
        om.set_llm_billing_user(9937001805)
        with patch.object(om, "_run_task_text_via_responses") as responses, \
                patch.object(om, "_run_task_text_via_assistants") as assistants, \
                patch.object(om, "_log_openai_usage_event") as ledger:
            text = self._execute()

        responses.assert_not_called()
        assistants.assert_not_called()
        self.assertIn("synthetic", text)
        # Nothing reaches the ledger either — a load run leaves no phantom cost behind.
        ledger.assert_not_called()

    def _execute_against_stubbed_gateways(self, answer):
        """Run llm_execute with BOTH gateways stubbed (which one is used depends on the
        task's routing) and report whether the real path was taken."""
        async def _real(*args, **kwargs):
            return answer

        with patch.object(om, "_run_task_text_via_responses", side_effect=_real) as responses, \
                patch.object(om, "_run_task_text_via_assistants", side_effect=_real) as assistants:
            text = self._execute()
        return text, responses.call_count + assistants.call_count

    def test_real_actor_still_goes_to_the_model(self):
        om.set_llm_billing_user(117649764)
        text, calls = self._execute_against_stubbed_gateways("echtes Modellergebnis")
        self.assertEqual(calls, 1)
        self.assertEqual(text, "echtes Modellergebnis")

    def test_no_actor_set_still_goes_to_the_model(self):
        # Nightly pool jobs run without a billing user — they must keep working for real.
        text, calls = self._execute_against_stubbed_gateways("pool result")
        self.assertEqual(calls, 1)
        self.assertEqual(text, "pool result")


if __name__ == "__main__":
    unittest.main()
