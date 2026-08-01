"""Attribution guard: SYSTEM (shared pool/content) OpenAI tasks must ALWAYS book
user_id=NULL, even when a billing user is set in context (a user's inline lookup or an
admin running a pool-build command). Personal/dual tasks keep the contextvar's user.

This is the source-of-truth replacement for the old cost-cap exclusion list — see
openai_manager._SYSTEM_ATTRIBUTION_TASKS and _resolve_billing_user_for_task."""
import unittest
from unittest.mock import patch

from backend import openai_manager as om


class ResolveBillingUserForTaskTests(unittest.TestCase):
    def test_system_pool_tasks_forced_to_null(self):
        for task in (
            "enrich_word", "enrich_word_multilang",
            "dictionary_enrichment_multilang",
            "sprint_distractor_setter", "sprint_distractor_prosecutor",
            "sprint_distractor_judge", "sprint_correct_examples",
            "article_noun_gen", "aufgabe_pin_image", "battle_image",
            "translate_subtitles_multilang", "pool_world_news",
        ):
            with self.subTest(task=task):
                # Even with a real user set (e.g. admin triggered the build inline),
                # a shared task must never land on that user.
                self.assertIsNone(om._resolve_billing_user_for_task(task, 117649764))

    def test_aufgabe_pool_items_and_shared_chunking_are_system(self):
        # Verified 2026-08-01 at the call sites: the "error"/"hoerluecke" items and their
        # verifier are produced inside the aufgabe_pool generator (bot_3.py, log tag
        # "aufgabe_pool:"), and tts_chunk_de is cached in bt_3_tts_chunk_cache under a hash
        # of the sentence, so one split serves every learner. Triggering a pool build from
        # an admin command must not book the run on that admin.
        for task in ("aufgabe_error", "verify_aufgabe_error", "aufgabe_hoerluecke", "tts_chunk_de"):
            with self.subTest(task=task):
                self.assertIsNone(om._resolve_billing_user_for_task(task, 117649764))

    def test_personal_and_dual_tasks_keep_the_user(self):
        for task in (
            "check_translation_multilang", "recheck_translation",
            "theory_generation", "generate_sentences_a2",
            "generate_mystery_story", "dictionary_assistant_multilang",
            "feel_word", "shortcut_split", "dictionary_lookup",
        ):
            with self.subTest(task=task):
                self.assertEqual(om._resolve_billing_user_for_task(task, 777), 777)

    def test_none_user_stays_none(self):
        # Background/prewarm work (no context) stays NULL regardless of task class.
        self.assertIsNone(om._resolve_billing_user_for_task("generate_sentences_a2", None))
        self.assertIsNone(om._resolve_billing_user_for_task("enrich_word", None))

    def test_unknown_task_defaults_to_personal(self):
        # A task not in the system set defaults to the contextvar user (personal).
        self.assertEqual(om._resolve_billing_user_for_task("some_new_task", 42), 42)


class StoreLastUsageGuardTests(unittest.TestCase):
    """End-to-end at the chokepoint: with a billing user set in context, a SYSTEM task
    still logs user_id=None; a PERSONAL task logs the real user."""

    def _run(self, task_name, ctx_user):
        om.set_llm_billing_user(ctx_user)
        try:
            fake_status = {"usage": {"prompt_tokens": 10, "completion_tokens": 5,
                                     "total_tokens": 15}, "model": "gpt-4.1-mini"}
            with patch.object(om, "_log_openai_usage_event") as logged:
                om._store_last_usage(fake_status, task_name=task_name)
            self.assertTrue(logged.called)
            # signature: _log_openai_usage_event(task_name, usage, user_id)
            return logged.call_args.args[2]
        finally:
            om.set_llm_billing_user(None)

    def test_system_task_logs_null_even_with_user_context(self):
        self.assertIsNone(self._run("enrich_word_multilang", 117649764))

    def test_personal_task_logs_real_user(self):
        self.assertEqual(self._run("check_translation_multilang", 555), 555)


if __name__ == "__main__":
    unittest.main()
