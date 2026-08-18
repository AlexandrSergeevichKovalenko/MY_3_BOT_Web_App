"""Разбор ошибок, за который уже заплатили, должен переживать закрытие модалки.

До 18.08.2026 разбор жил только в памяти вкладки: закрыл — и вернуться к нему было
нельзя ничем, а после перезагрузки мини-аппа повторный взгляд стоил нового запроса к
модели и единицы дневного лимита. Теперь он ложится в базу на ТЕКУЩИЙ ДЕНЬ и отдаётся
бесплатно. Здесь проверяется вся дверь: запись, чтение, флаги для списка и то, что
чтение не зовёт модель.
"""

import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import backend.backend_server as server


class SavedTranslationExplanationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = server.app.test_client()

    def _auth(self):
        return (
            patch.object(server, "_telegram_hash_is_valid", return_value=True),
            patch.object(server, "_parse_telegram_init_data", return_value={"user": {"id": 77, "username": "Iryna"}}),
            patch.object(server, "_get_user_language_pair", return_value=("ru", "de", {})),
            patch.object(server, "_resolve_webapp_user_allowed", return_value=(True, "test")),
        )

    def _explain_payload(self, **extra):
        payload = {
            "initData": "valid",
            "original_text": "Я хочу пить.",
            "user_translation": "Ich will trinken.",
            "explanation_language": "ru",
        }
        payload.update(extra)
        return payload

    def test_explain_saves_breakdown_under_translation_id(self):
        structured = {"summary": "Кратко", "errors": [], "alternatives": [], "synonyms": []}
        auth = self._auth()
        with auth[0], auth[1], auth[2], auth[3], \
             patch.object(server, "reserve_free_feature_usage", return_value={"ok": True, "blocked": False}), \
             patch.object(server, "run_translation_explanation_structured", AsyncMock(return_value=structured)), \
             patch.object(server, "get_last_llm_usage", return_value={}), \
             patch.object(server, "_billing_log_event_safe"), \
             patch.object(server, "_billing_log_openai_usage"), \
             patch.object(server, "purge_stale_translation_explanations", return_value=0) as purge_mock, \
             patch.object(server, "save_translation_explanation") as save_mock:
            response = self.client.post("/api/webapp/explain", json=self._explain_payload(translation_id=4242))

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.get_json()["saved_for_replay"])
        purge_mock.assert_called_once()
        save_mock.assert_called_once()
        kwargs = save_mock.call_args.kwargs
        self.assertEqual(kwargs["translation_id"], 4242)
        self.assertEqual(kwargs["explanation_lang"], "ru")
        self.assertEqual(kwargs["errors_json"], structured)
        # Грамматика приходит отдельным запросом — эта половина строки не трогается.
        self.assertIsNone(kwargs["grammar_json"])

    def test_grammar_call_saves_only_the_grammar_half(self):
        grammar = {"grammar": [{"part": "Ich will trinken", "structure": "Modalverb"}]}
        auth = self._auth()
        with auth[0], auth[1], auth[2], auth[3], \
             patch.object(server, "reserve_free_feature_usage", return_value={"ok": True, "blocked": False}), \
             patch.object(server, "run_correct_sentence_grammar_structured", AsyncMock(return_value=grammar)), \
             patch.object(server, "get_last_llm_usage", return_value={}), \
             patch.object(server, "_billing_log_event_safe"), \
             patch.object(server, "_billing_log_openai_usage"), \
             patch.object(server, "purge_stale_translation_explanations", return_value=0), \
             patch.object(server, "save_translation_explanation") as save_mock:
            response = self.client.post(
                "/api/webapp/explain",
                json=self._explain_payload(translation_id=4242, mode="grammar"),
            )

        self.assertEqual(response.status_code, 200)
        kwargs = save_mock.call_args.kwargs
        self.assertEqual(kwargs["grammar_json"], grammar)
        self.assertIsNone(kwargs["errors_json"])

    def test_explain_still_answers_when_saving_fails(self):
        """Побочная запись не имеет права утащить за собой уже оплаченный ответ."""
        structured = {"summary": "Кратко", "errors": []}
        auth = self._auth()
        with auth[0], auth[1], auth[2], auth[3], \
             patch.object(server, "reserve_free_feature_usage", return_value={"ok": True, "blocked": False}), \
             patch.object(server, "run_translation_explanation_structured", AsyncMock(return_value=structured)), \
             patch.object(server, "get_last_llm_usage", return_value={}), \
             patch.object(server, "_billing_log_event_safe"), \
             patch.object(server, "_billing_log_openai_usage"), \
             patch.object(server, "purge_stale_translation_explanations", return_value=0), \
             patch.object(server, "save_translation_explanation", side_effect=RuntimeError("db down")):
            response = self.client.post("/api/webapp/explain", json=self._explain_payload(translation_id=4242))

        self.assertEqual(response.status_code, 200)
        body = response.get_json()
        self.assertEqual(body["explanation_json"], structured)
        # Провал записи виден наружу честным признаком, а не тишиной.
        self.assertFalse(body["saved_for_replay"])

    def test_explain_without_translation_id_does_not_save(self):
        structured = {"summary": "Кратко", "errors": []}
        auth = self._auth()
        with auth[0], auth[1], auth[2], auth[3], \
             patch.object(server, "reserve_free_feature_usage", return_value={"ok": True, "blocked": False}), \
             patch.object(server, "run_translation_explanation_structured", AsyncMock(return_value=structured)), \
             patch.object(server, "get_last_llm_usage", return_value={}), \
             patch.object(server, "_billing_log_event_safe"), \
             patch.object(server, "_billing_log_openai_usage"), \
             patch.object(server, "save_translation_explanation") as save_mock:
            response = self.client.post("/api/webapp/explain", json=self._explain_payload())

        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.get_json()["saved_for_replay"])
        save_mock.assert_not_called()

    def test_followup_answer_is_appended_to_the_saved_breakdown(self):
        auth = self._auth()
        with auth[0], auth[1], auth[2], auth[3], \
             patch.object(server, "reserve_free_feature_usage", return_value={"ok": True, "blocked": False}), \
             patch.object(server, "run_language_learning_private_question_detailed",
                          AsyncMock(return_value={"answer": "Потому что так говорят."})), \
             patch.object(server, "get_last_llm_usage", return_value={}), \
             patch.object(server, "_billing_log_event_safe"), \
             patch.object(server, "_billing_log_openai_usage"), \
             patch.object(server, "append_translation_explanation_followup", return_value=True) as append_mock:
            response = self.client.post(
                "/api/webapp/explain/question",
                json={
                    "initData": "valid",
                    "original_text": "Я хочу пить.",
                    "user_translation": "Ich will trinken.",
                    "explanation": "Разбор",
                    "learner_question": "Почему так?",
                    "translation_id": 4242,
                    "explanation_language": "ru",
                },
            )

        self.assertEqual(response.status_code, 200)
        append_mock.assert_called_once()
        kwargs = append_mock.call_args.kwargs
        self.assertEqual(kwargs["translation_id"], 4242)
        self.assertEqual(kwargs["question"], "Почему так?")
        self.assertEqual(kwargs["answer"], "Потому что так говорят.")

    def test_saved_endpoint_returns_the_breakdown_without_calling_the_model(self):
        saved = {
            "translation_id": 4242,
            "explanation_language": "ru",
            "explanation_json": {"summary": "Кратко"},
            "grammar": {"grammar": []},
            "followups": [{"question": "Почему?", "answer": "Потому."}],
            "updated_at": "2026-08-18T10:00:00+02:00",
        }
        auth = self._auth()
        with auth[0], auth[1], auth[2], auth[3], \
             patch.object(server, "get_saved_translation_explanation", return_value=saved) as get_mock, \
             patch.object(server, "run_translation_explanation_structured", AsyncMock()) as model_mock:
            response = self.client.post(
                "/api/webapp/explain/saved",
                json={"initData": "valid", "translation_id": 4242, "explanation_language": "ru"},
            )

        self.assertEqual(response.status_code, 200)
        body = response.get_json()
        self.assertTrue(body["found"])
        self.assertEqual(body["explanation_json"], {"summary": "Кратко"})
        self.assertEqual(body["followups"], [{"question": "Почему?", "answer": "Потому."}])
        get_mock.assert_called_once()
        model_mock.assert_not_called()

    def test_saved_endpoint_says_found_false_instead_of_an_empty_breakdown(self):
        auth = self._auth()
        with auth[0], auth[1], auth[2], auth[3], \
             patch.object(server, "get_saved_translation_explanation", return_value=None):
            response = self.client.post(
                "/api/webapp/explain/saved",
                json={"initData": "valid", "translation_id": 4242},
            )

        self.assertEqual(response.status_code, 200)
        body = response.get_json()
        self.assertFalse(body["found"])
        self.assertNotIn("explanation_json", body)

    def test_flags_endpoint_reports_which_sentences_have_a_saved_breakdown(self):
        auth = self._auth()
        with auth[0], auth[1], auth[2], auth[3], \
             patch.object(server, "get_saved_translation_explanation_langs",
                          return_value={4242: ["ru"], 4243: ["de", "ru"]}) as flags_mock:
            response = self.client.post(
                "/api/webapp/explain/saved/flags",
                json={"initData": "valid", "translation_ids": [4242, 4243, 0, "нет"]},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["items"], {"4242": ["ru"], "4243": ["de", "ru"]})
        # Мусор в списке отсеивается до запроса в базу, а не превращается в id 0.
        self.assertEqual(flags_mock.call_args.kwargs["translation_ids"], [4242, 4243])

    def test_saved_endpoint_is_not_behind_the_daily_cap(self):
        """Повторный взгляд на свой же разбор не может стоить дневной единицы."""
        self.assertNotIn("/api/webapp/explain/saved", server._BILLING_GUARD_RULES)
        self.assertNotIn("/api/webapp/explain/saved/flags", server._BILLING_GUARD_RULES)


class SavedExplanationLanguageKeyTests(unittest.TestCase):
    def test_unsupported_language_is_rejected_loudly(self):
        from backend.translation_workflow import _explanation_lang_key

        self.assertEqual(_explanation_lang_key("RU"), "ru")
        with self.assertRaises(ValueError):
            _explanation_lang_key("")
        with self.assertRaises(ValueError):
            _explanation_lang_key("kz")


if __name__ == "__main__":
    unittest.main()
