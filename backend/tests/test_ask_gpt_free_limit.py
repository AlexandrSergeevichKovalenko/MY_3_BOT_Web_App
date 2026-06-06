import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import backend.backend_server as server
import bot_3


class AskGptFreeLimitWebAppTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = server.app.test_client()

    def _auth_patches(self):
        return (
            patch.object(server, "_telegram_hash_is_valid", return_value=True),
            patch.object(server, "_parse_telegram_init_data", return_value={"user": {"id": 77, "username": "Iryna"}}),
            patch.object(server, "_get_user_language_pair", return_value=("ru", "de", {})),
            patch.object(server, "_resolve_webapp_user_allowed", return_value=(True, "test")),
        )

    def _payload(self):
        return {
            "initData": "valid",
            "original_text": "Я хочу пить.",
            "user_translation": "Ich will trinken.",
            "explanation": "Better: Ich habe Durst.",
            "learner_question": "Почему так естественнее?",
            "request_id": "req-ask-1",
        }

    def test_webapp_explain_question_consumes_usage_before_openai(self):
        with self._auth_patches()[0], self._auth_patches()[1], self._auth_patches()[2], self._auth_patches()[3], \
             patch.object(server, "reserve_free_feature_usage", return_value={"ok": True, "blocked": False, "event": {"id": 1}}) as reserve_mock, \
             patch.object(server, "run_language_learning_private_question_detailed", AsyncMock(return_value={"answer": "Потому что это идиоматично."})) as run_mock, \
             patch.object(server, "get_last_llm_usage", return_value={}), \
             patch.object(server, "_billing_log_event_safe"), \
             patch.object(server, "_billing_log_openai_usage"):
            response = self.client.post("/api/webapp/explain/question", json=self._payload())

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["answer"], "Потому что это идиоматично.")
        reserve_mock.assert_called_once()
        self.assertEqual(reserve_mock.call_args.kwargs["feature_key"], "ask_gpt_daily")
        run_mock.assert_called_once()

    def test_webapp_explain_question_blocked_before_openai(self):
        blocked_error = {
            "ok": False,
            "error": "free_limit_exceeded",
            "feature": "ask_gpt_daily",
            "feature_title": "Спросить GPT",
            "limit": 5,
            "used": 5,
            "reset_at": "2026-06-07T00:00:00+02:00",
        }
        with self._auth_patches()[0], self._auth_patches()[1], self._auth_patches()[2], self._auth_patches()[3], \
             patch.object(server, "reserve_free_feature_usage", return_value={"ok": False, "blocked": True, "error": blocked_error}) as reserve_mock, \
             patch.object(server, "run_language_learning_private_question_detailed", AsyncMock()) as run_mock:
            response = self.client.post("/api/webapp/explain/question", json=self._payload())

        self.assertEqual(response.status_code, 429)
        self.assertEqual(response.get_json()["feature"], "ask_gpt_daily")
        reserve_mock.assert_called_once()
        run_mock.assert_not_called()


class _FakeMessage:
    def __init__(self):
        self.replies = []
        self.chat = SimpleNamespace(type="private")

    async def reply_text(self, text, **kwargs):
        self.replies.append((text, kwargs))


class _FakeQuery:
    def __init__(self):
        self.from_user = SimpleNamespace(id=77)
        self.message = _FakeMessage()
        self.data = "langgpt:detail"
        self.answered = False

    async def answer(self, *args, **kwargs):
        self.answered = True


class AskGptFreeLimitTelegramTests(unittest.IsolatedAsyncioTestCase):
    def test_telegram_ask_gpt_reserve_uses_feature_key(self):
        with patch.object(bot_3, "reserve_free_feature_usage", return_value={"ok": True, "blocked": False}) as reserve_mock:
            result = bot_3._reserve_telegram_ask_gpt_daily(
                user_id=77,
                source_lang="ru",
                target_lang="de",
                origin="telegram_language_tutor",
                request_key="q1",
                question_len=12,
            )

        self.assertFalse(result["blocked"])
        reserve_mock.assert_called_once()
        self.assertEqual(reserve_mock.call_args.kwargs["feature_key"], "ask_gpt_daily")
        self.assertEqual(reserve_mock.call_args.kwargs["metadata"]["origin"], "telegram_language_tutor")

    async def test_telegram_detail_blocked_before_openai(self):
        query = _FakeQuery()
        update = SimpleNamespace(callback_query=query)
        context = SimpleNamespace(user_data={"language_tutor_last_exchange": {"question": "Warum?", "answer": "Weil."}})
        with patch.object(bot_3, "is_telegram_user_allowed", return_value=True), \
             patch.object(bot_3, "_language_tutor_pair_for_user", return_value=("ru", "de")), \
             patch.object(bot_3, "_reserve_telegram_ask_gpt_daily", return_value={"ok": False, "blocked": True}), \
             patch.object(bot_3, "run_language_learning_private_question_detailed", AsyncMock()) as run_mock:
            await bot_3.handle_language_tutor_detail_callback(update, context)

        run_mock.assert_not_called()
        self.assertTrue(any(bot_3.ASK_GPT_DAILY_LIMIT_MESSAGE in item[0] for item in query.message.replies))


if __name__ == "__main__":
    unittest.main()
