import json
import os
import unittest
from unittest.mock import ANY, patch

import backend.backend_server as server


class _FakeShortcutSplitMessage:
    def __init__(self, content: str):
        self.content = content


class _FakeShortcutSplitChoice:
    def __init__(self, content: str):
        self.message = _FakeShortcutSplitMessage(content)


class _FakeShortcutSplitResponse:
    def __init__(self, content: str):
        self.choices = [_FakeShortcutSplitChoice(content)]


class _FakeShortcutSplitCompletions:
    def __init__(self, outcomes):
        self.outcomes = list(outcomes)
        self.models: list[str] = []

    async def create(self, **kwargs):
        self.models.append(str(kwargs.get("model") or ""))
        if not self.outcomes:
            raise RuntimeError("no fake outcome configured")
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return _FakeShortcutSplitResponse(str(outcome))


class _FakeShortcutSplitChat:
    def __init__(self, completions):
        self.completions = completions


class _FakeShortcutSplitClient:
    def __init__(self, outcomes):
        self.completions = _FakeShortcutSplitCompletions(outcomes)
        self.chat = _FakeShortcutSplitChat(self.completions)


class _FakeRedis:
    def __init__(self):
        self.values: dict[str, int] = {}
        self.ttls: dict[str, int] = {}

    def eval(self, script, numkeys, key, window_seconds):
        current = int(self.values.get(key, 0)) + 1
        self.values[key] = current
        if current == 1:
            self.ttls[key] = int(window_seconds)
        return [current, self.ttls.get(key, int(window_seconds))]

    def get(self, key):
        return self.values.get(key)

    def ttl(self, key):
        if key not in self.values:
            return -2
        return self.ttls.get(key, 600)

    def incr(self, key):
        current = int(self.values.get(key, 0)) + 1
        self.values[key] = current
        return current

    def expire(self, key, window_seconds):
        self.ttls[key] = int(window_seconds)
        return True

    def delete(self, key):
        self.values.pop(key, None)
        self.ttls.pop(key, None)
        return 1


class ShortcutLookupSplitTests(unittest.TestCase):
    def setUp(self):
        self.client = server.app.test_client()
        self.redis = _FakeRedis()

    def test_normalize_unit_text_cleans_pedagogical_grammar_noise(self):
        self.assertEqual(
            server._shortcut_normalize_unit_text("  erinnert... an  +Akkusativ "),
            "erinnert an + Akkusativ",
        )
        self.assertEqual(
            server._shortcut_normalize_unit_text("ist… ähnlich +  Dativ"),
            "ist ähnlich + Dativ",
        )

    def test_extract_blocks_normalizes_wrapper_quotes(self):
        raw = json.dumps(
            {
                "blocks": [
                    {"term": '"Hat mich gefreut!"', "content": '"Hat mich gefreut!"'},
                    {"term": "1. Man sieht sich!", "content": "1. Man sieht sich!"},
                ]
            },
            ensure_ascii=False,
        )

        blocks = server._shortcut_extract_blocks_from_json(raw, ' "Hat mich gefreut!" 1. Man sieht sich! ')

        self.assertEqual(
            blocks,
            [
                ("Hat mich gefreut!", "Hat mich gefreut!"),
                ("Man sieht sich!", "Man sieht sich!"),
            ],
        )

    def test_shortcut_split_uses_mini_model_first(self):
        raw = json.dumps({"blocks": [{"term": "Haus", "content": "Haus"}]}, ensure_ascii=False)
        fake_client = _FakeShortcutSplitClient([raw])

        with patch("backend.openai_manager.client", fake_client), \
             patch.object(server, "_log_flow_observation") as log_mock, \
             patch.dict(os.environ, {
                 "LLM_TASK_MODEL_SHORTCUT_SPLIT": "",
                 "OPENAI_TASK_MODEL_SHORTCUT_SPLIT": "",
                 "LLM_TASK_MODEL_SHORTCUT_SPLIT_FALLBACK": "",
                 "OPENAI_TASK_MODEL_SHORTCUT_SPLIT_FALLBACK": "",
             }, clear=False):
            blocks = server._shortcut_split_blocks("Haus", origin="shortcut", user_id=123, request_key="rk1")

        self.assertEqual(blocks, [("Haus", "Haus")])
        self.assertEqual(fake_client.completions.models, ["gpt-4.1-mini"])
        log_mock.assert_any_call(
            "shortcut_split",
            "split_completed",
            origin="shortcut",
            user_id=123,
            request_id=None,
            request_key="rk1",
            model="gpt-4.1-mini",
            attempt_role="primary",
            final_status="success",
            parse_succeeded=True,
            blocks_count=1,
            input_length=4,
            output_length=ANY,
            fallback_reason=None,
            duration_ms=ANY,
        )

    def test_shortcut_split_fallback_uses_full_gpt41_after_invalid_primary(self):
        invalid = json.dumps({"items": []}, ensure_ascii=False)
        valid = json.dumps({"blocks": [{"term": "laufen", "content": "laufen"}]}, ensure_ascii=False)
        fake_client = _FakeShortcutSplitClient([invalid, valid])

        with patch("backend.openai_manager.client", fake_client), \
             patch.object(server, "_log_flow_observation") as log_mock, \
             patch.dict(os.environ, {
                 "LLM_TASK_MODEL_SHORTCUT_SPLIT": "",
                 "OPENAI_TASK_MODEL_SHORTCUT_SPLIT": "",
                 "LLM_TASK_MODEL_SHORTCUT_SPLIT_FALLBACK": "",
                 "OPENAI_TASK_MODEL_SHORTCUT_SPLIT_FALLBACK": "",
             }, clear=False):
            blocks = server._shortcut_split_blocks("laufen", origin="forwarded", user_id=456, request_key="rk2")

        self.assertEqual(blocks, [("laufen", "laufen")])
        self.assertEqual(fake_client.completions.models, ["gpt-4.1-mini", "gpt-4.1-2025-04-14"])
        log_mock.assert_any_call(
            "shortcut_split",
            "split_completed",
            origin="forwarded",
            user_id=456,
            request_id=None,
            request_key="rk2",
            model="gpt-4.1-2025-04-14",
            attempt_role="fallback",
            final_status="success",
            parse_succeeded=True,
            blocks_count=1,
            input_length=6,
            output_length=ANY,
            fallback_reason="primary_invalid_json_or_parse",
            duration_ms=ANY,
        )

    def test_shortcut_split_final_mechanical_fallback_still_works(self):
        fake_client = _FakeShortcutSplitClient([RuntimeError("mini down"), RuntimeError("full down")])

        with patch("backend.openai_manager.client", fake_client), \
             patch.object(server, "_log_flow_observation") as log_mock, \
             patch.dict(os.environ, {
                 "LLM_TASK_MODEL_SHORTCUT_SPLIT": "",
                 "OPENAI_TASK_MODEL_SHORTCUT_SPLIT": "",
                 "LLM_TASK_MODEL_SHORTCUT_SPLIT_FALLBACK": "",
                 "OPENAI_TASK_MODEL_SHORTCUT_SPLIT_FALLBACK": "",
             }, clear=False):
            blocks = server._shortcut_split_blocks("Haus\nlaufen", origin="shortcut", user_id=789, request_key="rk3")

        self.assertEqual(blocks, [("Haus", "Haus"), ("laufen", "laufen")])
        self.assertEqual(fake_client.completions.models, ["gpt-4.1-mini", "gpt-4.1-2025-04-14"])
        log_mock.assert_any_call(
            "shortcut_split",
            "split_completed",
            origin="shortcut",
            user_id=789,
            request_id=None,
            request_key="rk3",
            model=None,
            attempt_role="mechanical",
            final_status="success",
            parse_succeeded=False,
            blocks_count=2,
            input_length=11,
            output_length=None,
            fallback_reason="fallback_exception:RuntimeError",
            duration_ms=0,
        )

    def test_validate_coverage_allows_short_clean_units_from_long_noisy_text(self):
        original = (
            "Прощание. Финальные фразы — чтобы уйти красиво. "
            'Ich muss dann mal wieder. (Мне уже пора.) '
            "Klassischer, höflicher Abschluss eines Gesprächs. "
            "Hat mich gefreut! (Рад был пообщаться!)"
        )

        self.assertTrue(
            server._shortcut_validate_coverage(
                [
                    ("Ich muss dann mal wieder.", "Ich muss dann mal wieder."),
                    ("Hat mich gefreut!", "Hat mich gefreut!"),
                ],
                original,
            )
        )

    def test_shortcut_onboarding_text_mentions_action_button_and_back_tap(self):
        text = server._build_shortcut_onboarding_text(pairing_code="A7F4K2")

        self.assertIn("Action Button", text)
        self.assertIn("Back Tap", text)
        self.assertIn("A7F4K2", text)

    def test_shortcut_install_endpoint_redirects_to_configured_link(self):
        with patch.dict(os.environ, {"SHORTCUT_INSTALL_URL": "https://www.icloud.com/shortcuts/test-id"}, clear=False):
            response = self.client.get("/api/shortcut/install")

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.headers.get("Location"), "https://www.icloud.com/shortcuts/test-id")

    def test_shortcut_install_endpoint_requires_configured_link(self):
        with patch.dict(os.environ, {
            "SHORTCUT_INSTALL_URL": "",
            "SHORTCUT_ICLOUD_URL": "",
            "IOS_SHORTCUT_INSTALL_URL": "",
        }, clear=False):
            response = self.client.get("/api/shortcut/install")

        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.get_json()["error"], "Shortcut install link is not configured")

    def test_shortcut_pairing_code_endpoint_returns_code_for_allowed_user(self):
        with patch.dict(os.environ, {"SHORTCUT_BOT_SECRET": "adminsecret"}, clear=False), \
             patch.object(server, "get_redis_client", return_value=self.redis), \
             patch.object(server, "is_telegram_user_allowed", return_value=True), \
             patch.object(
                 server,
                 "create_shortcut_pairing_code",
                 return_value={
                     "pairing_code_id": 7,
                     "user_id": 117649764,
                     "pairing_code": "A7F4K2",
                     "created_at": None,
                     "expires_at": None,
                     "expires_in": 600,
                 },
             ):
            response = self.client.post(
                "/api/shortcut/pairing-code",
                json={"user_id": 117649764},
                headers={"Authorization": "Bearer adminsecret"},
            )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["pairing_code"], "A7F4K2")
        self.assertEqual(payload["user_id"], 117649764)

    def test_shortcut_link_endpoint_returns_install_token(self):
        with patch.object(
            server,
            "link_shortcut_installation",
            return_value={
                "status": "linked",
                "pairing_code_id": 7,
                "installation_id": 11,
                "user_id": 117649764,
                "install_token": "install-token-value",
                "created_at": None,
                "expires_at": None,
            },
        ), patch.object(server, "get_redis_client", return_value=self.redis):
            response = self.client.post(
                "/api/shortcut/link",
                json={"pairing_code": "A7F4K2"},
            )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["installation_id"], 11)
        self.assertEqual(payload["install_token"], "install-token-value")

    def test_shortcut_lookup_uses_install_token_only(self):
        with patch.object(server, "resolve_shortcut_install_token", return_value={"installation_id": 11, "user_id": 117649764}), \
             patch.object(server, "is_telegram_user_allowed", return_value=True), \
             patch.object(server, "_shortcut_dedup_reserve", return_value=False), \
             patch.object(server, "can_enqueue_background_jobs", return_value=True), \
             patch.object(server, "_start_shortcut_lookup_enqueue_runner", return_value="job-123") as enqueue_mock:
            response = self.client.post(
                "/api/shortcut/lookup",
                json={"text": "noisy input", "install_token": "install-token-value"},
            )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["accepted"])
        self.assertEqual(payload["job_id"], "job-123")
        enqueue_mock.assert_called_once_with(user_id=117649764, text="noisy input", origin="shortcut", request_id=ANY)


if __name__ == "__main__":
    unittest.main()
