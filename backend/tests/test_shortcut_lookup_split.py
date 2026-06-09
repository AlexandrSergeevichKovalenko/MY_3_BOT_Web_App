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

    def test_is_learnable_unit_drops_garbage(self):
        # Real garbage observed in production Shortcut output (numbers, symbols,
        # ASCII-art, dictionary annotations, handles, foreign-script-only).
        for raw in [
            "284", "19", "161", "2,291", "58", "(58", "•0  20", "/\\) )",
            "A) )", "...", "i:", "m–(e)s", "+ <-,-en>",
            "deutsch.laman o", "deutsch_erfolgreich", "田", "•",
        ]:
            cleaned = server._shortcut_normalize_unit_text(raw)
            self.assertFalse(
                bool(cleaned) and server._shortcut_is_learnable_unit(cleaned, cleaned),
                msg=f"expected DROP for {raw!r} (normalized {cleaned!r})",
            )

    def test_is_learnable_unit_keeps_german_units(self):
        # Genuine German words / phrases / sentences and number+word phrases must survive.
        for raw in [
            "284 Kilometer", "20 Euro", "Hallo meine Lieben", "in Darmstadt",
            "auf dem Feld arbeiten", "die Absage", "erinnern an + Akkusativ",
            "Er ist sich todsicher, dass er recht hat.", "Ei",
        ]:
            cleaned = server._shortcut_normalize_unit_text(raw)
            self.assertTrue(
                server._shortcut_is_learnable_unit(cleaned, cleaned),
                msg=f"expected KEEP for {raw!r} (normalized {cleaned!r})",
            )

    def test_is_learnable_unit_drops_screenshot_chrome(self):
        # Real leakage observed in production: Instagram/Telegram app navigation,
        # bare function words, Russian captions, hashtags and OCR scraps.
        for raw in [
            # app UI chrome (German + English locale)
            "Für dich", "Erkunden", "Gefolgt", "Home", "LIVE", "mehr",
            "Übersetzung anzeigen", "Explore", "Following", "For you",
            # bare standalone function words
            "auf", "an", "für", "und", "der", "sich",
            # Russian-dominant caption / hashtags
            "Пять фраз, которые лучше записать. #немецкийязык #немецкий mehr",
            "#немецкийязык #немецкий", "@deutsch_user",
            # OCR scraps
            "ch", "Il 66",
        ]:
            cleaned = server._shortcut_normalize_unit_text(raw)
            self.assertFalse(
                bool(cleaned) and server._shortcut_is_learnable_unit(cleaned, cleaned),
                msg=f"expected DROP for {raw!r} (normalized {cleaned!r})",
            )

    def test_is_learnable_unit_keeps_german_despite_new_rules(self):
        # The new chrome/function-word/cyrillic rules must NOT touch real German content,
        # including 2-letter nouns, phrases that merely contain a UI word, and DE+RU mixes
        # where German dominates.
        for raw in [
            "Ei", "Öl", "noch mehr Zeit", "sich verlassen auf + Akkusativ",
            "Du bist wirklich ein Meister der Mittelmäßigkeit",
            "ich ertrage das nicht", "Strafmaß", "Rückfallrisiko", "blamierst",
            "der Erfolg ist (успех) garantiert",
        ]:
            cleaned = server._shortcut_normalize_unit_text(raw)
            self.assertTrue(
                server._shortcut_is_learnable_unit(cleaned, cleaned),
                msg=f"expected KEEP for {raw!r} (normalized {cleaned!r})",
            )

    def test_rubezh1_drops_non_german_code_url_math_english(self):
        # Real garbage from forwarded non-German screenshots (coding course, emails, IG).
        for raw in [
            "No amount of evidence will ever persuade an idiot.",
            "from keras. models import Sequential",
            "(batch x 10000) x (10000 x 16) =",
            "GoIT | Simple way to IT | goit.com.ua",
            "https://payment.goit.ua/AddAgreements",
            "3, (batch x 16)x 16x1) → = (bAtch x 1)",
            "edu.goit.global",
        ]:
            cleaned = server._shortcut_normalize_unit_text(raw)
            self.assertFalse(
                bool(cleaned) and server._shortcut_is_learnable_unit(cleaned, cleaned),
                msg=f"expected DROP (non-German) for {raw!r}",
            )

    def test_rubezh1_keeps_german_even_without_umlauts(self):
        # German signal (umlaut/ß OR a German-only word OR grammar term) must always keep it.
        for raw in [
            "begegnen", "sich begegnen", "gesetzwidrig", "widerrechtlich", "herwärts",
            "meiner Ansicht nach", "Ich bin begeistert", "Das ist cringe",
            "sich verlassen auf + Akkusativ", "erinnern an + Akkusativ", "284 Kilometer",
        ]:
            cleaned = server._shortcut_normalize_unit_text(raw)
            self.assertTrue(
                server._shortcut_is_learnable_unit(cleaned, cleaned),
                msg=f"expected KEEP (German) for {raw!r}",
            )

    def test_normalize_strips_dangling_trailing_dash_and_leading_bullet(self):
        self.assertEqual(server._shortcut_normalize_unit_text("Hallo meine Lieben —"), "Hallo meine Lieben")
        self.assertEqual(server._shortcut_normalize_unit_text("• Darmstadt"), "Darmstadt")
        # Sentence terminators are preserved
        self.assertEqual(server._shortcut_normalize_unit_text("Wer bist du?"), "Wer bist du?")

    def test_extract_blocks_filters_out_garbage_blocks(self):
        raw = json.dumps(
            {
                "blocks": [
                    {"term": "284", "content": "284"},
                    {"term": "die Absage", "content": "die Absage"},
                    {"term": "deutsch_erfolgreich", "content": "deutsch_erfolgreich"},
                    {"term": "in Darmstadt", "content": "in Darmstadt"},
                    {"term": "...", "content": "..."},
                ]
            },
            ensure_ascii=False,
        )
        blocks = server._shortcut_extract_blocks_from_json(raw, "284 die Absage in Darmstadt")
        self.assertEqual(blocks, [("die Absage", "die Absage"), ("in Darmstadt", "in Darmstadt")])

    def test_mechanical_fallback_drops_garbage_lines(self):
        # Both LLM attempts fail → deterministic line split must still filter noise.
        fake_client = _FakeShortcutSplitClient([RuntimeError("mini down"), RuntimeError("full down")])
        with patch("backend.openai_manager.client", fake_client), \
             patch.object(server, "_log_flow_observation"), \
             patch.dict(os.environ, {
                 "LLM_TASK_MODEL_SHORTCUT_SPLIT": "",
                 "OPENAI_TASK_MODEL_SHORTCUT_SPLIT": "",
                 "LLM_TASK_MODEL_SHORTCUT_SPLIT_FALLBACK": "",
                 "OPENAI_TASK_MODEL_SHORTCUT_SPLIT_FALLBACK": "",
             }, clear=False):
            blocks = server._shortcut_split_blocks("284\ndie Absage\n/\\) )", origin="shortcut", user_id=1, request_key="rk")
        self.assertEqual(blocks, [("die Absage", "die Absage")])

    def test_single_line_garbage_yields_no_blocks(self):
        # A lone "284" with both models down must not be translated.
        fake_client = _FakeShortcutSplitClient([RuntimeError("mini down"), RuntimeError("full down")])
        with patch("backend.openai_manager.client", fake_client), \
             patch.object(server, "_log_flow_observation"), \
             patch.dict(os.environ, {
                 "LLM_TASK_MODEL_SHORTCUT_SPLIT": "",
                 "OPENAI_TASK_MODEL_SHORTCUT_SPLIT": "",
                 "LLM_TASK_MODEL_SHORTCUT_SPLIT_FALLBACK": "",
                 "OPENAI_TASK_MODEL_SHORTCUT_SPLIT_FALLBACK": "",
             }, clear=False):
            blocks = server._shortcut_split_blocks("284", origin="shortcut", user_id=1, request_key="rk")
        self.assertEqual(blocks, [])

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

    def test_delivery_skips_cross_request_duplicates(self):
        # A nightly batch sends each photo as a separate request; the same German word
        # extracted from several photos must reach the user as ONE card, not N. Here the
        # user already has "Strafmaß" pending (an earlier photo) and this request yields
        # "Strafmaß" again plus a new word — only the new word should be sent.
        sent_texts: list[str] = []

        def _fake_send(uid, text, reply_markup=None):
            sent_texts.append(text)

        with patch.object(server, "_shortcut_split_blocks",
                          return_value=[("Strafmaß", "Strafmaß"), ("Rückfallrisiko", "Rückfallrisiko")]), \
             patch.object(server, "_shortcut_existing_pending_norms",
                          return_value={server._shortcut_dedup_norm("Strafmaß")}), \
             patch.object(server, "_shortcut_append_pending_to_redis"), \
             patch.object(server, "_send_private_message", side_effect=_fake_send), \
             patch.object(server, "get_redis_client", return_value=None), \
             patch("time.sleep"):
            sent = server._run_shortcut_lookup_delivery(user_id=99, text="Strafmaß\nRückfallrisiko")

        self.assertEqual(sent, 1)
        self.assertEqual(len(sent_texts), 1)
        self.assertIn("Rückfallrisiko", sent_texts[0])
        self.assertNotIn("Strafmaß", sent_texts[0])

    def test_delivery_collapses_duplicates_within_one_request(self):
        # Two identical units inside a single request must also collapse to one card.
        sent_texts: list[str] = []

        with patch.object(server, "_shortcut_split_blocks",
                          return_value=[("die Absage", "die Absage"), ("die absage.", "die absage.")]), \
             patch.object(server, "_shortcut_existing_pending_norms", return_value=set()), \
             patch.object(server, "_shortcut_append_pending_to_redis"), \
             patch.object(server, "_send_private_message",
                          side_effect=lambda uid, text, reply_markup=None: sent_texts.append(text)), \
             patch.object(server, "get_redis_client", return_value=None), \
             patch("time.sleep"):
            sent = server._run_shortcut_lookup_delivery(user_id=99, text="die Absage\ndie absage.")

        self.assertEqual(sent, 1)
        self.assertEqual(len(sent_texts), 1)

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
             patch.object(server, "resolve_entitlement", return_value={"effective_mode": "free", "plan_code": "free"}), \
             patch.object(server, "get_free_feature_limit_metadata", return_value={"free_limit": 15}), \
             patch.object(server, "get_free_feature_usage_today", return_value=0.0), \
             patch.object(server, "increment_free_feature_usage") as increment_mock, \
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
        increment_mock.assert_called_once()
        self.assertEqual(increment_mock.call_args.kwargs["feature_key"], "shortcut_forwarded_message_daily")

    def test_shortcut_lookup_invalid_token_does_not_consume_limit(self):
        with patch.object(server, "resolve_shortcut_install_token", return_value=None), \
             patch.object(server, "get_free_feature_usage_today") as usage_mock, \
             patch.object(server, "increment_free_feature_usage") as increment_mock, \
             patch.object(server, "_start_shortcut_lookup_enqueue_runner") as enqueue_mock:
            response = self.client.post(
                "/api/shortcut/lookup",
                json={"text": "noisy input", "install_token": "bad-token"},
            )

        self.assertEqual(response.status_code, 401)
        usage_mock.assert_not_called()
        increment_mock.assert_not_called()
        enqueue_mock.assert_not_called()

    def test_free_shortcut_lookup_sixteenth_request_is_blocked_before_enqueue(self):
        with patch.object(server, "resolve_shortcut_install_token", return_value={"installation_id": 11, "user_id": 77}), \
             patch.object(server, "is_telegram_user_allowed", return_value=True), \
             patch.object(server, "resolve_entitlement", return_value={"effective_mode": "free", "plan_code": "free"}), \
             patch.object(server, "get_free_feature_limit_metadata", return_value={"free_limit": 15}), \
             patch.object(server, "get_free_feature_usage_today", return_value=15.0), \
             patch.object(server, "increment_free_feature_usage") as increment_mock, \
             patch.object(server, "_shortcut_dedup_reserve", return_value=False), \
             patch.object(server, "can_enqueue_background_jobs", return_value=True), \
             patch.object(server, "_start_shortcut_lookup_enqueue_runner") as enqueue_mock, \
             patch.object(server, "_shortcut_split_blocks") as split_mock:
            response = self.client.post(
                "/api/shortcut/lookup",
                json={"text": "noisy input", "install_token": "install-token-value"},
            )

        self.assertEqual(response.status_code, 429)
        payload = response.get_json()
        self.assertEqual(payload["error"], "free_limit_exceeded")
        self.assertEqual(payload["feature"], "shortcut_forwarded_message_daily")
        enqueue_mock.assert_not_called()
        split_mock.assert_not_called()
        increment_mock.assert_not_called()

    def test_pro_shortcut_lookup_is_not_blocked_by_free_limit(self):
        with patch.object(server, "resolve_shortcut_install_token", return_value={"installation_id": 11, "user_id": 77}), \
             patch.object(server, "is_telegram_user_allowed", return_value=True), \
             patch.object(server, "resolve_entitlement", return_value={"effective_mode": "pro", "plan_code": "pro"}), \
             patch.object(server, "get_free_feature_usage_today") as usage_mock, \
             patch.object(server, "increment_free_feature_usage") as increment_mock, \
             patch.object(server, "_shortcut_dedup_reserve", return_value=False), \
             patch.object(server, "can_enqueue_background_jobs", return_value=True), \
             patch.object(server, "_start_shortcut_lookup_enqueue_runner", return_value="job-456") as enqueue_mock:
            response = self.client.post(
                "/api/shortcut/lookup",
                json={"text": "noisy input", "install_token": "install-token-value"},
            )

        self.assertEqual(response.status_code, 200)
        enqueue_mock.assert_called_once()
        usage_mock.assert_not_called()
        increment_mock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
