import unittest
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import bot_3


class _FakeRedis:
    def __init__(self):
        self.values = {}
        self.hashes = {}
        self.lists = {}
        self.deleted = []

    def get(self, key):
        return self.values.get(key)

    def setex(self, key, ttl, value):
        self.values[key] = value

    def expire(self, key, ttl):
        return True

    def hset(self, key, field, value):
        self.hashes.setdefault(key, {})[field] = value

    def hgetall(self, key):
        return dict(self.hashes.get(key, {}))

    def hdel(self, key, field):
        if key in self.hashes:
            self.hashes[key].pop(field, None)

    def rpush(self, key, value):
        self.lists.setdefault(key, []).append(value)

    def lrange(self, key, start, end):
        values = list(self.lists.get(key, []))
        if end == -1:
            return values[start:]
        return values[start:end + 1]

    def delete(self, key):
        self.deleted.append(key)
        self.values.pop(key, None)
        self.hashes.pop(key, None)
        self.lists.pop(key, None)


class _FakeBatchBot:
    def __init__(self):
        self.sent_messages = []
        self.edited_messages = []
        self._message_id = 1000

    async def send_message(self, **kwargs):
        self._message_id += 1
        self.sent_messages.append(kwargs)
        return SimpleNamespace(
            chat_id=kwargs.get("chat_id"),
            message_id=self._message_id,
        )

    async def edit_message_reply_markup(self, **kwargs):
        self.edited_messages.append(kwargs)
        return True


class PrivateDictionaryBatchFastButtonTests(unittest.TestCase):
    def setUp(self):
        self._orig_pending = dict(bot_3.pending_dictionary_lookup_requests)
        bot_3.pending_dictionary_lookup_requests.clear()

    def tearDown(self):
        bot_3.pending_dictionary_lookup_requests.clear()
        bot_3.pending_dictionary_lookup_requests.update(self._orig_pending)

    def test_private_keyboard_includes_batch_fast_button(self):
        markup = bot_3._build_private_language_tutor_reply_keyboard()
        labels = [
            str(getattr(button, "text", "") or "")
            for row in getattr(markup, "keyboard", []) or []
            for button in row or []
        ]
        self.assertIn(bot_3.DICTIONARY_BATCH_FAST_BUTTON_TEXT, labels)

    def test_private_keyboard_places_shortcut_buttons_on_bottom_row(self):
        markup = bot_3._build_private_language_tutor_reply_keyboard()
        rows = [
            [str(getattr(button, "text", "") or "") for button in row or []]
            for row in getattr(markup, "keyboard", []) or []
        ]

        self.assertEqual(rows[0], [bot_3.LANGUAGE_TUTOR_BUTTON_TEXT])
        self.assertEqual(rows[1], [bot_3.DICTIONARY_BATCH_FAST_BUTTON_TEXT])
        self.assertEqual(rows[-1], [bot_3.SHORTCUT_INSTALL_BUTTON_TEXT, bot_3.SHORTCUT_CONNECT_BUTTON_TEXT])

    def test_open_private_chat_keyboard_uses_direct_bot_link(self):
        context = SimpleNamespace(bot=SimpleNamespace(username="TestDeutschBot"))

        markup = bot_3.asyncio.run(bot_3._build_open_private_chat_keyboard(context, start="quiz"))
        button = markup.inline_keyboard[0][0]

        self.assertEqual(button.text, "💬 Открыть личку с ботом")
        self.assertEqual(button.url, "https://t.me/TestDeutschBot?start=quiz")

    def test_dictionary_lookup_accepts_long_single_sentence(self):
        text = (
            "Eine Biene verschwendet ihre Energie nicht damit, einer Fliege zu erklären, "
            "dass Honig besser schmeckt als Scheiße"
        )

        self.assertTrue(bot_3._is_dictionary_lookup_candidate(text))

    def test_dictionary_lookup_still_rejects_large_paragraphs(self):
        text = " ".join(["Dieses lange Beispiel"] * 20)

        self.assertFalse(bot_3._is_dictionary_lookup_candidate(text))

    def test_language_tutor_reply_labels_save_button_targets(self):
        message = bot_3._build_language_tutor_reply_message(
            {
                "answer": (
                    "*💬 Примеры в контексте*\n"
                    "- Beispiel eins — первый пример.\n"
                    "- Beispiel zwei — второй пример.\n"
                    "- Beispiel drei — третий пример."
                ),
                "source_lang": "ru",
                "target_lang": "de",
                "save_variants": [
                    {
                        "source_text": "Я случайно тебе позвонил.",
                        "target_text": "Ich habe dich aus Versehen angerufen.",
                    },
                    {
                        "source_text": "Я набрал не тот номер.",
                        "target_text": "Ich habe die falsche Nummer gewählt.",
                    },
                ],
            }
        )

        self.assertIn("*💾 Что сохранят кнопки:*", message)
        self.assertIn("1. RU: Я случайно тебе позвонил.", message)
        self.assertIn("   DE: Ich habe dich aus Versehen angerufen.", message)
        self.assertIn("2. RU: Я набрал не тот номер.", message)
        self.assertIn("   DE: Ich habe die falsche Nummer gewählt.", message)

    def test_quiz_commentary_normalizes_synonym_scale(self):
        items = bot_3._normalize_quiz_result_commentary(
            {
                "items": [
                    {"emoji": "🔎", "text": "Verblüfft сильнее, чем überrascht."},
                    {
                        "type": "synonym_scale",
                        "title": "Синонимы по силе",
                        "scale": [
                            {"de": "überrascht", "ru": "удивлён"},
                            {"de": "erstaunt", "ru": "поражён"},
                            {"de": "verblüfft", "ru": "ошеломлён"},
                            {"de": "fassungslos", "ru": "в полном шоке"},
                        ],
                    },
                ]
            }
        )

        self.assertEqual(items[1]["type"], "synonym_scale")
        self.assertEqual(items[1]["scale"][0], {"de": "überrascht", "ru": "удивлён"})
        self.assertEqual(items[1]["scale"][-1], {"de": "fassungslos", "ru": "в полном шоке"})

    def test_legacy_translation_capture_is_disabled_for_numbered_text_by_default(self):
        text = (
            "Am 10.06.2024 habe ich bereits einen Termin bei Dr. Bell.\n"
            "Daher möchte ich fragen, ob ich die Blutprobe zweimal abnehmen lassen muss.\n"
            "2. Der Brief zu Doktor Bell:\n"
            "Fragen ob sie mir einen Test meines Cholesterin-niveau machen könnten."
        )
        context = SimpleNamespace(user_data={})

        with patch.object(bot_3, "ENABLE_LEGACY_TRANSLATION_TEXT_CAPTURE", False):
            self.assertEqual(bot_3._extract_legacy_translation_submissions(text, context), [])

    def test_legacy_translation_capture_requires_active_legacy_session(self):
        text = "1. Ich habe den Termin vereinbart.\n2. Ich komme morgen."

        with patch.object(bot_3, "ENABLE_LEGACY_TRANSLATION_TEXT_CAPTURE", True):
            self.assertEqual(
                bot_3._extract_legacy_translation_submissions(text, SimpleNamespace(user_data={})),
                [],
            )
            self.assertEqual(
                bot_3._extract_legacy_translation_submissions(
                    text,
                    SimpleNamespace(user_data={"pending_translations": []}),
                ),
                [
                    ("1", "Ich habe den Termin vereinbart."),
                    ("2", "Ich komme morgen."),
                ],
            )

    def test_pending_dictionary_lookup_keys_filtered_by_user(self):
        bot_3.pending_dictionary_lookup_requests["k1"] = {"user_id": 11, "text": "eins"}
        bot_3.pending_dictionary_lookup_requests["k2"] = {"user_id": 22, "text": "zwei"}
        bot_3.pending_dictionary_lookup_requests["k3"] = {"user_id": 11, "text": "drei"}

        self.assertEqual(
            bot_3._list_pending_dictionary_lookup_request_keys_for_user(11),
            ["k1", "k3"],
        )
        self.assertEqual(
            bot_3._list_pending_dictionary_lookup_request_keys_for_user(22),
            ["k2"],
        )

    def test_shortcut_pending_is_promoted_to_primary_redis_queue(self):
        redis = _FakeRedis()
        redis.values["dict_pending_user:11"] = "[]"
        redis.values["dict_pending_shortcut:11"] = (
            '[{"key": "sc1", "user_id": 11, "text": "eins"}]'
        )

        with patch("backend.job_queue.get_redis_client", return_value=redis):
            self.assertEqual(
                bot_3._list_pending_dictionary_lookup_request_keys_for_user(11),
                ["sc1"],
            )

        self.assertIn("dict_pending_shortcut:11", redis.deleted)
        self.assertIn('"key": "sc1"', redis.values["dict_pending_user:11"])
        self.assertNotIn("dict_pending_shortcut:11", redis.values)

    def test_listing_merges_in_memory_and_shortcut_pending(self):
        redis = _FakeRedis()
        redis.values["dict_pending_shortcut:11"] = (
            '[{"key": "sc2", "user_id": 11, "text": "zwei"}]'
        )
        bot_3.pending_dictionary_lookup_requests["k1"] = {"user_id": 11, "text": "eins"}

        with patch("backend.job_queue.get_redis_client", return_value=redis):
            self.assertEqual(
                bot_3._list_pending_dictionary_lookup_request_keys_for_user(11),
                ["k1", "sc2"],
            )

    def test_listing_restores_all_hash_pending_entries(self):
        redis = _FakeRedis()
        redis.hashes["dict_pending_user_hash:11"] = {
            "h1": json.dumps({"key": "h1", "user_id": 11, "text": "eins"}, ensure_ascii=False),
            "h2": json.dumps({"key": "h2", "user_id": 11, "text": "zwei"}, ensure_ascii=False),
            "h3": json.dumps({"key": "h3", "user_id": 11, "text": "drei"}, ensure_ascii=False),
        }

        with patch("backend.job_queue.get_redis_client", return_value=redis):
            self.assertEqual(
                bot_3._list_pending_dictionary_lookup_request_keys_for_user(11),
                ["h1", "h2", "h3"],
            )

    def test_quick_prepare_skips_collocation_generation(self):
        lookup = {
            "word_source": "Biernachschub",
            "word_target": "поставка пива",
            "source_lang": "de",
            "target_lang": "ru",
            "save_worthy_options": [
                {"source": "der Biernachschub", "target": "поставка пива"},
            ],
        }

        with patch.object(bot_3, "_run_dictionary_lookup_for_pair", new=AsyncMock(return_value=lookup)), \
             patch.object(bot_3, "_generate_dictionary_save_options", new=AsyncMock(side_effect=AssertionError("slow path"))), \
             patch.object(bot_3, "_resolve_private_dictionary_save_folder", return_value={"folder_id": None, "name": "GENERAL", "icon": "📁"}):
            prepared = bot_3.asyncio.run(
                bot_3._prepare_dictionary_lookup_response(
                    user_id=11,
                    lookup_input="Biernachschub",
                    source_lang="de",
                    target_lang="ru",
                    max_options=2,
                    fast_options=True,
                )
            )

        self.assertEqual(prepared["options"][0]["source"], "Biernachschub")
        self.assertEqual(prepared["options"][0]["target"], "поставка пива")
        self.assertEqual(prepared["options"][1]["source"], "der Biernachschub")

    def test_batch_fast_uses_single_batch_lookup_payloads(self):
        bot_3.pending_dictionary_lookup_requests["k1"] = {"user_id": 11, "text": "Biernachschub", "message_id": 501}
        bot_3.pending_dictionary_lookup_requests["k2"] = {"user_id": 11, "text": "schwindeln", "message_id": 502}
        fake_bot = _FakeBatchBot()
        context = SimpleNamespace(bot=fake_bot, user_data={})
        batch_payloads = {
            "k1": {
                "word_source": "der Biernachschub",
                "word_target": "поставка пива",
                "save_worthy_options": [
                    {"source": "der Biernachschub", "target": "поставка пива"},
                ],
            },
            "k2": {
                "word_source": "schwindeln",
                "word_target": "жульничать",
                "save_worthy_options": [
                    {"source": "schwindeln", "target": "жульничать"},
                ],
            },
        }

        with patch.object(bot_3, "run_dictionary_lookup_multilang_core_fast_batch", new=AsyncMock(return_value=batch_payloads)) as batch_mock, \
             patch.object(bot_3, "_run_dictionary_lookup_for_pair", new=AsyncMock(side_effect=AssertionError("per-word lookup should not run"))), \
             patch.object(bot_3, "_resolve_private_dictionary_save_folder", return_value={"folder_id": None, "name": "GENERAL", "icon": "📁"}), \
             patch.object(bot_3, "add_service_msg_id"), \
             patch.object(bot_3, "_remove_pending_from_redis"), \
             patch.object(bot_3, "_sync_pending_to_redis"):
            bot_3.asyncio.run(
                bot_3._run_dictionary_batch_fast_for_user(
                    context,
                    user_id=11,
                    chat_id=777,
                    pending_snapshot=dict(bot_3.pending_dictionary_lookup_requests),
                )
            )

        batch_mock.assert_awaited_once()
        self.assertEqual(len(fake_bot.sent_messages), 2)
        self.assertIn("der Biernachschub", fake_bot.sent_messages[0]["text"])
        self.assertIn("schwindeln", fake_bot.sent_messages[1]["text"])


if __name__ == "__main__":
    unittest.main()
