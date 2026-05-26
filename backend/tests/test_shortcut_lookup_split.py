import json
import os
import unittest
from unittest.mock import patch

import backend.backend_server as server


class ShortcutLookupSplitTests(unittest.TestCase):
    def setUp(self):
        self.client = server.app.test_client()

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

    def test_shortcut_lookup_sends_one_clean_request_per_block(self):
        with patch.dict(os.environ, {"SHORTCUT_SECRET": "secret"}, clear=False), \
             patch.object(server, "is_telegram_user_allowed", return_value=True), \
             patch.object(server, "_shortcut_dedup_check", return_value=False), \
             patch.object(
                 server,
                 "_shortcut_split_blocks",
                 return_value=[
                     ("Hat mich gefreut!", "Hat mich gefreut!"),
                     ("Man sieht sich!", "Man sieht sich!"),
                 ],
             ), \
             patch.object(server, "_send_private_message") as send_mock, \
             patch.object(server, "time") as time_mock:
            time_mock.time.return_value = 1234567890.0
            response = self.client.post(
                "/api/shortcut/lookup",
                json={"text": "noisy input", "user_id": 117649764},
                headers={"Authorization": "Bearer secret"},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["blocks_sent"], 2)
        self.assertEqual(send_mock.call_count, 2)
        self.assertEqual(
            send_mock.call_args_list[0].args[1],
            "Запрос: Hat mich gefreut!\n\nВыберите языковую пару для перевода:",
        )
        self.assertEqual(
            send_mock.call_args_list[1].args[1],
            "Запрос: Man sieht sich!\n\nВыберите языковую пару для перевода:",
        )
