import asyncio
import json
import os
import unittest
from unittest.mock import patch

import backend.backend_server as server
import backend.openai_manager as om


class ShortcutScreenshotsPayloadTests(unittest.TestCase):
    def test_screenshots_list_is_joined(self):
        text, tok = server._shortcut_lookup_request_payload(
            {"install_token": "T", "screenshots": ["Hallo", "  ", "Welt da", ""]}
        )
        self.assertEqual(text, "Hallo\nWelt da")
        self.assertEqual(tok, "T")

    def test_legacy_text_still_works(self):
        text, tok = server._shortcut_lookup_request_payload({"install_token": "T", "text": "alt"})
        self.assertEqual((text, tok), ("alt", "T"))

    def test_empty_screenshots_falls_back(self):
        text, _ = server._shortcut_lookup_request_payload({"screenshots": [], "text": "x"})
        self.assertEqual(text, "x")

    def test_screenshots_as_newline_string(self):
        # iOS Shortcuts often serializes a List variable as one newline-joined string.
        text, _ = server._shortcut_lookup_request_payload(
            {"install_token": "T", "screenshots": "Hallo\nWelt\nHaus"}
        )
        self.assertEqual(text, "Hallo\nWelt\nHaus")

    def test_screenshots_as_json_encoded_string(self):
        text, _ = server._shortcut_lookup_request_payload(
            {"install_token": "T", "screenshots": '["Hallo", "Welt", "Haus"]'}
        )
        self.assertEqual(text, "Hallo\nWelt\nHaus")

    def test_screenshots_single_string(self):
        text, _ = server._shortcut_lookup_request_payload(
            {"install_token": "T", "screenshots": "Nur ein Screenshot"}
        )
        self.assertEqual(text, "Nur ein Screenshot")


class FastBatchChunkingTests(unittest.TestCase):
    def test_large_batch_is_chunked_and_fully_returned(self):
        # 50 items with chunk size 20 -> 3 chunks -> all 50 returned (was the 352->7 bug).
        items = [{"key": f"k{i}", "word": f"w{i}"} for i in range(50)]
        chunk_sizes_seen = []

        async def _fake_llm_execute(*, user_message, **kwargs):
            payload = json.loads(user_message)
            its = payload["items"]
            chunk_sizes_seen.append(len(its))
            return json.dumps({"items": [{"key": it["key"], "word_target": "ru_" + it["key"]} for it in its]})

        with patch.dict(os.environ, {"DICTIONARY_FAST_BATCH_CHUNK_SIZE": "20"}, clear=False), \
             patch.object(om, "llm_execute", side_effect=_fake_llm_execute):
            result = asyncio.run(
                om.run_dictionary_lookup_multilang_core_fast_batch(items, "de", "ru")
            )

        self.assertEqual(len(result), 50)  # nothing dropped
        self.assertEqual(set(result.keys()), {f"k{i}" for i in range(50)})
        self.assertEqual(sorted(chunk_sizes_seen), [10, 20, 20])  # 20+20+10
        self.assertEqual(result["k7"]["word_target"], "ru_k7")

    def test_one_bad_chunk_does_not_kill_the_rest(self):
        items = [{"key": f"k{i}", "word": f"w{i}"} for i in range(30)]

        async def _fake_llm_execute(*, user_message, **kwargs):
            payload = json.loads(user_message)
            its = payload["items"]
            if any(it["key"] == "k0" for it in its):  # first chunk returns garbage
                return "not json at all"
            return json.dumps({"items": [{"key": it["key"], "word_target": "x"} for it in its]})

        with patch.dict(os.environ, {"DICTIONARY_FAST_BATCH_CHUNK_SIZE": "20"}, clear=False), \
             patch.object(om, "llm_execute", side_effect=_fake_llm_execute):
            result = asyncio.run(
                om.run_dictionary_lookup_multilang_core_fast_batch(items, "de", "ru")
            )
        # second chunk (k20..k29) still returned despite first chunk failing
        self.assertEqual(len(result), 10)
        self.assertIn("k25", result)


if __name__ == "__main__":
    unittest.main()
