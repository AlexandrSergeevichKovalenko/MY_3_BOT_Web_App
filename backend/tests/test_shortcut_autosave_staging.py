import json
import unittest
from contextlib import ExitStack
from unittest.mock import patch

import backend.backend_server as server


class _FakeRedis:
    """Minimal in-memory Redis covering the ops the autosave staging/flush path uses."""

    def __init__(self):
        self.kv: dict[str, str] = {}
        self.hashes: dict[str, dict[str, str]] = {}

    # --- string ops ---
    def get(self, key):
        return self.kv.get(key)

    def set(self, key, value, nx=False, ex=None):
        if nx and key in self.kv:
            return None
        self.kv[key] = value
        return True

    def setex(self, key, ttl, value):
        self.kv[key] = value
        return True

    def delete(self, *keys):
        for k in keys:
            self.kv.pop(k, None)
            self.hashes.pop(k, None)
        return True

    def expire(self, key, ttl):
        return True

    # --- hash ops ---
    def hset(self, key, field, value):
        self.hashes.setdefault(key, {})[field] = value
        return 1

    def hkeys(self, key):
        return list(self.hashes.get(key, {}).keys())

    def hgetall(self, key):
        return dict(self.hashes.get(key, {}))


class ShortcutAutosaveStagingTests(unittest.TestCase):
    def setUp(self):
        self.redis = _FakeRedis()
        self.sent = []  # (text, reply_markup)

    def _enter_common(self, stack):
        stack.enter_context(patch.object(server, "get_redis_client", return_value=self.redis))
        stack.enter_context(patch.object(
            server, "_send_private_message",
            side_effect=lambda uid, text, reply_markup=None: self.sent.append((text, reply_markup)),
        ))

    def test_staging_dedups_across_requests(self):
        # Two "photos" (separate requests); "Strafmaß" appears in both → staged once.
        with ExitStack() as stack:
            stack.enter_context(patch.object(
                server, "_shortcut_split_blocks",
                side_effect=[
                    [("Strafmaß", "Strafmaß"), ("Rückfallrisiko", "Rückfallrisiko")],
                    [("strafmaß.", "strafmaß."), ("blamierst", "blamierst")],
                ],
            ))
            stack.enter_context(patch.object(server, "_AUTOSAVE_FLUSH_EXECUTOR"))
            self._enter_common(stack)
            n1 = server._run_shortcut_autosave_staging(user_id=42, text="photo1")
            n2 = server._run_shortcut_autosave_staging(user_id=42, text="photo2")

        self.assertEqual(n1, 2)
        self.assertEqual(n2, 1)  # only "blamierst" is new
        staged = self.redis.hashes[server._autosave_stage_key(42)]
        self.assertEqual(len(staged), 3)

    def test_maybe_flush_respects_debounce_window(self):
        # flush-at is in the FUTURE → a newer request is pending, must NOT flush.
        self.redis.hset(server._autosave_stage_key(7), "x", json.dumps({"term": "x", "content": "x"}))
        self.redis.kv[server._autosave_flush_at_key(7)] = f"{server.time.time() + 999:.3f}"
        with patch.object(server, "_run_autosave_flush") as flush_mock, \
             patch.object(server, "get_redis_client", return_value=self.redis):
            server._autosave_maybe_flush(7)
        flush_mock.assert_not_called()

    def test_flush_translates_and_sends_multiselect_digest(self):
        stage_key = server._autosave_stage_key(99)
        self.redis.hset(stage_key, "strafmaß", json.dumps({"term": "Strafmaß", "content": "Strafmaß", "added_at": 1.0}))
        self.redis.hset(stage_key, "blamierst", json.dumps({"term": "blamierst", "content": "blamierst", "added_at": 2.0}))

        with ExitStack() as stack:
            stack.enter_context(patch.object(server, "_get_user_language_pair", return_value=("ru", "de", {})))
            stack.enter_context(patch.object(
                server, "_autosave_translate_terms", return_value=["мера наказания", "ты позоришь"]))
            self._enter_common(stack)
            server._run_autosave_flush(99)

        # One digest sent, staging consumed
        self.assertEqual(len(self.sent), 1)
        self.assertNotIn(stage_key, self.redis.hashes)
        text, markup = self.sent[0]
        self.assertIn("Ночная подборка", text)
        rows = markup["inline_keyboard"]
        # 2 toggle rows + save + delete
        self.assertEqual(len(rows), 4)
        self.assertIn("Strafmaß", rows[0][0]["text"])
        self.assertIn("мера наказания", rows[0][0]["text"])
        self.assertTrue(rows[0][0]["callback_data"].startswith("asv_tog:"))
        self.assertIn("Сохранить выбранные (0)", rows[2][0]["text"])
        self.assertTrue(rows[2][0]["callback_data"].startswith("asv_save:"))
        self.assertTrue(rows[3][0]["callback_data"].startswith("asv_del:"))
        # digest state persisted for the bot callbacks
        digest_id = rows[0][0]["callback_data"].split(":")[1]
        state = json.loads(self.redis.kv[server._autosave_digest_key(digest_id)])
        self.assertEqual(len(state["items"]), 2)
        self.assertEqual(state["selected"], [False, False])
        self.assertEqual(state["source_lang"], "de")
        self.assertEqual(state["target_lang"], "ru")

    def test_plural_words(self):
        self.assertEqual(server._autosave_plural_words(1), "слово")
        self.assertEqual(server._autosave_plural_words(3), "слова")
        self.assertEqual(server._autosave_plural_words(12), "слов")
        self.assertEqual(server._autosave_plural_words(21), "слово")


if __name__ == "__main__":
    unittest.main()
