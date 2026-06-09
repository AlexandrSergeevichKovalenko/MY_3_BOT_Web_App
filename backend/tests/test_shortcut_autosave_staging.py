import fnmatch
import json
import unittest
from contextlib import ExitStack
from unittest.mock import patch

import backend.backend_server as server


class _FakeRedis:
    """Minimal in-memory Redis covering the ops the autosave staging/sweep/flush path uses."""

    def __init__(self):
        self.kv: dict[str, str] = {}
        self.lists: dict[str, list] = {}

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
            self.lists.pop(k, None)
        return True

    def expire(self, key, ttl):
        return True

    def scan_iter(self, match="*", count=100):
        for k in list(self.kv.keys()):
            if fnmatch.fnmatch(k, match):
                yield k

    # --- list ops ---
    def rpush(self, key, *values):
        self.lists.setdefault(key, []).extend(values)
        return len(self.lists[key])

    def lrange(self, key, start, end):
        lst = self.lists.get(key, [])
        if end == -1:
            return list(lst[start:])
        return list(lst[start:end + 1])


class ShortcutAutosaveStagingTests(unittest.TestCase):
    def setUp(self):
        self.redis = _FakeRedis()
        self.sent = []  # (text, reply_markup)

    def _enter_common(self, stack):
        stack.enter_context(patch.object(server, "get_redis_client", return_value=self.redis))
        stack.enter_context(patch.object(
            server, "_send_private_message",
            side_effect=lambda uid, text, reply_markup=None, parse_mode=None: self.sent.append((text, reply_markup)),
        ))

    def test_staging_is_pure_redis_append_no_split(self):
        # The request path must NOT split (no LLM) — only RPUSH raw text + arm flush-at.
        with ExitStack() as stack:
            split = stack.enter_context(patch.object(server, "_shortcut_split_blocks"))
            stack.enter_context(patch.object(server, "get_redis_client", return_value=self.redis))
            n1 = server._run_shortcut_autosave_staging(user_id=42, text="Foto 1 text")
            n2 = server._run_shortcut_autosave_staging(user_id=42, text="Foto 2 text")
        split.assert_not_called()  # no per-request split
        self.assertEqual(n1, 1)
        self.assertEqual(n2, 2)  # raw queue length grows
        self.assertEqual(self.redis.lists[server._autosave_raw_key(42)], ["Foto 1 text", "Foto 2 text"])
        self.assertIn(server._autosave_flush_at_key(42), self.redis.kv)  # debounce armed

    def test_collect_due_claims_only_elapsed(self):
        now = server.time.time()
        self.redis.kv[server._autosave_flush_at_key(7)] = f"{now - 5:.3f}"      # due
        self.redis.kv[server._autosave_flush_at_key(8)] = f"{now + 999:.3f}"    # not yet
        with patch.object(server, "get_redis_client", return_value=self.redis):
            due = server._autosave_collect_due_user_ids()
        self.assertEqual(due, [7])
        # claimed (deleted) so it won't be re-enqueued; the future one stays
        self.assertNotIn(server._autosave_flush_at_key(7), self.redis.kv)
        self.assertIn(server._autosave_flush_at_key(8), self.redis.kv)

    def test_flush_one_split_one_prepare_then_digest(self):
        uid = 99
        raw_key = server._autosave_raw_key(uid)
        self.redis.rpush(raw_key, "Strafmaß\nblamierst", "Strafmaß noch mal")
        self.redis.kv[server._autosave_flush_at_key(uid)] = "1.0"

        with ExitStack() as stack:
            split = stack.enter_context(patch.object(
                server, "_shortcut_split_blocks",
                return_value=[("Strafmaß", "Strafmaß"), ("blamierst", "blamierst"), ("strafmaß.", "strafmaß.")],
            ))
            prep = stack.enter_context(patch.object(
                server, "_autosave_prepare_cards",
                return_value=[
                    {"canonical": "das Strafmaß", "translation": "мера наказания", "semantic_category": "Право"},
                    {"canonical": "blamieren", "translation": "позорить", "semantic_category": "Эмоции"},
                ],
            ))
            stack.enter_context(patch.object(server, "_get_user_language_pair", return_value=("ru", "de", {})))
            self._enter_common(stack)
            server._run_autosave_flush(uid)

        # ONE split over the whole concatenated batch (not per photo)
        self.assertEqual(split.call_count, 1)
        self.assertIn("Strafmaß\nblamierst\nStrafmaß noch mal", split.call_args[0][0])
        # cross-photo dedup → "strafmaß." collapsed, 2 unique terms passed to prepare
        self.assertEqual(prep.call_args[0][0], ["Strafmaß", "blamierst"])
        # one digest sent, raw queue consumed
        self.assertEqual(len(self.sent), 1)
        self.assertNotIn(raw_key, self.redis.lists)
        self.assertNotIn(server._autosave_flush_at_key(uid), self.redis.kv)
        text, markup = self.sent[0]
        self.assertIn("1. <b>das Strafmaß</b> — мера наказания", text)
        rows = markup["inline_keyboard"]
        self.assertEqual(rows[0][0]["text"], "✅ 1")
        self.assertIn("Сохранить выбранные (2)", rows[1][0]["text"])
        # digest state carries semantic_category for folder routing on save
        digest_id = rows[1][0]["callback_data"].split(":")[1]
        state = json.loads(self.redis.kv[server._autosave_digest_key(digest_id)])
        self.assertEqual(state["items"][0]["semantic_category"], "Право")
        self.assertEqual(state["selected"], [True, True])

    def test_flush_is_nx_locked_against_concurrent(self):
        uid = 55
        self.redis.rpush(server._autosave_raw_key(uid), "text")
        # Pre-hold the lock → flush must bail without sending.
        self.redis.kv[server._autosave_flush_lock_key(uid)] = "1"
        with ExitStack() as stack:
            self._enter_common(stack)
            stack.enter_context(patch.object(server, "_shortcut_split_blocks", return_value=[("x", "x")]))
            server._run_autosave_flush(uid)
        self.assertEqual(self.sent, [])

    def test_plural_words(self):
        self.assertEqual(server._autosave_plural_words(1), "слово")
        self.assertEqual(server._autosave_plural_words(3), "слова")
        self.assertEqual(server._autosave_plural_words(12), "слов")
        self.assertEqual(server._autosave_plural_words(21), "слово")


if __name__ == "__main__":
    unittest.main()
