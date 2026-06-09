import asyncio
import json
import unittest
from contextlib import ExitStack
from unittest.mock import patch

import bot_3


class _FakeRedis:
    def __init__(self):
        self.kv = {}

    def get(self, key):
        return self.kv.get(key)

    def setex(self, key, ttl, value):
        self.kv[key] = value

    def delete(self, *keys):
        for k in keys:
            self.kv.pop(k, None)


class _FakeUser:
    def __init__(self, uid):
        self.id = uid


class _FakeMessage:
    def __init__(self):
        self.replies = []

    async def reply_text(self, text, **kwargs):
        self.replies.append(text)


class _FakeQuery:
    def __init__(self, data, uid):
        self.data = data
        self.from_user = _FakeUser(uid)
        self.message = _FakeMessage()
        self.answers = []
        self.markup_edits = []
        self.text_edits = []

    async def answer(self, text="", show_alert=False):
        self.answers.append(text)

    async def edit_message_reply_markup(self, reply_markup=None):
        self.markup_edits.append(reply_markup)

    async def edit_message_text(self, text, reply_markup=None, parse_mode=None):
        self.text_edits.append(text)


class _FakeUpdate:
    def __init__(self, query):
        self.callback_query = query


def _digest_state(uid=99):
    return {
        "user_id": uid,
        "source_lang": "de",
        "target_lang": "ru",
        "items": [
            {"term": "Strafmaß", "canonical": "das Strafmaß", "translation": "мера наказания", "semantic_category": "Право"},
            {"term": "blamierst", "canonical": "blamieren", "translation": "позорить", "semantic_category": "Эмоции"},
        ],
        "selected": [False, False],
        "created_at": 1.0,
    }


class BotAutosaveDigestTests(unittest.TestCase):
    def setUp(self):
        self.redis = _FakeRedis()
        self.uid = 99
        self.digest_id = "abc123"
        self.redis.kv[bot_3._autosave_digest_redis_key(self.digest_id)] = json.dumps(_digest_state(self.uid))

    def _patch_redis(self, stack):
        stack.enter_context(patch("backend.job_queue.get_redis_client", return_value=self.redis))

    def test_keyboard_number_toggles_and_footer(self):
        kb = bot_3._autosave_build_digest_keyboard("d", _digest_state()["items"], [True, False])
        rows = kb.inline_keyboard
        # number toggles in one row (2 items), then a single save footer — no delete
        self.assertEqual(rows[0][0].text, "✅ 1")
        self.assertEqual(rows[0][1].text, "⬜️ 2")
        self.assertEqual(rows[0][0].callback_data, "asv_tog:d:0")
        self.assertIn("Сохранить выбранные (1)", rows[1][0].text)
        self.assertEqual(rows[1][0].callback_data, "asv_save:d")
        self.assertFalse(any("asv_del" in b.callback_data for row in rows for b in row))

    def test_toggle_flips_and_persists(self):
        query = _FakeQuery(f"asv_tog:{self.digest_id}:0", self.uid)
        with ExitStack() as stack:
            self._patch_redis(stack)
            asyncio.run(bot_3.handle_autosave_digest_toggle_callback(_FakeUpdate(query), None))
        state = json.loads(self.redis.kv[bot_3._autosave_digest_redis_key(self.digest_id)])
        self.assertEqual(state["selected"], [True, False])
        self.assertEqual(len(query.markup_edits), 1)

    def test_toggle_rejects_non_author(self):
        query = _FakeQuery(f"asv_tog:{self.digest_id}:0", 12345)  # different user
        with ExitStack() as stack:
            self._patch_redis(stack)
            asyncio.run(bot_3.handle_autosave_digest_toggle_callback(_FakeUpdate(query), None))
        state = json.loads(self.redis.kv[bot_3._autosave_digest_redis_key(self.digest_id)])
        self.assertEqual(state["selected"], [False, False])  # unchanged
        self.assertTrue(any("автору" in a for a in query.answers))

    def test_save_persists_canonical_form_and_clears_digest(self):
        # select item 0 only → saves its CANONICAL form (with article), background path
        st = _digest_state(self.uid)
        st["selected"] = [True, False]
        self.redis.kv[bot_3._autosave_digest_redis_key(self.digest_id)] = json.dumps(st)
        saved_calls = []

        def _fake_save(*, payload, chosen, user_id):
            saved_calls.append((chosen, payload))
            return True, "ok", 1, False

        # context=None → context.application raises → handler awaits the bg save inline.
        query = _FakeQuery(f"asv_save:{self.digest_id}", self.uid)
        with ExitStack() as stack:
            self._patch_redis(stack)
            stack.enter_context(patch.object(bot_3, "_save_dictionary_option_for_user", side_effect=_fake_save))
            asyncio.run(bot_3.handle_autosave_digest_save_callback(_FakeUpdate(query), None))

        self.assertEqual(len(saved_calls), 1)
        chosen, payload = saved_calls[0]
        self.assertEqual(chosen["source"], "das Strafmaß")  # canonical, not raw "Strafmaß"
        self.assertEqual(chosen["target"], "мера наказания")
        # semantic_category routed into lookup so the save lands in the right folder
        self.assertEqual(payload["lookup"].get("semantic_category"), "Право")
        # buttons removed immediately + digest consumed
        self.assertIn(None, query.markup_edits)
        self.assertNotIn(bot_3._autosave_digest_redis_key(self.digest_id), self.redis.kv)
        self.assertTrue(any("Сохранено в словарь: 1" in r for r in query.message.replies))

    def test_save_requires_selection(self):
        query = _FakeQuery(f"asv_save:{self.digest_id}", self.uid)  # nothing selected
        with ExitStack() as stack:
            self._patch_redis(stack)
            stack.enter_context(patch.object(bot_3, "_save_dictionary_option_for_user"))
            asyncio.run(bot_3.handle_autosave_digest_save_callback(_FakeUpdate(query), None))
        self.assertTrue(any("Отметьте" in a for a in query.answers))
        # digest still present (not consumed)
        self.assertIn(bot_3._autosave_digest_redis_key(self.digest_id), self.redis.kv)


class _FakeReplyMessage:
    def __init__(self):
        self.replies = []  # (text, reply_markup)

    async def reply_text(self, text, parse_mode=None, reply_markup=None, **kwargs):
        self.replies.append((text, reply_markup))


class _FakeChat:
    type = "private"


class _FakeTapUpdate:
    def __init__(self, uid):
        self.effective_user = _FakeUser(uid)
        self.effective_chat = _FakeChat()
        self.message = _FakeReplyMessage()


class BotAutosaveToggleButtonTests(unittest.TestCase):
    def setUp(self):
        bot_3._AUTOSAVE_STATE_CACHE.clear()

    def test_button_label_reflects_state(self):
        with patch.object(bot_3, "get_shortcut_autosave_enabled", return_value=True):
            bot_3._AUTOSAVE_STATE_CACHE.clear()
            self.assertEqual(bot_3._autosave_button_text(5), "🌙 Автосейв: ВКЛ")
        with patch.object(bot_3, "get_shortcut_autosave_enabled", return_value=False):
            bot_3._AUTOSAVE_STATE_CACHE.clear()
            self.assertEqual(bot_3._autosave_button_text(5), "🌙 Автосейв: ВЫКЛ")
        # unknown user → neutral fallback
        self.assertEqual(bot_3._autosave_button_text(None), bot_3.SHORTCUT_AUTOSAVE_BUTTON_TEXT)

    def test_tap_flips_state_and_rerenders_keyboard(self):
        store = {"v": False}

        def _get(uid):
            return store["v"]

        def _set(uid, val):
            store["v"] = bool(val)
            return bool(val)

        update = _FakeTapUpdate(7)
        with patch.object(bot_3, "get_shortcut_autosave_enabled", side_effect=_get), \
             patch.object(bot_3, "set_shortcut_autosave_enabled", side_effect=_set):
            asyncio.run(bot_3._handle_autosave_button_tap(update, None))

        self.assertTrue(store["v"])  # flipped OFF→ON
        self.assertEqual(len(update.message.replies), 1)
        text, markup = update.message.replies[0]
        self.assertIn("включён", text)
        # re-rendered reply keyboard shows the ON label
        flat = [btn.text for row in markup.keyboard for btn in row]
        self.assertIn("🌙 Автосейв: ВКЛ", flat)


if __name__ == "__main__":
    unittest.main()
