import asyncio
import unittest
from unittest.mock import patch

import bot_3


class _FakeMsg:
    def __init__(self):
        self.replies = []
    async def reply_text(self, text, **kw):
        self.replies.append(text)


class _FakeQuery:
    def __init__(self, uid=5):
        from types import SimpleNamespace
        self.from_user = SimpleNamespace(id=uid)
        self.message = _FakeMsg()
        self.answers = []
        self.markups = []  # sequence of reply_markup edits
    async def answer(self, text="", show_alert=False):
        self.answers.append(text)
    async def edit_message_reply_markup(self, reply_markup=None):
        self.markups.append(reply_markup)


class InPlaceSaveTests(unittest.TestCase):
    def _labels(self, q):
        out = []
        for m in q.markups:
            if m is None:
                out.append(None)
            else:
                out.append(m.inline_keyboard[0][0].text)
        return out

    def test_success_shows_saving_then_saved_no_second_message(self):
        q = _FakeQuery()
        payload = {"source_lang": "de", "target_lang": "ru", "card_key": "c1"}
        opts = [{"source": "die Herberge", "target": "хостел"}]
        with patch.object(bot_3, "_save_dictionary_option_for_user", return_value=(True, "ok", 1, True)), \
             patch.object(bot_3, "pending_dictionary_save_options", {"ok1": payload}):
            asyncio.run(bot_3._save_dictionary_variants_in_place(
                q, None, option_key="ok1", payload=payload, user_id=5,
                selected_idxs=[0], options=opts,
            ))
        # in-place: Сохраняем… then Сохранено ; NO second chat message
        self.assertEqual(self._labels(q), ["💾 Сохраняем…", "✅ Сохранено"])
        self.assertEqual(q.message.replies, [])

    def test_multi_shows_count(self):
        q = _FakeQuery()
        payload = {"source_lang": "de", "target_lang": "ru"}
        opts = [{"source": "a", "target": "1"}, {"source": "b", "target": "2"}]
        with patch.object(bot_3, "_save_dictionary_option_for_user", return_value=(True, "ok", 1, True)):
            asyncio.run(bot_3._save_dictionary_variants_in_place(
                q, None, option_key="k", payload=payload, user_id=5,
                selected_idxs=[0, 1], options=opts,
            ))
        self.assertEqual(self._labels(q)[-1], "✅ Сохранено (2)")
        self.assertEqual(q.message.replies, [])

    def test_failure_shows_warning(self):
        q = _FakeQuery()
        payload = {"source_lang": "de", "target_lang": "ru"}
        opts = [{"source": "a", "target": "1"}]
        with patch.object(bot_3, "_save_dictionary_option_for_user", return_value=(False, "Лимит исчерпан", 0, False)):
            asyncio.run(bot_3._save_dictionary_variants_in_place(
                q, None, option_key="k", payload=payload, user_id=5,
                selected_idxs=[0], options=opts,
            ))
        self.assertEqual(self._labels(q)[-1], "⚠️ Лимит бесплатного тарифа")
        self.assertEqual(q.message.replies, [])


if __name__ == "__main__":
    unittest.main()
