import asyncio
import contextlib
import unittest
from unittest.mock import patch

import bot_3


@contextlib.contextmanager
def _state_table(rows=None):
    """Хранилище состояний карточки — в памяти теста.

    Варианты сохранения переехали из dict в памяти бота в таблицу
    bt_3_telegram_pending_input_states (31.08.2026), поэтому обработчик теперь ходит
    в базу. Тест боевую базу не трогает: подменяем три функции доступа.
    """
    table = dict(rows or {})

    def _upsert(*, state_key, user_id, state_type, payload, ttl_seconds):
        table[str(state_key)] = {
            "state_key": str(state_key), "user_id": int(user_id),
            "state_type": str(state_type), "payload": payload,
        }

    def _get(state_key):
        return table.get(str(state_key))

    def _delete(*, state_key, user_id=None):
        table.pop(str(state_key), None)

    with patch.object(bot_3, "upsert_pending_telegram_input_state", _upsert), \
         patch.object(bot_3, "get_pending_telegram_input_state", _get), \
         patch.object(bot_3, "delete_pending_telegram_input_state", _delete):
        yield table


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
        rows = {"ok1": {"state_key": "ok1", "user_id": 5,
                        "state_type": bot_3.PENDING_INPUT_STATE_DICTIONARY_SAVE_OPTIONS,
                        "payload": payload}}
        with patch.object(bot_3, "_save_dictionary_option_for_user", return_value=(True, "ok", 1, True)), \
             _state_table(rows):
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
        with patch.object(bot_3, "_save_dictionary_option_for_user", return_value=(True, "ok", 1, True)), \
             _state_table():
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
        with patch.object(bot_3, "_save_dictionary_option_for_user", return_value=(False, "Лимит исчерпан", 0, False)), \
             _state_table():
            asyncio.run(bot_3._save_dictionary_variants_in_place(
                q, None, option_key="k", payload=payload, user_id=5,
                selected_idxs=[0], options=opts,
            ))
        self.assertEqual(self._labels(q)[-1], "⚠️ Лимит бесплатного тарифа")
        self.assertEqual(q.message.replies, [])


if __name__ == "__main__":
    unittest.main()
