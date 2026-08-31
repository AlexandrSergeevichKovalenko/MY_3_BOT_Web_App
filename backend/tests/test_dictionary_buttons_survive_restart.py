# -*- coding: utf-8 -*-
"""Кнопки под карточкой словаря переживают перезапуск бота.

Повод (31.08.2026). Человек получил «Быстрый перевод» в 16:27, в 16:33 ушёл деплой
(девятый за три часа), в 16:40 он нажал «Сохранить 1» и увидел «Варианты устарели.
Запросите перевод снова.» Варианты лежали в обычном dict в памяти процесса, и
перезапуск стирал их целиком; тот же результат давал потолок 500 записей на пачке
«Быстрого перевода» — уже без всякого деплоя.

Здесь проверяется ровно это: варианты и карточка уходят в хранилище состояний
(bt_3_telegram_pending_input_states), а кнопка читает их ОТТУДА, а не из памяти.
Тесты живой базы не касаются: подменяются три функции доступа к таблице.
"""
import asyncio
import contextlib
import json
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import bot_3


class _FakeStateTable:
    """Таблица состояний в памяти теста. Payload гоняется через JSON — это ловит
    поля, которые в JSONB не лягут (в проде такая запись просто упала бы)."""

    def __init__(self):
        self.rows = {}
        self.ttls = {}

    def upsert(self, *, state_key, user_id, state_type, payload, ttl_seconds):
        assert int(ttl_seconds or 0) > 0, "без TTL строка останется в таблице навсегда"
        self.rows[str(state_key)] = {
            "state_key": str(state_key),
            "user_id": int(user_id),
            "state_type": str(state_type),
            "payload": json.loads(json.dumps(payload)),
        }
        self.ttls[str(state_key)] = int(ttl_seconds)

    def get(self, state_key):
        return self.rows.get(str(state_key))

    def delete(self, *, state_key, user_id=None):
        self.rows.pop(str(state_key), None)


@contextlib.contextmanager
def _table(store):
    with patch.object(bot_3, "upsert_pending_telegram_input_state", store.upsert), \
         patch.object(bot_3, "get_pending_telegram_input_state", store.get), \
         patch.object(bot_3, "delete_pending_telegram_input_state", store.delete):
        yield store


class _FakeMessage:
    def __init__(self, text=""):
        self.text = text
        self.caption = ""
        self.chat_id = 777
        self.message_id = 42
        self.reply_markup = None
        self.replies = []

    async def reply_text(self, text, **kw):
        self.replies.append(text)


class _FakeQuery:
    def __init__(self, data, uid=5, text=""):
        self.data = data
        self.from_user = SimpleNamespace(id=uid)
        self.message = _FakeMessage(text)
        self.answers = []
        self.markups = []

    async def answer(self, text="", show_alert=False):
        self.answers.append(text)

    async def edit_message_reply_markup(self, reply_markup=None):
        self.markups.append(reply_markup)

    def labels(self):
        return [
            None if m is None else m.inline_keyboard[0][0].text
            for m in self.markups
        ]


_PAYLOAD = {
    "user_id": 5,
    "card_key": "card-1",
    "direction": "de-ru",
    "source_lang": "de",
    "target_lang": "ru",
    "lookup": {"word_source": "die Herberge", "word_target": "хостел"},
    "options": [{"source": "die Herberge", "target": "хостел"}],
    "selected": [],
    "keyboard_mode": "quick",
}

_CARD_TEXT = (
    "⚡ Быстрый перевод\n"
    "🌐 DE → RU\n"
    "\n"
    "• Запрос: Zum Zeitpunkt ihres letzten Antrags\n"
    "\n"
    "📌 Варианты для сохранения\n"
    "\n"
    "1. DE: Zum Zeitpunkt ihres letzten Antrags\n"
    "   RU: на момент подачи её последнего заявления\n"
)


class DictionaryButtonsSurviveRestartTests(unittest.TestCase):
    def test_variants_land_in_the_state_table_not_in_process_memory(self):
        store = _FakeStateTable()
        with _table(store):
            bot_3._put_dictionary_pending_state(
                "opt-1", bot_3.PENDING_INPUT_STATE_DICTIONARY_SAVE_OPTIONS, _PAYLOAD,
            )
        self.assertEqual(list(store.rows), ["opt-1"])
        self.assertEqual(store.rows["opt-1"]["state_type"], "dictionary_save_options")
        self.assertEqual(store.rows["opt-1"]["user_id"], 5)
        self.assertGreater(store.ttls["opt-1"], 24 * 60 * 60)
        # Никакого dict в памяти модуля больше нет — иначе перезапуск снова всё сотрёт.
        self.assertFalse(hasattr(bot_3, "pending_dictionary_save_options"))
        self.assertFalse(hasattr(bot_3, "pending_dictionary_cards"))

    def test_key_of_another_kind_is_not_read_as_variants(self):
        store = _FakeStateTable()
        with _table(store):
            bot_3._put_dictionary_pending_state(
                "card-1", bot_3.PENDING_INPUT_STATE_DICTIONARY_CARD, _PAYLOAD,
            )
            self.assertIsNone(bot_3._get_dictionary_pending_state(
                "card-1", bot_3.PENDING_INPUT_STATE_DICTIONARY_SAVE_OPTIONS,
            ))
            self.assertIsNotNone(bot_3._get_dictionary_pending_state(
                "card-1", bot_3.PENDING_INPUT_STATE_DICTIONARY_CARD,
            ))

    def test_save_one_works_after_the_bot_process_restarted(self):
        store = _FakeStateTable()
        with _table(store):
            bot_3._put_dictionary_pending_state(
                "opt-1", bot_3.PENDING_INPUT_STATE_DICTIONARY_SAVE_OPTIONS, _PAYLOAD,
            )
            # «Перезапуск»: в памяти процесса не осталось ничего, таблица — осталась.
            q = _FakeQuery("dictquicksave:opt-1:0", text=_CARD_TEXT)
            with patch.object(bot_3, "_save_dictionary_option_for_user",
                              return_value=(True, "ok", 1, True)):
                asyncio.run(bot_3.handle_dictionary_quick_save_callback(
                    SimpleNamespace(callback_query=q), None,
                ))
        self.assertNotIn("Варианты устарели. Запросите перевод снова.", q.answers)
        self.assertEqual(q.labels()[-1], "✅ Сохранено")
        self.assertEqual(q.message.replies, [])
        self.assertNotIn("opt-1", store.rows)  # сохранённое из таблицы убрано

    def test_card_sent_before_the_move_is_rebuilt_from_its_own_text(self):
        # У карточек, отправленных до 31.08.2026, строки в таблице нет вообще.
        # Варианты восстанавливаются из текста, который человек видит на экране.
        store = _FakeStateTable()
        q = _FakeQuery("dictquicksave:opt-old:0", text=_CARD_TEXT)
        with _table(store), \
             patch.object(bot_3, "_resolve_private_dictionary_save_folder", return_value={}), \
             patch.object(bot_3, "_save_dictionary_option_for_user",
                          return_value=(True, "ok", 1, True)):
            asyncio.run(bot_3.handle_dictionary_quick_save_callback(
                SimpleNamespace(callback_query=q), None,
            ))
        self.assertNotIn("Варианты устарели. Запросите перевод снова.", q.answers)
        self.assertEqual(q.labels()[-1], "✅ Сохранено")


if __name__ == "__main__":
    unittest.main()
