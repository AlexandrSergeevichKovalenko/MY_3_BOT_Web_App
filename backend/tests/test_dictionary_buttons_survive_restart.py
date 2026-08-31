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
import logging
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import patch

import bot_3


class _FakeStateTable:
    """Таблица состояний в памяти теста. Payload гоняется через JSON — это ловит
    поля, которые в JSONB не лягут (в проде такая запись просто упала бы)."""

    def __init__(self):
        self.rows = {}
        self.ttls = {}
        self.misses = []

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
    """Подменяет ВСЕ четыре обращения к базе: три к состоянию карточки и запись
    промаха в телеметрию. Без последней тесты дописывали бы промахи в живую
    ведомость, и недельный отчёт владельцу считал бы прогоны pytest."""
    with patch.object(bot_3, "upsert_pending_telegram_input_state", store.upsert), \
         patch.object(bot_3, "get_pending_telegram_input_state", store.get), \
         patch.object(bot_3, "delete_pending_telegram_input_state", store.delete), \
         patch.object(bot_3, "record_dictionary_button_state_miss",
                      lambda **kw: store.misses.append(kw)):
        yield store


class _FakeMessage:
    def __init__(self, text="", date=None):
        self.date = date
        self.text = text
        self.caption = ""
        self.chat_id = 777
        self.message_id = 42
        self.reply_markup = None
        self.replies = []

    async def reply_text(self, text, **kw):
        self.replies.append(text)


class _FakeQuery:
    def __init__(self, data, uid=5, text="", sent_at=None):
        self.data = data
        self.from_user = SimpleNamespace(id=uid)
        self.message = _FakeMessage(text, date=sent_at)
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


class DictionaryStateMissIsCountedTests(unittest.TestCase):
    """Промах кнопки обязан оставлять строку в логе.

    Пока промахи нигде не считались, «дыра осталась» и «дыры нет» выглядели
    одинаково. Строка отвечает на этот вопрос числом, а recovered= отделяет
    незаметное для человека восстановление от настоящего отказа.
    """

    def test_refusal_writes_recovered_no(self):
        store = _FakeStateTable()
        q = _FakeQuery("dictquicksave:opt-gone:0", text="ничего похожего на карточку")
        with _table(store), self.assertLogs(level=logging.WARNING) as logs:
            asyncio.run(bot_3.handle_dictionary_quick_save_callback(
                SimpleNamespace(callback_query=q), None,
            ))
        line = [x for x in logs.output if "dictionary_state_miss" in x]
        self.assertEqual(len(line), 1, logs.output)
        self.assertIn("button=quick_save", line[0])
        self.assertIn("key=opt-gone", line[0])
        self.assertIn("recovered=no", line[0])
        self.assertIn("user_id=5", line[0])
        # человеку при этом сказано словами, а не молча
        self.assertIn("Варианты устарели. Запросите перевод снова.", q.answers)

    def test_silent_recovery_writes_recovered_yes(self):
        store = _FakeStateTable()
        q = _FakeQuery("dictquicksave:opt-old:0", text=_CARD_TEXT)
        with _table(store), \
             patch.object(bot_3, "_resolve_private_dictionary_save_folder", return_value={}), \
             patch.object(bot_3, "_save_dictionary_option_for_user",
                          return_value=(True, "ok", 1, True)), \
             self.assertLogs(level=logging.INFO) as logs:
            asyncio.run(bot_3.handle_dictionary_quick_save_callback(
                SimpleNamespace(callback_query=q), None,
            ))
        line = [x for x in logs.output if "dictionary_state_miss" in x]
        self.assertEqual(len(line), 1, logs.output)
        self.assertIn("recovered=yes", line[0])
        self.assertNotIn("Варианты устарели. Запросите перевод снова.", q.answers)

    def test_line_carries_the_age_of_the_card(self):
        # Возраст отличает свежую карточку (настоящая дыра) от архивной (истёк срок).
        store = _FakeStateTable()
        sent_at = datetime.now(timezone.utc) - timedelta(minutes=13)
        q = _FakeQuery("dictquicksave:opt-gone:0", text="не карточка", sent_at=sent_at)
        with _table(store), self.assertLogs(level=logging.WARNING) as logs:
            asyncio.run(bot_3.handle_dictionary_quick_save_callback(
                SimpleNamespace(callback_query=q), None,
            ))
        line = [x for x in logs.output if "dictionary_state_miss" in x][0]
        self.assertIn("age_min=13", line)

    def test_hit_writes_nothing(self):
        # Строка обязана появляться ТОЛЬКО на промахе: иначе её число ничего не значит.
        caught = []

        class _Catch(logging.Handler):
            def emit(self, record):
                caught.append(record.getMessage())

        handler = _Catch()
        root = logging.getLogger()
        root.addHandler(handler)
        previous_level = root.level
        root.setLevel(logging.INFO)
        try:
            store = _FakeStateTable()
            with _table(store):
                bot_3._put_dictionary_pending_state(
                    "opt-1", bot_3.PENDING_INPUT_STATE_DICTIONARY_SAVE_OPTIONS, _PAYLOAD,
                )
                q = _FakeQuery("dictquicksave:opt-1:0", text=_CARD_TEXT)
                with patch.object(bot_3, "_save_dictionary_option_for_user",
                                  return_value=(True, "ok", 1, True)):
                    asyncio.run(bot_3.handle_dictionary_quick_save_callback(
                        SimpleNamespace(callback_query=q), None,
                    ))
        finally:
            root.removeHandler(handler)
            root.setLevel(previous_level)
        self.assertEqual([x for x in caught if "dictionary_state_miss" in x], [])


class DictionaryMissReachesTheWeeklyReportTests(unittest.TestCase):
    """Строка в логе отвечает на вопрос, только если её кто-то читает.

    Промах кладётся в общую телеметрию, а недельный отчёт по словарю (Пн 10:00,
    приходит сам) печатает из неё число. Владельцу не нужно ничего вызывать.
    """

    def test_refusal_is_written_to_the_counter(self):
        store = _FakeStateTable()
        q = _FakeQuery("dictquicksave:opt-gone:0", text="не карточка",
                       sent_at=datetime.now(timezone.utc) - timedelta(minutes=4))
        calls = []
        with _table(store), \
             patch.object(bot_3, "record_dictionary_button_state_miss",
                          lambda **kw: calls.append(kw)):
            asyncio.run(bot_3.handle_dictionary_quick_save_callback(
                SimpleNamespace(callback_query=q), None,
            ))
        self.assertEqual(len(calls), 1, calls)
        self.assertEqual(calls[0]["button"], "quick_save")
        self.assertIs(calls[0]["recovered"], False)
        self.assertEqual(calls[0]["user_id"], 5)
        self.assertEqual(calls[0]["age_minutes"], 4)

    def test_unknown_age_is_none_not_zero(self):
        # Ноль означал бы «карточка отправлена только что» — то есть худший случай.
        store = _FakeStateTable()
        q = _FakeQuery("dictquicksave:opt-gone:0", text="не карточка")  # без даты
        calls = []
        with _table(store), \
             patch.object(bot_3, "record_dictionary_button_state_miss",
                          lambda **kw: calls.append(kw)):
            asyncio.run(bot_3.handle_dictionary_quick_save_callback(
                SimpleNamespace(callback_query=q), None,
            ))
        self.assertIsNone(calls[0]["age_minutes"])

    def test_broken_counter_does_not_break_the_button_but_shouts(self):
        store = _FakeStateTable()
        q = _FakeQuery("dictquicksave:opt-1:0", text=_CARD_TEXT)

        def _boom(**kw):
            raise RuntimeError("база недоступна")

        with _table(store):
            bot_3._put_dictionary_pending_state(
                "opt-1", bot_3.PENDING_INPUT_STATE_DICTIONARY_SAVE_OPTIONS, _PAYLOAD,
            )
            bot_3._drop_dictionary_pending_state("opt-1")  # состояния нет — будет промах
            with patch.object(bot_3, "record_dictionary_button_state_miss", _boom), \
                 patch.object(bot_3, "_resolve_private_dictionary_save_folder", return_value={}), \
                 patch.object(bot_3, "_save_dictionary_option_for_user",
                              return_value=(True, "ok", 1, True)), \
                 self.assertLogs(level=logging.WARNING) as logs:
                asyncio.run(bot_3.handle_dictionary_quick_save_callback(
                    SimpleNamespace(callback_query=q), None,
                ))
        # человек всё равно сохранил
        self.assertEqual(q.labels()[-1], "✅ Сохранено")
        # но недосчёт виден
        self.assertTrue([x for x in logs.output if "НЕ попал в счётчик" in x], logs.output)


class WeeklyReportBlockTests(unittest.TestCase):
    def test_clean_week_says_nobody_lost_a_word(self):
        from backend.admin_economics import format_dictionary_button_miss_block
        text = "\n".join(format_dictionary_button_miss_block({
            "days": 7, "refused": 0, "refused_fresh": 0, "refused_age_unknown": 0,
            "recovered": 12, "refused_users": 0, "by_button": [],
        }))
        self.assertIn("отказов «устарело»: 0", text)
        self.assertIn("ни один человек не потерял слово", text)
        self.assertIn("тихих восстановлений из текста карточки: 12", text)
        self.assertNotIn("⚠️", text)

    def test_fresh_refusals_are_called_a_hole(self):
        from backend.admin_economics import format_dictionary_button_miss_block
        text = "\n".join(format_dictionary_button_miss_block({
            "days": 7, "refused": 9, "refused_fresh": 4, "refused_age_unknown": 1,
            "recovered": 0, "refused_users": 3,
            "by_button": [{"button": "quick_save", "count": 7},
                          {"button": "folder_new", "count": 2}],
        }))
        self.assertIn("отказов «устарело»: 9 (людей: 3)", text)
        self.assertIn("моложе суток): 4", text)
        self.assertIn("возраст карточки неизвестен: 1", text)
        self.assertIn("Сохранить 1 / 2 / оба: 7", text)   # человеческое имя кнопки
        self.assertIn("новая папка: 2", text)
        self.assertIn("⚠️", text)


if __name__ == "__main__":
    unittest.main()
