"""Каждое засчитываемое задание обязано попасть в ведомость дня.

Ведомость (`bt_3_interactive_inbox`) — единственное место, по которому кнопка
«Следующее задание» понимает, сколько из дневного лимита человек уже получил.
Отправитель, который карточку послал, а строку не записал, делает слот «как будто
неизрасходованным»: 02.08.2026 бесплатный аккаунт получил шесть заданий, а в
ведомости их было пять — тренировка синонимов/антонимов и артикль-квиз строку не
писали, и кнопка была готова выдать седьмое сверх лимита.

Правило: счётная выдача (make_rotation_gated) пишется всегда, бонусная
(make_bonus_gated: повтор тренировки, спринт по рельсу) — никогда, иначе подарок
съест слот из дневных шести. Группа — не человек, для неё ведомости нет.
"""

import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import backend.interactive_card as interactive_card
import bot_3


UID = 8546091375
GROUP_ID = -1001234567890


def _context(message_id: int = 4242):
    bot = SimpleNamespace(
        send_photo=AsyncMock(return_value=SimpleNamespace(message_id=message_id)),
        send_message=AsyncMock(return_value=SimpleNamespace(message_id=message_id)),
    )
    return SimpleNamespace(bot=bot)


class TrainerLedgerTests(unittest.IsolatedAsyncioTestCase):
    async def _send(self, *, chat_id: int, repeat: bool):
        recorder = Mock()
        with patch.object(bot_3, "create_trainer_dispatch", Mock(return_value=777)), \
             patch.object(bot_3, "update_trainer_dispatch_message_id", Mock()), \
             patch.object(bot_3, "record_interactive_inbox", recorder), \
             patch.object(bot_3, "_append_free_pro_teaser", Mock(side_effect=lambda c, _cid: c)), \
             patch.object(interactive_card, "render_trainer_relation_card", Mock(return_value=None)):
            ok = await bot_3.send_trainer_to_chat(
                _context(), entry={"sprint_id": "s-1", "wort": "aufhören", "hint_ru": ""},
                relation="antonym", slot_date="2026-08-02", slot_hour=1600,
                chat_id=chat_id, target_user_id=UID, repeat=repeat)
        self.assertTrue(ok, "карточка тренировки не ушла — тест ниже проверять нечего")
        return recorder

    async def test_first_pass_lands_in_the_ledger(self):
        """Первая тренировка — одно из дневных заданий, её обязаны записать."""
        recorder = await self._send(chat_id=UID, repeat=False)
        self.assertEqual(recorder.call_count, 1,
                         "первую тренировку не записали в ведомость дня")
        self.assertEqual(recorder.call_args.kwargs["kind"], "tr")
        self.assertEqual(recorder.call_args.kwargs["deeplink"], "ans_tr_777")

    async def test_repeat_stays_out_of_the_ledger(self):
        """Повтор на следующий день — подарок сверх плана, слот тратить не должен."""
        recorder = await self._send(chat_id=UID, repeat=True)
        recorder.assert_not_called()

    async def test_group_send_is_not_a_personal_task(self):
        recorder = await self._send(chat_id=GROUP_ID, repeat=False)
        recorder.assert_not_called()


class ArticleQuizLedgerTests(unittest.IsolatedAsyncioTestCase):
    async def _send(self, *, chat_id: int):
        recorder = Mock()
        with patch.object(bot_3, "record_article_quiz_dispatch", Mock(return_value=555)), \
             patch.object(bot_3, "update_article_quiz_dispatch_telegram_id", Mock()), \
             patch.object(bot_3, "record_interactive_inbox", recorder), \
             patch.object(bot_3, "_append_free_pro_teaser", Mock(side_effect=lambda c, _cid: c)):
            ok = await bot_3.send_article_quiz_to_chat(
                _context(), entry={"word_id": "w-1", "word": "Haus", "meaning_ru": "дом"},
                image_url="https://example.invalid/x.jpg", slot_date="2026-08-02",
                slot_hour=1315, chat_id=chat_id, target_user_id=UID)
        self.assertTrue(ok, "карточка артикль-квиза не ушла")
        return recorder

    async def test_quiz_lands_in_the_ledger_without_a_link(self):
        """Считается в дневной лимит, но открывать в приложении нечего: отвечают
        кнопками в чате, значит ссылки у строки нет."""
        recorder = await self._send(chat_id=UID)
        self.assertEqual(recorder.call_count, 1,
                         "артикль-квиз не записали в ведомость дня")
        self.assertEqual(recorder.call_args.kwargs["kind"], "aq")
        self.assertEqual(recorder.call_args.kwargs["deeplink"], "")

    async def test_group_send_is_not_a_personal_task(self):
        recorder = await self._send(chat_id=GROUP_ID)
        recorder.assert_not_called()


if __name__ == "__main__":
    unittest.main()
