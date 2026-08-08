"""Бесплатный человек со «своим расписанием» не должен пропадать из рассылки.

Расписание — фича полного доступа: часы выбирает только оплаченный, и капельную
выдачу (_drip_delivery_job) получает тоже только он. Но строка расписания остаётся
в базе после приветственного триала — человек настроил часы, пока доступ был, потом
доступ кончился. Если такого считать «с окном», обычная рассылка его выкинет («его
обслужит капельная»), а капельная не возьмёт («он не оплачен») — и он не получит
за день НИ ОДНОГО задания. Ровно это и случилось на проде 30.07.2026 у трёх из
шести бесплатных аккаунтов.

Правило: окно существует только для оплаченного. Для бесплатного — обычная
рассылка по слотам и его дневной лимит.
"""

import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import bot_3


FREE_UID = 8546091375
PAID_UID = 883565092
# Часы, в которые прямо сейчас точно НЕ попадаем ни при каком часовом поясе:
# пустой список окон на оба типа дня = «никогда».
NEVER_OPEN = {"weekday": [], "weekend": []}


class FreeUserStaleScheduleDeliveryTests(unittest.IsolatedAsyncioTestCase):
    async def _collect(self, *, uid: int, is_paid: bool, schedule):
        """Прогоняет коллектор рассылки для одного получателя в личку."""
        prefs = {uid: {"preset": "rare", "schedule": schedule, "tz_name": "Europe/Vienna"}}
        token = bot_3._current_scheduled_send.set(("listening", 18, 30))
        try:
            with patch.object(bot_3, "_collect_scheduler_candidate_user_ids",
                              AsyncMock(return_value=[uid])), \
                 patch.object(bot_3, "_build_user_delivery_map",
                              AsyncMock(return_value={uid: uid})), \
                 patch.object(bot_3, "_is_quiz_delivery_target_suppressed", Mock(return_value=False)), \
                 patch.object(bot_3, "_is_user_pro_cached", Mock(return_value=is_paid)), \
                 patch.object(bot_3, "get_user_prefs_bulk", Mock(return_value=prefs)), \
                 patch.object(bot_3, "_drip_delivery_enabled", Mock(return_value=True)), \
                 patch.object(bot_3, "_tier_delivery_enabled", Mock(return_value=True)):
                return await bot_3._collect_quiz_delivery_user_targets(SimpleNamespace())
        finally:
            bot_3._current_scheduled_send.reset(token)

    async def test_free_user_with_leftover_schedule_still_gets_the_slot(self):
        """Слот аудирования — первый в очереди бесплатного, он обязан дойти."""
        targets = await self._collect(uid=FREE_UID, is_paid=False, schedule=NEVER_OPEN)
        self.assertEqual([t["chat_id"] for t in targets], [FREE_UID],
                         "бесплатного с остаточным расписанием выкинули из рассылки")

    async def test_paid_user_with_schedule_is_left_to_the_drip(self):
        """У оплаченного часы работают: обычная рассылка его не трогает."""
        targets = await self._collect(uid=PAID_UID, is_paid=True, schedule=NEVER_OPEN)
        self.assertEqual(targets, [],
                         "оплаченного с окном должна обслуживать только капельная выдача")


class ListeningPoolCoversCooldownTests(unittest.TestCase):
    def test_bank_is_bigger_than_cooldown_can_freeze(self):
        """Банк меньше, чем кулдаун × расход, = слот молча встаёт на несколько дней.

        Потребителей банка ТРИ: слот 18:30, капельная выдача и вечерний добор. Замер
        07.08.2026: уходит по 3 записи в день, банк считался на 2 — и слот встал.
        """
        self.assertGreater(bot_3.LISTENING_POOL_TARGET, bot_3.LISTENING_COOLDOWN_DAYS * 3)


class ArtikelLearnMarksTheLedgerTests(unittest.IsolatedAsyncioTestCase):
    """Слот, который занимает место в счётном плане дня, обязан отметиться в ведомости.

    08.08.2026: Artikel Trainer 08:00 стоял в плане бесплатного, карточка уходила
    человеку — но записи в `bt_3_interactive_inbox` не появлялось. Для счёта задания
    не существовало: вечерний добор дослал лишнее, а отчёт писал «не дошло» про
    доставленную карточку.
    """

    async def test_slot_records_the_inbox_entry(self):
        inbox = Mock()
        bot = SimpleNamespace(
            send_message=AsyncMock(return_value=SimpleNamespace(message_id=555)),
            send_photo=AsyncMock(return_value=SimpleNamespace(message_id=555)),
        )
        with patch.object(bot_3, "_artikel_sprint_enabled", Mock(return_value=True)), \
             patch.object(bot_3, "_is_quiet_hours_now", Mock(return_value=False)), \
             patch.object(bot_3, "get_daily_article_sprint_set_id", Mock(return_value="set-1")), \
             patch.object(bot_3, "get_article_sprint_set", Mock(return_value={"theme_key": ""})), \
             patch.object(bot_3, "_backfill_artikel_audio_for_set", AsyncMock(return_value=0)), \
             patch("backend.article_learn.ensure_daily_learn_mnemonics", Mock(return_value=0)), \
             patch("backend.database.record_artikel_nightly_metrics", Mock()), \
             patch("backend.interactive_card.render_artikel_learn_card", Mock(return_value=None)), \
             patch.object(bot_3, "_collect_quiz_delivery_user_targets",
                          AsyncMock(return_value=[{"chat_id": FREE_UID}])), \
             patch.object(bot_3, "create_article_sprint_dispatch", Mock(return_value=42)), \
             patch.object(bot_3, "update_article_sprint_dispatch_message_id", Mock()), \
             patch.object(bot_3, "record_interactive_inbox", inbox):
            await bot_3._send_scheduled_artikel_learn(SimpleNamespace(bot=bot))

        self.assertTrue(inbox.called, "слот отправил карточку, но в ведомости её нет")
        self.assertEqual(inbox.call_args.kwargs.get("kind"), "al")
        self.assertEqual(int(inbox.call_args.kwargs.get("user_id")), FREE_UID)


if __name__ == "__main__":
    unittest.main()
