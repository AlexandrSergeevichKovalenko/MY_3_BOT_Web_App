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

        Расход до двух записей в день: обычный слот 18:30 плюс капельная выдача.
        """
        self.assertGreater(bot_3.LISTENING_POOL_TARGET, bot_3.LISTENING_COOLDOWN_DAYS * 2)


if __name__ == "__main__":
    unittest.main()
