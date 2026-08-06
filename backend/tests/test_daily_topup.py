"""Вечерний добор: человек получает столько заданий, сколько выбрал.

Замер по проду 05.08.2026. «Редко» со своими часами (капля) дал ровно 8 из 8 и восемь
разных типов — там правило «сперва то, чего сегодня не было» уже работает. А слотовая
рассылка обещание не сдержала: «обычно» 11 из 12 при семи разных типах, «интенсив»
18 из 20 при одиннадцати, у бесплатных 6 из 6, но кроссворд дважды.

Слотовому пути это не по силам: если у слота пуст пул, место дня теряется — догонять
некому; а тип с двумя слотами в сутках занимает два места из нормы, пока типы, не
попавшие в сегодняшнюю ротацию, не приходят вовсе.

Добор вечером доводит человека до нормы, беря готовое правило капли. По одному
заданию за заход, чтобы недобор не свалился пачкой.
"""

import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import bot_3


FREE_UID = 572603263      # бесплатный, норма 6
PRO_UID = 7263482531      # «обычно», норма 12, своих часов нет
WINDOWED_UID = 883565092  # «редко» со своими часами — его ведёт капля
OWN_HOURS = {"weekday": [[6 * 60, 9 * 60]], "weekend": [[6 * 60, 9 * 60]]}


class DailyTopupTests(unittest.IsolatedAsyncioTestCase):
    async def _run(self, *, uid: int, is_pro: bool, preset: str, delivered_today: int,
                   schedule=None, active: bool = True, drip_on: bool = True):
        """Прогон добора для одного человека. Возвращает список выданных (uid, счётчик)."""
        sent: list = []

        async def _deliver(context, user_id, delivered_idx, now, *, held=False):
            sent.append((int(user_id), int(delivered_idx)))
            return True

        prefs = {uid: {"preset": preset, "schedule": schedule, "tz_name": "Europe/Berlin"}}
        with patch.object(bot_3, "_collect_scheduler_candidate_user_ids",
                          AsyncMock(return_value=[uid] if active else [])), \
             patch.object(bot_3, "get_user_prefs_bulk", Mock(return_value=prefs)), \
             patch.object(bot_3, "_is_user_pro_cached", Mock(return_value=is_pro)), \
             patch.object(bot_3, "_drip_delivery_enabled", Mock(return_value=drip_on)), \
             patch.object(bot_3, "_is_quiet_hours_now", Mock(return_value=False)), \
             patch.object(bot_3, "get_inbox_delivery_stats_today",
                          Mock(return_value=(delivered_today, None))), \
             patch.object(bot_3, "_drip_deliver_one", AsyncMock(side_effect=_deliver)):
            await bot_3._daily_topup_job(SimpleNamespace())
        return sent

    async def test_pro_below_budget_gets_topped_up(self):
        """«Обычно» — 12 в день. Пришло 11 → добор досылает недостающее."""
        sent = await self._run(uid=PRO_UID, is_pro=True, preset="normal", delivered_today=11)
        self.assertEqual(sent, [(PRO_UID, 11)],
                         "человеку с нормой 12 не дослали двенадцатое задание")

    async def test_free_below_budget_gets_topped_up(self):
        sent = await self._run(uid=FREE_UID, is_pro=False, preset="normal", delivered_today=4)
        self.assertEqual(sent, [(FREE_UID, 4)], "бесплатному с нормой 6 не дослали")

    async def test_nobody_is_topped_up_above_the_budget(self):
        """Норма выбрана человеком — добор её не превышает."""
        sent = await self._run(uid=PRO_UID, is_pro=True, preset="normal", delivered_today=12)
        self.assertEqual(sent, [], "добор выдал сверх выбранной нормы")

    async def test_one_task_per_run(self):
        """Недобор в несколько штук расходится по вечеру, а не падает пачкой."""
        sent = await self._run(uid=PRO_UID, is_pro=True, preset="normal", delivered_today=3)
        self.assertEqual(len(sent), 1, f"за один заход ушло {len(sent)} заданий вместо одного")

    async def test_own_hours_user_is_left_to_the_drip(self):
        """У кого свои часы — тем занимается капля, со своим счётом и темпом."""
        sent = await self._run(uid=WINDOWED_UID, is_pro=True, preset="rare",
                               delivered_today=2, schedule=OWN_HOURS)
        self.assertEqual(sent, [], "добор влез к человеку, которого ведёт капля")

    async def test_own_hours_but_free_is_served_by_topup(self):
        """У бесплатного часы не действуют — его ведут слоты, значит и добор его."""
        sent = await self._run(uid=FREE_UID, is_pro=False, preset="rare",
                               delivered_today=2, schedule=OWN_HOURS)
        self.assertEqual(sent, [(FREE_UID, 2)],
                         "бесплатный с остаточным расписанием выпал из добора")

    async def test_silence_is_respected(self):
        """«Тишина» — норма 0: добор молчит вместе с рассылкой."""
        sent = await self._run(uid=PRO_UID, is_pro=True, preset="silent", delivered_today=0)
        self.assertEqual(sent, [], "добор написал тому, кто просил тишины")

    async def test_quiet_hours_stop_the_topup(self):
        with patch.object(bot_3, "_is_quiet_hours_now", Mock(return_value=True)), \
             patch.object(bot_3, "_drip_deliver_one", AsyncMock()) as deliver:
            await bot_3._daily_topup_job(SimpleNamespace())
        deliver.assert_not_awaited()

    def test_topup_finishes_before_the_silence(self):
        """Последний заход добора обязан быть до начала тишины."""
        self.assertLess(bot_3.DAILY_TOPUP_END_HOUR, bot_3.QUIET_HOURS_START[0],
                        "добор заходит в часы тишины")


if __name__ == "__main__":
    unittest.main()
