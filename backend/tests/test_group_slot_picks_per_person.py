"""Общий слот подбирает каждому своё, а не одну карточку на всех.

Разбор с владельцем 20.08.2026. Слоты в 11:45 и 17:45 брали ОДИН кроссворд и рассылали
его всем подряд, не спрашивая, кто его уже решал. Отсюда случай, который владелец назвал
глупым: решил кроссворд — и он же приходит снова.

Экономии в «одном на всех» нет. Слот и так шлёт каждому человеку отдельное сообщение,
картинка уже нарисована, а подбор личной карточки стоит 0.1 мс по индексу (замер на
боевой базе 20.08.2026). Зато общая карточка расходуется сразу на всех: банк из 61
кроссворда при двух слотах в день проходится группой за месяц. При личном подборе тот
же кроссворд обслуживает одного человека в августе, другого в октябре, новичка в декабре.

Отдельно проверяется главное правило: человеку, у которого свежего не осталось, НЕ
подсовывается решённое. Мы честно никого не отправляем, считаем это и говорим владельцу
— чинить надо банк, а не выдачу.
"""

import unittest
from datetime import datetime
from unittest.mock import AsyncMock, patch

import bot_3


class GroupSlotTests(unittest.IsolatedAsyncioTestCase):
    async def _run_slot(self, *, blocked_by_user: dict, bank: list):
        """Слот с двумя получателями и банком `bank` (список номеров кроссвордов)."""
        sent = []

        def _pick(*, exclude_ids=None, **kw):
            skip = set(exclude_ids or ())
            for cid in bank:
                if cid not in skip:
                    return {"crossword_id": cid, "image_object_key": f"key/{cid}"}
            return None

        async def _blocked(uid, kind):
            return blocked_by_user.get(int(uid), [])

        async def _send(context, *, crossword_entry, image_url, slot_date, slot_hour,
                        chat_id, target_user_id, held):
            sent.append((int(chat_id), str(crossword_entry["crossword_id"])))
            return True

        alerts = AsyncMock()
        with patch.object(bot_3, "_is_quiet_hours_now", return_value=False), \
             patch.object(bot_3, "_crosswords_enabled", return_value=True), \
             patch.object(bot_3, "_is_crossword_slot", return_value=True), \
             patch.object(bot_3, "_get_quiz_schedule_now",
                          return_value=datetime(2026, 8, 20, 11, 45)), \
             patch.object(bot_3, "_collect_quiz_delivery_user_targets",
                          new=AsyncMock(return_value=[{"chat_id": 1}, {"chat_id": 2}])), \
             patch.object(bot_3, "_drip_blocked_ids", new=_blocked), \
             patch.object(bot_3, "pick_next_crossword", new=_pick), \
             patch.object(bot_3, "r2_public_url",
                          side_effect=lambda k, *, version="": f"https://x/{k}"
                                                               + (f"?v={version}" if version else "")), \
             patch.object(bot_3, "send_crossword_to_chat", new=_send), \
             patch.object(bot_3, "mark_crossword_sent", return_value=None), \
             patch.object(bot_3, "mark_crossword_send_failed", return_value=None), \
             patch.object(bot_3, "_alert_admin_interactive", new=alerts):
            await bot_3._send_scheduled_crossword(None)
        return sent, alerts

    async def test_person_who_solved_it_gets_a_different_one(self):
        """Ровно случай владельца: первому кроссворд №1 закрыт — он получает №2,
        а не тот, который уже решил."""
        sent, _ = await self._run_slot(blocked_by_user={1: ["c1"]}, bank=["c1", "c2"])
        self.assertEqual(dict(sent), {1: "c2", 2: "c1"})

    async def test_everyone_still_gets_a_task(self):
        """Пропускать человека только потому, что общая карточка ему закрыта, нельзя —
        он получает своё из того же банка."""
        sent, _ = await self._run_slot(blocked_by_user={}, bank=["c1", "c2"])
        self.assertEqual(len(sent), 2)

    async def test_solved_task_is_never_resent_even_when_bank_is_empty(self):
        """Банк исчерпан лично для человека: повтор решённого запрещён, подмена другой
        игрой тоже. Никого не отправляем и говорим владельцу."""
        sent, alerts = await self._run_slot(blocked_by_user={1: ["c1"], 2: ["c1"]},
                                            bank=["c1"])
        self.assertEqual(sent, [], "решённое не уходит по второму разу")
        alerts.assert_awaited()
        text = alerts.await_args[0][1]
        self.assertIn("нечего показать", text)
        self.assertIn("2 чел.", text, "владелец должен видеть, скольким не хватило")


if __name__ == "__main__":
    unittest.main()
