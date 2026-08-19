"""Ночной дозаказ: включается сам, когда запас проседает, и молчит, когда всё цело.

Владелец 15.08.2026 спросил прямо: «а ты тестами проверил, что пополнение включится
само?» Здесь это и проверяется — не расчёт в вакууме, а сама ночная работа: кого она
зовёт, с какой целью и когда не зовёт никого.
"""

import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import bot_3


def _run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


def _row(**kw):
    base = {"kind": "rb", "title": "Ребусы", "bank_total": 338, "people": 2,
            "per_day": 1.0, "blocked_deepest": 0, "available": 338,
            "supply_days": 338.0, "order_now": 0}
    base.update(kw)
    return base


class NightlyTopupTests(unittest.IsolatedAsyncioTestCase):
    async def _run_job(self, rows):
        ctx = MagicMock()
        ctx.bot.send_message = AsyncMock()
        with patch("backend.database.measure_all_task_supply", return_value=rows), \
             patch.object(bot_3, "get_admin_telegram_ids", return_value=[1]), \
             patch.object(bot_3, "_run_task_supply_topup",
                          new=AsyncMock(return_value="Ребусы: заказано 25")) as topup:
            await bot_3._task_supply_watch_job(ctx)
        return topup, ctx.bot.send_message

    async def test_healthy_supply_orders_nothing_and_stays_silent(self):
        """13 неглубоких людей: ночью не трогаем модель и не будим владельца."""
        topup, send = await self._run_job([_row()])
        topup.assert_not_awaited()
        send.assert_not_awaited()

    async def test_thin_supply_triggers_the_order_by_itself(self):
        topup, send = await self._run_job([_row(available=10, supply_days=10.0,
                                                order_now=20)])
        topup.assert_awaited_once()
        item = topup.await_args[0][0]
        self.assertEqual(item["kind"], "rb")
        self.assertEqual(item["tonight"], 20)
        self.assertEqual(item["target_ready"], 338 + 20,
                         "пополнялка наполняет банк ДО цели, а не делает N сверх")
        send.assert_awaited()

    async def test_headline_matches_the_letter(self):
        """Шапка «⚠️ Заканчиваются задания» — только когда правда заканчиваются.
        При запасе 29 дней это обычный ночной дозаказ, а не тревога, и пустой
        строки под тревожным заголовком быть не должно."""
        _, send = await self._run_job([_row(available=61, supply_days=29.0,
                                            order_now=2)])
        text = send.await_args.kwargs["text"]
        self.assertIn("Ночной дозаказ", text)
        self.assertNotIn("Заканчиваются задания", text)

        _, send = await self._run_job([_row(available=3, supply_days=3.0,
                                            order_now=25)])
        text = send.await_args.kwargs["text"]
        self.assertIn("Заканчиваются задания", text)

    async def test_broken_measurement_orders_nothing(self):
        topup, _ = await self._run_job([{"kind": "cw", "error": "замер не удался"}])
        topup.assert_not_awaited()

    async def test_a_failing_measurement_never_breaks_the_night(self):
        ctx = MagicMock()
        ctx.bot.send_message = AsyncMock()
        with patch("backend.database.measure_all_task_supply",
                   side_effect=RuntimeError):
            await bot_3._task_supply_watch_job(ctx)


class TopupDispatchTests(unittest.IsolatedAsyncioTestCase):
    async def test_pool_tasks_are_reported_as_not_ordered_not_silently_skipped(self):
        """Пул наполняется по форматам своей работой — молча подменять её нельзя,
        но и промолчать про пропуск тоже нельзя."""
        out = await bot_3._run_task_supply_topup(
            {"kind": "au", "title": "Задания пула", "tonight": 5, "target_ready": 50})
        self.assertIn("не заказано", out)

    async def test_listening_is_actually_ordered(self):
        """15.08.2026: аудирование — единственный вид, которому заказ реально нужен
        (банк 23 при расходе 0.87 в сутки), и оно единственное не было подключено."""
        with patch("backend.listening_generator.prepare_listening_pool",
                   return_value={"attempted": 3, "succeeded": 3, "failed": 0,
                                 "skipped": 0, "needed": 3}) as gen:
            out = await bot_3._run_task_supply_topup(
                {"kind": "ls", "title": "Аудирование", "tonight": 3, "target_ready": 26})
        gen.assert_called_once()
        self.assertEqual(gen.call_args.kwargs["target_ready"], 26)
        self.assertIn("сделано 3", out)

    async def test_report_tells_the_fact_not_the_plan(self):
        """19.08.2026: отчёт четвёртое утро подряд писал «Кроссворды: заказано 2»,
        а рождался один — вторую попытку отклоняла приёмка слов. Намерение и
        результат в отчёте владельцу обязаны отличаться."""
        with patch("backend.crossword_generator.prepare_crossword_pool",
                   return_value={"attempted": 2, "succeeded": 1, "failed": 1,
                                 "needed": 2, "skipped": 0, "retired": 0,
                                 "re_render": 0, "rendered": 0, "render_failed": 0,
                                 "reasons": ["загаданы неходовые слова (TASTEN)"]}):
            out = await bot_3._run_task_supply_topup(
                {"kind": "cw", "title": "Кроссворды", "tonight": 2, "target_ready": 62})
        self.assertIn("сделано 1 из 2", out)
        self.assertIn("не вышло 1", out)
        self.assertIn("TASTEN", out, "причина отказа обязана дойти до владельца")

    async def test_anagram_break_is_not_reported_as_success(self):
        """Дубль слова обрывал цикл, а строка рапортовала полный заказ: в ночь на
        19.08 отчёт сказал «Анаграммы: заказано 1» при нуле новых карточек."""
        with patch.object(bot_3, "_ensure_anagram_card",
                          new=AsyncMock(return_value=None)):
            out = await bot_3._run_task_supply_topup(
                {"kind": "ag", "title": "Анаграммы", "tonight": 2, "target_ready": 62})
        self.assertIn("сделано 0 из 2", out)
        self.assertNotIn("заказано 2", out)

    async def test_full_order_reads_plainly(self):
        """Когда всё вышло — строка короткая, без «из N» и без причин."""
        with patch.object(bot_3, "_ensure_anagram_card",
                          new=AsyncMock(return_value={"card_id": "x"})):
            out = await bot_3._run_task_supply_topup(
                {"kind": "ag", "title": "Анаграммы", "tonight": 2, "target_ready": 62})
        self.assertEqual(out, "Анаграммы: сделано 2")

    async def test_failure_is_reported_not_swallowed(self):
        with patch("backend.rebus_generator.prepare_rebus_pool",
                   side_effect=RuntimeError):
            out = await bot_3._run_task_supply_topup(
                {"kind": "rb", "title": "Ребусы", "tonight": 5, "target_ready": 50})
        self.assertIn("НЕ ПРОШЁЛ", out)


if __name__ == "__main__":
    unittest.main()
