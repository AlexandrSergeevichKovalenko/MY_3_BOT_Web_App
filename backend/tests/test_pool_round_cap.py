"""Правило трёх кругов: задание снимается, даже если толпа не набралась.

Обычная ротация ждёт, пока задание разберёт минимум несколько человек. Пока в боте
один активный человек, порог не берётся НИКОГДА — и он получает одно и то же снова и
снова (на проде 30.07.2026 записи аудирования отработали по 11–12 кругов, отвечал один).
Правило трёх кругов — второй выход: отработало свои круги — уходит из ротации, сколько
бы человек ни ответило. Справились — насовсем, нет (или никто не открыл) — в отстой.
"""

import unittest
from unittest.mock import Mock, patch

import backend.database as db
import bot_3


class RoundCapTests(unittest.IsolatedAsyncioTestCase):
    async def _run(self, domain, rounds, ratios):
        retired_calls, parked_calls = [], []

        def _retire(_d, ids, reason):
            retired_calls.append((list(ids), reason))
            return len(ids)

        def _park(_d, ids, *, days):
            parked_calls.append((list(ids), days))
            return len(ids)

        with patch.object(db, "pool_item_rounds", Mock(return_value=rounds)), \
             patch.object(db, "retire_pool_items_with_reason", Mock(side_effect=_retire)), \
             patch.object(db, "park_pool_items", Mock(side_effect=_park)):
            total = await bot_3._rotate_by_round_cap(domain, ratios)
        return total, retired_calls, parked_calls

    async def test_mastered_item_is_dropped_for_good(self):
        """Отвечал один человек, но справился — задание своё отработало."""
        total, retired, parked = await self._run("listening", {"a": 3}, {"a": 1.0})
        self.assertEqual(total, 1)
        self.assertEqual(retired, [(["a"], "rounds_passed")])
        self.assertEqual(parked, [([], bot_3.POOL_PARK_DAYS)])

    async def test_failed_item_goes_to_the_parking_lot(self):
        """Не справились — не выбрасываем, вернём, когда забудется."""
        total, retired, parked = await self._run("listening", {"b": 3}, {"b": 0.0})
        self.assertEqual(total, 1)
        self.assertEqual(retired, [([], "rounds_passed")])
        self.assertEqual(parked, [(["b"], bot_3.POOL_PARK_DAYS)])

    async def test_nobody_opened_it_also_goes_to_the_parking_lot(self):
        """Ответов нет вообще — усвоено оно или нет, мы не знаем. Значит не удаляем."""
        total, retired, parked = await self._run("anagram", {"c": 4}, {})
        self.assertEqual(total, 1)
        self.assertEqual(parked, [(["c"], bot_3.POOL_PARK_DAYS)])

    async def test_ratio_is_looked_up_when_the_caller_does_not_supply_it(self):
        """Анаграмма отмечает ответ «верно/неверно» — долю берём из базы сами."""
        with patch.object(db, "pool_item_rounds", Mock(return_value={"d": 3})), \
             patch.object(db, "pool_item_pass_ratio", Mock(return_value={"d": 1.0})) as ratio, \
             patch.object(db, "retire_pool_items_with_reason", Mock(return_value=1)), \
             patch.object(db, "park_pool_items", Mock(return_value=0)):
            total = await bot_3._rotate_by_round_cap("anagram")
        self.assertEqual(total, 1)
        ratio.assert_called_once()

    async def test_domains_outside_the_scope_are_untouched(self):
        """Ребус, кроссворд и словник артиклей живут по-старому."""
        for domain in ("rebus", "crossword", "article_quiz"):
            total, retired, parked = await self._run(domain, {"x": 99}, {"x": 0.0})
            self.assertEqual((total, retired, parked), (0, [], []), domain)


class RoundCapSettingsTests(unittest.TestCase):
    def test_a_round_is_at_least_a_week_of_life(self):
        """Круги разносит кулдаун: три круга аудирования — это минимум три недели,
        а не три подряд идущих дня. Если кулдаун обнулят, правило станет мясорубкой."""
        self.assertGreaterEqual(bot_3.LISTENING_COOLDOWN_DAYS, 5)
        self.assertGreaterEqual(bot_3.POOL_MAX_ROUNDS, 1)

    def test_parking_is_long_enough_to_forget(self):
        self.assertGreaterEqual(bot_3.POOL_PARK_DAYS, 30)


if __name__ == "__main__":
    unittest.main()
