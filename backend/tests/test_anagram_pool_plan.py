"""Ночной добор анаграмм обязан смотреть на запас ЧЕЛОВЕКА, а не на размер банка.

Живой случай 13.09.2026: в банке 51 неснятая карточка при цели 12 — добор не делал
ничего и не сделал бы ещё месяц. При этом у самого активного человека оставалось 33
непоказанных карточки, то есть 16 дней: выдача показывает только то, чего он не видел.
Общий счётчик такое не видит в принципе.

Владелец: «Мне нужно, чтобы автоматически закрывался пробел в заданиях. Автоматически».
"""
import unittest

from backend.anagram_pool_plan import (
    CAP_PER_RUN, MIN_RUNWAY_DAYS, refill_target, runway_days,
)

СЛОТОВ_В_ДЕНЬ = 2          # ANAGRAM_SLOT_TIMES: 12:15 и 19:15
ПРЕЖНЯЯ_ЦЕЛЬ = 12          # ANAGRAM_POOL_TARGET


class ДоборПросыпаетсяОтЗапасаЧеловека(unittest.TestCase):
    def test_полный_банк_но_человеку_осталось_мало(self):
        """Тот самый случай: банк полон, а человек в двух неделях от пустоты."""
        цель = refill_target(have=51, weakest_unseen=20, slots_per_day=СЛОТОВ_В_ДЕНЬ,
                             pool_target=ПРЕЖНЯЯ_ЦЕЛЬ)
        self.assertEqual(цель, 51 + (MIN_RUNWAY_DAYS * СЛОТОВ_В_ДЕНЬ - 20))
        self.assertGreater(цель, 51, "добор обязан проснуться, а не спать на полном банке")

    def test_запаса_хватает_добор_молчит(self):
        """33 непоказанных при пороге 28 — работы нет, лишних карточек не делаем."""
        цель = refill_target(have=51, weakest_unseen=33, slots_per_day=СЛОТОВ_В_ДЕНЬ,
                             pool_target=ПРЕЖНЯЯ_ЦЕЛЬ)
        self.assertEqual(цель, 51)

    def test_за_один_прогон_не_больше_потолка(self):
        """Даже если человек на нуле, ночь не делает сотню карточек разом."""
        цель = refill_target(have=51, weakest_unseen=0, slots_per_day=СЛОТОВ_В_ДЕНЬ,
                             pool_target=ПРЕЖНЯЯ_ЦЕЛЬ)
        self.assertEqual(цель, 51 + CAP_PER_RUN)

    def test_прежняя_цель_остаётся_нижней_границей(self):
        """Пустой банк у нового бота добирается до 12, как и раньше."""
        цель = refill_target(have=0, weakest_unseen=None, slots_per_day=СЛОТОВ_В_ДЕНЬ,
                             pool_target=ПРЕЖНЯЯ_ЦЕЛЬ)
        self.assertEqual(цель, ПРЕЖНЯЯ_ЦЕЛЬ)

    def test_нет_активных_людей_это_не_нулевой_запас(self):
        """«Некому считать запас» и «запас кончился» — разные вещи, и путать их нельзя:
        иначе бот без людей каждую ночь делал бы карточки в пустоту."""
        без_людей = refill_target(have=51, weakest_unseen=None, slots_per_day=СЛОТОВ_В_ДЕНЬ,
                                  pool_target=ПРЕЖНЯЯ_ЦЕЛЬ)
        на_нуле = refill_target(have=51, weakest_unseen=0, slots_per_day=СЛОТОВ_В_ДЕНЬ,
                                pool_target=ПРЕЖНЯЯ_ЦЕЛЬ)
        self.assertEqual(без_людей, ПРЕЖНЯЯ_ЦЕЛЬ)
        self.assertEqual(на_нуле, 51 + CAP_PER_RUN)

    def test_запас_в_днях_считается_от_расписания(self):
        self.assertEqual(runway_days(33, СЛОТОВ_В_ДЕНЬ), 16.5)
        self.assertIsNone(runway_days(None, СЛОТОВ_В_ДЕНЬ))


if __name__ == "__main__":
    unittest.main()
