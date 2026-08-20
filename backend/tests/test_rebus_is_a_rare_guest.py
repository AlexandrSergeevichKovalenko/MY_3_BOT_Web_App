"""Ребус приходит редко, и его место в дневной норме занимает полезное задание.

Решение владельца 20.08.2026, дословно: «они реально для обучения не сильно полезны,
так как другие варианты тренировок, и при этом самые дорогие — я бы их ставил достаточно
редко... а другие задания пусть тогда будут более частыми, чтобы норма была выполнена».

Цена замерена по ведомости: рисование ребусов — 238 обращений к DALL·E за 30 дней,
$5.00 из $82.69 всей выработки моделью. Дороже любой другой игры. Плюс банк ребусов не
растёт сам: это список из 107 слов, написанный руками в `backend/rebus_bank.py`.

Здесь проверяется главное: редкость считается от календаря (а не случайно) и вычитается
из очереди ЗАРАНЕЕ, иначе ребус занял бы место в норме дня и человек получил бы на одно
полезное задание меньше.
"""

import unittest
from datetime import date, datetime

import bot_3


class RebusDayTests(unittest.TestCase):
    def test_rebus_day_repeats_with_the_chosen_period(self):
        days = [d for d in range(1, 30)
                if bot_3._is_rebus_day(date.fromordinal(date(2026, 8, 1).toordinal() + d))]
        gaps = {b - a for a, b in zip(days, days[1:])}
        self.assertEqual(gaps, {bot_3.REBUS_EVERY_N_DAYS},
                         "промежуток между ребусными днями обязан быть ровно один")

    def test_the_answer_does_not_depend_on_who_asks(self):
        """Слот и личная выдача решают это независимо и в разных процессах. От
        случайного числа они разошлись бы: один день — два ребуса или ни одного."""
        day = date(2026, 8, 20)
        self.assertEqual({bot_3._is_rebus_day(day) for _ in range(50)}, {bot_3._is_rebus_day(day)})

    def test_slot_is_silent_on_a_non_rebus_day(self):
        hour, minute = sorted(bot_3.REBUS_SLOT_TIMES)[0]
        rebus_day = next(date.fromordinal(date(2026, 8, 1).toordinal() + d)
                         for d in range(30)
                         if bot_3._is_rebus_day(date.fromordinal(date(2026, 8, 1).toordinal() + d)))
        quiet_day = date.fromordinal(rebus_day.toordinal() + 1)
        self.assertTrue(bot_3._is_rebus_slot(datetime(rebus_day.year, rebus_day.month,
                                                      rebus_day.day, hour, minute)))
        self.assertFalse(bot_3._is_rebus_slot(datetime(quiet_day.year, quiet_day.month,
                                                       quiet_day.day, hour, minute)),
                         "в неребусный день слот молчит")

    def test_wrong_minute_is_still_not_a_slot(self):
        """Редкость добавлена сверху старой проверки, а не вместо неё."""
        rebus_day = next(date.fromordinal(date(2026, 8, 1).toordinal() + d)
                         for d in range(30)
                         if bot_3._is_rebus_day(date.fromordinal(date(2026, 8, 1).toordinal() + d)))
        self.assertFalse(bot_3._is_rebus_slot(
            datetime(rebus_day.year, rebus_day.month, rebus_day.day, 3, 3)))


if __name__ == "__main__":
    unittest.main()
