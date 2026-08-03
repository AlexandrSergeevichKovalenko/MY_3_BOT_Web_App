"""Капельная выдача: дневная норма уходит целиком, а типы каждый день разные.

Замер по проду 03.08.2026, аккаунт со «своими часами» 06:00–09:00 и 18:00–22:30:
шесть дней подряд приходил ОДИН И ТОТ ЖЕ набор из шести заданий в одно и то же время,
хотя по тарифу положено восемь.

Две причины, обе чинятся здесь:

1. Шаг между заданиями брался один раз (длина окна ÷ норма) и отмерялся от предыдущей
   карточки. Любой пропуск — перезапуск, пустой пул, доступ включился среди дня —
   сдвигал всю цепочку вправо, и хвост нормы не помещался до закрытия окна.
   Теперь план на «сейчас» считается от прожитой части окна и не зависит от истории
   отправок, поэтому пропуск догоняется сам.

2. Типы брались неизменным списком, в котором обязательная база стоит первой. Обязательных
   ровно столько же, сколько влезало заданий, — до хвоста списка очередь не доходила
   никогда. Теперь порядок берётся из той же ротации дня, что и у всех остальных.
"""

import unittest

import bot_3


# Утро 06:00–09:00 и вечер 18:00–22:30 — то самое расписание с прода.
MORNING_EVENING = {
    "weekday": [[6 * 60, 9 * 60], [18 * 60, 22 * 60 + 30]],
    "weekend": [[6 * 60, 9 * 60], [18 * 60, 22 * 60 + 30]],
}
TZ = "Europe/Berlin"
BUDGET = 8  # пресет «редко»


def _due(now_min: int, *, budget: int = BUDGET, schedule=MORNING_EVENING) -> int:
    return bot_3._drip_due_by_now(schedule, TZ, budget, now_min=now_min)


def _run_whole_day(schedule, budget: int, *, from_min: int = 0) -> list:
    """Прогон дня по заходам капли: во сколько ушла каждая карточка.
    `from_min` — с какой минуты человек вообще стал получать (например, доступ
    включился среди дня)."""
    spans = bot_3._window_spans_today(schedule, TZ)
    limit = bot_3._drip_per_tick_limit(schedule, TZ, budget)
    sent: list = []
    for minute in range(0, 24 * 60, bot_3.DRIP_TICK_MINUTES):
        if minute < from_min or not any(s <= minute < e for s, e in spans):
            continue
        due = min(bot_3._drip_due_by_now(schedule, TZ, budget, now_min=minute), budget)
        for _ in range(min(due - len(sent), limit)):
            sent.append(minute)
    return sent


class DripPacingTests(unittest.TestCase):
    def test_whole_budget_fits_into_the_chosen_hours(self):
        """К закрытию последнего окна должна быть выдана вся дневная норма."""
        self.assertEqual(_due(22 * 60 + 29), BUDGET,
                         "к концу выбранных часов норма дня обязана уйти целиком")

    def test_nothing_before_the_window_opens(self):
        self.assertEqual(_due(5 * 60 + 59), 0, "до открытия окна писать нельзя")

    def test_first_task_right_at_the_open(self):
        self.assertEqual(_due(6 * 60), 1, "с первой минуты окна человек ждёт задание")

    def test_pace_is_even_across_both_windows(self):
        """План растёт равномерно и никогда не обгоняет норму."""
        seen = [_due(m) for m in range(6 * 60, 22 * 60 + 30)]
        self.assertEqual(seen, sorted(seen), "план не может уменьшаться в течение дня")
        self.assertLessEqual(max(seen), BUDGET, "план не может превысить дневную норму")
        # Утро — 180 минут из 450, то есть 8 × 180/450 = 3.2 → к 09:00 должно быть 4.
        self.assertEqual(_due(8 * 60 + 59), 4, "утреннее окно должно давать свою долю")

    def test_quiet_hours_do_not_shrink_the_chosen_window(self):
        """Человек выставил 06:00 — значит выдача идёт с 06:00, а не с конца тишины.

        Раньше общее «молчим до 07:30» съедало полтора часа утреннего окна, но шаг
        считался по полным трём часам — хвост нормы не помещался.
        """
        self.assertGreaterEqual(_due(7 * 60), 2,
                                "к 07:00 внутри выбранных часов уже должно быть выдано два")

    def test_late_start_is_caught_up_the_same_day(self):
        """Доступ включился в 08:00 (награда за серию) — норма всё равно уходит за день."""
        self.assertEqual(_due(8 * 60), 3, "к 08:00 план уже требует три задания")
        self.assertEqual(_due(22 * 60 + 29), BUDGET, "остаток догоняется до закрытия окна")

    def test_narrow_window_compresses_the_step(self):
        """Узкое окно + большая норма: шаг сжимается, а не теряет задания."""
        narrow = {"weekday": [[19 * 60, 21 * 60]], "weekend": [[19 * 60, 21 * 60]]}
        self.assertEqual(bot_3._drip_due_by_now(narrow, TZ, 12, now_min=20 * 60 + 59), 12,
                         "в два часа обязаны уложиться все двенадцать")


class DripWholeDayTests(unittest.TestCase):
    """Главная проверка: прогон дня по заходам должен отдать ВСЮ норму.

    Считать план по минутам мало — выдаёт капля не поминутно, а раз в четверть часа,
    и последние минуты окна ни на один заход не приходятся. Пока план дотягивался
    ровно до закрытия, последнее задание за день так и не уходило.
    """

    def test_wide_windows_deliver_everything(self):
        sent = _run_whole_day(MORNING_EVENING, BUDGET)
        self.assertEqual(len(sent), BUDGET,
                         f"за день ушло {len(sent)} из {BUDGET}: {sent}")

    def test_late_access_still_delivers_everything(self):
        """Доступ включился в 08:00 — как сегодня по награде за серию."""
        sent = _run_whole_day(MORNING_EVENING, BUDGET, from_min=8 * 60)
        self.assertEqual(len(sent), BUDGET,
                         f"после позднего старта ушло {len(sent)} из {BUDGET}: {sent}")

    def test_narrow_window_delivers_everything(self):
        narrow = {"weekday": [[19 * 60, 21 * 60]], "weekend": [[19 * 60, 21 * 60]]}
        sent = _run_whole_day(narrow, 12)
        self.assertEqual(len(sent), 12, f"за два часа ушло {len(sent)} из 12: {sent}")

    def test_catch_up_does_not_dump_a_pile(self):
        """Догон не должен валить пачку: за один заход — не больше трёх карточек."""
        sent = _run_whole_day(MORNING_EVENING, BUDGET, from_min=8 * 60)
        biggest = max(sent.count(minute) for minute in set(sent))
        self.assertLessEqual(biggest, bot_3.DRIP_MAX_PER_TICK,
                             f"за один заход ушло {biggest} карточек подряд")

    def test_single_morning_window(self):
        morning = {"weekday": [[6 * 60, 9 * 60]], "weekend": [[6 * 60, 9 * 60]]}
        sent = _run_whole_day(morning, 6)
        self.assertEqual(len(sent), 6, f"за утро ушло {len(sent)} из 6: {sent}")

    def test_no_schedule_means_no_drip(self):
        self.assertEqual(bot_3._drip_due_by_now(None, TZ, BUDGET, now_min=12 * 60), 0)

    def test_empty_day_means_no_drip(self):
        blank = {"weekday": [], "weekend": []}
        self.assertEqual(bot_3._drip_due_by_now(blank, TZ, BUDGET, now_min=12 * 60), 0)


class DripKindRotationTests(unittest.TestCase):
    def test_mandatory_base_comes_first(self):
        """Обязательные типы забирают начало очереди — как и в обычной рассылке."""
        order = bot_3._drip_kind_order_today()
        head = order[:len(bot_3.MANDATORY_DELIVERY_KINDS)]
        self.assertTrue(set(head).issubset(bot_3.MANDATORY_DELIVERY_KINDS),
                        f"в начале очереди должна стоять обязательная база, а стоит {head}")

    def test_only_kinds_the_drip_can_actually_send(self):
        order = bot_3._drip_kind_order_today()
        self.assertTrue(set(order).issubset(bot_3._DRIP_CAPABLE_KINDS),
                        "в очередь попал тип, который капля отправить не умеет")

    def test_no_duplicates(self):
        order = bot_3._drip_kind_order_today()
        self.assertEqual(len(order), len(set(order)), "тип не должен встречаться дважды")

    def test_tail_changes_from_day_to_day(self):
        """Хвост очереди — вращение дня, а не вечный список.

        Проверяем по календарю: за две недели хвост обязан принять больше одного вида.
        """
        import datetime as _dt

        tails = set()
        base = _dt.datetime(2026, 8, 3, 12, 0)
        for day in range(14):
            moment = base + _dt.timedelta(days=day)
            bot_3._rotation_active_cache = None
            bot_3._tiered_rank_cache = None
            order = bot_3._drip_kind_order_today(moment)
            tails.add(tuple(order[len(bot_3.MANDATORY_DELIVERY_KINDS):]))
        bot_3._rotation_active_cache = None
        bot_3._tiered_rank_cache = None
        self.assertGreater(len(tails), 1,
                           "хвост очереди одинаков во все дни — вращения нет")

    def test_kinds_that_were_unreachable_are_now_in_the_list(self):
        """Тренировка, спринт-квиз и артикль-квиз раньше до этих людей не доходили."""
        for kind in ("trainer", "article_quiz", "mc"):
            self.assertIn(kind, bot_3._DRIP_CAPABLE_KINDS,
                          f"«{kind}» по-прежнему недоступен человеку со своими часами")


if __name__ == "__main__":
    unittest.main()
