"""Вечерний отчёт «получили ли бесплатные свои шесть» должен говорить правду.

Отчёт нужен ровно для одного решения: доходят ли до бесплатного обещанные шесть
заданий. Значит он обязан (а) брать план оттуда же, откуда его берёт рассылка,
(б) не путать бонусы (работа над ошибками, спринт, повтор тренировки) со счётными
заданиями, (в) называть, чего именно не хватило.
"""

import unittest
from datetime import date
from unittest.mock import Mock, patch

import backend.free_delivery_report as report
import bot_3


PLAN = [("adjektiv_sprint", 10, 0), ("numdict", 15, 10), ("trainer", 16, 0),
        ("wofrage_sprint", 16, 30), ("listening", 18, 30), ("artikel_sprint", 19, 0)]
FULL_DAY = {"ad": 1, "nd": 1, "tr": 1, "wf": 1, "ls": 1, "as": 1}
NAMES = {1: "Лилия", 2: "Олег", 3: "Мария"}


def _text(delivered, *, free=(1,), bonus=None):
    with patch.object(report, "_delivered_by_user", Mock(return_value=delivered)), \
         patch.object(report, "_bonus_recipients",
                      Mock(return_value=bonus or {"sprint": set(), "trainer_repeat": set()})), \
         patch.object(report, "_news_line", Mock(return_value="📰 Новость дня: ушла утром")), \
         patch("backend.database.get_user_display_names", Mock(return_value=NAMES)):
        return report.build_free_delivery_text(
            day=date(2026, 8, 2), tz_name="Europe/Vienna", plan=PLAN, budget=6,
            free_user_ids=free, repeat_slot_hours=(1300, 1800))


class ReportTextTests(unittest.TestCase):
    def test_everyone_got_six(self):
        txt = _text({1: dict(FULL_DAY), 2: dict(FULL_DAY)}, free=(1, 2))
        self.assertIn("Все свои 6 получили", txt)
        self.assertIn("✅ 6 из 6 — Лилия, Олег", txt)
        self.assertNotIn("⚠️", txt)

    def test_missing_task_is_named(self):
        short = {k: v for k, v in FULL_DAY.items() if k != "ls"}
        txt = _text({1: dict(FULL_DAY), 2: short}, free=(1, 2))
        self.assertIn("Недобрали 1 из 2", txt)
        self.assertIn("⚠️ 5 из 6 — Олег · не дошло: 🎧 Аудирование", txt)

    def test_six_arrived_but_not_the_planned_ones(self):
        """Случай 07.08: слот аудирования не отработал, вечерний добор закрыл дырку
        артикль-квизом. Норма выполнена — «недобрали» тут враньё, но и промолчать нельзя:
        человек не получил того, что стояло в плане."""
        swapped = {k: v for k, v in FULL_DAY.items() if k != "ls"}
        swapped["aq"] = 1
        txt = _text({1: dict(FULL_DAY), 2: swapped}, free=(1, 2))
        self.assertNotIn("Недобрали", txt)
        self.assertIn("Все свои 6 получили (2 человека).", txt)
        self.assertIn("Но у 1 человека пришло не то, что стояло в плане.", txt)
        self.assertIn("🔀 6 из 6 — Олег · не дошло: 🎧 Аудирование "
                      "· вместо этого: 🧩 Артикль-квиз", txt)

    def test_shortfall_and_swap_are_reported_separately(self):
        """Недобор по числу и подмена типа — разные беды, обе должны быть названы."""
        swapped = {k: v for k, v in FULL_DAY.items() if k != "ls"}
        swapped["aq"] = 1
        short = {k: v for k, v in FULL_DAY.items() if k not in ("ls", "as")}
        txt = _text({1: swapped, 2: short}, free=(1, 2))
        self.assertIn("Недобрали 1 из 2 человек.", txt)
        self.assertIn("Но у 1 человека пришло не то, что стояло в плане.", txt)
        self.assertIn("🔀 6 из 6 — Лилия", txt)
        self.assertIn("⚠️ 4 из 6 — Олег", txt)

    def test_bonus_does_not_count_as_a_daily_task(self):
        """Работа над ошибками приходит сверх шести: с ней «5 из 6» не станет «6 из 6»."""
        short = {k: v for k, v in FULL_DAY.items() if k != "ls"}
        short["rv"] = 1
        txt = _text({1: short})
        self.assertIn("⚠️ 5 из 6 — Лилия", txt)
        self.assertIn("работа над ошибками — 1 человек", txt)

    def test_long_absent_user_is_a_shortfall_not_an_excuse(self):
        """Раньше давно не заходившего отчёт выводил строкой «не пишем» и не считал
        недобором. Теперь задания идут всем, поэтому пустой день у такого человека —
        обычная поломка доставки, и отчёт обязан её назвать."""
        txt = _text({1: dict(FULL_DAY)}, free=(1, 3))
        self.assertIn("Недобрали 1 из 2", txt)
        self.assertIn("⛔ 0 из 6 — Мария · не дошло ничего", txt)
        self.assertNotIn("не пишем", txt)

    def test_nothing_arrived_says_so_plainly(self):
        txt = _text({}, free=(1,))
        self.assertIn("⛔ 0 из 6 — Лилия · не дошло ничего", txt)

    def test_sprint_zero_is_explained_not_alarming(self):
        txt = _text({1: dict(FULL_DAY)})
        self.assertIn("🏃 спринт — 0", txt)
        self.assertIn("прошёл тренировку", txt)


class SendBudgetTests(unittest.TestCase):
    """Дневной лимит не зависит от того, как давно человек заходил.

    Раньше бесплатному, который 21 день не появлялся, бюджет обнулялся — и он переставал
    получать ровно те задания, ради которых мог бы вернуться. Задания общие и лежат в
    пулах, лишний получатель не стоит ни токена, так что молчать было не за чем.
    Единственный ноль, который остался, — «Тишина», её человек выбирает сам."""

    def test_free_user_always_gets_the_free_budget(self):
        self.assertEqual(bot_3._user_send_budget(424242, is_pro=False), bot_3.FREE_SEND_BUDGET)

    def test_silence_still_means_silence(self):
        self.assertEqual(bot_3._user_send_budget(424242, is_pro=True, preset="silent"), 0)

    def test_paid_preset_is_untouched(self):
        self.assertEqual(bot_3._user_send_budget(424242, is_pro=True, preset="normal"),
                         bot_3.DEFAULT_PRO_SEND_BUDGET)


class PlanSourceTests(unittest.TestCase):
    def test_plan_is_the_same_list_the_delivery_uses(self):
        """План отчёта = первые FREE_SEND_BUDGET слотов очереди бесплатного тарифа;
        именно по ней рассылка решает, кому что слать."""
        plan = bot_3._free_plan_today()
        self.assertEqual(len(plan), bot_3.FREE_SEND_BUDGET)
        for kind, hour, minute in plan:
            self.assertLess(bot_3._tiered_slot_position(kind, hour, minute, free=True),
                            bot_3.FREE_SEND_BUDGET,
                            f"{kind} {hour:02d}:{minute:02d} в плане, но рассылка его не отдаст")

    def test_every_counted_kind_has_a_ledger_code(self):
        """Вид задания без кода в ведомости отчёт посчитать не сможет и тихо соврёт.
        Спринт исключён: он бонус и в ведомость не пишется вовсе."""
        kinds = {e.kind for e in bot_3._build_rotation_catalog()} - {"sprint"}
        missing = sorted(kinds - set(report.KIND_TO_INBOX_CODE))
        self.assertEqual(missing, [], f"нет кода ведомости для: {missing}")
        self.assertEqual(sorted(set(report.KIND_TO_INBOX_CODE) - kinds), [],
                         "в таблице кодов остался вид, которого больше нет в расписании")


if __name__ == "__main__":
    unittest.main()
