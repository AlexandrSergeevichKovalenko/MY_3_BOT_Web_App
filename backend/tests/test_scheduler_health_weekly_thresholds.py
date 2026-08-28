"""Недельное задание не имеет права нести дневной порог свежести.

Повод (28.08.2026): у «Стендап — отчёт о состоянии пула (вс 11:00)» порог стоял 10 ч
при cron'е раз в неделю. Отчёт уходил исправно (heartbeat completed, sent=1), но
вечерняя проверка 158 часов из 168 показывала его как «⚠️ ПРОТУХЛО». Ложная тревога
того же ранга, что и молчание: владелец перестаёт верить проверке.

Правило: если ярлык задания называет ОДИН день недели как всю его периодичность —
порог обязан покрывать неделю (168 ч) с запасом. Задания на несколько дней в неделю
(«вт и пт») правилом не проверяются: у них разрыв меньше недели, и они считают порог
по своему максимальному разрыву.
"""
import re
import unittest

import bot_3

_WEEKDAY = re.compile(r"(?<![А-Яа-яЁё])(Пн|Вт|Ср|Чт|Пт|Сб|Вс|пн|вт|ср|чт|пт|сб|вс)(?![А-Яа-яЁё])")

WEEK_HOURS = 168


class SchedulerHealthWeeklyThresholdTests(unittest.TestCase):
    def test_weekly_jobs_carry_at_least_a_week_of_slack(self):
        checked = 0
        for job_key, label, max_age_h, _default_on, _source in bot_3._SCHEDULER_HEALTH_CATALOG:
            days = set(_WEEKDAY.findall(label))
            if len(days) != 1:
                continue  # ежедневные, «вт и пт» и прочие — не про это правило
            checked += 1
            self.assertGreaterEqual(
                max_age_h, WEEK_HOURS,
                msg=(f"{job_key} ({label}) идёт раз в неделю, а протухает через "
                     f"{max_age_h} ч — вечерняя проверка будет ложно кричать "
                     f"«ПРОТУХЛО» почти всю неделю"),
            )
        self.assertGreater(checked, 0, "в каталоге не нашлось ни одного недельного задания — "
                                       "проверь, не изменился ли формат ярлыков")


if __name__ == "__main__":
    unittest.main()
