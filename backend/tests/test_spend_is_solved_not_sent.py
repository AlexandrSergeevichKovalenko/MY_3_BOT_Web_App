"""Расход банка — это РЕШЁННЫЕ задания, а не отправленные.

Решение владельца 24.08.2026, после разбора на живых данных:

  «Отправил в чат, а его никто не открыл — это не расход. Он его не решал и не знает,
   что внутри; этот же кроссворд может прийти ему завтра».

Замер, из которого это выросло (боевая база, 24.08.2026, слоты 11:45 и 17:45):
  • отправлено кроссвордов за 30 дней ...... 157
  • взялся хоть за одно слово ................ 15  (9,5 %)
  • девять человек из одиннадцати ............. 0 из 88
  • в банке 107 готовых, хоть кем-то решались . 22

Пока расход считался по отправкам, ночной дозаказ пополнял банк под тех, кто ничего не
открывал: цель гналась за рассылкой, а не за учёбой. Здесь проверяется, что замер берёт
сдачи, что отдых карточки снят вместе с ним, и что кроссворд уходит раз в день.
"""

import unittest
from unittest.mock import MagicMock, patch

import backend.database as db


class SpendBasisTests(unittest.TestCase):
    def test_crossword_and_anagram_count_solved_tasks(self):
        """Источник расхода — таблицы сдач, а не журнал отправок."""
        self.assertIn("bt_3_crossword_answers", db._SOLVED_CONSUMPTION_SQL["cw"])
        self.assertIn("bt_3_anagram_answers", db._SOLVED_CONSUMPTION_SQL["ag"])
        for sql in db._SOLVED_CONSUMPTION_SQL.values():
            self.assertNotIn("bt_3_interactive_inbox", sql,
                             "журнал отправок здесь считать нечего")
            self.assertIn("COUNT(DISTINCT dispatch_id)", sql,
                          "одна сдача = одно задание, даже если слов в нём четыре")

    def test_every_game_measures_solved_tasks(self):
        """Правило раскатано на все игры (решение владельца 24.08.2026): банк
        расходуют решения, а не рассылка. Вид, у которого нет своего запроса сдач,
        замер обязан уронить, а не молча посчитать по отправкам."""
        self.assertEqual(set(db._SOLVED_CONSUMPTION_SQL), set(db._TASK_BANKS))
        for kind, sql in db._SOLVED_CONSUMPTION_SQL.items():
            with self.subTest(kind=kind):
                self.assertNotIn("bt_3_interactive_inbox", sql)
                self.assertIn("COUNT(DISTINCT dispatch_id)", sql)

    def test_listening_counts_the_moment_the_answer_was_handed_in(self):
        """У аудирования проверка идёт отдельно и позже: израсходовано задание тогда,
        когда человек СДАЛ ответ, а не когда судья дочитал."""
        self.assertIn("submitted_at", db._SOLVED_CONSUMPTION_SQL["ls"])

    def test_free_pool_is_divided_by_sends_not_by_solves(self):
        """«Свободно прямо сейчас» — про рассылку. Карточку выедает ПОКАЗ: показали
        кому угодно — она выпала у всех на срок отдыха. Решают втрое реже, чем
        получают, и раздели эту строку на решения — она пообещает запас, которого нет.
        """
        from backend.task_supply_report import build_task_supply_report
        row = {"kind": "rb", "title": "Ребусы", "bank_total": 78, "available": 78,
               "per_day": 1.0, "per_day_measured": 0.83, "per_day_avg": 0.5,
               "supply_days": 78.0, "order_now": 0, "spend_basis": "solved",
               "free_now": 30, "cooldown_days": 15, "sent_per_day": 6.0,
               "sent_total_per_day": 12.0}
        text = build_task_supply_report([row])
        # 30 свободных при 6 отправках в сутки — это пять дней, а не тридцать.
        self.assertIn("это на 5 дн. рассылки", text)

    def test_sent_but_never_solved_is_not_called_unsent(self):
        """Две разные беды. «Не выдавали» чинится расписанием, «выдаём, а никто не
        открывает» — самим заданием. До 24.08.2026 обе писались одной строкой, и
        вторая читалась как «игра выключена»."""
        from backend.task_supply_report import build_task_supply_report
        ignored = {"kind": "ls", "title": "Аудирование", "bank_total": 68,
                   "available": 68, "per_day": 0.0, "supply_days": float("inf"),
                   "order_now": 0, "sent_total_per_day": 3.4}
        text = build_task_supply_report([ignored])
        self.assertIn("Отправляем, но никто не решает", text)
        self.assertIn("3.4/сутки", text)
        self.assertNotIn("Не выдавались", text)

        silent = dict(ignored, title="Ребусы", sent_total_per_day=0.0)
        text = build_task_supply_report([silent])
        self.assertIn("Не выдавались", text)
        self.assertNotIn("никто не решает", text)

    def test_report_says_which_number_it_shows(self):
        """«Решает» и «получает» — разные слова для разных чисел."""
        from backend.task_supply_report import build_task_supply_report
        row = {"kind": "cw", "title": "Кроссворды", "bank_total": 107, "available": 107,
               "per_day": 1.0, "per_day_measured": 0.83, "per_day_avg": 0.2,
               "supply_days": 107.0, "order_now": 0, "spend_basis": "solved"}
        text = build_task_supply_report([row])
        self.assertIn("решает самый активный", text)
        self.assertNotIn("отдыхают", text, "у кроссворда отдыха больше нет")

        sent_row = dict(row, kind="rb", title="Ребусы", spend_basis="sent",
                        free_now=10, cooldown_days=15)
        text = build_task_supply_report([sent_row])
        self.assertIn("получает самый активный", text)
        self.assertIn("отдыхают 15 дн.", text)


class NoSharedRestTests(unittest.TestCase):
    """Общий отдых карточки снят: он был замком, из-за которого банк приходилось
    растить от ЧИСЛА ЛЮДЕЙ (11 чел. × 14 дн. = 168 карточек, а на 2000 — 28 000)."""

    def test_crossword_and_anagram_have_no_cooldown_entry(self):
        from backend.task_cooldowns import COOLDOWN_DAYS_BY_KIND
        self.assertNotIn("cw", COOLDOWN_DAYS_BY_KIND)
        self.assertNotIn("ag", COOLDOWN_DAYS_BY_KIND)
        # У остальных игр отдых на месте — их этой правкой не трогали.
        self.assertIn("rb", COOLDOWN_DAYS_BY_KIND)
        self.assertIn("article_quiz", COOLDOWN_DAYS_BY_KIND)

    def test_the_measure_skips_the_free_now_line_when_there_is_no_rest(self):
        """Без отдыха строки «свободно прямо сейчас» в отчёте быть не должно —
        печатать её нулями значило бы показать отдых, которого нет."""
        cur = MagicMock()
        cur.fetchone.side_effect = [(107,), (107,)]
        cur.fetchall.return_value = []
        conn = MagicMock()
        conn.cursor.return_value.__enter__.return_value = cur
        ctx = MagicMock()
        ctx.__enter__.return_value = conn
        with patch.object(db, "get_db_connection_context", return_value=ctx), \
             patch.object(db, "ensure_task_rotation_schema"):
            out = db.measure_task_supply("cw")
        self.assertNotIn("free_now", out)
        self.assertNotIn("cooldown_days", out)
        self.assertEqual(out["spend_basis"], "solved")
        self.assertEqual(out["bank_total"], 107)


class OneCrosswordPerDayTests(unittest.TestCase):
    def test_crossword_goes_out_once_a_day(self):
        """Решение владельца 24.08.2026: было два слота (11:45 и 17:45), стал один.
        Кроссворд — единственная игра, чья карточка стоит денег (0,35 цента за штуку,
        замер по ведомости 24.08.2026) и почти не переиспользуется."""
        import bot_3
        self.assertEqual(bot_3.CROSSWORD_SLOT_TIMES, {(11, 45)})


if __name__ == "__main__":
    unittest.main()
