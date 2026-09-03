# -*- coding: utf-8 -*-
"""ЧИСЛО В ПИСЬМЕ И ЧИСЛО НА ЭКРАНЕ — ОДНО И ТО ЖЕ ИЛИ НАЗВАНО РАЗНО.

ПОВОД, 03.09.2026. Владелец: «Ты пишешь в сообщении осталось 55, а тут число смотри
какое!» В ночном письме стояло «Осталось тебе: 55» и кнопка «Разобрать (55)», а экран
за той же кнопкой показывал «осталось 178».

Оба числа были настоящие и считали РАЗНОЕ:
  55  — сколько из 60 взятых за ночь ГРАММАТИЧЕСКИХ споров ночь не смогла применить
        сама (третий судья рассудил, но там дописаны слова либо наша проверка правку
        не пропустила);
  178 — сколько вопросов ВСЕГО ждёт владельца на экране: 58 грамматических + 70 о
        карточках + 50 о переводах (замер живой базы 03.09.2026).

Слово «осталось» читается как «всего осталось». Здесь заперто, что настоящий остаток
берётся из того же источника, что и экран.
"""
import pathlib
import unittest


def _src(rel: str) -> str:
    return (pathlib.Path(__file__).resolve().parents[2] / rel).read_text(encoding="utf-8")


def _тело_письма() -> str:
    bot = _src("bot_3.py")
    i = bot.index("def _send_settled_disputes_report(")
    return bot[i:bot.index("\ndef ", i + 1)]


def _код_письма() -> str:
    """Только КОД, без пояснительной рамки: в ней прежние формулировки цитируются
    нарочно — как запись о том, что было сломано."""
    тело = _тело_письма()
    конец_рассказа = тело.index('"""', тело.index('"""') + 3) + 3
    return тело[конец_рассказа:]


class ПисьмоИЭкранСчитаютОдноTests(unittest.TestCase):

    def test_the_remaining_number_comes_from_the_screens_own_source(self):
        код = _код_письма()
        self.assertIn("count_open_phrase_reviews", код,
                      "остаток снова считается своим способом, а не так же, как экран")

    def test_the_nightly_batch_number_is_never_called_the_remainder(self):
        """«оставлено владельцу» — остаток НОЧНОЙ ПОРЦИИ, а не очереди владельца."""
        код = _код_письма()
        # Число ночной порции больше не подписывается словом «осталось».
        self.assertNotIn("Осталось тебе", код)
        i = код.index('"оставлено владельцу"')
        имя = код[код.rindex("\n", 0, i):i]
        self.assertIn("не_смогли", имя,
                      "число ночной порции снова названо остатком")

    def test_the_button_carries_the_same_number_as_the_screen(self):
        код = _код_письма()
        self.assertIn("Разобрать ({ждёт_решения})", код)
        self.assertNotIn("Разобрать ({осталось})", код)

    def test_an_unreadable_count_shows_no_number_at_all(self):
        """Выдуманное число хуже отсутствующего: не прочитали — пишем без числа."""
        код = _код_письма()
        self.assertIn("ждёт_решения = -1", код)
        self.assertIn('"Разобрать" if ждёт_решения < 0', код)
        self.assertIn("if ждёт_решения > 0:", код,
                      "строка с остатком печатается даже когда его не прочитали")

    def test_the_screen_and_the_counter_look_at_the_same_rows(self):
        """`count_open_phrase_reviews` обязан считать ровно то, что показывает экран:
        всё открытое, кроме очереди АВТОРОВ фраз."""
        src = _src("backend/database.py")
        i = src.index("def count_open_phrase_reviews(")
        счётчик = src[i:src.index("\ndef ", i + 1)]
        j = src.index("def list_open_phrase_reviews(")
        экран = src[j:src.index("\ndef ", j + 1)]
        for кусок in ("status = 'open'", "<> 'personal'"):
            self.assertIn(кусок, счётчик)
        self.assertIn("r.status = 'open'", экран)
        self.assertIn("<> 'personal'", экран)


if __name__ == "__main__":
    unittest.main()
