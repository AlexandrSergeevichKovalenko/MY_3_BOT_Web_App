# -*- coding: utf-8 -*-
"""Письмо об обещаниях — ОДНО в утро и только про то, где нужен человек.

ПОВОД, 16.09.2026. Утром владельцу пришло пять сообщений подряд: отчёт и четыре письма
«⛔ Обещание нарушено». Дословно: «я вообще не понимаю зачем я это получаю... я не понимаю
как с ними обращаться, для чего они нужны... какой результат я должен из этого сделать».

Разбор того утра: из четырёх нарушенных обещаний действий требовало ОДНО. Два числа сами
ехали к нулю ночными прогонами (78 → 68 за сутки), третье было починено накануне и ждало
первого пересчёта. Письма при этом были одинаково тревожные и одинаково инженерные —
в каждом строка «Перемерить: python3 scripts/...», написанная для агента, а не для него.

Тесты держат ровно это:
  • «догоняет само» отличается от «стоит» по ЖУРНАЛУ замеров, а не по пометке в коде;
  • письмо уходит одно и только за тех, кто стоит, вырос или чью историю не прочитали;
  • у каждого обещания в общем письме своя кнопка «снять», и снятие одного не трогает
    остальные;
  • инженерного «как перемерить» в письме владельцу нет.
"""
import datetime
import pathlib
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[2]
BOT = ROOT / "bot_3.py"

ВЧЕРА = datetime.date.today() - datetime.timedelta(days=1)
ПОЗАВЧЕРА = datetime.date.today() - datetime.timedelta(days=2)


def _реестр():
    from backend.fix_promises import Promise
    return (
        Promise("stuck", "Строк в списке доступа, за которыми нет человека",
                "14.09.2026", 0, lambda: 2, "как перемерить: python3 scripts/xxx.py"),
        Promise("catching", "Слов, чьи примеры ещё не проверены",
                "15.09.2026", 0, lambda: 68, "как перемерить: /recheck_examples"),
        Promise("grew", "Слов с потерянным родом",
                "15.09.2026", 0, lambda: 9, "как перемерить: sql"),
        Promise("fresh", "Слотов, которые не ушли ни одному человеку",
                "16.09.2026", 0, lambda: 1, "как перемерить: sql"),
        Promise("held", "Всё хорошо", "10.09.2026", 0, lambda: 0, "sql"),
    )


ДВИЖЕНИЕ = {
    "stuck":    [(ПОЗАВЧЕРА, 2), (ВЧЕРА, 2)],      # стоит на месте
    "catching": [(ПОЗАВЧЕРА, 88), (ВЧЕРА, 78)],    # едет к нулю
    "grew":     [(ПОЗАВЧЕРА, 4), (ВЧЕРА, 5)],      # отдаляется
    # "fresh" истории не имеет — первое утро после починки
}


def _итог():
    from backend.fix_promises import check_all
    return check_all(record=False, promises=_реестр(), muted=set(), trends=ДВИЖЕНИЕ)


class ДвижениеЧислаБерётсяИзЖурнала(unittest.TestCase):
    def test_trend_is_measured_against_the_promised_number(self):
        from backend.fix_promises import (trend_of, CATCHING_UP, STUCK, GREW, FIRST_MORNING)
        self.assertEqual(CATCHING_UP, trend_of([88, 78], expected=0))
        self.assertEqual(STUCK, trend_of([2, 2], expected=0))
        self.assertEqual(GREW, trend_of([4, 5], expected=0))
        self.assertEqual(FIRST_MORNING, trend_of([2], expected=0))
        # Обещано может быть и не ноль: там рост — это приближение, а не ухудшение.
        self.assertEqual(CATCHING_UP, trend_of([0, 1], expected=1))

    def test_unreadable_history_is_not_silently_no_history(self):
        """Не прочитали журнал — это отдельный исход, и он ведёт к письму, а не к тишине."""
        from unittest import mock
        from backend.fix_promises import classify, BROKEN, TREND_UNKNOWN, needs_owner
        строка = {"key": "x", "status": BROKEN, "value": 5, "expected": 0}
        with mock.patch("backend.fix_promises.history", side_effect=RuntimeError("база молчит")):
            classify([строка])
        self.assertEqual(TREND_UNKNOWN, строка["trend"])
        self.assertIn("база молчит", строка["trend_error"])
        self.assertTrue(needs_owner(строка), "ослепли — значит зовём человека, а не молчим")

    def test_who_needs_the_owner(self):
        from backend.fix_promises import needs_owner
        по_ключу = {r["key"]: r for r in _итог()}
        self.assertTrue(needs_owner(по_ключу["stuck"]))
        self.assertTrue(needs_owner(по_ключу["grew"]))
        self.assertFalse(needs_owner(по_ключу["catching"]), "ночь уже везёт его к нулю")
        self.assertFalse(needs_owner(по_ключу["fresh"]), "первое утро после починки законно")
        self.assertFalse(needs_owner(по_ключу["held"]))


class ОтчётДелитНарушенноеНаДвеКучи(unittest.TestCase):
    def test_report_separates_waiting_from_self_catching(self):
        from backend.fix_promises import report_lines
        текст = "\n".join(report_lines(_итог()))
        self.assertIn("<b>2</b> ждут тебя", текст)
        self.assertIn("<b>2</b> идут сами", текст)
        self.assertIn("Идут сами, от тебя сегодня ничего не нужно", текст)
        # То, что едет к нулю, названо движением, а не одним числом.
        self.assertIn("вчера 78 → сегодня 68", текст)
        self.assertIn("не двигается: столько же, что и вчера", текст)
        self.assertIn("первое утро после починки", текст)


class ПисьмоОдноИБезИнженерногоТекста(unittest.TestCase):
    def test_one_letter_only_about_those_who_wait(self):
        from backend.fix_promises import digest_alert
        письмо = digest_alert(_итог())
        self.assertIsNotNone(письмо)
        текст, разметка = письмо
        self.assertIn("Ждут тебя: 2", текст)
        self.assertIn("Строк в списке доступа", текст)
        self.assertIn("Слов с потерянным родом", текст)
        self.assertNotIn("Слов, чьи примеры ещё не проверены", текст,
                         "то, что догоняет само, письма не порождает")
        self.assertNotIn("Слотов, которые не ушли", текст)

    def test_no_engineering_how_to_remeasure_in_the_owners_letter(self):
        from backend.fix_promises import digest_alert
        текст, _ = digest_alert(_итог())
        self.assertNotIn("как перемерить", текст)
        self.assertNotIn("python3", текст)
        self.assertNotIn("scripts/", текст)

    def test_every_promise_keeps_its_own_mute_button(self):
        from backend.fix_promises import digest_alert
        _, разметка = digest_alert(_итог())
        кнопки = [b["callback_data"] for ряд in разметка["inline_keyboard"] for b in ряд]
        self.assertEqual(["fp:mute:stuck", "fp:mute:grew", "fp:keep:all"], кнопки)

    def test_nothing_waiting_means_no_letter_at_all(self):
        from backend.fix_promises import Promise, check_all, digest_alert
        спокойно = (Promise("a", "x", "10.09.2026", 0, lambda: 0, "sql"),)
        self.assertIsNone(digest_alert(check_all(record=False, promises=спокойно, muted=set())))

    def test_unmeasured_still_reaches_the_owner(self):
        from unittest import mock
        from backend.fix_promises import Promise, check_all, digest_alert
        реестр = (Promise("c", "проверка сломана", "10.09.2026", 0,
                          mock.Mock(side_effect=RuntimeError("база молчит")), "sql"),)
        текст, разметка = digest_alert(check_all(record=False, promises=реестр, muted=set()))
        self.assertIn("Не удалось проверить: 1", текст)
        self.assertIn("база молчит", текст)
        self.assertIn("fp:mute:c", [b["callback_data"] for ряд in разметка["inline_keyboard"]
                                    for b in ряд])


class УтроШлётОдноПисьмоИЗаписываетЭто(unittest.TestCase):
    def test_sender_builds_one_digest_and_logs_it(self):
        src = BOT.read_text(encoding="utf-8")
        начало = src.index("def _send_fix_promise_alerts")
        блок = src[начало:src.index("\ndef ", начало + 10)]
        self.assertIn("digest_alert(", блок)
        self.assertIn("record_alert(", блок)
        self.assertNotIn("for r in results", блок, "письмо на каждое обещание не возвращаем")

    def test_muting_one_does_not_wipe_the_rest(self):
        src = BOT.read_text(encoding="utf-8")
        начало = src.index("async def handle_fix_promise_callback")
        блок = src[начало:src.index("\nasync def ", начало + 10)]
        self.assertIn('action == "keep" and key == "all"', блок,
                      "кнопка «Держать все» из общего письма должна обрабатываться")
        self.assertIn("остаток", блок)
        self.assertIn("текст +", блок, "старый текст письма сохраняется, а не затирается")

    def test_promise_is_registered(self):
        from backend.fix_promises import by_key
        p = by_key("one_promise_letter_per_morning")
        self.assertIsNotNone(p, "починка без зарегистрированного обещания не считается сделанной")
        self.assertEqual(0, p.expected)
        self.assertIsNotNone(p.screen, "экран «после» обязан приходить сам")


if __name__ == "__main__":
    unittest.main()
