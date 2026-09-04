# -*- coding: utf-8 -*-
"""Реестр обещаний: «готово» без зарегистрированного обещания не готово.

ПОВОД, 04.09.2026. Владелец: «это уже 20-е подряд задание, которое ты говоришь, что
исправил, но нихера не исправил. Как проверять тебя в моменте?». Ответ — реестр: каждая
починка оставляет измеримое обещание, система проверяет его сама каждое утро, нарушенное
приходит владельцу с кнопками. Тесты держат три вещи:
  • реестр устроен так, что его нельзя заполнить криво (ключи уникальны, числа — числа);
  • у проверки ТРИ исхода, и «не измерено» никогда не выдаётся за «держится»;
  • первый повод — карантин старого банка — снесён, а не «закрыт замком».
"""
import pathlib
import unittest
from unittest import mock

ROOT = pathlib.Path(__file__).resolve().parents[2]
DB = ROOT / "backend" / "database.py"
BOT = ROOT / "bot_3.py"
SERVER = ROOT / "backend" / "backend_server.py"


class РеестрУстроенЧестно(unittest.TestCase):
    def test_keys_are_unique_and_values_are_numbers(self):
        from backend.fix_promises import PROMISES
        ключи = [p.key for p in PROMISES]
        self.assertEqual(len(ключи), len(set(ключи)), "два обещания с одним ключом")
        for p in PROMISES:
            self.assertTrue(p.key and p.title and p.since and p.how, p)
            self.assertIsInstance(p.expected, int)
            self.assertTrue(callable(p.measure))
            self.assertRegex(p.since, r"^\d{2}\.\d{2}\.\d{4}$", "дата обещания — ДД.ММ.ГГГГ")

    def test_first_promise_is_the_reason_the_registry_exists(self):
        from backend.fix_promises import by_key
        self.assertIsNotNone(by_key("old_bank_quarantine_traces"))
        self.assertEqual(0, by_key("old_bank_quarantine_traces").expected)


class ТриИсходаПроверки(unittest.TestCase):
    def _реестр(self):
        from backend.fix_promises import Promise
        return (
            Promise("a", "держится", "04.09.2026", 0, lambda: 0, "x"),
            Promise("b", "нарушено", "04.09.2026", 0, lambda: 3, "x"),
            Promise("c", "не измерено", "04.09.2026", 1,
                    mock.Mock(side_effect=RuntimeError("база молчит")), "x"),
            Promise("d", "снято", "04.09.2026", 0, lambda: 99, "x"),
        )

    def test_statuses_are_distinct_and_muted_is_skipped(self):
        from backend.fix_promises import check_all, HELD, BROKEN, UNMEASURED
        итог = check_all(record=False, promises=self._реестр(), muted={"d"})
        по_ключу = {r["key"]: r for r in итог}
        self.assertEqual(HELD, по_ключу["a"]["status"])
        self.assertEqual(BROKEN, по_ключу["b"]["status"])
        self.assertEqual(3, по_ключу["b"]["value"])
        self.assertEqual(UNMEASURED, по_ключу["c"]["status"])
        self.assertIn("база молчит", по_ключу["c"]["error"])
        self.assertNotIn("d", по_ключу, "снятое владельцем не проверяется и не показывается")

    def test_report_line_names_broken_and_unmeasured_but_not_held(self):
        from backend.fix_promises import check_all, report_lines
        строки = report_lines(check_all(record=False, promises=self._реестр(), muted={"d"}))
        self.assertIn("<b>1</b> держится", строки[0])
        self.assertIn("<b>1</b> нарушено", строки[0])
        self.assertIn("<b>1</b> не измерено", строки[0])
        текст = "\n".join(строки)
        self.assertIn("⛔ нарушено: обещано <b>0</b>, сейчас <b>3</b>", текст)
        self.assertIn("❓ не измерено", текст)
        self.assertNotIn("⛔ держится", текст)

    def test_all_held_is_one_calm_line(self):
        from backend.fix_promises import Promise, check_all, report_lines
        реестр = (Promise("a", "x", "04.09.2026", 0, lambda: 0, "x"),
                  Promise("b", "y", "04.09.2026", 5, lambda: 5, "x"))
        строки = report_lines(check_all(record=False, promises=реестр, muted=set()))
        self.assertEqual(["🤝 Обещания: <b>2</b> держатся."], строки)

    def test_alert_has_two_buttons_and_silence_keeps_the_promise(self):
        from backend.fix_promises import broken_alert, BROKEN
        text, markup = broken_alert({"key": "b", "title": "t", "since": "04.09.2026",
                                     "expected": 0, "value": 3, "how": "sql",
                                     "status": BROKEN, "error": ""})
        кнопки = [b["callback_data"] for row in markup["inline_keyboard"] for b in row]
        self.assertEqual(["fp:keep:b", "fp:mute:b"], кнопки)
        self.assertIn("Ничего не нажать — тоже ответ", text)


class ОбещаниеВшитоВУтро(unittest.TestCase):
    def test_morning_report_carries_the_block_and_the_alerts(self):
        src = BOT.read_text(encoding="utf-8")
        начало = src.index("def _send_pool_enrich_morning_report")
        блок = src[начало:src.index("\ndef ", начало + 10)]
        self.assertIn("_check_fix_promises()", блок)
        self.assertIn("_fix_promises_block(", блок)
        self.assertIn("_send_fix_promise_alerts(", блок)

    def test_buttons_have_a_handler(self):
        src = BOT.read_text(encoding="utf-8")
        self.assertIn('pattern=r"^fp:"', src)
        self.assertIn('CommandHandler("admin_promises"', src)


class КарантинСтарогоБанкаСнесён(unittest.TestCase):
    """Первый повод для реестра. Не «дверь закрыта» — писать метку больше нечем."""

    def test_nothing_writes_the_old_mark(self):
        for path in (DB, SERVER, BOT):
            src = path.read_text(encoding="utf-8")
            self.assertNotIn("'enrich_attempts',\n", src.replace("jsonb_build_object(\n", ""),
                             f"{path.name}: кто-то снова пишет enrich_attempts")
            self.assertNotIn("def mark_pool_entry_enrich_failed", src)

    def test_screen_command_and_weekly_mail_are_gone(self):
        src = BOT.read_text(encoding="utf-8")
        self.assertNotIn('CommandHandler("admin_pool_quarantine"', src)
        self.assertNotIn('pattern=r"^qz:"', src)
        self.assertNotIn("_send_quarantine_review_weekly", src)
        self.assertNotIn("В карантине:", src)

    def test_thin_queue_no_longer_hides_words_behind_a_counter(self):
        src = DB.read_text(encoding="utf-8")
        начало = src.index("def get_thin_pool_entries_for_enrichment")
        блок = src[начало:начало + 4000]
        self.assertNotIn("enrich_attempts", блок)


if __name__ == "__main__":
    unittest.main()


class КарточкаСловаНовостиДняСобранаВНовомВиде(unittest.TestCase):
    """Обещание 04.09.2026: карточка слова «Новость дня» переведена на наш стиль.
    Измеритель читает собранный CSS; здесь он проверяется на образцах, а не на сборке."""

    def test_old_look_is_counted_and_new_look_is_zero(self):
        from backend.fix_promises import _count_worldnews_old_look_rules
        старый = (".worldnews-step{flex:1 1 0;clip-path:polygon(0 0,100% 50%)}"
                  ".worldnews-card-de{font-family:Georgia,serif;font-size:30px}")
        новый = (".worldnews-step{flex:1 1 0;border-radius:10px}"
                 ".worldnews-step.is-active{background:#fff}"
                 ".worldnews-card-de{font-family:-apple-system,system-ui;font-size:34px}")
        self.assertEqual(2, _count_worldnews_old_look_rules(старый))
        self.assertEqual(0, _count_worldnews_old_look_rules(новый))

    def test_wrong_file_is_unmeasured_not_zero(self):
        from backend.fix_promises import _count_worldnews_old_look_rules
        with self.assertRaises(LookupError):
            _count_worldnews_old_look_rules("body{margin:0}")

    def test_promise_is_registered(self):
        from backend.fix_promises import by_key
        self.assertEqual(0, by_key("worldnews_card_old_look").expected)
