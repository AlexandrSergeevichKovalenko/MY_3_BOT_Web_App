"""Наполнение темы останавливает прирост, а не заветная цифра.

Цель «150 слов» ничего не решает: в «Чувствах» ходовых слов навалом, в «Вечеринках» их
физически нет. Неприкосновенная цифра заставляла генератор скрести дно — так в банк попали
«центрифуга для салата» и «тёрка для муската». Теперь два пустых прогона подряд ставят
тему на паузу, и владелец решает кнопками: остановить, продолжить или дать свои слова.
"""
import unittest
from unittest.mock import patch

import backend.article_fill_control as ctl
import backend.article_sprint_generator as gen
import backend.database as db


class ReportWordingTests(unittest.TestCase):
    ROWS = [
        {"theme_key": "gefuehle", "label": "Чувства", "target": 150, "state": "auto",
         "dry_streak": 0, "state_at": None, "words": 213, "fresh": 12},
        {"theme_key": "party", "label": "Вечеринки", "target": 150, "state": "auto",
         "dry_streak": 1, "state_at": None, "words": 50, "fresh": 0},
        {"theme_key": "haushalt", "label": "Уборка", "target": 150, "state": "paused",
         "dry_streak": 2, "state_at": None, "words": 55, "fresh": 0},
    ]

    def test_the_report_separates_growing_from_stalled(self):
        text = "\n".join(ctl.report_lines(self.ROWS))
        self.assertIn("Растут", text)
        self.assertIn("Чувства", text)
        self.assertIn("Прирост кончается", text)
        self.assertIn("Добор не идёт", text)

    def test_a_theme_at_its_target_is_not_called_exhausted(self):
        # Тему, набравшую цель, ночной прогон не трогает — прироста у неё нет по
        # здоровой причине, и пугать ею владельца незачем.
        rows = [{"theme_key": "med", "label": "Медицина", "target": 150, "state": "auto",
                 "dry_streak": 0, "state_at": None, "words": 172, "fresh": 0}]
        text = "\n".join(ctl.report_lines(rows))
        self.assertIn("Набрали цель", text)
        self.assertNotIn("Прирост кончается", text)

    def test_words_are_counted_in_russian(self):
        self.assertEqual(ctl._words_ru(1), "1 слово")
        self.assertEqual(ctl._words_ru(3), "3 слова")
        self.assertEqual(ctl._words_ru(11), "11 слов")
        self.assertEqual(ctl._words_ru(172), "172 слова")

    def test_a_paused_theme_says_so_in_russian(self):
        text = "\n".join(ctl.report_lines(self.ROWS))
        self.assertIn("на паузе", text)
        self.assertNotIn("paused", text, "в отчёт для человека внутренние слова не идут")

    def test_an_empty_bank_does_not_crash_the_report(self):
        self.assertTrue(ctl.report_lines([]))


class FillDecisionTests(unittest.TestCase):
    def _decide(self, action, theme="haushalt"):
        calls = []
        with patch.object(db, "get_article_sprint_theme",
                          lambda t: {"label_ru": "Уборка"}), \
                patch.object(db, "set_theme_fill_state",
                             lambda t, s, **k: calls.append((t, s)) or True):
            text = ctl.apply_fill_decision(action, theme)
        return text, calls

    def test_stop_pauses_the_fill_but_keeps_the_theme_in_the_game(self):
        text, calls = self._decide("stop")
        self.assertEqual(calls, [("haushalt", "paused")])
        self.assertIn("В игре тема остаётся", text)

    def test_go_resumes_the_fill(self):
        _, calls = self._decide("go")
        self.assertEqual(calls, [("haushalt", "auto")])

    def test_cancel_means_the_owner_found_nothing(self):
        text, calls = self._decide("cancel")
        self.assertEqual(calls, [("haushalt", "paused")])
        self.assertIn("слов не нашлось", text)

    def test_an_unknown_button_does_not_change_anything(self):
        _, calls = self._decide("wat")
        self.assertEqual(calls, [])


class ManualWordsTests(unittest.TestCase):
    def _accept(self, raw, result=None):
        seen = {}

        def _add(theme_key, entries):
            seen["entries"] = entries
            return result or {"added": len(entries), "skipped_dup": 0, "rejected": 0,
                              "final_verified": 60}

        def _state(theme_key, state, **kwargs):
            seen["state"] = state
            return True

        with patch.object(db, "get_article_sprint_theme", lambda t: {"label_ru": "Уборка"}), \
                patch.object(db, "set_theme_fill_state", _state), \
                patch.object(gen, "add_manual_words", _add):
            text = ctl.accept_manual_words("haushalt", raw)
        return text, seen

    def test_words_are_split_by_commas_and_lines(self):
        _, seen = self._accept("Besen, Eimer\nLappen\n  Schrubber  ")
        self.assertEqual([e["word"] for e in seen["entries"]],
                         ["Besen", "Eimer", "Lappen", "Schrubber"])

    def test_the_theme_stays_paused_after_manual_words(self):
        # Включать автодобор за владельца нельзя: он его сам и остановил.
        text, seen = self._accept("Besen, Eimer")
        self.assertEqual(seen["state"], "paused")
        self.assertIn("на паузе", text)

    def test_an_empty_message_gets_a_human_answer_not_a_crash(self):
        text, seen = self._accept("   \n  ")
        self.assertNotIn("entries", seen, "пустое сообщение до банка доходить не должно")
        self.assertIn("не нашёл ни одного слова", text)

    def test_duplicates_and_rejects_are_named_in_the_answer(self):
        text, _ = self._accept("Besen, Eimer", result={
            "added": 1, "skipped_dup": 1, "rejected": 2, "final_verified": 61})
        self.assertIn("добавлено: 1", text)
        self.assertIn("уже были в теме: 1", text)
        self.assertIn("не взял: 2", text)


class ExhaustionSignalTests(unittest.TestCase):
    """Сигнал «тема выдохлась» = два прогона подряд без прироста."""

    def test_growth_resets_the_streak(self):
        self.assertGreaterEqual(db.THEME_FILL_MIN_GROWTH, 1)
        self.assertEqual(db.THEME_FILL_DRY_LIMIT, 2)

    def test_autofill_skips_a_theme_that_is_not_on_auto(self):
        filled = []
        with patch.object(db, "list_theme_fill_report", lambda: [
                {"theme_key": "party", "state": "paused"},
                {"theme_key": "gefuehle", "state": "auto"}]), \
                patch.object(db, "record_theme_fill_run",
                             lambda t, a: {"dry_streak": 0, "exhausted": False}), \
                patch.object(db, "count_article_sprint_nouns", lambda *a, **k: 10), \
                patch("backend.article_sprint_themes.article_sprint_themes", lambda: [
                    {"key": "party", "target_count": 150},
                    {"key": "gefuehle", "target_count": 150}]), \
                patch.object(gen, "fill_theme",
                             lambda k, **kw: filled.append(k) or {"added": 5}):
            gen.autofill_themes_below_target(per_theme_cap=10, total_cap=50)
        self.assertEqual(filled, ["gefuehle"], "остановленную тему ночной прогон не трогает")

    def test_an_exhausted_theme_is_reported_back(self):
        with patch.object(db, "list_theme_fill_report", lambda: []), \
                patch.object(db, "record_theme_fill_run",
                             lambda t, a: {"dry_streak": 2, "exhausted": True}), \
                patch.object(db, "count_article_sprint_nouns", lambda *a, **k: 10), \
                patch("backend.article_sprint_themes.article_sprint_themes",
                      lambda: [{"key": "party", "target_count": 150}]), \
                patch.object(gen, "fill_theme", lambda k, **kw: {"added": 0}):
            res = gen.autofill_themes_below_target(per_theme_cap=10, total_cap=50)
        self.assertEqual(res["exhausted"], ["party"])


if __name__ == "__main__":
    unittest.main()
