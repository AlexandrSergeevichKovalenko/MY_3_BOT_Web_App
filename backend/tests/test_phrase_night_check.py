"""Ночная проверка фраз: молча правим только то, в чём согласны ОБА судьи.

Почему два судьи. Замер 06.08.2026 на выборке 80 фраз: один судья дал 10 «ошибок», и
примерно треть из них — выдумка. Признак выдумки виден сразу: «предлог не тот»,
а в исправленном варианте дописана только точка. Второй независимый прогон такое не
повторяет, поэтому расхождение — надёжный сигнал «не уверен».

Почему порядок слов никогда не правим молча. Кусок, вырванный из предложения, законно
выглядит переставленным: «Was Sie sich dadurch erhoffen?» верно как придаточное и
неверно как самостоятельный вопрос. Откуда человек взял фразу, мы не знаем — значит
решает владелец, а не мы. Решение владельца 06.08.2026.
"""
import unittest

from backend.phrase_night_check import SILENT_CATEGORIES, _both_agree


def v(verdict="error", category="rechtschreibung", corrected="", why=""):
    return {"verdict": verdict, "category": category, "corrected": corrected, "why": why}


class BothJudgesMustAgreeTests(unittest.TestCase):
    def test_word_for_word_agreement_is_accepted(self):
        agreed, category, fix = _both_agree([
            v(corrected="Ich habe mir beim Sport das Bein gezerrt"),
            v(corrected="Ich habe mir beim Sport das Bein gezerrt"),
        ])
        self.assertTrue(agreed)
        self.assertEqual(category, "rechtschreibung")
        self.assertEqual(fix, "Ich habe mir beim Sport das Bein gezerrt")

    def test_different_correction_is_refused(self):
        """Та самая выдумка: категория одна, а правки разные."""
        agreed, _c, _f = _both_agree([
            v(category="praeposition", corrected="Heute bin ich mit Arbeit überlastet."),
            v(category="praeposition", corrected="Heute bin ich durch Arbeit überlastet"),
        ])
        self.assertFalse(agreed)

    def test_different_category_is_refused(self):
        agreed, _c, _f = _both_agree([
            v(category="kasus", corrected="одинаково"),
            v(category="praeposition", corrected="одинаково"),
        ])
        self.assertFalse(agreed)

    def test_one_judge_says_context_and_nothing_is_applied(self):
        agreed, _c, _f = _both_agree([
            v(corrected="Was erhoffen Sie sich dadurch?"),
            v(verdict="context", category="wortstellung", corrected=""),
        ])
        self.assertFalse(agreed)

    def test_empty_correction_is_refused(self):
        agreed, _c, _f = _both_agree([v(corrected=""), v(corrected="")])
        self.assertFalse(agreed)

    def test_a_dead_judge_never_lets_a_fix_through(self):
        self.assertFalse(_both_agree([v(corrected="x"), {}])[0])
        self.assertFalse(_both_agree([{}, {}])[0])
        self.assertFalse(_both_agree([v(corrected="x")])[0])


class WhatWeFixSilentlyTests(unittest.TestCase):
    def test_word_order_is_never_fixed_silently(self):
        self.assertNotIn("wortstellung", SILENT_CATEGORIES)
        self.assertNotIn("stil", SILENT_CATEGORIES)

    def test_context_independent_errors_are_fixed_silently(self):
        for category in ("rechtschreibung", "kongruenz", "kasus", "praeposition"):
            self.assertIn(category, SILENT_CATEGORIES)


class DeletionRuleTests(unittest.TestCase):
    """Правило владельца 06.08.2026: удаляя фразу из общего словаря, убираем и подписные
    карточки — но ТОЛЬКО те, куда человек не вписал своих полей. Свою карточку человека
    не трогаем никогда."""

    def test_rule_is_written_down_in_the_deletion_query(self):
        import pathlib
        src = (pathlib.Path(__file__).resolve().parents[1] / "database.py").read_text(encoding="utf-8")
        start = src.index("def apply_phrase_review_decision")
        block = src[start:start + 3000]
        self.assertIn("origin_process = 'subscription'", block,
                      "удаляем не только подписные карточки")
        self.assertIn("user_notes", block, "не проверяем личные поля человека")
        self.assertIn("jsonb_array_length(q.user_notes) = 0", block)


if __name__ == "__main__":
    unittest.main()
