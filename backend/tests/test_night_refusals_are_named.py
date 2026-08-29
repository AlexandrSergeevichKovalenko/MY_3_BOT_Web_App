# -*- coding: utf-8 -*-
"""Ночь называет СЛОВА, которые не записала, и отличает отказ судьи от поломки.

ПОВОД, 29.08.2026. Утром пришло «Ошибок: 63». Владелец потребовал показать эти 63 —
показать было нечего: примеры хранились только для «пропущено», а логи за 03:10 к утру
уже вытеснены. Число, к которому нельзя приложить слова, — не работа.

И оно врало по существу. Почти все 63 были случаем «судья забраковал»: второй голос
нашёл кривой пример и не пустил текст к людям. Это дверь СРАБОТАЛА, а называлось
«ошибкой» — рядом с настоящими поломками. Отличить рабочую ночь от сломанной было
нельзя.
"""
import unittest
from unittest import mock

from backend import lex_units


class ПричинаОтказаНазывается(unittest.TestCase):
    def test_empty_card_says_why(self):
        причины: list[str] = []
        self.assertFalse(lex_units.save_unit_card(1, {}, reasons=причины))
        self.assertEqual(["пустой разбор"], причины)

    def test_judge_rejection_is_named_as_a_judge_rejection(self):
        """Слово «забраковал» — не косметика: по нему ночь отделяет работу двери
        от поломки. Сменишь формулировку — отчёт снова смешает два мира."""
        причины: list[str] = []
        карточка = {"usage_examples": [{"de": "Das Haus.", "ru": "Дом."}]}
        with mock.patch.dict("os.environ", {"SECOND_VOICE_CHECK_DISABLED": ""}):
            with mock.patch("backend.lex_units.get_db_connection_context") as соединение:
                курсор = mock.MagicMock()
                курсор.fetchone.return_value = ("Haus", "word")
                соединение.return_value.__enter__.return_value.cursor.return_value \
                    .__enter__.return_value = курсор
                with mock.patch("backend.second_voice_check.review_new_card",
                                return_value={"checked": True, "ok": False,
                                              "fields": ["examples"],
                                              "why": "пример не о том слове"}):
                    ok = lex_units.save_unit_card(1, карточка, source="обогащение",
                                                  reasons=причины)
        self.assertFalse(ok)
        self.assertEqual(1, len(причины))
        self.assertTrue(причины[0].startswith("судья забраковал"),
                        f"причина не опознаётся как отказ судьи: {причины[0]!r}")
        self.assertIn("пример не о том слове", причины[0])

    def test_silent_judge_is_not_called_a_rejection(self):
        """«Судья не ответил» — это ПОЛОМКА, и путать её с «забраковал» нельзя:
        первая требует моего вмешательства, вторая — нормальная работа двери."""
        причины: list[str] = []
        карточка = {"usage_examples": [{"de": "Das Haus.", "ru": "Дом."}]}
        with mock.patch.dict("os.environ", {"SECOND_VOICE_CHECK_DISABLED": ""}):
            with mock.patch("backend.lex_units.get_db_connection_context") as соединение:
                курсор = mock.MagicMock()
                курсор.fetchone.return_value = ("Haus", "word")
                соединение.return_value.__enter__.return_value.cursor.return_value \
                    .__enter__.return_value = курсор
                with mock.patch("backend.second_voice_check.review_new_card",
                                return_value={"checked": False, "ok": False,
                                              "why": "HTTP 429"}):
                    lex_units.save_unit_card(1, карточка, source="обогащение",
                                             reasons=причины)
        self.assertFalse(причины[0].startswith("судья забраковал"),
                         "молчание судьи посчитали работой двери — сломанная ночь "
                         "будет выглядеть нормальной")
        self.assertIn("не ответил", причины[0])

    def test_reasons_is_optional(self):
        """Старые вызывающие не передают reasons — они не должны падать."""
        self.assertFalse(lex_units.save_unit_card(1, {}))


class ОтчётПоказываетСлова(unittest.TestCase):
    def _строки(self, meta):
        import bot_3
        return bot_3._night_refusal_lines(meta)

    def test_words_reach_the_report(self):
        текст = self._строки({
            "rejected_by_judge": 2, "errors": 0,
            "judge_samples": [{"word": "Spalte", "why": "пример не о том слове"}],
        })
        self.assertIn("Spalte", текст)
        self.assertIn("пример не о том слове", текст)
        self.assertIn("Судья забраковал", текст)

    def test_error_line_is_shown_even_at_zero(self):
        """Пропавшая строка неотличима от отвалившегося счётчика — это уже
        проходили на планировщике."""
        self.assertIn("Ошибок", self._строки({"errors": 0}))

    def test_breakage_words_are_shown_too(self):
        текст = self._строки({
            "errors": 1,
            "error_samples": [{"word": "Zufall", "why": "судья не ответил: HTTP 429"}],
        })
        self.assertIn("Zufall", текст)
        self.assertIn("HTTP 429", текст)


if __name__ == "__main__":
    unittest.main()
