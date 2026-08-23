# -*- coding: utf-8 -*-
"""Второй голос стоит в двери записи разбора — и молчание не считается согласием.

Что здесь заперто:
  • проверяются ТОЛЬКО источники, где текст сочинила модель; перенос готового текста
    и живое сохранение человеком лишним платным запросом не облагаются;
  • «не смогли спросить» ≠ «хорошо»: разбор не пишется, слово остаётся кандидатом;
  • забракованный разбор не пишется;
  • внутри чужой транзакции (передан cursor) к модели не ходим вовсе — это занятое
    соединение пула на несколько секунд;
  • выключатель для тестов работает только когда его выставили явно.
"""
import os
import unittest
from unittest import mock

os.environ["SECOND_VOICE_CHECK_DISABLED"] = "0"        # в этих тестах проверка ВКЛЮЧЕНА
os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")

from backend import lex_units  # noqa: E402

CARD = {"translation_ru": "быть главным",
        "usage_examples": [{"source": "Wer die Hose anhat, bestimmt.",
                            "target": "Кто главный, тот решает."}]}


def _no_db():
    """Заглушка соединения: тесты не ходят в базу, нас интересует только развилка."""
    return mock.patch.object(lex_units, "get_db_connection_context", side_effect=RuntimeError)


class SecondVoiceOnWriteTests(unittest.TestCase):
    def test_invented_source_is_checked(self):
        with mock.patch("backend.second_voice_check.review_new_card") as review, \
             mock.patch.object(lex_units, "get_db_connection_context") as conn:
            conn.return_value.__enter__.return_value.cursor.return_value.__enter__ \
                .return_value.fetchone.return_value = ("die Hose anhaben", "collocation")
            review.return_value = {"checked": True, "ok": False,
                                   "fields": ["examples"], "why": "пример не про эту фразу"}
            self.assertFalse(lex_units.save_unit_card(1, dict(CARD), source="обогащение"))
            review.assert_called_once()

    def test_unchecked_is_not_saved(self):
        """Модель не ответила — запись отклоняется, а не проходит «по умолчанию»."""
        with mock.patch("backend.second_voice_check.review_new_card") as review, \
             mock.patch.object(lex_units, "get_db_connection_context") as conn:
            conn.return_value.__enter__.return_value.cursor.return_value.__enter__ \
                .return_value.fetchone.return_value = ("die Hose anhaben", "collocation")
            review.return_value = {"checked": False, "ok": False, "why": "Timeout"}
            self.assertFalse(lex_units.save_unit_card(1, dict(CARD), source="пересбор"))

    def test_carried_over_text_is_not_charged_a_check(self):
        """«Подъём из карточки» переносит готовый текст — модель там ничего не сочиняла."""
        with mock.patch("backend.second_voice_check.review_new_card") as review, \
             mock.patch.object(lex_units, "get_db_connection_context"):
            lex_units.save_unit_card(1, dict(CARD), source="подъём из карточки")
            review.assert_not_called()

    def test_human_save_is_not_checked(self):
        with mock.patch("backend.second_voice_check.review_new_card") as review, \
             mock.patch.object(lex_units, "get_db_connection_context"):
            lex_units.save_unit_card(1, dict(CARD), source="сохранение")
            review.assert_not_called()

    def test_inside_foreign_transaction_we_never_call_the_model(self):
        """Курсор = вызывающий держит соединение. Секундный запрос туда пускать нельзя."""
        cursor = mock.Mock()
        cursor.fetchone.return_value = ("de",)
        with mock.patch("backend.second_voice_check.review_new_card") as review:
            self.assertFalse(
                lex_units.save_unit_card(1, dict(CARD), source="обогащение", cursor=cursor))
            review.assert_not_called()

    def test_switch_is_off_unless_set_explicitly(self):
        with mock.patch.dict(os.environ, {"SECOND_VOICE_CHECK_DISABLED": ""}, clear=False):
            self.assertFalse(lex_units._second_voice_disabled())
        with mock.patch.dict(os.environ, {"SECOND_VOICE_CHECK_DISABLED": "1"}, clear=False):
            self.assertTrue(lex_units._second_voice_disabled())


class ReviewAnswerTests(unittest.TestCase):
    def test_missing_key_is_not_a_verdict(self):
        from backend import second_voice_check
        with mock.patch.dict(os.environ, {"GEMINI_API_KEY": ""}, clear=False):
            answer = second_voice_check.review_new_card(headword="x", card=CARD)
        self.assertFalse(answer["checked"])
        self.assertFalse(answer["ok"])

    def test_entry_without_examples_needs_no_check(self):
        from backend import second_voice_check
        with mock.patch.dict(os.environ, {"GEMINI_API_KEY": "test"}, clear=False):
            answer = second_voice_check.review_new_card(
                headword="x", card={"translation_ru": "смысл"})
        self.assertTrue(answer["checked"])
        self.assertTrue(answer["ok"])


if __name__ == "__main__":
    unittest.main()
