# -*- coding: utf-8 -*-
"""Одно и то же не приходит владельцу из двух очередей.

ПОВОД, 29.08.2026. Владелец прислал скриншот письма «Слова, которых не знает ни один
справочник» с кнопками «убрать из словаря / слово настоящее» — а внутри лежали ЦЕЛЫЕ
ПРЕДЛОЖЕНИЯ: «Ich fange langsam an, Vertrauen zu jemandem zu haben», «Er brannte bis
auf die Grundmauern nieder», «Ich sitze vor dem Fernseher». Те же самые предложения он
в тот же день уже разобрал на экране проверки фраз.

Дословно: «ко мне приходит одно и то же со всех сторон».

Две причины, обе подтверждены замером:
  1. выборка письма НЕ проверяла, слово перед ней или нет — из 63 ждущих ответа
     словами были 13, остальные 50 — предложения, обороты и записи без вида;
  2. очереди опознают записи по-разному (экран — по номеру, письмо — по тексту) и не
     знали друг о друге: 20 из 20 проверенных примеров были «разобрано на экране» и
     одновременно «ЖДЁТ» в письме.
"""
import re
import unittest
from unittest import mock

from backend import word_confirm_digest as письмо


class ПисьмоСпрашиваетТолькоПроСлова(unittest.TestCase):
    def test_filter_exists_and_counts_spaces_not_grammar(self):
        self.assertIn("position(' ' in", письмо._ТОЛЬКО_СЛОВА,
                      "признак «одно слово» подменили догадкой о грамматике")

    def test_every_selection_uses_the_filter(self):
        """Фильтр обязан стоять во ВСЕХ выборках модуля. Пропустишь одну — владелец
        снова получит предложение, и найдём мы это только по его скриншоту."""
        исходник = open(письмо.__file__, encoding="utf-8").read()
        выборки = re.findall(r"w\.status IN \('не подтверждено', 'не слово'\)"
                             r"(.{0,200})", исходник, re.S)
        self.assertGreaterEqual(len(выборки), 3, "выборки перестали находиться")
        for кусок in выборки:
            self.assertIn("{только_слова}", кусок,
                          "выборка из очереди слов осталась без фильтра вида")

    def test_a_sentence_would_be_excluded(self):
        """Проверяем сам предикат на живых примерах владельца — без базы, строкой."""
        предикат = письмо._ТОЛЬКО_СЛОВА.format(bare="x")
        self.assertEqual("position(' ' in btrim(x)) = 0", предикат)
        # То, что предикат отсеет и что оставит, читается однозначно:
        for текст in ("Ich sitze vor dem Fernseher", "Bestehen auf etwas",
                      "Er brannte bis auf die Grundmauern nieder"):
            self.assertIn(" ", текст, "пример перестал быть многословным")
        for текст in ("Scheinwerferglas", "Nachtdämmerung", "Ragebait"):
            self.assertNotIn(" ", текст, "пример перестал быть словом")


class РазобралНаЭкранеЗакрытоВПисьме(unittest.TestCase):
    def test_decision_closes_the_same_text_in_the_word_queue(self):
        from backend import database
        курсор = mock.MagicMock()
        database._закрыть_вопрос_во_всех_очередях(курсор, "  Ich Sitze Vor Dem Fernseher ")
        запросы = " ".join(str(c.args[0]) for c in курсор.execute.call_args_list)
        self.assertIn("UPDATE bt_3_word_check", запросы)
        self.assertIn("reviewed = TRUE", запросы)
        self.assertIn("SAVEPOINT", запросы,
                      "побочная правка не под SAVEPOINT — она утащит за собой "
                      "решение владельца, если упадёт")

    def test_empty_text_touches_nothing(self):
        from backend import database
        курсор = mock.MagicMock()
        database._закрыть_вопрос_во_всех_очередях(курсор, "   ")
        курсор.execute.assert_not_called()

    def test_failure_is_logged_not_swallowed(self):
        """Молча проглоченная ошибка вернёт дубли, а мы будем думать, что починили."""
        from backend import database
        курсор = mock.MagicMock()
        курсор.execute.side_effect = [None, RuntimeError("нет связи"), None]
        with self.assertLogs(level="WARNING") as логи:
            database._закрыть_вопрос_во_всех_очередях(курсор, "Fernseher")
        self.assertTrue(any("во второй очереди" in s for s in логи.output))


if __name__ == "__main__":
    unittest.main()
