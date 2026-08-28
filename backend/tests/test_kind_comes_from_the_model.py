# -*- coding: utf-8 -*-
"""Вид записи называет МОДЕЛЬ, а не счёт слов — и только при согласии обоих судей.

ПОВОД, 28.08.2026. Вид (предложение / словосочетание) определялся правилом «больше
четырёх слов или точка в конце» (`lex_units._kind_for_text`). Это догадка, и она
ошибается на коротких предложениях: «Das geht nicht» после снятия артикля — два слова,
«Ich bin verwirrt» — три. Замер по всей очереди двумя независимыми моделями: 453 из
1822 (25%) были законченными предложениями под видом словосочетаний.

Цена ошибки двойная: такие записи грелись платным разбором вопреки решению владельца
(«предложения не греем»), и судья грамматики проверял их не как предложения — то есть
порядок слов у них не смотрел вовсе.

Решение владельца 28.08.2026: спрашивать вид у модели тем же запросом, которым и так
проверяется грамматика каждой фразы, — отдельных денег это не стоит.

Планка та же, что и для молчаливых правок текста: меняем ТОЛЬКО когда оба независимых
судьи назвали одно и то же. Один голос, молчание или спор — вид остаётся прежним.
"""
import unittest
from unittest import mock

from backend import phrase_night_check as ночь


class ВидЗаписиОтМодели(unittest.TestCase):
    def test_both_judges_agree_gives_the_kind(self):
        судьи = [{"kind": "sentence"}, {"kind": "sentence"}]
        self.assertEqual("sentence", ночь._both_name_the_same_kind(судьи))

    def test_judges_disagree_changes_nothing(self):
        судьи = [{"kind": "sentence"}, {"kind": "collocation"}]
        self.assertEqual("", ночь._both_name_the_same_kind(судьи),
                         "спор судей принят за вердикт — вид перепишется ни на чём")

    def test_two_silences_are_not_an_agreement(self):
        """Главная ловушка: пустой ответ обоих формально «одинаков». Считать это
        согласием значит переписать вид на основании двух молчаний."""
        for судьи in ([{}, {}], [{"kind": ""}, {"kind": ""}], [{"kind": "sentence"}, {}]):
            self.assertEqual("", ночь._both_name_the_same_kind(судьи),
                             f"молчание принято за согласие: {судьи}")

    def test_invented_kind_is_refused(self):
        судьи = [{"kind": "satz"}, {"kind": "satz"}]
        self.assertEqual("", ночь._both_name_the_same_kind(судьи),
                         "вид не из нашего списка принят — в базу уедет чужое слово")

    def test_one_judge_only_is_not_enough(self):
        self.assertEqual("", ночь._both_name_the_same_kind([{"kind": "sentence"}]))


class ЗаписьВидаЗащищена(unittest.TestCase):
    def test_unknown_kind_never_reaches_the_database(self):
        from backend import lex_units
        with mock.patch.object(lex_units, "get_db_connection_context") as соединение:
            self.assertFalse(lex_units.set_unit_kind(1, "предложение"))
            соединение.assert_not_called()

    def test_the_night_asks_for_the_kind_in_the_same_request(self):
        """Вопрос обязан ехать тем же запросом, что и грамматика: отдельный стоил бы
        денег на каждую фразу, а владелец согласился именно на бесплатный."""
        from backend import openai_manager
        исходник = open(openai_manager.__file__, encoding="utf-8").read()
        начало = исходник.index("def run_phrase_grammar_verdict")
        тело = исходник[начало:начало + 12000]
        self.assertIn('\\"kind\\":\\"sentence|collocation\\"', тело,
                      "поле вида пропало из схемы ответа судьи")
        self.assertIn("judge the grammar under YOUR OWN answer", тело,
                      "судья снова судит под НАШ вид — тогда предложение, названное "
                      "обрывком, опять не проверят на порядок слов")


if __name__ == "__main__":
    unittest.main()
