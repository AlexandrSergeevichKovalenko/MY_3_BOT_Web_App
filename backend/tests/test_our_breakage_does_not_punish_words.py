# -*- coding: utf-8 -*-
"""Слово не наказывается за НАШУ поломку, но бесконечно не повторяется.

ДОКАЗАНО 29.08.2026 на живых данных, а не рассуждением:
  • у слоя слов нет ни одной колонки про неудачные попытки и нет отдельной таблицы —
    помнить отказ было НЕЧЕМ;
  • выборка ночи смотрела только «разбора нет»;
  • из 20 слов, взятых ночью 29.08, у 4 разбор так и не появился — и ВСЕ 4 ИЗ 4 снова
    стояли в очереди на следующую ночь. Два платных запроса за заведомый отказ, каждую
    ночь, бесконечно.

Почему нельзя было просто вернуть старый счётчик пула: он считал ЛЮБУЮ неудачу и
потому наказывал слова за наши аварии. Пока второй голос молчал без ключа, отклонялось
всё подряд — и базовые глаголы вроде `zeigen` и `halten` оказались в карантине как
«негодные». Проверка 29.08: 4 из 6 таких слов собираются прямо сейчас и проходят судью.

Отсюда правило: считается только отказ ПРО СЛОВО. Наша поломка молчит в логе.
"""
import unittest
from unittest import mock

from backend import database


class НашаПоломкаНеНаказываетСлово(unittest.TestCase):
    def test_our_fault_writes_nothing_to_the_database(self):
        with mock.patch.object(database, "get_db_connection_context") as соединение:
            with self.assertLogs(level="WARNING") as логи:
                database.note_unit_enrich_refusal(
                    7, "судья не ответил: HTTP 429", наша_вина=True)
        соединение.assert_not_called()
        self.assertTrue(any("ПО НАШЕЙ вине" in s for s in логи.output),
                        "наша поломка прошла молча — она станет невидимой")

    def test_word_fault_is_counted(self):
        with mock.patch.object(database, "get_db_connection_context") as соединение:
            курсор = mock.MagicMock()
            соединение.return_value.__enter__.return_value.cursor.return_value \
                .__enter__.return_value = курсор
            database.note_unit_enrich_refusal(
                7, "судья забраковал: пример не о том слове", наша_вина=False)
        запросы = " ".join(str(c.args[0]) for c in курсор.execute.call_args_list)
        self.assertIn("bt_3_unit_enrich_refusals", запросы)
        self.assertIn("refusals + 1", запросы)

    def test_success_forgets_past_refusals(self):
        """Иначе одна плохая ночь метит слово на неделю вперёд."""
        with mock.patch.object(database, "get_db_connection_context") as соединение:
            курсор = mock.MagicMock()
            соединение.return_value.__enter__.return_value.cursor.return_value \
                .__enter__.return_value = курсор
            database.forget_unit_enrich_refusals(7)
        запросы = " ".join(str(c.args[0]) for c in курсор.execute.call_args_list)
        self.assertIn("DELETE FROM bt_3_unit_enrich_refusals", запросы)

    def test_bad_id_touches_nothing(self):
        with mock.patch.object(database, "get_db_connection_context") as соединение:
            database.note_unit_enrich_refusal(None, "что-то", наша_вина=False)
            database.note_unit_enrich_refusal(0, "что-то", наша_вина=False)
        соединение.assert_not_called()


class ОчередьПропускаетОтложенные(unittest.TestCase):
    def test_the_picker_skips_paused_words(self):
        from backend import lex_units
        исходник = open(lex_units.__file__, encoding="utf-8").read()
        начало = исходник.index("def units_needing_card")
        тело = исходник[начало:начало + 6000]
        self.assertIn("bt_3_unit_enrich_refusals", тело,
                      "выборка ночи снова не смотрит на отказы — слово вернётся "
                      "бесконечно, по два платных запроса за ночь")
        self.assertIn("refusals >= 3", тело)
        self.assertIn("INTERVAL '7 days'", тело,
                      "ссылка стала вечной: слово обязано возвращаться само, "
                      "справочники и модель меняются")


class ЧислоОтложенныхВидноВладельцу(unittest.TestCase):
    def test_paused_count_reaches_the_report(self):
        import bot_3
        текст = bot_3._night_refusal_lines({"errors": 0, "paused_after_refusals": 12})
        self.assertIn("Отложено", текст)
        self.assertIn("12", текст)
        self.assertIn("Вернутся сами", текст)

    def test_zero_paused_adds_no_noise(self):
        import bot_3
        self.assertNotIn("Отложено",
                         bot_3._night_refusal_lines({"errors": 0,
                                                     "paused_after_refusals": 0}))


if __name__ == "__main__":
    unittest.main()
