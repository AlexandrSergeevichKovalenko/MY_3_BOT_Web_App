# -*- coding: utf-8 -*-
"""Обрезок не становится заголовком общего словаря, а живое сохранение остаётся быстрым.

ПОВОД, 28.08.2026. «Spal» (недописанное «Spalte») стал словом общего словаря: быстрый
словарь переводит по паузе, человек сохранил недописанное, а ночная привязка подняла
его личную карточку в ОБЩИЙ заголовок. Дверь слова при этом звалась с выключенными
сетью и моделью, поэтому вердикта «не слово» у «Spal» не появилось вовсе — и механизм
подсказок («вы имели в виду Spalte?») остался пустым: он строит подсказки только тем,
кому вердикт уже вынесен.

Тест держит ДВА обещания, и второе не менее важно первого:
  • ночная привязка спрашивает дверь ПОЛНОСТЬЮ — иначе обрезки снова пойдут в словарь;
  • живое сохранение НЕ ходит в сеть и к модели — иначе человек будет ждать секунды
    и мы заплатим за каждое сохранение. Это решение владельца, и его легко потерять
    одной строкой «а давайте включим везде».
"""
import unittest
from unittest import mock

from backend import lex_units


class ГлубокаяДверьТолькоНочью(unittest.TestCase):
    def _позвать(self, **kwargs):
        """Вернуть аргументы, с которыми дверь спросили про немецкое слово."""
        with mock.patch.dict("os.environ", {"WORD_GATE_LOOKUP": "1"}):
            with mock.patch("backend.german_word_gate.check_word") as дверь:
                дверь.return_value = {"status": "подтверждено", "text": "Spalte"}
                lex_units._word_gate_for_new_unit(
                    "Spalte", "spalte", "word", "de", **kwargs)
        return дверь.call_args

    def test_live_save_never_waits_for_network_or_model(self):
        вызов = self._позвать()
        self.assertFalse(вызов.kwargs["allow_network"],
                         "живое сохранение пошло в сеть — человек будет ждать")
        self.assertFalse(вызов.kwargs["allow_model"],
                         "живое сохранение пошло к модели — каждое сохранение станет платным")

    def test_night_path_asks_the_full_gate(self):
        вызов = self._позвать(deep=True)
        self.assertTrue(вызов.kwargs["allow_network"],
                        "ночная дверь без сети — обрезок снова станет заголовком")
        self.assertTrue(вызов.kwargs["allow_model"],
                        "ночная дверь без модели — вердикта «не слово» не будет, "
                        "и подсказка «вы имели в виду …» опять останется пустой")

    def test_the_night_pickup_passes_deep(self):
        """Сама ночная привязка обязана просить глубокую дверь. Без этого две
        предыдущие проверки зелёные, а дефект жив."""
        with mock.patch.object(lex_units, "attach_entry_to_unit",
                               return_value=1) as привязка:
            with mock.patch.object(lex_units, "get_db_connection_context") as соединение:
                курсор = mock.MagicMock()
                курсор.fetchall.return_value = [(7, "Spal", None, "de", "ru")]
                соединение.return_value.__enter__.return_value.cursor.return_value \
                    .__enter__.return_value = курсор
                lex_units.attach_missing_entries(limit=1)
        self.assertTrue(привязка.call_args.kwargs.get("deep"),
                        "ночной подбор зовёт дверь дёшево — обрезки снова пойдут "
                        "в общий словарь молча")


if __name__ == "__main__":
    unittest.main()
