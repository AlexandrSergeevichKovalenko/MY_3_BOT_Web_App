# -*- coding: utf-8 -*-
"""Скачанная таблица склонений укладывается в наш формат один в один.

ЗАЧЕМ ЭТОТ ТЕСТ. 23.08.2026 к нам загружено 86 840 таблиц склонения из пакета
`german-nouns` (CC BY-SA, собран из de.wiktionary). Раньше склонение спрашивалось у
Wiktionary ПО ОДНОМУ СЛОВУ через сеть, и это давало два изъяна:

    отказ по частоте НЕОТЛИЧИМ от «слова нет» — прогон объявил неизвестными
    «Ratte», «Scherbe», «Rivalität», «Verbindlichkeit», которые справочник знает;
    ночная порция всего 120 слов, дальше начинался отказ.

Формат преобразования проверяется здесь, потому что ошибка в нём тихо разложила бы
неверные артикли по 86 тысячам слов — и заметить это можно было бы только глазами.

СВЕРЕНО НА ЖИВЫХ ДАННЫХ ДО ЗАГРУЗКИ: из 2162 слов, где у нас уже была таблица от
Wiktionary, скачанная сошлась в 2161.
"""
from __future__ import annotations

import importlib.util
import pathlib
import unittest

_ПУТЬ = pathlib.Path(__file__).resolve().parents[2] / "scripts" / "load_offline_noun_tables.py"
_спец = importlib.util.spec_from_file_location("load_offline_noun_tables", _ПУТЬ)
_модуль = importlib.util.module_from_spec(_спец)
_спец.loader.exec_module(_модуль)


def _строка(**поля) -> dict:
    """Строка скачанной таблицы: только те колонки, что нужны в проверке."""
    основа = {"lemma": поля.pop("lemma", "Haus"), "genus": поля.pop("genus", "n")}
    основа.update(поля)
    return основа


class АртиклиРасставленыВерноTest(unittest.TestCase):
    def test_мужской_род(self):
        т = _модуль._в_наш_формат(_строка(
            lemma="Baum", genus="m",
            **{"nominativ singular": "Baum", "genitiv singular": "Baumes",
               "dativ singular": "Baum", "akkusativ singular": "Baum",
               "nominativ plural": "Bäume", "dativ plural": "Bäumen"}))
        ряды = {r["case"]: r for r in т["m"]["rows"]}
        self.assertEqual(ряды["nom"]["singular"], "der Baum")
        self.assertEqual(ряды["gen"]["singular"], "des Baumes")
        self.assertEqual(ряды["dat"]["singular"], "dem Baum")
        self.assertEqual(ряды["akk"]["singular"], "den Baum")
        self.assertEqual(ряды["nom"]["plural"], "die Bäume")
        self.assertEqual(ряды["dat"]["plural"], "den Bäumen")

    def test_женский_род(self):
        т = _модуль._в_наш_формат(_строка(
            lemma="Ratte", genus="f",
            **{"nominativ singular": "Ratte", "genitiv singular": "Ratte",
               "dativ singular": "Ratte", "akkusativ singular": "Ratte"}))
        ряды = {r["case"]: r for r in т["f"]["rows"]}
        self.assertEqual(ряды["nom"]["singular"], "die Ratte")
        self.assertEqual(ряды["gen"]["singular"], "der Ratte")
        self.assertEqual(ряды["dat"]["singular"], "der Ratte")
        self.assertEqual(ряды["akk"]["singular"], "die Ratte")

    def test_средний_род(self):
        т = _модуль._в_наш_формат(_строка(
            lemma="Haus", genus="n",
            **{"nominativ singular": "Haus", "genitiv singular": "Hauses",
               "dativ singular": "Haus", "akkusativ singular": "Haus"}))
        ряды = {r["case"]: r for r in т["n"]["rows"]}
        self.assertEqual(ряды["nom"]["singular"], "das Haus")
        self.assertEqual(ряды["gen"]["singular"], "des Hauses")
        self.assertEqual(ряды["akk"]["singular"], "das Haus")


class ПроисхождениеИОтказыTest(unittest.TestCase):
    def test_подпись_источника_стоит(self):
        """Ответ модели не имеет права выдавать себя за справочник — подпись обязательна."""
        т = _модуль._в_наш_формат(_строка(**{"nominativ singular": "Haus"}))
        self.assertEqual(т["source"], "german-nouns")

    def test_без_рода_таблицы_не_строим(self):
        """Род не назван — артикль ставить не из чего. Молчим, а не подставляем «der»."""
        self.assertIsNone(_модуль._в_наш_формат(_строка(genus="", lemma="-algie")))

    def test_без_форм_таблицы_не_строим(self):
        self.assertIsNone(_модуль._в_наш_формат(_строка(genus="m", lemma="Haus")))

    def test_нумерованный_вариант_подхватывается(self):
        """У омографов источник нумерует колонки: «Kiefer» с двумя родами, «Band» с
        двумя множественными. Пустая основная колонка не значит «формы нет»."""
        т = _модуль._в_наш_формат(_строка(
            lemma="Band", genus="n", **{"nominativ singular 1": "Band",
                                        "nominativ plural 1": "Bänder"}))
        ряды = {r["case"]: r for r in т["n"]["rows"]}
        self.assertEqual(ряды["nom"]["singular"], "das Band")
        self.assertEqual(ряды["nom"]["plural"], "die Bänder")


if __name__ == "__main__":
    unittest.main()
