# -*- coding: utf-8 -*-
"""Разбор, размноженный сам на себя, не записывается — и не рушит живой текст.

ЧТО ЗАЩИЩАЕМ. 16.08.2026 разовый скрипт применил одну замену шесть раз подряд к уже
заменённому тексту. Заголовок слова с 20.08 защищён правилом в самой базе, а РАЗБОР —
примеры, устойчивые сочетания, объяснения — не был защищён ничем, хотя порча дошла
именно туда: 15 слов и 143 карточки людей. CHECK на разбор поставить нельзя: это
дерево значений, а не колонка. Значит проверка обязана стоять в коде, на дне записи.

ПОЧЕМУ ТЕСТ ПРО ЛОЖНЫЕ СРАБАТЫВАНИЯ ВАЖНЕЕ, ЧЕМ ПРО ЛОВЛЮ. Замер 21.08.2026 по всем
10454 разборам живой базы: грубое правило «буква повторена 4+ раз» даёт 6 попаданий,
и пять из них — законный текст, наши же мнемоники («звук „пииии“», «звук „рррр“»).
Страж, отвергающий такое, ломает продукт молча: разбор просто перестаёт сохраняться,
а человек видит пустую карточку и не понимает почему.
"""
from __future__ import annotations

import unittest

from backend.mangled_text import is_mangled, mangled_strings_inside


class ЛовитПорчуTest(unittest.TestCase):
    def test_три_размера_хвоста(self):
        for текст in (
            "Er erlag der Versuchung......",
            "Das Anliegen an jemanden jemanden jemanden jemanden jemanden jemanden",
            "sterile Gazennnnnn",
        ):
            with self.subTest(текст=текст):
                self.assertTrue(is_mangled(текст))

    def test_порча_в_середине_предложения(self):
        """Три прежних правила привязаны к концу строки. Внутри разбора порча попадает
        в середину, и конец при этом чистый — живой случай из карточки «die Gaze»."""
        self.assertTrue(is_mangled(
            "sterile Gazennnnn wird oft in der Wundversorgung benutzt."))
        self.assertTrue(is_mangled(
            "POV: POV: POV: POV: POV: POV: Er will ins Freibad gehen!"))

    def test_находит_на_любой_глубине_разбора(self):
        разбор = {"word_de": "die Gaze",
                  "examples": [{"source": "sterile Gazennnnnn", "target": "стерильная марля"}],
                  "common_collocations": ["ein Stück Gaze"]}
        найдено = mangled_strings_inside(разбор)
        self.assertEqual(найдено, ["sterile Gazennnnnn"])


class НеТрогаетЖивойТекстTest(unittest.TestCase):
    def test_мнемоники_проходят(self):
        """Пять из шести попаданий грубого правила на живой базе — вот такие."""
        for текст in (
            "представьте себе детский лепет или звук 'пииии', чтобы легче запомнить",
            "Звук «рррр» передает движение по кругу",
        ):
            with self.subTest(текст=текст):
                self.assertFalse(is_mangled(текст))

    def test_законный_немецкий_проходит(self):
        for текст in ("Schifffahrt", "Brennnessel", "die Sauerstoffflasche"):
            with self.subTest(текст=текст):
                self.assertFalse(is_mangled(текст))

    def test_многоточие_и_повторы_до_порога_проходят(self):
        for текст in ("Es kommt darauf an...", "ja ja ja", "Nein!!!"):
            with self.subTest(текст=текст):
                self.assertFalse(is_mangled(текст))

    def test_целый_разбор_проходит(self):
        разбор = {"word_de": "die Gaze", "translation_ru": "марля",
                  "examples": [{"source": "Die Wunde wurde mit Gaze abgedeckt.",
                                "target": "Рану накрыли марлей."}],
                  "mnemonic": "представьте звук 'пииии'"}
        self.assertEqual(mangled_strings_inside(разбор), [])


class СтражСтоитНаОбоихПутяхЗаписиTest(unittest.TestCase):
    """Проверка стоит в коде, а не в намерении: путей записи в разбор два."""

    def test_запись_разбора_на_слове(self):
        import inspect
        from backend import lex_units
        src = inspect.getsource(lex_units.save_unit_card)
        self.assertIn("mangled_strings_inside", src,
                      "save_unit_card пишет разбор без проверки на размноженный текст")

    def test_ночной_добор_синонимов(self):
        import inspect
        from backend import backend_server
        src = inspect.getsource(backend_server._run_synonym_backfill)
        self.assertIn("mangled_strings_inside", src,
                      "добор синонимов пишет в разбор напрямую и без проверки")


if __name__ == "__main__":
    unittest.main()
