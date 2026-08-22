# -*- coding: utf-8 -*-
"""Каждая запись чистит вход САМА, а не надеется на вызывающего.

ОТКУДА ЗАДАЧА. Карта путей записи (22.08.2026) насчитала четырнадцать мест, где
немецкий текст попадает в базу мимо всех проверок. Три из них — здесь:

    update_webapp_dictionary_entry        восемь вызывающих, ночное обогащение
    update_dictionary_entry_full_columns  добор перевода быстрого словаря
    retitle_unit                          переименование слова

Общее у них одно: чистка была обязанностью того, кто зовёт. Такое правило держится
ровно до следующего вызывающего — и `retitle_unit` это доказала.

ЧТО ПРОЕХАЛО ЧЕРЕЗ `retitle_unit` (замер по 41 628 словам живой базы):

    'трениe'      латинская «e» внутри русского слова
    'плаксa'      латинская «a»
    'устройcтво'  латинская «c»
    'Грубo …'     латинская «o»
    'Кроме того,' хвостовая запятая

Латинская буква не видна глазом, но она попадает и в ключ поиска — значит слово не
находится по своему же имени НИКОГДА. Шесть из семи таких заголовков оказались
дубликатами уже существующего правильного слова.
"""
from __future__ import annotations

import inspect
import unittest

from backend import database, lex_units
from backend.dictionary_intake import clean_text


class ЧисткаСтоитВнутриЗаписиTest(unittest.TestCase):
    """Проверяем ФАКТ наличия чистки в теле функции, а не намерение в комментарии."""

    def test_переименование_слова(self):
        src = inspect.getsource(lex_units.retitle_unit)
        self.assertIn("clean_text", src,
                      "retitle_unit пишет заголовок без общей чистки")

    def test_запись_разбора_карточки(self):
        src = inspect.getsource(database.update_webapp_dictionary_entry)
        self.assertIn("clean_text", src)
        self.assertIn("mangled_strings_inside", src,
                      "разбор карточки пишется без стража целостности")

    def test_перезапись_всех_колонок_карточки(self):
        src = inspect.getsource(database.update_dictionary_entry_full_columns)
        self.assertIn("clean_all", src)
        self.assertIn("mangled_strings_inside", src)


class ЧистяткаСнимаетИменноЭтуГрязьTest(unittest.TestCase):
    """Живые случаи из базы: чистка обязана их узнавать."""

    def test_латинская_буква_внутри_русского_слова(self):
        for грязно, чисто in (
            ("трениe", "трение"),
            ("плаксa", "плакса"),
            ("устройcтво", "устройство"),
        ):
            with self.subTest(грязно=грязно):
                self.assertEqual(clean_text(грязно), чисто)

    def test_хвостовая_запятая(self):
        self.assertEqual(clean_text("Кроме того,"), "Кроме того")
        self.assertEqual(clean_text("Садиться ,"), "Садиться")
        self.assertEqual(clean_text("Falle nicht auf Betrüger herein,"),
                         "Falle nicht auf Betrüger herein")

    def test_чистое_остаётся_нетронутым(self):
        for текст in ("трение", "die Abschiebung", "Kommen Sie bitte vorbei.",
                      "Es kommt darauf an...", "Schifffahrt"):
            with self.subTest(текст=текст):
                self.assertEqual(clean_text(текст), текст)


class ПорченыйРазборНеЗаписываетсяTest(unittest.TestCase):
    """Запись отвергается целиком, а не подчищается тихо."""

    def test_разбор_с_размноженным_текстом_не_доходит_до_базы(self):
        писали = []

        class _Курсор:
            def execute(self, *a, **k):
                писали.append(a)

            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

        class _Соединение:
            def cursor(self):
                return _Курсор()

            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

        import contextlib
        from unittest import mock

        @contextlib.contextmanager
        def _соединение():
            yield _Соединение()

        порченый = {"examples": [{"source": "sterile Gazennnnnn"}]}
        with mock.patch.object(database, "get_db_connection_context", _соединение):
            database.update_webapp_dictionary_entry(1, порченый)
        self.assertEqual(писали, [], "порченый разбор всё-таки дошёл до запроса")

    def test_целый_разбор_записывается(self):
        писали = []

        class _Курсор:
            def execute(self, *a, **k):
                писали.append(a)

            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

        class _Соединение:
            def cursor(self):
                return _Курсор()

            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

        import contextlib
        from unittest import mock

        @contextlib.contextmanager
        def _соединение():
            yield _Соединение()

        целый = {"examples": [{"source": "Die Wunde wurde mit Gaze abgedeckt."}]}
        with mock.patch.object(database, "get_db_connection_context", _соединение):
            database.update_webapp_dictionary_entry(1, целый)
        self.assertEqual(len(писали), 1, "целый разбор не записался")


if __name__ == "__main__":
    unittest.main()
