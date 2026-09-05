# -*- coding: utf-8 -*-
"""Одна подпись над чипами во всех пяти местах, и обогащение только для НОВОГО слова.

Тап по слову «уже есть» до 05.09.2026 всё равно запускал запрос разбора (это деньги,
если слова нет в общем пуле). Теперь обогащение ждёт ответа сохранения и идёт только
при inserted=true. Проверка по исходникам: фронт-тестов в проекте нет."""
import pathlib
import re
import unittest

КОРЕНЬ = pathlib.Path(__file__).resolve().parents[2] / "frontend/src"
МЕСТА = ["answer/TrainerGame.jsx", "answer/SprintGame.jsx", "answer/ArtikelSprintGame.jsx",
         "answer/AdjektivLearnGame.jsx", "answer/AnswerOverlay.jsx", "answer/SaveWordChip.jsx"]


class ПодписьОднаНаВсех(unittest.TestCase):

    def test_каждое_место_с_чипами_берёт_подпись_из_одного_файла(self):
        for имя in МЕСТА:
            src = (КОРЕНЬ / имя).read_text(encoding="utf-8")
            self.assertIn("from './pickCopy.js'", src, имя)
            self.assertNotIn("нажми, чтобы сохранить в словарь", src, имя)

    def test_подпись_обещает_и_словарь_и_завтра(self):
        src = (КОРЕНЬ / "answer/pickCopy.js").read_text(encoding="utf-8")
        self.assertIn("сохранится в словарь, если его ещё нет, и завтра придёт на повторение", src)
        self.assertIn("завтра повторим", src)

    def test_обогащение_только_после_ответа_и_только_для_нового(self):
        src = (КОРЕНЬ / "dictionary/saveUtils.js").read_text(encoding="utf-8")
        тело = src.split("export async function saveGermanWordViaLookup", 1)[1].split("\nasync function saveDictionaryCard", 1)[0]
        self.assertIn("if (res?.inserted !== false) void enrichSavedGermanWord", тело)
        self.assertIn("pickedForDay", тело)
        self.assertIsNone(re.search(r"void enrichSavedGermanWord\([^)]*\);\s*\n\s*//[^\n]*\n\s*//[^\n]*\n\s*return", тело),
                          "обогащение всё ещё уходит до разбора ответа")
