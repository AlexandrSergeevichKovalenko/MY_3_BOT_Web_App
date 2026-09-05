# -*- coding: utf-8 -*-
"""«Слова со вчерашних тренировок»: дверь записи отбора и чистые функции слотов.
Стратегия: docs/tasks/word_pick_review_strategy.md."""
import os
import unittest
from datetime import date, datetime
from zoneinfo import ZoneInfo

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")

from backend import word_pick  # noqa: E402

VIENNA = ZoneInfo("Europe/Vienna")


class Слоты(unittest.TestCase):

    def test_до_вечернего_слота_идёт_утренний_проход(self):
        self.assertEqual(word_pick.slot_now(datetime(2026, 9, 5, 7, 25, tzinfo=VIENNA)), "am")
        self.assertEqual(word_pick.slot_now(datetime(2026, 9, 5, 19, 34, tzinfo=VIENNA)), "am")

    def test_с_19_35_идёт_вечерний_проход(self):
        self.assertEqual(word_pick.slot_now(datetime(2026, 9, 5, 19, 35, tzinfo=VIENNA)), "pm")
        self.assertEqual(word_pick.slot_now(datetime(2026, 9, 5, 23, 59, tzinfo=VIENNA)), "pm")

    def test_день_из_ссылки_разбирается_строго(self):
        self.assertEqual(word_pick.parse_day("20260905"), date(2026, 9, 5))
        self.assertEqual(word_pick.parse_day("2026-09-05"), date(2026, 9, 5))
        for плохое in ("", None, "2026095", "abc", "20261305", 20260905.0):
            self.assertIsNone(word_pick.parse_day(плохое), плохое)

    def test_номер_строки_ведомости_различает_утро_и_вечер(self):
        self.assertEqual(word_pick.day_id(date(2026, 9, 5), "am"), 202609051)
        self.assertEqual(word_pick.day_id(date(2026, 9, 5), "pm"), 202609052)
        self.assertEqual(word_pick.deeplink_for(date(2026, 9, 5)), "ans_wp_20260905")


import pathlib
from unittest import mock

from backend import database as db  # noqa: E402

КОРЕНЬ = pathlib.Path(__file__).resolve().parents[2]


def _соединение_с_курсором(курсор):
    ctx = mock.MagicMock()
    ctx.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = курсор
    return ctx


class ДверьОтбора(unittest.TestCase):

    def setUp(self):
        db._word_pick_schema_ready = True

    def test_все_источники_тапа_разрешены_и_не_превращаются_в_unknown(self):
        """interactive_save (дискетка в оверлее) до 05.09.2026 в списке не было: такие
        сохранения писались как unknown и затирали источник у старых слов."""
        for источник in db.WORD_PICK_ORIGINS:
            self.assertEqual(db._normalize_dictionary_origin_process(источник), источник, источник)
        self.assertEqual(db.WORD_PICK_ORIGINS, frozenset({
            "trainer_save", "synonym_save", "artikel_sprint_save", "adjektiv_trainer",
            "interactive_save", "rebus_save", "anagram_save", "crossword_save",
            "artikel_learn_save", "wofrage_learn_save"}))

    def test_каждый_источник_который_шлёт_фронт_разрешён_на_сервере(self):
        """Класс, а не список. 05.09.2026 нашлось восемь источников, которые фронт шлёт, а
        сервер превращал в «unknown» (пять экранов дискетки и три поверхности словаря).
        Неразрешённый источник теряет поверхность И затирает источник у старого слова."""
        import re
        фронт = КОРЕНЬ / "frontend/src"
        шлёт: set[str] = set()
        for путь in фронт.rglob("*.js*"):
            src = путь.read_text(encoding="utf-8", errors="ignore")
            шлёт.update(re.findall(r"""origin(?:_process|Process)?\s*[:=]\s*["']([a-z_]+)["']""", src))
        self.assertTrue(шлёт, "разбор фронта не нашёл ни одного источника — регулярка сломалась")
        неразрешённые = sorted(o for o in шлёт if db._normalize_dictionary_origin_process(o) != o)
        self.assertEqual(неразрешённые, [], "фронт шлёт источники, которые сервер превращает в unknown")

    def test_каждый_экран_дискетки_отбирает_на_завтра(self):
        """Все originProcess, которые экраны передают в SaveWordChip, входят в дверь отбора."""
        import re
        фронт = КОРЕНЬ / "frontend/src"
        дискетка: set[str] = set()
        for путь in фронт.rglob("*.jsx"):
            src = путь.read_text(encoding="utf-8", errors="ignore")
            дискетка.update(re.findall(r"""originProcess\s*=\s*["']([a-z_]+)["']""", src))
        self.assertTrue(дискетка)
        self.assertEqual(sorted(дискетка - db.WORD_PICK_ORIGINS), [])

    def test_источники_тапа_показываются_в_группе_тренажёров(self):
        группа = db.DICTIONARY_ORIGIN_GROUPS["trainer"][2]
        for источник in ("trainer_save", "synonym_save", "adjektiv_trainer", "interactive_save"):
            self.assertIn(источник, группа)

    def test_первый_тап_пишет_запись_на_завтра(self):
        курсор = mock.MagicMock()
        курсор.fetchone.return_value = (date(2026, 9, 6),)
        with mock.patch.object(db, "get_db_connection_context", _соединение_с_курсором(курсор)):
            out = db.record_word_pick(user_id=7, card_id=99, origin_process="trainer_save")
        self.assertEqual(out, {"for_day": date(2026, 9, 6), "inserted": True})
        sql = курсор.execute.call_args_list[0].args[0]
        self.assertIn("INSERT INTO bt_3_word_picks", sql)
        self.assertIn("ON CONFLICT (user_id, card_id, for_day) DO NOTHING", sql)
        self.assertIn("Europe/Vienna", str(курсор.execute.call_args_list[0].args[1]))

    def test_второй_тап_в_тот_же_день_не_дублирует(self):
        курсор = mock.MagicMock()
        курсор.fetchone.side_effect = [None, (date(2026, 9, 6),)]
        with mock.patch.object(db, "get_db_connection_context", _соединение_с_курсором(курсор)):
            out = db.record_word_pick(user_id=7, card_id=99, origin_process="trainer_save")
        self.assertEqual(out, {"for_day": date(2026, 9, 6), "inserted": False})

    def test_дверь_стоит_в_сохранении_слова_и_ответ_называет_день(self):
        """Проверка по исходнику: функция сохранения огромна и тянет право доступа, поэтому
        сверяем, что вызов стоит ИМЕННО в ней и что ответ несёт pick_for_day."""
        src = (КОРЕНЬ / "backend/backend_server.py").read_text(encoding="utf-8")
        тело = src.split("def save_webapp_dictionary_entry(", 1)[1].split("\n@app.route", 1)[0]
        self.assertIn("record_word_pick(", тело)
        self.assertIn("WORD_PICK_ORIGINS", тело)
        self.assertIn('"pick_for_day"', тело)
