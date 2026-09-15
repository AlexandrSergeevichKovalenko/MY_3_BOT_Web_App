# -*- coding: utf-8 -*-
"""Модель обязана вернуть столько же строк, сколько ей прислали.

ПОВОД, 15.09.2026. Перевод субтитров раскладывался по репликам ПО ПОРЯДКУ, и проверки
длины не было нигде. Модель склеивала две обрывочные фразы в одну русскую — и весь
остаток пачки съезжал на реплику: русский шёл вперёд. Съехавшее сохранялось в базу
навсегда и уходило всем следующим зрителям.

Что здесь закреплено:
  • несовпадение числа строк — ошибка, а не повод «добить пустыми» или «обрезать»;
  • переспрашиваем РОВНО один раз, и во второй раз называем число прямо в задании;
  • не вышло — пачка НЕ сохраняется, и это видно счётчиком;
  • нечитаемый ответ модели тоже не превращается в пустые строки нужной длины;
  • в эндпоинте стоит последний замок на равенство длин.
"""
import asyncio
import json
import pathlib
import unittest
from unittest import mock

ROOT = pathlib.Path(__file__).resolve().parents[2]
SERVER = ROOT / "backend" / "backend_server.py"
DB = ROOT / "backend" / "database.py"
APP_JSX = ROOT / "frontend" / "src" / "App.jsx"


class РазборОтветаМодели(unittest.TestCase):
    def test_ровно_столько_строк_сколько_просили(self):
        from backend.openai_manager import _parse_subtitle_translations

        got = _parse_subtitle_translations('{"translations": [" а ", "б"]}', 2)
        self.assertEqual(got, ["а", "б"])

    def test_строк_меньше_это_ошибка_а_не_добивание_пустыми(self):
        from backend.openai_manager import (SubtitleTranslationCountMismatch,
                                            _parse_subtitle_translations)

        with self.assertRaises(SubtitleTranslationCountMismatch) as ctx:
            _parse_subtitle_translations('{"translations": ["склеила две в одну"]}', 2)
        self.assertEqual(ctx.exception.expected, 2)
        self.assertEqual(ctx.exception.got, 1)

    def test_строк_больше_тоже_ошибка(self):
        from backend.openai_manager import (SubtitleTranslationCountMismatch,
                                            _parse_subtitle_translations)

        with self.assertRaises(SubtitleTranslationCountMismatch):
            _parse_subtitle_translations('{"translations": ["а", "б", "в"]}', 2)

    def test_нечитаемый_ответ_не_становится_пустыми_строками(self):
        from backend.openai_manager import _parse_subtitle_translations

        with self.assertRaises(Exception):
            _parse_subtitle_translations("не json вовсе", 2)
        with self.assertRaises(ValueError):
            _parse_subtitle_translations('{"что-то": "другое"}', 2)


class ПереспросОдинРаз(unittest.TestCase):
    def _run(self, answers):
        from backend import openai_manager

        calls = []

        async def fake_llm(**kwargs):
            calls.append(json.loads(kwargs["user_message"]))
            return answers[len(calls) - 1]

        recorded = []
        with mock.patch.object(openai_manager, "llm_execute", fake_llm), \
             mock.patch("backend.subtitle_translate_counter.record_batch",
                        side_effect=lambda **kw: recorded.append(kw)):
            result = asyncio.run(openai_manager.run_translate_subtitles_ru(["eins", "zwei"]))
        return result, calls, recorded

    def test_со_второго_раза_получилось(self):
        result, calls, recorded = self._run([
            '{"translations": ["один и два"]}',          # сбилась со счёта
            '{"translations": ["один", "два"]}',          # переспросили — попала
        ])
        self.assertEqual(result, ["один", "два"])
        self.assertEqual(len(calls), 2, "переспрашиваем ровно один раз")
        self.assertNotIn("must_return_exactly", calls[0])
        self.assertEqual(calls[1]["must_return_exactly"], 2,
                         "во второй раз число названо прямо в задании")
        self.assertEqual(recorded, [{"mismatched": True, "refused": False}])

    def test_с_первого_раза_попала_переспроса_нет(self):
        result, calls, recorded = self._run(['{"translations": ["один", "два"]}'])
        self.assertEqual(result, ["один", "два"])
        self.assertEqual(len(calls), 1)
        self.assertEqual(recorded, [{"mismatched": False, "refused": False}])

    def test_не_попала_дважды_пачка_отвергается(self):
        from backend.openai_manager import SubtitleTranslationCountMismatch

        with self.assertRaises(SubtitleTranslationCountMismatch):
            self._run(['{"translations": ["одна"]}', '{"translations": ["снова одна"]}'])

    def test_отказ_попадает_в_счётчик(self):
        from backend import openai_manager

        recorded = []

        async def fake_llm(**kwargs):
            return '{"translations": ["одна"]}'

        with mock.patch.object(openai_manager, "llm_execute", fake_llm), \
             mock.patch("backend.subtitle_translate_counter.record_batch",
                        side_effect=lambda **kw: recorded.append(kw)):
            with self.assertRaises(Exception):
                asyncio.run(openai_manager.run_translate_subtitles_ru(["eins", "zwei"]))
        self.assertEqual(recorded, [{"mismatched": True, "refused": True}])


class ЗамкиНаМесте(unittest.TestCase):
    def test_в_эндпоинте_есть_сверка_длин_перед_раскладкой(self):
        text = SERVER.read_text(encoding="utf-8")
        i = text.index("update_map = {}")
        before = text[i - 900:i]
        self.assertIn("if len(translated) != len(missing_indices):", before,
                      "молчаливый zip и был первопричиной сдвига")

    def test_сырая_ошибка_сервера_человеку_не_уходит(self):
        text = SERVER.read_text(encoding="utf-8")
        self.assertNotIn('f"translation error: {exc}"', text)
        self.assertIn("subtitle_translation_count_mismatch", text)

    def test_замена_немецких_реплик_стирает_старый_перевод(self):
        text = DB.read_text(encoding="utf-8")
        i = text.index("INSERT INTO bt_3_youtube_transcripts (video_id, items, language")
        block = text[i:i + 2600]
        self.assertIn("IS DISTINCT FROM EXCLUDED.items", block)
        self.assertIn("THEN '{}'::jsonb", block)

    def test_обрыв_перевода_в_браузере_не_молчит(self):
        text = APP_JSX.read_text(encoding="utf-8")
        i = text.index("'/api/webapp/youtube/translate_rows'")
        block = text[i:i + 3000]
        self.assertNotIn(".catch(() => {})", block,
                         "пустой catch оставлял человека с многоточиями навсегда")
        self.assertIn("setYoutubeTranslationNotice", block)

    def test_непереведённое_и_переведённое_пустым_это_разные_состояния(self):
        # Иначе плеер бесконечно перезаказывает одно и то же место, а человек так
        # ничего и не видит.
        text = APP_JSX.read_text(encoding="utf-8")
        i = text.index("'/api/webapp/youtube/translate_rows'")
        block = text[i - 1600:i]
        self.assertIn("=== undefined", block)


if __name__ == "__main__":
    unittest.main()
