# -*- coding: utf-8 -*-
"""Единица перевода — предложение, и возвращается оно под своим ярлыком.

ПОВОД, 15.09.2026, решение владельца «делаем как у лидеров». Кадр субтитра — обрывок
(«dich mal so am Beckenrand festhalten»). Немецкий ставит глагол в конец, русский
порядок слов другой, и перевести обрывок отдельно нельзя. Модель отвечает на это
склейкой соседних строк — то есть покадровый перевод ЗАСТАВЛЯЕТ её ошибаться, а
раскладка по позиции в списке превращает ошибку в сдвиг всех последующих строк.

Здесь закреплено:
  • реплики собираются в предложения по немецкой пунктуации и паузам В ЗВУКЕ;
  • длина русского текста в решении о границе НЕ участвует (иначе абзацы прыгают,
    когда доезжает перевод) — правило смотрит только на немецкий;
  • у строки настоящее время начала и конца, а не вычисленная доля;
  • перевод раскладывается ПО ЯРЛЫКУ; потерялся ярлык или появился лишний — пачка
    отвергается целиком, наугад ничего не раскладывается;
  • группировки в браузере больше нет — правило живёт в одном экземпляре.
"""
import asyncio
import json
import pathlib
import unittest
from unittest import mock

from backend.subtitle_cues import group_cues_into_sentences, row_id, split_row_translation_key

ROOT = pathlib.Path(__file__).resolve().parents[2]
APP_JSX = ROOT / "frontend" / "src" / "App.jsx"
SERVER = ROOT / "backend" / "backend_server.py"


def _cues(pairs):
    """[(text, start, duration)] → реплики."""
    return [{"text": t, "start": s, "duration": d} for t, s, d in pairs]


class СборкаПредложений(unittest.TestCase):
    def test_точка_заканчивает_строку(self):
        rows = group_cues_into_sentences(_cues([
            ("Ich sag, nein.", 0.0, 2.0),
            ("Das ist die Thrillerpfeife.", 2.0, 2.0),
        ]))
        self.assertEqual([r["text"] for r in rows],
                         ["Ich sag, nein.", "Das ist die Thrillerpfeife."])
        self.assertEqual([r["id"] for r in rows], ["0-0", "1-1"])

    def test_обрывки_одной_фразы_собираются_в_одну_строку(self):
        rows = group_cues_into_sentences(_cues([
            ("und der hat ja", 0.0, 2.0),
            ("so eine weite Hose, wenn", 2.0, 2.0),
            ("der dann noch mal guckt.", 4.0, 2.0),
        ]))
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["id"], "0-2")
        self.assertEqual(rows[0]["text"],
                         "und der hat ja so eine weite Hose, wenn der dann noch mal guckt.")

    def test_у_строки_настоящее_время_начала_и_конца(self):
        rows = group_cues_into_sentences(_cues([
            ("und der hat ja", 10.0, 2.0),
            ("eine weite Hose.", 12.0, 3.0),
        ]))
        self.assertEqual(rows[0]["start"], 10.0)
        self.assertEqual(rows[0]["end"], 15.0)

    def test_длинная_пауза_режет_строку(self):
        rows = group_cues_into_sentences(_cues([
            ("und der hat ja", 0.0, 2.0),
            ("weiter geht es", 10.0, 2.0),   # пауза 8 секунд
        ]))
        self.assertEqual(len(rows), 2)

    def test_строчная_буква_в_начале_следующего_кадра_не_даёт_резать(self):
        # Пауза 1.2 с — сама по себе повод разрезать, но фраза явно продолжается.
        rows = group_cues_into_sentences(_cues([
            ("und der hat ja", 0.0, 2.0),
            ("eine weite Hose", 3.2, 2.0),
        ]))
        self.assertEqual(len(rows), 1)

    def test_русский_текст_на_границы_не_влияет(self):
        # Границы обязаны зависеть ТОЛЬКО от немецкого: иначе абзац перескакивает в
        # момент, когда доезжает перевод.
        import inspect

        from backend import subtitle_cues
        source = inspect.getsource(subtitle_cues.group_cues_into_sentences)
        self.assertNotIn("translation", source)

    def test_пустые_кадры_своей_строки_не_заводят(self):
        rows = group_cues_into_sentences(_cues([
            ("Hallo.", 0.0, 2.0), ("   ", 2.0, 2.0), ("Tschüss.", 4.0, 2.0),
        ]))
        self.assertEqual([r["text"] for r in rows], ["Hallo.", "Tschüss."])

    def test_ярлык_это_номера_первой_и_последней_реплики(self):
        self.assertEqual(row_id(12, 15), "12-15")
        self.assertEqual(split_row_translation_key("ru#12-15"), ("ru", "12-15"))
        self.assertIsNone(split_row_translation_key("ru:17"), "покадровый ключ — не ярлык строки")
        self.assertIsNone(split_row_translation_key("ru#мусор"))


class РаскладкаПоЯрлыку(unittest.TestCase):
    ROWS = [{"id": "0-1", "text": "Ich sag, nein."}, {"id": "2-4", "text": "Das ist die Pfeife."}]

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
            result = asyncio.run(openai_manager.run_translate_subtitle_rows(
                rows=self.ROWS, source_lang="de", target_lang="ru"))
        return result, calls, recorded

    def test_порядок_в_ответе_не_важен_важен_ярлык(self):
        result, _calls, _rec = self._run(['{"rows": ['
                                          '{"id": "2-4", "translation": "Это тот свисток."},'
                                          '{"id": "0-1", "translation": "Я говорю, нет."}]}'])
        self.assertEqual(result, {"0-1": "Я говорю, нет.", "2-4": "Это тот свисток."})

    def test_потерянный_ярлык_отвергает_пачку(self):
        from backend.openai_manager import SubtitleRowLabelsMismatch

        with self.assertRaises(SubtitleRowLabelsMismatch):
            self._run(['{"rows": [{"id": "0-1", "translation": "только одна"}]}'] * 2)

    def test_выдуманный_ярлык_отвергает_пачку(self):
        from backend.openai_manager import SubtitleRowLabelsMismatch

        answer = ('{"rows": [{"id": "0-1", "translation": "а"}, {"id": "2-4", "translation": "б"},'
                  '{"id": "9-9", "translation": "откуда?"}]}')
        with self.assertRaises(SubtitleRowLabelsMismatch):
            self._run([answer, answer])

    def test_переспрашиваем_один_раз_и_называем_ярлыки(self):
        result, calls, recorded = self._run([
            '{"rows": [{"id": "0-1", "translation": "только одна"}]}',
            '{"rows": [{"id": "0-1", "translation": "а"}, {"id": "2-4", "translation": "б"}]}',
        ])
        self.assertEqual(result, {"0-1": "а", "2-4": "б"})
        self.assertEqual(len(calls), 2)
        self.assertNotIn("must_return_ids", calls[0])
        self.assertEqual(calls[1]["must_return_ids"], ["0-1", "2-4"])
        self.assertEqual(recorded, [{"mismatched": True, "refused": False}])


class ПравилоЖивётВОдномЭкземпляре(unittest.TestCase):
    def test_браузер_больше_не_группирует(self):
        text = APP_JSX.read_text(encoding="utf-8")
        self.assertNotIn("shouldFlush", text,
                         "правила «где кончается фраза» вернулись в браузер")
        self.assertNotIn("hardStopPattern", text)

    def test_запрет_записан_рядом_в_коде(self):
        text = APP_JSX.read_text(encoding="utf-8")
        self.assertIn("ГРУППИРОВКУ В ПРЕДЛОЖЕНИЯ СЮДА НЕ ВОЗВРАЩАТЬ", text)

    def test_сервер_отдаёт_строки_вместе_с_субтитрами(self):
        # Каждый ответ, несущий реплики, обязан нести и строки: браузер их не собирает,
        # и ответ без rows означает ПУСТУЮ панель субтитров у человека.
        text = SERVER.read_text(encoding="utf-8")
        self.assertEqual(text.count('"rows": _youtube_sentence_rows('), 3,
                         "из кеша, после скачивания и после ручной вставки расшифровки")
        self.assertIn('@app.route("/api/webapp/youtube/translate_rows", methods=["POST"])', text)

    def test_ручная_вставка_расшифровки_возвращает_строки(self):
        text = SERVER.read_text(encoding="utf-8")
        i = text.index("_YT_TRANSCRIPT_CACHE_MAX)\n\n    # Возвращаем реплики")
        block = text[i:i + 700]
        self.assertIn('"rows": _youtube_sentence_rows(', block)

    def test_перевод_строк_считается_отчётом(self):
        from backend.subtitle_sync_report import audit_one_video

        report = audit_one_video({
            "video_id": "rows1", "cues_rolled": True,
            "items": [{"text": "Ich sag, nein.", "start": 0.0, "duration": 2.0},
                      {"text": "Das ist die Pfeife.", "start": 2.0, "duration": 2.0}],
            "translations": {"ru#0-0": "Я говорю, нет.", "ru#1-1": "Это тот свисток."},
        })
        self.assertEqual(report["row_lines"], 2)
        self.assertEqual(report["lines"], 2)
        self.assertEqual(report["orphan"], 0)

    def test_ярлык_за_концом_списка_реплик_это_осиротевший(self):
        from backend.subtitle_sync_report import audit_one_video

        report = audit_one_video({
            "video_id": "rows2", "cues_rolled": True,
            "items": [{"text": "Ich sag, nein.", "start": 0.0, "duration": 2.0}],
            "translations": {"ru#0-0": "Я говорю, нет.", "ru#7-9": "из прошлой нарезки"},
        })
        self.assertEqual(report["orphan"], 1)


if __name__ == "__main__":
    unittest.main()
