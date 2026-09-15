"""Примеры тренажёра не должны противоречить сами себе.

Повод (замер по живому банку 15.09.2026). Предложение для партнёра строится подстановкой
его в фразу ГОЛОВНОГО слова. У синонима смысл тот же — подстановка законна. У антонима
смысл переворачивается, а остаток фразы продолжает нести прежний: придаточное причины
(weil), цели (um…zu), уступки (trotz), условия (wenn), отрицание, immer/nie. Выходило:

    Die Prüfung wurde als einwandfrei bewertet, weil viele Fehler gemacht wurden.
    «Экзамен был оценён как безупречный, потому что было сделано много ошибок.»

Прочитано вручную 14 случайных антонимов — 6 противоречат себе; у синонимов 0 из 14.
Корень был в самой инструкции модели: она требовала «менять ТОЛЬКО то, чего требует
подстановка», то есть прямо запрещала чинить логику.

Чинилось двумя половинами, и тесты держат обе:
  • инструкция переписана (модель обязана переворачивать и контекст);
  • на выходе стоит судья, который обязан вернуть ИСПРАВЛЕННЫЙ текст, а его молчание
    НЕ выдаётся за «проверено».
"""
import asyncio
import json
import unittest
from unittest.mock import patch

from backend import openai_manager as om
from backend import sprint_distractors as sd


def _run(coro):
    # asyncio.run, а НЕ get_event_loop(): в общем прогоне соседние тесты закрывают
    # текущий цикл, и get_event_loop() падает с «There is no current event loop».
    # Поодиночке файл при этом оставался зелёным — ровно та ловушка, из-за которой
    # «тесты зелёные» нельзя говорить, не прогнав всю папку.
    return asyncio.run(coro)


class ИнструкцияГенерации(unittest.TestCase):
    def test_запрет_менять_только_подстановку_снят(self):
        txt = om.system_message["sprint_correct_examples"]
        self.assertNotIn("change ONLY what the swap requires", txt,
                         "эта фраза и запрещала модели чинить логику предложения")

    def test_инструкция_требует_перевернуть_контекст_у_антонима(self):
        txt = om.system_message["sprint_correct_examples"]
        for нужное in ("weil", "trotz", "immer/nie", "POLARITY"):
            self.assertIn(нужное, txt, f"в инструкции нет ориентира «{нужное}»")
        # живой пример дефекта стоит прямо в инструкции как «так нельзя»
        self.assertIn("weil viele Fehler gemacht wurden", txt)


class СудьяПримеров(unittest.TestCase):
    ПРИМЕРЫ = [
        {"word": "einwandfrei",
         "sentence_de": "Die Prüfung wurde als einwandfrei bewertet, weil viele Fehler gemacht wurden.",
         "sentence_ru": "Экзамен был оценён как безупречный, потому что было сделано много ошибок."},
        {"word": "fehlerfrei",
         "sentence_de": "Die Prüfung war fehlerfrei.",
         "sentence_ru": "Экзамен был безошибочным."},
    ]

    def _ответ(self, results):
        async def fake(**kwargs):
            return json.dumps({"results": results}, ensure_ascii=False)
        return fake

    def test_чинит_противоречие_и_помечает_починку(self):
        results = [
            {"index": 0, "ok": False,
             "sentence_de": "Die Prüfung wurde als einwandfrei bewertet, weil keine Fehler gefunden wurden.",
             "sentence_ru": "Экзамен был оценён как безупречный, потому что ошибок не нашлось.",
             "why": "причина противоречила оценке"},
            {"index": 1, "ok": True,
             "sentence_de": "Die Prüfung war fehlerfrei.",
             "sentence_ru": "Экзамен был безошибочным."},
        ]
        with patch.object(om, "llm_execute", self._ответ(results)):
            out = _run(om.run_check_example_consistency(items=self.ПРИМЕРЫ))
        self.assertEqual(len(out), 2)
        self.assertTrue(out[0]["repaired"])
        self.assertIn("keine Fehler", out[0]["sentence_de"])
        self.assertFalse(out[1]["repaired"])

    def test_ответ_собирается_по_index_а_не_по_порядку(self):
        # Модель вернула элементы задом наперёд. Позиционная сборка приклеила бы
        # вердикт чужого слова — тот же урок, что записан у run_article_verify.
        results = [
            {"index": 1, "ok": True, "sentence_de": "Die Prüfung war fehlerfrei.",
             "sentence_ru": "Экзамен был безошибочным."},
            {"index": 0, "ok": False,
             "sentence_de": "Die Prüfung wurde als einwandfrei bewertet, weil keine Fehler gefunden wurden.",
             "sentence_ru": "Починено.", "why": "причина противоречила оценке"},
        ]
        with patch.object(om, "llm_execute", self._ответ(results)):
            out = _run(om.run_check_example_consistency(items=self.ПРИМЕРЫ))
        self.assertEqual(out[0]["word"], "einwandfrei")
        self.assertTrue(out[0]["repaired"])
        self.assertEqual(out[1]["word"], "fehlerfrei")
        self.assertFalse(out[1]["repaired"])

    def test_неполный_ответ_не_отдаётся_как_проверенный(self):
        # Пришёл вердикт только на одно слово из двух: второе осталось бы
        # непроверенным, но выглядело бы проверенным. Наружу — пусто.
        results = [{"index": 0, "ok": True, "sentence_de": "A.", "sentence_ru": "А."}]
        with patch.object(om, "llm_execute", self._ответ(results)):
            out = _run(om.run_check_example_consistency(items=self.ПРИМЕРЫ))
        self.assertEqual(out, [])

    def test_починка_без_текста_не_считается_починкой(self):
        results = [
            {"index": 0, "ok": False, "sentence_de": "", "sentence_ru": "", "why": "плохо"},
            {"index": 1, "ok": True, "sentence_de": "Die Prüfung war fehlerfrei.",
             "sentence_ru": "Экзамен был безошибочным."},
        ]
        with patch.object(om, "llm_execute", self._ответ(results)):
            out = _run(om.run_check_example_consistency(items=self.ПРИМЕРЫ))
        self.assertEqual(out[0]["sentence_de"], self.ПРИМЕРЫ[0]["sentence_de"])
        self.assertFalse(out[0]["repaired"])

    def test_отказ_модели_даёт_пустой_список(self):
        async def упал(**kwargs):
            raise RuntimeError("сеть")
        with patch.object(om, "llm_execute", упал):
            self.assertEqual(_run(om.run_check_example_consistency(items=self.ПРИМЕРЫ)), [])


class ФлагПроверки(unittest.TestCase):
    def test_непроверенное_не_становится_проверенным(self):
        tj = sd.trainer_json_from_result({"kept": [], "correct_examples": [{"word": "x"}]})
        self.assertIn("examples_checked", tj)
        self.assertFalse(tj["examples_checked"])

    def test_проверенное_переносится_в_банк(self):
        tj = sd.trainer_json_from_result(
            {"kept": [], "correct_examples": [{"word": "x"}], "examples_checked": True})
        self.assertTrue(tj["examples_checked"])


class Биллинг(unittest.TestCase):
    def test_проверка_примеров_это_общая_работа_а_не_расход_человека(self):
        self.assertIsNone(om._resolve_billing_user_for_task("sprint_example_consistency", 117649764))
