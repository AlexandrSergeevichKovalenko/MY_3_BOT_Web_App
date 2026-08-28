# -*- coding: utf-8 -*-
"""«Дополни предложение»: поддельных заданий больше нет, днём модель не дёргаем.

ПОВОД. Владелец 28.08.2026 попросил показать ему настоящие задания. Прислали три из
базы, и он согласился: «это не очень хорошее задание».

ЧТО БЫЛО. Запасной путь брал неверные варианты ИЗ ТОГО ЖЕ ПРЕДЛОЖЕНИЯ, а когда их не
хватало — из шести вшитых в код глаголов (gehen, machen, geben, nehmen, stellen,
tragen). Замер по живой базе: 162 из 260 сохранённых заданий — 62% — собраны так.

    «das ___ sich nicht»  →  sich · das · eignet · nicht

Все четыре варианта стоят в самом предложении. Решается без знания немецкого.

И это был НЕ редкий сбой: на набор из 15 заданий выделялось 10 обращений к модели,
всё сверх собиралось подделкой сразу — модель даже не спрашивали.

ЗАМЕР НА ЖИВОЙ МОДЕЛИ (то же слово): 1,88 с на задание, результат —
    «Das ___ sich nicht für Kinder.» → eignet · passt · funktioniert · dient
То есть модель делает ровно то, что нужно: близкие по смыслу, но сюда не годятся.
Проблема была не в качестве модели, а в том, что её не спрашивали.

РЕШЕНИЯ ВЛАДЕЛЬЦА 28.08.2026:
  · подделок нет вовсе — не сложилось задание, берём следующее слово;
  · ДНЁМ К МОДЕЛИ НЕ ХОДИМ («мы просто повесим сервер днём»): 15 заданий подряд —
    это 28 секунд ожидания и занятый поток из восьми;
  · сначала выдаём УЖЕ ПРОГРЕТОЕ: «активный тянет пул вперёд, а тот, кто заходит
    редко, получает те же самые готовые — новые для него не греются».
"""
import os
import unittest
from unittest import mock

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")

import backend.backend_server as server  # noqa: E402


class ПодделкиУдаленыИзКодаНавсегда(unittest.TestCase):

    def test_строителя_поддельных_заданий_больше_нет(self):
        self.assertFalse(hasattr(server, "_build_fallback_sentence_context_quiz"),
                         "функция вернулась — вместе с ней вернутся и 62% негодных заданий")

    def test_вшитых_глаголов_в_коде_не_осталось(self):
        import pathlib
        текст = pathlib.Path(server.__file__).read_text(encoding="utf-8")
        self.assertNotIn('random.choice(["gehen", "machen", "geben"', текст)

    def test_нет_кеша_и_нет_права_на_модель_значит_нет_задания(self):
        """Раньше здесь рождалась подделка. Теперь честное «нечего показать»."""
        запись = {"id": 1, "response_json": {}}
        with mock.patch.object(server, "_extract_sentence_training_pair",
                               return_value=("Das eignet sich nicht.", "Не подходит.", {})), \
             mock.patch.object(server, "_looks_like_german_sentence", return_value=True):
            итог = server._build_sentence_quiz_from_dictionary_entry(
                запись, source_lang="ru", target_lang="de", allow_llm=False)
        self.assertIsNone(итог)


class ДнёмКМоделиНеХодим(unittest.TestCase):

    def test_бюджет_живых_обращений_по_умолчанию_ноль(self):
        """1,88 с на задание × 15 = 28 секунд ожидания и занятый поток из восьми."""
        if os.getenv("SENTENCE_TRAINING_LLM_MAX_PER_REQUEST"):
            self.skipTest("значение задано окружением — проверяем только умолчание")
        self.assertEqual(server.SENTENCE_TRAINING_LLM_MAX_PER_REQUEST, 0)

    def test_значение_по_умолчанию_прописано_нулём_в_коде(self):
        import pathlib
        текст = pathlib.Path(server.__file__).read_text(encoding="utf-8")
        self.assertIn('os.getenv("SENTENCE_TRAINING_LLM_MAX_PER_REQUEST") or "0"', текст)


class СначалаВыдаётсяУжеПрогретое(unittest.TestCase):
    """Требование владельца: редко заходящий получает ТЕ ЖЕ готовые задания, а новые
    для него не греются. Задание кешируется на СЛОВЕ — таблица пула не знает про людей,
    поэтому прогретое одним доступно всем, у кого это слово есть."""

    def test_готовые_идут_первыми(self):
        import pathlib
        текст = pathlib.Path(server.__file__).read_text(encoding="utf-8")
        self.assertIn("прогретые, холодные = [], []", текст)
        self.assertIn("dictionary_candidates = прогретые + холодные", текст)


class ПричиныНеудачСчитаютсяПоотдельности(unittest.TestCase):
    """Владелец: «если падает модель, то почему это происходит?» Раньше четыре разные
    причины валились в одну строку лога, которую никто не считал."""

    def setUp(self):
        server.take_sentence_quiz_misses()

    def test_причины_копятся_и_забираются_один_раз(self):
        server._note_sentence_quiz_miss("нет в кеше, к модели не ходим")
        server._note_sentence_quiz_miss("нет в кеше, к модели не ходим")
        server._note_sentence_quiz_miss("наш страж отклонил ответ модели: пусто")
        снимок = server.take_sentence_quiz_misses()
        self.assertEqual(снимок["нет в кеше, к модели не ходим"], 2)
        self.assertEqual(len(снимок), 2)
        self.assertEqual(server.take_sentence_quiz_misses(), {},
                         "забрали — значит обнулили, иначе владелец увидит их дважды")

    def test_отказ_нашего_стража_не_записывается_как_отказ_модели(self):
        """Это не «модель упала» — это мы её забраковали. Разные вещи, разное лечение."""
        server._note_sentence_quiz_miss("наш страж отклонил ответ модели: duplicate options")
        ключи = list(server.take_sentence_quiz_misses().keys())
        self.assertTrue(any("страж" in k for k in ключи))


if __name__ == "__main__":
    unittest.main()
