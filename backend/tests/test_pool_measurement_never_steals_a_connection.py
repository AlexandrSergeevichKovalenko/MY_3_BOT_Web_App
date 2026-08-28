# -*- coding: utf-8 -*-
"""Замер пула не отнимает соединение у живой работы, а сигнал называет виновника.

ПОВОД — НАСТОЯЩЕЕ ПАДЕНИЕ В ПРОДЕ 28.08.2026. Владельцу пришло:

    🛑 Запросы ПАДАЮТ: базе не хватает соединений
    5 запрос(ов) за последние ~30 мин прождали свободное соединение все 1500 мс и не
    дождались — упали с ошибкой «DB pool exhausted». Человек на экране увидел ошибку.
    Кто именно не дождался — НЕ ЗАПИСАНО: у этих мест нет метки db_acquire_scope().

Разбор: голод только у MY_3_BOT (пул 8) — 11 случаев за сутки, у остальных пяти
сервисов ноль. Бот перезапустился в 20:17 (деплой) и на старте делает залп работы с
базой; пул из восьми залпа не выдержал.

⛔ МОЯ ЧАСТЬ ВИНЫ ПРЯМАЯ. Замер занятости, который я добавил в тот же день,
вызывается ИЗНУТРИ пути получения соединения (`_record_db_acquire_event`) и раз в
минуту брал ВТОРОЕ соединение из того же пула, НЕ ВЫПУСТИВ ПЕРВОГО. При полном пуле
он сам становился тем, кто не дождался, и отнимал место у живого запроса.

ДВА ПРАВИЛА, КОТОРЫЕ ЗДЕСЬ СТЕРЕГУТСЯ:
  1. замер пишет из ОТДЕЛЬНОГО ПОТОКА — путь получения соединения его не ждёт;
  2. свободных мест в пуле нет — замер НЕ ПИШЕТ ВООБЩЕ. Потерянный замер стоит строки
     в логе; отнятое соединение — ошибки у человека на экране.

Плюс сигнал научился называть виновника: метки `db_acquire_scope()` есть не везде и
никогда не будут везде (вызывающих сотни), поэтому при голоде снимается короткий след
стека. Голод редок — порог 5 событий за 30 минут, — на горячий путь это не попадает.
"""
import os
import threading
import unittest
from unittest import mock

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")

from backend import database as db  # noqa: E402


class ЗамерНеОтнимаетСоединение(unittest.TestCase):

    def setUp(self):
        db._POOL_PEAK_FLUSH_LAST = 0.0
        db._POOL_PEAK_SEEN = 0

    def test_пул_забит_замер_молчит(self):
        """Ровно тот случай, что уронил запросы: писать при нуле свободных нельзя."""
        with mock.patch.object(db, "record_capacity") as запись, \
             mock.patch.object(threading, "Thread") as поток:
            db._note_pool_usage_peak(8, pool_available_count=0)
        запись.assert_not_called()
        поток.assert_not_called()

    def test_место_есть_пишем_но_ОТДЕЛЬНЫМ_потоком(self):
        """Путь получения соединения не должен ждать наш замер."""
        with mock.patch.object(threading, "Thread") as поток:
            db._note_pool_usage_peak(5, pool_available_count=3)
        поток.assert_called_once()
        self.assertTrue(поток.call_args.kwargs.get("daemon"),
                        "поток замера обязан быть демоном — он не должен держать выход")

    def test_замер_помечает_себя_меткой(self):
        """Иначе следующее письмо о голоде снова не назовёт виновника."""
        import inspect
        код = inspect.getsource(db._note_pool_usage_peak)
        self.assertIn('db_acquire_scope("capacity_measurement_flush")', код)

    def test_вызов_из_пути_получения_передаёт_свободные_места(self):
        import inspect
        код = inspect.getsource(db._record_db_acquire_event)
        self.assertIn("_note_pool_usage_peak(pool_used_count, pool_available_count)", код)


class СигналНазываетВиновника(unittest.TestCase):

    def test_след_стека_снимается_и_читается(self):
        """Проверяем из файла внутри backend/ — именно такие кадры и ловим."""
        след = db._откуда_позвали()
        self.assertTrue(след, "след пуст — письмо снова не назовёт виновника")
        self.assertIn(":", след)

    def test_сам_слой_базы_в_виновники_не_попадает(self):
        """Интересен тот, кто ПРИШЁЛ за соединением, а не путь внутри database.py."""
        self.assertNotIn("database.py", db._откуда_позвали())

    def test_без_метки_виновник_всё_равно_записывается(self):
        import inspect
        код = inspect.getsource(db._maybe_alert_db_pool_saturation)
        self.assertIn("_откуда_позвали()", код)
        self.assertIn('имя == "unspecified"', код)


if __name__ == "__main__":
    unittest.main()
