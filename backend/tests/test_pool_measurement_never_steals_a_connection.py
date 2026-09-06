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


class ПисьмоНеПоказываетПальцемНаЖертву(unittest.TestCase):
    """Переписано 06.09.2026 по прямой просьбе владельца.

    Письмо 05.09 назвало виновником `_record_sched_heartbeat` (он всего лишь пришёл за
    соединением последним) и посоветовало поднять DB_POOL_MAXCONN — совет, который в
    нашем проде делает ХУЖЕ: настоящим горлышком был потолок PgBouncer
    MAX_DB_CONNECTIONS=4 на всё приложение. Владелец: «А какой у нас тогда слабое
    место? что ты имеешь в виду?» Обе неправды закрыты тестом, чтобы не вернулись.
    """

    def setUp(self):
        db._DB_LONG_HOLD_EVENTS.clear()

    @staticmethod
    def _голод():
        import time as _t
        сейчас = _t.time()
        return [(сейчас, "failed", "bot_3.py:11474 _record_sched_heartbeat")] * 5

    def test_совет_поднять_пул_сервиса_НЕ_возвращается(self):
        письмо = db._build_db_pool_starvation_message(self._голод())
        self.assertNotIn("поднять DB_POOL_MAXCONN", письмо,
                         "этот совет расширяет приёмную перед той же дверью — он вреден")

    def test_пришедшего_называют_пострадавшим_а_не_виновником(self):
        письмо = db._build_db_pool_starvation_message(self._голод())
        self.assertIn("Кого срезало", письмо)
        self.assertIn("пострадавшие", письмо)
        self.assertNotIn("Кто не дождался", письмо)

    def test_держали_долго_письмо_ведёт_в_лог_медленных_запросов(self):
        db._note_long_hold(118_000, "unspecified")
        db._note_long_hold(70_000, "unspecified")
        письмо = db._build_db_pool_starvation_message(self._голод())
        self.assertIn("Кто держал", письмо)
        self.assertIn("118 с", письмо, "самое долгое удержание обязано быть названо")
        self.assertIn("duration:", письмо, "иначе владельцу негде искать виновный запрос")
        self.assertIn("бесполезно", письмо)

    def test_не_держали_письмо_ведёт_к_общему_потолку_pgbouncer(self):
        письмо = db._build_db_pool_starvation_message(self._голод())
        self.assertIn("НЕ БЫЛО", письмо)
        self.assertIn("MAX_DB_CONNECTIONS", письмо,
                      "общий потолок делится между всеми сервисами — вот куда смотреть")

    def test_короткое_удержание_виновником_не_делает(self):
        db._note_long_hold(1_600, "unspecified")   # длинное по логу, но не по вине
        письмо = db._build_db_pool_starvation_message(self._голод())
        self.assertIn("НЕ БЫЛО", письмо,
                      "1.6 с для тяжёлого запроса — норма, виновником это не делает")

    def test_длинное_удержание_вправду_запоминается_на_возврате(self):
        import inspect
        код = inspect.getsource(db._record_db_checkout_return_event)
        self.assertIn("_note_long_hold(", код,
                      "без записи на возврате письмо снова не отличит две причины")


if __name__ == "__main__":
    unittest.main()
