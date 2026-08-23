# -*- coding: utf-8 -*-
"""Сторож озвучки: он должен запускаться и он не должен врать.

Повод (23.08.2026). В базе три месяца висела 31 запись «звук уже делается»: слова
`Haus`, `Wort`, `Spiel` и ещё 26 строк не озвучивались НИ У КОГО — человек жал 🔊 и
слышал тишину. Два отдельных дефекта сложились в один:

1. Сторож, который это замечает, перестал запускаться 01.06.2026, когда у веб-процесса
   отобрали фоновые задачи (`backend/web_service.py`). Никто не заметил: молчащий
   сторож выглядит ровно как сторож, которому не о чем доложить.
2. Когда он ещё работал, в письме печаталось «Recovery checked: 0 / requeued: 0» —
   и ровно то же самое печаталось, когда расклинивание было ВЫКЛЮЧЕНО рубильником и
   не запускалось вовсе. Ноль «проверил и чисто» неотличим от нуля «никто не смотрел».

Тесты ниже держат обе половины.
"""
import io
import os
import unittest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _source(relative_path: str) -> str:
    return io.open(os.path.join(REPO_ROOT, relative_path), encoding="utf-8").read()


class WatchdogIsActuallyScheduledTests(unittest.TestCase):
    """Проверяем цепочку запуска: планировщик -> актёр -> обёртка -> сама проверка."""

    def test_scheduler_service_dispatches_the_watchdog(self):
        source = _source("backend/scheduler_service.py")
        self.assertIn("_dispatch_tts_admin_alerts", source,
                      "планировщик перестал отправлять сторожа озвучки")
        self.assertIn("run_tts_admin_alerts_actor", source,
                      "планировщик не знает актёра сторожа")

    def test_worker_has_the_actor(self):
        source = _source("backend/background_jobs.py")
        self.assertIn("def run_tts_admin_alerts_actor", source,
                      "у воркера нет актёра сторожа — отправлять некому")

    def test_wrapper_exists_and_is_importable(self):
        from backend import tts_scheduler
        self.assertTrue(hasattr(tts_scheduler, "run_tts_admin_alerts_scheduler_job"))


class DailyDigestIsHumanAndWiredTests(unittest.TestCase):
    """Сводка по озвучке: раз в день, по-русски, без выдуманных чисел.

    До 23.08.2026 это были сорок строк английского лога дважды в день, и они вообще
    не отправлялись — задача умерла вместе со сторожем. Владелец попросил пять строк
    человеческим языком и одну отправку в сутки.
    """

    def test_scheduler_service_sends_the_digest_once_a_day(self):
        source = _source("backend/scheduler_service.py")
        self.assertIn("_dispatch_tts_admin_digest", source,
                      "суточная сводка не отправляется планировщиком")
        self.assertIn("TTS_ADMIN_DIGEST_HOUR", source,
                      "у сводки нет часа отправки — значит она не раз в день")

    def test_evening_send_stays_removed(self):
        source = _source("backend/backend_server.py")
        self.assertNotIn("TTS_ADMIN_DIGEST_EVENING_HOUR", source,
                         "вернулась вторая отправка за день — владелец её отменил")

    def test_worker_has_the_digest_actor(self):
        source = _source("backend/background_jobs.py")
        self.assertIn("def run_tts_admin_digest_actor", source)

    def test_money_is_not_printed_as_a_bare_zero_dollar(self):
        from backend.backend_server import _format_money_usd
        # $0.02 глазами читается как ноль. Копейки показываем копейками.
        self.assertEqual(_format_money_usd(0.0198), "2,0 ¢")
        self.assertEqual(_format_money_usd(0), "0 ¢")
        self.assertTrue(_format_money_usd(2.5).startswith("$"))

    def test_plural_reads_like_russian(self):
        from backend.backend_server import _plural_ru
        self.assertEqual(_plural_ru(1, "знак", "знака", "знаков"), "знак")
        self.assertEqual(_plural_ru(2172, "знак", "знака", "знаков"), "знака")
        self.assertEqual(_plural_ru(15, "знак", "знака", "знаков"), "знаков")
        self.assertEqual(_plural_ru(111, "знак", "знака", "знаков"), "знаков")


class RecoveryOutcomeNeverLiesTests(unittest.TestCase):
    """Текст про расклинивание обязан отличать «проверил» от «не запускался»."""

    @classmethod
    def setUpClass(cls):
        from backend.backend_server import _describe_tts_recovery_outcome
        cls.describe = staticmethod(_describe_tts_recovery_outcome)

    def test_disabled_recovery_says_so_instead_of_printing_zeros(self):
        text = self.describe({"ok": True, "skipped": True, "reason": "disabled"})
        self.assertIn("ВЫКЛЮЧЕНО", text)
        self.assertNotIn("проверено застрявших", text)

    def test_crashed_recovery_is_not_reported_as_a_clean_run(self):
        text = self.describe(None)
        self.assertIn("НЕ ОТРАБОТАЛО", text)

    def test_real_run_reports_its_numbers(self):
        text = self.describe({"ok": True, "attempted": 31, "queued": 31, "duplicates": 0})
        self.assertIn("31", text)
        self.assertIn("отработало", text)

    def test_queue_refusals_are_not_hidden_among_duplicates(self):
        # Отказ брокера означает «не делается никто»; дубль означает «уже делают».
        # Складывать их нельзя: получится бодрый отчёт о работе, которой нет.
        text = self.describe({"ok": True, "attempted": 5, "queued": 0, "duplicates": 0,
                              "errors": 5, "error_reason": "redis_unavailable"})
        self.assertIn("НЕ ПРИНЯЛА ОЧЕРЕДЬ", text)
        self.assertIn("redis_unavailable", text)


class QueueTellsOutageApartFromDuplicateTests(unittest.TestCase):
    """Недоступный Redis не имеет права представляться как «уже делается»."""

    def test_enqueue_reports_redis_outage_separately(self):
        from backend import job_queue
        real_client = job_queue.get_redis_client
        real_flag = job_queue.is_tts_generation_async_enabled
        job_queue.get_redis_client = lambda: None
        job_queue.is_tts_generation_async_enabled = lambda: True
        try:
            result = job_queue.enqueue_tts_generation_job({"cache_key": "abc"})
        finally:
            job_queue.get_redis_client = real_client
            job_queue.is_tts_generation_async_enabled = real_flag
        self.assertFalse(result.get("queued"))
        self.assertEqual(result.get("reason"), "redis_unavailable")


if __name__ == "__main__":
    unittest.main()
