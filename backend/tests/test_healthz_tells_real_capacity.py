# -*- coding: utf-8 -*-
"""/api/healthz называет РЕАЛЬНУЮ пропускную способность процесса.

ПОВОД. 28.08.2026 поднимали потолок веб-сервиса (4 одновременных запроса → 8) и не
смогли доказать, что он поднялся. Переменные окружения показывают, что ЗАДАНО, а не
что процесс подхватил. Свои настройки приложение пишет в лог только при старте —
Railway к тому моменту отдаёт уже другой хвост. Оставалось ответить владельцу
догадкой, а это в проекте запрещено.

Теперь числа берутся из самого процесса: threads/workers — из его окружения (ровно
те, по которым gunicorn себя построил, см. CMD в Dockerfile.backend), db_pool_max —
настоящая константа слоя базы, а не пересказ переменной.

⚠ ЗАЧЕМ СЛЕДИТЬ ЗА СООТНОШЕНИЕМ: одновременных запросов = workers × threads, а
соединений с базой на процесс — db_pool_max. Если пул меньше числа потоков, лишние
потоки просто стоят в очереди за соединением, и поднятие threads не даёт ничего.
"""
import os
import unittest
from unittest import mock

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")

import backend.backend_server as server  # noqa: E402


class ЗдоровьеНазываетПропускнуюСпособность(unittest.TestCase):

    def _снять(self, **окружение):
        with mock.patch.dict(os.environ, окружение, clear=False):
            return server._runtime_capacity_info()

    def test_одновременные_запросы_это_воркеры_на_потоки(self):
        итог = self._снять(WEB_CONCURRENCY="2", GUNICORN_THREADS="8")
        self.assertEqual(итог["workers"], 2)
        self.assertEqual(итог["threads"], 8)
        self.assertEqual(итог["concurrent_requests"], 16)

    def test_пул_базы_берётся_из_слоя_базы_а_не_из_переменной(self):
        """Переменная говорит, что задано; константа — то, с чем процесс работает."""
        from backend import database
        итог = self._снять()
        self.assertEqual(итог["db_pool_max"], int(database.DB_POOL_MAXCONN))

    def test_переменная_не_число_не_выдаётся_за_число(self):
        """Молчаливая подстановка тут была бы хуже пустоты: мы бы отчитались числом,
        которого в процессе нет, и потолок «подняли» бы на бумаге."""
        итог = self._снять(GUNICORN_THREADS="восемь")
        self.assertIsNone(итог["threads"])
        self.assertIsNone(итог["concurrent_requests"])

    def test_ручка_здоровья_отдаёт_это_наружу(self):
        with server.app.test_client() as клиент:
            ответ = клиент.get("/api/healthz")
        self.assertEqual(ответ.status_code, 200)
        данные = ответ.get_json()
        self.assertTrue(данные.get("ok"))
        self.assertIn("concurrent_requests", данные.get("runtime") or {})


if __name__ == "__main__":
    unittest.main()
