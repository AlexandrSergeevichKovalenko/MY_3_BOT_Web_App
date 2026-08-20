"""Прогон тестов не открывает соединение с боевой базой — ни одного.

19–20.08.2026 пуш дважды подряд объявлялся красным на совершенно зелёном коде.
Разбор: `bot_3.py` при импорте делал проверку «база жива» (`SELECT version()`).
В сервисе это правильно — не те креденшелы, и бот обязан упасть сразу. Но
`import bot_3` стоит в десятках тестовых модулей, а в окружении разработчика лежат
БОЕВЫЕ креденшелы, поэтому каждый прогон стучался в живую базу. Когда публичный
прокси моргал, падал весь СБОР тестов с psycopg2 timeout — то есть красным
становился не код, а сеть. Цена каждого такого случая — два прогона по две минуты
и непонимание, что вообще сломалось.

Правило репозитория «тесты не должны трогать боевую базу» существовало и раньше,
и под него уже был заведён флаг `SKIP_STARTUP_SCHEMA_BOOTSTRAP` (его ставит
`backend/tests/conftest.py`). Он глушил стартовые фазы `backend_server`, но не
проверку соединения в `bot_3` — дыра была ровно в одну строку.

Тест запускает ОТДЕЛЬНЫЙ процесс: внутри самого прогона модули давно импортированы,
и повторный `import` ничего не выполнит. В подменённом процессе `psycopg2.connect`
падает с ошибкой, поэтому любая попытка соединения станет видна сразу.
"""
import os
import subprocess
import sys
import unittest

_REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Тот же набор переменных, что ставит conftest тестового прогона.
_PROBE = """
import sys, traceback
import psycopg2

def refuse(*a, **k):
    raise AssertionError("СОЕДИНЕНИЕ С БАЗОЙ ПРИ ИМПОРТЕ:\\n" + "".join(
        f for f in traceback.format_stack()[:-1] if "site-packages" not in f))

psycopg2.connect = refuse
import {module}
print("OK")
"""


class ImportingTheAppOpensNoConnectionTests(unittest.TestCase):
    def _import_in_subprocess(self, module: str) -> subprocess.CompletedProcess:
        env = dict(os.environ)
        env.update({
            "SKIP_STARTUP_SCHEMA_BOOTSTRAP": "1",
            "SKIP_BILLING_LEDGER_WRITES": "1",
            "SKIP_TASK_ROTATION_WRITES": "1",
            "PYTHONPATH": _REPO,
        })
        return subprocess.run(
            [sys.executable, "-c", _PROBE.format(module=module)],
            cwd=_REPO, env=env, capture_output=True, text=True, timeout=300,
        )

    def test_importing_bot_3_opens_no_database_connection(self):
        done = self._import_in_subprocess("bot_3")
        self.assertIn("OK", done.stdout,
                      "импорт bot_3 полез в базу:\n" + done.stderr[-3000:])

    def test_importing_backend_server_opens_no_database_connection(self):
        done = self._import_in_subprocess("backend.backend_server")
        self.assertIn("OK", done.stdout,
                      "импорт backend_server полез в базу:\n" + done.stderr[-3000:])


if __name__ == "__main__":
    unittest.main()
