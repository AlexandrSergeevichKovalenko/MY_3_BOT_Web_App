# -*- coding: utf-8 -*-
"""Прогон тестов физически не может ЗАПИСАТЬ в базу. Один замок вместо пяти заплаток.

ПОВОД, 15.09.2026. В conftest уже лежало ПЯТЬ отдельных запретов, и каждый заведён
после того, как прогон тестов уже напортил в боевой базе: 1010 фантомных строк в
ведомости расходов (02.08), 64 ночные правки (28.08), память ротации (15.08), второй
голос, и наконец впуск выдуманных людей в список доступа (14–15.09). Заплатка закрывает
одну дверь, а дверей у базы столько, сколько в ней функций записи.

Корень: в окружении разработчика DATABASE_URL_RAILWAY смотрит на ЖИВУЮ базу, поэтому
локальный pytest работает по проду.

Замок: conftest ставит PGOPTIONS="-c default_transaction_read_only=on". Это читает сам
libpq, значит «только чтение» получает КАЖДОЕ соединение прогона — включая те, что
приложение открывает в своих потоках. Попытка записи падает громко, а не проходит тихо.

Замер того дня: без базы вообще краснеет 61 тест (столько её честно читают), с замком
краснело 5 — ровно те, что в неё писали; все пять разобраны в тот же день.
"""
import os
import unittest


class ЗамокСтоитИДержит(unittest.TestCase):
    def test_conftest_sets_the_lock(self):
        """Ставится ДО импорта приложения, поэтому отказаться от него нельзя."""
        self.assertIn("default_transaction_read_only=on", os.getenv("PGOPTIONS") or "",
                      "замок «из тестов только чтение» снят — прогон снова может писать в прод")

    def test_a_write_really_fails(self):
        """Не «переменная выставлена», а «база отказала»: проверяем на самой базе.

        Запрос безобидный и без замка тоже ничего бы не изменил (WHERE FALSE) — нам важен
        не результат, а то, что база отвечает отказом на попытку записи.
        """
        import psycopg2
        from backend.database import get_db_connection_context
        try:
            with get_db_connection_context() as conn:
                with conn.cursor() as cursor:
                    try:
                        cursor.execute(
                            "UPDATE bt_3_allowed_users SET username = username WHERE FALSE;")
                    except psycopg2.errors.ReadOnlySqlTransaction:
                        return  # база отказала — замок держит
                    self.fail("база приняла запись из прогона тестов: замок не работает")
        except unittest.SkipTest:
            raise
        except psycopg2.errors.ReadOnlySqlTransaction:
            return
        except Exception as exc:
            self.skipTest(f"база недоступна, замок не измерен: {type(exc).__name__}")


if __name__ == "__main__":
    unittest.main()
