"""Прогон тестов не имеет права оставить след в боевой ведомости.

На машине разработчика лежат боевые креденшелы (см. conftest). Раньше это значило, что
каждый локальный `pytest` дописывал строки в живую таблицу расходов: за неделю набежало
1010 фантомных «обращений к OpenAI» от тестовых пользователей 123 и 456 и ещё 108 от
судьи кроссвордов. В ежедневном отчёте это выглядело как 1037 обращений вместо 27
настоящих — то есть свои же тесты врали владельцу про расход.

Перехватить это patch'ем внутри теста нельзя: запись уходит в демон-поток, который
живёт дольше самого теста. Поэтому запрет стоит в самом `log_billing_event` и включается
переменной окружения из conftest.
"""
import os
import unittest

from backend import database as db


class BillingLedgerTestIsolationTests(unittest.TestCase):
    def test_conftest_switched_the_ledger_off_for_the_whole_run(self):
        self.assertEqual(
            (os.getenv("SKIP_BILLING_LEDGER_WRITES") or "").strip().lower(),
            "1",
            "conftest обязан выключать запись в ведомость до импорта приложения",
        )

    def test_log_billing_event_writes_nothing_while_the_switch_is_on(self):
        # Никаких моков БД: если запрет не сработает, вызов пойдёт в настоящую базу и
        # тест это заметит — вернётся строка вместо None (а в боевой ведомости появится
        # мусор, что и есть та самая ошибка).
        result = db.log_billing_event(
            idempotency_key="test-isolation-probe",
            user_id=456,
            action_type="shortcut_split",
            provider="openai",
            units_type="requests",
            units_value=1.0,
        )
        self.assertIsNone(result)

    def test_internal_bookkeeping_still_writes_even_with_the_switch_on(self):
        # Счётчик бесплатных лимитов (increment_free_feature_usage) идёт через ту же
        # функцию с provider='app_internal' и ПАДАЕТ, если запись вернёт None. Запрет
        # обязан пропускать такие строки, иначе тесты лимитов увидят чужое поведение.
        from unittest.mock import patch

        with patch.object(db, "get_db_connection_context") as conn_mock:
            conn_mock.side_effect = RuntimeError("должны были дойти до записи в БД")
            with self.assertRaises(RuntimeError):
                db.log_billing_event(
                    idempotency_key="test-isolation-probe-internal",
                    user_id=77,
                    action_type="shortcut_forwarded_message_daily",
                    provider="app_internal",
                    units_type="requests",
                    units_value=1.0,
                )

    def test_the_switch_is_off_by_default_so_production_keeps_writing(self):
        # Обратная сторона: без переменной запись обязана идти как обычно, иначе мы
        # молча потеряли бы весь учёт расхода в проде.
        from unittest.mock import patch

        with patch.dict(os.environ, {"SKIP_BILLING_LEDGER_WRITES": ""}, clear=False):
            with patch.object(db, "get_db_connection_context") as conn_mock:
                conn_mock.side_effect = RuntimeError("должны были дойти до записи в БД")
                with self.assertRaises(RuntimeError):
                    db.log_billing_event(
                        idempotency_key="test-isolation-probe-off",
                        user_id=456,
                        action_type="shortcut_split",
                        provider="openai",
                        units_type="requests",
                        units_value=1.0,
                    )


if __name__ == "__main__":
    unittest.main()
