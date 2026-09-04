# -*- coding: utf-8 -*-
"""Бесплатный месяц: начало отсчёта — одно на всю жизнь и из своего источника.

ПОВОД. Владелец 04.09.2026: постоянного бесплатного тарифа больше нет — 30 дней после
первого контакта, потом «Лайт» или «Полный доступ». Точка отсчёта не сдвигается никогда:
ни выход, ни блокировка бота, ни удаление личных данных, ни повторный вход.
Стратегия — docs/tasks/light_tier_strategy.md.

ЧТО СТЕРЕЖЁТСЯ:
  1. Запись начала — только INSERT … ON CONFLICT DO NOTHING. UPDATE здесь запрещён.
  2. Заливка существующим касается только записей СТАРШЕ дня деплоя — новичку старт
     раньше его первого контакта поставить нельзя.
  3. Удаление личных данных таблицу начала отсчёта не трогает.
  4. Обещание access_period_night_sweep зарегистрировано: страховка ночью не должна
     находить никого, все четыре двери обязаны записывать сами.
"""
import inspect
import os
import unittest
from datetime import datetime, timedelta, timezone
from unittest import mock

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")

from backend import database as db  # noqa: E402
from backend import fix_promises  # noqa: E402


def _соединение_с_курсором(курсор):
    ctx = mock.MagicMock()
    ctx.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = курсор
    return ctx


class НачалоОтсчёта(unittest.TestCase):

    def setUp(self):
        db._access_period_schema_ready = True

    def test_запись_только_вставкой_без_обновления(self):
        курсор = mock.MagicMock()
        курсор.fetchone.return_value = (8546091375,)
        with mock.patch.object(db, "get_db_connection_context", _соединение_с_курсором(курсор)), \
             mock.patch.object(db, "_bust_entitlement_cache") as сброс:
            self.assertTrue(db.start_access_period(8546091375, "bot_start"))
        sql = курсор.execute.call_args[0][0].upper()
        self.assertIn("ON CONFLICT (USER_ID) DO NOTHING", sql)
        self.assertNotIn("UPDATE", sql)
        сброс.assert_called_once_with(8546091375)

    def test_повторный_вход_не_создаёт_и_не_сдвигает(self):
        курсор = mock.MagicMock()
        курсор.fetchone.return_value = None
        with mock.patch.object(db, "get_db_connection_context", _соединение_с_курсором(курсор)), \
             mock.patch.object(db, "_bust_entitlement_cache") as сброс:
            self.assertFalse(db.start_access_period(8546091375, "bootstrap"))
        сброс.assert_not_called()

    def test_неизвестная_дверь_это_ошибка_а_не_молчание(self):
        with self.assertRaises(ValueError):
            db.start_access_period(8546091375, "откуда-то")

    def test_ненастоящий_id_не_пишется(self):
        with mock.patch.object(db, "get_db_connection_context") as соединение:
            self.assertFalse(db.start_access_period(0, "bot_start"))
        соединение.assert_not_called()

    def test_конец_месяца_считается_от_старта(self):
        старт = datetime(2026, 9, 10, 12, 0, tzinfo=timezone.utc)
        курсор = mock.MagicMock()
        курсор.fetchone.return_value = (старт, "signup")
        with mock.patch.object(db, "get_db_connection_context", _соединение_с_курсором(курсор)):
            период = db.get_access_period(8546091375)
        self.assertEqual(период["ends_at"], старт + timedelta(days=db.ACCESS_PERIOD_FREE_DAYS))
        self.assertEqual(период["source"], "signup")


class ЗаливкаСуществующим(unittest.TestCase):

    def setUp(self):
        db._access_period_schema_ready = True

    def test_только_записи_старше_дня_деплоя(self):
        курсор = mock.MagicMock()
        курсор.rowcount = 13
        with mock.patch.object(db, "get_db_connection_context", _соединение_с_курсором(курсор)), \
             mock.patch.object(db, "ensure_user_identity_schema"):
            self.assertEqual(db.backfill_access_periods_for_existing_users(), 13)
        sql, params = курсор.execute.call_args[0]
        self.assertEqual(sql.upper().count("CREATED_AT < %S::TIMESTAMPTZ"), 3)
        self.assertEqual(params, (db.ACCESS_PERIOD_BACKFILL_AT,) * 4)
        self.assertIn("ON CONFLICT (user_id) DO NOTHING", sql)

    def test_страховка_берёт_самый_ранний_след_а_не_сейчас(self):
        курсор = mock.MagicMock()
        курсор.rowcount = 0
        with mock.patch.object(db, "get_db_connection_context", _соединение_с_курсором(курсор)), \
             mock.patch.object(db, "ensure_user_identity_schema"):
            self.assertEqual(db.sweep_access_periods_for_known_users(), 0)
        sql = курсор.execute.call_args[0][0]
        self.assertIn("MIN(u.created_at)", sql)
        self.assertNotIn("NOW()", sql)


class ДвериЗаписи(unittest.TestCase):

    def test_самозапись_по_ссылке_пишет_старт(self):
        курсор = mock.MagicMock()
        курсор.fetchone.return_value = (8546091375,)
        with mock.patch.object(db, "is_access_denied_for_user", return_value=False), \
             mock.patch.object(db, "_public_access_cap_reached", return_value=False), \
             mock.patch.object(db, "get_db_connection_context", _соединение_с_курсором(курсор)), \
             mock.patch.object(db, "invalidate_telegram_user_allowed_cache"), \
             mock.patch.object(db, "_invalidate_webapp_allowlist_redis"), \
             mock.patch.object(db, "start_access_period") as старт:
            self.assertTrue(db.auto_grant_telegram_user(8546091375, "Кто-то", "invite"))
        старт.assert_called_once_with(8546091375, "signup")

    def test_удаление_личных_данных_не_трогает_начало_отсчёта(self):
        исходник = inspect.getsource(db.purge_telegram_user_personal_data)
        тело = "\n".join(l for l in исходник.splitlines() if not l.strip().startswith("#"))
        self.assertNotIn("bt_3_access_period", тело)
        self.assertNotIn("bt_3_pro_grants", тело)


class Обещание(unittest.TestCase):

    def test_зарегистрировано_и_ждёт_ноль(self):
        обещание = fix_promises.by_key("access_period_night_sweep")
        self.assertIsNotNone(обещание)
        self.assertEqual(обещание.expected, 0)


if __name__ == "__main__":
    unittest.main()
