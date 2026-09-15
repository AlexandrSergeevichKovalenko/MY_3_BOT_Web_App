# -*- coding: utf-8 -*-
"""Прогон тестов НЕ имеет права впускать человека в боевой список доступа.

ЧТО СЛУЧИЛОСЬ 14.09.2026. Три строки, за которыми нет человека (77, 777, 987654321),
убрали руками — и через час они вернулись: 19:02, 19:05, 19:07, пометка «self-serve
access: приложение (Mini App)», added_by пуст, имени ни у одной (живой человек всегда
приходит с first_name от Telegram). Это подпись ПРОГОНА ТЕСТОВ: тест патчит
_telegram_hash_is_valid → True, отдаёт выдуманный id и идёт тестовым клиентом Flask в
/api/webapp/*; сторож before_request не находит id в списке и зовёт self-serve дверь.
База при этом боевая — в окружении разработчика DATABASE_URL_RAILWAY смотрит на zephyr.
Владельцу за те три минуты ушло три письма «🆕 Новый пользователь подключился», а потом
каждая рассылка бота стучалась в чаты, которых нет.

Класс не новый: ровно так прогон оставил 1010 строк в ведомости расходов (02.08.2026) и
применил 64 ночные правки (28.08.2026). Лечится тем же способом — переменной, которую
conftest ставит до импорта приложения, а прод не ставит никогда.

Здесь стерегутся ОБА замка двери:
  · прогон тестов не пишет ни строки в список доступа и не шлёт владельцу «+1»;
  · id, который по правилу проекта не может принадлежать человеку (`is_real_telegram_user_id`,
    оба конца), дверь не впускает НИКОГДА — даже в проде.
"""
import os
import unittest
from unittest import mock

from backend import database as db


class ПрогонНикогоНеВпускает(unittest.TestCase):
    def test_conftest_sets_the_guard(self):
        """Ставится ДО импорта приложения, поэтому отказаться от него нельзя."""
        self.assertEqual(os.getenv("SKIP_ACCESS_GRANT_WRITES"), "1")
        self.assertTrue(db._access_grant_writes_disabled())

    def test_no_row_and_no_letter_under_the_guard(self):
        """Под запретом дверь отвечает «впустили», но в базу не ходит вообще."""
        with mock.patch.object(db, "get_db_connection_context") as соединение, \
             mock.patch.object(db, "is_access_denied_for_user") as запрет, \
             mock.patch.object(db, "start_access_period") as период:
            результат = db.auto_grant_telegram_user(987654321, None, "приложение (Mini App)")
        self.assertTrue(результат, "тест должен идти дальше своим путём")
        соединение.assert_not_called()
        запрет.assert_not_called()
        период.assert_not_called()

    def test_admin_letter_is_suppressed_under_the_guard(self):
        """Письма «🆕 Новый пользователь» из прогона владельцу не приходит."""
        import backend.backend_server as server
        with mock.patch.object(server, "get_admin_telegram_ids") as админы:
            server._notify_admins_new_user_async(987654321, "", "приложение (Mini App)")
        админы.assert_not_called()

    def test_production_still_lets_a_real_person_in(self):
        """Запрет ровно один и снимается только отсутствием переменной."""
        курсор = mock.MagicMock()
        курсор.fetchone.return_value = (117649764,)
        соединение = mock.MagicMock()
        соединение.__enter__.return_value.cursor.return_value.__enter__.return_value = курсор
        with mock.patch.dict(os.environ, {"SKIP_ACCESS_GRANT_WRITES": "",
                                          "SKIP_STARTUP_SCHEMA_BOOTSTRAP": ""}), \
             mock.patch.object(db, "is_access_denied_for_user", return_value=False), \
             mock.patch.object(db, "_public_access_cap_reached", return_value=False), \
             mock.patch.object(db, "get_db_connection_context", return_value=соединение), \
             mock.patch.object(db, "invalidate_telegram_user_allowed_cache"), \
             mock.patch.object(db, "_invalidate_webapp_allowlist_redis"), \
             mock.patch.object(db, "start_access_period") as период:
            self.assertTrue(db.auto_grant_telegram_user(117649764, "Настоящий", "прямая ссылка"))
        период.assert_called_once()


class ВыдуманныйIdНеВпускаетсяНикогда(unittest.TestCase):
    """Второй замок работает и в проде, а не только в тестах."""

    def _впустить(self, uid: int):
        with mock.patch.dict(os.environ, {"SKIP_ACCESS_GRANT_WRITES": "",
                                          "SKIP_STARTUP_SCHEMA_BOOTSTRAP": ""}), \
             mock.patch.object(db, "is_access_denied_for_user", return_value=False), \
             mock.patch.object(db, "_public_access_cap_reached", return_value=False), \
             mock.patch.object(db, "get_db_connection_context") as соединение, \
             mock.patch.object(db, "start_access_period"):
            результат = db.auto_grant_telegram_user(uid, None, "приложение (Mini App)")
        return результат, соединение

    def test_short_id_is_refused(self):
        """77 и 777 — именно то, что легло в базу 14.09.2026."""
        for uid in (7, 77, 777, 99_999):
            результат, соединение = self._впустить(uid)
            self.assertFalse(результат, f"id {uid} впустили")
            соединение.assert_not_called()

    def test_synthetic_id_is_refused(self):
        результат, соединение = self._впустить(int(db.SYNTHETIC_TELEGRAM_USER_ID_MIN) + 5)
        self.assertFalse(результат, "синтетический id нагрузочного прогона впустили")
        соединение.assert_not_called()

    def test_the_rule_is_the_projects_own_one(self):
        """Правило берётся из одного места, а не переписано здесь вторым текстом."""
        self.assertFalse(db.is_real_telegram_user_id(777))
        self.assertTrue(db.is_real_telegram_user_id(117649764))


if __name__ == "__main__":
    unittest.main()
