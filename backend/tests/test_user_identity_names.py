import unittest
from unittest.mock import patch

from backend import database
from backend.database import clean_identity_name


class CleanIdentityNameTest(unittest.TestCase):
    """Имя человека или ничего. Заглушки в текст пользователю попадать не должны."""

    def test_real_names_survive(self):
        for value in ("Aleksandr", "Iryna Kovalenko", "@lingua_fox", " Liliya "):
            self.assertTrue(clean_identity_name(value))

    def test_placeholders_are_rejected(self):
        for value in (
            None,
            "",
            "   ",
            "None",
            "null",
            "Unknown",
            "Unknown User",
            "user_117649764",
            "useer_117649764",
            "User 42",
            "load_test_9937001849",
            "owner_admin",
            "Student",
        ):
            self.assertEqual(clean_identity_name(value), "", value)

    def test_handle_loses_the_at_sign(self):
        self.assertEqual(clean_identity_name("@lingua_fox"), "lingua_fox")


class RealUserIdTest(unittest.TestCase):
    """Тестовые и нагрузочные id не попадают в таблицу личности — иначе прогон тестов
    дописывает в неё несуществующих людей."""

    def test_test_ids_are_not_real(self):
        for uid in (0, -5, 55, 111, 4242, 99_999):
            self.assertFalse(database.is_real_telegram_user_id(uid), uid)

    def test_synthetic_load_ids_are_not_real(self):
        self.assertFalse(database.is_real_telegram_user_id(9_937_001_811))

    def test_real_ids_pass(self):
        for uid in (117_649_764, 8_546_091_375):
            self.assertTrue(database.is_real_telegram_user_id(uid), uid)

    def test_test_id_never_written(self):
        with patch.object(database, "get_db_connection_context") as conn_ctx:
            self.assertEqual(database.remember_user_identity(111, display_name="tester"), "")
        conn_ctx.assert_not_called()


class RememberUserIdentityTest(unittest.TestCase):
    """Пустое имя никогда не затирает настоящее и не тратит запись."""

    def test_blank_name_does_not_write(self):
        with patch.object(database, "get_db_connection_context") as conn_ctx, patch.object(
            database, "get_user_display_name", return_value="Aleksandr"
        ):
            result = database.remember_user_identity(117649764, first_name="  ", tg_username="")
        self.assertEqual(result, "Aleksandr")
        conn_ctx.assert_not_called()

    def test_placeholder_name_does_not_write(self):
        with patch.object(database, "get_db_connection_context") as conn_ctx, patch.object(
            database, "get_user_display_name", return_value=""
        ):
            result = database.remember_user_identity(117649764, display_name="user_117649764")
        self.assertEqual(result, "")
        conn_ctx.assert_not_called()


if __name__ == "__main__":
    unittest.main()
