# -*- coding: utf-8 -*-
"""Правило «в списке доступа только настоящие люди» живёт В САМОЙ ЗАПИСИ.

ПОВОД. 14.09.2026 владелец увидел в подписи своего батла «🚫 Не дошло: 10» — три адресата
из десяти были строками 77, 777 и 987654321, за которыми нет ни одного чата. 15.09 замок
поставили на дверь самостоятельного входа, и обещание allowed_rows_are_real_people всё
равно 16.09 показало 2 вместо 0.

Разбор 16.09.2026: дверей в bt_3_allowed_users ПЯТЬ, а замок стоял на одной.
Мимо правила писали: команда /allow, кнопка «одобрить заявку», посев админов при каждом
старте бота, ночной впуск из очереди — и шестым путём нагрузочный прогон со своим
подключением к базе.

Решение владельца 16.09.2026, дословно: «поставить правило „это должен быть настоящий
человек“ в саму запись, один раз, вместо замка на каждую дверь. Тогда неважно, кто
пишет — прогон, посев, очередь или новый путь, которого ещё нет».

Тесты держат ровно это:
  • правило одно, и оно НЕ молчит — отказ виден вызывающему и в логе;
  • каждая из дверей на него опирается, ни у одной нет своей копии правила;
  • в самой таблице стоит ограничение — на случай пути, которого ещё нет;
  • накопленные строки НЕ удаляются деплоем: их показывают владельцу, решает он.
"""
import os
import pathlib
import unittest
from unittest import mock

ROOT = pathlib.Path(__file__).resolve().parents[2]
DB = ROOT / "backend" / "database.py"
BOT = ROOT / "bot_3.py"
LOAD = ROOT / "scripts" / "run_translation_load_test.py"


class ПравилоОдноИОноНеМолчит(unittest.TestCase):
    def test_ghost_ids_are_refused_and_real_ones_pass(self):
        from backend.database import (refuse_if_not_a_person, NotARealPerson,
                                      SYNTHETIC_TELEGRAM_USER_ID_MIN)
        for плохой in (0, None, 7, 77, 777, SYNTHETIC_TELEGRAM_USER_ID_MIN,
                       SYNTHETIC_TELEGRAM_USER_ID_MIN + 5):
            with self.assertRaises(NotARealPerson, msg=плохой):
                refuse_if_not_a_person(плохой, дверь="тест")
        # 117 649 764 — самый маленький id среди настоящих впущенных (замер 15.09.2026).
        self.assertEqual(117649764, refuse_if_not_a_person(117649764, дверь="тест"))

    def test_refusal_names_the_door_and_the_bounds(self):
        from backend.database import refuse_if_not_a_person, NotARealPerson
        with self.assertRaises(NotARealPerson) as поймано:
            refuse_if_not_a_person(777, дверь="allow_telegram_user")
        текст = str(поймано.exception)
        self.assertIn("777", текст)
        self.assertIn("allow_telegram_user", текст, "по ошибке должно быть видно дверь")

    def test_number_rule_cannot_catch_a_plausible_looking_id(self):
        """ЧЕСТНАЯ ГРАНИЦА ПРАВИЛА, ПРОВЕРЕНО 16.09.2026 — не поднимать как находку.

        987654321 лежит ВНУТРИ диапазона настоящих telegram id, и числом его не отличить.
        14.09.2026 его нашли не правилом, а живым запросом getChat («Chat not found»).
        Правило закрывает то, что закрывает: id вне диапазона. Строку, похожую на
        настоящую, но без чата за ней, ловит не запись, а опрос телеграма."""
        from backend.database import refuse_if_not_a_person
        self.assertEqual(987654321, refuse_if_not_a_person(987654321, дверь="тест"))

    def test_no_door_keeps_its_own_copy_of_the_rule(self):
        """Второго текста правила быть не должно — иначе двери разойдутся."""
        src = DB.read_text(encoding="utf-8")
        self.assertEqual(1, src.count("def refuse_if_not_a_person"))
        for дверь in ("def allow_telegram_user", "def auto_grant_telegram_user"):
            начало = src.index(дверь)
            блок = src[начало:src.index("\ndef ", начало + 10)]
            self.assertIn("refuse_if_not_a_person(", блок, дверь)
            self.assertNotIn("if not is_real_telegram_user_id(", блок,
                             f"{дверь}: своя копия правила вместо общего")


class ДверьНеПишетДоТогоКакСпросила(unittest.TestCase):
    def test_allow_telegram_user_refuses_without_touching_the_database(self):
        from backend.database import allow_telegram_user, NotARealPerson
        with mock.patch("backend.database.get_db_connection_context") as подключение:
            with self.assertRaises(NotARealPerson):
                allow_telegram_user(777, username="призрак", added_by=1, note="тест")
        подключение.assert_not_called()

    def test_self_serve_door_says_no_and_writes_nothing(self):
        from backend.database import auto_grant_telegram_user
        среда = {"SKIP_ACCESS_GRANT_WRITES": "", "SKIP_STARTUP_SCHEMA_BOOTSTRAP": ""}
        with mock.patch.dict(os.environ, среда, clear=False):
            with mock.patch("backend.database.get_db_connection_context") as подключение:
                self.assertFalse(auto_grant_telegram_user(777, source="mini-app"))
            подключение.assert_not_called()

    def test_night_admission_filters_ghosts_in_the_select(self):
        """Ночной впуск не должен «пропускать» строку — он не должен её ВЫБИРАТЬ:
        пропущенная встала бы в голову очереди и съедала место живых каждую ночь."""
        src = DB.read_text(encoding="utf-8")
        начало = src.index("def admit_from_access_waitlist")
        блок = src[начало:src.index("\ndef ", начало + 10)]
        self.assertIn("admitted_at IS NULL AND user_id >= %s AND user_id < %s", блок)
        self.assertIn("_MIN_REAL_TELEGRAM_USER_ID, SYNTHETIC_TELEGRAM_USER_ID_MIN", блок)


class ОграничениеСтоитВСамойТаблице(unittest.TestCase):
    class _Курсор:
        def __init__(self, стоящие):
            self.стоящие = list(стоящие)
            self.выполнено = []

        def execute(self, sql, params=None):
            self.выполнено.append(sql)

        def fetchall(self):
            return [(имя,) for имя in self.стоящие]

    def _имя(self):
        from backend.database import _MIN_REAL_TELEGRAM_USER_ID, SYNTHETIC_TELEGRAM_USER_ID_MIN
        return f"bt_3_allowed_users_person_{_MIN_REAL_TELEGRAM_USER_ID}_{SYNTHETIC_TELEGRAM_USER_ID_MIN}"

    def test_constraint_is_added_when_missing(self):
        from backend.database import _ensure_allowed_users_person_check
        курсор = self._Курсор([])
        _ensure_allowed_users_person_check(курсор)
        добавление = [s for s in курсор.выполнено if "ADD CONSTRAINT" in s]
        self.assertEqual(1, len(добавление))
        self.assertIn(self._имя(), добавление[0])
        self.assertIn("NOT VALID", добавление[0],
                      "накопленные строки не трогаем: их судьбу решает владелец")

    def test_existing_constraint_is_left_alone(self):
        from backend.database import _ensure_allowed_users_person_check
        курсор = self._Курсор([self._имя()])
        _ensure_allowed_users_person_check(курсор)
        self.assertEqual([], [s for s in курсор.выполнено if "ALTER TABLE" in s],
                         "каждый старт не должен дёргать блокировку таблицы зря")

    def test_stale_bounds_are_replaced_not_kept(self):
        from backend.database import _ensure_allowed_users_person_check
        курсор = self._Курсор(["bt_3_allowed_users_person_1_999"])
        _ensure_allowed_users_person_check(курсор)
        текст = "\n".join(курсор.выполнено)
        self.assertIn('DROP CONSTRAINT "bt_3_allowed_users_person_1_999"', текст)
        self.assertIn(self._имя(), текст)

    def test_missing_lock_is_not_measured_as_zero(self):
        """Ноль при снятом замке — правда ровно до первой записи. Выдавать его за
        «держится» нельзя: это и есть «пустой результат от поломки»."""
        from unittest import mock
        from backend.fix_promises import _allowed_rows_not_real_people
        with mock.patch("backend.database.allowed_users_person_check_is_in_place",
                        return_value=False), \
             mock.patch("backend.database.count_allowed_rows_not_real_people", return_value=0):
            with self.assertRaises(RuntimeError) as поймано:
                _allowed_rows_not_real_people()
        self.assertIn("снят с таблицы", str(поймано.exception))

    def test_lock_failure_does_not_take_down_the_whole_bootstrap(self):
        """Прав не хватило — падает ПРАВКА, а не подготовка базы: иначе бот не поднимется."""
        from backend.database import _ensure_allowed_users_person_check

        class Падающий(self._Курсор.__mro__[0]):
            def execute(self, sql, params=None):
                super().execute(sql, params)
                if "ADD CONSTRAINT" in sql:
                    raise RuntimeError("must be owner of table")

        курсор = Падающий([])
        _ensure_allowed_users_person_check(курсор)   # не бросает наружу
        текст = "\n".join(курсор.выполнено)
        self.assertIn("SAVEPOINT allowed_users_person_check", текст)
        self.assertIn("ROLLBACK TO SAVEPOINT", текст)

    def test_core_schema_actually_calls_it(self):
        src = DB.read_text(encoding="utf-8")
        self.assertIn("_ensure_allowed_users_person_check(cursor)", src)


class ДверейБольшеНетНиОдной(unittest.TestCase):
    def test_load_test_no_longer_writes_synthetic_people_into_the_live_list(self):
        src = LOAD.read_text(encoding="utf-8")
        начало = src.index("def ensure_synthetic_users")
        блок = src[начало:src.index("\ndef ", начало + 10)]
        self.assertIn("raise RuntimeError", блок)
        self.assertNotIn("INSERT INTO bt_3_allowed_users", блок,
                         "тело удалено, а не оставлено за raise: мёртвый код выглядит живым")
        self.assertIn("отдельной", блок, "сказано, что делать вместо этого")
        self.assertNotIn("INSERT INTO bt_3_allowed_users", src,
                         "нагрузочный прогон больше не пишет в боевой список доступа")

    def test_admin_seeding_says_it_out_loud(self):
        src = BOT.read_text(encoding="utf-8")
        начало = src.index("def _seed_admins_into_allowlist")
        блок = src[начало:src.index("\ndef ", начало + 10)]
        self.assertIn("except NotARealPerson", блок)
        self.assertIn("BOT_ADMIN_TELEGRAM_IDS", блок)

    def test_admin_hands_get_a_human_refusal(self):
        src = BOT.read_text(encoding="utf-8")
        self.assertIn("не похоже на telegram id человека, доступ не выдан", src)
        self.assertIn("не похоже на telegram id человека. Доступ не выдан.", src)
        self.assertIn("    NotARealPerson,\n", src)


class НакопленноеПоказываютАНеЧистятМолча(unittest.TestCase):
    def test_deletion_needs_named_ids_and_never_takes_a_real_person(self):
        from backend.database import delete_allowed_rows_not_real_people
        with mock.patch("backend.database.get_db_connection_context") as подключение:
            self.assertEqual(0, delete_allowed_rows_not_real_people([]))
            self.assertEqual(0, delete_allowed_rows_not_real_people([117649764]))
        подключение.assert_not_called()

    def test_cleanup_script_shows_before_it_deletes(self):
        src = (ROOT / "scripts" / "allowed_users_drop_test_rows.py").read_text(encoding="utf-8")
        self.assertIn("list_allowed_rows_not_real_people", src)
        self.assertIn("Сухой прогон", src)
        self.assertLess(src.index("Найдено строк без человека"), src.index("--apply\")"),
                        "сначала показать, потом предлагать удалять")

    def test_promise_carries_a_screen_that_names_the_rows(self):
        from backend.fix_promises import by_key
        p = by_key("allowed_rows_are_real_people")
        self.assertIsNotNone(p.screen, "число «2» не говорит, кто их записал")
        self.assertEqual("16.09.2026", p.since, "обещание переподтверждено после разбора")

    def test_screen_names_door_and_date(self):
        from backend.fix_promises import _allowed_rows_screen
        import datetime
        строки = [{"user_id": 777, "username": None, "added_by": None,
                   "note": "load_test_translation_check_2026_03_23",
                   "created_at": datetime.datetime(2026, 9, 14, 19, 2),
                   "updated_at": None}]
        with mock.patch("backend.database.list_allowed_rows_not_real_people", return_value=строки), \
             mock.patch("backend.database.count_allowed_users", return_value=30):
            текст = _allowed_rows_screen()
        self.assertIn("777", текст)
        self.assertIn("load_test", текст)
        self.assertIn("14.09.2026 19:02", текст)

    def test_clean_screen_says_zero_not_nothing(self):
        from backend.fix_promises import _allowed_rows_screen
        with mock.patch("backend.database.list_allowed_rows_not_real_people", return_value=[]), \
             mock.patch("backend.database.count_allowed_users", return_value=30):
            self.assertIn("чисто", _allowed_rows_screen())


if __name__ == "__main__":
    unittest.main()
