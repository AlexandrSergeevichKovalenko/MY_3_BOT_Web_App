# -*- coding: utf-8 -*-
"""Карантин пула: клеймо не ставится прогоном тестов, а у слова три исхода.

ПОВОД, 01.09.2026. Владелец открыл карантин: 31 слово, и все хорошие — die
Vorgehensweise, ausführlich, zeigen, ankommen, Verschwörer. Замер по живой базе: у 26 из
31 разбор ЕСТЬ и лежит в слое слов, откуда его и показывает приложение. Экран говорил
«карточка не собралась» про слова с собранной карточкой.

Клеймо ставил НЕ ночной добор. В проде он с 27.07.2026 ходит в слой слов, и ветка по
старому банку — единственная, что вешает метку, — не выполняется вообще. Метку ставил
ПРОГОН ТЕСТОВ на машине разработчика: рубильника DICTIONARY_UNITS_LOOKUP_ENABLED там
нет, а база боевая; тест ночного добора запускал настоящий добор с пустым ответом
модели, и тот брал из живой базы САМОЕ ВОСТРЕБОВАННОЕ слово и метил его.

И второе. Экран открывался со всеми словами, помеченными 🗑: одно нажатие «Применить»
без единого тапа — и 31 хорошее слово исчезало. Владелец: «Где кнопка оставить слово в
базе без разбора? Вот как есть — пусть так и будет слово доступно пользователю!!!»
"""
import io
import os
import pathlib
import unittest
from unittest import mock

DB = pathlib.Path(__file__).resolve().parents[1] / "database.py"
BOT = pathlib.Path(__file__).resolve().parents[2] / "bot_3.py"


class КлеймоНеСтавитсяВПрогонеТестов(unittest.TestCase):
    def test_guard_is_on_in_the_test_run(self):
        self.assertEqual(os.getenv("SKIP_NIGHT_SIDE_EFFECTS"), "1")

    def test_no_write_under_the_guard(self):
        from backend import database
        with mock.patch.object(database, "get_db_connection_context") as соединение:
            database.mark_pool_entry_enrich_failed(123456, "empty")
        соединение.assert_not_called()

    def test_production_still_marks(self):
        """Запрет ровно один и снимается только отсутствием переменной."""
        from backend import database
        with mock.patch.dict(os.environ, {"SKIP_NIGHT_SIDE_EFFECTS": ""}):
            with mock.patch.object(database, "get_db_connection_context") as соединение:
                database.mark_pool_entry_enrich_failed(123456, "empty")
        соединение.assert_called_once()


class РешениеВладельцаОставитьКакЕсть(unittest.TestCase):
    def test_keep_writes_a_decision_not_a_silence(self):
        src = DB.read_text(encoding="utf-8")
        начало = src.index("def keep_pool_entries_as_is")
        блок = src[начало:начало + 2000]
        self.assertIn("quarantine_owner_keep", блок)
        self.assertIn("quarantine_owner_keep_at", блок,
                      "решение без даты — не решение, а след неизвестно чего")

    def test_kept_words_never_come_back_to_the_list(self):
        """Иначе список превращается в то, от чего владелец уходил: одно и то же
        каждую неделю."""
        src = DB.read_text(encoding="utf-8")
        for имя in ("def count_quarantined_pool_entries", "def get_quarantined_pool_entries"):
            начало = src.index(имя)
            блок = src[начало:начало + 2200]
            self.assertIn("quarantine_owner_keep", блок,
                          f"{имя} снова покажет то, по чему решение уже принято")


class ЭкранНеРешаетЗаВладельца(unittest.TestCase):
    def _цикл(self):
        from backend.database import QUARANTINE_MARK_CYCLE
        return QUARANTINE_MARK_CYCLE

    def test_default_is_no_decision(self):
        """Исходное состояние — «решения нет», а не «удалить»."""
        self.assertEqual("", self._цикл()[0])

    def test_all_three_outcomes_are_reachable_by_tap(self):
        self.assertEqual(("", "as_is", "redo", "drop"), self._цикл())

    def test_untouched_word_is_counted_apart(self):
        """Нерешённое считается отдельно и НЕ попадает ни в одно действие."""
        import importlib.util
        spec = importlib.util.spec_from_file_location("bot_3_карантин", BOT)
        # Импортировать весь bot_3 ради одной чистой функции дорого и небезопасно
        # (модуль поднимает приложение). Берём её текстом — она без зависимостей.
        src = BOT.read_text(encoding="utf-8")
        начало = src.index("def _quarantine_counts")
        конец = src.index("def _build_quarantine_review", начало)
        пространство: dict = {}
        exec(compile(src[начало:конец], str(BOT), "exec"), пространство)
        сессия = {
            "candidates": [{"id": 1}, {"id": 2}, {"id": 3}, {"id": 4}],
            "marks": {1: "as_is", 2: "redo", 3: "drop"},
        }
        self.assertEqual(
            {"as_is": 1, "redo": 1, "drop": 1, "": 1},
            пространство["_quarantine_counts"](сессия),
        )

    def test_apply_refuses_when_nothing_is_marked(self):
        """Без единого тапа «Применить» не имеет права ничего сделать."""
        src = BOT.read_text(encoding="utf-8")
        начало = src.index("async def handle_quarantine_callback")
        сдвиг = src.index('elif action == "del":', начало)
        блок = src[сдвиг:сдвиг + 700]
        self.assertIn("Ничего не отмечено", блок)


if __name__ == "__main__":
    unittest.main()
