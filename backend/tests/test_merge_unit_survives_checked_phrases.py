# -*- coding: utf-8 -*-
"""Слияние двойника не имеет права падать из-за того, что ОБЕ фразы уже проверены.

ПОВОД, 28.08.2026. Убирали двойника «Ich schämemich für dich» → «ich schäme mich für
dich». `merge_unit_into` упала с `duplicate key value violates unique constraint
bt_3_phrase_check_pkey`, и вместе с ней откатилась ВСЯ уборка — включая снятие 25
обрезков, которое к слиянию отношения не имело.

Причина: у bt_3_phrase_check колонка unit_id это первичный ключ, а строки этой таблицы
(и bt_3_phrase_review) переносились голым UPDATE — единственные две таблицы в функции
без защиты «а нет ли уже такой у настоящего слова». Ночная проверка грамматики смотрит
все фразы подряд, так что для любой пары давно живущих записей проверены ОБЕ, и слияние
падало гарантированно.

Что проверяет тест: для обеих таблиц перенос идёт с оговоркой NOT EXISTS, а остаток
строк двойника снимается. Это структурная проверка запросов, а не прогон по базе:
живой базы у тестов нет и быть не должно. Настоящая проверка сделана на проде — слияние
26929 → 25429 прошло, у настоящего предложения стало 11 карточек.
"""
import re
import unittest

from backend.lex_units import merge_unit_into

ТАБЛИЦЫ = ("bt_3_phrase_check", "bt_3_phrase_review")


class ЗаписнойКурсор:
    """Ничего не исполняет — только запоминает, что ему велели."""

    def __init__(self):
        self.запросы: list[str] = []

    def execute(self, sql, params=None):
        self.запросы.append(" ".join(str(sql).split()))


class MergeSurvivesCheckedPhrases(unittest.TestCase):
    def setUp(self):
        self.cur = ЗаписнойКурсор()
        merge_unit_into(self.cur, 26929, 25429)

    def _про(self, таблица: str, начало: str) -> list[str]:
        return [q for q in self.cur.запросы
                if q.upper().startswith(начало) and таблица in q]

    def test_move_is_guarded_for_both_check_tables(self):
        for таблица in ТАБЛИЦЫ:
            переносы = self._про(таблица, "UPDATE")
            self.assertTrue(переносы, f"перенос строк {таблица} исчез из слияния")
            for q in переносы:
                self.assertIn(
                    "NOT EXISTS", q.upper(),
                    f"перенос {таблица} снова без защиты — слияние упадёт на паре "
                    f"уже проверенных фраз и утащит за собой всю уборку",
                )

    def test_leftover_rows_of_the_twin_are_dropped(self):
        for таблица in ТАБЛИЦЫ:
            self.assertTrue(
                self._про(таблица, "DELETE"),
                f"строки двойника в {таблица} не снимаются — слияние оставит мусор, "
                f"указывающий на удалённую единицу",
            )

    def test_every_table_in_the_merge_is_guarded(self):
        """Защита нужна не только этим двум: если завтра добавят девятую таблицу
        голым UPDATE, урок 19.08 и 28.08 придётся учить в третий раз."""
        без_защиты = [
            q for q in self.cur.запросы
            if q.upper().startswith("UPDATE")
            and re.search(r"SET\s+(unit_id|from_unit|to_unit)\s*=", q, re.I)
            and "NOT EXISTS" not in q.upper()
            # Личные карточки — исключение: у lex_unit_id нет уникальности, человек
            # вправе иметь несколько карточек на одно слово, и все они переезжают.
            and "bt_3_webapp_dictionary_queries" not in q
        ]
        self.assertEqual([], без_защиты,
                         "перенос без защиты от совпадения: " + "; ".join(без_защиты))


if __name__ == "__main__":
    unittest.main()
