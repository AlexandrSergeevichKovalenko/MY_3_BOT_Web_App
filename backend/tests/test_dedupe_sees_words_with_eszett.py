# -*- coding: utf-8 -*-
"""Уборка повторов обязана видеть немецкие слова с «ß».

ЧТО СЛОМАЛОСЬ. Кандидатов на повтор `_dedupe_webapp_dictionary_entry_after_insert`
отбирал условием `LOWER(BTRIM(word_de)) = %s`, а значение для сравнения готовил
питоновским `.casefold()`. На «ß» эти две нормализации РАЗНЫЕ:

    python .casefold('Weißt')  → 'weisst'
    SQL     lower('Weißt')     → 'weißt'

Условие не совпадало никогда, кандидатов не находилось, и уборка молча возвращала ноль
— то есть повтор со словом на «ß» не снимался ни при каких данных.

Поймано 28.08.2026 на живом повторе карточек 620 и 1035 владельца
(«Weißt du zufällig Bescheid, wem sie gehört?»): строки совпадали до буквы, а уборка их
не видела. Замер тогда же: 1278 карточек из 26 120 (4,9%) содержат «ß» — для всех них
точечная уборка была слепа; действующих повторов среди них на тот момент 16.

Это ТОТ ЖЕ урок, что уже записан в `test_word_audit_identity`: выражение в питоне и
выражение в SQL обязаны нормализовать одинаково, иначе связь между ними рвётся молча.
"""
import unittest

from backend.database import _squash_space


class НормализацияСовпадаетССиквелом(unittest.TestCase):
    """SQL LOWER() не трогает «ß». Питон обязан вести себя так же."""

    СЛОВА = [
        "Weißt du zufällig Bescheid, wem sie gehört?",
        "Straße",
        "Fußgängerzone",
        "GROSSE Straße",
        "Abschiebung",           # без «ß» — поведение не должно измениться
        "die Entscheidung",
    ]

    def test_python_key_matches_sql_lower(self):
        for слово in self.СЛОВА:
            питон = _squash_space(слово).lower()
            sql = слово.lower()          # ровно то, что делает LOWER() в Postgres
            self.assertEqual(питон, sql, слово)

    def test_casefold_would_break_it_again(self):
        """Сторож против возврата: casefold и LOWER расходятся именно на «ß»."""
        слово = "Weißt du zufällig Bescheid, wem sie gehört?"
        self.assertNotEqual(слово.casefold(), слово.lower(),
                            "если это сравнялось, проверьте, не сменилась ли версия юникода")
        self.assertEqual(_squash_space(слово).lower(), слово.lower())


if __name__ == "__main__":
    unittest.main()
