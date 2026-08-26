# -*- coding: utf-8 -*-
"""Кеш быстрого словаря отдаёт СВЕЖЕЕ слово, а не снимок годичной давности.

ПОВОД. Кеш живёт десять лет (DICTIONARY_PERSISTENT_CACHE_TTL_SEC = 315 360 000 секунд).
Замер 26.08.2026: 2 882 записи, и у 2 121 слово чинили УЖЕ ПОСЛЕ того, как ответ туда
лёг. Мы правим слово — оно верное в словаре и в тренировке, а быстрый словарь отдаёт
мартовский снимок со старой ошибкой. Это было последнее место, где жило старое.

⚠ ГЛАВНОЕ, ЧТО СТЕРЕЖЁТ ЭТОТ ТЕСТ. Первая версия склейки искала разбор как
`единица["card"]`, а `lex_units.lookup` отдаёт САМ РАЗБОР. Функция всегда получала None
и молча возвращала кеш нетронутым: склейка «работала», не делая ничего. Такую поломку
не видно ни в логах, ни на глаз — только сравнением до и после на живом слове.
"""
import os
import unittest
from unittest import mock

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SECOND_VOICE_CHECK_DISABLED", "1")

from backend.backend_server import _with_fresh_unit_content  # noqa: E402

СТАРЫЙ_ОТВЕТ = {"item": {"word_de": "kreieren", "translation_ru": "создавать",
                         "usage_examples": []}}
СВЕЖЕЕ_СЛОВО = {"__lex_has_card": True, "__lex_unit_id": 27926,
                "word_de": "kreieren", "translation_ru": "создавать",
                "usage_examples": [{"source": "Die Modedesignerin kreiert elegante Abendkleider.",
                                    "target": "Модельер создаёт элегантные вечерние платья."}]}


class CacheServesFresh(unittest.TestCase):
    def test_fresh_examples_reach_the_reader(self):
        with mock.patch("backend.lex_units.lookup", return_value=dict(СВЕЖЕЕ_СЛОВО)):
            свежий = _with_fresh_unit_content(dict(СТАРЫЙ_ОТВЕТ), "kreieren", "de")
        примеры = свежий["item"].get("usage_examples") or []
        self.assertTrue(примеры, "склейка снова молча вернула кеш нетронутым")
        self.assertIn("Modedesignerin", примеры[0]["source"])

    def test_service_keys_do_not_leak_to_the_reader(self):
        """Служебные пометки слоя человеку не показываем."""
        with mock.patch("backend.lex_units.lookup", return_value=dict(СВЕЖЕЕ_СЛОВО)):
            свежий = _with_fresh_unit_content(dict(СТАРЫЙ_ОТВЕТ), "kreieren", "de")
        self.assertNotIn("__lex_unit_id", свежий["item"])
        self.assertNotIn("__lex_has_card", свежий["item"])

    def test_word_without_unit_keeps_the_cached_answer(self):
        """Слова нет в слое — отдаём кеш как есть: прежний ответ лучше пустого экрана."""
        with mock.patch("backend.lex_units.lookup", return_value=None):
            свежий = _with_fresh_unit_content(dict(СТАРЫЙ_ОТВЕТ), "kreieren", "de")
        self.assertEqual(свежий, СТАРЫЙ_ОТВЕТ)

    def test_unit_without_card_keeps_the_cached_answer(self):
        with mock.patch("backend.lex_units.lookup",
                        return_value={"__lex_has_card": False, "__lex_unit_id": 1}):
            свежий = _with_fresh_unit_content(dict(СТАРЫЙ_ОТВЕТ), "kreieren", "de")
        self.assertEqual(свежий, СТАРЫЙ_ОТВЕТ)

    def test_failure_does_not_break_the_answer(self):
        """Склейка — улучшение, а не условие ответа. Упала — человек всё равно получает
        то, что было в кеше."""
        with mock.patch("backend.lex_units.lookup", side_effect=RuntimeError("слой молчит")):
            свежий = _with_fresh_unit_content(dict(СТАРЫЙ_ОТВЕТ), "kreieren", "de")
        self.assertEqual(свежий, СТАРЫЙ_ОТВЕТ)

    def test_non_german_lookup_is_left_alone(self):
        with mock.patch("backend.lex_units.lookup") as поиск:
            свежий = _with_fresh_unit_content(dict(СТАРЫЙ_ОТВЕТ), "создавать", "ru")
        поиск.assert_not_called()
        self.assertEqual(свежий, СТАРЫЙ_ОТВЕТ)


if __name__ == "__main__":
    unittest.main()
