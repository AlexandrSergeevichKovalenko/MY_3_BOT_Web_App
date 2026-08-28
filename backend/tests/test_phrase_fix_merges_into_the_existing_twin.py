# -*- coding: utf-8 -*-
"""Правильная фраза уже есть в словаре — сливаем, а не выбрасываем правку.

ПОВОД, 28.08.2026. В `apply_phrase_review_decision` стояло: новое написание уже занято
другой единицей → `status='closed'` и выход. То есть человек нажимал «Да, правильно
так», вопрос закрывался НАВСЕГДА, а у него в словаре оставалась кривая фраза — и на
проверку она больше не приходила никогда.

Замер по живой базе в тот же день: 6 таких правок за всё время, две из них в тот день:

    #307 «Ich bin es gewohnt, früh aufzustechen» — правильная лежала единицей 1858
    #335 «Wir sind nicht im Stand zu kommen»     — правильная лежала единицей 25144

Обе — карточки владельца, обе остались с кривым немецким.

Владелец 28.08.2026: «сливай автоматически». Кривая единица уезжает в правильную
(`lex_units.merge_unit_into` — перенос, а не удаление), а тексты карточек правятся ДО
переноса, пока они ещё висят на кривой единице.
"""
import os
import unittest
from unittest import mock

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SECOND_VOICE_CHECK_DISABLED", "1")

from backend import database  # noqa: E402

СТАРЫЙ = "Ich bin es gewohnt, früh aufzustechen"
НОВЫЙ = "Ich bin es gewohnt, früh aufzustehen"
КРИВАЯ_ЕДИНИЦА = 21938
ПРАВИЛЬНАЯ_ЕДИНИЦА = 1858
ПРОВЕРКА_ПРОШЛА = {"checked": True, "grammar_ok": True, "meaning_kept": True}
СУДЬИ = [{"verdict": "error", "category": "rechtschreibung", "corrected": НОВЫЙ,
          "corrected_ru": "Я привык рано вставать",
          "corrected_check": dict(ПРОВЕРКА_ПРОШЛА)}]


class Курсор:
    def __init__(self, двойник):
        self._двойник = двойник
        self.шаги = 0
        self.запросы = []

    def execute(self, sql, params=None):
        self.запросы.append((" ".join(str(sql).split()), params))

    def fetchone(self):
        self.шаги += 1
        if self.шаги == 1:
            return (КРИВАЯ_ЕДИНИЦА, СТАРЫЙ, СУДЬИ, None)
        return (self._двойник,) if self._двойник else None

    def fetchall(self):
        # Карточки, которые переезжают на выжившее слово: одна, владельца.
        return [(2365, 117649764)]

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False


class Соединение:
    def __init__(self, курсор):
        self._курсор = курсор
        self.commits = 0

    def cursor(self, *a, **k):
        return self._курсор

    def commit(self):
        self.commits += 1

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False


def _применить(двойник):
    курсор = Курсор(двойник)
    with mock.patch.object(database, "get_db_connection_context",
                           return_value=Соединение(курсор)), \
         mock.patch.object(database, "_ensure_phrase_check_tables"), \
         mock.patch.object(database, "spread_correction_everywhere",
                           return_value={"cards": 1, "places": 4}) as развоз, \
         mock.patch.object(database, "rebuild_unit_breakdown", return_value=True) as разбор, \
         mock.patch.object(database, "promote_owner_translation", return_value=True), \
         mock.patch("backend.lex_units.merge_unit_into") as слияние, \
         mock.patch.object(database, "_прибрать_повторы_после_слияния",
                           return_value=0) as уборка, \
         mock.patch("backend.lex_units.retitle_unit") as переименование, \
         mock.patch("backend.card_complaints.подчистить_после_переименования",
                    return_value={"пул": 0, "кеш": 0}):
        итог = database.apply_phrase_review_decision(
            307, "accept", "", 0, "", chosen_text=НОВЫЙ)
    return итог, курсор, слияние, переименование, развоз, разбор, уборка


class ДвойникЕсть(unittest.TestCase):
    def setUp(self):
        (self.итог, self.курсор, self.слияние, self.переименование,
         self.развоз, self.разбор, self.уборка) = _применить(ПРАВИЛЬНАЯ_ЕДИНИЦА)

    def test_the_fix_is_not_thrown_away(self):
        """Главное. Раньше здесь возвращалась пустая правка и вопрос закрывался."""
        self.assertEqual(self.итог["text"], НОВЫЙ)
        self.assertEqual(self.итог["merged_into"], ПРАВИЛЬНАЯ_ЕДИНИЦА)

    def test_the_crooked_unit_is_merged_into_the_right_one(self):
        self.слияние.assert_called_once()
        аргументы = self.слияние.call_args.args
        self.assertEqual(аргументы[1], КРИВАЯ_ЕДИНИЦА)
        self.assertEqual(аргументы[2], ПРАВИЛЬНАЯ_ЕДИНИЦА)

    def test_nothing_is_renamed_into_an_occupied_title(self):
        """Заголовок правильной единицы уже верный — переименовывать нечего и некуда."""
        self.переименование.assert_not_called()

    def test_card_texts_are_fixed_before_the_move(self):
        """Развоз идёт по КРИВОЙ единице: карточки ещё висят на ней."""
        self.развоз.assert_called_once()
        self.assertEqual(self.развоз.call_args.kwargs["unit_id"], КРИВАЯ_ЕДИНИЦА)
        self.assertEqual(self.развоз.call_args.kwargs["new_text"], НОВЫЙ)

    def test_the_question_is_closed_as_decided_not_as_dropped(self):
        сохранённые = [(sql, p) for sql, p in self.курсор.запросы
                       if "UPDATE bt_3_phrase_review" in sql]
        self.assertTrue(сохранённые, "решение не записано вовсе")
        self.assertIn("accepted", сохранённые[0][1])
        self.assertIn(НОВЫЙ, сохранённые[0][1])

    def test_the_breakdown_is_built_for_the_surviving_unit(self):
        """Карточка человека теперь висит на выжившей единице — разбор нужен ей."""
        self.разбор.assert_called_once()
        self.assertEqual(self.разбор.call_args.args[0], ПРАВИЛЬНАЯ_ЕДИНИЦА)

    def test_the_person_is_not_left_with_the_same_card_twice(self):
        """Переехавшая карточка могла встретить на выжившем слове свою же копию.

        Найдено по дороге 28.08.2026: пять починенных правок дали пять таких встреч
        у владельца, в двух карточки совпали целиком. Сносим только полное совпадение
        — правило берётся из продукта (`dedupe_personal_entry_after_save`)."""
        self.уборка.assert_called_once()
        переехавшие, выжившая = self.уборка.call_args.args
        self.assertEqual(выжившая, ПРАВИЛЬНАЯ_ЕДИНИЦА)
        self.assertEqual(переехавшие, [(2365, 117649764)],
                         "список переехавших снят ПОСЛЕ слияния — там их уже не отличить")


class ДвойникаНет(unittest.TestCase):
    """Обычный путь не задет: нет двойника — обычное переименование."""

    def setUp(self):
        (self.итог, self.курсор, self.слияние, self.переименование,
         self.развоз, self.разбор, self.уборка) = _применить(None)

    def test_it_renames_as_before(self):
        self.переименование.assert_called_once()
        self.слияние.assert_not_called()
        self.assertEqual(self.итог["text"], НОВЫЙ)
        self.assertNotIn("merged_into", self.итог)


if __name__ == "__main__":
    unittest.main()
