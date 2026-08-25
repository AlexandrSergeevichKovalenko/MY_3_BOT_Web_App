# -*- coding: utf-8 -*-
"""Новая карточка в тренировке обязана показывать то же, что словарь.

ПОВОД. Владелец 25.08.2026 спросил прямо: «сейчас я могу видеть нормальную информацию в
карточке, когда тренирую?» Проверка по живой базе ответила: на ПОВТОРЕНИИ — да, склейка
с общим словом там стояла; на НОВОЙ карточке — нет, отдавалась личная копия как есть.

Замер того же дня: из 544 слов, которым накануне переписали примеры, копия в карточке
владельца разошлась с единицей у 540. В словаре он видел новый пример, на первом показе
слова — прежний, а в одном случае русский текст в поле немецкого примера.

Здесь заперто, что оба пути тренировки берут разбор из ОДНОГО места. Тест смотрит на
исходный код, а не на базу: у нас уже был случай, когда правило существовало, работало
в одной ветке и молча отсутствовало в соседней — и заметили это только через неделю.
"""
import inspect
import os
import unittest

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SECOND_VOICE_CHECK_DISABLED", "1")

from backend import database  # noqa: E402


class BothTrainingPathsShareOneRule(unittest.TestCase):
    def test_due_card_takes_content_from_the_word(self):
        """Карточка на повторении: склейка была и должна остаться."""
        source = inspect.getsource(database.get_next_due_srs_card)
        self.assertIn("attach_unit_content_to_cards", source)

    def test_new_card_takes_content_from_the_word(self):
        """Новая карточка: та самая забытая склейка, ради которой тест и написан."""
        source = inspect.getsource(database.get_next_new_srs_candidate)
        self.assertIn("attach_unit_content_to_cards", source,
                      "новая карточка снова отдаёт личную копию — человек учит старое")

    def test_new_card_passes_the_cursor_along(self):
        """Курсор передаётся дальше: без него берётся второе соединение из пула,
        пока первое держит вызывающий. Это известная ловушка проекта."""
        source = inspect.getsource(database.get_next_new_srs_candidate)
        self.assertIn("cursor=cur", source.replace(" ", ""))

    def test_no_second_copy_of_the_rule(self):
        """Правило живёт в одной функции. Своя копия склейки в выдаче карточки означала
        бы два разных ответа на один вопрос — это мы уже проходили."""
        source = inspect.getsource(database.get_next_new_srs_candidate)
        self.assertNotIn("merge_unit_card_for_serve", source,
                         "склейка позвана напрямую мимо общей функции")


if __name__ == "__main__":
    unittest.main()
