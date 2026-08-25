# -*- coding: utf-8 -*-
"""Словосочетания попадают в ночную очередь, предложения — нет.

РЕШЕНИЕ ВЛАДЕЛЬЦА 25.08.2026, дословно: «словосочетания греем, предложения не греем —
там есть само предложение и перевод, этого достаточно».

Почему это не мелочь. Пока в очереди стояло условие `kind = 'word'`, словосочетания не
попадали в неё ВООБЩЕ и копились пустыми: 1 793 штуки на 25.08.2026, и на них подписаны
живые люди. У словосочетания перевода мало — «Die Jagd auf» не объясняет ни падежа, ни
употребления, поэтому разбор ему нужен. У целого предложения перевод самодостаточен.

Тест смотрит на текст запроса, а не ходит в базу: очередь — это SQL, и сломать её можно
одним словом в условии.
"""
import inspect
import os
import re
import unittest

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SECOND_VOICE_CHECK_DISABLED", "1")

from backend import lex_units  # noqa: E402


class NightQueueScope(unittest.TestCase):
    def test_collocations_are_in_the_queue(self):
        код = inspect.getsource(lex_units.units_needing_card)
        условие = " ".join(код.split())
        self.assertIn("kind IN ('word', 'collocation')", условие,
                      "словосочетания снова выпали из ночной очереди")

    def test_sentences_are_not_in_the_queue(self):
        """Предложения греть не надо — это решение владельца, а не недосмотр."""
        код = " ".join(inspect.getsource(lex_units.units_needing_card).split())
        self.assertNotIn("'sentence'", код)

    def test_counter_counts_the_same_thing_as_the_queue(self):
        """Отчёт «осталось N» обязан считать ТО ЖЕ, что берёт очередь.

        Иначе владелец видит меньшее число, чем есть на самом деле, — а мы уже знаем,
        чем это кончается: 1 793 пустых словосочетания не были видны в отчёте вовсе."""
        счётчик = " ".join(inspect.getsource(lex_units.count_units_needing_card).split())
        self.assertIn("kind IN ('word', 'collocation')", счётчик)


if __name__ == "__main__":
    unittest.main()
