"""Варианты ответа из общего пула: запрос должен ВЫПОЛНЯТЬСЯ и отдавать словарные единицы.

Дефект №1 (30.07.2026). Запрос доливки вариантов падал всегда, с самого коммита
7a02c9db (26.07.2026): `SELECT DISTINCT ... ORDER BY RANDOM()` Postgres не принимает —
«for SELECT DISTINCT, ORDER BY expressions must appear in select list». Проверено
дословным прогоном боевого SQL на живой базе. Эндпоинт глотал исключение и отдавал
пустой список, поэтому в сессии с одним словом квиз так и оставался с одним вариантом —
ровно то, что коммит обещал исправить. Случайный порядок теперь снаружи подзапроса.


Находка 30.07.2026. В тренажёре «Карточки», когда своих слов в сессии мало (разбор
одного слова, свежий аккаунт), варианты ответа доливаются случайными значениями из
общего пула — `fetch_random_pool_words`. Фильтра не было ни в SQL, ни на экране.

Замер на живой базе по паре ru/de: из 14 339 разных значений `word_de` одиночных
слов — 26.7 %, каждое пятое значение содержит точку (это целое предложение), 17 %
длиннее 40 символов, 0.5 % вообще с кириллицей. То есть рядом со словом человеку
могло встать «1972 erkannte die USA China an.» или «$900 zuzüglich Mehrwertsteuer.» —
правильный ответ очевиден, а экран выглядит сломанным.

Дефект №2. Фильтра формы не было ни в SQL, ни на экране. Замер на живой базе по паре
ru/de: из 14 339 разных значений `word_de` одиночных слов — 26.7 %, каждое пятое значение
содержит точку (это целое предложение), 17 % длиннее 40 символов, 0.5 % с кириллицей.
То есть в тот момент, когда падающий запрос починили бы, рядом со словом человеку встало
бы «1972 erkannte die USA China an.» — правильный ответ очевиден, экран сломан.

Причина не в мусоре: люди сохраняют в пул фразы и предложения из читалки и новостей,
и это нормально. Ненормально предлагать их как вариант ответа на слово.
"""

import unittest
from unittest.mock import patch

import backend.database as db


class _DummyCursor:
    def __init__(self):
        self.executed = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchall(self):
        return []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def _db_context(cursor):
    class _Conn:
        def cursor(self):
            return cursor

    class _Ctx:
        def __enter__(self):
            return _Conn()

        def __exit__(self, *exc):
            return False

    return lambda *a, **kw: _Ctx()


class DistractorQueryRunsTests(unittest.TestCase):
    def test_random_order_is_applied_outside_the_distinct_subquery(self):
        """Иначе Postgres отклоняет запрос целиком и вариантов не будет вовсе."""
        cursor = _DummyCursor()
        with patch("backend.database.get_db_connection_context", _db_context(cursor)):
            db.fetch_random_pool_words(source_lang="ru", target_lang="de", limit=12)
        sql = cursor.executed[0][0]
        distinct_at = sql.index("SELECT DISTINCT")
        order_at = sql.index("ORDER BY RANDOM()")
        closing_at = sql.index(") AS candidates")
        self.assertLess(distinct_at, closing_at, "DISTINCT должен жить в подзапросе")
        self.assertLess(closing_at, order_at, "ORDER BY RANDOM() — снаружи подзапроса")


class DistractorShapeGuardTests(unittest.TestCase):
    def _sql(self, learning_lang):
        cursor = _DummyCursor()
        with patch("backend.database.get_db_connection_context", _db_context(cursor)):
            db.fetch_random_pool_words(
                source_lang="ru", target_lang="de", learning_lang=learning_lang, limit=12,
            )
        self.assertEqual(len(cursor.executed), 1)
        return cursor.executed[0][0]

    def test_german_distractors_exclude_sentences_and_long_values(self):
        sql = self._sql("de")
        self.assertIn("!~ '[.!?…]'", sql)
        self.assertIn("length(TRIM(word_de)) <= 32", sql)
        self.assertIn("<= 2", sql)  # не больше двух значимых слов

    def test_german_distractors_exclude_cyrillic(self):
        """В немецкой стороне пула встречаются склеенные двуязычные строки."""
        sql = self._sql("de")
        self.assertIn("TRIM(word_de) !~ '[А-Яа-яЁё]'", sql)

    def test_russian_distractors_require_cyrillic(self):
        sql = self._sql("ru")
        self.assertIn("TRIM(word_ru) ~ '[А-Яа-яЁё]'", sql)
        self.assertIn("length(TRIM(word_ru)) <= 32", sql)

    def test_article_is_stripped_before_counting_words(self):
        """«die Auffassung» — одно значимое слово, а не два; артикль не должен съедать лимит."""
        sql = self._sql("de")
        self.assertIn("der|die|das|ein|eine|einen|einem|einer|eines", sql)


if __name__ == "__main__":
    unittest.main()
