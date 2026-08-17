"""Снятие негодной формы слова и его причина — одно действие, разлучить нельзя.

Дефект 17.08.2026. Справочник родов узнавал негодные написания по стоп-листу, а снятие
и запись в стоп-лист были ДВУМЯ разными действиями в разных скриптах. Скрипт, снявший
«die Schuhe» и «das Seifenblasen» без записи, оставил справочник слепым — и тот
продолжал выдавать род у формы множественного числа. Через эту подмену german_surface
объявляет множественное документированным существительным в единственном числе, и в
базу ложится карточка «der Handschuhe».

Поэтому причина переехала на САМУ СТРОКУ: тот же UPDATE, что снимает слово, пишет и
причину. Забыть её нельзя — это физически один запрос.
"""

import unittest
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import backend.database as db


def _fake_conn():
    cur = MagicMock()
    cur.rowcount = 1
    cur.fetchall.return_value = []
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur

    @contextmanager
    def ctx(*a, **k):
        yield conn

    return ctx, cur, conn


def _sqls(cur):
    return [" ".join(c[0][0].split()) for c in cur.execute.call_args_list]


class OneDoorTests(unittest.TestCase):
    def _run(self, words):
        ctx, cur, conn = _fake_conn()
        with patch.object(db, "get_db_connection_context", ctx), \
             patch.object(db, "ensure_article_sprint_schema", lambda: None):
            res = db.retire_bad_word_forms(words)
        return res, cur, conn

    def test_retire_and_reason_are_one_statement(self):
        """Главное свойство: причина пишется тем же UPDATE, что снимает слово."""
        _res, cur, _conn = self._run(["Mängel"])
        upd = [s for s in _sqls(cur) if s.startswith("UPDATE bt_3_article_sprint_nouns")]
        self.assertEqual(len(upd), 1, "снятие должно быть ровно одним UPDATE")
        self.assertIn("retired = TRUE", upd[0])
        self.assertIn("retire_reason = %s", upd[0])

    def test_it_also_stops_the_night_from_proposing_it_again(self):
        _res, cur, _conn = self._run(["Mängel"])
        self.assertTrue(any("bt_3_article_word_blacklist" in s for s in _sqls(cur)))

    def test_everything_commits_together(self):
        """Один коммит: иначе снятие останется, а стоп-лист нет — та же рассинхронизация."""
        _res, _cur, conn = self._run(["Mängel"])
        self.assertEqual(conn.commit.call_count, 1)

    def test_marked_reviewed_so_it_never_asks_the_owner_again(self):
        _res, cur, _conn = self._run(["Mängel"])
        upd = [s for s in _sqls(cur) if s.startswith("UPDATE bt_3_article_sprint_nouns")][0]
        self.assertIn("retire_reviewed = TRUE", upd)

    def test_blacklist_stores_the_spelling_not_the_lemma(self):
        """В стоп-лист идёт «Mängel», чтобы единственное «Mangel» осталось свободным."""
        _res, cur, _conn = self._run(["Mängel"])
        params = [c[0][1] for c in cur.execute.call_args_list if len(c[0]) > 1]
        self.assertTrue(any("mängel" in str(p) for p in params))
        self.assertFalse(any(str(p).count("mangel") and "mängel" not in str(p) for p in params))

    def test_empty_input_touches_nothing(self):
        ctx, cur, _conn = _fake_conn()
        with patch.object(db, "get_db_connection_context", ctx):
            res = db.retire_bad_word_forms([])
        self.assertEqual(res, {"retired": 0, "blacklisted": 0})
        cur.execute.assert_not_called()

    def test_database_silence_is_not_a_silent_success(self):
        """Сбой возвращает нули, а не делает вид, что сработало."""
        @contextmanager
        def boom(*a, **k):
            raise RuntimeError("нет базы")
            yield
        with patch.object(db, "get_db_connection_context", boom), \
             patch.object(db, "ensure_article_sprint_schema", lambda: None):
            self.assertEqual(db.retire_bad_word_forms(["X"]), {"retired": 0, "blacklisted": 0})


class BadFormListTests(unittest.TestCase):
    def test_reads_both_the_row_reason_and_the_old_blacklist(self):
        """Причина на строке — основной источник, стоп-лист — исторический. Объединение,
        а не выбор: записи, сделанные до появления колонки, терять нельзя."""
        ctx, cur, _conn = _fake_conn()
        cur.fetchall.return_value = [("mängel",), ("zitate",)]
        with patch.object(db, "get_db_connection_context", ctx):
            out = db.list_bad_word_forms()
        self.assertEqual(out, {"mängel", "zitate"})
        sql = _sqls(cur)[0]
        self.assertIn("retire_reason ILIKE", sql)
        self.assertIn("bt_3_article_word_blacklist", sql)
        self.assertIn("UNION", sql)

    def test_the_reason_string_lives_in_one_place(self):
        self.assertEqual(db.RETIRE_REASON_BAD_FORM, "форма множественного числа")

    def test_reading_never_touches_the_schema(self):
        """Список читает справочник родов на горячем пути. ALTER TABLE ждёт монопольной
        блокировки, и один долгий читатель рядом подвесил бы выдачу артиклей всему
        продукту — поймано 17.08.2026: собственная проверка висела девять минут."""
        ctx, _cur, _conn = _fake_conn()
        with patch.object(db, "get_db_connection_context", ctx), \
             patch.object(db, "ensure_article_sprint_schema",
                          MagicMock(side_effect=AssertionError("схему трогать нельзя"))):
            db.list_bad_word_forms()


if __name__ == "__main__":
    unittest.main()
