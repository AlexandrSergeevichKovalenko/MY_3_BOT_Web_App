"""Спринт синонимов/антонимов начинает помнить, какие слова человек назвал.

До 15.08.2026 в базу уходил только счёт «7 слов за минуту»: какие именно слова человек
вспомнил, а какие упустил, не сохранялось НИГДЕ. Поэтому нельзя было ни вернуть ему
слово, на котором он сел, ни перестать давать пройденное.

Выдачу спринта здесь СОЗНАТЕЛЬНО не трогаем: она привязана к тренажёру («the rail» —
берётся слово, которое человек тренировал три дня назад). Личная выдача разорвала бы эту
учебную связку, а такое решение принимает владелец.
"""

import unittest
from unittest.mock import MagicMock, patch

import backend.database as db


def _fake_conn():
    cur = MagicMock()
    cur.fetchone.return_value = None
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    ctx = MagicMock()
    ctx.__enter__.return_value = conn
    return ctx, cur


class WordOutcomeTests(unittest.TestCase):
    def _record(self, **kw):
        ctx, cur = _fake_conn()
        with patch.object(db, "get_db_connection_context", return_value=ctx), \
             patch.object(db, "ensure_sprint_word_outcomes_schema"), \
             patch.object(db, "_task_rotation_writes_disabled", return_value=False):
            db.record_sprint_word_outcomes(**kw)
        return [c[0][1] for c in cur.execute.call_args_list]

    def test_found_and_missed_are_both_saved(self):
        rows = self._record(user_id=7, sprint_id="s1", relation="synonym",
                            found=["schnell"], missed=["rasch", "flink"])
        self.assertEqual(len(rows), 3)
        flags = {r[3]: r[4] for r in rows}
        self.assertTrue(flags["schnell"])
        self.assertFalse(flags["rasch"])
        self.assertFalse(flags["flink"])

    def test_empty_round_writes_nothing(self):
        self.assertEqual(self._record(user_id=7, sprint_id="s1", relation="synonym",
                                      found=[], missed=[]), [])

    def test_broken_database_never_breaks_the_round(self):
        with patch.object(db, "get_db_connection_context", side_effect=RuntimeError), \
             patch.object(db, "ensure_sprint_word_outcomes_schema"), \
             patch.object(db, "_task_rotation_writes_disabled", return_value=False):
            db.record_sprint_word_outcomes(user_id=7, sprint_id="s1", relation="synonym",
                                           found=["a"], missed=[])


class PickerTests(unittest.TestCase):
    def test_picker_can_skip_what_the_person_passed(self):
        """Возможность есть и проверена; в рассылку она пока НЕ подключена —
        там слово привязано к тренажёру."""
        ctx, cur = _fake_conn()
        with patch.object(db, "get_db_connection_context", return_value=ctx):
            db.pick_next_sprint(relation="synonym", cooldown_days=21,
                                exclude_ids=["sp:s1", "sp:s2"])
        sql = " ".join(cur.execute.call_args[0][0].split())
        params = cur.execute.call_args[0][1]
        self.assertIn("<> ALL(%s)", sql)
        self.assertIn(["s1", "s2"], list(params),
                      "префикс вида должен сниматься: банку нужен голый номер")

    def test_without_exclusions_the_query_is_unchanged(self):
        ctx, cur = _fake_conn()
        with patch.object(db, "get_db_connection_context", return_value=ctx):
            db.pick_next_sprint(relation="synonym", cooldown_days=21)
        self.assertNotIn("<> ALL(%s)", " ".join(cur.execute.call_args[0][0].split()))


if __name__ == "__main__":
    unittest.main()
