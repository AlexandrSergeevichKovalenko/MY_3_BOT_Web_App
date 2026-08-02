"""Время активной учёбы словам: дневная сумма живёт на сервере, отрезки идемпотентны.

Что охраняем (боль, из-за которой счётчик переписан):
  * бейдж терял ВСЁ время сеанса, если приложение убивали без события паузы —
    поэтому открытый отрезок досылается под одним segment_id и должен ложиться
    в UPSERT, а не плодить копии;
  * опоздавшая досылка не должна откатывать уже учтённое время (GREATEST);
  * подкрученные часы на телефоне не должны сыпать время в чужой день.

SQL проверяем по форме на моках: живая база в тестах запрещена.
"""
import unittest
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from unittest.mock import patch

import backend.database as db


class _FakeCursor:
    def __init__(self, totals):
        self._totals = list(totals)
        self.statements = []
        self.params = []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        self.statements.append(" ".join(str(sql).split()))
        self.params.append(list(params or []))

    def fetchone(self):
        return (self._totals.pop(0),) if self._totals else (0,)


class _FakeConn:
    def __init__(self, cursor):
        self._cursor = cursor
        self.commits = 0

    def cursor(self):
        return self._cursor

    def commit(self):
        self.commits += 1


@contextmanager
def _fake_ctx(conn):
    yield conn


class StudyTimeSegmentWriteTests(unittest.TestCase):
    def _upsert(self, **kwargs):
        cursor = _FakeCursor(totals=[kwargs.pop("_total", 900)])
        conn = _FakeConn(cursor)
        with patch.object(db, "ensure_study_time_schema", lambda: None), \
                patch.object(db, "get_db_connection_context", lambda: _fake_ctx(conn)):
            result = db.upsert_study_time_segment(**kwargs)
        return result, cursor

    def test_repeat_of_same_segment_updates_in_place_and_never_shrinks(self):
        _total, cursor = self._upsert(
            user_id=42,
            surface="words",
            local_day=date(2026, 8, 2),
            segment_id="seg-1",
            active_seconds=75,
            started_at=datetime(2026, 8, 2, 9, 0, tzinfo=timezone.utc),
        )
        insert_sql = cursor.statements[0]
        # Один и тот же отрезок дошлётся много раз — вставка обязана быть UPSERT'ом
        # по (user_id, segment_id), иначе каждая досылка задвоила бы время.
        self.assertIn("ON CONFLICT (user_id, segment_id) DO UPDATE", insert_sql)
        # Опоздавшая копия несёт МЕНЬШЕ секунд, чем уже записано: берём большее.
        self.assertIn("GREATEST(", insert_sql)
        self.assertIn(75, cursor.params[0])

    def test_segment_length_is_capped(self):
        _total, cursor = self._upsert(
            user_id=42,
            surface="words",
            local_day=date(2026, 8, 2),
            segment_id="seg-2",
            active_seconds=99 * 60 * 60,
        )
        self.assertIn(db.STUDY_TIME_MAX_SEGMENT_SECONDS, cursor.params[0])

    def test_day_total_is_capped(self):
        total, _cursor = self._upsert(
            user_id=42,
            surface="words",
            local_day=date(2026, 8, 2),
            segment_id="seg-3",
            active_seconds=60,
            _total=99 * 60 * 60,
        )
        self.assertEqual(total, db.STUDY_TIME_MAX_DAY_SECONDS)

    def test_empty_segment_id_is_rejected(self):
        with patch.object(db, "ensure_study_time_schema", lambda: None):
            with self.assertRaises(ValueError):
                db.upsert_study_time_segment(
                    user_id=42,
                    surface="words",
                    local_day=date(2026, 8, 2),
                    segment_id="   ",
                    active_seconds=10,
                )

    def test_unknown_surface_falls_back_to_words(self):
        self.assertEqual(db._normalize_study_surface("prank"), "words")
        self.assertEqual(db._normalize_study_surface("words"), "words")


class StudyTimeLocalDayTests(unittest.TestCase):
    """День берём у клиента (счётчик показывает ЕГО сегодня), но не любой."""

    def setUp(self):
        import backend.backend_server as bs
        self.bs = bs
        self.server_today = date(2026, 8, 2)

    def _resolve(self, raw):
        with patch.object(self.bs, "_get_local_today_date", lambda *a, **k: self.server_today):
            return self.bs._resolve_study_time_local_day(raw)

    def test_client_day_is_accepted_when_adjacent(self):
        for offset in (-1, 0, 1):
            candidate = self.server_today + timedelta(days=offset)
            self.assertEqual(self._resolve(candidate.isoformat()), candidate)

    def test_far_away_day_falls_back_to_server_today(self):
        self.assertEqual(self._resolve("2019-01-01"), self.server_today)
        self.assertEqual(self._resolve("2030-01-01"), self.server_today)

    def test_garbage_and_missing_fall_back_to_server_today(self):
        for raw in ("", None, "не дата", "2026-13-45"):
            self.assertEqual(self._resolve(raw), self.server_today)


if __name__ == "__main__":
    unittest.main()
