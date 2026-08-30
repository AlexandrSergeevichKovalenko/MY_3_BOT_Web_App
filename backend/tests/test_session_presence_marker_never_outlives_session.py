"""Плашка «набор переводов не закончен» не должна пережить сам набор.

Повод (30.08.2026): владелец увидел на главном экране «5 из 7» от набора, который
ночное задание закрыло сутки назад. Сама сессия в базе была закрыта правильно, врал
указатель: он лежит в двух хранилищах, а закрытие гасило только быстрое (Redis).
Долгая копия в bt_3_user_api_snapshots осталась со словом "active" — и дошла до экрана,
потому что проверка на чтении при ошибке базы отвечала «активна» (fail-open).

Тесты держат три вещи, каждая из которых по отдельности возвращает дефект:
  1. закрыта сессия — гаснет указатель в ОБОИХ хранилищах;
  2. «не смогли проверить» — это не «активна»: плашку не показываем;
  3. закрытая сессия не проходит на экран даже из долгой копии.
"""
import unittest
from unittest.mock import patch

import backend.backend_server as server


ACTIVE_CARD = {
    "state": "active",
    "session_id": "812051317516",
    "session_type": "regular",
    "source_lang": "ru",
    "target_lang": "de",
    "projection_status": "ready",
}


class SessionPresenceMarkerTests(unittest.TestCase):
    def test_unknown_verdict_is_not_treated_as_active(self):
        """База не ответила → «не знаю». Плашку не показываем и карточку не трогаем."""
        with (
            patch.object(server, "get_session_presence_card", return_value=dict(ACTIVE_CARD)),
            patch.object(server, "_is_translation_session_active_in_db", return_value=None),
            patch.object(server, "get_user_api_snapshot", return_value=None),
            patch.object(server, "_evict_stale_session_presence_card") as evict_mock,
        ):
            payload, source = server._load_session_presence_projection_with_source(117649764)

        self.assertIsNone(payload, "«не смогли проверить» не должно выглядеть как открытый набор")
        self.assertIsNone(source)
        evict_mock.assert_not_called()

    def test_closed_session_never_reaches_the_screen_from_the_long_copy(self):
        """Долгая копия говорит active, база говорит «закрыта» → наружу уходит «набора нет»."""
        with (
            patch.object(server, "get_session_presence_card", return_value=None),
            patch.object(server, "get_user_api_snapshot", return_value={"payload": dict(ACTIVE_CARD)}),
            patch.object(server, "_is_translation_session_active_in_db", return_value=False),
        ):
            payload, _source = server._load_session_presence_projection_with_source(117649764)

        self.assertIsNone(payload)

    def test_open_session_still_shows_the_card(self):
        """Обратная сторона: живой набор обязан доходить до экрана, иначе мы сломали фичу."""
        with (
            patch.object(server, "get_session_presence_card", return_value=dict(ACTIVE_CARD)),
            patch.object(server, "_is_translation_session_active_in_db", return_value=True),
        ):
            payload, source = server._load_session_presence_projection_with_source(117649764)

        self.assertIsInstance(payload, dict)
        self.assertEqual(source, "projection_redis")
        response = server._build_session_presence_response_payload(payload)
        self.assertEqual(response.get("type"), "regular")
        self.assertEqual(response.get("session_id"), "812051317516")

    def test_verify_failure_is_counted_so_the_owner_sees_the_number(self):
        """«Не знаю» — незакрытая задача, а не нормальный исход: оно обязано считаться."""
        def _boom(*_args, **_kwargs):
            raise RuntimeError("db is down")

        with (
            patch.object(server, "get_db_connection_context", _boom),
            patch.object(server, "increment_session_presence_verify_unknown", return_value=3) as counter_mock,
        ):
            verdict = server._is_translation_session_active_in_db(117649764, "812051317516")

        self.assertIsNone(verdict, "ошибка базы не имеет права превращаться в «сессия активна»")
        counter_mock.assert_called_once()

    def test_closing_a_session_clears_the_marker_in_both_stores(self):
        """Закрытие сессии и гашение указателя — одно действие, а не два независимых."""
        import backend.database as database

        calls = {}

        class _FakeCursor:
            rowcount = 1

            def execute(self, *_args, **_kwargs):
                calls["closed"] = True

            def fetchall(self):
                return []

            def __enter__(self):
                return self

            def __exit__(self, *_exc):
                return False

        class _FakeConn:
            def cursor(self):
                return _FakeCursor()

            def __enter__(self):
                return self

            def __exit__(self, *_exc):
                return False

        with (
            patch.object(database, "get_db_connection_context", lambda *a, **k: _FakeConn()),
            patch.object(
                database,
                "clear_stale_translation_session_presence_markers",
            ) as clear_mock,
        ):
            closed = database.close_stale_open_translation_sessions_for_user(user_id=117649764)

        self.assertEqual(closed, 1)
        clear_mock.assert_called_once_with(user_ids=[117649764])


if __name__ == "__main__":
    unittest.main()
