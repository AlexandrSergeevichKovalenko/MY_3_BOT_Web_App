"""Разбор артиклей в личке: спрошенное слово не приезжает вторым комплектом.

25.08.2026 владелец нажал /artikel_review несколько раз подряд и получил ОДНИ И ТЕ ЖЕ
существительные на подтверждение. Подтверждение при этом работало верно (все семь слов
со скриншота ушли из очереди, verified=True) — ломалась именно повторная отправка:
очередь собиралась запросом «первые N строк без подтверждённого рода» и не помнила,
что эти карточки уже висят у владельца в чате.

Здесь закреплены три вещи, чтобы это не вернулось:
  1. очередь не отдаёт слово, про которое уже спросили;
  2. пометка «спрошено» ставится ТОЛЬКО после доставки — иначе недоставленное слово
     тихо исчезло бы из очереди и осталось без рода навсегда;
  3. через REASK_DAYS дней неотвеченное возвращается (решение владельца: напомнить
     через неделю), и в карточке видно, что это повтор.
"""
import unittest
from unittest.mock import patch

import backend.article_review as ar


class _FakeCursor:
    def __init__(self, store):
        self.store = store
        self.sql = ""
        self.params = None
        self.rowcount = 0

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        self.sql = " ".join(str(sql).split())
        self.params = params
        if self.sql.startswith("UPDATE bt_3_article_sprint_nouns SET review_asked_at"):
            self.store["asked"] = list(params[0])
            self.rowcount = len(params[0])


class _FakeConn:
    def __init__(self, cursor):
        self._cursor = cursor
        self.committed = False

    def cursor(self):
        return self._cursor

    def commit(self):
        self.committed = True


class ОчередьНеПовторяетСпрошенное(unittest.TestCase):
    def test_запрос_очереди_отсекает_спрошенное_и_возвращает_его_через_неделю(self):
        """SQL очереди обязан спрашивать про review_asked_at, а не только про verified."""
        import backend.database as db
        курсор = _FakeCursor({})
        курсор.fetchall = lambda: []
        with patch.object(db, "get_db_connection_context") as ctx:
            ctx.return_value.__enter__ = lambda s: _FakeConn(курсор)
            ctx.return_value.__exit__ = lambda s, *a: False
            db.list_unverified_article_nouns(limit=8, reask_days=7)
        self.assertIn("review_asked_at IS NULL", курсор.sql)
        self.assertIn("make_interval(days => %s)", курсор.sql)
        self.assertIn("ORDER BY review_asked_at NULLS FIRST", курсор.sql)
        self.assertEqual(курсор.params, (7, 8))

    def test_пометка_ставится_только_у_неразобранных_строк(self):
        import backend.database as db
        хранилище = {}
        курсор = _FakeCursor(хранилище)
        with patch.object(db, "get_db_connection_context") as ctx:
            ctx.return_value.__enter__ = lambda s: _FakeConn(курсор)
            ctx.return_value.__exit__ = lambda s, *a: False
            сколько = db.mark_article_nouns_asked([11, 12])
        self.assertEqual(сколько, 2)
        self.assertEqual(хранилище["asked"], [11, 12])
        self.assertIn("NOT verified AND NOT retired", курсор.sql)

    def test_пустой_список_ничего_не_обновляет(self):
        import backend.database as db
        with patch.object(db, "get_db_connection_context") as ctx:
            self.assertEqual(db.mark_article_nouns_asked([]), 0)
            ctx.assert_not_called()


class ПовторныйВызовНеШлётТеЖеКарточки(unittest.TestCase):
    ЭЛЕМЕНТ = {"id": 11276, "theme_key": "technik_computer", "word": "SIM",
               "draft_article": "die", "meaning_ru": "сим-карта",
               "source": "gpt-unverified", "asked_at": None}

    def _послать(self, *, очередь, всего, ответ_телеграма=200):
        отправленные: list[int] = []
        помеченные: list[list[int]] = []

        class _Ответ:
            status_code = ответ_телеграма
            text = "" if ответ_телеграма < 400 else "Forbidden"

        def _post(url, **kw):
            отправленные.append(int(kw["json"]["chat_id"]))
            return _Ответ()

        with patch("backend.database.get_admin_telegram_ids", return_value=[117649764]), \
             patch("backend.database.list_unverified_article_nouns", return_value=очередь), \
             patch("backend.database.count_unverified_article_nouns", return_value=всего), \
             patch("backend.database.mark_article_nouns_asked",
                   side_effect=lambda ids: помеченные.append(list(ids))), \
             patch("backend.telegram_delivery.send_telegram_message",
                   return_value=(ответ_телеграма < 400, "")), \
             patch.object(ar.requests, "post", side_effect=_post), \
             patch.dict("os.environ", {"TELEGRAM_Deutsch_BOT_TOKEN": "т"}):
            итог = ar.send_article_review_dm(force=True)
        return итог, отправленные, помеченные

    def test_первый_вызов_шлёт_и_помечает(self):
        итог, отправленные, помеченные = self._послать(очередь=[self.ЭЛЕМЕНТ], всего=7)
        self.assertEqual(итог["sent"], 1)
        self.assertEqual(отправленные, [117649764])
        self.assertEqual(помеченные, [[11276]])

    def test_второй_вызов_молчит_и_называет_число_ждущих(self):
        """Очередь пуста, а неподтверждённых 7 — значит все семь уже спрошены."""
        итог, отправленные, помеченные = self._послать(очередь=[], всего=7)
        self.assertEqual(итог["reason"], "already_asked")
        self.assertEqual(итог["left"], 7)
        self.assertEqual(отправленные, [], "карточки не должны уйти вторым комплектом")
        self.assertEqual(помеченные, [])

    def test_совсем_пустая_очередь_это_другой_ответ(self):
        итог, _, _ = self._послать(очередь=[], всего=0)
        self.assertEqual(итог["reason"], "nothing_to_review")

    def test_недоставленное_не_помечается_спрошенным(self):
        """Иначе слово исчезло бы из очереди, так и не будучи показанным."""
        итог, _, помеченные = self._послать(очередь=[self.ЭЛЕМЕНТ], всего=7,
                                            ответ_телеграма=403)
        self.assertEqual(итог["sent"], 0)
        self.assertEqual(помеченные, [], "непоказанное слово нельзя прятать из очереди")


class ПовторВиденВКарточке(unittest.TestCase):
    def test_у_залежавшегося_слова_написано_с_какого_числа_оно_ждёт(self):
        from datetime import datetime, timezone
        элемент = {"word": "SIM", "meaning_ru": "сим-карта", "draft_article": "die",
                   "asked_at": datetime(2026, 8, 18, 9, 30, tzinfo=timezone.utc)}
        текст = ar._word_text(элемент, index=1, total=3, left=7)
        self.assertIn("Спрашиваю повторно", текст)
        self.assertIn("18.08", текст)

    def test_у_нового_слова_такой_строки_нет(self):
        элемент = {"word": "SIM", "meaning_ru": "сим-карта", "draft_article": "die",
                   "asked_at": None}
        self.assertNotIn("повторно", ar._word_text(элемент, index=1, total=3, left=7))


if __name__ == "__main__":
    unittest.main()
