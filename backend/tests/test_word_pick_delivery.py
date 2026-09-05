# -*- coding: utf-8 -*-
"""«Слова со вчерашних тренировок»: набор дня, получатели, отметка прохода, норма."""
import os
import unittest
from datetime import date
from unittest import mock

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")

from backend import database as db  # noqa: E402


def _соединение_с_курсором(курсор):
    ctx = mock.MagicMock()
    ctx.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = курсор
    return ctx


class НаборДня(unittest.TestCase):

    def setUp(self):
        db._word_pick_schema_ready = True

    def test_получатели_это_все_у_кого_есть_отбор_на_день(self):
        курсор = mock.MagicMock()
        курсор.fetchall.return_value = [(7, 3), (9, 1)]
        with mock.patch.object(db, "get_db_connection_context", _соединение_с_курсором(курсор)):
            out = db.list_word_pick_recipients(date(2026, 9, 6))
        self.assertEqual(out, [{"user_id": 7, "count": 3}, {"user_id": 9, "count": 1}])
        sql = курсор.execute.call_args.args[0]
        self.assertIn("FROM bt_3_word_picks", sql)
        self.assertIn("for_day = %s", sql)

    def test_набор_дня_берёт_карточку_и_состояние_без_проверки_срока(self):
        """Срок карточки (due_at) НЕ фильтруется: вечером набор показывается снова,
        даже если утром поставили «Легко» (решение владельца 04.09.2026)."""
        курсор = mock.MagicMock()
        курсор.fetchall.return_value = [(
            99, "быстрый", None, "schnell", "быстрый", "de", "ru", {"word_de": "schnell"}, [],
            "new", None, None, 0, 0, 0, 0.0, 0.0, 0, None, None, "trainer_save",
        )]
        with mock.patch.object(db, "get_db_connection_context", _соединение_с_курсором(курсор)), \
             mock.patch.object(db, "attach_unit_content_to_cards", lambda items, **kw: None):
            out = db.list_word_pick_cards(user_id=7, for_day=date(2026, 9, 6))
        sql = курсор.execute.call_args.args[0]
        self.assertNotIn("due_at <=", sql)
        self.assertIn("LEFT JOIN bt_3_card_srs_state", sql)
        self.assertEqual(out[0]["card"]["id"], 99)
        self.assertEqual(out[0]["srs"]["status"], "new")
        self.assertIsNone(out[0]["am_rated_at"])

    def test_карточка_без_состояния_отдаёт_srs_none(self):
        курсор = mock.MagicMock()
        курсор.fetchall.return_value = [(
            99, "быстрый", None, "schnell", "быстрый", "de", "ru", {}, [],
            None, None, None, None, None, None, None, None, None, None, None, "trainer_save",
        )]
        with mock.patch.object(db, "get_db_connection_context", _соединение_с_курсором(курсор)), \
             mock.patch.object(db, "attach_unit_content_to_cards", lambda items, **kw: None):
            out = db.list_word_pick_cards(user_id=7, for_day=date(2026, 9, 6))
        self.assertIsNone(out[0]["srs"])

    def test_отметка_прохода_ставится_один_раз_и_только_в_свою_колонку(self):
        курсор = mock.MagicMock()
        db.mark_word_pick_rated(user_id=7, card_id=99, for_day=date(2026, 9, 6), slot="pm", cursor=курсор)
        sql = курсор.execute.call_args.args[0]
        self.assertIn("SET pm_rated_at = COALESCE(pm_rated_at, NOW())", sql)
        self.assertNotIn("am_rated_at", sql)
        with self.assertRaises(ValueError):
            db.mark_word_pick_rated(user_id=7, card_id=99, for_day=date(2026, 9, 6), slot="noon", cursor=курсор)


class СверхНормы(unittest.TestCase):

    def test_код_wp_исключён_из_нормы_везде_где_исключён_rv(self):
        self.assertEqual(tuple(db.INBOX_BONUS_KINDS), ("rv", "wp"))
        import inspect
        for fn in (db.get_inbox_delivery_stats_today, db.get_inbox_kinds_today):
            src = inspect.getsource(fn)
            self.assertNotIn("kind <> 'rv'", src, fn.__name__)
            self.assertIn("_INBOX_BONUS_KINDS_SQL", src, fn.__name__)
        self.assertIn("'wp'", db._INBOX_BONUS_KINDS_SQL)
        from backend.free_delivery_report import BONUS_INBOX_KINDS
        self.assertEqual(BONUS_INBOX_KINDS, {"rv", "wp"})


class Рассылка(unittest.TestCase):

    def test_постер_рисуется_и_называет_число_слов(self):
        from backend import interactive_card as ic
        if ic.Image is None:
            self.skipTest("Pillow не установлен")
        png = ic.render_word_pick_card(count=7)
        self.assertTrue(png and png[:8] == b"\x89PNG\r\n\x1a\n")
        self.assertEqual(ic._ru_words(1), "1 слово")
        self.assertEqual(ic._ru_words(3), "3 слова")
        self.assertEqual(ic._ru_words(7), "7 слов")
        self.assertEqual(ic._ru_words(21), "21 слово")
        # Мотив — карточка Space Rep (выбор владельца 05.09.2026), не der/die/das.
        import inspect
        self.assertIn("motif=_motif_srs_card", inspect.getsource(ic.render_word_pick_card))

    def test_бот_шлёт_ссылку_которую_приложение_умеет_открыть(self):
        """Та же сверка, что в test_every_bot_link_opens, только адресно про wp."""
        import pathlib
        корень = pathlib.Path(__file__).resolve().parents[2]
        bot = (корень / "bot_3.py").read_text(encoding="utf-8")
        jsx = (корень / "frontend/src/answer/AnswerOverlay.jsx").read_text(encoding="utf-8")
        self.assertIn("deeplink_for(", bot)
        self.assertRegex(jsx, r"\^ans_\([a-z|]*\bwp\b[a-z|]*\)_")

    def test_слоты_рассылки_равны_решению_владельца(self):
        import re, pathlib
        корень = pathlib.Path(__file__).resolve().parents[2]
        bot = (корень / "bot_3.py").read_text(encoding="utf-8")
        m = re.search(r"WORD_PICK_SLOT_TIMES\s*=\s*\{([^}]*)\}", bot)
        self.assertIsNotNone(m)
        self.assertIn("(7, 25)", m.group(1)); self.assertIn("(19, 35)", m.group(1))
        # Регистрация крона идёт через make_bonus_gated — сверх нормы, без тарифного среза.
        self.assertIn('make_bonus_gated("word_pick"', bot)
        # Тихие часы в отправителе НЕ проверяются (решение владельца 04.09.2026).
        тело = bot.split("async def _send_word_pick_reviews(", 1)[1].split("\nasync def ", 1)[0]
        self.assertNotIn("_is_quiet_hours_now", тело)
        self.assertIn("_is_access_locked_cached", тело)
        self.assertIn("_user_send_budget", тело)          # «Тишина»
        self.assertIn("list_bot_blocked_user_ids", тело)  # заблокировавшие бота


class ОбещанияИОтчёт(unittest.TestCase):

    def test_обещания_зарегистрированы(self):
        from backend import fix_promises
        ключи = {p.key for p in fix_promises.PROMISES}
        self.assertIn("word_pick_door_writes_every_tap", ключи)
        self.assertIn("word_pick_two_posters_per_picker", ключи)
        for p in fix_promises.PROMISES:
            if p.key.startswith("word_pick_"):
                self.assertEqual(p.expected, 0)
                self.assertTrue(p.how)

    def test_дверь_меряется_по_вчерашним_тапам_из_следа_двери(self):
        """05.09.2026: раньше проверка перебирала сохранения словаря по origin_process и
        времени правки карточки — и приносила «нарушено» за день ДО двери и после каждого
        ночного обогащения. Теперь она читает след тапа, который пишет сама дверь
        (bt_3_word_pick_taps), с границей рождения двери. Разбор — у
        count_word_pick_door_misses и в test_word_pick_door."""
        курсор = mock.MagicMock()
        курсор.fetchone.return_value = (0,)
        with mock.patch.object(db, "get_db_connection_context", _соединение_с_курсором(курсор)):
            self.assertEqual(db.count_word_pick_door_misses(), 0)
        sql = курсор.execute.call_args.args[0]
        self.assertIn("FROM bt_3_word_pick_taps", sql)
        self.assertIn("LEFT JOIN bt_3_word_picks", sql)
        self.assertIn("p.id IS NULL", sql)
        self.assertIn(db.WORD_PICK_DOOR_BORN_AT, курсор.execute.call_args.args[1])

    def test_строка_отчёта_есть_в_утреннем_отчёте(self):
        import pathlib
        bot = (pathlib.Path(__file__).resolve().parents[2] / "bot_3.py").read_text(encoding="utf-8")
        self.assertIn("text += _word_pick_report_line()", bot)
        self.assertIn("🔁 <b>Повтор слов</b>", bot)


    def test_превью_умеет_показать_завтрашний_набор(self):
        import pathlib
        bot = (pathlib.Path(__file__).resolve().parents[2] / "bot_3.py").read_text(encoding="utf-8")
        тело = bot.split("async def _admin_wordpick_preview_command(", 1)[1].split("\nasync def ", 1)[0]
        self.assertIn('"завтра"', тело)
        self.assertIn("timedelta(days=1)", тело)
