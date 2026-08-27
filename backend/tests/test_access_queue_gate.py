# -*- coding: utf-8 -*-
"""Потолок впуска и очередь: человеку говорят, ЧТО с ним и ПОЧЕМУ.

ПОВОД. Владелец 27.08.2026, при разборе плана запуска: наращивать людей ступенями,
чтобы на каждой видеть, справляется ли программа. До этого дня двери не было вовсе —
`_grant_self_serve_webapp_access` впускал любого, кто открыл мини-апп, без счёта и
потолка. А тот, кому всё-таки отказывали (админ запретил), видел тупик:
«Доступ к приложению закрыт. Напишите боту — откроем.»

ТРИ ВЕЩИ, КОТОРЫЕ ЗДЕСЬ СТЕРЕГУТСЯ:

  1. Потолок стоит в ОДНОЙ двери — `auto_grant_telegram_user`. Через неё входят оба
     входа, бот /start и мини-апп; поставь его в каждый по отдельности — и один из
     них рано или поздно останется без него.
  2. Отказ и очередь — РАЗНЫЕ состояния. «Закрыт администратором» человеку, который
     просто пришёл по ссылке, сообщает, что его за что-то наказали. Его никто не
     наказывал: он пришёл раньше, чем мы открыли дверь шире. Ему полагается номер,
     причина человеческими словами и обещание написать самим.
  3. Потолок живёт В БАЗЕ. Решение владельца — двигать ступень КНОПКОЙ, а кнопка не
     может править переменную окружения на Railway: для этого нужен передеплой.
     PUBLIC_ACCESS_CAP осталась начальным значением, не более.

⚠ ПО УМОЛЧАНИЮ ПОТОЛКА НЕТ. Потолок 0 = дверь открыта всем, ровно как было до
27.08.2026. Включает его владелец, осознанно.
"""
import os
import unittest
from unittest import mock

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")

import backend.backend_server as server  # noqa: E402
from backend import database as db  # noqa: E402


class ПотолокСтоитВОднойДвери(unittest.TestCase):

    def setUp(self):
        db._ACCESS_COUNT_CACHE.clear()

    def _впустить(self, cap, впущено):
        """Попытка входа при данном потолке. Возвращает (пустили?, встал_в_очередь?)."""
        очередь = []
        with mock.patch.object(db, "is_access_denied_for_user", return_value=False), \
             mock.patch.object(db, "public_access_cap", return_value=cap), \
             mock.patch.object(db, "count_allowed_users", return_value=впущено), \
             mock.patch.object(db, "add_to_access_waitlist",
                               side_effect=lambda uid, **k: очередь.append(uid)), \
             mock.patch.object(db, "get_db_connection_context") as соединение, \
             mock.patch.object(db, "invalidate_telegram_user_allowed_cache"), \
             mock.patch.object(db, "_invalidate_webapp_allowlist_redis"):
            курсор = mock.MagicMock()
            курсор.fetchone.return_value = (555,)
            соединение.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = курсор
            пустили = db.auto_grant_telegram_user(555, "Кто-то", "invite")
        return пустили, bool(очередь)

    def test_потолка_нет_значит_пускаем_всех(self):
        """Значение по умолчанию — прежнее поведение, без всякой очереди."""
        пустили, в_очереди = self._впустить(cap=0, впущено=10_000)
        self.assertTrue(пустили)
        self.assertFalse(в_очереди)

    def test_место_есть_пускаем(self):
        пустили, в_очереди = self._впустить(cap=50, впущено=49)
        self.assertTrue(пустили)
        self.assertFalse(в_очереди)

    def test_упёрлись_в_потолок_ставим_в_очередь_а_не_в_никуда(self):
        пустили, в_очереди = self._впустить(cap=50, впущено=50)
        self.assertFalse(пустили)
        self.assertTrue(в_очереди, "человек обязан оказаться в очереди, а не просто получить отказ")

    def test_не_смогли_посчитать_не_пускаем(self):
        """Сбой счёта не выдаётся за «место есть»: впустить лишнего можно следующей
        ступенью, а впустить толпу на слабый сервер обратно не отыграешь."""
        with mock.patch.object(db, "public_access_cap", return_value=50), \
             mock.patch.object(db, "count_allowed_users", side_effect=RuntimeError("база молчит")):
            self.assertTrue(db._public_access_cap_reached(555))


class ОчередьИЗапретЭтоРазныеСостояния(unittest.TestCase):

    def test_человеку_в_очереди_дают_его_номер(self):
        with mock.patch("backend.database.public_access_cap", return_value=50), \
             mock.patch("backend.database.access_waitlist_position", return_value=34):
            ответ = server._access_denied_payload(555)
        self.assertEqual(ответ["reason"], "in_queue")
        self.assertEqual(ответ["queue_position"], 34)
        self.assertNotIn("закрыт администратором", ответ["error"])

    def test_запрещённому_админом_прежний_экран(self):
        with mock.patch("backend.database.public_access_cap", return_value=0):
            ответ = server._access_denied_payload(555)
        self.assertEqual(ответ["reason"], "access_closed")

    def test_не_выяснили_какое_состояние_не_врём_ни_в_одну_сторону(self):
        """Третье, незаконное состояние: мы не смогли выяснить, очередь это или запрет.

        Нельзя ни «закрыт администратором» — это обвинило бы человека, которого никто
        не запрещал, за нашу неудачу с базой; ни «вы в очереди» — это пообещало бы
        письмо, которое некому отправить: в очередь его не поставили. Говорим правду:
        у нас не вышло, зайдите позже."""
        with mock.patch("backend.database.public_access_cap",
                        side_effect=RuntimeError("база молчит")):
            ответ = server._access_denied_payload(555)
        self.assertEqual(ответ["reason"], "access_check_failed")
        self.assertNotIn("администратором", ответ["error"])
        self.assertNotIn("очереди", ответ["error"])


class ЭкранОчередиГоворитЧеловеческимЯзыком(unittest.TestCase):

    def setUp(self):
        import pathlib
        корень = pathlib.Path(__file__).resolve().parents[2]
        self.главный = (корень / "frontend" / "src" / "main.jsx").read_text(encoding="utf-8")

    def test_экран_очереди_существует_и_отдельный_от_отказа(self):
        self.assertIn("function showAccessQueueGate(", self.главный)
        self.assertIn("function showAccessClosedGate(", self.главный)

    def test_перехватчик_работает_и_внутри_telegram(self):
        """Токенные шимы выходят раньше внутри Telegram — а очередь обязана
        показываться именно там: оттуда приходит большинство людей."""
        self.assertIn("function installAccessGateInterceptor(", self.главный)
        self.assertIn("installAccessGateInterceptor();", self.главный)

    def test_на_экране_есть_номер_причина_и_обещание(self):
        начало = self.главный.index("function showAccessQueueGate(")
        экран = self.главный[начало:начало + 2600]
        self.assertIn("Ваш номер", экран)
        self.assertIn("порциями", экран)              # причина
        self.assertIn("делать ничего не нужно", экран)  # что от него требуется


class БотРазличаетОчередьИЗапрет(unittest.TestCase):

    def setUp(self):
        import pathlib
        корень = pathlib.Path(__file__).resolve().parents[2]
        self.бот = (корень / "bot_3.py").read_text(encoding="utf-8")

    def test_в_очереди_человек_видит_номер_а_не_запрет(self):
        self.assertIn("_send_access_queue_reply", self.бот)
        self.assertIn("Ваш номер —", self.бот)

    def test_кнопка_поднимает_потолок_вместе_с_впуском(self):
        """Впустить, не подняв потолок, — тут же упереться в него снова."""
        начало = self.бот.index("async def handle_access_gate_callback")
        обработчик = self.бот[начало:начало + 3000]
        self.assertIn("raise_access_cap", обработчик)
        self.assertIn("admit_from_access_waitlist", обработчик)

    def test_впущенному_обязательно_говорят_что_открыли(self):
        self.assertIn("_notify_admitted_waitlist_users", self.бот)
        self.assertIn("Дверь открыта", self.бот)

    def test_недоставленное_обещание_досылается_утром(self):
        """Не приняли сообщение — человек остаётся в выборке, а не теряется навсегда."""
        начало = self.бот.index("async def _daily_access_digest_job")
        задача = self.бот[начало:начало + 2000]
        self.assertIn("_notify_admitted_waitlist_users", задача)

    def test_состояние_двери_приходит_само_с_кнопками(self):
        """Владелец: «всё, что я должен вызывать командой, я забуду»."""
        начало = self.бот.index("async def _daily_access_digest_job")
        задача = self.бот[начало:начало + 2000]
        self.assertIn("_format_access_gate_text", задача)
        self.assertIn("_access_gate_keyboard", задача)


if __name__ == "__main__":
    unittest.main()
