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


class ДверьИОтчётСчитаютОдноИТоЖе(unittest.TestCase):
    """Владелец 28.08.2026: «А почему 16 пользователей, если вот мой отчёт приходит:
    Всего живых пользователей: 14?»

    Дверь брала сырой COUNT(*), отчёт — с фильтром. Разницу давали строки id=7 и
    id=777, осевшие от прогонов кода по боевой базе (телеграмных id такой длины не
    бывает). Два экрана, дающих два ответа на один вопрос, — дефект того же рода, что
    и заглушка: оба выглядят рабочими. Правило вынесено в REAL_ALLOWED_USER_SQL, и оба
    места берут его оттуда."""

    def test_правило_живого_человека_одно_на_проект(self):
        self.assertTrue(hasattr(db, "REAL_ALLOWED_USER_SQL"))

    def test_дверь_не_считает_сырым_count(self):
        """Именно сырой COUNT(*) и дал расхождение 16 против 14."""
        import inspect
        код = inspect.getsource(db.count_allowed_users)
        self.assertIn("REAL_ALLOWED_USER_SQL", код)
        self.assertNotIn("SELECT COUNT(*) FROM bt_3_allowed_users;", код)

    def test_отчёт_берёт_то_же_правило_а_не_свою_копию(self):
        """Копия правила разошлась бы снова — молча и незаметно."""
        import inspect
        код = inspect.getsource(db.get_access_growth_snapshot)
        self.assertIn("REAL_ALLOWED_USER_SQL", код)
        self.assertNotIn("NOT LIKE 'load_test", код)


class ПроверитьДверьСвоимиРукамиМожноБезПотерь(unittest.TestCase):
    """Владелец 28.08.2026: «хочу отключиться на втором аккаунте, чтобы потом войти
    как новый пользователь». Для этого нельзя предлагать /deny: он ставит данные в
    очередь на стирание и отменяет подписку — аккаунт потерял бы слова и прогресс."""

    def test_сброс_убирает_только_пропуск_и_место_в_очереди(self):
        import inspect
        код = inspect.getsource(db.forget_user_for_retest)
        self.assertIn("DELETE FROM bt_3_allowed_users", код)
        self.assertIn("DELETE FROM bt_3_access_waitlist", код)
        # Ничего из данных человека трогать не имеем права.
        for запретное in ("bt_3_user_removal_queue", "bt_3_dictionary", "bt_3_user_progress",
                          "subscription", "DROP", "TRUNCATE"):
            self.assertNotIn(запретное, код, f"сброс не имеет права трогать {запретное}")


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

    def test_кнопка_чата_открывается_штатным_способом_telegram(self):
        """Замер 28.08.2026 на живом телефоне: кнопка не делала НИЧЕГО.

        Внутри мини-аппа переход по tg:// и по https://t.me через window.location.href
        оболочка игнорирует. Штатный способ ровно один — openTelegramLink(), им
        пользуется весь остальной проект. Прежний код писался для экрана «доступ
        закрыт», а тот показывается только ВНЕ Telegram, поэтому поломка не всплывала.

        Кнопка не украшение: человеку, который пришёл в приложение и с ботом ни разу не
        переписывался, написать физически нельзя. Нажатие создаёт чат — и только после
        этого обещание «напишу, когда откроем» становится выполнимым."""
        # Сама реализация переехала в общий модуль: таких кнопок в проекте четыре,
        # и все они обязаны вести себя одинаково (см.
        # test_bot_chat_buttons_actually_leave.py). Здесь стережём, что ЭТОТ экран
        # берёт общую, а не заводит свою копию с прежней поломкой.
        self.assertIn("from './telegramNav.js'", self.главный)
        начало = self.главный.index("function showAccessQueueGate(")
        экран = self.главный[начало:начало + 2600]
        self.assertIn("openBotChat(uname, 'queue')", экран)
        self.assertNotIn("window.location.href", экран,
                         "переход внутри мини-аппа так не работает — это и была поломка")

    def test_кнопка_закрывает_мини_апп_иначе_она_выглядит_мёртвой(self):
        """Замер на живом телефоне 28.08.2026: нажатие не делало ВИДИМО ничего.

        Причина не в ссылке. Чат с ботом лежит ПРЯМО ПОД мини-аппом — человек открыл
        приложение из этого же чата. Telegram честно открывает чат, но поверх остаётся
        то же приложение, и со стороны это неотличимо от мёртвой кнопки.

        Закрытие мини-аппа И ЕСТЬ переход в чат для того, кто пришёл из чата. Вызов
        openTelegramLink остаётся для второго случая — человека, попавшего в приложение
        по прямой ссылке: у него чата с ботом нет, и ссылка его создаёт."""
        import pathlib
        модуль = (pathlib.Path(__file__).resolve().parents[2]
                  / "frontend" / "src" / "telegramNav.js").read_text(encoding="utf-8")
        self.assertIn("tg.openTelegramLink(httpsUrl)", модуль)
        self.assertIn("tg.close()", модуль)

    def test_на_экране_видно_какая_это_сборка(self):
        """«А этот телефон вообще выполняет новый код?» — вопрос, на котором в проекте
        уже сгорело три дня. Теперь ответ виден глазами, на самом экране."""
        self.assertIn("__BUILD_STAMP__", self.главный)
        self.assertIn("buildStampNode()", self.главный)

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

    def test_письмо_уходит_один_раз_и_ничего_не_откладывается(self):
        """Решение владельца 28.08.2026: «мне не нужно доставлять ничего пользователю
        позже». Отложенного долга нет — и его не жаль: доступ открывает СТРОКА В
        ТАБЛИЦЕ, а не письмо. Человек внутри в ту же секунду; не дошло письмо — он
        увидит открытое приложение сам. Утренний дайджест писем НЕ шлёт."""
        начало = self.бот.index("async def _daily_access_digest_job")
        задача = self.бот[начало:начало + 2000]
        self.assertNotIn("_notify_admitted_waitlist_users", задача)

    def test_неудача_письма_не_отменяет_впуск(self):
        """Не смогли написать — человек всё равно впущен. Письмо только зовёт вернуться."""
        начало = self.бот.index("async def _notify_admitted_waitlist_users")
        отправка = self.бот[начало:начало + 2200]
        self.assertIn("continue", отправка)
        self.assertIn("он ВНУТРИ", отправка)

    def test_закрыть_и_открыть_дверь_без_арифметики(self):
        """Владельцу нельзя предлагать считать в уме, сколько людей внутри: число
        меняется само, и он ошибётся ровно один раз — незаметно."""
        self.assertIn('if команда in ("zakryt", "закрыть")', self.бот)
        self.assertIn('if команда in ("otkryt", "открыть")', self.бот)

    def test_сброс_аккаунта_не_зовёт_deny(self):
        начало = self.бот.index('if команда in ("zabud", "забудь")')
        кусок = self.бот[начало:начало + 1500]
        self.assertIn("forget_user_for_retest", кусок)
        self.assertNotIn("deny", кусок.lower().replace("/deny", ""))

    def test_сводка_по_очереди_приходит_несколько_раз_в_день(self):
        """Владелец 28.08.2026: «что они будут ждать сутки целые подключения?! я могу
        забывать вызывать этот отчёт». Человек, вставший в очередь утром, не должен
        ждать впуска до следующего утра из-за того, что отчёт приходит раз в сутки."""
        self.assertIn("ЧАСЫ_СВОДКИ_ОЧЕРЕДИ = (10, 13, 16, 19)", self.бот)
        self.assertIn("async def _access_queue_pulse_job", self.бот)
        self.assertIn("name=f\"access_queue_pulse_{час}\"", self.бот)

    def test_сводка_молчит_когда_молчать_правильно(self):
        """Четыре сообщения в день «никого нет» превратятся в шум, который перестают
        читать, — и тогда пропустят настоящее."""
        начало = self.бот.index("async def _access_queue_pulse_job")
        задача = self.бот[начало:начало + 1600]
        self.assertIn('if not дверь.get("cap_enabled"):', задача)
        self.assertIn("if ждут <= 0:", задача)

    def test_порции_впуска_крупнее_чем_были(self):
        """Владелец спросил, есть ли что-то кроме 25 и 50. Сотня — крупный шаг роста,
        а любое другое число задаётся командой."""
        self.assertIn("ПОРЦИИ_ВПУСКА = (25, 50, 100)", self.бот)

    def test_состояние_двери_приходит_само_с_кнопками(self):
        """Владелец: «всё, что я должен вызывать командой, я забуду»."""
        начало = self.бот.index("async def _daily_access_digest_job")
        задача = self.бот[начало:начало + 2000]
        self.assertIn("_format_access_gate_text", задача)
        self.assertIn("_access_gate_keyboard", задача)


if __name__ == "__main__":
    unittest.main()
