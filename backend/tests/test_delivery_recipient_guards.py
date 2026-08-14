"""Задания готовятся только тем, кому их вообще можно доставить.

Разбор 14.08.2026. Прогон кода приложения против БОЕВОЙ базы 11.08 записал в список
доступа три выдуманных id (5555, 987654, 776655), и рассыльщик стал каждый день печь
им персональные задания: 198 из 242 подготовленных строк (82% дневной работы) уходили
четырём адресатам, у которых telegram_message_id всегда NULL.

Правило владельца, которое эти тесты обязаны защитить с ОБЕИХ сторон:
  • нажал «Старт» — получает задания ВСЕГДА, даже если никогда не заходит.
    Бездействие адресата не отключает выдачу — никогда;
  • не готовим только тому, кого физически не существует: выдуманный номер либо
    Telegram отвечает «чата нет / аккаунт удалён / бот заблокирован».
"""

import unittest

from telegram.error import BadRequest, Forbidden

import bot_3


class RecipientIdTests(unittest.TestCase):
    """Годится ли номер как получатель. Одного верхнего порога синтетики мало."""

    def test_real_people_are_recipients(self):
        for uid in (117649764, 883565092, 7263482531, 689892478, 8546091375):
            self.assertTrue(bot_3._is_deliverable_recipient_user_id(uid), uid)

    def test_made_up_short_ids_are_not(self):
        # 5555 — из прогона по боевой базе 11.08.2026; 11 и 77 — из тестов.
        for uid in (5555, 11, 77, 1, 99_999):
            self.assertFalse(bot_3._is_deliverable_recipient_user_id(uid), uid)

    def test_load_test_and_negative_ids_are_not(self):
        for uid in (9_100_000_001, 9_937_001_842, 900_000_000_000, -900000002, 0):
            self.assertFalse(bot_3._is_deliverable_recipient_user_id(uid), uid)

    def test_garbage_never_raises(self):
        for value in (None, "", "abc", object()):
            self.assertFalse(bot_3._is_deliverable_recipient_user_id(value))


class RecipientGoneTests(unittest.TestCase):
    """Что считать «адресата больше нет». Здесь цена ошибки — замолчавший живой человек."""

    PRIVATE = 117649764
    GROUP = -1001234567890

    def test_blocked_bot_means_gone(self):
        self.assertTrue(bot_3._recipient_is_gone(Forbidden("bot was blocked by the user"), self.PRIVATE))

    def test_chat_not_found_means_gone(self):
        # Так выглядит выдуманный id: чата под ним не существовало никогда.
        self.assertTrue(bot_3._recipient_is_gone(BadRequest("Chat not found"), self.PRIVATE))

    def test_deleted_account_means_gone(self):
        self.assertTrue(bot_3._recipient_is_gone(BadRequest("User is deactivated"), self.PRIVATE))

    def test_rights_errors_do_NOT_silence_a_live_person(self):
        # «Это сообщение не прошло» ≠ «человека нет». По таким ошибкам глушить нельзя:
        # живой человек перестал бы получать задания навсегда.
        for message in ("not enough rights", "polls can't be sent to private chats",
                        "have no rights to send a message", "message is too long"):
            self.assertFalse(bot_3._recipient_is_gone(BadRequest(message), self.PRIVATE), message)

    def test_transient_errors_do_NOT_silence_anyone(self):
        for exc in (TimeoutError("timed out"), RuntimeError("boom"), Exception("chat not found")):
            self.assertFalse(bot_3._recipient_is_gone(exc, self.PRIVATE), exc)

    def test_groups_are_not_handled_here(self):
        # У групп своя механика выбытия (_is_dead_group_error) — сюда они не попадают.
        self.assertFalse(bot_3._recipient_is_gone(Forbidden("bot was kicked"), self.GROUP))
        self.assertFalse(bot_3._recipient_is_gone(BadRequest("chat not found"), self.GROUP))


class DeliveryPromiseTests(unittest.TestCase):
    """Обещание тарифа не зависит от того, заходит человек или нет."""

    def test_inactivity_never_reduces_the_daily_budget(self):
        self.assertEqual(bot_3._user_send_budget(117649764, is_pro=False), bot_3.FREE_SEND_BUDGET)
        self.assertEqual(bot_3._user_send_budget(117649764, is_pro=True, preset="normal"),
                         bot_3.DEFAULT_PRO_SEND_BUDGET)

    def test_only_silence_stops_delivery(self):
        self.assertEqual(bot_3._user_send_budget(117649764, is_pro=True, preset="silent"), 0)

    def test_lookback_horizon_stays_effectively_unlimited(self):
        # Горизонт «кого считать активным» намеренно огромный: он не должен превратиться
        # в глушение по неактивности (снято 11.08.2026 сознательным решением владельца).
        self.assertGreaterEqual(bot_3.TASK_DELIVERY_LOOKBACK_DAYS, 3650)


if __name__ == "__main__":
    unittest.main()
