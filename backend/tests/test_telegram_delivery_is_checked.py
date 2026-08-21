"""Отправка в Telegram обязана ЗНАТЬ, дошло ли сообщение.

Повод, 20.08.2026. Владелец сказал: «мне ничего не приходило». Отчёт о ночной проверке
фраз существовал, стоял в расписании на 07:05 и нёс кнопку разбора — но все девять мест,
откуда бот пишет в личку, были написаны так:

    try:
        requests.post(f"https://api.telegram.org/bot{token}/sendMessage", json=payload)
    except Exception:
        logging.debug("... send failed", exc_info=True)

Две дыры сразу. `except` ловит только обрыв связи, а Telegram отказывает ТЕЛОМ ответа
(`{"ok": false, "description": "..."}`) — это не исключение. И результат `post` никто не
читал. Поймано вживую: с мёртвым токеном Telegram вернул 401, а код напечатал
«отправлено». То есть отчёт мог не доходить неделями, не оставив следа нигде.

Здесь два правила:
  1. `send_telegram_message` возвращает ЧЕСТНЫЙ ответ на все три исхода — принято,
     отказано телом, оборвалась связь;
  2. в `bot_3.py` не осталось прямых `sendMessage` мимо неё.
"""
import re
import unittest
from pathlib import Path
from unittest.mock import patch

from backend.telegram_delivery import send_telegram_message, send_telegram_message_to_all

BOT_FILE = Path(__file__).resolve().parents[2] / "bot_3.py"


class FakeResponse:
    def __init__(self, status_code, payload):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        return self._payload


class TelegramDeliveryIsCheckedTests(unittest.TestCase):
    def test_accepted_message_is_reported_as_delivered(self):
        with patch("backend.telegram_delivery.requests.post",
                   return_value=FakeResponse(200, {"ok": True, "result": {}})):
            ok, reason = send_telegram_message(chat_id=1, text="привет", token="t")
        self.assertTrue(ok)
        self.assertEqual(reason, "")

    def test_refusal_in_the_body_is_not_mistaken_for_success(self):
        """Главный случай. HTTP 200 и «ok»: false — сообщение НЕ дошло."""
        with patch("backend.telegram_delivery.requests.post",
                   return_value=FakeResponse(200, {
                       "ok": False, "description": "bot was blocked by the user"})):
            ok, reason = send_telegram_message(chat_id=1, text="привет", token="t")
        self.assertFalse(ok)
        self.assertIn("blocked", reason)

    def test_dead_token_is_reported(self):
        """Ровно то, на чём поймали: 401 при мёртвом токене."""
        with patch("backend.telegram_delivery.requests.post",
                   return_value=FakeResponse(401, {
                       "ok": False, "error_code": 401, "description": "Unauthorized"})):
            ok, reason = send_telegram_message(chat_id=1, text="привет", token="t")
        self.assertFalse(ok)
        self.assertEqual(reason, "Unauthorized")

    def test_network_failure_is_reported_not_swallowed(self):
        with patch("backend.telegram_delivery.requests.post",
                   side_effect=OSError("connection reset")):
            ok, reason = send_telegram_message(chat_id=1, text="привет", token="t")
        self.assertFalse(ok)
        self.assertIn("connection reset", reason)

    def test_missing_token_or_addressee_never_claims_success(self):
        """Ни токена, ни адресата — отказ БЕЗ похода в сеть.

        Токен не передан — берётся из окружения, поэтому здесь оно вычищается: иначе
        тест ушёл бы в настоящий Telegram с боевым токеном разработчика. `requests.post`
        подменён так, чтобы падать: если сюда всё-таки дойдёт вызов, это видно сразу.
        """
        def must_not_be_called(*_args, **_kwargs):
            raise AssertionError("до сети доходить не должно")

        with patch.dict("os.environ", {"TELEGRAM_Deutsch_BOT_TOKEN": ""}, clear=False), \
             patch("backend.telegram_delivery.requests.post", side_effect=must_not_be_called):
            ok, reason = send_telegram_message(chat_id=1, text="привет", token="")
            self.assertFalse(ok)
            self.assertEqual(reason, "нет токена бота")
            ok, reason = send_telegram_message(chat_id=0, text="привет", token="t")
            self.assertFalse(ok)
            self.assertEqual(reason, "нет адресата")

    def test_batch_reports_who_did_not_get_it(self):
        answers = [FakeResponse(200, {"ok": True}),
                   FakeResponse(200, {"ok": False, "description": "chat not found"})]
        with patch("backend.telegram_delivery.requests.post", side_effect=answers):
            delivered, failures = send_telegram_message_to_all(
                [10, 20], text="привет", token="t")
        self.assertEqual(delivered, 1)
        self.assertEqual(failures, [(20, "chat not found")])

    def test_no_raw_send_message_calls_are_left_in_the_bot(self):
        """Ни одной отправки мимо проверяемой доставки.

        Страж на будущее: новое место, написанное по старому образцу, снова начнёт
        терять сообщения молча — и никто об этом не узнает, потому что молчание и
        успех выглядят одинаково.
        """
        source = BOT_FILE.read_text(encoding="utf-8")
        offenders = [
            f"bot_3.py:{source[:m.start()].count(chr(10)) + 1}"
            for m in re.finditer(r"/sendMessage", source)
        ]
        self.assertEqual(
            offenders, [],
            "Отправка в обход backend/telegram_delivery.py — сообщение снова может "
            "потеряться молча:\n" + "\n".join(offenders),
        )


if __name__ == "__main__":
    unittest.main()
