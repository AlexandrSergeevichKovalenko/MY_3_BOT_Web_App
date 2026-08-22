# -*- coding: utf-8 -*-
"""«Отправлено» — это ДОШЛО, а не «мы вызвали отправку».

ОТКУДА ЗАДАЧА. Группа Г из главного плана: отметка о выполнении ставится не по факту.
Владелец месяц не видел отчёт о фразах, потому что «отправлено» писалось в момент
вызова: Telegram отвечает на отказ не исключением, а телом `{"ok": false, …}`, и такой
отказ никем не читался. С мёртвым токеном код печатал «отправлено 1».

ЗАМЕР 22.08.2026 по всему коду: 38 мест отправки в Telegram. Из них шесть считали
успехом сам вызов, а два самых злых при этом ещё и растили счётчик, который потом ехал
в отчёт работы как «выполнено»:

    dictionary_layer_report   sent += 1 сразу после вызова, ответ не читался вовсе
    article_review            отказ ПИСАЛСЯ В ЛОГ строкой выше, а счётчик всё равно рос
    article_retire_review     то же самое
    admin_economics           разбор расходов
    карточка «Ярлыка»         уходит ЧЕЛОВЕКУ, отказ не виден никому
    тревога о злоупотреблении здесь стояло `except Exception: pass`

Тест держит класс: у этих мест отправка идёт через `telegram_delivery`, которая
возвращает честный результат, а счётчик растёт только по факту доставки.
"""
from __future__ import annotations

import inspect
import re
import unittest

ЧЕСТНАЯ_ДОСТАВКА = re.compile(r"send_telegram_message(_to_all)?\s*\(")


class СчётчикРастётТолькоПоФактуTest(unittest.TestCase):
    def test_отчёт_о_слое_словаря(self):
        from backend import dictionary_layer_report as m
        src = inspect.getsource(m.send_dictionary_layer_report)
        self.assertRegex(src, ЧЕСТНАЯ_ДОСТАВКА)
        self.assertNotIn("sent += 1", src,
                         "счётчик снова считает вызовы, а не доставки")

    def test_разбор_артиклей(self):
        from backend import article_review as m
        src = inspect.getsource(m.send_article_review_dm)
        self.assertRegex(src, ЧЕСТНАЯ_ДОСТАВКА)
        self.assertIn("если все_дошли", src.replace("if все_дошли", "если все_дошли"),
                      "счётчик растёт независимо от того, дошли ли карточки")

    def test_разбор_снятых_слов(self):
        from backend import article_retire_review as m
        src = inspect.getsource(m.send_retire_review_dm)
        self.assertRegex(src, ЧЕСТНАЯ_ДОСТАВКА)
        self.assertIn("если все_дошли", src.replace("if все_дошли", "если все_дошли"))

    def test_разбор_расходов(self):
        from backend import admin_economics as m
        src = inspect.getsource(m)
        i = src.index("def send_cost_breakdown_report")
        кусок = src[i:i + 3000]
        self.assertRegex(кусок, ЧЕСТНАЯ_ДОСТАВКА)
        self.assertIn("все_части_дошли", кусок)


class ОтправкаЧеловекуНеМолчитTest(unittest.TestCase):
    """Сообщение человеку, потерянное молча, — потеря для человека, а не для лога."""

    def test_карточка_ярлыка(self):
        from backend import backend_server as B
        src = inspect.getsource(B)
        i = src.index("shortcut DM send failed")
        кусок = src[max(0, i - 2200):i]
        self.assertRegex(кусок, ЧЕСТНАЯ_ДОСТАВКА)
        self.assertIn('тело.get("ok")', кусок,
                      "картинка уходит без взгляда на ответ Telegram")

    def test_тревога_о_злоупотреблении_не_глушится(self):
        from backend import backend_server as B
        src = inspect.getsource(B)
        i = src.index("тревога о злоупотреблении Ярлыком")
        кусок = src[max(0, i - 900):i + 600]
        self.assertRegex(кусок, ЧЕСТНАЯ_ДОСТАВКА)
        self.assertNotIn("except Exception:\n                pass", кусок)


class ЧестнаяДоставкаОтличаетОтказОтУспехаTest(unittest.TestCase):
    """Сама основа: HTTP 200 с `ok: false` — это НЕ доставлено."""

    def _ответ(self, код, тело):
        class _Ответ:
            status_code = код
            content = b"x"

            def json(self_inner):
                return тело
        return _Ответ()

    def test_двести_с_отказом_в_теле_считается_недоставленным(self):
        from unittest import mock
        from backend import telegram_delivery as d
        ответ = self._ответ(200, {"ok": False, "description": "bot was blocked by the user"})
        with mock.patch.object(d.requests, "post", lambda *a, **k: ответ):
            дошло, почему = d.send_telegram_message(chat_id=1, text="привет", token="t")
        self.assertFalse(дошло)
        self.assertIn("blocked", почему)

    def test_настоящий_успех_считается_доставленным(self):
        from unittest import mock
        from backend import telegram_delivery as d
        ответ = self._ответ(200, {"ok": True, "result": {"message_id": 5}})
        with mock.patch.object(d.requests, "post", lambda *a, **k: ответ):
            дошло, почему = d.send_telegram_message(chat_id=1, text="привет", token="t")
        self.assertTrue(дошло)
        self.assertEqual(почему, "")


if __name__ == "__main__":
    unittest.main()
