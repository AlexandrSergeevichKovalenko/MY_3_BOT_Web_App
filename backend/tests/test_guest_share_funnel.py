# -*- coding: utf-8 -*-
"""Гость по ссылке «Поделиться» видит слово и получает приглашение в бот.

ПОВОД. Владелец 28.08.2026 прошёл этот путь вторым аккаунтом и увидел вместо разбора
экран очереди. Разбирая, нашли, что воронка «покажи разбор → позови в бота» не
работала НИКОГДА, ещё до двери:

  · оба гостевых обработчика (`/api/webapp/dictionary/shared` и `.../diff/shared`)
    написаны открытыми для всех — так и сказано у них в описании: «NO allow-list and
    NO ownership check — so a recipient who isn't a bot user yet can still see the
    breakdown and be funnelled to request access»;
  · но общий сторож `enforce_webapp_access` проверяет ВСЕ пути `/api/webapp/`, а этих
    двух в списке исключений не было. Он резал их ДО обработчика. Чужой человек
    упирался в отказ вместо слова, и звать его было уже некуда.

РЕШЕНИЕ ВЛАДЕЛЬЦА 28.08.2026: «пускать смотреть слово всегда, а очередь показывать,
только когда он захочет большего». Разбор уже посчитан и отдаётся на чтение — стоит
нам ноль.

ВТОРАЯ ПОЛОВИНА — ЧТОБЫ ПРИГЛАШЕНИЕ ЗАМЕТИЛИ. Владелец: «не каждый перейдёт, не
каждый заметит». Поймать человека в момент закрытия НЕЛЬЗЯ: у Telegram нет события
до закрытия (проверено по их telegram-web-app.js — ни beforeClose, ни closeRequested,
ни willClose, ноль совпадений). Поэтому зовём, пока он ещё здесь: через 7 секунд
после ПОКАЗА разбора всплывает окно с кнопкой и словами о том, что та же кнопка ждёт
внизу страницы.
"""
import os
import pathlib
import unittest

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")

import backend.backend_server as server  # noqa: E402

ФРОНТ = pathlib.Path(__file__).resolve().parents[2] / "frontend" / "src"


class ГостьВидитСловоДажеПриЗакрытойДвери(unittest.TestCase):

    def test_оба_гостевых_пути_пропускаются_сторожем(self):
        for путь in ("/api/webapp/dictionary/shared", "/api/webapp/dictionary/diff/shared"):
            self.assertIn(путь, server._ACCESS_PUBLIC_WEBAPP_PATHS,
                          f"{путь} обещает открытость в своём описании — сторож обязан его пускать")

    def test_дверь_не_перекрывает_экран_гостя(self):
        """Даже если что-то другое на этом экране ответит отказом, гость обязан
        увидеть слово: он ещё не знает, ради чего ему вставать в очередь."""
        текст = (ФРОНТ / "main.jsx").read_text(encoding="utf-8")
        self.assertIn("const гостьПоСсылке = /^(share_|wdiff_)/i.test(answerStartParam);", текст)
        self.assertIn("if (!гостьПоСсылке) {", текст)

    def test_остальные_пути_сторож_по_прежнему_проверяет(self):
        """Открыли ровно две двери и ни одной лишней."""
        self.assertEqual(len(server._ACCESS_PUBLIC_WEBAPP_PATHS), 4)
        for путь in ("/api/webapp/dictionary", "/api/webapp/dictionary/save"):
            self.assertNotIn(путь, server._ACCESS_PUBLIC_WEBAPP_PATHS)


class ПриглашениеВБотНеЗаметитьНельзя(unittest.TestCase):

    def setUp(self):
        self.экран = (ФРОНТ / "dictionary" / "DeepAnalysis.jsx").read_text(encoding="utf-8")

    def test_окно_всплывает_через_семь_секунд(self):
        self.assertIn("}, 7000);", self.экран)

    def test_отсчёт_идёт_от_показа_разбора_а_не_от_открытия_экрана(self):
        """Пока крутится загрузка, человек ничего не увидел — звать его не за что."""
        начало = self.экран.index("const inviteShownRef")
        кусок = self.экран[начало:начало + 700]
        self.assertIn("phase !== 'done'", кусок)
        self.assertIn("!isGuest", кусок)

    def test_окно_показывается_один_раз(self):
        """Закрыл — больше не лезем: внизу его ждёт та же кнопка."""
        начало = self.экран.index("const inviteShownRef")
        кусок = self.экран[начало:начало + 700]
        self.assertIn("inviteShownRef.current", кусок)

    def test_в_окне_есть_кнопка_и_подсказка_про_кнопку_внизу(self):
        """Владелец: дать перейти сейчас ЛИБО объяснить, что кнопка есть внизу."""
        self.assertIn("🤖 Перейти в бот", self.экран)
        self.assertIn("внизу страницы та же кнопка", self.экран)
        self.assertIn("Дочитаю разбор", self.экран)

    def test_кнопка_окна_ведёт_туда_же_куда_кнопка_внизу(self):
        """Два входа в одну дверь — не две разные логики, которые разойдутся."""
        self.assertIn("setInviteOpen(false); requestAccess();", self.экран)

    def test_окно_только_для_гостя(self):
        self.assertIn("{isGuest && inviteOpen && (", self.экран)


if __name__ == "__main__":
    unittest.main()
