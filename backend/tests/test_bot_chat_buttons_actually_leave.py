# -*- coding: utf-8 -*-
"""Кнопки «перейти в чат с ботом» правда уводят из мини-аппа.

ПОВОД. Замер на живом телефоне владельца 28.08.2026 (видео): кнопка «Открыть чат с
ботом» на экране очереди не делала ВИДИМО ничего.

Причина не в ссылке. openTelegramLink отрабатывает честно, но мини-апп остаётся НА
ЭКРАНЕ, поверх открытого чата. Владелец сам это и описал: «внизу открыт Telegram, а
сверху я открыл приложение… я жму открыть чат с ботом, а я уже по факту в чате».
Со стороны это неотличимо от мёртвой кнопки.

Таких кнопок в проекте нашлось ЧЕТЫРЕ, и все были написаны одинаково. Это класс, а
не один случай, поэтому реализация теперь одна — frontend/src/telegramNav.js:
  · экран очереди и «доступ закрыт» (main.jsx);
  · «Запросить доступ к боту» гостю, пришедшему по ссылке «Поделиться» (DeepAnalysis);
  · «Открыть бота» в игре, открытой из группы (AnswerOverlay);
  · «Открыть бота» в словаре при заблокированном боте (DictionaryOverlay).

⚠ ГРАНИЦА, КОТОРУЮ НЕЛЬЗЯ ПЕРЕЙТИ: закрывать мини-апп нельзя для ссылок
`t.me/share/url` (окно «поделиться») и для `?startapp=` (переоткрытие приложения на
другом экране) — оттуда человек должен ВЕРНУТЬСЯ в приложение. Это тоже стережётся.
"""
import pathlib
import unittest

КОРЕНЬ = pathlib.Path(__file__).resolve().parents[2] / "frontend" / "src"


def читать(*части) -> str:
    return (КОРЕНЬ.joinpath(*части)).read_text(encoding="utf-8")


class РеализацияОднаНаВесьПроект(unittest.TestCase):

    def setUp(self):
        self.модуль = читать("telegramNav.js")

    def test_модуль_и_открывает_ссылку_и_закрывает_приложение(self):
        self.assertIn("openTelegramLink(httpsUrl)", self.модуль)
        self.assertIn("tg.close()", self.модуль)

    def test_вне_telegram_приложение_не_закрывается(self):
        """Там закрывать нечего, и обычный переход работает сам."""
        хвост = self.модуль[self.модуль.index("// Вне Telegram"):]
        self.assertIn("window.open(httpsUrl", хвост)
        self.assertNotIn("close()", хвост)


class ВсеЧетыреКнопкиБерутОбщуюРеализацию(unittest.TestCase):

    def test_экран_очереди_и_отказа(self):
        текст = читать("main.jsx")
        self.assertIn("from './telegramNav.js'", текст)
        self.assertIn("openBotChat(uname, 'queue')", текст)
        self.assertIn("openBotChat(uname, 'access')", текст)

    def test_гость_пришедший_по_ссылке_поделиться(self):
        текст = читать("dictionary", "DeepAnalysis.jsx")
        self.assertIn("from '../telegramNav.js'", текст)
        self.assertIn("openBotChat(botUsername, 'access')", текст)
        self.assertNotIn("tg.openTelegramLink(`https://t.me/${botUsername}", текст)

    def test_игра_открытая_из_группы(self):
        текст = читать("answer", "AnswerOverlay.jsx")
        self.assertIn("from '../telegramNav.js'", текст)
        self.assertIn("openBotChat(hint.bot_username", текст)

    def test_словарь_при_заблокированном_боте(self):
        текст = читать("dictionary", "DictionaryOverlay.jsx")
        начало = текст.index("function openBotLink(")
        функция = текст[начало:начало + 1800]
        self.assertIn("tgApp.close?.()", функция)


class ГраницаГдеЗакрыватьНЕЛЬЗЯ(unittest.TestCase):
    """Окно «поделиться» и переоткрытие приложения на другом экране — человек обязан
    вернуться в приложение. Закрыть его там значит сломать рабочую вещь."""

    def test_окно_поделиться_не_закрывает_приложение(self):
        for путь in (("dictionary", "DeepAnalysis.jsx"), ("dictionary", "DictionaryOverlay.jsx")):
            текст = читать(*путь)
            for кусок in текст.split("t.me/share/url")[1:]:
                self.assertNotIn("close()", кусок[:400],
                                 f"{путь}: из окна «поделиться» человек должен вернуться")

    def test_переоткрытие_приложения_через_startapp_не_закрывается(self):
        текст = читать("dictionary", "DictionaryOverlay.jsx")
        начало = текст.index("function openBotLink(")
        функция = текст[начало:начало + 1800]
        # Закрытие обязано стоять под условием «переход в чат», то есть без startapp.
        self.assertIn("if (!start) {", функция)


if __name__ == "__main__":
    unittest.main()
