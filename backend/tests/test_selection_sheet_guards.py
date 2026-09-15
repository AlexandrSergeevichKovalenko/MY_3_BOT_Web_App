"""Окно «Разбор слова» поверх видео: шапка не врёт, и видео под окном не запускается.

Повод — владелец, 15.09.2026, два экрана подряд.

ДЕФЕКТ 1. Тап по слову «Schleäuserkorridore» (в субтитрах ролика опечатка, лишняя ä).
Крупно в шапке стояло «Коридоры Шлейера» с подписью «машинный перевод — этого слова нет
в словаре», а НИЖЕ в том же окне лежала верная статья «коридор для нелегальной миграции»,
и она же верно ушла в словарь («der Schleuserkorridor»). Причина: окно наполняют два
запроса. Этап 1 (машинный переводчик) писал шапку, этап 2 (словарная статья) клал только
dictionaryItem и шапку не трогал — то есть шапка была ЗАПОМНЕННЫМ значением, а не выводом
из статьи. В быстром словаре (DictionaryOverlay.jsx) шапка вычисляемая и этого дефекта
нет; здесь этих строк просто не было.

ДЕФЕКТ 2. «Провожу пальцем по этому экрану — и видео запускается само». Тап по слову
субтитров ставит видео на паузу и помечает youtubePausedBySelectionRef; clearSelection
трактует снятие выделения как «карточку закрыли» и возвращает playVideo. А слушатель
pointerdown висит на документе в фазе захвата и считает «касанием мимо карточки» любую
точку экрана, кроме самой карточки выделения, — окно разбора в исключение не входило.
Механизм старый (март 2026), вылезло после 841cf4d7 от 30.08.2026: до него в окне стояли
две строки и прокручивать там было нечего.

Сторожа стоят по исходнику App.jsx — ровно так же, как клиентская половина починки
позиции просмотра (test_youtube_watch_position.py).
"""

import io
import os
import unittest


APP_JSX = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "frontend", "src", "App.jsx",
)


class SelectionSheetHeadTranslationTests(unittest.TestCase):
    """Шапка «ПЕРЕВОД» обязана читать пришедшую статью, а не замерший ответ машины."""

    def setUp(self):
        self.source = io.open(APP_JSX, encoding="utf-8").read()

    def test_head_translation_is_computed_from_the_card(self):
        self.assertIn(
            "const getSelectionGptCardTranslation = () => {",
            self.source,
            "исчез вывод перевода из статьи — шапка снова замрёт на ответе машины",
        )
        self.assertIn(
            "getSelectionGptCardTranslation() || selectionGptData.translation || '—'",
            self.source,
            "шапка перестала предпочитать статью машинному переводу",
        )

    def test_head_translation_never_substitutes_the_german_word(self):
        """Берём targetText, а не getDictionaryDisplayedTranslation.

        У последнего есть хвост `|| sourceText`: не назови статья перевода — в строке
        «ПЕРЕВОД» встало бы само немецкое слово, то есть подстановка вместо ответа.
        Пусто обязано означать пусто: тогда в шапке честно остаётся машинный ответ
        С ПОДПИСЬЮ, а не выдумка.
        """
        self.assertIn(
            "getDictionarySourceTarget(item, direction).targetText",
            self.source,
            "перевод в шапке перестали брать строго из targetText статьи",
        )

    def test_machine_label_goes_dark_once_the_card_has_answered(self):
        """Подпись говорит про то, что человек видит КРУПНО.

        Приехала статья — «этого слова нет в словаре» становится ложью: слово есть,
        мы его показываем.
        """
        self.assertIn(
            "selectionGptData.machine && !selectionGptData.formOf && !getSelectionGptCardTranslation() &&",
            self.source,
            "подпись «машинный перевод» снова висит поверх ответа из словаря",
        )


class SelectionSheetDoesNotStartTheVideoTests(unittest.TestCase):
    """Пока окно разбора открыто, касание экрана не снимает выделение и не будит видео."""

    def setUp(self):
        self.source = io.open(APP_JSX, encoding="utf-8").read()

    def test_pointerdown_respects_the_open_sheet(self):
        self.assertIn(
            "if (selectionGptOpen) return;",
            self.source,
            "слушатель pointerdown снова снимает выделение под открытым окном разбора",
        )

    def test_the_listener_knows_when_the_sheet_opens(self):
        """Проверка бесполезна, если эффект не пересоздаётся при открытии окна:
        замыкание слушателя держало бы selectionGptOpen === false навсегда."""
        self.assertIn(
            "}, [selectionText, selectionPos, selectionGptOpen]);",
            self.source,
            "selectionGptOpen убрали из зависимостей — слушатель не узнает об окне",
        )

    def test_closing_the_sheet_is_what_releases_the_video(self):
        """Возврат видео обязан остаться: убрать сторож и не дать закрытию снять
        выделение — значит поставить видео на паузу навсегда."""
        head = self.source.split("const closeSelectionGptSheet = () => {", 1)
        self.assertEqual(len(head), 2, "исчезла функция закрытия окна разбора")
        body = head[1].split("\n  };", 1)[0]
        self.assertIn(
            "clearSelection();",
            body,
            "закрытие окна перестало снимать выделение — видео останется на паузе",
        )


if __name__ == "__main__":
    unittest.main()
