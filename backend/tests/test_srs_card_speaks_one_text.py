# -*- coding: utf-8 -*-
"""Карточка тренировки озвучивает ОДИН текст — и подогрев, и автозапуск, и кнопка.

Повод (23.08.2026). Имя звукового файла считается из текста, поэтому три места, считавшие
текст по-разному, готовили и просили РАЗНЫЕ файлы:

    подогрев   getDictionarySourceTarget -> сторона по графе + артикль + чистка -> «der Tisch»
    автозапуск resolveDictionaryTargetTts -> сырое поле                          -> «Tisch»
    кнопка 🔊  resolveDictionaryTargetTts -> сырое поле                          -> «Tisch»

Итог: подогретый файл никто не просил (оплаченный впустую синтез), а карточка молчала,
пока настоящий файл делался в момент показа. У карточек с перепутанными сторонами было
хуже: подогрев синтезировал РУССКОЕ слово для немецкой карточки.

Фронтового прогона в проекте нет, поэтому страж проверяет исходник: правило должно
остаться в одной точке. Поведение самого правила проверено прогоном настоящих функций
на четырёх случаях (артикль, перепутанные стороны, обычная фраза, обратная карточка) —
все четыре сошлись.
"""
import io
import os
import re
import unittest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
APP_JSX = os.path.join(REPO_ROOT, "frontend", "src", "App.jsx")


def _app_source() -> str:
    return io.open(APP_JSX, encoding="utf-8").read()


class OneRuleForWhatIsSpokenTests(unittest.TestCase):
    def test_the_shared_rule_exists(self):
        self.assertIn("const resolveSrsSpokenTts = useCallback", _app_source(),
                      "исчезло общее правило «что звучит на карточке тренировки»")

    def test_all_three_places_ask_the_shared_rule(self):
        """Подогрев, автозапуск и кнопка динамика — три вызова, не меньше."""
        source = _app_source()
        # Объявление записано как «= useCallback(» и под этот счёт не попадает —
        # считаем только настоящие вызовы: подогрев, автозапуск, кнопка динамика.
        calls = len(re.findall(r"resolveSrsSpokenTts\(", source))
        self.assertGreaterEqual(calls, 3,
                                "кто-то перестал спрашивать общее правило: "
                                "тексты снова разъедутся, а синтез оплатится дважды")

    def test_srs_effects_no_longer_compute_the_text_themselves(self):
        """Ни автозапуск, ни подогрев не должны сами собирать текст из сторон карточки."""
        source = _app_source()
        self.assertNotIn("const _srsSide = resolveDictionaryTargetTts", source,
                         "автозапуск снова считает текст сам")
        self.assertNotIn("const _cardTexts = getDictionarySourceTarget", source,
                         "подогрев снова считает текст сам")


if __name__ == "__main__":
    unittest.main()
