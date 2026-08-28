# -*- coding: utf-8 -*-
"""Подсказка про род не выдумывает исключений, а предупреждает о ловушке.

ПОВОД. Владелец 28.08.2026 спросил, что делать с «der Kuchen». Программа писала:
«Обычно -chen → всегда das (уменьшительное), но «Kuchen» — исключение: der».

Это ложь. Kuchen не уменьшительное от «Kuch» — он просто КОНЧАЕТСЯ на те же буквы.
Ученик запоминал несуществующее исключение.

ЗАМЕР на всех 5835 словах живого банка артиклей. Правило не сошлось у 19 слов:
  -chen  12 (der Kuchen, der Knochen, der Rachen, der Pfannkuchen, der Käsekuchen…)
  -ment   3 (der Moment, der Zement, der Konsument)
  -ung    2 (der Sprung, der Vorsprung)
  -ling   1 (die Reling)      -lein 1 (der Lein)
  -tion / -heit / -keit / -schaft / -tät / -sion / -ismus — НОЛЬ, не ошибаются вовсе.
НАСТОЯЩИХ исключений среди девятнадцати — ноль. Все образованы не этим суффиксом.

РЕШЕНИЕ ВЛАДЕЛЬЦА: не молчать, а ОБЪЯСНИТЬ ловушку — «человек может видеть der Kuchen,
и у него может закрепиться, что это как-то связано с уменьшительным -chen».
Предупреждённая ловушка учит больше, чем молчание.

⚠ ТА ЖЕ ЛОЖЬ БЫЛА ВТОРОЙ ПОЛОВИНОЙ — В ПРОМПТЕ. Мнемонику обычно сочиняет модель
(детерминированная подсказка — запасной путь), и ей было прямо велено: «Если слово —
ИСКЛЮЧЕНИЕ из правила окончания, явно припиши "исключение, запомни"». Чинить одну
половину без другой бессмысленно.
"""
import os
import unittest

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")

from backend.article_learn import gender_tip, _STRONG_SUFFIX_RULES  # noqa: E402

# Ровно те 19 слов, что нашёл замер по живому банку.
ЛОВУШКИ = [("Kuchen", "der"), ("Knochen", "der"), ("Rachen", "der"), ("Pfannkuchen", "der"),
           ("Käsekuchen", "der"), ("Wangenknochen", "der"), ("Moment", "der"),
           ("Zement", "der"), ("Konsument", "der"), ("Sprung", "der"),
           ("Vorsprung", "der"), ("Reling", "die"), ("Lein", "der")]

СОВПАДАЮЩИЕ = [("Mädchen", "das"), ("Brötchen", "das"), ("Ordnung", "die"),
               ("Freiheit", "die"), ("Möglichkeit", "die"), ("Nation", "die")]


class СловаИсключениеБольшеНеЗвучит(unittest.TestCase):

    def test_ни_одна_из_живых_ловушек_не_названа_исключением(self):
        for слово, артикль in ЛОВУШКИ:
            подсказка = gender_tip(слово, артикль)
            self.assertNotIn("исключение", подсказка.lower(),
                             f"«{слово}» снова объявлено исключением: {подсказка}")

    def test_ловушка_названа_ловушкой_и_даёт_верный_артикль(self):
        подсказка = gender_tip("Kuchen", "der")
        self.assertIn("Ловушка", подсказка)
        self.assertIn("-chen", подсказка)
        self.assertIn("der Kuchen", подсказка)

    def test_ловушка_объясняет_почему_правило_не_годится(self):
        """Владелец: человеку надо понять, а не просто увидеть верный артикль."""
        подсказка = gender_tip("Sprung", "der")
        self.assertIn("НЕ распространяется", подсказка)
        self.assertIn("не образовано", подсказка)

    def test_совпавшее_правило_показывается_как_прежде(self):
        for слово, артикль in СОВПАДАЮЩИЕ:
            подсказка = gender_tip(слово, артикль)
            self.assertTrue(подсказка.startswith("✔️"),
                            f"«{слово}» потерял верную подсказку: {подсказка}")

    def test_ни_одно_твёрдое_правило_не_умеет_говорить_исключение(self):
        """Проверяем ВЕСЬ класс, а не список слов: для каждого твёрдого правила берём
        слово с этим окончанием и НЕ тем артиклем."""
        другой = {"der": "die", "die": "das", "das": "der"}
        for суффикс, ожидаемый, _правило in _STRONG_SUFFIX_RULES:
            слово = ("Test" + суффикс).capitalize()
            подсказка = gender_tip(слово, другой[ожидаемый])
            self.assertNotIn("исключение", подсказка.lower(),
                             f"правило -{суффикс} всё ещё выдумывает исключение")
            self.assertIn("Ловушка", подсказка)


class МодельТожеБольшеНеУчитЛожномуИсключению(unittest.TestCase):
    """Мнемонику обычно сочиняет модель — детерминированная подсказка лишь запасная."""

    def setUp(self):
        from backend.openai_manager import system_message
        self.промпт = system_message["article_gender_hint"]

    def test_модели_запрещено_слово_исключение_для_твёрдых_правил(self):
        self.assertIn("это ЛОВУШКА, а НЕ «исключение»", self.промпт)
        self.assertIn("слова «исключение» быть НЕ ДОЛЖНО", self.промпт)

    def test_у_модели_есть_пример_ловушки(self):
        self.assertIn("der Kuchen", self.промпт)

    def test_мягкие_тенденции_исключения_сохраняют(self):
        """У -e род правда бывает разным: das Auge — настоящее исключение."""
        self.assertIn("das Auge", self.промпт)


if __name__ == "__main__":
    unittest.main()
