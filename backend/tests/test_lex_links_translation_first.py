# -*- coding: utf-8 -*-
"""Первой связью слова встаёт ПЕРЕВОД, а не толкование.

ЧТО ЛОМАЛОСЬ 31.08.2026. Строка «ПЕРЕВОД» в быстром словаре — это первая по рангу связь
слова с русской стороной, а связи строились из `card.meanings`. У промпта это поле
«значение», и туда законно попадает толкование: «Verschwörer» показывал человеку
«человек, участвующий в заговоре», хотя в той же карточке рядом лежало «заговорщик».

Порядок собирает `card_link_values`, и проверяется он здесь — без базы.
"""
import unittest

from backend.lex_units import card_link_values, looks_like_example_not_translation


def значения(card, kind="word"):
    return [item["value"] for item in card_link_values(card, kind=kind)]


class TestПереводПередТолкованием(unittest.TestCase):
    def test_перевод_идёт_раньше_толкования(self):
        """Живой случай владельца: «Verschwörer»."""
        card = {
            "translations": [
                {"value": "заговорщик", "is_primary": True},
                {"value": "конспиратор"},
            ],
            "meanings": {
                "primary": {"value": "человек, участвующий в заговоре",
                            "context": "политический или криминальный заговор"},
                "secondary": [{"value": "тот, кто тайно организует преступные действия"}],
            },
        }
        self.assertEqual(значения(card)[0], "заговорщик")

    def test_толкование_не_пропадает(self):
        """Значения не теряются — они идут следом и остаются связями рангом ниже."""
        card = {
            "translations": [{"value": "заговорщик"}],
            "meanings": {"primary": {"value": "человек, участвующий в заговоре"}},
        }
        self.assertIn("человек, участвующий в заговоре", значения(card))

    def test_без_переводов_берём_значения(self):
        """Списка переводов нет — работаем по значениям, как раньше."""
        card = {"meanings": {"primary": {"value": "затишье"},
                             "secondary": [{"value": "застой"}]}}
        self.assertEqual(значения(card), ["затишье", "застой"])

    def test_пример_в_переводах_снимает_заслон(self):
        """У «abbestellen» в поле переводов лежит целое предложение. Порядок его не
        выбрасывает — это делает заслон при записи связи; проверяем оба конца, иначе
        первой связью встало бы «Ваша подписка на рассылку успешно отменена»."""
        card = {
            "translations": [{"value": "Ваша подписка на рассылку успешно отменена"}],
            "meanings": {"primary": {"value": "отменять (заказ, подписку)"}},
        }
        values = значения(card)
        self.assertTrue(looks_like_example_not_translation(values[0]))
        self.assertIn("отменять", values)

    def test_повтор_не_теряет_пояснение(self):
        """Одно и то же слово в обоих полях — берём один раз, пояснение сохраняем."""
        card = {
            "translations": [{"value": "застой"}],
            "meanings": {"primary": {"value": "застой", "context": "в делах, в экономике"}},
        }
        items = card_link_values(card)
        self.assertEqual([i["value"] for i in items], ["застой"])
        self.assertIn("в делах, в экономике", items[0]["note"])

    def test_у_фразы_порядок_прежний(self):
        """У оборота `translations[]` держит обрывок или пример, а перевод лежит в
        значении: «in der Dämmerung» → «в сумерках», а не «сумерки». Порядок для фразы
        трогать нельзя — замерено на 100 живых единицах 31.08.2026."""
        card = {
            "translations": [{"value": "сумерки"}],
            "meanings": {"primary": {"value": "в сумерках"}},
        }
        self.assertEqual(значения(card, kind="phrase")[0], "в сумерках")
        self.assertEqual(значения(card, kind="word")[0], "сумерки")

    def test_пустая_карточка(self):
        self.assertEqual(card_link_values({}), [])
        self.assertEqual(card_link_values(None), [])


if __name__ == "__main__":
    unittest.main()
