"""Регистр немецкого заголовка правится в ДВЕРИ ЗАПИСИ, а не у каждого вызывающего.

Живой случай 13.09.2026: правило `german_headword_case` стояло в обработчике окна
словаря (`backend_server.py`), то есть у одной двери из нескольких. Замер: после его
постановки 19.08.2026 в журнал всё равно легли шесть записей с заглавной — «Genau»,
«Wieso», «Echt», «Wehr»: человек набрал слово с большой буквы в другом входе, где
правила не было. Класс подрастал примерно на две записи в неделю.

Теперь правило стоит в `_create_or_attach_user_dictionary_entry_with_cursor` — там же,
где общая механическая чистка, и через неё проходят все входы: окно словаря, бот,
«Ярлык», импорт.
"""
import unittest

from backend.database import _headword_case_if_single_word


class РегистрПравитсяПоЧастиРечи(unittest.TestCase):
    def test_глагол_и_прилагательное_опускаются(self):
        self.assertEqual(_headword_case_if_single_word("Behaupten", "verb"), "behaupten")
        self.assertEqual(_headword_case_if_single_word("Echt", "adjective"), "echt")
        self.assertEqual(_headword_case_if_single_word("Wieso", "adverb"), "wieso")

    def test_существительное_поднимается(self):
        self.assertEqual(_headword_case_if_single_word("zugehörigkeit", "noun"),
                         "Zugehörigkeit")

    def test_фразу_не_трогаем(self):
        """У фразы и предложения заглавная законна: «Guten Tag», начало предложения."""
        for текст in ("Guten Tag", "Beeren pflücken", "Hätten wir mehr Zeit"):
            with self.subTest(текст=текст):
                self.assertEqual(_headword_case_if_single_word(текст, "verb"), текст)

    def test_без_части_речи_ничего_не_решаем(self):
        """Пустая часть речи — не разрешение: под ней прячутся имена собственные."""
        self.assertEqual(_headword_case_if_single_word("Berlin", ""), "Berlin")

    def test_верное_написание_остаётся_как_есть(self):
        self.assertEqual(_headword_case_if_single_word("behaupten", "verb"), "behaupten")
        self.assertIsNone(_headword_case_if_single_word(None, "verb"))


if __name__ == "__main__":
    unittest.main()
