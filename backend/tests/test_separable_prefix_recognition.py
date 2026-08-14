"""Какие глаголы бот считает отделяемыми — от этого зависит, дойдёт ли задание.

Разбор 14.08.2026: 8 готовых заданий из 199 не доходили до людей вообще, потому
что `durch/fort/fertig/bereit` не было в списке отделяемых приставок и квиз по
ним не собирался. Приставки добавлены — вместе с оговоркой про «durch», которая
бывает и неотделяемой.
"""
import unittest

import bot_3


class SeparablePrefixRecognitionTests(unittest.TestCase):
    def test_verbs_that_were_unreachable_are_recognized(self):
        for verb in ("durchsehen", "fertigschreiben", "bereitlegen", "fortfahren"):
            with self.subTest(verb=verb):
                self.assertTrue(bot_3._is_valid_prefix_quiz_verb(verb))

    def test_known_separable_verbs_still_recognized(self):
        for verb in ("annehmen", "aufstehen", "zurückfahren", "fortsetzen", "bereitstellen"):
            with self.subTest(verb=verb):
                self.assertTrue(bot_3._is_valid_prefix_quiz_verb(verb))

    def test_inseparable_durch_verbs_are_rejected(self):
        """«Er durchquert die Wüste», а не «Er quert die Wüste durch». В упражнении
        про отделяемые глаголы таким не место — ни ответом, ни вариантом."""
        for verb in ("durchqueren", "durchsuchen", "durchdringen", "durchlaufen"):
            with self.subTest(verb=verb):
                self.assertFalse(bot_3._is_valid_prefix_quiz_verb(verb))

    def test_plain_and_inseparable_verbs_are_rejected(self):
        for verb in ("arbeiten", "verstehen", "bekommen", "bereiten", "fertigen"):
            with self.subTest(verb=verb):
                self.assertFalse(bot_3._is_valid_prefix_quiz_verb(verb))


if __name__ == "__main__":
    unittest.main()
