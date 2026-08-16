"""Заголовок в банке артиклей — единственное число, кроме слов, у которых его нет.

Замер 16.08.2026 по живой базе: из 5103 живых слов около двух десятков оказались формой
множественного числа другого слова того же банка — die Zitate (das Zitat), die Mängel
(der Mangel), die Handschuhe (der Handschuh), die Papiere (das Papier), die Beiträge
(der Beitrag), das Fotos (das Foto). Это не массовая беда (0.4%), но каждое такое слово
учит неправильному: у формы множественного числа артикль ВСЕГДА die, знать там нечего,
и вопрос «der/die/das?» становится без ответа.

Владелец сразу назвал границу правила: «если слово используется в основном во
множественном — конечно будет во множественном». Так и есть: die Eltern, die Kosten,
die Leute, die Schulden, die Ferien единственного числа не имеют и остаются как есть.

Дешёвого признака, который отделяет одно от другого, НЕТ — проверял два:
  • «заголовок совпадает с чужим полем мн. числа» даёт ложные срабатывания
    (die Kohle ≠ мн. от der Kohl, die Montage ≠ мн. от der Montag, der Westen ≠ die Weste);
  • «своё поле мн. числа пустое» тоже: пусто и у законных die Schulden, das Streben.
Поэтому решение оставлено проверяющей модели, а в коде закреплено, что её вердикт
доходит по назначению.
"""

import unittest

import backend.article_sprint_generator as gen
import backend.openai_manager as om


class VerifierIsToldTheRuleTests(unittest.TestCase):
    def setUp(self):
        self.verify = om.system_message["article_verify"]
        self.gen = om.system_message["article_noun_gen"]

    def test_generator_is_told_not_to_return_plurals(self):
        self.assertIn("NEVER give a PLURAL form", self.gen)
        self.assertIn("die Zitate", self.gen)

    def test_generator_keeps_plural_only_nouns(self):
        """Иначе правило превратило бы die Eltern в несуществующее «der Elter»."""
        for word in ("die Eltern", "die Kosten", "die Leute", "die Ferien"):
            self.assertIn(word, self.gen, word)

    def test_verifier_rejects_plural_headwords(self):
        self.assertIn("plural_form", self.verify)
        self.assertIn("die Mängel", self.verify)

    def test_verifier_keeps_plural_only_nouns(self):
        for word in ("die Eltern", "die Kosten", "die Leute", "die Schulden"):
            self.assertIn(word, self.verify, word)

    def test_plural_form_is_a_listed_reason(self):
        self.assertIn('"plural_form"', self.verify)


class VerdictReachesTheRightPlaceTests(unittest.TestCase):
    def test_reason_has_a_human_label(self):
        """Отчёт по наполнению читает человек — «plural_form» ему ничего не скажет."""
        self.assertEqual(gen._reason_bucket("форма множественного числа"),
                         "это множественное число — нужно единственное")

    def test_plural_reason_is_not_swallowed_by_the_generic_bucket(self):
        self.assertNotEqual(gen._reason_bucket("форма множественного числа"),
                            gen._reason_bucket("артикль не подтверждён"))

    def test_sense_dependent_words_still_escape_the_blacklist(self):
        """Двуродовым и субстантивированным в стоп-лист нельзя — правило не задето."""
        src = _source_of_verify_branch()
        self.assertIn('reason in ("ambiguous", "person_adjective")', src)

    def test_plural_form_goes_to_the_blacklist_by_spelling(self):
        """В стоп-лист идёт «Mängel», а не корень: единственное «Mangel» должно
        остаться свободным и прийти в набор завтра же."""
        src = _source_of_verify_branch()
        self.assertIn('elif reason == "plural_form"', src)
        self.assertIn('to_blacklist.append((w, "форма множественного числа", theme_key))', src)


def _source_of_verify_branch() -> str:
    import inspect
    return inspect.getsource(gen.fill_theme)


if __name__ == "__main__":
    unittest.main()
