"""Игра «der/die/das?» не должна спрашивать про форму чужого слова.

У «Bänder» верного ответа нет: это множественное от «das Band», спрашивать надо
про само слово. А вот «die Eltern», «die Masern», «die Kosten» — законный материал:
у них множественное И ЕСТЬ словарная форма, ответ «die» верен. Прогон уборки
вхолостую 29.07.2026 как раз и поймал, что первая версия правила выносила из банка
эти десять полезных слов.
"""

import unittest
from unittest.mock import patch

import backend.german_surface as gs
from backend.article_sprint_generator import is_ambiguous_noun, resolve_article


def _verdict(number, lemma, confidence="high", article=""):
    return {"number": number, "lemma": lemma, "article": article,
            "source": "тест", "confidence": confidence}


class ArticleGamePluralGateTests(unittest.TestCase):
    def test_form_of_another_word_is_kept_out_of_the_game(self):
        with patch.object(gs, "german_surface", return_value=_verdict(gs.PL, "Band")):
            self.assertTrue(is_ambiguous_noun("Bänder"))

    def test_plural_only_word_stays_in_the_game(self):
        """«die Eltern» — словарная форма, а не форма чужого слова."""
        with patch.object(gs, "german_surface", return_value=_verdict(gs.PL, "Eltern")):
            self.assertFalse(is_ambiguous_noun("Eltern"))

    def test_guess_is_not_enough_to_drop_a_word(self):
        with patch.object(gs, "german_surface",
                          return_value=_verdict(gs.PL, "Sturm", confidence="low")):
            self.assertFalse(is_ambiguous_noun("Stürmer"))

    def test_serving_a_plural_never_uses_the_lemma_gender(self):
        """Даже если в банке лежит «das», у множественного артикль «die»."""
        with patch.object(gs, "german_surface", return_value=_verdict(gs.PL, "Band")):
            self.assertEqual(resolve_article("Bänder", "das"), "die")

    def test_singular_serving_is_unchanged(self):
        with patch.object(gs, "german_surface", return_value=_verdict(gs.SG, "Band")):
            self.assertEqual(resolve_article("Band", "das"), "das")


if __name__ == "__main__":
    unittest.main()
