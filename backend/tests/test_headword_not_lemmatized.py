"""Существительное в заголовке не отдаётся лемматизатору spaCy.

Находка 29.07.2026. В общем пуле нашлись заголовки «die Felg», «die Abflughall»,
«der Gepäckwag», «die Gefriertruh», «der Wassertropfe». Ответ модели при этом был
ПРАВИЛЬНЫМ везде — и в примерах, и в таблице склонения, и в save_worthy_options
(«die Abflughalle»). Резал наш код: заголовок прогонялся через
`_normalize_german_text` → spaCy de_core_news_sm → token.lemma_.

Замер на живой модели в проде:
    Felge → Felg, Gefriertruhe → Gefriertruh, Abflughalle → Abflughall,
    Gepäckwagen → Gepäckwag, Wassertropfen → Wassertropfe, Scheibe → Scheib
    (а Blume, Halle, Wagen, Tisch, Problem — не тронуты, потому и не замечали).

Правило то же, что и с артиклем: догадка не имеет права переписывать слово.
"""

import unittest
from unittest.mock import patch

import backend.backend_server as bs


class HeadwordNotLemmatizedTests(unittest.TestCase):
    def _headword(self, word, *, pos="noun", article="die"):
        # Лемматизатор нарочно «портит» слово: если его позовут — тест это увидит.
        with patch.object(bs, "_normalize_german_text", side_effect=lambda t: t[:-1]):
            return bs._normalize_saved_german_single_word(word, part_of_speech=pos, article=article)

    def test_noun_keeps_its_ending(self):
        for word, expected in (("Felge", "die Felge"), ("Abflughalle", "die Abflughalle"),
                               ("Gefriertruhe", "die Gefriertruhe"), ("Scheibe", "die Scheibe")):
            with self.subTest(word=word):
                self.assertEqual(self._headword(word), expected)

    def test_noun_with_article_already_written_keeps_its_ending(self):
        self.assertEqual(self._headword("die Abflughalle"), "die Abflughalle")

    def test_plural_headword_is_not_replaced_by_its_lemma(self):
        """Спросили множественное — заголовок остаётся множественным: «die Probleme».
        Слово («das Problem») едет рядом отдельным полем, а не подменяет запрос."""
        self.assertEqual(self._headword("Probleme"), "die Probleme")

    def test_verb_is_still_lemmatized(self):
        """Глаголам лемматизация нужна и работает — трогаем только существительные."""
        with patch.object(bs, "_normalize_german_text", return_value="gehen"):
            self.assertEqual(
                bs._normalize_saved_german_single_word("ging", part_of_speech="verb", article=""),
                "gehen",
            )

    def test_cyrillic_is_never_decorated_as_a_german_headword(self):
        self.assertEqual(self._headword("Понос"), "Понос")


if __name__ == "__main__":
    unittest.main()
