"""Заголовок карточки артиклей — только словарная форма. И только по справочнику.

Два вида брака, оба найдены в живом банке 20.08.2026:

  • «die Die Feier», «die Die Fete» — АРТИКЛЬ ВКЛЕЕН В ЗАГОЛОВОК. Карточка
    показывает ответ прямо в вопросе. Обе прошли не через генератор: слово легло
    в банк непроверенным, приехало владельцу на подтверждение РОДА, он нажал «die» —
    и артикль записался, а написание не посмотрел никто. Поймать это владелец не мог.

  • «die Bänder» (мн. от das Band), «die Sorten» (от die Sorte) — ФОРМА СЛОВА вместо
    словарной. У множественного артикль всегда die: спрашивать нечего, а человек
    видит «Band» и «Bänder» как два разных слова.

ПОЧЕМУ ПРИЗНАК ИМЕННО ТАКОЙ. Разбор 16.08.2026 отказался от дешёвых признаков и
поставил сюда модель тремя голосами, потому что оба опробованных врали: «заголовок
значится чужим полем мн. числа» ловил die Kohle и die Montage, а «своё поле мн.
числа пустое» ловило законные die Schulden. Третий признак не пробовали, и он не
эвристика, а прямое утверждение справочника: формам словоизменения de.wiktionary
заводит отдельные страницы с пометкой `{{Wortart|Deklinierte Form}}`.

Тест держит главное: законные pluralia tantum (die Eltern, die Kosten, die Leute,
die Ferien) НЕ должны попадать под нож — у них своя статья `{{Wortart|Substantiv}}`.
"""
import unittest
from unittest.mock import patch

import backend.article_headword as hw


def _page(*wortarten: str, grundform: str = "") -> str:
    body = "\n".join("{{Wortart|%s|Deutsch}}" % w for w in wortarten)
    if grundform:
        body += "\n{{Grundformverweis Dekl|%s}}" % grundform
    return "== Wort ({{Sprache|Deutsch}}) ==\n" + body + "\n"


class GluedArticleTests(unittest.TestCase):
    """Артикль в заголовке виден без справочника — это чистая строка."""

    def test_an_article_glued_to_the_word_is_stripped(self):
        self.assertEqual(hw.glued_article("Die Feier"), "Feier")
        self.assertEqual(hw.glued_article("der Kumpel"), "Kumpel")
        self.assertEqual(hw.glued_article("Das Fenster"), "Fenster")

    def test_a_normal_headword_is_left_alone(self):
        for word in ("Feier", "Fenster", "Diele", "Dasein", "Dermatologe"):
            self.assertEqual(hw.glued_article(word), "", f"{word} — обычное слово")

    def test_the_verdict_needs_no_reference_for_a_glued_article(self):
        with patch.object(hw, "_cached", return_value={}), \
             patch.object(hw, "_fetch_wikitext") as fetch:
            verdicts = hw.headword_verdicts(["Die Feier"])
        self.assertEqual(verdicts["Die Feier"], (hw.GLUED_ARTICLE, "Feier"))
        fetch.assert_not_called()


class DeclinedFormTests(unittest.TestCase):
    """Форма словоизменения помечена самим справочником."""

    def test_a_plural_form_page_is_recognised(self):
        self.assertEqual(hw.judge_page(_page("Deklinierte Form", grundform="Band")),
                         (hw.DECLINED, "Band"))

    def test_a_plurale_tantum_is_a_proper_headword(self):
        # die Eltern, die Kosten, die Leute, die Ferien — у них СВОЯ статья слова.
        # Владелец 16.08.2026: «если слово используется в основном во множественном —
        # конечно, оно будет во множественном».
        self.assertEqual(hw.judge_page(_page("Substantiv")), (hw.LEMMA, ""))

    def test_a_spelling_that_is_both_a_word_and_a_form_stays(self):
        # «Band» — полноценное слово (в трёх родах!) и одновременно чья-то форма.
        # Своя статья существительного перевешивает.
        self.assertEqual(hw.judge_page(_page("Substantiv", "Deklinierte Form",
                                             grundform="Bande"))[0], hw.LEMMA)

    def test_a_missing_page_is_not_an_accusation(self):
        # Справочник промолчал. Снять слово на этом основании нельзя.
        self.assertEqual(hw.judge_page(None)[0], hw.UNKNOWN)
        self.assertEqual(hw.judge_page("== Wort ({{Sprache|Englisch}}) ==\n")[0], hw.UNKNOWN)

    def test_silence_never_reaches_the_cut_list(self):
        with patch.object(hw, "_cached", return_value={}), \
             patch.object(hw, "_remember"), \
             patch.object(hw, "_fetch_wikitext", return_value={"Autowäsche": None}):
            self.assertEqual(hw.bad_headwords(["Autowäsche"]), {})

    def test_a_declined_form_reaches_the_cut_list(self):
        with patch.object(hw, "_cached", return_value={}), \
             patch.object(hw, "_remember"), \
             patch.object(hw, "_fetch_wikitext",
                          return_value={"Bänder": _page("Deklinierte Form", grundform="Band")}):
            bad = hw.bad_headwords(["Bänder"])
        self.assertEqual(bad["Bänder"]["verdict"], hw.DECLINED)
        self.assertEqual(bad["Bänder"]["lemma"], "Band")


class UnknownIsNeverCachedTests(unittest.TestCase):
    """Молчание справочника не застывает в «проверено»."""

    def test_unknown_verdicts_are_not_remembered(self):
        remembered: list = []
        with patch.object(hw, "_cached", return_value={}), \
             patch.object(hw, "_remember", side_effect=lambda rows: remembered.extend(rows)), \
             patch.object(hw, "_fetch_wikitext", return_value={"Autowäsche": None}):
            hw.headword_verdicts(["Autowäsche"])
        self.assertEqual(remembered, [])


class ConfirmationDoesNotLetABrokenHeadwordIn(unittest.TestCase):
    """Тап владельца по роду не должен впускать негодный заголовок."""

    def test_the_review_answer_explains_the_refusal(self):
        import backend.article_review as review
        with patch("backend.database.confirm_article_noun",
                   return_value={"word": "Die Feier", "article": "die", "theme_key": "",
                                 "rejected": "артикль в заголовке", "suggested": "Feier"}), \
             patch("backend.database.count_unverified_article_nouns", return_value=0):
            text = review.apply_review("die", 1, admin_id=1)
        self.assertIn("в игру не пустил", text)
        self.assertIn("Feier", text)
        self.assertNotIn("записано, слово в тренировке", text)


if __name__ == "__main__":
    unittest.main()
