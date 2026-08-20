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

Справочника ДВА, и это не перестраховка. de.wiktionary знает «Pocken» (оспа),
«Windpocken» (ветрянка) и «Putzen» (уборка) только как формы других слов — отдельной
статьи слова у них нет. Все три при этом нормальные существительные, и en.wiktionary
даёт им раздел German → Noun. По одному справочнику правило вынесло бы «ветрянку».

Тест держит главное: законные pluralia tantum (die Eltern, die Kosten, die Leute,
die Ferien) НЕ должны попадать под нож.
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




class TheSecondReferenceRescuesRealWordsTests(unittest.TestCase):
    """Один справочник бывает неполон — осуждать по нему одному нельзя.

    Замер 20.08.2026: de.wiktionary знает «Pocken» (оспа), «Windpocken» (ветрянка)
    и «Putzen» (уборка) ТОЛЬКО как формы других слов — отдельной статьи слова у них
    нет. Все три при этом нормальные немецкие существительные, и en.wiktionary даёт
    им раздел German → Noun (у Windpocken прямо `{{de-noun|fp}}` — законное
    pluralia tantum). Без второго мнения правило вынесло бы из игры «ветрянку».
    """

    DE_FORM = ("== Windpocken ({{Sprache|Deutsch}}) ==\n"
               "{{Wortart|Deklinierte Form|Deutsch}}\n"
               "{{Grundformverweis Dekl|Windpocke}}\n")
    EN_NOUN = "==German==\n===Noun===\n{{de-noun|fp}}\n# [[chickenpox]]\n"
    EN_NOTHING = "==English==\n===Noun===\n"
    # У формы раздел «Noun» ТОЖЕ есть — отличает только пометка.
    EN_FORM = ("==German==\n===Noun===\n{{head|de|noun form|g=n}}\n"
               "# {{inflection of|de|Band||nom//acc//gen|p}}\n")

    def _verdict(self, en_page):
        pages = iter([{"Windpocken": self.DE_FORM}, {"Windpocken": en_page}])
        with patch.object(hw, "_cached", return_value={}), \
             patch.object(hw, "_remember"), \
             patch.object(hw, "_fetch_wikitext",
                          side_effect=lambda titles, **kw: next(pages)):
            return hw.headword_verdicts(["Windpocken"])["Windpocken"]

    def test_a_word_the_second_reference_knows_is_kept(self):
        self.assertEqual(self._verdict(self.EN_NOUN)[0], hw.LEMMA)

    def test_a_form_neither_reference_calls_a_noun_is_still_cut(self):
        self.assertEqual(self._verdict(self.EN_NOTHING)[0], hw.DECLINED)

    def test_a_noun_section_alone_does_not_rescue_a_form(self):
        # Раздел «Noun» есть и у формы. Смотреть надо на пометку, иначе правило
        # спасает всех подряд — 20.08.2026 так и вышло с «Bänder» и «Sorten».
        self.assertEqual(self._verdict(self.EN_FORM)[0], hw.DECLINED)

    def test_a_word_that_is_also_someone_elses_form_stays(self):
        # У «das Putzen» (уборка) есть И пометка слова, И пометка формы от «die Putze».
        # Слово существует — заголовок законный. Обратный порядок вынес бы его из игры.
        both = ("==German==\n===Noun===\n{{de-noun|n.sg}}\n# {{gerund of|de|putzen}}\n"
                "===Noun===\n{{head|de|noun form}}\n# {{inflection of|de|Putze||dat|p}}\n")
        self.assertEqual(self._verdict(both)[0], hw.LEMMA)

    def test_the_second_reference_is_asked_only_about_the_accused(self):
        # За слово, признанное словарной формой сразу, второй справочник не платим.
        page = "== Feier ({{Sprache|Deutsch}}) ==\n{{Wortart|Substantiv|Deutsch}}\n"
        with patch.object(hw, "_cached", return_value={}), \
             patch.object(hw, "_remember"), \
             patch.object(hw, "_fetch_wikitext", return_value={"Feier": page}) as fetch:
            hw.headword_verdicts(["Feier"])
        self.assertEqual(fetch.call_count, 1)


if __name__ == "__main__":
    unittest.main()
